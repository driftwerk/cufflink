use crate::errors::PlatformError;
use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use std::convert::Infallible;

const MAX_PARTS: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Part {
    pub name: Option<String>,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub data: Bytes,
}

pub(super) async fn read(body: Body, limit: usize) -> Result<Bytes, Response> {
    Limited::new(body, limit)
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|e| {
            if e.downcast_ref::<LengthLimitError>().is_some() {
                too_large()
            } else {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Failed to buffer the request body: {e}"),
                )
                    .into_response()
            }
        })
}

fn too_large() -> Response {
    rejected("too_large");
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        "Failed to buffer the request body: length limit exceeded",
    )
        .into_response()
}

fn rejected(reason: &'static str) {
    metrics::counter!("cufflink_request_body_rejected_total", "reason" => reason).increment(1);
}

pub(super) async fn multipart(
    headers: &HeaderMap,
    body: Bytes,
    limit: u64,
) -> Result<Vec<Part>, PlatformError> {
    let boundary = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| malformed("expected Content-Type: multipart/form-data".into()))
        .and_then(|content_type| {
            multer::parse_boundary(content_type).map_err(|e| malformed(e.to_string()))
        })?;
    let constraints =
        multer::Constraints::new().size_limit(multer::SizeLimit::new().per_field(limit));
    let stream = futures_util::stream::once(async move { Ok::<_, Infallible>(body) });
    let mut form = multer::Multipart::with_constraints(stream, boundary, constraints);
    let mut parts = Vec::new();
    while let Some(field) = form
        .next_field()
        .await
        .map_err(|e| malformed(e.to_string()))?
    {
        if parts.len() == MAX_PARTS {
            return Err(malformed(format!("more than {MAX_PARTS} parts")));
        }
        let name = field.name().map(str::to_string);
        let filename = field.file_name().map(str::to_string);
        let content_type = field.content_type().map(ToString::to_string);
        let data = field.bytes().await.map_err(|e| malformed(e.to_string()))?;
        parts.push(Part {
            name,
            filename,
            content_type,
            data,
        });
    }
    Ok(parts)
}

fn malformed(reason: String) -> PlatformError {
    rejected("multipart");
    PlatformError::BadRequest(format!("multipart: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[tokio::test]
    async fn an_oversized_body_gets_the_extractors_413() {
        use axum::extract::{DefaultBodyLimit, FromRequest};
        use tower::{Layer, ServiceExt};
        let extractor = DefaultBodyLimit::max(4).layer(tower::service_fn(
            |request: axum::extract::Request| async move {
                Ok::<_, Infallible>(match Bytes::from_request(request, &()).await {
                    Ok(_) => StatusCode::OK.into_response(),
                    Err(rejection) => rejection.into_response(),
                })
            },
        ));
        let expected = extractor
            .oneshot(
                axum::extract::Request::builder()
                    .body(Body::from("12345"))
                    .expect("request"),
            )
            .await
            .expect("infallible");
        let body = |r: Response| axum::body::to_bytes(r.into_body(), 1024);
        let (status, content_type) = (
            expected.status(),
            expected.headers()[header::CONTENT_TYPE].clone(),
        );
        let expected_body = body(expected).await.expect("body");
        let ours = read(Body::from("12345"), 4).await.expect_err("too large");
        assert_eq!(ours.status(), status);
        assert_eq!(ours.headers()[header::CONTENT_TYPE], content_type);
        assert_eq!(body(ours).await.expect("body"), expected_body);
        assert_eq!(
            read(Body::from("1234"), 4).await.expect("fits"),
            Bytes::from_static(b"1234")
        );
    }

    fn form(content_type: &str, body: &str) -> (HeaderMap, Bytes) {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(content_type).expect("header"),
        );
        (headers, Bytes::from(body.replace('\n', "\r\n")))
    }

    const FORM: &str = "--X\nContent-Disposition: form-data; name=\"title\"\n\nQ3 report\n\
        --X\nContent-Disposition: form-data; name=\"file\"; filename=\"r.csv\"\n\
        Content-Type: text/csv\n\na,b\n1,2\n--X--\n";

    #[tokio::test]
    async fn multipart_parts_carry_names_files_and_bytes() {
        let (headers, body) = form("multipart/form-data; boundary=X", FORM);
        let parts = multipart(&headers, body, 1024).await.expect("parses");
        assert_eq!(
            parts,
            vec![
                Part {
                    name: Some("title".into()),
                    filename: None,
                    content_type: None,
                    data: Bytes::from_static(b"Q3 report"),
                },
                Part {
                    name: Some("file".into()),
                    filename: Some("r.csv".into()),
                    content_type: Some("text/csv".into()),
                    data: Bytes::from_static(b"a,b\r\n1,2"),
                },
            ]
        );
    }

    #[tokio::test]
    async fn malformed_or_oversized_forms_are_bad_requests() {
        let too_many: String = (0..=MAX_PARTS)
            .map(|i| format!("--X\nContent-Disposition: form-data; name=\"f{i}\"\n\nv\n"))
            .chain(std::iter::once("--X--\n".to_string()))
            .collect();
        for (content_type, body, limit, needle) in [
            (
                "application/json",
                FORM.to_string(),
                1024,
                "not multipart/form-data",
            ),
            (
                "multipart/form-data; boundary=Y",
                FORM.to_string(),
                1024,
                "multipart:",
            ),
            (
                "multipart/form-data; boundary=X",
                FORM.to_string(),
                4,
                "multipart:",
            ),
            (
                "multipart/form-data; boundary=X",
                too_many,
                1024,
                "more than 100 parts",
            ),
        ] {
            let (headers, body) = form(content_type, &body);
            let err = multipart(&headers, body, limit).await.unwrap_err();
            assert!(
                matches!(&err, PlatformError::BadRequest(m) if m.contains(needle)),
                "{content_type}: {err}"
            );
        }
        let err = multipart(&HeaderMap::new(), Bytes::new(), 1)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("multipart/form-data"), "{err}");
    }
}
