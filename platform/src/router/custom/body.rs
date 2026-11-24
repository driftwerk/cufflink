use crate::errors::PlatformError;
use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::BytesMut;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use http_body::Body as _;
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

pub(crate) struct Unread {
    pub(crate) received: usize,
    pub(crate) refusal: Response,
}

pub(crate) fn declared(body: &Body) -> usize {
    usize::try_from(body.size_hint().lower()).unwrap_or(usize::MAX)
}

pub(crate) async fn received(body: Body, limit: usize, reserve: usize) -> Result<Bytes, Unread> {
    let mut body = Limited::new(body, limit);
    let mut buffer = BytesMut::with_capacity(reserve.min(limit));
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| Unread {
            received: buffer.len(),
            refusal: refusal(&*e),
        })?;
        if let Ok(data) = frame.into_data() {
            buffer.extend_from_slice(&data);
        }
    }
    Ok(buffer.freeze())
}

pub(super) async fn read(body: Body, limit: usize) -> Result<Bytes, Response> {
    received(body, limit, 0).await.map_err(|unread| {
        if unread.refusal.status() == StatusCode::PAYLOAD_TOO_LARGE {
            rejected("too_large");
        }
        unread.refusal
    })
}

const UNBUFFERED: &str = "Failed to buffer the request body";

fn unread(e: &(dyn std::error::Error + Send + Sync + 'static)) -> PlatformError {
    if e.downcast_ref::<LengthLimitError>().is_some() {
        PlatformError::PayloadTooLarge(format!("{UNBUFFERED}: length limit exceeded"))
    } else {
        PlatformError::BadRequest(format!("{UNBUFFERED}: {e}"))
    }
}

/// Answers `e` as a refusal of the body in the extractor's plain text, and
/// any other error as it answers itself.
pub(crate) fn answer(e: PlatformError) -> Response {
    match e {
        PlatformError::PayloadTooLarge(reason) => {
            (StatusCode::PAYLOAD_TOO_LARGE, reason).into_response()
        }
        PlatformError::BadRequest(reason) if reason.starts_with(UNBUFFERED) => {
            (StatusCode::BAD_REQUEST, reason).into_response()
        }
        e => e.into_response(),
    }
}

fn refusal(e: &(dyn std::error::Error + Send + Sync + 'static)) -> Response {
    answer(unread(e))
}

pub(crate) fn too_large() -> Response {
    answer(PlatformError::PayloadTooLarge(format!(
        "{UNBUFFERED}: length limit exceeded"
    )))
}

pub(crate) fn streamed(body: Body, limit: usize) -> BoxStream<'static, eyre::Result<Bytes>> {
    Limited::new(body, limit)
        .into_data_stream()
        .map_err(|e| eyre::Report::new(unread(&*e)))
        .boxed()
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

        let streamed_refusal = streamed(Body::from("12345"), 4)
            .try_collect::<Vec<_>>()
            .await
            .expect_err("too large")
            .downcast::<PlatformError>()
            .map(answer)
            .expect("a refusal of the body");
        assert_eq!(streamed_refusal.status(), status);
        assert_eq!(
            streamed_refusal.headers()[header::CONTENT_TYPE],
            content_type
        );
        assert_eq!(body(streamed_refusal).await.expect("body"), expected_body);
        assert_eq!(
            streamed(Body::from("1234"), 4)
                .try_collect::<Vec<_>>()
                .await
                .expect("fits")
                .concat(),
            b"1234"
        );
    }

    #[tokio::test]
    async fn a_body_cut_short_or_over_its_limit_says_how_much_of_it_arrived() {
        let frames = |second: Result<Bytes, std::io::Error>| {
            Body::from_stream(futures_util::stream::iter([
                Ok(Bytes::from_static(b"12")),
                second,
            ]))
        };
        let cut = received(frames(Err(std::io::Error::other("reset"))), 16, 16)
            .await
            .expect_err("cut short");
        assert_eq!(
            (cut.received, cut.refusal.status()),
            (2, StatusCode::BAD_REQUEST)
        );
        let over = received(frames(Ok(Bytes::from_static(b"345"))), 4, usize::MAX)
            .await
            .expect_err("over the limit");
        assert_eq!(
            (over.received, over.refusal.status()),
            (2, StatusCode::PAYLOAD_TOO_LARGE)
        );
        assert_eq!(
            received(frames(Ok(Bytes::from_static(b"34"))), 4, 0)
                .await
                .map_err(|unread| unread.received)
                .expect("fits"),
            Bytes::from_static(b"1234")
        );
    }

    #[test]
    fn a_body_declares_the_length_it_is_known_to_have_and_nothing_otherwise() {
        assert_eq!(declared(&Body::from("1234")), 4);
        assert_eq!(declared(&Body::empty()), 0);
        assert_eq!(
            declared(&Body::from_stream(futures_util::stream::iter([Ok::<
                _,
                std::io::Error,
            >(
                Bytes::from_static(b"12"),
            )]))),
            0
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
