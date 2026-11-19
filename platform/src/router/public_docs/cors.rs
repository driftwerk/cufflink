use crate::registry::ServiceRegistry;
use crate::router::strict::split_svc_path;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use cufflink_types::PublicDocCors;

const ANY_METHODS: HeaderValue = HeaderValue::from_static("GET,HEAD");
const VARY: HeaderValue = HeaderValue::from_static(
    "origin, access-control-request-method, access-control-request-headers",
);

pub(crate) async fn layer(
    State(registry): State<ServiceRegistry>,
    request: Request,
    next: Next,
) -> Response {
    let Some(mode) = open_mode(&registry, request.uri().path()).await else {
        return next.run(request).await;
    };
    let preflight = request.method() == Method::OPTIONS;
    let answered = headers(mode, request.headers(), preflight);
    let mut response = match preflight {
        true => StatusCode::OK.into_response(),
        false => next.run(request).await,
    };
    replace(response.headers_mut(), answered);
    response
}

async fn open_mode(registry: &ServiceRegistry, path: &str) -> Option<PublicDocCors> {
    if !registry.any_open_docs() {
        return None;
    }
    let (tenant, service, rest) = split_svc_path(path)?;
    let docs = registry.public_docs(tenant, service).await?;
    docs.target(rest)
        .map(|target| docs.cors(&target))
        .filter(|mode| !mode.is_restricted())
}

fn replace(target: &mut HeaderMap, answered: HeaderMap) {
    let stale: Vec<HeaderName> = target
        .keys()
        .filter(|name| *name == header::VARY || name.as_str().starts_with("access-control-"))
        .cloned()
        .collect();
    stale.iter().for_each(|name| {
        target.remove(name);
    });
    target.extend(answered);
}

pub(crate) fn headers(mode: PublicDocCors, request: &HeaderMap, preflight: bool) -> HeaderMap {
    let echoed = |name: HeaderName| request.get(&name).cloned();
    let pairs: Vec<(HeaderName, Option<HeaderValue>)> = match mode {
        PublicDocCors::Any => [
            vec![(
                header::ACCESS_CONTROL_ALLOW_ORIGIN,
                Some(HeaderValue::from_static("*")),
            )],
            match preflight {
                true => vec![
                    (header::ACCESS_CONTROL_ALLOW_METHODS, Some(ANY_METHODS)),
                    (
                        header::ACCESS_CONTROL_ALLOW_HEADERS,
                        Some(HeaderValue::from_static("*")),
                    ),
                ],
                false => vec![],
            },
        ]
        .concat(),
        PublicDocCors::Mirror => [
            vec![
                (
                    header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
                    Some(HeaderValue::from_static("true")),
                ),
                (header::ACCESS_CONTROL_ALLOW_ORIGIN, echoed(header::ORIGIN)),
                (header::VARY, Some(VARY)),
            ],
            match preflight {
                true => vec![
                    (
                        header::ACCESS_CONTROL_ALLOW_METHODS,
                        echoed(header::ACCESS_CONTROL_REQUEST_METHOD),
                    ),
                    (
                        header::ACCESS_CONTROL_ALLOW_HEADERS,
                        echoed(header::ACCESS_CONTROL_REQUEST_HEADERS),
                    ),
                ],
                false => vec![],
            },
        ]
        .concat(),
        PublicDocCors::Restricted | PublicDocCors::Unknown => vec![],
    };
    pairs
        .into_iter()
        .filter_map(|(name, value)| value.map(|value| (name, value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::{ServiceBuilder, ServiceExt};
    use tower_http::cors::CorsLayer;

    async fn very_permissive(request: axum::http::Request<Body>) -> HeaderMap {
        ServiceBuilder::new()
            .layer(CorsLayer::very_permissive())
            .service_fn(|_: axum::http::Request<Body>| async {
                Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
            })
            .oneshot(request)
            .await
            .map(|response| response.headers().clone())
            .unwrap_or_default()
    }

    fn request(method: &str, headers: &[(&str, &str)]) -> axum::http::Request<Body> {
        headers
            .iter()
            .fold(
                axum::http::Request::builder()
                    .method(method)
                    .uri("/svc/acme/catalog-service/docs/public-api/openapi.json"),
                |builder, (name, value)| builder.header(*name, *value),
            )
            .body(Body::empty())
            .expect("request")
    }

    fn sorted(headers: &HeaderMap) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = headers
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        pairs.sort();
        pairs
    }

    #[tokio::test]
    async fn mirror_sends_what_a_very_permissive_cors_layer_sends() {
        let cases: [(&str, &[(&str, &str)]); 4] = [
            ("GET", &[("origin", "https://acme.example.com")]),
            ("GET", &[]),
            (
                "OPTIONS",
                &[
                    ("origin", "https://globex.example.com"),
                    ("access-control-request-method", "GET"),
                    ("access-control-request-headers", "x-api-key, content-type"),
                ],
            ),
            (
                "OPTIONS",
                &[
                    ("origin", "https://globex.example.com"),
                    ("access-control-request-method", "GET"),
                ],
            ),
        ];
        for (method, sent) in cases {
            let expected = very_permissive(request(method, sent)).await;
            let request = request(method, sent);
            let mirrored = headers(
                PublicDocCors::Mirror,
                request.headers(),
                method == "OPTIONS",
            );
            assert_eq!(sorted(&mirrored), sorted(&expected), "{method} {sent:?}");
        }
    }

    #[test]
    fn any_allows_every_origin_without_credentials() {
        let request = request("GET", &[("origin", "https://harbor.example.com")]);
        assert_eq!(
            sorted(&headers(PublicDocCors::Any, request.headers(), false)),
            [("access-control-allow-origin".to_string(), "*".to_string())]
        );
        assert_eq!(
            sorted(&headers(PublicDocCors::Any, request.headers(), true)),
            [
                ("access-control-allow-headers".to_string(), "*".to_string()),
                (
                    "access-control-allow-methods".to_string(),
                    "GET,HEAD".to_string()
                ),
                ("access-control-allow-origin".to_string(), "*".to_string()),
            ]
        );
        assert!(headers(PublicDocCors::Restricted, request.headers(), true).is_empty());
    }

    #[test]
    fn the_answer_keeps_no_access_control_or_vary_header_of_the_inner_layers() {
        let mut target = HeaderMap::new();
        target.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("https://app.example.com"),
        );
        target.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("*"),
        );
        target.insert(header::VARY, HeaderValue::from_static("origin"));
        target.insert(header::ETAG, HeaderValue::from_static("\"a\""));
        replace(
            &mut target,
            headers(PublicDocCors::Any, &HeaderMap::new(), false),
        );
        assert_eq!(
            sorted(&target),
            [
                ("access-control-allow-origin".to_string(), "*".to_string()),
                ("etag".to_string(), "\"a\"".to_string()),
            ]
        );
    }
}
