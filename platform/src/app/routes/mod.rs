pub(super) mod control;
mod crud;
mod custom;
mod meta;

use crate::AppState;
use axum::body::Body;
use axum::http::Request;
use axum::response::Response;
use axum::Router;
use std::convert::Infallible;
use std::sync::Arc;
use tower::util::BoxCloneSyncService;
use tower::ServiceBuilder;

pub(crate) type SvcService = BoxCloneSyncService<Request<Body>, Response, Infallible>;

pub(crate) fn svc_service(state: Arc<AppState>) -> SvcService {
    BoxCloneSyncService::new(
        ServiceBuilder::new()
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::router::error_profile::render,
            ))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::router::strict::route,
            ))
            .service(svc_routes(state)),
    )
}

fn svc_routes(state: Arc<AppState>) -> Router {
    Router::new()
        .merge(custom::routes())
        .merge(meta::routes())
        .merge(crud::routes(&state.config.http))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use crate::shutdown::Shutdown;
    use crate::wasm_runtime::{WasmEngine, WasmLimits};
    use crate::AppState;
    use axum::body::Body;
    use axum::http::Request;
    use std::time::Duration;
    use tower::ServiceExt;

    const NIL: &str = "00000000-0000-0000-0000-000000000000";

    async fn app() -> axum::Router {
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        crate::app::router(AppState::minimal(engine).await)
    }

    #[tokio::test]
    async fn every_route_reaches_its_handler() {
        let app = app().await;
        let jobs = format!("/svc/acme/_jobs/{NIL}");
        let (binary, environment) = (
            format!("/api/runner/native/{NIL}/{}", "0".repeat(64)),
            format!("/api/runner/native/{NIL}/environment"),
        );
        let no_runner_key = r#"{"error":"Invalid runner key"}"#;
        let missing_service = r#"{"error":"Service 'shop' not found"}"#;
        let unauthenticated = r#"{"error":"Missing Authorization header"}"#;
        let json_required = "Expected request with `Content-Type: application/json`";
        let cases: [(&str, &str, u16, Option<&str>, &str); 31] = [
            ("GET", "/svc", 404, None, ""),
            ("GET", "/svc/", 404, None, ""),
            ("GET", "/svc/acme", 404, None, ""),
            ("GET", "/svc/acme/shop", 404, None, ""),
            ("GET", "/svc/acme/shop/", 404, None, ""),
            ("GET", "/svc/acme/shop/items", 404, None, missing_service),
            ("GET", "/svc/acme/shop/items/", 404, None, ""),
            ("PATCH", "/svc/acme/shop/items", 405, Some("GET,HEAD,POST"), ""),
            (
                "PATCH",
                "/svc/acme/shop/items/7",
                405,
                Some("GET,HEAD,PUT,DELETE"),
                "",
            ),
            ("DELETE", "/svc/acme/shop/items/7", 404, None, missing_service),
            ("HEAD", "/svc/acme/shop/items", 404, None, ""),
            (
                "GET",
                "/svc/acme/shop/items/stream",
                500,
                None,
                "Missing request extension: Extension of type `axum::extract::connect_info::ConnectInfo",
            ),
            ("POST", "/svc/acme/shop/items/batch", 415, None, json_required),
            ("GET", "/svc/acme/shop/items/batch", 405, Some("POST"), ""),
            ("POST", "/svc/acme/shop/graphql", 415, None, json_required),
            ("GET", "/svc/acme/shop/graphql/schema", 401, None, unauthenticated),
            (
                "GET",
                "/svc/acme/shop/_meta/health",
                404,
                None,
                r#"{"error":"Service not found"}"#,
            ),
            ("HEAD", "/svc/acme/shop/_meta/health", 404, None, ""),
            ("GET", "/svc/acme/shop/_meta/openapi.json", 401, None, unauthenticated),
            ("GET", "/svc/acme/shop/_meta/docs", 401, None, unauthenticated),
            (
                "POST",
                "/svc/acme/shop/_fn/hello",
                404,
                None,
                r#"{"error":"Service not found"}"#,
            ),
            ("OPTIONS", "/svc/acme/shop/_fn/hello", 200, None, ""),
            ("GET", jobs.as_str(), 401, None, unauthenticated),
            (
                "GET",
                "/svc/acme/_jobs/not-a-uuid",
                400,
                None,
                "Invalid URL: Cannot parse `job_id` with value `not-a-uuid`",
            ),
            ("GET", "/svc/acme/shop/items/7/extra", 404, None, ""),
            ("GET", "/api/version", 200, None, r#"{"api_version":1"#),
            ("GET", "/api/runner/native", 401, None, no_runner_key),
            ("GET", binary.as_str(), 401, None, no_runner_key),
            ("GET", environment.as_str(), 401, None, no_runner_key),
            ("POST", environment.as_str(), 405, Some("GET,HEAD"), ""),
            ("GET", "/nope", 404, None, ""),
        ];
        for (method, path, status, allow, body) in cases {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("infallible");
            assert_eq!(response.status().as_u16(), status, "{method} {path}");
            assert_eq!(
                response
                    .headers()
                    .get("allow")
                    .and_then(|v| v.to_str().ok()),
                allow,
                "{method} {path}"
            );
            let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .expect("body");
            let text = String::from_utf8_lossy(&bytes);
            if body.is_empty() {
                assert!(text.is_empty(), "{method} {path}: {text}");
            } else {
                assert!(text.starts_with(body), "{method} {path}: {text}");
            }
        }
    }

    #[tokio::test]
    async fn cors_answers_a_preflight_to_a_custom_route_without_allow() {
        let response = app()
            .await
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/svc/acme/shop/_fn/hello")
                    .header("origin", "https://app.example.com")
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("infallible");
        assert_eq!(response.status().as_u16(), 200);
        let headers = response.headers();
        assert_eq!(headers["access-control-allow-origin"], "*");
        assert!(headers.contains_key("access-control-allow-methods"));
        assert!(!headers.contains_key("allow"), "{headers:?}");
    }

    #[tokio::test]
    async fn body_limits_are_unchanged() {
        let app = app().await;
        for size in [4 * 1024 * 1024, 64 * 1024 * 1024 + 1] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/svc/acme/shop/_fn/hello")
                        .body(Body::from(vec![b'x'; size]))
                        .expect("request"),
                )
                .await
                .expect("infallible");
            assert_eq!(
                response.status().as_u16(),
                404,
                "a custom route's body is read only once the route resolves: {size} bytes"
            );
        }
        let large_row = format!(r#"{{"blob":"{}"}}"#, "x".repeat(8 * 1024 * 1024));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/svc/acme/shop/items")
                    .header("content-type", "application/json")
                    .body(Body::from(large_row))
                    .expect("request"),
            )
            .await
            .expect("infallible");
        assert_eq!(
            response.status().as_u16(),
            404,
            "data routes keep the router-wide artifact limit"
        );
    }
}
