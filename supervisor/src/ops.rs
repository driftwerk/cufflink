use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{routing::get, Json, Router};
use cufflink_shutdown::Shutdown;
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::{json, Value};
use std::future::Future;
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub const HEALTH_PATH: &str = "/health";
pub const READY_PATH: &str = "/_cufflink/ready";
pub const STATUS_PATH: &str = "/_cufflink/status";
pub const METRICS_PATH: &str = "/_cufflink/metrics";

pub fn routes<S, Ready, Status, Answer>(
    ready: Ready,
    shutdown: Shutdown,
    token: Option<String>,
    status: Status,
    metrics: PrometheusHandle,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    Ready: Fn() -> bool + Clone + Send + Sync + 'static,
    Status: Fn() -> Answer + Clone + Send + Sync + 'static,
    Answer: Future<Output = Value> + Send + 'static,
{
    let routes = Router::new()
        .route(
            HEALTH_PATH,
            get(|| async { Json(json!({ "status": "ok" })) }),
        )
        .route(
            READY_PATH,
            get(move || {
                let answer = readiness(ready(), shutdown.is_triggered());
                async move { answer }
            }),
        );
    let Some(token) = token else {
        return routes;
    };
    let token = Arc::new(token);
    let status = {
        let token = token.clone();
        move |headers: HeaderMap| {
            let (token, status) = (token.clone(), status.clone());
            async move {
                match bearer_matches(&headers, &token) {
                    true => Json(status().await).into_response(),
                    false => StatusCode::UNAUTHORIZED.into_response(),
                }
            }
        }
    };
    let render = move |headers: HeaderMap| {
        let answer = match bearer_matches(&headers, &token) {
            true => metrics.render().into_response(),
            false => StatusCode::UNAUTHORIZED.into_response(),
        };
        async move { answer }
    };
    routes
        .route(STATUS_PATH, get(status))
        .route(METRICS_PATH, get(render))
}

pub fn readiness(ready: bool, shutting_down: bool) -> (StatusCode, Json<Value>) {
    match ready && !shutting_down {
        true => (StatusCode::OK, Json(json!({ "status": "ready" }))),
        false => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not ready" })),
        ),
    }
}

fn bearer_matches(headers: &HeaderMap, token: &str) -> bool {
    let presented = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map_or("", |(_, credentials)| credentials.trim());
    bool::from(presented.as_bytes().ct_eq(token.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::TcpListener;

    async fn serve(token: Option<&str>) -> String {
        let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
            .build_recorder()
            .handle();
        let app: Router = routes(
            || true,
            Shutdown::new(Duration::from_secs(1)),
            token.map(str::to_string),
            || async { json!({ "children": [] }) },
            metrics,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move { axum::serve(listener, app).await });
        url
    }

    async fn status(url: &str, path: &str, bearer: Option<&str>) -> u16 {
        let request = reqwest::Client::new().get(format!("{url}{path}"));
        let request = match bearer {
            Some(token) => request.bearer_auth(token),
            None => request,
        };
        request.send().await.expect("request").status().as_u16()
    }

    #[tokio::test]
    async fn ops_endpoints_need_the_status_token() {
        let open = serve(None).await;
        assert_eq!(status(&open, STATUS_PATH, None).await, 404);
        assert_eq!(status(&open, METRICS_PATH, Some("x")).await, 404);
        assert_eq!(status(&open, HEALTH_PATH, None).await, 200);
        assert_eq!(status(&open, READY_PATH, None).await, 200);

        let guarded = serve(Some("s3cret")).await;
        for path in ["/status", "/metrics", "/ready"] {
            assert_eq!(
                status(&guarded, path, Some("s3cret")).await,
                404,
                "a child's own {path} is not the runner's"
            );
        }
        for path in [STATUS_PATH, METRICS_PATH] {
            assert_eq!(status(&guarded, path, None).await, 401, "{path}");
            assert_eq!(status(&guarded, path, Some("guess")).await, 401, "{path}");
            assert_eq!(status(&guarded, path, Some("s3cret")).await, 200, "{path}");
        }
    }

    #[test]
    fn ready_only_while_ready_and_not_shutting_down() {
        assert_eq!(readiness(true, false).0, StatusCode::OK);
        assert_eq!(readiness(false, false).0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(readiness(true, true).0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn bearer_tokens_compare_whole() {
        let headers = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, value.parse().expect("header"));
            headers
        };
        assert!(bearer_matches(&headers("Bearer abc"), "abc"));
        assert!(bearer_matches(&headers("bearer abc"), "abc"));
        assert!(!bearer_matches(&headers("Bearer"), "abc"));
        assert!(!bearer_matches(&headers("Bearer ab"), "abc"));
        assert!(!bearer_matches(&headers("Bearer abcd"), "abc"));
        assert!(!bearer_matches(&headers("Basic abc"), "abc"));
        assert!(!bearer_matches(&HeaderMap::new(), "abc"));
    }
}
