use crate::errors::{GuestOriginated, PlatformErrorBody};
use crate::router::strict::split_svc_path;
use crate::AppState;
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::sync::Arc;

const MAX_MESSAGE_BYTES: usize = 64 * 1024;

pub(crate) async fn render(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let service = split_svc_path(request.uri().path())
        .map(|(tenant, service, _)| (tenant.to_string(), service.to_string()));
    let response = next.run(request).await;
    let Some((tenant, service)) = service.filter(|_| rewritable(&response)) else {
        return response;
    };
    match state
        .registry
        .get_by_slug(&tenant, &service)
        .await
        .and_then(|service| service.manifest.errors.clone())
    {
        Some(profile) => rerender(response, &profile.key).await,
        None => response,
    }
}

fn rewritable(response: &Response) -> bool {
    let error = response.status().is_client_error() || response.status().is_server_error();
    let extensions = response.extensions();
    error
        && extensions.get::<GuestOriginated>().is_none()
        && (extensions.get::<PlatformErrorBody>().is_some() || !is_json(response))
}

fn is_json(response: &Response) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"))
}

async fn rerender(response: Response, key: &str) -> Response {
    let (mut parts, body) = response.into_parts();
    let message = match parts.extensions.get::<PlatformErrorBody>() {
        Some(platform) => platform.message.clone(),
        None => text_message(body, parts.status).await,
    };
    let rendered = Json(json!({ key: message })).into_response();
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Response::from_parts(parts, rendered.into_body())
}

async fn text_message(body: axum::body::Body, status: StatusCode) -> String {
    axum::body::to_bytes(body, MAX_MESSAGE_BYTES)
        .await
        .ok()
        .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| status.canonical_reason().unwrap_or("Error").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::PlatformError;

    async fn body_of(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json")
    }

    #[tokio::test]
    async fn platform_errors_and_rejections_take_the_profile_key() {
        let platform = PlatformError::Forbidden("Missing permission: notes:edit".into());
        assert!(rewritable(&platform.into_response()));
        let rendered = rerender(
            PlatformError::NotFound("Not found".into()).into_response(),
            "error_message",
        )
        .await;
        assert_eq!(rendered.status(), StatusCode::NOT_FOUND);
        assert_eq!(rendered.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(
            body_of(rendered).await,
            json!({"error_message": "Not found"})
        );

        let too_large = (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Failed to buffer the request body: length limit exceeded",
        )
            .into_response();
        assert!(rewritable(&too_large));
        let rendered = rerender(too_large, "error_message").await;
        assert_eq!(rendered.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            body_of(rendered).await,
            json!({"error_message": "Failed to buffer the request body: length limit exceeded"})
        );

        let mut not_allowed = StatusCode::METHOD_NOT_ALLOWED.into_response();
        not_allowed
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET,HEAD"));
        let rendered = rerender(not_allowed, "error_message").await;
        assert_eq!(rendered.headers()[header::ALLOW], "GET,HEAD");
        assert_eq!(
            body_of(rendered).await,
            json!({"error_message": "Method Not Allowed"})
        );
    }

    #[test]
    fn guest_responses_successes_and_other_json_are_left_alone() {
        let mut guest = (StatusCode::NOT_FOUND, Json(json!({"error": "gone"}))).into_response();
        guest.extensions_mut().insert(GuestOriginated);
        assert!(!rewritable(&guest));
        let mut guest_text = (StatusCode::BAD_REQUEST, "bad").into_response();
        guest_text.extensions_mut().insert(GuestOriginated);
        assert!(!rewritable(&guest_text));
        assert!(!rewritable(&Json(json!({"ok": true})).into_response()));
        assert!(!rewritable(&(StatusCode::OK, "fine").into_response()));
        assert!(!rewritable(
            &(
                StatusCode::CONFLICT,
                Json(json!({"error": "handler's own"}))
            )
                .into_response()
        ));
    }
}
