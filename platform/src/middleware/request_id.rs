use axum::http::HeaderMap;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};

pub const HEADER: &str = "x-request-id";

pub fn set_layer() -> SetRequestIdLayer<MakeRequestUuid> {
    SetRequestIdLayer::x_request_id(MakeRequestUuid)
}

pub fn propagate_layer() -> PropagateRequestIdLayer {
    PropagateRequestIdLayer::x_request_id()
}

pub fn request_id(headers: &HeaderMap) -> Option<&str> {
    headers.get(HEADER).and_then(|v| v.to_str().ok())
}
