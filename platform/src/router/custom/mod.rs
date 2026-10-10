mod body;
mod dispatch;
mod request;
mod stream;

use crate::errors::PlatformError;
use crate::registry::RegisteredService;
use crate::AppState;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::{Path, Query, Request, State};
use axum::response::{IntoResponse, Response};
use dispatch::CustomCall;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub(crate) use dispatch::{execute, Execution};
#[cfg(test)]
pub(crate) use request::guest_headers;
pub(crate) use request::{strip_credential_headers, StrictTarget};

pub(crate) const FEATURES: &[&str] = &[
    cufflink_types::features::ROUTE_MATCHING,
    cufflink_types::features::ROUTE_LIMITS,
    cufflink_types::features::RESPONSE_V2,
    cufflink_types::features::ERROR_PROFILE,
    cufflink_types::features::SSE_BRIDGE,
    cufflink_types::features::TYPED_ROUTES,
    cufflink_types::features::INTERNAL_POSTURE,
    cufflink_types::features::API_KEY_POSTURE,
    cufflink_types::features::SCOPED_POSTURE,
];

pub async fn wasm_route_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name, handler_name)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let query = match Query::<HashMap<String, String>>::try_from_uri(request.uri()) {
        Ok(Query(query)) => query,
        Err(rejection) => return rejection.into_response(),
    };
    let Some(service) = state
        .registry
        .get_by_slug(&tenant_slug, &service_name)
        .await
    else {
        return PlatformError::NotFound("Service not found".into()).into_response();
    };
    let Some(route) = service.manifest.http_route_index(&handler_name) else {
        return PlatformError::NotFound("Not found".into()).into_response();
    };
    dispatch::run(&state, call(service, route, None, query, request)).await
}

pub(crate) async fn strict_route_handler(
    state: Arc<AppState>,
    service: Arc<RegisteredService>,
    route: usize,
    target: StrictTarget,
    request: Request,
) -> Response {
    match Query::<HashMap<String, String>>::try_from_uri(request.uri()) {
        Ok(Query(query)) => {
            dispatch::run(&state, call(service, route, Some(target), query, request)).await
        }
        Err(rejection) => rejection.into_response(),
    }
}

fn call(
    service: Arc<RegisteredService>,
    route: usize,
    strict: Option<StrictTarget>,
    query: HashMap<String, String>,
    request: Request,
) -> CustomCall {
    let (parts, body) = request.into_parts();
    CustomCall {
        service,
        route,
        strict,
        method: parts.method,
        peer: parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(peer)| *peer),
        uri: parts.uri,
        query,
        headers: parts.headers,
        body,
    }
}
