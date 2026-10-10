use crate::{router, AppState};
use axum::routing::get;
use axum::Router;
use std::sync::Arc;

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/svc/{tenant}/{service}/_meta/health",
            get(router::meta::health_handler),
        )
        .route(
            "/svc/{tenant}/{service}/_meta/openapi.json",
            get(router::meta::openapi_handler),
        )
        .route(
            "/svc/{tenant}/{service}/_meta/docs",
            get(router::meta::docs_handler),
        )
}
