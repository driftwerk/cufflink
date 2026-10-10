use crate::{api, router, AppState};
use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/svc/{tenant}/_jobs/{job_id}", get(api::jobs::get_job))
        .route(
            "/svc/{tenant}/_jobs/{job_id}/retry",
            post(api::jobs::retry_job),
        )
        .route(
            "/svc/{tenant}/{service}/_fn/{handler}",
            get(router::custom::wasm_route_handler)
                .post(router::custom::wasm_route_handler)
                .put(router::custom::wasm_route_handler)
                .delete(router::custom::wasm_route_handler)
                .patch(router::custom::wasm_route_handler),
        )
}
