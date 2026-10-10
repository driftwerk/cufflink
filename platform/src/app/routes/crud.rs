use crate::app::layers::{data_group, HttpLimits};
use crate::{router, AppState};
use axum::routing::{delete, get, post, put};
use axum::Router;
use std::sync::Arc;

pub(super) fn routes(limits: &HttpLimits) -> Router<Arc<AppState>> {
    let data = Router::new()
        .route(
            "/svc/{tenant}/{service}/graphql",
            post(router::graphql::graphql_handler),
        )
        .route(
            "/svc/{tenant}/{service}/graphql/schema",
            get(router::graphql::graphql_schema_handler),
        )
        .route(
            "/svc/{tenant}/{service}/{table}/batch",
            post(router::crud::batch_handler),
        )
        .route(
            "/svc/{tenant}/{service}/{table}",
            get(router::crud::list_handler),
        )
        .route(
            "/svc/{tenant}/{service}/{table}",
            post(router::crud::create_handler),
        )
        .route(
            "/svc/{tenant}/{service}/{table}/{id}",
            get(router::crud::get_handler),
        )
        .route(
            "/svc/{tenant}/{service}/{table}/{id}",
            put(router::crud::update_handler),
        )
        .route(
            "/svc/{tenant}/{service}/{table}/{id}",
            delete(router::crud::delete_handler),
        );
    data_group(data, limits).route(
        "/svc/{tenant}/{service}/{table}/stream",
        get(router::sse::stream_handler),
    )
}
