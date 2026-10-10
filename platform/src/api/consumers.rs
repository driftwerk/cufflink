use crate::auth::DeployAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::nats_manager::consumers::{apply, desired};
use crate::AppState;
use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

const HANDOVERS_SHOWN: i64 = 50;

pub async fn list(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    let service = super::get_service_for_tenant(&state.platform_db, id, user.tenant_id).await?;
    let registered = state
        .registry
        .get_by_slug(&user.tenant_slug, &service.name)
        .await
        .ok_or_else(|| PlatformError::NotFound("the service is not deployed".into()))?;
    let identity = registered.identity();
    let streams = state
        .stream_index
        .refresh(&state.jetstream)
        .await
        .map_err(|e| PlatformError::Unavailable(format!("JetStream is not reachable: {e}")))?;
    let wanted = desired::of_service(&state, &identity, &registered.manifest, &streams).await?;
    let running = state.subscription_tasks.running();

    let mut consumers = Vec::with_capacity(wanted.len());
    for consumer in &wanted {
        let observed = match state.jetstream.get_stream(&consumer.stream).await {
            Ok(stream) => apply::info_of(&stream, consumer.name.as_str())
                .await
                .map_err(|e| PlatformError::Unavailable(e.to_string()))?,
            Err(e) if crate::nats_manager::replicas::is_stream_not_found(e.kind()) => None,
            Err(e) => {
                return Err(PlatformError::Unavailable(format!(
                    "JetStream is not reachable: {e}"
                )))
            }
        };
        consumers.push(json!({
            "handler": consumer.key.handler,
            "subject": consumer.key.subject,
            "stream": consumer.stream,
            "durable": consumer.name.as_str(),
            "bound": consumer.name.is_bound(),
            "filters": consumer.filters,
            "running_here": running.iter().any(|running| running.key == consumer.key),
            "server": observed.map(|info| json!({
                "num_pending": info.num_pending,
                "num_ack_pending": info.num_ack_pending,
                "num_redelivered": info.num_redelivered,
                "ack_floor": info.ack_floor.stream_sequence,
                "delivered": info.delivered.stream_sequence,
                "ack_wait_secs": info.config.ack_wait.as_secs(),
                "max_deliver": info.config.max_deliver,
                "max_ack_pending": info.config.max_ack_pending,
                "filters": apply::observed(&info).filters,
                "created": info.created.to_string(),
            })),
        }));
    }
    let handovers = crate::repo::consumer_handovers::list_for_service(
        &state.platform_db.get().await?,
        service.id,
        HANDOVERS_SHOWN,
    )
    .await?;
    Ok(Json(json!({
        "service": service.name,
        "version": registered.version,
        "consumers": consumers,
        "handovers": handovers,
    })))
}
