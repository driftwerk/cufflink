use crate::readiness::{self, Probe};
use crate::AppState;
use axum::{extract::State, response::IntoResponse, Json};
use std::sync::Arc;
use std::time::Duration;

const DB_READY_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn health() -> impl IntoResponse {
    "cufflink-platform ok"
}

pub async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let db_ok = tokio::time::timeout(DB_READY_TIMEOUT, async {
        let client = state.platform_db.get().await.ok()?;
        client.query_one("SELECT 1", &[]).await.ok()
    })
    .await
    .ok()
    .flatten()
    .is_some();
    let snapshot = state.readiness.snapshot();
    readiness::record_state(snapshot.state);
    let (status, body) = readiness::evaluate(
        snapshot,
        Probe {
            db_ok,
            nats_connected: crate::nats_conn::is_connected(&state.nats),
            redis: state.redis.status(),
            registry_version: state.registry.version(),
        },
    );
    (status, Json(body))
}
