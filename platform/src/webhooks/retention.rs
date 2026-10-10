use crate::repo::webhook_deliveries;
use crate::AppState;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const SWEEP_EVERY: Duration = Duration::from_secs(60 * 60);
const BATCH: i64 = 1000;

pub async fn sweep(pool: &deadpool_postgres::Pool, retention: Duration) -> eyre::Result<u64> {
    let before = chrono::Utc::now() - chrono::Duration::from_std(retention)?;
    let client = pool.get().await?;
    let mut deleted = 0;
    loop {
        let batch = webhook_deliveries::prune(&client, before, BATCH).await?;
        deleted += batch;
        if batch < BATCH.unsigned_abs() {
            return Ok(deleted);
        }
    }
}

pub fn start(state: Arc<AppState>) {
    let token = state.shutdown.token();
    let shutdown = state.shutdown.clone();
    shutdown.spawn("webhook_delivery_retention", async move {
        loop {
            match sweep(&state.platform_db, state.config.webhook_delivery.retention).await {
                Ok(0) => {}
                Ok(deleted) => info!(deleted, "Webhook deliveries past their retention deleted"),
                Err(e) => warn!(error = %e, "Webhook deliveries not swept; trying again later"),
            }
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_EVERY) => {}
            }
        }
    });
}
