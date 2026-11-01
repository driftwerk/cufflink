use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use cufflink_platform::api::version::platform_version;
use cufflink_platform::AppState;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{error, info};

pub const EXPECTED_CONSUMERS: usize = 3;
const DB_READY_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Default)]
pub struct WorkerReadiness {
    catalog_loaded: AtomicBool,
    consumers_up: AtomicUsize,
    shutting_down: AtomicBool,
}

pub struct ConsumerUp(Arc<WorkerReadiness>);

impl Drop for ConsumerUp {
    fn drop(&mut self) {
        self.0.consumers_up.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub catalog_loaded: bool,
    pub consumers_up: usize,
    pub shutting_down: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct Probe {
    pub db_ok: bool,
    pub nats_connected: bool,
    pub redis: &'static str,
}

impl WorkerReadiness {
    pub fn consumer_up(self: &Arc<Self>) -> ConsumerUp {
        self.consumers_up.fetch_add(1, Ordering::SeqCst);
        ConsumerUp(Arc::clone(self))
    }

    pub fn mark_catalog_loaded(&self) {
        self.catalog_loaded.store(true, Ordering::SeqCst);
    }

    pub fn mark_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            catalog_loaded: self.catalog_loaded.load(Ordering::SeqCst),
            consumers_up: self.consumers_up.load(Ordering::SeqCst),
            shutting_down: self.shutting_down.load(Ordering::SeqCst),
        }
    }
}

pub fn evaluate(snapshot: Snapshot, probe: Probe) -> (StatusCode, Value) {
    let state = if snapshot.shutting_down {
        "shutting_down"
    } else if snapshot.catalog_loaded && snapshot.consumers_up >= EXPECTED_CONSUMERS {
        "ready"
    } else {
        "starting"
    };
    let status = if state == "ready" && probe.db_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = json!({
        "state": state,
        "db": if probe.db_ok { "ok" } else { "error" },
        "nats": if probe.nats_connected { "connected" } else { "disconnected" },
        "redis": probe.redis,
        "catalog": if snapshot.catalog_loaded { "loaded" } else { "loading" },
        "consumers": { "up": snapshot.consumers_up, "expected": EXPECTED_CONSUMERS },
        "version": platform_version(),
    });
    (status, body)
}

pub struct Probes {
    close: oneshot::Sender<()>,
    served: JoinHandle<()>,
}

impl Probes {
    pub async fn serve(
        app: Arc<AppState>,
        readiness: Arc<WorkerReadiness>,
        port: u16,
    ) -> eyre::Result<Self> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        info!("Worker HTTP listening on {}", listener.local_addr()?);
        let (close, closed) = oneshot::channel();
        let served = tokio::spawn(async move {
            let served = axum::serve(listener, router(app, readiness))
                .with_graceful_shutdown(async {
                    closed.await.ok();
                })
                .await;
            if let Err(e) = served {
                error!(error = %e, "Worker HTTP server ended with an error");
            }
        });
        Ok(Self { close, served })
    }

    pub async fn close(self) {
        self.close.send(()).ok();
        if let Err(e) = self.served.await {
            error!(error = %e, "Worker HTTP server task failed");
        }
    }
}

#[derive(Clone)]
struct HttpState {
    app: Arc<AppState>,
    readiness: Arc<WorkerReadiness>,
}

pub fn router(app: Arc<AppState>, readiness: Arc<WorkerReadiness>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(HttpState { app, readiness })
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "version": platform_version() }))
}

async fn ready(State(state): State<HttpState>) -> impl IntoResponse {
    let db_ok = tokio::time::timeout(DB_READY_TIMEOUT, async {
        let client = state.app.platform_db.get().await.ok()?;
        client.query_one("SELECT 1", &[]).await.ok()
    })
    .await
    .ok()
    .flatten()
    .is_some();
    let (status, body) = evaluate(
        state.readiness.snapshot(),
        Probe {
            db_ok,
            nats_connected: cufflink_platform::nats_conn::is_connected(&state.app.nats),
            redis: state.app.redis.status(),
        },
    );
    (status, Json(body))
}

async fn metrics(State(state): State<HttpState>) -> String {
    cufflink_platform::telemetry::record_pool_gauges(&state.app);
    if let Some(engine) = &state.app.wasm_engine {
        engine.publish_gauges();
    }
    state.app.metrics_handle.render()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(db_ok: bool) -> Probe {
        Probe {
            db_ok,
            nats_connected: true,
            redis: "disabled",
        }
    }

    #[test]
    fn ready_needs_the_catalog_every_consumer_a_database_and_no_shutdown() {
        for catalog_loaded in [true, false] {
            for consumers_up in 0..=EXPECTED_CONSUMERS {
                for shutting_down in [false, true] {
                    for db_ok in [true, false] {
                        let snapshot = Snapshot {
                            catalog_loaded,
                            consumers_up,
                            shutting_down,
                        };
                        let (status, body) = evaluate(snapshot, probe(db_ok));
                        let started = catalog_loaded && consumers_up == EXPECTED_CONSUMERS;
                        assert_eq!(
                            status == StatusCode::OK,
                            started && !shutting_down && db_ok,
                            "{snapshot:?} db_ok={db_ok}: {body}"
                        );
                        let expected_state = match (shutting_down, started) {
                            (true, _) => "shutting_down",
                            (false, true) => "ready",
                            (false, false) => "starting",
                        };
                        assert_eq!(body["state"], expected_state);
                        assert_eq!(body["consumers"]["up"], consumers_up);
                        assert_eq!(body["db"], if db_ok { "ok" } else { "error" });
                        assert_eq!(
                            body["catalog"],
                            if catalog_loaded { "loaded" } else { "loading" }
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn consumer_guards_count_while_alive() {
        let readiness = Arc::new(WorkerReadiness::default());
        let a = readiness.consumer_up();
        let b = readiness.consumer_up();
        assert_eq!(readiness.snapshot().consumers_up, 2);
        drop(a);
        assert_eq!(readiness.snapshot().consumers_up, 1);
        readiness.mark_shutting_down();
        drop(b);
        assert_eq!(
            readiness.snapshot(),
            Snapshot {
                catalog_loaded: false,
                consumers_up: 0,
                shutting_down: true
            }
        );
    }
}
