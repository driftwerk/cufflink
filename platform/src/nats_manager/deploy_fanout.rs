use crate::repo::deployments::Deployment;
use crate::shutdown::Shutdown;
use crate::wasm_runtime::WasmEngine;
use async_nats::Client;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

const DEPLOY_READY_SUBJECT: &str = "cufflink.platform.deploy.ready";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployReady {
    pub service_id: Uuid,
    pub version: i32,
    pub service_name: String,
}

impl DeployReady {
    pub fn of(deployment: &Deployment, service_name: &str) -> Option<Self> {
        deployment.wasm_hash.as_ref().map(|_| Self {
            service_id: deployment.service_id,
            version: deployment.version,
            service_name: service_name.to_string(),
        })
    }
}

pub async fn publish_deploy_ready(nats: &Client, event: &DeployReady) {
    let payload = match serde_json::to_vec(event) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "Failed to serialize deploy-ready event");
            return;
        }
    };

    if let Err(e) = crate::nats_conn::publish_bounded(
        nats,
        DEPLOY_READY_SUBJECT.to_string(),
        None,
        payload.into(),
    )
    .await
    {
        warn!(
            error = %e,
            service = %event.service_name,
            version = event.version,
            "Failed to publish deploy-ready event"
        );
    }
}

pub fn start_listener(
    nats: Client,
    db: deadpool_postgres::Pool,
    engine: Option<WasmEngine>,
    shutdown: &Shutdown,
) {
    let Some(engine) = engine else {
        return;
    };

    let token = shutdown.token();
    shutdown.spawn("deploy_fanout", async move {
        let mut subscriber = match nats.subscribe(DEPLOY_READY_SUBJECT).await {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "Failed to subscribe to deploy fan-out subject");
                return;
            }
        };

        info!("Deploy fan-out listener started");

        loop {
            let msg = tokio::select! {
                _ = token.cancelled() => return,
                msg = subscriber.next() => msg,
            };
            let Some(msg) = msg else {
                break;
            };
            let event: DeployReady = match serde_json::from_slice(&msg.payload) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "Failed to decode deploy-ready event");
                    continue;
                }
            };

            let expected = match crate::repo::deployments::execution_context(
                &db,
                event.service_id,
                event.version,
            )
            .await
            {
                Ok(Some(context)) => context.wasm_hash,
                Ok(None) => {
                    warn!(
                        service = %event.service_name,
                        version = event.version,
                        "Deploy-ready event for an unknown deployment"
                    );
                    continue;
                }
                Err(e) => {
                    warn!(error = %e, "Deployment lookup for deploy-ready event failed");
                    continue;
                }
            };
            match engine
                .get_module(event.service_id, event.version, expected.as_deref())
                .await
            {
                Some(_) => {
                    info!(
                        service = %event.service_name,
                        version = event.version,
                        "Pre-warmed WASM module from fan-out"
                    );
                }
                None => {
                    error!(
                        service = %event.service_name,
                        version = event.version,
                        "WASM module not retrievable after deploy-ready event"
                    );
                }
            }
        }

        warn!("Deploy fan-out listener stopped");
    });
}
