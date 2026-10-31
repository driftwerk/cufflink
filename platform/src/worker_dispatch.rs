use crate::errors::PlatformError;
use crate::registry::RegisteredService;
use crate::repo::deployments::ExecutionContext;
use crate::wasm_runtime::budget::{ChargedJson, ChargedJsonError, Hold, Refused};
use crate::wasm_runtime::{GuestOutput, WasmEngine};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct WasmJob {
    pub service_id: Uuid,
    pub version: i32,
    pub handler: String,
    pub request_json: String,
    pub tenant_id: Uuid,
    pub tenant_slug: String,
    pub service_schema: String,
    #[serde(default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub publishes: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nested: bool,
}

impl WasmJob {
    pub fn of(service: &RegisteredService, handler: &str, request_json: String) -> Self {
        let identity = service.identity();
        Self {
            service_id: identity.service_id,
            version: service.version,
            handler: handler.to_string(),
            request_json,
            tenant_id: identity.tenant_id,
            tenant_slug: identity.tenant_slug,
            service_schema: identity.schema,
            service_name: Some(identity.service_name),
            publishes: service.manifest.publishes.clone(),
            nested: false,
        }
    }

    pub fn nested(self, nested: bool) -> Self {
        Self { nested, ..self }
    }

    pub fn matches(&self, context: &ExecutionContext) -> bool {
        self.tenant_id == context.tenant_id
            && self.tenant_slug == context.tenant_slug
            && self.service_schema == context.schema
            && self
                .service_name
                .as_ref()
                .is_none_or(|name| *name == context.service_name)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WasmJobResult {
    pub ok: bool,
    pub response: Option<String>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ran: Option<bool>,
}

impl WasmJobResult {
    pub fn answered(response: String) -> Self {
        Self {
            ok: true,
            response: Some(response),
            error: None,
            retry_after_ms: None,
            ran: None,
        }
    }

    pub fn failed(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            response: None,
            error: Some(error.into()),
            retry_after_ms: None,
            ran: None,
        }
    }

    pub fn of_error(error: &crate::wasm_runtime::ExecutionError, message: String) -> Self {
        match error {
            crate::wasm_runtime::ExecutionError::Overloaded {
                retry_after, ran, ..
            } => Self {
                retry_after_ms: Some(u64::try_from(retry_after.as_millis()).unwrap_or(u64::MAX)),
                ran: Some(*ran),
                ..Self::failed(message)
            },
            _ => Self::failed(message),
        }
    }

    pub fn into_payload(self, hold: &mut Hold, engine: &WasmEngine) -> Vec<u8> {
        match ChargedJson::write(&self, hold) {
            Ok(text) => text.into_bytes(),
            Err(ChargedJsonError::Capacity(refused)) => {
                let error = engine.refusal(refused, true);
                Self::of_error(&error, error.to_string()).into_payload(&mut Hold::none(), engine)
            }
            Err(ChargedJsonError::Syntax(e)) => serde_json::json!({
                "ok": false,
                "error": format!("Failed to serialize result: {e}"),
            })
            .to_string()
            .into_bytes(),
        }
    }
}

pub const BUDGET_HEADER: &str = "Cufflink-Budget-Ms";

pub fn requested_budget(
    headers: Option<&async_nats::HeaderMap>,
    max: Duration,
) -> Option<Duration> {
    headers
        .and_then(|headers| headers.get(BUDGET_HEADER))
        .and_then(|value| value.as_str().parse::<u64>().ok())
        .map(|millis| Duration::from_millis(millis).min(max))
}

pub async fn execute_on_worker(
    nats: &async_nats::Client,
    service_name: &str,
    job: WasmJob,
    timeout: Duration,
    hold: &Hold,
    engine: Option<&WasmEngine>,
) -> Result<GuestOutput, PlatformError> {
    let subject = format!("cufflink.wasm.{}.{}", service_name, job.handler);

    let payload = serde_json::to_vec(&job)
        .map_err(|e| PlatformError::Internal(format!("Failed to serialize WASM job: {}", e)))?;

    tracing::debug!(
        "Dispatching WASM job to worker: {} (service={}, handler={})",
        subject,
        service_name,
        job.handler
    );

    let mut headers = crate::telemetry::traced_nats_headers();
    headers.insert(BUDGET_HEADER, timeout.as_millis().to_string().as_str());
    let reply = tokio::time::timeout(
        timeout,
        nats.request_with_headers(subject, headers, payload.into()),
    )
    .await
    .map_err(|_| PlatformError::Internal("WASM worker execution timed out".into()))?
    .map_err(|e| PlatformError::Internal(format!("NATS request failed: {}", e)))?;

    let mut hold = hold.delivering();
    hold.grow(reply.payload.len().saturating_mul(2))
        .map_err(|capacity| match engine {
            Some(engine) => crate::router::map_execution_error(
                "WASM",
                engine.refusal(Refused::Budget(capacity), true),
            ),
            None => PlatformError::Internal(format!("WASM worker reply refused: {capacity:?}")),
        })?;
    let result: WasmJobResult = serde_json::from_slice(&reply.payload)
        .map_err(|e| PlatformError::Internal(format!("Failed to parse worker response: {}", e)))?;
    drop(reply);

    if result.ok {
        let text = result
            .response
            .ok_or_else(|| PlatformError::Internal("Worker returned ok but no response".into()))?;
        hold.shrink_to(text.len());
        Ok(GuestOutput { text, hold })
    } else if let Some(retry_after_ms) = result.retry_after_ms {
        Err(crate::router::retry_later(
            Duration::from_millis(retry_after_ms),
            result.ran.unwrap_or(true),
        ))
    } else {
        let error_msg = result
            .error
            .unwrap_or_else(|| "Unknown worker error".into());
        Err(PlatformError::Internal(format!(
            "WASM worker error: {}",
            error_msg
        )))
    }
}

pub async fn publish_async_job(
    jetstream: &async_nats::jetstream::Context,
    service_name: &str,
    job: WasmJob,
) -> Result<(), PlatformError> {
    let subject = format!("cufflink.wasm.async.{}.{}", service_name, job.handler);

    let payload = serde_json::to_vec(&job)
        .map_err(|e| PlatformError::Internal(format!("Failed to serialize WASM job: {}", e)))?;

    tracing::debug!(
        "Publishing async WASM job: {} (service={}, handler={})",
        subject,
        service_name,
        job.handler
    );

    crate::nats_conn::jetstream_publish_bounded(
        jetstream,
        subject,
        Some(crate::telemetry::traced_nats_headers()),
        payload.into(),
    )
    .await
    .map_err(|e| PlatformError::Internal(format!("JetStream publish failed: {}", e)))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> ExecutionContext {
        ExecutionContext {
            tenant_id: Uuid::nil(),
            tenant_slug: "default".into(),
            service_id: Uuid::nil(),
            service_name: "stock-service".into(),
            schema: "stock_service".into(),
            wasm_hash: None,
            publishes: Some(vec![]),
            extensions: Vec::new(),
        }
    }

    fn job() -> WasmJob {
        WasmJob {
            service_id: Uuid::nil(),
            version: 1,
            handler: "h".into(),
            request_json: "{}".into(),
            tenant_id: Uuid::nil(),
            tenant_slug: "default".into(),
            service_schema: "stock_service".into(),
            service_name: Some("stock-service".into()),
            publishes: None,
            nested: false,
        }
    }

    #[test]
    fn a_nested_route_says_so_on_the_wire_and_an_older_job_is_interactive() {
        let wire = serde_json::to_value(job()).expect("wire");
        assert!(wire.get("nested").is_none(), "{wire}");
        let nested = serde_json::to_value(job().nested(true)).expect("wire");
        assert_eq!(nested["nested"], true);
        let older: WasmJob = serde_json::from_value(wire).expect("older job");
        assert!(!older.nested);
    }

    #[test]
    fn the_callers_budget_travels_in_a_header_and_is_capped() {
        let max = Duration::from_secs(120);
        let headers = |value: &str| {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(BUDGET_HEADER, value);
            headers
        };
        assert_eq!(
            requested_budget(Some(&headers("60000")), max),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            requested_budget(Some(&headers("999999999")), max),
            Some(max)
        );
        assert_eq!(
            requested_budget(Some(&headers("0")), max),
            Some(Duration::ZERO)
        );
        for value in ["-1", "soon"] {
            assert_eq!(
                requested_budget(Some(&headers(value)), max),
                None,
                "{value}"
            );
        }
        assert_eq!(
            requested_budget(Some(&async_nats::HeaderMap::new()), max),
            None
        );
        assert_eq!(requested_budget(None, max), None);
    }

    #[test]
    fn process_job_rejects_wire_identity_mismatch() {
        assert!(job().matches(&context()));
        let forged = [
            WasmJob {
                tenant_id: Uuid::new_v4(),
                ..job()
            },
            WasmJob {
                tenant_slug: "victim".into(),
                ..job()
            },
            WasmJob {
                service_schema: "other_service".into(),
                ..job()
            },
            WasmJob {
                service_name: Some("other-service".into()),
                ..job()
            },
        ];
        assert!(forged.iter().all(|j| !j.matches(&context())));
    }

    #[test]
    fn a_job_runs_in_the_schema_its_deployment_declares() {
        let declared = ExecutionContext {
            schema: "stocks".into(),
            ..context()
        };
        assert!(!job().matches(&declared));
        assert!(WasmJob {
            service_schema: "stocks".into(),
            ..job()
        }
        .matches(&declared));
        assert_eq!(declared.identity().schema, "stocks");
    }

    #[test]
    fn a_job_an_older_platform_enqueued_names_no_service_and_still_runs() {
        let wire = serde_json::json!({
            "service_id": Uuid::nil(),
            "version": 1,
            "handler": "h",
            "request_json": "{}",
            "tenant_id": Uuid::nil(),
            "tenant_slug": "default",
            "service_schema": "stock_service",
        });
        let older: WasmJob = serde_json::from_value(wire).expect("decodes");
        assert_eq!(older.service_name, None);
        assert!(older.matches(&context()));
        let sent = serde_json::to_value(job()).expect("encodes");
        assert_eq!(sent["service_schema"], "stock_service");
        assert_eq!(sent["service_name"], "stock-service");
    }
}
