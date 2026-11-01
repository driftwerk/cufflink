pub mod dead_letter;
pub mod state;

use crate::errors::{PlatformError, PlatformResult};
use crate::nats_manager::events::LONG_JOBS_SUBJECT_PREFIX;
use crate::registry::RegisteredService;
use crate::repo::wasm_jobs::{self, Enqueued, Failure, NewJob, WasmJob};
use crate::router::custom::strip_credential_headers;
use crate::AppState;
use axum::http::StatusCode;
use axum::Json;
use cufflink_types::JobConfig;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

pub const FEATURES: &[&str] = &[cufflink_types::features::JOBS_V2];

pub const JOB_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

pub const TENANT_BACKLOG_LIMIT: i64 = 1000;

pub fn subject_for(tenant_slug: &str, service_slug: &str, handler: &str) -> String {
    format!(
        "{}{}.{}.{}",
        LONG_JOBS_SUBJECT_PREFIX, tenant_slug, service_slug, handler
    )
}

#[derive(Clone)]
pub struct AuthSnapshot {
    pub subject: String,
    pub claims: Value,
}

impl AuthSnapshot {
    pub fn from_request_auth(request_json: &Value) -> Option<Self> {
        Self::from_auth(request_json.get("auth")?.clone())
    }

    pub fn from_auth(auth: Value) -> Option<Self> {
        let subject = auth.get("sub")?.as_str()?.to_string();
        Some(Self {
            subject,
            claims: auth,
        })
    }
}

pub struct JobIdentity<'a> {
    pub tenant_id: Uuid,
    pub tenant_slug: &'a str,
    pub service_id: Uuid,
    pub service_slug: &'a str,
    pub service_version: i32,
}

pub struct JobRequest<'a> {
    pub handler: &'a str,
    pub request_json: &'a Value,
    pub auth: Option<&'a AuthSnapshot>,
    pub config: &'a JobConfig,
    pub idempotency_key: Option<&'a str>,
    pub schedule_name: Option<&'a str>,
}

impl<'a> JobRequest<'a> {
    pub fn new(
        handler: &'a str,
        request_json: &'a Value,
        auth: Option<&'a AuthSnapshot>,
        config: &'a JobConfig,
    ) -> Self {
        Self {
            handler,
            request_json,
            auth,
            config,
            idempotency_key: None,
            schedule_name: None,
        }
    }
}

fn kept_request(request_json: &Value) -> Value {
    let mut kept = request_json.clone();
    if let Some(headers) = kept.get_mut("headers").and_then(Value::as_object_mut) {
        strip_credential_headers(headers);
    }
    kept
}

fn current_trace_context() -> Option<Value> {
    let mut carrier = HashMap::new();
    crate::telemetry::inject_current_context(&mut carrier);
    (!carrier.is_empty()).then(|| json!(carrier))
}

pub fn enqueued_under(job: &WasmJob) -> Option<opentelemetry::Context> {
    let carrier: HashMap<String, String> = job
        .trace_context
        .as_ref()
        .and_then(Value::as_object)?
        .iter()
        .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_string())))
        .collect();
    crate::telemetry::context_from_map(&carrier)
}

fn refused_for_backlog(unfinished: i64) -> Option<PlatformError> {
    (unfinished >= TENANT_BACKLOG_LIMIT).then(|| {
        PlatformError::TooManyRequests(format!(
            "job backlog limit reached ({TENANT_BACKLOG_LIMIT} pending or running)"
        ))
    })
}

pub async fn record(pool: &deadpool_postgres::Pool, job: NewJob<'_>) -> PlatformResult<Enqueued> {
    let before = match job.idempotency_key {
        Some(key) => wasm_jobs::enqueued_under(pool, job.service_id, key).await?,
        None => None,
    };
    match before {
        Some(job) => Ok(Enqueued::Existing(job)),
        None => {
            let unfinished = wasm_jobs::unfinished(pool, job.tenant_id).await?;
            refused_for_backlog(unfinished).map_or(Ok(()), Err)?;
            wasm_jobs::create(pool, job).await
        }
    }
}

pub async fn enqueue_job(
    state: &Arc<AppState>,
    ident: &JobIdentity<'_>,
    request: JobRequest<'_>,
) -> PlatformResult<Uuid> {
    let JobRequest {
        handler,
        auth,
        config,
        ..
    } = request;
    let enqueued = record(
        &state.platform_db,
        NewJob {
            tenant_id: ident.tenant_id,
            tenant_slug: ident.tenant_slug,
            service_id: ident.service_id,
            service_slug: ident.service_slug,
            service_version: ident.service_version,
            handler,
            request_json: &kept_request(request.request_json),
            auth_subject: auth.map(|a| a.subject.as_str()),
            auth_claims: auth.map(|a| &a.claims),
            max_attempts: i32::try_from(config.max_attempts)
                .unwrap_or(JobConfig::DEFAULT_MAX_ATTEMPTS as i32),
            timeout_secs: i32::try_from(config.timeout_secs)
                .unwrap_or(JobConfig::DEFAULT_TIMEOUT_SECS as i32),
            retry_on: &config.retry_on,
            backoff: config.backoff,
            trace_context: current_trace_context().as_ref(),
            idempotency_key: request.idempotency_key,
            schedule_name: request.schedule_name,
            retain_for: JOB_RETENTION,
        },
    )
    .await?;
    let job = match enqueued {
        Enqueued::Created(job) => job,
        Enqueued::Existing(job) => {
            info!(job_id = %job.id, handler = %handler, "Job already enqueued under its key");
            return Ok(job.id);
        }
    };

    match publish(state, &job).await {
        Ok(()) => info!(
            tenant = %ident.tenant_slug,
            service = %ident.service_slug,
            handler = %handler,
            job_id = %job.id,
            "Job enqueued"
        ),
        Err(e) => warn!(
            job_id = %job.id,
            error = %e,
            "Job recorded but not published; the reclaimer publishes it"
        ),
    }
    Ok(job.id)
}

pub async fn submit_job(
    state: &Arc<AppState>,
    service: &RegisteredService,
    handler: &str,
    request_json: &Value,
    auth_snapshot: Option<AuthSnapshot>,
    config: &JobConfig,
) -> PlatformResult<(StatusCode, Json<Value>)> {
    let ident = JobIdentity {
        tenant_id: service.tenant_id,
        tenant_slug: &service.tenant_slug,
        service_id: service.service_id,
        service_slug: &service.name,
        service_version: service.version,
    };
    let job_id = enqueue_job(
        state,
        &ident,
        JobRequest::new(handler, request_json, auth_snapshot.as_ref(), config),
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(accepted(job_id))))
}

pub(crate) fn accepted(job_id: Uuid) -> Value {
    json!({ "job_id": job_id, "status": "pending" })
}

pub fn count_transition(to: &'static str) {
    metrics::counter!("cufflink_job_transitions_total", "to" => to).increment(1);
}

pub async fn fail_lost(state: &AppState, job: &WasmJob) -> PlatformResult<Option<WasmJob>> {
    let error = state::exhausted_error(job);
    let failure = Failure {
        error: &error,
        class: state::EXHAUSTED_CLASS,
    };
    let failed =
        wasm_jobs::fail_exhausted(&state.platform_db, job.id, failure, JOB_RETENTION).await?;
    if let Some(failed) = &failed {
        count_transition("failed");
        warn!(
            job_id = %failed.id,
            attempts = failed.attempts,
            "Failed a job whose worker was lost on its last attempt"
        );
        dead_letter::file(state, failed).await;
    }
    Ok(failed)
}

pub fn message_id(job: &WasmJob) -> String {
    format!("job:{}:a{}", job.id, job.attempts + 1)
}

pub async fn publish(state: &AppState, job: &WasmJob) -> PlatformResult<()> {
    let mut headers = crate::telemetry::traced_nats_headers();
    headers.insert("Nats-Msg-Id", message_id(job).as_str());
    crate::nats_conn::jetstream_publish_bounded(
        &state.jetstream,
        subject_for(&job.tenant_slug, &job.service_slug, &job.handler),
        Some(headers),
        json!({ "job_id": job.id }).to_string().into(),
    )
    .await
    .map_err(|e| PlatformError::Unavailable(e.to_string()))?;
    wasm_jobs::set_published(&state.platform_db, job.id, true).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::wasm_jobs::JobStatus;

    #[test]
    fn subject_format_is_stable() {
        let s = subject_for("acme", "stock-service", "handle_run_report");
        assert_eq!(s, "cufflink.jobs.acme.stock-service.handle_run_report");
    }

    #[test]
    fn auth_snapshot_extracts_subject_and_claims() {
        let req = json!({ "auth": { "sub": "user-1", "permissions": ["media:edit"] } });
        let snap = AuthSnapshot::from_request_auth(&req).expect("snapshot");
        assert_eq!(snap.subject, "user-1");
        assert_eq!(snap.claims["permissions"][0], "media:edit");
    }

    #[test]
    fn auth_snapshot_none_when_unauthenticated() {
        assert!(AuthSnapshot::from_request_auth(&json!({})).is_none());
        assert!(AuthSnapshot::from_request_auth(&json!({ "auth": {} })).is_none());
        assert!(AuthSnapshot::from_request_auth(&json!({ "auth": "x" })).is_none());
    }

    #[test]
    fn a_kept_request_has_everything_but_its_callers_credentials() {
        let request = json!({
            "method": "POST",
            "handler": "handle_run_report",
            "headers": {
                "authorization": "Bearer eyJ",
                "cookie": "session=1",
                "proxy-authorization": "Basic dTpw",
                "x-api-key": "cfp_live_1",
                "content-type": "application/json",
                "x-request-id": "r-1",
            },
            "body": {"params": {"id": "item-1"}, "authorization": "a body is the caller's own"},
            "tenant": "acme",
            "service": "stock-service",
            "auth": {"sub": "user-1", "permissions": ["items:edit"]},
        });
        assert_eq!(
            kept_request(&request),
            json!({
                "method": "POST",
                "handler": "handle_run_report",
                "headers": {"content-type": "application/json", "x-request-id": "r-1"},
                "body": {"params": {"id": "item-1"}, "authorization": "a body is the caller's own"},
                "tenant": "acme",
                "service": "stock-service",
                "auth": {"sub": "user-1", "permissions": ["items:edit"]},
            })
        );
        for without_headers in [
            json!({"method": "POST", "handler": "h", "body": null, "auth": null}),
            json!({"headers": "not a map"}),
            json!("legacy-string-payload"),
        ] {
            assert_eq!(kept_request(&without_headers), without_headers);
        }
    }

    #[test]
    fn a_tenant_at_its_backlog_limit_is_refused_with_429() {
        assert!(refused_for_backlog(0).is_none());
        assert!(refused_for_backlog(TENANT_BACKLOG_LIMIT - 1).is_none());
        for unfinished in [TENANT_BACKLOG_LIMIT, TENANT_BACKLOG_LIMIT + 1] {
            assert!(matches!(
                refused_for_backlog(unfinished),
                Some(PlatformError::TooManyRequests(message))
                    if message == "job backlog limit reached (1000 pending or running)"
            ));
        }
    }

    #[test]
    fn a_message_is_named_after_the_attempt_it_asks_for() {
        let job = |attempts| state::tests::job(JobStatus::Pending, attempts, 3);
        assert_eq!(
            message_id(&job(0)),
            "job:00000000-0000-0000-0000-000000000000:a1"
        );
        assert_eq!(
            message_id(&job(2)),
            "job:00000000-0000-0000-0000-000000000000:a3"
        );
    }

    #[test]
    fn a_job_runs_under_the_trace_it_was_enqueued_under() {
        use opentelemetry::trace::TraceContextExt;
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let enqueued = |trace_context| WasmJob {
            trace_context,
            ..state::tests::job(JobStatus::Running, 1, 3)
        };
        let parent = enqueued_under(&enqueued(Some(json!({
            "traceparent": "00-97c9e5c13f4a4d0fa53a2273c4be3aad-00f067aa0ba902b7-01",
            "tracestate": "",
        }))))
        .expect("a parent");
        let parent = parent.span().span_context().clone();
        assert_eq!(
            parent.trace_id().to_string(),
            "97c9e5c13f4a4d0fa53a2273c4be3aad"
        );
        assert_eq!(parent.span_id().to_string(), "00f067aa0ba902b7");
        assert!(parent.is_remote());

        for without in [
            None,
            Some(json!({})),
            Some(json!({"traceparent": "garbage"})),
            Some(json!({"traceparent": 7})),
            Some(json!(
                "00-97c9e5c13f4a4d0fa53a2273c4be3aad-00f067aa0ba902b7-01"
            )),
        ] {
            assert!(
                enqueued_under(&enqueued(without.clone())).is_none(),
                "{without:?}"
            );
        }
    }

    #[test]
    fn a_job_enqueued_outside_any_trace_keeps_none() {
        assert_eq!(current_trace_context(), None);
    }
}
