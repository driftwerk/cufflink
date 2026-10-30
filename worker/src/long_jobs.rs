use crate::http::WorkerReadiness;
use crate::jobs;
use chrono::Utc;
use cufflink_platform::errors::PlatformResult;
use cufflink_platform::nats_manager::delivery::{decide, Decision, Outcome, CAPACITY_REASON};
use cufflink_platform::nats_manager::dlq::DeadLetter;
use cufflink_platform::nats_manager::events::LONG_JOBS_STREAM;
use cufflink_platform::nats_manager::pump::{
    self, heartbeat_for, MessageMeta, PumpConfig, Work, WorkLabel,
};
use cufflink_platform::repo::deployments::{execution_context, ExecutionContext};
use cufflink_platform::repo::wasm_jobs::{self, Claim, Failure, Fence, Fenced, Requeue, WasmJob};
use cufflink_platform::router::jobs::state::{
    classify, decide_job, inspect, Attempt, Inspection, JobDecision, JobErrorClass,
};
use cufflink_platform::router::jobs::{
    count_transition, dead_letter, enqueued_under, fail_lost, JOB_RETENTION,
};
use cufflink_platform::wasm_runtime::budget::Hold;
use cufflink_platform::wasm_runtime::{Class, Invocation, InvocationKind, WasmEngine};
use cufflink_platform::AppState;
use cufflink_telemetry::Kind;
use cufflink_types::backoff::Backoff;
use cufflink_types::host::request::JobMeta;
use cufflink_types::AckMode;
use eyre::Context;
use futures_util::future::{select, BoxFuture, Either};
use futures_util::FutureExt;
use serde_json::{json, Value};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

const CONSUMER_NAME: &str = "cufflink-long-jobs";
const UNLIMITED_DELIVERIES: i64 = -1;

pub fn subscribe_pattern() -> String {
    std::env::var("LONG_JOBS_SUBSCRIBE_PATTERN").unwrap_or_else(|_| "cufflink.jobs.>".to_string())
}

pub fn max_concurrent() -> usize {
    std::env::var("MAX_CONCURRENT_JOBS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(4)
}

pub fn ack_wait_secs() -> u64 {
    std::env::var("LONG_JOBS_ACK_WAIT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(60)
}

const MIN_LEASE_SECS: u64 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    pub lease: Duration,
    pub longest_redelivery: Duration,
}

impl Settings {
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let vars = cufflink_db::Vars(var);
        let lease = match vars.parsed("JOB_LEASE_SECS", 60)? {
            secs @ MIN_LEASE_SECS.. => Duration::from_secs(secs),
            _ => eyre::bail!(
                "JOB_LEASE_SECS must be at least {MIN_LEASE_SECS}: a heartbeat every third of it keeps it"
            ),
        };
        Ok(Self {
            lease,
            longest_redelivery: Duration::from_secs(vars.parsed("JOB_NAK_MAX_DELAY_SECS", 60)?),
        })
    }
}

fn requeue_for(wait: Duration, longest_redelivery: Duration) -> Requeue {
    if wait <= longest_redelivery {
        Requeue::Redelivered
    } else {
        Requeue::Republished
    }
}

fn lease_owner() -> String {
    format!(
        "{}:{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "worker".to_string()),
        std::process::id(),
        Uuid::new_v4()
    )
}

const STORE_RETRY: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct LongJobs {
    app_state: Arc<AppState>,
    wasm_engine: WasmEngine,
    owner: Arc<str>,
    lease: Duration,
    longest_redelivery: Duration,
}

pub async fn run(
    app_state: Arc<AppState>,
    wasm_engine: WasmEngine,
    readiness: Arc<WorkerReadiness>,
    settings: Settings,
) -> eyre::Result<()> {
    use async_nats::jetstream::consumer;

    let js = &app_state.jetstream;
    let stream = js
        .get_stream(LONG_JOBS_STREAM)
        .await
        .wrap_err_with(|| format!("Long-jobs stream '{LONG_JOBS_STREAM}' not found"))?;

    let pattern = subscribe_pattern();
    let consumer_name = consumer_name_for(&pattern);
    let ack_wait = Duration::from_secs(ack_wait_secs());
    let concurrency = max_concurrent();

    let consumer = stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(consumer_name.clone()),
            filter_subject: pattern.clone(),
            ack_wait,
            max_deliver: UNLIMITED_DELIVERIES,
            ..Default::default()
        })
        .await?;

    info!(
        "Long-jobs consumer '{}' subscribed to '{}' (concurrency={}, ack_wait={}s)",
        consumer_name,
        pattern,
        concurrency,
        ack_wait.as_secs()
    );

    let _up = readiness.consumer_up();
    let exit = pump::run(
        consumer,
        PumpConfig::sequential(ack_wait).concurrent(concurrency),
        Arc::new(LongJobs {
            app_state: app_state.clone(),
            wasm_engine,
            owner: lease_owner().into(),
            lease: settings.lease,
            longest_redelivery: settings.longest_redelivery,
        }),
        Arc::new(app_state.jetstream.clone()),
        pump::Stop::new(),
        app_state.shutdown.clone(),
    )
    .await;
    jobs::ended("long-jobs", exit)
}

impl Work for LongJobs {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "job",
            subscription: None,
        }
    }

    fn handle(
        &self,
        msg: Arc<async_nats::jetstream::Message>,
        _meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let worker = self.clone();
        async move {
            let _in_flight = jobs::in_flight("long");
            worker.run_job(&msg.payload).await
        }
        .boxed()
    }

    fn decide(&self, outcome: &Outcome, meta: &MessageMeta) -> Decision {
        decide(
            AckMode::OnSuccess,
            outcome,
            meta.delivered,
            UNLIMITED_DELIVERIES,
            &Backoff::explicit(vec![STORE_RETRY]),
            false,
            meta.stream_sequence,
        )
    }

    fn headroom(&self) -> BoxFuture<'_, usize> {
        self.wasm_engine.headroom_for_any_handler().boxed()
    }

    fn dead_letter<'a>(
        &'a self,
        _msg: &'a async_nats::jetstream::Message,
        _meta: &'a MessageMeta,
        _reason: &'a str,
    ) -> BoxFuture<'a, Option<DeadLetter>> {
        async { None }.boxed()
    }
}

fn consumer_name_for(pattern: &str) -> String {
    if pattern == "cufflink.jobs.>" {
        return CONSUMER_NAME.to_string();
    }
    let suffix: String = pattern
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    format!("{}-{}", CONSUMER_NAME, suffix.trim_matches('_'))
}

fn fence_lost(op: &'static str, fence: &Fence) {
    metrics::counter!("cufflink_job_fence_lost_total", "op" => op).increment(1);
    warn!(
        event = "job_fence_lost",
        job_id = %fence.job_id,
        attempt = fence.attempt,
        op,
        "The job's lease was lost; what this attempt did to the job is discarded"
    );
}

fn retry_in(wait: Duration, reason: impl Into<String>) -> Outcome {
    Outcome::Retry {
        after: Some(wait),
        reason: reason.into(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Beat {
    Extended,
    Refused,
    Failed,
}

fn held_after(
    beat: Beat,
    asked: Instant,
    now: Instant,
    held_until: Instant,
    lease: Duration,
) -> Option<Instant> {
    match beat {
        Beat::Extended => Some(asked + lease),
        Beat::Refused => None,
        Beat::Failed => (now < held_until).then_some(held_until),
    }
}

async fn hold_lease<B: Future<Output = Beat>>(
    lease: Duration,
    claimed_at: Instant,
    mut heartbeat: impl FnMut() -> B,
) {
    let every = heartbeat_for(lease);
    let mut beats = tokio::time::interval_at(claimed_at + every, every);
    beats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut held_until = claimed_at + lease;
    loop {
        let next = async {
            beats.tick().await;
            (Instant::now(), heartbeat().await)
        };
        let (asked, beat) = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(held_until) => return,
            answered = next => answered,
        };
        match held_after(beat, asked, Instant::now(), held_until, lease) {
            Some(until) => held_until = until,
            None => return,
        }
    }
}

async fn beat(pool: &deadpool_postgres::Pool, fence: &Fence, lease: Duration) -> Beat {
    let heartbeat = wasm_jobs::heartbeat(pool, fence, lease);
    match tokio::time::timeout(heartbeat_for(lease), heartbeat).await {
        Ok(Ok(Fenced::Held)) => Beat::Extended,
        Ok(Ok(Fenced::Lost)) => Beat::Refused,
        Ok(Err(e)) => {
            warn!(job_id = %fence.job_id, error = %e, "Could not extend a job's lease");
            Beat::Failed
        }
        Err(_) => {
            warn!(job_id = %fence.job_id, "Extending a job's lease timed out");
            Beat::Failed
        }
    }
}

impl LongJobs {
    fn pool(&self) -> &deadpool_postgres::Pool {
        &self.app_state.platform_db
    }

    async fn run_job(&self, payload: &[u8]) -> Outcome {
        let Some(job_id) = parse_job_id(payload) else {
            error!(
                "Invalid long-job payload (no job_id): {} bytes",
                payload.len()
            );
            return Outcome::Invalid {
                reason: "the payload names no job".to_string(),
            };
        };
        let claimed_at = Instant::now();
        match wasm_jobs::claim(self.pool(), job_id, &self.owner, self.lease).await {
            Ok(Some(claim)) => {
                count_transition("running");
                info!(
                    tenant = %claim.job.tenant_slug,
                    service = %claim.job.service_slug,
                    handler = %claim.job.handler,
                    job_id = %claim.job.id,
                    attempt = claim.job.attempts,
                    "Executing long-running job"
                );
                let span = jobs::span("long", None);
                if let Some(enqueued_under) = enqueued_under(&claim.job) {
                    let _ = span.set_parent(enqueued_under);
                }
                async {
                    let (attempt, _response) = self.execute(&claim, claimed_at).await;
                    self.conclude(&claim, attempt).await
                }
                .instrument(span)
                .await
            }
            Ok(None) => self.unclaimed(job_id).await,
            Err(e) => {
                warn!(
                    "Failed to claim job {} ({}); it is delivered again",
                    job_id, e
                );
                retry_in(STORE_RETRY, e.to_string())
            }
        }
    }

    async fn unclaimed(&self, job_id: Uuid) -> Outcome {
        let job = match wasm_jobs::get(self.pool(), job_id).await {
            Ok(job) => job,
            Err(e) => return retry_in(STORE_RETRY, e.to_string()),
        };
        match inspect(job.as_ref(), Utc::now()) {
            Inspection::Exhausted(job) => self.fail_exhausted(job).await,
            Inspection::Wait(wait) => retry_in(wait, "the job is not due"),
            Inspection::Settled(reason) => {
                debug!(job_id = %job_id, reason, "Nothing to do for a long-job message");
                Outcome::Ok
            }
        }
    }

    async fn fail_exhausted(&self, job: &WasmJob) -> Outcome {
        match fail_lost(&self.app_state, job).await {
            Ok(failed) => {
                if failed.is_some() {
                    jobs::record("long", "failed");
                }
                Outcome::Ok
            }
            Err(e) => retry_in(STORE_RETRY, e.to_string()),
        }
    }

    async fn execute(&self, claim: &Claim, claimed_at: Instant) -> (Attempt, Hold) {
        let lost = CancellationToken::new();
        let lease_lost = async {
            hold_lease(self.lease, claimed_at, || {
                beat(self.pool(), &claim.fence, self.lease)
            })
            .await;
            metrics::counter!("cufflink_job_lease_lost_total").increment(1);
            lost.cancel();
        };
        let attempt = self.attempt(&claim.job, &lost);
        let attempt = match select(pin!(attempt), pin!(lease_lost)).await {
            Either::Left((attempt, _)) => attempt,
            Either::Right(((), attempt)) => attempt.await,
        };
        if lost.is_cancelled() {
            (Attempt::LeaseLost, Hold::none())
        } else {
            attempt
        }
    }

    async fn attempt(&self, job: &WasmJob, lost: &CancellationToken) -> (Attempt, Hold) {
        let (service_id, version) = (job.service_id, job.service_version);
        let lookup = execution_context(self.pool(), service_id, version).await;
        let context = match job_context(lookup, job) {
            Ok(context) => context,
            Err(attempt) => return (attempt, Hold::none()),
        };
        context
            .identity()
            .scope(version)
            .running(Kind::Job, job.handler.as_str())
            .record_on(&tracing::Span::current());
        let Some(compiled) = self
            .wasm_engine
            .get_module(service_id, version, context.wasm_hash.as_deref())
            .await
        else {
            return (
                Attempt::failed(
                    JobErrorClass::HandlerError,
                    format!(
                        "WASM module for service {service_id} v{version} not loaded on this worker"
                    ),
                ),
                Hold::none(),
            );
        };
        let run = self
            .wasm_engine
            .run(
                self.app_state.clone(),
                Invocation {
                    compiled,
                    handler: job.handler.clone(),
                    request_json: with_job_context(&job.request_json, job),
                    identity: context.identity(),
                    publishes: context.publishes,
                    extensions: context.extensions,
                    kind: InvocationKind::Job,
                    scope_kind: None,
                    budget: Duration::from_secs(
                        u64::try_from(job.timeout_secs.max(1)).unwrap_or(1),
                    ),
                    cancel: Some(lost.clone()),
                    class: Class::Background,
                    admission: None,
                },
            )
            .await;
        classify(
            run,
            job,
            self.app_state.shutdown.hard_deadline_reached(),
            self.wasm_engine.capacity_deferrals_allowed(),
            |refused| self.wasm_engine.refusal(refused, true),
        )
    }

    async fn conclude(&self, claim: &Claim, attempt: Attempt) -> Outcome {
        let Claim { job, fence } = claim;
        let written = match &attempt {
            Attempt::Completed(result) => {
                wasm_jobs::complete(self.pool(), fence, result, JOB_RETENTION)
                    .await
                    .map(|fenced| (Transition::Completed, fenced, Outcome::Ok))
            }
            Attempt::Failed { class, error } => self.conclude_failure(claim, class, error).await,
            Attempt::Interrupted(reason) => {
                info!(
                    "Job {} interrupted by shutdown ({}); handing it back for immediate redelivery",
                    job.id, reason
                );
                wasm_jobs::release(self.pool(), fence)
                    .await
                    .map(|fenced| (Transition::Released, fenced, Outcome::Interrupted))
            }
            Attempt::Deferred(after) => {
                wasm_jobs::defer(self.pool(), fence, *after)
                    .await
                    .map(|fenced| {
                        (
                            Transition::Deferred,
                            fenced,
                            retry_in(*after, CAPACITY_REASON),
                        )
                    })
            }
            Attempt::LeaseLost => Ok((Transition::Executed, Fenced::Lost, Outcome::Ok)),
        };
        match written {
            Ok((transition, Fenced::Held, outcome)) => {
                transition.count();
                outcome
            }
            Ok((transition, Fenced::Lost, _)) => {
                fence_lost(transition.op(), fence);
                jobs::record("long", "lease_lost");
                Outcome::Ok
            }
            Err(e) => {
                error!("Failed to record how job {} ended: {}", job.id, e);
                retry_in(STORE_RETRY, e.to_string())
            }
        }
    }

    async fn conclude_failure(
        &self,
        claim: &Claim,
        class: &JobErrorClass,
        error: &str,
    ) -> PlatformResult<(Transition, Fenced, Outcome)> {
        let Claim { job, fence } = claim;
        let failure = Failure {
            error,
            class: class.as_str(),
        };
        let now = Utc::now();
        let decision = decide_job(
            class,
            u32::try_from(job.attempts).unwrap_or(u32::MAX),
            u32::try_from(job.max_attempts).unwrap_or_default(),
            &job.retry_on,
            &job.backoff(),
            now,
            job.jitter_seed(),
        );
        match decision {
            JobDecision::Retry { at } => {
                warn!(
                    job_id = %job.id,
                    attempt = job.attempts,
                    error,
                    "A job's attempt failed; another follows"
                );
                let wait = (at - now).to_std().unwrap_or_default();
                let requeue = requeue_for(wait, self.longest_redelivery);
                wasm_jobs::retry(self.pool(), fence, failure, at, requeue)
                    .await
                    .map(|fenced| {
                        (
                            Transition::Retried {
                                crashed: *class == JobErrorClass::Crash,
                            },
                            fenced,
                            match requeue {
                                Requeue::Redelivered => retry_in(wait, error),
                                Requeue::Republished => Outcome::Ok,
                            },
                        )
                    })
            }
            JobDecision::Fail => {
                let failed = wasm_jobs::fail(self.pool(), fence, failure, JOB_RETENTION).await?;
                if let Some(failed) = &failed {
                    dead_letter::file(&self.app_state, failed).await;
                }
                let fenced = failed.map_or(Fenced::Lost, |_| Fenced::Held);
                Ok((Transition::Failed, fenced, Outcome::Ok))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transition {
    Completed,
    Retried { crashed: bool },
    Failed,
    Released,
    Deferred,
    Executed,
}

impl Transition {
    fn op(self) -> &'static str {
        match self {
            Transition::Completed => "complete",
            Transition::Retried { .. } => "retry",
            Transition::Failed => "fail",
            Transition::Released => "release",
            Transition::Deferred => "defer",
            Transition::Executed => "execute",
        }
    }

    fn labels(self) -> Option<(&'static str, &'static str)> {
        match self {
            Transition::Completed => Some(("completed", "ok")),
            Transition::Retried { crashed: true } => Some(("retry", "crashed")),
            Transition::Retried { crashed: false } => Some(("retry", "retry")),
            Transition::Failed => Some(("failed", "failed")),
            Transition::Released => Some(("released", "interrupted")),
            Transition::Deferred => Some(("deferred", "deferred")),
            Transition::Executed => None,
        }
    }

    fn count(self) {
        if let Some((to, result)) = self.labels() {
            count_transition(to);
            jobs::record("long", result);
        }
    }
}

fn with_job_context(request: &Value, job: &WasmJob) -> String {
    let envelope = json!(JobMeta {
        id: job.id.to_string(),
        attempt: u32::try_from(job.attempts).unwrap_or_default(),
        max_attempts: u32::try_from(job.max_attempts).unwrap_or_default(),
        last_error: job.last_error.clone(),
        retry_on: job.retry_on.clone(),
    });
    let merged = match request {
        Value::Object(map) => {
            let mut out = map.clone();
            out.insert("_job".to_string(), envelope);
            Value::Object(out)
        }
        other => json!({ "body": other, "_job": envelope }),
    };
    merged.to_string()
}

fn job_context(
    lookup: PlatformResult<Option<ExecutionContext>>,
    job: &WasmJob,
) -> Result<ExecutionContext, Attempt> {
    let (service_id, version) = (job.service_id, job.service_version);
    let terminal = |error: String| {
        Attempt::failed(
            JobErrorClass::Terminal {
                reason: error.clone(),
            },
            error,
        )
    };
    match lookup {
        Err(e) => Err(Attempt::failed(
            JobErrorClass::Crash,
            format!("deployment lookup for service {service_id} v{version} failed: {e}"),
        )),
        Ok(None) => Err(terminal(format!(
            "service {service_id} v{version} has no deployment"
        ))),
        Ok(Some(context))
            if context.tenant_id != job.tenant_id || context.tenant_slug != job.tenant_slug =>
        {
            metrics::counter!("cufflink_worker_job_identity_mismatch_total").increment(1);
            Err(terminal(format!(
                "job of tenant {} does not match service {service_id} v{version} of tenant {}",
                job.tenant_slug, context.tenant_slug
            )))
        }
        Ok(Some(context)) => Ok(context),
    }
}

fn parse_job_id(payload: &[u8]) -> Option<Uuid> {
    let v: Value = serde_json::from_slice(payload).ok()?;
    v.get("job_id")
        .and_then(|j| j.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_platform::repo::wasm_jobs::JobStatus;
    use cufflink_types::RetryOn;
    use serde_json::json;

    #[test]
    fn parse_job_id_accepts_valid_uuid() {
        let id = Uuid::new_v4();
        let payload = json!({ "job_id": id.to_string() }).to_string();
        assert_eq!(parse_job_id(payload.as_bytes()), Some(id));
    }

    #[test]
    fn parse_job_id_rejects_garbage() {
        assert_eq!(parse_job_id(b"not json"), None);
        assert_eq!(
            parse_job_id(json!({ "job_id": "nope" }).to_string().as_bytes()),
            None
        );
        assert_eq!(parse_job_id(json!({}).to_string().as_bytes()), None);
    }

    #[test]
    fn consumer_name_normalizes_filter_pattern() {
        assert_eq!(consumer_name_for("cufflink.jobs.>"), CONSUMER_NAME);
        let custom = consumer_name_for("cufflink.jobs.acme.stock-service.>");
        assert!(custom.starts_with(CONSUMER_NAME));
        assert!(!custom.contains('.'));
    }

    fn job_fixture(id: Uuid, attempts: i32, max_attempts: i32) -> WasmJob {
        let now = chrono::Utc::now();
        WasmJob {
            id,
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".to_string(),
            service_id: Uuid::nil(),
            service_slug: "stock-service".to_string(),
            service_version: 1,
            handler: "handle_run_report".to_string(),
            status: JobStatus::Running,
            request_json: Value::Null,
            auth_subject: None,
            auth_claims: None,
            result_json: None,
            error: None,
            attempts,
            max_attempts,
            timeout_secs: 900,
            created_at: now,
            started_at: Some(now),
            finished_at: None,
            expires_at: now + chrono::Duration::hours(24),
            lease_owner: Some("worker-1".to_string()),
            lease_until: Some(now + chrono::Duration::seconds(60)),
            next_attempt_at: now,
            published_at: Some(now),
            last_error: None,
            error_class: None,
            retry_on: Vec::new(),
            backoff_base_secs: None,
            backoff_max_secs: None,
            trace_context: None,
            dead_lettered_at: None,
            idempotency_key: None,
            capacity_deferrals: 0,
            schedule_name: None,
        }
    }

    #[test]
    fn a_lookup_that_failed_is_retried_and_a_foreign_deployment_fails_the_job() {
        let job = job_fixture(Uuid::new_v4(), 1, 3);
        let context = |tenant_id, tenant_slug: &str| ExecutionContext {
            tenant_id,
            tenant_slug: tenant_slug.to_string(),
            service_id: job.service_id,
            service_name: "stock-service".to_string(),
            schema: "stock_service".to_string(),
            wasm_hash: None,
            publishes: None,
            extensions: Vec::new(),
        };
        assert!(matches!(
            job_context(
                Err(cufflink_platform::errors::PlatformError::Internal(
                    "db blip".into()
                )),
                &job
            ),
            Err(Attempt::Failed {
                class: JobErrorClass::Crash,
                ..
            })
        ));
        assert!(matches!(
            job_context(Ok(None), &job),
            Err(Attempt::Failed {
                class: JobErrorClass::Terminal { .. },
                ..
            })
        ));
        for foreign in [
            context(Uuid::new_v4(), "acme"),
            context(Uuid::nil(), "other"),
        ] {
            assert!(matches!(
                job_context(Ok(Some(foreign)), &job),
                Err(Attempt::Failed {
                    class: JobErrorClass::Terminal { .. },
                    ..
                })
            ));
        }
        assert!(job_context(Ok(Some(context(Uuid::nil(), "acme"))), &job).is_ok());
    }

    fn settings(vars: &[(&str, &str)]) -> eyre::Result<Settings> {
        let vars: std::collections::HashMap<&str, &str> = vars.iter().copied().collect();
        Settings::from_vars(|name| vars.get(name).map(|value| value.to_string()))
    }

    #[test]
    fn a_lease_is_a_minute_unless_set_to_three_seconds_or_more() {
        let lease = |raw: &str| settings(&[("JOB_LEASE_SECS", raw)]).map(|set| set.lease.as_secs());
        assert_eq!(settings(&[]).expect("defaults").lease.as_secs(), 60);
        assert_eq!(lease("15").expect("set"), 15);
        assert_eq!(lease(" 3 ").expect("set"), 3);
        for unusable in ["2", "0", "-5", "soon", ""] {
            let refused = lease(unusable).expect_err(unusable);
            assert!(
                refused.to_string().starts_with("JOB_LEASE_SECS"),
                "{unusable:?}: {refused}"
            );
        }
    }

    #[test]
    fn no_two_runs_of_a_worker_are_the_same_owner() {
        let (first, second) = (lease_owner(), lease_owner());
        assert_ne!(first, second);
        assert!(
            first.contains(&format!(":{}:", std::process::id())),
            "{first}"
        );
    }

    #[test]
    fn a_lease_is_held_for_as_long_as_its_last_heartbeat_gave_it() {
        let lease = Duration::from_secs(60);
        let start = Instant::now();
        let after = |secs| start + Duration::from_secs(secs);
        assert_eq!(
            held_after(Beat::Extended, after(20), after(21), after(60), lease),
            Some(after(80)),
            "from when the heartbeat was asked for, not from when it was answered"
        );
        assert_eq!(
            held_after(Beat::Refused, after(20), after(21), after(60), lease),
            None
        );
        assert_eq!(
            held_after(Beat::Failed, after(40), after(59), after(60), lease),
            Some(after(60)),
            "a heartbeat that failed leaves the lease as long as it was"
        );
        assert_eq!(
            held_after(Beat::Failed, after(40), after(60), after(60), lease),
            None,
            "and it is given up when it has run out"
        );
    }

    const LEASE: Duration = Duration::from_secs(60);

    fn heartbeats(
        first: Beat,
        then: Beat,
        takes: Duration,
    ) -> impl FnMut() -> BoxFuture<'static, Beat> {
        let mut answers = std::iter::once(first).chain(std::iter::repeat(then));
        move || {
            let beat = answers.next().unwrap_or(then);
            async move {
                tokio::time::sleep(takes).await;
                beat
            }
            .boxed()
        }
    }

    async fn lost_after<B: Future<Output = Beat>>(
        claimed_at: Instant,
        heartbeat: impl FnMut() -> B,
    ) -> Duration {
        hold_lease(LEASE, claimed_at, heartbeat).await;
        claimed_at.elapsed()
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_runs_from_before_its_claim_was_sent() {
        let claimed_at = Instant::now();
        tokio::time::advance(Duration::from_secs(25)).await;
        assert_eq!(
            lost_after(
                claimed_at,
                heartbeats(Beat::Failed, Beat::Failed, Duration::ZERO)
            )
            .await,
            LEASE,
            "what the claim and the module took is off the lease"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_that_runs_out_while_a_heartbeat_is_under_way_is_lost_then() {
        let slow = Duration::from_secs(19);
        assert_eq!(
            lost_after(
                Instant::now(),
                heartbeats(Beat::Extended, Beat::Failed, slow)
            )
            .await,
            Duration::from_secs(20) + LEASE,
            "extended by the heartbeat asked for at 20 s, and lost at 80 s"
        );
        assert_eq!(
            lost_after(
                Instant::now(),
                heartbeats(Beat::Failed, Beat::Failed, Duration::from_secs(3600))
            )
            .await,
            LEASE,
            "a heartbeat that never answers is not waited for"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_is_lost_once_refused_and_held_while_extended() {
        assert_eq!(
            lost_after(
                Instant::now(),
                heartbeats(Beat::Extended, Beat::Refused, Duration::from_secs(1))
            )
            .await,
            Duration::from_secs(41),
            "the second heartbeat, asked for at 40 s, found the fence gone"
        );
        let held = tokio::time::timeout(
            LEASE * 100,
            hold_lease(
                LEASE,
                Instant::now(),
                heartbeats(Beat::Extended, Beat::Extended, Duration::from_secs(1)),
            ),
        );
        assert!(held.await.is_err(), "a lease that is extended is not lost");
    }

    #[test]
    fn a_job_that_waits_longer_than_a_message_may_is_published_when_due() {
        let longest = |raw: &str| {
            settings(&[("JOB_NAK_MAX_DELAY_SECS", raw)]).map(|set| set.longest_redelivery)
        };
        assert_eq!(
            settings(&[]).expect("defaults").longest_redelivery,
            Duration::from_secs(60)
        );
        assert_eq!(longest("5").expect("set"), Duration::from_secs(5));
        assert_eq!(longest("0").expect("every retry published"), Duration::ZERO);
        for unusable in ["soon", "-1", ""] {
            let refused = longest(unusable).expect_err(unusable);
            assert!(
                refused.to_string().starts_with("JOB_NAK_MAX_DELAY_SECS"),
                "{unusable:?}: {refused}"
            );
        }
        let minute = Duration::from_secs(60);
        assert_eq!(
            requeue_for(Duration::from_secs(36), minute),
            Requeue::Redelivered
        );
        assert_eq!(requeue_for(minute, minute), Requeue::Redelivered);
        assert_eq!(
            requeue_for(Duration::from_secs(61), minute),
            Requeue::Republished
        );
        assert_eq!(
            requeue_for(Duration::from_secs(1), Duration::ZERO),
            Requeue::Republished
        );
    }

    #[test]
    fn a_message_is_handed_back_for_as_long_as_its_job_waits() {
        assert_eq!(
            retry_in(Duration::from_secs(30), "upstream 503"),
            Outcome::Retry {
                after: Some(Duration::from_secs(30)),
                reason: "upstream 503".into()
            }
        );
    }

    #[test]
    fn every_transition_is_counted_under_its_own_labels() {
        let transitions = [
            Transition::Completed,
            Transition::Retried { crashed: true },
            Transition::Retried { crashed: false },
            Transition::Failed,
            Transition::Released,
        ];
        assert_eq!(
            transitions.map(|transition| (transition.op(), transition.labels())),
            [
                ("complete", Some(("completed", "ok"))),
                ("retry", Some(("retry", "crashed"))),
                ("retry", Some(("retry", "retry"))),
                ("fail", Some(("failed", "failed"))),
                ("release", Some(("released", "interrupted"))),
            ]
        );
        assert_eq!(Transition::Executed.op(), "execute");
        assert_eq!(Transition::Executed.labels(), None);
    }

    #[test]
    fn with_job_context_injects_envelope_into_object_request() {
        let job_id = Uuid::new_v4();
        let job = job_fixture(job_id, 2, 3);
        let request = json!({
            "method": "POST",
            "handler": "handle_run_report",
            "body": {"id": "item-1"},
            "tenant": "acme",
            "service": "stock-service",
            "auth": {"sub": "user-1"},
        });

        let out: Value = serde_json::from_str(&with_job_context(&request, &job)).unwrap();

        assert_eq!(out["method"], "POST");
        assert_eq!(out["handler"], "handle_run_report");
        assert_eq!(out["body"]["id"], "item-1");
        assert_eq!(out["auth"]["sub"], "user-1");
        assert_eq!(out["_job"]["id"], job_id.to_string());
        assert_eq!(out["_job"]["attempt"], 2);
        assert_eq!(out["_job"]["max_attempts"], 3);
    }

    #[test]
    fn a_job_reads_the_text_it_always_read() {
        let job = job_fixture(Uuid::nil(), 2, 3);
        let request = json!({
            "method": "POST",
            "handler": "handle_run_report",
            "body": {"id": "item-1"},
            "tenant": "acme",
            "service": "stock-service",
            "auth": {"sub": "user-1"},
        });
        assert_eq!(
            with_job_context(&request, &job),
            r#"{"_job":{"attempt":2,"id":"00000000-0000-0000-0000-000000000000","max_attempts":3},"auth":{"sub":"user-1"},"body":{"id":"item-1"},"handler":"handle_run_report","method":"POST","service":"stock-service","tenant":"acme"}"#
        );
        assert_eq!(
            with_job_context(&json!("legacy-string-payload"), &job),
            r#"{"_job":{"attempt":2,"id":"00000000-0000-0000-0000-000000000000","max_attempts":3},"body":"legacy-string-payload"}"#
        );
    }

    #[test]
    fn the_job_envelope_has_the_keys_it_always_had() {
        let job = job_fixture(Uuid::nil(), 2, 3);
        let out: Value =
            serde_json::from_str(&with_job_context(&json!({"method": "POST"}), &job)).unwrap();
        assert_eq!(
            out,
            json!({
                "method": "POST",
                "_job": {
                    "id": "00000000-0000-0000-0000-000000000000",
                    "attempt": 2,
                    "max_attempts": 3,
                },
            })
        );
    }

    #[test]
    fn a_later_attempt_is_told_what_the_last_failed_of_and_what_is_retried() {
        let job = WasmJob {
            last_error: Some("Handler answered 503".into()),
            retry_on: vec![RetryOn::Error, RetryOn::Timeout],
            ..job_fixture(Uuid::nil(), 2, 3)
        };
        assert_eq!(
            with_job_context(&json!({"method": "POST"}), &job),
            r#"{"_job":{"attempt":2,"id":"00000000-0000-0000-0000-000000000000","last_error":"Handler answered 503","max_attempts":3,"retry_on":["error","timeout"]},"method":"POST"}"#
        );
    }

    #[test]
    fn a_message_is_settled_as_its_jobs_row_was_written() {
        let jobs_decide = |outcome: &Outcome, delivered| {
            decide(
                AckMode::OnSuccess,
                outcome,
                delivered,
                UNLIMITED_DELIVERIES,
                &Backoff::explicit(vec![STORE_RETRY]),
                false,
                7,
            )
        };
        for delivered in [1, 50] {
            assert_eq!(jobs_decide(&Outcome::Ok, delivered), Decision::Ack);
            assert_eq!(
                jobs_decide(&retry_in(Duration::from_secs(36), "upstream"), delivered),
                Decision::Nak(Duration::from_secs(36)),
                "however often its message was delivered: the row counts attempts"
            );
            assert_eq!(
                jobs_decide(&Outcome::Interrupted, delivered),
                Decision::Nak(Duration::ZERO)
            );
            assert!(matches!(
                jobs_decide(
                    &Outcome::Invalid {
                        reason: "the payload names no job".into()
                    },
                    delivered
                ),
                Decision::Term { .. }
            ));
        }
    }

    #[test]
    fn with_job_context_preserves_first_attempt() {
        let job = job_fixture(Uuid::new_v4(), 1, 2);
        let request = json!({"method": "POST", "body": Value::Null});

        let out: Value = serde_json::from_str(&with_job_context(&request, &job)).unwrap();
        assert_eq!(out["_job"]["attempt"], 1);
    }

    #[test]
    fn with_job_context_wraps_non_object_request() {
        let job = job_fixture(Uuid::new_v4(), 1, 2);
        let request = json!("legacy-string-payload");

        let out: Value = serde_json::from_str(&with_job_context(&request, &job)).unwrap();
        assert_eq!(out["body"], "legacy-string-payload");
        assert_eq!(out["_job"]["attempt"], 1);
    }
}
