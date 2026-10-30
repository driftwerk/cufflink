mod backup;
mod http;
mod jobs;
mod long_jobs;
mod reclaimer;

use cufflink_platform::api::version::platform_version;
use cufflink_platform::app::{until_ready, Abort};
use cufflink_platform::auth::service_identity::ServiceIdentityKeys;
use cufflink_platform::config::{PlatformConfig, TenantPoolDefaults};
use cufflink_platform::errors::PlatformError;
use cufflink_platform::nats_conn::{self, Backoff};
use cufflink_platform::nats_manager::bootstrap::spawn_ensure_dead_letter_stream;
use cufflink_platform::nats_manager::delivery::{decide, Decision, Outcome};
use cufflink_platform::nats_manager::dlq::{dlq_subject, DeadLetter, DlqKind, UNOWNED_TENANT};
use cufflink_platform::nats_manager::events;
use cufflink_platform::nats_manager::pump::{self, MessageMeta, PumpConfig, Work, WorkLabel};
use cufflink_platform::nats_manager::subjects::{patterns_overlap, PLATFORM_INBOX_PREFIX};
use cufflink_platform::redis_conn::RedisHandle;
use cufflink_platform::repo::deployments::{execution_context, ExecutionContext};
use cufflink_platform::shutdown::{self, Shutdown, DEPENDENCY_FLUSH_CAP};
use cufflink_platform::storage::S3Storage;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_platform::wasm_runtime::budget::Hold;
use cufflink_platform::wasm_runtime::{
    Class, ExecutionError, GuestOutput, Invocation, InvocationKind, WasmEngine,
};
use cufflink_platform::worker_dispatch::{requested_budget, WasmJob, WasmJobResult};
use cufflink_platform::{AppState, Assembly};
use cufflink_telemetry::Kind;
use cufflink_types::backoff::Backoff as RetryDelays;
use cufflink_types::AckMode;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::StreamExt;
use http::WorkerReadiness;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{error, info, warn, Instrument, Level};
use uuid::Uuid;

const WORKER_ROUTE_BUDGET: Duration = Duration::from_secs(30);
const MAX_ROUTE_BUDGET: Duration = Duration::from_secs(120);
const HEALTHY_CONSUMER_RUN: Duration = Duration::from_secs(60);
const TENANT_POOL_DEFAULTS: TenantPoolDefaults = TenantPoolDefaults {
    max_pools: 32,
    max_conns_per_tenant: 2,
};

#[derive(Debug)]
struct WorkerSettings {
    http_port: Option<u16>,
    max_concurrent_sync: usize,
    after_hook_concurrency: usize,
    subscribe_pattern: String,
    queue_group: String,
    long_jobs: long_jobs::Settings,
    reclaimer: reclaimer::Settings,
}

impl WorkerSettings {
    fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let vars = cufflink_db::Vars(&var);
        Ok(Self {
            http_port: Some(vars.parsed("WORKER_HTTP_PORT", 8081u16)?).filter(|port| *port > 0),
            max_concurrent_sync: vars.positive("WORKER_MAX_CONCURRENT_SYNC", 32)?,
            after_hook_concurrency: vars.positive("WORKER_AFTER_HOOK_CONCURRENCY", 1)?,
            subscribe_pattern: subscribe_pattern(vars.get("SUBSCRIBE_PATTERN"))?,
            queue_group: vars
                .get("QUEUE_GROUP")
                .unwrap_or_else(|| "wasm-workers".into()),
            long_jobs: long_jobs::Settings::from_vars(&var)?,
            reclaimer: reclaimer::Settings::from_vars(&var)?,
        })
    }
}

fn subscribe_pattern(configured: Option<String>) -> eyre::Result<String> {
    let pattern = configured.unwrap_or_else(|| "cufflink.wasm.>".into());
    let calls = format!("{}.>", cufflink_types::ext::SUBJECT_PREFIX);
    if patterns_overlap(&pattern, &calls) {
        eyre::bail!(
            "SUBSCRIBE_PATTERN '{pattern}' overlaps {calls}, where extension calls are answered; \
             subscribe the worker to its jobs only (the default is cufflink.wasm.>)"
        );
    }
    Ok(pattern)
}

fn main() -> eyre::Result<()> {
    cufflink_platform::wasm_runtime::budget::runtime()?.block_on(run())
}

async fn run() -> eyre::Result<()> {
    cufflink_telemetry::init(cufflink_telemetry::Component::Worker, &[])?;
    info!(
        version = platform_version(),
        "Starting Cufflink WASM Worker"
    );

    let vars = cufflink_settings::from_env()?;
    let config = PlatformConfig::load_with(TENANT_POOL_DEFAULTS, &vars)?;
    let settings = WorkerSettings::from_vars(&vars)?;
    let metrics_handle = cufflink_telemetry::install_metrics()?;
    let shutdown = Shutdown::new(config.shutdown_grace);

    let platform_db = cufflink_db::build_pool(&config.database_url, 4)?;
    let tenant_pools = TenantPoolManager::new(
        config.tenant_database_url(),
        config.max_tenant_pools,
        config.max_conns_per_tenant,
    )?
    .with_pooler(config.tenant_db_pooler)
    .with_direct_url(&config.database_url)?;

    let nats = nats_conn::connect(&config.nats_url, "cufflink-worker", &config.nats_auth).await?;
    info!(
        "NATS client started for {} (connects in the background)",
        nats_conn::redact_urls(&config.nats_url)
    );

    let object_storage = cufflink_storage::optional_from_vars(&vars)?.map(S3Storage::from_inner);
    match &object_storage {
        Some(store) => info!(
            "Object storage configured (backend: {}, bucket: {})",
            store.backend_kind(),
            store.primary_bucket()
        ),
        None => info!("Object storage not configured"),
    }

    let wasm_engine = WasmEngine::new(config.wasm.clone(), shutdown.clone())?;
    if let Some(store) = &object_storage {
        wasm_engine.set_storage(store.clone()).await;
    }
    wasm_engine.set_artifact_index(platform_db.clone());
    info!("WASM runtime initialized");

    let redis = match config.redis_url.as_deref() {
        Some(url) => match RedisHandle::connect_in_background(url) {
            Ok(handle) => {
                info!("Redis configured; connecting in the background");
                handle
            }
            Err(e) => {
                warn!("Invalid Redis URL, running without Redis: {}", e);
                RedisHandle::disabled()
            }
        },
        None => RedisHandle::disabled(),
    };

    let service_identity =
        ServiceIdentityKeys::from_setting(config.service_identity_keys.as_deref())?;
    warn_on_service_call_setup(&config, &service_identity);
    let app_state = Arc::new(AppState::new(Assembly {
        config,
        platform_db,
        tenant_pools,
        nats: nats.clone(),
        wasm_engine: Some(wasm_engine.clone()),
        metrics_handle,
        storage: object_storage,
        keycloak_auth: None,
        redis,
        shutdown: shutdown.clone(),
        service_identity,
    }));
    let readiness = Arc::new(WorkerReadiness::default());
    for kind in jobs::KINDS {
        metrics::gauge!("cufflink_worker_inflight", "kind" => kind).set(0.0);
    }

    let probes = match settings.http_port {
        Some(port) => Some(http::Probes::serve(app_state.clone(), readiness.clone(), port).await?),
        None => None,
    };
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown::wait_for_signal().await;
            info!("Shutdown signal received");
            shutdown.trigger();
        }
    });

    let catalog = until_ready(&shutdown, Level::ERROR, "tenant catalog", || {
        app_state.tenants.reload()
    })
    .await;
    let startup_failed = match catalog {
        Ok(()) => {
            info!("Tenant catalog loaded");
            readiness.mark_catalog_loaded();
            app_state.tenants.start_sync_listener(&shutdown);
            app_state
                .stream_index
                .start_sync(&shutdown, &app_state.nats, &app_state.jetstream);
            serve_jobs(&app_state, &wasm_engine, &readiness, &settings).await?;
            None
        }
        Err(Abort::Shutdown) => None,
        Err(Abort::Fatal(error)) => {
            error!(error = %error, "Startup failed; shutting down");
            Some(error)
        }
    };

    shutdown.trigger();
    readiness.mark_shutting_down();
    let tasks = shutdown::phase(&shutdown, "tasks", shutdown.drain_tasks()).await;
    if let Some(probes) = probes {
        shutdown::phase_within(&shutdown, "http", DEPENDENCY_FLUSH_CAP, probes.close()).await;
    }
    let finished = shutdown::finish(
        &shutdown,
        &[("tasks", shutdown::drained(tasks))],
        nats_conn::flush_and_drain(&nats),
    )
    .await;
    startup_failed.map_or(finished, Err)
}

fn warn_on_service_call_setup(config: &PlatformConfig, keys: &ServiceIdentityKeys) {
    if config.platform_internal_url.is_none() {
        warn!("PLATFORM_INTERNAL_URL is unset: service calls from guests on this worker fail");
    }
    if !keys.is_shared() {
        warn!(
            "service_call from the worker requires CUFFLINK_SERVICE_IDENTITY_KEYS shared with the platform"
        );
    }
}

async fn serve_jobs(
    app_state: &Arc<AppState>,
    wasm_engine: &WasmEngine,
    readiness: &Arc<WorkerReadiness>,
    settings: &WorkerSettings,
) -> eyre::Result<()> {
    let shutdown = &app_state.shutdown;
    let mut subscriber = app_state
        .nats
        .queue_subscribe(
            settings.subscribe_pattern.clone(),
            settings.queue_group.clone(),
        )
        .await?;
    info!(
        "Subscribing to '{}' (queue group: '{}')",
        settings.subscribe_pattern, settings.queue_group
    );

    spawn_supervised("Backup consumer", shutdown, {
        let app_state = app_state.clone();
        let readiness = readiness.clone();
        move || backup::run_backup_consumer(app_state.clone(), readiness.clone())
    });
    spawn_supervised("JetStream consumer", shutdown, {
        let app_state = app_state.clone();
        let wasm_engine = wasm_engine.clone();
        let readiness = readiness.clone();
        let concurrency = settings.after_hook_concurrency;
        move || {
            run_jetstream_consumer(
                app_state.clone(),
                wasm_engine.clone(),
                readiness.clone(),
                concurrency,
            )
        }
    });
    spawn_supervised("Long-jobs consumer", shutdown, {
        let app_state = app_state.clone();
        let wasm_engine = wasm_engine.clone();
        let readiness = readiness.clone();
        let settings = settings.long_jobs;
        move || {
            long_jobs::run(
                app_state.clone(),
                wasm_engine.clone(),
                readiness.clone(),
                settings,
            )
        }
    });
    spawn_supervised("Job reclaimer", shutdown, {
        let app_state = app_state.clone();
        let settings = settings.reclaimer;
        move || reclaimer::run(app_state.clone(), settings)
    });
    info!("Cufflink WASM Worker ready, waiting for jobs...");

    let permits = Arc::new(Semaphore::new(settings.max_concurrent_sync));
    loop {
        let permit = tokio::select! {
            _ = shutdown.cancelled() => break,
            permit = Arc::clone(&permits).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let msg = tokio::select! {
            _ = shutdown.cancelled() => break,
            msg = subscriber.next() => match msg {
                Some(msg) => msg,
                None => break,
            },
        };
        if is_async_job_subject(&msg.subject) {
            continue;
        }
        let app_state = app_state.clone();
        let wasm_engine = wasm_engine.clone();
        shutdown.spawn("sync_job", async move {
            let _permit = permit;
            reply_to_sync_job(&app_state, &wasm_engine, msg).await;
        });
    }

    if let Err(e) = subscriber.unsubscribe().await {
        warn!(error = %e, "Failed to unsubscribe the request/reply queue");
    }
    Ok(())
}

fn spawn_supervised<F, Fut>(name: &'static str, shutdown: &Shutdown, mut run: F)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = eyre::Result<()>> + Send + 'static,
{
    let token = shutdown.token();
    shutdown.spawn(name, async move {
        let mut backoff = Backoff::new();
        loop {
            let started = tokio::time::Instant::now();
            let outcome = run().await;
            if token.is_cancelled() {
                return;
            }
            match outcome {
                Ok(()) => warn!("{} exited; restarting", name),
                Err(e) => error!("{} error: {}; restarting", name, e),
            }
            if started.elapsed() >= HEALTHY_CONSUMER_RUN {
                backoff.reset();
            }
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(backoff.next_delay()) => {}
            }
        }
    });
}

fn refuses_reply(reply: Option<&str>) -> bool {
    reply.is_some_and(|reply| !reply.starts_with(PLATFORM_INBOX_PREFIX))
}

async fn reply_to_sync_job(
    app_state: &Arc<AppState>,
    wasm_engine: &WasmEngine,
    msg: async_nats::Message,
) {
    if refuses_reply(msg.reply.as_deref()) {
        metrics::counter!("cufflink_worker_reply_refused_total").increment(1);
        warn!(subject = %msg.subject, "Refused a job whose reply subject is not an inbox");
        return;
    }
    let _in_flight = jobs::in_flight("sync");
    let budget =
        requested_budget(msg.headers.as_ref(), MAX_ROUTE_BUDGET).unwrap_or(WORKER_ROUTE_BUDGET);
    let outcome = process_job(
        app_state,
        wasm_engine,
        &msg.payload,
        budget,
        Kind::WorkerRoute,
        Class::Interactive,
    )
    .instrument(jobs::span("sync", msg.headers.as_ref()))
    .await;
    jobs::record("sync", if outcome.is_ok() { "ok" } else { "error" });

    let (result, mut hold) = match outcome {
        Ok(response) => (WasmJobResult::answered(response.text), response.hold),
        Err(e) => (
            match &e {
                JobError::Execution(error) => WasmJobResult::of_error(error, e.to_string()),
                _ => WasmJobResult::failed(e.to_string()),
            },
            Hold::none(),
        ),
    };
    let reply_payload = result.into_payload(&mut hold, wasm_engine);
    if let Some(reply) = msg.reply {
        if let Err(e) = nats_conn::publish_bounded(
            &app_state.nats,
            reply.to_string(),
            None,
            reply_payload.into(),
        )
        .await
        {
            error!("Failed to send reply: {}", e);
        }
    }
}

#[derive(Debug)]
enum JobError {
    InvalidPayload(serde_json::Error),
    DeploymentLookup(PlatformError),
    UnknownDeployment,
    IdentityMismatch {
        claimed_tenant: String,
        tenant: String,
    },
    ModuleMissing {
        service_id: Uuid,
        version: i32,
    },
    Execution(ExecutionError),
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobError::InvalidPayload(e) => write!(f, "Invalid job payload: {e}"),
            JobError::DeploymentLookup(e) => write!(f, "Deployment lookup failed: {e}"),
            JobError::UnknownDeployment => write!(f, "unknown service/version"),
            JobError::IdentityMismatch { .. } => {
                write!(f, "job identity does not match its deployment")
            }
            JobError::ModuleMissing {
                service_id,
                version,
            } => write!(
                f,
                "No WASM module found for service {service_id} v{version}"
            ),
            JobError::Execution(e @ ExecutionError::DeadlineExceeded { .. }) => write!(f, "{e}"),
            JobError::Execution(e) => write!(f, "WASM execution error: {e}"),
        }
    }
}

async fn resolve_job(
    app_state: &AppState,
    payload: &[u8],
) -> Result<(WasmJob, ExecutionContext), JobError> {
    let job: WasmJob = serde_json::from_slice(payload).map_err(JobError::InvalidPayload)?;
    let context = execution_context(&app_state.platform_db, job.service_id, job.version)
        .await
        .map_err(JobError::DeploymentLookup)?
        .ok_or(JobError::UnknownDeployment)?;
    if job.matches(&context) {
        Ok((job, context))
    } else {
        Err(JobError::IdentityMismatch {
            claimed_tenant: job.tenant_slug,
            tenant: context.tenant_slug,
        })
    }
}

fn class_of(class: Class, nested: bool) -> Class {
    match (class, nested) {
        (Class::Interactive, true) => Class::Nested,
        (class, _) => class,
    }
}

async fn process_job(
    app_state: &Arc<AppState>,
    wasm_engine: &WasmEngine,
    payload: &[u8],
    budget: Duration,
    kind: Kind,
    class: Class,
) -> Result<GuestOutput, JobError> {
    let (job, context) = resolve_job(app_state, payload).await.inspect_err(|e| {
        if let JobError::IdentityMismatch {
            claimed_tenant,
            tenant,
        } = e
        {
            metrics::counter!("cufflink_worker_job_identity_mismatch_total").increment(1);
            warn!(
                claimed_tenant = %claimed_tenant,
                tenant = %tenant,
                "Rejected a WASM job whose identity does not match its deployment"
            );
        }
    })?;

    context
        .identity()
        .scope(job.version)
        .running(kind, job.handler.as_str())
        .record_on(&tracing::Span::current());

    tracing::debug!(
        "Processing WASM job: handler='{}' service_id={} v{}",
        job.handler,
        job.service_id,
        job.version
    );

    let compiled = wasm_engine
        .get_module(job.service_id, job.version, context.wasm_hash.as_deref())
        .await
        .ok_or(JobError::ModuleMissing {
            service_id: job.service_id,
            version: job.version,
        })?;

    let class = class_of(class, job.nested);
    let identity = context.identity();
    let invocation = |request_json| Invocation {
        compiled: Arc::clone(&compiled),
        handler: job.handler.clone(),
        request_json,
        identity: identity.clone(),
        publishes: context.publishes.clone(),
        extensions: context.extensions.clone(),
        kind: InvocationKind::WorkerRoute,
        scope_kind: Some(kind),
        budget,
        cancel: None,
        class,
        admission: None,
    };
    let run = match class {
        Class::Background => {
            wasm_engine
                .run_retrying_capacity(
                    app_state.clone(),
                    InvocationKind::WorkerRoute,
                    job.request_json.clone(),
                    invocation,
                )
                .await
        }
        _ => {
            wasm_engine
                .run(app_state.clone(), invocation(job.request_json.clone()))
                .await
        }
    };
    run.map_err(JobError::Execution)
}

const ASYNC_JOB_SUBJECT_PREFIX: &str = "cufflink.wasm.async.";

fn is_async_job_subject(subject: &str) -> bool {
    subject.starts_with(ASYNC_JOB_SUBJECT_PREFIX)
}

const AFTER_HOOK_DURABLE: &str = "wasm-worker";
const AFTER_HOOK_ACK_WAIT: Duration = Duration::from_secs(60);
const AFTER_HOOK_MAX_DELIVER: i64 = 3;

async fn run_jetstream_consumer(
    app_state: Arc<AppState>,
    wasm_engine: WasmEngine,
    readiness: Arc<WorkerReadiness>,
    concurrency: usize,
) -> eyre::Result<()> {
    use async_nats::jetstream::consumer;

    spawn_ensure_dead_letter_stream(&app_state);
    let stream = events::async_jobs_stream(&app_state.jetstream).await?;
    let consumer = stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(AFTER_HOOK_DURABLE.to_string()),
            ack_wait: AFTER_HOOK_ACK_WAIT,
            max_deliver: AFTER_HOOK_MAX_DELIVER,
            ..Default::default()
        })
        .await?;
    info!("JetStream consumer '{AFTER_HOOK_DURABLE}' ready for async jobs");
    let _up = readiness.consumer_up();
    let exit = pump::run(
        consumer,
        PumpConfig::sequential(AFTER_HOOK_ACK_WAIT).concurrent(concurrency),
        Arc::new(AfterHookWork {
            app_state: app_state.clone(),
            wasm_engine,
            retries: RetryDelays::exponential(Duration::from_secs(5), Duration::from_secs(60)),
        }),
        Arc::new(app_state.jetstream.clone()),
        pump::Stop::new(),
        app_state.shutdown.clone(),
    )
    .await;
    jobs::ended("after-hook", exit)
}

struct AfterHookWork {
    app_state: Arc<AppState>,
    wasm_engine: WasmEngine,
    retries: RetryDelays,
}

fn after_hook_outcome(result: Result<GuestOutput, JobError>, at_hard_deadline: bool) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(JobError::Execution(e)) if e.interrupted_by_shutdown(at_hard_deadline) => {
            Outcome::Interrupted
        }
        Err(JobError::Execution(ExecutionError::Overloaded { retry_after, .. })) => {
            Outcome::Deferred { after: retry_after }
        }
        Err(e @ JobError::InvalidPayload(_)) => Outcome::Invalid {
            reason: e.to_string(),
        },
        Err(e @ (JobError::UnknownDeployment | JobError::IdentityMismatch { .. })) => {
            Outcome::Terminal {
                reason: e.to_string(),
            }
        }
        Err(e) => Outcome::Retry {
            after: None,
            reason: e.to_string(),
        },
    }
}

fn after_hook_dlq_subject(resolved: Option<&(WasmJob, ExecutionContext)>, subject: &str) -> String {
    match resolved {
        Some((job, deployment)) => dlq_subject(
            &deployment.tenant_slug,
            &deployment.service_name,
            DlqKind::Hook,
            &job.handler,
        ),
        None => {
            let mut named = subject
                .strip_prefix(ASYNC_JOB_SUBJECT_PREFIX)
                .unwrap_or_default()
                .splitn(2, '.');
            let (service, handler) = (
                named.next().unwrap_or_default(),
                named.next().unwrap_or_default(),
            );
            dlq_subject(UNOWNED_TENANT, service, DlqKind::Hook, handler)
        }
    }
}

impl Work for AfterHookWork {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "hook",
            subscription: None,
        }
    }

    fn handle(
        &self,
        msg: Arc<async_nats::jetstream::Message>,
        _meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let (app_state, wasm_engine) = (self.app_state.clone(), self.wasm_engine.clone());
        async move {
            let _in_flight = jobs::in_flight("async");
            let result = process_job(
                &app_state,
                &wasm_engine,
                &msg.payload,
                WORKER_ROUTE_BUDGET,
                Kind::Hook,
                Class::Background,
            )
            .instrument(jobs::span("async", msg.headers.as_ref()))
            .await;
            match &result {
                Ok(_) => jobs::record("async", "ok"),
                Err(JobError::Execution(e))
                    if e.interrupted_by_shutdown(app_state.shutdown.hard_deadline_reached()) =>
                {
                    jobs::record("async", "interrupted");
                    info!(
                        "Async WASM job interrupted by shutdown ({}); handing it back",
                        e
                    );
                }
                Err(e) => {
                    jobs::record("async", "error");
                    warn!("Async WASM job failed: {}", e);
                }
            }
            after_hook_outcome(result, app_state.shutdown.hard_deadline_reached())
        }
        .boxed()
    }

    fn headroom(&self) -> BoxFuture<'_, usize> {
        self.wasm_engine.headroom_for_any_handler().boxed()
    }

    fn decide(&self, outcome: &Outcome, meta: &MessageMeta) -> Decision {
        decide(
            AckMode::OnSuccess,
            outcome,
            meta.delivered,
            AFTER_HOOK_MAX_DELIVER,
            &self.retries,
            true,
            meta.stream_sequence,
        )
    }

    fn dead_letter<'a>(
        &'a self,
        msg: &'a async_nats::jetstream::Message,
        meta: &'a MessageMeta,
        reason: &'a str,
    ) -> BoxFuture<'a, Option<DeadLetter>> {
        async move {
            let resolved = resolve_job(&self.app_state, &msg.payload).await.ok();
            Some(DeadLetter::now(
                after_hook_dlq_subject(resolved.as_ref(), &meta.subject),
                msg.payload.clone(),
                meta,
                reason,
            ))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn a_routed_call_with_a_caller_is_nested_and_a_hook_stays_background() {
        assert_eq!(class_of(Class::Interactive, false), Class::Interactive);
        assert_eq!(class_of(Class::Interactive, true), Class::Nested);
        assert_eq!(class_of(Class::Background, true), Class::Background);
        assert_eq!(class_of(Class::Background, false), Class::Background);
    }

    #[test]
    fn an_after_hook_is_retried_only_when_a_retry_can_help() {
        let at_hard_deadline = false;
        assert_eq!(
            after_hook_outcome(
                Ok(GuestOutput {
                    text: "{}".into(),
                    hold: Hold::none(),
                }),
                at_hard_deadline
            ),
            Outcome::Ok
        );
        assert_eq!(
            after_hook_outcome(
                Err(JobError::Execution(ExecutionError::ShuttingDown)),
                at_hard_deadline
            ),
            Outcome::Interrupted
        );
        let deadline = || {
            Err(JobError::Execution(ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(30),
            }))
        };
        assert_eq!(after_hook_outcome(deadline(), true), Outcome::Interrupted);
        assert!(matches!(
            after_hook_outcome(deadline(), false),
            Outcome::Retry { after: None, .. }
        ));
        assert!(matches!(
            after_hook_outcome(
                Err(JobError::ModuleMissing {
                    service_id: Uuid::nil(),
                    version: 3
                }),
                at_hard_deadline
            ),
            Outcome::Retry { reason, .. } if reason.contains("No WASM module found")
        ));
        for unfixable in [JobError::UnknownDeployment, mismatch()] {
            assert!(matches!(
                after_hook_outcome(Err(unfixable), at_hard_deadline),
                Outcome::Terminal { .. }
            ));
        }
        let invalid = serde_json::from_slice::<WasmJob>(b"{").expect_err("invalid");
        assert!(matches!(
            after_hook_outcome(Err(JobError::InvalidPayload(invalid)), at_hard_deadline),
            Outcome::Invalid { .. }
        ));
    }

    fn mismatch() -> JobError {
        JobError::IdentityMismatch {
            claimed_tenant: "victim".into(),
            tenant: "acme".into(),
        }
    }

    #[test]
    fn an_after_hooks_dead_letter_is_filed_under_the_deployment_it_ran_as() {
        let resolved = (
            WasmJob {
                service_id: Uuid::nil(),
                version: 1,
                handler: "after_create".into(),
                request_json: "{}".into(),
                tenant_id: Uuid::nil(),
                tenant_slug: "victim".into(),
                service_schema: "victim_schema".into(),
                service_name: Some("victim-service".into()),
                publishes: None,
                nested: false,
            },
            ExecutionContext {
                tenant_id: Uuid::nil(),
                tenant_slug: "acme".into(),
                service_id: Uuid::nil(),
                service_name: "stock-service".into(),
                schema: "stock_service".into(),
                wasm_hash: None,
                publishes: None,
                extensions: Vec::new(),
            },
        );
        let subject = "cufflink.wasm.async.stock-service.after_create";
        assert_eq!(
            after_hook_dlq_subject(Some(&resolved), subject),
            "cufflink.dlq.acme.stock-service.hook.after_create",
            "the tenant and the service are the deployment's, whatever the job says"
        );
        assert_eq!(
            after_hook_dlq_subject(None, subject),
            "cufflink.dlq._.stock-service.hook.after_create"
        );
        assert_eq!(
            after_hook_dlq_subject(None, "elsewhere"),
            "cufflink.dlq._._.hook._"
        );
    }

    fn settings(vars: &[(&str, &str)]) -> eyre::Result<WorkerSettings> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        WorkerSettings::from_vars(|name| vars.get(name).cloned())
    }

    #[test]
    fn settings_default_and_refuse_a_zero_sync_concurrency() {
        let defaults = settings(&[]).expect("defaults");
        assert_eq!(defaults.http_port, Some(8081));
        assert_eq!(defaults.max_concurrent_sync, 32);
        assert_eq!(
            defaults.after_hook_concurrency, 1,
            "after-hooks run one at a time, in order, unless the operator opts in"
        );
        assert!(settings(&[("WORKER_AFTER_HOOK_CONCURRENCY", "0")]).is_err());
        assert_eq!(
            settings(&[("WORKER_AFTER_HOOK_CONCURRENCY", "2")])
                .expect("set")
                .after_hook_concurrency,
            2
        );
        assert_eq!(defaults.subscribe_pattern, "cufflink.wasm.>");
        assert_eq!(defaults.queue_group, "wasm-workers");
        assert_eq!(
            settings(&[("WORKER_HTTP_PORT", "0")])
                .expect("disabled")
                .http_port,
            None
        );
        let err = settings(&[("WORKER_MAX_CONCURRENT_SYNC", "0")]).expect_err("zero permits");
        assert!(
            err.to_string().contains("WORKER_MAX_CONCURRENT_SYNC"),
            "{err}"
        );
    }

    #[test]
    fn a_subscribe_pattern_that_reaches_the_extension_calls_fails_the_start() {
        for overlapping in [">", "cufflink.>", "cufflink.*.>", "cufflink.ext.acme.x"] {
            let refused = settings(&[("SUBSCRIBE_PATTERN", overlapping)]).expect_err(overlapping);
            assert!(
                refused.to_string().contains(&format!(
                    "SUBSCRIBE_PATTERN '{overlapping}' overlaps cufflink.ext.>"
                )),
                "{refused}"
            );
        }
        for kept in ["cufflink.wasm.>", "cufflink.wasm.acme.*", "jobs.>"] {
            assert_eq!(
                settings(&[("SUBSCRIBE_PATTERN", kept)])
                    .expect(kept)
                    .subscribe_pattern,
                kept
            );
        }
    }

    #[test]
    fn a_job_setting_that_cannot_be_used_fails_the_start() {
        for (name, unusable) in [
            ("JOB_LEASE_SECS", "2"),
            ("JOB_NAK_MAX_DELAY_SECS", "soon"),
            ("JOB_RECLAIM_INTERVAL_SECS", "soon"),
            ("JOB_REPUBLISH_AFTER_SECS", "0"),
        ] {
            let refused = settings(&[(name, unusable)]).expect_err(name);
            assert!(refused.to_string().contains(name), "{refused}");
        }
        let set =
            settings(&[("JOB_LEASE_SECS", "9"), ("JOB_REPUBLISH_AFTER_SECS", "45")]).expect("set");
        assert_eq!(set.long_jobs.lease, Duration::from_secs(9));
        assert_eq!(set.reclaimer.republish_after, Duration::from_secs(45));
    }

    #[test]
    fn only_an_inbox_is_answered() {
        assert!(!refuses_reply(None));
        assert!(!refuses_reply(Some("_INBOX.abc.1")));
        for reply in [
            "dw.acme.orders.created",
            "_INBOXES.x",
            "cufflink.wasm.x",
            "_inbox.x",
        ] {
            assert!(refuses_reply(Some(reply)), "{reply}");
        }
    }

    #[test]
    fn job_errors_keep_their_reply_wording() {
        let id = Uuid::nil();
        assert_eq!(
            JobError::ModuleMissing {
                service_id: id,
                version: 3
            }
            .to_string(),
            format!("No WASM module found for service {id} v3")
        );
        assert_eq!(
            JobError::Execution(ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(30)
            })
            .to_string(),
            ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(30)
            }
            .to_string()
        );
        assert_eq!(
            JobError::UnknownDeployment.to_string(),
            "unknown service/version"
        );
        assert_eq!(
            mismatch().to_string(),
            "job identity does not match its deployment"
        );
        assert!(JobError::Execution(ExecutionError::ShuttingDown)
            .to_string()
            .starts_with("WASM execution error: "));
        let invalid = serde_json::from_slice::<WasmJob>(b"{").expect_err("invalid");
        assert!(JobError::InvalidPayload(invalid)
            .to_string()
            .starts_with("Invalid job payload: "));
    }

    #[test]
    fn async_job_subjects_are_left_to_jetstream() {
        assert!(is_async_job_subject(
            "cufflink.wasm.async.orders-service.after_create"
        ));
        assert!(!is_async_job_subject(
            "cufflink.wasm.orders-service.handle_checkout"
        ));
        assert!(!is_async_job_subject("cufflink.wasm.async"));
    }
}
