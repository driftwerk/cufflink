use crate::deployer::manifest::{HandlerType, SubscriptionDefinition};
use crate::deployer::validator::is_safe_identifier;
use crate::egress::{EgressPolicy, EgressTarget, OutboundRequest};
use crate::nats_manager::consumers::plan::DesiredConsumer;
use crate::nats_manager::consumers::LoopKey;
use crate::nats_manager::delivery::{decide, Decision, Outcome};
use crate::nats_manager::dlq::{dlq_subject, DeadLetter, DlqKind};
use crate::nats_manager::pump::{
    self, MessageMeta, PumpConfig, PumpExit, Stop, SubscriptionLabel, Work, WorkLabel,
};
use crate::nats_manager::reconciler::{DesiredLoop, RunningLoop, Served, WorkSpec};
use crate::nats_manager::subjects::{is_valid_publish_subject, PLATFORM_INBOX_PREFIX};
use crate::registry::ServiceIdentity;
use crate::router::guest_response::{self, GuestResponse};
use crate::schedules::work::ScheduleWork;
use crate::shutdown::DrainOutcome;
use crate::tenant_sessions::{Purpose, Statements};
use crate::wasm_runtime::{Class, ExecutionError, Invocation, InvocationKind};
use crate::AppState;
use async_nats::jetstream;
use async_nats::HeaderMap;
use cufflink_telemetry::{scoped_span, Kind};
use cufflink_types::backoff::Backoff;
use cufflink_types::delivery::DeliveryControl;
use cufflink_types::host::nats::header_has_prefix;
use cufflink_types::host::request::{GuestRequest, SubscriptionMeta, METHOD_SUBSCRIPTION};
use cufflink_types::AckMode;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use reqwest::header::{HeaderValue, CONTENT_TYPE};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing::{error, info, warn};

pub const FEATURES: &[&str] = &[
    cufflink_types::features::NATS_CONSUMER_CONFIG,
    cufflink_types::features::NATS_MESSAGE_META,
    cufflink_types::features::STREAMS,
    cufflink_types::features::DELIVERY_CONTROL,
    cufflink_types::features::CONSUMER_RECONCILER,
    cufflink_types::features::SCHEDULES,
];

pub const CUFFLINK_REPLY_HEADER: &str = "Cufflink-Reply";

type PullConsumer = jetstream::consumer::Consumer<jetstream::consumer::pull::Config>;

type KeyedLocks<K> = Mutex<HashMap<K, Arc<tokio::sync::Mutex<()>>>>;

struct TrackedLoop {
    generation: u64,
    spec_hash: u64,
    stop: Stop,
    handle: JoinHandle<()>,
}

#[derive(Default)]
pub struct SubscriptionTasks {
    loops: Mutex<HashMap<LoopKey, TrackedLoop>>,
    retiring: Mutex<Vec<JoinHandle<()>>>,
    next_generation: AtomicU64,
    setup_locks: KeyedLocks<(String, String)>,
    tenant_stream_locks: KeyedLocks<String>,
}

impl SubscriptionTasks {
    fn next_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed)
    }

    fn track(&self, key: LoopKey, tracked: TrackedLoop) {
        if let Some(old) = lock(&self.loops).insert(key, tracked) {
            self.retire([old], Stop::replace);
        }
    }

    fn retire(&self, loops: impl IntoIterator<Item = TrackedLoop>, how: fn(&Stop)) {
        let mut retiring = lock(&self.retiring);
        retiring.retain(|handle| !handle.is_finished());
        retiring.extend(loops.into_iter().map(|tracked| {
            how(&tracked.stop);
            tracked.handle
        }));
    }

    fn stop_where(&self, unwanted: impl Fn(&LoopKey) -> bool) {
        let stopped: Vec<TrackedLoop> = lock(&self.loops)
            .extract_if(|key, _| unwanted(key))
            .map(|(_, tracked)| tracked)
            .collect();
        self.retire(stopped, Stop::stop);
    }

    pub fn stop(&self, keys: &[LoopKey]) {
        self.stop_where(|key| keys.contains(key));
    }

    pub fn stop_service(&self, tenant_slug: &str, service_name: &str) {
        self.stop_where(|key| key.tenant_slug == tenant_slug && key.service_name == service_name);
    }

    fn forget(&self, key: &LoopKey, generation: u64) {
        let mut loops = lock(&self.loops);
        if loops.get(key).is_some_and(|t| t.generation == generation) {
            loops.remove(key);
        }
    }

    pub async fn lock_service_setup(
        &self,
        tenant_slug: &str,
        service_name: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        keyed_lock(
            &self.setup_locks,
            (tenant_slug.to_string(), service_name.to_string()),
        )
        .lock_owned()
        .await
    }

    pub(super) async fn lock_tenant_stream(
        &self,
        tenant_slug: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        keyed_lock(&self.tenant_stream_locks, tenant_slug.to_string())
            .lock_owned()
            .await
    }

    pub fn running(&self) -> Vec<RunningLoop> {
        lock(&self.loops)
            .iter()
            .map(|(key, tracked)| RunningLoop {
                key: key.clone(),
                spec_hash: tracked.spec_hash,
            })
            .collect()
    }

    pub fn running_for(&self, tenant_slug: &str, service_name: &str) -> usize {
        lock(&self.loops)
            .keys()
            .filter(|k| k.tenant_slug == tenant_slug && k.service_name == service_name)
            .count()
    }

    pub fn loop_count(&self) -> usize {
        lock(&self.loops).len()
    }

    pub fn stop_all(&self) {
        for tracked in lock(&self.loops).values() {
            tracked.stop.stop();
        }
    }

    pub async fn join_all(&self, deadline: Duration) -> DrainOutcome {
        let current: Vec<JoinHandle<()>> = lock(&self.loops)
            .drain()
            .map(|(_, tracked)| tracked.handle)
            .collect();
        let handles: Vec<JoinHandle<()>> = current
            .into_iter()
            .chain(lock(&self.retiring).drain(..))
            .collect();
        let total = handles.len();
        let joined = tokio::time::timeout(deadline, async {
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await;
        match joined {
            Ok(()) => DrainOutcome::Completed,
            Err(_) => DrainOutcome::TimedOut {
                still_running: total,
            },
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn keyed_lock<K: std::hash::Hash + Eq>(
    locks: &KeyedLocks<K>,
    key: K,
) -> Arc<tokio::sync::Mutex<()>> {
    lock(locks).entry(key).or_default().clone()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionSpec {
    handler: String,
    handler_type: HandlerType,
    config: BTreeMap<String, String>,
    ack: AckMode,
    timeout: Duration,
    concurrency: usize,
    batch: Option<usize>,
    backoff: Vec<Duration>,
    dead_letter: bool,
}

impl SubscriptionSpec {
    pub fn of(sub: &SubscriptionDefinition) -> Self {
        let declared = sub.consumer.clone().unwrap_or_default();
        let count = |n: u32| usize::try_from(n).unwrap_or(usize::MAX).max(1);
        Self {
            handler: sub.handler.clone(),
            handler_type: sub.handler_type.clone(),
            config: sub.config.clone().into_iter().collect(),
            ack: sub.ack,
            timeout: Duration::from_secs(
                sub.timeout_secs
                    .unwrap_or(SubscriptionDefinition::DEFAULT_TIMEOUT_SECS)
                    .into(),
            ),
            concurrency: declared.concurrency.map_or(1, count),
            batch: declared.batch.map(count),
            backoff: declared
                .backoff_secs
                .iter()
                .map(|secs| Duration::from_secs((*secs).into()))
                .collect(),
            dead_letter: declared
                .dead_letter
                .unwrap_or(sub.ack == AckMode::OnSuccess),
        }
    }

    fn backoff(&self, bound: bool, ack_wait: Duration) -> Backoff {
        match (self.backoff.is_empty(), bound) {
            (false, _) => Backoff::explicit(self.backoff.clone()),
            (true, true) => Backoff::explicit(vec![ack_wait]),
            (true, false) => Backoff::exponential(RETRY_BASE, RETRY_MAX),
        }
    }

    pub(crate) fn pump(&self, ack_wait: Duration) -> PumpConfig {
        let concurrent = PumpConfig::sequential(ack_wait).concurrent(self.concurrency);
        PumpConfig {
            batch: self.batch.unwrap_or(concurrent.batch),
            ..concurrent
        }
    }
}

const RETRY_BASE: Duration = Duration::from_secs(5);
const RETRY_MAX: Duration = Duration::from_secs(300);
pub const RETRY_AFTER_MAX: Duration = Duration::from_secs(60 * 60);

pub async fn start(state: &Arc<AppState>, wanted: &DesiredLoop) -> eyre::Result<()> {
    let consumer: PullConsumer = state
        .jetstream
        .get_stream(&wanted.consumer.stream)
        .await?
        .get_consumer(wanted.consumer.name.as_str())
        .await
        .map_err(|e| {
            eyre::eyre!(
                "durable '{}' on stream '{}' cannot be consumed: {e}",
                wanted.consumer.name,
                wanted.consumer.stream
            )
        })?;
    info!(
        "Starting subscription: {} -> {} ({}) on stream '{}'",
        wanted.key().subject,
        wanted.key().handler,
        wanted.consumer.name,
        wanted.consumer.stream
    );
    let served = Served::of(&consumer.cached_info().config);
    let (work, pump): (Arc<dyn Work>, PumpConfig) = match &wanted.work {
        WorkSpec::Subscription(spec) => (
            Arc::new(SubscriptionWork {
                ctx: Arc::new(SubscriptionContext {
                    ack: spec.ack,
                    handler_type: spec.handler_type.clone(),
                    handler_name: spec.handler.clone(),
                    config: spec.config.clone().into_iter().collect(),
                    timeout: spec.timeout,
                    max_deliver: served.max_deliver,
                    backoff: spec.backoff(wanted.consumer.name.is_bound(), served.ack_wait),
                    dead_letter: spec.dead_letter,
                    state: state.clone(),
                    identity: wanted.identity.clone(),
                    subject: wanted.key().subject.clone(),
                }),
            }),
            spec.pump(served.ack_wait),
        ),
        WorkSpec::Schedule(run) => (
            Arc::new(ScheduleWork::new(
                state,
                &wanted.identity,
                run,
                served.max_deliver,
            )),
            run.pump(served.ack_wait),
        ),
    };
    let tasks = &state.subscription_tasks;
    let generation = tasks.next_generation();
    let stop = Stop::new();
    let handle = tokio::spawn(consume(
        consumer,
        pump,
        work,
        Consuming {
            state: state.clone(),
            consumer: wanted.consumer.clone(),
            stop: stop.clone(),
            generation,
        },
    ));
    tasks.track(
        wanted.key().clone(),
        TrackedLoop {
            generation,
            spec_hash: wanted.spec_hash(served),
            stop,
            handle,
        },
    );
    Ok(())
}

struct Consuming {
    state: Arc<AppState>,
    consumer: DesiredConsumer,
    stop: Stop,
    generation: u64,
}

struct SubscriptionContext {
    ack: AckMode,
    handler_type: HandlerType,
    handler_name: String,
    config: std::collections::HashMap<String, String>,
    timeout: Duration,
    max_deliver: i64,
    backoff: Backoff,
    dead_letter: bool,
    state: Arc<AppState>,
    identity: ServiceIdentity,
    subject: String,
}

fn subscription_span(
    identity: &ServiceIdentity,
    handler: &str,
    subject: &str,
    headers: Option<&HeaderMap>,
) -> tracing::Span {
    let span = scoped_span!(
        parent: None,
        "subscription",
        identity.scope(None).running(Kind::Subscription, handler),
        otel.name = %format!("{}/{}/{}", identity.tenant_slug, identity.service_name, handler),
        otel.kind = "consumer",
        otel.status_code = tracing::field::Empty,
        messaging.subject = %subject,
    );
    if let Some(parent) = headers.and_then(crate::telemetry::context_from_nats_headers) {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let _ = span.set_parent(parent);
    }
    span
}

async fn consume(
    consumer: PullConsumer,
    pump: PumpConfig,
    work: Arc<dyn Work>,
    consuming: Consuming,
) {
    let subject = &consuming.consumer.key.subject;
    info!("Subscription consumer started for: {}", subject);
    let exit = pump::run(
        consumer,
        pump,
        work,
        Arc::new(consuming.state.jetstream.clone()),
        consuming.stop.clone(),
        consuming.state.shutdown.clone(),
    )
    .await;
    match exit {
        PumpExit::ConsumerGone => {
            warn!(
                stream = %consuming.consumer.stream,
                durable = %consuming.consumer.name,
                "The durable of {} is gone; its loop ends",
                subject
            );
            consuming
                .state
                .subscription_tasks
                .forget(&consuming.consumer.key, consuming.generation);
            consuming.state.reconciler.trigger();
        }
        PumpExit::Stopped | PumpExit::ShuttingDown => info!(
            "Subscription consumer for {} stopped (replaced, undeployed or shutting down)",
            subject
        ),
    }
}

struct SubscriptionWork {
    ctx: Arc<SubscriptionContext>,
}

impl Work for SubscriptionWork {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "subscription",
            subscription: Some(SubscriptionLabel {
                tenant: self.ctx.identity.tenant_slug.clone(),
                service: self.ctx.identity.service_name.clone(),
                handler: self.ctx.handler_name.clone(),
            }),
        }
    }

    fn handle(
        &self,
        msg: Arc<jetstream::Message>,
        meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let ctx = Arc::clone(&self.ctx);
        async move { handle_message(&ctx, &msg, &meta).await }.boxed()
    }

    fn decide(&self, outcome: &Outcome, meta: &MessageMeta) -> Decision {
        decide(
            self.ctx.ack,
            outcome,
            meta.delivered,
            self.ctx.max_deliver,
            &self.ctx.backoff,
            self.ctx.dead_letter,
            meta.stream_sequence,
        )
    }

    fn headroom(&self) -> BoxFuture<'_, usize> {
        match (&self.ctx.handler_type, &self.ctx.state.wasm_engine) {
            (HandlerType::Wasm, Some(engine)) => engine
                .headroom(&self.ctx.identity, Some(&self.ctx.handler_name))
                .boxed(),
            _ => std::future::ready(usize::MAX).boxed(),
        }
    }

    fn dead_letter<'a>(
        &'a self,
        msg: &'a jetstream::Message,
        meta: &'a MessageMeta,
        reason: &'a str,
    ) -> BoxFuture<'a, Option<DeadLetter>> {
        std::future::ready(Some(DeadLetter::now(
            dlq_subject(
                &self.ctx.identity.tenant_slug,
                &self.ctx.identity.service_name,
                DlqKind::Subscription,
                &self.ctx.handler_name,
            ),
            msg.payload.clone(),
            meta,
            reason,
        )))
        .boxed()
    }
}

pub(crate) enum HandlerFailure {
    Interrupted(eyre::Report),
    Failed(eyre::Report),
    Deferred {
        after: Duration,
        reason: eyre::Report,
    },
}

impl HandlerFailure {
    pub(crate) fn outcome(&self) -> Outcome {
        match self {
            HandlerFailure::Interrupted(_) => Outcome::Interrupted,
            HandlerFailure::Deferred { after, .. } => Outcome::Deferred { after: *after },
            HandlerFailure::Failed(e) => Outcome::Retry {
                after: None,
                reason: e.to_string(),
            },
        }
    }
}

impl From<eyre::Report> for HandlerFailure {
    fn from(error: eyre::Report) -> Self {
        HandlerFailure::Failed(error)
    }
}

enum HandlerOutcome {
    Rpc(Result<String, HandlerFailure>),
    SideEffect(eyre::Result<()>),
}

fn answers_when_interrupted(ack: AckMode) -> bool {
    ack == AckMode::Always
}

impl HandlerOutcome {
    fn error(&self) -> Option<String> {
        match self {
            HandlerOutcome::Rpc(Ok(_)) | HandlerOutcome::SideEffect(Ok(())) => None,
            HandlerOutcome::Rpc(Err(
                HandlerFailure::Interrupted(e)
                | HandlerFailure::Failed(e)
                | HandlerFailure::Deferred { reason: e, .. },
            ))
            | HandlerOutcome::SideEffect(Err(e)) => Some(e.to_string()),
        }
    }

    fn outcome(&self, ack: AckMode) -> Outcome {
        match self {
            HandlerOutcome::Rpc(Ok(response)) if ack == AckMode::OnSuccess => answered(response),
            HandlerOutcome::Rpc(Ok(_)) | HandlerOutcome::SideEffect(Ok(())) => Outcome::Ok,
            HandlerOutcome::Rpc(Err(failure)) => failure.outcome(),
            HandlerOutcome::SideEffect(Err(e)) => Outcome::Retry {
                after: None,
                reason: e.to_string(),
            },
        }
    }
}

pub(crate) fn answered(response: &str) -> Outcome {
    let (status, delivery) = match guest_response::decode(response.to_string()) {
        Ok(GuestResponse::Legacy { status, .. }) => (status, None),
        Ok(GuestResponse::V2(v2)) => (v2.status, v2.delivery),
        Err(e) => {
            return Outcome::Retry {
                after: None,
                reason: format!("handler answered an {e}"),
            }
        }
    };
    match delivery.map(|delivery| *delivery) {
        Some(DeliveryControl::RetryAfter { retry_after_ms }) => Outcome::Retry {
            after: Some(Duration::from_millis(retry_after_ms).min(RETRY_AFTER_MAX)),
            reason: format!("handler asked to be retried ({status})"),
        },
        Some(DeliveryControl::Terminal { terminal }) => Outcome::Terminal { reason: terminal },
        None if status.is_server_error() => Outcome::Retry {
            after: None,
            reason: format!("handler answered {}", status.as_u16()),
        },
        None if status.is_client_error() => Outcome::Terminal {
            reason: format!("handler rejected the message with {}", status.as_u16()),
        },
        None => Outcome::Ok,
    }
}

async fn handle_message(
    ctx: &SubscriptionContext,
    msg: &jetstream::Message,
    meta: &MessageMeta,
) -> Outcome {
    let payload: Value = match serde_json::from_slice(&msg.payload) {
        Ok(v) => v,
        Err(e) => {
            warn!("Invalid JSON in event: {}", e);
            return Outcome::Invalid {
                reason: format!("the payload is not JSON: {e}"),
            };
        }
    };

    let reply_inbox = reply_inbox(msg.headers.as_ref());

    let handler_span = subscription_span(
        &ctx.identity,
        &ctx.handler_name,
        &ctx.subject,
        msg.headers.as_ref(),
    );
    let handled = async {
        match ctx.handler_type {
            HandlerType::DeleteCascade => HandlerOutcome::SideEffect(
                handle_delete_cascade(&ctx.state, &ctx.identity, &ctx.config, &payload).await,
            ),
            HandlerType::UpdateField => HandlerOutcome::SideEffect(
                handle_update_field(&ctx.state, &ctx.identity, &ctx.config, &payload).await,
            ),
            HandlerType::Webhook => HandlerOutcome::SideEffect(
                handle_webhook(
                    &ctx.state,
                    ctx.identity.tenant_id,
                    &ctx.identity.tenant_slug,
                    &ctx.config,
                    &payload,
                )
                .await,
            ),
            HandlerType::Wasm => HandlerOutcome::Rpc(handle_wasm(ctx, &payload, meta).await),
        }
    }
    .instrument(handler_span.clone())
    .await;

    let outcome = handled.outcome(ctx.ack);
    let failed = handled.error().or_else(|| match &outcome {
        Outcome::Retry { reason, .. } | Outcome::Terminal { reason } => Some(reason.clone()),
        _ => None,
    });
    if let Some(reason) = failed {
        handler_span.record("otel.status_code", "ERROR");
        handler_span
            .in_scope(|| error!("Subscription handler error for {}: {}", ctx.subject, reason));
    }
    drop(handler_span);

    match (reply_inbox.as_deref(), handled) {
        (Some(inbox), HandlerOutcome::Rpc(Ok(response))) => {
            publish_reply(&ctx.state.nats, inbox, response).await;
        }
        (Some(inbox), HandlerOutcome::Rpc(Err(HandlerFailure::Failed(e)))) => {
            publish_reply(&ctx.state.nats, inbox, error_envelope(&e.to_string())).await;
        }
        (Some(inbox), HandlerOutcome::Rpc(Err(HandlerFailure::Interrupted(e)))) => {
            if answers_when_interrupted(ctx.ack) {
                publish_reply(&ctx.state.nats, inbox, error_envelope(&e.to_string())).await;
            }
        }
        (Some(_), HandlerOutcome::Rpc(Err(HandlerFailure::Deferred { .. }))) => {}
        (Some(_), HandlerOutcome::SideEffect(_)) => {
            warn!(
                "Subscription {} received `{}` for non-RPC handler `{:?}`; ignoring",
                ctx.subject, CUFFLINK_REPLY_HEADER, ctx.handler_type
            );
        }
        (None, _) => {}
    }
    outcome
}

const INTERNAL_HEADER_PREFIX: &str = "cufflink-";

pub(crate) fn delivery_meta(meta: &MessageMeta) -> SubscriptionMeta {
    SubscriptionMeta {
        subject: meta.subject.clone(),
        stream: meta.stream.clone(),
        consumer: meta.consumer.clone(),
        stream_sequence: meta.stream_sequence,
        consumer_sequence: meta.consumer_sequence,
        num_delivered: meta.delivered,
        num_pending: meta.pending,
        published_at: meta
            .published
            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        headers: meta
            .headers
            .iter()
            .filter(|(name, _)| !header_has_prefix(name, INTERNAL_HEADER_PREFIX))
            .cloned()
            .collect(),
    }
}

fn subscription_request(
    tenant_slug: &str,
    service_name: &str,
    handler_name: &str,
    event: &Value,
    meta: &MessageMeta,
) -> GuestRequest {
    GuestRequest {
        method: METHOD_SUBSCRIPTION.to_string(),
        handler: handler_name.to_string(),
        headers: Some(serde_json::Map::new()),
        body: event.clone(),
        tenant: tenant_slug.to_string(),
        service: service_name.to_string(),
        auth: Some(Value::Null),
        subscription: Some(delivery_meta(meta)),
        ..GuestRequest::default()
    }
}

pub(crate) struct Run<'a> {
    pub identity: &'a ServiceIdentity,
    pub handler: &'a str,
    pub request: &'a GuestRequest,
    pub budget: Duration,
    pub kind: Kind,
}

async fn handle_wasm(
    ctx: &SubscriptionContext,
    event: &Value,
    meta: &MessageMeta,
) -> Result<String, HandlerFailure> {
    run_handler(
        &ctx.state,
        Run {
            identity: &ctx.identity,
            handler: &ctx.handler_name,
            request: &subscription_request(
                &ctx.identity.tenant_slug,
                &ctx.identity.service_name,
                &ctx.handler_name,
                event,
                meta,
            ),
            budget: ctx.timeout,
            kind: Kind::Subscription,
        },
    )
    .await
}

pub(crate) async fn run_handler(
    state: &Arc<AppState>,
    run: Run<'_>,
) -> Result<String, HandlerFailure> {
    let (tenant_slug, service_name, handler_name, what) = (
        run.identity.tenant_slug.as_str(),
        run.identity.service_name.as_str(),
        run.handler,
        run.kind.as_str(),
    );
    let wasm_engine = state
        .wasm_engine
        .as_ref()
        .ok_or_else(|| eyre::eyre!("WASM runtime not available"))?;

    let service = state
        .registry
        .get_by_slug(tenant_slug, service_name)
        .await
        .ok_or_else(|| {
            eyre::eyre!(
                "Service '{}/{}' not found in registry",
                tenant_slug,
                service_name
            )
        })?;
    service
        .scope()
        .running(run.kind, handler_name)
        .record_on(&tracing::Span::current());

    let compiled = wasm_engine
        .get_module(
            service.service_id,
            service.version,
            service.wasm_hash.as_deref(),
        )
        .await
        .ok_or_else(|| {
            eyre::eyre!(
                "No WASM module loaded for {}/{} v{}",
                tenant_slug,
                service_name,
                service.version
            )
        })?;

    let request_str = crate::router::guest_text(run.request).map_err(eyre::Report::from)?;

    info!(
        tenant = %tenant_slug,
        service = %service_name,
        "Executing WASM {} handler '{}'", what, handler_name
    );

    let identity = service.identity();
    let response_str = wasm_engine
        .run_retrying_capacity(
            state.clone(),
            InvocationKind::Subscription,
            request_str,
            |request_json| Invocation {
                compiled: Arc::clone(&compiled),
                handler: handler_name.to_string(),
                request_json,
                identity: identity.clone(),
                publishes: service.manifest.publishes.clone(),
                extensions: service.manifest.extensions.clone(),
                kind: InvocationKind::Subscription,
                scope_kind: Some(run.kind),
                budget: run.budget,
                cancel: None,
                class: Class::Background,
                admission: None,
            },
        )
        .await
        .map_err(|e| {
            let interrupted = e.interrupted_by_shutdown(state.shutdown.hard_deadline_reached());
            let deferred = match &e {
                ExecutionError::Overloaded { retry_after, .. } => Some(*retry_after),
                _ => None,
            };
            let report = match e {
                ExecutionError::DeadlineExceeded { budget } => eyre::eyre!(
                    "WASM {} handler timed out after {}s",
                    what,
                    budget.as_secs()
                ),
                other => eyre::eyre!("WASM execution error: {}", other),
            };
            match (deferred, interrupted) {
                (Some(after), _) => HandlerFailure::Deferred {
                    after,
                    reason: report,
                },
                (None, true) => HandlerFailure::Interrupted(report),
                (None, false) => HandlerFailure::Failed(report),
            }
        })?;

    info!(
        tenant = %tenant_slug,
        service = %service_name,
        "WASM {} handler '{}' completed: {}",
        what,
        handler_name,
        response_preview(&response_str)
    );

    Ok(response_str)
}

fn response_preview(response: &str) -> String {
    let preview: String = response.chars().take(200).collect();
    if preview.len() < response.len() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn reply_inbox(headers: Option<&HeaderMap>) -> Option<String> {
    let requested = headers
        .and_then(|h| h.get(CUFFLINK_REPLY_HEADER))
        .map(|v| v.as_str().trim())
        .filter(|s| !s.is_empty())?;
    if requested.starts_with(PLATFORM_INBOX_PREFIX) && is_valid_publish_subject(requested) {
        Some(requested.to_string())
    } else {
        metrics::counter!("cufflink_nats_reply_ignored_total").increment(1);
        warn!(reply = %requested, "Ignoring a Cufflink-Reply header that names no platform inbox");
        None
    }
}

fn error_envelope(message: &str) -> String {
    json!({ "error": message }).to_string()
}

async fn publish_reply(nats: &async_nats::Client, inbox: &str, payload: String) {
    if let Err(e) = crate::nats_conn::publish_bounded(
        nats,
        inbox.to_string(),
        None,
        payload.into_bytes().into(),
    )
    .await
    {
        warn!("Failed to publish RPC reply to {}: {}", inbox, e);
    }
}

async fn handle_delete_cascade(
    state: &Arc<AppState>,
    identity: &ServiceIdentity,
    config: &std::collections::HashMap<String, String>,
    event: &Value,
) -> eyre::Result<()> {
    let table = config
        .get("table")
        .ok_or_else(|| eyre::eyre!("delete_cascade requires 'table' config"))?;
    let fk = config
        .get("foreign_key")
        .ok_or_else(|| eyre::eyre!("delete_cascade requires 'foreign_key' config"))?;

    if !is_safe_identifier(table) {
        return Err(eyre::eyre!(
            "Unsafe table name in delete_cascade config: {}",
            table
        ));
    }
    if !is_safe_identifier(fk) {
        return Err(eyre::eyre!(
            "Unsafe foreign_key in delete_cascade config: {}",
            fk
        ));
    }

    let deleted_id = event["data"]["id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Event missing data.id"))?;

    let parsed_id: uuid::Uuid = deleted_id.parse()?;

    let client = state
        .tenant_sessions
        .acquire(identity, Purpose::Platform, None)
        .await?;
    let schema = client.schema_ident();
    let sql = format!("DELETE FROM {}.{} WHERE {} = $1", schema, table, fk);
    let count = client.execute(&sql, &[&parsed_id]).await?;

    info!(
        "delete_cascade: deleted {} rows from {}.{} where {}={}",
        count, schema, table, fk, deleted_id
    );

    Ok(())
}

async fn handle_update_field(
    state: &Arc<AppState>,
    identity: &ServiceIdentity,
    config: &std::collections::HashMap<String, String>,
    event: &Value,
) -> eyre::Result<()> {
    let table = config
        .get("table")
        .ok_or_else(|| eyre::eyre!("update_field requires 'table' config"))?;
    let where_col = config
        .get("where_column")
        .ok_or_else(|| eyre::eyre!("update_field requires 'where_column' config"))?;
    let set_col = config
        .get("set_column")
        .ok_or_else(|| eyre::eyre!("update_field requires 'set_column' config"))?;
    let set_value = config
        .get("set_value")
        .ok_or_else(|| eyre::eyre!("update_field requires 'set_value' config"))?;

    if !is_safe_identifier(table) {
        return Err(eyre::eyre!(
            "Unsafe table name in update_field config: {}",
            table
        ));
    }
    if !is_safe_identifier(where_col) {
        return Err(eyre::eyre!(
            "Unsafe where_column in update_field config: {}",
            where_col
        ));
    }
    if !is_safe_identifier(set_col) {
        return Err(eyre::eyre!(
            "Unsafe set_column in update_field config: {}",
            set_col
        ));
    }
    if !set_value
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == ' ' || c == '+' || c == '-' || c == '.')
    {
        return Err(eyre::eyre!(
            "Unsafe set_value in update_field config: {}",
            set_value
        ));
    }

    let entity_id = event["data"]["id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Event missing data.id"))?;

    let client = state
        .tenant_sessions
        .acquire(identity, Purpose::Platform, None)
        .await?;
    let schema = client.schema_ident();
    let sql = format!(
        "UPDATE {}.{} SET {} = {} WHERE {} = $1",
        schema, table, set_col, set_value, where_col
    );
    let parsed_id: uuid::Uuid = entity_id.parse()?;
    client.execute(&sql, &[&parsed_id]).await?;

    info!(
        "update_field: updated {}.{} set {}={} where {}={}",
        schema, table, set_col, set_value, where_col, entity_id
    );

    Ok(())
}

async fn handle_webhook(
    state: &Arc<AppState>,
    tenant_id: uuid::Uuid,
    tenant_slug: &str,
    config: &std::collections::HashMap<String, String>,
    event: &Value,
) -> eyre::Result<()> {
    let url = config
        .get("url")
        .ok_or_else(|| eyre::eyre!("webhook requires 'url' config"))?;
    let method = match config.get("method").map(|m| m.to_uppercase()).as_deref() {
        Some("PUT") => reqwest::Method::PUT,
        _ => reqwest::Method::POST,
    };
    let policies = state.tenants.policies_or_load(tenant_id).await?;
    let policy = EgressPolicy::new(&policies, tenant_slug);
    let check = |target: &EgressTarget| policy.check(target);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let request = OutboundRequest {
        method: method.clone(),
        url: url.clone(),
        headers,
        body: Some(serde_json::to_vec(event)?),
        timeout: WEBHOOK_HANDLER_TIMEOUT,
    };
    let response = state.egress.send(request, &check, true).await?;
    let status = response.status();
    info!("webhook: {} {} -> {}", method, url, status);
    if status.is_success() {
        Ok(())
    } else {
        Err(eyre::eyre!("webhook {url} answered {status}"))
    }
}

const WEBHOOK_HANDLER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(values: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in values {
            h.append(*k, *v);
        }
        h
    }

    fn delivery(headers: &[(&str, &[&str])]) -> MessageMeta {
        MessageMeta {
            subject: "dw.acme.books.created".into(),
            stream: "DW_ACME".into(),
            consumer: "loans_service_on_book".into(),
            stream_sequence: 81_234,
            consumer_sequence: 17,
            delivered: 2,
            pending: 3,
            published: chrono::DateTime::from_timestamp(1_790_589_600, 250_000_000)
                .expect("instant"),
            headers: headers
                .iter()
                .map(|(name, values)| {
                    (
                        name.to_string(),
                        values.iter().map(|value| value.to_string()).collect(),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn a_message_starts_its_own_trace_under_its_pump() {
        let identity = crate::schedules::tests::identity("acme", "notes");
        assert!(crate::telemetry::is_a_root_under_a_pump(|| {
            subscription_span(&identity, "on_book", "dw.acme.books.created", None)
        }));
    }

    #[test]
    fn a_subscription_request_has_the_keys_it_always_had_and_its_delivery() {
        let meta = delivery(&[
            ("Cufflink-Reply", &["_INBOX.x"]),
            ("cufflink-internal", &["1"]),
            ("Nats-Msg-Id", &["book-7"]),
            ("traceparent", &["00-abc-def-01"]),
            ("x-source", &["a", "b"]),
        ]);
        assert_eq!(
            subscription_request("acme", "loans-service", "on_book", &json!({"id": 7}), &meta)
                .to_value()
                .expect("request"),
            json!({
                "method": "SUBSCRIPTION",
                "handler": "on_book",
                "headers": {},
                "body": {"id": 7},
                "tenant": "acme",
                "service": "loans-service",
                "auth": null,
                "subscription": {
                    "subject": "dw.acme.books.created",
                    "stream": "DW_ACME",
                    "consumer": "loans_service_on_book",
                    "stream_sequence": 81234,
                    "consumer_sequence": 17,
                    "num_delivered": 2,
                    "num_pending": 3,
                    "published_at": "2026-09-28T10:00:00.250Z",
                    "headers": {
                        "Nats-Msg-Id": ["book-7"],
                        "traceparent": ["00-abc-def-01"],
                        "x-source": ["a", "b"],
                    },
                },
            })
        );
    }

    #[test]
    fn a_subscription_reads_the_text_it_always_read() {
        assert_eq!(
            crate::router::guest_text(&subscription_request(
                "acme",
                "loans-service",
                "on_book",
                &json!({"id": 7, "data": {"title": "x", "amount": 3}}),
                &delivery(&[]),
            ))
            .expect("text"),
            concat!(
                r#"{"auth":null,"body":{"data":{"amount":3,"title":"x"},"id":7},"handler":"on_book","headers":{},"method":"SUBSCRIPTION","service":"loans-service","#,
                r#""subscription":{"consumer":"loans_service_on_book","consumer_sequence":17,"headers":{},"num_delivered":2,"num_pending":3,"published_at":"2026-09-28T10:00:00.250Z","stream":"DW_ACME","stream_sequence":81234,"subject":"dw.acme.books.created"},"#,
                r#""tenant":"acme"}"#
            )
        );
    }

    #[test]
    fn an_interrupted_handler_is_answered_only_when_it_is_not_delivered_again() {
        assert!(answers_when_interrupted(AckMode::Always));
        assert!(!answers_when_interrupted(AckMode::OnSuccess));
        let interrupted = || {
            HandlerOutcome::Rpc(Err(HandlerFailure::Interrupted(eyre::eyre!(
                "WASM execution error: shutting down"
            ))))
        };
        for ack in [AckMode::Always, AckMode::OnSuccess] {
            assert_eq!(interrupted().outcome(ack), Outcome::Interrupted);
        }
        assert_eq!(
            interrupted().error().as_deref(),
            Some("WASM execution error: shutting down")
        );
        assert_eq!(HandlerOutcome::SideEffect(Ok(())).error(), None);
    }

    #[test]
    fn reply_inbox_returns_none_when_headers_absent() {
        assert_eq!(reply_inbox(None), None);
    }

    #[test]
    fn reply_inbox_returns_none_when_header_missing() {
        let h = headers_with(&[("Other", "value")]);
        assert_eq!(reply_inbox(Some(&h)), None);
    }

    #[test]
    fn reply_inbox_extracts_value() {
        let h = headers_with(&[(CUFFLINK_REPLY_HEADER, "_INBOX.abc")]);
        assert_eq!(reply_inbox(Some(&h)), Some("_INBOX.abc".to_string()));
    }

    #[test]
    fn reply_inbox_trims_whitespace() {
        let h = headers_with(&[(CUFFLINK_REPLY_HEADER, "  _INBOX.xyz  ")]);
        assert_eq!(reply_inbox(Some(&h)), Some("_INBOX.xyz".to_string()));
    }

    #[test]
    fn reply_inbox_rejects_empty_value() {
        let h = headers_with(&[(CUFFLINK_REPLY_HEADER, "   ")]);
        assert_eq!(reply_inbox(Some(&h)), None);
    }

    #[test]
    fn reply_inbox_ignores_anything_but_a_platform_inbox() {
        for target in [
            "dw.victim.logs",
            "cufflink.wasm.svc.h",
            "_INBOX.*",
            "_INBOX",
        ] {
            let h = headers_with(&[(CUFFLINK_REPLY_HEADER, target)]);
            assert_eq!(reply_inbox(Some(&h)), None, "{target}");
        }
    }

    #[test]
    fn reply_inbox_returns_first_when_multiple_values() {
        let h = headers_with(&[
            (CUFFLINK_REPLY_HEADER, "_INBOX.first"),
            (CUFFLINK_REPLY_HEADER, "_INBOX.second"),
        ]);
        assert_eq!(reply_inbox(Some(&h)), Some("_INBOX.first".to_string()));
    }

    #[test]
    fn error_envelope_serializes_message() {
        let parsed: Value = serde_json::from_str(&error_envelope("boom")).unwrap();
        assert_eq!(parsed["error"], json!("boom"));
        assert_eq!(parsed.as_object().unwrap().len(), 1);
    }

    #[test]
    fn error_envelope_escapes_quotes() {
        let parsed: Value =
            serde_json::from_str(&error_envelope(r#"bad "quoted" payload"#)).unwrap();
        assert_eq!(parsed["error"], json!(r#"bad "quoted" payload"#));
    }
}

#[cfg(test)]
mod task_tracking_tests {
    use super::*;

    fn key(service: &str, handler: &str) -> LoopKey {
        LoopKey {
            tenant_slug: "acme".into(),
            service_name: service.into(),
            handler: handler.into(),
            subject: format!("dw.acme.{handler}"),
        }
    }

    fn running(
        tasks: &SubscriptionTasks,
        key: LoopKey,
        spec_hash: u64,
        handle: JoinHandle<()>,
    ) -> (u64, Stop) {
        let generation = tasks.next_generation();
        let stop = Stop::new();
        tasks.track(
            key,
            TrackedLoop {
                generation,
                spec_hash,
                stop: stop.clone(),
                handle,
            },
        );
        (generation, stop)
    }

    fn track(tasks: &SubscriptionTasks, key: LoopKey) -> (u64, Stop) {
        running(tasks, key, 0, tokio::spawn(async {}))
    }

    #[tokio::test]
    async fn starting_again_replaces_and_stops_the_old_loop() {
        let tasks = SubscriptionTasks::default();
        let (_, old) = track(&tasks, key("orders", "on_paid"));
        let (_, new) = track(&tasks, key("orders", "on_paid"));
        assert!(old.is_stopped() && old.is_replaced());
        assert!(!new.is_stopped());
        assert_eq!(tasks.running_for("acme", "orders"), 1);
    }

    #[tokio::test]
    async fn stopped_loops_stop_and_the_others_are_untouched() {
        let tasks = SubscriptionTasks::default();
        let (_, kept) = track(&tasks, key("orders", "on_paid"));
        let (_, dropped) = track(&tasks, key("orders", "on_refund"));
        let (_, other) = track(&tasks, key("billing", "on_refund"));
        tasks.stop(&[key("orders", "on_refund"), key("orders", "never_ran")]);
        assert!(!kept.is_stopped());
        assert!(dropped.is_stopped() && !dropped.is_replaced());
        assert!(!other.is_stopped());
        assert_eq!(tasks.running_for("acme", "orders"), 1);

        tasks.stop_service("acme", "orders");
        assert!(kept.is_stopped() && !kept.is_replaced());
        assert_eq!(tasks.running_for("acme", "orders"), 0);
        assert_eq!(tasks.running_for("acme", "billing"), 1);
    }

    #[tokio::test]
    async fn the_loops_that_run_are_told_with_what_they_do() {
        let tasks = SubscriptionTasks::default();
        running(&tasks, key("orders", "on_paid"), 7, tokio::spawn(async {}));
        running(&tasks, key("orders", "on_paid"), 9, tokio::spawn(async {}));
        assert_eq!(
            tasks.running(),
            [RunningLoop {
                key: key("orders", "on_paid"),
                spec_hash: 9,
            }]
        );
    }

    #[tokio::test]
    async fn join_all_waits_for_replaced_and_stopped_loops_to_finish() {
        let tasks = SubscriptionTasks::default();
        let (release, released) = tokio::sync::watch::channel(false);
        let draining = |key: LoopKey| {
            let mut released = released.clone();
            running(
                &tasks,
                key,
                0,
                tokio::spawn(async move {
                    let _ = released.wait_for(|done| *done).await;
                }),
            );
        };
        draining(key("orders", "on_paid"));
        draining(key("orders", "on_refund"));
        track(&tasks, key("orders", "on_paid"));
        tasks.stop(&[key("orders", "on_refund")]);
        assert_eq!(tasks.running_for("acme", "orders"), 1);
        assert_eq!(
            tasks.join_all(Duration::from_millis(50)).await,
            DrainOutcome::TimedOut { still_running: 3 },
            "the shutdown waits for loops still handling their message"
        );

        draining(key("orders", "on_paid"));
        track(&tasks, key("orders", "on_paid"));
        release.send_replace(true);
        assert_eq!(
            tasks.join_all(Duration::from_secs(1)).await,
            DrainOutcome::Completed
        );
    }

    #[tokio::test]
    async fn forget_ignores_a_replaced_loop() {
        let tasks = SubscriptionTasks::default();
        let (first, _) = track(&tasks, key("orders", "on_paid"));
        let (second, _) = track(&tasks, key("orders", "on_paid"));
        tasks.forget(&key("orders", "on_paid"), first);
        assert_eq!(tasks.running_for("acme", "orders"), 1);
        tasks.forget(&key("orders", "on_paid"), second);
        assert_eq!(tasks.running_for("acme", "orders"), 0);
    }

    #[tokio::test]
    async fn stop_all_flags_every_loop_and_join_all_empties_the_table() {
        let tasks = SubscriptionTasks::default();
        let (_, a) = track(&tasks, key("orders", "on_paid"));
        let (_, b) = track(&tasks, key("billing", "on_refund"));
        assert_eq!(tasks.loop_count(), 2);
        tasks.stop_all();
        assert!(a.is_stopped() && b.is_stopped());
        assert!(!a.is_replaced() && !b.is_replaced());
        assert_eq!(
            tasks.join_all(Duration::from_secs(1)).await,
            DrainOutcome::Completed
        );
        assert_eq!(tasks.loop_count(), 0);

        running(
            &tasks,
            key("orders", "on_paid"),
            0,
            tokio::spawn(std::future::pending()),
        );
        assert_eq!(
            tasks.join_all(Duration::from_millis(50)).await,
            DrainOutcome::TimedOut { still_running: 1 }
        );
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;
    use cufflink_types::ConsumerSpec;

    fn retried(outcome: Outcome) -> Option<Option<Duration>> {
        match outcome {
            Outcome::Retry { after, .. } => Some(after),
            _ => None,
        }
    }

    #[test]
    fn a_status_says_what_becomes_of_the_message() {
        for ok in [
            r#"{"ok":true}"#,
            r#"{"error":"said, not answered"}"#,
            r#"{"__status":200,"__body":{"error":"x"}}"#,
            r#"{"__status":204}"#,
            r#"{"__v":2,"__status":201,"__body":{}}"#,
            "plain text",
            "",
        ] {
            assert_eq!(answered(ok), Outcome::Ok, "{ok}");
        }
        for (failed, status) in [
            (r#"{"__status":500,"__body":{"error":"x"}}"#, 500),
            (r#"{"__v":2,"__status":503,"__body":{"error":"x"}}"#, 503),
        ] {
            assert_eq!(
                answered(failed),
                Outcome::Retry {
                    after: None,
                    reason: format!("handler answered {status}"),
                }
            );
        }
        for (rejected, status) in [
            (r#"{"__status":404,"__body":{"error":"gone"}}"#, 404),
            (r#"{"__v":2,"__status":422,"__body":{"error":"x"}}"#, 422),
        ] {
            assert_eq!(
                answered(rejected),
                Outcome::Terminal {
                    reason: format!("handler rejected the message with {status}"),
                }
            );
        }
    }

    #[test]
    fn what_the_handler_asks_for_outweighs_its_status() {
        assert_eq!(
            retried(answered(
                r#"{"__v":2,"__status":503,"__body":{},"__delivery":{"retry_after_ms":1500}}"#
            )),
            Some(Some(Duration::from_millis(1500)))
        );
        assert_eq!(
            retried(answered(
                r#"{"__v":2,"__status":200,"__body":{},"__delivery":{"retry_after_ms":0}}"#
            )),
            Some(Some(Duration::ZERO))
        );
        for (asked_ms, waits) in [
            (3_600_000, RETRY_AFTER_MAX),
            (3_600_001, RETRY_AFTER_MAX),
            (u64::MAX, RETRY_AFTER_MAX),
            (3_599_999, Duration::from_millis(3_599_999)),
        ] {
            assert_eq!(
                retried(answered(&format!(
                    r#"{{"__v":2,"__status":503,"__body":{{}},"__delivery":{{"retry_after_ms":{asked_ms}}}}}"#
                ))),
                Some(Some(waits)),
                "{asked_ms}"
            );
        }
        assert!(
            i64::try_from(RETRY_AFTER_MAX.as_nanos()).is_ok(),
            "a wait the server can count"
        );
        assert_eq!(
            answered(
                r#"{"__v":2,"__status":500,"__body":{},"__delivery":{"terminal":"unknown book"}}"#
            ),
            Outcome::Terminal {
                reason: "unknown book".into()
            }
        );
    }

    #[test]
    fn an_answer_that_cannot_be_read_is_delivered_again() {
        for unreadable in [
            r#"{"__v":2,"__status":99}"#,
            r#"{"__v":2,"__status":200,"__delivery":{"retry_after_ms":"soon"}}"#,
            r#"{"__v":2,"__status":200,"__delivery":{"terminal":"a","retry_after_ms":1}}"#,
        ] {
            assert_eq!(retried(answered(unreadable)), Some(None), "{unreadable}");
        }
        assert_eq!(
            answered(r#"{"__status":500,"__delivery":{"terminal":"x"}}"#),
            Outcome::Retry {
                after: None,
                reason: "handler answered 500".into()
            },
            "only a v2 answer settles its message"
        );
    }

    #[test]
    fn under_always_an_answer_is_a_success() {
        let failed = || HandlerOutcome::Rpc(Ok(r#"{"__status":500}"#.to_string()));
        assert_eq!(failed().outcome(AckMode::Always), Outcome::Ok);
        assert!(retried(failed().outcome(AckMode::OnSuccess)).is_some());
        let built_in = || HandlerOutcome::SideEffect(Err(eyre::eyre!("no such table")));
        for ack in [AckMode::Always, AckMode::OnSuccess] {
            assert_eq!(retried(built_in().outcome(ack)), Some(None));
            assert_eq!(HandlerOutcome::SideEffect(Ok(())).outcome(ack), Outcome::Ok);
        }
    }

    fn sub(ack: AckMode, consumer: Option<ConsumerSpec>) -> SubscriptionDefinition {
        SubscriptionDefinition {
            ack,
            consumer,
            ..SubscriptionDefinition::new("x", "h", HandlerType::Wasm)
        }
    }

    #[test]
    fn a_subscription_that_declares_nothing_runs_as_it_always_did() {
        let spec = SubscriptionSpec::of(&sub(AckMode::Always, None));
        assert_eq!(
            (spec.ack, spec.timeout, spec.concurrency, spec.dead_letter),
            (AckMode::Always, Duration::from_secs(60), 1, false)
        );
        assert_eq!(
            spec.pump(Duration::from_secs(30)),
            PumpConfig::sequential(Duration::from_secs(30))
        );
        assert_eq!(spec.handler_type, HandlerType::Wasm);
    }

    #[test]
    fn what_is_declared_is_how_the_loop_runs() {
        let spec = SubscriptionSpec::of(&SubscriptionDefinition {
            timeout_secs: Some(120),
            ..sub(
                AckMode::OnSuccess,
                Some(ConsumerSpec {
                    concurrency: Some(4),
                    batch: Some(16),
                    backoff_secs: vec![10, 60],
                    ..Default::default()
                }),
            )
        });
        assert!(spec.dead_letter, "on unless declared off");
        assert_eq!(spec.timeout, Duration::from_secs(120));
        let pump = spec.pump(Duration::from_secs(30));
        assert_eq!((pump.concurrency, pump.batch), (4, 16));
        assert_eq!(
            spec.backoff(true, Duration::from_secs(30)),
            Backoff::explicit(vec![Duration::from_secs(10), Duration::from_secs(60)])
        );
        let off = SubscriptionSpec::of(&sub(
            AckMode::OnSuccess,
            Some(ConsumerSpec {
                dead_letter: Some(false),
                ..Default::default()
            }),
        ));
        assert!(!off.dead_letter);
        for handler_type in [
            HandlerType::DeleteCascade,
            HandlerType::UpdateField,
            HandlerType::Webhook,
        ] {
            let spec =
                SubscriptionSpec::of(&SubscriptionDefinition::new("x", "h", handler_type.clone()));
            assert_eq!(spec.handler_type, handler_type);
        }
    }

    #[test]
    fn a_bound_durable_retries_after_its_ack_wait_and_a_managed_one_backs_off() {
        let spec = SubscriptionSpec::of(&sub(AckMode::OnSuccess, None));
        assert_eq!(
            spec.backoff(true, Duration::from_secs(30)),
            Backoff::explicit(vec![Duration::from_secs(30)])
        );
        assert_eq!(
            spec.backoff(false, Duration::from_secs(30)),
            Backoff::exponential(Duration::from_secs(5), Duration::from_secs(300))
        );
    }
}
