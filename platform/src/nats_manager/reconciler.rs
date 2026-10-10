use super::consumers::apply;
use super::consumers::desired::{self, Declared, Wanted};
use super::consumers::plan::DesiredConsumer;
use super::consumers::LoopKey;
use super::subscriptions::SubscriptionSpec;
use crate::nats_conn::{needs_replica_upgrade, stream_replicas};
use crate::registry::{RegisteredService, ServiceIdentity};
use crate::repo::deploy_lock::DeployLock;
use crate::schedules::work::ScheduleRun;
use crate::AppState;
use async_nats::jetstream::consumer;
use futures_util::TryStreamExt;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{error, info, warn};

pub const SETTLE_WITHIN: Duration = Duration::from_secs(10);
const RETRY_INITIAL: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(60);
const FAR: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkSpec {
    Subscription(SubscriptionSpec),
    Schedule(ScheduleRun),
}

impl WorkSpec {
    fn of(declared: Declared<'_>) -> Self {
        match declared {
            Declared::Subscription(sub) => WorkSpec::Subscription(SubscriptionSpec::of(sub)),
            Declared::Schedule(schedule) => WorkSpec::Schedule(ScheduleRun::of(schedule)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Served {
    pub ack_wait: Duration,
    pub max_deliver: i64,
}

impl Served {
    pub fn of(config: &consumer::Config) -> Self {
        Self {
            ack_wait: config.ack_wait,
            max_deliver: config.max_deliver,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DesiredLoop {
    pub identity: ServiceIdentity,
    pub version: i32,
    pub consumer: DesiredConsumer,
    pub work: WorkSpec,
}

impl DesiredLoop {
    pub fn key(&self) -> &LoopKey {
        &self.consumer.key
    }

    fn durable(&self) -> Durable {
        (self.consumer.stream.clone(), self.consumer.name.to_string())
    }

    fn service(&self) -> ServiceKey {
        service_key(&self.identity)
    }

    pub fn spec_hash(&self, served: Served) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        (
            &self.work,
            &self.consumer.stream,
            &self.consumer.name,
            &self.consumer.filters,
            served,
        )
            .hash(&mut hasher);
        hasher.finish()
    }
}

type Durable = (String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningLoop {
    pub key: LoopKey,
    pub spec_hash: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ReconcilePlan {
    pub start: Vec<DesiredLoop>,
    pub restart: Vec<DesiredLoop>,
    pub stop: Vec<LoopKey>,
    pub recreate: Vec<DesiredLoop>,
    pub missing_bound: Vec<DesiredLoop>,
    pub missing_stream: Vec<DesiredLoop>,
}

pub fn reconcile(
    desired: &[DesiredLoop],
    running: &[RunningLoop],
    present: &HashMap<Durable, Served>,
    streams: &HashSet<String>,
) -> ReconcilePlan {
    let runs = |wanted: &DesiredLoop| running.iter().find(|running| &running.key == wanted.key());
    let plan = desired
        .iter()
        .fold(ReconcilePlan::default(), |mut plan, wanted| {
            let has_stream = streams.contains(&wanted.consumer.stream);
            let served = present.get(&wanted.durable()).copied();
            let list = match (has_stream, served, runs(wanted)) {
                (false, _, _) => Some(&mut plan.missing_stream),
                (true, None, _) if wanted.consumer.name.is_bound() => Some(&mut plan.missing_bound),
                (true, None, _) => Some(&mut plan.recreate),
                (true, Some(_), None) => Some(&mut plan.start),
                (true, Some(served), Some(running))
                    if running.spec_hash != wanted.spec_hash(served) =>
                {
                    Some(&mut plan.restart)
                }
                (true, Some(_), Some(_)) => None,
            };
            if let Some(list) = list {
                list.push(wanted.clone());
            }
            plan
        });
    let unwanted = running
        .iter()
        .filter(|running| !desired.iter().any(|wanted| wanted.key() == &running.key))
        .map(|running| running.key.clone());
    let lost = plan
        .missing_bound
        .iter()
        .filter(|wanted| runs(wanted).is_some())
        .map(|wanted| wanted.key().clone());
    ReconcilePlan {
        stop: unwanted.chain(lost).collect(),
        ..plan
    }
}

fn retry_in(failures: u32) -> Duration {
    RETRY_INITIAL
        .checked_mul(
            1u32.checked_shl(failures.saturating_sub(1))
                .unwrap_or(u32::MAX),
        )
        .map_or(RETRY_MAX, |delay| delay.min(RETRY_MAX))
}

type ServiceKey = (String, String);

fn service_key(identity: &ServiceIdentity) -> ServiceKey {
    (identity.tenant_slug.clone(), identity.service_name.clone())
}

#[derive(Debug, Clone, Copy)]
struct Failing {
    failures: u32,
    retry_at: Instant,
}

#[derive(Default)]
struct Memory {
    failing: HashMap<ServiceKey, Failing>,
    missing: BTreeSet<(String, String, String)>,
    outside: HashSet<(ServiceKey, String)>,
}

impl Memory {
    fn waits(&self, service: &ServiceKey, now: Instant) -> bool {
        self.failing
            .get(service)
            .is_some_and(|failing| failing.retry_at > now)
    }

    fn next_retry(&self) -> Option<Instant> {
        self.failing.values().map(|failing| failing.retry_at).min()
    }

    fn still_fails(&mut self, service: &ServiceKey, reason: &str) {
        if !self.waits(service, Instant::now()) {
            self.failed(service, reason);
        }
    }

    fn failed(&mut self, service: &ServiceKey, reason: &str) {
        let failures = self
            .failing
            .get(service)
            .map_or(1, |failing| failing.failures.saturating_add(1));
        let delay = retry_in(failures);
        self.failing.insert(
            service.clone(),
            Failing {
                failures,
                retry_at: Instant::now() + delay,
            },
        );
        failures_gauge(service).set(f64::from(failures));
        warn!(
            tenant = %service.0,
            service = %service.1,
            failures,
            retry_in = ?delay,
            "The service's consumers could not be reconciled: {reason}"
        );
    }

    fn settle(&mut self, succeeded: &HashSet<ServiceKey>, registered: &HashSet<ServiceKey>) {
        self.failing.retain(|service, _| {
            let keep = registered.contains(service) && !succeeded.contains(service);
            if !keep {
                failures_gauge(service).set(0.0);
            }
            keep
        });
        self.outside
            .retain(|(service, _)| registered.contains(service));
    }

    fn report_missing(&mut self, missing: BTreeSet<(String, String, String)>) {
        for (tenant, service, durable) in missing.difference(&self.missing) {
            warn!(
                event = "consumer_missing",
                %tenant,
                %service,
                %durable,
                "A bound durable is gone from the server. It is not created again by itself, \
                 since it would read its stream from the start: deploy the service to create it"
            );
        }
        for found in self.missing.difference(&missing) {
            missing_gauge(found).set(0.0);
        }
        for lost in &missing {
            missing_gauge(lost).set(1.0);
        }
        self.missing = missing;
    }
}

fn failures_gauge(service: &ServiceKey) -> metrics::Gauge {
    metrics::gauge!(
        "cufflink_consumer_reconcile_failures",
        "tenant" => service.0.clone(),
        "service" => service.1.clone(),
    )
}

fn missing_gauge((tenant, service, durable): &(String, String, String)) -> metrics::Gauge {
    metrics::gauge!(
        "cufflink_consumer_missing",
        "tenant" => tenant.clone(),
        "service" => service.clone(),
        "durable" => durable.clone(),
    )
}

struct Inner {
    running: AtomicBool,
    requested: watch::Sender<u64>,
    completed: watch::Sender<u64>,
}

#[derive(Clone)]
pub struct ConsumerReconciler {
    inner: Arc<Inner>,
}

impl Default for ConsumerReconciler {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                running: AtomicBool::new(false),
                requested: watch::Sender::new(0),
                completed: watch::Sender::new(0),
            }),
        }
    }
}

impl ConsumerReconciler {
    pub fn trigger(&self) -> u64 {
        let mut request = 0;
        self.inner.requested.send_modify(|requested| {
            *requested += 1;
            request = *requested;
        });
        request
    }

    pub async fn settled(&self, within: Duration) {
        if !self.inner.running.load(Ordering::Acquire) {
            return;
        }
        let request = self.trigger();
        let mut completed = self.inner.completed.subscribe();
        let _ = tokio::time::timeout(within, completed.wait_for(|pass| *pass >= request)).await;
    }

    pub fn start(&self, state: Arc<AppState>) {
        if self.inner.running.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner = Arc::clone(&self.inner);
        state
            .shutdown
            .clone()
            .spawn("consumer_reconciler", run(state, inner));
    }
}

async fn run(state: Arc<AppState>, inner: Arc<Inner>) {
    let shutdown = state.shutdown.clone();
    let mut requested = inner.requested.subscribe();
    let mut registry = state.registry.subscribe_version();
    let mut every = tokio::time::interval(state.config.eventing.reconcile_interval);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut memory = Memory::default();
    let mut failed_passes: u32 = 0;
    loop {
        let request = *requested.borrow_and_update();
        registry.borrow_and_update();
        let ended = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            ended = pass(&state, &mut memory) => ended,
        };
        metrics::counter!(
            "cufflink_consumer_reconcile_runs_total",
            "result" => if ended.is_ok() { "ok" } else { "error" },
        )
        .increment(1);
        inner.completed.send_replace(request);
        failed_passes = match &ended {
            Ok(_) => 0,
            Err(e) => {
                warn!(error = %format!("{e:#}"), "A consumer reconcile pass failed; retrying");
                failed_passes.saturating_add(1)
            }
        };
        if matches!(ended, Ok(Next::Again)) {
            continue;
        }
        let retry = (failed_passes > 0)
            .then(|| Instant::now() + retry_in(failed_passes))
            .into_iter()
            .chain(memory.next_retry())
            .min()
            .map_or(FAR, |at| at.saturating_duration_since(Instant::now()));
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = requested.changed() => {}
            _ = registry.changed() => {}
            _ = every.tick() => {}
            _ = tokio::time::sleep(retry) => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    Wait,
    Again,
}

struct Wants {
    service: Arc<RegisteredService>,
    loops: Result<Vec<DesiredLoop>, String>,
}

fn loops_of(service: &RegisteredService, wanted: Vec<Wanted<'_>>) -> Vec<DesiredLoop> {
    wanted
        .into_iter()
        .map(|wanted| DesiredLoop {
            identity: service.identity(),
            version: service.version,
            work: WorkSpec::of(wanted.declared),
            consumer: wanted.consumer,
        })
        .collect()
}

async fn wants_of(
    state: &AppState,
    memory: &mut Memory,
    service: Arc<RegisteredService>,
    streams: &super::stream_index::StreamSnapshot,
) -> Wants {
    let identity = service.identity();
    let loops = match desired::namespace_of(state, &identity).await {
        Err(e) => Err(e.to_string()),
        Ok(namespace) => {
            report_outside(memory, &service, &namespace);
            desired::desired_consumers(
                &identity,
                &service.manifest,
                desired::in_scope(&service.manifest, &namespace),
                streams,
                desired::defaults(state),
            )
            .map(|wanted| loops_of(&service, wanted))
            .map_err(|e| e.to_string())
        }
    };
    Wants { service, loops }
}

struct Observed {
    durables: Vec<consumer::Info>,
    replicas: usize,
}

async fn observe(state: &AppState, stream: &str) -> eyre::Result<Observed> {
    let stream = state.jetstream.get_stream(stream).await?;
    Ok(Observed {
        replicas: stream.cached_info().config.num_replicas,
        durables: stream.consumers().try_collect().await?,
    })
}

async fn observe_each(
    state: &AppState,
    streams: BTreeSet<&str>,
) -> (Vec<Observed>, BTreeMap<String, String>) {
    let mut observed = Vec::with_capacity(streams.len());
    let mut unreadable = BTreeMap::new();
    for stream in streams {
        match observe(state, stream).await {
            Ok(found) => {
                metrics::gauge!("cufflink_stream_replicas_below_target", "stream" => stream.to_string())
                    .set(f64::from(u8::from(needs_replica_upgrade(
                        found.replicas,
                        stream_replicas(),
                    ))));
                observed.push(found);
            }
            Err(e) => {
                metrics::counter!("cufflink_consumer_stream_unreadable_total", "stream" => stream.to_string())
                    .increment(1);
                unreadable.insert(stream.to_string(), format!("{e:#}"));
            }
        }
    }
    (observed, unreadable)
}

fn blinded(
    desired: &[DesiredLoop],
    unreadable: &BTreeMap<String, String>,
) -> BTreeMap<ServiceKey, String> {
    desired
        .iter()
        .filter_map(|wanted| {
            let stream = &wanted.consumer.stream;
            unreadable.get(stream).map(|reason| {
                (
                    wanted.service(),
                    format!("stream '{stream}' cannot be read: {reason}"),
                )
            })
        })
        .collect()
}

fn record_lag(wanted: &[DesiredLoop], durables: &[consumer::Info]) {
    for wanted in wanted {
        let Some(info) = durables
            .iter()
            .find(|info| (info.stream_name.clone(), info.name.clone()) == wanted.durable())
        else {
            continue;
        };
        let gauge = |name: &'static str| {
            metrics::gauge!(
                name,
                "tenant" => wanted.identity.tenant_slug.clone(),
                "service" => wanted.identity.service_name.clone(),
                "handler" => wanted.key().handler.clone(),
                "durable" => info.name.clone(),
            )
        };
        gauge("cufflink_consumer_pending").set(info.num_pending as f64);
        gauge("cufflink_consumer_ack_pending").set(info.num_ack_pending as f64);
    }
}

async fn pass(state: &Arc<AppState>, memory: &mut Memory) -> eyre::Result<Next> {
    let tasks = &state.subscription_tasks;
    let services: Vec<Arc<RegisteredService>> = state
        .registry
        .all_services()
        .await
        .into_iter()
        .filter(|service| service.manifest.mode.runs_on_platform())
        .collect();
    let registered: HashSet<ServiceKey> = services
        .iter()
        .map(|service| service_key(&service.identity()))
        .collect();
    let consumes = services
        .iter()
        .any(|service| desired::consumes(&service.manifest));
    if !consumes && tasks.loop_count() == 0 {
        memory.settle(&registered, &registered);
        memory.report_missing(BTreeSet::new());
        return Ok(Next::Wait);
    }

    let server = state.stream_index.refresh(&state.jetstream).await?;
    let streams = desired::with_planned(
        &server,
        services
            .iter()
            .flat_map(|service| desired::deployed_streams(&service.manifest, &service.tenant_slug)),
    );
    let mut wants = Vec::with_capacity(services.len());
    for service in services {
        wants.push(wants_of(state, memory, service, &streams).await);
    }
    let placed: Vec<DesiredLoop> = wants
        .iter()
        .filter_map(|wanted| wanted.loops.as_ref().ok())
        .flatten()
        .cloned()
        .collect();

    let (observed, unreadable) = observe_each(
        state,
        placed
            .iter()
            .map(|wanted| wanted.consumer.stream.as_str())
            .filter(|stream| server.get(stream).is_some())
            .collect(),
    )
    .await;
    let unplaced = wants.iter().filter_map(|wanted| {
        let reason = wanted.loops.as_ref().err()?;
        Some((service_key(&wanted.service.identity()), reason.clone()))
    });
    let failing: BTreeMap<ServiceKey, String> =
        unplaced.chain(blinded(&placed, &unreadable)).collect();
    for (service, reason) in &failing {
        memory.still_fails(service, reason);
    }
    let mut failed: HashSet<ServiceKey> = failing.into_keys().collect();

    let desired: Vec<DesiredLoop> = placed
        .into_iter()
        .filter(|wanted| !failed.contains(&wanted.service()))
        .collect();
    let running: Vec<RunningLoop> = tasks
        .running()
        .into_iter()
        .filter(|running| {
            !failed.contains(&(
                running.key.tenant_slug.clone(),
                running.key.service_name.clone(),
            ))
        })
        .collect();
    for found in &observed {
        record_lag(&desired, &found.durables);
    }
    let present: HashMap<Durable, Served> = observed
        .into_iter()
        .flat_map(|found| found.durables)
        .map(|info| ((info.stream_name, info.name), Served::of(&info.config)))
        .collect();
    let plan = reconcile(
        &desired,
        &running,
        &present,
        &server.iter().map(|stream| stream.name.clone()).collect(),
    );

    tasks.stop(&plan.stop);
    memory.report_missing(
        plan.missing_bound
            .iter()
            .map(|wanted| {
                (
                    wanted.identity.tenant_slug.clone(),
                    wanted.identity.service_name.clone(),
                    wanted.consumer.name.to_string(),
                )
            })
            .collect(),
    );

    let now = Instant::now();
    let mut next = Next::Wait;
    let mut by_service: HashMap<ServiceKey, Vec<(Action, DesiredLoop)>> = HashMap::new();
    for (action, loops) in [
        (Action::Start, plan.start),
        (Action::Start, plan.restart),
        (Action::Recreate, plan.recreate),
        (Action::RestoreStream, plan.missing_stream),
    ] {
        for wanted in loops {
            by_service
                .entry(wanted.service())
                .or_default()
                .push((action, wanted));
        }
    }
    for (service, actions) in by_service {
        if memory.waits(&service, now) {
            failed.insert(service);
            continue;
        }
        match act(state, &actions).await {
            Ok(Next::Again) => next = Next::Again,
            Ok(Next::Wait) => {}
            Err(e) => {
                memory.failed(&service, &format!("{e:#}"));
                failed.insert(service);
            }
        }
    }
    let succeeded = registered.difference(&failed).cloned().collect();
    memory.settle(&succeeded, &registered);
    Ok(next)
}

fn report_outside(
    memory: &mut Memory,
    service: &RegisteredService,
    namespace: &crate::nats_manager::subjects::Namespace,
) {
    let key = service_key(&service.identity());
    let outside: Vec<(String, String)> = service
        .manifest
        .subscriptions
        .iter()
        .filter_map(|sub| {
            crate::deployer::validator::subscription_scope_violation(sub, namespace)
                .map(|reason| (sub.handler.clone(), reason))
        })
        .collect();
    memory.outside.retain(|(of, handler)| {
        of != &key || outside.iter().any(|(outside, _)| outside == handler)
    });
    for (handler, reason) in outside {
        if memory.outside.insert((key.clone(), handler)) {
            error!(
                tenant = %key.0,
                service = %key.1,
                "Not starting a subscription outside its tenant: {reason}"
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Start,
    Recreate,
    RestoreStream,
}

fn as_declared_last<'a>(
    actions: &'a [(Action, DesiredLoop)],
    wanted: &'a DesiredLoop,
) -> &'a DesiredConsumer {
    actions
        .iter()
        .rev()
        .map(|(_, other)| other)
        .find(|other| other.durable() == wanted.durable())
        .map_or(&wanted.consumer, |other| &other.consumer)
}

async fn act(state: &Arc<AppState>, actions: &[(Action, DesiredLoop)]) -> eyre::Result<Next> {
    let Some((_, first)) = actions.first() else {
        return Ok(Next::Wait);
    };
    let identity = &first.identity;
    let restore: Vec<DesiredConsumer> = actions
        .iter()
        .filter(|(action, _)| *action == Action::RestoreStream)
        .map(|(_, wanted)| wanted.consumer.clone())
        .collect();
    if !restore.is_empty() {
        let Some(service) = state
            .registry
            .get_by_slug(&identity.tenant_slug, &identity.service_name)
            .await
        else {
            return Ok(Next::Again);
        };
        super::streams::ensure_for_service(state, &service, &restore).await?;
    }

    let _setup = state
        .subscription_tasks
        .lock_service_setup(&identity.tenant_slug, &identity.service_name)
        .await;
    let current = state
        .registry
        .get_by_slug(&identity.tenant_slug, &identity.service_name)
        .await
        .is_some_and(|service| service.version == first.version);
    if !current {
        return Ok(Next::Again);
    }
    for (action, wanted) in actions {
        match action {
            Action::Start => super::subscriptions::start(state, wanted).await?,
            Action::Recreate => recreate(state, wanted, as_declared_last(actions, wanted)).await?,
            Action::RestoreStream => {}
        }
    }
    Ok(if restore.is_empty() {
        Next::Wait
    } else {
        Next::Again
    })
}

async fn recreate(
    state: &Arc<AppState>,
    wanted: &DesiredLoop,
    durable: &DesiredConsumer,
) -> eyre::Result<()> {
    let identity = &wanted.identity;
    let Some(lock) = DeployLock::try_acquire(&state.platform_db, identity.service_id).await? else {
        return Err(eyre::eyre!(
            "durable '{}' is gone and the service is being deployed; the deploy decides",
            durable.name
        ));
    };
    let restored = restore_if_active(state, identity, durable).await;
    lock.release().await;
    if restored? {
        super::subscriptions::start(state, wanted).await?;
    }
    Ok(())
}

async fn restore_if_active(
    state: &Arc<AppState>,
    identity: &ServiceIdentity,
    durable: &DesiredConsumer,
) -> eyre::Result<bool> {
    let names = desired::wanted_by_active(state, identity, &state.stream_index.snapshot())
        .await?
        .contains(&(durable.stream.clone(), durable.name.to_string()));
    if !names {
        info!(
            tenant = %identity.tenant_slug,
            service = %identity.service_name,
            durable = %durable.name,
            "The active version no longer consumes a durable this replica still ran; not creating it"
        );
        return Ok(false);
    }
    let stream = state.jetstream.get_stream(&durable.stream).await?;
    if apply::restore(&stream, durable).await? {
        info!(
            tenant = %identity.tenant_slug,
            service = %identity.service_name,
            durable = %durable.name,
            stream = %durable.stream,
            "Created a durable the server had lost"
        );
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_manager::consumers::naming::DurableName;
    use crate::nats_manager::consumers::plan::{ConsumerKind, CreatedWith};
    use async_nats::jetstream::stream::RetentionPolicy;
    use cufflink_types::{
        AckMode, ConsumerSpec, DeliverPolicySpec, HandlerType, SubscriptionDefinition,
    };
    use std::collections::BTreeMap;
    use uuid::Uuid;

    const STREAM: &str = "DW_DEFAULT";

    fn subscription(handler: &str) -> SubscriptionDefinition {
        SubscriptionDefinition::new("dw.{tenant}.stocks.created", handler, HandlerType::Wasm)
    }

    fn wanted_as(name: DurableName, sub: &SubscriptionDefinition, version: i32) -> DesiredLoop {
        DesiredLoop {
            identity: ServiceIdentity {
                tenant_id: Uuid::nil(),
                tenant_slug: "default".into(),
                service_id: Uuid::nil(),
                service_name: "stock-service".into(),
                schema: "stock_service".into(),
            },
            version,
            consumer: DesiredConsumer {
                key: LoopKey {
                    tenant_slug: "default".into(),
                    service_name: "stock-service".into(),
                    handler: sub.handler.clone(),
                    subject: "dw.default.stocks.created".into(),
                },
                stream: STREAM.into(),
                retention: RetentionPolicy::Interest,
                name,
                filters: ["dw.default.stocks.created".to_string()].into(),
                ack_wait: None,
                max_deliver: None,
                max_ack_pending: None,
                created_with: CreatedWith {
                    ack_wait: Duration::from_secs(30),
                    max_deliver: -1,
                },
                deliver_policy: DeliverPolicySpec::All,
                kind: ConsumerKind::Subscription,
                metadata: BTreeMap::new(),
            },
            work: WorkSpec::Subscription(SubscriptionSpec::of(sub)),
        }
    }

    fn wanted(handler: &str) -> DesiredLoop {
        wanted_as(
            DurableName::Legacy(format!("stock_service_{handler}")),
            &subscription(handler),
            1,
        )
    }

    fn bound(handler: &str) -> DesiredLoop {
        wanted_as(
            DurableName::Named(format!("bound-{handler}")),
            &subscription(handler),
            1,
        )
    }

    const SERVED: Served = Served {
        ack_wait: Duration::from_secs(30),
        max_deliver: -1,
    };

    fn runs(wanted: &DesiredLoop) -> RunningLoop {
        RunningLoop {
            key: wanted.key().clone(),
            spec_hash: wanted.spec_hash(SERVED),
        }
    }

    fn has(loops: &[&DesiredLoop]) -> HashMap<Durable, Served> {
        loops
            .iter()
            .map(|wanted| (wanted.durable(), SERVED))
            .collect()
    }

    fn streams() -> HashSet<String> {
        HashSet::from([STREAM.to_string()])
    }

    fn handlers(loops: &[DesiredLoop]) -> Vec<&str> {
        loops
            .iter()
            .map(|wanted| wanted.key().handler.as_str())
            .collect()
    }

    #[test]
    fn a_schedules_loop_is_restarted_when_what_it_does_changes() {
        use cufflink_types::{Overlap, ScheduleDefinition, ScheduleSpec};
        let schedule = ScheduleDefinition::new(
            "order_retry",
            "retry_orders",
            ScheduleSpec::Every { secs: 30 },
        );
        let looped = |schedule: &ScheduleDefinition| DesiredLoop {
            work: WorkSpec::of(Declared::Schedule(schedule)),
            ..wanted("retry_orders")
        };
        let running = [runs(&looped(&schedule))];
        let plan = |schedule: &ScheduleDefinition| {
            let wanted = looped(schedule);
            reconcile(
                std::slice::from_ref(&wanted),
                &running,
                &has(&[&wanted]),
                &streams(),
            )
        };
        let rescheduled = plan(&ScheduleDefinition {
            spec: ScheduleSpec::Every { secs: 60 },
            ..schedule.clone()
        });
        assert!(
            rescheduled.restart.is_empty() && rescheduled.start.is_empty(),
            "when it fires is the scheduler's"
        );
        let overlapping = plan(&ScheduleDefinition {
            overlap: Overlap::Allow,
            ..schedule.clone()
        });
        assert_eq!(handlers(&overlapping.restart), ["retry_orders"]);
        assert_ne!(
            looped(&schedule).spec_hash(SERVED),
            wanted("retry_orders").spec_hash(SERVED),
            "a schedule is not a subscription of the same handler"
        );
    }

    #[test]
    fn each_loop_is_brought_to_what_the_registry_wants() {
        let (kept, new, changed, lost, lost_bound, stranded) = (
            wanted("kept"),
            wanted("new"),
            wanted("changed"),
            wanted("lost"),
            bound("lost_bound"),
            DesiredLoop {
                consumer: DesiredConsumer {
                    stream: "BOOKS".into(),
                    ..wanted("stranded").consumer
                },
                ..wanted("stranded")
            },
        );
        let as_it_ran = RunningLoop {
            spec_hash: changed.spec_hash(SERVED) ^ 1,
            ..runs(&changed)
        };
        let gone = runs(&wanted("undeployed"));
        let plan = reconcile(
            &[
                kept.clone(),
                new.clone(),
                changed.clone(),
                lost.clone(),
                lost_bound.clone(),
                stranded.clone(),
            ],
            &[runs(&kept), as_it_ran, gone.clone(), runs(&lost_bound)],
            &has(&[&kept, &new, &changed]),
            &streams(),
        );
        assert_eq!(handlers(&plan.start), ["new"]);
        assert_eq!(handlers(&plan.restart), ["changed"]);
        assert_eq!(handlers(&plan.recreate), ["lost"]);
        assert_eq!(handlers(&plan.missing_bound), ["lost_bound"]);
        assert_eq!(handlers(&plan.missing_stream), ["stranded"]);
        assert_eq!(
            plan.stop,
            [gone.key, lost_bound.key().clone()],
            "what nothing wants, and what consumes a bound durable that is gone"
        );
    }

    #[test]
    fn nothing_registered_stops_everything() {
        let running = [runs(&wanted("a")), runs(&wanted("b"))];
        let plan = reconcile(&[], &running, &HashMap::new(), &streams());
        assert_eq!(plan.stop.len(), 2);
        assert!(plan.start.is_empty() && plan.recreate.is_empty());
    }

    #[test]
    fn a_new_version_alone_restarts_nothing() {
        let (v1, v2) = (
            wanted("on_created"),
            DesiredLoop {
                version: 2,
                ..wanted("on_created")
            },
        );
        assert_eq!(v1.spec_hash(SERVED), v2.spec_hash(SERVED));
        let plan = reconcile(&[v2], &[runs(&v1)], &has(&[&v1]), &streams());
        assert!(plan.restart.is_empty() && plan.start.is_empty() && plan.stop.is_empty());
    }

    #[test]
    fn what_changes_how_a_loop_works_restarts_it() {
        let plain = wanted("on_created");
        let declared = |edit: fn(&mut SubscriptionDefinition)| {
            let mut sub = subscription("on_created");
            edit(&mut sub);
            wanted_as(plain.consumer.name.clone(), &sub, 1)
        };
        let changed = [
            declared(|sub| sub.ack = AckMode::OnSuccess),
            declared(|sub| sub.timeout_secs = Some(120)),
            declared(|sub| sub.handler_type = HandlerType::Webhook),
            declared(|sub| {
                sub.config
                    .insert("url".into(), "https://example.com".into());
            }),
            declared(|sub| {
                sub.consumer = Some(ConsumerSpec {
                    concurrency: Some(4),
                    ..Default::default()
                })
            }),
            declared(|sub| {
                sub.consumer = Some(ConsumerSpec {
                    backoff_secs: vec![1, 2],
                    ..Default::default()
                })
            }),
            declared(|sub| {
                sub.consumer = Some(ConsumerSpec {
                    dead_letter: Some(false),
                    ..Default::default()
                });
                sub.ack = AckMode::OnSuccess;
            }),
            declared(|sub| {
                sub.consumer = Some(ConsumerSpec {
                    batch: Some(8),
                    ..Default::default()
                })
            }),
            wanted_as(
                DurableName::Scoped("default_stock-service_on_created_0badcafe".into()),
                &subscription("on_created"),
                1,
            ),
            wanted_as(
                DurableName::Named("stock_service_on_created".into()),
                &subscription("on_created"),
                1,
            ),
            DesiredLoop {
                consumer: DesiredConsumer {
                    filters: ["dw.default.stocks.created", "dw.default.stocks.updated"]
                        .map(String::from)
                        .into(),
                    ..plain.consumer.clone()
                },
                ..plain.clone()
            },
            DesiredLoop {
                consumer: DesiredConsumer {
                    stream: "ASSETS".into(),
                    ..plain.consumer.clone()
                },
                ..plain.clone()
            },
        ];
        let hashes: HashSet<u64> = changed
            .iter()
            .chain([&plain])
            .map(|wanted| wanted.spec_hash(SERVED))
            .collect();
        assert_eq!(hashes.len(), changed.len() + 1);
        assert_eq!(
            plain.spec_hash(SERVED),
            wanted("on_created").spec_hash(SERVED)
        );
    }

    #[test]
    fn a_durable_served_otherwise_restarts_its_loop() {
        let wanted = wanted("on_created");
        let served = [
            SERVED,
            Served {
                max_deliver: 3,
                ..SERVED
            },
            Served {
                max_deliver: 5,
                ..SERVED
            },
            Served {
                ack_wait: Duration::from_secs(6),
                ..SERVED
            },
        ];
        let hashes: HashSet<u64> = served
            .iter()
            .map(|served| wanted.spec_hash(*served))
            .collect();
        assert_eq!(hashes.len(), served.len());

        let lowered = Served {
            max_deliver: 3,
            ack_wait: Duration::from_secs(6),
        };
        let plan = |served: Served| {
            reconcile(
                std::slice::from_ref(&wanted),
                &[runs(&wanted)],
                &HashMap::from([(wanted.durable(), served)]),
                &streams(),
            )
        };
        assert_eq!(handlers(&plan(lowered).restart), ["on_created"]);
        assert!(plan(lowered).start.is_empty() && plan(lowered).stop.is_empty());
        assert!(plan(SERVED).restart.is_empty());
        assert_eq!(
            Served::of(&consumer::Config {
                ack_wait: Duration::from_secs(6),
                max_deliver: 3,
                max_ack_pending: 1000,
                ..Default::default()
            }),
            lowered
        );
    }

    #[test]
    fn a_stream_that_cannot_be_read_fails_the_services_that_consume_it_alone() {
        let on = |tenant: &str, service: &str, stream: &str| {
            let wanted = wanted("on_created");
            DesiredLoop {
                identity: ServiceIdentity {
                    tenant_slug: tenant.into(),
                    service_name: service.into(),
                    ..wanted.identity
                },
                consumer: DesiredConsumer {
                    stream: stream.into(),
                    ..wanted.consumer
                },
                ..wanted
            }
        };
        let desired = [
            on("harbor", "loan-service", "BOOKS"),
            on("harbor", "loan-service", "DW_HARBOR"),
            on("harbor", "receipt-service", "RECEIPTS"),
            on("default", "stock-service", STREAM),
        ];
        let unreadable =
            BTreeMap::from([("BOOKS".to_string(), "the stream has no leader".to_string())]);
        assert_eq!(
            blinded(&desired, &unreadable),
            BTreeMap::from([(
                ("harbor".to_string(), "loan-service".to_string()),
                "stream 'BOOKS' cannot be read: the stream has no leader".to_string()
            )])
        );
        assert!(blinded(&desired, &BTreeMap::new()).is_empty());
    }

    #[test]
    fn a_durable_loops_share_is_restored_as_the_last_declares_it() {
        let (first, second) = (
            wanted("on_asset"),
            DesiredLoop {
                consumer: DesiredConsumer {
                    key: LoopKey {
                        subject: "dw.default.stocks.updated".into(),
                        ..wanted("on_asset").consumer.key
                    },
                    filters: ["dw.default.stocks.updated".to_string()].into(),
                    ..wanted("on_asset").consumer
                },
                ..wanted("on_asset")
            },
        );
        let alone = wanted("on_deleted");
        let actions = [
            (Action::Recreate, first.clone()),
            (Action::Recreate, second.clone()),
            (Action::Recreate, alone.clone()),
        ];
        for shares in [&first, &second] {
            assert_eq!(as_declared_last(&actions, shares), &second.consumer);
        }
        assert_eq!(as_declared_last(&actions, &alone), &alone.consumer);
        assert_eq!(as_declared_last(&[], &first), &first.consumer);
    }

    #[test]
    fn a_failing_service_is_retried_ever_more_slowly_and_never_given_up() {
        let delays: Vec<u64> = [1, 2, 3, 4, 5, 6, 7, 8, 40, u32::MAX]
            .into_iter()
            .map(|failures| retry_in(failures).as_secs())
            .collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60, 60, 60]);
        assert_eq!(retry_in(0), Duration::from_secs(1));
    }

    #[test]
    fn a_service_waits_out_its_delay_and_is_forgiven_when_it_succeeds() {
        let mut memory = Memory::default();
        let (failing, gone) = (
            ("default".to_string(), "stock-service".to_string()),
            ("default".to_string(), "deleted-service".to_string()),
        );
        memory.failed(&failing, "stream not found");
        memory.failed(&failing, "stream not found");
        memory.failed(&gone, "stream not found");
        assert!(memory.waits(&failing, Instant::now()));
        assert!(!memory.waits(&failing, Instant::now() + Duration::from_secs(3)));
        assert_eq!(memory.failing[&failing].failures, 2);
        assert!(memory.next_retry().is_some());

        let registered = HashSet::from([failing.clone()]);
        memory.settle(&HashSet::new(), &registered);
        assert!(memory.failing.contains_key(&failing));
        assert!(!memory.failing.contains_key(&gone), "no longer registered");
        memory.settle(&registered, &registered);
        assert!(memory.failing.is_empty());
        assert_eq!(memory.next_retry(), None);
    }

    #[test]
    fn a_failure_every_pass_finds_is_counted_once_for_each_wait() {
        let mut memory = Memory::default();
        let service = ("default".to_string(), "stock-service".to_string());
        memory.still_fails(&service, "stream 'BOOKS' cannot be read");
        memory.still_fails(&service, "stream 'BOOKS' cannot be read");
        assert_eq!(memory.failing[&service].failures, 1);
        memory
            .failing
            .entry(service.clone())
            .and_modify(|failing| failing.retry_at = Instant::now());
        memory.still_fails(&service, "stream 'BOOKS' cannot be read");
        assert_eq!(memory.failing[&service].failures, 2);
    }

    #[test]
    fn what_was_reported_of_a_service_goes_with_it() {
        let mut memory = Memory::default();
        let (kept, gone) = (
            ("default".to_string(), "stock-service".to_string()),
            ("default".to_string(), "deleted-service".to_string()),
        );
        memory.outside.insert((kept.clone(), "theirs".to_string()));
        memory.outside.insert((gone, "theirs".to_string()));
        let registered = HashSet::from([kept.clone()]);
        memory.settle(&registered, &registered);
        assert_eq!(
            memory.outside,
            HashSet::from([(kept, "theirs".to_string())])
        );
    }

    #[tokio::test]
    async fn passes_asked_for_together_are_one_pass() {
        let reconciler = ConsumerReconciler::default();
        reconciler.settled(Duration::from_secs(5)).await;
        assert_eq!(
            *reconciler.inner.requested.borrow(),
            0,
            "where no reconciler runs there is no pass to wait for"
        );
        assert_eq!((reconciler.trigger(), reconciler.trigger()), (1, 2));
        let mut requested = reconciler.inner.requested.subscribe();
        assert_eq!(*requested.borrow_and_update(), 2);
        assert!(!requested.has_changed().expect("open"));
    }
}
