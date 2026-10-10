use crate::deployer::manifest::ServiceManifest;
use crate::deployer::validator::validate_tenant_scope;
use crate::errors::{PlatformError, PlatformResult};
use crate::nats_manager::consumers::apply::{self, ApplyError, ApplyPhase, Handover};
use crate::nats_manager::consumers::desired;
use crate::nats_manager::consumers::plan::{
    plan_consumers, ConsumerOp, ConsumerPlan, DesiredConsumer, ObservedConsumer,
};
use crate::nats_manager::reconciler::SETTLE_WITHIN;
use crate::nats_manager::streams::{self, StreamOp, StreamPlan};
use crate::nats_manager::subjects::{tenant_stream_name, Namespace};
use crate::registry::ServiceIdentity;
use crate::repo::deploy_lock::DeployLock;
use crate::AppState;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

const MULTI_FILTER_SERVER: (u64, u64) = (2, 10);

#[derive(Debug, Clone, Default)]
pub(crate) struct EventingPlan {
    version: i32,
    streams: Vec<StreamPlan>,
    declared: Vec<(String, String)>,
    consumers: ConsumerPlan,
    tenant_subjects: Option<Vec<String>>,
    schedules: Option<Vec<String>>,
    handovers: Arc<OnceLock<Vec<Handover>>>,
    skipped: bool,
}

impl EventingPlan {
    pub(crate) fn skipped() -> Self {
        Self {
            skipped: true,
            ..Self::default()
        }
    }

    fn carried_out(&self) -> PlatformResult<()> {
        match self.handovers.get() {
            Some(_) => Ok(()),
            None => Err(PlatformError::Internal(format!(
                "the eventing plan of version {} was made and not carried out; nothing follows it",
                self.version
            ))),
        }
    }

    fn changes_nothing(&self) -> bool {
        self.consumers.ops.is_empty() && self.streams.is_empty()
    }

    fn started_at(&self, op: &ConsumerOp) -> Option<u64> {
        let ConsumerOp::Handover { to, .. } = op else {
            return None;
        };
        self.handovers
            .get()?
            .iter()
            .find(|handed| handed.stream == to.stream && handed.to == to.name.as_str())
            .and_then(|handed| handed.start_sequence)
    }

    pub(crate) fn summary(&self) -> Value {
        json!({
            "streams": self
                .streams
                .iter()
                .map(|stream| stream.op.summary())
                .collect::<Vec<_>>(),
            "consumers": self
                .consumers
                .ops
                .iter()
                .map(|op| op.summary(self.started_at(op)))
                .collect::<Vec<_>>(),
            "warnings": self
                .streams
                .iter()
                .flat_map(|stream| &stream.warnings)
                .chain(&self.consumers.warnings)
                .collect::<Vec<_>>(),
        })
    }
}

pub(crate) async fn admit(
    state: &AppState,
    manifest: &ServiceManifest,
    tenant_id: Uuid,
    tenant_slug: &str,
) -> PlatformResult<()> {
    let namespace = state.tenants.get_or_load(tenant_id).await?.map_or_else(
        || Namespace::own(tenant_slug),
        |entry| entry.namespace.clone(),
    );
    validate_tenant_scope(
        manifest,
        &namespace,
        &state.tenants.foreign_subject_claims(tenant_id),
    )
}

fn refused(error: ApplyError) -> PlatformError {
    match error {
        ApplyError::Refused { .. } => PlatformError::Validation(error.to_string()),
        ApplyError::Unreachable { .. } => PlatformError::Unavailable(error.to_string()),
    }
}

fn server_version(version: &str) -> Option<(u64, u64)> {
    let mut parts = version.split(['.', '-']).map(str::parse::<u64>);
    match (parts.next(), parts.next()) {
        (Some(Ok(major)), Some(Ok(minor))) => Some((major, minor)),
        _ => None,
    }
}

fn require_multi_filter(plan: &ConsumerPlan, version: &str) -> PlatformResult<()> {
    let several = plan.ops.iter().any(|op| match op {
        ConsumerOp::Create { desired, .. }
        | ConsumerOp::Update { desired, .. }
        | ConsumerOp::Handover { to: desired, .. } => desired.filters.len() > 1,
        _ => false,
    });
    match server_version(version) {
        Some(server) if several && server < MULTI_FILTER_SERVER => {
            Err(PlatformError::Validation(format!(
                "filter_subjects needs NATS >= {}.{}; server is {version}",
                MULTI_FILTER_SERVER.0, MULTI_FILTER_SERVER.1
            )))
        }
        _ => Ok(()),
    }
}

async fn active_manifest(
    state: &AppState,
    identity: &ServiceIdentity,
) -> PlatformResult<Option<ServiceManifest>> {
    Ok(
        crate::repo::deployments::current_manifest(&state.platform_db, identity.service_id)
            .await?
            .and_then(|manifest| {
                serde_json::from_value(manifest)
                    .inspect_err(|e| {
                        warn!(
                            service = %identity.service_name,
                            tenant = %identity.tenant_slug,
                            error = %e,
                            "The active manifest no longer reads; planning as if it consumed nothing"
                        )
                    })
                    .ok()
            }),
    )
}

fn unavailable(e: eyre::Report) -> PlatformError {
    PlatformError::Unavailable(format!("JetStream is not reachable: {e}"))
}

async fn plan_streams(
    state: &AppState,
    manifest: &ServiceManifest,
    identity: &ServiceIdentity,
) -> PlatformResult<Vec<StreamPlan>> {
    let others: Vec<_> = state
        .registry
        .get_by_tenant(identity.tenant_id)
        .await
        .into_iter()
        .filter(|service| service.service_id != identity.service_id)
        .collect();
    let mut plans = Vec::with_capacity(manifest.streams.len());
    for declared in &manifest.streams {
        if let Some(refused) =
            streams::declared_differently(declared, others.iter().map(|other| other.as_ref()))
        {
            return Err(PlatformError::Validation(refused.to_string()));
        }
        let existing = streams::existing(&state.jetstream, &declared.name)
            .await
            .map_err(unavailable)?;
        plans.push(
            streams::plan_stream(declared, existing.as_ref(), &identity.tenant_slug)
                .map_err(|e| PlatformError::Validation(e.to_string()))?,
        );
    }
    Ok(plans)
}

async fn observe(
    state: &AppState,
    identity: &ServiceIdentity,
    consumers: &[&[DesiredConsumer]],
) -> PlatformResult<Vec<ObservedConsumer>> {
    let named: BTreeSet<(&str, &str)> = consumers
        .iter()
        .copied()
        .flatten()
        .map(|consumer| (consumer.stream.as_str(), consumer.name.as_str()))
        .collect();
    apply::observe(
        &state.jetstream,
        named.iter().map(|(stream, _)| *stream),
        |observed| {
            observed.is_managed_for(&identity.tenant_slug, &identity.service_name)
                || named.contains(&(observed.stream.as_str(), observed.name.as_str()))
        },
    )
    .await
    .map_err(refused)
}

pub(crate) async fn prepare(
    state: &AppState,
    manifest: &ServiceManifest,
    identity: &ServiceIdentity,
) -> PlatformResult<EventingPlan> {
    let service = crate::repo::services::get_by_id(&state.platform_db, identity.service_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service disappeared".into()))?;
    prepare_version(state, manifest, identity, service.current_version + 1).await
}

pub(crate) async fn prepare_version(
    state: &AppState,
    manifest: &ServiceManifest,
    identity: &ServiceIdentity,
    version: i32,
) -> PlatformResult<EventingPlan> {
    let active = active_manifest(state, identity).await?;
    if !desired::consumes(manifest)
        && manifest.streams.is_empty()
        && !active.as_ref().is_some_and(desired::consumes)
    {
        return Ok(EventingPlan {
            version,
            ..Default::default()
        });
    }

    let stream_plans = plan_streams(state, manifest, identity).await?;
    let server = state
        .stream_index
        .refresh(&state.jetstream)
        .await
        .map_err(unavailable)?;
    let changed: Vec<_> = stream_plans
        .iter()
        .filter_map(|plan| plan.op.planned())
        .collect();
    let streams = desired::with_planned(
        &desired::with_changed(&server, &changed),
        [desired::tenant_stream(&identity.tenant_slug)],
    );
    let wanted = desired::of_service(state, identity, manifest, &streams).await?;
    let prior = match &active {
        Some(active) => desired::placeable_of_service(state, identity, active, &streams).await?,
        None => Vec::new(),
    };
    let observed = observe(state, identity, &[&wanted, &prior]).await?;
    let elsewhere = desired::wanted_elsewhere(state, &streams, identity.service_id).await?;
    let consumers = plan_consumers(&wanted, &observed, &prior, &elsewhere)
        .map_err(|e| PlatformError::Validation(e.to_string()))?;
    require_multi_filter(&consumers, &state.nats.server_info().version)?;

    let tenant_stream = tenant_stream_name(&identity.tenant_slug);
    Ok(EventingPlan {
        version,
        streams: stream_plans,
        declared: manifest
            .streams
            .iter()
            .map(|declared| (declared.name.clone(), streams::config_hash(declared)))
            .collect(),
        consumers,
        tenant_subjects: wanted
            .iter()
            .any(|consumer| consumer.stream == tenant_stream)
            .then(|| desired::uncaptured_tenant_subjects(&wanted, &streams, &identity.tenant_slug)),
        schedules: std::iter::once(manifest)
            .chain(&active)
            .any(|manifest| !manifest.schedules.is_empty())
            .then(|| {
                manifest
                    .schedules
                    .iter()
                    .map(|schedule| schedule.name.clone())
                    .collect()
            }),
        handovers: Arc::default(),
        skipped: false,
    })
}

async fn introduce_schedules(
    state: &Arc<AppState>,
    identity: &ServiceIdentity,
    schedules: &[String],
) -> PlatformResult<()> {
    if schedules.is_empty() {
        return Ok(());
    }
    crate::nats_manager::events::setup_schedules_stream(&state.jetstream)
        .await
        .map_err(|e| {
            PlatformError::Unavailable(format!(
                "the stream of the schedules' fires could not be set up: {e}"
            ))
        })?;
    crate::repo::schedule_state::introduce(
        &state.platform_db.get().await?,
        identity.service_id,
        schedules,
        chrono::Utc::now(),
    )
    .await
}

async fn declare_streams(
    state: &Arc<AppState>,
    identity: &ServiceIdentity,
    plan: &EventingPlan,
) -> PlatformResult<()> {
    let client = state.platform_db.get().await?;
    for (name, hash) in &plan.declared {
        let held = crate::repo::tenant_streams::declare(
            &client,
            identity.tenant_id,
            name,
            identity.service_id,
            hash,
        )
        .await?;
        if !held {
            return Err(PlatformError::Validation(format!(
                "stream '{name}' is declared by another service of the tenant, and \
                 differently; every service that declares a stream declares it the same"
            )));
        }
    }
    let ops: Vec<StreamOp> = plan.streams.iter().map(|plan| plan.op.clone()).collect();
    streams::apply_stream_ops(state, &ops).await.map_err(|e| {
        let reason = format!("a declared stream could not be set up: {e}");
        if crate::nats_manager::replicas::is_refused(&e) {
            PlatformError::Validation(reason)
        } else {
            PlatformError::Unavailable(reason)
        }
    })
}

pub(crate) async fn before_activation(
    state: &Arc<AppState>,
    identity: &ServiceIdentity,
    plan: &EventingPlan,
) -> PlatformResult<()> {
    let handovers = if plan.changes_nothing() {
        Vec::new()
    } else {
        carry_out(state, identity, plan).await?
    };
    let _ = plan.handovers.set(handovers);
    Ok(())
}

async fn carry_out(
    state: &Arc<AppState>,
    identity: &ServiceIdentity,
    plan: &EventingPlan,
) -> PlatformResult<Vec<Handover>> {
    declare_streams(state, identity, plan).await?;
    introduce_schedules(
        state,
        identity,
        plan.schedules.as_deref().unwrap_or_default(),
    )
    .await?;
    if let Some(subjects) = &plan.tenant_subjects {
        crate::nats_manager::streams::expand_tenant_stream(state, &identity.tenant_slug, subjects)
            .await
            .map_err(|e| {
                PlatformError::Unavailable(format!(
                    "the stream of tenant '{}' could not be set up: {e}",
                    identity.tenant_slug
                ))
            })?;
    }
    let handovers = apply::apply(
        &state.jetstream,
        &plan.consumers.ops,
        ApplyPhase::BeforeActivation,
        identity,
    )
    .await
    .map_err(refused)?;
    let client = state.platform_db.get().await?;
    for handover in &handovers {
        crate::repo::consumer_handovers::record(
            &client,
            identity.service_id,
            plan.version,
            handover,
        )
        .await?;
    }
    Ok(handovers)
}

async fn release_streams(state: &Arc<AppState>, identity: &ServiceIdentity, plan: &EventingPlan) {
    let declared: Vec<String> = plan.declared.iter().map(|(name, _)| name.clone()).collect();
    let heirs: Vec<(String, Uuid)> = state
        .registry
        .get_by_tenant(identity.tenant_id)
        .await
        .iter()
        .filter(|service| service.service_id != identity.service_id)
        .flat_map(|service| {
            service
                .manifest
                .streams
                .iter()
                .map(|stream| (stream.name.clone(), service.service_id))
        })
        .collect();
    let released = async {
        crate::repo::tenant_streams::release(
            &state.platform_db.get().await?,
            identity.tenant_id,
            identity.service_id,
            &declared,
            &heirs,
        )
        .await
    }
    .await;
    if let Err(e) = released {
        warn!(
            service = %identity.service_name,
            tenant = %identity.tenant_slug,
            error = %e,
            "Could not give up the streams the service no longer declares"
        );
    }
}

async fn forget_schedules(state: &Arc<AppState>, identity: &ServiceIdentity, plan: &EventingPlan) {
    let Some(schedules) = &plan.schedules else {
        return;
    };
    let forgotten = async {
        crate::repo::schedule_state::keep_only(
            &state.platform_db.get().await?,
            identity.service_id,
            schedules,
        )
        .await
    }
    .await;
    if let Err(e) = forgotten {
        warn!(
            service = %identity.service_name,
            tenant = %identity.tenant_slug,
            error = %e,
            "Could not forget the timetables of the schedules the service no longer declares"
        );
    }
}

const LOCK_WAIT: Duration = Duration::from_secs(3);
const LOCK_RETRY: Duration = Duration::from_millis(100);

async fn lock_within(state: &AppState, service_id: Uuid) -> PlatformResult<Option<DeployLock>> {
    let deadline = tokio::time::Instant::now() + LOCK_WAIT;
    loop {
        if let Some(lock) = DeployLock::try_acquire(&state.platform_db, service_id).await? {
            return Ok(Some(lock));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(LOCK_RETRY).await;
    }
}

async fn retire(
    state: &Arc<AppState>,
    identity: &ServiceIdentity,
    plan: &EventingPlan,
) -> PlatformResult<()> {
    if plan.consumers.ops.iter().all(|op| op.retired().is_none()) {
        return Ok(());
    }
    let Some(lock) = lock_within(state, identity.service_id).await? else {
        info!(
            service = %identity.service_name,
            tenant = %identity.tenant_slug,
            "The service's deploy lock stays held; the next deploy decides which durables go"
        );
        return Ok(());
    };
    let retired = async {
        let streams = state.stream_index.snapshot();
        let wanted: HashSet<(String, String)> =
            desired::wanted_by_active(state, identity, &streams)
                .await?
                .into_iter()
                .chain(desired::wanted_elsewhere(state, &streams, identity.service_id).await?)
                .collect();
        apply::retire(&state.jetstream, &plan.consumers.ops, &wanted)
            .await
            .map_err(|e| {
                PlatformError::Internal(format!(
                    "Version {} of '{}' is active, but a durable it no longer consumes was not \
                     deleted: {e}. The next deploy deletes it",
                    plan.version, identity.service_name
                ))
            })
    }
    .await;
    lock.release().await;
    retired
}

async fn settle(
    state: &Arc<AppState>,
    manifest: &ServiceManifest,
    identity: &ServiceIdentity,
    plan: &EventingPlan,
) -> PlatformResult<()> {
    if let Err(e) =
        crate::nats_manager::streams::ensure_tenant_stream(state, &identity.tenant_slug).await
    {
        warn!(tenant = %identity.tenant_slug, error = %e, "The tenant's stream is not ready");
    }
    let deleted = retire(state, identity, plan).await;
    release_streams(state, identity, plan).await;
    forget_schedules(state, identity, plan).await;
    state.reconciler.settled(SETTLE_WITHIN).await;
    info!(
        "{} consumer loops run for '{}'",
        state
            .subscription_tasks
            .running_for(&identity.tenant_slug, &manifest.name),
        manifest.name
    );
    deleted
}

async fn heal(
    state: &Arc<AppState>,
    manifest: &ServiceManifest,
    identity: &ServiceIdentity,
) -> PlatformResult<EventingPlan> {
    let Some(lock) = DeployLock::try_acquire(&state.platform_db, identity.service_id).await? else {
        return Ok(EventingPlan::default());
    };
    let healed = async {
        let plan = prepare(state, manifest, identity).await?;
        before_activation(state, identity, &plan).await?;
        Ok(plan)
    }
    .await;
    lock.release().await;
    healed
}

pub(crate) async fn after_activation(
    state: &Arc<AppState>,
    manifest: &ServiceManifest,
    identity: &ServiceIdentity,
    plan: &EventingPlan,
) -> PlatformResult<()> {
    if plan.skipped {
        let healed = heal(state, manifest, identity).await?;
        settle(state, manifest, identity, &healed).await
    } else {
        plan.carried_out()?;
        settle(state, manifest, identity, plan).await
    }
}

pub(crate) async fn after_rollback(
    state: &Arc<AppState>,
    manifest: &ServiceManifest,
    identity: &ServiceIdentity,
    plan: &EventingPlan,
) -> PlatformResult<()> {
    plan.carried_out()?;
    settle(state, manifest, identity, plan).await
}

pub(crate) struct Leaving(Option<(ServiceIdentity, ConsumerPlan)>);

pub(crate) async fn leave(
    state: &AppState,
    tenant_id: Uuid,
    tenant_slug: &str,
    service_id: Uuid,
) -> PlatformResult<Leaving> {
    let manifest = crate::repo::deployments::current_manifest(&state.platform_db, service_id)
        .await?
        .and_then(|manifest| serde_json::from_value::<ServiceManifest>(manifest).ok())
        .filter(desired::consumes);
    let Some(manifest) = manifest else {
        return Ok(Leaving(None));
    };
    let identity = ServiceIdentity {
        tenant_id,
        tenant_slug: tenant_slug.to_string(),
        service_id,
        service_name: manifest.name.clone(),
        schema: manifest.schema(),
    };
    let left = |reason: &str| {
        warn!(
            service = %identity.service_name,
            tenant = %identity.tenant_slug,
            reason,
            "The durables of a service being deleted are left"
        )
    };
    let Some(lock) = lock_within(state, service_id).await? else {
        left("its deploy lock is held");
        return Ok(Leaving(None));
    };
    let planned = removal(state, &identity, &manifest).await;
    lock.release().await;
    match planned {
        Ok(plan) => Ok(Leaving(Some((identity, plan)))),
        Err(e) => {
            left(&e.to_string());
            Ok(Leaving(None))
        }
    }
}

async fn removal(
    state: &AppState,
    identity: &ServiceIdentity,
    manifest: &ServiceManifest,
) -> PlatformResult<ConsumerPlan> {
    let streams = state
        .stream_index
        .refresh(&state.jetstream)
        .await
        .map_err(unavailable)?;
    let prior = desired::placeable_of_service(state, identity, manifest, &streams).await?;
    let observed = observe(state, identity, &[&prior]).await?;
    let elsewhere = desired::wanted_elsewhere(state, &streams, identity.service_id).await?;
    plan_consumers(&[], &observed, &prior, &elsewhere)
        .map_err(|e| PlatformError::Validation(e.to_string()))
}

impl Leaving {
    pub(crate) async fn complete(self, state: &AppState) {
        let Some((identity, plan)) = self.0 else {
            return;
        };
        if let Err(e) = apply::retire(&state.jetstream, &plan.ops, &HashSet::new()).await {
            warn!(
                service = %identity.service_name,
                tenant = %identity.tenant_slug,
                error = %e,
                "A durable of a deleted service was not deleted"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_manager::consumers::naming::DurableName;
    use crate::nats_manager::consumers::plan::{
        ConsumerKind, CreatedWith, DeliverStart, HandoverMode, Retire,
    };
    use crate::nats_manager::consumers::LoopKey;
    use crate::shutdown::Shutdown;
    use crate::wasm_runtime::{WasmEngine, WasmLimits};
    use async_nats::jetstream::consumer::AckPolicy;
    use async_nats::jetstream::stream::RetentionPolicy;
    use cufflink_types::DeliverPolicySpec;
    use std::collections::BTreeMap;

    fn subscribing_to(subject: &str) -> ServiceManifest {
        ServiceManifest {
            name: "loan-service".into(),
            subscriptions: vec![cufflink_types::SubscriptionDefinition::new(
                subject,
                "on_book_deleted",
                cufflink_types::HandlerType::Wasm,
            )],
            ..Default::default()
        }
    }

    fn consumer(name: DurableName, filters: &[&str]) -> DesiredConsumer {
        DesiredConsumer {
            key: LoopKey {
                tenant_slug: "acme".into(),
                service_name: "loan-service".into(),
                handler: "on_book_deleted".into(),
                subject: filters[0].into(),
            },
            stream: "DW_ACME".into(),
            retention: RetentionPolicy::Interest,
            name,
            filters: filters.iter().map(|filter| filter.to_string()).collect(),
            ack_wait: Some(Duration::from_secs(30)),
            max_deliver: Some(-1),
            max_ack_pending: None,
            created_with: CreatedWith {
                ack_wait: Duration::from_secs(30),
                max_deliver: -1,
            },
            deliver_policy: DeliverPolicySpec::All,
            kind: ConsumerKind::Subscription,
            metadata: BTreeMap::new(),
        }
    }

    fn handover() -> ConsumerOp {
        ConsumerOp::Handover {
            from: ObservedConsumer {
                stream: "DW_ACME".into(),
                name: "loan_service_on_book_deleted".into(),
                filters: ["dw.acme.books.deleted".to_string()].into(),
                ack_wait: Duration::from_secs(30),
                max_deliver: -1,
                max_ack_pending: 1000,
                ack_policy: AckPolicy::Explicit,
                metadata: BTreeMap::new(),
                ack_floor_stream_seq: 40,
                delivered: 40,
            },
            to: consumer(
                DurableName::Scoped("acme_loan-service_on_book_deleted_0badcafe".into()),
                &["dw.acme.books.deleted"],
            ),
            mode: HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(41)),
            retire: Retire::Delete,
        }
    }

    #[tokio::test]
    async fn a_manifest_outside_its_tenant_is_not_admitted() {
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        let state = AppState::minimal(engine).await;
        assert!(admit(
            &state,
            &subscribing_to("dw.{tenant}.books.deleted"),
            Uuid::nil(),
            "acme"
        )
        .await
        .is_ok());
        assert!(matches!(
            admit(
                &state,
                &subscribing_to("dw.other.books.deleted"),
                Uuid::nil(),
                "acme"
            )
            .await,
            Err(PlatformError::Validation(_))
        ));
    }

    #[tokio::test]
    async fn a_plan_that_changes_nothing_asks_nothing_of_the_server() {
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        let state = AppState::minimal(engine).await;
        let identity = ServiceIdentity {
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            service_id: Uuid::nil(),
            service_name: "loan-service".into(),
            schema: "loan_service".into(),
        };
        let plan = EventingPlan::default();
        assert!(plan.changes_nothing());
        before_activation(&state, &identity, &plan)
            .await
            .expect("neither NATS nor the database is reachable, and neither is asked");
        assert_eq!(
            plan.summary(),
            json!({"streams": [], "consumers": [], "warnings": []})
        );
    }

    #[tokio::test]
    async fn a_plan_only_made_is_followed_by_nothing() {
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        let state = AppState::minimal(engine).await;
        let identity = ServiceIdentity {
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            service_id: Uuid::nil(),
            service_name: "loan-service".into(),
            schema: "loan_service".into(),
        };
        let manifest = subscribing_to("dw.{tenant}.books.deleted");
        let shown = EventingPlan {
            version: 8,
            consumers: ConsumerPlan {
                ops: vec![handover()],
                warnings: Vec::new(),
            },
            ..Default::default()
        };
        for followed in [
            after_activation(&state, &manifest, &identity, &shown).await,
            after_rollback(&state, &manifest, &identity, &shown).await,
            after_activation(&state, &manifest, &identity, &EventingPlan::default()).await,
        ] {
            assert!(
                matches!(&followed, Err(PlatformError::Internal(reason)) if reason.contains("not carried out")),
                "neither NATS nor the database is asked: {followed:?}"
            );
        }
        let nothing = EventingPlan {
            version: 8,
            ..Default::default()
        };
        before_activation(&state, &identity, &nothing)
            .await
            .expect("carried out without asking");
        assert!(nothing.carried_out().is_ok());
    }

    #[test]
    fn a_deploy_reports_where_a_handover_started() {
        let plan = EventingPlan {
            version: 8,
            consumers: ConsumerPlan {
                ops: vec![handover()],
                warnings: vec!["shared".into()],
            },
            ..Default::default()
        };
        assert_eq!(plan.summary()["consumers"][0]["start_sequence"], json!(41));
        plan.handovers
            .set(vec![Handover {
                stream: "DW_ACME".into(),
                from: "loan_service_on_book_deleted".into(),
                to: "acme_loan-service_on_book_deleted_0badcafe".into(),
                mode: "create_then_delete",
                start_sequence: Some(47),
            }])
            .expect("set once");
        assert_eq!(plan.summary()["consumers"][0]["start_sequence"], json!(47));
        assert_eq!(plan.summary()["warnings"], json!(["shared"]));
    }

    #[test]
    fn server_versions_are_read_by_their_first_two_numbers() {
        for (version, read) in [
            ("2.15.0", Some((2, 15))),
            ("2.9.25", Some((2, 9))),
            ("2.11.0-RC.1", Some((2, 11))),
            ("2.10", Some((2, 10))),
            ("", None),
            ("dev", None),
        ] {
            assert_eq!(server_version(version), read, "{version}");
        }
    }

    #[test]
    fn several_filters_need_a_server_that_has_them() {
        let several = ConsumerPlan {
            ops: vec![ConsumerOp::Create {
                desired: consumer(
                    DurableName::Named("loans-service-book-consumer".into()),
                    &["books.due_date.set", "books.deleted.loans"],
                ),
                deliver: DeliverStart::All,
            }],
            warnings: Vec::new(),
        };
        let error = require_multi_filter(&several, "2.9.25")
            .expect_err("refused")
            .to_string();
        assert!(
            error.contains("filter_subjects needs NATS >= 2.10; server is 2.9.25"),
            "{error}"
        );
        for version in ["2.10.0", "2.15.0", ""] {
            assert!(require_multi_filter(&several, version).is_ok(), "{version}");
        }
        let one = ConsumerPlan {
            ops: vec![handover()],
            warnings: Vec::new(),
        };
        assert!(require_multi_filter(&one, "2.9.25").is_ok());
    }
}
