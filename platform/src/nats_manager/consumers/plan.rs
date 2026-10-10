use super::naming::DurableName;
use super::LoopKey;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::stream::RetentionPolicy;
use cufflink_types::DeliverPolicySpec;
use serde::Serialize;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

pub const METADATA_TENANT: &str = "cufflink.tenant";
pub const METADATA_SERVICE: &str = "cufflink.service";
pub const METADATA_HANDLER: &str = "cufflink.handler";
pub const METADATA_MANAGED: &str = "cufflink.managed";
pub const METADATA_KIND: &str = "cufflink.kind";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerKind {
    Subscription,
    Tick,
    Schedule,
}

impl ConsumerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ConsumerKind::Subscription | ConsumerKind::Tick => "subscription",
            ConsumerKind::Schedule => "schedule",
        }
    }

    pub(super) fn is_timetable(self) -> bool {
        matches!(self, ConsumerKind::Tick | ConsumerKind::Schedule)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedWith {
    pub ack_wait: Duration,
    pub max_deliver: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredConsumer {
    pub key: LoopKey,
    pub stream: String,
    pub retention: RetentionPolicy,
    pub name: DurableName,
    pub filters: BTreeSet<String>,
    pub ack_wait: Option<Duration>,
    pub max_deliver: Option<i64>,
    pub max_ack_pending: Option<i64>,
    pub created_with: CreatedWith,
    pub deliver_policy: DeliverPolicySpec,
    pub kind: ConsumerKind,
    pub metadata: BTreeMap<String, String>,
}

impl DesiredConsumer {
    fn id(&self) -> (&str, &str) {
        (&self.stream, self.name.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedConsumer {
    pub stream: String,
    pub name: String,
    pub filters: BTreeSet<String>,
    pub ack_wait: Duration,
    pub max_deliver: i64,
    pub max_ack_pending: i64,
    pub ack_policy: AckPolicy,
    pub metadata: BTreeMap<String, String>,
    pub ack_floor_stream_seq: u64,
    pub delivered: u64,
}

impl ObservedConsumer {
    fn id(&self) -> (&str, &str) {
        (&self.stream, &self.name)
    }

    fn is(&self, key: &str, value: &str) -> bool {
        self.metadata.get(key).is_some_and(|v| v == value)
    }

    pub fn is_managed_for(&self, tenant_slug: &str, service_name: &str) -> bool {
        self.is(METADATA_MANAGED, "1")
            && self.is(METADATA_TENANT, tenant_slug)
            && self.is(METADATA_SERVICE, service_name)
    }

    fn managed_for_another(&self, key: &LoopKey) -> Option<String> {
        (self.is(METADATA_MANAGED, "1")
            && !self.is_managed_for(&key.tenant_slug, &key.service_name))
        .then(|| {
            let of = |field| self.metadata.get(field).map_or("?", String::as_str);
            format!(
                "managed by the platform for service '{}' of tenant '{}'",
                of(METADATA_SERVICE),
                of(METADATA_TENANT)
            )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverStart {
    All,
    New,
    Last,
    LastPerSubject,
    ByStartSequence(u64),
}

impl From<DeliverPolicySpec> for DeliverStart {
    fn from(policy: DeliverPolicySpec) -> Self {
        match policy {
            DeliverPolicySpec::All => DeliverStart::All,
            DeliverPolicySpec::New => DeliverStart::New,
            DeliverPolicySpec::Last => DeliverStart::Last,
            DeliverPolicySpec::LastPerSubject => DeliverStart::LastPerSubject,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoverMode {
    CreateThenDelete(DeliverStart),
    DeleteThenCreate,
}

impl HandoverMode {
    pub fn as_str(self) -> &'static str {
        match self {
            HandoverMode::CreateThenDelete(_) => "create_then_delete",
            HandoverMode::DeleteThenCreate => "delete_then_create",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retire {
    Delete,
    Keep,
    Release,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerOp {
    Keep {
        stream: String,
        name: String,
    },
    Create {
        desired: DesiredConsumer,
        deliver: DeliverStart,
    },
    Update {
        desired: DesiredConsumer,
        changes: Vec<&'static str>,
    },
    Handover {
        from: ObservedConsumer,
        to: DesiredConsumer,
        mode: HandoverMode,
        retire: Retire,
    },
    Delete {
        stream: String,
        name: String,
    },
    Release {
        stream: String,
        name: String,
    },
}

impl ConsumerOp {
    pub fn label(&self) -> &'static str {
        match self {
            ConsumerOp::Keep { .. } => "keep",
            ConsumerOp::Create { .. } => "create",
            ConsumerOp::Update { .. } => "update",
            ConsumerOp::Handover { .. } => "handover",
            ConsumerOp::Delete { .. } => "delete",
            ConsumerOp::Release { .. } => "release",
        }
    }
}

impl ConsumerOp {
    pub fn retired(&self) -> Option<(&str, &str)> {
        match self {
            ConsumerOp::Delete { stream, name }
            | ConsumerOp::Handover {
                from: ObservedConsumer { stream, name, .. },
                mode: HandoverMode::CreateThenDelete(_),
                retire: Retire::Delete,
                ..
            } => Some((stream, name)),
            _ => None,
        }
    }

    pub fn summary(&self, started_at: Option<u64>) -> serde_json::Value {
        let of = |desired: &DesiredConsumer| {
            json!({
                "op": self.label(),
                "stream": desired.stream,
                "durable": desired.name.as_str(),
                "handler": desired.key.handler,
            })
        };
        let with = |summary: serde_json::Value, key: &str, value: serde_json::Value| {
            let mut summary = summary;
            summary[key] = value;
            summary
        };
        match self {
            ConsumerOp::Keep { stream, name }
            | ConsumerOp::Delete { stream, name }
            | ConsumerOp::Release { stream, name } => {
                json!({"op": self.label(), "stream": stream, "durable": name})
            }
            ConsumerOp::Create { desired, deliver } => with(of(desired), "deliver", json!(deliver)),
            ConsumerOp::Update { desired, changes } => with(of(desired), "changes", json!(changes)),
            ConsumerOp::Handover { from, to, mode, .. } => {
                let planned = match mode {
                    HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(sequence)) => {
                        Some(*sequence)
                    }
                    _ => None,
                };
                json!({
                    "op": self.label(),
                    "stream": to.stream,
                    "handler": to.key.handler,
                    "from": from.name,
                    "to": to.name.as_str(),
                    "mode": mode.as_str(),
                    "start_sequence": started_at.or(planned),
                })
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsumerPlan {
    pub ops: Vec<ConsumerOp>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConsumerPlanError {
    #[error(
        "durable '{name}' on stream '{stream}' has {field} '{found}'; the platform consumes \
         with explicit acknowledgements and changes no durable's {field}"
    )]
    Incompatible {
        stream: String,
        name: String,
        field: &'static str,
        found: String,
    },
    #[error(
        "subscriptions '{first}' and '{second}' both consume durable '{name}' on stream \
         '{stream}'; give each a durable of its own"
    )]
    SharedDurable {
        stream: String,
        name: String,
        first: String,
        second: String,
    },
    #[error(
        "durable '{from}' on work-queue stream '{stream}' must go before '{to}' can consume \
         its subjects, and the platform does not delete it: {reason}"
    )]
    CannotRetire {
        stream: String,
        from: String,
        to: String,
        reason: &'static str,
    },
    #[error(
        "durable '{name}' on stream '{stream}' is {owner}; a manifest binds only a durable \
         no other service consumes"
    )]
    NotOwned {
        stream: String,
        name: String,
        owner: String,
    },
}

pub fn plan_handover(from: &ObservedConsumer, to: &DesiredConsumer) -> HandoverMode {
    match (to.retention, to.kind.is_timetable()) {
        (RetentionPolicy::WorkQueue, _) => HandoverMode::DeleteThenCreate,
        (_, true) => HandoverMode::CreateThenDelete(DeliverStart::New),
        (_, false) => HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(
            from.ack_floor_stream_seq.saturating_add(1),
        )),
    }
}

fn ack_policy_name(policy: AckPolicy) -> &'static str {
    match policy {
        AckPolicy::Explicit => "explicit",
        AckPolicy::None => "none",
        AckPolicy::All => "all",
        AckPolicy::FlowControl => "flow_control",
    }
}

fn changes(desired: &DesiredConsumer, observed: &ObservedConsumer) -> Vec<&'static str> {
    [
        ("filters", desired.filters != observed.filters),
        (
            "ack_wait",
            desired
                .ack_wait
                .is_some_and(|wait| wait != observed.ack_wait),
        ),
        (
            "max_deliver",
            desired
                .max_deliver
                .is_some_and(|max| max != observed.max_deliver),
        ),
        (
            "max_ack_pending",
            desired
                .max_ack_pending
                .is_some_and(|max| max != observed.max_ack_pending),
        ),
    ]
    .into_iter()
    .filter_map(|(field, changed)| changed.then_some(field))
    .collect()
}

fn per_durable(
    desired: &[DesiredConsumer],
) -> Result<(Vec<&DesiredConsumer>, Vec<String>), ConsumerPlanError> {
    desired.iter().try_fold(
        (Vec::<&DesiredConsumer>::new(), Vec::new()),
        |(mut kept, mut warnings), consumer| {
            match kept.iter().position(|other| other.id() == consumer.id()) {
                None => kept.push(consumer),
                Some(at) if kept[at].key == consumer.key => kept[at] = consumer,
                Some(at) if consumer.name.is_legacy() && kept[at].name.is_legacy() => {
                    warnings.push(format!(
                        "subscriptions '{}' and '{}' share durable '{}' on stream '{}': it \
                         filters on the subject of the last declared; declare \
                         `consumers: scoped` to give each its own",
                        kept[at].key.subject, consumer.key.subject, consumer.name, consumer.stream
                    ));
                    kept[at] = consumer;
                }
                Some(at) => {
                    return Err(ConsumerPlanError::SharedDurable {
                        stream: consumer.stream.clone(),
                        name: consumer.name.to_string(),
                        first: kept[at].key.handler.clone(),
                        second: consumer.key.handler.clone(),
                    })
                }
            }
            Ok((kept, warnings))
        },
    )
}

struct Known<'a> {
    observed: &'a [ObservedConsumer],
    prior: &'a [DesiredConsumer],
    wanted_elsewhere: &'a HashSet<(String, String)>,
}

impl Known<'_> {
    fn observed(&self, id: (&str, &str)) -> Option<&ObservedConsumer> {
        self.observed.iter().find(|consumer| consumer.id() == id)
    }

    fn owns(&self, desired: &DesiredConsumer) -> Result<(), ConsumerPlanError> {
        if !desired.name.is_bound() || desired.kind == ConsumerKind::Tick {
            return Ok(());
        }
        let id = (desired.stream.clone(), desired.name.to_string());
        let owner = if self.wanted_elsewhere.contains(&id) {
            Some("consumed by another service".to_string())
        } else {
            self.observed(desired.id())
                .and_then(|existing| existing.managed_for_another(&desired.key))
        };
        owner.map_or(Ok(()), |owner| {
            Err(ConsumerPlanError::NotOwned {
                stream: id.0,
                name: id.1,
                owner,
            })
        })
    }

    fn retire(&self, consumer: &ObservedConsumer) -> Retire {
        let bound = self
            .prior
            .iter()
            .any(|prior| prior.id() == consumer.id() && prior.name.is_bound());
        let shared = self
            .wanted_elsewhere
            .contains(&(consumer.stream.clone(), consumer.name.clone()));
        match (bound, shared) {
            (true, _) => Retire::Release,
            (false, true) => Retire::Keep,
            (false, false) => Retire::Delete,
        }
    }

    fn prior_names(&self, desired: &DesiredConsumer) -> bool {
        self.prior.iter().any(|prior| prior.id() == desired.id())
    }

    fn predecessor(
        &self,
        desired: &DesiredConsumer,
        wanted: &[&DesiredConsumer],
    ) -> Option<&ObservedConsumer> {
        self.prior
            .iter()
            .filter(|prior| {
                prior.key == desired.key
                    && prior.stream == desired.stream
                    && prior.name.as_str() != desired.name.as_str()
            })
            .filter_map(|prior| self.observed(prior.id()))
            .find(|from| !wanted.iter().any(|other| other.id() == from.id()))
    }
}

fn handover(
    from: &ObservedConsumer,
    to: &DesiredConsumer,
    retire: Retire,
) -> Result<ConsumerOp, ConsumerPlanError> {
    let mode = plan_handover(from, to);
    let refused = |reason| {
        Err(ConsumerPlanError::CannotRetire {
            stream: to.stream.clone(),
            from: from.name.clone(),
            to: to.name.to_string(),
            reason,
        })
    };
    match (mode, retire) {
        (HandoverMode::DeleteThenCreate, Retire::Release) => {
            refused("the manifest bound it; delete it by hand or keep its name")
        }
        (HandoverMode::DeleteThenCreate, Retire::Keep) => refused("another service consumes it"),
        _ => Ok(ConsumerOp::Handover {
            from: from.clone(),
            to: to.clone(),
            mode,
            retire,
        }),
    }
}

pub fn plan_consumers(
    desired: &[DesiredConsumer],
    observed: &[ObservedConsumer],
    prior: &[DesiredConsumer],
    wanted_elsewhere: &HashSet<(String, String)>,
) -> Result<ConsumerPlan, ConsumerPlanError> {
    let (wanted, warnings) = per_durable(desired)?;
    let known = Known {
        observed,
        prior,
        wanted_elsewhere,
    };
    let (mut ops, taken) = wanted.iter().try_fold(
        (Vec::new(), Vec::new()),
        |(mut ops, mut taken), consumer| {
            known.owns(consumer)?;
            let predecessor = known.predecessor(consumer, &wanted);
            let op = match (known.observed(consumer.id()), predecessor) {
                (Some(existing), _) if existing.ack_policy != AckPolicy::Explicit => {
                    return Err(ConsumerPlanError::Incompatible {
                        stream: existing.stream.clone(),
                        name: existing.name.clone(),
                        field: "ack_policy",
                        found: ack_policy_name(existing.ack_policy).to_string(),
                    })
                }
                (Some(left), Some(from))
                    if left.delivered == 0
                        && !consumer.name.is_bound()
                        && !known.prior_names(consumer) =>
                {
                    taken.push(from.id());
                    handover(from, consumer, known.retire(from))?
                }
                (Some(existing), _) => match changes(consumer, existing) {
                    changed if changed.is_empty() => ConsumerOp::Keep {
                        stream: existing.stream.clone(),
                        name: existing.name.clone(),
                    },
                    changed => ConsumerOp::Update {
                        desired: (*consumer).clone(),
                        changes: changed,
                    },
                },
                (None, Some(from)) => {
                    taken.push(from.id());
                    handover(from, consumer, known.retire(from))?
                }
                (None, None) => ConsumerOp::Create {
                    desired: (*consumer).clone(),
                    deliver: consumer.deliver_policy.into(),
                },
            };
            ops.push(op);
            Ok((ops, taken))
        },
    )?;
    ops.extend(
        observed
            .iter()
            .filter(|consumer| {
                !taken.contains(&consumer.id())
                    && !wanted.iter().any(|other| other.id() == consumer.id())
            })
            .map(|consumer| {
                let (stream, name) = (consumer.stream.clone(), consumer.name.clone());
                match known.retire(consumer) {
                    Retire::Release => ConsumerOp::Release { stream, name },
                    Retire::Keep => ConsumerOp::Keep { stream, name },
                    Retire::Delete => ConsumerOp::Delete { stream, name },
                }
            }),
    );
    let released = ops.iter().filter_map(|op| match op {
        ConsumerOp::Release { stream, name }
        | ConsumerOp::Handover {
            from: ObservedConsumer { stream, name, .. },
            retire: Retire::Release,
            ..
        } => Some(format!(
            "durable '{name}' on stream '{stream}' is no longer consumed by this service; it is kept"
        )),
        _ => None,
    });
    let warnings = warnings.into_iter().chain(released).collect();
    Ok(ConsumerPlan { ops, warnings })
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM: &str = "DW_DEFAULT";
    const TICKS: &str = "CUFFLINK_TICKS";

    fn key(handler: &str, subject: &str) -> LoopKey {
        LoopKey {
            tenant_slug: "default".into(),
            service_name: "stock-service".into(),
            handler: handler.into(),
            subject: subject.into(),
        }
    }

    fn metadata() -> BTreeMap<String, String> {
        [
            (METADATA_TENANT, "default"),
            (METADATA_SERVICE, "stock-service"),
            (METADATA_HANDLER, "on_created"),
            (METADATA_MANAGED, "1"),
            (METADATA_KIND, "subscription"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn desired(name: DurableName, subject: &str) -> DesiredConsumer {
        let bound = name.is_bound();
        DesiredConsumer {
            key: key("on_created", subject),
            stream: STREAM.into(),
            retention: RetentionPolicy::Interest,
            name,
            filters: BTreeSet::from([subject.to_string()]),
            ack_wait: (!bound).then_some(Duration::from_secs(30)),
            max_deliver: (!bound).then_some(-1),
            max_ack_pending: None,
            created_with: CreatedWith {
                ack_wait: Duration::from_secs(30),
                max_deliver: -1,
            },
            deliver_policy: DeliverPolicySpec::All,
            kind: ConsumerKind::Subscription,
            metadata: if bound { BTreeMap::new() } else { metadata() },
        }
    }

    fn legacy() -> DesiredConsumer {
        desired(
            DurableName::Legacy("stock_service_on_created".into()),
            "dw.default.stocks.created",
        )
    }

    fn scoped() -> DesiredConsumer {
        desired(
            DurableName::Scoped("default_stock-service_on_created_1a2b3c4d".into()),
            "dw.default.stocks.created",
        )
    }

    fn bound() -> DesiredConsumer {
        DesiredConsumer {
            stream: "BOOKS".into(),
            retention: RetentionPolicy::Limits,
            filters: BTreeSet::from([
                "books.due_date.set".to_string(),
                "books.deleted.loans".to_string(),
            ]),
            ..desired(
                DurableName::Named("loans-service-book-consumer".into()),
                "books.due_date.set",
            )
        }
    }

    fn on(stream: &str, retention: RetentionPolicy, consumer: DesiredConsumer) -> DesiredConsumer {
        DesiredConsumer {
            stream: stream.into(),
            retention,
            ..consumer
        }
    }

    fn tick(consumer: DesiredConsumer) -> DesiredConsumer {
        DesiredConsumer {
            kind: ConsumerKind::Tick,
            ..on(TICKS, RetentionPolicy::Interest, consumer)
        }
    }

    fn server_has(consumer: &DesiredConsumer, floor: u64) -> ObservedConsumer {
        ObservedConsumer {
            stream: consumer.stream.clone(),
            name: consumer.name.to_string(),
            filters: consumer.filters.clone(),
            ack_wait: consumer.ack_wait.unwrap_or(Duration::from_secs(30)),
            max_deliver: consumer.max_deliver.unwrap_or(-1),
            max_ack_pending: consumer.max_ack_pending.unwrap_or(1000),
            ack_policy: AckPolicy::Explicit,
            metadata: consumer.metadata.clone(),
            ack_floor_stream_seq: floor,
            delivered: floor,
        }
    }

    fn nowhere() -> HashSet<(String, String)> {
        HashSet::new()
    }

    fn ops(
        desired: &[DesiredConsumer],
        observed: &[ObservedConsumer],
        prior: &[DesiredConsumer],
        elsewhere: &HashSet<(String, String)>,
    ) -> Vec<ConsumerOp> {
        plan_consumers(desired, observed, prior, elsewhere)
            .expect("plan")
            .ops
    }

    fn keep(consumer: &DesiredConsumer) -> ConsumerOp {
        ConsumerOp::Keep {
            stream: consumer.stream.clone(),
            name: consumer.name.to_string(),
        }
    }

    #[test]
    fn a_durable_that_is_as_declared_is_kept() {
        let plan = plan_consumers(
            &[legacy()],
            &[server_has(&legacy(), 7)],
            &[legacy()],
            &nowhere(),
        )
        .expect("plan");
        assert_eq!(plan.ops, [keep(&legacy())]);
        assert!(plan.warnings.is_empty());
    }

    #[test]
    fn a_durable_created_before_metadata_is_kept_as_it_is() {
        let pre_r3 = ObservedConsumer {
            metadata: BTreeMap::new(),
            ..server_has(&legacy(), 7)
        };
        assert_eq!(
            ops(&[legacy()], &[pre_r3], &[legacy()], &nowhere()),
            [keep(&legacy())]
        );
    }

    #[test]
    fn what_the_server_changes_in_place_is_an_update() {
        let wants = DesiredConsumer {
            filters: BTreeSet::from(["dw.default.stocks.>".to_string()]),
            ack_wait: Some(Duration::from_secs(45)),
            max_deliver: Some(5),
            max_ack_pending: Some(50),
            ..legacy()
        };
        assert_eq!(
            ops(
                std::slice::from_ref(&wants),
                &[server_has(&legacy(), 7)],
                &[legacy()],
                &nowhere()
            ),
            [ConsumerOp::Update {
                desired: wants,
                changes: vec!["filters", "ack_wait", "max_deliver", "max_ack_pending"],
            }]
        );
    }

    #[test]
    fn a_bound_durable_keeps_what_the_manifest_does_not_declare() {
        let theirs = ObservedConsumer {
            ack_wait: Duration::from_secs(30),
            max_deliver: 3,
            max_ack_pending: 1000,
            ..server_has(&bound(), 4)
        };
        assert_eq!(
            ops(&[bound()], std::slice::from_ref(&theirs), &[], &nowhere()),
            [keep(&bound())],
            "nothing is declared, so nothing is compared"
        );
        let declares = DesiredConsumer {
            max_deliver: Some(3),
            ack_wait: Some(Duration::from_secs(60)),
            ..bound()
        };
        assert_eq!(
            ops(std::slice::from_ref(&declares), &[theirs], &[], &nowhere()),
            [ConsumerOp::Update {
                desired: declares,
                changes: vec!["ack_wait"],
            }]
        );
    }

    #[test]
    fn a_durable_that_acknowledges_otherwise_is_refused() {
        for (consumer, policy, found) in [
            (bound(), AckPolicy::None, "none"),
            (legacy(), AckPolicy::All, "all"),
        ] {
            let theirs = ObservedConsumer {
                ack_policy: policy,
                ..server_has(&consumer, 0)
            };
            assert_eq!(
                plan_consumers(std::slice::from_ref(&consumer), &[theirs], &[], &nowhere()),
                Err(ConsumerPlanError::Incompatible {
                    stream: consumer.stream.clone(),
                    name: consumer.name.to_string(),
                    field: "ack_policy",
                    found: found.into(),
                })
            );
        }
    }

    #[test]
    fn a_durable_the_server_lacks_is_created_where_its_policy_says() {
        for (policy, deliver) in [
            (DeliverPolicySpec::All, DeliverStart::All),
            (DeliverPolicySpec::New, DeliverStart::New),
            (DeliverPolicySpec::Last, DeliverStart::Last),
            (
                DeliverPolicySpec::LastPerSubject,
                DeliverStart::LastPerSubject,
            ),
        ] {
            for consumer in [legacy(), scoped(), bound()] {
                let wants = DesiredConsumer {
                    deliver_policy: policy,
                    ..consumer
                };
                assert_eq!(
                    ops(std::slice::from_ref(&wants), &[], &[], &nowhere()),
                    [ConsumerOp::Create {
                        desired: wants,
                        deliver,
                    }]
                );
            }
        }
    }

    #[test]
    fn a_renamed_durable_hands_over_above_its_predecessors_floor() {
        for retention in [RetentionPolicy::Interest, RetentionPolicy::Limits] {
            let (from, to) = (
                on(STREAM, retention, legacy()),
                on(STREAM, retention, scoped()),
            );
            assert_eq!(
                ops(
                    std::slice::from_ref(&to),
                    &[server_has(&from, 81_233)],
                    std::slice::from_ref(&from),
                    &nowhere()
                ),
                [ConsumerOp::Handover {
                    from: server_has(&from, 81_233),
                    to,
                    mode: HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(81_234)),
                    retire: Retire::Delete,
                }]
            );
        }
    }

    #[test]
    fn a_successor_a_failed_deploy_left_starts_again_where_its_predecessor_stops_now() {
        let left = ObservedConsumer {
            delivered: 0,
            ..server_has(&scoped(), 5_000)
        };
        let predecessor = server_has(&legacy(), 7_000);
        assert_eq!(
            ops(
                &[scoped()],
                &[predecessor.clone(), left],
                &[legacy()],
                &nowhere()
            ),
            [ConsumerOp::Handover {
                from: predecessor.clone(),
                to: scoped(),
                mode: HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(7_001)),
                retire: Retire::Delete,
            }]
        );
        assert_eq!(
            ops(
                &[scoped()],
                &[predecessor.clone(), server_has(&scoped(), 5_000)],
                &[legacy()],
                &nowhere()
            ),
            [
                keep(&scoped()),
                ConsumerOp::Delete {
                    stream: STREAM.into(),
                    name: legacy().name.to_string(),
                }
            ],
            "one that has delivered is consumed already"
        );
        let unused = ObservedConsumer {
            delivered: 0,
            ..server_has(&scoped(), 5_000)
        };
        assert_eq!(
            ops(&[scoped()], &[unused], &[scoped()], &nowhere()),
            [keep(&scoped())],
            "one the active version consumes is its own, delivered or not"
        );
        let binds = desired(
            DurableName::Named("quiet-consumer".into()),
            "dw.default.stocks.created",
        );
        let quiet = ObservedConsumer {
            delivered: 0,
            ..server_has(&binds, 0)
        };
        assert_eq!(
            ops(
                std::slice::from_ref(&binds),
                &[predecessor, quiet],
                &[legacy()],
                &nowhere()
            ),
            [
                keep(&binds),
                ConsumerOp::Delete {
                    stream: STREAM.into(),
                    name: legacy().name.to_string(),
                }
            ],
            "a bound durable that has delivered nothing is bound, not replaced"
        );
    }

    #[test]
    fn on_a_work_queue_the_predecessor_goes_first() {
        let (from, to) = (
            on("RECEIPTS", RetentionPolicy::WorkQueue, legacy()),
            on("RECEIPTS", RetentionPolicy::WorkQueue, scoped()),
        );
        assert_eq!(
            ops(
                std::slice::from_ref(&to),
                &[server_has(&from, 20)],
                std::slice::from_ref(&from),
                &nowhere()
            ),
            [ConsumerOp::Handover {
                from: server_has(&from, 20),
                to,
                mode: HandoverMode::DeleteThenCreate,
                retire: Retire::Delete,
            }]
        );
    }

    #[test]
    fn a_timetable_consumer_starts_at_what_comes_next() {
        let schedule = |consumer| DesiredConsumer {
            kind: ConsumerKind::Schedule,
            ..on("CUFFLINK_EVERY", RetentionPolicy::Limits, consumer)
        };
        for (from, to) in [
            (tick(legacy()), tick(scoped())),
            (schedule(legacy()), schedule(scoped())),
        ] {
            assert_eq!(
                plan_handover(&server_has(&from, 99), &to),
                HandoverMode::CreateThenDelete(DeliverStart::New)
            );
        }
        let queued = DesiredConsumer {
            kind: ConsumerKind::Schedule,
            ..on("CUFFLINK_SCHEDULES", RetentionPolicy::WorkQueue, scoped())
        };
        assert_eq!(
            plan_handover(&server_has(&legacy(), 99), &queued),
            HandoverMode::DeleteThenCreate
        );
    }

    #[test]
    fn a_rollback_hands_back() {
        assert_eq!(
            ops(
                &[legacy()],
                &[server_has(&scoped(), 12)],
                &[scoped()],
                &nowhere()
            ),
            [ConsumerOp::Handover {
                from: server_has(&scoped(), 12),
                to: legacy(),
                mode: HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(13)),
                retire: Retire::Delete,
            }]
        );
    }

    #[test]
    fn a_bound_durable_no_longer_consumed_is_released() {
        let plan =
            plan_consumers(&[], &[server_has(&bound(), 4)], &[bound()], &nowhere()).expect("plan");
        assert_eq!(
            plan.ops,
            [ConsumerOp::Release {
                stream: "BOOKS".into(),
                name: "loans-service-book-consumer".into(),
            }]
        );
        assert_eq!(
            plan.warnings,
            [
                "durable 'loans-service-book-consumer' on stream 'BOOKS' is no longer consumed by \
              this service; it is kept"
            ]
        );
    }

    #[test]
    fn a_bound_durable_is_kept_when_its_successor_takes_over() {
        let to = DesiredConsumer {
            name: DurableName::Scoped("harbor_loan-service_on_book_event_00c0ffee".into()),
            ack_wait: Some(Duration::from_secs(30)),
            max_deliver: Some(-1),
            ..bound()
        };
        let plan = plan_consumers(
            std::slice::from_ref(&to),
            &[server_has(&bound(), 4)],
            &[bound()],
            &nowhere(),
        )
        .expect("plan");
        assert_eq!(
            plan.ops,
            [ConsumerOp::Handover {
                from: server_has(&bound(), 4),
                to: to.clone(),
                mode: HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(5)),
                retire: Retire::Release,
            }]
        );
        assert_eq!(plan.warnings.len(), 1);

        let queued = |consumer| on("RECEIPTS", RetentionPolicy::WorkQueue, consumer);
        assert_eq!(
            plan_consumers(
                &[queued(to.clone())],
                &[server_has(&queued(bound()), 4)],
                &[queued(bound())],
                &nowhere()
            ),
            Err(ConsumerPlanError::CannotRetire {
                stream: "RECEIPTS".into(),
                from: "loans-service-book-consumer".into(),
                to: to.name.to_string(),
                reason: "the manifest bound it; delete it by hand or keep its name",
            })
        );
    }

    #[test]
    fn a_durable_nothing_wants_is_deleted() {
        let dropped = desired(
            DurableName::Legacy("stock_service_on_deleted".into()),
            "dw.default.stocks.deleted",
        );
        assert_eq!(
            ops(
                &[legacy()],
                &[server_has(&legacy(), 3), server_has(&dropped, 3)],
                &[legacy(), dropped.clone()],
                &nowhere()
            ),
            [
                keep(&legacy()),
                ConsumerOp::Delete {
                    stream: STREAM.into(),
                    name: "stock_service_on_deleted".into(),
                }
            ]
        );
        let orphan = server_has(&scoped(), 0);
        assert_eq!(
            ops(&[legacy()], &[orphan], &[], &nowhere()),
            [
                ConsumerOp::Create {
                    desired: legacy(),
                    deliver: DeliverStart::All,
                },
                ConsumerOp::Delete {
                    stream: STREAM.into(),
                    name: scoped().name.to_string(),
                }
            ],
            "what a deploy that failed left behind goes with the next"
        );
    }

    #[test]
    fn a_durable_another_service_wants_is_kept() {
        let (from, to) = (tick(legacy()), tick(scoped()));
        let elsewhere = HashSet::from([(TICKS.to_string(), from.name.to_string())]);
        assert_eq!(
            ops(
                std::slice::from_ref(&to),
                &[server_has(&from, 40)],
                std::slice::from_ref(&from),
                &elsewhere
            ),
            [ConsumerOp::Handover {
                from: server_has(&from, 40),
                to,
                mode: HandoverMode::CreateThenDelete(DeliverStart::New),
                retire: Retire::Keep,
            }]
        );
        assert_eq!(
            ops(
                &[],
                &[server_has(&from, 40)],
                std::slice::from_ref(&from),
                &elsewhere
            ),
            [keep(&from)]
        );
        let queued = |consumer| on("RECEIPTS", RetentionPolicy::WorkQueue, consumer);
        let shared = HashSet::from([("RECEIPTS".to_string(), legacy().name.to_string())]);
        assert!(matches!(
            plan_consumers(
                &[queued(scoped())],
                &[server_has(&queued(legacy()), 1)],
                &[queued(legacy())],
                &shared
            ),
            Err(ConsumerPlanError::CannotRetire {
                reason: "another service consumes it",
                ..
            })
        ));
    }

    #[test]
    fn a_handler_on_two_subjects_shares_its_legacy_durable_with_a_warning() {
        let (first, second) = (
            legacy(),
            desired(legacy().name, "dw.default.stocks.updated"),
        );
        let plan = plan_consumers(
            &[first.clone(), second.clone()],
            &[server_has(&first, 9)],
            &[first, second.clone()],
            &nowhere(),
        )
        .expect("plan");
        assert_eq!(
            plan.ops,
            [ConsumerOp::Update {
                desired: second,
                changes: vec!["filters"],
            }],
            "the last declared decides the filter, as it always did"
        );
        assert_eq!(plan.warnings.len(), 1);
        assert!(plan.warnings[0].contains("consumers: scoped"), "{plan:?}");
    }

    #[test]
    fn a_legacy_durable_a_handler_shared_hands_over_to_the_successor_of_each_subject() {
        let updated = "dw.default.stocks.updated";
        for (retention, mode) in [
            (
                RetentionPolicy::Interest,
                HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(5_001)),
            ),
            (RetentionPolicy::WorkQueue, HandoverMode::DeleteThenCreate),
        ] {
            let prior = [
                on(STREAM, retention, legacy()),
                on(STREAM, retention, desired(legacy().name, updated)),
            ];
            let successors = [
                on(STREAM, retention, scoped()),
                on(
                    STREAM,
                    retention,
                    desired(
                        DurableName::Scoped("default_stock-service_on_created_5e6f7a8b".into()),
                        updated,
                    ),
                ),
            ];
            let shared = server_has(&prior[1], 5_000);
            assert_eq!(
                ops(
                    &successors,
                    std::slice::from_ref(&shared),
                    &prior,
                    &nowhere()
                ),
                successors
                    .iter()
                    .map(|to| ConsumerOp::Handover {
                        from: shared.clone(),
                        to: to.clone(),
                        mode,
                        retire: Retire::Delete,
                    })
                    .collect::<Vec<_>>(),
                "every subject starts above the floor, none from the stream's start"
            );
        }
    }

    #[test]
    fn a_manifest_binds_no_durable_another_service_consumes() {
        let managed_for = |tenant: &str, service: &str| ObservedConsumer {
            metadata: [
                (METADATA_TENANT, tenant),
                (METADATA_SERVICE, service),
                (METADATA_MANAGED, "1"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            ..server_has(&bound(), 7)
        };
        let not_owned = |owner: &str| ConsumerPlanError::NotOwned {
            stream: "BOOKS".into(),
            name: bound().name.to_string(),
            owner: owner.into(),
        };
        let plan = |observed: &[ObservedConsumer], elsewhere: &HashSet<(String, String)>| {
            plan_consumers(&[bound()], observed, &[], elsewhere).map(|plan| plan.ops)
        };
        assert_eq!(
            plan(&[managed_for("default", "receipt-service")], &nowhere()),
            Err(not_owned(
                "managed by the platform for service 'receipt-service' of tenant 'default'"
            ))
        );
        assert_eq!(
            plan(&[managed_for("harbor", "stock-service")], &nowhere()),
            Err(not_owned(
                "managed by the platform for service 'stock-service' of tenant 'harbor'"
            ))
        );
        let consumed_elsewhere = HashSet::from([("BOOKS".to_string(), bound().name.to_string())]);
        for observed in [vec![], vec![server_has(&bound(), 7)]] {
            assert_eq!(
                plan(&observed, &consumed_elsewhere),
                Err(not_owned("consumed by another service"))
            );
        }
        assert_eq!(
            plan(&[server_has(&bound(), 7)], &nowhere()),
            Ok(vec![keep(&bound())]),
            "a durable the legacy consumer created, which no service of the platform consumes"
        );
        assert_eq!(
            plan(&[managed_for("default", "stock-service")], &nowhere()),
            Ok(vec![keep(&bound())]),
            "one the platform managed for this service"
        );
    }

    #[test]
    fn a_tick_durable_is_bound_by_every_tenant_that_runs_the_service() {
        let own = tick(desired(
            DurableName::Named("stock_service_on_created".into()),
            "cufflink.tick.5min",
        ));
        let shared = HashSet::from([(TICKS.to_string(), own.name.to_string())]);
        assert_eq!(
            ops(
                std::slice::from_ref(&own),
                &[server_has(&own, 3)],
                &[],
                &shared
            ),
            [keep(&own)]
        );
    }

    #[test]
    fn two_subscriptions_on_one_chosen_durable_are_refused() {
        for name in [scoped().name, bound().name] {
            let other = DesiredConsumer {
                key: key("on_updated", "dw.default.stocks.updated"),
                ..desired(name.clone(), "dw.default.stocks.updated")
            };
            assert_eq!(
                plan_consumers(
                    &[desired(name.clone(), "dw.default.stocks.created"), other],
                    &[],
                    &[],
                    &nowhere()
                ),
                Err(ConsumerPlanError::SharedDurable {
                    stream: STREAM.into(),
                    name: name.to_string(),
                    first: "on_created".into(),
                    second: "on_updated".into(),
                })
            );
        }
    }

    #[test]
    fn a_subscription_declared_twice_is_one_consumer() {
        assert_eq!(
            ops(&[scoped(), scoped()], &[], &[], &nowhere()),
            [ConsumerOp::Create {
                desired: scoped(),
                deliver: DeliverStart::All,
            }]
        );
    }

    #[test]
    fn a_predecessor_the_new_version_still_consumes_is_no_predecessor() {
        let renamed = desired(scoped().name, "dw.default.stocks.created");
        let keeps_old = DesiredConsumer {
            key: key("on_updated", "dw.default.stocks.created"),
            ..legacy()
        };
        assert_eq!(
            ops(
                &[renamed.clone(), keeps_old.clone()],
                &[server_has(&legacy(), 5)],
                &[legacy()],
                &nowhere()
            ),
            [
                ConsumerOp::Create {
                    desired: renamed,
                    deliver: DeliverStart::All,
                },
                keep(&keeps_old),
            ]
        );
    }

    #[test]
    fn a_durable_on_another_stream_is_no_predecessor() {
        let moved = on("ASSETS", RetentionPolicy::Limits, scoped());
        assert_eq!(
            ops(
                std::slice::from_ref(&moved),
                &[server_has(&legacy(), 5)],
                &[legacy()],
                &nowhere()
            ),
            [
                ConsumerOp::Create {
                    desired: moved,
                    deliver: DeliverStart::All,
                },
                ConsumerOp::Delete {
                    stream: STREAM.into(),
                    name: legacy().name.to_string(),
                }
            ]
        );
    }

    #[test]
    fn the_platforms_own_durables_are_told_by_their_metadata() {
        let ours = server_has(&scoped(), 0);
        assert!(ours.is_managed_for("default", "stock-service"));
        assert!(!ours.is_managed_for("preview-pr-1", "stock-service"));
        assert!(!ours.is_managed_for("default", "stock"));
        assert!(!server_has(&bound(), 0).is_managed_for("default", "stock-service"));
        let claims_to_be = ObservedConsumer {
            metadata: BTreeMap::from([
                (METADATA_TENANT.to_string(), "default".to_string()),
                (METADATA_SERVICE.to_string(), "stock-service".to_string()),
            ]),
            ..server_has(&bound(), 0)
        };
        assert!(!claims_to_be.is_managed_for("default", "stock-service"));
    }

    #[test]
    fn a_deploy_reports_each_change() {
        assert_eq!(
            keep(&legacy()).summary(None),
            json!({"op": "keep", "stream": STREAM, "durable": "stock_service_on_created"})
        );
        assert_eq!(
            ConsumerOp::Create {
                desired: legacy(),
                deliver: DeliverStart::All,
            }
            .summary(None),
            json!({
                "op": "create", "stream": STREAM, "durable": "stock_service_on_created",
                "handler": "on_created", "deliver": "all",
            })
        );
        assert_eq!(
            ConsumerOp::Update {
                desired: legacy(),
                changes: vec!["filters", "ack_wait"],
            }
            .summary(None)["changes"],
            json!(["filters", "ack_wait"])
        );
        let handover = |mode| ConsumerOp::Handover {
            from: server_has(&legacy(), 81_233),
            to: scoped(),
            mode,
            retire: Retire::Delete,
        };
        let planned = handover(HandoverMode::CreateThenDelete(
            DeliverStart::ByStartSequence(81_234),
        ));
        assert_eq!(
            planned.summary(None),
            json!({
                "op": "handover", "stream": STREAM, "handler": "on_created",
                "from": "stock_service_on_created",
                "to": "default_stock-service_on_created_1a2b3c4d",
                "mode": "create_then_delete", "start_sequence": 81_234,
            })
        );
        assert_eq!(
            planned.summary(Some(81_240))["start_sequence"],
            json!(81_240),
            "where it started, not where the plan expected it to"
        );
        assert_eq!(
            handover(HandoverMode::DeleteThenCreate).summary(None)["start_sequence"],
            json!(null)
        );
    }

    #[test]
    fn only_a_delete_and_a_predecessor_created_over_go_after_the_activation() {
        let handover = |mode, retire| ConsumerOp::Handover {
            from: server_has(&legacy(), 0),
            to: scoped(),
            mode,
            retire,
        };
        let over = HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(1));
        let legacy_name = legacy().name.to_string();
        for (op, retired) in [
            (
                handover(over, Retire::Delete),
                Some((STREAM, legacy_name.as_str())),
            ),
            (handover(over, Retire::Keep), None),
            (handover(over, Retire::Release), None),
            (
                handover(HandoverMode::DeleteThenCreate, Retire::Delete),
                None,
            ),
            (
                ConsumerOp::Delete {
                    stream: STREAM.into(),
                    name: "x".into(),
                },
                Some((STREAM, "x")),
            ),
            (keep(&legacy()), None),
            (
                ConsumerOp::Release {
                    stream: STREAM.into(),
                    name: "x".into(),
                },
                None,
            ),
        ] {
            assert_eq!(op.retired(), retired, "{op:?}");
        }
    }

    #[test]
    fn every_op_has_its_label() {
        let labels: Vec<&str> = [
            keep(&legacy()),
            ConsumerOp::Create {
                desired: legacy(),
                deliver: DeliverStart::All,
            },
            ConsumerOp::Update {
                desired: legacy(),
                changes: vec!["filters"],
            },
            ConsumerOp::Handover {
                from: server_has(&legacy(), 0),
                to: scoped(),
                mode: HandoverMode::DeleteThenCreate,
                retire: Retire::Delete,
            },
            ConsumerOp::Delete {
                stream: STREAM.into(),
                name: "x".into(),
            },
            ConsumerOp::Release {
                stream: STREAM.into(),
                name: "x".into(),
            },
        ]
        .iter()
        .map(ConsumerOp::label)
        .collect();
        assert_eq!(
            labels,
            ["keep", "create", "update", "handover", "delete", "release"]
        );
        assert_eq!(
            HandoverMode::CreateThenDelete(DeliverStart::New).as_str(),
            "create_then_delete"
        );
        assert_eq!(
            HandoverMode::DeleteThenCreate.as_str(),
            "delete_then_create"
        );
    }
}
