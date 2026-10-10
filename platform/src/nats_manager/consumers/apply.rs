use super::plan::{
    ConsumerOp, DeliverStart, DesiredConsumer, HandoverMode, ObservedConsumer, METADATA_MANAGED,
    METADATA_SERVICE, METADATA_TENANT,
};
use crate::registry::ServiceIdentity;
use async_nats::jetstream::consumer::{self, AckPolicy, DeliverPolicy};
use async_nats::jetstream::context::{ConsumerInfoErrorKind, GetStreamErrorKind};
use async_nats::jetstream::stream::{
    ConsumerCreateStrictErrorKind, ConsumerErrorKind, ConsumerUpdateErrorKind, RetentionPolicy,
    Stream,
};
use async_nats::jetstream::{self, ErrorCode};
use futures_util::TryStreamExt;
use serde::Serialize;
use std::collections::{BTreeSet, HashSet};
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyPhase {
    BeforeActivation,
    AfterActivation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Handover {
    pub stream: String,
    pub from: String,
    pub to: String,
    pub mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApplyError {
    #[error("{what} on stream '{stream}' was refused: {reason}")]
    Refused {
        what: String,
        stream: String,
        reason: String,
    },
    #[error("{what} on stream '{stream}' failed: {reason}")]
    Unreachable {
        what: String,
        stream: String,
        reason: String,
    },
}

impl ApplyError {
    fn of(what: String, stream: &str, refused: bool, reason: String) -> Self {
        let stream = stream.to_string();
        if refused {
            ApplyError::Refused {
                what,
                stream,
                reason,
            }
        } else {
            ApplyError::Unreachable {
                what,
                stream,
                reason,
            }
        }
    }
}

fn filters_of(config: &consumer::Config) -> BTreeSet<String> {
    std::iter::once(&config.filter_subject)
        .chain(&config.filter_subjects)
        .filter(|subject| !subject.is_empty())
        .cloned()
        .collect()
}

pub fn observed(info: &consumer::Info) -> ObservedConsumer {
    ObservedConsumer {
        stream: info.stream_name.clone(),
        name: info.name.clone(),
        filters: filters_of(&info.config),
        ack_wait: info.config.ack_wait,
        max_deliver: info.config.max_deliver,
        max_ack_pending: info.config.max_ack_pending,
        ack_policy: info.config.ack_policy,
        metadata: info
            .config
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        ack_floor_stream_seq: info.ack_floor.stream_sequence,
        delivered: info.delivered.consumer_sequence,
    }
}

fn deliver_policy(start: DeliverStart) -> DeliverPolicy {
    match start {
        DeliverStart::All => DeliverPolicy::All,
        DeliverStart::New => DeliverPolicy::New,
        DeliverStart::Last => DeliverPolicy::Last,
        DeliverStart::LastPerSubject => DeliverPolicy::LastPerSubject,
        DeliverStart::ByStartSequence(start_sequence) => {
            DeliverPolicy::ByStartSequence { start_sequence }
        }
    }
}

fn with_filters(config: consumer::Config, filters: &BTreeSet<String>) -> consumer::Config {
    let mut subjects = filters.iter().cloned();
    match (subjects.next(), filters.len()) {
        (Some(only), 1) => consumer::Config {
            filter_subject: only,
            filter_subjects: Vec::new(),
            ..config
        },
        _ => consumer::Config {
            filter_subject: String::new(),
            filter_subjects: filters.iter().cloned().collect(),
            ..config
        },
    }
}

pub fn created(desired: &DesiredConsumer, start: DeliverStart) -> consumer::Config {
    with_filters(
        consumer::Config {
            durable_name: Some(desired.name.to_string()),
            deliver_policy: deliver_policy(start),
            ack_policy: AckPolicy::Explicit,
            ack_wait: desired.ack_wait.unwrap_or(desired.created_with.ack_wait),
            max_deliver: desired
                .max_deliver
                .unwrap_or(desired.created_with.max_deliver),
            max_ack_pending: desired.max_ack_pending.unwrap_or_default(),
            metadata: desired.metadata.clone().into_iter().collect(),
            ..Default::default()
        },
        &desired.filters,
    )
}

pub fn updated(current: consumer::Config, desired: &DesiredConsumer) -> consumer::Config {
    let metadata = current
        .metadata
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .chain(desired.metadata.clone())
        .collect();
    with_filters(
        consumer::Config {
            ack_wait: desired.ack_wait.unwrap_or(current.ack_wait),
            max_deliver: desired.max_deliver.unwrap_or(current.max_deliver),
            max_ack_pending: desired.max_ack_pending.unwrap_or(current.max_ack_pending),
            metadata,
            ..current
        },
        &desired.filters,
    )
}

fn marked(current: consumer::Config, owner: &ServiceIdentity) -> Option<consumer::Config> {
    let mark = [
        (METADATA_MANAGED, "1"),
        (METADATA_TENANT, owner.tenant_slug.as_str()),
        (METADATA_SERVICE, owner.service_name.as_str()),
    ]
    .map(|(key, value)| (key.to_string(), value.to_string()));
    (!current.metadata.contains_key(METADATA_MANAGED)).then(|| consumer::Config {
        metadata: current
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .chain(mark)
            .collect(),
        ..current
    })
}

async fn mark(stream: &Stream, name: &str, owner: &ServiceIdentity) -> Result<(), ApplyError> {
    match info_of(stream, name)
        .await?
        .and_then(|info| marked(info.config, owner))
    {
        Some(config) => update(stream, config, name).await,
        None => Ok(()),
    }
}

fn consumer_refused(kind: &ConsumerErrorKind) -> bool {
    matches!(kind, ConsumerErrorKind::JetStream(_))
}

fn consumer_missing(kind: &ConsumerErrorKind) -> bool {
    matches!(kind, ConsumerErrorKind::JetStream(e) if e.error_code() == ErrorCode::CONSUMER_NOT_FOUND)
}

async fn stream_of(js: &jetstream::Context, name: &str) -> Result<Option<Stream>, ApplyError> {
    match js.get_stream(name).await {
        Ok(stream) => Ok(Some(stream)),
        Err(e) if crate::nats_manager::replicas::is_stream_not_found(e.kind()) => Ok(None),
        Err(e) => Err(ApplyError::of(
            "looking the stream up".into(),
            name,
            matches!(e.kind(), GetStreamErrorKind::JetStream(_)),
            e.to_string(),
        )),
    }
}

async fn existing_stream(js: &jetstream::Context, name: &str) -> Result<Stream, ApplyError> {
    stream_of(js, name)
        .await?
        .ok_or_else(|| ApplyError::Refused {
            what: "looking the stream up".into(),
            stream: name.to_string(),
            reason: "the stream does not exist".into(),
        })
}

pub async fn info_of(stream: &Stream, name: &str) -> Result<Option<consumer::Info>, ApplyError> {
    match stream.consumer_info(name).await {
        Ok(info) => Ok(Some(info)),
        Err(e) if e.kind() == ConsumerInfoErrorKind::NotFound => Ok(None),
        Err(e) => Err(ApplyError::of(
            format!("reading durable '{name}'"),
            &stream.cached_info().config.name,
            matches!(
                e.kind(),
                ConsumerInfoErrorKind::JetStream(_) | ConsumerInfoErrorKind::StreamNotFound
            ),
            e.to_string(),
        )),
    }
}

pub async fn observe(
    js: &jetstream::Context,
    streams: impl IntoIterator<Item = &str>,
    ours: impl Fn(&ObservedConsumer) -> bool,
) -> Result<Vec<ObservedConsumer>, ApplyError> {
    let mut found = Vec::new();
    for name in streams.into_iter().collect::<BTreeSet<_>>() {
        let Some(stream) = stream_of(js, name).await? else {
            continue;
        };
        let infos: Vec<consumer::Info> = stream.consumers().try_collect().await.map_err(|e| {
            ApplyError::of("listing the durables".into(), name, false, e.to_string())
        })?;
        found.extend(infos.iter().map(observed).filter(|consumer| ours(consumer)));
    }
    Ok(found)
}

pub async fn clear_for(stream: &Stream, desired: &DesiredConsumer) -> Result<(), ApplyError> {
    if !(desired.kind.is_timetable() && desired.retention == RetentionPolicy::WorkQueue) {
        return Ok(());
    }
    for filter in &desired.filters {
        stream.purge().filter(filter).await.map_err(|e| {
            ApplyError::of(
                format!("clearing '{filter}' for durable '{}'", desired.name),
                &desired.stream,
                false,
                e.to_string(),
            )
        })?;
    }
    Ok(())
}

async fn create(
    stream: &Stream,
    desired: &DesiredConsumer,
    start: DeliverStart,
) -> Result<(), ApplyError> {
    clear_for(stream, desired).await?;
    stream
        .create_consumer_strict(created(desired, start))
        .await
        .map(drop)
        .map_err(|e| {
            ApplyError::of(
                format!("creating durable '{}'", desired.name),
                &desired.stream,
                matches!(
                    e.kind(),
                    ConsumerCreateStrictErrorKind::JetStream(_)
                        | ConsumerCreateStrictErrorKind::AlreadyExists
                ),
                e.to_string(),
            )
        })
}

pub async fn restore(stream: &Stream, desired: &DesiredConsumer) -> Result<bool, ApplyError> {
    match info_of(stream, desired.name.as_str()).await? {
        Some(_) => Ok(false),
        None => create(stream, desired, desired.deliver_policy.into())
            .await
            .map(|()| true),
    }
}

async fn update(stream: &Stream, config: consumer::Config, name: &str) -> Result<(), ApplyError> {
    let stream_name = stream.cached_info().config.name.clone();
    stream.update_consumer(config).await.map(drop).map_err(|e| {
        ApplyError::of(
            format!("updating durable '{name}'"),
            &stream_name,
            matches!(
                e.kind(),
                ConsumerUpdateErrorKind::JetStream(_) | ConsumerUpdateErrorKind::DoesNotExist
            ),
            e.to_string(),
        )
    })
}

async fn update_to(stream: &Stream, desired: &DesiredConsumer) -> Result<(), ApplyError> {
    let name = desired.name.as_str();
    match info_of(stream, name).await? {
        Some(info) => update(stream, updated(info.config, desired), name).await,
        None => create(stream, desired, desired.deliver_policy.into()).await,
    }
}

async fn delete(stream: &Stream, name: &str) -> Result<(), ApplyError> {
    match stream.delete_consumer(name).await {
        Ok(_) => Ok(()),
        Err(e) if consumer_missing(&e.kind()) => Ok(()),
        Err(e) => Err(ApplyError::of(
            format!("deleting durable '{name}'"),
            &stream.cached_info().config.name,
            consumer_refused(&e.kind()),
            e.to_string(),
        )),
    }
}

async fn clear_unused(stream: &Stream, to: &DesiredConsumer) -> Result<(), ApplyError> {
    if to.name.is_bound() {
        return Ok(());
    }
    match info_of(stream, to.name.as_str()).await? {
        Some(left) if left.delivered.consumer_sequence == 0 => {
            delete(stream, to.name.as_str()).await
        }
        _ => Ok(()),
    }
}

async fn hand_over(
    stream: &Stream,
    from: &ObservedConsumer,
    to: &DesiredConsumer,
    mode: HandoverMode,
) -> Result<Handover, ApplyError> {
    clear_unused(stream, to).await?;
    let start = match mode {
        HandoverMode::DeleteThenCreate => {
            delete(stream, &from.name).await?;
            DeliverStart::All
        }
        HandoverMode::CreateThenDelete(DeliverStart::ByStartSequence(planned)) => {
            DeliverStart::ByStartSequence(
                info_of(stream, &from.name).await?.map_or(planned, |info| {
                    info.ack_floor.stream_sequence.saturating_add(1)
                }),
            )
        }
        HandoverMode::CreateThenDelete(start) => start,
    };
    create(stream, to, start).await?;
    let handed = Handover {
        stream: to.stream.clone(),
        from: from.name.clone(),
        to: to.name.to_string(),
        mode: mode.as_str(),
        start_sequence: match start {
            DeliverStart::ByStartSequence(sequence) => Some(sequence),
            _ => None,
        },
    };
    info!(
        event = "consumer_handover",
        stream = %handed.stream,
        from = %handed.from,
        to = %handed.to,
        mode = handed.mode,
        start_sequence = ?handed.start_sequence,
        "A durable handed over to its successor"
    );
    Ok(handed)
}

fn count(op: &ConsumerOp) {
    metrics::counter!("cufflink_consumer_ops_total", "op" => op.label()).increment(1);
}

async fn before(
    js: &jetstream::Context,
    op: &ConsumerOp,
    owner: &ServiceIdentity,
) -> Result<Option<Handover>, ApplyError> {
    count(op);
    if let Some((stream, name)) = op.retired() {
        if let Some(stream) = stream_of(js, stream).await? {
            mark(&stream, name, owner).await?;
        }
    }
    match op {
        ConsumerOp::Create { desired, deliver } => {
            let stream = existing_stream(js, &desired.stream).await?;
            create(&stream, desired, *deliver).await.map(|()| None)
        }
        ConsumerOp::Update { desired, .. } => {
            let stream = existing_stream(js, &desired.stream).await?;
            update_to(&stream, desired).await.map(|()| None)
        }
        ConsumerOp::Handover { from, to, mode, .. } => {
            let stream = existing_stream(js, &to.stream).await?;
            hand_over(&stream, from, to, *mode).await.map(Some)
        }
        ConsumerOp::Keep { .. } | ConsumerOp::Delete { .. } | ConsumerOp::Release { .. } => {
            Ok(None)
        }
    }
}

async fn after(js: &jetstream::Context, (stream, name): (&str, &str)) -> Result<(), ApplyError> {
    match stream_of(js, stream).await? {
        Some(stream) => delete(&stream, name).await,
        None => Ok(()),
    }
}

pub async fn apply(
    js: &jetstream::Context,
    ops: &[ConsumerOp],
    phase: ApplyPhase,
    owner: &ServiceIdentity,
) -> Result<Vec<Handover>, ApplyError> {
    match phase {
        ApplyPhase::BeforeActivation => {
            let mut handovers = Vec::new();
            for op in ops {
                handovers.extend(before(js, op, owner).await?);
            }
            Ok(handovers)
        }
        ApplyPhase::AfterActivation => retire(js, ops, &HashSet::new()).await.map(|()| Vec::new()),
    }
}

pub async fn retire(
    js: &jetstream::Context,
    ops: &[ConsumerOp],
    wanted: &HashSet<(String, String)>,
) -> Result<(), ApplyError> {
    let retired = ops
        .iter()
        .filter_map(ConsumerOp::retired)
        .filter(|(stream, name)| !wanted.contains(&(stream.to_string(), name.to_string())))
        .fold(Vec::new(), |mut retired: Vec<(&str, &str)>, durable| {
            if !retired.contains(&durable) {
                retired.push(durable);
            }
            retired
        });
    let mut first = None;
    for durable in retired {
        if let Err(e) = after(js, durable).await {
            warn!(error = %e, "A durable its service no longer consumes was not deleted; the next deploy deletes it");
            first.get_or_insert(e);
        }
    }
    first.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_manager::consumers::naming::DurableName;
    use crate::nats_manager::consumers::plan::{ConsumerKind, CreatedWith};
    use crate::nats_manager::consumers::LoopKey;
    use async_nats::jetstream::stream::RetentionPolicy;
    use cufflink_types::DeliverPolicySpec;
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    fn desired(name: DurableName, filters: &[&str]) -> DesiredConsumer {
        DesiredConsumer {
            key: LoopKey {
                tenant_slug: "default".into(),
                service_name: "stock-service".into(),
                handler: "on_created".into(),
                subject: filters[0].into(),
            },
            stream: "DW_DEFAULT".into(),
            retention: RetentionPolicy::Interest,
            filters: filters.iter().map(|f| f.to_string()).collect(),
            ack_wait: Some(Duration::from_secs(30)),
            max_deliver: Some(-1),
            max_ack_pending: None,
            created_with: CreatedWith {
                ack_wait: Duration::from_secs(30),
                max_deliver: -1,
            },
            deliver_policy: DeliverPolicySpec::All,
            kind: ConsumerKind::Subscription,
            metadata: BTreeMap::from([
                ("cufflink.managed".to_string(), "1".to_string()),
                ("cufflink.tenant".to_string(), "default".to_string()),
            ]),
            name,
        }
    }

    fn legacy() -> DesiredConsumer {
        desired(
            DurableName::Legacy("stock_service_on_created".into()),
            &["dw.default.stocks.created"],
        )
    }

    #[test]
    fn a_created_durable_is_explicit_and_carries_what_was_declared() {
        assert_eq!(
            created(&legacy(), DeliverStart::All),
            consumer::Config {
                durable_name: Some("stock_service_on_created".into()),
                filter_subject: "dw.default.stocks.created".into(),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_deliver: -1,
                metadata: HashMap::from([
                    ("cufflink.managed".to_string(), "1".to_string()),
                    ("cufflink.tenant".to_string(), "default".to_string()),
                ]),
                ..Default::default()
            }
        );
        let bound = DesiredConsumer {
            ack_wait: None,
            max_deliver: None,
            max_ack_pending: Some(1000),
            created_with: CreatedWith {
                ack_wait: Duration::from_secs(45),
                max_deliver: 5,
            },
            metadata: BTreeMap::new(),
            ..desired(
                DurableName::Named("loans-service-book-consumer".into()),
                &["books.due_date.set", "books.deleted.loans"],
            )
        };
        assert_eq!(
            created(&bound, DeliverStart::ByStartSequence(81_234)),
            consumer::Config {
                durable_name: Some("loans-service-book-consumer".into()),
                filter_subjects: vec!["books.deleted.loans".into(), "books.due_date.set".into()],
                deliver_policy: DeliverPolicy::ByStartSequence {
                    start_sequence: 81_234
                },
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(45),
                max_deliver: 5,
                max_ack_pending: 1000,
                ..Default::default()
            }
        );
    }

    #[test]
    fn an_update_changes_only_what_is_held() {
        let theirs = consumer::Config {
            durable_name: Some("loans-service-book-consumer".into()),
            name: Some("loans-service-book-consumer".into()),
            description: Some("the legacy consumer".into()),
            filter_subjects: vec!["books.due_date.set".into()],
            deliver_policy: DeliverPolicy::New,
            ack_wait: Duration::from_secs(30),
            max_deliver: 3,
            max_ack_pending: 1000,
            max_waiting: 512,
            backoff: vec![Duration::from_secs(1)],
            metadata: HashMap::from([("owner".to_string(), "harbor".to_string())]),
            ..Default::default()
        };
        let wants = DesiredConsumer {
            ack_wait: None,
            max_deliver: Some(5),
            max_ack_pending: None,
            metadata: BTreeMap::new(),
            ..desired(
                DurableName::Named("loans-service-book-consumer".into()),
                &["books.due_date.set", "books.deleted.loans"],
            )
        };
        assert_eq!(
            updated(theirs.clone(), &wants),
            consumer::Config {
                max_deliver: 5,
                filter_subjects: vec!["books.deleted.loans".into(), "books.due_date.set".into()],
                ..theirs.clone()
            }
        );
        let one_filter = DesiredConsumer {
            filters: ["books.deleted.loans".to_string()].into(),
            ..wants
        };
        let narrowed = updated(theirs, &one_filter);
        assert_eq!(narrowed.filter_subject, "books.deleted.loans");
        assert!(narrowed.filter_subjects.is_empty());
    }

    #[test]
    fn an_update_of_a_managed_durable_adds_its_metadata_to_the_servers() {
        let current = consumer::Config {
            durable_name: Some("stock_service_on_created".into()),
            filter_subject: "dw.default.stocks.created".into(),
            metadata: HashMap::from([("_nats.ver".to_string(), "2.15.0".to_string())]),
            ..Default::default()
        };
        let config = updated(current, &legacy());
        assert_eq!(config.metadata.len(), 3);
        assert_eq!(config.metadata["_nats.ver"], "2.15.0");
        assert_eq!(config.metadata["cufflink.managed"], "1");
    }

    #[test]
    fn a_durable_to_delete_is_marked_once() {
        let owner = ServiceIdentity {
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "default".into(),
            service_id: uuid::Uuid::nil(),
            service_name: "stock-service".into(),
            schema: "stock_service".into(),
        };
        let pre_r3 = consumer::Config {
            durable_name: Some("stock_service_on_created".into()),
            filter_subject: "dw.default.stocks.created".into(),
            ..Default::default()
        };
        let config = marked(pre_r3.clone(), &owner).expect("unmarked");
        assert!(observed_with(&config).is_managed_for("default", "stock-service"));
        assert_eq!(
            consumer::Config {
                metadata: HashMap::new(),
                ..config.clone()
            },
            pre_r3,
            "nothing else of it changes"
        );
        assert_eq!(marked(config, &owner), None);
    }

    fn info(config: &consumer::Config) -> consumer::Info {
        serde_json::from_value(serde_json::json!({
            "stream_name": "BOOKS",
            "name": "loans-service-book-consumer",
            "created": "2026-09-28T10:00:00Z",
            "config": config,
            "delivered": {"consumer_seq": 9, "stream_seq": 12},
            "ack_floor": {"consumer_seq": 4, "stream_seq": 4},
            "num_ack_pending": 2,
            "num_redelivered": 0,
            "num_waiting": 0,
            "num_pending": 6,
        }))
        .expect("info")
    }

    fn observed_with(config: &consumer::Config) -> ObservedConsumer {
        observed(&info(config))
    }

    #[test]
    fn what_the_server_reports_is_read_whichever_way_it_filters() {
        let one = observed_with(&consumer::Config {
            durable_name: Some("loans-service-book-consumer".into()),
            filter_subject: "books.due_date.set".into(),
            ack_wait: Duration::from_secs(30),
            max_deliver: 3,
            max_ack_pending: 1000,
            ..Default::default()
        });
        assert_eq!(
            one,
            ObservedConsumer {
                stream: "BOOKS".into(),
                name: "loans-service-book-consumer".into(),
                filters: ["books.due_date.set".to_string()].into(),
                ack_wait: Duration::from_secs(30),
                max_deliver: 3,
                max_ack_pending: 1000,
                ack_policy: AckPolicy::Explicit,
                metadata: BTreeMap::new(),
                ack_floor_stream_seq: 4,
                delivered: 9,
            }
        );
        let several = observed_with(&consumer::Config {
            filter_subjects: vec!["books.deleted.loans".into(), "books.created".into()],
            ..Default::default()
        });
        assert_eq!(
            several.filters,
            ["books.created", "books.deleted.loans"]
                .map(String::from)
                .into()
        );
    }
}
