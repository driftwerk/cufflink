use cufflink_testkit as common;

use async_nats::jetstream::consumer::{self, pull, AckPolicy, DeliverPolicy};
use async_nats::jetstream::{self, stream};
use common::until;
use cufflink_platform::nats_manager::consumers::apply::{self, ApplyPhase, Handover};
use cufflink_platform::nats_manager::consumers::desired::{desired_consumers, Defaults};
use cufflink_platform::nats_manager::consumers::plan::{
    plan_consumers, ConsumerKind, ConsumerOp, ConsumerPlan, ConsumerPlanError, DesiredConsumer,
};
use cufflink_platform::nats_manager::delivery::{Decision, Outcome};
use cufflink_platform::nats_manager::dlq::DeadLetter;
use cufflink_platform::nats_manager::events;
use cufflink_platform::nats_manager::pump::{
    self, MessageMeta, PumpConfig, PumpExit, Work, WorkLabel,
};
use cufflink_platform::nats_manager::stream_index::StreamSnapshot;
use cufflink_platform::registry::ServiceIdentity;
use cufflink_platform::shutdown::Shutdown;
use cufflink_types::ServiceManifest;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const ACK_WAIT: Duration = Duration::from_secs(30);
const SOON: Duration = Duration::from_secs(60);

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12]).to_uppercase()
}

async fn jetstream(nats_url: &str) -> jetstream::Context {
    jetstream::new(async_nats::connect(nats_url).await.expect("nats"))
}

fn identity(tenant: &str) -> ServiceIdentity {
    ServiceIdentity {
        tenant_id: Uuid::new_v4(),
        tenant_slug: tenant.into(),
        service_id: Uuid::new_v4(),
        service_name: "stock-service".into(),
        schema: "stock_service".into(),
    }
}

async fn stream_of(
    js: &jetstream::Context,
    name: &str,
    retention: stream::RetentionPolicy,
) -> (stream::Stream, String) {
    let subject = format!("{}.created", name.to_lowercase());
    let stream = js
        .create_stream(stream::Config {
            name: name.to_string(),
            subjects: vec![format!("{}.>", name.to_lowercase())],
            retention,
            storage: stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .expect("stream");
    (stream, subject)
}

fn manifest(subscription: Value, naming: &str) -> ServiceManifest {
    serde_json::from_value(json!({
        "name": "stock-service",
        "version": "0.1.0",
        "tables": [],
        "consumers": naming,
        "subscriptions": [subscription],
    }))
    .expect("manifest")
}

fn subscription(subject: &str, stream: &str) -> Value {
    json!({"subject": subject, "handler": "on_created", "handler_type": "wasm", "stream": stream})
}

async fn wanted(
    js: &jetstream::Context,
    identity: &ServiceIdentity,
    manifest: &ServiceManifest,
) -> Vec<DesiredConsumer> {
    let streams = StreamSnapshot::read(js).await.expect("streams");
    desired_consumers(
        identity,
        manifest,
        &manifest.subscriptions,
        &streams,
        Defaults { ack_wait: ACK_WAIT },
    )
    .expect("placed")
    .into_iter()
    .map(|wanted| wanted.consumer)
    .collect()
}

async fn plan(
    js: &jetstream::Context,
    identity: &ServiceIdentity,
    desired: &[DesiredConsumer],
    prior: &[DesiredConsumer],
    elsewhere: &HashSet<(String, String)>,
) -> Result<ConsumerPlan, ConsumerPlanError> {
    let named: BTreeSet<(&str, &str)> = desired
        .iter()
        .chain(prior)
        .map(|consumer| (consumer.stream.as_str(), consumer.name.as_str()))
        .collect();
    let observed = apply::observe(
        js,
        desired
            .iter()
            .chain(prior)
            .map(|consumer| consumer.stream.as_str()),
        |consumer| {
            consumer.is_managed_for(&identity.tenant_slug, &identity.service_name)
                || named.contains(&(consumer.stream.as_str(), consumer.name.as_str()))
        },
    )
    .await
    .expect("observed");
    plan_consumers(desired, &observed, prior, elsewhere)
}

async fn before(
    js: &jetstream::Context,
    identity: &ServiceIdentity,
    plan: &ConsumerPlan,
) -> Vec<Handover> {
    apply::apply(js, &plan.ops, ApplyPhase::BeforeActivation, identity)
        .await
        .expect("applied before the activation")
}

async fn after(js: &jetstream::Context, identity: &ServiceIdentity, plan: &ConsumerPlan) {
    apply::apply(js, &plan.ops, ApplyPhase::AfterActivation, identity)
        .await
        .expect("applied after the activation");
}

async fn publish(js: &jetstream::Context, subject: &str, payload: String) {
    js.publish(subject.to_string(), payload.into())
        .await
        .expect("publish")
        .await
        .expect("stored");
}

async fn info(stream: &stream::Stream, name: &str) -> Option<consumer::Info> {
    apply::info_of(stream, name).await.expect("info")
}

type Seen = Arc<Mutex<Vec<(String, u64)>>>;

struct Recorder {
    seen: Seen,
    takes: Duration,
}

impl Work for Recorder {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "subscription",
            subscription: None,
        }
    }

    fn handle(
        &self,
        _msg: Arc<jetstream::Message>,
        meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let (seen, takes) = (Arc::clone(&self.seen), self.takes);
        async move {
            tokio::time::sleep(takes).await;
            seen.lock()
                .expect("seen")
                .push((meta.consumer, meta.stream_sequence));
            Outcome::Ok
        }
        .boxed()
    }

    fn decide(&self, _outcome: &Outcome, _meta: &MessageMeta) -> Decision {
        Decision::Ack
    }

    fn dead_letter<'a>(
        &'a self,
        _msg: &'a jetstream::Message,
        _meta: &'a MessageMeta,
        _reason: &'a str,
    ) -> BoxFuture<'a, Option<DeadLetter>> {
        std::future::ready(None).boxed()
    }
}

struct Loop {
    stop: CancellationToken,
    running: tokio::task::JoinHandle<PumpExit>,
}

impl Loop {
    async fn start(
        js: &jetstream::Context,
        stream: &stream::Stream,
        durable: &str,
        seen: &Seen,
        shutdown: &Shutdown,
    ) -> Self {
        let consumer = stream
            .get_consumer::<pull::Config>(durable)
            .await
            .expect("durable");
        let stop = CancellationToken::new();
        let running = tokio::spawn(pump::run(
            consumer,
            PumpConfig {
                fetch_expires: Duration::from_secs(1),
                ..PumpConfig::sequential(ACK_WAIT)
            },
            Arc::new(Recorder {
                seen: Arc::clone(seen),
                takes: Duration::from_millis(2),
            }),
            Arc::new(js.clone()),
            stop.clone(),
            shutdown.clone(),
        ));
        Self { stop, running }
    }

    async fn stop(self) {
        self.stop.cancel();
        assert_eq!(self.running.await.expect("pump"), PumpExit::Stopped);
    }
}

async fn drain(stream: &stream::Stream, durable: &str, max: usize) -> Vec<u64> {
    let consumer = stream
        .get_consumer::<pull::Config>(durable)
        .await
        .expect("durable");
    let mut messages = consumer
        .fetch()
        .max_messages(max)
        .expires(Duration::from_secs(1))
        .messages()
        .await
        .expect("fetch");
    let mut sequences = Vec::new();
    while let Some(message) = messages.next().await {
        let message = message.expect("message");
        sequences.push(message.info().expect("info").stream_sequence);
        message.double_ack().await.expect("ack");
    }
    sequences
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_rename_loses_nothing() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    const MESSAGES: u64 = 300;
    let js = jetstream(nats_url).await;
    let name = unique("RENAME");
    let (stream, subject) = stream_of(&js, &name, stream::RetentionPolicy::Interest).await;
    let identity = identity("it-rename");
    let legacy = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "legacy"),
    )
    .await;
    let scoped = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "scoped"),
    )
    .await;
    assert_eq!(legacy[0].name.as_str(), "stock_service_on_created");
    assert_ne!(scoped[0].name.as_str(), legacy[0].name.as_str());

    let first = plan(&js, &identity, &legacy, &[], &HashSet::new())
        .await
        .expect("plan");
    assert!(matches!(first.ops[..], [ConsumerOp::Create { .. }]));
    before(&js, &identity, &first).await;

    let seen: Seen = Arc::default();
    let shutdown = Shutdown::new(Duration::from_secs(30));
    let old = Loop::start(&js, &stream, legacy[0].name.as_str(), &seen, &shutdown).await;
    let publisher = tokio::spawn({
        let (js, subject) = (js.clone(), subject.clone());
        async move {
            let mut every = tokio::time::interval(Duration::from_millis(20));
            for n in 1..=MESSAGES {
                every.tick().await;
                publish(&js, &subject, format!(r#"{{"n":{n}}}"#)).await;
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    let rename = plan(&js, &identity, &scoped, &legacy, &HashSet::new())
        .await
        .expect("plan");
    assert!(
        matches!(rename.ops[..], [ConsumerOp::Handover { .. }]),
        "{rename:?}"
    );
    let handed = before(&js, &identity, &rename).await;
    assert_eq!(handed.len(), 1);
    let start = handed[0].start_sequence.expect("a start sequence");
    assert!(start > 1 && start <= MESSAGES, "{handed:?}");
    assert_eq!(handed[0].mode, "create_then_delete");
    assert!(
        info(&stream, legacy[0].name.as_str()).await.is_some(),
        "the predecessor stays until the new version is active"
    );

    let new = Loop::start(&js, &stream, scoped[0].name.as_str(), &seen, &shutdown).await;
    old.stop().await;
    after(&js, &identity, &rename).await;
    assert_eq!(info(&stream, legacy[0].name.as_str()).await, None);

    publisher.await.expect("publisher");
    until("the successor caught up", SOON, async || {
        info(&stream, scoped[0].name.as_str())
            .await
            .is_some_and(|info| info.num_pending == 0 && info.num_ack_pending == 0)
    })
    .await
    .expect("in time");
    new.stop().await;

    let seen = seen.lock().expect("seen").clone();
    let sequences: BTreeSet<u64> = seen.iter().map(|(_, sequence)| *sequence).collect();
    assert_eq!(
        sequences,
        (1..=MESSAGES).collect::<BTreeSet<u64>>(),
        "every message was handled at least once"
    );
    let duplicates = seen.len() - sequences.len();
    assert!(
        duplicates <= 50,
        "{duplicates} messages were handled twice: more than were in flight at the handover"
    );
    let by_successor: Vec<u64> = seen
        .iter()
        .filter(|(consumer, _)| consumer == scoped[0].name.as_str())
        .map(|(_, sequence)| *sequence)
        .collect();
    assert!(
        by_successor
            .iter()
            .min()
            .is_some_and(|first| *first >= start),
        "the successor starts where its predecessor's acknowledgements stopped ({start}), or \
         past what the predecessor acknowledged since: {by_successor:?}"
    );
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn a_shared_legacy_durable_hands_over_to_each_subjects_successor() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("SHARED");
    let (stream, created) = stream_of(&js, &name, stream::RetentionPolicy::Limits).await;
    let updated = format!("{}.updated", name.to_lowercase());
    let identity = identity("it-shared");
    let on_both = |naming: &str| -> ServiceManifest {
        serde_json::from_value(json!({
            "name": "stock-service",
            "version": "0.1.0",
            "tables": [],
            "consumers": naming,
            "subscriptions": [subscription(&created, &name), subscription(&updated, &name)],
        }))
        .expect("manifest")
    };
    let legacy = wanted(&js, &identity, &on_both("legacy")).await;
    let scoped = wanted(&js, &identity, &on_both("scoped")).await;
    assert_eq!(legacy[0].name, legacy[1].name);
    stream
        .create_consumer(pull::Config {
            durable_name: Some(legacy[0].name.to_string()),
            filter_subject: updated.clone(),
            ..Default::default()
        })
        .await
        .expect("the durable as v0.27.0 left it: filtering on the last subject");
    for subject in [&created, &updated, &created, &updated] {
        publish(&js, subject, "before".into()).await;
    }
    assert_eq!(drain(&stream, legacy[0].name.as_str(), 10).await, [2, 4]);
    until("the floor is 4", SOON, async || {
        info(&stream, legacy[0].name.as_str())
            .await
            .is_some_and(|info| info.ack_floor.stream_sequence == 4)
    })
    .await
    .expect("in time");
    publish(&js, &created, "after".into()).await;
    publish(&js, &updated, "after".into()).await;

    let rename = plan(&js, &identity, &scoped, &legacy, &HashSet::new())
        .await
        .expect("plan");
    let handed = before(&js, &identity, &rename).await;
    assert_eq!(
        handed
            .iter()
            .map(|handed| (
                handed.from.as_str(),
                handed.to.as_str(),
                handed.start_sequence
            ))
            .collect::<Vec<_>>(),
        scoped
            .iter()
            .map(|to| (legacy[0].name.as_str(), to.name.as_str(), Some(5)))
            .collect::<Vec<_>>(),
        "{rename:?}"
    );
    after(&js, &identity, &rename).await;
    assert_eq!(info(&stream, legacy[0].name.as_str()).await, None);
    for (successor, sequence) in scoped.iter().zip([5, 6]) {
        assert_eq!(
            drain(&stream, successor.name.as_str(), 10).await,
            [sequence],
            "{}",
            successor.key.subject
        );
    }
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn a_durable_wanted_again_before_the_deletes_stays() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("READDED");
    let (stream, subject) = stream_of(&js, &name, stream::RetentionPolicy::Interest).await;
    let identity = identity("it-readded");
    let consumed = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "legacy"),
    )
    .await;
    let first = plan(&js, &identity, &consumed, &[], &HashSet::new())
        .await
        .expect("plan");
    before(&js, &identity, &first).await;
    let removal = plan(&js, &identity, &[], &consumed, &HashSet::new())
        .await
        .expect("plan");
    let durable = (name.clone(), consumed[0].name.to_string());
    assert_eq!(
        removal.ops,
        [ConsumerOp::Delete {
            stream: durable.0.clone(),
            name: durable.1.clone(),
        }]
    );

    apply::retire(&js, &removal.ops, &HashSet::from([durable.clone()]))
        .await
        .expect("retired");
    assert!(info(&stream, &durable.1).await.is_some());
    apply::retire(&js, &removal.ops, &HashSet::new())
        .await
        .expect("retired");
    assert_eq!(info(&stream, &durable.1).await, None);
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn a_successor_a_failed_deploy_left_starts_where_its_predecessor_stops_now() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("RETRIED");
    let (stream, subject) = stream_of(&js, &name, stream::RetentionPolicy::Limits).await;
    let identity = identity("it-retried");
    let legacy = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "legacy"),
    )
    .await;
    let scoped = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "scoped"),
    )
    .await;
    let first = plan(&js, &identity, &legacy, &[], &HashSet::new())
        .await
        .expect("plan");
    before(&js, &identity, &first).await;
    for n in 1..=3 {
        publish(&js, &subject, format!(r#"{{"n":{n}}}"#)).await;
    }
    assert_eq!(drain(&stream, legacy[0].name.as_str(), 10).await, [1, 2, 3]);

    let failed = plan(&js, &identity, &scoped, &legacy, &HashSet::new())
        .await
        .expect("plan");
    assert_eq!(
        before(&js, &identity, &failed).await[0].start_sequence,
        Some(4)
    );
    for n in 4..=6 {
        publish(&js, &subject, format!(r#"{{"n":{n}}}"#)).await;
    }
    assert_eq!(
        drain(&stream, legacy[0].name.as_str(), 10).await,
        [4, 5, 6],
        "the version that stayed active went on"
    );
    until("the floor is 6", SOON, async || {
        info(&stream, legacy[0].name.as_str())
            .await
            .is_some_and(|info| info.ack_floor.stream_sequence == 6)
    })
    .await
    .expect("in time");

    let retried = plan(&js, &identity, &scoped, &legacy, &HashSet::new())
        .await
        .expect("plan");
    assert!(
        matches!(retried.ops[..], [ConsumerOp::Handover { .. }]),
        "{retried:?}"
    );
    assert_eq!(
        before(&js, &identity, &retried).await[0].start_sequence,
        Some(7)
    );
    after(&js, &identity, &retried).await;
    publish(&js, &subject, r#"{"n":7}"#.into()).await;
    assert_eq!(drain(&stream, scoped[0].name.as_str(), 10).await, [7]);
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn workqueue_handover_keeps_unacked() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("QUEUE");
    let (stream, subject) = stream_of(&js, &name, stream::RetentionPolicy::WorkQueue).await;
    let identity = identity("it-queue");
    let legacy = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "legacy"),
    )
    .await;
    let scoped = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "scoped"),
    )
    .await;
    before(
        &js,
        &identity,
        &plan(&js, &identity, &legacy, &[], &HashSet::new())
            .await
            .expect("plan"),
    )
    .await;
    for n in 1..=25 {
        publish(&js, &subject, format!(r#"{{"n":{n}}}"#)).await;
    }
    assert_eq!(
        drain(&stream, legacy[0].name.as_str(), 5).await,
        [1, 2, 3, 4, 5]
    );
    let in_flight = stream
        .get_consumer::<pull::Config>(legacy[0].name.as_str())
        .await
        .expect("durable")
        .fetch()
        .max_messages(3)
        .expires(Duration::from_secs(1))
        .messages()
        .await
        .expect("fetch")
        .collect::<Vec<_>>()
        .await;
    assert_eq!(in_flight.len(), 3, "delivered, never acknowledged");

    let rename = plan(&js, &identity, &scoped, &legacy, &HashSet::new())
        .await
        .expect("plan");
    let handed = before(&js, &identity, &rename).await;
    assert_eq!(
        handed,
        [Handover {
            stream: name.clone(),
            from: legacy[0].name.to_string(),
            to: scoped[0].name.to_string(),
            mode: "delete_then_create",
            start_sequence: None,
        }]
    );
    assert_eq!(info(&stream, legacy[0].name.as_str()).await, None);
    after(&js, &identity, &rename).await;

    assert_eq!(
        drain(&stream, scoped[0].name.as_str(), 50).await,
        (6..=25).collect::<Vec<u64>>(),
        "the twenty the predecessor had not acknowledged, those in flight included"
    );
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn shared_tick_durable_kept_for_other_tenant() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("TICKS");
    let (stream, subject) = stream_of(&js, &name, stream::RetentionPolicy::Interest).await;
    let (default, preview) = (identity("default"), identity("preview-pr-1"));
    let of = |naming: &'static str| manifest(subscription(&subject, &name), naming);
    let ticking = |consumers: Vec<DesiredConsumer>| -> Vec<DesiredConsumer> {
        consumers
            .into_iter()
            .map(|consumer| DesiredConsumer {
                kind: ConsumerKind::Tick,
                ..consumer
            })
            .collect()
    };
    let legacy = ticking(wanted(&js, &default, &of("legacy")).await);
    let scoped = ticking(wanted(&js, &default, &of("scoped")).await);
    assert_eq!(
        wanted(&js, &preview, &of("legacy")).await[0].name,
        legacy[0].name,
        "a legacy name is the same for every tenant"
    );
    stream
        .create_consumer(pull::Config {
            durable_name: Some(legacy[0].name.to_string()),
            filter_subject: subject.clone(),
            ..Default::default()
        })
        .await
        .expect("the durable both tenants have consumed since the release before");
    publish(&js, &subject, r#"{"tick":1}"#.into()).await;

    let elsewhere = HashSet::from([(name.clone(), legacy[0].name.to_string())]);
    let rename = plan(&js, &default, &scoped, &legacy, &elsewhere)
        .await
        .expect("plan");
    before(&js, &default, &rename).await;
    after(&js, &default, &rename).await;

    let kept = info(&stream, legacy[0].name.as_str())
        .await
        .expect("the durable the preview consumes is kept");
    assert!(
        !kept
            .config
            .metadata
            .keys()
            .any(|key| key.starts_with("cufflink.")),
        "and not marked as this service's to delete: {:?}",
        kept.config.metadata
    );
    publish(&js, &subject, r#"{"tick":2}"#.into()).await;
    assert_eq!(drain(&stream, legacy[0].name.as_str(), 5).await, [1, 2]);
    assert_eq!(
        drain(&stream, scoped[0].name.as_str(), 5).await,
        [2],
        "a successor on a shared stream starts at what is published next"
    );

    let gone = plan(&js, &default, &[], &scoped, &elsewhere)
        .await
        .expect("plan");
    before(&js, &default, &gone).await;
    after(&js, &default, &gone).await;
    assert_eq!(info(&stream, scoped[0].name.as_str()).await, None);
    assert!(info(&stream, legacy[0].name.as_str()).await.is_some());
    js.delete_stream(&name).await.expect("delete stream");
}

fn legacy_durable(subjects: &[String]) -> pull::Config {
    pull::Config {
        name: Some("loan-service-book-consumer".into()),
        durable_name: Some("loan-service-book-consumer".into()),
        filter_subjects: subjects.to_vec(),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        max_ack_pending: 1000,
        max_deliver: 3,
        ack_wait: Duration::from_secs(30),
        ..Default::default()
    }
}

fn bound(subjects: &[String], stream: &str, spec: Value) -> ServiceManifest {
    let mut consumer = json!({
        "durable_name": "loan-service-book-consumer",
        "filter_subjects": [subjects[1]],
    });
    consumer
        .as_object_mut()
        .expect("object")
        .extend(spec.as_object().expect("object").clone());
    manifest(
        json!({
            "subject": subjects[0], "handler": "on_book_event", "handler_type": "wasm",
            "stream": stream, "ack": "on_success", "consumer": consumer,
        }),
        "legacy",
    )
}

#[tokio::test]
async fn a_durable_the_platform_manages_for_another_service_is_not_bound() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("BOOKS");
    let (stream, _) = stream_of(&js, &name, stream::RetentionPolicy::Limits).await;
    let prefix = name.to_lowercase();
    let subjects = [
        format!("{prefix}.due_date.set"),
        format!("{prefix}.deleted.loans"),
    ];
    stream
        .create_consumer(pull::Config {
            metadata: [
                ("cufflink.managed", "1"),
                ("cufflink.tenant", "harbor"),
                ("cufflink.service", "receipt-service"),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
            ..legacy_durable(&subjects)
        })
        .await
        .expect("a durable of receipt-service");
    let identity = identity("harbor");
    let binds = wanted(&js, &identity, &bound(&subjects, &name, json!({}))).await;
    assert_eq!(
        plan(&js, &identity, &binds, &[], &HashSet::new()).await,
        Err(ConsumerPlanError::NotOwned {
            stream: name.clone(),
            name: "loan-service-book-consumer".into(),
            owner: "managed by the platform for service 'receipt-service' of tenant 'harbor'"
                .into(),
        })
    );
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn bound_durable_binds_without_replay() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    for retention in [
        stream::RetentionPolicy::Limits,
        stream::RetentionPolicy::WorkQueue,
    ] {
        let name = unique("BOOKS");
        let (stream, _) = stream_of(&js, &name, retention).await;
        let prefix = name.to_lowercase();
        let subjects = [
            format!("{prefix}.due_date.set"),
            format!("{prefix}.deleted.loans"),
        ];
        stream
            .create_consumer(legacy_durable(&subjects))
            .await
            .expect("the legacy consumer's durable");
        for n in 0..10 {
            publish(&js, &subjects[n % 2], format!(r#"{{"n":{n}}}"#)).await;
        }
        assert_eq!(
            drain(&stream, "loan-service-book-consumer", 4).await,
            [1, 2, 3, 4]
        );
        let as_it_was = info(&stream, "loan-service-book-consumer")
            .await
            .expect("durable");

        let identity = identity("harbor");
        let declared = json!({"ack_wait_secs": 30, "max_deliver": 3, "max_ack_pending": 1000});
        for spec in [declared, json!({})] {
            let binds = wanted(&js, &identity, &bound(&subjects, &name, spec)).await;
            let binding = plan(&js, &identity, &binds, &[], &HashSet::new())
                .await
                .expect("plan");
            assert_eq!(
                binding.ops,
                [ConsumerOp::Keep {
                    stream: name.clone(),
                    name: "loan-service-book-consumer".into(),
                }]
            );
            assert_eq!(before(&js, &identity, &binding).await, []);
            after(&js, &identity, &binding).await;
        }
        let bound_durable = info(&stream, "loan-service-book-consumer")
            .await
            .expect("durable");
        assert_eq!(
            (bound_durable.config, bound_durable.created),
            (as_it_was.config, as_it_was.created),
            "neither recreated nor changed, its metadata included"
        );
        assert_eq!(
            drain(&stream, "loan-service-book-consumer", 50).await,
            [5, 6, 7, 8, 9, 10],
            "what was acknowledged is not delivered again"
        );

        let unbinds = plan(
            &js,
            &identity,
            &[],
            &wanted(&js, &identity, &bound(&subjects, &name, json!({}))).await,
            &HashSet::new(),
        )
        .await
        .expect("plan");
        assert!(matches!(unbinds.ops[..], [ConsumerOp::Release { .. }]));
        before(&js, &identity, &unbinds).await;
        after(&js, &identity, &unbinds).await;
        assert!(
            info(&stream, "loan-service-book-consumer").await.is_some(),
            "a bound durable outlives the service that consumed it"
        );
        js.delete_stream(&name).await.expect("delete stream");
    }
}

#[tokio::test]
async fn a_bound_durable_gains_only_what_the_manifest_declares() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("BOOKS");
    let (stream, _) = stream_of(&js, &name, stream::RetentionPolicy::Limits).await;
    let prefix = name.to_lowercase();
    let subjects = [
        format!("{prefix}.due_date.set"),
        format!("{prefix}.deleted.loans"),
    ];
    stream
        .create_consumer(pull::Config {
            description: Some("the legacy consumer".into()),
            filter_subjects: Vec::new(),
            filter_subject: subjects[0].clone(),
            metadata: [("owner".to_string(), "harbor".to_string())].into(),
            ..legacy_durable(&subjects)
        })
        .await
        .expect("durable");
    for n in 0..6 {
        publish(&js, &subjects[n % 2], format!(r#"{{"n":{n}}}"#)).await;
    }
    assert_eq!(
        drain(&stream, "loan-service-book-consumer", 2).await,
        [1, 3]
    );
    let as_it_was = info(&stream, "loan-service-book-consumer")
        .await
        .expect("durable");

    let identity = identity("harbor");
    let binds = wanted(
        &js,
        &identity,
        &bound(&subjects, &name, json!({"max_deliver": 5})),
    )
    .await;
    let binding = plan(&js, &identity, &binds, &[], &HashSet::new())
        .await
        .expect("plan");
    assert!(
        matches!(
            &binding.ops[..],
            [ConsumerOp::Update { changes, .. }] if changes[..] == ["filters", "max_deliver"]
        ),
        "{binding:?}"
    );
    before(&js, &identity, &binding).await;

    let updated = info(&stream, "loan-service-book-consumer")
        .await
        .expect("durable");
    assert_eq!(updated.created, as_it_was.created, "updated in place");
    assert_eq!(updated.config.max_deliver, 5);
    let mut filters = updated.config.filter_subjects.clone();
    filters.sort();
    let mut expected = subjects.to_vec();
    expected.sort();
    assert_eq!(filters, expected);
    assert_eq!(
        (
            updated.config.description.as_deref(),
            updated.config.ack_wait,
            updated.config.max_ack_pending,
            updated.config.metadata.get("owner").map(String::as_str),
        ),
        (
            Some("the legacy consumer"),
            Duration::from_secs(30),
            1000,
            Some("harbor")
        )
    );
    assert!(
        !updated
            .config
            .metadata
            .keys()
            .any(|key| key.starts_with("cufflink.")),
        "{:?}",
        updated.config.metadata
    );
    assert_eq!(
        updated.ack_floor.stream_sequence,
        as_it_was.ack_floor.stream_sequence
    );
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn bound_durable_incompatible_is_refused() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("BOOKS");
    let (stream, _) = stream_of(&js, &name, stream::RetentionPolicy::Limits).await;
    let prefix = name.to_lowercase();
    let subjects = [
        format!("{prefix}.due_date.set"),
        format!("{prefix}.deleted.loans"),
    ];
    stream
        .create_consumer(consumer::Config {
            durable_name: Some("loan-service-book-consumer".into()),
            deliver_subject: Some(format!("deliver.{prefix}")),
            filter_subjects: subjects.to_vec(),
            ack_policy: AckPolicy::None,
            ..Default::default()
        })
        .await
        .expect("durable");
    let as_it_was = info(&stream, "loan-service-book-consumer")
        .await
        .expect("durable");

    let identity = identity("harbor");
    let binds = wanted(&js, &identity, &bound(&subjects, &name, json!({}))).await;
    assert_eq!(
        plan(&js, &identity, &binds, &[], &HashSet::new()).await,
        Err(ConsumerPlanError::Incompatible {
            stream: name.clone(),
            name: "loan-service-book-consumer".into(),
            field: "ack_policy",
            found: "none".into(),
        })
    );
    assert_eq!(
        info(&stream, "loan-service-book-consumer").await,
        Some(as_it_was),
        "untouched"
    );
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn a_durable_a_failed_deploy_left_goes_with_the_next() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("ORPHAN");
    let (stream, subject) = stream_of(&js, &name, stream::RetentionPolicy::Interest).await;
    let identity = identity("it-orphan");
    let legacy = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "legacy"),
    )
    .await;
    let scoped = wanted(
        &js,
        &identity,
        &manifest(subscription(&subject, &name), "scoped"),
    )
    .await;
    stream
        .create_consumer(pull::Config {
            durable_name: Some(legacy[0].name.to_string()),
            filter_subject: subject.clone(),
            ..Default::default()
        })
        .await
        .expect("a durable of the release before durables were marked");

    let rename = plan(&js, &identity, &scoped, &legacy, &HashSet::new())
        .await
        .expect("plan");
    before(&js, &identity, &rename).await;
    let marked: BTreeMap<String, String> = info(&stream, legacy[0].name.as_str())
        .await
        .expect("durable")
        .config
        .metadata
        .into_iter()
        .filter(|(key, _)| key.starts_with("cufflink."))
        .collect();
    assert_eq!(
        marked,
        BTreeMap::from([
            ("cufflink.managed".to_string(), "1".to_string()),
            ("cufflink.service".to_string(), "stock-service".to_string()),
            ("cufflink.tenant".to_string(), "it-orphan".to_string()),
        ]),
        "marked before its successor exists, so a delete that fails is found again"
    );

    let next = plan(&js, &identity, &scoped, &scoped, &HashSet::new())
        .await
        .expect("the plan of the deploy after one whose delete failed");
    assert_eq!(
        next.ops,
        [
            ConsumerOp::Keep {
                stream: name.clone(),
                name: scoped[0].name.to_string(),
            },
            ConsumerOp::Delete {
                stream: name.clone(),
                name: legacy[0].name.to_string(),
            }
        ]
    );
    before(&js, &identity, &next).await;
    after(&js, &identity, &next).await;
    assert_eq!(info(&stream, legacy[0].name.as_str()).await, None);
    assert!(info(&stream, scoped[0].name.as_str()).await.is_some());
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn a_lost_schedule_durable_restored_by_two_replicas_keeps_its_pending_fire() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    events::setup_schedules_stream(&js)
        .await
        .expect("schedules stream");
    let identity = identity(&format!("it-{}", unique("lost").to_lowercase()));
    let manifest: ServiceManifest = serde_json::from_value(json!({
        "name": "stock-service",
        "version": "0.1.0",
        "tables": [],
        "schedules": [{"name": "nightly", "handler": "sweep",
                       "spec": {"kind": "every", "secs": 60}}],
    }))
    .expect("manifest");
    let [schedule] = <[DesiredConsumer; 1]>::try_from(wanted(&js, &identity, &manifest).await)
        .expect("one durable");
    assert_eq!(schedule.kind, ConsumerKind::Schedule);
    let stream = js
        .get_stream(events::SCHEDULES_STREAM)
        .await
        .expect("stream");
    let fires = schedule.filters.first().expect("subject").clone();
    let pending = async || {
        info(&stream, schedule.name.as_str())
            .await
            .expect("durable")
            .num_pending
    };

    publish(&js, &fires, "gathered while the durable was gone".into()).await;
    assert!(apply::restore(&stream, &schedule).await.expect("replica a"));
    assert_eq!(pending().await, 0, "what gathered is stale");
    publish(&js, &fires, "fired once it was back".into()).await;
    assert!(!apply::restore(&stream, &schedule).await.expect("replica b"));
    assert_eq!(pending().await, 1, "the fire waits for its loop");
    stream
        .delete_consumer(schedule.name.as_str())
        .await
        .expect("delete");
    stream.purge().filter(&fires).await.expect("purge");
}
