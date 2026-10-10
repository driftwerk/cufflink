use cufflink_testkit as common;

use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self, stream};
use common::jetstream::acknowledged;
use common::PlatformProcess;
use cufflink_platform::nats_manager::delivery::{decide, Decision, Outcome};
use cufflink_platform::nats_manager::dlq::{
    self, dlq_subject, DeadLetter, DlqKind, DLQ_STREAM, REASON_HEADER, SEQUENCE_HEADER,
    SUBJECT_HEADER, WITHHELD_HEADER,
};
use cufflink_platform::nats_manager::events;
use cufflink_platform::nats_manager::pump::{
    self, MessageMeta, PumpConfig, PumpExit, Work, WorkLabel,
};
use cufflink_platform::shutdown::Shutdown;
use cufflink_types::backoff::Backoff;
use cufflink_types::AckMode;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple()).to_uppercase()
}

async fn jetstream(nats_url: &str) -> jetstream::Context {
    jetstream::new(async_nats::connect(nats_url).await.expect("nats"))
}

async fn consumer_on(
    js: &jetstream::Context,
    name: &str,
    ack_wait: Duration,
) -> (stream::Stream, jetstream::consumer::PullConsumer, String) {
    let subject = format!("pump.{}", name.to_lowercase());
    let stream = js
        .create_stream(stream::Config {
            name: name.to_string(),
            subjects: vec![subject.clone()],
            storage: stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .expect("stream");
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("worker".into()),
            ack_wait,
            ..Default::default()
        })
        .await
        .expect("consumer");
    (stream, consumer, subject)
}

async fn publish(js: &jetstream::Context, subject: &str, payload: &'static str) {
    js.publish(subject.to_string(), payload.into())
        .await
        .expect("publish")
        .await
        .expect("stored");
}

struct Slow {
    takes: Duration,
    deliveries: Arc<AtomicU64>,
}

impl Work for Slow {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "subscription",
            subscription: None,
        }
    }

    fn handle(
        &self,
        _msg: Arc<jetstream::Message>,
        _meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let (takes, deliveries) = (self.takes, Arc::clone(&self.deliveries));
        async move {
            deliveries.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(takes).await;
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

struct Cut {
    ack: AckMode,
    shutdown: Shutdown,
    deliveries: Arc<AtomicU64>,
}

impl Work for Cut {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "subscription",
            subscription: None,
        }
    }

    fn handle(
        &self,
        _msg: Arc<jetstream::Message>,
        _meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let (shutdown, deliveries) = (self.shutdown.clone(), Arc::clone(&self.deliveries));
        async move {
            deliveries.fetch_add(1, Ordering::SeqCst);
            shutdown.cancelled().await;
            Outcome::Interrupted
        }
        .boxed()
    }

    fn decide(&self, outcome: &Outcome, meta: &MessageMeta) -> Decision {
        decide(
            self.ack,
            outcome,
            meta.delivered,
            -1,
            &Backoff::explicit(Vec::new()),
            false,
            meta.stream_sequence,
        )
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

async fn until_delivered(deliveries: &AtomicU64, count: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while deliveries.load(Ordering::SeqCst) < count {
        assert!(Instant::now() < deadline, "delivery {count} never came");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn next_delivery(
    consumer: &jetstream::consumer::PullConsumer,
    within: Duration,
) -> Option<jetstream::Message> {
    consumer
        .fetch()
        .max_messages(1)
        .expires(within)
        .messages()
        .await
        .expect("fetch")
        .next()
        .await
        .map(|message| message.expect("message"))
}

const PROMPTLY: Duration = Duration::from_secs(20);
const ACK_WAIT: Duration = Duration::from_secs(60);

#[tokio::test]
async fn a_handler_a_shutdown_cuts_is_acknowledged_under_always_and_handed_back_under_on_success() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    for ack in [AckMode::Always, AckMode::OnSuccess] {
        let name = unique("PUMP_CUT");
        let (stream, mut consumer, subject) = consumer_on(&js, &name, ACK_WAIT).await;
        publish(&js, &subject, "{}").await;

        let deliveries = Arc::new(AtomicU64::new(0));
        let shutdown = Shutdown::new(Duration::from_secs(30));
        let running = tokio::spawn(pump::run(
            consumer.clone(),
            PumpConfig::sequential(ACK_WAIT),
            Arc::new(Cut {
                ack,
                shutdown: shutdown.clone(),
                deliveries: Arc::clone(&deliveries),
            }),
            Arc::new(js.clone()),
            CancellationToken::new(),
            shutdown.clone(),
        ));
        until_delivered(&deliveries, 1).await;
        shutdown.trigger();
        assert_eq!(running.await.expect("pump"), PumpExit::ShuttingDown);
        shutdown.drain_tasks().await;

        let handed_back_at = Instant::now();
        match ack {
            AckMode::Always => {
                let info = acknowledged(&mut consumer, 1, PROMPTLY).await;
                assert_eq!(
                    (info.ack_floor.stream_sequence, info.num_ack_pending),
                    (1, 0),
                    "acknowledged, as a subscription's message always was: {info:?}"
                );
                assert!(
                    next_delivery(&consumer, Duration::from_secs(1))
                        .await
                        .is_none(),
                    "an acknowledged message is never delivered again"
                );
            }
            AckMode::OnSuccess => {
                let again = next_delivery(&consumer, PROMPTLY)
                    .await
                    .expect("the message is delivered again");
                assert!(handed_back_at.elapsed() < PROMPTLY);
                assert_eq!(again.info().expect("info").delivered, 2);
                again.double_ack().await.expect("ack");
            }
        }
        assert_eq!(deliveries.load(Ordering::SeqCst), 1);
        stream.delete_consumer("worker").await.expect("delete");
        js.delete_stream(&name).await.expect("delete stream");
    }
}

#[tokio::test]
async fn a_shutdown_hands_back_the_rest_of_the_batch() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("PUMP_BATCH");
    let (stream, consumer, subject) = consumer_on(&js, &name, ACK_WAIT).await;
    publish(&js, &subject, r#"{"n":1}"#).await;

    let deliveries = Arc::new(AtomicU64::new(0));
    let shutdown = Shutdown::new(Duration::from_secs(30));
    let running = tokio::spawn(pump::run(
        consumer.clone(),
        PumpConfig::sequential(ACK_WAIT).concurrent(2),
        Arc::new(Slow {
            takes: Duration::from_secs(2),
            deliveries: Arc::clone(&deliveries),
        }),
        Arc::new(js.clone()),
        CancellationToken::new(),
        shutdown.clone(),
    ));
    until_delivered(&deliveries, 1).await;
    shutdown.trigger();
    publish(&js, &subject, r#"{"n":2}"#).await;
    assert_eq!(running.await.expect("pump"), PumpExit::ShuttingDown);
    let handed_back_at = Instant::now();
    shutdown.drain_tasks().await;

    let next = next_delivery(&consumer, PROMPTLY)
        .await
        .expect("the message the batch received is delivered again");
    assert!(handed_back_at.elapsed() < PROMPTLY);
    assert_eq!(&next.payload[..], br#"{"n":2}"#);
    assert_eq!(
        next.info().expect("info").delivered,
        2,
        "it reached the stopping pump, which handed it back"
    );
    next.double_ack().await.expect("ack");
    assert_eq!(
        deliveries.load(Ordering::SeqCst),
        1,
        "the message in hand finished; the other was never started"
    );
    let mut consumer = consumer;
    let info = acknowledged(&mut consumer, 2, PROMPTLY).await;
    assert_eq!(
        (info.ack_floor.stream_sequence, info.num_ack_pending),
        (2, 0),
        "{info:?}"
    );
    stream.delete_consumer("worker").await.expect("delete");
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn heartbeat_prevents_redelivery() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("PUMP_HEARTBEAT");
    let ack_wait = Duration::from_secs(10);
    let (stream, mut consumer, subject) = consumer_on(&js, &name, ack_wait).await;
    publish(&js, &subject, "{}").await;

    let deliveries = Arc::new(AtomicU64::new(0));
    let (stop, shutdown) = (
        CancellationToken::new(),
        Shutdown::new(Duration::from_secs(30)),
    );
    let running = tokio::spawn(pump::run(
        consumer.clone(),
        PumpConfig::sequential(ack_wait),
        Arc::new(Slow {
            takes: Duration::from_secs(21),
            deliveries: Arc::clone(&deliveries),
        }),
        Arc::new(js.clone()),
        stop.clone(),
        shutdown.clone(),
    ));

    let deadline = Instant::now() + Duration::from_secs(60);
    while consumer
        .info()
        .await
        .expect("info")
        .ack_floor
        .stream_sequence
        < 1
    {
        assert!(
            Instant::now() < deadline,
            "the message was never acknowledged"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let info = consumer.info().await.expect("info").clone();
    assert_eq!(
        deliveries.load(Ordering::SeqCst),
        1,
        "a handler outlasting ack_wait twice over is delivered once: {info:?}"
    );
    assert_eq!((info.num_redelivered, info.num_ack_pending), (0, 0));

    stop.cancel();
    assert_eq!(running.await.expect("pump"), PumpExit::Stopped);
    stream.delete_consumer("worker").await.expect("delete");
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn a_pump_gives_up_a_consumer_the_server_no_longer_has() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let name = unique("PUMP_GONE");
    let (stream, consumer, _) = consumer_on(&js, &name, Duration::from_secs(30)).await;
    let running = tokio::spawn(pump::run(
        consumer,
        PumpConfig {
            fetch_expires: Duration::from_secs(2),
            ..PumpConfig::sequential(Duration::from_secs(30))
        },
        Arc::new(Slow {
            takes: Duration::ZERO,
            deliveries: Arc::default(),
        }),
        Arc::new(js.clone()),
        CancellationToken::new(),
        Shutdown::new(Duration::from_secs(30)),
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;
    stream.delete_consumer("worker").await.expect("delete");

    let exit = tokio::time::timeout(Duration::from_secs(30), running)
        .await
        .expect("the pump ends once the consumer is gone")
        .expect("pump");
    assert_eq!(exit, PumpExit::ConsumerGone);
    js.delete_stream(&name).await.expect("delete stream");
}

#[tokio::test]
async fn dead_letter_publish_is_idempotent() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream(nats_url).await;
    let queue = events::dlq_stream(&js, Duration::from_secs(3600))
        .await
        .expect("dead-letter stream");
    let tenant = format!("it-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let letter = DeadLetter::now(
        dlq_subject(
            &tenant,
            "loan-service",
            DlqKind::Subscription,
            "on_book_event",
        ),
        bytes::Bytes::from_static(br#"{"id":42}"#),
        &MessageMeta {
            subject: "books.due_date.set".into(),
            stream: unique("BOOKS"),
            consumer: "loan-service-book-consumer".into(),
            stream_sequence: 81_234,
            consumer_sequence: 17,
            delivered: 3,
            pending: 0,
            published: chrono::Utc::now(),
            headers: vec![
                ("traceparent".into(), vec!["00-abc-def-01".into()]),
                ("Cufflink-Reply".into(), vec!["_INBOX.x".into()]),
                ("Nats-Expected-Stream".into(), vec!["BOOKS".into()]),
                ("Nats-Expected-Last-Sequence".into(), vec!["7".into()]),
                ("Nats-Msg-Id".into(), vec!["book-42".into()]),
            ],
        },
        "handler answered 503",
    );
    for _ in 0..3 {
        dlq::publish_dead_letter(&js, &letter)
            .await
            .expect("stored");
    }

    let reader = queue
        .create_consumer(pull::Config {
            filter_subject: format!("cufflink.dlq.{tenant}.>"),
            ..Default::default()
        })
        .await
        .expect("reader");
    let mut letters = reader
        .fetch()
        .max_messages(5)
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .expect("fetch");
    let stored = letters
        .next()
        .await
        .expect("one dead letter")
        .expect("message");
    assert!(
        letters.next().await.is_none(),
        "published thrice, stored once"
    );
    assert_eq!(&stored.payload[..], br#"{"id":42}"#);
    assert_eq!(
        stored.subject.as_str(),
        format!("cufflink.dlq.{tenant}.loan-service.sub.on_book_event")
    );
    let headers = stored.headers.as_ref().expect("headers");
    let header = |name: &str| headers.get(name).map(|value| value.as_str().to_string());
    assert_eq!(
        header(REASON_HEADER).as_deref(),
        Some("handler answered 503")
    );
    assert_eq!(
        header(SUBJECT_HEADER).as_deref(),
        Some("books.due_date.set")
    );
    assert_eq!(header(SEQUENCE_HEADER).as_deref(), Some("81234"));
    assert_eq!(header("traceparent").as_deref(), Some("00-abc-def-01"));
    assert_eq!(header("Cufflink-Reply"), None);
    assert_eq!(
        header("Nats-Expected-Stream"),
        None,
        "carried, it would have failed the publish: the queue is not that stream"
    );
    assert_eq!(
        header(WITHHELD_HEADER)
            .and_then(|record| dlq::withheld_headers(&record))
            .map(serde_json::Value::Object),
        Some(json!({
            "Nats-Expected-Last-Sequence": ["7"],
            "Nats-Expected-Stream": ["BOOKS"],
            "Nats-Msg-Id": ["book-42"],
        }))
    );
    assert_eq!(
        dlq::tenant_of(stored.subject.as_str()),
        Some(tenant.as_str())
    );
    assert_eq!(DLQ_STREAM, "CUFFLINK_DLQ");
}

#[tokio::test]
async fn always_ack_unchanged() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let manifest = json!({
        "name": "loan-service",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "loans", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "book_id", "column_type": "uuid", "primary_key": false, "nullable": false,
             "auto_generate": false},
        ]}],
        "subscriptions": [{
            "subject": "dw.{tenant}.books.deleted",
            "handler": "drop_loans",
            "handler_type": "delete_cascade",
            "config": {"table": "loans", "foreign_key": "book_id"},
        }],
        "publishes": [],
    });
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "{body}");

    let js = jetstream(nats_url).await;
    let subject = format!("dw.{}.books.deleted", tenant.slug);
    publish(
        &js,
        &subject,
        r#"{"data":{"id":"6f1c2a3e-0000-4000-8000-000000000001"}}"#,
    )
    .await;
    publish(&js, &subject, r#"{"data":{}}"#).await;
    publish(&js, &subject, "not json").await;

    let stream_name = format!("DW_{}", tenant.slug.to_uppercase().replace('-', "_"));
    let mut stream = js.get_stream(&stream_name).await.expect("tenant stream");
    let mut consumer = stream
        .get_consumer::<pull::Config>("loan_service_drop_loans")
        .await
        .expect("the durable keeps the name it always had");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let info = consumer.info().await.expect("info").clone();
        if info.ack_floor.stream_sequence >= 3 {
            assert_eq!(
                (info.num_redelivered, info.num_ack_pending, info.num_pending),
                (0, 0, 0),
                "each message was delivered once and acknowledged: {info:?}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "not every message was acknowledged: {info:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(stream.info().await.expect("stream").state.messages, 0);

    for (result, count) in [("ok", 1.0), ("error", 1.0), ("invalid", 1.0)] {
        assert_eq!(
            platform
                .metric(
                    "cufflink_subscription_messages_total",
                    &[
                        ("tenant", tenant.slug.as_str()),
                        ("handler", "drop_loans"),
                        ("result", result)
                    ],
                )
                .await,
            Some(count),
            "{result}"
        );
    }
    let queue = js.get_stream(DLQ_STREAM).await.expect("dead-letter stream");
    let reader = queue
        .create_consumer(pull::Config {
            filter_subject: format!("cufflink.dlq.{}.>", tenant.slug),
            ..Default::default()
        })
        .await
        .expect("reader");
    let mut letters = reader
        .fetch()
        .max_messages(1)
        .expires(Duration::from_secs(1))
        .messages()
        .await
        .expect("fetch");
    assert!(
        letters.next().await.is_none(),
        "an always-ack subscription dead-letters nothing"
    );
}
