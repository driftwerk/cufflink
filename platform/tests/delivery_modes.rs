use cufflink_testkit as common;

use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self};
use common::jetstream::tenant_stream;
use common::{infra, PlatformProcess, Tenant, TestDb};
use cufflink_platform::nats_manager::dlq::{
    CONSUMER_HEADER, DELIVERIES_HEADER, DLQ_STREAM, REASON_HEADER, SEQUENCE_HEADER, STREAM_HEADER,
    SUBJECT_HEADER,
};
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_test_guests::Profile;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const SERVICE: &str = "mail-service";

async fn jetstream(nats_url: &str) -> jetstream::Context {
    jetstream::new(async_nats::connect(nats_url).await.expect("nats"))
}

fn durable(handler: &str) -> String {
    format!("mail_service_{handler}")
}

fn column(name: &str, column_type: &str) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": true,
           "auto_generate": false})
}

fn manifest(handlers: &[(&str, Value)]) -> Value {
    let subscriptions: Vec<Value> = handlers
        .iter()
        .map(|(handler, delivery)| {
            let mut subscription = json!({
                "subject": format!("dw.{{tenant}}.mail.{handler}"),
                "handler": handler,
                "handler_type": "wasm",
            });
            subscription
                .as_object_mut()
                .expect("object")
                .extend(delivery.as_object().expect("object").clone());
            subscription
        })
        .collect();
    json!({
        "name": SERVICE,
        "version": "0.1.0",
        "mode": "wasm",
        "tables": [{"name": "deliveries", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            column("handler", "text"),
            column("body", "text"),
            column("attempt", "biginteger"),
            column("subject", "text"),
            column("stream", "text"),
            column("consumer", "text"),
            column("sequence", "biginteger"),
            column("msg_id", "text"),
        ]}],
        "subscriptions": subscriptions,
        "publishes": [],
    })
}

struct Deployed {
    platform: PlatformProcess,
    tenant: Tenant,
    js: jetstream::Context,
    rows: deadpool_postgres::Object,
    wasm: Vec<u8>,
    db: TestDb,
}

async fn deployed(handlers: &[(&str, Value)]) -> Option<Deployed> {
    deployed_with(handlers, &[]).await
}

async fn deployed_with(handlers: &[(&str, Value)], env: &[(&str, &str)]) -> Option<Deployed> {
    let infra = common::infra().await?;
    let nats_url = infra.require_nats()?;
    let wasm = common::sdk::sdk_guest("eventing", Profile::Debug)?;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), env).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy_module(&tenant, &manifest(handlers), &wasm)
        .await;
    assert_eq!(status, 200, "{body}");
    let rows = cufflink_db::build_pool(
        &infra::with_dbname(
            &infra.database_url,
            &TenantPoolManager::managed_db_name(&tenant.slug),
        ),
        1,
    )
    .expect("tenant pool")
    .get()
    .await
    .expect("tenant database");
    Some(Deployed {
        platform,
        js: jetstream(nats_url).await,
        tenant,
        rows,
        wasm,
        db,
    })
}

impl Deployed {
    async fn redeploy(&self, handlers: &[(&str, Value)]) -> Value {
        let (status, body) = self
            .platform
            .deploy_module(&self.tenant, &manifest(handlers), &self.wasm)
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn publish(&self, handler: &str, payload: &'static str) -> u64 {
        self.js
            .publish(
                format!("dw.{}.mail.{handler}", self.tenant.slug),
                payload.into(),
            )
            .await
            .expect("publish")
            .await
            .expect("stored")
            .sequence
    }

    async fn deliveries(&self, handler: &str) -> i64 {
        self.rows
            .query_one(
                "SELECT count(*) FROM mail_service.deliveries WHERE handler = $1",
                &[&handler],
            )
            .await
            .expect("deliveries")
            .get(0)
    }

    async fn counted(&self, handler: &str, result: &str) -> f64 {
        self.platform
            .metric(
                "cufflink_subscription_messages_total",
                &[
                    ("tenant", self.tenant.slug.as_str()),
                    ("handler", handler),
                    ("result", result),
                ],
            )
            .await
            .unwrap_or_default()
    }

    async fn consumer(&self, handler: &str) -> jetstream::consumer::Info {
        self.js
            .get_stream(tenant_stream(&self.tenant))
            .await
            .expect("stream")
            .consumer_info(durable(handler))
            .await
            .expect("durable")
    }

    async fn metric(&self, name: &str) -> f64 {
        self.platform.metric(name, &[]).await.unwrap_or_default()
    }

    async fn until_settled(&self, handler: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let info = self.consumer(handler).await;
            if info.num_pending == 0
                && info.num_ack_pending == 0
                && info.delivered.stream_sequence > 0
            {
                return;
            }
            assert!(Instant::now() < deadline, "never settled: {info:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn dead_letters(&self, handler: &str) -> Vec<async_nats::jetstream::Message> {
        let queue = self.js.get_stream(DLQ_STREAM).await.expect("queue");
        let reader = queue
            .create_consumer(pull::Config {
                filter_subject: format!(
                    "cufflink.dlq.{}.{SERVICE}.sub.{handler}",
                    self.tenant.slug
                ),
                ..Default::default()
            })
            .await
            .expect("reader");
        reader
            .fetch()
            .max_messages(10)
            .expires(Duration::from_secs(1))
            .messages()
            .await
            .expect("fetch")
            .map(|message| message.expect("message"))
            .collect()
            .await
    }
}

fn header(message: &async_nats::jetstream::Message, name: &str) -> Option<String> {
    message
        .headers
        .as_ref()
        .and_then(|headers| headers.get(name))
        .map(|value| value.as_str().to_string())
}

fn on_success(consumer: Value) -> Value {
    json!({"ack": "on_success", "consumer": consumer})
}

#[tokio::test]
async fn on_success_retries_then_dead_letters() {
    let Some(deployed) = deployed(&[(
        "fail",
        on_success(json!({"max_deliver": 3, "backoff_secs": [1, 2]})),
    )])
    .await
    else {
        return;
    };
    let published_at = Instant::now();
    let sequence = deployed.publish("fail", r#"{"to":"a@example.com"}"#).await;
    deployed.until_settled("fail").await;

    assert_eq!(deployed.deliveries("fail").await, 3);
    assert!(
        published_at.elapsed() >= Duration::from_millis(2400),
        "redelivered after its backoff of 1 s and 2 s (less a fifth), not at once: {:?}",
        published_at.elapsed()
    );
    let letters = deployed.dead_letters("fail").await;
    assert_eq!(letters.len(), 1);
    let letter = &letters[0];
    assert_eq!(&letter.payload[..], br#"{"to":"a@example.com"}"#);
    for (name, value) in [
        (REASON_HEADER, "handler answered 503".to_string()),
        (STREAM_HEADER, tenant_stream(&deployed.tenant)),
        (
            SUBJECT_HEADER,
            format!("dw.{}.mail.fail", deployed.tenant.slug),
        ),
        (SEQUENCE_HEADER, sequence.to_string()),
        (DELIVERIES_HEADER, "3".to_string()),
        (CONSUMER_HEADER, durable("fail")),
    ] {
        assert_eq!(header(letter, name), Some(value), "{name}");
    }
    assert_eq!(deployed.counted("fail", "retry").await, 2.0);
    assert_eq!(deployed.counted("fail", "dead_letter").await, 1.0);
}

#[tokio::test]
async fn a_lowered_max_deliver_dead_letters_at_the_new_limit() {
    let Some(deployed) = deployed(&[(
        "fail",
        on_success(json!({"max_deliver": 5, "backoff_secs": [1]})),
    )])
    .await
    else {
        return;
    };
    let lowered = deployed
        .redeploy(&[(
            "fail",
            on_success(json!({"max_deliver": 3, "backoff_secs": [1]})),
        )])
        .await;
    assert_eq!(
        (
            &lowered["plan"]["consumers"][0]["op"],
            &lowered["plan"]["consumers"][0]["durable"]
        ),
        (&json!("update"), &json!(durable("fail"))),
        "{lowered}"
    );
    let durable_now = deployed
        .js
        .get_stream(tenant_stream(&deployed.tenant))
        .await
        .expect("stream")
        .consumer_info(durable("fail"))
        .await
        .expect("durable");
    assert_eq!(durable_now.config.max_deliver, 3);

    deployed.publish("fail", r#"{"n":1}"#).await;
    deployed.until_settled("fail").await;
    let letters = deployed.dead_letters("fail").await;
    assert_eq!(
        letters.len(),
        1,
        "the loop gives the message up where its durable now does"
    );
    assert_eq!(header(&letters[0], DELIVERIES_HEADER), Some("3".into()));
    assert_eq!(
        deployed.deliveries("fail").await,
        3,
        "every delivery ran the handler, one the replaced loop was handed included"
    );
    assert_eq!(deployed.counted("fail", "dead_letter").await, 1.0);
}

#[tokio::test]
async fn a_handler_that_recovers_is_acknowledged_and_nothing_is_dead_lettered() {
    let Some(deployed) = deployed(&[
        ("fail_twice", on_success(json!({"backoff_secs": [1]}))),
        ("retry_later", on_success(json!({"backoff_secs": [60]}))),
        ("say_error", on_success(json!({}))),
    ])
    .await
    else {
        return;
    };
    deployed.publish("fail_twice", r#"{"n":1}"#).await;
    let asked_at = Instant::now();
    deployed.publish("retry_later", r#"{"n":2}"#).await;
    deployed.publish("say_error", r#"{"n":3}"#).await;
    for handler in ["fail_twice", "retry_later", "say_error"] {
        deployed.until_settled(handler).await;
        assert_eq!(deployed.dead_letters(handler).await.len(), 0, "{handler}");
        assert_eq!(deployed.counted(handler, "ok").await, 1.0, "{handler}");
    }
    assert_eq!(deployed.deliveries("fail_twice").await, 3);
    assert_eq!(deployed.counted("fail_twice", "retry").await, 2.0);
    assert_eq!(deployed.deliveries("retry_later").await, 2);
    assert!(
        asked_at.elapsed() < Duration::from_secs(30),
        "delivered again when the handler asked, not after the consumer's backoff of a minute"
    );
    assert_eq!(
        deployed.deliveries("say_error").await,
        1,
        "an answer of 200 is a success whatever its body says"
    );
}

#[tokio::test]
async fn what_can_never_succeed_is_given_up_at_once() {
    let Some(deployed) = deployed(&[
        ("reject", on_success(json!({}))),
        ("give_up", on_success(json!({}))),
        ("succeed", on_success(json!({}))),
        (
            "fail",
            on_success(json!({"dead_letter": false, "max_deliver": 1})),
        ),
    ])
    .await
    else {
        return;
    };
    deployed.publish("reject", r#"{"book":1}"#).await;
    deployed.publish("give_up", r#"{"book":2}"#).await;
    deployed.publish("succeed", "not json").await;
    deployed.publish("fail", r#"{"book":4}"#).await;
    for (handler, deliveries, reason) in [
        ("reject", 1, Some("handler rejected the message with 422")),
        ("give_up", 1, Some("unknown book")),
        ("succeed", 0, Some("the payload is not JSON")),
        ("fail", 1, None),
    ] {
        deployed.until_settled(handler).await;
        assert_eq!(deployed.deliveries(handler).await, deliveries, "{handler}");
        let letters = deployed.dead_letters(handler).await;
        match reason {
            Some(reason) => {
                assert_eq!(letters.len(), 1, "{handler}");
                let recorded = header(&letters[0], REASON_HEADER).unwrap_or_default();
                assert!(recorded.starts_with(reason), "{handler}: {recorded}");
                assert_eq!(header(&letters[0], DELIVERIES_HEADER).as_deref(), Some("1"));
                assert_eq!(deployed.counted(handler, "dead_letter").await, 1.0);
            }
            None => {
                assert_eq!(letters.len(), 0, "{handler} keeps no dead letters");
                assert_eq!(deployed.counted(handler, "terminal").await, 1.0);
            }
        }
    }
}

#[tokio::test]
async fn under_always_an_answer_changes_nothing() {
    let Some(deployed) = deployed(&[("fail", json!({})), ("give_up", json!({}))]).await else {
        return;
    };
    for handler in ["fail", "give_up"] {
        deployed.publish(handler, r#"{"n":1}"#).await;
        deployed.until_settled(handler).await;
        assert_eq!(deployed.deliveries(handler).await, 1);
        assert_eq!(deployed.dead_letters(handler).await.len(), 0);
        assert_eq!(
            deployed.counted(handler, "ok").await,
            1.0,
            "counted as v0.27.0 counted a handler that answered"
        );
    }
}

#[tokio::test]
async fn a_handler_is_told_of_its_delivery() {
    let Some(deployed) = deployed(&[
        (
            "fail",
            on_success(json!({"max_deliver": 2, "backoff_secs": [1]})),
        ),
        ("succeed", json!({})),
    ])
    .await
    else {
        return;
    };
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "mail-7");
    let sequence = deployed
        .js
        .publish_with_headers(
            format!("dw.{}.mail.fail", deployed.tenant.slug),
            headers,
            r#"{"n":7}"#.into(),
        )
        .await
        .expect("publish")
        .await
        .expect("stored")
        .sequence;
    deployed.publish("succeed", r#"{"n":8}"#).await;
    deployed.until_settled("fail").await;
    deployed.until_settled("succeed").await;

    let told: Vec<Told> = deployed
        .rows
        .query(
            "SELECT handler, attempt, subject, stream, consumer, sequence, msg_id \
             FROM mail_service.deliveries ORDER BY handler, attempt",
            &[],
        )
        .await
        .expect("deliveries")
        .iter()
        .map(|row| Told {
            handler: row.get(0),
            attempt: row.get(1),
            subject: row.get(2),
            stream: row.get(3),
            consumer: row.get(4),
            sequence: row.get(5),
            msg_id: row.get(6),
        })
        .collect();
    let failed = |attempt| Told {
        handler: "fail".into(),
        attempt,
        subject: format!("dw.{}.mail.fail", deployed.tenant.slug),
        stream: tenant_stream(&deployed.tenant),
        consumer: durable("fail"),
        sequence: i64::try_from(sequence).expect("sequence"),
        msg_id: Some("mail-7".into()),
    };
    assert_eq!(told.len(), 3, "{told:?}");
    assert_eq!(told[..2], [failed(1), failed(2)]);
    assert_eq!(
        (
            told[2].handler.as_str(),
            told[2].attempt,
            told[2].consumer.clone(),
            &told[2].msg_id
        ),
        ("succeed", 1, durable("succeed"), &None),
        "under `ack: always` too"
    );
}

const BUDGETED: &[(&str, &str)] = &[
    ("WASM_MEMORY_BUDGET_MODE", "enforce"),
    ("WASM_MEMORY_SERVICE_SHARE_PERCENT", "100"),
    ("WASM_MEMORY_BACKGROUND_SHARE_PERCENT", "100"),
    ("WASM_MEMORY_ADMIT_PERCENT", "100"),
    ("WASM_THREAD_STACK_KB", "1024"),
    ("WASM_CAPACITY_RETRIES", "1"),
];

fn budgeted(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    BUDGETED.iter().chain(extra).copied().collect()
}

async fn until(what: &str, reached: impl AsyncFnMut() -> bool) {
    common::until(what, Duration::from_secs(10), reached)
        .await
        .expect("in time");
}

#[tokio::test]
async fn messages_run_only_as_room_allows_and_each_is_delivered_once() {
    let env = budgeted(&[
        ("WASM_MEMORY_BUDGET_MB", "64"),
        ("WASM_MEMORY_COLD_ESTIMATE_MB", "24"),
    ]);
    let Some(deployed) =
        deployed_with(&[("grow", on_success(json!({"concurrency": 8})))], &env).await
    else {
        return;
    };
    let gate = common::Gate::closed(&deployed.db.tenant_url(&deployed.tenant.slug)).await;
    for _ in 0..4 {
        deployed.publish("grow", r#"{"mib":1,"gate":4242}"#).await;
    }
    until("two messages run", async || gate.waiting().await == 2).await;
    for _ in 0..5 {
        assert_eq!(
            gate.waiting().await,
            2,
            "a 64 MiB budget fits two 24 MiB estimates, not three"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    gate.open().await;
    deployed.until_settled("grow").await;
    assert_eq!(deployed.deliveries("grow").await, 4);
    assert_eq!(
        deployed.consumer("grow").await.delivered.consumer_sequence,
        4,
        "four deliveries for four messages"
    );
    assert_eq!(
        deployed
            .metric("cufflink_wasm_capacity_retries_total")
            .await,
        0.0
    );
    assert!(deployed.dead_letters("grow").await.is_empty());
}

#[tokio::test]
async fn an_ack_always_message_denied_mid_run_is_run_again_in_place_then_deferred_and_not_lost() {
    let env = budgeted(&[
        ("WASM_MEMORY_BUDGET_MB", "64"),
        ("WASM_MEMORY_COLD_ESTIMATE_MB", "2"),
        ("WASM_ADMISSION_RETRY_AFTER_MS", "200"),
    ]);
    let always = || json!({"ack": "always"});
    let Some(deployed) = deployed_with(
        &[
            ("hold", always()),
            ("hold_more", always()),
            ("grow", always()),
        ],
        &env,
    )
    .await
    else {
        return;
    };
    let tenant_db = deployed.db.tenant_url(&deployed.tenant.slug);
    let first = common::Gate::closed_at(&tenant_db, 1).await;
    let second = common::Gate::closed_at(&tenant_db, 2).await;
    deployed.publish("hold", r#"{"mib":20,"gate":1}"#).await;
    deployed
        .publish("hold_more", r#"{"mib":20,"gate":2}"#)
        .await;
    until("both holders wait at their gates", async || {
        first.waiting().await == 1 && second.waiting().await == 1
    })
    .await;

    deployed.publish("grow", r#"{"mib":[24,50]}"#).await;
    until(
        "the first run is denied and waits to run again",
        async || {
            deployed
                .metric("cufflink_wasm_capacity_retries_total")
                .await
                == 1.0
        },
    )
    .await;
    second.open().await;
    until(
        "the second run is denied too and the message deferred",
        async || deployed.counted("grow", "deferred").await == 1.0,
    )
    .await;
    assert_eq!(deployed.deliveries("grow").await, 0);

    first.open().await;
    deployed.until_settled("grow").await;
    assert_eq!(deployed.deliveries("grow").await, 1, "run once room freed");
    let info = deployed.consumer("grow").await;
    assert_eq!(
        info.delivered.consumer_sequence, 2,
        "the one deferral is the one redelivery"
    );
    assert_eq!(deployed.counted("grow", "dead_letter").await, 0.0);
    assert!(deployed.dead_letters("grow").await.is_empty());
}

#[tokio::test]
async fn an_idle_durable_holds_nothing_in_the_ledger() {
    let env = budgeted(&[("WASM_MEMORY_BUDGET_MB", "64")]);
    let Some(deployed) = deployed_with(&[("grow", on_success(json!({})))], &env).await else {
        return;
    };
    until("the pump is pulling", async || {
        deployed.consumer("grow").await.num_waiting >= 1
    })
    .await;
    assert_eq!(
        deployed
            .platform
            .metric(
                "cufflink_wasm_memory_in_use_bytes",
                &[("class", "background")]
            )
            .await,
        Some(0.0)
    );
}

#[derive(Debug, PartialEq)]
struct Told {
    handler: String,
    attempt: i64,
    subject: String,
    stream: String,
    consumer: String,
    sequence: i64,
    msg_id: Option<String>,
}
