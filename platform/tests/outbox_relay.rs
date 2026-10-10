use cufflink_testkit as common;

use async_nats::jetstream::{self, consumer::DeliverPolicy, stream};
use common::infra::Infra;
use common::outbox::{audit_records, dead_letters, forge, identity, seal, Filed};
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::outbox::{OutboxKind, OutboxRow, OutboxSeal, OutboxWriter};
use cufflink_platform::secrets::KeyRing;
use cufflink_platform::tenant_sessions::{Purpose, TenantSessions};
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

const SERVICE: &str = "notes";

fn short_id() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

struct Relayed {
    db: TestDb,
    admin: String,
    sessions: TenantSessions,
    nats: async_nats::Client,
    js: jetstream::Context,
    nats_url: String,
}

impl Relayed {
    async fn start(infra: &Infra) -> Option<Self> {
        let nats_url = infra.require_nats()?.to_string();
        let db = infra.create_migrated_database().await;
        let nats = async_nats::connect(&nats_url).await.expect("nats");
        Some(Self {
            admin: format!("ApiKey {}", db.mint_admin_key().await),
            sessions: common::outbox::sessions(&db, &nats),
            js: jetstream::new(nats.clone()),
            nats,
            db,
            nats_url,
        })
    }

    async fn platform(&self, extra: &[(&str, &str)]) -> PlatformProcess {
        let platform = PlatformProcess::start(&self.db.url, Some(&self.nats_url), extra).await;
        platform.wait_ready().await;
        platform
    }

    async fn put(&self, platform: &PlatformProcess, path: &str, body: Value) {
        let (status, answer) = platform
            .send(Method::PUT, path, &self.admin, Some(&body))
            .await;
        assert_eq!(status, 200, "PUT {path}: {answer}");
    }

    async fn set_outbox(&self, platform: &PlatformProcess, tenant: &Tenant, mode: &str) {
        self.put(
            platform,
            &format!("/api/tenants/{}/policies", tenant.slug),
            json!({"policies": {"outbox": {"mode": mode}}}),
        )
        .await;
    }

    async fn claim(
        &self,
        platform: &PlatformProcess,
        tenant: &Tenant,
        subject: &str,
        stream: &str,
    ) {
        self.put(
            platform,
            &format!("/api/tenants/{}/nats-claims", tenant.slug),
            json!({"subjects": [subject], "streams": [stream]}),
        )
        .await;
    }

    async fn write(&self, tenant: &Tenant, rows: &[OutboxRow]) {
        let mut session = self
            .sessions
            .acquire(&identity(tenant, SERVICE), Purpose::Crud, None)
            .await
            .expect("session");
        let tx = session.transaction().await.expect("begin");
        OutboxWriter::new(&tx, tenant.id, &tenant.slug, &seal())
            .write(rows)
            .await
            .expect("write");
        tx.commit().await.expect("commit");
    }

    async fn held(&self, tenant: &Tenant) -> Vec<(i64, i32, Option<String>)> {
        self.sessions
            .platform(tenant.id, None)
            .await
            .expect("connection")
            .query(
                "SELECT id, attempts, last_error FROM _cufflink.outbox ORDER BY id",
                &[],
            )
            .await
            .expect("outbox")
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect()
    }

    async fn leased(&self, tenant: &Tenant) -> bool {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM outbox_relay_leases WHERE tenant_id = $1)",
                &[&tenant.id],
            )
            .await
            .expect("leases")
            .get(0)
    }

    async fn limits_stream(&self, name: &str, subjects: &str) -> stream::Stream {
        self.js
            .create_stream(stream::Config {
                name: name.into(),
                subjects: vec![subjects.into()],
                ..Default::default()
            })
            .await
            .expect("stream")
    }
}

struct Gate {
    url: String,
    open: tokio::sync::watch::Sender<bool>,
}

impl Gate {
    async fn before(nats_url: &str) -> Self {
        let server = nats_url.trim_start_matches("nats://").to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("nats://{}", listener.local_addr().expect("addr"));
        let (open, is_open) = tokio::sync::watch::channel(true);
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let (server, mut is_open) = (server.clone(), is_open.clone());
                tokio::spawn(async move {
                    if !*is_open.borrow() {
                        return;
                    }
                    let Ok(mut outbound) = tokio::net::TcpStream::connect(&server).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = is_open.wait_for(|open| !open) => {}
                    }
                });
            }
        });
        Self { url, open }
    }

    fn set(&self, open: bool) {
        self.open.send_replace(open);
    }
}

fn kek(byte: u8) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode([byte; 32])
}

fn event(subject: &str, n: usize) -> OutboxRow {
    OutboxRow::new(
        OutboxKind::Event,
        SERVICE,
        subject.to_string(),
        json!({ "n": n }).to_string().into_bytes(),
        vec![("traceparent".into(), format!("00-{n:032x}-{n:016x}-01"))],
    )
}

async fn eventually<F, Fut>(what: &str, within: Duration, mut done: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + within;
    while !done().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: not within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn stored(stream: &stream::Stream, count: usize) -> Vec<(String, Value, String)> {
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .expect("consumer");
    let mut messages = consumer.messages().await.expect("messages");
    let mut found = Vec::with_capacity(count);
    while found.len() < count {
        let message = tokio::time::timeout(Duration::from_secs(10), messages.next())
            .await
            .expect("a message within 10 s")
            .expect("the stream of messages")
            .expect("a message");
        found.push((
            message.subject.to_string(),
            serde_json::from_slice(&message.payload).expect("json"),
            message
                .headers
                .as_ref()
                .and_then(|headers| headers.get("Nats-Msg-Id"))
                .map(|id| id.to_string())
                .unwrap_or_default(),
        ));
        message.ack().await.expect("ack");
    }
    found
}

async fn message_count(stream: &mut stream::Stream) -> u64 {
    stream.info().await.expect("stream info").state.messages
}

#[tokio::test]
async fn jetstream_publish_to_core_only_subject_times_out() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let subject = format!("dw.{}.notes.notes.created", tenant.slug);
    let mut heard = test
        .nats
        .subscribe(format!("dw.{}.>", tenant.slug))
        .await
        .expect("subscribe");

    let mut impatient = jetstream::new(test.nats.clone());
    impatient.set_timeout(Duration::from_secs(1));
    let unanswered = impatient
        .publish(subject.clone(), "{}".into())
        .await
        .expect("sent")
        .await
        .expect_err("no stream captures the subject");
    assert_eq!(
        unanswered.kind(),
        jetstream::context::PublishErrorKind::TimedOut,
        "with a listener on the subject the server does not say that nobody answers: {unanswered}"
    );
    let probe = heard.next().await.expect("the listener heard the publish");
    assert_eq!(probe.subject.as_str(), subject);

    let platform = test.platform(&[("OUTBOX_RELAY_POLL_MS", "60000")]).await;
    test.set_outbox(&platform, &tenant, "enforce").await;
    eventually("the replica books", Duration::from_secs(20), || async {
        platform
            .metric("cufflink_outbox_relay_leader", &[("tenant", &tenant.slug)])
            .await
            == Some(1.0)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let rows: Vec<OutboxRow> = (1..=5).map(|n| event(&subject, n)).collect();
    test.write(&tenant, &rows).await;

    for row in &rows {
        let message = tokio::time::timeout(Duration::from_secs(10), heard.next())
            .await
            .expect("relayed within 10 s, a poll being a minute away")
            .expect("a message");
        assert_eq!(message.payload.as_ref(), row.payload.as_slice());
        let headers = message.headers.expect("headers");
        assert_eq!(
            headers.get("Nats-Msg-Id").map(|id| id.to_string()),
            Some(row.msg_id.to_string())
        );
        assert_eq!(
            headers.get("traceparent").map(|value| value.to_string()),
            Some(row.headers[0].1.clone())
        );
    }
    eventually("the outbox is empty", Duration::from_secs(10), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;
    let again = tokio::time::timeout(Duration::from_millis(500), heard.next()).await;
    assert!(again.is_err(), "each row is published once: {again:?}");
    assert_eq!(
        platform
            .metric(
                "cufflink_outbox_published_total",
                &[
                    ("tenant", &tenant.slug),
                    ("route", "core"),
                    ("result", "ok")
                ]
            )
            .await,
        Some(5.0)
    );
}

#[tokio::test]
async fn a_row_that_fails_is_tried_again_before_any_written_after_it() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let id = short_id();
    let (name, prefix) = (format!("IT_FULL_{}", id.to_uppercase()), format!("it_{id}"));
    let full = stream::Config {
        name: name.clone(),
        subjects: vec![format!("{prefix}.>")],
        max_messages: 2,
        discard: stream::DiscardPolicy::New,
        ..Default::default()
    };
    let mut stream = test.js.create_stream(full.clone()).await.expect("stream");

    let platform = test.platform(&[]).await;
    test.claim(&platform, &tenant, &format!("{prefix}.>"), &name)
        .await;
    let subject = format!("{prefix}.created");
    let rows: Vec<OutboxRow> = (1..=6).map(|n| event(&subject, n)).collect();
    test.set_outbox(&platform, &tenant, "enforce").await;
    test.write(&tenant, &rows[..3]).await;
    test.write(&tenant, &rows[3..]).await;

    eventually(
        "the stream took what it has room for",
        Duration::from_secs(30),
        || async { test.held(&tenant).await.len() == 4 },
    )
    .await;
    eventually(
        "the row the stream refused waits",
        Duration::from_secs(10),
        || async {
            test.held(&tenant)
                .await
                .first()
                .is_some_and(|row| row.1 > 0)
        },
    )
    .await;
    let held = test.held(&tenant).await;
    let ids: Vec<i64> = held.iter().map(|row| row.0).collect();
    assert_eq!(ids, [3, 4, 5, 6]);
    assert!(
        held[0]
            .2
            .as_deref()
            .is_some_and(|error| error.contains("maximum messages exceeded")),
        "{held:?}"
    );
    assert_eq!(message_count(&mut stream).await, 2);

    test.js
        .update_stream(stream::Config {
            max_messages: -1,
            ..full
        })
        .await
        .expect("room");
    eventually("the outbox is empty", Duration::from_secs(30), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;
    let published = stored(&stream, 6).await;
    let expected: Vec<(String, Value, String)> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            (
                subject.clone(),
                json!({"n": index + 1}),
                row.msg_id.to_string(),
            )
        })
        .collect();
    assert_eq!(published, expected, "in the order they were written");
    assert_eq!(message_count(&mut stream).await, 6);
}

#[tokio::test]
async fn a_row_the_tenant_may_not_publish_and_one_out_of_attempts_are_dead_lettered() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let (tenant, other) = (test.db.mint_tenant().await, test.db.mint_tenant().await);
    let id = short_id();
    let (name, prefix) = (format!("IT_FULL_{}", id.to_uppercase()), format!("it_{id}"));
    test.js
        .create_stream(stream::Config {
            name: name.clone(),
            subjects: vec![format!("{prefix}.>")],
            max_messages: 1,
            discard: stream::DiscardPolicy::New,
            ..Default::default()
        })
        .await
        .expect("stream");
    let mut foreign = test
        .nats
        .subscribe(format!("dw.{}.>", other.slug))
        .await
        .expect("subscribe");

    let platform = test.platform(&[("OUTBOX_MAX_ATTEMPTS", "1")]).await;
    test.claim(&platform, &tenant, &format!("{prefix}.>"), &name)
        .await;
    let rows = [
        event(&format!("dw.{}.notes.notes.created", other.slug), 1),
        OutboxRow {
            kind: OutboxKind::Audit,
            ..event(&format!("cufflink.audit.{}.notes", other.slug), 2)
        },
        event(&format!("{prefix}.created"), 3),
        event(&format!("{prefix}.created"), 4),
    ];
    test.set_outbox(&platform, &tenant, "enforce").await;
    test.write(&tenant, &rows).await;
    eventually("the outbox is empty", Duration::from_secs(30), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;

    let filed: Vec<(String, String, String, Value)> = dead_letters(&test.js, &tenant, 3)
        .await
        .into_iter()
        .map(|letter| {
            (
                letter.subject,
                letter.original,
                letter.stream,
                letter.payload,
            )
        })
        .collect();
    let slug = &tenant.slug;
    assert_eq!(
        filed,
        [
            (
                format!("cufflink.dlq.{slug}.notes.outbox.rejected"),
                rows[0].subject.clone(),
                "_cufflink.outbox".to_string(),
                json!({"n": 1}),
            ),
            (
                format!("cufflink.dlq.{slug}.notes.outbox.rejected"),
                rows[1].subject.clone(),
                "_cufflink.outbox".to_string(),
                json!({"n": 2}),
            ),
            (
                format!("cufflink.dlq.{slug}.notes.outbox.event"),
                rows[3].subject.clone(),
                "_cufflink.outbox".to_string(),
                json!({"n": 4}),
            ),
        ]
    );
    let overheard = tokio::time::timeout(Duration::from_millis(500), foreign.next()).await;
    assert!(
        overheard.is_err(),
        "nothing reached the other tenant: {overheard:?}"
    );
    for (reason, count) in [("rejected", 2.0), ("exhausted", 1.0)] {
        assert_eq!(
            platform
                .metric(
                    "cufflink_outbox_dead_lettered_total",
                    &[("tenant", slug), ("reason", reason)]
                )
                .await,
            Some(count),
            "{reason}"
        );
    }
}

#[tokio::test]
async fn a_row_the_server_cannot_carry_is_given_up_and_the_rows_behind_it_are_published() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let mut stream = test
        .limits_stream(
            &format!("IT_LARGE_{}", short_id().to_uppercase()),
            &format!("dw.{}.>", tenant.slug),
        )
        .await;
    let max_payload = test.nats.server_info().max_payload;
    assert!(max_payload > 0, "the server says what it carries");
    let subject = format!("dw.{}.notes.notes.created", tenant.slug);
    let oversized = OutboxRow {
        payload: format!("{{\"n\":2,\"pad\":\"{}\"}}", "x".repeat(max_payload)).into_bytes(),
        ..event(&subject, 2)
    };
    let rows = [event(&subject, 1), oversized.clone(), event(&subject, 3)];

    let platform = test.platform(&[]).await;
    test.set_outbox(&platform, &tenant, "enforce").await;
    test.write(&tenant, &rows).await;
    eventually("the outbox is empty", Duration::from_secs(30), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;
    assert_eq!(message_count(&mut stream).await, 2);
    let published: Vec<Value> = stored(&stream, 2)
        .await
        .into_iter()
        .map(|(_, payload, _)| payload["n"].clone())
        .collect();
    assert_eq!(published, [json!(1), json!(3)]);

    let letters = dead_letters(&test.js, &tenant, 1).await;
    let Filed {
        subject: filed_under,
        original,
        reason,
        payload: reference,
        ..
    } = &letters[0];
    assert_eq!(
        filed_under,
        &format!("cufflink.dlq.{}.notes.outbox.rejected", tenant.slug)
    );
    assert_eq!(original, &subject);
    assert!(
        reason.contains(&format!(
            "more than the {max_payload} bytes the server carries in one message"
        )) && reason.ends_with(&format!(
            "; the payload of {} bytes is not stored",
            oversized.payload.len()
        )),
        "{reason}"
    );
    assert_eq!(reference["truncated"], true);
    assert_eq!(reference["msg_id"], json!(oversized.msg_id));
    assert_eq!(reference["subject"], subject);
    assert_eq!(reference["payload_bytes"], oversized.payload.len());
    assert!(reference["payload_sha256"]
        .as_str()
        .is_some_and(|hex| hex.len() == 64));
    for (metric, labels, count) in [
        (
            "cufflink_outbox_dead_lettered_total",
            vec![("tenant", tenant.slug.as_str()), ("reason", "rejected")],
            1.0,
        ),
        (
            "cufflink_outbox_dead_letter_truncated_total",
            vec![("tenant", tenant.slug.as_str())],
            1.0,
        ),
    ] {
        assert_eq!(
            platform.metric(metric, &labels).await,
            Some(count),
            "{metric}"
        );
    }
}

fn orders_with_notes() -> Value {
    let mut manifest = orders();
    manifest["tables"][0]["columns"]
        .as_array_mut()
        .expect("columns")
        .push(
            json!({"name": "note", "column_type": "text", "primary_key": false,
                     "nullable": true, "auto_generate": false}),
        );
    manifest
}

#[tokio::test]
async fn a_write_whose_event_the_server_cannot_carry_is_refused_before_it_is_written() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let platform = test.platform(&[]).await;
    let (status, deployed) = platform.deploy(&tenant, &orders_with_notes()).await;
    assert_eq!(status, 200, "{deployed}");
    let max_payload = test.nats.server_info().max_payload;
    let key = format!("ApiKey {}", tenant.api_key);
    let path = format!("/svc/{}/shop/orders", tenant.slug);
    let large = json!({"n": 1, "note": "x".repeat(max_payload)});
    let written = || async {
        test.sessions
            .platform(tenant.id, None)
            .await
            .expect("connection")
            .query_one("SELECT count(*) FROM shop.orders", &[])
            .await
            .expect("count")
            .get::<_, i64>(0)
    };

    let (status, answer) = platform.send(Method::POST, &path, &key, Some(&large)).await;
    assert_eq!(
        status, 200,
        "with the outbox off the event is dropped, as it always was: {answer}"
    );
    assert_eq!(written().await, 1);

    test.set_outbox(&platform, &tenant, "enforce").await;
    let (status, refused) = platform.send(Method::POST, &path, &key, Some(&large)).await;
    assert_eq!(status, 413, "{refused}");
    let error = refused["error"].as_str().unwrap_or_default();
    assert!(
        error.starts_with("the event of this write is ")
            && error.contains(&format!(
                "more than the {max_payload} bytes the NATS server carries in one message"
            )),
        "{error}"
    );
    assert_eq!(written().await, 1, "nothing was written");
    assert!(test.held(&tenant).await.is_empty());
    let (status, small) = platform
        .send(
            Method::POST,
            &path,
            &key,
            Some(&json!({"n": 2, "note": "small"})),
        )
        .await;
    assert_eq!(status, 200, "{small}");
    assert_eq!(written().await, 2);
}

async fn heard_until_quiet(
    heard: &mut async_nats::Subscriber,
    expected: usize,
    quiet: Duration,
) -> Vec<(String, Value)> {
    let mut messages = Vec::new();
    loop {
        let patience = if messages.len() < expected {
            Duration::from_secs(30)
        } else {
            quiet
        };
        match tokio::time::timeout(patience, heard.next()).await {
            Ok(Some(message)) => messages.push((
                message.subject.to_string(),
                serde_json::from_slice(&message.payload).expect("json"),
            )),
            _ => return messages,
        }
    }
}

#[tokio::test]
async fn a_row_deleted_twice_at_once_is_published_as_deleted_once() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let platform = test.platform(&[]).await;
    let mut slow_to_delete = common::wasm_manifest("shop", &[]);
    slow_to_delete["tables"][0]["hooks"] = json!({"before_delete": "sleep2"});
    let (status, deployed) = platform.deploy(&tenant, &slow_to_delete).await;
    assert_eq!(status, 200, "{deployed}");
    let mut heard = test
        .nats
        .subscribe(format!("dw.{}.>", tenant.slug))
        .await
        .expect("subscribe");
    let key = format!("ApiKey {}", tenant.api_key);
    let items = format!("/svc/{}/shop/items", tenant.slug);

    for (mode, answers, published) in [("off", [200, 200], 2), ("enforce", [200, 404], 1)] {
        test.set_outbox(&platform, &tenant, mode).await;
        let (status, created) = platform
            .send(Method::POST, &items, &key, Some(&json!({"title": mode})))
            .await;
        assert_eq!(status, 200, "{mode}: {created}");
        let row = format!("{items}/{}", created["id"].as_str().expect("an id"));
        let (first, second) = tokio::join!(
            platform.send(Method::DELETE, &row, &key, None),
            platform.send(Method::DELETE, &row, &key, None),
        );
        let mut statuses = [first.0, second.0];
        statuses.sort_unstable();
        assert_eq!(statuses, answers, "{mode}: {first:?}, {second:?}");

        let messages =
            heard_until_quiet(&mut heard, 2 + 2 * published, Duration::from_secs(3)).await;
        let count = |subject: String, action: &str| {
            messages
                .iter()
                .filter(|(heard, payload)| *heard == subject && payload["action"] == action)
                .count()
        };
        let event = |action: &str| format!("dw.{}.shop.items.{action}", tenant.slug);
        let audit = || format!("dw.{}.logs", tenant.slug);
        assert_eq!(
            (count(event("created"), "created"), count(audit(), "create")),
            (1, 1),
            "{mode}: {messages:?}"
        );
        assert_eq!(
            (count(event("deleted"), "deleted"), count(audit(), "delete")),
            (published, published),
            "{mode}: {messages:?}"
        );
        assert_eq!(messages.len(), 2 + 2 * published, "{mode}: {messages:?}");
    }
    assert!(test.held(&tenant).await.is_empty());
}

async fn leader(replicas: &[&PlatformProcess], tenant: &Tenant) -> Vec<usize> {
    let mut leading = Vec::new();
    for (index, replica) in replicas.iter().enumerate() {
        let books = replica
            .metric("cufflink_outbox_relay_leader", &[("tenant", &tenant.slug)])
            .await;
        if books == Some(1.0) {
            leading.push(index);
        }
    }
    leading
}

#[tokio::test]
async fn outbox_relay_single_leader_and_failover() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenants = [test.db.mint_tenant().await, test.db.mint_tenant().await];
    let mut streams = Vec::new();
    for tenant in &tenants {
        let name = format!("IT_RELAY_{}", short_id().to_uppercase());
        streams.push(
            test.limits_stream(&name, &format!("dw.{}.>", tenant.slug))
                .await,
        );
    }
    let (first, second) = (test.platform(&[]).await, test.platform(&[]).await);
    for tenant in &tenants {
        test.set_outbox(&first, tenant, "enforce").await;
    }
    for tenant in &tenants {
        eventually("one replica books", Duration::from_secs(20), || async {
            leader(&[&first, &second], tenant).await.len() == 1
        })
        .await;
    }
    let subject = |tenant: &Tenant| format!("dw.{}.notes.notes.created", tenant.slug);
    for (tenant, stream) in tenants.iter().zip(&mut streams) {
        let rows: Vec<OutboxRow> = (1..=20).map(|n| event(&subject(tenant), n)).collect();
        test.write(tenant, &rows).await;
        eventually("the outbox is empty", Duration::from_secs(20), || async {
            test.held(tenant).await.is_empty()
        })
        .await;
        assert_eq!(message_count(stream).await, 20);
        assert_eq!(leader(&[&first, &second], tenant).await.len(), 1);
    }

    let tenant = &tenants[0];
    let leading = leader(&[&first, &second], tenant).await[0];
    let (hung, standby) = match leading {
        0 => (&first, &second),
        _ => (&second, &first),
    };
    hung.sigstop();
    let stopped = tokio::time::Instant::now();
    let rows: Vec<OutboxRow> = (21..=40).map(|n| event(&subject(tenant), n)).collect();
    test.write(tenant, &rows).await;
    eventually(
        "the standby books once the lease ran out",
        Duration::from_secs(20),
        || async { leader(&[standby], tenant).await.len() == 1 },
    )
    .await;
    assert!(
        stopped.elapsed() >= Duration::from_secs(5),
        "a lease that is renewed is not taken: {:?}",
        stopped.elapsed()
    );
    eventually("the outbox is empty", Duration::from_secs(20), || async {
        test.held(tenant).await.is_empty()
    })
    .await;
    hung.sigcont();
    eventually(
        "the replica that hung follows",
        Duration::from_secs(20),
        || async { leader(&[hung], tenant).await.is_empty() },
    )
    .await;
    assert_eq!(leader(&[standby], tenant).await.len(), 1);
    assert_eq!(message_count(&mut streams[0]).await, 40);
    let published = stored(&streams[0], 40).await;
    let numbers: Vec<Value> = published
        .iter()
        .map(|message| message.1["n"].clone())
        .collect();
    let expected: Vec<Value> = (1..=40).map(|n| json!(n)).collect();
    assert_eq!(numbers, expected, "each row once, in the order written");

    standby.sigterm();
    let handed_over = tokio::time::Instant::now();
    eventually(
        "a replica that shuts down hands over",
        Duration::from_secs(10),
        || async { leader(&[hung], tenant).await.len() == 1 },
    )
    .await;
    assert!(
        handed_over.elapsed() < Duration::from_secs(10),
        "sooner than a lease runs out"
    );
    assert!(test.leased(tenant).await);
}

#[tokio::test]
async fn an_outbox_written_no_more_is_drained_and_then_let_go() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let mut stream = test
        .limits_stream(
            &format!("IT_RELAY_{}", short_id().to_uppercase()),
            &format!("dw.{}.>", tenant.slug),
        )
        .await;
    let subject = format!("dw.{}.notes.notes.created", tenant.slug);

    let mut platform = test.platform(&[]).await;
    test.set_outbox(&platform, &tenant, "enforce").await;
    eventually("the replica books", Duration::from_secs(20), || async {
        leader(&[&platform], &tenant).await.len() == 1
    })
    .await;
    test.set_outbox(&platform, &tenant, "off").await;
    platform.sigterm();
    platform
        .wait_exit(Duration::from_secs(30))
        .await
        .expect("the platform exits");
    drop(platform);
    assert!(
        test.leased(&tenant).await,
        "a replica that shuts down leaves the lease for the next"
    );

    let rows: Vec<OutboxRow> = (1..=3).map(|n| event(&subject, n)).collect();
    test.write(&tenant, &rows).await;

    let platform = test.platform(&[]).await;
    eventually("the outbox is empty", Duration::from_secs(30), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;
    assert_eq!(message_count(&mut stream).await, 3);
    assert!(
        test.leased(&tenant).await,
        "not before the outbox stayed empty"
    );
    eventually(
        "the relay ends and gives the lease up",
        Duration::from_secs(60),
        || async { !test.leased(&tenant).await },
    )
    .await;
    assert_eq!(leader(&[&platform], &tenant).await, Vec::<usize>::new());
}

fn orders() -> Value {
    json!({
        "name": "shop",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "orders", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "n", "column_type": "integer", "primary_key": false, "nullable": false,
             "auto_generate": false},
        ]}],
        "publishes": [],
    })
}

#[tokio::test]
async fn outbox_atomic_with_row() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let mut stream = test
        .limits_stream(
            &format!("IT_ATOMIC_{}", short_id().to_uppercase()),
            &format!("dw.{}.>", tenant.slug),
        )
        .await;
    let gate = Gate::before(&test.nats_url).await;
    let platform = PlatformProcess::start(&test.db.url, Some(&gate.url), &[]).await;
    platform.wait_ready().await;
    let (status, deployed) = platform.deploy(&tenant, &orders()).await;
    assert_eq!(status, 200, "{deployed}");
    let (_, version) = platform.api_get(&tenant, "/api/version").await;
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("outbox"))),
        "{version}"
    );
    test.set_outbox(&platform, &tenant, "enforce").await;
    eventually("the replica books", Duration::from_secs(20), || async {
        leader(&[&platform], &tenant).await.len() == 1
    })
    .await;

    gate.set(false);
    let key = format!("ApiKey {}", tenant.api_key);
    let path = format!("/svc/{}/shop/orders", tenant.slug);
    let began = tokio::time::Instant::now();
    for n in 1..=50 {
        let (status, created) = platform
            .send(Method::POST, &path, &key, Some(&json!({ "n": n })))
            .await;
        assert_eq!(status, 200, "{n}: {created}");
    }
    assert!(
        began.elapsed() < Duration::from_secs(30),
        "a write waits for no server: {:?}",
        began.elapsed()
    );
    let (status, refused) = platform
        .send(Method::POST, &path, &key, Some(&json!({})))
        .await;
    assert_eq!(
        status, 422,
        "the database refuses a row without n: {refused}"
    );
    let database = test
        .sessions
        .platform(tenant.id, None)
        .await
        .expect("connection");
    database
        .batch_execute(
            "CREATE FUNCTION _cufflink.closed() RETURNS trigger LANGUAGE plpgsql AS
                 $$ BEGIN RAISE EXCEPTION 'the outbox is closed'; END $$;
             CREATE TRIGGER closed BEFORE INSERT ON _cufflink.outbox
                 FOR EACH STATEMENT EXECUTE FUNCTION _cufflink.closed()",
        )
        .await
        .expect("an outbox that takes nothing");
    let (status, refused) = platform
        .send(Method::POST, &path, &key, Some(&json!({ "n": 51 })))
        .await;
    assert_eq!(
        status, 500,
        "the outbox refuses what the inserted row publishes: {refused}"
    );
    database
        .batch_execute("DROP TRIGGER closed ON _cufflink.outbox; DROP FUNCTION _cufflink.closed()")
        .await
        .expect("an outbox as it was");
    let orders: Vec<i32> = database
        .query("SELECT n FROM shop.orders ORDER BY n", &[])
        .await
        .expect("orders")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        orders,
        (1..=50).collect::<Vec<i32>>(),
        "a row is not kept without what it publishes"
    );
    drop(database);
    assert_eq!(
        test.held(&tenant).await.len(),
        150,
        "an event and its two audit records for every row that was written, and for no other"
    );
    assert_eq!(message_count(&mut stream).await, 0);

    gate.set(true);
    eventually("the outbox is empty", Duration::from_secs(90), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;
    assert_eq!(message_count(&mut stream).await, 100);
    let published = stored(&stream, 100).await;
    let in_order: Vec<(String, Value)> = published
        .iter()
        .map(|(subject, payload, _)| (subject.clone(), payload["data"]["n"].clone()))
        .collect();
    let expected: Vec<(String, Value)> = (1..=50)
        .flat_map(|n| {
            [
                (format!("dw.{}.shop.orders.created", tenant.slug), json!(n)),
                (format!("dw.{}.logs", tenant.slug), json!(n)),
            ]
        })
        .collect();
    assert_eq!(in_order, expected, "in the order the rows were committed");

    let kept = test
        .js
        .get_stream("CUFFLINK_AUDIT")
        .await
        .expect("the stream of durable records")
        .create_consumer(jetstream::consumer::pull::Config {
            filter_subject: format!("cufflink.audit.{}.shop", tenant.slug),
            ..Default::default()
        })
        .await
        .expect("consumer");
    let mut records = kept.messages().await.expect("messages");
    let mut durable = Vec::with_capacity(50);
    while durable.len() < 50 {
        let record = tokio::time::timeout(Duration::from_secs(10), records.next())
            .await
            .expect("a durable record within 10 s")
            .expect("the stream of records")
            .expect("a record");
        let payload: Value = serde_json::from_slice(&record.payload).expect("json");
        durable.push((payload["v"].clone(), payload["data"]["n"].clone()));
        record.ack().await.expect("ack");
    }
    let expected: Vec<(Value, Value)> = (1..=50).map(|n| (json!(2), json!(n))).collect();
    assert_eq!(
        durable, expected,
        "every record is kept, in the order written"
    );
}

#[tokio::test]
async fn a_guest_forges_no_audit_record() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let platform = test.platform(&[]).await;
    test.set_outbox(&platform, &tenant, "enforce").await;
    let slug = &tenant.slug;
    let audit = |n: usize, subject: &str| OutboxRow {
        kind: OutboxKind::Audit,
        ..event(subject, n)
    };
    let (legacy, durable) = (
        format!("dw.{slug}.logs"),
        format!("cufflink.audit.{slug}.{SERVICE}"),
    );
    let mut heard = test
        .nats
        .subscribe(legacy.clone())
        .await
        .expect("subscribe");
    forge(
        &test.sessions,
        &tenant,
        SERVICE,
        &[
            audit(1, &legacy),
            audit(2, &durable),
            OutboxRow {
                payload: json!({"n": 3}).to_string().into_bytes(),
                ..seal().seal(tenant.id, audit(0, &durable))
            },
            seal().seal(Uuid::new_v4(), audit(4, &durable)),
        ],
    )
    .await;
    test.write(&tenant, &[audit(5, &durable)]).await;
    eventually("the outbox is empty", Duration::from_secs(30), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;

    let kept: Vec<Value> = audit_records(&test.js, &tenant, 1)
        .await
        .into_iter()
        .map(|record| record["n"].clone())
        .collect();
    assert_eq!(kept, [json!(5)], "the platform's record alone is kept");
    let overheard = tokio::time::timeout(Duration::from_millis(500), heard.next()).await;
    assert!(overheard.is_err(), "no forged legacy record: {overheard:?}");
    let filed: Vec<(String, Value, bool)> = dead_letters(&test.js, &tenant, 4)
        .await
        .into_iter()
        .map(|letter| {
            let told = letter.reason.contains("carries no MAC")
                || letter.reason.contains("does not match");
            (letter.subject, letter.payload["n"].clone(), told)
        })
        .collect();
    let unauthenticated = format!("cufflink.dlq.{slug}.{SERVICE}.outbox.unauthenticated");
    assert_eq!(
        filed,
        (1..=4)
            .map(|n| (unauthenticated.clone(), json!(n), true))
            .collect::<Vec<_>>()
    );
    for (metric, labels) in [
        (
            "cufflink_outbox_rows_unauthenticated_total",
            [("tenant", slug.as_str()), ("kind", "audit")],
        ),
        (
            "cufflink_outbox_dead_lettered_total",
            [("tenant", slug.as_str()), ("reason", "unauthenticated")],
        ),
    ] {
        assert_eq!(
            platform.metric(metric, &labels).await,
            Some(4.0),
            "{metric}"
        );
    }
}

#[tokio::test]
async fn a_row_sealed_before_a_rotation_is_relayed_while_its_key_is_given() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let tenant = test.db.mint_tenant().await;
    let (old, new, retired) = (kek(1), kek(2), kek(3));
    let ring = |kek: &str, id: &str| KeyRing::parse(Some(kek), Some(id), None).expect("ring");
    let previous = format!("old:{old}");
    let platform = test
        .platform(&[
            ("CUFFLINK_SECRETS_KEK", new.as_str()),
            ("CUFFLINK_SECRETS_KEK_ID", "new"),
            ("CUFFLINK_SECRETS_KEK_PREVIOUS", previous.as_str()),
        ])
        .await;
    test.set_outbox(&platform, &tenant, "enforce").await;
    let subject = format!("cufflink.audit.{}.{SERVICE}", tenant.slug);
    let record = |n: usize, under: &str, id: &str| {
        OutboxSeal::of(&ring(under, id), None)
            .expect("a seal")
            .seal(
                tenant.id,
                OutboxRow {
                    kind: OutboxKind::Audit,
                    ..event(&subject, n)
                },
            )
    };
    forge(
        &test.sessions,
        &tenant,
        SERVICE,
        &[
            record(1, &old, "old"),
            record(2, &retired, "retired"),
            record(3, &new, "new"),
        ],
    )
    .await;
    eventually("the outbox is empty", Duration::from_secs(30), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;
    let kept: Vec<Value> = audit_records(&test.js, &tenant, 2)
        .await
        .into_iter()
        .map(|record| record["n"].clone())
        .collect();
    assert_eq!(kept, [json!(1), json!(3)]);
    let letters = dead_letters(&test.js, &tenant, 1).await;
    assert_eq!(
        (letters[0].subject.as_str(), &letters[0].payload),
        (
            format!(
                "cufflink.dlq.{}.{SERVICE}.outbox.unauthenticated",
                tenant.slug
            )
            .as_str(),
            &json!({"n": 2})
        )
    );
}

#[tokio::test]
async fn cf3_a_relay_pass_with_rows_exports_its_tenant_alone() {
    const POLL: Duration = Duration::from_millis(50);
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(mut test) = Relayed::start(infra).await else {
        return;
    };
    let capture = common::otlp::Capture::start().await.expect("capture");
    let tenant = test.db.mint_tenant().await;
    let poll = POLL.as_millis().to_string();
    let settings: Vec<_> = [("OUTBOX_RELAY_POLL_MS", poll.as_str())]
        .into_iter()
        .chain(capture.env())
        .collect();
    let platform = test.platform(&settings).await;
    test.set_outbox(&platform, &tenant, "enforce").await;
    let books = [("tenant", tenant.slug.as_str())];
    eventually("the relay books", Duration::from_secs(30), || async {
        platform
            .metric("cufflink_outbox_relay_leader", &books)
            .await
            == Some(1.0)
    })
    .await;
    let passes = || {
        capture
            .spans()
            .into_iter()
            .filter(|span| span.text == "outbox_relay")
            .collect::<Vec<_>>()
    };
    tokio::time::sleep(POLL * 40).await;
    assert_eq!(passes(), [], "forty polls of an empty outbox");

    let subject = format!("dw.{}.notes.notes.created", tenant.slug);
    test.write(&tenant, &[event(&subject, 1)]).await;
    eventually("the outbox is empty", Duration::from_secs(30), || async {
        test.held(&tenant).await.is_empty()
    })
    .await;
    let pass = capture
        .span("the pass that relayed the row", |span| {
            span.text == "outbox_relay"
        })
        .await
        .expect("exported");
    assert_eq!(pass.scope(), [("cufflink.tenant", tenant.slug.as_str())]);
    assert_eq!(pass.parent_span_id, "", "a pass is its own trace");
    tokio::time::sleep(POLL * 40).await;
    assert_eq!(passes().len(), 1, "one pass had rows");
}
