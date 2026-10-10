use cufflink_testkit as common;

use async_nats::jetstream::{self, consumer::pull};
use common::{infra, PlatformProcess, Tenant, TestDb};
use cufflink_platform::nats_manager::dlq::{
    dlq_subject, publish_dead_letter, DeadLetter, DlqKind, DLQ_STREAM, SUBJECT_HEADER,
};
use cufflink_platform::nats_manager::pump::MessageMeta;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const SERVICE: &str = "mail-service";

async fn jetstream(nats_url: &str) -> jetstream::Context {
    jetstream::new(async_nats::connect(nats_url).await.expect("nats"))
}

fn column(name: &str, column_type: &str) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": true,
           "auto_generate": false})
}

fn manifest() -> Value {
    json!({
        "name": SERVICE,
        "version": "0.1.0",
        "mode": "wasm",
        "tables": [{"name": "deliveries", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            column("handler", "text"), column("body", "text"), column("attempt", "biginteger"),
            column("subject", "text"), column("stream", "text"), column("consumer", "text"),
            column("sequence", "biginteger"), column("msg_id", "text"),
        ]}],
        "subscriptions": [{
            "subject": "dw.{tenant}.mail.fail_twice",
            "handler": "fail_twice",
            "handler_type": "wasm",
            "ack": "on_success",
            "consumer": {"max_deliver": 1},
        }],
        "publishes": [],
    })
}

struct Queue {
    platform: PlatformProcess,
    tenant: Tenant,
    other: Tenant,
    admin: String,
    js: jetstream::Context,
    rows: deadpool_postgres::Object,
    service_id: String,
    db: TestDb,
}

async fn deployed() -> Option<Queue> {
    let infra = common::infra().await?;
    let nats_url = infra.require_nats()?;
    let wasm = common::sdk::sdk_guest("eventing", Profile::Debug)?;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let other = db.mint_tenant().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let (status, body) = platform.deploy_module(&tenant, &manifest(), &wasm).await;
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
    Some(Queue {
        platform,
        js: jetstream(nats_url).await,
        service_id: body["service_id"].as_str().expect("id").to_string(),
        tenant,
        other,
        admin,
        rows,
        db,
    })
}

impl Queue {
    fn key(&self, tenant: &Tenant) -> String {
        format!("ApiKey {}", tenant.api_key)
    }

    async fn call(&self, method: Method, path: &str, authorization: &str) -> (u16, Value) {
        let response = self
            .platform
            .http
            .request(method, format!("{}{path}", self.platform.base_url))
            .header("authorization", authorization)
            .header("x-tenant-slug", &self.tenant.slug)
            .send()
            .await
            .expect("request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn list(&self, slug: &str, authorization: &str, query: &str) -> (u16, Value) {
        self.call(
            Method::GET,
            &format!("/api/tenants/{slug}/dlq{query}"),
            authorization,
        )
        .await
    }

    async fn publish(&self, payload: &'static str) {
        self.js
            .publish(
                format!("dw.{}.mail.fail_twice", self.tenant.slug),
                payload.into(),
            )
            .await
            .expect("publish")
            .await
            .expect("stored");
    }

    async fn file(
        &self,
        letter: String,
        original: &str,
        stream: &str,
        consumer: &str,
        sequence: u64,
        payload: String,
    ) {
        let letter = DeadLetter::now(
            letter,
            payload.into(),
            &MessageMeta {
                subject: original.into(),
                stream: stream.into(),
                consumer: consumer.into(),
                stream_sequence: sequence,
                consumer_sequence: sequence,
                delivered: 3,
                pending: 0,
                published: chrono::Utc::now(),
                headers: Vec::new(),
            },
            "handler answered 500",
        );
        publish_dead_letter(&self.js, &letter)
            .await
            .expect("stored");
    }

    async fn replay(&self, sequence: u64) -> (u16, Value) {
        self.call(
            Method::POST,
            &format!("/api/tenants/{}/dlq/{sequence}/replay", self.tenant.slug),
            &self.key(&self.tenant),
        )
        .await
    }

    async fn deliveries(&self) -> i64 {
        self.rows
            .query_one("SELECT count(*) FROM mail_service.deliveries", &[])
            .await
            .expect("deliveries")
            .get(0)
    }

    async fn until_letters(&self, count: usize) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let (status, body) = self
                .list(&self.tenant.slug, &self.key(&self.tenant), "")
                .await;
            assert_eq!(status, 200, "{body}");
            let letters = body["letters"].as_array().cloned().unwrap_or_default();
            if letters.len() == count {
                return letters;
            }
            assert!(
                Instant::now() < deadline,
                "{} letters, not {count}: {body}",
                letters.len()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn until_delivered(&self, count: i64) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while self.deliveries().await < count {
            assert!(Instant::now() < deadline, "never delivered {count} times");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::test]
async fn a_tenant_lists_replays_and_drops_its_own_letters_and_no_others() {
    let Some(queue) = deployed().await else {
        return;
    };
    queue.publish(r#"{"n":1}"#).await;
    queue.until_delivered(1).await;
    let letters = queue.until_letters(1).await;
    let letter = &letters[0];
    assert_eq!(letter["tenant"], queue.tenant.slug);
    assert_eq!(letter["service"], SERVICE);
    assert_eq!(letter["kind"], "sub");
    assert_eq!(letter["handler"], "fail_twice");
    assert_eq!(letter["deliveries"], 1);
    assert_eq!(letter["reason"], "handler answered 500");
    assert_eq!(letter["payload"], json!({"n": 1}));
    assert_eq!(
        letter["original_subject"],
        format!("dw.{}.mail.fail_twice", queue.tenant.slug)
    );
    let sequence = letter["sequence"].as_u64().expect("sequence");

    let (status, body) = queue
        .list(
            &queue.tenant.slug,
            &queue.key(&queue.tenant),
            "?service=mail-service&limit=1",
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["letters"].as_array().map(Vec::len), Some(1));
    let (status, body) = queue
        .list(
            &queue.tenant.slug,
            &queue.key(&queue.tenant),
            "?service=other-service",
        )
        .await;
    assert_eq!(
        (status, body["letters"].as_array().map(Vec::len)),
        (200, Some(0))
    );

    for (method, path) in [
        (
            Method::GET,
            format!("/api/tenants/{}/dlq", queue.tenant.slug),
        ),
        (
            Method::POST,
            format!("/api/tenants/{}/dlq/{sequence}/replay", queue.tenant.slug),
        ),
        (
            Method::DELETE,
            format!("/api/tenants/{}/dlq/{sequence}", queue.tenant.slug),
        ),
    ] {
        let (status, body) = queue
            .call(method.clone(), &path, &queue.key(&queue.other))
            .await;
        assert_eq!(status, 403, "another tenant, {method} {path}: {body}");
    }
    for (method, path) in [
        (
            Method::GET,
            format!("/api/tenants/{}/dlq", queue.other.slug),
        ),
        (
            Method::POST,
            format!("/api/tenants/{}/dlq/{sequence}/replay", queue.other.slug),
        ),
        (
            Method::DELETE,
            format!("/api/tenants/{}/dlq/{sequence}", queue.other.slug),
        ),
    ] {
        let (status, body) = queue
            .call(method.clone(), &path, &queue.key(&queue.other))
            .await;
        assert!(
            status == 404 || (status == 200 && body["letters"] == json!([])),
            "a letter is not another tenant's to reach by its sequence, {method} {path}: {status} {body}"
        );
    }
    let (status, body) = queue.list("_", &queue.key(&queue.tenant), "").await;
    assert_eq!(status, 403, "{body}");
    let (status, body) = queue.list("_", &queue.admin, "").await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body["letters"]
            .as_array()
            .is_some_and(|letters| letters.iter().all(|letter| letter["tenant"] == "_")),
        "the letters of no tenant, and no tenant's: {body}"
    );

    let (status, body) = queue.replay(sequence).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["replayed"], true);
    assert_eq!(
        body["delivered"]["stream"],
        format!("DW_{}", queue.tenant.slug.to_uppercase().replace('-', "_"))
    );
    queue.until_delivered(2).await;
    let (status, body) = queue.replay(sequence).await;
    assert_eq!(status, 404, "replayed once: {body}");
    let letters = queue.until_letters(1).await;
    let again = letters[0]["sequence"].as_u64().expect("sequence");
    assert_ne!(
        again, sequence,
        "the replay was given up on again, as a new letter"
    );
    assert_eq!(letters[0]["deliveries"], 1);

    let (status, body) = queue
        .call(
            Method::DELETE,
            &format!("/api/tenants/{}/dlq/{again}", queue.tenant.slug),
            &queue.key(&queue.tenant),
        )
        .await;
    assert_eq!((status, &body["dropped"]), (200, &json!(true)), "{body}");
    queue.until_letters(0).await;
    assert_eq!(
        queue.deliveries().await,
        2,
        "a dropped letter is not delivered"
    );

    let audited: i64 = queue
        .db
        .pool()
        .get()
        .await
        .expect("platform database")
        .query_one(
            "SELECT count(*) FROM audit_log WHERE tenant_id = $1 AND resource_type = 'dead_letter' \
             AND action IN ('dlq.replay', 'dlq.drop')",
            &[&queue.tenant.id],
        )
        .await
        .expect("audit")
        .get(0);
    assert_eq!(audited, 2);
}

#[tokio::test]
async fn a_letter_of_a_subject_outside_the_tenant_is_not_replayed() {
    let Some(queue) = deployed().await else {
        return;
    };
    queue
        .file(
            dlq_subject(&queue.tenant.slug, SERVICE, DlqKind::Subscription, "sweep"),
            "cufflink.tick.5min",
            "CUFFLINK_TICKS",
            &format!("mail_service_sweep_{}", queue.tenant.slug),
            5,
            "{}".into(),
        )
        .await;
    let letters = queue.until_letters(1).await;
    assert_eq!(letters[0]["original_subject"], "cufflink.tick.5min");
    let sequence = letters[0]["sequence"].as_u64().expect("sequence");
    let (status, body) = queue.replay(sequence).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("not a subject of tenant")),
        "{body}"
    );
    let stream = queue.js.get_stream(DLQ_STREAM).await.expect("queue");
    let kept = stream.get_raw_message(sequence).await.expect("kept");
    assert_eq!(
        kept.headers.get(SUBJECT_HEADER).map(|v| v.as_str()),
        Some("cufflink.tick.5min")
    );
}

#[tokio::test]
async fn a_jobs_letter_names_the_retry_of_the_job() {
    let Some(queue) = deployed().await else {
        return;
    };
    let job = uuid::Uuid::new_v4().to_string();
    queue
        .file(
            dlq_subject(&queue.tenant.slug, SERVICE, DlqKind::Job, "export"),
            &format!("cufflink.jobs.{}.{SERVICE}.export", queue.tenant.slug),
            "CUFFLINK_LONG_JOBS",
            &job,
            3,
            json!({"job_id": job, "handler": "export", "attempts": 3}).to_string(),
        )
        .await;
    let letters = queue.until_letters(1).await;
    assert_eq!(letters[0]["kind"], "job");
    let sequence = letters[0]["sequence"].as_u64().expect("sequence");
    let (status, body) = queue.replay(sequence).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains(&job) && e.contains("_jobs/{id}/retry")),
        "{body}"
    );
    queue.until_letters(1).await;
}

#[tokio::test]
async fn a_replay_the_stream_would_not_deliver_keeps_the_letter() {
    let Some(queue) = deployed().await else {
        return;
    };
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "order-1");
    queue
        .js
        .publish_with_headers(
            format!("dw.{}.mail.fail_twice", queue.tenant.slug),
            headers,
            r#"{"n":1}"#.into(),
        )
        .await
        .expect("publish")
        .await
        .expect("stored");
    queue.until_delivered(1).await;
    let letters = queue.until_letters(1).await;
    assert_eq!(letters[0]["headers"]["Nats-Msg-Id"], json!(["order-1"]));
    let sequence = letters[0]["sequence"].as_u64().expect("sequence");

    let (status, body) = queue.replay(sequence).await;
    assert_eq!(status, 409, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("duplicate window")),
        "{body}"
    );
    let kept = queue.until_letters(1).await;
    assert_eq!(kept[0]["sequence"], sequence, "the letter it was");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(queue.deliveries().await, 1, "nothing was delivered again");
}

#[tokio::test]
async fn a_deep_queue_answers_its_newest_letters() {
    const LETTERS: u64 = 1_300;
    let Some(queue) = deployed().await else {
        return;
    };
    for n in 1..=LETTERS {
        let (tenant, service) = match n % 10 {
            0 => (&queue.other.slug, SERVICE),
            5 => (&queue.tenant.slug, "other-service"),
            _ => (&queue.tenant.slug, SERVICE),
        };
        queue
            .file(
                dlq_subject(tenant, service, DlqKind::Subscription, "deep"),
                &format!("dw.{tenant}.mail.deep"),
                "DW_DEEP",
                &format!("deep_{}", queue.tenant.slug),
                n,
                json!({"n": n}).to_string(),
            )
            .await;
    }
    let numbers = |body: &Value| -> Vec<u64> {
        body["letters"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|letter| letter["payload"]["n"].as_u64())
            .collect()
    };
    let key = queue.key(&queue.tenant);

    let (status, body) = queue.list(&queue.tenant.slug, &key, "?limit=4").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        numbers(&body),
        [1_299, 1_298, 1_297, 1_296],
        "the newest first of a queue too deep to read through, and none of another tenant"
    );
    let (status, body) = queue
        .list(&queue.tenant.slug, &key, "?service=other-service&limit=3")
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        numbers(&body),
        [1_295, 1_285, 1_275],
        "a queue read through"
    );
    let (status, body) = queue.list(&queue.tenant.slug, &key, "?limit=200").await;
    assert_eq!(status, 200, "{body}");
    let newest = numbers(&body);
    assert_eq!(newest.len(), 200);
    assert!(
        newest.windows(2).all(|pair| pair[0] > pair[1]),
        "{newest:?}"
    );
    assert_eq!(newest.first(), Some(&1_299));

    let readers = queue
        .js
        .get_stream(DLQ_STREAM)
        .await
        .expect("queue")
        .get_info()
        .await
        .expect("info")
        .state
        .consumer_count;
    assert!(
        readers < 32,
        "{readers} readers are left: a listing closes those it opened"
    );
}

#[tokio::test]
async fn the_letters_of_no_tenant_are_an_admins() {
    let Some(queue) = deployed().await else {
        return;
    };
    let unowned = dlq_subject("", "forged", DlqKind::Hook, "after_create");
    assert_eq!(unowned, "cufflink.dlq._.forged.hook.after_create");
    queue
        .file(
            unowned,
            "cufflink.wasm.async.forged.after_create",
            "CUFFLINK_WASM_JOBS",
            &format!("wasm-worker-{}", queue.tenant.slug),
            9,
            "{}".into(),
        )
        .await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let ours = format!("wasm-worker-{}", queue.tenant.slug);
    let letter = loop {
        let (status, body) = queue.list("_", &queue.admin, "?limit=200").await;
        assert_eq!(status, 200, "{body}");
        let letters = body["letters"].as_array().cloned().unwrap_or_default();
        assert!(letters.iter().all(|letter| letter["tenant"] == "_"));
        if let Some(letter) = letters
            .into_iter()
            .find(|letter| letter["consumer"] == ours)
        {
            break letter;
        }
        assert!(Instant::now() < deadline, "{body}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let sequence = letter["sequence"].as_u64().expect("sequence");
    let (status, body) = queue
        .list(&queue.tenant.slug, &queue.key(&queue.tenant), "")
        .await;
    assert_eq!(
        (status, &body["letters"]),
        (200, &json!([])),
        "not a tenant's: {body}"
    );
    let (status, body) = queue
        .call(
            Method::POST,
            &format!("/api/tenants/_/dlq/{sequence}/replay"),
            &queue.admin,
        )
        .await;
    assert_eq!(status, 422, "nobody's to replay: {body}");
    let (status, body) = queue
        .call(
            Method::DELETE,
            &format!("/api/tenants/_/dlq/{sequence}"),
            &queue.admin,
        )
        .await;
    assert_eq!((status, &body["dropped"]), (200, &json!(true)), "{body}");
}

#[tokio::test]
async fn a_services_durables_are_listed_as_the_server_has_them() {
    let Some(queue) = deployed().await else {
        return;
    };
    queue.publish(r#"{"n":1}"#).await;
    queue.until_delivered(1).await;
    queue.until_letters(1).await;
    let (status, body) = queue
        .call(
            Method::GET,
            &format!("/api/services/{}/consumers", queue.service_id),
            &queue.key(&queue.tenant),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["service"], SERVICE);
    let consumer = &body["consumers"][0];
    assert_eq!(consumer["handler"], "fail_twice");
    assert_eq!(consumer["durable"], "mail_service_fail_twice");
    assert_eq!(consumer["bound"], false);
    assert_eq!(consumer["running_here"], true);
    assert_eq!(consumer["server"]["max_deliver"], 1);
    assert_eq!(consumer["server"]["num_pending"], 0);
    assert_eq!(consumer["server"]["ack_floor"], 1);
    assert_eq!(body["handovers"], json!([]));

    let (status, body) = queue
        .call(
            Method::GET,
            &format!("/api/services/{}/consumers", queue.service_id),
            &queue.key(&queue.other),
        )
        .await;
    assert_eq!(status, 404, "another tenant's service: {body}");

    let stream = queue
        .js
        .get_stream(format!(
            "DW_{}",
            queue.tenant.slug.to_uppercase().replace('-', "_")
        ))
        .await
        .expect("stream");
    let _reader: jetstream::consumer::Consumer<pull::Config> = stream
        .get_consumer("mail_service_fail_twice")
        .await
        .expect("the durable the listing names");
}

#[tokio::test]
async fn a_letter_whose_audit_record_fails_is_still_replayed_and_dropped() {
    let Some(queue) = deployed().await else {
        return;
    };
    queue.publish(r#"{"n":1}"#).await;
    queue.until_delivered(1).await;
    let first = queue.until_letters(1).await[0]["sequence"]
        .as_u64()
        .expect("sequence");
    queue
        .db
        .pool()
        .get()
        .await
        .expect("platform database")
        .batch_execute("DROP TABLE audit_log")
        .await
        .expect("no audit log");

    let (status, body) = queue.replay(first).await;
    assert_eq!(status, 200, "{body}");
    queue.until_delivered(2).await;
    let second = queue.until_letters(1).await[0]["sequence"]
        .as_u64()
        .expect("the replay failed again");
    assert_ne!(second, first);
    let (status, body) = queue
        .call(
            Method::DELETE,
            &format!("/api/tenants/{}/dlq/{second}", queue.tenant.slug),
            &queue.key(&queue.tenant),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    queue.until_letters(0).await;
    let reported = queue
        .platform
        .output_tail()
        .matches("A dead letter's audit record could not be written")
        .count();
    assert_eq!(reported, 2, "the replay and the drop");
}
