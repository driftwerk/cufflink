use cufflink_testkit as common;

use async_nats::jetstream::consumer;
use async_nats::jetstream::{self};
use common::jetstream::tenant_stream;
use common::{infra, until_loops, PlatformProcess, Tenant, TestDb};
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_test_guests::Profile;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

const FAST: &[(&str, &str)] = &[("CONSUMER_RECONCILE_INTERVAL_SECS", "2")];

async fn jetstream(nats_url: &str) -> jetstream::Context {
    jetstream::new(async_nats::connect(nats_url).await.expect("nats"))
}

fn subscription(handler: &str) -> Value {
    json!({
        "subject": format!("dw.{{tenant}}.books.{handler}"),
        "handler": handler,
        "handler_type": "delete_cascade",
        "config": {"table": "loans", "foreign_key": "book_id"},
    })
}

fn manifest(subscriptions: Vec<Value>) -> Value {
    json!({
        "name": "loan-service",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "loans", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "book_id", "column_type": "uuid", "primary_key": false, "nullable": false,
             "auto_generate": false},
        ]}],
        "subscriptions": subscriptions,
        "publishes": [],
    })
}

async fn durable(js: &jetstream::Context, stream: &str, name: &str) -> Option<consumer::Info> {
    js.get_stream(stream)
        .await
        .ok()?
        .consumer_info(name)
        .await
        .ok()
}

async fn deployed(platform: &PlatformProcess, tenant: &Tenant, manifest: &Value) -> Value {
    let (status, body) = platform.deploy(tenant, manifest).await;
    assert_eq!(status, 200, "{body}");
    body
}

async fn until_metric(platform: &PlatformProcess, name: &str, labels: &[(&str, &str)], is: f64) {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let found = platform.metric(name, labels).await;
        if found == Some(is) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{name}{labels:?} is {found:?}, not {is}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn api(
    platform: &PlatformProcess,
    method: reqwest::Method,
    path: &str,
    authorization: &str,
    body: Option<Value>,
) -> Value {
    let (status, answer) = platform
        .send(method, path, authorization, body.as_ref())
        .await;
    assert_eq!(status, 200, "{path}: {answer}");
    answer
}

#[tokio::test]
async fn consumer_created_on_every_replica() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let a = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
    a.wait_ready().await;
    let b = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
    b.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let key = format!("ApiKey {}", tenant.api_key);
    let js = jetstream(nats_url).await;
    let stream = tenant_stream(&tenant);
    let bound = format!("bound-{}", tenant.slug);
    let mut binds = subscription("merged");
    binds["consumer"] = json!({"durable_name": bound});

    let body = deployed(&a, &tenant, &manifest(vec![subscription("deleted"), binds])).await;
    until_loops(&[&a, &b], 2.0).await.expect("in time");
    let service = format!("/api/services/{}", body["service_id"].as_str().expect("id"));

    for (body, refusal) in [
        (json!({}), "Missing 'status' field"),
        (json!({"status": 1}), "Missing 'status' field"),
        (
            json!({"status": "disabled"}),
            "Status must be 'active' or 'inactive'",
        ),
    ] {
        let refused = b
            .send(
                reqwest::Method::PATCH,
                &format!("{service}/status"),
                &key,
                Some(&body),
            )
            .await;
        assert_eq!(refused, (400, json!({"error": refusal})), "{body}");
    }
    for (status, loops) in [("inactive", 0.0), ("active", 2.0)] {
        let answer = api(
            &b,
            reqwest::Method::PATCH,
            &format!("{service}/status"),
            &key,
            Some(json!({"status": status})),
        )
        .await;
        assert_eq!(answer, json!({"status": status, "name": "loan-service"}));
        until_loops(&[&a, &b], loops).await.expect("in time");
    }

    api(&a, reqwest::Method::DELETE, &service, &key, None).await;
    until_loops(&[&a, &b], 0.0).await.expect("in time");
    assert_eq!(
        durable(&js, &stream, "loan_service_deleted").await,
        None,
        "a deleted service's durables go with it"
    );
    assert!(
        durable(&js, &stream, &bound).await.is_some(),
        "but a bound one, which the platform never deletes"
    );

    deployed(&b, &tenant, &manifest(vec![subscription("deleted")])).await;
    until_loops(&[&a, &b], 1.0).await.expect("in time");
    api(
        &a,
        reqwest::Method::DELETE,
        &format!("/api/tenants/{}", tenant.slug),
        &format!("ApiKey {}", db.mint_admin_key().await),
        None,
    )
    .await;
    until_loops(&[&a, &b], 0.0).await.expect("in time");
}

#[tokio::test]
async fn rollback_reconciles_consumers() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let a = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
    a.wait_ready().await;
    let b = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
    b.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let js = jetstream(nats_url).await;
    let stream = tenant_stream(&tenant);

    let v1 = deployed(&a, &tenant, &manifest(vec![subscription("deleted")])).await;
    let v2 = deployed(
        &a,
        &tenant,
        &manifest(vec![subscription("deleted"), subscription("merged")]),
    )
    .await;
    until_loops(&[&a, &b], 2.0).await.expect("in time");
    assert!(durable(&js, &stream, "loan_service_merged").await.is_some());

    let rolled_back = api(
        &a,
        reqwest::Method::POST,
        &format!(
            "/api/services/{}/rollback",
            v2["service_id"].as_str().expect("id")
        ),
        &format!("ApiKey {}", tenant.api_key),
        Some(json!({"target_version": v1["version"]})),
    )
    .await;
    let ops: BTreeSet<(String, String)> = rolled_back["plan"]["consumers"]
        .as_array()
        .expect("ops")
        .iter()
        .map(|op| {
            (
                op["op"].as_str().unwrap_or_default().to_string(),
                op["durable"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    assert_eq!(
        ops,
        BTreeSet::from([
            ("delete".to_string(), "loan_service_merged".to_string()),
            ("keep".to_string(), "loan_service_deleted".to_string()),
        ])
    );
    until_loops(&[&a, &b], 1.0).await.expect("in time");
    assert_eq!(durable(&js, &stream, "loan_service_merged").await, None);
    assert!(durable(&js, &stream, "loan_service_deleted")
        .await
        .is_some());

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(durable(&js, &stream, "loan_service_merged").await, None);
    until_loops(&[&a, &b], 1.0).await.expect("in time");
}

#[tokio::test]
async fn reconciler_never_recreates_bound() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let js = jetstream(nats_url).await;
    let stream_name = tenant_stream(&tenant);
    let bound = format!("bound-{}", tenant.slug);
    let mut binds = subscription("merged");
    binds["consumer"] = json!({"durable_name": bound, "max_deliver": 3, "ack_wait_secs": 45});
    let manifest = manifest(vec![subscription("deleted"), binds]);

    let body = deployed(&platform, &tenant, &manifest).await;
    assert_eq!(body["plan"]["consumers"][1]["op"], "create");
    until_loops(&[&platform], 2.0).await.expect("in time");
    let created = durable(&js, &stream_name, &bound).await.expect("durable");
    assert_eq!(
        (created.config.max_deliver, created.config.ack_wait),
        (3, Duration::from_secs(45))
    );
    assert!(
        !created
            .config
            .metadata
            .keys()
            .any(|key| key.starts_with("cufflink.")),
        "a bound durable is given no metadata"
    );

    let stream = js.get_stream(&stream_name).await.expect("stream");
    for name in [bound.as_str(), "loan_service_deleted"] {
        stream.delete_consumer(name).await.expect("delete");
    }
    let missing = [
        ("tenant", tenant.slug.as_str()),
        ("service", "loan-service"),
        ("durable", bound.as_str()),
    ];
    until_metric(&platform, "cufflink_consumer_missing", &missing, 1.0).await;
    let deadline = Instant::now() + Duration::from_secs(40);
    while durable(&js, &stream_name, "loan_service_deleted")
        .await
        .is_none()
    {
        assert!(
            Instant::now() < deadline,
            "a durable the platform manages and the active version names is created again"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    until_loops(&[&platform], 1.0).await.expect("in time");
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        durable(&js, &stream_name, &bound).await,
        None,
        "a bound durable is not: it would read its stream from the start"
    );

    let (status, again) = platform.deploy(&tenant, &manifest).await;
    assert_eq!((status, &again["skipped"]), (200, &json!(true)), "{again}");
    let recreated = durable(&js, &stream_name, &bound)
        .await
        .expect("deploying the service creates it, as declared");
    assert_eq!(
        (recreated.config.max_deliver, recreated.config.ack_wait),
        (3, Duration::from_secs(45))
    );
    until_loops(&[&platform], 2.0).await.expect("in time");
    until_metric(&platform, "cufflink_consumer_missing", &missing, 0.0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rename_through_a_deploy_loses_nothing() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let Some(wasm) = common::sdk::sdk_guest("eventing", Profile::Debug) else {
        return;
    };
    const MESSAGES: i64 = 200;
    let mut db: TestDb = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let js = jetstream(nats_url).await;
    let column = |name: &str, column_type: &str| {
        json!({"name": name, "column_type": column_type, "primary_key": false,
               "nullable": true, "auto_generate": false})
    };
    let mail = |naming: &str| {
        json!({
            "name": "mail-service",
            "version": "0.1.0",
            "mode": "wasm",
            "consumers": naming,
            "tables": [{"name": "deliveries", "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
                 "auto_generate": true},
                column("handler", "text"), column("body", "text"),
                column("attempt", "biginteger"), column("subject", "text"),
                column("stream", "text"), column("consumer", "text"),
                column("sequence", "biginteger"), column("msg_id", "text"),
            ]}],
            "subscriptions": [{
                "subject": "dw.{tenant}.mail.succeed", "handler": "succeed",
                "handler_type": "wasm", "ack": "on_success",
            }],
            "publishes": [],
        })
    };
    let (status, body) = platform
        .deploy_module(&tenant, &mail("legacy"), &wasm)
        .await;
    assert_eq!(status, 200, "{body}");
    until_loops(&[&platform], 1.0).await.expect("in time");

    let publisher = tokio::spawn({
        let (js, subject) = (js.clone(), format!("dw.{}.mail.succeed", tenant.slug));
        async move {
            let mut every = tokio::time::interval(Duration::from_millis(20));
            for n in 1..=MESSAGES {
                every.tick().await;
                js.publish(subject.clone(), format!(r#"{{"n":{n}}}"#).into())
                    .await
                    .expect("publish")
                    .await
                    .expect("stored");
            }
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (status, renamed) = platform
        .deploy_module(&tenant, &mail("scoped"), &wasm)
        .await;
    assert_eq!(status, 200, "{renamed}");
    let handover = &renamed["plan"]["consumers"][0];
    assert_eq!(
        (&handover["op"], &handover["from"], &handover["mode"]),
        (
            &json!("handover"),
            &json!("mail_service_succeed"),
            &json!("create_then_delete")
        ),
        "{renamed}"
    );
    assert!(handover["start_sequence"].as_u64().is_some_and(|at| at > 1));
    let scoped = handover["to"].as_str().expect("successor").to_string();
    publisher.await.expect("publisher");

    let stream = tenant_stream(&tenant);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = durable(&js, &stream, &scoped).await.expect("successor");
        if info.num_pending == 0 && info.num_ack_pending == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "never caught up: {info:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(durable(&js, &stream, "mail_service_succeed").await, None);
    until_loops(&[&platform], 1.0).await.expect("in time");

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
    let counted = rows
        .query_one(
            "SELECT count(DISTINCT body), count(*) FROM mail_service.deliveries",
            &[],
        )
        .await
        .expect("deliveries");
    let (handled, deliveries): (i64, i64) = (counted.get(0), counted.get(1));
    assert_eq!(handled, MESSAGES, "every message was handled");
    assert!(
        deliveries - handled <= 50,
        "{} were handled twice: more than were in flight at the handover",
        deliveries - handled
    );
    let recorded: i64 = db
        .pool()
        .get()
        .await
        .expect("platform database")
        .query_one(
            "SELECT count(*) FROM consumer_handovers WHERE from_name = 'mail_service_succeed' \
             AND to_name = $1 AND start_sequence = $2",
            &[&scoped, &handover["start_sequence"].as_i64()],
        )
        .await
        .expect("handovers")
        .get(0);
    assert_eq!(recorded, 1);
}

#[tokio::test]
async fn lost_streams_come_back_with_their_managed_durables() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let js = jetstream(nats_url).await;
    let id = uuid::Uuid::new_v4().simple().to_string()[..10].to_string();
    let (declared, prefix) = (format!("BOOKS_{}", id.to_uppercase()), format!("books{id}"));
    api(
        &platform,
        reqwest::Method::PUT,
        &format!("/api/tenants/{}/nats-claims", tenant.slug),
        &format!("ApiKey {}", db.mint_admin_key().await),
        Some(json!({"subjects": [format!("{prefix}.>")], "streams": [declared]})),
    )
    .await;
    let own = tenant_stream(&tenant);
    let bound = format!("bound-{}", tenant.slug);
    let mut binds = subscription("merged");
    binds["consumer"] = json!({"durable_name": bound});
    let mut claimed = subscription("claimed");
    claimed["subject"] = json!(format!("{prefix}.claimed"));
    claimed["stream"] = json!(declared);
    let mut manifest = manifest(vec![subscription("deleted"), binds, claimed]);
    manifest["streams"] = json!([{
        "name": declared, "subjects": [format!("{prefix}.>")], "retention": "limits",
    }]);
    deployed(&platform, &tenant, &manifest).await;
    until_loops(&[&platform], 3.0).await.expect("in time");

    for stream in [&own, &declared] {
        js.delete_stream(stream).await.expect("lost");
    }
    let missing = [
        ("tenant", tenant.slug.as_str()),
        ("service", "loan-service"),
        ("durable", bound.as_str()),
    ];
    until_metric(&platform, "cufflink_consumer_missing", &missing, 1.0).await;
    for (stream, name) in [
        (&own, "loan_service_deleted"),
        (&declared, "loan_service_claimed"),
    ] {
        let deadline = Instant::now() + Duration::from_secs(40);
        while durable(&js, stream, name).await.is_none() {
            assert!(
                Instant::now() < deadline,
                "{name} on {stream} never came back"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    until_loops(&[&platform], 2.0).await.expect("in time");
    assert_eq!(
        durable(&js, &own, &bound).await,
        None,
        "a bound durable is the deploy's to bring back"
    );
    js.delete_stream(&declared).await.expect("delete stream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bound_durable_is_shared_with_the_legacy_consumer_still_pulling() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let Some(wasm) = common::sdk::sdk_guest("eventing", Profile::Debug) else {
        return;
    };
    const MESSAGES: i64 = 100;
    let mut db: TestDb = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let js = jetstream(nats_url).await;
    let stream_name = tenant_stream(&tenant);
    let subject = format!("dw.{}.mail.succeed", tenant.slug);
    let legacy = format!("legacy-{}", tenant.slug);
    let column = |name: &str, column_type: &str| {
        json!({"name": name, "column_type": column_type, "primary_key": false,
               "nullable": true, "auto_generate": false})
    };
    let mail = json!({
        "name": "mail-service",
        "version": "0.1.0",
        "mode": "wasm",
        "tables": [{"name": "deliveries", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            column("handler", "text"), column("body", "text"),
            column("attempt", "biginteger"), column("subject", "text"),
            column("stream", "text"), column("consumer", "text"),
            column("sequence", "biginteger"), column("msg_id", "text"),
        ]}],
        "subscriptions": [{
            "subject": "dw.{tenant}.mail.succeed", "handler": "succeed",
            "handler_type": "wasm",
            "consumer": {"durable_name": legacy},
        }],
        "publishes": [],
    });
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![format!("dw.{}.>", tenant.slug)],
            retention: async_nats::jetstream::stream::RetentionPolicy::Interest,
            ..Default::default()
        })
        .await
        .expect("the tenant's stream");
    stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(legacy.clone()),
            filter_subject: subject.clone(),
            ack_policy: consumer::AckPolicy::Explicit,
            max_deliver: 3,
            ack_wait: Duration::from_secs(30),
            max_ack_pending: 1000,
            ..Default::default()
        })
        .await
        .expect("the legacy consumer's durable");
    let as_it_was = durable(&js, &stream_name, &legacy).await.expect("durable");
    let (status, body) = platform.deploy_module(&tenant, &mail, &wasm).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (
            &body["plan"]["consumers"][0]["op"],
            &body["plan"]["consumers"][0]["durable"]
        ),
        (&json!("keep"), &json!(legacy)),
        "{body}"
    );
    until_loops(&[&platform], 1.0).await.expect("in time");

    let stop = tokio_util::sync::CancellationToken::new();
    let puller = tokio::spawn({
        let (stream, legacy, stop) = (stream.clone(), legacy.clone(), stop.clone());
        async move {
            let consumer = stream
                .get_consumer::<consumer::pull::Config>(&legacy)
                .await
                .expect("the legacy consumer's durable");
            let mut pulled = Vec::new();
            while !stop.is_cancelled() {
                let mut batch = consumer
                    .fetch()
                    .max_messages(5)
                    .expires(Duration::from_millis(200))
                    .messages()
                    .await
                    .expect("fetch");
                while let Some(message) = futures_util::StreamExt::next(&mut batch).await {
                    let message = message.expect("message");
                    pulled.push(String::from_utf8_lossy(&message.payload).to_string());
                    message.ack().await.expect("ack");
                }
            }
            pulled
        }
    });
    for n in 1..=MESSAGES {
        js.publish(subject.clone(), format!(r#"{{"n":{n}}}"#).into())
            .await
            .expect("publish")
            .await
            .expect("stored");
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = durable(&js, &stream_name, &legacy).await.expect("durable");
        if info.num_pending == 0 && info.num_ack_pending == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "never drained: {info:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.cancel();
    let pulled = puller.await.expect("puller");

    let handled: Vec<String> = cufflink_db::build_pool(
        &infra::with_dbname(
            &infra.database_url,
            &TenantPoolManager::managed_db_name(&tenant.slug),
        ),
        1,
    )
    .expect("tenant pool")
    .get()
    .await
    .expect("tenant database")
    .query("SELECT body FROM mail_service.deliveries", &[])
    .await
    .expect("deliveries")
    .iter()
    .map(|row| row.get(0))
    .collect();
    let every: BTreeSet<String> = handled.iter().chain(&pulled).cloned().collect();
    assert_eq!(
        every.len() as i64,
        MESSAGES,
        "every message reached one of them: the platform {} and the legacy consumer {}",
        handled.len(),
        pulled.len()
    );
    assert_eq!(
        durable(&js, &stream_name, &legacy)
            .await
            .expect("bound")
            .config,
        as_it_was.config,
        "the platform changes nothing of a durable it shares"
    );
}
