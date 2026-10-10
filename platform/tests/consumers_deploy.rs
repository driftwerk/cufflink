use cufflink_testkit as common;

use async_nats::jetstream::consumer::{self, pull};
use async_nats::jetstream::{self, stream};
use common::jetstream::tenant_stream;
use common::{until_loops, PlatformProcess, Tenant};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

async fn jetstream(nats_url: &str) -> jetstream::Context {
    jetstream::new(async_nats::connect(nats_url).await.expect("nats"))
}

fn manifest(subjects: &[&str]) -> Value {
    let subscriptions: Vec<Value> = subjects
        .iter()
        .map(|subject| {
            json!({
                "subject": format!("dw.{{tenant}}.books.{subject}"),
                "handler": "drop_loans",
                "handler_type": "delete_cascade",
                "config": {"table": "loans", "foreign_key": "book_id"},
            })
        })
        .collect();
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
    let stream = js.get_stream(stream).await.ok()?;
    stream.consumer_info(name).await.ok()
}

async fn deployed(platform: &PlatformProcess, tenant: &Tenant, manifest: &Value) -> Value {
    let (status, body) = platform.deploy(tenant, manifest).await;
    assert_eq!(status, 200, "{body}");
    body
}

#[tokio::test]
async fn a_deploy_and_a_rollback_bring_the_durables_to_their_manifest() {
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
    let js = jetstream(nats_url).await;
    let stream = tenant_stream(&tenant);
    const DURABLE: &str = "loan_service_drop_loans";

    let first = deployed(&platform, &tenant, &manifest(&["deleted"])).await;
    assert_eq!(
        first["plan"],
        json!({
            "schema_changes": [
                "CREATE TABLE IF NOT EXISTS \"loan_service\".\"loans\" (\"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid(), \"book_id\" UUID NOT NULL)"
            ],
            "streams": [],
            "consumers": [{
                "op": "create", "stream": stream, "durable": DURABLE,
                "handler": "drop_loans", "deliver": "all",
            }],
            "warnings": [],
        })
    );
    let created = durable(&js, &stream, DURABLE).await.expect("the durable");
    assert_eq!(
        (
            created.config.filter_subject.as_str(),
            created.config.ack_wait,
            created.config.max_deliver,
            created.config.ack_policy,
        ),
        (
            format!("dw.{}.books.deleted", tenant.slug).as_str(),
            Duration::from_secs(30),
            -1,
            consumer::AckPolicy::Explicit
        ),
        "as every durable was created"
    );
    for (key, value) in [
        ("cufflink.tenant", tenant.slug.as_str()),
        ("cufflink.service", "loan-service"),
        ("cufflink.handler", "drop_loans"),
        ("cufflink.managed", "1"),
        ("cufflink.kind", "subscription"),
    ] {
        assert_eq!(
            created.config.metadata.get(key).map(String::as_str),
            Some(value)
        );
    }
    until_loops(&[&platform], 1.0).await.expect("in time");

    let second = deployed(&platform, &tenant, &manifest(&["removed"])).await;
    assert_eq!(
        second["plan"]["consumers"],
        json!([{
            "op": "update", "stream": stream, "durable": DURABLE,
            "handler": "drop_loans", "changes": ["filters"],
        }])
    );
    let updated = durable(&js, &stream, DURABLE).await.expect("the durable");
    assert_eq!(
        updated.config.filter_subject,
        format!("dw.{}.books.removed", tenant.slug)
    );
    assert_eq!(
        updated.created, created.created,
        "a filter is changed in place: the durable is not created again, to read its stream \
         from the start"
    );
    until_loops(&[&platform], 1.0).await.expect("in time");

    let third = deployed(&platform, &tenant, &manifest(&[])).await;
    assert_eq!(
        third["plan"]["consumers"],
        json!([{"op": "delete", "stream": stream, "durable": DURABLE}])
    );
    assert_eq!(durable(&js, &stream, DURABLE).await, None);
    until_loops(&[&platform], 0.0).await.expect("in time");

    let service_id = third["service_id"].as_str().expect("service id");
    let (status, rolled_back) = platform
        .send(
            reqwest::Method::POST,
            &format!("/api/services/{service_id}/rollback"),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({"target_version": second["version"]})),
        )
        .await;
    assert_eq!(status, 200, "{rolled_back}");
    assert_eq!(
        rolled_back["plan"]["consumers"],
        json!([{
            "op": "create", "stream": stream, "durable": DURABLE,
            "handler": "drop_loans", "deliver": "all",
        }])
    );
    let restored = durable(&js, &stream, DURABLE).await.expect("the durable");
    assert_eq!(
        restored.config.filter_subject,
        format!("dw.{}.books.removed", tenant.slug)
    );
    until_loops(&[&platform], 1.0).await.expect("in time");

    let (status, rolled_back) = platform
        .send(
            reqwest::Method::POST,
            &format!("/api/services/{service_id}/rollback"),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({"target_version": third["version"]})),
        )
        .await;
    assert_eq!(status, 200, "{rolled_back}");
    assert_eq!(
        rolled_back["plan"]["consumers"],
        json!([{"op": "delete", "stream": stream, "durable": DURABLE}])
    );
    assert_eq!(durable(&js, &stream, DURABLE).await, None);
    until_loops(&[&platform], 0.0).await.expect("in time");
}

async fn planned(platform: &PlatformProcess, tenant: &Tenant, manifest: &Value) -> Value {
    use base64::Engine;
    let (status, body) = platform
        .send(
            reqwest::Method::POST,
            "/api/services/deploy?plan=true",
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({
                "manifest": manifest,
                "wasm_base64": base64::engine::general_purpose::STANDARD
                    .encode(common::guest::module_bytes()),
                "api_version": 1,
            })),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    body
}

#[tokio::test]
async fn a_plan_shows_what_a_deploy_would_do_to_the_durables_and_does_none_of_it() {
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
    let js = jetstream(nats_url).await;
    let stream = tenant_stream(&tenant);
    const DURABLE: &str = "loan_service_drop_loans";

    let first = planned(&platform, &tenant, &manifest(&["deleted"])).await;
    assert_eq!(first["new"], true, "{first}");
    assert_eq!(
        first["plan"]["consumers"],
        json!([{
            "op": "create", "stream": stream, "durable": DURABLE,
            "handler": "drop_loans", "deliver": "all",
        }]),
        "{first}"
    );
    assert_eq!(
        first["plan"]["schema_changes"].as_array().map(Vec::len),
        Some(1),
        "{first}"
    );
    assert!(durable(&js, &stream, DURABLE).await.is_none());

    deployed(&platform, &tenant, &manifest(&["deleted"])).await;
    let created = durable(&js, &stream, DURABLE).await.expect("the durable");
    until_loops(&[&platform], 1.0).await.expect("in time");

    let unchanged = planned(&platform, &tenant, &manifest(&["deleted"])).await;
    assert_eq!(
        (&unchanged["skipped"], &unchanged["plan"]["consumers"]),
        (&json!(true), &json!([])),
        "{unchanged}"
    );

    for (subjects, op) in [
        (
            &["removed"][..],
            json!({
                "op": "update", "stream": stream, "durable": DURABLE,
                "handler": "drop_loans", "changes": ["filters"],
            }),
        ),
        (
            &[][..],
            json!({"op": "delete", "stream": stream, "durable": DURABLE}),
        ),
    ] {
        let plan = planned(&platform, &tenant, &manifest(subjects)).await;
        assert_eq!(plan["skipped"], false, "{plan}");
        assert_eq!(plan["plan"]["consumers"], json!([op]), "{plan}");
        let kept = durable(&js, &stream, DURABLE).await.expect("the durable");
        assert_eq!(
            (kept.created, kept.config.filter_subject),
            (created.created, created.config.filter_subject.clone()),
            "a plan changes no durable"
        );
    }
    until_loops(&[&platform], 1.0).await.expect("in time");
}

#[tokio::test]
async fn a_durable_of_the_release_before_is_kept_as_it_is() {
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
    let js = jetstream(nats_url).await;
    let subject = format!("dw.{}.books.deleted", tenant.slug);
    let stream = js
        .create_stream(stream::Config {
            name: tenant_stream(&tenant),
            subjects: vec![format!("dw.{}.>", tenant.slug)],
            retention: stream::RetentionPolicy::Interest,
            max_age: Duration::from_secs(7 * 24 * 60 * 60),
            ..Default::default()
        })
        .await
        .expect("the tenant's stream");
    stream
        .create_consumer(pull::Config {
            durable_name: Some("loan_service_drop_loans".into()),
            filter_subject: subject.clone(),
            ..Default::default()
        })
        .await
        .expect("the durable as v0.27.0 created it");
    js.publish(subject, r#"{"data":{}}"#.into())
        .await
        .expect("publish")
        .await
        .expect("stored");
    let as_it_was = stream
        .consumer_info("loan_service_drop_loans")
        .await
        .expect("durable");

    let body = deployed(&platform, &tenant, &manifest(&["deleted"])).await;
    assert_eq!(
        body["plan"]["consumers"],
        json!([{
            "op": "keep", "stream": tenant_stream(&tenant), "durable": "loan_service_drop_loans",
        }])
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let kept = loop {
        let kept = stream
            .consumer_info("loan_service_drop_loans")
            .await
            .expect("durable");
        if kept.ack_floor.stream_sequence == 1 {
            break kept;
        }
        assert!(
            Instant::now() < deadline,
            "what was published before the deploy was never consumed: {kept:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        (kept.config, kept.created),
        (as_it_was.config, as_it_was.created)
    );
}

#[tokio::test]
async fn a_refused_deploy_changes_nothing_on_the_server() {
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
    let js = jetstream(nats_url).await;
    let stream_name = tenant_stream(&tenant);
    deployed(&platform, &tenant, &manifest(&["deleted"])).await;
    let stream = js.get_stream(&stream_name).await.expect("stream");
    stream
        .create_consumer(pull::Config {
            durable_name: Some("receipt_service_on_book".into()),
            filter_subject: format!("dw.{}.books.merged", tenant.slug),
            metadata: [
                ("cufflink.managed", "1"),
                ("cufflink.tenant", tenant.slug.as_str()),
                ("cufflink.service", "receipt-service"),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
            ..Default::default()
        })
        .await
        .expect("a durable of another service");
    let server = async || {
        let mut stream = js.get_stream(&stream_name).await.expect("stream");
        let config = stream.info().await.expect("info").config.clone();
        let mut durables: Vec<_> = stream
            .consumers()
            .map(|info| {
                let info = info.expect("durable");
                (
                    info.name,
                    info.config,
                    info.created,
                    info.delivered,
                    info.ack_floor,
                )
            })
            .collect()
            .await;
        durables.sort_by(|a, b| a.0.cmp(&b.0));
        (config, durables)
    };
    let before = server().await;

    let mut renamed = manifest(&["deleted", "merged"]);
    renamed["consumers"] = json!("scoped");
    renamed["subscriptions"][1]["consumer"] = json!({"durable_name": "receipt_service_on_book"});
    let (status, body) = platform.deploy(&tenant, &renamed).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("managed by the platform for service 'receipt-service'")),
        "{body}"
    );
    assert_eq!(server().await, before);
}
