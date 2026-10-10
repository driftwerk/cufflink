use cufflink_testkit as common;

use async_nats::jetstream;
use common::{PlatformProcess, Tenant};
use cufflink_test_guests::Profile;
use futures_util::{StreamExt, TryStreamExt};
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

const SERVICE: &str = "publisher";

struct Guest {
    platform: PlatformProcess,
    tenant: Tenant,
    js: jetstream::Context,
    nats: async_nats::Client,
    claim: String,
    _db: common::TestDb,
}

async fn deployed() -> Option<Guest> {
    let infra = common::infra().await?;
    let nats_url = infra.require_nats()?;
    let wasm = common::sdk::sdk_guest("nats-v2", Profile::Debug)?;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let claim = format!("pub{}", &Uuid::new_v4().simple().to_string()[..8]);
    let stream = claim.to_uppercase();
    let (status, body) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/nats-claims", tenant.slug),
            &format!("ApiKey {}", db.mint_admin_key().await),
            Some(&json!({"subjects": [format!("{claim}.>")], "streams": [stream]})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let routes: Vec<Value> = ["publish", "publish_core"]
        .iter()
        .map(|handler| {
            json!({
                "method": "POST",
                "path": format!("/{handler}"),
                "handler": handler,
                "posture": {"kind": "public", "reason": "integration test"},
            })
        })
        .collect();
    let manifest = json!({
        "name": SERVICE,
        "version": "0.1.0",
        "mode": "wasm",
        "tables": [{"name": "items", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
        ]}],
        "custom_routes": routes,
        "streams": [{"name": stream, "subjects": [format!("{claim}.books.>")],
                     "retention": "limits", "duplicate_window_secs": 120}],
        "publishes": [format!("{claim}.>")],
    });
    let (status, body) = platform.deploy_module(&tenant, &manifest, &wasm).await;
    assert_eq!(status, 200, "{body}");
    let nats = async_nats::connect(nats_url).await.expect("nats");
    Some(Guest {
        platform,
        js: jetstream::new(nats.clone()),
        nats,
        tenant,
        claim,
        _db: db,
    })
}

impl Guest {
    async fn call(&self, handler: &str, body: Value) -> Value {
        let response = self
            .platform
            .http
            .post(self.platform.fn_url(&self.tenant, SERVICE, handler))
            .json(&body)
            .send()
            .await
            .expect("fn request");
        assert_eq!(response.status().as_u16(), 200, "{handler}");
        response.json().await.expect("json body")
    }

    fn own(&self, subject: &str) -> String {
        format!("{}.{subject}", self.claim)
    }

    async fn publish(&self, subject: &str, msg_id: Option<&str>) -> Value {
        let mut body = json!({"subject": subject, "payload": {"n": 1}});
        if let Some(id) = msg_id {
            body["msg_id"] = json!(id);
        }
        self.call("publish", body).await
    }
}

#[tokio::test]
async fn publish_v2_ack_and_dedupe() {
    let Some(guest) = deployed().await else {
        return;
    };
    let stream_name = guest.claim.to_uppercase();
    let stream = guest
        .js
        .get_stream(&stream_name)
        .await
        .expect("the stream the manifest declares");
    let stored_on = |subject: String| {
        let stream = stream.clone();
        async move {
            stream
                .info_with_subjects(subject)
                .await
                .expect("info")
                .try_next()
                .await
                .expect("subjects")
                .map_or(0, |(_, count)| count)
        }
    };

    let first = guest
        .publish(&guest.own("books.created"), Some("book-7"))
        .await;
    assert_eq!(first["ack"]["stream"], stream_name, "{first}");
    assert_eq!(first["ack"]["duplicate"], false);
    let sequence = first["ack"]["sequence"].as_u64().expect("sequence");
    let again = guest
        .publish(&guest.own("books.created"), Some("book-7"))
        .await;
    assert_eq!(
        again["ack"],
        json!({"stream": stream_name, "sequence": sequence, "duplicate": true}),
        "{again}"
    );
    let another = guest
        .publish(&guest.own("books.created"), Some("book-8"))
        .await;
    assert!(
        another["ack"]["sequence"].as_u64() > Some(sequence),
        "another message, stored after the first: {another}"
    );
    assert_eq!(another["ack"]["duplicate"], false);
    assert_eq!(
        stored_on(guest.own("books.created")).await,
        2,
        "the repeat was stored once"
    );

    let stored = stream.get_raw_message(sequence).await.expect("the message");
    assert_eq!(&stored.payload[..], br#"{"n":1}"#);
    let headers = stored.headers;
    assert_eq!(
        headers.get("Nats-Msg-Id").map(|value| value.as_str()),
        Some("book-7")
    );

    let uncaptured = guest
        .publish(&format!("{}.orders.created", guest.claim), None)
        .await;
    assert_eq!(uncaptured["error"]["kind"], "no_stream", "{uncaptured}");
    let denied = guest.publish("dw.other-tenant.books.created", None).await;
    assert_eq!(denied["error"]["kind"], "denied", "{denied}");
    let unclaimed = guest.publish("someone.elses.subject", None).await;
    assert_eq!(unclaimed["error"]["kind"], "denied", "{unclaimed}");
    let headed = guest
        .call(
            "publish",
            json!({"subject": guest.own("books.created"), "payload": {},
                   "headers": [["Nats-Expected-Stream", "OTHER"]]}),
        )
        .await;
    assert_eq!(headed["error"]["kind"], "invalid", "{headed}");
    let too_large = guest
        .call(
            "publish",
            json!({"subject": guest.own("books.created"), "size": 2 * 1024 * 1024}),
        )
        .await;
    assert_eq!(too_large["error"]["kind"], "too_large", "{too_large}");
    assert_eq!(
        stored_on(guest.own("books.created")).await,
        2,
        "nothing refused was published"
    );
    for (result, count) in [
        ("ack", 2.0),
        ("duplicate", 1.0),
        ("no_stream", 1.0),
        ("denied", 2.0),
    ] {
        assert_eq!(
            guest
                .platform
                .metric(
                    "cufflink_nats_guest_publish_total",
                    &[("mode", "jetstream"), ("result", result)],
                )
                .await,
            Some(count),
            "{result}"
        );
    }
}

#[tokio::test]
async fn what_cannot_travel_is_refused_and_the_host_keeps_serving() {
    let Some(guest) = deployed().await else {
        return;
    };
    let subject = guest.own("books.created");
    let refused = [
        json!({"headers": [["", "v"]]}),
        json!({"headers": [["x bad", "v"]]}),
        json!({"headers": [["x:bad", "v"]]}),
        json!({"headers": [["x\rbad", "v"]]}),
        json!({"headers": [["x\nbad", "v"]]}),
        json!({"headers": [["x\u{0}bad", "v"]]}),
        json!({"headers": [["naïve", "v"]]}),
        json!({"headers": [["x-ok", "fine"], ["x-source", "a\r\nNats-Expected-Stream: OTHER"]]}),
        json!({"headers": [["x-source", "a\nb"]]}),
        json!({"headers": [["x-source", "a\u{0}b"]]}),
        json!({"headers": [["traceparent", "00-a\r\nb"]]}),
        json!({"msg_id": "a\nb"}),
        json!({"msg_id": "a\r\nNats-Rollup: all"}),
        json!({"msg_id": "a\u{0}b"}),
    ];
    for carried in &refused {
        let mut body = json!({"subject": subject, "payload": {"n": 1}});
        body.as_object_mut()
            .expect("object")
            .extend(carried.as_object().expect("object").clone());
        let answer = guest.call("publish", body).await;
        assert_eq!(answer["error"]["kind"], "invalid", "{carried}: {answer}");
        assert!(
            !answer["error"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("panicked"),
            "{answer}"
        );
    }
    let core = guest
        .call(
            "publish_core",
            json!({"subject": subject, "payload": {}, "headers": [["x bad", "v"]]}),
        )
        .await;
    assert_eq!(core["error"]["kind"], "invalid", "{core}");

    let max_payload = guest.nats.server_info().max_payload;
    let heavy = guest
        .call(
            "publish",
            json!({"subject": subject, "size": max_payload - 16,
                   "headers": [["x-source", "x".repeat(64)]]}),
        )
        .await;
    assert_eq!(
        heavy["error"]["kind"], "too_large",
        "a payload the server would take, were it not for its headers: {heavy}"
    );
    let oversized = guest
        .call(
            "publish",
            json!({"subject": subject, "payload": {},
                   "headers": [["x-source", "x".repeat(2 * max_payload)]]}),
        )
        .await;
    assert_eq!(oversized["error"]["kind"], "too_large", "{oversized}");

    let carried = guest
        .call(
            "publish",
            json!({"subject": subject, "payload": {"n": 1}, "msg_id": "book 7: naïve",
                   "headers": [["x-source", "naïve value"], ["x-source", ""]]}),
        )
        .await;
    let sequence = carried["ack"]["sequence"]
        .as_u64()
        .unwrap_or_else(|| panic!("the host serves, and publishes what can travel: {carried}"));
    let stream = guest
        .js
        .get_stream(guest.claim.to_uppercase())
        .await
        .expect("stream");
    let stored = stream
        .create_consumer(jetstream::consumer::pull::Config {
            deliver_policy: jetstream::consumer::DeliverPolicy::ByStartSequence {
                start_sequence: sequence,
            },
            ..Default::default()
        })
        .await
        .expect("reader")
        .fetch()
        .max_messages(1)
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .expect("fetch")
        .next()
        .await
        .expect("the message")
        .expect("read");
    let headers = stored.headers.clone().expect("headers");
    assert_eq!(
        headers
            .get_all("x-source")
            .map(|value| value.as_str())
            .collect::<Vec<_>>(),
        ["naïve value", ""]
    );
    assert_eq!(
        headers.get("Nats-Msg-Id").map(|value| value.as_str()),
        Some("book 7: naïve")
    );
    assert_eq!(
        stream.cached_info().state.messages,
        1,
        "nothing refused was published"
    );
    for (mode, result, count) in [
        ("jetstream", "invalid", refused.len() as f64),
        ("core", "invalid", 1.0),
        ("jetstream", "too_large", 2.0),
        ("jetstream", "ack", 1.0),
    ] {
        assert_eq!(
            guest
                .platform
                .metric(
                    "cufflink_nats_guest_publish_total",
                    &[("mode", mode), ("result", result)],
                )
                .await,
            Some(count),
            "{mode} {result}"
        );
    }
}

#[tokio::test]
async fn publish_core_reaches_a_core_subscriber() {
    let Some(guest) = deployed().await else {
        return;
    };
    let subject = format!("{}.progress.7", guest.claim);
    let mut subscriber = guest
        .nats
        .subscribe(subject.clone())
        .await
        .expect("subscribe");
    let sent = guest
        .call(
            "publish_core",
            json!({"subject": subject, "payload": {"pct": 40}}),
        )
        .await;
    assert_eq!(sent, json!({"sent": true}));
    let received = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
        .await
        .expect("delivered in time")
        .expect("a message");
    assert_eq!(&received.payload[..], br#"{"pct":40}"#);
    let headers = received.headers.expect("headers");
    assert_eq!(headers.get("x-source").map(|v| v.as_str()), Some("guest"));
    assert_eq!(
        guest
            .platform
            .metric(
                "cufflink_nats_guest_publish_total",
                &[("mode", "core"), ("result", "sent")],
            )
            .await,
        Some(1.0)
    );
}
