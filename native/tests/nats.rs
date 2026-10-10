mod common;

use async_nats::{Client, Request, RequestErrorKind};
use common::{hold, in_ms, request, Serving};
use cufflink_native::prelude::*;
use cufflink_native::{NatsAuth, NatsProvider, Service};
use cufflink_types::ext::call_subject;
use std::io::Write;
use std::time::Duration;

fn nats_url() -> Option<String> {
    let url = std::env::var("CUFFLINK_TEST_NATS_URL")
        .ok()
        .filter(|url| !url.is_empty());
    if url.is_none() {
        let reason = "no NATS (CUFFLINK_TEST_NATS_URL unset)";
        assert!(
            std::env::var_os("CUFFLINK_CI").is_none_or(|ci| ci.is_empty()),
            "test skipped in CI: {reason}"
        );
        let _ = writeln!(std::io::stderr().lock(), "skipped: {reason}");
    }
    url
}

fn tenant() -> String {
    format!("t-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
}

fn echo(ctx: &CallContext, payload: Value) -> Result<Value> {
    Ok(json!({ "tenant": ctx.tenant, "payload": payload }))
}

fn sleep(_: &CallContext, payload: Value) -> Result<Value> {
    std::thread::sleep(Duration::from_millis(payload["ms"].as_u64().unwrap_or(0)));
    Ok(payload)
}

async fn provide(url: &str, tenant: &str, service: Service) -> Serving {
    let provider = NatsProvider::new(tenant, url, NatsAuth::Anonymous).expect("provider");
    let server = service.bind_nats(provider).await.expect("bound");
    Serving::start(|stopped| server.serve(stopped))
}

const ANSWERED: Duration = Duration::from_secs(10);
const UNANSWERED: Duration = Duration::from_millis(1_500);

async fn ask(
    client: &Client,
    subject: String,
    body: Vec<u8>,
    wait: Duration,
) -> std::result::Result<Value, RequestErrorKind> {
    client
        .send_request(
            subject,
            Request::new().payload(body.into()).timeout(Some(wait)),
        )
        .await
        .map(|reply| serde_json::from_slice(&reply.payload).expect("a JSON reply"))
        .map_err(|e| e.kind())
}

async fn call(client: &Client, tenant: &str, extension: &str, payload: Value) -> Value {
    ask(
        client,
        call_subject(tenant, extension),
        request(tenant, extension, payload, in_ms(30_000)),
        ANSWERED,
    )
    .await
    .expect("answered")
}

#[tokio::test]
async fn a_provider_answers_its_tenants_calls_on_their_subjects() {
    let Some(url) = nats_url() else {
        return;
    };
    let tenant = tenant();
    let serving = provide(
        &url,
        &tenant,
        cufflink_native::service("ext-echo")
            .extension("test.echo", echo, ["*"])
            .extension(
                "test.fail",
                |_: &CallContext, _: Value| -> Result<Value> {
                    Err(ExtError::new("quota", "daily quota exhausted"))
                },
                ["*"],
            ),
    )
    .await;
    let client = async_nats::connect(&url).await.expect("client");

    assert_eq!(
        call(&client, &tenant, "test.echo", json!({"n": 1})).await,
        json!({"ok": {"tenant": tenant, "payload": {"n": 1}}})
    );
    assert_eq!(
        call(&client, &tenant, "test.fail", json!(null)).await,
        json!({"error": {"code": "quota", "message": "daily quota exhausted"}})
    );
    let other = self::tenant();
    for (subject, body) in [
        (
            call_subject(&tenant, "test.echo"),
            request(&tenant, "test.fail", json!(null), in_ms(5_000)),
        ),
        (
            call_subject(&tenant, "test.echo"),
            request(&other, "test.echo", json!(null), in_ms(5_000)),
        ),
        (call_subject(&tenant, "test.echo"), b"not a call".to_vec()),
    ] {
        let refused = ask(&client, subject, body, ANSWERED)
            .await
            .expect("answered");
        assert_eq!(refused["error"]["code"], "invalid", "{refused}");
    }
    assert_eq!(
        ask(
            &client,
            call_subject(&other, "test.echo"),
            request(&other, "test.echo", json!(null), in_ms(30_000)),
            ANSWERED
        )
        .await,
        Err(RequestErrorKind::NoResponders),
        "a provider serves its own tenant alone"
    );

    client
        .publish(
            call_subject(&tenant, "test.echo"),
            request(&tenant, "test.echo", json!(0), in_ms(5_000)).into(),
        )
        .await
        .expect("a call nobody waits for");
    assert_eq!(
        call(&client, &tenant, "test.echo", json!(2)).await["ok"]["payload"],
        2
    );
    serving.stop().await;
}

#[tokio::test]
async fn a_caller_that_no_longer_waits_gets_no_reply() {
    let Some(url) = nats_url() else {
        return;
    };
    let tenant = tenant();
    let serving = provide(
        &url,
        &tenant,
        cufflink_native::service("ext-sleep").extension("test.sleep", sleep, ["*"]),
    )
    .await;
    let client = async_nats::connect(&url).await.expect("client");
    let subject = call_subject(&tenant, "test.sleep");
    for body in [
        request(&tenant, "test.sleep", json!({"ms": 1_000}), in_ms(200)),
        request(&tenant, "test.sleep", json!({"ms": 0}), in_ms(0) - 1_000),
    ] {
        assert_eq!(
            ask(&client, subject.clone(), body, UNANSWERED).await,
            Err(RequestErrorKind::TimedOut)
        );
    }
    assert_eq!(
        ask(
            &client,
            subject,
            request(&tenant, "test.sleep", json!({"ms": 10}), in_ms(30_000)),
            ANSWERED
        )
        .await,
        Ok(json!({"ok": {"ms": 10}}))
    );
    serving.stop().await;
}

#[tokio::test]
async fn a_stopping_provider_leaves_the_queue_group_and_answers_what_it_took() {
    let Some(url) = nats_url() else {
        return;
    };
    let tenant = tenant();
    let (hold, held) = hold();
    let serving = provide(
        &url,
        &tenant,
        cufflink_native::service("ext-hold").extension("test.hold", hold, ["*"]),
    )
    .await;
    let client = async_nats::connect(&url).await.expect("client");
    let subject = call_subject(&tenant, "test.hold");
    let in_flight = tokio::spawn({
        let (client, subject) = (client.clone(), subject.clone());
        let body = request(&tenant, "test.hold", json!("held"), in_ms(30_000));
        async move { ask(&client, subject, body, ANSWERED).await }
    });
    let release = held.started().await;

    serving.stop.send(()).expect("still serving");
    tokio::time::timeout(Duration::from_secs(5), async {
        while ask(&client, subject.clone(), b"{}".to_vec(), ANSWERED).await
            != Err(RequestErrorKind::NoResponders)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the provider leaves the queue group");
    assert!(!serving.served.is_finished());

    release.send(()).expect("the held call waits");
    assert_eq!(
        in_flight.await.expect("in-flight call"),
        Ok(json!({"ok": "held"}))
    );
    serving.served.await.expect("serve task").expect("served");
}

#[tokio::test]
async fn serving_stops_even_before_nats_is_reached() {
    let provider =
        NatsProvider::new("harbor", "nats://127.0.0.1:9", NatsAuth::Anonymous).expect("provider");
    tokio::time::timeout(
        Duration::from_secs(5),
        cufflink_native::service("ext-echo")
            .extension("test.echo", echo, ["*"])
            .serve_nats(provider, tokio::time::sleep(Duration::from_millis(100))),
    )
    .await
    .expect("stopped")
    .expect("stopped cleanly");
}
