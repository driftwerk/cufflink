use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_test_guests::Profile;
use serde_json::{json, Value};
use std::time::Duration;

fn route(path: &str, handler: &str, subject: &str, reducer: Option<&str>) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "handler": handler,
        "posture": {"kind": "public", "reason": "integration test"},
        "sse": {"subject": subject, "reducer": reducer, "anonymous": true},
    })
}

async fn read_stream(response: &mut reqwest::Response, within: Duration) -> (String, bool) {
    let deadline = tokio::time::Instant::now() + within;
    let mut seen = String::new();
    loop {
        match tokio::time::timeout_at(deadline, response.chunk()).await {
            Ok(Ok(Some(chunk))) => seen.push_str(&String::from_utf8_lossy(&chunk)),
            Ok(Ok(None)) => return (seen, true),
            Ok(Err(_)) | Err(_) => return (seen, false),
        }
    }
}

fn events(stream: &str) -> Vec<Value> {
    stream
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|data| serde_json::from_str(data).ok())
        .collect()
}

#[tokio::test]
async fn sse_bridge_progress_stream() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let Some(wasm) = common::sdk::sdk_guest("sse", Profile::Debug) else {
        return;
    };
    let mut db = infra.create_database().await;
    let capture = common::otlp::Capture::start().await.expect("capture");
    let settings: Vec<_> = [("SSE_MAX_STREAMS_PER_PRINCIPAL", "1")]
        .into_iter()
        .chain(capture.env())
        .collect();
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &settings).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let mut manifest = common::wasm_manifest("receipts", &[]);
    manifest["routing"] = json!("strict");
    manifest["custom_routes"] = json!([
        route(
            "/receipts/book/{book_id}/progress",
            "receipt_progress",
            "dw.{tenant}.receipts.pdf.progress.{book_id}",
            Some("receipt_progress_reduce"),
        ),
        route(
            "/receipts/book/{book_id}/summary",
            "not_a_stream",
            "dw.{tenant}.receipts.pdf.progress.{book_id}",
            None,
        ),
        route("/feed", "receipt_progress", "dw.{tenant}.feed", None),
        route(
            "/assets/created",
            "receipt_progress",
            "dw.{tenant}.assets.items.created",
            None
        ),
        route(
            "/assets/updated",
            "receipt_progress",
            "dw.{tenant}.assets.items.updated",
            None
        ),
    ]);
    manifest["subscriptions"] = json!([{
        "subject": "dw.{tenant}.assets.items.updated",
        "handler": "receipt_progress",
        "handler_type": "wasm",
    }]);
    let (status, body) = platform
        .deploy_module(&tenant, &common::wasm_manifest("assets", &[]), &wasm)
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let (status, body) = platform.deploy_module(&tenant, &manifest, &wasm).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let version = format!("v{}", body["version"].as_i64().expect("version"));
    let nats = async_nats::connect(nats_url).await.expect("nats");
    let url = |path: &str| format!("{}/svc/{}/receipts{path}", platform.base_url, tenant.slug);
    let publish = |subject: String, payload: Value| {
        let nats = nats.clone();
        async move {
            nats.publish(subject, payload.to_string().into())
                .await
                .expect("publish");
            nats.flush().await.expect("flush");
        }
    };
    let progress = format!("dw.{}.receipts.pdf.progress.L7", tenant.slug);

    let request = platform
        .http
        .get(url("/receipts/book/L7/progress?snapshot_ms=1500&total=2"))
        .send();
    let early = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        publish(
            progress.clone(),
            json!({"receipt_id": "q1", "status": "completed"}),
        )
        .await;
    };
    let (response, ()) = tokio::join!(request, early);
    let mut response = response.expect("stream");
    assert_eq!(response.status().as_u16(), 200);
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    assert_eq!(header("content-type").as_deref(), Some("text/event-stream"));
    assert_eq!(
        header("cache-control").as_deref(),
        Some("no-cache, no-store, must-revalidate")
    );
    assert_eq!(header("x-accel-buffering").as_deref(), Some("no"));
    let (so_far, ended) = read_stream(&mut response, Duration::from_secs(1)).await;
    assert!(!ended, "{so_far}");
    publish(
        progress.clone(),
        json!({"receipt_id": "q2", "status": "completed"}),
    )
    .await;
    let (rest, ended) = read_stream(&mut response, Duration::from_secs(10)).await;
    assert!(ended, "all_completed closes the stream: {so_far}{rest}");
    assert_eq!(
        events(&format!("{so_far}{rest}")),
        vec![
            json!({"type": "snapshot", "book_id": "L7", "completed": 0, "total": 2}),
            json!({"type": "receipt_completed", "receipt_id": "q1", "completed": 1}),
            json!({"type": "receipt_completed", "receipt_id": "q2", "completed": 2}),
            json!({"type": "all_completed"}),
        ],
        "the message published while the snapshot was built is delivered"
    );
    let reduced = capture
        .span("a run of the reducer", |span| {
            span.text == "wasm_handler" && span.attribute("cufflink.kind") == Some("sse_reducer")
        })
        .await
        .expect("exported");
    assert_eq!(
        reduced.scope(),
        [
            ("cufflink.handler", "receipt_progress_reduce"),
            ("cufflink.kind", "sse_reducer"),
            ("cufflink.service", "receipts"),
            ("cufflink.tenant", tenant.slug.as_str()),
            ("cufflink.version", version.as_str()),
        ],
        "a reducer runs in a task of the stream's, scoped as itself"
    );

    let mut pinged = platform
        .http
        .get(url("/receipts/book/L8/progress?ping=1&timeout=3"))
        .send()
        .await
        .expect("stream");
    let (stream, ended) = read_stream(&mut pinged, Duration::from_secs(10)).await;
    assert!(ended, "the stream times out: {stream}");
    let pings = events(&stream)
        .iter()
        .filter(|event| event["type"] == "ping")
        .count();
    assert!((1..=3).contains(&pings), "{stream}");

    let summary = platform
        .http
        .get(url("/receipts/book/L7/summary"))
        .send()
        .await
        .expect("summary");
    assert_eq!(summary.status().as_u16(), 200);
    assert_eq!(
        summary.json::<Value>().await.expect("json"),
        json!({"stream": false})
    );

    let mut feed = platform
        .http
        .get(url("/feed?timeout=30"))
        .send()
        .await
        .expect("feed");
    assert_eq!(feed.status().as_u16(), 200);
    let capped = platform
        .http
        .get(url("/feed"))
        .send()
        .await
        .expect("second stream");
    assert_eq!(capped.status().as_u16(), 429, "one stream per principal");
    read_stream(&mut feed, Duration::from_millis(300)).await;
    nats.publish(format!("dw.{}.feed", tenant.slug), "plain text".into())
        .await
        .expect("publish");
    let (stream, _) = read_stream(&mut feed, Duration::from_secs(2)).await;
    assert!(stream.contains("data: plain text\n"), "{stream}");
    drop(feed);
    let opens = |path: &'static str| {
        let (platform, url) = (&platform, &url);
        async move {
            loop {
                let response = platform.http.get(url(path)).send().await.expect("stream");
                if response.status().as_u16() == 200 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    tokio::time::timeout(
        Duration::from_secs(10),
        opens("/receipts/book/L9/progress?timeout=1"),
    )
    .await
    .expect("a disconnect frees the slot");

    let unsubscribed = platform
        .http
        .get(url("/assets/created"))
        .send()
        .await
        .expect("request");
    assert_eq!(
        unsubscribed.status().as_u16(),
        403,
        "another service's events the manifest does not subscribe to"
    );
    tokio::time::timeout(Duration::from_secs(10), opens("/assets/updated?timeout=1"))
        .await
        .expect("another service's events a subscription covers");

    let dotted = platform
        .http
        .get(url("/receipts/book/a.b/progress"))
        .send()
        .await
        .expect("request");
    assert_eq!(dotted.status().as_u16(), 400);
    assert_eq!(
        dotted.json::<Value>().await.expect("json")["error"],
        "path parameter 'book_id' must be one subject token (no '.', '*', '>' or whitespace)"
    );
}
