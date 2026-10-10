use cufflink_testkit as common;

use common::PlatformProcess;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn manifest() -> Value {
    let id = json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true});
    let created = json!({"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true});
    let title = json!({"name": "title", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false});
    json!({
        "name": "layers",
        "version": "0.1.0",
        "mode": "wasm",
        "tables": [
            {"name": "slow", "columns": [id, title, created], "hooks": {"before_create": "sleep2"}},
            {"name": "notes", "columns": [id, title, created]}
        ],
        "publishes": [],
    })
}

#[tokio::test]
async fn http_layers_hardened() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        Some(nats_url),
        &[
            ("CUFFLINK_HTTP_PROFILE", "hardened"),
            ("CORS_ALLOWED_ORIGINS", "https://app.example.com"),
            ("HTTP_DATA_TIMEOUT_SECS", "1"),
            ("HTTP_MAX_CONCURRENT_REQUESTS", "1"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform.deploy(&tenant, &manifest()).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let url = |path: &str| format!("{}/svc/{}/layers{path}", platform.base_url, tenant.slug);
    let key = format!("ApiKey {}", tenant.api_key);
    let post = |path: &str, body: String| {
        platform
            .http
            .post(url(path))
            .header("authorization", &key)
            .header("content-type", "application/json")
            .body(body)
            .send()
    };

    let limit = 4 * 1024 * 1024;
    let past_the_limit = [
        format!(
            "POST /svc/{}/layers/notes HTTP/1.1\r\nHost: localhost\r\nAuthorization: {key}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            tenant.slug,
            5 * 1024 * 1024
        )
        .into_bytes(),
        vec![b'x'; limit + 1],
    ]
    .concat();
    assert_eq!(
        platform.status_line(&past_the_limit).await,
        "HTTP/1.1 413 Payload Too Large",
        "4 MiB data-route bodies"
    );
    let response = post("/notes", json!({"title": "fits"}).to_string())
        .await
        .expect("request");
    assert_eq!(response.status().as_u16(), 200);

    let started = Instant::now();
    let slow = tokio::spawn(post("/slow", json!({"title": "t"}).to_string()));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let shed = platform
        .http
        .get(url("/notes"))
        .header("authorization", &key)
        .send()
        .await
        .expect("request");
    assert_eq!(shed.status().as_u16(), 503);
    assert_eq!(
        shed.json::<Value>().await.expect("json"),
        json!({"error": "server busy"})
    );
    let slow = slow.await.expect("join").expect("request");
    assert_eq!(slow.status().as_u16(), 503);
    assert_eq!(
        slow.json::<Value>().await.expect("json"),
        json!({"error": "request timed out"})
    );
    assert!(
        started.elapsed() < Duration::from_millis(1900),
        "cut off at the timeout, not after the 2 s hook: {:?}",
        started.elapsed()
    );
    assert!(platform
        .metric("cufflink_http_load_shed_total", &[])
        .await
        .is_some_and(|shed| shed >= 1.0));

    let mut stream = platform
        .http
        .get(url("/notes/stream"))
        .header("authorization", &key)
        .send()
        .await
        .expect("stream");
    assert_eq!(stream.status().as_u16(), 200);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let response = post("/notes", json!({"title": "after the timeout"}).to_string())
        .await
        .expect("request");
    assert_eq!(response.status().as_u16(), 200);
    let mut seen = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while let Ok(Ok(Some(chunk))) = tokio::time::timeout_at(deadline, stream.chunk()).await {
        seen.push_str(&String::from_utf8_lossy(&chunk));
        if seen.contains("after the timeout") {
            break;
        }
    }
    assert!(
        seen.contains("after the timeout"),
        "the stream outlives the data timeout: {seen}"
    );
    drop(stream);

    let preflight = |origin: &'static str| {
        platform
            .http
            .request(reqwest::Method::OPTIONS, url("/notes"))
            .header("origin", origin)
            .header("access-control-request-method", "POST")
            .send()
    };
    let allowed = preflight("https://app.example.com")
        .await
        .expect("preflight");
    assert_eq!(
        allowed.headers()["access-control-allow-origin"],
        "https://app.example.com"
    );
    let refused = preflight("https://evil.example.com")
        .await
        .expect("preflight");
    assert!(
        !refused
            .headers()
            .contains_key("access-control-allow-origin"),
        "{:?}",
        refused.headers()
    );
}
