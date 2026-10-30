use cufflink_testkit as common;

use common::PlatformProcess;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const SERVICE: &str = "loan-service";

struct Answer {
    status: u16,
    retry_after: Option<String>,
    body: Value,
    took: Duration,
}

async fn call(platform: &PlatformProcess, url: &str, after: Duration) -> Answer {
    tokio::time::sleep(after).await;
    let started = Instant::now();
    let response = platform
        .http
        .post(url)
        .json(&json!({}))
        .send()
        .await
        .expect("fn request");
    let status = response.status().as_u16();
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    Answer {
        status,
        retry_after,
        body: response.json().await.unwrap_or(Value::Null),
        took: started.elapsed(),
    }
}

#[tokio::test]
async fn a_route_runs_max_concurrent_calls_at_once_and_refuses_the_rest_unrun() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("route_concurrency"))),
        "{version}"
    );

    let mut manifest = common::wasm_manifest(SERVICE, &[("sleep2", "/sleep2"), ("say", "/say")]);
    manifest["custom_routes"][0]["max_concurrent"] = json!(2);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let limited = platform.fn_url(&tenant, SERVICE, "sleep2");
    let free = platform.fn_url(&tenant, SERVICE, "say");
    let (first, second, third, other) = tokio::join!(
        call(&platform, &limited, Duration::ZERO),
        call(&platform, &limited, Duration::ZERO),
        call(&platform, &limited, Duration::from_millis(400)),
        call(&platform, &free, Duration::from_millis(400)),
    );

    assert_eq!((first.status, second.status), (200, 200));
    assert_eq!(
        other.status, 200,
        "another route has no limit: {}",
        other.body
    );
    assert_eq!(third.status, 503, "{}", third.body);
    assert_eq!(third.retry_after.as_deref(), Some("2"));
    assert_eq!(
        third.body,
        json!({"error": "guest capacity exhausted; the request was not run"})
    );
    assert!(
        third.took >= Duration::from_secs(1) && third.took < Duration::from_secs(2),
        "the third call waits WASM_ADMISSION_WAIT_MS for a slot, not for the handlers: {:?}",
        third.took
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_wasm_admission_refused_total",
                &[
                    ("tenant", tenant.slug.as_str()),
                    ("service", SERVICE),
                    ("reason", "route_concurrency"),
                ],
            )
            .await,
        Some(1.0)
    );

    let again = call(&platform, &limited, Duration::ZERO).await;
    assert_eq!(again.status, 200, "slots are freed: {}", again.body);
}
