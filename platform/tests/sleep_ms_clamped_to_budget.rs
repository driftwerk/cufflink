use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use cufflink_test_guests::Profile;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const SERVICE: &str = "indexer";
const BUDGET_SECS: u64 = 5;
const LONGEST_MS: u64 = 2_000;

async fn call(platform: &PlatformProcess, tenant: &Tenant, handler: &str, body: Value) -> Value {
    let response = platform
        .http
        .post(platform.fn_url(tenant, SERVICE, handler))
        .json(&body)
        .send()
        .await
        .expect("fn request");
    assert_eq!(response.status().as_u16(), 200, "{handler}");
    response.json().await.expect("json body")
}

fn route(handler: &str, timeout_secs: Option<u64>) -> Value {
    json!({
        "method": "POST",
        "path": format!("/{handler}"),
        "handler": handler,
        "posture": {"kind": "public", "reason": "integration test"},
        "timeout_secs": timeout_secs,
    })
}

async fn started(
    wasm: &[u8],
    longest_ms: Option<&str>,
) -> Option<(PlatformProcess, Tenant, common::TestDb)> {
    let infra = common::infra().await?;
    let mut db = infra.create_database().await;
    let longest: Vec<(&str, &str)> = longest_ms
        .map(|ms| ("UTIL_SLEEP_MAX_MS", ms))
        .into_iter()
        .collect();
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &longest).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("util_v2"))),
        "{version}"
    );
    let mut manifest = common::wasm_manifest(SERVICE, &[]);
    manifest["custom_routes"] = json!([route("sleep", Some(BUDGET_SECS)), route("now", None),]);
    let (status, body) = platform.deploy_module(&tenant, &manifest, wasm).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    Some((platform, tenant, db))
}

#[tokio::test]
async fn sleep_ms_clamped_to_budget() {
    let Some(wasm) = common::sdk::sdk_guest("util-v2", Profile::Debug) else {
        return;
    };
    let Some((platform, tenant, _db)) = started(&wasm, None).await else {
        return;
    };

    let before = chrono::Utc::now().timestamp_millis();
    let now = call(&platform, &tenant, "now", json!({})).await;
    let after = chrono::Utc::now().timestamp_millis();
    let millis = now["millis"].as_i64().expect("millis");
    assert!((before..=after).contains(&millis), "{before} {now} {after}");
    let time = chrono::DateTime::parse_from_rfc3339(now["time"].as_str().expect("time"))
        .expect("RFC 3339")
        .timestamp_millis();
    assert!((before..=after).contains(&time), "{before} {now} {after}");

    let short = call(&platform, &tenant, "sleep", json!({"ms": 300})).await;
    let slept = short["slept"].as_u64().expect("slept");
    assert!((295..=300).contains(&slept), "{short}");
    assert!(
        short["elapsed"].as_i64().is_some_and(|ms| ms >= 295),
        "{short}"
    );

    let started = Instant::now();
    let long = call(&platform, &tenant, "sleep", json!({"ms": 60_000})).await;
    let elapsed = started.elapsed();
    let slept = long["slept"].as_u64().expect("slept");
    let budget = BUDGET_SECS * 1_000;
    assert!(
        (budget - 2_000..=budget - 1_000).contains(&slept),
        "a second of the budget is left to answer in: {long}"
    );
    assert!(elapsed < Duration::from_secs(BUDGET_SECS), "{elapsed:?}");
}

#[tokio::test]
async fn a_sleep_is_no_longer_than_the_install_allows() {
    let Some(wasm) = common::sdk::sdk_guest("util-v2", Profile::Debug) else {
        return;
    };
    let longest = LONGEST_MS.to_string();
    let Some((platform, tenant, _db)) = started(&wasm, Some(&longest)).await else {
        return;
    };
    let started = Instant::now();
    let capped = call(&platform, &tenant, "sleep", json!({"ms": 4_000})).await;
    let slept = capped["slept"].as_u64().expect("slept");
    assert!((LONGEST_MS - 5..=LONGEST_MS).contains(&slept), "{capped}");
    assert!(started.elapsed() < Duration::from_millis(4_000));
}
