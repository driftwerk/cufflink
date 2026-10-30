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
        third.took >= Duration::from_secs(1),
        "the third call waits WASM_ADMISSION_WAIT_MS for a slot: {:?}",
        third.took
    );
    assert!(
        third.took + Duration::from_millis(400) < first.took.min(second.took),
        "and is refused while the handlers holding the slots still run: {:?} {:?} {:?}",
        third.took,
        first.took,
        second.took
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

const MIB: u64 = 1024 * 1024;

async fn budgeted(mode: &str) -> Option<(PlatformProcess, common::TestDb)> {
    let infra = common::infra().await?;
    let db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("WASM_MEMORY_BUDGET_MODE", mode),
            ("WASM_MEMORY_BUDGET_MB", "128"),
        ],
    )
    .await;
    platform.wait_ready().await;
    Some((platform, db))
}

#[tokio::test]
async fn a_burst_under_enforce_is_answered_503_with_retry_after_and_the_process_survives() {
    let Some((platform, mut db)) = budgeted("enforce").await else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let manifest = common::wasm_manifest("catalog-service", &[("hold_48", "/hold_48")]);
    let (status, body) = platform.deploy(&catalog, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let url = platform.fn_url(&catalog, "catalog-service", "hold_48");
    let health = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        platform.status("/health").await
    };
    let (answers, healthy) = tokio::join!(
        futures_util::future::join_all((0..12).map(|_| call(&platform, &url, Duration::ZERO))),
        health,
    );
    assert_eq!(healthy, Some(200), "health answers during the burst");
    let ok = answers.iter().filter(|a| a.status == 200).count();
    assert!(ok >= 1, "some calls run");
    for answer in answers.iter().filter(|a| a.status != 200) {
        assert_eq!(answer.status, 503, "never a 500: {}", answer.body);
        assert_eq!(answer.retry_after.as_deref(), Some("2"));
        assert_eq!(
            answer.body,
            json!({"error": "guest capacity exhausted; the request was not run"})
        );
    }
    assert!(
        ok < 12,
        "a 128 MiB budget cannot run twelve 48 MiB guests at once"
    );
    assert_eq!(
        platform.status("/health").await,
        Some(200),
        "the process that took the burst still answers"
    );
    assert_eq!(
        platform
            .metric("cufflink_wasm_memory_budget_bytes", &[],)
            .await,
        Some((128 * MIB) as f64)
    );
}

#[tokio::test]
async fn under_warn_the_same_burst_runs_and_counts_what_would_have_waited() {
    let Some((platform, mut db)) = budgeted("warn").await else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let manifest = common::wasm_manifest("catalog-service", &[("hold_48", "/hold_48")]);
    let (status, body) = platform.deploy(&catalog, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let url = platform.fn_url(&catalog, "catalog-service", "hold_48");
    let answers =
        futures_util::future::join_all((0..6).map(|_| call(&platform, &url, Duration::ZERO))).await;
    assert!(
        answers.iter().all(|answer| answer.status == 200),
        "warn refuses nothing"
    );
    let counted = platform
        .metric(
            "cufflink_wasm_admission_refused_total",
            &[("mode", "warn"), ("tenant", catalog.slug.as_str())],
        )
        .await
        .unwrap_or_default();
    assert!(counted >= 1.0, "would-be refusals are counted: {counted}");
}
