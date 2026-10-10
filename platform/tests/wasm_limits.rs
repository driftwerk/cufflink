use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_platform::tenant_db::TenantPoolManager;
use serde_json::json;
use std::time::{Duration, Instant};

fn thread_count(pid: u32) -> Option<usize> {
    if cfg!(target_os = "linux") {
        std::fs::read_dir(format!("/proc/{pid}/task"))
            .ok()
            .map(|entries| entries.count())
    } else if cfg!(target_os = "macos") {
        let output = std::process::Command::new("ps")
            .args(["-M", &pid.to_string()])
            .output()
            .ok()?;
        Some(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .count()
                .saturating_sub(1),
        )
    } else {
        None
    }
}

#[tokio::test]
async fn runaway_handler_is_killed_at_deadline() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("WASM_TEST_ROUTE_BUDGET_SECS", "1")],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy(
            &tenant,
            &common::wasm_manifest("spin-svc", &[("spin", "/spin")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let started = Instant::now();
    let (status, body) = platform.call_fn(&tenant, "spin-svc", "spin").await;
    assert_eq!(status, 500);
    assert_eq!(body["error"], "WASM execution timed out after 1s");
    assert!(
        started.elapsed() < Duration::from_millis(1_500),
        "the caller waited {:?}",
        started.elapsed()
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        platform
            .metric("cufflink_wasm_inflight", &[("kind", "route")])
            .await,
        Some(0.0)
    );

    let baseline = thread_count(platform.pid());
    for _ in 0..20 {
        let (status, _) = platform.call_fn(&tenant, "spin-svc", "spin").await;
        assert_eq!(status, 500);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    if let (Some(before), Some(after)) = (baseline, thread_count(platform.pid())) {
        assert!(
            after <= before + 2,
            "blocking threads grew from {before} to {after} across timed-out handlers"
        );
    }
    assert_eq!(platform.status("/health").await, Some(200));
}

#[tokio::test]
async fn one_gigabyte_allocation_is_refused() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;

    let mut enforce = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CUFFLINK_POLICY_WASM_LIMITS", "enforce")],
    )
    .await;
    enforce.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = enforce
        .deploy(
            &tenant,
            &common::wasm_manifest("hog-enforce", &[("hog", "/hog")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let (status, body) = enforce.call_fn(&tenant, "hog-enforce", "hog").await;
    assert_eq!(status, 500, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("exceeded its memory limit")),
        "{body}"
    );
    assert_eq!(
        enforce
            .metric(
                "cufflink_wasm_limit_events_total",
                &[("limit", "memory"), ("mode", "enforce")]
            )
            .await,
        Some(1.0)
    );
    enforce.kill();

    let warn = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CUFFLINK_POLICY_WASM_LIMITS", "warn")],
    )
    .await;
    warn.wait_ready().await;
    let (status, body) = warn
        .deploy(
            &tenant,
            &common::wasm_manifest("hog-warn", &[("hog", "/hog")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let (status, body) = warn.call_fn(&tenant, "hog-warn", "hog").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, json!({ "ok": true }));
    assert_eq!(
        warn.metric(
            "cufflink_wasm_limit_events_total",
            &[("limit", "memory"), ("mode", "warn")]
        )
        .await,
        Some(1.0)
    );
}

#[tokio::test]
async fn negative_length_returns_minus_one() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy(
            &tenant,
            &common::wasm_manifest("len-svc", &[("neg_len", "/neg_len")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let (status, body) = platform.call_fn(&tenant, "len-svc", "neg_len").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, json!({ "rc": -1 }));
    assert_eq!(platform.status("/health").await, Some(200));
    assert_eq!(
        platform
            .metric(
                "cufflink_wasm_limit_events_total",
                &[("limit", "length"), ("mode", "enforce")]
            )
            .await,
        Some(1.0)
    );
}

#[tokio::test]
async fn before_hook_does_not_hold_the_tenant_connection() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform =
        PlatformProcess::start(&db.url, infra.nats_url(), &[("MAX_CONNS_PER_TENANT", "1")]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("hook-svc", &[]);
    manifest["tables"][0]["hooks"] = json!({ "before_update": "hook_query" });
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let items = format!("{}/svc/{}/hook-svc/items", platform.base_url, tenant.slug);
    let created: serde_json::Value = platform
        .http
        .post(&items)
        .json(&json!({ "title": "before" }))
        .send()
        .await
        .expect("create")
        .json()
        .await
        .expect("created row");
    let id = created["id"].as_str().expect("row id").to_string();

    let response = platform
        .http
        .put(format!("{items}/{id}"))
        .json(&json!({ "title": "after" }))
        .send()
        .await
        .expect("update");
    let status = response.status().as_u16();
    let body: serde_json::Value = response.json().await.unwrap_or_default();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["title"], "after");
}

#[tokio::test]
async fn before_update_hook_sees_a_soft_deleted_row() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("soft-svc", &[]);
    manifest["tables"][0]["soft_delete"] = json!(true);
    manifest["tables"][0]["columns"]
        .as_array_mut()
        .expect("columns")
        .push(json!({
            "name": "deleted_at", "column_type": "timestamp", "primary_key": false,
            "nullable": true, "auto_generate": false
        }));
    manifest["tables"][0]["hooks"] = json!({ "before_update": "hook_query" });
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let items = format!("{}/svc/{}/soft-svc/items", platform.base_url, tenant.slug);
    let created: serde_json::Value = platform
        .http
        .post(&items)
        .json(&json!({ "title": "before" }))
        .send()
        .await
        .expect("create")
        .json()
        .await
        .expect("created row");
    let id = created["id"].as_str().expect("row id").to_string();
    let deleted = platform
        .http
        .delete(format!("{items}/{id}"))
        .send()
        .await
        .expect("soft delete");
    assert!(deleted.status().is_success(), "{}", deleted.status());

    let response = platform
        .http
        .put(format!("{items}/{id}"))
        .json(&json!({ "title": "after" }))
        .send()
        .await
        .expect("update");
    let status = response.status().as_u16();
    let body: serde_json::Value = response.json().await.unwrap_or_default();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["title"], "after");
    assert!(
        !body["deleted_at"].is_null(),
        "the row stays soft-deleted: {body}"
    );
}

#[tokio::test]
async fn slow_statement_hits_statement_timeout_not_zombie() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("WASM_TEST_ROUTE_BUDGET_SECS", "3")],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy(
            &tenant,
            &common::wasm_manifest("slow-svc", &[("slow_sql", "/slow"), ("echo", "/echo")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let admin = db.pool();
    let tenant_db = TenantPoolManager::managed_db_name(&tenant.slug);
    let sleeping = || async {
        admin
            .get()
            .await
            .expect("admin checkout")
            .query_one(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = $1 AND state = 'active' AND query ILIKE '%pg_sleep(600)%'",
                &[&tenant_db],
            )
            .await
            .expect("pg_stat_activity")
            .get::<_, i64>(0)
    };

    let started = Instant::now();
    let call = tokio::spawn({
        let http = platform.http.clone();
        let url = platform.fn_url(&tenant, "slow-svc", "slow_sql");
        async move { http.post(url).json(&json!({})).send().await }
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(sleeping().await, 1, "the guest statement is not running");
    let response = call.await.expect("call task").expect("fn request");
    let elapsed = started.elapsed();
    let status = response.status().as_u16();
    let body: serde_json::Value = response.json().await.unwrap_or_default();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, json!({ "rc": -1 }));
    assert!(
        elapsed > Duration::from_secs(2) && elapsed < Duration::from_secs(3),
        "statement was cut at {elapsed:?}, expected ~2.5s"
    );

    tokio::time::sleep(Duration::from_secs(4) - elapsed.min(Duration::from_secs(4))).await;
    assert_eq!(
        sleeping().await,
        0,
        "pg_sleep kept running after the statement timeout"
    );
    let in_transaction: i64 = admin
        .get()
        .await
        .expect("admin checkout")
        .query_one(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = $1 AND state LIKE 'idle in transaction%'",
            &[&tenant_db],
        )
        .await
        .expect("pg_stat_activity")
        .get(0);
    assert_eq!(
        in_transaction, 0,
        "a guest connection was returned mid-transaction"
    );

    let (status, _) = platform.call_fn(&tenant, "slow-svc", "echo").await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn oversized_response_is_refused_and_counted() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform =
        PlatformProcess::start(&db.url, infra.nats_url(), &[("WASM_MAX_RESPONSE_MB", "1")]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy(
            &tenant,
            &common::wasm_manifest("big-svc", &[("big_response", "/big")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    assert_eq!(
        platform
            .metric("cufflink_wasm_module_cache_entries", &[])
            .await,
        Some(1.0),
        "the deployed module is cached"
    );

    let (status, body) = platform.call_fn(&tenant, "big-svc", "big_response").await;
    assert_eq!(status, 500, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("exceeds the 1048576 byte cap")),
        "{body}"
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_wasm_limit_events_total",
                &[("limit", "response"), ("mode", "enforce")]
            )
            .await,
        Some(1.0)
    );
    assert_eq!(platform.status("/health").await, Some(200));
}
