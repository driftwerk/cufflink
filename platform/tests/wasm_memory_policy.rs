use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use cufflink_platform::policy::{PolicyKey, PolicyMode};
use cufflink_platform::repo::tenant_policies;
use serde_json::json;

const MIB: u64 = 1024 * 1024;

async fn set_policy(
    db: &common::TestDb,
    tenant: &Tenant,
    mode: PolicyMode,
    memory_bytes: Option<u64>,
) {
    tenant_policies::upsert(
        &db.pool().get().await.expect("checkout"),
        tenant.id,
        PolicyKey::WasmLimits,
        mode,
        &json!({ "memory_bytes": memory_bytes }),
        "integration",
    )
    .await
    .expect("policy row");
}

#[tokio::test]
async fn wasm_memory_policy_per_tenant() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CUFFLINK_WASM_LIMITS", "enforce")],
    )
    .await;
    platform.wait_ready().await;

    let enforced = db.mint_tenant().await;
    set_policy(&db, &enforced, PolicyMode::Enforce, Some(256 * MIB)).await;
    let warned = db.mint_tenant().await;
    set_policy(&db, &warned, PolicyMode::Warn, Some(256 * MIB)).await;
    let above_ceiling = db.mint_tenant().await;
    set_policy(&db, &above_ceiling, PolicyMode::Enforce, Some(4096 * MIB)).await;
    let defaults = db.mint_tenant().await;

    for tenant in [&enforced, &warned, &above_ceiling, &defaults] {
        let (status, body) = platform
            .deploy(
                tenant,
                &common::wasm_manifest("hog-svc", &[("hog", "/hog")]),
            )
            .await;
        assert_eq!(status, 200, "deploy failed: {body}");
    }
    let hog = |tenant| platform.call_fn(tenant, "hog-svc", "hog");

    let (status, body) = hog(&enforced).await;
    assert_eq!(status, 500, "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(
        error.contains(&format!("limit {} bytes", 256 * MIB)),
        "{error}"
    );

    assert_eq!(hog(&warned).await, (200, json!({"ok": true})));

    let (status, body) = hog(&above_ceiling).await;
    assert_eq!(status, 500, "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(
        error.contains(&format!("limit {} bytes", 512 * MIB)),
        "a tenant cap above WASM_MAX_MEMORY_MB is held to it: {error}"
    );

    assert_eq!(
        hog(&defaults).await,
        (200, json!({"ok": true})),
        "CUFFLINK_WASM_LIMITS no longer sets the platform default"
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_wasm_limit_events_total",
                &[("limit", "memory"), ("mode", "enforce")]
            )
            .await,
        Some(2.0)
    );
}
