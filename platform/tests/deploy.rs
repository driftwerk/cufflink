use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_test_guests::Profile;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use uuid::Uuid;

async fn service_id(db: &common::TestDb, name: &str) -> Uuid {
    db.pool()
        .get()
        .await
        .expect("checkout")
        .query_one("SELECT id FROM services WHERE name = $1", &[&name])
        .await
        .expect("service row")
        .get(0)
}

async fn scalar<T>(
    db: &common::TestDb,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> T
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
    db.pool()
        .get()
        .await
        .expect("checkout")
        .query_one(sql, params)
        .await
        .expect("query")
        .get(0)
}

async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if probe().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{what} did not happen within {timeout:?}");
}

#[tokio::test]
async fn deploy_metadata_is_atomic() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let tenant;
    {
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
        platform.wait_ready().await;
        tenant = db.mint_tenant().await;
        let (status, body) = platform
            .deploy(
                &tenant,
                &common::wasm_manifest("atomic-svc", &[("echo", "/echo")]),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["version"], 2);
    }

    let failing = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CUFFLINK_TEST_FAIL_AFTER_SCHEMA", "1")],
    )
    .await;
    failing.wait_ready().await;
    let mut changed = common::wasm_manifest("atomic-svc", &[("echo", "/echo"), ("spin", "/spin")]);
    changed["tables"][0]["columns"].as_array_mut().expect("columns").push(json!(
        {"name": "note", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}
    ));
    let (status, body) = failing.deploy(&tenant, &changed).await;
    assert_eq!(status, 500, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("CUFFLINK_TEST_FAIL_AFTER_SCHEMA")),
        "{body}"
    );

    let id = service_id(&db, "atomic-svc").await;
    let current: i32 = scalar(
        &db,
        "SELECT current_version FROM services WHERE id = $1",
        &[&id],
    )
    .await;
    assert_eq!(
        current, 2,
        "current_version advanced without a deployment row"
    );
    let rows: i64 = scalar(
        &db,
        "SELECT count(*) FROM deployments WHERE service_id = $1 AND version = 3",
        &[&id],
    )
    .await;
    assert_eq!(
        rows, 0,
        "a deployments row was written for the failed version"
    );
    let snapshots: i64 = scalar(
        &db,
        "SELECT count(*) FROM schema_snapshots WHERE service_id = $1",
        &[&id],
    )
    .await;
    assert_eq!(snapshots, 1, "schema_snapshots changed on a failed deploy");
}

#[tokio::test]
async fn manifest_lookups_use_the_current_version_not_an_orphan() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy(&tenant, &common::wasm_manifest("orphan-svc", &[]))
        .await;
    assert_eq!(status, 200, "{body}");
    let id = service_id(&db, "orphan-svc").await;
    let current: i32 = scalar(
        &db,
        "SELECT current_version FROM services WHERE id = $1",
        &[&id],
    )
    .await;
    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO deployments (service_id, version, manifest_hash, manifest, commit_hash) \
             VALUES ($1, $2, 'orphan', $3, 'orphan')",
            &[
                &id,
                &(current + 1),
                &json!({ "mode": "crud", "tables": [{ "name": "ghost" }] }),
            ],
        )
        .await
        .expect("plant an orphan row");

    let (status, body) = platform
        .api_get(&tenant, &format!("/api/services/{id}/schema"))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["tables"][0]["name"], "items", "{body}");

    let (status, health) = platform
        .get_json(&format!("/svc/{}/orphan-svc/_meta/health", tenant.slug))
        .await
        .expect("_meta/health");
    assert_eq!(status, 200, "{health}");
    assert_eq!(health["version"], current, "{health}");
    assert_ne!(health["commit_hash"], "orphan", "{health}");

    let snapshot_hash: String = scalar(
        &db,
        "SELECT hash FROM schema_snapshots WHERE service_id = $1 AND version = $2",
        &[&id, &current],
    )
    .await;
    let (status, body) = platform
        .api_get(&tenant, &format!("/api/services/{id}/deployments"))
        .await;
    assert_eq!(status, 200, "{body}");
    let listed = body["deployments"].as_array().expect("deployments");
    let at = |version: i32| {
        listed
            .iter()
            .find(|d| d["version"] == version)
            .unwrap_or_else(|| panic!("v{version} missing from {body}"))
    };
    assert_eq!(at(current)["schema_hash"], snapshot_hash.as_str());
    assert_eq!(at(current)["is_current"], true);
    assert!(at(current + 1)["schema_hash"].is_null(), "{body}");
}

#[tokio::test]
async fn rollback_refuses_a_version_subscribed_outside_the_tenant() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy(&tenant, &common::wasm_manifest("scope-svc", &[]))
        .await;
    assert_eq!(status, 200, "{body}");
    let id = service_id(&db, "scope-svc").await;
    let current: i32 = scalar(
        &db,
        "SELECT current_version FROM services WHERE id = $1",
        &[&id],
    )
    .await;
    let mut manifest: Value = scalar(
        &db,
        "SELECT manifest FROM deployments WHERE service_id = $1 AND version = $2",
        &[&id, &current],
    )
    .await;
    manifest["subscriptions"] = json!([{
        "subject": "dw.victim.logs",
        "handler": "echo",
        "handler_type": "wasm",
    }]);
    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO deployments (service_id, version, manifest_hash, manifest, wasm_hash) \
             SELECT service_id, $2, 'pre-upgrade', $3, wasm_hash FROM deployments \
             WHERE service_id = $1 AND version = $4",
            &[&id, &(current + 1), &manifest, &current],
        )
        .await
        .expect("a version deployed before the scope check");

    let (status, body) = platform
        .send(
            reqwest::Method::POST,
            &format!("/api/services/{id}/rollback"),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({ "target_version": current + 1 })),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("outside this tenant")),
        "{body}"
    );
    let after: i32 = scalar(
        &db,
        "SELECT current_version FROM services WHERE id = $1",
        &[&id],
    )
    .await;
    assert_eq!(after, current, "nothing was activated");
}

#[tokio::test]
async fn whole_manifest_hash_deploys_publishes_change() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let mut manifest = common::wasm_manifest("hash-svc", &[("echo", "/echo")]);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["version"], 2);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["skipped"], true,
        "an identical deploy is skipped: {body}"
    );

    manifest["publishes"] = json!(["hash-svc.orders.>"]);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.get("skipped").is_none(), "{body}");
    assert_eq!(body["version"], 3);
}

#[tokio::test]
async fn deploy_rejects_two_documented_routes_of_one_legacy_operation() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("docs-svc", &[("echo", "/a"), ("echo", "/b")]);
    manifest["custom_routes"][0]["doc"] = json!({"operation_id": "first"});
    manifest["custom_routes"][1]["doc"] = json!({"operation_id": "second"});

    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("two routes document POST /_fn/echo")),
        "{body}"
    );
}

#[tokio::test]
async fn deploy_rejects_unknown_imports() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let module = wat::parse_str(
        r#"(module
             (import "env" "no_such_host_fn" (func (param i32 i32) (result i32)))
             (memory (export "memory") 1)
             (func (export "alloc") (param i32) (result i32) (i32.const 1024))
             (func (export "echo") (param i32 i32) (result i32) (i32.const 0)))"#,
    )
    .expect("wat");
    let (status, body) = platform
        .deploy_module(
            &tenant,
            &common::wasm_manifest("import-svc", &[("echo", "/echo")]),
            &module,
        )
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains("env::no_such_host_fn"), "{error}");
    assert!(error.contains("db_execute_all"), "{error}");
    assert_eq!(
        platform
            .metric("cufflink_deploy_import_rejected_total", &[])
            .await,
        Some(1.0)
    );

    let id = service_id(&db, "import-svc").await;
    let (status, body) = platform
        .api_get(&tenant, &format!("/api/services/{id}/wasm/status"))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["local_cache"], false, "{body}");
    assert_eq!(body["db_state"], false, "{body}");
}

#[tokio::test]
async fn deploy_and_upload_require_the_features_a_module_declares() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let declaring = |section: &str| {
        wat::parse_str(common::guest::module_wat().replacen(
            "(module",
            &format!(r#"(module (@custom "cufflink.features" "{section}")"#),
            1,
        ))
        .expect("wat")
    };
    let manifest = common::wasm_manifest("featured-svc", &[("echo", "/echo")]);

    let (status, body) = platform
        .deploy_module(&tenant, &manifest, &declaring("no_such_feature\\n"))
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains("'no_such_feature'"), "{error}");

    let (status, body) = platform
        .deploy_module(&tenant, &manifest, &declaring("DB-V2\\n"))
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains("invalid feature name"), "{error}");

    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "{body}");
    let id = service_id(&db, "featured-svc").await;
    let upload = |wasm: Vec<u8>| {
        use base64::Engine;
        json!({ "wasm_base64": base64::engine::general_purpose::STANDARD.encode(wasm) })
    };
    let (status, body) = platform
        .send(
            reqwest::Method::POST,
            &format!("/api/services/{id}/wasm"),
            &format!("ApiKey {}", tenant.api_key),
            Some(&upload(declaring("no_such_feature\\n"))),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains("'no_such_feature'"), "{error}");
}

#[tokio::test]
async fn a_guest_built_for_what_the_platform_has_not_yet_is_refused() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    let supports = version["supports"].as_array().cloned().unwrap_or_default();
    let manifest = common::wasm_manifest("ahead-svc", &[("echo", "/echo")]);
    let ahead = [
        cufflink_types::features::DELIVERY_CONTROL,
        cufflink_types::features::NATS_PUBLISH_V2,
        cufflink_types::features::HTTP_V2,
        cufflink_types::features::STORAGE_V2,
        cufflink_types::features::UTIL_V2,
    ]
    .into_iter()
    .filter(|feature| !supports.contains(&json!(feature)));
    for feature in ahead {
        let wasm = wat::parse_str(common::guest::module_wat().replacen(
            "(module",
            &format!(r#"(module (@custom "cufflink.features" "{feature}\n")"#),
            1,
        ))
        .expect("wat");
        let (status, body) = platform.deploy_module(&tenant, &manifest, &wasm).await;
        assert_eq!(status, 422, "{feature}: {body}");
        let error = body["error"].as_str().unwrap_or_default();
        assert!(error.contains(&format!("'{feature}'")), "{error}");
    }
}

const GUEST_FEATURES: [&str; 4] = [
    cufflink_types::features::CACHE,
    cufflink_types::features::DB_V2,
    cufflink_types::features::RESPONSE_V2,
    cufflink_types::features::SERVICE_CALL,
];

#[test]
fn sdk_guests_declare_their_features_under_every_profile() {
    for profile in Profile::ALL {
        let declared = |guest| {
            common::sdk::sdk_guest(guest, profile).map(|wasm| {
                cufflink_types::wasm_features::declared(&wasm).expect("readable sections")
            })
        };
        let Some(featured) = declared("features") else {
            return;
        };
        assert_eq!(
            featured,
            GUEST_FEATURES.map(String::from).into(),
            "{profile:?}"
        );
        assert_eq!(
            declared("no-features"),
            Some(Default::default()),
            "{profile:?}"
        );
    }
}

#[tokio::test]
async fn a_debug_built_sdk_guest_deploys_where_its_features_are_supported() {
    let Some(wasm) = common::sdk::sdk_guest("features", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    let supports = version["supports"].as_array().cloned().unwrap_or_default();
    assert!(
        GUEST_FEATURES
            .iter()
            .all(|feature| supports.contains(&json!(feature))),
        "{version}"
    );
    let (status, body) = platform
        .deploy_module(
            &tenant,
            &common::wasm_manifest("sdk-svc", &[("ping", "/ping")]),
            &wasm,
        )
        .await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn registry_reload_skips_bad_rows_and_swaps_atomically() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform =
        PlatformProcess::start(&db.url, infra.nats_url(), &[("REGISTRY_RELOAD_SECS", "1")]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    for name in ["good-svc", "bad-svc"] {
        let (status, body) = platform
            .deploy(&tenant, &common::wasm_manifest(name, &[("echo", "/echo")]))
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(platform.call_fn(&tenant, name, "echo").await.0, 200);
    }
    assert_eq!(
        platform
            .metric("cufflink_wasm_module_cache_entries", &[])
            .await,
        Some(2.0)
    );

    let bad = service_id(&db, "bad-svc").await;
    let stored: Value = scalar(
        &db,
        "SELECT d.manifest FROM deployments d JOIN services s ON s.id = d.service_id \
         WHERE s.id = $1 AND d.version = s.current_version",
        &[&bad],
    )
    .await;
    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE deployments d SET manifest = '{\"bogus\": 1}' FROM services s \
             WHERE d.service_id = s.id AND s.id = $1 AND d.version = s.current_version",
            &[&bad],
        )
        .await
        .expect("corrupt the row");
    wait_for(
        "a reload that skips the bad row and drops its module",
        Duration::from_secs(5),
        || async {
            platform.call_fn(&tenant, "bad-svc", "echo").await.0 == 404
                && platform
                    .metric("cufflink_wasm_module_cache_entries", &[])
                    .await
                    == Some(1.0)
                && platform
                    .metric("cufflink_registry_load_errors_total", &[])
                    .await
                    .is_some_and(|n| n >= 1.0)
        },
    )
    .await;

    assert_eq!(platform.call_fn(&tenant, "good-svc", "echo").await.0, 200);
    assert_eq!(platform.status("/ready").await, Some(200));

    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE deployments d SET manifest = $2 FROM services s \
             WHERE d.service_id = s.id AND s.id = $1 AND d.version = s.current_version",
            &[&bad, &stored],
        )
        .await
        .expect("restore the row");
    let version_before = platform
        .get_json("/ready")
        .await
        .and_then(|(_, body)| body["registry_version"].as_u64())
        .expect("registry version");
    wait_for(
        "a reload that restores the service",
        Duration::from_secs(5),
        || async {
            platform
                .get_json("/ready")
                .await
                .and_then(|(_, body)| body["registry_version"].as_u64())
                .is_some_and(|v| v > version_before)
                && platform.call_fn(&tenant, "bad-svc", "echo").await.0 == 200
        },
    )
    .await;
    assert_eq!(platform.call_fn(&tenant, "good-svc", "echo").await.0, 200);
}
