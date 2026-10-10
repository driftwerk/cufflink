use cufflink_testkit as common;

use common::{ChildProcess, Tenant, TestDb};
use serde_json::json;
use std::path::Path;
use uuid::Uuid;

const BASE_DOMAIN: &str = "apps.test";

async fn recorded(db: &TestDb, tenant: &Tenant, name: &str, mode: &str, key: &str) {
    let client = db.pool().get().await.expect("checkout");
    let service_id: Uuid = client
        .query_one(
            "INSERT INTO services (tenant_id, name) VALUES ($1, $2) RETURNING id",
            &[&tenant.id, &name],
        )
        .await
        .expect("service")
        .get(0);
    client
        .execute(
            "INSERT INTO deployments (service_id, version, manifest_hash, manifest,
                                      artifact_s3_key, artifact_hash, artifact_size)
             VALUES ($1, 1, 'm', $2, $3, 'ab', 2)",
            &[
                &service_id,
                &json!({"name": name, "mode": mode, "tables": []}),
                &key,
            ],
        )
        .await
        .expect("deployment");
}

#[tokio::test]
async fn a_native_service_is_no_web_app_to_the_runner() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    recorded(&db, &tenant, "ext-native", "native", "native/x/ab").await;
    recorded(&db, &tenant, "site", "web", "web/site/v1.tar.gz").await;

    let (storage, apps) = (
        tempfile::tempdir().expect("storage"),
        tempfile::tempdir().expect("apps"),
    );
    let (storage_dir, apps_dir) = (
        storage.path().to_string_lossy().to_string(),
        apps.path().to_string_lossy().to_string(),
    );
    let runner = ChildProcess::spawn(
        Path::new(env!("CARGO_BIN_EXE_cufflink-web-runner")),
        "RUNNER_PORT",
        &[
            ("DATABASE_URL", db.url.as_str()),
            ("NATS_URL", nats_url),
            ("CUFFLINK_BASE_DOMAIN", BASE_DOMAIN),
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", storage_dir.as_str()),
            ("APPS_DIR", apps_dir.as_str()),
            ("WEB_PORT_RANGE", "21000-21010"),
        ],
    )
    .await;

    let answer = |service: &str| {
        let request = runner
            .http
            .get(format!("{}/", runner.base_url))
            .header("host", format!("{service}--{}.{BASE_DOMAIN}", tenant.slug));
        async move { request.send().await.expect("a request").status().as_u16() }
    };
    assert_eq!(
        answer("ext-native").await,
        404,
        "a native service has no web deployment"
    );
    assert_eq!(
        answer("site").await,
        502,
        "a web app's bundle is looked for, and missing"
    );
}
