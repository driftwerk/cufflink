use cufflink_testkit as common;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::PlatformProcess;
use cufflink_platform::secrets::{KeyRing, SecretBox, StorageMode, Stored, SERVICE_CONFIG_VALUE};
use reqwest::Method;
use serde_json::json;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

fn secret_file(dir: &Path, name: &str, content: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, content).expect("write the secret file");
    path.to_str().expect("a UTF-8 path").to_string()
}

#[tokio::test]
async fn cf1_platform_starts_from_secret_files() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let dir = tempfile::tempdir().expect("a directory for the secret files");
    let kek = STANDARD.encode([7u8; 32]);
    let database_url = secret_file(dir.path(), "database-url", &format!("{}\n", db.url));
    let kek_file = secret_file(dir.path(), "kek", &format!("{kek}\r\n"));
    let signing_key = secret_file(dir.path(), "signing-key", common::platform::SIGNING_KEY);

    let platform = PlatformProcess::start_with(&[
        ("DATABASE_URL_FILE", &database_url),
        ("CUFFLINK_SECRETS_KEK_FILE", &kek_file),
        ("CUFFLINK_SECRETS_KEK_ID", "a"),
        ("CUFFLINK_CACHE_SIGNING_KEY_FILE", &signing_key),
        ("NATS_URL", infra.nats_url().unwrap_or("nats://127.0.0.1:1")),
        ("PLATFORM_HOST", "127.0.0.1"),
        ("KEYCLOAK_URL", ""),
        ("STORAGE_BACKEND", "memory"),
        ("RUST_LOG", "info"),
    ])
    .await;
    platform.wait_ready().await;

    let tenant = db.mint_tenant().await;
    let manifest = common::platform::wasm_manifest("cfg", &[]);
    let (status, deployed) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy: {deployed}");
    let service_id: Uuid = deployed["service_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("the service's id");
    let (status, body) = platform
        .send(
            Method::PUT,
            &format!("/api/services/{service_id}/config"),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({"key": "API_TOKEN", "value": "tok-1", "is_secret": true})),
        )
        .await;
    assert_eq!(status, 200, "set: {body}");

    let client = db.pool().get().await.expect("checkout");
    let row = client
        .query_one(
            "SELECT value, value_enc FROM service_configs WHERE service_id = $1 AND key = 'API_TOKEN'",
            &[&service_id],
        )
        .await
        .expect("the stored value");
    let stored = Stored::new(row.get(0), row.get(1));
    let reader = SecretBox::new(
        db.pool(),
        KeyRing::parse(Some(&kek), Some("a"), None).expect("ring"),
        StorageMode::Dual,
    );
    let aad = SERVICE_CONFIG_VALUE.aad([service_id.to_string(), "API_TOKEN".to_string()]);
    let opened = reader
        .open(
            &client,
            tenant.id,
            &aad,
            stored.sealed().expect("sealed under the key of the file"),
        )
        .await
        .expect("opens under the same key");
    assert_eq!(opened.expose(), "tok-1");

    let mut refused = PlatformProcess::launch(
        &db.url,
        infra.nats_url(),
        &[("DATABASE_URL_FILE", &database_url)],
    );
    let status = refused
        .wait_exit(Duration::from_secs(30))
        .await
        .expect("the platform exits");
    assert!(!status.success());
    let said = refused.output_tail();
    assert!(
        said.contains("DATABASE_URL and DATABASE_URL_FILE are both set; set one"),
        "{said}"
    );
    assert!(!said.contains(&db.url), "no secret is echoed: {said}");
}
