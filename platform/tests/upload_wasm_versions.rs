use cufflink_testkit as common;

use common::PlatformProcess;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use uuid::Uuid;

const SECOND_MODULE: &str = r#"
(module
  (memory (export "memory") 1)
  (data (i32.const 0) "\07\00\00\00{\"v\":2}")
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "echo") (param i32 i32) (result i32) (i32.const 0)))
"#;

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

async fn upload(
    platform: &PlatformProcess,
    tenant: &common::Tenant,
    id: Uuid,
    wasm: &[u8],
    commit_hash: Option<&str>,
) -> (u16, Value) {
    use base64::Engine;
    platform
        .send(
            reqwest::Method::POST,
            &format!("/api/services/{id}/wasm"),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({
                "wasm_base64": base64::engine::general_purpose::STANDARD.encode(wasm),
                "commit_hash": commit_hash,
            })),
        )
        .await
}

#[tokio::test]
async fn upload_wasm_creates_version() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let dir = std::env::temp_dir().join(format!("cufflink-upload-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("storage dir");
    let dir_setting = dir.display().to_string();
    let env = [
        ("STORAGE_BACKEND", "local"),
        ("STORAGE_LOCAL_DIR", dir_setting.as_str()),
    ];
    let first = PlatformProcess::start(&db.url, Some(nats), &env).await;
    let second = PlatformProcess::start(&db.url, Some(nats), &env).await;
    first.wait_ready().await;
    second.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let (status, body) = first
        .deploy(
            &tenant,
            &common::wasm_manifest("upload-svc", &[("echo", "/echo")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let id: Uuid = body["service_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("service id");
    let deployed = body["version"].as_i64().expect("version");
    let original = common::guest::module_bytes();

    let (status, body) = upload(&first, &tenant, id, &original, None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["unchanged"], true);
    assert_eq!(body["version"], deployed);

    let replacement = wat::parse_str(SECOND_MODULE).expect("wat");
    let (status, body) = upload(&first, &tenant, id, &replacement, Some("abc123")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["commit_hash"], "abc123");
    assert_eq!(body["version"], deployed + 1);
    assert_eq!(body["previous_version"], deployed);
    assert_eq!(body["wasm_hash"], sha256_hex(&replacement));

    let store = cufflink_storage::local_dir(&dir, "cufflink").expect("store");
    let kept = store
        .download(&format!("wasm/{id}/v{deployed}.wasm"))
        .await
        .expect("the previous version's module");
    assert_eq!(sha256_hex(&kept), sha256_hex(&original), "left untouched");

    for replica in [&first, &second] {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let (status, body) = replica.call_fn(&tenant, "upload-svc", "echo").await;
            if (status, &body) == (200, &json!({"v": 2})) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "replica on port {} still serves {status} {body}",
                replica.port
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let (status, body) = first
        .api_get(&tenant, &format!("/api/services/{id}/deployments"))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["current_version"], deployed + 1);
    let hashes: Vec<Value> = body["deployments"]
        .as_array()
        .expect("deployments")
        .iter()
        .map(|d| json!([d["version"], d["wasm_hash"], d["commit_hash"]]))
        .collect();
    assert_eq!(
        hashes,
        vec![
            json!([deployed + 1, sha256_hex(&replacement), "abc123"]),
            json!([deployed, sha256_hex(&original), null]),
        ]
    );
    let audited = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT details FROM audit_log WHERE action = 'upload_wasm' AND resource_id = $1",
            &[&id.to_string()],
        )
        .await
        .expect("one upload audited")
        .get::<_, Value>(0);
    assert_eq!(
        audited,
        json!({
            "version": deployed + 1,
            "previous_version": deployed,
            "wasm_hash": sha256_hex(&replacement),
            "commit_hash": "abc123",
        })
    );

    let (status, body) = upload(&first, &tenant, id, &replacement, None).await;
    assert_eq!((status, &body["unchanged"]), (200, &json!(true)), "{body}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn an_unchanged_upload_restores_a_module_storage_lost() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let dir = std::env::temp_dir().join(format!("cufflink-heal-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("storage dir");
    let dir_setting = dir.display().to_string();
    let env = [
        ("STORAGE_BACKEND", "local"),
        ("STORAGE_LOCAL_DIR", dir_setting.as_str()),
    ];
    let first = PlatformProcess::start(&db.url, Some(nats), &env).await;
    first.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = first
        .deploy(
            &tenant,
            &common::wasm_manifest("heal-svc", &[("echo", "/echo")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let id: Uuid = body["service_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("service id");
    let version = body["version"].as_i64().expect("version");
    let module = common::guest::module_bytes();

    let store = cufflink_storage::local_dir(&dir, "cufflink").expect("store");
    let artifacts: Vec<String> = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT version, engine_fingerprint, sha256 FROM module_artifacts WHERE service_id = $1",
            &[&id],
        )
        .await
        .expect("artifacts")
        .iter()
        .map(|row| {
            format!(
                "wasm/{id}/v{}/{}/{}.cwasm",
                row.get::<_, i32>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2)
            )
        })
        .chain([format!("wasm/{id}/v{version}.wasm")])
        .collect();
    for key in &artifacts {
        store.delete(key).await.expect("delete");
    }

    let second = PlatformProcess::start(&db.url, Some(nats), &env).await;
    second.wait_ready().await;
    let (status, body) = second.call_fn(&tenant, "heal-svc", "echo").await;
    assert_eq!(status, 500, "the lost module cannot load: {body}");

    let (status, body) = upload(&first, &tenant, id, &module, None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (&body["unchanged"], &body["restored"], &body["version"]),
        (&json!(true), &json!(true), &json!(version))
    );
    let stored = store
        .download(&format!("wasm/{id}/v{version}.wasm"))
        .await
        .expect("stored again");
    assert_eq!(sha256_hex(&stored), sha256_hex(&module));

    let (status, body) = second.call_fn(&tenant, "heal-svc", "echo").await;
    assert_eq!(status, 200, "the replica loads the restored module: {body}");

    let (status, body) = upload(&first, &tenant, id, &module, None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (&body["unchanged"], &body["restored"]),
        (&json!(true), &json!(false))
    );
    let _ = std::fs::remove_dir_all(&dir);
}
