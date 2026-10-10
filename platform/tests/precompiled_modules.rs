use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_storage::ObjectStorage;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

struct SharedStorage {
    dir: PathBuf,
    store: Arc<dyn ObjectStorage>,
}

impl SharedStorage {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("cufflink-cwasm-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("storage dir");
        let store = cufflink_storage::local_dir(&dir, "cufflink").expect("local store");
        Self { dir, store }
    }

    fn env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("STORAGE_BACKEND", "local".into()),
            ("STORAGE_LOCAL_DIR", self.dir.display().to_string()),
        ]
    }
}

impl Drop for SharedStorage {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn start(
    db: &common::TestDb,
    nats: Option<&str>,
    storage: &SharedStorage,
) -> PlatformProcess {
    let env = storage.env();
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let platform = PlatformProcess::start(&db.url, nats, &env).await;
    platform.wait_ready().await;
    platform
}

async fn artifacts(db: &common::TestDb, service: &str) -> Vec<(i32, String, String)> {
    db.pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT a.version, a.engine_fingerprint, a.sha256 FROM module_artifacts a \
             JOIN services s ON s.id = a.service_id WHERE s.name = $1 ORDER BY a.version",
            &[&service],
        )
        .await
        .expect("artifacts")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

async fn service_id(db: &common::TestDb, service: &str) -> Uuid {
    db.pool()
        .get()
        .await
        .expect("checkout")
        .query_one("SELECT id FROM services WHERE name = $1", &[&service])
        .await
        .expect("service")
        .get(0)
}

fn key(service: Uuid, version: i32, fingerprint: &str, sha: &str) -> String {
    format!("wasm/{service}/v{version}/{fingerprint}/{sha}.cwasm")
}

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !check().await {
        assert!(Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn loads(platform: &PlatformProcess, source: &str) -> Option<f64> {
    platform
        .metric(
            "cufflink_wasm_module_load_seconds_count",
            &[("source", source)],
        )
        .await
}

#[tokio::test]
async fn precompiled_module_path() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let storage = SharedStorage::new();
    let first = start(&db, infra.nats_url(), &storage).await;
    let tenant = db.mint_tenant().await;
    let manifest = common::wasm_manifest("cwasm-svc", &[("echo", "/echo")]);
    let (status, body) = first.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let written = artifacts(&db, "cwasm-svc").await;
    assert_eq!(written.len(), 1, "{written:?}");
    let (version, fingerprint, sha) = written[0].clone();
    assert_eq!(
        i64::from(version),
        body["version"].as_i64().expect("version")
    );
    let id = service_id(&db, "cwasm-svc").await;
    let object = key(id, version, &fingerprint, &sha);
    assert!(storage.store.head(&object).await.is_ok(), "{object}");
    drop(first);

    let second = start(&db, infra.nats_url(), &storage).await;
    eventually(
        "the second replica loads the precompiled module",
        async || loads(&second, "cwasm").await == Some(1.0),
    )
    .await;
    assert_eq!(loads(&second, "compile").await, None);
    assert_eq!(second.call_fn(&tenant, "cwasm-svc", "echo").await.0, 200);
    drop(second);

    storage
        .store
        .upload(
            &object,
            b"not a module".to_vec(),
            "application/octet-stream",
        )
        .await
        .expect("corrupt the artefact");
    let third = start(&db, infra.nats_url(), &storage).await;
    eventually("a corrupted artefact falls back to compiling", async || {
        loads(&third, "compile").await == Some(1.0)
    })
    .await;
    assert_eq!(
        third
            .metric("cufflink_wasm_artifact_mismatch_total", &[])
            .await,
        Some(1.0)
    );
    assert_eq!(third.call_fn(&tenant, "cwasm-svc", "echo").await.0, 200);
    assert_eq!(
        artifacts(&db, "cwasm-svc").await,
        written,
        "the mismatched row is kept for forensics"
    );

    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute("DELETE FROM module_artifacts WHERE service_id = $1", &[&id])
        .await
        .expect("forget the artefact");
    drop(third);
    let fourth = start(&db, infra.nats_url(), &storage).await;
    eventually(
        "a replica that compiled stores the missing artefact",
        async || artifacts(&db, "cwasm-svc").await.len() == 1,
    )
    .await;

    let mut changed = manifest.clone();
    changed["publishes"] = serde_json::json!(["cwasm-svc.events.>"]);
    let (status, body) = fourth.deploy(&tenant, &changed).await;
    assert_eq!(status, 200, "{body}");
    let versions: Vec<i32> = artifacts(&db, "cwasm-svc")
        .await
        .iter()
        .map(|a| a.0)
        .collect();
    assert_eq!(versions, vec![version, version + 1]);
    let restored = artifacts(&db, "cwasm-svc").await[0].clone();
    let old_object = key(id, version, &restored.1, &restored.2);
    let (status, body) = fourth
        .send(
            reqwest::Method::DELETE,
            &format!("/api/services/{id}/deployments/{version}"),
            &format!("ApiKey {}", tenant.api_key),
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let versions: Vec<i32> = artifacts(&db, "cwasm-svc")
        .await
        .iter()
        .map(|a| a.0)
        .collect();
    assert_eq!(
        versions,
        vec![version + 1],
        "deleting a version deletes its artefact rows"
    );
    assert!(
        storage.store.head(&old_object).await.is_err(),
        "and its objects"
    );
}

fn rewritten_module() -> Vec<u8> {
    let response = r#"{"__status":200,"__body":{"rewritten":true}}"#;
    let length: String = u32::try_from(response.len())
        .expect("length")
        .to_le_bytes()
        .iter()
        .map(|b| format!("\\{b:02x}"))
        .collect();
    wat::parse_str(format!(
        r#"(module (memory (export "memory") 1)
             (data (i32.const 16) "{length}{}")
             (func (export "alloc") (param i32) (result i32) (i32.const 1024))
             (func (export "echo") (param i32 i32) (result i32) (i32.const 16)))"#,
        response.replace('"', "\\\"")
    ))
    .expect("wat")
}

async fn artifact_row(db: &common::TestDb, service: Uuid) -> (String, String) {
    let row = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT wasm_sha256, sha256 FROM module_artifacts WHERE service_id = $1",
            &[&service],
        )
        .await
        .expect("one artefact");
    (row.get(0), row.get(1))
}

#[tokio::test]
async fn a_module_rewritten_in_place_replaces_its_stale_artefact() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let storage = SharedStorage::new();
    let first = start(&db, infra.nats_url(), &storage).await;
    let tenant = db.mint_tenant().await;
    let manifest = common::wasm_manifest("stale-svc", &[("echo", "/echo")]);
    let (status, body) = first.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    drop(first);
    let id = service_id(&db, "stale-svc").await;
    let version = i32::try_from(body["version"].as_i64().expect("version")).expect("version");
    let deployed_hash = body["wasm_hash"].as_str().expect("wasm hash").to_string();
    let (recorded_wasm, stale_sha) = artifact_row(&db, id).await;
    assert_eq!(recorded_wasm, deployed_hash, "the artefact names its .wasm");
    let fingerprint = artifacts(&db, "stale-svc").await[0].1.clone();

    let rewritten = rewritten_module();
    let rewritten_hash = cufflink_platform::wasm_runtime::sha256_hex(&rewritten);
    storage
        .store
        .upload(
            &format!("wasm/{id}/v{version}.wasm"),
            rewritten,
            "application/wasm",
        )
        .await
        .expect("rewrite the module in place, as an R1 upload does");
    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE deployments SET wasm_hash = $1 WHERE service_id = $2 AND version = $3",
            &[&rewritten_hash, &id, &version],
        )
        .await
        .expect("record the rewritten module");

    let second = start(&db, infra.nats_url(), &storage).await;
    let (status, body) = second.call_fn(&tenant, "stale-svc", "echo").await;
    assert_eq!(
        (status, body),
        (200, serde_json::json!({"rewritten": true})),
        "the rewritten module is served, not the stale artefact"
    );
    assert_eq!(loads(&second, "cwasm").await, None);
    eventually("the stale artefact is replaced", async || {
        artifact_row(&db, id).await.0 == rewritten_hash
    })
    .await;
    let (_, fresh_sha) = artifact_row(&db, id).await;
    assert!(
        storage
            .store
            .head(&key(id, version, &fingerprint, &stale_sha))
            .await
            .is_err(),
        "the stale object is deleted"
    );
    let fresh = key(id, version, &fingerprint, &fresh_sha);
    assert!(storage.store.head(&fresh).await.is_ok(), "{fresh}");
    drop(second);

    storage.store.delete(&fresh).await.expect("lose the object");
    let third = start(&db, infra.nats_url(), &storage).await;
    assert_eq!(third.call_fn(&tenant, "stale-svc", "echo").await.0, 200);
    eventually(
        "an artefact whose object is gone is stored again",
        async || storage.store.head(&fresh).await.is_ok(),
    )
    .await;
    assert_eq!(artifact_row(&db, id).await, (rewritten_hash, fresh_sha));
    drop(third);

    let fourth = start(&db, infra.nats_url(), &storage).await;
    assert_eq!(fourth.call_fn(&tenant, "stale-svc", "echo").await.0, 200);
    assert_eq!(loads(&fourth, "cwasm").await, Some(1.0));
}
