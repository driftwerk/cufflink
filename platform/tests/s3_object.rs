use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_test_guests::Profile;
use serde_json::{json, Value};

const TWENTY_MIB: usize = 20 * 1024 * 1024;

#[tokio::test]
async fn s3_object_streams() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(wasm) = common::sdk::sdk_guest("responses", Profile::Debug) else {
        return;
    };
    let dir = std::env::temp_dir().join(format!("cufflink-s3-stream-{}", uuid::Uuid::new_v4()));
    let store = cufflink_storage::local_dir(&dir, "cufflink").expect("local storage");
    let object: Vec<u8> = (0..TWENTY_MIB).map(|i| (i % 251) as u8).collect();
    store
        .upload_to_bucket(
            "media",
            "tenants/acme/big.bin",
            object.clone(),
            "application/octet-stream",
        )
        .await
        .expect("upload");

    let mut db = infra.create_database().await;
    let dir_env = dir.to_string_lossy().into_owned();
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", dir_env.as_str()),
            ("S3_BUCKET", "cufflink"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy_module(
            &tenant,
            &common::wasm_manifest("files", &[("object", "/object")]),
            &wasm,
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let fetch = |body: Value| {
        let request = platform
            .http
            .post(platform.fn_url(&tenant, "files", "object"))
            .json(&body);
        async move { request.send().await.expect("request") }
    };

    let streamed = fetch(json!({"bucket": "media", "key": "tenants/acme/big.bin"})).await;
    assert_eq!(streamed.status().as_u16(), 200);
    let header = |name: &str| {
        streamed
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    assert_eq!(header("content-length"), Some(TWENTY_MIB.to_string()));
    assert_eq!(
        header("content-type").as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(
        header("content-disposition").as_deref(),
        Some("attachment; filename=\"big.bin\"")
    );
    assert!(
        streamed.bytes().await.expect("body") == object,
        "the streamed bytes differ"
    );

    let typed = fetch(json!({"bucket": "media", "key": "tenants/acme/big.bin", "content_type": "application/pdf"})).await;
    assert_eq!(
        typed
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/pdf")
    );

    let missing = fetch(json!({"bucket": "media", "key": "tenants/acme/none.bin"})).await;
    assert_eq!(missing.status().as_u16(), 404);
    assert_eq!(
        missing.json::<Value>().await.expect("json"),
        json!({"error": "Not found"})
    );

    let reserved = fetch(json!({"bucket": "cufflink", "key": "wasm/anything.wasm"})).await;
    assert_eq!(reserved.status().as_u16(), 500);
    assert_eq!(
        reserved.json::<Value>().await.expect("json"),
        json!({"error": "WASM execution error: storage access denied"})
    );

    let streamed_bytes = platform.metric("cufflink_s3_stream_bytes_total", &[]).await;
    assert!(
        streamed_bytes.is_some_and(|bytes| bytes >= TWENTY_MIB as f64),
        "{streamed_bytes:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}
