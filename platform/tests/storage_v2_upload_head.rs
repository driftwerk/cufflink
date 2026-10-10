use cufflink_testkit as common;

use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{PlatformProcess, Tenant};
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use tokio::sync::mpsc;

const SERVICE: &str = "receipts";
const PLATFORM_BUCKET: &str = "platform-objects";
const PDF: &[u8] = &[0x25, 0x50, 0x44, 0x46, 0x2d, 0x00, 0xff, 0xfe, 0x0d, 0x0a];
const UPLOAD_CAP: usize = 1024;
const ROUTES: [(&str, &str); 5] = [
    ("upload", "/upload"),
    ("head", "/head"),
    ("download", "/download"),
    ("receipt_pdf", "/receipt-pdf"),
    ("send_receipts", "/send-receipts"),
];

struct Guest<'a> {
    platform: &'a PlatformProcess,
    tenant: &'a Tenant,
}

impl Guest<'_> {
    async fn call(&self, handler: &str, body: Value) -> Value {
        let response = self
            .platform
            .http
            .post(self.platform.fn_url(self.tenant, SERVICE, handler))
            .json(&body)
            .send()
            .await
            .expect("fn request");
        assert_eq!(response.status().as_u16(), 200, "{handler}");
        response.json().await.expect("json body")
    }

    async fn upload(&self, bucket: &str, key: &str, body: &[u8]) -> Value {
        self.call(
            "upload",
            json!({"bucket": bucket, "key": key, "content_type": "application/pdf", "body": body}),
        )
        .await
    }

    async fn refused(&self, bucket: &str, key: &str, body: &[u8]) -> String {
        let refused = self.upload(bucket, key, body).await;
        refused["error"]
            .as_str()
            .unwrap_or_else(|| panic!("{bucket}/{key} was stored: {refused}"))
            .to_string()
    }
}

async fn deployed(platform: &PlatformProcess, tenant: &Tenant, wasm: &[u8]) {
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("storage_v2"))),
        "{version}"
    );
    let (status, body) = platform
        .deploy_module(tenant, &common::wasm_manifest(SERVICE, &ROUTES), wasm)
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
}

#[tokio::test]
async fn storage_v2_upload_head() {
    let Some(wasm) = common::sdk::sdk_guest("storage-v2", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let cap = UPLOAD_CAP.to_string();
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("S3_BUCKET", PLATFORM_BUCKET),
            ("HOST_STORAGE_MAX_UPLOAD_BYTES", cap.as_str()),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    deployed(&platform, &tenant, &wasm).await;
    let guest = Guest {
        platform: &platform,
        tenant: &tenant,
    };

    let key = "0b5c7a9e/7d1f3c2a_Riverside.pdf";
    let absent = json!({"bucket": "receipts", "key": key});
    assert_eq!(
        guest.call("head", absent.clone()).await,
        json!({"found": null})
    );
    let stored = guest
        .call(
            "upload",
            json!({
                "bucket": "receipts",
                "key": key,
                "content_type": "application/pdf",
                "content_disposition": "attachment; filename=\"receipt.pdf\"",
                "cache_control": "no-store",
                "body": PDF,
            }),
        )
        .await;
    assert_eq!(stored["size"], PDF.len(), "{stored}");
    let found = guest.call("head", absent.clone()).await["found"].clone();
    assert_eq!(found["size"], PDF.len(), "{found}");
    assert_eq!(found["content_type"], "application/pdf");
    assert_eq!(found["etag"], stored["etag"]);
    let modified = found["last_modified"].as_str().expect("last_modified");
    assert!(
        chrono::DateTime::parse_from_rfc3339(modified).is_ok(),
        "{modified}"
    );
    assert_eq!(
        guest.call("download", absent.clone()).await,
        json!({"base64": STANDARD.encode(PDF)})
    );
    let replaced = guest.upload("receipts", key, b"%PDF-1.7").await;
    assert_eq!(replaced["size"], 8, "{replaced}");
    assert_eq!(guest.call("head", absent).await["found"]["size"], 8);
    let at_the_cap = guest.upload("receipts", "cap", &[7; UPLOAD_CAP]).await;
    assert_eq!(at_the_cap["size"], UPLOAD_CAP, "{at_the_cap}");

    assert_eq!(
        guest
            .refused("receipts", "over", &[7; UPLOAD_CAP + 1])
            .await,
        "too_large"
    );
    for (bucket, key) in [
        (PLATFORM_BUCKET, "wasm/receipts/v1.wasm"),
        (PLATFORM_BUCKET, "backups/receipts/export.json"),
        (PLATFORM_BUCKET, "files/a"),
        (PLATFORM_BUCKET, "/web/receipts/v3.tar.gz"),
        ("receipts", "a/../b"),
        ("receipts/wasm", "a"),
        ("", "a"),
        ("receipts", ""),
    ] {
        assert_eq!(
            guest.refused(bucket, key, PDF).await,
            "denied",
            "{bucket}/{key}"
        );
        let unseen = guest
            .call("head", json!({"bucket": bucket, "key": key}))
            .await;
        if !key.is_empty() {
            assert_eq!(unseen["error"], "denied", "{bucket}/{key}: {unseen}");
        }
    }
    let beside = guest.upload(PLATFORM_BUCKET, "exports/a.pdf", PDF).await;
    assert_eq!(beside["size"], PDF.len(), "{beside}");
    assert_eq!(
        guest
            .call(
                "upload",
                json!({
                    "bucket": "receipts",
                    "key": "injected",
                    "content_type": "text/html\r\nx-amz-acl: public-read",
                    "body": PDF,
                }),
            )
            .await["error"],
        "denied"
    );

    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let policies = format!("/api/tenants/{}/policies", tenant.slug);
    let scope = |config: Value| json!({"policies": {"storage_scope": {"mode": "enforce", "config": config}}});
    let (status, body) = platform
        .send(Method::PUT, &policies, &admin, Some(&scope(json!({}))))
        .await;
    assert_eq!(status, 200, "{body}");
    let shared = json!({"bucket": "shared", "key": "x.pdf"});
    assert_eq!(
        guest.upload("shared", "x.pdf", PDF).await["size"],
        PDF.len()
    );
    assert_eq!(
        guest.call("head", shared.clone()).await["found"]["size"],
        PDF.len()
    );
    let claimant = db.mint_tenant().await;
    let (status, body) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/policies", claimant.slug),
            &admin,
            Some(&scope(json!({"allowed_buckets": ["shared"]}))),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        guest.call("head", shared).await["error"],
        "denied",
        "a bucket another tenant claims, though the object is there"
    );
    assert_eq!(
        guest
            .call("head", json!({"bucket": "shared", "key": "missing.pdf"}))
            .await["error"],
        "denied"
    );
    assert_eq!(guest.refused("shared", "y.pdf", PDF).await, "denied");

    let (status, body) = platform
        .send(
            Method::PUT,
            &policies,
            &admin,
            Some(&scope(json!({"allowed_buckets": ["receipts"]}))),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(guest.refused("emails", "a.pdf", PDF).await, "denied");
    assert_eq!(
        guest.upload("receipts", "a.pdf", PDF).await["size"],
        PDF.len()
    );

    let (status, body) = platform
        .send(
            Method::PUT,
            &policies,
            &admin,
            Some(&scope(
                json!({"scope": "strict", "allowed_buckets": ["receipts"]}),
            )),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let own = format!("tenants/{}/{SERVICE}/a.pdf", tenant.slug);
    let others = format!("tenants/{}/emails/a.pdf", tenant.slug);
    assert_eq!(guest.upload("receipts", &own, PDF).await["size"], PDF.len());
    assert_eq!(guest.refused("receipts", &others, PDF).await, "denied");
    assert_eq!(guest.refused("receipts", "a.pdf", PDF).await, "denied");
    assert_eq!(
        guest
            .call("head", json!({"bucket": "receipts", "key": "a.pdf"}))
            .await["error"],
        "denied"
    );

    for (op, result, calls) in [
        ("upload", "ok", 7.0),
        ("upload", "too_large", 1.0),
        ("upload", "denied", 13.0),
        ("head", "ok", 3.0),
        ("head", "not_found", 1.0),
        ("head", "denied", 11.0),
    ] {
        let counted = platform
            .metric(
                "cufflink_host_storage_v2_total",
                &[("op", op), ("result", result)],
            )
            .await;
        assert_eq!(counted, Some(calls), "{op} {result}");
    }
}

const ENVELOPE_CAP: usize = 774_144;

#[tokio::test]
async fn an_upload_is_held_to_what_its_envelope_can_carry() {
    let Some(wasm) = common::sdk::sdk_guest("storage-v2", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("S3_BUCKET", PLATFORM_BUCKET),
            ("WASM_MAX_GUEST_LEN_MB", "1"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    deployed(&platform, &tenant, &wasm).await;
    let guest = Guest {
        platform: &platform,
        tenant: &tenant,
    };
    let sized = |len: usize| {
        guest.call(
            "upload",
            json!({"bucket": "receipts", "key": "sized", "content_type": "application/pdf", "body_len": len}),
        )
    };

    let at_the_cap = sized(ENVELOPE_CAP).await;
    assert_eq!(at_the_cap["size"], ENVELOPE_CAP, "{at_the_cap}");
    let over = sized(ENVELOPE_CAP + 1).await;
    assert_eq!(over["error"], "too_large", "{over}");
    assert_eq!(
        over["message"],
        format!(
            "upload of {} bytes exceeds the cap of {ENVELOPE_CAP} bytes",
            ENVELOPE_CAP + 1
        )
    );
    let unreadable = sized(1024 * 1024).await;
    assert_eq!(unreadable["error"], "too_large", "{unreadable}");
    assert_eq!(
        unreadable["message"],
        "the platform could not read the request (it may exceed the size limit)"
    );
    for (result, calls) in [("ok", Some(1.0)), ("too_large", Some(1.0))] {
        let counted = platform
            .metric(
                "cufflink_host_storage_v2_total",
                &[("op", "upload"), ("result", result)],
            )
            .await;
        assert_eq!(counted, calls, "upload {result}");
    }
    let over_the_envelope = platform
        .metric(
            "cufflink_wasm_limit_events_total",
            &[("limit", "length"), ("mode", "enforce")],
        )
        .await;
    assert_eq!(over_the_envelope, Some(1.0));
}

async fn upstream() -> (String, mpsc::UnboundedReceiver<(String, Value)>) {
    let (emails, received) = mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/convert/html",
            post(|_: Bytes| async { ([("content-type", "application/pdf")], PDF) }),
        )
        .route(
            "/messages",
            post(move |headers: HeaderMap, Json(email): Json<Value>| {
                let emails = emails.clone();
                async move {
                    let header = |name: &str| {
                        headers
                            .get(name)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string()
                    };
                    assert_eq!(header("accept"), "application/json");
                    let _ = emails.send((header("x-api-token"), email));
                    Json(json!({"id": "m-1", "status": "accepted"}))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (base, received)
}

#[tokio::test]
async fn a_receipt_pdf_goes_from_the_renderer_to_storage_to_a_mail_attachment() {
    let Some(wasm) = common::sdk::sdk_guest("storage-v2", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let objects = tempfile::tempdir().expect("storage directory");
    let directory = objects.path().to_str().expect("utf-8 path");
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", directory),
            ("S3_BUCKET", PLATFORM_BUCKET),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    deployed(&platform, &tenant, &wasm).await;
    let guest = Guest {
        platform: &platform,
        tenant: &tenant,
    };
    let (base, mut emails) = upstream().await;
    let (book_id, receipt_id) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());

    let rendered = guest
        .call(
            "receipt_pdf",
            json!({
                "renderer": base,
                "bucket": "harbor-receipts",
                "book_id": book_id,
                "receipt_id": receipt_id,
                "branch": "Riverside",
                "html": "<h1>Receipt</h1>",
            }),
        )
        .await;
    let key = format!("{book_id}/{receipt_id}_Riverside.pdf");
    assert_eq!(rendered, json!({"key": key, "size": PDF.len()}));
    let stored = cufflink_storage::local_dir(objects.path(), PLATFORM_BUCKET)
        .expect("the platform's directory")
        .download_from_bucket("harbor-receipts", &key)
        .await
        .expect("the stored PDF");
    assert_eq!(stored, PDF);

    let sent = guest
        .call(
            "send_receipts",
            json!({
                "mail_api": base,
                "token": "server-token",
                "bucket": "harbor-receipts",
                "from": "Info <receipts@example.com>",
                "to": "client@example.com",
                "subject": "Your receipts",
                "html": "<p>Attached.</p>",
                "attachments": [{"key": key, "filename": "Riverside.pdf"}],
            }),
        )
        .await;
    assert_eq!(
        sent,
        json!({"status": 200, "sent": {"id": "m-1", "status": "accepted"}})
    );
    let (token, email) = emails.recv().await.expect("an email");
    assert_eq!(token, "server-token");
    assert_eq!(
        email,
        json!({
            "from": "Info <receipts@example.com>",
            "to": "client@example.com",
            "html": "<p>Attached.</p>",
            "subject": "Your receipts",
            "attachments": [{
                "name": "Riverside.pdf",
                "content": STANDARD.encode(PDF),
                "content_type": "application/pdf",
            }],
        })
    );

    let missing = guest
        .call(
            "send_receipts",
            json!({
                "mail_api": base,
                "bucket": "harbor-receipts",
                "attachments": [{"key": "missing.pdf", "filename": "missing.pdf"}],
            }),
        )
        .await;
    assert_eq!(missing["error"], "attachment", "{missing}");
    assert!(
        emails.try_recv().is_err(),
        "nothing was sent without its PDF"
    );
}
