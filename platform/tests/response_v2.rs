use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_test_guests::Profile;
use serde_json::{json, Value};

const HANDLERS: [&str; 11] = [
    "created",
    "no_content",
    "export_xlsx",
    "download_pdf",
    "view_pdf",
    "redirect",
    "plain",
    "owned_headers",
    "invalid_status",
    "validation",
    "legacy_error",
];

#[tokio::test]
async fn response_v2_shapes() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(wasm) = common::sdk::sdk_guest("responses", Profile::Debug) else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let routes: Vec<(&str, String)> = HANDLERS.iter().map(|h| (*h, format!("/{h}"))).collect();
    let routes: Vec<(&str, &str)> = routes.iter().map(|(h, p)| (*h, p.as_str())).collect();
    let (status, body) = platform
        .deploy_module(&tenant, &common::wasm_manifest("exports", &routes), &wasm)
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client");
    let call = |handler: &str| {
        let request = client.post(platform.fn_url(&tenant, "exports", handler));
        async move { request.send().await.expect("request") }
    };
    let header = |response: &reqwest::Response, name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };

    let created = call("created").await;
    assert_eq!(created.status().as_u16(), 201);
    assert_eq!(
        created.json::<Value>().await.expect("json"),
        json!({"id": 1})
    );

    let empty = call("no_content").await;
    assert_eq!(empty.status().as_u16(), 204);
    assert_eq!(header(&empty, "content-type"), None);
    assert!(empty.bytes().await.expect("body").is_empty());

    let xlsx = call("export_xlsx").await;
    assert_eq!(xlsx.status().as_u16(), 200);
    assert_eq!(
        header(&xlsx, "content-type").as_deref(),
        Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    );
    assert_eq!(
        header(&xlsx, "content-disposition").as_deref(),
        Some("attachment; filename=\"test.xlsx\"")
    );
    assert_eq!(&xlsx.bytes().await.expect("body")[..], &[1, 2, 3, 4, 5]);

    let pdf = call("download_pdf").await;
    assert_eq!(
        header(&pdf, "content-type").as_deref(),
        Some("application/pdf")
    );
    assert_eq!(
        header(&pdf, "content-disposition").as_deref(),
        Some("attachment; filename=\"receipt.pdf\"")
    );
    assert_eq!(
        header(&pdf, "cache-control").as_deref(),
        Some("no-cache, no-store, must-revalidate")
    );
    assert_eq!(header(&pdf, "pragma").as_deref(), Some("no-cache"));
    assert_eq!(&pdf.bytes().await.expect("body")[..], b"%PDF-1.4");

    let inline = call("view_pdf").await;
    assert_eq!(
        header(&inline, "content-disposition").as_deref(),
        Some("inline; filename=\"receipt.pdf\"")
    );

    let redirect = call("redirect").await;
    assert_eq!(redirect.status().as_u16(), 303);
    assert_eq!(header(&redirect, "location").as_deref(), Some("/books/7"));

    let plain = call("plain").await;
    assert_eq!(
        header(&plain, "content-type").as_deref(),
        Some("text/plain; charset=utf-8")
    );
    assert_eq!(plain.text().await.expect("text"), "pong");

    let owned = call("owned_headers").await;
    assert_eq!(owned.status().as_u16(), 200);
    assert_eq!(header(&owned, "set-cookie"), None);
    assert_eq!(header(&owned, "x-total-count").as_deref(), Some("3"));
    assert_ne!(header(&owned, "x-request-id").as_deref(), Some("forged"));
    assert_eq!(
        owned.json::<Value>().await.expect("json"),
        json!({"ok": true})
    );

    let invalid = call("invalid_status").await;
    assert_eq!(invalid.status().as_u16(), 500);
    assert_eq!(
        invalid.json::<Value>().await.expect("json"),
        json!({"error": "WASM execution error: invalid response status"})
    );

    let validation = call("validation").await;
    assert_eq!(validation.status().as_u16(), 422);
    assert_eq!(
        validation.json::<Value>().await.expect("json"),
        json!({"validation_errors": {"content": ["Content cannot be empty"]}})
    );

    let legacy = call("legacy_error").await;
    assert_eq!(legacy.status().as_u16(), 404);
    assert_eq!(
        legacy.json::<Value>().await.expect("json"),
        json!({"error": "gone"})
    );

    for name in ["set-cookie", "access-control-allow-origin", "x-request-id"] {
        assert_eq!(
            platform
                .metric("cufflink_guest_header_dropped_total", &[("name", name)])
                .await,
            Some(1.0),
            "{name}"
        );
    }
}
