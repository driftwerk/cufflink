use cufflink_testkit as common;

use axum::body::Bytes;
use axum::extract::RawQuery;
use axum::http::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use axum::response::AppendHeaders;
use axum::routing::{get, post};
use axum::Router;
use common::{PlatformProcess, Tenant};
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use tokio::sync::mpsc;

const SERVICE: &str = "receipts";
const PDF: &[u8] = &[0x25, 0x50, 0x44, 0x46, 0x2d, 0x00, 0xff, 0xfe, 0x0d, 0x0a];
const HTML: &str = "<h1>Receipt £120</h1>\r\n--not-a-boundary--\r\n";
const MARGIN_RESET: &str =
    "<style>@page { margin: 0; } html, body { margin: 0; padding: 0; }</style>";
const RESPONSE_CEILING: usize = 4096;

type Form = (String, Bytes);

async fn upstream() -> (String, mpsc::UnboundedReceiver<Form>) {
    let (forms, received) = mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/convert/html",
            post(move |headers: HeaderMap, body: Bytes| {
                let forms = forms.clone();
                async move {
                    let content_type = headers
                        .get(CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let _ = forms.send((content_type, body));
                    (
                        [(CONTENT_TYPE, "application/pdf")],
                        AppendHeaders([("set-cookie", "a=1"), ("set-cookie", "b=2")]),
                        PDF,
                    )
                }
            }),
        )
        .route(
            "/v1/promotion_codes",
            get(|RawQuery(query): RawQuery| async move { query.unwrap_or_default() }),
        )
        .route("/big", get(|| async { "x".repeat(RESPONSE_CEILING + 1) }))
        .route(
            "/moved",
            get(|| async {
                (
                    StatusCode::FOUND,
                    [("location", "/v1/promotion_codes?a=1")],
                    "",
                )
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

fn renderer_form(boundary: &str) -> Vec<u8> {
    let fields = [
        ("emulatedMediaType", "print"),
        ("printBackground", "true"),
        ("preferCssPageSize", "true"),
        ("waitDelay", "1s"),
        ("marginTop", "0"),
        ("marginBottom", "0"),
        ("marginLeft", "0"),
        ("marginRight", "0"),
    ]
    .iter()
    .map(|(name, value)| {
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
        )
    })
    .collect::<String>();
    format!(
        "{fields}--{boundary}\r\n\
         Content-Disposition: form-data; name=\"files\"; filename=\"index.html\"\r\n\
         Content-Type: text/html\r\n\r\n\
         {MARGIN_RESET}{HTML}\r\n\
         --{boundary}--\r\n"
    )
    .into_bytes()
}

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

    async fn fetch(&self, request: Value) -> Value {
        self.call("fetch", request).await
    }
}

fn text(response: &Value) -> String {
    let bytes = response["body"]
        .as_array()
        .unwrap_or_else(|| panic!("no body: {response}"))
        .iter()
        .map(|byte| u8::try_from(byte.as_u64().expect("byte")).expect("byte"))
        .collect();
    String::from_utf8(bytes).expect("utf8")
}

#[tokio::test]
async fn http_v2_bytes_and_multipart() {
    let Some(wasm) = common::sdk::sdk_guest("http-v2", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let ceiling = RESPONSE_CEILING.to_string();
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("HOST_HTTP_V2_MAX_RESPONSE_BYTES", ceiling.as_str()),
            ("HOST_HTTP_MAX_REQUEST_BYTES", "2048"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("http_v2"))),
        "{version}"
    );
    let routes = [
        ("convert", "/convert"),
        ("fetch", "/fetch"),
        ("legacy", "/legacy"),
    ];
    let (status, body) = platform
        .deploy_module(&tenant, &common::wasm_manifest(SERVICE, &routes), &wasm)
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let guest = Guest {
        platform: &platform,
        tenant: &tenant,
    };
    let (base, mut forms) = upstream().await;

    let converted = guest
        .call("convert", json!({"renderer": base, "html": HTML}))
        .await;
    assert_eq!(
        converted,
        json!({
            "status": 200,
            "content_type": "application/pdf",
            "cookies": ["a=1", "b=2"],
            "body": PDF,
        })
    );
    let (content_type, form) = forms.recv().await.expect("a form");
    let boundary = content_type
        .strip_prefix("multipart/form-data; boundary=")
        .unwrap_or_else(|| panic!("{content_type}"));
    assert!(
        boundary.len() == 64 && boundary.bytes().all(|b| b.is_ascii_hexdigit()),
        "{boundary}"
    );
    assert_eq!(
        String::from_utf8_lossy(&form),
        String::from_utf8_lossy(&renderer_form(boundary))
    );
    assert_eq!(form, renderer_form(boundary));
    guest
        .call("convert", json!({"renderer": base, "html": HTML}))
        .await;
    let (again, _) = forms.recv().await.expect("a form");
    assert_ne!(again, content_type, "every form has a boundary of its own");

    let query = "code=SPRING&expand[]=data.coupon&expand[]=data.customer";
    let url = format!("{base}/v1/promotion_codes?{query}");
    assert_eq!(
        guest.call("legacy", json!({"url": url})).await,
        json!({"status": 200, "body": query})
    );
    let listed = guest.fetch(json!({"method": "GET", "url": url})).await;
    assert_eq!(
        (listed["status"].as_u64(), text(&listed)),
        (Some(200), query.to_string())
    );

    let moved = format!("{base}/moved");
    let unfollowed = guest.fetch(json!({"method": "GET", "url": moved})).await;
    assert_eq!(unfollowed["status"], 302, "{unfollowed}");
    let followed = guest
        .fetch(json!({"method": "GET", "url": moved, "follow_redirects": true}))
        .await;
    assert_eq!(
        (followed["status"].as_u64(), text(&followed)),
        (Some(200), "a=1".to_string())
    );

    let kind = |response: Value| response["error"].as_str().unwrap_or_default().to_string();
    let big = format!("{base}/big");
    for request in [
        json!({"method": "GET", "url": big, "max_response_bytes": 1_000_000}),
        json!({"method": "GET", "url": url, "max_response_bytes": 8}),
        json!({"method": "POST", "url": url, "body": vec![7; 2049]}),
    ] {
        let refused = guest.fetch(request.clone()).await;
        assert_eq!(kind(refused.clone()), "too_large", "{request}: {refused}");
    }
    for (request, expected) in [
        (
            json!({"method": "GET", "url": "http://169.254.169.254/latest/meta-data/"}),
            "denied",
        ),
        (
            json!({"method": "GET", "url": "http://metadata.google.internal/"}),
            "denied",
        ),
        (
            json!({"method": "GET", "url": "file:///etc/passwd"}),
            "invalid_request",
        ),
        (json!({"method": "TRACE", "url": url}), "invalid_request"),
        (
            json!({"method": "GET", "url": url, "headers": [["x-a", "b\r\nx-c: d"]]}),
            "invalid_request",
        ),
    ] {
        let refused = guest.fetch(request.clone()).await;
        assert_eq!(kind(refused.clone()), expected, "{request}: {refused}");
    }

    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let policies = format!("/api/tenants/{}/policies", tenant.slug);
    let enforce = json!({"policies": {"egress": {"mode": "enforce"}}});
    let (status, body) = platform
        .send(Method::PUT, &policies, &admin, Some(&enforce))
        .await;
    assert_eq!(status, 200, "{body}");
    let refused = guest.fetch(json!({"method": "GET", "url": url})).await;
    assert_eq!(kind(refused.clone()), "denied", "{refused}");
    assert_eq!(
        guest.call("legacy", json!({"url": url})).await,
        json!({"none": true}),
        "the legacy import fails as it always did"
    );
    assert!(
        forms.try_recv().is_err(),
        "nothing else reached the PDF renderer"
    );

    for (result, calls) in [
        ("ok", 5.0),
        ("too_large", 3.0),
        ("denied", 3.0),
        ("error", 3.0),
    ] {
        let counted = platform
            .metric("cufflink_host_http_v2_total", &[("result", result)])
            .await;
        assert_eq!(counted, Some(calls), "{result}");
    }
}
