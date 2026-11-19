use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use reqwest::header::HeaderMap;
use reqwest::Method;
use serde_json::{json, Value};
use std::sync::LazyLock;
use std::time::Duration;

static UNREDIRECTED: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client")
});

const ALLOWED: &str = "https://app.example.com";
const FOREIGN: &str = "https://harbor.example.com";

const EMBEDDED: [(&str, &str); 7] = [
    ("swagger-initializer.js", "text/javascript"),
    ("swagger-ui.css", "text/css"),
    ("index.css", "text/css"),
    ("swagger-ui-bundle.js", "text/javascript"),
    ("swagger-ui-standalone-preset.js", "text/javascript"),
    ("favicon-32x32.png", "image/png"),
    ("favicon-16x16.png", "image/png"),
];

fn doc(name: &str, mount: &str, cors: &str) -> Value {
    json!({
        "name": name,
        "mount": mount,
        "include_tags": ["Catalog"],
        "info": {"title": format!("Catalog {name}"), "version": "1.0.0"},
        "cors": cors,
    })
}

fn catalog(docs: Value) -> Value {
    let mut manifest = common::wasm_manifest("catalog-service", &[]);
    manifest["routing"] = json!("strict");
    manifest["errors"] = json!({"key": "error_message"});
    manifest["openapi"] = json!({"title": "Catalog", "docs": docs});
    manifest["custom_routes"] = json!([{
        "method": "GET",
        "path": "/orders",
        "handler": "echo",
        "posture": {"kind": "public", "reason": "integration test"},
        "doc": {"tags": ["Catalog"]},
    }]);
    manifest
}

fn documented() -> Value {
    catalog(json!([
        doc("public-api", "/docs/public-api", "mirror"),
        doc("open-api", "/docs/open", "any"),
        doc("own-api", "/docs/own", "restricted"),
    ]))
}

struct Answer {
    status: u16,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
}

async fn send(
    platform: &PlatformProcess,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
) -> Answer {
    let response = headers
        .iter()
        .fold(
            UNREDIRECTED.request(method, format!("{}{path}", platform.base_url)),
            |request, (name, value)| request.header(*name, *value),
        )
        .send()
        .await
        .expect("request");
    Answer {
        status: response.status().as_u16(),
        headers: response.headers().clone(),
        body: response.bytes().await.expect("body").to_vec(),
    }
}

async fn get(platform: &PlatformProcess, path: &str) -> Answer {
    send(platform, Method::GET, path, &[]).await
}

async fn deployed(platform: &PlatformProcess, tenant: &Tenant, manifest: &Value) -> Value {
    let (status, body) = platform.deploy(tenant, manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    body
}

#[tokio::test]
async fn public_documents_are_served_without_a_token() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CORS_ALLOWED_ORIGINS", ALLOWED)],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let other = db.mint_tenant().await;

    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|s| s.contains(&json!("public_docs"))),
        "{version}"
    );

    let service = deployed(&platform, &tenant, &documented()).await;
    deployed(&platform, &other, &catalog(json!([]))).await;
    let svc = format!("/svc/{}/catalog-service", tenant.slug);
    let mount = format!("{svc}/docs/public-api");

    let spec = get(&platform, &format!("{mount}/openapi.json")).await;
    assert_eq!(spec.status, 200);
    assert_eq!(spec.header("content-type"), Some("application/json"));
    assert_eq!(spec.header("cache-control"), Some("no-cache"));
    let document = spec.json();
    assert_eq!(document["info"]["title"], "Catalog public-api");
    assert_eq!(
        document["paths"]
            .as_object()
            .expect("paths")
            .keys()
            .collect::<Vec<_>>(),
        ["/orders"]
    );
    let etag = spec.header("etag").expect("etag").to_string();

    let page = get(&platform, &format!("{mount}/swagger-ui/")).await;
    let index = get(&platform, &format!("{mount}/swagger-ui/index.html")).await;
    assert_eq!((page.status, index.status), (200, 200));
    assert_eq!(
        page.header("content-type"),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(page.body, index.body);
    assert!(String::from_utf8_lossy(&page.body).contains("./swagger-initializer.js"));
    for (file, content_type) in EMBEDDED {
        let answer = get(&platform, &format!("{mount}/swagger-ui/{file}")).await;
        assert_eq!(answer.status, 200, "{file}");
        assert_eq!(answer.header("content-type"), Some(content_type), "{file}");
        assert!(!answer.body.is_empty(), "{file}");
    }
    let initializer = get(
        &platform,
        &format!("{mount}/swagger-ui/swagger-initializer.js"),
    )
    .await;
    assert!(String::from_utf8_lossy(&initializer.body).contains(r#""url": "../openapi.json""#));

    let redirect = get(&platform, &format!("{mount}/swagger-ui")).await;
    assert_eq!(redirect.status, 303);
    assert_eq!(redirect.header("location"), Some("swagger-ui/"));
    assert!(redirect.body.is_empty());

    for path in ["openapi.json", "swagger-ui/swagger-ui-bundle.js"] {
        let full = get(&platform, &format!("{mount}/{path}")).await;
        let head = send(&platform, Method::HEAD, &format!("{mount}/{path}"), &[]).await;
        assert_eq!(head.status, 200, "{path}");
        assert_eq!(
            head.header("content-length"),
            Some(full.body.len().to_string().as_str()),
            "{path}"
        );
        assert!(head.body.is_empty(), "{path}");
    }

    let revalidated = send(
        &platform,
        Method::GET,
        &format!("{mount}/openapi.json"),
        &[("if-none-match", &format!("W/{etag}"))],
    )
    .await;
    assert_eq!(revalidated.status, 304);
    assert_eq!(revalidated.header("etag"), Some(etag.as_str()));
    assert!(revalidated.body.is_empty());

    for path in [
        mount.clone(),
        format!("{mount}/"),
        format!("{mount}/swagger-ui/oauth2-redirect.html"),
        format!("{mount}/swagger-ui/swagger-ui.js.map"),
        format!("{mount}/deeper/still"),
    ] {
        let missing = get(&platform, &path).await;
        assert_eq!(missing.status, 404, "{path}");
        assert_eq!(
            missing.json(),
            json!({"error_message": "Not found"}),
            "{path}"
        );
    }
    let refused = send(
        &platform,
        Method::POST,
        &format!("{mount}/openapi.json"),
        &[],
    )
    .await;
    assert_eq!(refused.status, 405);
    assert_eq!(refused.header("allow"), Some("GET,HEAD"));
    assert_eq!(
        refused.json(),
        json!({"error_message": "Method Not Allowed"}),
        "the error profile renders the document's errors"
    );

    let orders = get(&platform, &format!("{svc}/orders")).await;
    assert_eq!(orders.status, 200, "the service's own route still answers");

    let elsewhere = get(&platform, &format!("{svc}/docs/public-api-v2/x")).await;
    assert_eq!(elsewhere.status, 404);
    let isolated = get(
        &platform,
        &format!(
            "/svc/{}/catalog-service/docs/public-api/openapi.json",
            other.slug
        ),
    )
    .await;
    assert_eq!(
        isolated.status, 404,
        "another tenant's service has no documents"
    );

    cross_origin(&platform, &svc).await;

    let meta = format!("{svc}/_meta/docs");
    for (file, content_type) in EMBEDDED.iter().skip(1) {
        let asset = get(&platform, &format!("{meta}/{file}")).await;
        assert_eq!(asset.status, 200, "{file}");
        assert_eq!(asset.header("content-type"), Some(*content_type), "{file}");
    }
    for file in ["index.html", "swagger-initializer.js", "swagger-ui.js"] {
        assert_eq!(
            get(&platform, &format!("{meta}/{file}")).await.status,
            404,
            "{file}"
        );
    }
    assert_eq!(
        get(&platform, &meta).await.status,
        401,
        "the _meta page still needs a user of its tenant"
    );

    deployed(&platform, &tenant, &catalog(json!([]))).await;
    assert_eq!(
        get(&platform, &format!("{mount}/openapi.json"))
            .await
            .status,
        404,
        "a redeploy without the document removes it"
    );

    deployed(&platform, &tenant, &documented()).await;
    assert_eq!(
        get(&platform, &format!("{mount}/openapi.json"))
            .await
            .status,
        200
    );
    let (status, body) = platform
        .send(
            Method::PATCH,
            &format!(
                "/api/services/{}/status",
                service["service_id"].as_str().expect("service id")
            ),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({"status": "inactive"})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        get(&platform, &format!("{mount}/openapi.json"))
            .await
            .status,
        404,
        "an inactive service publishes nothing"
    );

    refusals(&platform, &tenant).await;
}

async fn cross_origin(platform: &PlatformProcess, svc: &str) {
    let origin = |origin: Option<&'static str>| -> Vec<(&'static str, &'static str)> {
        origin
            .map(|origin| ("origin", origin))
            .into_iter()
            .collect()
    };
    for (mount, mode) in [
        ("docs/public-api", "mirror"),
        ("docs/open", "any"),
        ("docs/own", "restricted"),
    ] {
        for path in ["openapi.json", "swagger-ui", "nothing-here"] {
            for sent in [Some(ALLOWED), Some(FOREIGN), None] {
                let answer = send(
                    platform,
                    Method::GET,
                    &format!("{svc}/{mount}/{path}"),
                    &origin(sent),
                )
                .await;
                let expected = match (mode, sent) {
                    ("mirror", sent) => sent,
                    ("any", _) => Some("*"),
                    (_, Some(ALLOWED)) => Some(ALLOWED),
                    _ => None,
                };
                let context = format!("{mode} {path} {sent:?} -> {}", answer.status);
                assert_eq!(
                    answer.header("access-control-allow-origin"),
                    expected,
                    "{context}"
                );
                assert_eq!(
                    answer.header("access-control-allow-credentials"),
                    (mode == "mirror").then_some("true"),
                    "{context}"
                );
            }
        }
        let preflight = send(
            platform,
            Method::OPTIONS,
            &format!("{svc}/{mount}/openapi.json"),
            &[
                ("origin", FOREIGN),
                ("access-control-request-method", "GET"),
                ("access-control-request-headers", "x-api-key"),
            ],
        )
        .await;
        assert_eq!(preflight.status, 200, "{mode}");
        let allowed = |name| preflight.header(name);
        match mode {
            "mirror" => assert_eq!(
                (
                    allowed("access-control-allow-origin"),
                    allowed("access-control-allow-methods"),
                    allowed("access-control-allow-headers"),
                    allowed("access-control-allow-credentials"),
                ),
                (Some(FOREIGN), Some("GET"), Some("x-api-key"), Some("true"))
            ),
            "any" => assert_eq!(
                (
                    allowed("access-control-allow-origin"),
                    allowed("access-control-allow-methods"),
                    allowed("access-control-allow-headers"),
                    allowed("access-control-allow-credentials"),
                ),
                (Some("*"), Some("GET,HEAD"), Some("*"), None)
            ),
            _ => assert_eq!(
                (
                    allowed("access-control-allow-origin"),
                    allowed("access-control-allow-credentials"),
                ),
                (None, None),
                "a foreign origin is not allowed by the platform's policy"
            ),
        }
    }

    for path in ["orders", "docs/public-api-v2/x"] {
        let answer = send(
            platform,
            Method::GET,
            &format!("{svc}/{path}"),
            &[("origin", FOREIGN)],
        )
        .await;
        assert_eq!(answer.header("access-control-allow-origin"), None, "{path}");
        let preflight = send(
            platform,
            Method::OPTIONS,
            &format!("{svc}/{path}"),
            &[
                ("origin", FOREIGN),
                ("access-control-request-method", "GET"),
            ],
        )
        .await;
        assert_eq!(
            preflight.header("access-control-allow-origin"),
            None,
            "{path}: the platform's own policy answers"
        );
        assert_eq!(
            preflight.header("access-control-allow-credentials"),
            None,
            "{path}"
        );
    }
}

async fn refusals(platform: &PlatformProcess, tenant: &Tenant) {
    let mut legacy = documented();
    legacy["name"] = json!("legacy-catalog");
    legacy["routing"] = json!("legacy");
    let mut colliding = documented();
    colliding["name"] = json!("colliding-catalog");
    colliding["custom_routes"]
        .as_array_mut()
        .expect("routes")
        .push(json!({
            "method": "GET",
            "path": "/docs/open/openapi.json",
            "handler": "shadow",
            "posture": {"kind": "public", "reason": "integration test"},
        }));
    for (manifest, refusal) in [
        (legacy, "public document 'public-api' needs routing: strict"),
        (
            colliding,
            "public document 'open-api': route 'GET /docs/open/openapi.json' (shadow) reaches under its mount '/docs/open'",
        ),
    ] {
        let (status, body) = platform.deploy(tenant, &manifest).await;
        assert_eq!(status, 422, "{body}");
        assert!(
            body.to_string().contains(refusal),
            "{body} does not name {refusal}"
        );
    }
}

#[tokio::test]
async fn a_stored_document_that_cannot_be_served_leaves_its_service_routed() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let writer = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    writer.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let manifest = catalog(json!([
        doc("public-api", "/docs/public-api", "mirror"),
        doc("status-api", "/docs/status", "any"),
    ]));
    let service = deployed(&writer, &tenant, &manifest).await;
    let service_id: uuid::Uuid = service["service_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("service id");
    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE deployments d \
             SET manifest = jsonb_set(jsonb_set(d.manifest, \
                 '{openapi,docs,0,cors}', '\"credentialed\"'), \
                 '{openapi,docs,1,mount}', '\"/docs/{version}\"') \
             FROM services s \
             WHERE d.service_id = s.id AND s.id = $1 AND d.version = s.current_version",
            &[&service_id],
        )
        .await
        .expect("write the stored manifest past validation");

    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("CORS_ALLOWED_ORIGINS", ALLOWED),
            ("REGISTRY_RELOAD_SECS", "1"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let svc = format!("/svc/{}/catalog-service", tenant.slug);

    let unrouted = get(&platform, &format!("{svc}/nothing/here")).await;
    assert_eq!(
        (unrouted.status, unrouted.json()),
        (404, json!({"error_message": "Not found"})),
        "the service stays routed, its strict routes and error profile with it"
    );
    let spec = send(
        &platform,
        Method::GET,
        &format!("{svc}/docs/public-api/openapi.json"),
        &[("origin", FOREIGN)],
    )
    .await;
    assert_eq!(
        spec.status, 200,
        "an unknown cors mode still serves its document"
    );
    assert_eq!(
        spec.header("access-control-allow-origin"),
        None,
        "with the platform's own cors policy"
    );
    assert_eq!(spec.header("access-control-allow-credentials"), None);
    for mount in ["docs/status", "docs/1"] {
        assert_eq!(
            get(&platform, &format!("{svc}/{mount}/openapi.json"))
                .await
                .status,
            404,
            "a document that cannot be built answers 404"
        );
    }
    assert_eq!(
        platform
            .metric("cufflink_public_docs_build_errors_total", &[])
            .await,
        Some(1.0)
    );

    let reloaded = platform
        .get_json("/ready")
        .await
        .and_then(|(_, body)| body["registry_version"].as_u64())
        .expect("registry version");
    common::until(
        "two more registry reloads",
        Duration::from_secs(15),
        async || {
            platform
                .get_json("/ready")
                .await
                .and_then(|(_, body)| body["registry_version"].as_u64())
                .is_some_and(|version| version >= reloaded + 2)
        },
    )
    .await
    .expect("reloaded");
    assert_eq!(
        platform
            .metric("cufflink_public_docs_build_errors_total", &[])
            .await,
        Some(1.0),
        "a reload of the same version builds nothing again"
    );
    assert_eq!(
        platform
            .output_tail()
            .matches("cors mode is not one this platform knows")
            .count(),
        1,
        "the unknown mode is reported once"
    );
}
