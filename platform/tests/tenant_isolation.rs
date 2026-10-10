use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use reqwest::Method;
use serde_json::{json, Value};

struct Route {
    method: Method,
    path: &'static str,
    body: Body,
}

enum Body {
    None,
    Json(Value),
    Multipart,
}

fn route(method: Method, path: &'static str, body: Body) -> Route {
    Route { method, path, body }
}

fn service_routes() -> Vec<Route> {
    vec![
        route(Method::GET, "", Body::None),
        route(
            Method::PATCH,
            "/status",
            Body::Json(json!({"status": "active"})),
        ),
        route(Method::GET, "/permission-areas", Body::None),
        route(Method::GET, "/openapi.json", Body::None),
        route(Method::GET, "/deployments", Body::None),
        route(Method::GET, "/consumers", Body::None),
        route(
            Method::POST,
            "/deployments/prune",
            Body::Json(json!({"keep": 10})),
        ),
        route(Method::DELETE, "/deployments/1", Body::None),
        route(
            Method::POST,
            "/rollback",
            Body::Json(json!({"target_version": 1})),
        ),
        route(Method::POST, "/seed", Body::Json(json!({}))),
        route(Method::POST, "/backup/export", Body::Json(json!({}))),
        route(
            Method::POST,
            "/backup/restore",
            Body::Json(json!({"s3_key": "backups/x/export.jsonl"})),
        ),
        route(Method::GET, "/backup/jobs", Body::None),
        route(
            Method::GET,
            "/backup/jobs/00000000-0000-0000-0000-000000000001",
            Body::None,
        ),
        route(
            Method::POST,
            "/backup/jobs/00000000-0000-0000-0000-000000000001/cancel",
            Body::None,
        ),
        route(
            Method::GET,
            "/backup/jobs/00000000-0000-0000-0000-000000000001/download",
            Body::None,
        ),
        route(Method::POST, "/backup/upload", Body::None),
        route(Method::GET, "/schema", Body::None),
        route(
            Method::POST,
            "/query",
            Body::Json(json!({"sql": "SELECT 1"})),
        ),
        route(Method::GET, "/logs", Body::None),
        route(Method::GET, "/logs/tail", Body::None),
        route(Method::GET, "/config", Body::None),
        route(
            Method::PUT,
            "/config",
            Body::Json(json!({"key": "PLAIN", "value": "v"})),
        ),
        route(Method::DELETE, "/config/PLAIN", Body::None),
        route(
            Method::POST,
            "/webhooks",
            Body::Json(json!({
                "table_name": "items",
                "event": "create",
                "url": "https://hooks.example.test/in",
                "secret": "whsec",
            })),
        ),
        route(Method::GET, "/webhooks", Body::None),
        route(
            Method::DELETE,
            "/webhooks/00000000-0000-0000-0000-000000000001",
            Body::None,
        ),
        route(
            Method::PUT,
            "/webhooks/00000000-0000-0000-0000-000000000001",
            Body::Json(json!({"active": false})),
        ),
        route(
            Method::GET,
            "/webhooks/00000000-0000-0000-0000-000000000001/deliveries",
            Body::None,
        ),
        route(Method::POST, "/files", Body::Multipart),
        route(Method::GET, "/files", Body::None),
        route(
            Method::GET,
            "/files/00000000-0000-0000-0000-000000000001",
            Body::None,
        ),
        route(
            Method::DELETE,
            "/files/00000000-0000-0000-0000-000000000001",
            Body::None,
        ),
        route(Method::POST, "/web", Body::None),
        route(Method::GET, "/web/status", Body::None),
        route(
            Method::POST,
            "/wasm",
            Body::Json(json!({"wasm_base64": "AGFzbQEAAAA="})),
        ),
        route(Method::GET, "/wasm/status", Body::None),
        route(Method::DELETE, "", Body::None),
    ]
}

struct World {
    platform: PlatformProcess,
    db: TestDb,
}

impl World {
    async fn start() -> Option<(Self, Tenant, Tenant)> {
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
        platform.wait_ready().await;
        let (own, other) = (db.mint_tenant().await, db.mint_tenant().await);
        Some((Self { platform, db }, own, other))
    }

    async fn call(&self, tenant: &Tenant, service: &str, route: &Route) -> (u16, Value) {
        let request = self
            .platform
            .http
            .request(
                route.method.clone(),
                format!(
                    "{}/api/services/{service}{}",
                    self.platform.base_url, route.path
                ),
            )
            .header("authorization", format!("ApiKey {}", tenant.api_key));
        let request = match &route.body {
            Body::None => request,
            Body::Json(body) => request.json(body),
            Body::Multipart => request
                .header("content-type", "multipart/form-data; boundary=x")
                .body("--x--\r\n"),
        };
        let response = request.send().await.expect("a request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }
}

#[tokio::test]
async fn another_tenants_service_is_not_found_on_every_route() {
    let Some((w, own, other)) = World::start().await else {
        return;
    };
    let manifest = json!({
        "name": "ledger",
        "version": "0.1.0",
        "tables": [{"name": "items", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true}
        ]}],
    });
    let (status, deployed) = w.platform.deploy(&own, &manifest).await;
    assert_eq!(status, 200, "{deployed}");
    let service = deployed["service_id"].as_str().expect("a service id");

    for route in service_routes() {
        let (status, refused) = w.call(&other, service, &route).await;
        assert_eq!(
            (status, &refused["error"]),
            (404, &json!("Service not found")),
            "{} {}: {refused}",
            route.method,
            route.path
        );
        let (_, answered) = w.call(&own, service, &route).await;
        assert_ne!(
            answered["error"], "Service not found",
            "{} {} reaches the service for its own tenant",
            route.method, route.path
        );
    }
}

async fn as_admin(w: &World, admin: &str, method: Method, path: &str, body: Value) -> (u16, Value) {
    w.platform.send(method, path, admin, Some(&body)).await
}

#[tokio::test]
async fn tenants_that_are_not_previews_never_share_a_realm_and_deploy_role() {
    let Some((mut w, _, _)) = World::start().await else {
        return;
    };
    let admin = format!("ApiKey {}", w.db.mint_admin_key().await);
    let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
    let realm = format!("shared-{suffix}");
    let [first, second, preview] =
        ["first", "second", "preview"].map(|name| format!("{name}-{suffix}"));
    for slug in [&first, &second, &preview] {
        w.db.adopt_tenant_db(slug);
    }
    let create = |slug: &str, extra: Value| {
        let mut body = json!({"name": slug, "slug": slug, "keycloak_realm": realm});
        body.as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        body
    };

    let (status, body) = as_admin(
        &w,
        &admin,
        Method::POST,
        "/api/tenants",
        create(&first, json!({})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, refused) = as_admin(
        &w,
        &admin,
        Method::POST,
        "/api/tenants",
        create(&second, json!({})),
    )
    .await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .is_some_and(|error| error.starts_with(&format!(
                "tenant '{first}' already uses realm '{realm}' with deploy role 'cufflink-deploy'"
            ))),
        "{refused}"
    );
    let (status, body) = as_admin(
        &w,
        &admin,
        Method::POST,
        "/api/tenants",
        create(&preview, json!({"clone_rbac_from": first})),
    )
    .await;
    assert_eq!(status, 200, "a preview shares its source's realm: {body}");
    let (status, body) = as_admin(
        &w,
        &admin,
        Method::POST,
        "/api/tenants",
        create(&second, json!({"deploy_role": "second-deploy"})),
    )
    .await;
    assert_eq!(status, 200, "a deploy role of its own: {body}");

    let (status, refused) = as_admin(
        &w,
        &admin,
        Method::PUT,
        &format!("/api/tenants/{second}"),
        json!({"deploy_role": "cufflink-deploy"}),
    )
    .await;
    assert_eq!(status, 409, "{refused}");
    let (status, refused) = as_admin(
        &w,
        &admin,
        Method::PUT,
        &format!("/api/tenants/{first}"),
        json!({"deploy_role": "second-deploy"}),
    )
    .await;
    assert_eq!(status, 409, "{refused}");
    let (status, listed) = w
        .platform
        .send(Method::GET, "/api/tenants", &admin, None)
        .await;
    assert_eq!(status, 200, "{listed}");
    let roles: Vec<(&str, &str)> = listed
        .as_array()
        .expect("tenants")
        .iter()
        .filter(|tenant| {
            [first.as_str(), second.as_str()].contains(&tenant["slug"].as_str().unwrap_or_default())
        })
        .map(|tenant| {
            (
                tenant["slug"].as_str().unwrap_or_default(),
                tenant["deploy_role"].as_str().unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        roles,
        [
            (first.as_str(), "cufflink-deploy"),
            (second.as_str(), "second-deploy")
        ],
        "a refused update changes nothing"
    );

    let (earlier, later) = (w.db.mint_tenant().await, w.db.mint_tenant().await);
    let pool = w.db.pool();
    pool.get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE tenants SET keycloak_realm = $1 WHERE id = ANY($2)",
            &[&format!("legacy-{suffix}"), &vec![earlier.id, later.id]],
        )
        .await
        .expect("two tenants that already share a realm");
    let (status, refused) = as_admin(
        &w,
        &admin,
        Method::PUT,
        &format!("/api/tenants/{}", later.slug),
        json!({"name": "renamed"}),
    )
    .await;
    assert_eq!(
        status, 409,
        "sharing tenants are refused once updated: {refused}"
    );
    let (status, body) = as_admin(
        &w,
        &admin,
        Method::PUT,
        &format!("/api/tenants/{}", later.slug),
        json!({"name": "renamed", "deploy_role": "later-deploy"}),
    )
    .await;
    assert_eq!(
        status, 200,
        "until they take a deploy role of their own: {body}"
    );
}
