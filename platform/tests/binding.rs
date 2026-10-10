use cufflink_testkit as common;

use common::keycloak::MockKeycloak;
use common::{PlatformProcess, TestDb};
use cufflink_platform::repo::tenants::NewTenant;
use reqwest::Method;
use serde_json::{json, Value};

const SERVICE: &str = "portal";

fn manifest() -> Value {
    json!({
        "name": SERVICE,
        "version": "0.1.0",
        "mode": "wasm",
        "tables": [{
            "name": "items",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "title", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false},
                {"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true}
            ],
            "auth_required": true
        }],
        "custom_routes": [
            {"method": "POST", "path": "/me", "handler": "echo", "posture": {"kind": "authenticated"}},
            {"method": "POST", "path": "/proxy", "handler": "echo_forwarded", "posture": {"kind": "public", "reason": "forwards the caller"}, "forward_auth": true},
            {"method": "POST", "path": "/work", "handler": "sleep2", "posture": {"kind": "authenticated"}, "job": {"timeout_secs": 60, "max_attempts": 1}}
        ],
        "publishes": [],
    })
}

struct World {
    platform: PlatformProcess,
    keycloak: MockKeycloak,
    admin: String,
    default: common::Tenant,
    preview: common::Tenant,
    harbor: common::Tenant,
    harbor_realm: String,
    realm: String,
    _db: TestDb,
}

impl World {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        Self::launch(infra, infra.nats_url()).await
    }

    async fn with_nats() -> Option<Self> {
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        Self::launch(infra, Some(nats_url)).await
    }

    async fn launch(infra: &common::Infra, nats_url: Option<&str>) -> Option<Self> {
        let keycloak = MockKeycloak::start().await;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(
            &db.url,
            nats_url,
            &[
                ("KEYCLOAK_URL", keycloak.url.as_str()),
                ("KEYCLOAK_ISSUER", keycloak.issuer_base.as_str()),
            ],
        )
        .await;
        platform.wait_ready().await;
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let realm = format!("orchard-{suffix}");
        let harbor_realm = format!("harbor-{suffix}");
        let on = |realm| NewTenant {
            keycloak_url: Some(&keycloak.url),
            keycloak_realm: Some(realm),
            ..Default::default()
        };
        let default = db.mint_tenant_with(on(&realm)).await;
        let preview = db.mint_tenant_with(on(&realm)).await;
        let harbor = db.mint_tenant_with(on(&harbor_realm)).await;
        for tenant in [&default, &preview] {
            let (status, body) = platform.deploy(tenant, &manifest()).await;
            assert_eq!(status, 200, "deploy failed: {body}");
        }
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        Some(Self {
            platform,
            keycloak,
            admin,
            default,
            preview,
            harbor,
            harbor_realm,
            realm,
            _db: db,
        })
    }

    async fn set_binding(&self, tenant: &common::Tenant, mode: &str) {
        let (status, body) = self
            .platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", tenant.slug),
                &self.admin,
                Some(&json!({"policies": {"tenant_binding": {"mode": mode}}})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    fn user(&self) -> String {
        format!(
            "Bearer {}",
            self.keycloak.token(&self.realm, json!({"sub": "staff-1"}))
        )
    }

    fn outsider(&self) -> String {
        format!(
            "Bearer {}",
            self.keycloak
                .token(&self.harbor_realm, json!({"sub": "harbor-1"}))
        )
    }

    async fn credentials_forwarded(&self, outcome: &str) -> Option<f64> {
        self.platform
            .metric(
                "cufflink_policy_violation_total",
                &[
                    ("tenant", &self.preview.slug),
                    ("reason", "auth_header_forwarded"),
                    ("outcome", outcome),
                ],
            )
            .await
    }

    async fn call(
        &self,
        tenant: &common::Tenant,
        handler_path: &str,
        authorization: &str,
        tenant_header: Option<&str>,
    ) -> (u16, Value) {
        let request = self
            .platform
            .http
            .post(format!(
                "{}/svc/{}/{SERVICE}/_fn/{handler_path}",
                self.platform.base_url, tenant.slug
            ))
            .header("authorization", authorization)
            .header("cookie", "session=abc")
            .json(&json!({}));
        let request = match tenant_header {
            Some(slug) => request.header("x-tenant-slug", slug),
            None => request,
        };
        let response = request.send().await.expect("call");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }
}

#[tokio::test]
async fn bearer_only_calls_reach_the_preview_and_credentials_follow_forward_auth() {
    let Some(w) = World::start().await else {
        return;
    };

    let (status, body) = w.call(&w.preview, "echo", &w.user(), None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["auth"]["sub"], "staff-1");
    assert!(
        body["headers"]["authorization"].is_string(),
        "warn still forwards credentials: {body}"
    );
    let (status, _) = w.call(&w.preview, "echo", &w.outsider(), None).await;
    assert_eq!(status, 401, "another realm's user is anonymous under warn");
    let (status, body) = w
        .call(&w.preview, "echo_forwarded", &w.outsider(), None)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["auth"], Value::Null, "no foreign claims reach a guest");
    assert_eq!(
        w.credentials_forwarded("warned").await,
        Some(1.0),
        "warn records the credentials it still forwards"
    );

    w.set_binding(&w.preview, "enforce").await;
    let (status, body) = w.call(&w.preview, "echo", &w.user(), None).await;
    assert_eq!(
        status, 200,
        "a same-realm Bearer-only call binds to the preview: {body}"
    );
    assert!(body["headers"].get("authorization").is_none(), "{body}");
    assert!(body["headers"].get("cookie").is_none(), "{body}");
    assert_eq!(
        w.credentials_forwarded("denied").await,
        None,
        "stripping credentials under enforce is no policy violation"
    );
    assert_eq!(
        w.platform
            .metric(
                "cufflink_route_credentials_stripped_total",
                &[("tenant", &w.preview.slug), ("service", SERVICE)]
            )
            .await,
        Some(1.0)
    );
    let (status, body) = w.call(&w.preview, "echo_forwarded", &w.user(), None).await;
    assert_eq!(status, 200, "{body}");
    assert!(body["headers"]["authorization"].is_string(), "{body}");
    assert_eq!(body["headers"]["cookie"], "session=abc");

    let (status, _) = w.call(&w.preview, "echo", &w.outsider(), None).await;
    assert_eq!(status, 403, "another realm's user is refused under enforce");
    let (status, body) = w
        .call(&w.preview, "echo_forwarded", &w.outsider(), None)
        .await;
    assert_eq!(
        status, 200,
        "a public route serves the refused caller anonymously"
    );
    assert_eq!(body["auth"], Value::Null);

    let (status, _) = w
        .call(&w.preview, "echo", &w.user(), Some(&w.default.slug))
        .await;
    assert_eq!(status, 403, "a header naming another tenant is a mismatch");
    let (status, _) = w
        .call(&w.preview, "echo", &w.user(), Some(&w.preview.slug))
        .await;
    assert_eq!(status, 200);
    let (status, _) = w
        .call(&w.preview, "echo", &w.user(), Some(&w.harbor.slug))
        .await;
    assert_eq!(
        status, 401,
        "a header naming a tenant of another realm resolves no tenant"
    );
}

#[tokio::test]
async fn crud_serves_a_foreign_api_key_anonymously_under_warn_and_refuses_it_under_enforce() {
    let Some(w) = World::start().await else {
        return;
    };
    let items = format!("/svc/{}/{SERVICE}/items", w.preview.slug);
    let foreign = format!("ApiKey {}", w.default.api_key);
    let own = format!("ApiKey {}", w.preview.api_key);

    let (status, body) = w.platform.send(Method::GET, &items, &foreign, None).await;
    assert_eq!(
        status, 401,
        "an auth_required table admits no foreign key under warn: {body}"
    );
    assert_eq!(
        w.platform
            .metric(
                "cufflink_policy_violation_total",
                &[("key", "tenant_binding"), ("outcome", "warned")]
            )
            .await,
        Some(1.0)
    );

    w.set_binding(&w.preview, "enforce").await;
    let (status, body) = w.platform.send(Method::GET, &items, &foreign, None).await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "tenant_binding: api_key_foreign_tenant");
    let (status, _) = w.platform.send(Method::GET, &items, &own, None).await;
    assert_eq!(status, 200);
    let (status, _) = w
        .platform
        .send(
            Method::POST,
            &items,
            &w.user(),
            Some(&json!({"title": "bound to the preview"})),
        )
        .await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn a_preview_job_is_polled_with_a_bearer_only_token() {
    let Some(w) = World::with_nats().await else {
        return;
    };
    let (status, body) = w.call(&w.preview, "sleep2", &w.user(), None).await;
    assert_eq!(status, 202, "{body}");
    let job_id = body["job_id"].as_str().expect("job id").to_string();
    let job = format!("/svc/{}/_jobs/{job_id}", w.preview.slug);

    let (status, body) = w.platform.send(Method::GET, &job, &w.user(), None).await;
    assert_eq!(status, 200, "the submitter reaches its preview job: {body}");
    assert_eq!(body["handler"], "sleep2");
    let (status, _) = w
        .platform
        .send(Method::GET, &job, &w.outsider(), None)
        .await;
    assert_eq!(status, 404, "another realm's user learns nothing");
    let other_tenant = format!(
        "/svc/{}/_jobs/{}",
        w.default.slug,
        body["job_id"].as_str().unwrap_or_default()
    );
    let (status, _) = w
        .platform
        .send(Method::GET, &other_tenant, &w.user(), None)
        .await;
    assert_eq!(status, 404, "the job belongs to the preview");
}
