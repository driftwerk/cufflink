use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use cufflink_platform::auth::service_identity::{
    self, Claims, DelegatedMethod, DelegatedToken, Delegation, ServiceIdentityKeys,
};
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeMap;

const KEYS: &str = "k1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const TARGET: &str = "loan-service";

fn route(path: &str, posture: Value) -> Value {
    json!({"method": "POST", "path": path, "handler": "principal", "posture": posture})
}

fn manifest() -> Value {
    let mut manifest = common::wasm_manifest(TARGET, &[]);
    manifest["routing"] = json!("strict");
    manifest["custom_routes"] = json!([
        route("/signed-in", json!({"kind": "authenticated"})),
        route(
            "/approve",
            json!({"kind": "permission", "area": "loans", "operation": "approve"})
        ),
        route(
            "/open",
            json!({"kind": "public", "reason": "integration test"})
        ),
        route(
            "/desk",
            json!({"kind": "internal", "callers": ["catalog-service"]})
        ),
        route("/feed", json!({"kind": "api_key", "scope": "loans:ingest"})),
        route(
            "/mine",
            json!({"kind": "scoped", "area": "loans", "operation": "view"})
        ),
    ]);
    manifest["authorization"] = json!({
        "areas": [{"name": "loans", "operations": ["view", "approve"]}]
    });
    manifest["tables"] = json!([{
        "name": "ledgers",
        "permission_area": "loans",
        "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
            {"name": "title", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false},
            {"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true}
        ]
    }]);
    manifest
}

fn delegation(subject: &str, expires_at: i64) -> Delegation {
    Delegation {
        subject: subject.into(),
        method: DelegatedMethod::Jwt,
        name: Some(format!("{subject} at acme")),
        email: Some(format!("{subject}@example.com")),
        service_account: false,
        realm: Some("acme".into()),
        expires_at: Some(expires_at),
        token: Some(DelegatedToken {
            preferred_username: Some(subject.into()),
            realm_roles: Vec::new(),
            claims: BTreeMap::from([("exp".to_string(), json!(expires_at))]),
        }),
    }
}

struct Identity<'a> {
    method: &'a str,
    path: &'a str,
    depth: u8,
    acting_for: Option<Delegation>,
}

fn token(tenant_id: uuid::Uuid, identity: Identity<'_>) -> String {
    let now = chrono::Utc::now().timestamp();
    service_identity::mint(
        &ServiceIdentityKeys::from_setting(Some(KEYS)).expect("keys"),
        &Claims {
            tenant_id,
            caller_service: "catalog-service".into(),
            caller_service_id: uuid::Uuid::new_v4(),
            caller_version: 1,
            target_service: TARGET.into(),
            method: identity.method.into(),
            path: identity.path.into(),
            issued_at: now,
            expires_at: now + 30,
            jti: uuid::Uuid::new_v4(),
            depth: identity.depth,
            on_behalf_of: identity.acting_for.as_ref().map(|act| act.subject.clone()),
            acting_for: identity.acting_for,
        },
    )
    .expect("token")
}

fn as_user(tenant: &Tenant, path: &str, subject: &str) -> String {
    token(
        tenant.id,
        Identity {
            method: "POST",
            path,
            depth: 1,
            acting_for: Some(delegation(subject, chrono::Utc::now().timestamp() + 300)),
        },
    )
}

fn service_only(tenant: &Tenant, path: &str) -> String {
    token(
        tenant.id,
        Identity {
            method: "POST",
            path,
            depth: 1,
            acting_for: None,
        },
    )
}

async fn call(
    platform: &PlatformProcess,
    tenant: &Tenant,
    method: Method,
    path: &str,
    identity: &str,
) -> (u16, Value) {
    let response = platform
        .http
        .request(
            method.clone(),
            format!("{}/svc/{}/{TARGET}{path}", platform.base_url, tenant.slug),
        )
        .header(service_identity::HEADER, identity);
    let response = match method {
        Method::GET => response,
        _ => response.json(&json!({})),
    }
    .send()
    .await
    .expect("request");
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or(Value::Null))
}

async fn grant(platform: &PlatformProcess, tenant: &Tenant, subject: &str, permissions: &[&str]) {
    let key = format!("ApiKey {}", tenant.api_key);
    let (status, role) = platform
        .send(
            Method::POST,
            &format!("/api/tenants/{}/roles", tenant.id),
            &key,
            Some(&json!({"name": format!("role-{subject}"), "permissions": permissions})),
        )
        .await;
    assert_eq!(status, 200, "{role}");
    let (status, body) = platform
        .send(
            Method::POST,
            &format!("/api/tenants/{}/roles/assign", tenant.id),
            &key,
            Some(&json!({"user_id": subject, "role_id": role["id"]})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn a_delegated_identity_admits_the_user_with_their_own_rights_once() {
    let Some(wasm) = common::sdk::sdk_guest("internal", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CUFFLINK_SERVICE_IDENTITY_KEYS", KEYS)],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform.deploy_module(&tenant, &manifest(), &wasm).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    grant(
        &platform,
        &tenant,
        "clara",
        &["loans:approve", "loans:view@all"],
    )
    .await;

    let post = |path: &'static str, identity: String| {
        let (platform, tenant) = (&platform, &tenant);
        async move { call(platform, tenant, Method::POST, path, &identity).await }
    };

    let (status, body) = post("/signed-in", as_user(&tenant, "/signed-in", "clara")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (&body["sub"], &body["service"]),
        (&json!("clara"), &json!("catalog-service")),
        "{body}"
    );
    let (status, body) = post("/signed-in", service_only(&tenant, "/signed-in")).await;
    assert_eq!(status, 401, "a service identity alone has no user: {body}");

    let (status, body) = post("/approve", as_user(&tenant, "/approve", "clara")).await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body["permissions"]
            .as_array()
            .is_some_and(|granted| granted.contains(&json!("loans:approve"))),
        "{body}"
    );
    let (status, body) = post("/approve", as_user(&tenant, "/approve", "dana")).await;
    assert_eq!(status, 403, "the user's own permissions apply: {body}");

    let (status, body) = post("/mine", as_user(&tenant, "/mine", "clara")).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = post("/mine", as_user(&tenant, "/mine", "dana")).await;
    assert_eq!(status, 403, "{body}");

    let once = as_user(&tenant, "/signed-in", "clara");
    let (status, body) = post("/signed-in", once.clone()).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = post("/signed-in", once).await;
    assert_eq!(status, 401, "a replayed delegated identity: {body}");
    let plain = service_only(&tenant, "/open");
    for _ in 0..2 {
        let (status, body) = post("/open", plain.clone()).await;
        assert_eq!(
            (status, &body["sub"]),
            (200, &Value::Null),
            "a service-only identity is bound to its call, not single-use: {body}"
        );
    }

    let expired = token(
        tenant.id,
        Identity {
            method: "POST",
            path: "/open",
            depth: 1,
            acting_for: Some(delegation("clara", chrono::Utc::now().timestamp() - 60)),
        },
    );
    let (status, body) = post("/open", expired).await;
    assert_eq!(
        status, 401,
        "an expired delegation on a public route: {body}"
    );

    let (status, body) = post("/desk", as_user(&tenant, "/desk", "clara")).await;
    assert_eq!(
        (status, &body["sub"], &body["service"]),
        (200, &json!("clara"), &json!("catalog-service")),
        "{body}"
    );
    let (status, body) = post("/desk", service_only(&tenant, "/desk")).await;
    assert_eq!(
        (status, &body["sub"]),
        (200, &Value::Null),
        "an internal route without a delegation has a service principal: {body}"
    );

    let (status, body) = post("/feed", as_user(&tenant, "/feed", "clara")).await;
    assert_eq!(
        status, 401,
        "only partner keys pass an api_key route: {body}"
    );

    let at_depth_zero = |path: &'static str| {
        token(
            tenant.id,
            Identity {
                method: "POST",
                path,
                depth: 0,
                acting_for: Some(delegation("clara", chrono::Utc::now().timestamp() + 300)),
            },
        )
    };
    let (status, body) = post("/desk", at_depth_zero("/desk")).await;
    assert_eq!(status, 401, "{body}");
    let (status, body) = post("/open", at_depth_zero("/open")).await;
    assert_eq!(
        (status, &body["sub"]),
        (200, &Value::Null),
        "a malformed identity is ignored on a lenient route, as before: {body}"
    );

    let ledgers = |subject: Option<&str>| {
        token(
            tenant.id,
            Identity {
                method: "GET",
                path: "/ledgers",
                depth: 1,
                acting_for: subject
                    .map(|subject| delegation(subject, chrono::Utc::now().timestamp() + 300)),
            },
        )
    };
    for (subject, expected) in [(Some("clara"), 200), (Some("dana"), 403), (None, 401)] {
        let (status, body) = call(
            &platform,
            &tenant,
            Method::GET,
            "/ledgers",
            &ledgers(subject),
        )
        .await;
        assert_eq!(status, expected, "{subject:?} lists ledgers: {body}");
    }

    for reason in ["replayed", "delegation_expired"] {
        assert!(
            platform
                .metric(
                    "cufflink_service_identity_rejected_total",
                    &[("reason", reason)]
                )
                .await
                .is_some_and(|count| count >= 1.0),
            "{reason}"
        );
    }
}
