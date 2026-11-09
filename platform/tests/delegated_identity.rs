use cufflink_testkit as common;

use async_nats::jetstream::{self, consumer::DeliverPolicy};
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::auth::service_identity::{
    self, Claims, DelegatedMethod, DelegatedToken, Delegation, ServiceIdentityKeys,
};
use cufflink_test_guests::Profile;
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

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
        route("/owned", json!({"kind": "owner", "field": "owner_id"})),
        route(
            "/mine",
            json!({"kind": "scoped", "area": "loans", "operation": "view"})
        ),
    ]);
    manifest["authorization"] = json!({
        "areas": [{"name": "loans", "operations": ["view", "approve", "create"]}]
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
        method: DelegatedMethod::Jwt {},
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

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn token(tenant_id: uuid::Uuid, identity: Identity<'_>) -> String {
    let now = now();
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

fn as_user_at(tenant: &Tenant, method: &str, path: &str, subject: &str, expires_at: i64) -> String {
    token(
        tenant.id,
        Identity {
            method,
            path,
            depth: 1,
            acting_for: Some(delegation(subject, expires_at)),
        },
    )
}

fn as_user(tenant: &Tenant, path: &str, subject: &str) -> String {
    as_user_at(tenant, "POST", path, subject, now() + 300)
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

struct Loans {
    platform: PlatformProcess,
    tenant: Tenant,
    owner: String,
    _db: TestDb,
}

impl Loans {
    async fn start(settings: &[(&str, &str)]) -> Option<Self> {
        let wasm = common::sdk::sdk_guest("internal", Profile::Debug)?;
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(
            &db.url,
            infra.nats_url(),
            &[&[("CUFFLINK_SERVICE_IDENTITY_KEYS", KEYS)], settings].concat(),
        )
        .await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let (status, body) = platform.deploy_module(&tenant, &manifest(), &wasm).await;
        assert_eq!(status, 200, "deploy failed: {body}");
        let owner = db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT keycloak_user_id FROM tenants WHERE id = $1",
                &[&tenant.id],
            )
            .await
            .expect("the tenant's owner")
            .get(0);
        let loans = Self {
            platform,
            tenant,
            owner,
            _db: db,
        };
        loans
            .grant(
                "clara",
                &["loans:approve", "loans:view@all", "loans:create"],
            )
            .await;
        Some(loans)
    }

    async fn grant(&self, subject: &str, permissions: &[&str]) {
        let key = format!("ApiKey {}", self.tenant.api_key);
        let (status, role) = self
            .platform
            .send(
                Method::POST,
                &format!("/api/tenants/{}/roles", self.tenant.id),
                &key,
                Some(&json!({"name": format!("role-{subject}"), "permissions": permissions})),
            )
            .await;
        assert_eq!(status, 200, "{role}");
        let (status, body) = self
            .platform
            .send(
                Method::POST,
                &format!("/api/tenants/{}/roles/assign", self.tenant.id),
                &key,
                Some(&json!({"user_id": subject, "role_id": role["id"]})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    async fn partner_key(&self, scope: &str) -> String {
        let (status, body) = self
            .platform
            .send(
                Method::POST,
                &format!("/api/tenants/{}/partner-keys", self.tenant.slug),
                &format!("ApiKey {}", self.tenant.api_key),
                Some(&json!({"name": "globex-feed", "scopes": [scope]})),
            )
            .await;
        assert_eq!(status, 201, "{body}");
        body["key"].as_str().expect("key").to_string()
    }

    async fn call(&self, method: Method, path: &str, headers: &[(&str, &str)]) -> (u16, Value) {
        let request = headers.iter().fold(
            self.platform.http.request(
                method.clone(),
                format!(
                    "{}/svc/{}/{TARGET}{path}",
                    self.platform.base_url, self.tenant.slug
                ),
            ),
            |request, (name, value)| request.header(*name, *value),
        );
        let response = match method {
            Method::GET => request,
            _ => request.json(&json!({})),
        }
        .send()
        .await
        .expect("request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn post(&self, path: &str, identity: &str) -> (u16, Value) {
        self.call(Method::POST, path, &[(service_identity::HEADER, identity)])
            .await
    }

    async fn rejected(&self, reason: &str) -> bool {
        self.platform
            .metric(
                "cufflink_service_identity_rejected_total",
                &[("reason", reason)],
            )
            .await
            .is_some_and(|count| count >= 1.0)
    }
}

#[tokio::test]
async fn a_delegated_identity_admits_the_user_with_their_own_rights_once() {
    let Some(loans) = Loans::start(&[]).await else {
        return;
    };
    let tenant = &loans.tenant;

    let (status, body) = loans
        .post("/signed-in", &as_user(tenant, "/signed-in", "clara"))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (&body["sub"], &body["service"]),
        (&json!("clara"), &json!("catalog-service")),
        "{body}"
    );
    let (status, body) = loans
        .post("/signed-in", &service_only(tenant, "/signed-in"))
        .await;
    assert_eq!(status, 401, "a service identity alone has no user: {body}");

    let (status, body) = loans
        .post("/approve", &as_user(tenant, "/approve", "clara"))
        .await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body["permissions"]
            .as_array()
            .is_some_and(|granted| granted.contains(&json!("loans:approve"))),
        "{body}"
    );
    let (status, body) = loans
        .post("/approve", &as_user(tenant, "/approve", "dana"))
        .await;
    assert_eq!(status, 403, "the user's own permissions apply: {body}");
    let (status, body) = loans
        .post("/approve", &as_user(tenant, "/signed-in", "clara"))
        .await;
    assert_eq!(
        status, 401,
        "a delegation bound to another path is not a user here: {body}"
    );

    let (status, body) = loans
        .post("/mine", &as_user(tenant, "/mine", "clara"))
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = loans.post("/mine", &as_user(tenant, "/mine", "dana")).await;
    assert_eq!(status, 403, "{body}");

    let (status, body) = loans
        .post("/owned", &as_user(tenant, "/owned", "clara"))
        .await;
    assert_eq!((status, &body["sub"]), (200, &json!("clara")), "{body}");

    let plain = service_only(tenant, "/open");
    for _ in 0..2 {
        let (status, body) = loans.post("/open", &plain).await;
        assert_eq!(
            (status, &body["sub"]),
            (200, &Value::Null),
            "a service-only identity is bound to its call, not single-use: {body}"
        );
    }

    let (status, body) = loans
        .post("/desk", &as_user(tenant, "/desk", "clara"))
        .await;
    assert_eq!(
        (status, &body["sub"], &body["service"]),
        (200, &json!("clara"), &json!("catalog-service")),
        "{body}"
    );
    let (status, body) = loans.post("/desk", &service_only(tenant, "/desk")).await;
    assert_eq!(
        (status, &body["sub"]),
        (200, &Value::Null),
        "an internal route without a delegation has a service principal: {body}"
    );

    let partner = loans.partner_key("loans:ingest").await;
    let (status, body) = loans
        .call(
            Method::POST,
            "/feed",
            &[
                (service_identity::HEADER, &as_user(tenant, "/feed", "clara")),
                ("x-api-key", &partner),
            ],
        )
        .await;
    assert_eq!(
        (status, &body["sub"]),
        (200, &Value::Null),
        "an api_key route admits the partner key and sets the user aside: {body}"
    );
    let (status, body) = loans
        .post("/feed", &as_user(tenant, "/feed", "clara"))
        .await;
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
                acting_for: Some(delegation("clara", now() + 300)),
            },
        )
    };
    let (status, body) = loans.post("/desk", &at_depth_zero("/desk")).await;
    assert_eq!(status, 401, "{body}");
    let (status, body) = loans.post("/open", &at_depth_zero("/open")).await;
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
                acting_for: subject.map(|subject| delegation(subject, now() + 300)),
            },
        )
    };
    for (subject, expected) in [(Some("clara"), 200), (Some("dana"), 403), (None, 401)] {
        let (status, body) = loans
            .call(
                Method::GET,
                "/ledgers",
                &[(service_identity::HEADER, &ledgers(subject))],
            )
            .await;
        assert_eq!(status, expected, "{subject:?} lists ledgers: {body}");
    }
}

#[tokio::test]
async fn a_refused_delegation_answers_401_on_every_posture() {
    let Some(loans) = Loans::start(&[]).await else {
        return;
    };
    let tenant = &loans.tenant;
    let partner = loans.partner_key("loans:ingest").await;
    let routes = [
        (Method::POST, "/signed-in", None),
        (Method::POST, "/approve", None),
        (Method::POST, "/open", None),
        (Method::POST, "/desk", None),
        (Method::POST, "/mine", None),
        (Method::POST, "/owned", None),
        (Method::POST, "/feed", Some(("x-api-key", partner.as_str()))),
        (Method::GET, "/ledgers", None),
    ];
    for (method, path, credential) in routes {
        let send = |identity: String| {
            let method = method.clone();
            let loans = &loans;
            async move {
                let headers: Vec<(&str, &str)> = [(service_identity::HEADER, identity.as_str())]
                    .into_iter()
                    .chain(credential)
                    .collect();
                loans.call(method, path, &headers).await
            }
        };
        let identity = |expires_at| as_user_at(tenant, method.as_str(), path, "clara", expires_at);

        let (status, body) = send(identity(now() - 60)).await;
        assert_eq!(status, 401, "{method} {path} with an expired user: {body}");

        let once = identity(now() + 300);
        let (status, body) = send(once.clone()).await;
        assert_eq!(status, 200, "{method} {path} on first use: {body}");
        let (status, body) = send(once).await;
        assert_eq!(status, 401, "{method} {path} replayed: {body}");
    }
    for reason in ["replayed", "delegation_expired"] {
        assert!(loans.rejected(reason).await, "{reason}");
    }
}

#[tokio::test]
async fn a_credential_beside_a_delegation_never_widens_the_delegated_user() {
    let Some(loans) = Loans::start(&[]).await else {
        return;
    };
    let tenant = &loans.tenant;
    loans
        .grant(&loans.owner, &["loans:approve", "loans:view@all"])
        .await;
    let key = format!("ApiKey {}", tenant.api_key);

    let (status, body) = loans
        .call(Method::POST, "/approve", &[("authorization", &key)])
        .await;
    assert_eq!(status, 200, "the key alone is admitted: {body}");

    let (status, body) = loans
        .call(
            Method::POST,
            "/approve",
            &[
                (
                    service_identity::HEADER,
                    &as_user(tenant, "/approve", "dana"),
                ),
                ("authorization", &key),
            ],
        )
        .await;
    assert_eq!(status, 403, "the delegated user, not the key: {body}");

    let (status, body) = loans
        .call(
            Method::GET,
            "/ledgers",
            &[
                (
                    service_identity::HEADER,
                    &as_user_at(tenant, "GET", "/ledgers", "dana", now() + 300),
                ),
                ("authorization", &key),
            ],
        )
        .await;
    assert_eq!(status, 403, "the delegated user, not the key: {body}");
}

#[tokio::test]
async fn a_full_seen_set_refuses_a_new_delegation_rather_than_forget_one() {
    let Some(loans) = Loans::start(&[("SERVICE_IDENTITY_REPLAY_MAX", "1")]).await else {
        return;
    };
    let tenant = &loans.tenant;
    let (status, body) = loans
        .post("/signed-in", &as_user(tenant, "/signed-in", "clara"))
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = loans
        .post("/signed-in", &as_user(tenant, "/signed-in", "clara"))
        .await;
    assert_eq!(status, 401, "a second live delegation: {body}");
    assert!(loans.rejected("replay_capacity").await);
    let (status, body) = loans.post("/open", &service_only(tenant, "/open")).await;
    assert_eq!(
        status, 200,
        "a service-only identity is not remembered: {body}"
    );
}

#[tokio::test]
async fn an_unreachable_redis_refuses_a_delegation_that_arrived_over_the_network() {
    let Some(loans) = Loans::start(&[("REDIS_URL", "redis://127.0.0.1:1")]).await else {
        return;
    };
    let tenant = &loans.tenant;
    let (status, body) = loans
        .post("/signed-in", &as_user(tenant, "/signed-in", "clara"))
        .await;
    assert_eq!(status, 401, "fail-closed: {body}");
    assert!(loans.rejected("replay_unavailable").await);
    let (status, body) = loans.post("/open", &service_only(tenant, "/open")).await;
    assert_eq!(
        status, 200,
        "a service-only identity never asks Redis: {body}"
    );
}

#[tokio::test]
async fn a_delegated_write_is_recorded_as_the_user_via_the_calling_service() {
    let Some(nats_url) = common::infra()
        .await
        .and_then(|infra| infra.require_nats().map(str::to_string))
    else {
        return;
    };
    let Some(loans) = Loans::start(&[("CUFFLINK_AUDIT_DURABLE_ALL", "true")]).await else {
        return;
    };
    let tenant = &loans.tenant;
    let (status, created) = loans
        .call(
            Method::POST,
            "/ledgers",
            &[(
                service_identity::HEADER,
                &as_user(tenant, "/ledgers", "clara"),
            )],
        )
        .await;
    assert_eq!(status, 200, "{created}");

    let records = jetstream::new(async_nats::connect(&nats_url).await.expect("nats"))
        .get_stream("CUFFLINK_AUDIT")
        .await
        .expect("the stream of durable records")
        .create_consumer(jetstream::consumer::pull::Config {
            filter_subject: format!("cufflink.audit.{}.{TARGET}", tenant.slug),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .expect("consumer");
    let record = tokio::time::timeout(
        Duration::from_secs(15),
        records.messages().await.expect("messages").next(),
    )
    .await
    .expect("a durable record")
    .expect("the stream of records")
    .expect("a record");
    let record: Value = serde_json::from_slice(&record.payload).expect("json");
    assert_eq!(
        (
            &record["action"],
            &record["user_id"],
            &record["user_name"],
            &record["actor"]
        ),
        (
            &json!("create"),
            &json!("clara"),
            &json!("clara at acme"),
            &json!({"sub": "clara", "kind": "user", "key_id": null, "azp": null, "via": "catalog-service"})
        ),
        "{record}"
    );
}
