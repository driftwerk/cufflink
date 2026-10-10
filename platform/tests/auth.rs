use cufflink_testkit as common;

use common::keycloak::MockKeycloak;
use common::{PlatformProcess, TestDb};
use cufflink_platform::repo::api_keys;
use cufflink_platform::repo::tenants::NewTenant;
use reqwest::Method;
use serde_json::{json, Value};
use uuid::Uuid;

const PLATFORM_REALM: &str = "cufflink-platform";

async fn start(db: &TestDb, keycloak: &MockKeycloak, extra: &[(&str, &str)]) -> PlatformProcess {
    let infra = common::infra().await.expect("infra");
    let env: Vec<(&str, &str)> = [
        ("KEYCLOAK_URL", keycloak.url.as_str()),
        ("KEYCLOAK_ISSUER", keycloak.issuer_base.as_str()),
        ("KEYCLOAK_REALM", PLATFORM_REALM),
        ("CUFFLINK_SERVICE_ACCOUNT_CLIENTS", "orchard-ops"),
    ]
    .into_iter()
    .chain(extra.iter().copied())
    .collect();
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
    platform.wait_ready().await;
    platform
}

fn realm() -> String {
    format!("realm-{}", &Uuid::new_v4().simple().to_string()[..8])
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

#[tokio::test]
async fn tenant_tokens_are_checked_against_the_configured_issuer() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let keycloak = MockKeycloak::start().await;
    let mut db = infra.create_database().await;
    let platform = start(&db, &keycloak, &[]).await;
    let realm = realm();
    let tenant = db
        .mint_tenant_with(NewTenant {
            keycloak_url: Some(&keycloak.url),
            keycloak_realm: Some(&realm),
            ..Default::default()
        })
        .await;

    let internal_issuer = keycloak.token(
        &realm,
        json!({"iss": format!("{}/realms/{realm}", keycloak.url)}),
    );
    let (status, _) = platform
        .send(Method::GET, "/api/auth/me", &bearer(&internal_issuer), None)
        .await;
    assert_eq!(
        status, 401,
        "an issuer other than the configured one is refused"
    );

    let (status, body) = platform
        .send(
            Method::GET,
            "/api/auth/me",
            &bearer(&keycloak.token(&realm, json!({}))),
            None,
        )
        .await;
    assert_eq!(
        status, 200,
        "a refused token must not pin the issuer: {body}"
    );
    assert_eq!(body["tenant_slug"], tenant.slug);

    let (status, _) = platform
        .send(
            Method::GET,
            "/api/auth/me",
            &bearer(&keycloak.token("no-such-realm", json!({}))),
            None,
        )
        .await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn a_discovery_blip_does_not_pin_an_issuer() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let keycloak = MockKeycloak::start().await;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("KEYCLOAK_URL", keycloak.url.as_str()),
            ("KEYCLOAK_REALM", PLATFORM_REALM),
        ],
    )
    .await;
    platform.wait_ready().await;
    let realm = realm();
    db.mint_tenant_with(NewTenant {
        keycloak_url: Some(&keycloak.url),
        keycloak_realm: Some(&realm),
        ..Default::default()
    })
    .await;
    let me = || async {
        platform
            .send(
                Method::GET,
                "/api/auth/me",
                &bearer(&keycloak.token(&realm, json!({}))),
                None,
            )
            .await
            .0
    };

    keycloak.set_discovery(false);
    assert_eq!(me().await, 401, "no issuer while discovery is down");
    keycloak.set_discovery(true);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while me().await != 200 {
        assert!(
            std::time::Instant::now() < deadline,
            "discovery was never retried"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn platform_admin_requires_the_admin_role_and_is_audited() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let keycloak = MockKeycloak::start().await;
    let mut db = infra.create_database().await;
    let platform = start(&db, &keycloak, &[]).await;
    let tenant = db.mint_tenant().await;

    let deployer = keycloak.token(
        PLATFORM_REALM,
        json!({"sub": "ops-1", "realm_access": {"roles": ["cufflink-deploy"]}}),
    );
    let (status, body) = platform
        .send(Method::GET, "/api/tenants", &bearer(&deployer), None)
        .await;
    assert_eq!(status, 403, "{body}");
    assert!(body["error"]
        .as_str()
        .is_some_and(|e| e.contains("cufflink-platform-admin")));

    let admin = keycloak.token(
        PLATFORM_REALM,
        json!({"sub": "ops-2", "realm_access": {"roles": ["cufflink-platform-admin"]}}),
    );
    let (status, _) = platform
        .send(Method::GET, "/api/tenants", &bearer(&admin), None)
        .await;
    assert_eq!(status, 200);
    let audited: i64 = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM audit_log
             WHERE actor = 'ops-2' AND action = 'GET /api/tenants' AND resource_type = 'platform_admin'",
            &[],
        )
        .await
        .expect("audit rows")
        .get(0);
    assert_eq!(audited, 1);
    assert_eq!(
        platform
            .metric(
                "cufflink_platform_admin_calls_total",
                &[("route", "/api/tenants")]
            )
            .await,
        Some(1.0)
    );

    let (status, _) = platform
        .send(
            Method::GET,
            "/api/users/lookup?q=it",
            &format!("ApiKey {}", tenant.api_key),
            None,
        )
        .await;
    assert_eq!(
        status, 401,
        "directory lookup across tenants is platform-admin only"
    );

    let in_tenant = |token: &str| {
        platform
            .http
            .get(format!("{}/api/services", platform.base_url))
            .header("authorization", bearer(token))
            .header("x-tenant-slug", &tenant.slug)
            .send()
    };
    assert_eq!(
        in_tenant(&deployer)
            .await
            .expect("request")
            .status()
            .as_u16(),
        403,
        "cufflink-deploy alone gives no authority inside a tenant"
    );
    assert_eq!(
        in_tenant(&admin).await.expect("request").status().as_u16(),
        200
    );
}

#[tokio::test]
async fn audience_and_service_account_policies() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let keycloak = MockKeycloak::start().await;
    let mut db = infra.create_database().await;
    let platform = start(&db, &keycloak, &[]).await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let realm = realm();
    let tenant = db
        .mint_tenant_with(NewTenant {
            keycloak_url: Some(&keycloak.url),
            keycloak_realm: Some(&realm),
            ..Default::default()
        })
        .await;
    let (status, body) = platform
        .deploy(
            &tenant,
            &common::wasm_manifest("echo", &[("echo", "/echo")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let policies = format!("/api/tenants/{}/policies", tenant.slug);
    let echo = format!("/svc/{}/echo/_fn/echo", tenant.slug);
    let auth_of = |body: &Value| body["auth"].clone();

    let ops = keycloak.token(&realm, json!({"azp": "orchard-ops"}));
    let (status, body) = platform
        .send(Method::POST, &echo, &bearer(&ops), Some(&json!({})))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        auth_of(&body)["is_service_account"],
        true,
        "warn keeps the env list"
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_policy_violation_total",
                &[("key", "service_accounts"), ("outcome", "warned")]
            )
            .await,
        Some(1.0)
    );

    let enforce_sa = json!({"policies": {"service_accounts": {"mode": "enforce"}}});
    let (status, _) = platform
        .send(Method::PUT, &policies, &admin, Some(&enforce_sa))
        .await;
    assert_eq!(status, 200);
    let (_, body) = platform
        .send(Method::POST, &echo, &bearer(&ops), Some(&json!({})))
        .await;
    assert_eq!(
        auth_of(&body)["is_service_account"],
        false,
        "enforce drops the env list"
    );
    let (status, _) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}", tenant.slug),
            &admin,
            Some(&json!({"service_account_clients": ["orchard-ops"]})),
        )
        .await;
    assert_eq!(status, 200);
    let (_, body) = platform
        .send(Method::POST, &echo, &bearer(&ops), Some(&json!({})))
        .await;
    assert_eq!(
        auth_of(&body)["is_service_account"],
        true,
        "listed on the tenant"
    );

    let warn_audience =
        json!({"policies": {"audience": {"mode": "warn", "config": {"allowed": ["portal"]}}}});
    platform
        .send(Method::PUT, &policies, &admin, Some(&warn_audience))
        .await;
    let outsider = keycloak.token(&realm, json!({"azp": "outsider", "aud": "account"}));
    let (status, _) = platform
        .send(Method::GET, "/api/auth/me", &bearer(&outsider), None)
        .await;
    assert_eq!(status, 200, "warn records and allows");
    let enforce_audience =
        json!({"policies": {"audience": {"mode": "enforce", "config": {"allowed": ["portal"]}}}});
    platform
        .send(Method::PUT, &policies, &admin, Some(&enforce_audience))
        .await;
    let (status, body) = platform
        .send(Method::GET, "/api/auth/me", &bearer(&outsider), None)
        .await;
    assert_eq!(status, 401, "{body}");
    let (status, _) = platform
        .send(
            Method::GET,
            "/api/auth/me",
            &bearer(&keycloak.token(&realm, json!({"azp": "portal"}))),
            None,
        )
        .await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn api_keys_expire_list_revoke_and_end_with_the_login_session() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let keycloak = MockKeycloak::start().await;
    let mut db = infra.create_database().await;
    let platform = start(&db, &keycloak, &[]).await;
    let realm = realm();
    let tenant = db
        .mint_tenant_with(NewTenant {
            keycloak_url: Some(&keycloak.url),
            keycloak_realm: Some(&realm),
            ..Default::default()
        })
        .await;
    let other = db.mint_tenant().await;
    let key = format!("ApiKey {}", tenant.api_key);

    let (status, created) = platform
        .send(
            Method::POST,
            "/api/auth/api-keys",
            &key,
            Some(&json!({"name": "ci"})),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    let expires: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(created["expires_at"].clone()).expect("expires_at");
    let days = (expires - chrono::Utc::now()).num_days();
    assert!((89..=90).contains(&days), "{days}");

    let (status, listed) = platform
        .send(Method::GET, "/api/auth/api-keys", &key, None)
        .await;
    assert_eq!(status, 200);
    assert!(listed["keys"].as_array().is_some_and(|keys| keys
        .iter()
        .any(|k| k["id"] == created["id"] && k.get("key").is_none())));

    let path = format!("/api/auth/api-keys/{}", created["id"].as_str().expect("id"));
    let (status, _) = platform
        .send(
            Method::DELETE,
            &path,
            &format!("ApiKey {}", other.api_key),
            None,
        )
        .await;
    assert_eq!(status, 404, "another tenant cannot revoke it");
    let ci = format!("ApiKey {}", created["key"].as_str().expect("key"));
    assert_eq!(
        platform
            .send(Method::GET, "/api/auth/api-keys", &ci, None)
            .await
            .0,
        200
    );
    assert_eq!(
        platform.send(Method::DELETE, &path, &key, None).await.0,
        200
    );
    assert_eq!(
        platform
            .send(Method::GET, "/api/auth/api-keys", &ci, None)
            .await
            .0,
        401
    );

    let login = keycloak.token(
        &realm,
        json!({"sub": "dev-1", "preferred_username": "dev", "realm_access": {"roles": ["cufflink-deploy"]}}),
    );
    let exchange = json!({"access_token": login, "tenant_slug": tenant.slug});
    let response = platform
        .http
        .post(format!("{}/api/auth/device/token", platform.base_url))
        .json(&exchange)
        .send()
        .await
        .expect("exchange");
    assert_eq!(response.status().as_u16(), 200);
    let exchanged: Value = response.json().await.expect("json");
    assert!(exchanged["expires_at"].is_string());
    let cli = format!("ApiKey {}", exchanged["api_key"].as_str().expect("api key"));
    assert_eq!(
        platform
            .send(Method::GET, "/api/auth/api-keys", &cli, None)
            .await
            .0,
        200
    );

    platform
        .send(Method::POST, "/api/auth/logout", &bearer(&login), None)
        .await;
    assert_eq!(
        platform
            .send(Method::GET, "/api/auth/api-keys", &cli, None)
            .await
            .0,
        401,
        "logout revokes the keys of the user's CLI logins"
    );
    assert_eq!(
        platform
            .send(Method::GET, "/api/auth/api-keys", &key, None)
            .await
            .0,
        200,
        "keys not bound to that user survive"
    );
}

#[tokio::test]
async fn role_changes_invalidate_the_permission_cache() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(redis_url) = infra.require_redis() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform =
        PlatformProcess::start(&db.url, infra.nats_url(), &[("REDIS_URL", redis_url)]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let other = db.mint_tenant().await;
    let key = format!("ApiKey {}", tenant.api_key);

    let mut redis = redis::Client::open(redis_url)
        .expect("redis url")
        .get_multiplexed_async_connection()
        .await
        .expect("redis");
    let cached = |tenant_id: Uuid, subject: &str| format!("auth:perms:{tenant_id}:{subject}");
    let keys = [
        cached(tenant.id, "alice"),
        cached(tenant.id, "bob"),
        cached(other.id, "carol"),
    ];
    for k in &keys {
        redis::cmd("SET")
            .arg(k)
            .arg("cached")
            .query_async::<()>(&mut redis)
            .await
            .expect("seed");
    }
    let exists = |k: String| {
        let mut redis = redis.clone();
        async move {
            redis::cmd("EXISTS")
                .arg(k)
                .query_async::<bool>(&mut redis)
                .await
                .expect("exists")
        }
    };

    let roles = format!("/api/tenants/{}/roles", tenant.id);
    let (status, role) = platform
        .send(
            Method::POST,
            &roles,
            &key,
            Some(&json!({"name": "staff", "permissions": ["items:view"]})),
        )
        .await;
    assert_eq!(status, 200, "{role}");
    let assign = json!({"user_id": "alice", "role_id": role["id"]});
    let (status, _) = platform
        .send(
            Method::POST,
            &format!("{roles}/assign"),
            &key,
            Some(&assign),
        )
        .await;
    assert_eq!(status, 200);
    assert!(
        !exists(keys[0].clone()).await,
        "the assignee's entry is dropped"
    );
    assert!(exists(keys[1].clone()).await);

    let (status, _) = platform
        .send(
            Method::PUT,
            &format!("{roles}/{}", role["id"].as_str().expect("role id")),
            &key,
            Some(&json!({"name": "staff", "permissions": ["items:*"]})),
        )
        .await;
    assert_eq!(status, 200);
    assert!(
        !exists(keys[1].clone()).await,
        "a role edit drops the whole tenant"
    );
    assert!(exists(keys[2].clone()).await, "other tenants are untouched");

    let foreign_role = json!({"user_id": "alice", "role_id": Uuid::new_v4()});
    let (status, _) = platform
        .send(
            Method::POST,
            &format!("{roles}/assign"),
            &key,
            Some(&foreign_role),
        )
        .await;
    assert_eq!(status, 404, "a role outside the tenant cannot be assigned");

    let reseed = || async {
        redis::cmd("SET")
            .arg(&keys[1])
            .arg("cached")
            .query_async::<()>(&mut redis.clone())
            .await
            .expect("seed");
    };
    let deploy_with = |permissions: &[&str]| {
        let mut manifest = common::wasm_manifest("roles-svc", &[]);
        manifest["authorization"] = json!({
            "areas": [],
            "default_roles": [{"name": "reporter", "permissions": permissions}],
        });
        let (tenant, platform) = (&tenant, &platform);
        async move {
            let (status, body) = platform.deploy(tenant, &manifest).await;
            assert_eq!(status, 200, "{body}");
        }
    };
    reseed().await;
    deploy_with(&["reports:view"]).await;
    assert!(
        !exists(keys[1].clone()).await,
        "a seeded role drops the tenant"
    );
    reseed().await;
    deploy_with(&["reports:view"]).await;
    assert!(
        exists(keys[1].clone()).await,
        "seeding that changes nothing keeps the cache"
    );
    deploy_with(&["reports:view", "reports:export"]).await;
    assert!(
        !exists(keys[1].clone()).await,
        "a permission a deploy adds is visible at once"
    );
}

#[tokio::test]
async fn upgrade_gives_existing_tenant_keys_an_expiry() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = db.pool().get().await.expect("checkout");
    client
        .batch_execute(include_str!("../migrations/V1__baseline.sql"))
        .await
        .expect("legacy schema");
    client
        .batch_execute(
            "INSERT INTO tenants (id, keycloak_user_id, name, slug)
               VALUES ('00000000-0000-0000-0000-00000000000a', 'platform-a', 'a', 'a');
             INSERT INTO api_keys (tenant_id, key_hash, name)
               VALUES ('00000000-0000-0000-0000-00000000000a', 'h-tenant', 'legacy');
             INSERT INTO api_keys (key_hash, name, is_platform_admin)
               VALUES ('h-admin', 'admin', true);",
        )
        .await
        .expect("legacy keys");
    cufflink_platform::repo::migrations::run(&cufflink_platform::repo::migrations::Target {
        config: cufflink_db::parse_config(&db.url).expect("url"),
        settings: cufflink_db::PoolSettings::from_vars(1, |_| None).expect("defaults"),
    })
    .await
    .expect("migrations");
    let pool = db.pool();
    let expire = || {
        api_keys::expire_unbounded(
            &pool,
            api_keys::KeyClass::Tenant,
            chrono::Duration::days(30),
        )
    };
    assert_eq!(expire().await.expect("backfill"), 1, "only the tenant key");

    let expiry = |hash: &'static str| {
        let client = &client;
        async move {
            client
                .query_one(
                    "SELECT expires_at FROM api_keys WHERE key_hash = $1",
                    &[&hash],
                )
                .await
                .expect("key")
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
        }
    };
    let tenant_expiry = expiry("h-tenant").await.expect("backfilled");
    let days = (tenant_expiry - chrono::Utc::now()).num_days();
    assert!(
        (29..=30).contains(&days),
        "CUFFLINK_API_KEY_TTL_DAYS decides: {days}"
    );
    assert_eq!(expire().await.expect("again"), 0, "idempotent");
    assert_eq!(expiry("h-tenant").await, Some(tenant_expiry));
    assert_eq!(
        expiry("h-admin").await,
        None,
        "platform-admin keys do not expire"
    );
    assert!(matches!(
        api_keys::find(&db.pool(), "h-tenant")
            .await
            .expect("lookup"),
        api_keys::KeyLookup::Active(_)
    ));
}
