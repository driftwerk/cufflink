use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_platform::repo::partner_keys;
use reqwest::Method;
use serde_json::{json, Value};

#[tokio::test]
async fn partner_keys_api() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let other = db.mint_tenant().await;
    let key = format!("ApiKey {}", tenant.api_key);
    let keys = format!("/api/tenants/{}/partner-keys", tenant.slug);
    let create = |body: Value| {
        let (platform, key, keys) = (&platform, key.clone(), keys.clone());
        async move { platform.send(Method::POST, &keys, &key, Some(&body)).await }
    };

    let (status, version) = platform.send(Method::GET, "/api/version", "", None).await;
    assert_eq!(status, 200);
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|s| s.contains(&json!("partner_keys"))),
        "{version}"
    );

    for (body, reason) in [
        (json!({"name": "acme", "scopes": []}), "at least one scope"),
        (
            json!({"name": "acme", "scopes": ["Books"]}),
            "Invalid scope",
        ),
        (
            json!({"name": "acme", "scopes": ["books"], "expires_in_days": 400}),
            "between 1 and 365",
        ),
    ] {
        let (status, error) = create(body).await;
        assert_eq!(status, 422, "{error}");
        assert!(
            error["error"].as_str().is_some_and(|e| e.contains(reason)),
            "{error}"
        );
    }

    let (status, created) =
        create(json!({"name": "acme", "scopes": ["books:ingest", "books:*"]})).await;
    assert_eq!(status, 201, "{created}");
    let raw = created["key"].as_str().expect("key shown once");
    let prefix = created["prefix"].as_str().expect("prefix");
    assert!(raw.starts_with(&format!("cfp_{prefix}_")), "{raw}");
    let expires: chrono::DateTime<chrono::Utc> = created["expires_at"]
        .as_str()
        .and_then(|t| t.parse().ok())
        .expect("expiry");
    assert_eq!((expires - chrono::Utc::now()).num_days(), 89);

    let (stored, secret_hash) = partner_keys::find_by_prefix(&db.pool(), prefix)
        .await
        .expect("lookup")
        .expect("stored");
    let secret = raw.rsplit('_').next().expect("secret");
    assert_eq!(secret_hash, partner_keys::secret_hash(secret));
    assert_ne!(secret_hash, secret, "only the hash is kept");
    assert_eq!(stored.tenant_id, tenant.id);
    assert_eq!(stored.created_by, db_subject(&platform, &key).await);

    let (status, listed) = platform.send(Method::GET, &keys, &key, None).await;
    assert_eq!(status, 200, "{listed}");
    let listed_key = &listed["keys"][0];
    assert_eq!(listed_key["prefix"], prefix);
    assert_eq!(listed_key["scopes"], json!(["books:ingest", "books:*"]));
    let text = listed.to_string();
    assert!(
        !text.contains(secret) && !text.contains(&secret_hash),
        "{text}"
    );

    let (status, _) = platform
        .send(
            Method::GET,
            &format!("/api/tenants/{}/partner-keys", other.slug),
            &key,
            None,
        )
        .await;
    assert_eq!(status, 403, "another tenant's keys are not the caller's");
    assert!(platform
        .metric("cufflink_partner_keys_active", &[])
        .await
        .is_some_and(|n| n >= 1.0));

    let id = created["id"].as_str().expect("id");
    let (status, body) = platform
        .send(Method::DELETE, &format!("{keys}/{id}"), &key, None)
        .await;
    assert_eq!((status, body), (200, json!({"revoked": true})));
    let (status, _) = platform
        .send(Method::DELETE, &format!("{keys}/{id}"), &key, None)
        .await;
    assert_eq!(status, 404, "already revoked");
    let (revoked, _) = partner_keys::find_by_prefix(&db.pool(), prefix)
        .await
        .expect("lookup")
        .expect("kept for audit");
    assert!(revoked.revoked_at.is_some());
    let foreign = format!("/api/tenants/{}/partner-keys/{id}", other.slug);
    let (status, _) = platform
        .send(
            Method::DELETE,
            &foreign,
            &format!("ApiKey {}", other.api_key),
            None,
        )
        .await;
    assert_eq!(status, 404, "a key of another tenant is not found");
}

async fn db_subject(platform: &PlatformProcess, key: &str) -> String {
    let (_, me) = platform.send(Method::GET, "/api/auth/me", key, None).await;
    me["subject"].as_str().expect("subject").to_string()
}

#[tokio::test]
async fn managing_partner_keys_needs_the_grant() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let keycloak = common::keycloak::MockKeycloak::start().await;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("KEYCLOAK_URL", keycloak.url.as_str()),
            ("KEYCLOAK_ISSUER", keycloak.issuer_base.as_str()),
        ],
    )
    .await;
    platform.wait_ready().await;
    let realm = format!("realm-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let tenant = db
        .mint_tenant_with(cufflink_platform::repo::tenants::NewTenant {
            keycloak_url: Some(&keycloak.url),
            keycloak_realm: Some(&realm),
            ..Default::default()
        })
        .await;
    let deployer = format!("ApiKey {}", tenant.api_key);
    let keys = format!("/api/tenants/{}/partner-keys", tenant.slug);
    let body = json!({"name": "acme", "scopes": ["books:ingest"]});
    let (status, role) = platform
        .send(
            Method::POST,
            &format!("/api/tenants/{}/roles", tenant.id),
            &deployer,
            Some(&json!({"name": "integrations", "permissions": ["partner_keys:manage"]})),
        )
        .await;
    assert_eq!(status, 200, "{role}");
    let (status, assigned) = platform
        .send(
            Method::POST,
            &format!("/api/tenants/{}/roles/assign", tenant.id),
            &deployer,
            Some(&json!({"user_id": "ivy", "role_id": role["id"]})),
        )
        .await;
    assert_eq!(status, 200, "{assigned}");

    let bearer = |sub: &str| format!("Bearer {}", keycloak.token(&realm, json!({"sub": sub})));
    let (status, body_without) = platform
        .send(Method::POST, &keys, &bearer("sam"), Some(&body))
        .await;
    assert_eq!(status, 403, "{body_without}");
    let (status, created) = platform
        .send(Method::POST, &keys, &bearer("ivy"), Some(&body))
        .await;
    assert_eq!(status, 201, "{created}");
    let (status, _) = platform
        .send(Method::GET, &keys, &bearer("sam"), None)
        .await;
    assert_eq!(status, 403);
}
