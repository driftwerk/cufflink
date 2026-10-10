use cufflink_testkit::{
    guest, unless_absent, Platform, PlatformOptions, TenantOptions, TestkitError,
};
use cufflink_types::policy::{PolicyKey, PolicyMode};
use serde_json::{json, Value};

const SERVICE: &str = "kit-service";

async fn started(options: PlatformOptions) -> Option<Platform> {
    unless_absent(Platform::start(options).await).expect("a platform")
}

fn manifest() -> Value {
    let route = |path: &str, posture: Value| json!({"method": "POST", "path": path, "handler": "echo", "posture": posture});
    json!({
        "name": SERVICE,
        "version": "0.1.0",
        "mode": "wasm",
        "routing": "strict",
        "tables": [{
            "name": "items",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "title", "column_type": "text", "primary_key": false, "nullable": false, "auto_generate": false}
            ]
        }],
        "custom_routes": [
            route("/open", json!({"kind": "public", "reason": "a test"})),
            route("/me", json!({"kind": "authenticated"})),
            route("/notes", json!({"kind": "permission", "area": "notes", "operation": "create"})),
        ],
        "publishes": [],
    })
}

#[tokio::test]
async fn a_platform_starts_answers_and_stops() {
    let Some(platform) = started(PlatformOptions::default()).await else {
        return;
    };
    let anonymous = platform.anonymous();
    let version = anonymous.get("/api/version").await.expect("version");
    assert_eq!(version.status, 200);
    assert!(version.body["supports"]
        .as_array()
        .is_some_and(|supports| supports.contains(&json!("tenant_policies"))));
    let ready = anonymous.get("/ready").await.expect("ready");
    assert_eq!((ready.status, &ready.body["state"]), (200, &json!("ready")));
    let refused = anonymous.get("/api/tenants").await.expect("tenants");
    assert_eq!(refused.status, 401);
    assert!(matches!(
        refused.success("listing tenants"),
        Err(TestkitError::Status { status: 401, .. })
    ));
    platform
        .nats()
        .await
        .expect("nats")
        .flush()
        .await
        .expect("flush");
    let base_url = platform.base_url().to_string();
    platform.shutdown().await.expect("a clean shutdown");
    assert!(
        reqwest::get(format!("{base_url}/health")).await.is_err(),
        "the listener is closed"
    );
}

#[tokio::test]
async fn a_tenant_is_served_through_the_platforms_own_authentication() {
    let Some(platform) = started(PlatformOptions::default()).await else {
        return;
    };
    let tenant = platform
        .tenant(&Platform::unique_slug("kit"), TenantOptions::default())
        .await
        .expect("tenant");
    assert_eq!(tenant.realm, tenant.slug);
    let deployed = platform
        .deploy(&tenant, &manifest(), Some(&guest::module_bytes()))
        .await
        .expect("deploy");
    assert!(deployed.is_success(), "{deployed:?}");

    let operator = platform.client(&tenant).expect("client");
    let (open, me, notes) = (
        operator.svc(SERVICE, "/open"),
        operator.svc(SERVICE, "/me"),
        operator.svc(SERVICE, "/notes"),
    );
    let anonymous = operator.clone().anonymous();
    let body = json!({"id": 7});
    assert_eq!(
        anonymous.post(&open, &body).await.expect("open").status,
        200
    );
    assert_eq!(anonymous.post(&me, &body).await.expect("me").status, 401);

    let reader = platform
        .token(&tenant)
        .subject("reader-1")
        .claim("branch_id", "org-7")
        .sign()
        .await
        .expect("token");
    let as_reader = operator.clone().with_token(&reader);
    let seen = as_reader.post(&me, &body).await.expect("me");
    assert_eq!(seen.status, 200, "{seen:?}");
    assert_eq!(seen.body["tenant"], json!(tenant.slug));
    assert_eq!(seen.body["auth"]["sub"], "reader-1");
    assert_eq!(seen.body["auth"]["claims"]["branch_id"], "org-7");
    assert_eq!(seen.body["body"]["id"], 7);
    assert_eq!(
        as_reader.post(&notes, &body).await.expect("notes").status,
        403
    );
    assert_eq!(
        as_reader
            .post(
                "/api/services/deploy",
                &json!({"manifest": manifest(), "api_version": 1})
            )
            .await
            .expect("deploy")
            .status,
        403,
        "a user who is not the operator deploys nothing"
    );

    let writer = platform
        .token(&tenant)
        .subject("writer-1")
        .permissions(&["notes:create"])
        .sign()
        .await
        .expect("token");
    let written = operator
        .clone()
        .with_token(&writer)
        .post(&notes, &body)
        .await
        .expect("notes");
    assert_eq!(written.status, 200, "{written:?}");
    assert_eq!(written.body["auth"]["permissions"], json!(["notes:create"]));

    let stranger = platform
        .issuer()
        .token("another-realm")
        .subject("reader-1")
        .sign()
        .expect("token");
    assert_eq!(
        operator
            .with_token(&stranger)
            .post(&me, &body)
            .await
            .expect("me")
            .status,
        401
    );
    platform.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_tenant_is_created_with_its_policies_and_claims() {
    let Some(platform) = started(PlatformOptions::default()).await else {
        return;
    };
    let slug = Platform::unique_slug("kit");
    let subject = format!("kit{}.books.>", &slug[4..]);
    let stream = format!("KIT_{}", slug[4..].to_uppercase());
    let tenant = platform
        .tenant(
            &slug,
            TenantOptions::default()
                .policy(PolicyKey::Graphql, PolicyMode::Enforce, None)
                .nats_subject(&subject)
                .nats_stream(&stream),
        )
        .await
        .expect("tenant");
    let admin = platform.admin().expect("admin");
    let claims = admin
        .get(&format!("/api/tenants/{}/nats-claims", tenant.slug))
        .await
        .expect("claims")
        .success("reading the claims")
        .expect("claims");
    assert_eq!(claims["subjects"], json!([subject]));
    assert_eq!(claims["streams"], json!([stream]));
    let policies = admin
        .get(&format!("/api/tenants/{}/policies", tenant.slug))
        .await
        .expect("policies")
        .success("reading the policies")
        .expect("policies");
    assert_eq!(policies["policies"]["graphql"]["mode"], "enforce");

    let again = platform.tenant(&slug, TenantOptions::default()).await;
    assert!(
        matches!(&again, Err(TestkitError::Status { status: 409, what, .. }) if what == "creating the tenant"),
        "{again:?}"
    );
    platform.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_database_made_for_a_tenant_to_adopt_goes_with_the_platform() {
    let Some(platform) = started(PlatformOptions::default()).await else {
        return;
    };
    let slug = Platform::unique_slug("kit");
    let database = format!("kit_adopted_{}", &slug[4..]);
    let url = platform
        .external_database(&database)
        .await
        .expect("a database");
    assert!(url.ends_with(&format!("/{database}")), "{url}");
    let tenant = platform
        .tenant(&slug, TenantOptions::default().database_name(&database))
        .await
        .expect("a tenant of that database");
    let listed = platform
        .admin()
        .expect("admin")
        .get("/api/tenants")
        .await
        .expect("tenants")
        .success("listing tenants")
        .expect("tenants");
    assert_eq!(
        listed
            .as_array()
            .into_iter()
            .flatten()
            .find(|listed| listed["slug"] == json!(tenant.slug))
            .map(|listed| &listed["database_name"]),
        Some(&json!(database))
    );
    let observer = cufflink_testkit::infra::try_infra()
        .await
        .expect("infra")
        .try_create_database()
        .await
        .expect("a database to look from");
    assert!(observer.database_exists(&database).await);
    platform.shutdown().await.expect("a clean shutdown");
    assert!(!observer.database_exists(&database).await);
}

#[tokio::test]
async fn a_process_holds_several_platforms() {
    let (Some(first), Some(second)) = tokio::join!(
        started(PlatformOptions::default()),
        started(PlatformOptions::default().configure(|config| {
            cufflink_platform::config::PlatformConfig {
                sql_max_rows: 7,
                ..config
            }
        }))
    ) else {
        return;
    };
    assert_ne!(first.base_url(), second.base_url());
    let slug = Platform::unique_slug("kit");
    first
        .tenant(&slug, TenantOptions::default())
        .await
        .expect("tenant");
    let elsewhere = second
        .admin()
        .expect("admin")
        .get("/api/tenants")
        .await
        .expect("tenants")
        .success("listing tenants")
        .expect("tenants");
    assert_eq!(elsewhere, json!([]), "each has a database of its own");
    drop(first);
    second.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn a_platform_without_nats_still_serves() {
    let Some(platform) = started(PlatformOptions::default().without_nats()).await else {
        return;
    };
    assert!(matches!(
        platform.nats().await,
        Err(TestkitError::Unavailable(_))
    ));
    assert_eq!(
        platform
            .anonymous()
            .get("/health")
            .await
            .expect("health")
            .status,
        200
    );
}
