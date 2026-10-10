use cufflink_testkit as common;

use common::{PlatformProcess, TestDb};
use cufflink_platform::outbox::bootstrap::{self, BootstrapError};
use cufflink_platform::policy::{PolicyKey, PolicyMode, PolicySet};
use cufflink_platform::repo::tenants::NewTenant;
use cufflink_platform::tenant_catalog::TenantCatalog;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_platform::tenant_sessions::TenantSessions;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_postgres::Client;
use uuid::Uuid;

fn suffix() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

async fn count(client: &Client, sql: &str) -> i64 {
    client.query_one(sql, &[]).await.expect(sql).get(0)
}

async fn has_outbox(client: &Client) -> bool {
    client
        .query_one("SELECT to_regclass('_cufflink.outbox') IS NOT NULL", &[])
        .await
        .expect("regclass")
        .get(0)
}

async fn applied(client: &Client) -> Vec<(i32, String)> {
    client
        .query(
            "SELECT version, name FROM _cufflink.schema_history ORDER BY version",
            &[],
        )
        .await
        .expect("history")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

async fn put_outbox(
    platform: &PlatformProcess,
    admin: &str,
    slug: &str,
    mode: &str,
) -> (u16, Value) {
    platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{slug}/policies"),
            admin,
            Some(&json!({"policies": {"outbox": {"mode": mode}}})),
        )
        .await
}

struct Adopted {
    platform: PlatformProcess,
    _db: TestDb,
    admin: String,
    slug: String,
    database: Client,
}

async fn adopted(before: &str) -> Option<Adopted> {
    let infra = common::infra().await?;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let suffix = suffix();
    let name = format!("harbor_{suffix}");
    let database = connect(&db.create_external_database(&name).await).await;
    database.batch_execute(before).await.expect("before");
    let slug = format!("it-outbox-{suffix}");
    let (status, tenant) = platform
        .send(
            Method::POST,
            "/api/tenants",
            &admin,
            Some(&json!({"name": slug, "slug": slug, "database_name": name})),
        )
        .await;
    assert_eq!(status, 200, "{tenant}");
    Some(Adopted {
        platform,
        _db: db,
        admin,
        slug,
        database,
    })
}

#[tokio::test]
async fn the_schema_is_made_once_in_an_adopted_database() {
    let Some(test) = adopted(
        "CREATE SCHEMA legacy; CREATE TABLE legacy.kept (id INT PRIMARY KEY);
         INSERT INTO legacy.kept VALUES (1), (2), (3)",
    )
    .await
    else {
        return;
    };
    assert!(
        !has_outbox(&test.database).await,
        "a tenant that enforces nothing is given nothing"
    );

    for round in 1..=2 {
        let (status, body) = put_outbox(&test.platform, &test.admin, &test.slug, "enforce").await;
        assert_eq!(status, 200, "round {round}: {body}");
        assert_eq!(body["policies"]["outbox"]["mode"], "enforce");
        assert_eq!(
            applied(&test.database).await,
            [
                (1, "outbox".to_string()),
                (2, "service_enqueue".to_string())
            ],
            "round {round}"
        );
    }

    assert_eq!(
        count(&test.database, "SELECT count(*) FROM legacy.kept").await,
        3
    );
    let open_to_all: bool = test
        .database
        .query_one(
            "SELECT has_schema_privilege('public', '_cufflink', 'USAGE')
                 OR has_function_privilege('public',
                    '_cufflink.outbox_enqueue_many(text[], text[], text[], bytea[], jsonb[], uuid[])',
                    'EXECUTE')
                 OR has_function_privilege('public',
                    '_cufflink.outbox_enqueue_own(text[], text[], bytea[], jsonb[], uuid[])',
                    'EXECUTE')
                 OR has_table_privilege('public', '_cufflink.service_roles', 'SELECT')",
            &[],
        )
        .await
        .expect("privileges")
        .get(0);
    assert!(!open_to_all, "PUBLIC is given nothing of the schema");

    test.database
        .batch_execute("LISTEN _cufflink_outbox")
        .await
        .expect("listen");
    test.database
        .execute(
            "SELECT _cufflink.outbox_enqueue_many(
                 ARRAY['event', 'audit'], ARRAY['notes', 'notes'],
                 ARRAY['dw.t.notes.notes.created', 'dw.t.logs'],
                 ARRAY['\\x7b7d'::bytea, '\\x7b7d'::bytea],
                 ARRAY['[]'::jsonb, '[[\"traceparent\", \"00-1-2-01\"]]'::jsonb],
                 ARRAY[gen_random_uuid(), gen_random_uuid()])",
            &[],
        )
        .await
        .expect("enqueue");
    let rows = test
        .database
        .query(
            "SELECT kind, subject, source_role::text = session_user::text, attempts,
                    available_at <= clock_timestamp()
             FROM _cufflink.outbox ORDER BY id",
            &[],
        )
        .await
        .expect("rows");
    let written: Vec<(String, String, bool, i32, bool)> = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
        .collect();
    assert_eq!(
        written,
        [
            (
                "event".to_string(),
                "dw.t.notes.notes.created".to_string(),
                true,
                0,
                true
            ),
            ("audit".to_string(), "dw.t.logs".to_string(), true, 0, true),
        ]
    );
}

#[tokio::test]
async fn a_database_that_refuses_the_schema_refuses_the_policy() {
    let Some(test) =
        adopted("CREATE SCHEMA _cufflink; CREATE TABLE _cufflink.outbox (id INT PRIMARY KEY)")
            .await
    else {
        return;
    };
    let (status, body) = put_outbox(&test.platform, &test.admin, &test.slug, "enforce").await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        body["error"],
        "outbox: migration V1__outbox failed: relation \"outbox\" already exists"
    );
    let (status, policies) = test
        .platform
        .send(
            Method::GET,
            &format!("/api/tenants/{}/policies", test.slug),
            &test.admin,
            None,
        )
        .await;
    assert_eq!(status, 200, "{policies}");
    assert_eq!(policies["policies"]["outbox"]["mode"], "off");
    assert!(applied(&test.database).await.is_empty());

    let (status, body) = put_outbox(&test.platform, &test.admin, &test.slug, "off").await;
    assert_eq!(
        status, 200,
        "a policy that needs nothing asks nothing: {body}"
    );
}

#[tokio::test]
async fn a_platform_without_the_privilege_is_told_by_the_database() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let suffix = suffix();
    let (name, role) = (format!("harbor_{suffix}"), format!("limited_{suffix}"));
    let external = db.create_external_database(&name).await;
    let owner = connect(&external).await;
    owner
        .batch_execute(&format!("CREATE ROLE \"{role}\" LOGIN PASSWORD 'limited'"))
        .await
        .expect("role");
    let slug = format!("it-outbox-{suffix}");
    let pool = db.pool();
    let tenant = cufflink_platform::repo::tenants::create_tenant(
        &pool.get().await.expect("checkout"),
        &NewTenant {
            name: &slug,
            slug: &slug,
            database_name: Some(&name),
            ..Default::default()
        },
    )
    .await
    .expect("tenant");
    let mut limited = url::Url::parse(&db.url).expect("url");
    limited.set_username(&role).expect("username");
    limited.set_password(Some("limited")).expect("password");
    let sessions = TenantSessions::new(
        TenantPoolManager::new(limited.as_str(), 4, 2)
            .expect("tenant pools")
            .with_direct_url(limited.as_str())
            .expect("direct"),
        TenantCatalog::new(
            pool,
            async_nats::connect(nats_url).await.expect("nats"),
            PolicySet::builtin_defaults(),
        ),
    );

    let refused = bootstrap::ensure(&sessions, tenant.id)
        .await
        .expect_err("no CREATE on the database");
    assert!(matches!(refused, BootstrapError::Schema(_)), "{refused:?}");
    assert_eq!(
        refused.to_string(),
        format!(
            "the schema _cufflink cannot be created in the tenant's database: \
             permission denied for database {name}"
        )
    );
    assert!(matches!(
        cufflink_platform::errors::PlatformError::from(refused),
        cufflink_platform::errors::PlatformError::Validation(_)
    ));

    owner
        .batch_execute(&format!(
            "GRANT CREATE ON DATABASE \"{name}\" TO \"{role}\""
        ))
        .await
        .expect("grant");
    let report = bootstrap::ensure(&sessions, tenant.id)
        .await
        .expect("granted");
    assert_eq!(report.applied.len(), 2);
    let again = bootstrap::ensure(&sessions, tenant.id)
        .await
        .expect("again");
    assert!(again.applied.is_empty());
    assert_eq!(again.verified, 2);

    drop(sessions);
    owner
        .batch_execute(&format!(
            "DROP SCHEMA _cufflink CASCADE; \
             REVOKE ALL ON DATABASE \"{name}\" FROM \"{role}\"; DROP ROLE \"{role}\""
        ))
        .await
        .expect("the role is the server's, not the database's");
}

#[tokio::test]
async fn a_statement_that_timed_out_is_no_refusal_of_the_database() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = connect(&db.url).await;
    client
        .batch_execute("SET statement_timeout = '20ms'")
        .await
        .expect("a timeout");
    let timed_out = client
        .batch_execute("SELECT pg_sleep(5)")
        .await
        .expect_err("cancelled");
    assert_eq!(
        timed_out.code(),
        Some(&tokio_postgres::error::SqlState::QUERY_CANCELED)
    );
    let answered =
        cufflink_platform::errors::PlatformError::from(BootstrapError::Schema(timed_out));
    assert!(
        matches!(&answered, cufflink_platform::errors::PlatformError::Unavailable(m)
            if m == "the schema _cufflink cannot be created in the tenant's database: \
                     canceling statement due to statement timeout"),
        "{answered}"
    );
}

#[tokio::test]
async fn replicas_that_start_together_make_the_schema_once() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let sessions = TenantSessions::new(
        TenantPoolManager::new(&db.url, 4, 2).expect("tenant pools"),
        TenantCatalog::new(
            db.pool(),
            async_nats::connect(nats_url).await.expect("nats"),
            PolicySet::builtin_defaults(),
        ),
    );
    let reports =
        futures_util::future::join_all((0..4).map(|_| bootstrap::ensure(&sessions, tenant.id)))
            .await;
    let applied: usize = reports
        .iter()
        .map(|report| report.as_ref().expect("ensured").applied.len())
        .sum();
    assert_eq!(applied, 2, "{reports:?}");
}

#[tokio::test]
async fn a_platform_that_starts_gives_the_tenants_that_need_it_their_schema() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let (enforcing, roles, plain) = (
        db.mint_tenant().await,
        db.mint_tenant().await,
        db.mint_tenant().await,
    );
    let client = db.pool().get().await.expect("checkout");
    for (tenant, key, mode) in [
        (&enforcing, PolicyKey::Outbox, PolicyMode::Enforce),
        (&roles, PolicyKey::DbRoles, PolicyMode::Warn),
    ] {
        cufflink_platform::repo::tenant_policies::upsert(
            &client,
            tenant.id,
            key,
            mode,
            &json!({}),
            "test",
        )
        .await
        .expect("policy");
    }
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;

    let database = |tenant: &common::Tenant| {
        common::infra::with_dbname(&db.url, &TenantPoolManager::managed_db_name(&tenant.slug))
    };
    for tenant in [&enforcing, &roles] {
        let client = connect(&database(tenant)).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while !has_outbox(&client).await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "no schema for {}: {}",
                tenant.slug,
                platform.output_tail()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    assert!(
        !has_outbox(&connect(&database(&plain)).await).await,
        "a tenant at the defaults is given nothing"
    );
}

#[tokio::test]
async fn a_tenant_created_under_an_enforcing_default_has_its_schema_at_once() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        Some(nats_url),
        &[("CUFFLINK_POLICY_OUTBOX", "enforce")],
    )
    .await;
    platform.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let slug = format!("it-outbox-{}", suffix());
    db.adopt_tenant_db(&slug);
    let (status, tenant) = platform
        .send(
            Method::POST,
            "/api/tenants",
            &admin,
            Some(&json!({"name": slug, "slug": slug})),
        )
        .await;
    assert_eq!(status, 200, "{tenant}");
    let client = connect(&common::infra::with_dbname(
        &db.url,
        &TenantPoolManager::managed_db_name(&slug),
    ))
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !has_outbox(&client).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sooner than the minute between two looks: {}",
            platform.output_tail()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn orders() -> Value {
    json!({
        "name": "shop",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "orders", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "n", "column_type": "integer", "primary_key": false, "nullable": false,
             "auto_generate": false},
        ]}],
        "publishes": [],
    })
}

#[tokio::test]
async fn a_write_that_finds_no_outbox_has_it_made_and_is_to_be_made_again() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let tenant = db.mint_tenant().await;
    let (status, deployed) = platform.deploy(&tenant, &orders()).await;
    assert_eq!(status, 200, "{deployed}");
    let (status, body) = put_outbox(&platform, &admin, &tenant.slug, "enforce").await;
    assert_eq!(status, 200, "{body}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while platform
        .metric("cufflink_outbox_depth", &[("tenant", &tenant.slug)])
        .await
        .is_none()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no relay: {}",
            platform.output_tail()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let database = connect(&common::infra::with_dbname(
        &db.url,
        &TenantPoolManager::managed_db_name(&tenant.slug),
    ))
    .await;
    let orders = || count(&database, "SELECT count(*) FROM shop.orders");
    let create = || {
        platform
            .http
            .post(format!(
                "{}/svc/{}/shop/orders",
                platform.base_url, tenant.slug
            ))
            .header("authorization", format!("ApiKey {}", tenant.api_key))
            .json(&json!({"n": 1}))
            .send()
    };

    database
        .batch_execute("DROP SCHEMA _cufflink CASCADE")
        .await
        .expect("the schema is gone");
    let refused = create().await.expect("request");
    assert_eq!(refused.status(), 503);
    assert_eq!(
        refused
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    let body: Value = refused.json().await.expect("json");
    assert_eq!(
        body["error"],
        "the tenant's outbox was not ready: nothing was written, and the request can be made again"
    );
    assert_eq!(orders().await, 0, "nothing was written");
    assert_eq!(
        applied(&database).await,
        [
            (1, "outbox".to_string()),
            (2, "service_enqueue".to_string())
        ]
    );

    let created = create().await.expect("request");
    assert_eq!(created.status(), 200);
    assert_eq!(orders().await, 1);
}
