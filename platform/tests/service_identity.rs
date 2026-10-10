use cufflink_testkit as common;

use common::{infra, PlatformProcess, Tenant, TestDb};
use cufflink_platform::errors::PlatformError;
use cufflink_platform::repo::deployments::{self, ActivateVersion};
use cufflink_platform::repo::services::{self, Status};
use cufflink_platform::tenant_db::TenantPoolManager;
use reqwest::Method;
use serde_json::{json, Value};

fn loans_manifest(name: &str, schema_name: Option<&str>) -> Value {
    let mut manifest = json!({
        "name": name,
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "loans", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "body", "column_type": "text", "primary_key": false, "nullable": false,
             "auto_generate": false},
            {"name": "created_at", "column_type": "timestamp", "primary_key": false,
             "nullable": false, "auto_generate": true},
        ]}],
        "publishes": [],
    });
    if let Some(schema) = schema_name {
        manifest["schema_name"] = json!(schema);
    }
    manifest
}

async fn tenant_client(db: &TestDb, tenant: &Tenant) -> deadpool_postgres::Object {
    let infra = common::infra().await.expect("infra");
    cufflink_db::build_pool(
        &infra::with_dbname(
            &infra.database_url,
            &TenantPoolManager::managed_db_name(&tenant.slug),
        ),
        1,
    )
    .expect("tenant pool")
    .get()
    .await
    .unwrap_or_else(|e| panic!("tenant database of {}: {e}", db.name))
}

async fn bodies(client: &deadpool_postgres::Object, schema: &str) -> Vec<String> {
    client
        .query(
            &format!("SELECT body FROM \"{schema}\".loans ORDER BY body"),
            &[],
        )
        .await
        .expect("loans")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

async fn found_or_created(
    pool: &deadpool_postgres::Pool,
    tenant_id: uuid::Uuid,
    name: &str,
    schema: &str,
) -> Result<services::Service, PlatformError> {
    services::find_or_create(pool, tenant_id, name, schema, Status::Active).await
}

fn validation(result: Result<services::Service, PlatformError>) -> String {
    match result {
        Err(PlatformError::Validation(message)) => message,
        other => panic!("expected a validation error, got {other:?}"),
    }
}

#[tokio::test]
async fn a_deploy_records_the_schema_the_service_always_had() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let (status, body) = platform
        .deploy(&tenant, &loans_manifest("loan-service", None))
        .await;
    assert_eq!(status, 200, "{body}");
    let service = services::find(&db.pool(), tenant.id, "loan-service")
        .await
        .expect("lookup")
        .expect("service");
    assert_eq!(service.schema_name.as_deref(), Some("loan_service"));
    assert_eq!(service.schema(), "loan_service");

    let (status, created) = platform
        .send(
            Method::POST,
            &format!("/svc/{}/loan-service/loans", tenant.slug),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({"body": "first"})),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    assert_eq!(
        bodies(&tenant_client(&db, &tenant).await, "loan_service").await,
        ["first"]
    );
}

#[tokio::test]
async fn a_declared_schema_is_where_the_service_reads_and_writes() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let service = found_or_created(&db.pool(), tenant.id, "loans-api", "loan_service")
        .await
        .expect("service");
    assert_eq!(service.schema(), "loan_service");
    deployments::activate_version(
        &db.pool(),
        ActivateVersion {
            service_id: service.id,
            version: 1,
            manifest_hash: "m",
            manifest: &serde_json::from_value(loans_manifest("loans-api", Some("loan_service")))
                .expect("a manifest"),
            wasm_hash: None,
            commit_hash: None,
            schema_json: None,
            schema_hash: "",
            native: None,
        },
    )
    .await
    .expect("deployment");
    let client = tenant_client(&db, &tenant).await;
    client
        .batch_execute(
            "CREATE SCHEMA loan_service;
             CREATE TABLE loan_service.loans (
                 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), body TEXT NOT NULL,
                 created_at TIMESTAMPTZ NOT NULL DEFAULT now());
             INSERT INTO loan_service.loans (body) VALUES ('adopted')",
        )
        .await
        .expect("the service's schema");

    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let loans = format!("/svc/{}/loans-api/loans", tenant.slug);
    let key = format!("ApiKey {}", tenant.api_key);
    let (status, created) = platform
        .send(
            Method::POST,
            &loans,
            &key,
            Some(&json!({"body": "written"})),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    let (status, listed) = platform.send(Method::GET, &loans, &key, None).await;
    assert_eq!(status, 200, "{listed}");
    let mut listed: Vec<&str> = listed["results"]
        .as_array()
        .expect("results")
        .iter()
        .filter_map(|loan| loan["body"].as_str())
        .collect();
    listed.sort_unstable();
    assert_eq!(listed, ["adopted", "written"]);
    assert_eq!(
        bodies(&client, "loan_service").await,
        ["adopted", "written"]
    );
    let derived: bool = client
        .query_one("SELECT to_regnamespace('loans_api') IS NOT NULL", &[])
        .await
        .expect("namespace")
        .get(0);
    assert!(!derived, "the schema the name would give is never created");
}

#[tokio::test]
async fn a_schema_is_one_services_and_stays_it() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let other_tenant = db.mint_tenant().await;
    let pool = db.pool();

    let loans = found_or_created(&pool, tenant.id, "loan-service", "loan_service")
        .await
        .expect("first deploy");
    assert_eq!(loans.schema_name.as_deref(), Some("loan_service"));
    let again = found_or_created(&pool, tenant.id, "loan-service", "loan_service")
        .await
        .expect("second deploy");
    assert_eq!(again.id, loans.id);

    assert_eq!(
        validation(found_or_created(&pool, tenant.id, "loans", "loan_service").await),
        "schema_name: 'loan_service' is the schema of service 'loan-service' of this tenant"
    );
    assert!(services::find(&pool, tenant.id, "loans")
        .await
        .expect("lookup")
        .is_none());
    found_or_created(&pool, other_tenant.id, "loans", "loan_service")
        .await
        .expect("another tenant's schema is its own");

    assert_eq!(
        validation(found_or_created(&pool, tenant.id, "loan-service", "loans_v2").await),
        "schema_name is immutable once deployed: loan_service \u{2192} loans_v2"
    );
}

#[tokio::test]
async fn a_service_deployed_before_schemas_were_recorded_keeps_its_own() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let pool = db.pool();
    pool.get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO services (tenant_id, name, current_version) VALUES ($1, 'stock-service', 7)",
            &[&tenant.id],
        )
        .await
        .expect("a service R2 deployed");

    let service = found_or_created(&pool, tenant.id, "stock-service", "stock_service")
        .await
        .expect("redeploy");
    assert_eq!(service.schema_name, None);
    assert_eq!(service.schema(), "stock_service");
    assert_eq!(
        validation(found_or_created(&pool, tenant.id, "stock-service", "stocks").await),
        "schema_name is immutable once deployed: stock_service \u{2192} stocks"
    );
    assert_eq!(
        validation(found_or_created(&pool, tenant.id, "stocks", "stock_service").await),
        "schema_name: 'stock_service' is the schema of service 'stock-service' of this tenant"
    );
}

#[tokio::test]
async fn a_reserved_schema_is_refused_to_a_new_service_and_left_to_one_that_has_it() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let pool = db.pool();

    for (name, schema) in [
        ("public", "public"),
        ("Public", "Public"),
        ("_cufflink", "_cufflink"),
        ("pg-catalog", "pg_catalog"),
        ("information-schema", "information_schema"),
        ("loans", "_cufflink_audit"),
    ] {
        assert!(
            validation(found_or_created(&pool, tenant.id, name, schema).await).starts_with(
                &format!("schema_name: schema '{schema}' of service '{name}' is reserved")
            ),
            "{name}"
        );
        assert!(services::find(&pool, tenant.id, name)
            .await
            .expect("lookup")
            .is_none());
    }

    pool.get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO services (tenant_id, name, current_version) VALUES ($1, 'public', 3)",
            &[&tenant.id],
        )
        .await
        .expect("a service an earlier release deployed");
    let kept = found_or_created(&pool, tenant.id, "public", "public")
        .await
        .expect("a service that has its row keeps its schema");
    assert_eq!((kept.schema(), kept.current_version), ("public".into(), 3));
}

#[tokio::test]
async fn two_services_whose_schemas_differ_by_case_alone_share_one() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let pool = db.pool();
    found_or_created(&pool, tenant.id, "Loan-Service", "Loan_Service")
        .await
        .expect("first");
    assert_eq!(
        validation(found_or_created(&pool, tenant.id, "loan-service", "loan_service").await),
        "schema_name: 'loan_service' is the schema of service 'Loan-Service' of this tenant"
    );
    let refused = pool
        .get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO services (tenant_id, name, schema_name) VALUES ($1, 'loans', 'LOAN_SERVICE')",
            &[&tenant.id],
        )
        .await
        .expect_err("the index holds what the check holds");
    assert_eq!(
        refused.as_db_error().and_then(|e| e.constraint()),
        Some(services::SCHEMA_INDEX)
    );
}

#[tokio::test]
async fn the_schema_sql_knows_is_the_schema_the_service_has() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let client = db.pool().get().await.expect("checkout");
    for (name, schema_name) in [
        ("stock-service", None),
        ("Order-Desk_2", None),
        ("loans", Some("loan_service")),
        ("order", None),
        ("a-b-c", None),
        ("plain", None),
    ] {
        client
            .execute(
                "INSERT INTO services (tenant_id, name, schema_name) VALUES ($1, $2, $3)",
                &[&tenant.id, &name, &schema_name],
            )
            .await
            .expect("service");
        let in_sql: String = client
            .query_one(
                &format!(
                    "SELECT {} FROM services WHERE tenant_id = $1 AND name = $2",
                    services::SCHEMA_SQL
                ),
                &[&tenant.id, &name],
            )
            .await
            .expect("schema")
            .get(0);
        let service = services::find(&db.pool(), tenant.id, name)
            .await
            .expect("lookup")
            .expect("service");
        assert_eq!(
            in_sql,
            cufflink_types::manifest::validate::folded(&service.schema()),
            "{name}"
        );
        assert_eq!(
            service.schema(),
            loans_manifest(name, schema_name)
                .as_object()
                .map(|_| cufflink_types::service_schema(name, schema_name))
                .expect("manifest"),
            "{name}"
        );
    }
}

#[tokio::test]
async fn a_schema_that_is_a_keyword_or_has_capitals_is_one_schema_everywhere() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let key = format!("ApiKey {}", tenant.api_key);
    let client = tenant_client(&db, &tenant).await;

    for (name, schema) in [("order", "order"), ("Order-Desk", "order_desk")] {
        let (status, body) = platform.deploy(&tenant, &loans_manifest(name, None)).await;
        assert_eq!(status, 200, "{name}: {body}");
        let loans = format!("/svc/{}/{name}/loans", tenant.slug);
        let (status, created) = platform
            .send(Method::POST, &loans, &key, Some(&json!({"body": "first"})))
            .await;
        assert_eq!(status, 200, "{name}: {created}");
        let (status, listed) = platform.send(Method::GET, &loans, &key, None).await;
        assert_eq!(status, 200, "{name}: {listed}");
        assert_eq!(listed["results"][0]["body"], "first", "{name}: {listed}");
        assert_eq!(bodies(&client, schema).await, ["first"], "{name}");

        let service = services::find(&db.pool(), tenant.id, name)
            .await
            .expect("lookup")
            .expect("service");
        let answer = platform
            .http
            .post(format!(
                "{}/api/services/{}/query",
                platform.base_url, service.id
            ))
            .header("authorization", &key)
            .json(&json!({"sql": "SELECT body FROM loans"}))
            .send()
            .await
            .expect("query")
            .text()
            .await
            .expect("frames");
        assert!(answer.contains("\"first\""), "{name}: {answer}");
    }

    let guest = common::wasm_manifest("user", &[("insert_slow", "/insert")]);
    let (status, body) = platform.deploy(&tenant, &guest).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = platform.call_fn(&tenant, "user", "insert_slow").await;
    assert_eq!(status, 200, "{body}");
    let titles: Vec<String> = client
        .query("SELECT title FROM \"user\".items", &[])
        .await
        .expect("items")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(titles, ["hook"], "the guest's SQL resolved in its schema");
}

#[tokio::test]
async fn an_index_over_several_columns_deploys_as_it_did() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let mut manifest = loans_manifest("loan-service", None);
    manifest["tables"][0]["indexes"] = json!([
        {"name": "idx_loans_body_created", "columns": ["body", "created_at"]},
        {"name": "idx_loans_id_body", "columns": ["id", "body"], "unique": true},
    ]);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "{body}");

    let indexes: Vec<(String, bool)> = tenant_client(&db, &tenant)
        .await
        .query(
            "SELECT c.relname::text, i.indisunique FROM pg_index i
             JOIN pg_class c ON c.oid = i.indexrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'loan_service' AND c.relname LIKE 'idx_loans_%' ORDER BY 1",
            &[],
        )
        .await
        .expect("indexes")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        indexes,
        [
            ("idx_loans_body_created".to_string(), false),
            ("idx_loans_id_body".to_string(), true)
        ]
    );
}

#[tokio::test]
async fn a_service_is_created_with_the_status_given_and_keeps_the_one_it_has() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let pool = db.pool();
    for (name, created, later) in [
        ("quiet", Status::Inactive, Status::Active),
        ("running", Status::Active, Status::Inactive),
    ] {
        let first = services::find_or_create(&pool, tenant.id, name, name, created)
            .await
            .expect("created");
        assert_eq!(first.status, created, "{name}");
        let again = services::find_or_create(&pool, tenant.id, name, name, later)
            .await
            .expect("found");
        assert_eq!((again.id, again.status), (first.id, created), "{name}");
        let stored = services::get_by_id(&pool, first.id)
            .await
            .expect("lookup")
            .expect("the service");
        assert_eq!(stored.status, created, "{name}");
    }
}
