use cufflink_testkit as common;

use common::{legacy, Deployment, PlatformProcess, Tenant, TestDb};
use cufflink_platform::repo::deployments;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use uuid::Uuid;

const SHOP: &str = "shop";

fn table(name: &str, declared: Value) -> Value {
    let mut table = json!({
        "name": name,
        "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
            {"name": "body", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false},
            {"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true}
        ]
    });
    if let (Some(table), Some(declared)) = (table.as_object_mut(), declared.as_object()) {
        table.extend(declared.clone());
    }
    table
}

fn shop(version: &str, tables: Value, private_tables: Value) -> Value {
    json!({"manifest": {
        "name": SHOP, "version": version, "mode": "crud", "publishes": [], "graphql": true,
        "tables": tables, "private_tables": private_tables,
    }})
}

fn names(tables: &Value) -> Vec<&str> {
    tables
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|table| table["name"].as_str().or_else(|| table.as_str()))
        .collect()
}

async fn service_id(d: &Deployment, name: &str) -> Uuid {
    d.db.pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT id FROM services WHERE tenant_id = $1 AND name = $2",
            &[&d.tenant.id, &name],
        )
        .await
        .expect("the service")
        .get(0)
}

async fn count(d: &Deployment, relation: &str) -> i64 {
    d.database
        .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
        .await
        .expect("count")
        .get(0)
}

async fn asked(
    d: &Deployment,
    method: &Method,
    table: &str,
    below: &str,
    body: &Option<Value>,
) -> (u16, String) {
    let (status, answer) = d
        .send(
            method.clone(),
            &format!("/svc/{}/{SHOP}/{table}{below}", d.tenant.slug),
            body.clone(),
        )
        .await;
    (status, answer.to_string().replace(table, "{table}"))
}

#[tokio::test]
async fn g1_a_private_table_is_served_nowhere() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let (status, version) = d.send(Method::GET, "/api/version", None).await;
    assert_eq!(status, 200, "{version}");
    assert!(
        names(&version["supports"]).contains(&"private_tables"),
        "{version}"
    );

    d.database
        .batch_execute(
            "CREATE SCHEMA shop;
             CREATE TABLE shop.closed (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), body text,
                 created_at timestamptz NOT NULL DEFAULT now());
             INSERT INTO shop.closed (body) VALUES ('its owner wrote this');",
        )
        .await
        .expect("the table its owner made");
    let closed = table("closed", json!({"managed": false}));
    let deployed = d
        .deployed(shop(
            "0.1.0",
            json!([table("open", json!({}))]),
            json!([closed]),
        ))
        .await;
    assert_eq!(deployed["plan"]["private_tables"], json!(["closed"]));
    assert_eq!(deployed["plan"].get("no_longer_served"), None, "{deployed}");
    let id = deployed["service_id"].as_str().expect("service id");
    let svc = format!("/svc/{}/{SHOP}", d.tenant.slug);

    let row = Some(json!({"body": "a row"}));
    let item = format!("/{}", Uuid::new_v4());
    for (method, below, body) in [
        (Method::GET, "", None),
        (Method::POST, "", row.clone()),
        (Method::GET, item.as_str(), None),
        (Method::PUT, item.as_str(), row.clone()),
        (Method::DELETE, item.as_str(), None),
        (Method::GET, "/stream", None),
        (Method::POST, "/batch", Some(json!([{"body": "a row"}]))),
    ] {
        let private = asked(&d, &method, "closed", below, &body).await;
        let undeclared = asked(&d, &method, "nosuch", below, &body).await;
        assert_eq!(private, undeclared, "{method} /closed{below}");
        assert!(
            (400..500).contains(&private.0),
            "{method} /closed{below}: {private:?}"
        );
    }

    let query = |table: &str| Some(json!({"query": format!("{{ {table} {{ id }} }}")}));
    let graphql = format!("{svc}/graphql");
    let (status, private) = d.send(Method::POST, &graphql, query("closed")).await;
    let (_, undeclared) = d.send(Method::POST, &graphql, query("nosuch")).await;
    assert_eq!(status, 400, "{private}");
    assert_eq!(
        private.to_string().replace("closed", "nosuch"),
        undeclared.to_string()
    );
    let schema = d
        .platform
        .http
        .get(format!("{}{svc}/graphql/schema", d.platform.base_url))
        .header("authorization", format!("ApiKey {}", d.tenant.api_key))
        .send()
        .await
        .expect("request")
        .text()
        .await
        .expect("the schema");
    assert!(schema.contains("open"), "{schema}");
    assert!(!schema.to_lowercase().contains("closed"), "{schema}");

    let seed = format!("/api/services/{id}/seed");
    let (status, refused) = d
        .send(
            Method::POST,
            &seed,
            Some(json!({"tables": {"closed": [{"body": "seeded"}]}, "clear_existing": true})),
        )
        .await;
    assert_eq!(status, 400, "{refused}");
    assert!(
        refused.to_string().contains("Unknown table: closed"),
        "{refused}"
    );
    assert_eq!(count(&d, "shop.closed").await, 1, "and nothing was cleared");

    let (status, created) = d.send(Method::POST, &format!("{svc}/open"), row).await;
    assert!(status == 200 || status == 201, "{created}");
    let (status, listed) = d.send(Method::GET, &format!("{svc}/open"), None).await;
    assert_eq!(status, 200, "{listed}");
    assert!(listed.to_string().contains("a row"), "{listed}");
    let (status, queried) = d.send(Method::POST, &graphql, query("open")).await;
    assert_eq!(status, 200, "{queried}");
    assert_eq!(queried["data"]["open"][0]["id"], created["id"], "{queried}");

    let (status, openapi) = d
        .send(Method::GET, &format!("{svc}/_meta/openapi.json"), None)
        .await;
    assert_eq!(status, 200, "{openapi}");
    let spec = openapi.to_string().to_lowercase();
    assert!(spec.contains("/open"), "{spec}");
    assert!(!spec.contains("closed"), "{spec}");

    let (status, health) = d
        .send(Method::GET, &format!("{svc}/_meta/health"), None)
        .await;
    assert_eq!(status, 200, "{health}");
    assert_eq!(health["table_count"], 1, "{health}");

    let (status, schema) = d
        .send(Method::GET, &format!("/api/services/{id}/schema"), None)
        .await;
    assert_eq!(status, 200, "{schema}");
    assert_eq!(names(&schema["tables"]), ["open"]);
    assert_eq!(names(&schema["private_tables"]), ["closed"]);
}

fn column(name: &str, column_type: Value) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": name == "id", "nullable": false})
}

fn loans(more: &[Value]) -> Value {
    let columns: Vec<Value> = [
        column("id", json!("uuid")),
        column("book_id", json!("uuid")),
        column("memo", json!("text")),
        column("loan_kind", json!({"varchar": {"length": 50}})),
        column("created_at", json!("timestamp_naive")),
    ]
    .into_iter()
    .chain(more.iter().cloned())
    .collect();
    json!({"name": "loans", "columns": columns})
}

fn loan_service(private_tables: Value) -> Value {
    json!({"manifest": {
        "name": "loan-service", "version": "0.1.0", "mode": "crud", "publishes": [],
        "schema": "adopted", "tables": [], "private_tables": private_tables,
    }})
}

#[tokio::test]
async fn g1_a_private_adopted_table_is_checked() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    legacy::migrated(&d.database).await;
    legacy::record_ddl(&d.database).await;

    let (status, refused) = d
        .deploy(loan_service(json!([loans(&[column(
            "archived_reason",
            json!("text")
        )])])))
        .await;
    assert_eq!(status, 422, "{refused}");
    assert!(
        refused.to_string().contains("archived_reason"),
        "the column the database lacks is named: {refused}"
    );
    let (status, refused) = d
        .deploy(loan_service(json!([
            loans(&[]),
            {"name": "loan_drafts", "columns": [column("id", json!("uuid"))]}
        ])))
        .await;
    assert_eq!(status, 422, "{refused}");
    assert!(refused.to_string().contains("loan_drafts"), "{refused}");

    let deployed = d.deployed(loan_service(json!([loans(&[])]))).await;
    assert_eq!(deployed["schema_changes"], 0, "{deployed}");
    assert_eq!(
        deployed["plan"]["adoption"],
        json!({"errors": [], "warnings": [], "undeclared_columns": {"loans": 2}}),
        "{deployed}"
    );
    assert_eq!(deployed["plan"]["private_tables"], json!(["loans"]));
    let id = service_id(&d, "loan-service").await;
    let snapshot_of = |answer: &Value, key: &str| {
        let version = answer[key]
            .as_i64()
            .and_then(|version| i32::try_from(version).ok())
            .expect("a version");
        let pool = d.db.pool();
        async move {
            deployments::snapshot(&pool, "loan-service", id, version)
                .await
                .expect("snapshot")
        }
    };
    let names = |snapshot: &deployments::Snapshot| -> Vec<String> {
        snapshot
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect()
    };
    let snapshot = snapshot_of(&deployed, "version").await;
    assert_eq!(names(&snapshot), ["loans"]);
    assert!(snapshot.adopted.table("loans").is_some());

    let without = d.deployed(loan_service(json!([]))).await;
    assert!(names(&snapshot_of(&without, "version").await).is_empty());
    let (status, back) = d
        .send(
            Method::POST,
            &format!("/api/services/{id}/rollback"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 200, "{back}");
    assert_eq!(back["rolled_back_to"], deployed["version"], "{back}");
    let restored = snapshot_of(&back, "new_version").await;
    assert_eq!(
        names(&restored),
        ["loans"],
        "a rollback records the private tables of the version it restores"
    );
    assert!(restored.adopted.table("loans").is_some());
    assert_eq!(
        legacy::ddl_statements(&d.database).await,
        legacy::DdlStatements::NONE
    );

    let (status, listed) = d
        .send(
            Method::GET,
            &format!("/svc/{}/loan-service/loans", d.tenant.slug),
            None,
        )
        .await;
    assert_eq!(status, 404, "{listed}");
}

async fn only_the_schema_was_ensured(d: &Deployment, after: &str) {
    let statements = legacy::ddl_statements(&d.database).await;
    assert!(
        statements
            .committed
            .iter()
            .all(|statement| statement.contains("CREATE SCHEMA IF NOT EXISTS")),
        "{after}: {statements:?}"
    );
    assert_eq!(
        statements.begun,
        statements.committed.len() as i64,
        "{after}: {statements:?}"
    );
    assert_eq!(
        legacy::recorded_ddl(&d.database, SHOP).await,
        Vec::<String>::new(),
        "{after}: nothing in the schema was made, changed or dropped"
    );
}

#[tokio::test]
async fn g1_a_table_moved_to_private_keeps_its_rows() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let open = || table("open", json!({}));
    let served = shop(
        "0.1.0",
        json!([
            open(),
            table("closed", json!({"search": {"columns": ["body"]}}))
        ]),
        json!([]),
    );
    let private = shop(
        "0.2.0",
        json!([open()]),
        json!([table("closed", json!({"managed": false}))]),
    );
    let deployed = d.deployed(served).await;
    let id = deployed["service_id"].as_str().expect("service id");
    let closed = format!("/svc/{}/{SHOP}/closed", d.tenant.slug);
    let (status, created) = d
        .send(Method::POST, &closed, Some(json!({"body": "kept"})))
        .await;
    assert!(status == 200 || status == 201, "{created}");
    let vector = "SELECT count(*) FROM shop.closed WHERE _search IS NOT NULL";
    assert_eq!(count(&d, "shop.closed").await, 1);
    legacy::record_ddl(&d.database).await;

    let (status, plan) = d.deploy_as("?plan=true", private.clone()).await;
    assert_eq!(status, 200, "{plan}");
    assert_eq!(plan["plan"]["schema_changes"], json!([]), "{plan}");
    assert_eq!(plan["plan"]["private_tables"], json!(["closed"]), "{plan}");
    assert_eq!(
        plan["plan"]["no_longer_served"],
        json!(["closed"]),
        "{plan}"
    );

    let moved = d.deployed(private).await;
    assert_eq!(moved["schema_changes"], 0, "{moved}");
    assert_eq!(moved["plan"]["no_longer_served"], json!(["closed"]));
    only_the_schema_was_ensured(&d, "the move").await;
    assert_eq!(count(&d, "shop.closed").await, 1, "its rows are there");
    assert_eq!(
        d.database
            .query_one(vector, &[])
            .await
            .expect("the vector column is there")
            .get::<_, i64>(0),
        1
    );
    let (status, gone) = d.send(Method::GET, &closed, None).await;
    assert_eq!(status, 404, "{gone}");

    let (status, back) = d
        .send(
            Method::POST,
            &format!("/api/services/{id}/rollback"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status, 200, "{back}");
    assert_eq!(back["rolled_back_to"], deployed["version"], "{back}");
    assert_eq!(back["plan"].get("private_tables"), None, "{back}");
    assert_eq!(
        back["plan"]["warnings"],
        json!([
            "table closed was adopted by the version being left and is declared managed now: \
             the platform makes it where it is not there, changes it as the manifest says, and \
             drops it when a later manifest leaves it out"
        ]),
        "a table that goes back to managed is taken over, as any adopted table is: {back}"
    );
    assert_eq!(
        back["plan"]["schema_changes"],
        json!([
            "CREATE TABLE IF NOT EXISTS \"shop\".\"closed\" (\"id\" UUID PRIMARY KEY DEFAULT \
             gen_random_uuid(), \"body\" TEXT, \"created_at\" TIMESTAMPTZ NOT NULL DEFAULT now())",
            "ALTER TABLE \"shop\".\"closed\" DROP COLUMN IF EXISTS \"_search\"",
            "ALTER TABLE \"shop\".\"closed\" ADD COLUMN \"_search\" tsvector GENERATED ALWAYS AS \
             (to_tsvector('simple'::regconfig, coalesce(\"body\"::text, ''))) STORED",
            "CREATE INDEX IF NOT EXISTS \"closed__search_gin\" ON \"shop\".\"closed\" USING gin \
             (\"_search\")"
        ]),
        "the takeover makes what is not there and rebuilds the vector it keeps: {back}"
    );
    let (status, listed) = d.send(Method::GET, &closed, None).await;
    assert_eq!(status, 200, "it is served again: {listed}");
    assert!(listed.to_string().contains("kept"), "{listed}");
    assert_eq!(count(&d, "shop.closed").await, 1);
    assert_eq!(
        d.database
            .query_one(vector, &[])
            .await
            .expect("the vector column is there")
            .get::<_, i64>(0),
        1
    );
}

const HANDLERS: [&str; 2] = ["query", "execute"];

struct Bound {
    platform: PlatformProcess,
    db: TestDb,
    tenant: Tenant,
    admin: String,
    tenant_db: tokio_postgres::Client,
    guest: Vec<u8>,
}

impl Bound {
    async fn start() -> Option<Self> {
        let guest = common::sdk::sdk_guest("db-roles", Profile::Debug)?;
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let kek = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [6u8; 32]);
        let platform = PlatformProcess::start(
            &db.url,
            infra.nats_url(),
            &[
                ("CUFFLINK_SECRETS_KEK", &kek),
                ("CUFFLINK_SECRETS_KEK_ID", "test"),
            ],
        )
        .await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let url =
            common::infra::with_dbname(&db.url, &TenantPoolManager::managed_db_name(&tenant.slug));
        let (tenant_db, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("tenant database");
        tokio::spawn(connection);
        Some(Self {
            platform,
            db,
            tenant,
            admin,
            tenant_db,
            guest,
        })
    }

    async fn deploy(&self, n: u32, private: bool) -> (u16, Value) {
        let paths = HANDLERS.map(|handler| format!("/{handler}"));
        let routes: Vec<(&str, &str)> = HANDLERS
            .iter()
            .zip(&paths)
            .map(|(handler, path)| (*handler, path.as_str()))
            .collect();
        let mut manifest = common::platform::wasm_manifest("loan-service", &routes);
        let unmanaged = |name: &str| table(name, json!({"managed": false}));
        let (tables, private_tables) = match private {
            true => (json!([unmanaged("open")]), json!([unmanaged("closed")])),
            false => (json!([unmanaged("open"), unmanaged("closed")]), json!([])),
        };
        manifest["version"] = json!(format!("0.1.{n}"));
        manifest["tables"] = tables;
        manifest["private_tables"] = private_tables;
        self.platform
            .deploy_module(&self.tenant, &manifest, &self.guest)
            .await
    }

    async fn db_role(&self, method: Method, step: &str, body: Option<Value>) -> Value {
        let (status, answer) = self
            .platform
            .send(
                method,
                &format!(
                    "/api/tenants/{}/services/loan-service/db-role{step}",
                    self.tenant.slug
                ),
                &self.admin,
                body.as_ref(),
            )
            .await;
        assert_eq!(status, 200, "{answer}");
        answer
    }

    async fn sql(&self, sql: &str) {
        self.tenant_db.batch_execute(sql).await.expect(sql);
    }

    async fn query(&self, sql: &str) -> (u16, Value) {
        let response = self
            .platform
            .http
            .post(self.platform.fn_url(&self.tenant, "loan-service", "query"))
            .json(&json!({ "sql": sql }))
            .send()
            .await
            .expect("fn request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }
}

fn failed(report: &Value) -> Vec<&str> {
    report["checks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|check| check["ok"] == false)
        .filter_map(|check| check["check"].as_str())
        .collect()
}

#[tokio::test]
async fn g1_a_private_table_without_grants_keeps_the_binding_verified() {
    let Some(test) = Bound::start().await else {
        return;
    };
    let role = test.db.role("loan_service_user");
    test.sql(&format!(
        "CREATE SCHEMA loan_service;
         CREATE TABLE loan_service.open (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), body text,
             created_at timestamptz NOT NULL DEFAULT now());
         CREATE TABLE loan_service.closed (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), body text,
             created_at timestamptz NOT NULL DEFAULT now());
         INSERT INTO loan_service.open (body) VALUES ('reached');
         INSERT INTO loan_service.closed (body) VALUES ('its owner alone reads this');
         CREATE ROLE {role} LOGIN PASSWORD 'pw-1';
         GRANT USAGE, CREATE ON SCHEMA loan_service TO {role};
         GRANT SELECT, INSERT, UPDATE, DELETE ON loan_service.open TO {role};
         REVOKE ALL ON loan_service.closed FROM {role}, PUBLIC;"
    ))
    .await;
    let (status, deployed) = test.deploy(0, true).await;
    assert_eq!(status, 200, "{deployed}");

    let bound = test
        .db_role(
            Method::PUT,
            "",
            Some(json!({"role": role, "password": "pw-1"})),
        )
        .await;
    for statement in bound["report"]["remediation"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        assert!(
            !statement.contains("closed"),
            "nothing is asked of a private table: {statement}"
        );
        test.sql(statement).await;
    }
    let verified = test.db_role(Method::POST, "/verify", None).await;
    assert_eq!(verified["report"]["verified"], true, "{verified}");
    let (status, enforced) = test
        .platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/policies", test.tenant.slug),
            &test.admin,
            Some(&json!({"policies": {"db_roles": {"mode": "enforce"}}})),
        )
        .await;
    assert_eq!(status, 200, "{enforced}");

    let (status, deployed) = test.deploy(1, true).await;
    assert_eq!(status, 200, "{deployed}");
    let binding = test.db_role(Method::GET, "", None).await;
    assert!(
        binding["binding"]["verified_at"].is_string(),
        "the deploy left the binding verified: {binding}"
    );
    let (status, reached) = test
        .query("SELECT body, current_user::text AS who FROM open")
        .await;
    assert_eq!(status, 200, "{reached}");
    assert_eq!(
        reached["rows"],
        json!([{"body": "reached", "who": role}]),
        "{reached}"
    );
    let (_, refused) = test.query("SELECT body FROM closed").await;
    assert!(
        refused
            .to_string()
            .contains("permission denied for table closed"),
        "the role holds nothing on it, and nothing ran as the platform: {refused}"
    );

    let (status, deployed) = test.deploy(2, false).await;
    assert_eq!(status, 200, "{deployed}");
    let served = test.db_role(Method::POST, "/verify", None).await;
    assert_eq!(failed(&served["report"]), ["table"], "{served}");
    assert_eq!(
        served["binding"]["verified_at"],
        Value::Null,
        "the same table, served, unverifies the binding as it always did: {served}"
    );
}
