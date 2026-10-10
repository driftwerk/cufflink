use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::tenant_db::TenantPoolManager;
use reqwest::Method;
use serde_json::{json, Value};

const SERVICE: &str = "stock-service";
const OTHER: &str = "other-service";
const PG16: i32 = 160000;

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn manifest(name: &str, tables: &[&str]) -> Value {
    let tables: Vec<Value> = tables
        .iter()
        .map(|table| {
            json!({
                "name": table,
                "columns": [
                    {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                    {"name": "body", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}
                ]
            })
        })
        .collect();
    json!({
        "name": name,
        "version": format!("0.{}.0", tables.len()),
        "mode": "crud",
        "tables": tables,
        "publishes": [],
    })
}

async fn connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

struct Unprivileged {
    platform: PlatformProcess,
    db: TestDb,
    tenant: Tenant,
    admin: String,
    as_platform: tokio_postgres::Client,
    as_operator: tokio_postgres::Client,
    platform_role: String,
    server_version_num: i32,
}

impl Unprivileged {
    async fn start(admin_url: &str, nats_url: Option<&str>) -> Self {
        let mut db = TestDb::create_unprivileged(admin_url).await;
        let kek = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [9u8; 32]);
        let platform = PlatformProcess::start(
            &db.url,
            nats_url,
            &[
                ("CUFFLINK_SECRETS_KEK", &kek),
                ("CUFFLINK_SECRETS_KEK_ID", "test"),
            ],
        )
        .await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let tenant_db = TenantPoolManager::managed_db_name(&tenant.slug);
        let as_platform = connect(&common::infra::with_dbname(&db.url, &tenant_db)).await;
        let as_operator = connect(&common::infra::with_dbname(db.admin_url(), &tenant_db)).await;
        let head = as_platform
            .query_one(
                "SELECT current_user::text, current_setting('server_version_num')::int,
                        (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)",
                &[],
            )
            .await
            .expect("who the platform is");
        assert!(!head.get::<_, bool>(2), "the platform is no superuser");
        Self {
            platform,
            db,
            tenant,
            admin,
            as_platform,
            as_operator,
            platform_role: head.get(0),
            server_version_num: head.get(1),
        }
    }

    async fn operator(&self, sql: &str) {
        self.as_operator.batch_execute(sql).await.expect(sql);
    }

    async fn one<T: for<'a> tokio_postgres::types::FromSql<'a>>(&self, sql: &str) -> T {
        self.as_operator
            .query_one(sql, &[])
            .await
            .expect(sql)
            .get(0)
    }

    async fn policy(&self, mode: &str, config: Value) {
        let (status, body) = self
            .platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &self.admin,
                Some(&json!({"policies": {"db_roles": {"mode": mode, "config": config}}})),
            )
            .await;
        assert_eq!(status, 200, "db_roles {mode}: {body}");
    }

    async fn deploy(&self, name: &str, tables: &[&str]) {
        let (status, body) = self
            .platform
            .deploy(&self.tenant, &manifest(name, tables))
            .await;
        assert_eq!(status, 200, "deploy of {name}: {body}");
    }

    async fn db_role(&self, method: Method, step: &str, body: Option<Value>) -> (u16, Value) {
        self.platform
            .send(
                method,
                &format!(
                    "/api/tenants/{}/services/{SERVICE}/db-role{step}",
                    self.tenant.slug
                ),
                &self.admin,
                body.as_ref(),
            )
            .await
    }

    async fn binding(&self) -> Value {
        let (status, body) = self.db_role(Method::GET, "", None).await;
        assert_eq!(status, 200, "{body}");
        body["binding"].clone()
    }

    async fn crud(&self, method: Method, path: &str, body: Option<Value>) -> (u16, Value) {
        self.platform
            .send(
                method,
                &format!("/svc/{}/{SERVICE}{path}", self.tenant.slug),
                &format!("ApiKey {}", self.tenant.api_key),
                body.as_ref(),
            )
            .await
    }

    async fn round_trip(&self, body: &str) {
        let (status, created) = self
            .crud(Method::POST, "/items", Some(json!({ "body": body })))
            .await;
        assert_eq!(status, 200, "{created}");
        let id = created["id"].as_str().unwrap_or_default();
        let (status, read) = self.crud(Method::GET, &format!("/items/{id}"), None).await;
        assert_eq!(status, 200, "{read}");
        assert_eq!(read["body"], body);
    }

    async fn owner_of(&self, relation: &str) -> String {
        self.one(&format!(
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = '{relation}'::regclass"
        ))
        .await
    }

    async fn logged_in_as(&self, role: &str) -> i64 {
        self.one(&format!(
            "SELECT count(*) FROM pg_stat_activity WHERE usename = '{role}' AND datname = current_database()"
        ))
        .await
    }
}

async fn tier(admin_url: &str, nats_url: Option<&str>) {
    let test = Unprivileged::start(admin_url, nats_url).await;
    let (reader, read_only) = (test.db.role("reporting_owner"), test.db.role("reports_ro"));
    test.operator(&format!(
        "CREATE ROLE {reader} NOLOGIN; CREATE ROLE {read_only} NOLOGIN;
         GRANT {read_only} TO \"{}\"; GRANT CREATE ON SCHEMA public TO PUBLIC",
        test.platform_role
    ))
    .await;
    test.deploy(SERVICE, &["items"]).await;
    test.deploy(OTHER, &["items"]).await;
    let role = test.db.managed_role(&test.tenant, SERVICE).await;
    let taken = test.db.managed_role(&test.tenant, OTHER).await;
    test.operator(&format!("CREATE ROLE {taken} LOGIN PASSWORD 'operator'"))
        .await;
    test.as_platform
        .batch_execute(
            "CREATE TABLE stock_service.counted (
                 id BIGSERIAL PRIMARY KEY,
                 n BIGINT GENERATED ALWAYS AS IDENTITY)",
        )
        .await
        .expect("a table of the platform's with sequences its columns own");

    test.policy("warn", json!({ "readers": [reader] })).await;
    test.deploy(SERVICE, &["items", "tags"]).await;
    test.deploy(OTHER, &["items", "tags"]).await;
    let binding = test.binding().await;
    assert_eq!(binding["role"], role.as_str(), "{binding}");
    assert_eq!(
        binding["verify_report"]["verified"], true,
        "a role the platform made without being a superuser is verified: {binding}"
    );
    for relation in [
        "stock_service.items",
        "stock_service.tags",
        "stock_service.counted",
        "stock_service.counted_id_seq",
    ] {
        assert_eq!(test.owner_of(relation).await, role, "{relation}");
    }
    assert!(
        !test
            .one::<bool>("SELECT has_schema_privilege('public', 'public', 'CREATE')")
            .await,
        "the platform closes public in the database it made"
    );
    assert!(
        test.one::<bool>(&format!(
            "SELECT has_table_privilege('{reader}', 'stock_service.tags', 'SELECT')"
        ))
        .await
    );
    assert_eq!(
        test.platform
            .metric(
                "cufflink_db_role_provision_failures_total",
                &[("tenant", &test.tenant.slug), ("service", OTHER)]
            )
            .await,
        Some(2.0),
        "a role of the name the operator made is refused, not taken"
    );
    assert!(
        test.one::<bool>(&format!(
            "SELECT rolpassword IS NOT NULL AND shobj_description(oid, 'pg_authid') IS NULL
             FROM pg_authid WHERE rolname = '{taken}'"
        ))
        .await
    );
    test.round_trip("under warn").await;
    test.operator(&format!("DROP ROLE {taken}")).await;
    test.deploy(OTHER, &["items", "tags", "notes"]).await;
    assert!(
        test.one::<bool>(&format!(
            "SELECT shobj_description(oid, 'pg_authid') LIKE 'cufflink:%' FROM pg_roles WHERE rolname = '{taken}'"
        ))
        .await,
        "once the name is free the platform makes the role"
    );

    let (status, body) = test
        .db_role(
            Method::PUT,
            "",
            Some(json!({ "read_only_role": read_only })),
        )
        .await;
    match test.server_version_num >= PG16 {
        true => {
            assert_eq!(status, 422, "{body}");
            assert!(
                body["error"].as_str().is_some_and(|e| e.contains(&format!(
                    "GRANT {read_only} TO {} WITH ADMIN OPTION",
                    test.platform_role
                ))),
                "{body}"
            );
            test.operator(&format!(
                "GRANT {read_only} TO \"{}\" WITH ADMIN OPTION",
                test.platform_role
            ))
            .await;
            let (status, body) = test
                .db_role(
                    Method::PUT,
                    "",
                    Some(json!({ "read_only_role": read_only })),
                )
                .await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(body["report"]["verified"], true, "{body}");
        }
        false => {
            assert_eq!(status, 200, "CREATEROLE grants any role before 16: {body}");
            assert_eq!(body["report"]["verified"], true, "{body}");
        }
    }

    let (status, body) = test.db_role(Method::POST, "/verify", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["report"]["verified"], true, "{body}");
    assert!(
        test.one::<bool>(&format!(
            "SELECT NOT has_function_privilege('{role}',
                 '_cufflink.outbox_enqueue_many(text[], text[], text[], bytea[], jsonb[], uuid[])', 'EXECUTE')
             AND has_function_privilege('{role}',
                 '_cufflink.outbox_enqueue_own(text[], text[], bytea[], jsonb[], uuid[])', 'EXECUTE')"
        ))
        .await,
        "the role enqueues for its own service alone"
    );

    test.policy("enforce", json!({ "readers": [reader] })).await;
    test.round_trip("under enforce").await;
    assert!(
        test.logged_in_as(&role).await >= 1,
        "the service's statements run on a login as its role"
    );
    assert!(
        test.one::<bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole,
                                  unnest(s.setconfig) AS setting
                            WHERE r.rolname = '{role}' AND starts_with(setting, 'statement_timeout='))"
        ))
        .await,
        "the role it made is given the platform's statement timeout"
    );
    let (status, body) = test.db_role(Method::POST, "/rotate", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["report"]["verified"], true, "{body}");
    test.round_trip("after a rotation").await;
}

#[tokio::test]
async fn db_roles_unprivileged_on_the_test_server() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let _one = ONE_AT_A_TIME.lock().await;
    tier(&infra.database_url, infra.nats_url()).await;
}

async fn pinned(version: &'static str) {
    let Some(infra) = common::infra().await else {
        return;
    };
    let _one = ONE_AT_A_TIME.lock().await;
    if let Some(url) = common::postgres_at(version).await {
        tier(&url, infra.nats_url()).await;
    }
}

#[tokio::test]
async fn db_roles_unprivileged_on_postgres_17() {
    pinned("17.6.0").await;
}

#[tokio::test]
async fn db_roles_unprivileged_on_postgres_18() {
    pinned("18.6.0").await;
}
