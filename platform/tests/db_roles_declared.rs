use cufflink_testkit as common;

use base64::Engine;
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::db_roles::login_pools::{login_config, probe};
use cufflink_platform::db_roles::verify::Probe;
use cufflink_platform::repo::services::{self, Status};
use cufflink_platform::secrets::SecretString;
use cufflink_platform::tenant_db::TenantPoolManager;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

const SERVICE: &str = "catalog-service";
const SCHEMA: &str = "catalog_service";
const PASSWORD: &str = "pw-declared-1";

fn manifest() -> Value {
    manifest_named(SERVICE)
}

fn manifest_named(name: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{
            "name": "items",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "body", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}
            ]
        }],
        "publishes": [],
    })
}

struct Declared {
    platform: PlatformProcess,
    db: TestDb,
    tenant: Tenant,
    admin: String,
    tenant_db: tokio_postgres::Client,
    role: String,
}

async fn platform(db: &TestDb, extra: &[(&str, &str)]) -> PlatformProcess {
    let kek = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
    let env: Vec<(&str, &str)> = [
        ("CUFFLINK_SECRETS_KEK", kek.as_str()),
        ("CUFFLINK_SECRETS_KEK_ID", "test"),
    ]
    .into_iter()
    .chain(extra.iter().copied())
    .collect();
    let infra = common::infra().await.expect("infra");
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
    platform.wait_ready().await;
    platform
}

impl Declared {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let platform = platform(&db, &[]).await;
        let tenant = db.mint_tenant().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let url =
            common::infra::with_dbname(&db.url, &TenantPoolManager::managed_db_name(&tenant.slug));
        let (tenant_db, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("tenant database");
        tokio::spawn(connection);
        let role = db.role("catalog_service_user");
        let test = Self {
            platform,
            db,
            tenant,
            admin,
            tenant_db,
            role,
        };
        test.make_role(&test.role, Some(SCHEMA)).await;
        Some(test)
    }

    async fn sql(&self, sql: &str) {
        self.tenant_db.batch_execute(sql).await.expect(sql);
    }

    async fn make_role(&self, role: &str, schema: Option<&str>) {
        self.sql(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}';
             GRANT CONNECT ON DATABASE {} TO {role};
             GRANT {role} TO CURRENT_USER;",
            TenantPoolManager::managed_db_name(&self.tenant.slug)
        ))
        .await;
        if let Some(schema) = schema {
            self.sql(&format!("CREATE SCHEMA {schema} AUTHORIZATION {role}"))
                .await;
        }
    }

    async fn policy(&self, mode: &str, declared: &[&str]) -> (u16, Value) {
        self.policy_of(&self.tenant, mode, declared).await
    }

    async fn policy_of(&self, tenant: &Tenant, mode: &str, declared: &[&str]) -> (u16, Value) {
        self.platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", tenant.slug),
                &self.admin,
                Some(&json!({"policies": {"db_roles": {
                    "mode": mode,
                    "config": {"declared_roles": declared},
                }}})),
            )
            .await
    }

    async fn allow(&self) {
        let (status, body) = self.policy("enforce", &[&self.role]).await;
        assert_eq!(status, 200, "policy: {body}");
    }

    async fn deploy(&self, query: &str, declared: Option<(&str, &str)>) -> (u16, Value) {
        self.deploy_manifest(query, &manifest(), declared).await
    }

    async fn deploy_manifest(
        &self,
        query: &str,
        manifest: &Value,
        declared: Option<(&str, &str)>,
    ) -> (u16, Value) {
        self.deploy_as(&self.tenant, query, manifest, declared)
            .await
    }

    async fn deploy_as(
        &self,
        tenant: &Tenant,
        query: &str,
        manifest: &Value,
        declared: Option<(&str, &str)>,
    ) -> (u16, Value) {
        let mut body = json!({
            "manifest": manifest,
            "wasm_base64": base64::engine::general_purpose::STANDARD.encode(common::guest::module_bytes()),
            "api_version": 1,
        });
        if let Some((role, password)) = declared {
            body["db_role"] = json!({"role": role, "password": password});
        }
        self.platform
            .send(
                Method::POST,
                &format!("/api/services/deploy{query}"),
                &format!("ApiKey {}", tenant.api_key),
                Some(&body),
            )
            .await
    }

    async fn declare(&self) -> (u16, Value) {
        self.deploy("", Some((&self.role, PASSWORD))).await
    }

    async fn show(&self) -> Value {
        let (status, body) = self
            .platform
            .send(
                Method::GET,
                &format!(
                    "/api/tenants/{}/services/{SERVICE}/db-role",
                    self.tenant.slug
                ),
                &self.admin,
                None,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn audited(&self, action: &str) -> i64 {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT count(*) FROM audit_log WHERE action = $1 AND tenant_id = $2",
                &[&action, &self.tenant.id],
            )
            .await
            .expect("audit")
            .get(0)
    }

    async fn bindings(&self) -> i64 {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT count(*) FROM service_db_roles r JOIN services s ON s.id = r.service_id
                 WHERE s.tenant_id = $1",
                &[&self.tenant.id],
            )
            .await
            .expect("bindings")
            .get(0)
    }

    async fn holds_cufflink(&self, role: &str) -> bool {
        self.tenant_db
            .query_one(
                "SELECT COALESCE(has_schema_privilege($1, '_cufflink', 'USAGE'), false)
                        OR EXISTS (SELECT 1 FROM _cufflink.service_roles WHERE role = $1)",
                &[&role],
            )
            .await
            .expect("grants")
            .get(0)
    }

    fn database(&self) -> String {
        TenantPoolManager::managed_db_name(&self.tenant.slug)
    }

    async fn set(&self, body: Value) -> (u16, Value) {
        self.platform
            .send(
                Method::PUT,
                &format!(
                    "/api/tenants/{}/services/{SERVICE}/db-role",
                    self.tenant.slug
                ),
                &self.admin,
                Some(&body),
            )
            .await
    }

    async fn verify(&self) -> Value {
        let (status, body) = self
            .platform
            .send(
                Method::POST,
                &format!(
                    "/api/tenants/{}/services/{SERVICE}/db-role/verify",
                    self.tenant.slug
                ),
                &self.admin,
                None,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn service_id(&self, tenant: &Tenant, name: &str) -> Uuid {
        services::find(&self.db.pool(), tenant.id, name)
            .await
            .expect("services")
            .expect("the service")
            .id
    }

    async fn logs_in(&self, role: &str) -> bool {
        self.tenant_db
            .query_one(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = $1",
                &[&role],
            )
            .await
            .expect("role")
            .get(0)
    }

    async fn tenant_client(&self, tenant: &Tenant) -> tokio_postgres::Client {
        let url = common::infra::with_dbname(
            &self.db.url,
            &TenantPoolManager::managed_db_name(&tenant.slug),
        );
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("tenant database");
        tokio::spawn(connection);
        client
    }

    async fn nothing_holds_the_password(&self) {
        let found: i64 = self
            .db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT (SELECT count(*) FROM audit_log WHERE details::text LIKE '%' || $1 || '%')
                      + (SELECT count(*) FROM service_db_roles
                         WHERE verify_report::text LIKE '%' || $1 || '%')
                      + (SELECT count(*) FROM deploy_jobs
                         WHERE concat(request::text, result::text, error) LIKE '%' || $1 || '%')",
                &[&PASSWORD],
            )
            .await
            .expect("rows")
            .get(0);
        assert_eq!(found, 0, "the password is stored nowhere in the clear");
    }
}

fn reasons(body: &Value) -> String {
    body.to_string()
}

fn failed(report: &Value) -> Vec<String> {
    report["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .filter(|check| check["ok"] == false)
        .map(|check| check["check"].as_str().expect("name").to_string())
        .collect()
}

fn excess(report: &Value) -> Vec<String> {
    report["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .filter(|check| check["excess"] == true)
        .map(|check| check["check"].as_str().expect("name").to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declared_role_is_bound_by_the_first_deploy_under_enforce() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;

    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "first deploy: {body}");
    assert_eq!(
        body["db_role"],
        json!({"role": test.role, "origin": "declared", "verified": true}),
        "{body}"
    );
    assert!(!body.to_string().contains(PASSWORD));
    let shown = test.show().await;
    assert_eq!(shown["binding"]["origin"], "declared", "{shown}");
    assert_eq!(shown["binding"]["managed"], false);
    let owner: String = test
        .tenant_db
        .query_one(
            "SELECT pg_get_userbyid(relowner)::text FROM pg_class
             WHERE oid = 'catalog_service.items'::regclass",
            &[],
        )
        .await
        .expect("table")
        .get(0);
    assert_eq!(owner, test.role, "the table is made as the declared role");
    let managed: i64 = test
        .tenant_db
        .query_one(
            "SELECT count(*) FROM pg_roles WHERE rolname LIKE 'svc\\_%' AND rolname LIKE $1",
            &[&format!("%{}%", test.tenant.slug.replace('-', "_"))],
        )
        .await
        .expect("roles")
        .get(0);
    assert_eq!(managed, 0, "no role is made for the service");
    assert_eq!(test.audited("db_role.declare").await, 1);

    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["skipped"], true, "{body}");
    assert_eq!(
        test.audited("db_role.declare").await,
        1,
        "the same role and password are taken as they are"
    );

    let mut changed = manifest();
    changed["tables"][0]["columns"]
        .as_array_mut()
        .expect("columns")
        .push(json!({"name": "note", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}));
    let (status, body) = test
        .deploy_manifest("", &changed, Some((&test.role, PASSWORD)))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(test.show().await["binding"]["origin"], "declared");
    test.nothing_holds_the_password().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_async_deploy_takes_the_declaration_and_keeps_its_password_out_of_the_job() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;

    let (status, accepted) = test
        .deploy("?async=true", Some((&test.role, PASSWORD)))
        .await;
    assert_eq!(status, 202, "{accepted}");
    let id = accepted["deploy_id"]
        .as_str()
        .expect("deploy_id")
        .to_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let job = loop {
        let (status, job) = test
            .platform
            .send(
                Method::GET,
                &format!("/api/deploys/{id}"),
                &format!("ApiKey {}", test.tenant.api_key),
                None,
            )
            .await;
        assert_eq!(status, 200, "{job}");
        if !matches!(job["status"].as_str(), Some("queued" | "running")) {
            break job;
        }
        assert!(std::time::Instant::now() < deadline, "the job ends: {job}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert_eq!(job["status"], "succeeded", "{job}");
    assert_eq!(
        job["request"]["db_role"],
        json!({"role": test.role}),
        "the job records the role alone: {job}"
    );
    assert!(!job.to_string().contains(PASSWORD), "{job}");
    let shown = test.show().await;
    assert_eq!(shown["binding"]["origin"], "declared", "{shown}");
    assert!(shown["binding"]["verified_at"].is_string(), "{shown}");
    test.nothing_holds_the_password().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_password_is_taken_by_a_deploy_that_changes_nothing_else() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");

    test.sql(&format!(
        "ALTER ROLE {} PASSWORD 'pw-declared-2'",
        test.role
    ))
    .await;
    let (status, body) = test.deploy("", Some((&test.role, "pw-declared-2"))).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["skipped"], true, "{body}");
    assert_eq!(body["db_role"]["verified"], true, "{body}");
    assert_eq!(test.audited("db_role.declare").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declaration_that_fails_a_gate_writes_and_grants_nothing() {
    let Some(test) = Declared::start().await else {
        return;
    };
    let (status, body) = test.policy("enforce", &[]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.declare().await;
    assert_eq!(status, 422, "not allowed: {body}");
    assert!(reasons(&body).contains("declared_roles"), "{body}");

    test.allow().await;
    let (status, body) = test.deploy("", Some((&test.role, "pw-wrong"))).await;
    assert_eq!(status, 422, "a wrong password: {body}");
    let wrong_password = reasons(&body);
    assert!(
        wrong_password.contains("does not log in to the tenant's database with the password given"),
        "{body}"
    );
    assert!(!wrong_password.contains("pw-wrong"));

    let (status, body) = test
        .deploy("?plan=true", Some((&test.role, "pw-wrong")))
        .await;
    assert_eq!(status, 422, "a plan proves the password too: {body}");
    assert_eq!(test.audited("db_role.declare_refused").await, 3);

    test.sql(&format!("ALTER ROLE {} NOLOGIN", test.role)).await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        reasons(&body),
        wrong_password,
        "a NOLOGIN role answers as a wrong password does: {body}"
    );
    test.sql(&format!("ALTER ROLE {} LOGIN", test.role)).await;

    test.sql(&format!("CREATE FUNCTION public.reach_{0}() RETURNS int LANGUAGE sql AS 'SELECT 1'; ALTER FUNCTION public.reach_{0}() OWNER TO {0}", test.role))
        .await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 422, "a role that reaches beyond its schema: {body}");
    assert!(
        reasons(&body).contains("reaches function public.reach_"),
        "{body}"
    );
    test.sql(&format!("DROP FUNCTION public.reach_{}()", test.role))
        .await;

    test.sql(&format!("GRANT pg_read_all_data TO {}", test.role))
        .await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 422, "{body}");
    assert!(reasons(&body).contains("pg_read_all_data"), "{body}");
    test.sql(&format!("REVOKE pg_read_all_data FROM {}", test.role))
        .await;

    assert_eq!(test.bindings().await, 0, "nothing was bound");
    assert!(
        !test.holds_cufflink(&test.role).await,
        "nothing was granted"
    );
    test.nothing_holds_the_password().await;

    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "once mended it is taken: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_role_bound_in_another_tenant_is_refused_without_naming_it() {
    let Some(mut test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    let orchard = test.db.mint_tenant().await;
    let orchard_db = TenantPoolManager::managed_db_name(&orchard.slug);
    test.sql(&format!(
        "GRANT CONNECT ON DATABASE {orchard_db} TO {}",
        test.role
    ))
    .await;
    let (status, body) = test.policy_of(&orchard, "warn", &[&test.role]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test
        .deploy_as(&orchard, "", &manifest(), Some((&test.role, PASSWORD)))
        .await;
    assert_eq!(status, 422, "{body}");
    let told = reasons(&body);
    assert!(told.contains("bound to another service"), "{told}");
    for hidden in [
        test.tenant.slug.clone(),
        TenantPoolManager::managed_db_name(&test.tenant.slug),
    ] {
        assert!(!told.contains(&hidden), "{hidden} in {told}");
    }
    let detail: String = test
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT details::text FROM audit_log
             WHERE action = 'db_role.declare_refused' AND tenant_id = $1",
            &[&orchard.id],
        )
        .await
        .expect("audited")
        .get(0);
    assert!(
        detail.contains(&test.tenant.slug),
        "the operator is told: {detail}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_plan_reports_what_a_deploy_would_take_and_writes_nothing() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let (status, body) = test
        .deploy("?plan=true", Some((&test.role, PASSWORD)))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["db_role"]["taking"], "bind", "{body}");
    assert_eq!(test.bindings().await, 0);
    assert!(!test.holds_cufflink(&test.role).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn under_enforce_a_first_version_needs_a_verified_role() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let (status, body) = test.deploy("", None).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        reasons(&body).contains("db_role"),
        "the refusal names the declaration: {body}"
    );
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "a refused first deploy is run again: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_administrators_binding_takes_precedence() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    let path = format!(
        "/api/tenants/{}/services/{SERVICE}/db-role",
        test.tenant.slug
    );
    let (status, body) = test
        .platform
        .send(
            Method::PUT,
            &path,
            &test.admin,
            Some(&json!({"role": test.role, "password": PASSWORD})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(test.show().await["binding"]["origin"], "admin");
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "the same role and password: {body}");
    assert_eq!(test.show().await["binding"]["origin"], "admin");

    let other = test.db.role("catalog_other_user");
    test.make_role(&other, None).await;
    let (status, body) = test.policy("enforce", &[&test.role, &other]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.deploy("", Some((&other, PASSWORD))).await;
    assert_eq!(status, 409, "{body}");
    assert!(
        reasons(&body).contains("bound by an administrator"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_declared_role_loses_what_the_platform_granted_it() {
    let Some(test) = Declared::start().await else {
        return;
    };
    let next = test.db.role("catalog_next_user");
    test.make_role(&next, None).await;
    let (status, body) = test.policy("warn", &[&test.role, &next]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    assert!(
        test.holds_cufflink(&test.role).await,
        "the bound role enqueues"
    );

    test.sql(&format!("REASSIGN OWNED BY {} TO {next}", test.role))
        .await;
    let (status, body) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        test.deploy("", Some((&next, PASSWORD))),
    )
    .await
    .expect("the replaced role is retired under the lock already held");
    assert_eq!(status, 200, "{body}");
    assert_eq!(test.show().await["binding"]["role"], next.as_str());
    assert!(test.holds_cufflink(&next).await, "the new role enqueues");
    assert!(
        !test.holds_cufflink(&test.role).await,
        "the replaced role holds nothing of _cufflink"
    );
    let logs_in: bool = test
        .tenant_db
        .query_one(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = $1",
            &[&test.role],
        )
        .await
        .expect("role")
        .get(0);
    assert!(logs_in, "a role the platform did not make keeps its login");
}

async fn mapped(test: &Declared, role: &str) -> bool {
    test.tenant_db
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM _cufflink.service_roles WHERE role = $1 AND service = $2)",
            &[&role, &SERVICE],
        )
        .await
        .expect("mappings")
        .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declaration_refused_after_its_grants_takes_them_back_and_the_bound_role_keeps_its_own() {
    let Some(mut test) = Declared::start().await else {
        return;
    };
    let next = test.db.role("catalog_next_user");
    test.make_role(&next, None).await;
    let (status, body) = test.policy("warn", &[&test.role, &next]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    assert!(mapped(&test, &test.role).await);
    test.sql(&format!(
        "REASSIGN OWNED BY {0} TO {next};
         GRANT USAGE ON SCHEMA {SCHEMA} TO {0};
         GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {SCHEMA} TO {0};",
        test.role
    ))
    .await;
    let (status, body) = test.policy("enforce", &[&test.role, &next]).await;
    assert_eq!(status, 200, "{body}");

    test.platform = platform(
        &test.db,
        &[("CUFFLINK_TEST_REVOKE_AFTER_DECLARED_GRANTS", "1")],
    )
    .await;
    let (status, body) = test.deploy("", Some((&next, "pw-wrong"))).await;
    assert_eq!(status, 422, "a wrong password: {body}");
    assert!(!test.holds_cufflink(&next).await);
    assert!(
        mapped(&test, &test.role).await,
        "the bound role keeps its mapping"
    );

    let (status, body) = test.deploy("", Some((&next, PASSWORD))).await;
    assert_eq!(status, 503, "the role changed while it was bound: {body}");
    assert!(
        !test.holds_cufflink(&next).await,
        "what was granted to the refused role is taken back"
    );
    assert!(
        mapped(&test, &test.role).await,
        "the bound role keeps its mapping"
    );
    assert_eq!(test.show().await["binding"]["role"], test.role.as_str());
    let (status, body) = test
        .platform
        .send(
            Method::POST,
            &format!("/svc/{}/{SERVICE}/items", test.tenant.slug),
            &format!("ApiKey {}", test.tenant.api_key),
            Some(&json!({"body": "still served"})),
        )
        .await;
    assert!(
        status < 300,
        "the bound role still serves and enqueues: {status} {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_login_probe_answers_by_what_the_server_says() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let (admin, connection) = tokio_postgres::connect(&db.url, tokio_postgres::NoTls)
        .await
        .expect("admin");
    tokio::spawn(connection);
    let [proven, unconnected, nologin, limited] = [
        "probe_proven",
        "probe_unconnected",
        "probe_nologin",
        "probe_limited",
    ]
    .map(|name| db.role(name));
    admin
        .batch_execute(&format!(
            "CREATE ROLE {proven} LOGIN PASSWORD '{PASSWORD}';
             CREATE ROLE {unconnected} LOGIN PASSWORD '{PASSWORD}';
             CREATE ROLE {nologin} NOLOGIN PASSWORD '{PASSWORD}';
             CREATE ROLE {limited} LOGIN PASSWORD '{PASSWORD}' CONNECTION LIMIT 0;
             REVOKE CONNECT ON DATABASE \"{0}\" FROM PUBLIC;
             GRANT CONNECT ON DATABASE \"{0}\" TO {proven}, {nologin}, {limited};",
            db.name
        ))
        .await
        .expect("roles");
    let base: tokio_postgres::Config = db.url.parse().expect("config");
    let settings = cufflink_db::PoolSettings::from_vars(1, |_| None).expect("defaults");
    let probed = |config: &tokio_postgres::Config, role: &str, password: &str| {
        probe(
            login_config(config, role, &SecretString::from(password)),
            &settings,
        )
    };
    let missing = {
        let mut config = base.clone();
        config.dbname(format!("cufflink_missing_{}", Uuid::new_v4().simple()));
        config
    };
    let unreachable = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        format!("postgres://127.0.0.1:{port}/cufflink")
            .parse::<tokio_postgres::Config>()
            .expect("config")
    };

    assert_eq!(probed(&base, &proven, PASSWORD).await, Probe::Accepted);
    assert_eq!(
        probed(&base, &unconnected, PASSWORD).await,
        Probe::Accepted,
        "42501 comes after the password was proven"
    );
    for (role, password, why) in [
        (&proven, "pw-wrong", "28P01"),
        (&nologin, PASSWORD, "28000"),
    ] {
        assert!(
            matches!(probed(&base, role, password).await, Probe::Refused(_)),
            "{why} refuses"
        );
    }
    for (config, role, why) in [
        (&base, &limited, "53300"),
        (&missing, &proven, "3D000"),
        (&unreachable, &proven, "a server that cannot be reached"),
    ] {
        assert!(
            matches!(probed(config, role, PASSWORD).await, Probe::Unanswered(_)),
            "{why} proves nothing"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declaration_is_refused_for_what_no_services_role_may_be() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let role = test.role.as_str();
    let (_, wrong_password) = test.deploy("", Some((role, "pw-wrong"))).await;
    for (break_it, mend, refused) in [
        (
            format!(
                "REVOKE CONNECT ON DATABASE {0} FROM PUBLIC; REVOKE CONNECT ON DATABASE {0} FROM {role}",
                test.database()
            ),
            format!("GRANT CONNECT ON DATABASE {} TO {role}", test.database()),
            None,
        ),
        (
            format!("COMMENT ON ROLE {role} IS 'cufflink:{}:{}'", Uuid::nil(), Uuid::nil()),
            format!("COMMENT ON ROLE {role} IS NULL"),
            Some("made by the platform"),
        ),
        (
            format!("ALTER ROLE {role} SUPERUSER"),
            format!("ALTER ROLE {role} NOSUPERUSER"),
            Some("superuser"),
        ),
        (
            format!("ALTER ROLE {role} CREATEROLE"),
            format!("ALTER ROLE {role} NOCREATEROLE"),
            Some("NOCREATEROLE"),
        ),
    ] {
        test.sql(&break_it).await;
        let (status, body) = test.declare().await;
        assert_eq!(status, 422, "{break_it}: {body}");
        match refused {
            Some(reason) => assert!(reasons(&body).contains(reason), "{break_it}: {body}"),
            None => assert_eq!(
                body, wrong_password,
                "no CONNECT answers as a wrong password does"
            ),
        }
        test.sql(&mend).await;
    }

    test.sql(&format!("ALTER ROLE {role} CONNECTION LIMIT 0"))
        .await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 503, "53300 leaves the password open: {body}");
    assert!(reasons(&body).contains("could not be tried"), "{body}");
    assert!(
        !reasons(&body).contains("too many connections"),
        "the server's answer goes to the audit row alone: {body}"
    );
    test.sql(&format!("ALTER ROLE {role} CONNECTION LIMIT -1"))
        .await;

    assert_eq!(test.bindings().await, 0, "nothing was bound");
    assert!(!test.holds_cufflink(role).await, "nothing was granted");
    assert_eq!(test.audited("db_role.declare_refused").await, 6);
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "once mended it is taken: {body}");

    test.sql(&format!(
        "GRANT CREATE ON SCHEMA _cufflink TO {role}; ALTER ROLE {role} PASSWORD 'pw-declared-2'"
    ))
    .await;
    let (status, body) = test.deploy("", Some((role, "pw-declared-2"))).await;
    assert_eq!(status, 422, "CREATE on _cufflink: {body}");
    assert!(reasons(&body).contains("_cufflink"), "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_role_whose_reach_stays_in_its_schema_is_taken() {
    let Some(mut test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let role = test.role.clone();
    let reader = test.db.role("catalog_reader");
    test.sql(&format!(
        "CREATE ROLE {reader} NOLOGIN;
         ALTER DEFAULT PRIVILEGES FOR ROLE {role} GRANT SELECT ON TABLES TO {reader};
         CREATE TABLE {SCHEMA}.ledger (id int);
         ALTER TABLE {SCHEMA}.ledger OWNER TO {role};
         ALTER TABLE {SCHEMA}.ledger ENABLE ROW LEVEL SECURITY;
         CREATE POLICY ledger_own ON {SCHEMA}.ledger TO {role} USING (true);
         CREATE COLLATION {SCHEMA}.plain (locale = 'C');
         ALTER COLLATION {SCHEMA}.plain OWNER TO {role};"
    ))
    .await;
    let orchard = test.db.mint_tenant().await;
    let orchard_db = test.tenant_client(&orchard).await;
    orchard_db
        .batch_execute(&format!(
            "CREATE TABLE public.shared_rates (id int); GRANT SELECT ON public.shared_rates TO {role};"
        ))
        .await
        .expect("a grant in another database");

    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    let report = test.verify().await["report"].clone();
    assert!(failed(&report).is_empty(), "{report}");
    assert!(
        excess(&report).contains(&"granted_elsewhere".to_string()),
        "a grant received in another database is reported: {report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn another_tenants_grant_to_a_declared_role_neither_refuses_nor_unverifies_it() {
    let Some(mut test) = Declared::start().await else {
        return;
    };
    let orchard = test.db.mint_tenant().await;
    let orchard_db = test.tenant_client(&orchard).await;
    let orchards = test.db.role("ledger_service_user");
    orchard_db
        .batch_execute(&format!(
            "CREATE ROLE {orchards} LOGIN PASSWORD '{PASSWORD}';
             CREATE SCHEMA ledger_service AUTHORIZATION {orchards};
             SET ROLE {orchards};
             CREATE TABLE ledger_service.entries (id int);
             GRANT SELECT ON ledger_service.entries TO {};
             RESET ROLE;",
            test.role
        ))
        .await
        .expect("orchard's role grants to the role acme declares");
    test.allow().await;
    let (status, body) = test.declare().await;
    assert_eq!(
        status, 200,
        "a grant received is not the role's reach: {body}"
    );
    let report = test.verify().await["report"].clone();
    assert_eq!(report["verified"], true, "{report}");
    assert!(
        excess(&report).contains(&"granted_elsewhere".to_string()),
        "{report}"
    );

    test.sql(&format!(
        "ALTER ROLE {} PASSWORD 'pw-declared-2'",
        test.role
    ))
    .await;
    let (status, body) = test.deploy("", Some((&test.role, "pw-declared-2"))).await;
    assert_eq!(
        status, 200,
        "nor is it refused at the next declaration: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grant_made_after_the_binding_unverifies_it_at_the_next_verify() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(test.verify().await["report"]["verified"], true);

    test.sql(&format!("GRANT pg_read_all_data TO {}", test.role))
        .await;
    let body = test.verify().await;
    assert_eq!(body["report"]["verified"], false, "{body}");
    assert!(
        failed(&body["report"]).contains(&"declared_memberships".to_string()),
        "{body}"
    );
    assert!(body["binding"]["verified_at"].is_null(), "{body}");

    let stray = test.db.role("catalog_stray_user");
    test.sql(&format!(
        "REVOKE pg_read_all_data FROM {0};
         CREATE ROLE {stray} NOLOGIN;
         INSERT INTO _cufflink.service_roles (role, service) VALUES ('{stray}', '{SERVICE}');",
        test.role
    ))
    .await;
    let report = test.verify().await["report"].clone();
    assert_eq!(report["verified"], true, "{report}");
    assert!(
        excess(&report).contains(&"outbox_mapped_elsewhere".to_string()),
        "another role mapped to the service is reported: {report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn under_warn_a_binding_that_is_not_ready_is_written_unverified() {
    let Some(test) = Declared::start().await else {
        return;
    };
    let (status, body) = test.policy("warn", &[&test.role]).await;
    assert_eq!(status, 200, "{body}");
    test.sql("GRANT CREATE ON SCHEMA public TO PUBLIC").await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["db_role"]["verified"], false, "{body}");
    let binding = test.show().await["binding"].clone();
    assert_eq!(binding["origin"], "declared");
    assert!(binding["verified_at"].is_null(), "{binding}");
    assert!(
        failed(&binding["verify_report"]).contains(&"public_schema".to_string()),
        "{binding}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_administrators_binding_is_refreshed_by_a_declaration_and_stays_the_administrators() {
    let Some(test) = Declared::start().await else {
        return;
    };
    let (status, body) = test.policy("warn", &[&test.role]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    let (owner, reader) = (
        test.db.role("catalog_owner"),
        test.db.role("catalog_readers"),
    );
    test.sql(&format!(
        "CREATE ROLE {owner} NOLOGIN; CREATE ROLE {reader} NOLOGIN;
         ALTER SCHEMA {SCHEMA} OWNER TO {owner};"
    ))
    .await;
    let (status, body) = test
        .set(json!({"role": test.role, "password": PASSWORD, "read_only_role": reader}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["binding"]["origin"], "admin", "{body}");

    test.sql(&format!(
        "ALTER ROLE {} PASSWORD 'pw-declared-2'",
        test.role
    ))
    .await;
    let (status, body) = test.deploy("", Some((&test.role, "pw-declared-2"))).await;
    assert_eq!(
        status, 200,
        "a refresh is not judged on what an administrator chose: {body}"
    );
    let binding = test.show().await["binding"].clone();
    assert_eq!(binding["origin"], "admin", "{binding}");
    assert_eq!(binding["read_only_role"], reader.as_str(), "{binding}");
    let report = test.verify().await["report"].clone();
    assert!(
        !failed(&report).contains(&"credentials".to_string()),
        "the refreshed password is the one stored: {report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_db_role_set_of_another_role_retires_the_declared_one() {
    let Some(test) = Declared::start().await else {
        return;
    };
    let (status, body) = test.policy("warn", &[&test.role]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    let chosen = test.db.role("catalog_chosen_user");
    test.make_role(&chosen, None).await;
    test.sql(&format!("REASSIGN OWNED BY {} TO {chosen}", test.role))
        .await;

    let (status, body) = tokio::time::timeout(
        Duration::from_secs(10),
        test.set(json!({"role": chosen, "password": PASSWORD})),
    )
    .await
    .expect("the declared role is retired under the lock already held");
    assert_eq!(status, 200, "{body}");
    let binding = test.show().await["binding"].clone();
    assert_eq!(binding["role"], chosen.as_str(), "{binding}");
    assert_eq!(binding["origin"], "admin", "{binding}");
    assert!(test.holds_cufflink(&chosen).await);
    assert!(
        !test.holds_cufflink(&test.role).await,
        "the declared role holds nothing of _cufflink"
    );
    assert!(test.logs_in(&test.role).await, "and keeps its login");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_db_role_set_and_declaration_leave_one_binding() {
    let Some(test) = Declared::start().await else {
        return;
    };
    let next = test.db.role("catalog_next_user");
    test.make_role(&next, None).await;
    let (status, body) = test.policy("warn", &[&test.role, &next]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    test.sql(&format!("ALTER SCHEMA {SCHEMA} OWNER TO {next}"))
        .await;

    let ((set, set_body), (declared, declared_body)) = tokio::join!(
        test.set(json!({"role": test.role, "password": PASSWORD})),
        test.deploy("", Some((&next, PASSWORD))),
    );
    assert_eq!(set, 200, "{set_body}");
    assert!(
        declared == 200 || declared == 409,
        "the declaration lands first or meets the administrator's binding: {declared} {declared_body}"
    );
    let binding = test.show().await["binding"].clone();
    assert_eq!(binding["role"], test.role.as_str(), "{binding}");
    assert_eq!(binding["origin"], "admin", "{binding}");
    assert!(
        !test.holds_cufflink(&next).await,
        "the declared role holds nothing of _cufflink either way"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_role_bound_by_another_tenant_at_once_loses_the_unique_index_and_holds_nothing() {
    let Some(mut test) = Declared::start().await else {
        return;
    };
    let orchard = test.db.mint_tenant().await;
    let (status, body) = test
        .deploy_as(&orchard, "", &manifest_named("ledger-service"), None)
        .await;
    assert_eq!(status, 200, "{body}");
    let orchards = test.service_id(&orchard, "ledger-service").await;
    test.allow().await;

    let pool = test.db.pool();
    let mut racing = pool.get().await.expect("checkout");
    let race = racing.transaction().await.expect("begin");
    race.execute(
        "INSERT INTO service_db_roles (service_id, role_name, managed, updated_by)
         VALUES ($1, $2, false, 'race')",
        &[&orchards, &test.role],
    )
    .await
    .expect("orchard's binding, not yet committed");
    let watcher = pool.get().await.expect("checkout");
    let ((status, body), ()) = tokio::join!(test.declare(), async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while watcher
            .query_one(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE datname = current_database() AND wait_event = 'transactionid'",
                &[],
            )
            .await
            .expect("activity")
            .get::<_, i64>(0)
            == 0
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the declaration never reached the write"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        race.commit().await.expect("orchard binds first");
    });
    assert_eq!(status, 422, "{body}");
    assert!(
        reasons(&body).contains("bound to another service"),
        "{body}"
    );
    assert!(!reasons(&body).contains(&orchard.slug), "{body}");
    assert_eq!(test.bindings().await, 0);
    assert!(
        !test.holds_cufflink(&test.role).await,
        "what the loser was granted is taken back"
    );
    assert_eq!(
        test.audited("db_role.declare_refused").await,
        1,
        "a lost race is audited"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_declared_service_leaves_its_role_logging_in_and_holding_nothing() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 200, "{body}");
    assert!(test.holds_cufflink(&test.role).await);
    let id = test.service_id(&test.tenant, SERVICE).await;
    let (status, body) = test
        .platform
        .send(
            Method::DELETE,
            &format!("/api/services/{id}"),
            &format!("ApiKey {}", test.tenant.api_key),
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert!(test.logs_in(&test.role).await);
    assert!(!test.holds_cufflink(&test.role).await);
    let owner: String = test
        .tenant_db
        .query_one(
            "SELECT pg_get_userbyid(nspowner)::text FROM pg_namespace WHERE nspname = $1",
            &[&SCHEMA],
        )
        .await
        .expect("schema")
        .get(0);
    assert_eq!(owner, test.role, "and keeps what it owns");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declaration_replaces_a_platform_made_binding_and_leaves_its_role_nologin() {
    let Some(test) = Declared::start().await else {
        return;
    };
    let ledger = test.db.role("ledger_service_user");
    let (status, body) = test.policy("warn", &[&ledger]).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test
        .deploy_manifest("", &manifest_named("ledger-service"), None)
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, made) = test
        .platform
        .send(
            Method::GET,
            &format!(
                "/api/tenants/{}/services/ledger-service/db-role",
                test.tenant.slug
            ),
            &test.admin,
            None,
        )
        .await;
    assert_eq!(status, 200, "{made}");
    assert_eq!(made["binding"]["origin"], "platform", "{made}");
    let made = made["binding"]["role"].as_str().expect("role").to_string();

    test.make_role(&ledger, None).await;
    test.sql(&format!("REASSIGN OWNED BY {made} TO {ledger}"))
        .await;
    let (status, body) = test
        .deploy_manifest(
            "",
            &manifest_named("ledger-service"),
            Some((&ledger, PASSWORD)),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["db_role"]["origin"], "declared", "{body}");
    assert!(
        !test.logs_in(&made).await,
        "the role the platform made logs in no more"
    );
    let audited: Value = test
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT details FROM audit_log WHERE action = 'db_role.declare' AND tenant_id = $1",
            &[&test.tenant.id],
        )
        .await
        .expect("audited")
        .get(0);
    assert_eq!(audited["previous_role"], made.as_str(), "{audited}");
    assert_eq!(audited["previous_origin"], "platform", "{audited}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_that_never_ran_blocks_no_enforce_and_may_first_run_inactive() {
    let Some(test) = Declared::start().await else {
        return;
    };
    test.allow().await;
    let (status, body) = test.deploy("", None).await;
    assert_eq!(status, 422, "{body}");
    for mode in ["warn", "enforce"] {
        let (status, body) = test.policy(mode, &[&test.role]).await;
        assert_eq!(status, 200, "a service that never ran: {mode}: {body}");
    }
    let (status, body) = test
        .deploy("?inactive=true", Some((&test.role, PASSWORD)))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["db_role"]["origin"], "declared", "{body}");
    assert_eq!(body["db_role"]["verified"], true, "{body}");
    let service = services::find(&test.db.pool(), test.tenant.id, SERVICE)
        .await
        .expect("services")
        .expect("the service");
    assert_eq!(service.status, Status::Inactive);
}
