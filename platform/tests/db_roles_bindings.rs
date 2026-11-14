use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::tenant_db::TenantPoolManager;
use reqwest::Method;
use serde_json::{json, Value};

fn manifest() -> Value {
    json!({
        "name": "loan-service",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{
            "name": "loans",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "body", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}
            ]
        }],
        "publishes": [],
    })
}

struct Bindings {
    platform: PlatformProcess,
    db: TestDb,
    tenant: Tenant,
    admin: String,
    tenant_db: tokio_postgres::Client,
}

impl Bindings {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let kek = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [5u8; 32]);
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
        let (status, body) = platform.deploy(&tenant, &manifest()).await;
        assert_eq!(status, 200, "deploy: {body}");
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
        })
    }

    fn path(&self, service: &str) -> String {
        format!(
            "/api/tenants/{}/services/{service}/db-role",
            self.tenant.slug
        )
    }

    async fn set(&self, body: Value) -> (u16, Value) {
        self.set_for("loan-service", body).await
    }

    async fn set_for(&self, service: &str, body: Value) -> (u16, Value) {
        self.platform
            .send(Method::PUT, &self.path(service), &self.admin, Some(&body))
            .await
    }

    async fn verify(&self) -> (u16, Value) {
        self.verify_for("loan-service").await
    }

    async fn verify_for(&self, service: &str) -> (u16, Value) {
        self.platform
            .send(
                Method::POST,
                &format!("{}/verify", self.path(service)),
                &self.admin,
                None,
            )
            .await
    }

    async fn show(&self) -> Value {
        self.show_for("loan-service").await
    }

    async fn show_for(&self, service: &str) -> Value {
        let (status, body) = self
            .platform
            .send(Method::GET, &self.path(service), &self.admin, None)
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn sql(&self, sql: &str) {
        self.tenant_db.batch_execute(sql).await.expect(sql);
    }

    async fn enforce_db_roles(&self) -> (u16, Value) {
        self.platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &self.admin,
                Some(&json!({"policies": {"db_roles": {"mode": "enforce"}}})),
            )
            .await
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

fn remediation(report: &Value) -> Vec<String> {
    report["remediation"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

#[tokio::test]
async fn a_binding_is_verified_by_what_the_role_can_reach_and_mended_by_what_it_reports() {
    let Some(test) = Bindings::start().await else {
        return;
    };
    let pool = test.db.pool();
    let (role, read_only) = (
        test.db.role("loan_service_user"),
        test.db.role("kiosk_report_user"),
    );
    test.sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'pw-1'; CREATE ROLE {read_only} NOLOGIN;"
    ))
    .await;

    assert_eq!(test.show().await["binding"], Value::Null);
    let (status, body) = test.set(json!({"password": "pw-1"})).await;
    assert_eq!(status, 422, "a first binding names its role: {body}");
    let (status, body) = test.set(json!({"role": "Loan-Service"})).await;
    assert_eq!(status, 422, "{body}");
    let (status, body) = test
        .platform
        .send(
            Method::PUT,
            &test.path("no-such-service"),
            &test.admin,
            Some(&json!({"role": role})),
        )
        .await;
    assert_eq!(status, 404, "{body}");

    let (status, body) = test.set(json!({"role": role, "password": "pw-1"})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["binding"]["role"], role);
    assert_eq!(body["binding"]["managed"], false);
    assert_eq!(body["binding"]["login"], true);
    assert_eq!(body["binding"]["verified_at"], Value::Null);
    assert!(
        body.get("password").is_none() && !body.to_string().contains("pw-1"),
        "no response carries the password: {body}"
    );
    let sealed: Option<Vec<u8>> = pool
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT password_enc FROM service_db_roles r JOIN services s ON s.id = r.service_id
             WHERE s.name = 'loan-service'",
            &[],
        )
        .await
        .expect("row")
        .get(0);
    assert!(
        sealed.is_some_and(|sealed| !sealed.windows(4).any(|w| w == b"pw-1")),
        "the password is stored sealed"
    );

    let (status, gate) = test.enforce_db_roles().await;
    assert_eq!(status, 422, "enforce waits for a verified binding: {gate}");
    assert!(gate["error"]
        .as_str()
        .is_some_and(|e| e.contains("loan-service")));

    let (status, body) = test.verify().await;
    assert_eq!(status, 200, "{body}");
    let report = &body["report"];
    assert_eq!(report["verified"], false);
    assert_eq!(
        failed(report),
        ["schema", "table"],
        "the role exists, logs in and connects; the grants are missing: {report}"
    );
    let statements = remediation(report);
    assert_eq!(
        statements,
        [
            format!("GRANT USAGE, CREATE ON SCHEMA \"loan_service\" TO \"{role}\";"),
            format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON \"loan_service\".\"loans\" TO \"{role}\";"
            ),
        ]
    );
    assert_eq!(body["binding"]["verified_at"], Value::Null);
    assert_eq!(
        test.show().await["binding"]["verify_report"]["verified"],
        false
    );

    for statement in &statements {
        test.sql(statement).await;
    }
    let (status, body) = test.verify().await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["report"]["verified"], true, "{body}");
    assert!(body["binding"]["verified_at"].is_string());
    let (status, body) = test.enforce_db_roles().await;
    assert_eq!(status, 200, "{body}");

    let (status, body) = test.set(json!({"read_only_role": read_only})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["binding"]["role"], role, "the role is kept");
    assert_eq!(body["binding"]["login"], true, "and so is the password");
    assert_eq!(
        body["binding"]["verified_at"],
        Value::Null,
        "a changed binding is verified again"
    );
    let (_, body) = test.verify().await;
    assert_eq!(failed(&body["report"]), ["read_only_role"]);
    assert_eq!(
        remediation(&body["report"]),
        [format!(
            "GRANT \"{read_only}\" TO \"{role}\" WITH INHERIT FALSE, SET TRUE;"
        )]
    );
    for statement in remediation(&body["report"]) {
        test.sql(&statement).await;
    }
    let (_, body) = test.verify().await;
    assert_eq!(body["report"]["verified"], true, "{body}");

    let (status, body) = test.set(json!({"password": "pw-wrong"})).await;
    assert_eq!(status, 200, "{body}");
    let (_, body) = test.verify().await;
    assert_eq!(failed(&body["report"]), ["credentials"], "{body}");
    assert!(
        !body.to_string().contains("pw-wrong"),
        "a failed login names no password: {body}"
    );
    let (_, body) = test.set(json!({"password": "pw-1"})).await;
    assert_eq!(
        body["report"]["verified"], true,
        "a binding is checked as it is set: {body}"
    );
    assert!(body["binding"]["verified_at"].is_string());
    let (_, body) = test.verify().await;
    assert_eq!(body["report"]["verified"], true, "{body}");

    let audited: i64 = pool
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM audit_log WHERE action IN ('db_role.set', 'db_role.verify')
             AND tenant_id = $1 AND resource_id = 'loan-service'",
            &[&test.tenant.id],
        )
        .await
        .expect("audit")
        .get(0);
    assert_eq!(
        audited, 14,
        "every set, the verification it runs, and every verification asked for are audited"
    );
}

fn catalog(version: &str, migrations: Value) -> Value {
    json!({
        "name": "catalog-service",
        "version": version,
        "mode": "crud",
        "publishes": [],
        "schema": "adopted",
        "tables": [],
        "private_tables": [{
            "name": "books",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false},
                {"name": "title", "column_type": "text", "primary_key": false, "nullable": true}
            ]
        }],
        "migrations": migrations,
        "migration_history": {"table": "schema_history", "baseline": 1},
    })
}

fn schema_checks(report: &Value) -> Vec<(&str, bool, bool)> {
    report["checks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|check| {
            check["check"]
                .as_str()
                .is_some_and(|name| name.starts_with("schema"))
        })
        .map(|check| {
            (
                check["check"].as_str().unwrap_or_default(),
                check["ok"] == true,
                check["excess"] == true,
            )
        })
        .collect()
}

#[tokio::test]
async fn a_role_the_platform_runs_no_ddl_as_is_verified_on_usage_of_its_adopted_schema() {
    let Some(test) = Bindings::start().await else {
        return;
    };
    let (writer, reader) = (
        test.db.role("loan_service_user"),
        test.db.role("catalog_service_user"),
    );
    test.sql(&format!(
        "CREATE SCHEMA catalog_service;
         CREATE TABLE catalog_service.books (id uuid PRIMARY KEY, title text);
         CREATE ROLE {reader} LOGIN PASSWORD 'pw-2';
         GRANT USAGE ON SCHEMA catalog_service TO {reader};
         GRANT SELECT ON catalog_service.books TO {reader};
         CREATE ROLE {writer} LOGIN PASSWORD 'pw-1';"
    ))
    .await;
    let (status, body) = test
        .platform
        .deploy(&test.tenant, &catalog("0.1.0", json!([])))
        .await;
    assert_eq!(status, 200, "deploy: {body}");

    let (status, body) = test
        .set_for(
            "catalog-service",
            json!({"role": reader, "password": "pw-2"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let report = &body["report"];
    assert_eq!(
        report["verified"], true,
        "USAGE is all a reader needs: {report}"
    );
    assert_eq!(
        schema_checks(report),
        [
            ("schema", true, false),
            ("schema_create_unneeded", true, false)
        ]
    );
    assert!(body["binding"]["verified_at"].is_string());

    test.sql(&format!(
        "GRANT CREATE ON SCHEMA catalog_service TO {reader}"
    ))
    .await;
    let (_, body) = test.verify_for("catalog-service").await;
    let report = &body["report"];
    assert_eq!(report["verified"], true, "{report}");
    assert!(remediation(report).is_empty(), "{report}");
    assert_eq!(
        schema_checks(report),
        [
            ("schema", true, false),
            ("schema_create_unneeded", true, true)
        ]
    );
    let excess = report["checks"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|check| check["check"] == "schema_create_unneeded")
        .expect("the excess is reported");
    assert_eq!(
        excess["remedy"],
        format!("REVOKE CREATE ON SCHEMA \"catalog_service\" FROM \"{reader}\";"),
        "with what takes it back"
    );
    test.sql(&format!(
        "REVOKE CREATE ON SCHEMA catalog_service FROM {reader}"
    ))
    .await;

    let (status, body) = test.set(json!({"role": writer, "password": "pw-1"})).await;
    assert_eq!(status, 200, "{body}");
    for statement in remediation(&body["report"]) {
        test.sql(&statement).await;
    }
    let (_, body) = test.verify().await;
    assert_eq!(body["report"]["verified"], true, "{body}");
    let (status, body) = test.enforce_db_roles().await;
    assert_eq!(status, 200, "{body}");

    let migrating = catalog(
        "0.1.1",
        json!([{"version": 2, "name": "shelves", "sql": "CREATE TABLE catalog_service.shelves (id integer PRIMARY KEY);"}]),
    );
    let (status, body) = test.platform.deploy(&test.tenant, &migrating).await;
    assert_ne!(
        status, 200,
        "a version that migrates as a role that may not create is refused: {body}"
    );
    assert!(
        body.to_string()
            .contains("service catalog-service has no database role"),
        "{body}"
    );
    let shown = test.show_for("catalog-service").await;
    assert_eq!(
        shown["binding"]["verified_at"],
        Value::Null,
        "the binding was judged for the coming version before it ran anything: {shown}"
    );
    let report = &shown["binding"]["verify_report"];
    assert_eq!(failed(report), ["schema"], "{report}");
    assert_eq!(
        remediation(report),
        [format!(
            "GRANT USAGE, CREATE ON SCHEMA \"catalog_service\" TO \"{reader}\";"
        )]
    );
    let shelves: bool = test
        .tenant_db
        .query_one(
            "SELECT to_regclass('catalog_service.shelves') IS NOT NULL",
            &[],
        )
        .await
        .expect("shelves")
        .get(0);
    assert!(!shelves, "nothing was applied");

    let (_, body) = test.verify_for("catalog-service").await;
    assert_eq!(
        body["report"]["verified"], true,
        "the running version still runs no DDL as the role: {body}"
    );

    test.sql(&format!(
        "GRANT CREATE ON SCHEMA catalog_service TO {reader}"
    ))
    .await;
    let (status, body) = test.platform.deploy(&test.tenant, &migrating).await;
    assert_eq!(status, 200, "deploy: {body}");
    let owner: String = test
        .tenant_db
        .query_one(
            "SELECT tableowner::text FROM pg_tables
             WHERE schemaname = 'catalog_service' AND tablename = 'shelves'",
            &[],
        )
        .await
        .expect("shelves")
        .get(0);
    assert_eq!(owner, reader, "the migration ran as the bound role");
    let (_, body) = test.verify_for("catalog-service").await;
    let report = &body["report"];
    assert_eq!(report["verified"], true, "{report}");
    assert_eq!(
        schema_checks(report),
        [("schema", true, false)],
        "a version that migrates needs CREATE, and holding it is no excess"
    );
}

#[tokio::test]
async fn a_role_that_reaches_beyond_its_schema_is_not_one_a_service_is_bound_to() {
    let Some(test) = Bindings::start().await else {
        return;
    };
    let platform_role: String = test
        .tenant_db
        .query_one("SELECT current_user::text", &[])
        .await
        .expect("role")
        .get(0);
    let (boss, wide) = (
        test.db.role("boss_service_user"),
        test.db.role("wide_service_user"),
    );
    test.sql(&format!(
        "CREATE ROLE {boss} LOGIN SUPERUSER PASSWORD 'pw-1';
         CREATE ROLE {wide} LOGIN CREATEROLE BYPASSRLS PASSWORD 'pw-1';
         GRANT pg_read_all_data TO {wide};
         GRANT USAGE, CREATE ON SCHEMA loan_service TO {wide};
         GRANT SELECT, INSERT, UPDATE, DELETE ON loan_service.loans TO {wide};
         GRANT CREATE ON SCHEMA public TO PUBLIC;"
    ))
    .await;
    for (role, why) in [
        (platform_role.as_str(), "the one the platform connects as"),
        (boss.as_str(), "a superuser"),
    ] {
        let (status, body) = test.set(json!({"role": role, "password": "pw-1"})).await;
        assert_eq!(status, 422, "{role}: {body}");
        assert!(
            body["error"].as_str().is_some_and(|e| e.contains(why)),
            "{body}"
        );
    }
    assert_eq!(
        test.show().await["binding"],
        Value::Null,
        "a refused role is not recorded"
    );

    let (status, body) = test.set(json!({"role": wide, "password": "pw-1"})).await;
    assert_eq!(status, 200, "{body}");
    let report = &body["report"];
    assert_eq!(body["binding"]["verified_at"], Value::Null, "{body}");
    assert_eq!(
        failed(report),
        ["role_attributes", "role_memberships", "public_schema"],
        "{report}"
    );
    let statements = remediation(report);
    assert_eq!(
        statements,
        [
            format!("ALTER ROLE \"{wide}\" NOCREATEROLE NOBYPASSRLS;"),
            format!("REVOKE \"pg_read_all_data\" FROM \"{wide}\";"),
            "REVOKE CREATE ON SCHEMA public FROM PUBLIC;".to_string(),
        ]
    );
    let (status, gate) = test.enforce_db_roles().await;
    assert_eq!(status, 422, "{gate}");
    for statement in &statements {
        test.sql(statement).await;
    }
    test.sql(&format!("GRANT CREATE ON SCHEMA public TO {wide}"))
        .await;
    let (_, body) = test.verify().await;
    assert_eq!(failed(&body["report"]), ["public_schema"], "{body}");
    assert_eq!(
        remediation(&body["report"]),
        [format!("REVOKE CREATE ON SCHEMA public FROM \"{wide}\";")]
    );
    test.sql(&format!("REVOKE CREATE ON SCHEMA public FROM {wide}"))
        .await;
    let (_, body) = test.verify().await;
    assert_eq!(body["report"]["verified"], true, "{body}");
    let (status, body) = test.enforce_db_roles().await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn a_login_that_could_not_be_tried_leaves_a_verified_binding_verified() {
    let Some(test) = Bindings::start().await else {
        return;
    };
    let probed = test.db.role("probed_service_user");
    test.sql(&format!(
        "CREATE ROLE {probed} LOGIN PASSWORD 'pw-1';
         GRANT USAGE, CREATE ON SCHEMA loan_service TO {probed};
         GRANT SELECT, INSERT, UPDATE, DELETE ON loan_service.loans TO {probed};"
    ))
    .await;
    let (status, body) = test.set(json!({"role": probed, "password": "pw-1"})).await;
    assert_eq!(status, 200, "{body}");
    let verified_at = body["binding"]["verified_at"].clone();
    assert!(verified_at.is_string(), "{body}");

    test.sql(&format!("ALTER ROLE {probed} CONNECTION LIMIT 0"))
        .await;
    let (status, body) = test.verify().await;
    assert_eq!(status, 200, "{body}");
    let report = &body["report"];
    assert_eq!(report["verified"], false, "{report}");
    assert_eq!(failed(report), ["credentials"]);
    let credentials = report["checks"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|check| check["check"] == "credentials")
        .expect("the check");
    assert_eq!(credentials["inconclusive"], true, "{credentials}");
    assert!(remediation(report).is_empty(), "{report}");
    assert_eq!(
        body["binding"]["verified_at"], verified_at,
        "what could not be asked unverifies nothing: {body}"
    );
    assert_eq!(
        test.platform
            .metric(
                "cufflink_db_role_verify_unanswered_total",
                &[("tenant", &test.tenant.slug), ("service", "loan-service")]
            )
            .await,
        Some(1.0)
    );
    let (status, body) = test.enforce_db_roles().await;
    assert_eq!(status, 200, "the binding still satisfies the gate: {body}");

    test.sql(&format!(
        "ALTER ROLE {probed} CONNECTION LIMIT -1 PASSWORD 'pw-2'"
    ))
    .await;
    let (_, body) = test.verify().await;
    assert_eq!(failed(&body["report"]), ["credentials"], "{body}");
    assert_eq!(
        body["binding"]["verified_at"],
        Value::Null,
        "a password Postgres refuses does: {body}"
    );
    let (status, body) = test
        .platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/policies", test.tenant.slug),
            &test.admin,
            Some(&json!({"policies": {"db_roles": {"mode": "enforce", "config": {"login_pool_max": 8}}}})),
        )
        .await;
    assert_eq!(
        status, 200,
        "a tenant that enforces already changes its config whatever its bindings: {body}"
    );
    let role_password: String = test
        .tenant_db
        .query_one(
            "SELECT rolpassword FROM pg_authid WHERE rolname = $1",
            &[&probed],
        )
        .await
        .expect("pg_authid")
        .get(0);
    let (_, body) = test.set(json!({"password": "pw-2"})).await;
    assert!(body["binding"]["verified_at"].is_string(), "{body}");
    assert_eq!(
        test.tenant_db
            .query_one(
                "SELECT rolpassword FROM pg_authid WHERE rolname = $1",
                &[&probed],
            )
            .await
            .expect("pg_authid")
            .get::<_, String>(0),
        role_password,
        "the password of a role an admin bound is never written by the platform"
    );
}

#[tokio::test]
async fn a_role_that_does_not_exist_is_reported_with_its_creation() {
    let Some(test) = Bindings::start().await else {
        return;
    };
    let (role, read_only) = (test.db.role("nobody"), test.db.role("readers"));
    let (status, body) = test
        .set(json!({"role": role, "password": "pw", "read_only_role": read_only}))
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.verify().await;
    assert_eq!(status, 200, "{body}");
    let report = &body["report"];
    assert_eq!(report["verified"], false);
    assert_eq!(report["checks"][0]["check"], "role_exists");
    assert_eq!(
        report["checks"][0]["remedy"],
        format!("CREATE ROLE \"{role}\" LOGIN;")
    );
    assert!(failed(report).contains(&"credentials"));
    assert!(failed(report).contains(&"read_only_role"));
    assert!(report["server_version_num"]
        .as_i64()
        .is_some_and(|v| v >= 130000));
}

#[tokio::test]
async fn roles_are_refused_where_a_recycle_leaves_what_a_session_set() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform =
        PlatformProcess::start(&db.url, infra.nats_url(), &[("DB_POOL_RECYCLE", "fast")]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    for mode in ["warn", "enforce"] {
        let (status, body) = platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", tenant.slug),
                &admin,
                Some(&json!({"policies": {"db_roles": {"mode": mode}}})),
            )
            .await;
        assert_eq!(status, 422, "{mode}: {body}");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|e| e.contains("db_roles needs DB_POOL_RECYCLE=clean")),
            "{body}"
        );
    }
    drop(platform);
    let mut refused = PlatformProcess::launch(
        &db.url,
        infra.nats_url(),
        &[
            ("DB_POOL_RECYCLE", "verified"),
            ("CUFFLINK_POLICY_DB_ROLES", "warn"),
        ],
    );
    let status = refused
        .wait_exit(std::time::Duration::from_secs(60))
        .await
        .expect("the platform exits");
    assert!(
        !status.success(),
        "a platform whose default asks for roles does not start so"
    );
    assert!(
        refused
            .output_tail()
            .contains("db_roles needs DB_POOL_RECYCLE=clean"),
        "{}",
        refused.output_tail()
    );
}

impl Bindings {
    async fn policy(&self, mode: &str) -> (u16, Value) {
        self.platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &self.admin,
                Some(&json!({"policies": {"db_roles": {"mode": mode}}})),
            )
            .await
    }

    /// The tenant under `enforce`, its first service given a role of the
    /// platform's own.
    async fn enforced(&self) {
        let (status, body) = self.policy("warn").await;
        assert_eq!(status, 200, "{body}");
        let (status, body) = self.set(json!({})).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["report"]["verified"], true, "{body}");
        let (status, body) = self.enforce_db_roles().await;
        assert_eq!(status, 200, "{body}");
    }

    /// A schema, and its table, that belong to a role that logs in.
    async fn owned_schema(&self, name: &str, password: &str) -> String {
        let owner = self.db.role(name);
        self.sql(&format!(
            "CREATE ROLE {owner} LOGIN PASSWORD '{password}';
             GRANT {owner} TO CURRENT_USER;
             CREATE SCHEMA catalog_service AUTHORIZATION {owner};
             CREATE TABLE catalog_service.books (
                 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                 title text,
                 written_by text NOT NULL DEFAULT current_user
             );
             ALTER TABLE catalog_service.books OWNER TO {owner};"
        ))
        .await;
        owner
    }

    async fn deploy_as(&self, query: &str, manifest: &Value) -> (u16, Value) {
        use base64::Engine;
        let wasm = base64::engine::general_purpose::STANDARD.encode(common::guest::module_bytes());
        self.platform
            .send(
                Method::POST,
                &format!("/api/services/deploy?{query}"),
                &format!("ApiKey {}", self.tenant.api_key),
                Some(&json!({"manifest": manifest, "wasm_base64": wasm, "api_version": 1})),
            )
            .await
    }

    async fn write_book(&self) -> (u16, Value) {
        self.platform
            .send(
                Method::POST,
                &format!("/svc/{}/catalog-service/books", self.tenant.slug),
                &format!("ApiKey {}", self.tenant.api_key),
                Some(&json!({"title": "Dune"})),
            )
            .await
    }

    async fn written_by(&self) -> Vec<String> {
        self.tenant_db
            .query(
                "SELECT written_by FROM catalog_service.books ORDER BY written_by",
                &[],
            )
            .await
            .expect("books")
            .iter()
            .map(|row| row.get(0))
            .collect()
    }

    async fn service_recorded(&self, name: &str) -> Option<(String, i64)> {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_opt(
                "SELECT s.status, (SELECT count(*) FROM deployments d WHERE d.service_id = s.id)
                 FROM services s WHERE s.tenant_id = $1 AND s.name = $2",
                &[&self.tenant.id, &name],
            )
            .await
            .expect("services")
            .map(|row| (row.get(0), row.get(1)))
    }
}

/// A service serving the adopted `books`, whose `written_by` it does not
/// declare: the database fills it with the role that wrote the row.
fn served_catalog() -> Value {
    json!({
        "name": "catalog-service",
        "version": "0.1.0",
        "mode": "crud",
        "publishes": [],
        "schema": "adopted",
        "tables": [{
            "name": "books",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "title", "column_type": "text", "primary_key": false, "nullable": true}
            ]
        }],
    })
}

#[tokio::test]
async fn a_service_whose_schema_a_role_owns_is_bound_before_its_first_deploy_and_runs_as_it() {
    let Some(test) = Bindings::start().await else {
        return;
    };
    test.enforced().await;
    let owner = test.owned_schema("catalog_service_user", "pw-3").await;
    let bind = |body: Value| test.set_for("catalog-service", body);

    let (status, body) = bind(json!({"role": owner, "password": "pw-3"})).await;
    assert_eq!(
        status, 404,
        "no service is recorded without register: {body}"
    );
    let (status, body) = bind(json!({"register": true, "password": "pw-3"})).await;
    assert_eq!(status, 422, "a registration names its role: {body}");
    let (status, body) = bind(json!({"role": owner, "schema": "catalog_service"})).await;
    assert_eq!(status, 422, "a schema is given only with register: {body}");
    let platform_role: String = test
        .tenant_db
        .query_one("SELECT current_user::text", &[])
        .await
        .expect("current user")
        .get(0);
    let (status, body) = test
        .set_for(
            "shelf-service",
            json!({"register": true, "role": platform_role, "password": "pw"}),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body.to_string()
            .contains("the one the platform connects as"),
        "{body}"
    );
    assert_eq!(
        test.service_recorded("shelf-service").await,
        None,
        "a registration whose binding is refused leaves no service"
    );

    let (status, body) = bind(json!({"register": true, "role": owner, "password": "pw-3"})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["registered"], true);
    assert_eq!(body["schema"], "catalog_service");
    assert_eq!(body["binding"]["role"], owner);
    assert_eq!(body["binding"]["managed"], false);
    assert_eq!(body["report"]["verified"], true, "{body}");
    assert!(!body.to_string().contains("pw-3"), "{body}");
    assert_eq!(
        test.service_recorded("catalog-service").await,
        Some(("active".to_string(), 0)),
        "recorded as a first deploy records it, and nothing of it runs"
    );
    let (status, body) = bind(json!({"register": true, "role": owner})).await;
    assert_eq!(
        status, 200,
        "a registration sent again binds as before: {body}"
    );
    assert_eq!(body["registered"], false);
    assert_eq!(
        body["report"]["verified"], true,
        "the password is kept: {body}"
    );
    let (status, body) = bind(json!({"register": true, "role": owner, "schema": "catalog"})).await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        body["error"],
        "schema_name is immutable once the service is recorded: catalog_service \u{2192} catalog",
        "{body}"
    );
    let audited: Vec<Value> = test
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT details FROM audit_log WHERE action = 'db_role.set'
             AND tenant_id = $1 AND resource_id = 'catalog-service' ORDER BY created_at",
            &[&test.tenant.id],
        )
        .await
        .expect("audit")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        audited
            .iter()
            .map(|details| (
                details["registered"].clone(),
                details["schema"].clone(),
                details["role"].clone()
            ))
            .collect::<Vec<_>>(),
        [
            (json!(true), json!("catalog_service"), json!(owner)),
            (json!(false), json!("catalog_service"), json!(owner)),
        ],
        "each set is audited with whether it recorded the service"
    );

    let mut elsewhere = served_catalog();
    elsewhere["schema_name"] = json!("catalog_v2");
    let (status, body) = test.platform.deploy(&test.tenant, &elsewhere).await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        body["error"],
        "schema_name is immutable once the service is recorded: catalog_service \u{2192} catalog_v2",
        "a first deploy declares the schema the service was registered with: {body}"
    );

    let (status, body) = test.platform.deploy(&test.tenant, &served_catalog()).await;
    assert_eq!(
        status, 200,
        "the first deploy goes through in one pass: {body}"
    );
    assert_eq!(body["status"], "active", "and takes the status it asks for");
    let (status, body) = test.write_book().await;
    assert!((200..300).contains(&status), "{status} {body}");
    assert_eq!(
        test.written_by().await,
        [owner.clone()],
        "the row was written as the bound role"
    );
    let shown = test.show_for("catalog-service").await;
    assert!(
        shown["binding"]["verified_at"].is_string(),
        "verified again against the version that runs: {shown}"
    );
}

#[tokio::test]
async fn a_first_deploy_refused_for_want_of_a_role_is_bound_and_deployed_again() {
    let Some(test) = Bindings::start().await else {
        return;
    };
    test.enforced().await;
    let owner = test.owned_schema("catalog_service_user", "pw-4").await;

    let (status, body) = test.platform.deploy(&test.tenant, &served_catalog()).await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(
        error.contains(&format!("schema catalog_service belongs to {owner}"))
            && error.contains("cufflink services db-role set"),
        "the refusal names the owner and the way out: {body}"
    );
    assert_eq!(
        test.service_recorded("catalog-service").await,
        Some(("active".to_string(), 0)),
        "the service is recorded, and no version of it runs"
    );
    let (status, body) = test.write_book().await;
    assert_ne!(status, 200, "{body}");

    let (status, body) = test
        .set_for(
            "catalog-service",
            json!({"role": owner, "password": "pw-4"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["registered"], false);
    assert_eq!(body["report"]["verified"], true, "{body}");
    let (status, body) = test.platform.deploy(&test.tenant, &served_catalog()).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test.write_book().await;
    assert!((200..300).contains(&status), "{status} {body}");
    assert_eq!(test.written_by().await, [owner]);
}

#[tokio::test]
async fn a_registered_service_takes_the_status_its_first_deploy_asks_for_and_leaves_its_role() {
    let Some(test) = Bindings::start().await else {
        return;
    };
    let owner = test.owned_schema("catalog_service_user", "pw-5").await;
    let (status, body) = test
        .set_for(
            "catalog-service",
            json!({"register": true, "role": owner, "password": "pw-5"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["registered"], true);

    let (status, body) = test.deploy_as("plan=true", &served_catalog()).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (body["new"].clone(), body["status"].clone()),
        (json!(false), json!("active")),
        "a plan answers the status the deploy would set: {body}"
    );
    let (status, body) = test.deploy_as("inactive=true", &served_catalog()).await;
    assert_eq!(
        status, 200,
        "a registration does not refuse an inactive first deploy: {body}"
    );
    assert_eq!(body["status"], "inactive");
    let (status, body) = test.deploy_as("inactive=true", &served_catalog()).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["status"], "inactive",
        "a service that runs keeps its status"
    );

    let shelf = test.db.role("shelf_service_user");
    test.sql(&format!(
        "CREATE ROLE {shelf} LOGIN PASSWORD 'pw-6';
         GRANT {shelf} TO CURRENT_USER;
         CREATE SCHEMA shelves AUTHORIZATION {shelf};"
    ))
    .await;
    let (status, body) = test
        .set_for(
            "shelf-service",
            json!({"register": true, "schema": "shelves", "role": shelf, "password": "pw-6"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["schema"], "shelves");
    let service_id: uuid::Uuid = test
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT id FROM services WHERE tenant_id = $1 AND name = 'shelf-service'",
            &[&test.tenant.id],
        )
        .await
        .expect("registered")
        .get(0);
    let (status, body) = test
        .platform
        .send(
            Method::DELETE,
            &format!("/api/services/{service_id}"),
            &format!("ApiKey {}", test.tenant.api_key),
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(test.service_recorded("shelf-service").await, None);
    let (status, _) = test
        .platform
        .send(Method::GET, &test.path("shelf-service"), &test.admin, None)
        .await;
    assert_eq!(status, 404, "the binding went with the service");
    let (can_login, owns): (bool, bool) = test
        .tenant_db
        .query_one(
            &format!(
                "SELECT rolcanlogin, (SELECT nspowner = r.oid FROM pg_namespace WHERE nspname = 'shelves')
                 FROM pg_roles r WHERE rolname = '{shelf}'"
            ),
            &[],
        )
        .await
        .map(|row| (row.get(0), row.get(1)))
        .expect("the role is still there");
    assert!(
        can_login && owns,
        "a role the platform did not make is left as it was"
    );
}
