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
        self.platform
            .send(
                Method::PUT,
                &self.path("loan-service"),
                &self.admin,
                Some(&body),
            )
            .await
    }

    async fn verify(&self) -> (u16, Value) {
        self.platform
            .send(
                Method::POST,
                &format!("{}/verify", self.path("loan-service")),
                &self.admin,
                None,
            )
            .await
    }

    async fn show(&self) -> Value {
        let (status, body) = self
            .platform
            .send(Method::GET, &self.path("loan-service"), &self.admin, None)
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
