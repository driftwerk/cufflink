use cufflink_testkit as common;

use base64::Engine;
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::tenant_db::TenantPoolManager;
use reqwest::Method;
use serde_json::{json, Value};

const SERVICE: &str = "catalog-service";
const SCHEMA: &str = "catalog_service";
const PASSWORD: &str = "pw-declared-1";

fn manifest() -> Value {
    json!({
        "name": SERVICE,
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
    assert!(
        reasons(&body).contains("does not log in from the platform"),
        "{body}"
    );
    assert!(!reasons(&body).contains("pw-wrong"));

    let (status, body) = test
        .deploy("?plan=true", Some((&test.role, "pw-wrong")))
        .await;
    assert_eq!(status, 422, "a plan proves the password too: {body}");
    assert_eq!(test.audited("db_role.declare_refused").await, 3);

    test.sql(&format!("ALTER ROLE {} NOLOGIN", test.role)).await;
    let (status, body) = test.declare().await;
    assert_eq!(status, 422, "{body}");
    assert!(reasons(&body).contains("may not log in"), "{body}");
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
