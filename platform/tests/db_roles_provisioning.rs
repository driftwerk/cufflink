use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::tenant_db::TenantPoolManager;
use reqwest::Method;
use serde_json::{json, Value};

fn manifest(tables: &[&str]) -> Value {
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
        "name": "stock-service",
        "version": format!("0.{}.0", tables.len()),
        "mode": "crud",
        "tables": tables,
        "publishes": [],
    })
}

struct Provisioning {
    platform: PlatformProcess,
    _db: TestDb,
    tenant: Tenant,
    admin: String,
    tenant_db: tokio_postgres::Client,
}

impl Provisioning {
    async fn start() -> Option<Self> {
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
            _db: db,
            tenant,
            admin,
            tenant_db,
        })
    }

    async fn policy(&self, mode: &str, config: Value) -> (u16, Value) {
        self.platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &self.admin,
                Some(&json!({"policies": {"db_roles": {"mode": mode, "config": config}}})),
            )
            .await
    }

    async fn deploy(&self, tables: &[&str]) -> Value {
        let (status, body) = self.platform.deploy(&self.tenant, &manifest(tables)).await;
        assert_eq!(status, 200, "deploy: {body}");
        body
    }

    async fn binding(&self) -> Value {
        let (status, body) = self
            .platform
            .send(
                Method::GET,
                &format!(
                    "/api/tenants/{}/services/stock-service/db-role",
                    self.tenant.slug
                ),
                &self.admin,
                None,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        body["binding"].clone()
    }

    async fn set(&self, body: Value) -> (u16, Value) {
        self.platform
            .send(
                Method::PUT,
                &format!(
                    "/api/tenants/{}/services/stock-service/db-role",
                    self.tenant.slug
                ),
                &self.admin,
                Some(&body),
            )
            .await
    }

    async fn sql(&self, sql: &str) {
        self.tenant_db.batch_execute(sql).await.expect(sql);
    }

    async fn one<T: for<'a> tokio_postgres::types::FromSql<'a>>(&self, sql: &str) -> T {
        self.tenant_db.query_one(sql, &[]).await.expect(sql).get(0)
    }

    async fn verifier_of(&self, role: &str) -> String {
        self.one(&format!(
            "SELECT rolpassword FROM pg_authid WHERE rolname = '{role}'"
        ))
        .await
    }

    async fn owner_of(&self, relation: &str) -> String {
        self.one(&format!(
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = '{relation}'::regclass"
        ))
        .await
    }

    async fn can_select(&self, who: &str, relation: &str) -> bool {
        self.one(&format!(
            "SELECT has_table_privilege('{who}', '{relation}', 'SELECT')"
        ))
        .await
    }
}

fn passed(report: &Value) -> Vec<&str> {
    report["checks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|check| check["ok"] == true)
        .filter_map(|check| check["check"].as_str())
        .collect()
}

#[tokio::test]
async fn db_roles_provisioning_pg16() {
    let Some(test) = Provisioning::start().await else {
        return;
    };
    let (reader, read_only) = (
        test._db.role("reporting_owner"),
        test._db.role("reports_ro"),
    );
    test.sql(&format!(
        "CREATE ROLE {reader} NOLOGIN; CREATE ROLE {read_only} NOLOGIN"
    ))
    .await;

    test.deploy(&["items"]).await;
    let role = test._db.managed_role(&test.tenant, "stock-service").await;
    assert_eq!(
        test.binding().await,
        Value::Null,
        "with db_roles off a deploy makes no role"
    );
    test.sql(
        "CREATE TABLE stock_service.counted (
             id BIGSERIAL PRIMARY KEY,
             n BIGINT GENERATED ALWAYS AS IDENTITY,
             m INTEGER GENERATED BY DEFAULT AS IDENTITY);
         CREATE SEQUENCE stock_service.alone;
         CREATE SEQUENCE stock_service.aaa_lent OWNED BY stock_service.counted.m;
         GRANT CREATE ON SCHEMA public TO PUBLIC",
    )
    .await;
    assert!(
        !test
            .one::<bool>(&format!(
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{}')",
                role
            ))
            .await
    );

    let (status, body) = test.policy("warn", json!({"readers": [reader]})).await;
    assert_eq!(status, 200, "{body}");
    test.deploy(&["items", "tags"]).await;
    let binding = test.binding().await;
    assert_eq!(binding["role"], role.as_str(), "{binding}");
    assert_eq!(binding["managed"], true);
    assert_eq!(
        binding["login"], true,
        "the platform holds the role's password"
    );
    assert!(
        binding["verified_at"].is_string(),
        "a role the platform made is verified as it is made: {binding}"
    );
    assert_eq!(binding["verify_report"]["verified"], true, "{binding}");
    assert!(
        test.one::<bool>(&format!(
            "SELECT has_schema_privilege('{role}', '_cufflink', 'USAGE')
                AND has_function_privilege('{role}',
                    '_cufflink.outbox_enqueue_own(text[], text[], bytea[], jsonb[], uuid[])',
                    'EXECUTE')"
        ))
        .await,
        "the platform's schema is made before the role is granted what is in it"
    );
    assert_eq!(
        passed(&binding["verify_report"]),
        [
            "role_exists",
            "can_login",
            "role_attributes",
            "own_role",
            "role_memberships",
            "platform_membership",
            "credentials",
            "database_connect",
            "schema",
            "public_schema",
            "table",
            "table",
            "cufflink_schema",
            "outbox_enqueue",
            "outbox_confined",
            "outbox_service",
        ],
        "and the verification checked it: {binding}"
    );
    let server_version: i64 = test
        .one::<i32>("SELECT current_setting('server_version_num')::int")
        .await
        .into();
    assert!(
        server_version >= 160000,
        "this test provisions on PostgreSQL 16 or later"
    );
    assert_eq!(
        test.one::<String>(
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'stock_service'"
        )
        .await,
        role,
        "the schema is the role's"
    );
    for table in ["stock_service.items", "stock_service.tags"] {
        assert_eq!(test.owner_of(table).await, role, "{table}");
        assert!(
            test.can_select(&reader, table).await,
            "{reader} reads {table}"
        );
    }
    for relation in [
        "stock_service.counted",
        "stock_service.counted_id_seq",
        "stock_service.counted_n_seq",
        "stock_service.aaa_lent",
        "stock_service.alone",
    ] {
        assert_eq!(
            test.owner_of(relation).await,
            role,
            "a sequence a column owns follows its table, one that stands alone is handed over: {relation}"
        );
    }
    test.sql(&format!(
        "SET ROLE {}; INSERT INTO stock_service.counted DEFAULT VALUES; RESET ROLE",
        role
    ))
    .await;
    assert!(
        !test
            .one::<bool>("SELECT has_schema_privilege('public', 'public', 'CREATE')")
            .await,
        "in a database of the platform's no role creates in public"
    );
    assert_eq!(
        test.one::<Vec<String>>(&format!(
            "SELECT array_remove(ARRAY[CASE WHEN rolsuper THEN 'SUPERUSER' END,
                 CASE WHEN rolcreatedb THEN 'CREATEDB' END, CASE WHEN rolcreaterole THEN 'CREATEROLE' END,
                 CASE WHEN rolreplication THEN 'REPLICATION' END, CASE WHEN rolbypassrls THEN 'BYPASSRLS' END,
                 CASE WHEN rolinherit THEN 'INHERIT' END], NULL)
             FROM pg_roles WHERE rolname = '{}'",
            role
        ))
        .await,
        Vec::<String>::new()
    );
    assert!(
        test.one::<bool>(&format!(
            "SELECT pg_has_role(current_user, '{role}', 'SET') AND pg_has_role(current_user, '{role}', 'USAGE')"
        ))
        .await,
        "the platform can SET ROLE to it and inherits its privileges"
    );
    test.sql(&format!(
        "SET ROLE {}; CREATE TABLE stock_service.later (id int); RESET ROLE",
        role
    ))
    .await;
    assert!(
        test.can_select(&reader, "stock_service.later").await,
        "a table the role makes later is readable by default"
    );

    test.sql("CREATE TABLE stock_service.by_platform (id int)")
        .await;
    assert_ne!(test.owner_of("stock_service.by_platform").await, role);
    test.deploy(&["items", "tags", "extras"]).await;
    for table in [
        "stock_service.extras",
        "stock_service.by_platform",
        "stock_service.later",
    ] {
        assert_eq!(
            test.owner_of(table).await,
            role,
            "every deploy hands the role what the platform made: {table}"
        );
        assert!(
            test.can_select(&reader, table).await,
            "{reader} reads {table}"
        );
    }
    assert!(
        !test.can_select(&read_only, "stock_service.items").await,
        "{read_only} is not a reader"
    );

    let (status, body) = test.set(json!({"password": "mine"})).await;
    assert_eq!(
        status, 422,
        "a managed role's password is the platform's: {body}"
    );
    let (status, body) = test
        .set(json!({"read_only_role": test._db.role("nobody")}))
        .await;
    assert_eq!(status, 422, "a read-only role that does not exist: {body}");
    let (status, body) = test.set(json!({"read_only_role": read_only})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["binding"]["managed"], true,
        "still the platform's role"
    );
    assert_eq!(body["binding"]["role"], role.as_str());
    assert_eq!(body["report"]["verified"], true, "{body}");
    assert!(
        test.one::<bool>(&format!(
            "SELECT pg_has_role('{}', '{read_only}', 'SET')",
            role
        ))
        .await,
        "the service role may switch to its read-only role"
    );
    let (status, body) = test.set(json!({"read_only_role": read_only})).await;
    assert_eq!(
        status, 200,
        "a membership that is there is not granted again: {body}"
    );
    assert_eq!(body["report"]["verified"], true, "{body}");

    let (status, body) = test.policy("enforce", json!({"readers": [reader]})).await;
    assert_eq!(
        status, 200,
        "a verified managed role satisfies the enforce gate: {body}"
    );
    let (status, body) = test
        .policy("warn", json!({"readers": ["missing_reader"]}))
        .await;
    assert_eq!(status, 200, "{body}");
    test.sql("CREATE TABLE stock_service.by_hand (id int)")
        .await;
    test.deploy(&["items", "tags", "extras", "more"]).await;
    let binding = test.binding().await;
    assert!(
        binding["verified_at"].is_string(),
        "a reader that does not exist stops the refresh and leaves the binding as it was: {binding}"
    );
    assert_ne!(
        test.owner_of("stock_service.by_hand").await,
        role,
        "nothing of the refresh ran"
    );
    assert_eq!(
        test.owner_of("stock_service.more").await,
        role,
        "what the deploy made it made as the role"
    );
    assert_eq!(
        test.platform
            .metric(
                "cufflink_db_role_provision_failures_total",
                &[("tenant", &test.tenant.slug), ("service", "stock-service")]
            )
            .await,
        Some(1.0)
    );
}

const SERVICES: usize = 8;

fn named(name: &str) -> Value {
    let mut manifest = manifest(&["items"]);
    manifest["name"] = json!(name);
    manifest
}

async fn all_verified(pool: &deadpool_postgres::Pool, tenant_id: uuid::Uuid) -> (i64, bool) {
    let row = pool
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*), COALESCE(bool_and(r.verified_at IS NOT NULL), true)
             FROM service_db_roles r JOIN services s ON s.id = r.service_id
             WHERE s.tenant_id = $1",
            &[&tenant_id],
        )
        .await
        .expect("bindings");
    (row.get(0), row.get(1))
}

#[tokio::test]
async fn db_roles_services_provisioned_at_once_and_a_binding_never_seen_unverified() {
    let Some(test) = Provisioning::start().await else {
        return;
    };
    let (status, body) = test.policy("warn", json!({})).await;
    assert_eq!(status, 200, "{body}");
    let names: Vec<String> = (0..SERVICES).map(|n| format!("svc-{n}")).collect();
    let deployed = futures_util::future::join_all(names.iter().map(|name| {
        let test = &test;
        async move { test.platform.deploy(&test.tenant, &named(name)).await }
    }))
    .await;
    for ((status, body), name) in deployed.iter().zip(&names) {
        assert_eq!(*status, 200, "deploy of {name}: {body}");
    }
    let pool = test._db.pool();
    assert_eq!(
        all_verified(&pool, test.tenant.id).await,
        (SERVICES as i64, true),
        "every service has its role, whichever came first"
    );
    for name in &names {
        assert_eq!(
            test.platform
                .metric(
                    "cufflink_db_role_provision_failures_total",
                    &[("tenant", &test.tenant.slug), ("service", name)]
                )
                .await,
            None,
            "{name}"
        );
    }

    let watching = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let watcher = tokio::spawn({
        let (pool, watching, tenant_id) = (pool.clone(), watching.clone(), test.tenant.id);
        async move {
            let mut reads = 0u32;
            while watching.load(std::sync::atomic::Ordering::Relaxed) {
                let (bound, verified) = all_verified(&pool, tenant_id).await;
                assert!(
                    verified && bound == SERVICES as i64,
                    "a binding that was verified is unverified while it is checked again"
                );
                reads += 1;
            }
            reads
        }
    });
    let refreshed = futures_util::future::join_all(names.iter().flat_map(|name| {
        let test = &test;
        [
            Box::pin(async move {
                test.platform
                    .send(
                        Method::POST,
                        &format!(
                            "/api/tenants/{}/services/{name}/db-role/verify",
                            test.tenant.slug
                        ),
                        &test.admin,
                        None,
                    )
                    .await
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = (u16, Value)>>>,
            Box::pin(async move {
                test.platform
                    .send(
                        Method::POST,
                        &format!(
                            "/api/tenants/{}/services/{name}/db-role/rotate",
                            test.tenant.slug
                        ),
                        &test.admin,
                        None,
                    )
                    .await
            }),
            Box::pin(async move { test.platform.deploy(&test.tenant, &named(name)).await }),
        ]
    }))
    .await;
    watching.store(false, std::sync::atomic::Ordering::Relaxed);
    let reads = watcher.await.expect("the watcher");
    for (status, body) in &refreshed {
        assert_eq!(*status, 200, "{body}");
    }
    assert!(
        reads > 0,
        "the bindings were read while they were refreshed"
    );
}

#[tokio::test]
async fn db_roles_a_role_the_platform_did_not_make_is_never_taken_over() {
    let Some(test) = Provisioning::start().await else {
        return;
    };
    test.deploy(&["items"]).await;
    let role = test._db.managed_role(&test.tenant, "stock-service").await;
    test.sql(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'theirs'; COMMENT ON ROLE {role} IS 'made by hand'"
    ))
    .await;
    let theirs = test.verifier_of(&role).await;

    let (status, body) = test.policy("warn", json!({})).await;
    assert_eq!(status, 200, "{body}");
    test.deploy(&["items", "tags"]).await;
    assert_eq!(
        test.binding().await,
        Value::Null,
        "a deploy binds no role it did not make"
    );
    assert_eq!(
        test.platform
            .metric(
                "cufflink_db_role_provision_failures_total",
                &[("tenant", &test.tenant.slug), ("service", "stock-service")]
            )
            .await,
        Some(2.0),
        "before and after the deploy"
    );
    let (status, body) = test.set(json!({})).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"].as_str().is_some_and(|e| e.contains(&format!(
            "role {role} exists and was not made by the platform for this service"
        ))),
        "{body}"
    );
    assert_eq!(
        test.verifier_of(&role).await,
        theirs,
        "its password is untouched"
    );
    assert_eq!(
        test.one::<String>(&format!(
            "SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = '{role}'"
        ))
        .await,
        "made by hand"
    );

    test.sql(&format!("DROP ROLE {role}")).await;
    let (status, body) = test.set(json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["binding"]["role"], role.as_str());
    assert_eq!(body["report"]["verified"], true, "{body}");
    assert!(
        test.one::<String>(&format!(
            "SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = '{role}'"
        ))
        .await
        .starts_with(&format!("cufflink:{}:", test.tenant.id)),
        "the role it made is marked as the service's"
    );

    let (status, body) = test
        .platform
        .deploy(&test.tenant, &named("other-service"))
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test
        .platform
        .send(
            Method::PUT,
            &format!(
                "/api/tenants/{}/services/other-service/db-role",
                test.tenant.slug
            ),
            &test.admin,
            Some(&json!({"role": role, "password": "guess"})),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"].as_str().is_some_and(|e| e.contains(&format!(
            "role '{role}' is bound to the service 'stock-service' of tenant '{}'",
            test.tenant.slug
        ))),
        "{body}"
    );
}

async fn as_login(test: &Provisioning, role: &str, sql: &str) -> Result<(), tokio_postgres::Error> {
    test.sql(&format!("SET SESSION AUTHORIZATION \"{role}\""))
        .await;
    let ran = test.tenant_db.batch_execute(sql).await;
    test.sql("RESET SESSION AUTHORIZATION").await;
    ran
}

fn enqueue_own(msg_id: uuid::Uuid) -> String {
    format!(
        "SELECT _cufflink.outbox_enqueue_own(ARRAY['event'], ARRAY['stock_service.created'],
             ARRAY['\\x01'::bytea], ARRAY['[]'::jsonb], ARRAY['{msg_id}'::uuid])"
    )
}

#[tokio::test]
async fn db_roles_a_service_enqueues_for_its_own_service_alone() {
    let Some(test) = Provisioning::start().await else {
        return;
    };
    let (status, body) = test.policy("warn", json!({})).await;
    assert_eq!(status, 200, "{body}");
    test.deploy(&["items"]).await;
    let role = test._db.managed_role(&test.tenant, "stock-service").await;
    let (status, body) = test
        .platform
        .send(
            Method::POST,
            &format!(
                "/api/tenants/{}/services/stock-service/db-role/verify",
                test.tenant.slug
            ),
            &test.admin,
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["report"]["verified"], true, "{body}");
    for check in ["outbox_enqueue", "outbox_confined", "outbox_service"] {
        assert!(passed(&body["report"]).contains(&check), "{check}: {body}");
    }
    assert_eq!(
        test.one::<String>(&format!(
            "SELECT service FROM _cufflink.service_roles WHERE role = '{role}'"
        ))
        .await,
        "stock-service"
    );

    let (logged_in, set) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    as_login(&test, &role, &enqueue_own(logged_in))
        .await
        .expect("a login as the role enqueues");
    test.sql(&format!(
        "SET ROLE \"{role}\"; {}; RESET ROLE",
        enqueue_own(set)
    ))
    .await;
    let platform_role: String = test.one("SELECT current_user::text").await;
    for (msg_id, source) in [(logged_in, role.as_str()), (set, platform_role.as_str())] {
        let row = test
            .tenant_db
            .query_one(
                "SELECT service, source_role::text FROM _cufflink.outbox WHERE msg_id = $1",
                &[&msg_id],
            )
            .await
            .expect("the row");
        assert_eq!(
            (row.get::<_, String>(0), row.get::<_, String>(1)),
            ("stock-service".to_string(), source.to_string()),
            "the service is the role's, whoever logged in"
        );
    }

    for refused in [
        "SELECT _cufflink.outbox_enqueue_many(ARRAY['event'], ARRAY['loan-service'], ARRAY['s'],
             ARRAY['\\x01'::bytea], ARRAY['[]'::jsonb], ARRAY[gen_random_uuid()])",
        "INSERT INTO _cufflink.outbox (msg_id, kind, service, subject, payload)
             VALUES (gen_random_uuid(), 'event', 'loan-service', 's', '\\x01')",
        "UPDATE _cufflink.outbox SET service = 'loan-service'",
        "DELETE FROM _cufflink.outbox",
        "UPDATE _cufflink.service_roles SET service = 'loan-service'",
    ] {
        let error = as_login(&test, &role, refused).await.expect_err(refused);
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE),
            "{refused}: {error}"
        );
    }

    let stranger = test._db.role("stranger");
    test.sql(&format!(
        "CREATE ROLE {stranger} NOLOGIN;
         GRANT USAGE ON SCHEMA _cufflink TO {stranger};
         GRANT EXECUTE ON FUNCTION _cufflink.outbox_enqueue_own(TEXT[], TEXT[], BYTEA[], JSONB[], UUID[]) TO {stranger}"
    ))
    .await;
    let error = as_login(&test, &stranger, &enqueue_own(uuid::Uuid::new_v4()))
        .await
        .expect_err("a role mapped to no service");
    assert!(
        error
            .as_db_error()
            .is_some_and(|e| e.message() == format!("role {stranger} enqueues for no service")),
        "{error}"
    );
    test.sql(&format!("DROP OWNED BY {stranger}; DROP ROLE {stranger}"))
        .await;

    let service =
        cufflink_platform::repo::services::find(&test._db.pool(), test.tenant.id, "stock-service")
            .await
            .expect("services")
            .expect("the service");
    let (status, body) = test
        .platform
        .send(
            Method::DELETE,
            &format!("/api/services/{}", service.id),
            &format!("ApiKey {}", test.tenant.api_key),
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert!(
        test.one::<bool>(&format!(
            "SELECT NOT rolcanlogin FROM pg_roles WHERE rolname = '{role}'"
        ))
        .await,
        "the role of a deleted service logs in no more, and keeps what it owns"
    );
    assert_eq!(
        test.one::<i64>(&format!(
            "SELECT count(*) FROM _cufflink.service_roles WHERE role = '{role}'"
        ))
        .await,
        0,
        "nor enqueues"
    );
}
