use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};

const LOANS: &str = "loan-service";
const STOCKS: &str = "stock-service";
const REPORTS: &str = "reports-service";
const HANDLERS: [&str; 7] = [
    "query",
    "execute",
    "transaction",
    "legacy",
    "legacy_execute",
    "batch",
    "read_only",
];

fn crud_manifest(name: &str, tables: &[&str]) -> Value {
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

fn guest_manifest(name: &str, n: u32) -> Value {
    let paths = HANDLERS.map(|handler| format!("/{handler}"));
    let routes: Vec<(&str, &str)> = HANDLERS
        .iter()
        .zip(&paths)
        .map(|(handler, path)| (*handler, path.as_str()))
        .collect();
    let mut manifest = common::platform::wasm_manifest(name, &routes);
    manifest["version"] = json!(format!("0.1.{n}"));
    manifest
}

struct Modes {
    platform: PlatformProcess,
    db: TestDb,
    tenant: Tenant,
    admin: String,
    tenant_db: tokio_postgres::Client,
    tenant_db_url: String,
    read_only_role: String,
    guest: Vec<u8>,
}

impl Modes {
    async fn start(read_only_role: &str) -> Option<Self> {
        let guest = common::sdk::sdk_guest("db-roles", Profile::Debug)?;
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let read_only_role = db.role(read_only_role);
        let kek = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [8u8; 32]);
        let platform = PlatformProcess::start(
            &db.url,
            infra.nats_url(),
            &[
                ("CUFFLINK_SECRETS_KEK", &kek),
                ("CUFFLINK_SECRETS_KEK_ID", "test"),
                ("DB_READ_ONLY_ROLE", &read_only_role),
            ],
        )
        .await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let tenant_db_url =
            common::infra::with_dbname(&db.url, &TenantPoolManager::managed_db_name(&tenant.slug));
        let (tenant_db, connection) =
            tokio_postgres::connect(&tenant_db_url, tokio_postgres::NoTls)
                .await
                .expect("tenant database");
        tokio::spawn(connection);
        Some(Self {
            platform,
            db,
            tenant,
            admin,
            tenant_db,
            tenant_db_url,
            read_only_role,
            guest,
        })
    }

    async fn role(&self, service: &str) -> String {
        self.db.managed_role(&self.tenant, service).await
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

    async fn set_policy(&self, mode: &str, config: Value) {
        let (status, body) = self.policy(mode, config).await;
        assert_eq!(status, 200, "db_roles {mode}: {body}");
    }

    async fn deploy_module(&self, manifest: &Value) {
        let (status, body) = self
            .platform
            .deploy_module(&self.tenant, manifest, &self.guest)
            .await;
        assert_eq!(status, 200, "deploy of {}: {body}", manifest["name"]);
    }

    async fn deploy_guest(&self, name: &str, n: u32) {
        self.deploy_module(&guest_manifest(name, n)).await;
    }

    async fn deploy_hooked(&self, name: &str) {
        let mut manifest = guest_manifest(name, 0);
        manifest["on_migrate"] = json!("migrate");
        self.deploy_module(&manifest).await;
    }

    async fn migrated_by(&self, schema: &str) -> (String, String) {
        let row = self
            .tenant_db
            .query_one(&format!("SELECT who, login FROM {schema}.migrated"), &[])
            .await
            .expect("the hook's row");
        (row.get(0), row.get(1))
    }

    async fn deploy(&self, manifest: &Value) -> (u16, Value) {
        self.platform.deploy(&self.tenant, manifest).await
    }

    async fn plan(&self, manifest: &Value) -> Value {
        let (status, plan) = self
            .platform
            .send(
                Method::POST,
                "/api/services/deploy?plan=true",
                &format!("ApiKey {}", self.tenant.api_key),
                Some(&json!({"manifest": manifest, "api_version": 1})),
            )
            .await;
        assert_eq!(status, 200, "{plan}");
        plan
    }

    async fn deploy_crud(&self, name: &str, tables: &[&str]) {
        let (status, body) = self.deploy(&crud_manifest(name, tables)).await;
        assert_eq!(status, 200, "deploy of {name}: {body}");
    }

    async fn db_role(
        &self,
        method: Method,
        service: &str,
        step: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        self.platform
            .send(
                method,
                &format!(
                    "/api/tenants/{}/services/{service}/db-role{step}",
                    self.tenant.slug
                ),
                &self.admin,
                body.as_ref(),
            )
            .await
    }

    async fn binding(&self, service: &str) -> Value {
        let (status, body) = self.db_role(Method::GET, service, "", None).await;
        assert_eq!(status, 200, "{body}");
        body["binding"].clone()
    }

    async fn ask(&self, service: &str, handler: &str, body: Value) -> Value {
        let response = self
            .platform
            .http
            .post(self.platform.fn_url(&self.tenant, service, handler))
            .json(&body)
            .send()
            .await
            .expect("fn request");
        let status = response.status().as_u16();
        let answer = response.json::<Value>().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{service}/{handler}: {answer}");
        answer
    }

    async fn query(&self, service: &str, sql: &str) -> Value {
        self.ask(service, "query", json!({ "sql": sql })).await
    }

    async fn read_only(&self, service: &str, sql: &str) -> Value {
        self.ask(service, "read_only", json!({ "sql": sql })).await
    }

    async fn api_query(&self, service: &str, sql: &str) -> Value {
        let service_id: uuid::Uuid = self
            .db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT id FROM services WHERE tenant_id = $1 AND name = $2",
                &[&self.tenant.id, &service],
            )
            .await
            .expect("the service")
            .get(0);
        let response = self
            .platform
            .http
            .post(format!(
                "{}/api/services/{service_id}/query",
                self.platform.base_url
            ))
            .header("authorization", format!("ApiKey {}", self.tenant.api_key))
            .json(&json!({ "sql": sql }))
            .send()
            .await
            .expect("query request");
        let status = response.status().as_u16();
        let text = response.text().await.expect("the answer");
        if status != 200 {
            let refused: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            return json!({ "status": status, "error": refused["error"] });
        }
        let frames: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("a frame"))
            .collect();
        let columns: Vec<String> = frames
            .iter()
            .find(|frame| frame["type"] == "columns")
            .map(|frame| {
                frame["columns"]
                    .as_array()
                    .expect("columns")
                    .iter()
                    .map(|column| column["name"].as_str().expect("a name").to_string())
                    .collect()
            })
            .unwrap_or_default();
        match frames.iter().find(|frame| frame["type"] == "error") {
            Some(error) => json!({ "error": error["message"] }),
            None => json!({
                "rows": frames
                    .iter()
                    .filter(|frame| frame["type"] == "row")
                    .map(|frame| {
                        columns
                            .iter()
                            .cloned()
                            .zip(frame["values"].as_array().expect("values").iter().cloned())
                            .collect::<serde_json::Map<String, Value>>()
                    })
                    .collect::<Vec<_>>()
            }),
        }
    }

    async fn crud(
        &self,
        method: Method,
        service: &str,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        self.platform
            .send(
                method,
                &format!("/svc/{}/{service}{path}", self.tenant.slug),
                &format!("ApiKey {}", self.tenant.api_key),
                body.as_ref(),
            )
            .await
    }

    async fn sql(&self, sql: &str) {
        self.tenant_db.batch_execute(sql).await.expect(sql);
    }

    async fn one<T: for<'a> tokio_postgres::types::FromSql<'a>>(&self, sql: &str) -> T {
        self.tenant_db.query_one(sql, &[]).await.expect(sql).get(0)
    }

    async fn owner_of(&self, relation: &str) -> String {
        self.one(&format!(
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = '{relation}'::regclass"
        ))
        .await
    }

    async fn metric(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.platform.metric(name, labels).await.unwrap_or(0.0)
    }

    async fn fallbacks(&self, service: &str) -> f64 {
        self.metric(
            "cufflink_db_role_fallback_total",
            &[("tenant", &self.tenant.slug), ("service", service)],
        )
        .await
    }

    async fn violations(&self, outcome: &str, reason: &str) -> f64 {
        self.metric(
            "cufflink_policy_violation_total",
            &[
                ("key", "db_roles"),
                ("tenant", &self.tenant.slug),
                ("outcome", outcome),
                ("reason", reason),
            ],
        )
        .await
    }

    async fn triggers(&self) {
        self.sql(
            "CREATE FUNCTION stock_service.stamp() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN NEW.body := current_user || ' of ' || session_user; RETURN NEW; END $$;
             CREATE TRIGGER stamp BEFORE INSERT ON stock_service.items
                 FOR EACH ROW EXECUTE FUNCTION stock_service.stamp();
             CREATE FUNCTION stock_service.copy_to_loans() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN INSERT INTO loan_service.items (title) VALUES (NEW.body); RETURN NEW; END $$;
             CREATE TRIGGER copy AFTER INSERT ON stock_service.audited
                 FOR EACH ROW EXECUTE FUNCTION stock_service.copy_to_loans();",
        )
        .await;
    }
}

fn rows(answer: &Value) -> &Vec<Value> {
    answer["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("rows: {answer}"))
}

fn refusal(answer: &Value) -> &str {
    answer["error"]["message"]
        .as_str()
        .or_else(|| answer["error"].as_str())
        .unwrap_or_else(|| panic!("a refusal: {answer}"))
}

#[tokio::test]
async fn db_roles_warn_fallback() {
    let Some(test) = Modes::start("warn_reports_ro").await else {
        return;
    };
    test.sql(&format!("CREATE ROLE {} NOLOGIN", test.read_only_role))
        .await;
    test.deploy_guest(LOANS, 0).await;
    test.deploy_crud(STOCKS, &["items", "audited"]).await;
    let platform_role: String = test.one("SELECT current_user::text").await;
    let who = test.query(LOANS, "SELECT current_user::text AS who").await;
    assert_eq!(
        rows(&who)[0]["who"],
        platform_role.as_str(),
        "with db_roles off a guest runs as the platform"
    );

    test.set_policy("warn", json!({})).await;
    assert_eq!(
        rows(&test.query(LOANS, "SELECT current_user::text AS who").await)[0]["who"],
        platform_role.as_str(),
        "and under warn too until its service has a verified role"
    );
    assert_eq!(test.violations("warned", "no_verified_binding").await, 1.0);
    test.query(LOANS, "SELECT 1 AS one").await;
    assert_eq!(
        test.violations("warned", "no_verified_binding").await,
        1.0,
        "a service without a role is recorded once a minute, not once a statement"
    );
    test.deploy_guest(LOANS, 1).await;
    test.deploy_crud(STOCKS, &["items", "audited", "extras"])
        .await;
    test.triggers().await;
    let (loans, stocks) = (test.role(LOANS).await, test.role(STOCKS).await);
    assert_eq!(
        test.owner_of("stock_service.extras").await,
        stocks,
        "what a deploy creates is the role's"
    );

    let who = test
        .query(
            LOANS,
            "SELECT current_user::text AS who, session_user::text AS login",
        )
        .await;
    assert_eq!(
        rows(&who)[0],
        json!({"who": loans, "login": platform_role}),
        "the role is set on the platform's connection"
    );
    let inserted = test
        .ask(
            LOANS,
            "execute",
            json!({"sql": "INSERT INTO items (title) VALUES ('own')"}),
        )
        .await;
    assert_eq!(inserted, json!({"affected": 1}));
    assert_eq!(
        test.fallbacks(LOANS).await,
        0.0,
        "what the role may do is not run again"
    );

    assert_eq!(
        rows(
            &test
                .api_query(
                    LOANS,
                    "SELECT current_user::text AS who, session_user::text AS login",
                )
                .await
        )[0],
        json!({"who": platform_role, "login": platform_role}),
        "under warn the query API runs as the platform, as it always has"
    );

    let (status, created) = test
        .crud(Method::POST, STOCKS, "/items", Some(json!({"body": "x"})))
        .await;
    assert_eq!(status, 200, "{created}");
    assert_eq!(
        created["body"],
        format!("{stocks} of {platform_role}"),
        "a data route's statement runs as the role too"
    );
    let created_id = created["id"].as_str().unwrap_or_default();
    let (status, read) = test
        .crud(Method::GET, STOCKS, &format!("/items/{created_id}"), None)
        .await;
    assert_eq!(status, 200, "{read}");
    assert_eq!(read["body"], created["body"]);
    assert_eq!(test.fallbacks(STOCKS).await, 0.0);

    let secret = "secret-literal-7f3a";
    let across = test
        .query(
            LOANS,
            &format!("SELECT count(*)::int AS n FROM stock_service.items WHERE body <> '{secret}'"),
        )
        .await;
    assert_eq!(
        rows(&across)[0]["n"],
        1,
        "a statement the role is refused runs as the platform: {across}"
    );
    assert!(test.fallbacks(LOANS).await >= 1.0);
    assert!(test.violations("warned", "role_refused").await >= 1.0);
    let log = test.platform.output_tail();
    assert!(
        log.contains("db_role_fallback")
            && log.contains("permission denied for schema stock_service"),
        "the refusal is logged with what Postgres said: {log}"
    );
    assert!(
        !log.contains(secret),
        "and never with the statement, which carries the guest's data"
    );

    let recorded = test.fallbacks(LOANS).await;
    let transaction = test
        .ask(
            LOANS,
            "transaction",
            json!({"statements": [
                "INSERT INTO items (title) VALUES ('first') RETURNING title",
                "SELECT count(*)::int AS n FROM stock_service.items",
                "SELECT current_user::text AS who",
                "INSERT INTO items (title) VALUES ('second') RETURNING title",
            ]}),
        )
        .await;
    assert_eq!(
        transaction,
        json!({
            "statements": [
                {"rows": [{"title": "first"}]},
                {"rows": [{"n": 1}]},
                {"rows": [{"who": loans}]},
                {"rows": [{"title": "second"}]},
            ],
            "committed": true,
        }),
        "the transaction goes on as the role after the statement that fell back"
    );
    assert!(test.fallbacks(LOANS).await > recorded);
    assert_eq!(
        test.one::<i64>(
            "SELECT count(*) FROM loan_service.items WHERE title IN ('first', 'second')"
        )
        .await,
        2
    );

    let recorded = test.fallbacks(LOANS).await;
    let failed = test
        .ask(
            LOANS,
            "transaction",
            json!({"statements": [
                "INSERT INTO items (title) VALUES ('lost') RETURNING title",
                "SELECT 1 / 0 AS n",
                "SELECT 1 AS one",
            ]}),
        )
        .await;
    assert_eq!(
        failed["statements"][0],
        json!({"rows": [{"title": "lost"}]})
    );
    assert_eq!(
        failed["statements"][1]["error"]["kind"], "data_exception",
        "{failed}"
    );
    assert_eq!(
        failed["statements"][2]["error"]["kind"], "in_failed_transaction",
        "a failure that is no refusal leaves the transaction as Postgres leaves it: {failed}"
    );
    assert_eq!(
        failed["committed"]["error"]["kind"],
        "in_failed_transaction"
    );
    assert_eq!(
        test.one::<i64>("SELECT count(*) FROM loan_service.items WHERE title = 'lost'")
            .await,
        0
    );
    assert_eq!(test.fallbacks(LOANS).await, recorded);

    let legacy = test
        .ask(
            LOANS,
            "legacy",
            json!({"sql": "SELECT count(*)::int AS n FROM stock_service.items"}),
        )
        .await;
    assert_eq!(legacy, json!({"rows": [{"n": 1}]}));
    let batch = test
        .ask(
            LOANS,
            "batch",
            json!({"statements": [
                "INSERT INTO items (title) VALUES ('batched')",
                "INSERT INTO stock_service.audited (body) VALUES ('from loans')",
            ]}),
        )
        .await;
    assert_eq!(batch, json!({"counts": [1, 1]}));
    let isolated = test
        .ask(
            LOANS,
            "batch",
            json!({"statements": [
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                "INSERT INTO items (title) SELECT current_setting('transaction_isolation')",
            ]}),
        )
        .await;
    assert_eq!(
        isolated,
        json!({"counts": [0, 1]}),
        "what Postgres runs in no savepoint runs in the transaction itself"
    );
    assert_eq!(
        test.one::<i64>("SELECT count(*) FROM loan_service.items WHERE title = 'serializable'")
            .await,
        1
    );
    let concurrently = test
        .ask(
            LOANS,
            "legacy_execute",
            json!({"sql": "CREATE INDEX CONCURRENTLY assets_body ON stock_service.items (body)"}),
        )
        .await;
    assert_eq!(
        concurrently,
        json!({"rc": 0}),
        "a statement no transaction can hold falls back on the session"
    );
    assert_eq!(
        rows(&test.query(LOANS, "SELECT current_user::text AS who").await)[0]["who"],
        loans.as_str(),
        "and the next session is the role's again"
    );

    test.deploy_crud(STOCKS, &["items", "audited", "extras", "more"])
        .await;
    assert_eq!(test.owner_of("stock_service.more").await, stocks);
    assert_eq!(
        test.fallbacks(STOCKS).await,
        0.0,
        "a deploy changes the schema as the role and is refused nothing"
    );

    let recorded = test.fallbacks(STOCKS).await;
    let (status, audited) = test
        .crud(
            Method::POST,
            STOCKS,
            "/audited",
            Some(json!({"body": "copied"})),
        )
        .await;
    assert_eq!(
        status, 200,
        "a data route whose trigger writes another service's table: {audited}"
    );
    assert!(test.fallbacks(STOCKS).await > recorded);
    assert_eq!(
        test.one::<i64>("SELECT count(*) FROM loan_service.items WHERE title = 'copied'")
            .await,
        1
    );

    test.deploy_hooked("hooked-service").await;
    let hooked = test.role("hooked-service").await;
    assert_eq!(
        test.migrated_by("hooked_service").await,
        (hooked.clone(), platform_role.clone()),
        "the first deploy of a service runs its hook as the role made for it"
    );
    for relation in ["hooked_service.items", "hooked_service.migrated"] {
        assert_eq!(test.owner_of(relation).await, hooked, "{relation}");
    }
    assert_eq!(test.fallbacks("hooked-service").await, 0.0);

    let recorded = test.fallbacks(LOANS).await;
    let read = test
        .read_only(LOANS, "SELECT current_user::text AS who")
        .await;
    assert_eq!(
        rows(&read)[0]["who"],
        test.read_only_role.as_str(),
        "a read-only query runs as the platform's role for them, as before"
    );
    let refused = test
        .read_only(LOANS, "SELECT count(*) FROM stock_service.items")
        .await;
    assert!(
        refusal(&refused).contains("permission denied"),
        "what the read-only role is refused is refused: {refused}"
    );
    let setting = test
        .read_only(
            LOANS,
            "SELECT current_setting('transaction_read_only') AS read_only",
        )
        .await;
    assert_eq!(
        rows(&setting)[0]["read_only"],
        "on",
        "in a transaction that stays read-only"
    );
    assert_eq!(
        test.fallbacks(LOANS).await,
        recorded,
        "and never run again as the platform"
    );
}

#[tokio::test]
async fn db_roles_enforce_isolates_schemas() {
    let Some(test) = Modes::start("enforce_platform_ro").await else {
        return;
    };
    let read_only = test.db.role("enforce_reports_ro");
    test.sql(&format!("CREATE ROLE {read_only} NOLOGIN")).await;
    test.set_policy("warn", json!({})).await;
    test.deploy_guest(LOANS, 0).await;
    test.deploy_crud(STOCKS, &["items", "audited"]).await;
    test.triggers().await;
    let (status, web) = test
        .deploy(&json!({"name": "site", "version": "0.1.0", "mode": "web", "tables": []}))
        .await;
    assert_eq!(status, 200, "{web}");
    let (status, bound) = test
        .db_role(
            Method::PUT,
            LOANS,
            "",
            Some(json!({"read_only_role": read_only})),
        )
        .await;
    assert_eq!(status, 200, "{bound}");
    let (status, body) = test.policy("enforce", json!({"login_pool_max": 2})).await;
    assert_eq!(
        status, 200,
        "every service that has a schema has a verified role, and a web app has none: {body}"
    );
    let (loans, stocks) = (test.role(LOANS).await, test.role(STOCKS).await);

    let who = test
        .query(
            LOANS,
            "SELECT current_user::text AS who, session_user::text AS login",
        )
        .await;
    assert_eq!(
        rows(&who)[0],
        json!({"who": loans, "login": loans}),
        "the session logged in as the role"
    );
    assert_eq!(
        test.ask(
            LOANS,
            "execute",
            json!({"sql": "INSERT INTO items (title) VALUES ('own')"})
        )
        .await,
        json!({"affected": 1})
    );
    assert_eq!(
        test.metric(
            "cufflink_db_pool_max",
            &[("pool", &format!("login:{}:{LOANS}", test.tenant.slug))]
        )
        .await,
        2.0,
        "through a pool of its own, as large as login_pool_max"
    );

    let across = test.query(LOANS, "SELECT * FROM stock_service.items").await;
    assert!(
        refusal(&across).contains("permission denied for schema stock_service"),
        "{across}"
    );
    for escape in [
        "RESET ROLE".to_string(),
        "SET ROLE NONE".to_string(),
        "RESET SESSION AUTHORIZATION".to_string(),
    ] {
        let escaped = test
            .ask(
                LOANS,
                "transaction",
                json!({"statements": [
                    escape,
                    "SELECT current_user::text AS who",
                    "SELECT count(*)::int AS n FROM stock_service.items",
                ]}),
            )
            .await;
        assert_eq!(
            escaped["statements"][1],
            json!({"rows": [{"who": loans}]}),
            "{escape} gives back the role that logged in: {escaped}"
        );
        assert!(
            refusal(&escaped["statements"][2]).contains("permission denied"),
            "{escape}: {escaped}"
        );
    }
    let platform_role: String = test.one("SELECT current_user::text").await;
    for escape in [
        format!("SET ROLE \"{platform_role}\""),
        format!("SET SESSION AUTHORIZATION \"{platform_role}\""),
        format!("SET ROLE \"{stocks}\""),
        format!("SELECT set_config('role', '{platform_role}', false)"),
        format!("ALTER ROLE \"{loans}\" SUPERUSER"),
        "CREATE ROLE squatter".to_string(),
        "CREATE TABLE public.squat (id int)".to_string(),
        "SELECT pg_read_file('/etc/passwd')".to_string(),
    ] {
        let escaped = test.ask(LOANS, "execute", json!({ "sql": escape })).await;
        assert!(
            refusal(&escaped).contains("permission denied"),
            "{escape}: {escaped}"
        );
    }
    let batch = test
        .ask(
            LOANS,
            "batch",
            json!({"statements": [
                "RESET ROLE",
                "INSERT INTO stock_service.audited (body) VALUES ('from loans')",
            ]}),
        )
        .await;
    assert!(refusal(&batch).contains("permission denied"), "{batch}");
    assert_eq!(test.fallbacks(LOANS).await, 0.0, "nothing falls back");

    let (status, created) = test
        .crud(Method::POST, STOCKS, "/items", Some(json!({"body": "x"})))
        .await;
    assert_eq!(status, 200, "{created}");
    assert_eq!(created["body"], format!("{stocks} of {stocks}"));
    let created_id = created["id"].as_str().unwrap_or_default();
    let (status, read) = test
        .crud(Method::GET, STOCKS, &format!("/items/{created_id}"), None)
        .await;
    assert_eq!(status, 200, "{read}");
    assert_eq!(read["body"], created["body"]);
    let (status, refused) = test
        .crud(
            Method::POST,
            STOCKS,
            "/audited",
            Some(json!({"body": "copied"})),
        )
        .await;
    assert_ne!(
        status, 200,
        "a trigger that writes another service's table is refused: {refused}"
    );
    assert_eq!(
        test.one::<i64>("SELECT count(*) FROM stock_service.audited")
            .await,
        0
    );

    let enqueued = test
        .ask(
            LOANS,
            "execute",
            json!({"sql": "SELECT _cufflink.outbox_enqueue_own(
                ARRAY['event'], ARRAY['dw.test.created'],
                ARRAY['\\x7b7d'::bytea], ARRAY['[]'::jsonb], ARRAY[gen_random_uuid()])"}),
        )
        .await;
    assert!(enqueued["error"].is_null(), "{enqueued}");
    let row = test
        .tenant_db
        .query_one(
            "SELECT service, source_role::text FROM _cufflink.outbox WHERE subject = 'dw.test.created'",
            &[],
        )
        .await
        .expect("the row");
    assert_eq!(
        (row.get::<_, String>(0), row.get::<_, String>(1)),
        (LOANS.to_string(), loans.clone()),
        "the outbox takes the service from the role that enqueues, and records who did"
    );
    let forged = test
        .query(
            LOANS,
            "SELECT _cufflink.outbox_enqueue_many(
                ARRAY['event'], ARRAY['stock-service'], ARRAY['dw.test.forged'],
                ARRAY['\\x7b7d'::bytea], ARRAY['[]'::jsonb], ARRAY[gen_random_uuid()])",
        )
        .await;
    assert!(
        refusal(&forged).contains("permission denied"),
        "and nothing enqueues in another service's name: {forged}"
    );
    let reached = test
        .query(LOANS, "SELECT count(*)::int AS n FROM _cufflink.outbox")
        .await;
    assert!(
        refusal(&reached).contains("permission denied"),
        "and gives it nothing to read: {reached}"
    );

    let read = test
        .read_only(
            LOANS,
            "SELECT current_user::text AS who, session_user::text AS login",
        )
        .await;
    assert_eq!(
        rows(&read)[0],
        json!({"who": read_only, "login": loans}),
        "a read-only query switches to the role the binding names"
    );
    assert_eq!(
        rows(
            &test
                .api_query(
                    LOANS,
                    "SELECT current_user::text AS who, session_user::text AS login, \
                     current_setting('transaction_read_only') AS read_only",
                )
                .await
        )[0],
        json!({"who": read_only, "login": loans, "read_only": "on"}),
        "the query API runs as the service's read-only role, never the platform's"
    );
    let across = test
        .api_query(LOANS, "SELECT count(*) FROM stock_service.items")
        .await;
    assert!(refusal(&across).contains("permission denied"), "{across}");
    test.deploy_guest("plain-service", 0).await;
    let plain = test.role("plain-service").await;
    assert_eq!(
        rows(
            &test
                .api_query("plain-service", "SELECT current_user::text AS who")
                .await
        )[0]["who"],
        plain.as_str(),
        "with no read-only role bound the query API runs as the service's role"
    );
    let read = test
        .read_only("plain-service", "SELECT current_user::text AS who")
        .await;
    assert_eq!(
        rows(&read)[0]["who"],
        plain.as_str(),
        "a service deployed under enforce has its role at once, and with no read-only role \
         bound its read-only queries stay its own"
    );
    let setting = test
        .read_only(
            "plain-service",
            "SELECT current_setting('transaction_read_only') AS read_only",
        )
        .await;
    assert_eq!(rows(&setting)[0]["read_only"], "on");

    test.deploy_hooked("hooked-service").await;
    let hooked = test.role("hooked-service").await;
    assert_eq!(
        test.migrated_by("hooked_service").await,
        (hooked.clone(), hooked.clone()),
        "the first deploy of a service runs its hook on a login as the role made for it"
    );
    for relation in ["hooked_service.items", "hooked_service.migrated"] {
        assert_eq!(test.owner_of(relation).await, hooked, "{relation}");
    }

    test.deploy_guest(LOANS, 1).await;
    test.deploy_crud(STOCKS, &["items", "audited", "extras"])
        .await;
    assert_eq!(
        test.owner_of("stock_service.extras").await,
        stocks,
        "a deploy under enforce changes the schema as the role"
    );
    let (status, created) = test
        .crud(
            Method::POST,
            STOCKS,
            "/extras",
            Some(json!({"body": "new"})),
        )
        .await;
    assert_eq!(status, 200, "{created}");

    let before = test.binding(LOANS).await;
    let (status, rotated) = test.db_role(Method::POST, LOANS, "/rotate", None).await;
    assert_eq!(status, 200, "{rotated}");
    assert_eq!(rotated["report"]["verified"], true, "{rotated}");
    assert_eq!(rotated["binding"]["read_only_role"], read_only);
    assert_ne!(rotated["binding"]["updated_at"], before["updated_at"]);
    assert!(
        !rotated.to_string().contains("password\":"),
        "no answer carries a password: {rotated}"
    );
    assert_eq!(
        rows(
            &test
                .query(LOANS, "SELECT session_user::text AS login")
                .await
        )[0]["login"],
        loans.as_str(),
        "after a rotation the service logs in with the new password"
    );

    let (status, body) = test
        .policy("enforce", json!({"readers": ["a_reader_nobody_made"]}))
        .await;
    assert_eq!(status, 200, "{body}");
    test.deploy_crud("late-service", &["items"]).await;
    assert_eq!(
        test.binding("late-service").await,
        Value::Null,
        "a role that could not be made"
    );
    let (status, refused) = test
        .crud(
            Method::POST,
            "late-service",
            "/items",
            Some(json!({"body": "x"})),
        )
        .await;
    assert_eq!(status, 503, "{refused}");
    assert_eq!(
        refused["error"],
        "service late-service has no database role; \
         `cufflink services db-role verify` says what it lacks"
    );
    assert_eq!(
        test.api_query("late-service", "SELECT 1 AS one").await,
        json!({"status": 503, "error": refused["error"]}),
        "nor does the query API run a service without a role as the platform"
    );
    assert!(test.violations("denied", "no_verified_binding").await >= 1.0);
    let (status, created) = test
        .crud(Method::POST, STOCKS, "/items", Some(json!({"body": "x"})))
        .await;
    assert_eq!(status, 200, "the other services run on: {created}");
    test.set_policy("warn", json!({})).await;
    let (status, made) = test
        .db_role(Method::PUT, "late-service", "", Some(json!({})))
        .await;
    assert_eq!(status, 200, "{made}");
    assert_eq!(made["report"]["verified"], true, "{made}");
    test.set_policy("enforce", json!({})).await;
    let (status, created) = test
        .crud(
            Method::POST,
            "late-service",
            "/items",
            Some(json!({"body": "x"})),
        )
        .await;
    assert_eq!(status, 200, "a role made is a role used: {created}");

    let adopted = test.db.role("adopted");
    test.sql(&format!("CREATE ROLE {adopted} LOGIN PASSWORD 'pw'"))
        .await;
    let (status, bound) = test
        .db_role(
            Method::PUT,
            "late-service",
            "",
            Some(json!({"role": adopted, "password": "pw"})),
        )
        .await;
    assert_eq!(status, 200, "{bound}");
    let (status, refused) = test
        .db_role(Method::POST, "late-service", "/rotate", None)
        .await;
    assert_eq!(
        status, 422,
        "the password of a role an admin bound is the admin's: {refused}"
    );
    let audited: i64 = test
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM audit_log WHERE action = 'db_role.rotate' AND tenant_id = $1",
            &[&test.tenant.id],
        )
        .await
        .expect("audit")
        .get(0);
    assert_eq!(audited, 1);
}

async fn reports_answer(test: &Modes, mode: &str) {
    let read = test
        .read_only(REPORTS, "SELECT title, stock FROM reporting.item_overview")
        .await;
    assert_eq!(
        read["rows"],
        json!([{"title": "shared", "stock": "shared"}]),
        "{mode}: a view over several schemas answers under its owner's rights"
    );
    let totals = test
        .read_only(
            REPORTS,
            "SELECT service, items::int AS items FROM reporting.service_totals ORDER BY service",
        )
        .await;
    assert_eq!(
        totals["rows"],
        json!([
            {"service": "loan-service", "items": 1},
            {"service": "reports-service", "items": 0},
            {"service": "stock-service", "items": 1},
        ]),
        "{mode}"
    );
    let who = test
        .read_only(REPORTS, "SELECT current_user::text AS who")
        .await;
    assert_eq!(rows(&who)[0]["who"], test.read_only_role.as_str(), "{mode}");
    let beneath = test
        .read_only(REPORTS, "SELECT title FROM loan_service.items")
        .await;
    assert!(
        refusal(&beneath).contains("permission denied"),
        "{mode}: the tables beneath the views stay out of reach: {beneath}"
    );
}

fn migrated_manifest(migrations: &[(u32, &str, &str)]) -> Value {
    let mut manifest = crud_manifest(LOANS, &["items"]);
    manifest["migrations"] = migrations
        .iter()
        .map(|(version, name, sql)| json!({"version": version, "name": name, "sql": sql}))
        .collect();
    manifest
}

#[tokio::test]
async fn db_roles_enforce_runs_sql_migrations_as_the_role() {
    const RAN_BY: (u32, &str, &str) = (
        1,
        "ran_by",
        "CREATE TABLE ran_by AS SELECT current_user::text AS who, session_user::text AS login;",
    );
    const REACHES_ACROSS: (u32, &str, &str) = (
        2,
        "reaches_across",
        "CREATE TABLE stock_service.squat (id int);",
    );
    let Some(test) = Modes::start("migrations_platform_ro").await else {
        return;
    };
    test.deploy_crud(STOCKS, &["items"]).await;
    test.set_policy("warn", json!({})).await;
    let role_made = format!(
        "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{}')",
        test.role(STOCKS).await
    );
    let planned = test
        .plan(&crud_manifest(STOCKS, &["items", "audited"]))
        .await;
    assert_eq!(planned["skipped"], false, "{planned}");
    assert!(
        !test.one::<bool>(&role_made).await,
        "a plan makes no role for a service that has none"
    );
    test.deploy_crud(STOCKS, &["items", "audited"]).await;
    assert!(test.one::<bool>(&role_made).await, "the deploy makes it");
    test.set_policy("enforce", json!({})).await;

    let planned = test.plan(&migrated_manifest(&[RAN_BY])).await;
    assert_eq!(
        planned["plan"]["sql_migrations"]["pending"][0]["version"], 1,
        "{planned}"
    );

    let (status, body) = test.deploy(&migrated_manifest(&[RAN_BY])).await;
    assert_eq!(status, 200, "{body}");
    let loans = test.role(LOANS).await;
    let ran_by = test
        .tenant_db
        .query_one("SELECT who, login FROM loan_service.ran_by", &[])
        .await
        .expect("the migration's row");
    assert_eq!(
        (ran_by.get::<_, String>(0), ran_by.get::<_, String>(1)),
        (loans.clone(), loans.clone()),
        "the migration logged in as the service's role"
    );
    assert_eq!(test.owner_of("loan_service.ran_by").await, loans);

    let (status, refused) = test
        .deploy(&migrated_manifest(&[RAN_BY, REACHES_ACROSS]))
        .await;
    assert_ne!(status, 200, "{refused}");
    assert!(
        refused
            .to_string()
            .contains("permission denied for schema stock_service"),
        "{refused}"
    );
    assert!(
        test.one::<bool>("SELECT to_regclass('stock_service.squat') IS NULL")
            .await,
        "a migration reaches no schema but its service's"
    );
}

#[tokio::test]
async fn db_roles_enforce_outbox_rows_are_the_roles_own() {
    use futures_util::StreamExt;
    use std::time::Duration;

    let Some(nats_url) = common::infra()
        .await
        .and_then(|infra| infra.require_nats().map(str::to_string))
    else {
        return;
    };
    let Some(test) = Modes::start("outbox_platform_ro").await else {
        return;
    };
    let slug = test.tenant.slug.clone();
    test.set_policy("warn", json!({})).await;
    test.deploy_crud(STOCKS, &["items"]).await;
    test.deploy_guest(LOANS, 0).await;
    test.set_policy("enforce", json!({})).await;
    let (status, body) = test
        .platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{slug}/policies"),
            &test.admin,
            Some(&json!({"policies": {"outbox": {"mode": "enforce"}}})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let stocks = test.role(STOCKS).await;
    assert!(
        !test
            .one::<bool>(&format!(
                "SELECT has_function_privilege('{stocks}', \
                 '_cufflink.outbox_enqueue_many(text[], text[], text[], bytea[], jsonb[], uuid[])', \
                 'EXECUTE')"
            ))
            .await,
        "the role may not name the service it writes for"
    );

    let nats = async_nats::connect(nats_url).await.expect("nats");
    let mut heard = nats
        .subscribe(format!("dw.{slug}.>"))
        .await
        .expect("subscribe");
    let (status, created) = test
        .crud(Method::POST, STOCKS, "/items", Some(json!({"body": "x"})))
        .await;
    assert_eq!(status, 200, "{created}");
    let logs = format!("dw.{slug}.logs");
    let mut subjects = Vec::new();
    let published = tokio::time::timeout(Duration::from_secs(30), async {
        while !(subjects.contains(&logs)
            && subjects.iter().any(|s: &String| s.ends_with(".created")))
        {
            let message = heard.next().await.expect("a message");
            subjects.push(message.subject.to_string());
        }
    })
    .await;
    assert!(
        published.is_ok(),
        "the write's event and audit record are published: {subjects:?}"
    );

    let forged = test
        .ask(
            LOANS,
            "execute",
            json!({"sql": format!(
                "SELECT _cufflink.outbox_enqueue_own(ARRAY['audit'], ARRAY['{logs}'], \
                 ARRAY['\\x7b7d'::bytea], ARRAY['[]'::jsonb], ARRAY[gen_random_uuid()])"
            )}),
        )
        .await;
    assert!(forged["error"].is_null(), "{forged}");
    let js = async_nats::jetstream::new(nats);
    let letters = common::outbox::dead_letters(&js, &test.tenant, 1).await;
    let letter = &letters[0];
    assert!(
        letter.subject.starts_with(&format!("cufflink.dlq.{slug}."))
            && letter.subject.ends_with(".outbox.unauthenticated"),
        "the first letter is the guest's row, and the platform's record none: {}",
        letter.subject
    );
    assert_eq!(letter.original, logs);
    assert!(
        letter.reason.contains("carries no MAC"),
        "{}",
        letter.reason
    );
}

#[tokio::test]
async fn db_roles_reporting_views_replumb() {
    let Some(test) = Modes::start("reports_ro").await else {
        return;
    };
    let (owner, dashboard_ro) = (
        test.db.role("reporting_owner"),
        test.db.role("dashboard_ro"),
    );
    let reporting = |sql: &str| {
        sql.replace("{reporting_owner}", &owner)
            .replace("{reports_ro}", &test.read_only_role)
            .replace("{dashboard_ro}", &dashboard_ro)
    };
    test.sql(&reporting(include_str!("fixtures/db_roles/reporting.sql")))
        .await;
    test.set_policy("warn", json!({"readers": [owner, dashboard_ro]}))
        .await;
    test.deploy_guest(LOANS, 0).await;
    test.deploy_crud(STOCKS, &["items"]).await;
    test.deploy_guest(REPORTS, 0).await;
    test.sql(&reporting(include_str!(
        "fixtures/db_roles/reporting_views.sql"
    )))
    .await;
    let (status, bound) = test
        .db_role(
            Method::PUT,
            REPORTS,
            "",
            Some(json!({"read_only_role": test.read_only_role})),
        )
        .await;
    assert_eq!(status, 200, "{bound}");
    assert_eq!(bound["report"]["verified"], true, "{bound}");

    test.ask(
        LOANS,
        "execute",
        json!({"sql": "INSERT INTO items (title) VALUES ('shared')"}),
    )
    .await;
    let (status, created) = test
        .crud(
            Method::POST,
            STOCKS,
            "/items",
            Some(json!({"body": "shared"})),
        )
        .await;
    assert_eq!(status, 200, "{created}");

    for (reader, reads) in [
        (owner.as_str(), true),
        (dashboard_ro.as_str(), true),
        (test.read_only_role.as_str(), false),
    ] {
        for table in [
            "loan_service.items",
            "stock_service.items",
            "reports_service.items",
        ] {
            assert_eq!(
                test.one::<bool>(&format!(
                    "SELECT has_table_privilege('{reader}', '{table}', 'SELECT')"
                ))
                .await,
                reads,
                "{reader} on {table}: the readers read the tables, the read-only role only \
                 the views over them"
            );
        }
    }
    for view in ["item_overview", "item_overview_scoped", "service_totals"] {
        assert_eq!(test.owner_of(&format!("reporting.{view}")).await, owner);
    }

    reports_answer(&test, "warn").await;

    let mut as_dashboard: tokio_postgres::Config =
        test.tenant_db_url.parse().expect("tenant database url");
    as_dashboard.user(&dashboard_ro).password("dashboard");
    let (dashboard, connection) = as_dashboard
        .connect(tokio_postgres::NoTls)
        .await
        .expect("dashboard logs in");
    tokio::spawn(connection);
    let seen: i64 = dashboard
        .query_one(
            "SELECT (SELECT count(*) FROM loan_service.items)
                  + (SELECT count(*) FROM reporting.item_overview)",
            &[],
        )
        .await
        .expect("dashboard reads the tables and the views")
        .get(0);
    assert_eq!(seen, 2);

    test.deploy_crud(STOCKS, &["items", "later"]).await;
    test.sql(&format!(
        "SET ROLE {owner};
         CREATE VIEW reporting.later_items AS SELECT body FROM stock_service.later;
         RESET ROLE"
    ))
    .await;
    let later = test
        .read_only(
            REPORTS,
            "SELECT count(*)::int AS n FROM reporting.later_items",
        )
        .await;
    assert_eq!(
        later["rows"],
        json!([{"n": 0}]),
        "a table a later deploy makes is the readers' by default, and a view over it the \
         read-only role's"
    );
    assert_eq!(
        dashboard
            .query_one("SELECT count(*) FROM stock_service.later", &[])
            .await
            .expect("dashboard reads the later table")
            .get::<_, i64>(0),
        0
    );

    test.set_policy(
        "enforce",
        json!({"readers": [owner, dashboard_ro], "login_pool_max": 2}),
    )
    .await;
    reports_answer(&test, "enforce").await;
    let login = test
        .read_only(REPORTS, "SELECT session_user::text AS login")
        .await;
    assert_eq!(
        rows(&login)[0]["login"],
        test.role(REPORTS).await.as_str(),
        "under enforce the service logs in as its role and switches to the read-only one"
    );
    let scoped = test
        .ask(
            REPORTS,
            "transaction",
            json!({"statements": [
                "SELECT set_config('app.scope', 'shared', true) AS scope",
                "SELECT count(*)::int AS n FROM reporting.item_overview_scoped",
            ]}),
        )
        .await;
    assert!(
        refusal(&scoped["statements"][1]).contains("permission denied"),
        "the service's own role was given no view, only its read-only role: {scoped}"
    );
    assert_eq!(test.fallbacks(REPORTS).await, 0.0);
}
