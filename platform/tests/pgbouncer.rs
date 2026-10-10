use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_test_guests::Profile;
use serde_json::{json, Value};

const HANDLERS: [&str; 9] = [
    "setup",
    "session_state",
    "bump",
    "leak",
    "count",
    "legacy",
    "concurrently",
    "timeout",
    "timeout_in_transaction",
];

fn pgbouncer_url() -> Option<String> {
    match std::env::var("CUFFLINK_TEST_PGBOUNCER_URL") {
        Ok(url) => Some(url),
        Err(_) if std::env::var_os("CUFFLINK_TEST_REQUIRE_PGBOUNCER").is_some() => {
            panic!("CUFFLINK_TEST_REQUIRE_PGBOUNCER is set but CUFFLINK_TEST_PGBOUNCER_URL is not")
        }
        Err(_) => {
            eprintln!("skipped: CUFFLINK_TEST_PGBOUNCER_URL is unset (the test-pgbouncer tier)");
            None
        }
    }
}

fn manifest() -> Value {
    let paths = HANDLERS.map(|handler| format!("/{handler}"));
    let routes: Vec<(&str, &str)> = HANDLERS
        .iter()
        .zip(&paths)
        .map(|(handler, path)| (*handler, path.as_str()))
        .collect();
    let mut manifest = common::wasm_manifest("bouncer-svc", &routes);
    manifest["tables"] = json!([{
        "name": "notes",
        "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
            {"name": "title", "column_type": "text", "primary_key": false, "nullable": false, "auto_generate": false},
            {"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true}
        ]
    }]);
    manifest
}

async fn post(
    platform: &PlatformProcess,
    tenant: &common::Tenant,
    handler: &str,
    body: Value,
) -> (u16, Value) {
    let response = platform
        .http
        .post(platform.fn_url(tenant, "bouncer-svc", handler))
        .json(&body)
        .send()
        .await
        .expect("fn request");
    (
        response.status().as_u16(),
        response.json().await.unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn pgbouncer_transaction_mode() {
    let Some(pgbouncer) = pgbouncer_url() else {
        return;
    };
    let wasm = match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => return common::skip(reason),
        None => cufflink_test_guests::wasm("pgbouncer", Profile::Debug).expect("built guest"),
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let role = cufflink_db::parse_config(&pgbouncer)
        .expect("PgBouncer URL")
        .get_user()
        .expect("a user in the PgBouncer URL")
        .to_string();
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("TENANT_DATABASE_URL", pgbouncer.as_str()),
            ("TENANT_DB_POOLER", "pgbouncer_transaction"),
            ("MAX_CONNS_PER_TENANT", "4"),
            ("DB_READ_ONLY_ROLE", role.as_str()),
            ("WASM_TEST_ROUTE_BUDGET_SECS", "5"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform.deploy_module(&tenant, &manifest(), &wasm).await;
    assert_eq!(status, 200, "deploy through PgBouncer: {body}");
    assert_eq!(
        post(&platform, &tenant, "setup", json!({})).await,
        (200, json!({"ok": true}))
    );

    let bumps = (0..20).map(|i| {
        let (platform, tenant) = (&platform, &tenant);
        async move {
            post(
                platform,
                tenant,
                "bump",
                json!({"marker": format!("call-{i}")}),
            )
            .await
        }
    });
    for (status, body) in futures_util::future::join_all(bumps).await {
        assert_eq!(
            (status, body),
            (200, json!({"marker_kept": true})),
            "a transaction's SET LOCAL, advisory lock and savepoint on a shared server connection"
        );
    }
    assert_eq!(
        post(&platform, &tenant, "count", json!({})).await,
        (200, json!({"n": 20}))
    );

    assert_eq!(
        post(&platform, &tenant, "session_state", json!({})).await,
        (
            200,
            json!({
                "v2": "not_supported",
                "v2_role": "not_supported",
                "v2_reset": "not_supported",
                "v2_in_transaction": "not_supported",
                "set_local": "ok",
                "legacy": -1,
                "legacy_query": [],
                "execute_all": {"Err": true},
            })
        ),
        "session state is refused behind a transaction pooler"
    );

    for _ in 0..5 {
        let (status, body) = post(&platform, &tenant, "leak", json!({})).await;
        assert!(
            body["leaked"].is_null() || body["leaked"] == "",
            "a session SET leaked onto a shared server connection: {body}"
        );
        assert_eq!(status, 200, "{body}");
        assert!(
            body["marker"].is_null() || body["marker"] == "",
            "SET LOCAL leaked into another transaction: {body}"
        );
        let row = &body["read_only"]["rows"][0];
        assert!(
            row["m"].is_null() || row["m"] == "",
            "SET LOCAL leaked: {body}"
        );
        assert!(
            !row["sp"]
                .as_str()
                .unwrap_or_default()
                .contains("bouncer_svc"),
            "search_path leaked: {body}"
        );
    }

    assert_eq!(
        post(&platform, &tenant, "legacy", json!({})).await,
        (
            200,
            json!({
                "execute": 1,
                "query": [{"n": 5}],
                "execute_all": {"Ok": [1, 1]},
                "read_only": {"Ok": [{"one": 1}]},
            })
        )
    );
    assert_eq!(
        post(&platform, &tenant, "concurrently", json!({})).await,
        (200, json!({"v2": "not_supported", "legacy": -1}))
    );
    for handler in ["timeout", "timeout_in_transaction"] {
        assert_eq!(
            post(&platform, &tenant, handler, json!({})).await,
            (200, json!({"timeout": "query_canceled"})),
            "{handler}"
        );
    }

    let notes = format!("/svc/{}/bouncer-svc/notes", tenant.slug);
    let bearer = format!("ApiKey {}", tenant.api_key);
    let (status, created) = platform
        .send(
            reqwest::Method::POST,
            &notes,
            &bearer,
            Some(&json!({"title": "a"})),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    let id = created["id"].as_str().expect("id").to_string();
    let (status, listed) = platform
        .send(reqwest::Method::GET, &notes, &bearer, None)
        .await;
    assert_eq!(status, 200, "{listed}");
    assert_eq!(listed["pagination"]["total_records"], 1, "{listed}");
    let (status, updated) = platform
        .send(
            reqwest::Method::PUT,
            &format!("{notes}/{id}"),
            &bearer,
            Some(&json!({"title": "b"})),
        )
        .await;
    assert_eq!((status, &updated["title"]), (200, &json!("b")), "{updated}");
    let (status, deleted) = platform
        .send(
            reqwest::Method::DELETE,
            &format!("{notes}/{id}"),
            &bearer,
            None,
        )
        .await;
    assert_eq!(status, 200, "{deleted}");
}

#[tokio::test]
async fn pgbouncer_enforce_logs_in_past_the_pooler() {
    let Some(pgbouncer) = pgbouncer_url() else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let kek = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [7u8; 32]);
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("TENANT_DATABASE_URL", pgbouncer.as_str()),
            ("TENANT_DB_POOLER", "pgbouncer_transaction"),
            ("CUFFLINK_SECRETS_KEK", &kek),
            ("CUFFLINK_SECRETS_KEK_ID", "test"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let policy = |mode: &'static str| {
        let (platform, admin, slug) = (&platform, &admin, &tenant.slug);
        async move {
            platform
                .send(
                    reqwest::Method::PUT,
                    &format!("/api/tenants/{slug}/policies"),
                    admin,
                    Some(&json!({"policies": {"db_roles": {"mode": mode}}})),
                )
                .await
        }
    };
    assert_eq!(policy("warn").await.0, 200);
    let manifest = json!({
        "name": "notes-svc",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{
            "name": "notes",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "title", "column_type": "text", "primary_key": false, "nullable": false, "auto_generate": false}
            ]
        }],
        "publishes": [],
    });
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy: {body}");
    let (status, binding) = platform
        .send(
            reqwest::Method::GET,
            &format!("/api/tenants/{}/services/notes-svc/db-role", tenant.slug),
            &admin,
            None,
        )
        .await;
    assert_eq!(status, 200, "{binding}");
    assert_eq!(
        binding["binding"]["verify_report"]["verified"], true,
        "the login it verifies with is the one its sessions make: {binding}"
    );
    let (status, body) = policy("enforce").await;
    assert_eq!(status, 200, "{body}");

    let notes = format!("/svc/{}/notes-svc/notes", tenant.slug);
    let bearer = format!("ApiKey {}", tenant.api_key);
    let (status, created) = platform
        .send(
            reqwest::Method::POST,
            &notes,
            &bearer,
            Some(&json!({"title": "past the pooler"})),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    let role = binding["binding"]["role"].as_str().expect("role");
    let (server, connection) = tokio_postgres::connect(
        &common::infra::with_dbname(
            &db.url,
            &cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(&tenant.slug),
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("the tenant database");
    tokio::spawn(connection);
    let row = server
        .query_one(
            "SELECT (SELECT count(*) FROM pg_stat_activity WHERE usename = $1),
                    EXISTS (SELECT 1 FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole,
                                  unnest(s.setconfig) AS setting
                            WHERE r.rolname = $1 AND starts_with(setting, 'statement_timeout='))",
            &[&role],
        )
        .await
        .expect("the role's connections and defaults");
    assert!(
        row.get::<_, i64>(0) >= 1,
        "the service's login is a connection of the server's own"
    );
    assert!(
        row.get::<_, bool>(1),
        "and a login as the role that sets no timeout, through the pooler or by hand, is given the platform's"
    );
}

#[tokio::test]
async fn an_outbox_is_relayed_through_a_transaction_pooler() {
    use futures_util::StreamExt;
    use std::time::Duration;

    let Some(pgbouncer) = pgbouncer_url() else {
        return;
    };
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
        &[
            ("TENANT_DATABASE_URL", pgbouncer.as_str()),
            ("TENANT_DB_POOLER", "pgbouncer_transaction"),
            ("MAX_CONNS_PER_TENANT", "4"),
            ("OUTBOX_RELAY_POLL_MS", "60000"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let mut notes = manifest();
    notes["custom_routes"] = json!([]);
    notes["mode"] = json!("crud");
    let (status, body) = platform.deploy(&tenant, &notes).await;
    assert_eq!(status, 200, "deploy through PgBouncer: {body}");
    let (status, body) = platform
        .send(
            reqwest::Method::PUT,
            &format!("/api/tenants/{}/policies", tenant.slug),
            &admin,
            Some(&json!({"policies": {"outbox": {"mode": "enforce"}}})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let mut heard = async_nats::connect(nats_url)
        .await
        .expect("nats")
        .subscribe(format!("dw.{}.bouncer-svc.>", tenant.slug))
        .await
        .expect("subscribe");

    let path = format!("/svc/{}/bouncer-svc/notes", tenant.slug);
    let bearer = format!("ApiKey {}", tenant.api_key);
    for n in 1..=5 {
        let (status, created) = platform
            .send(
                reqwest::Method::POST,
                &path,
                &bearer,
                Some(&json!({"title": format!("note {n}")})),
            )
            .await;
        assert_eq!(status, 200, "{created}");
    }
    let mut titles = Vec::new();
    while titles.len() < 5 {
        let event = tokio::time::timeout(Duration::from_secs(30), heard.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} of 5 events within 30 s, a look being a minute away as it is set\n{}",
                    titles.len(),
                    platform.output_tail()
                )
            })
            .expect("a message");
        let event: Value = serde_json::from_slice(&event.payload).expect("json");
        titles.push(event["data"]["title"].clone());
    }
    let written: Vec<Value> = (1..=5).map(|n| json!(format!("note {n}"))).collect();
    assert_eq!(titles, written, "in the order they were written");

    let database = common::legacy::tenant_database(&db, &tenant).await;
    let listening: i64 = database
        .query_one(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND query ILIKE 'LISTEN %'",
            &[],
        )
        .await
        .expect("activity")
        .get(0);
    assert_eq!(listening, 0, "nothing listens behind a pooler");
}
