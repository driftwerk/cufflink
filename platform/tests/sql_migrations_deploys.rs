use cufflink_testkit as common;

use base64::Engine;
use common::{legacy, Deployment};
use cufflink_migrate::{checksum, legacy_table_lock_key, TRY_LOCK};
use cufflink_types::SqlMigration;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio_postgres::Client;

const HISTORY: &str = "loan_service.schema_history";

fn migration(version: u32, name: &str, sql: &str) -> SqlMigration {
    SqlMigration {
        version,
        name: name.into(),
        sql: sql.into(),
        no_transaction: SqlMigration::declares_no_transaction(sql),
    }
}

fn loan_service(migrations: &[SqlMigration], history: Value) -> Value {
    json!({
        "name": "loan-service", "version": "0.1.0", "mode": "crud", "publishes": [],
        "tables": [{"name": "loan_tags", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
            {"name": "tag", "column_type": "text", "primary_key": false, "nullable": false}
        ]}],
        "migrations": migrations,
        "migration_history": history,
    })
}

fn adopted_loan_service(migrations: &[SqlMigration], history: Value) -> Value {
    let column = |name: &str, column_type: Value| json!({"name": name, "column_type": column_type, "primary_key": name == "id", "nullable": false});
    json!({
        "name": "loan-service", "version": "0.1.0", "mode": "crud", "publishes": [],
        "schema": "adopted",
        "tables": [{"name": "loans", "columns": [
            column("id", json!("uuid")),
            column("book_id", json!("uuid")),
            column("memo", json!("text")),
            column("loan_kind", json!({"varchar": {"length": 50}})),
            column("created_at", json!("timestamp_naive")),
        ]}],
        "migrations": migrations,
        "migration_history": history,
    })
}

fn legacy_history() -> Value {
    json!({"table": "schema_history", "lock": "legacy_table_hash"})
}

async fn start(env: &[(&str, &str)]) -> Option<Deployment> {
    let d = Deployment::start(env).await?;
    legacy::apply(&d.database, &[legacy::BOOTSTRAP]).await;
    Some(d)
}

async fn apply_with_refinery(d: &Deployment, migrations: &[SqlMigration]) {
    let name = cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(&d.tenant.slug);
    let mut client = legacy::connect(&common::with_dbname(&d.db.url, &name)).await;
    let migrations: Vec<refinery::Migration> = migrations
        .iter()
        .map(|m| refinery::Migration::unapplied(&m.file_stem(), &m.sql).expect("migration"))
        .collect();
    let mut runner = refinery::Runner::new(&migrations).set_grouped(false);
    runner.set_migration_table_name(HISTORY);
    runner.run_async(&mut client).await.expect("refinery run");
}

async fn holders(database: &Client, key: i64) -> i64 {
    database
        .query_one(
            "SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database \
             WHERE l.locktype = 'advisory' AND d.datname = current_database() \
             AND ((l.classid::bigint << 32) | l.objid::bigint) = $1",
            &[&key],
        )
        .await
        .expect("pg_locks")
        .get(0)
}

async fn deploy_behind_the_lock(
    d: &Deployment,
    body: Value,
) -> tokio::task::JoinHandle<(u16, Value)> {
    let url = format!("{}/api/services/deploy", d.platform.base_url);
    let auth = format!("ApiKey {}", d.tenant.api_key);
    let client = d.platform.http.clone();
    let waiting = tokio::spawn(async move {
        let response = client
            .post(url)
            .header("authorization", auth)
            .json(&body)
            .send()
            .await
            .expect("deploy");
        (
            response.status().as_u16(),
            response.json::<Value>().await.unwrap_or(Value::Null),
        )
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    while !trying_the_lock(&d.database).await {
        assert!(!waiting.is_finished(), "the deploy waits for the lock");
        assert!(Instant::now() < deadline, "the deploy never tried the lock");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!waiting.is_finished(), "the deploy waits for the lock");
    waiting
}

async fn trying_the_lock(database: &Client) -> bool {
    database
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid() AND query = $1)",
            &[&TRY_LOCK],
        )
        .await
        .expect("pg_stat_activity")
        .get(0)
}

async fn recorded(database: &Client, history: &str) -> Vec<(i32, String, String)> {
    database
        .query(
            &format!("SELECT version, name, checksum FROM {history} ORDER BY version"),
            &[],
        )
        .await
        .expect("history")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn history_of(migrations: &[SqlMigration]) -> Vec<(i32, String, String)> {
    migrations
        .iter()
        .map(|m| (m.version as i32, m.name.clone(), checksum(m).to_string()))
        .collect()
}

async fn exists(database: &Client, relation: &str) -> bool {
    database
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&relation])
        .await
        .expect("regclass")
        .get(0)
}

fn versions(list: &Value) -> Vec<u64> {
    list.as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["version"].as_u64())
        .collect()
}

#[tokio::test]
async fn sql_migrations_continue_refinery_history() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = legacy::loan_migrations();
    assert_eq!(
        local.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    apply_with_refinery(&d, &local[..2]).await;

    let body = d
        .deployed(json!({"manifest": loan_service(&local, legacy_history())}))
        .await;
    let migrations = &body["plan"]["sql_migrations"];
    assert_eq!(migrations["history"], HISTORY, "{body}");
    assert_eq!(migrations["mode"], "apply", "{body}");
    assert_eq!(migrations["verified"], 2, "{body}");
    assert_eq!(versions(&migrations["applied"]), [3], "{body}");
    assert_eq!(
        migrations["applied"][0]["checksum"],
        json!(checksum(&local[2])),
        "{body}"
    );
    assert_eq!(versions(&migrations["pending"]), Vec::<u64>::new());
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));
    assert!(exists(&d.database, "loan_service.idx_loans_renewals").await);
    assert!(exists(&d.database, "loan_service.loan_tags").await);

    let name = cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(&d.tenant.slug);
    let mut client = legacy::connect(&common::with_dbname(&d.db.url, &name)).await;
    let mut runner = refinery::Runner::new(&[]);
    runner.set_migration_table_name(HISTORY);
    let read_back = runner
        .get_applied_migrations_async(&mut client)
        .await
        .expect("refinery reads what the deploy wrote");
    assert_eq!(
        read_back
            .iter()
            .map(|m| (m.version(), m.checksum()))
            .collect::<Vec<_>>(),
        local
            .iter()
            .map(|m| (m.version, checksum(m)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        d.platform
            .metric(
                "cufflink_sql_migrations_applied_total",
                &[("tenant", &d.tenant.slug), ("service", "loan-service")]
            )
            .await,
        Some(1.0)
    );

    let mut changed = loan_service(&local, legacy_history());
    changed["tables"][0]["columns"]
        .as_array_mut()
        .expect("columns")
        .push(json!({"name": "colour", "column_type": "text", "primary_key": false, "nullable": true}));
    let again = d.deployed(json!({"manifest": changed})).await;
    assert_eq!(again["plan"]["sql_migrations"]["verified"], 3, "{again}");
    assert_eq!(
        versions(&again["plan"]["sql_migrations"]["applied"]),
        Vec::<u64>::new()
    );
}

#[tokio::test]
async fn sql_migrations_divergent_is_422() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = legacy::loan_migrations();
    apply_with_refinery(&d, &local[..2]).await;
    let mut edited = local.clone();
    edited[1].sql.push_str("\n-- edited after it was applied\n");

    let (status, body) = d
        .deploy(json!({"manifest": loan_service(&edited, legacy_history())}))
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    for named in [
        "V2".to_string(),
        checksum(&local[1]).to_string(),
        checksum(&edited[1]).to_string(),
    ] {
        assert!(error.contains(&named), "{named} in {error}");
    }
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..2])
    );
    assert!(!exists(&d.database, "loan_service.idx_loans_renewals").await);
    assert!(
        !exists(&d.database, "loan_service.loan_tags").await,
        "a history that is refused refuses the deploy before it changes anything"
    );

    let (status, body) = d
        .deploy(json!({"manifest": loan_service(&local[1..], legacy_history())}))
        .await;
    assert_eq!(
        status, 422,
        "an applied migration the manifest lacks: {body}"
    );
    assert!(
        body["error"].as_str().is_some_and(|e| e.contains("V1")),
        "{body}"
    );
}

#[tokio::test]
async fn sql_migrations_baseline_deferred_history() {
    let Some(d) = start(&[]).await else {
        return;
    };
    legacy::migrated_after_bootstrap(&d.database).await;
    legacy::apply(&d.database, &[legacy::DEFERRED_HISTORY]).await;
    let local = [migration(
        9,
        "books_on_loan_in_book_overview",
        "CREATE OR REPLACE VIEW reporting.book_overview_on_loan AS \
         SELECT * FROM reporting.book_overview WHERE book_status <> 'Available';\n",
    )];
    let deferred =
        |baseline: Value| json!({"table": "public.schema_history_deferred", "baseline": baseline});

    let (status, body) = d
        .deploy(json!({"manifest": loan_service(&local, deferred(Value::Null))}))
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(!exists(&d.database, "reporting.book_overview_on_loan").await);

    let body = d
        .deployed(json!({"manifest": loan_service(&local, deferred(json!(8)))}))
        .await;
    let migrations = &body["plan"]["sql_migrations"];
    assert_eq!(migrations["history"], "public.schema_history_deferred");
    assert_eq!(
        migrations["baselined"],
        json!((1..=8).collect::<Vec<i32>>()),
        "{body}"
    );
    assert_eq!(versions(&migrations["applied"]), [9], "{body}");
    assert!(exists(&d.database, "reporting.book_overview_on_loan").await);
    assert!(exists(&d.database, "reporting.book_overview").await);
    assert_eq!(
        recorded(&d.database, "public.schema_history_deferred")
            .await
            .len(),
        9
    );
}

#[tokio::test]
async fn legacy_lock_excludes_concurrent_runner() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = legacy::loan_migrations();
    let legacy_key = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        HISTORY.hash(&mut hasher);
        (hasher.finish() as i64).abs() % i64::MAX
    };
    assert_eq!(legacy_table_lock_key(HISTORY), legacy_key);
    d.database
        .execute("SELECT pg_advisory_lock($1)", &[&legacy_key])
        .await
        .expect("the legacy runner's lock");

    let waiting = deploy_behind_the_lock(
        &d,
        json!({"manifest": loan_service(&local, legacy_history())}),
    )
    .await;
    assert!(!exists(&d.database, "loan_service.loans").await);

    d.database
        .execute("SELECT pg_advisory_unlock($1)", &[&legacy_key])
        .await
        .expect("unlock");
    let (status, body) = waiting.await.expect("deploy task");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        versions(&body["plan"]["sql_migrations"]["applied"]),
        [1, 2, 3]
    );
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));
}

#[tokio::test]
async fn a_deploy_gives_up_on_a_lock_it_cannot_take() {
    let Some(d) = start(&[("SQL_MIGRATION_LOCK_WAIT_SECS", "2")]).await else {
        return;
    };
    d.database
        .execute(
            "SELECT pg_advisory_lock($1)",
            &[&legacy_table_lock_key(HISTORY)],
        )
        .await
        .expect("lock");
    let (status, body) = d
        .deploy(json!({"manifest": loan_service(&legacy::loan_migrations(), legacy_history())}))
        .await;
    assert_eq!(status, 503, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("migration lock of loan_service.schema_history")),
        "{body}"
    );
    assert!(!exists(&d.database, "loan_service.loans").await);
}

#[tokio::test]
async fn no_transaction_migration_concurrent_index() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let mut local = legacy::loan_migrations();
    local.push(migration(
        4,
        "loans_by_author",
        "-- cufflink:no-transaction\n\
         CREATE INDEX CONCURRENTLY idx_loans_author ON loan_service.loans(created_by);\n",
    ));
    assert!(local[3].no_transaction);
    let body = d
        .deployed(json!({"manifest": loan_service(&local, legacy_history())}))
        .await;
    assert_eq!(
        versions(&body["plan"]["sql_migrations"]["applied"]),
        [1, 2, 3, 4]
    );
    assert!(exists(&d.database, "loan_service.idx_loans_author").await);

    let mut in_transaction = local.clone();
    in_transaction.push(migration(
        5,
        "loans_by_update",
        "CREATE INDEX CONCURRENTLY idx_loans_updated ON loan_service.loans(updated_at);\n",
    ));
    let (status, body) = d
        .deploy(json!({"manifest": loan_service(&in_transaction, legacy_history())}))
        .await;
    assert_eq!(status, 500, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(error.contains("V5__loans_by_update"), "{error}");
    assert!(
        error.contains("cannot run inside a transaction block"),
        "{error}"
    );
    assert!(!exists(&d.database, "loan_service.idx_loans_updated").await);
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));
}

#[tokio::test]
async fn no_transaction_migration_recovers_invalid_index() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let mut local = legacy::loan_migrations();
    d.deployed(json!({"manifest": loan_service(&local, legacy_history())}))
        .await;
    d.database
        .batch_execute(
            "CREATE INDEX idx_loans_author ON loan_service.loans(created_by); \
             UPDATE pg_index SET indisvalid = false \
             WHERE indexrelid = 'loan_service.idx_loans_author'::regclass",
        )
        .await
        .expect("an index an interrupted build left invalid");
    local.push(migration(
        4,
        "loans_by_author",
        "-- cufflink:no-transaction\n\
         CREATE INDEX CONCURRENTLY idx_loans_author ON loan_service.loans(created_by);\n",
    ));
    let body = d
        .deployed(json!({"manifest": loan_service(&local, legacy_history())}))
        .await;
    let migrations = &body["plan"]["sql_migrations"];
    assert_eq!(versions(&migrations["applied"]), [4], "{body}");
    assert_eq!(
        migrations["recovered_indexes"],
        json!(["idx_loans_author"]),
        "{body}"
    );
    assert!(
        body["plan"]["warnings"][0]
            .as_str()
            .is_some_and(|w| w.contains("idx_loans_author")),
        "{body}"
    );
    let valid: bool = d
        .database
        .query_one(
            "SELECT indisvalid FROM pg_index \
             WHERE indexrelid = 'loan_service.idx_loans_author'::regclass",
            &[],
        )
        .await
        .expect("index")
        .get(0);
    assert!(valid);
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));
}

#[tokio::test]
async fn sql_migrations_verify_only_never_applies() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = legacy::loan_migrations();
    apply_with_refinery(&d, &local[..2]).await;
    legacy::record_ddl(&d.database).await;
    let verify_only =
        || json!({"table": "schema_history", "lock": "legacy_table_hash", "mode": "verify_only"});

    let body = d
        .deployed(json!({"manifest": loan_service(&local, verify_only())}))
        .await;
    let migrations = &body["plan"]["sql_migrations"];
    assert_eq!(migrations["mode"], "verify_only", "{body}");
    assert_eq!(migrations["verified"], 2, "{body}");
    assert_eq!(versions(&migrations["applied"]), Vec::<u64>::new());
    assert_eq!(versions(&migrations["pending"]), [3], "{body}");
    assert_eq!(
        body["plan"]["warnings"],
        json!([
            "migration_history.mode is verify_only: V3__renewal_index left to the legacy authority"
        ]),
        "{body}"
    );
    assert_eq!(
        legacy::recorded_ddl(&d.database, "loan_service").await,
        [
            "CREATE TABLE loan_service.loan_tags",
            "CREATE INDEX loan_service.loan_tags_pkey"
        ],
        "the manifest's own table, and nothing of the migrations"
    );
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..2])
    );
    assert!(!exists(&d.database, "loan_service.idx_loans_renewals").await);

    let mut edited = local.clone();
    edited[1].sql.push_str("\n-- edited\n");
    edited.push(migration(4, "later", "SELECT 1;\n"));
    let (status, body) = d
        .deploy(json!({"manifest": loan_service(&edited, verify_only())}))
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"].as_str().is_some_and(|e| e.contains("V2")),
        "{body}"
    );
}

#[tokio::test]
async fn migrations_run_after_the_tables_the_manifest_adds() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = [
        migration(
            1,
            "tag_rules",
            "ALTER TABLE loan_service.loan_tags \
                 ADD CONSTRAINT loan_tags_tag_check CHECK (tag <> '');\n\
             CREATE UNIQUE INDEX loan_tags_lower ON loan_service.loan_tags (lower(tag));\n",
        ),
        migration(
            2,
            "broken",
            "ALTER TABLE loan_service.no_such_table ADD COLUMN x int;\n",
        ),
    ];
    let body = d
        .deployed(
            json!({"manifest": loan_service(&local[..1], json!({"table": "schema_history"}))}),
        )
        .await;
    assert_eq!(versions(&body["plan"]["sql_migrations"]["applied"]), [1]);
    assert!(exists(&d.database, "loan_service.loan_tags_lower").await);
    let first = body["version"].clone();

    let more = [
        local[0].clone(),
        migration(
            2,
            "colours",
            "CREATE TABLE loan_service.colours (name text PRIMARY KEY);\n",
        ),
        migration(3, "broken", &local[1].sql),
    ];
    let (status, body) = d
        .deploy(json!({"manifest": loan_service(&more, json!({"table": "schema_history"}))}))
        .await;
    assert_eq!(status, 500, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(error.contains("V3__broken"), "{error}");
    assert!(
        error.contains("the 1 before it in this deploy stay applied"),
        "{error}"
    );
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&more[..2]));
    assert!(exists(&d.database, "loan_service.colours").await);
    let (_, health) = d
        .platform
        .get_json(&format!("/svc/{}/loan-service/_meta/health", d.tenant.slug))
        .await
        .expect("health");
    assert_eq!(health["version"], first, "{health}");
}

#[tokio::test]
async fn a_rollback_leaves_the_migrations_applied() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = legacy::loan_migrations();
    let first = d
        .deployed(json!({"manifest": loan_service(&local[..2], legacy_history())}))
        .await;
    d.deployed(json!({"manifest": loan_service(&local, legacy_history())}))
        .await;
    let (status, body) = d
        .platform
        .send(
            Method::POST,
            &format!(
                "/api/services/{}/rollback",
                first["service_id"].as_str().expect("id")
            ),
            &format!("ApiKey {}", d.tenant.api_key),
            Some(&json!({"target_version": first["version"]})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["plan"]["warnings"],
        json!(["SQL migrations are never rolled back: V3__renewal_index stay applied"]),
        "{body}"
    );
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));
    assert!(exists(&d.database, "loan_service.idx_loans_renewals").await);

    let again = d
        .deployed(json!({"manifest": loan_service(&local, legacy_history())}))
        .await;
    assert_eq!(again["plan"]["sql_migrations"]["verified"], 3, "{again}");
}

#[tokio::test]
async fn deploy_survives_client_disconnect() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let slow = [migration(
        1,
        "slow",
        "CREATE TABLE loan_service.first_half (id int);\n\
         SELECT pg_sleep(4);\n\
         CREATE TABLE loan_service.second_half (id int);\n",
    )];
    let abandoned = {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .expect("client");
        let url = format!("{}/api/services/deploy", d.platform.base_url);
        let auth = format!("ApiKey {}", d.tenant.api_key);
        let body = json!({"manifest": loan_service(&slow, json!({"table": "schema_history"}))});
        tokio::spawn(async move {
            client
                .post(url)
                .header("authorization", auth)
                .json(&body)
                .send()
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(1500)).await;
    abandoned.abort();
    let _ = abandoned.await;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let health = format!("/svc/{}/loan-service/_meta/health", d.tenant.slug);
    loop {
        let live = d.platform.get_json(&health).await;
        if live.is_some_and(|(status, _)| status == 200) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the abandoned deploy never activated its version"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(exists(&d.database, "loan_service.second_half").await);
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&slow));
    assert_eq!(
        d.platform
            .metric("cufflink_deploy_lock_dropped_total", &[])
            .await
            .unwrap_or(0.0),
        0.0,
        "the deploy released its lock itself"
    );

    let mut next = loan_service(&slow, json!({"table": "schema_history"}));
    next["tables"][0]["columns"]
        .as_array_mut()
        .expect("columns")
        .push(json!({"name": "colour", "column_type": "text", "primary_key": false, "nullable": true}));
    let (status, body) = d.deploy(json!({"manifest": next})).await;
    assert_eq!(status, 200, "the next deploy finds the lock free: {body}");
}

#[tokio::test]
async fn sql_migrations_missing_legacy_history_is_422() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = legacy::loan_migrations();
    apply_with_refinery(&d, &local[..2]).await;
    legacy::record_ddl(&d.database).await;
    let mistyped =
        |mode: &str| json!({"table": "schema_histroy", "lock": "legacy_table_hash", "mode": mode});
    let mut unnamed = adopted_loan_service(&local, Value::Null);
    unnamed
        .as_object_mut()
        .expect("manifest")
        .remove("migration_history");
    let refused = [
        (
            adopted_loan_service(&local, mistyped("apply")),
            "schema_histroy",
        ),
        (
            adopted_loan_service(&local, mistyped("verify_only")),
            "schema_histroy",
        ),
        (loan_service(&local, mistyped("apply")), "schema_histroy"),
        (
            loan_service(&local, mistyped("verify_only")),
            "schema_histroy",
        ),
        (unnamed, "refinery_schema_history"),
    ];
    for (manifest, table) in &refused {
        for query in ["", "?plan=true"] {
            let (status, body) = d.deploy_as(query, json!({"manifest": manifest})).await;
            assert_eq!(status, 422, "{query} {manifest}: {body}");
            let error = body["error"].as_str().expect("error");
            assert!(
                error.contains(&format!(
                    "migration_history.table: table '{table}' does not exist in schema \
                     'loan_service'"
                )),
                "{error}"
            );
        }
    }
    assert_eq!(
        legacy::ddl_statements(&d.database).await,
        legacy::DdlStatements::NONE,
        "a deploy that is refused for its history runs no DDL, committed or not"
    );
    for made in [
        "loan_service.schema_histroy",
        "loan_service.refinery_schema_history",
        "loan_service.loan_tags",
        "loan_service.idx_loans_renewals",
    ] {
        assert!(!exists(&d.database, made).await, "{made}");
    }
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..2])
    );
    let versions_activated: i64 =
        d.db.pool()
            .get()
            .await
            .expect("checkout")
            .query_one("SELECT count(*) FROM deployments", &[])
            .await
            .expect("deployments")
            .get(0);
    assert_eq!(versions_activated, 0);

    let body = d
        .deployed(json!({"manifest": adopted_loan_service(&local, legacy_history())}))
        .await;
    assert_eq!(body["plan"]["sql_migrations"]["verified"], 2, "{body}");
    assert_eq!(versions(&body["plan"]["sql_migrations"]["applied"]), [3]);
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));
}

#[tokio::test]
async fn a_history_is_begun_by_a_first_deploy_or_under_a_baseline() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let colours = [migration(
        1,
        "colours",
        "CREATE TABLE colours (name text PRIMARY KEY);\n",
    )];
    let tag_service = |migrations: &[SqlMigration], history: Value| {
        let mut manifest = loan_service(migrations, history);
        manifest["name"] = json!("tag-service");
        manifest
    };
    let body = d
        .deployed(json!({"manifest": tag_service(&colours, json!({"table": "schema_history"}))}))
        .await;
    assert_eq!(versions(&body["plan"]["sql_migrations"]["applied"]), [1]);
    assert_eq!(
        recorded(&d.database, "tag_service.schema_history").await,
        history_of(&colours)
    );
    assert!(exists(&d.database, "tag_service.colours").await);

    d.deployed(json!({"manifest": loan_service(&[], Value::Null)}))
        .await;
    let later = [migration(
        1,
        "tag_rules",
        "CREATE UNIQUE INDEX loan_tags_lower ON loan_service.loan_tags (lower(tag));\n",
    )];
    let (status, body) = d
        .deploy(json!({"manifest": loan_service(&later, json!({"table": "schema_history"}))}))
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(
        error.contains(
            "table 'schema_history' does not exist in schema 'loan_service', and schema \
             'loan_service' holds loan_tags"
        ) && error.contains("under a declared baseline"),
        "{error}"
    );
    assert!(!exists(&d.database, "loan_service.schema_history").await);
    assert!(!exists(&d.database, "loan_service.loan_tags_lower").await);

    let body = d.deployed(json!({"manifest": loan_service(&later, json!({"table": "schema_history", "baseline": 0}))}),
    )
    .await;
    assert_eq!(versions(&body["plan"]["sql_migrations"]["applied"]), [1]);
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&later));
    assert!(exists(&d.database, "loan_service.loan_tags_lower").await);

    let (status, body) = d
        .deploy(
            json!({"manifest": tag_service(&colours, json!({"table": "history", "baseline": 1}))}),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("V1__colours is at or under the baseline 1")),
        "{body}"
    );
    assert!(!exists(&d.database, "tag_service.history").await);
}

#[tokio::test]
async fn a_plan_and_a_verification_take_no_lock_and_run_no_ddl() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = legacy::loan_migrations();
    apply_with_refinery(&d, &local[..2]).await;
    legacy::record_ddl(&d.database).await;
    let key = legacy_table_lock_key(HISTORY);
    d.database
        .execute("SELECT pg_advisory_lock($1)", &[&key])
        .await
        .expect("the legacy runner's lock");
    let verify_only =
        json!({"table": "schema_history", "lock": "legacy_table_hash", "mode": "verify_only"});

    let started = std::time::Instant::now();
    let (status, plan) = d
        .deploy_as(
            "?plan=true",
            json!({"manifest": adopted_loan_service(&local, legacy_history())}),
        )
        .await;
    assert_eq!(status, 200, "{plan}");
    let verified = d
        .deployed(json!({"manifest": adopted_loan_service(&local, verify_only)}))
        .await;
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "neither waited for the lock: {:?}",
        started.elapsed()
    );
    for body in [&plan, &verified] {
        let migrations = &body["plan"]["sql_migrations"];
        assert_eq!(migrations["locked"], true, "{body}");
        assert_eq!(migrations["verified"], 2, "{body}");
        assert_eq!(versions(&migrations["pending"]), [3], "{body}");
        assert_eq!(versions(&migrations["applied"]), Vec::<u64>::new());
        assert!(
            body["plan"]["warnings"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .any(|warning| warning
                    == "the migration lock of loan_service.schema_history is held by another \
                        runner: what it applies is not in what was read"),
            "{body}"
        );
    }
    assert_eq!(
        holders(&d.database, key).await,
        1,
        "the lock has its holder and no other"
    );
    assert_eq!(
        legacy::ddl_statements(&d.database).await,
        legacy::DdlStatements::NONE,
        "an adopted schema under a history that is only verified sees no DDL"
    );
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..2])
    );

    d.database
        .execute("SELECT pg_advisory_unlock($1)", &[&key])
        .await
        .expect("unlock");
    let (status, plan) = d
        .deploy_as(
            "?plan=true",
            json!({"manifest": adopted_loan_service(&local, legacy_history())}),
        )
        .await;
    assert_eq!(status, 200, "{plan}");
    assert_eq!(
        plan["plan"]["sql_migrations"]["locked"],
        Value::Null,
        "{plan}"
    );
    assert_eq!(holders(&d.database, key).await, 0, "a plan takes no lock");
    assert_eq!(
        legacy::ddl_statements(&d.database).await,
        legacy::DdlStatements::NONE
    );
}

#[tokio::test]
async fn an_adopted_service_with_a_hook_and_a_verified_history_runs_no_ddl() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = legacy::loan_migrations();
    apply_with_refinery(&d, &local[..2]).await;
    legacy::record_ddl(&d.database).await;
    let mut manifest = adopted_loan_service(
        &local[..2],
        json!({"table": "schema_history", "lock": "legacy_table_hash", "mode": "verify_only"}),
    );
    manifest["on_migrate"] = json!("hook_query");
    let body = d
        .deployed(json!({
            "manifest": manifest,
            "wasm_base64": base64::engine::general_purpose::STANDARD
                .encode(common::guest::module_bytes()),
        }))
        .await;
    assert_eq!(body["schema_changes"], 0, "{body}");
    assert_eq!(body["plan"]["sql_migrations"]["verified"], 2, "{body}");
    assert_eq!(
        legacy::ddl_statements(&d.database).await,
        legacy::DdlStatements::NONE,
        "{body}"
    );
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..2])
    );
}

fn ledger_migrations() -> [SqlMigration; 2] {
    [
        migration(
            1,
            "loans",
            "CREATE TABLE loan_service.loans (id uuid PRIMARY KEY, memo text NOT NULL);\n",
        ),
        migration(
            2,
            "loan_state",
            "CREATE TYPE loan_service.loan_state AS ENUM ('open', 'closed');\n\
             ALTER TABLE loan_service.loans \
                 ADD COLUMN state loan_service.loan_state NOT NULL DEFAULT 'open';\n\
             CREATE TABLE loan_service.loan_notes (id uuid PRIMARY KEY, body text NOT NULL);\n",
        ),
    ]
}

fn ledger(migrations: &[SqlMigration], history: Value, stated: bool) -> Value {
    let column = |name: &str, primary_key: bool| {
        json!({"name": name, "column_type": if primary_key { "uuid" } else { "text" },
               "primary_key": primary_key, "nullable": false})
    };
    let loans = [column("id", true), column("memo", false)]
        .into_iter()
        .chain(stated.then(|| column("state", false)))
        .collect::<Vec<_>>();
    let notes = stated.then(
        || json!({"name": "loan_notes", "columns": [column("id", true), column("body", false)]}),
    );
    json!({
        "name": "loan-service", "version": "0.1.0", "mode": "crud", "publishes": [],
        "schema": "adopted",
        "tables": [{"name": "loans", "columns": loans}],
        "private_tables": notes.into_iter().collect::<Vec<_>>(),
        "migrations": migrations,
        "migration_history": history,
    })
}

fn errors(body: &Value) -> Vec<String> {
    body["plan"]["adoption"]["errors"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|error| error.as_str().map(str::to_string))
        .collect()
}

async fn served_version(d: &Deployment) -> Value {
    let (_, health) = d
        .platform
        .get_json(&format!("/svc/{}/loan-service/_meta/health", d.tenant.slug))
        .await
        .expect("health");
    health["version"].clone()
}

const UNSTATED: [&str; 2] = [
    "loan_service.loans.state is missing",
    "table loan_service.loan_notes is missing",
];

#[tokio::test]
async fn an_adopted_schema_is_checked_after_the_migrations_that_make_it() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = ledger_migrations();
    apply_with_refinery(&d, &local[..1]).await;
    let manifest = ledger(&local, legacy_history(), true);

    let (status, plan) = d
        .deploy_as("?plan=true", json!({"manifest": manifest}))
        .await;
    assert_eq!(status, 200, "{plan}");
    assert_eq!(plan["plan"]["adoption"]["deferred"], true, "{plan}");
    assert_eq!(errors(&plan), UNSTATED, "{plan}");
    assert_eq!(versions(&plan["plan"]["sql_migrations"]["pending"]), [2]);
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..1])
    );

    let body = d.deployed(json!({"manifest": manifest})).await;
    assert_eq!(versions(&body["plan"]["sql_migrations"]["applied"]), [2]);
    assert_eq!(body["plan"]["adoption"]["deferred"], Value::Null, "{body}");
    assert_eq!(errors(&body), Vec::<String>::new(), "{body}");
    assert_eq!(body["plan"]["private_tables"], json!(["loan_notes"]));
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));

    let loans = format!("/svc/{}/loan-service/loans", d.tenant.slug);
    let closed = uuid::Uuid::new_v4().to_string();
    let opened = uuid::Uuid::new_v4().to_string();
    for row in [
        json!({"id": closed, "memo": "returned", "state": "closed"}),
        json!({"id": opened, "memo": "lent"}),
    ] {
        let (status, created) = d.send(Method::POST, &loans, Some(row)).await;
        assert!(status == 200 || status == 201, "{status} {created}");
    }
    assert_eq!(
        d.texts(&format!(
            "SELECT state::text FROM loan_service.loans WHERE id IN ('{closed}', '{opened}') \
             ORDER BY memo DESC"
        ))
        .await,
        ["closed", "open"],
        "the version's snapshot holds the enum and the default the migration made"
    );
}

#[tokio::test]
async fn an_adoption_the_migrations_do_not_make_is_refused_after_them() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = ledger_migrations();
    apply_with_refinery(&d, &local[..1]).await;
    let first = d
        .deployed(json!({"manifest": ledger(&local[..1], legacy_history(), false)}))
        .await["version"]
        .clone();
    let unmade = [
        local[0].clone(),
        migration(
            2,
            "loan_stage",
            "ALTER TABLE loan_service.loans ADD COLUMN stage text;\n",
        ),
    ];
    let manifest = ledger(&unmade, legacy_history(), true);

    let (status, body) = d.deploy(json!({"manifest": manifest})).await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    for named in UNSTATED
        .into_iter()
        .chain(["the migrations this deploy applied stay applied: V2__loan_stage"])
    {
        assert!(error.contains(named), "{named} in {error}");
    }
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&unmade));
    assert!(exists(&d.database, "loan_service.loans").await);
    assert_eq!(served_version(&d).await, first);

    for query in ["", "?plan=true"] {
        let (status, body) = d.deploy_as(query, json!({"manifest": manifest})).await;
        assert_eq!(status, 422, "{query}: {body}");
        let error = body["error"].as_str().expect("error");
        assert!(
            error.contains(UNSTATED[0]) && !error.contains("stay applied"),
            "nothing pending is checked before anything runs: {error}"
        );
    }
    assert_eq!(served_version(&d).await, first);
}

#[tokio::test]
async fn a_failing_migration_under_an_adopted_schema_fails_as_it_did() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = ledger_migrations();
    apply_with_refinery(&d, &local[..1]).await;
    let broken = [
        local[0].clone(),
        migration(
            2,
            "broken",
            "ALTER TABLE loan_service.no_such_table ADD COLUMN x int;\n",
        ),
    ];
    let (status, body) = d
        .deploy(json!({"manifest": ledger(&broken, legacy_history(), true)}))
        .await;
    assert_eq!(status, 500, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(error.contains("V2__broken"), "{error}");
    assert!(!error.contains("does not hold"), "{error}");
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..1])
    );
}

#[tokio::test]
async fn a_verified_history_checks_adoption_before_anything_runs() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = ledger_migrations();
    apply_with_refinery(&d, &local[..1]).await;
    legacy::record_ddl(&d.database).await;
    let verify_only =
        json!({"table": "schema_history", "lock": "legacy_table_hash", "mode": "verify_only"});
    let manifest = ledger(&local, verify_only, true);
    for query in ["", "?plan=true"] {
        let (status, body) = d.deploy_as(query, json!({"manifest": manifest})).await;
        assert_eq!(status, 422, "{query}: {body}");
        let error = body["error"].as_str().expect("error");
        assert!(
            UNSTATED.iter().all(|unstated| error.contains(unstated)),
            "{error}"
        );
    }
    assert_eq!(
        legacy::ddl_statements(&d.database).await,
        legacy::DdlStatements::NONE
    );
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..1])
    );
}

#[tokio::test]
async fn adoption_is_checked_on_what_a_runner_holding_the_lock_made() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = ledger_migrations();
    apply_with_refinery(&d, &local[..1]).await;
    let key = legacy_table_lock_key(HISTORY);
    d.database
        .execute("SELECT pg_advisory_lock($1)", &[&key])
        .await
        .expect("the legacy runner's lock");

    let waiting = deploy_behind_the_lock(
        &d,
        json!({"manifest": ledger(&local, legacy_history(), true)}),
    )
    .await;

    d.database
        .batch_execute(&local[1].sql)
        .await
        .expect("the runner applies V2");
    d.database
        .execute(
            &format!(
                "INSERT INTO {HISTORY} (version, name, applied_on, checksum) \
                 VALUES ($1, $2, '2024-05-01T09:30:00.000000000Z', $3)"
            ),
            &[
                &(local[1].version as i32),
                &local[1].name,
                &checksum(&local[1]).to_string(),
            ],
        )
        .await
        .expect("the runner records V2");
    d.database
        .execute("SELECT pg_advisory_unlock($1)", &[&key])
        .await
        .expect("unlock");

    let (status, body) = waiting.await.expect("deploy task");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        versions(&body["plan"]["sql_migrations"]["applied"]),
        Vec::<u64>::new()
    );
    assert_eq!(errors(&body), Vec::<String>::new(), "{body}");
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));
}

fn shelf(migrations: &[SqlMigration], noted: bool, kept: bool) -> Value {
    let column = |name: &str, nullable: bool| json!({"name": name, "column_type": "text", "primary_key": false, "nullable": nullable});
    let items = [
        json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
               "auto_generate": true}),
        column("title", true),
    ]
    .into_iter()
    .chain(kept.then(|| column("legacy_note", true)))
    .chain((!kept).then(|| column("shade", true)))
    .collect::<Vec<_>>();
    let notes =
        [json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false})]
            .into_iter()
            .chain(noted.then(|| column("body", false)))
            .collect::<Vec<_>>();
    json!({
        "name": "loan-service", "version": "0.1.0", "mode": "crud", "publishes": [],
        "tables": [{"name": "items", "columns": items}],
        "private_tables": (!kept)
            .then(|| json!({"name": "loan_notes", "managed": false, "columns": notes}))
            .into_iter()
            .collect::<Vec<_>>(),
        "migrations": migrations,
        "migration_history": {"table": "schema_history"},
        "on_migrate": "insert_hooked",
    })
}

#[tokio::test]
async fn a_managed_schema_checks_the_private_table_its_migration_makes() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let module = base64::engine::general_purpose::STANDARD.encode(common::guest::module_bytes());
    let deploy = |manifest: Value| json!({"manifest": manifest, "wasm_base64": module, "allow_destructive": true});
    let local = [
        migration(
            1,
            "colours",
            "CREATE TABLE loan_service.colours (name text PRIMARY KEY);\n",
        ),
        migration(
            2,
            "loan_notes",
            "CREATE TABLE loan_service.loan_notes (id uuid PRIMARY KEY);\n",
        ),
        migration(
            3,
            "loan_note_body",
            "ALTER TABLE loan_service.loan_notes ADD COLUMN body text NOT NULL;\n",
        ),
    ];
    let first = d.deployed(deploy(shelf(&local[..1], false, true))).await["version"].clone();
    let hooked = || async {
        d.texts("SELECT count(*)::text FROM loan_service.items")
            .await
    };
    assert_eq!(hooked().await, ["1"]);

    let (status, body) = d.deploy(deploy(shelf(&local[..2], true, false))).await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(
        error.contains("loan_service.loan_notes.body is missing")
            && error.contains("the migrations this deploy applied stay applied: V2__loan_notes"),
        "{error}"
    );
    assert_eq!(served_version(&d).await, first);
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(&local[..2])
    );
    let columns = d
        .texts(
            "SELECT column_name::text FROM information_schema.columns \
             WHERE table_schema = 'loan_service' AND table_name = 'items' ORDER BY 1",
        )
        .await;
    assert_eq!(
        columns,
        ["id", "legacy_note", "shade", "title"],
        "the additive phase ran and the destructive one did not"
    );
    assert_eq!(hooked().await, ["1"], "on_migrate did not run");

    let body = d.deployed(deploy(shelf(&local, true, false))).await;
    assert_eq!(versions(&body["plan"]["sql_migrations"]["applied"]), [3]);
    assert_eq!(body["plan"]["private_tables"], json!(["loan_notes"]));
    assert_eq!(hooked().await, ["2"]);
    assert_ne!(served_version(&d).await, first);
}

#[tokio::test]
async fn drift_on_a_settled_declaration_is_refused_before_the_migrations() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = ledger_migrations();
    apply_with_refinery(&d, &local[..1]).await;
    d.deployed(json!({"manifest": ledger(&local[..1], legacy_history(), false)}))
        .await;
    let changed = d
        .deployed(json!({"manifest": ledger(&local, legacy_history(), true)}))
        .await;
    assert_eq!(
        versions(&changed["plan"]["sql_migrations"]["applied"]),
        [2],
        "a changed declaration the migration makes true passes: {changed}"
    );
    let served = served_version(&d).await;

    d.database
        .batch_execute("ALTER TABLE loan_service.loan_notes DROP COLUMN body")
        .await
        .expect("drift");
    let unrelated = [
        local[0].clone(),
        local[1].clone(),
        migration(
            3,
            "loan_tags",
            "CREATE TABLE loan_service.loan_tags (id uuid PRIMARY KEY);\n",
        ),
    ];
    let manifest = ledger(&unrelated, legacy_history(), true);
    for query in ["?plan=true", ""] {
        let (status, body) = d.deploy_as(query, json!({"manifest": manifest})).await;
        assert_eq!(status, 422, "{query}: {body}");
        let error = body["error"].as_str().expect("error");
        assert!(
            error.contains("loan_service.loan_notes.body is missing")
                && !error.contains("stay applied"),
            "{query}: {error}"
        );
    }
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&local));
    assert!(!exists(&d.database, "loan_service.loan_tags").await);
    assert_eq!(served_version(&d).await, served);
}

#[tokio::test]
async fn a_migration_that_breaks_a_settled_declaration_is_refused_after_it() {
    let breaks = [
        (
            migration(
                2,
                "drop_memo",
                "ALTER TABLE loan_service.loans DROP COLUMN memo;\n",
            ),
            "loan_service.loans.memo is missing",
        ),
        (
            migration(
                2,
                "count_memo",
                "ALTER TABLE loan_service.loans ALTER COLUMN memo TYPE integer USING length(memo);\n",
            ),
            "loan_service.loans.memo is integer, which a column declared TEXT cannot be",
        ),
    ];
    for (broken, finding) in breaks {
        let Some(d) = start(&[]).await else {
            return;
        };
        let local = ledger_migrations();
        apply_with_refinery(&d, &local[..1]).await;
        let first = d
            .deployed(json!({"manifest": ledger(&local[..1], legacy_history(), false)}))
            .await["version"]
            .clone();
        let both = [local[0].clone(), broken.clone()];
        let (status, body) = d
            .deploy(json!({"manifest": ledger(&both, legacy_history(), false)}))
            .await;
        assert_eq!(status, 422, "{body}");
        let error = body["error"].as_str().expect("error");
        assert!(
            error.contains(finding)
                && error.contains(&format!(
                    "the migrations this deploy applied stay applied: {}",
                    broken.file_stem()
                )),
            "{error}"
        );
        assert_eq!(recorded(&d.database, HISTORY).await, history_of(&both));
        assert_eq!(served_version(&d).await, first);
    }
}

#[tokio::test]
async fn the_first_plan_of_a_schema_not_made_yet_waits_for_the_migrations() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    assert!(
        d.texts("SELECT nspname::text FROM pg_namespace WHERE nspname = 'loan_service'")
            .await
            .is_empty(),
        "the schema is not there before the first deploy"
    );
    let local = [migration(
        1,
        "loan_notes",
        "CREATE TABLE loan_service.loan_notes (id uuid PRIMARY KEY);\n",
    )];
    let mut manifest = loan_service(&local, json!({"table": "schema_history"}));
    manifest["private_tables"] = json!([{"name": "loan_notes", "managed": false, "columns": [
        {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false}
    ]}]);

    let (status, plan) = d
        .deploy_as("?plan=true", json!({"manifest": manifest}))
        .await;
    assert_eq!(status, 200, "{plan}");
    assert_eq!(plan["plan"]["adoption"]["deferred"], true, "{plan}");
    assert_eq!(errors(&plan), ["schema loan_service is missing"], "{plan}");

    let body = d.deployed(json!({"manifest": manifest})).await;
    assert_eq!(versions(&body["plan"]["sql_migrations"]["applied"]), [1]);
    assert_eq!(errors(&body), Vec::<String>::new(), "{body}");
    assert!(exists(&d.database, "loan_service.loan_notes").await);
}

fn staged(local: &[SqlMigration]) -> [SqlMigration; 2] {
    [
        local[0].clone(),
        migration(
            2,
            "loan_stage",
            "ALTER TABLE loan_service.loans ADD COLUMN stage text;\n",
        ),
    ]
}

#[tokio::test]
async fn an_adopted_search_config_a_pending_migration_makes_is_checked_after_it() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = ledger_migrations();
    apply_with_refinery(&d, &local[..1]).await;
    let searched = |migrations: &[SqlMigration]| {
        let mut manifest = ledger(migrations, legacy_history(), false);
        manifest["tables"][0]["search"] = json!({"columns": ["memo"], "config": "loan_english"});
        json!({"manifest": manifest})
    };
    let unmade = staged(&local);
    let (status, body) = d.deploy(searched(&unmade)).await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(
        error.contains("no text search configuration 'loan_english'")
            && error.contains("the migrations this deploy applied stay applied: V2__loan_stage"),
        "{error}"
    );

    let made = [
        unmade[0].clone(),
        unmade[1].clone(),
        migration(
            3,
            "loan_english",
            "CREATE TEXT SEARCH CONFIGURATION loan_service.loan_english \
             (COPY = pg_catalog.english);\n",
        ),
    ];
    let (status, plan) = d.deploy_as("?plan=true", searched(&made)).await;
    assert_eq!(status, 200, "{plan}");
    assert_eq!(plan["plan"]["adoption"]["deferred"], true, "{plan}");
    assert!(
        errors(&plan)
            .iter()
            .all(|error| !error.contains("loan_english")),
        "a plan does not look for what the migrations are to create: {plan}"
    );
    let body = d.deployed(searched(&made)).await;
    assert_eq!(versions(&body["plan"]["sql_migrations"]["applied"]), [3]);
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&made));
    let served = served_version(&d).await;

    d.database
        .batch_execute("DROP TEXT SEARCH CONFIGURATION loan_service.loan_english")
        .await
        .expect("drift");
    let unrelated = made.iter().cloned().chain([migration(
        4,
        "loan_tags",
        "CREATE TABLE loan_service.loan_tags (id uuid PRIMARY KEY);\n",
    )]);
    let (status, body) = d.deploy(searched(&unrelated.collect::<Vec<_>>())).await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(
        error.contains("no text search configuration 'loan_english'")
            && !error.contains("stay applied"),
        "a settled declaration's configuration is checked before anything runs: {error}"
    );
    assert_eq!(recorded(&d.database, HISTORY).await, history_of(&made));
    assert!(!exists(&d.database, "loan_service.loan_tags").await);
    assert_eq!(served_version(&d).await, served);
}

#[tokio::test]
async fn an_async_deploy_refused_after_the_migrations_ends_in_the_adoption_phase() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let local = ledger_migrations();
    apply_with_refinery(&d, &local[..1]).await;
    d.deployed(json!({"manifest": ledger(&local[..1], legacy_history(), false)}))
        .await;
    let (status, accepted) = d
        .deploy_as(
            "?async=true",
            json!({"manifest": ledger(&staged(&local), legacy_history(), true)}),
        )
        .await;
    assert_eq!(status, 202, "{accepted}");
    let job = format!(
        "/api/deploys/{}",
        accepted["deploy_id"].as_str().expect("deploy_id")
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    let ended = loop {
        let (status, job) = d.send(Method::GET, &job, None).await;
        assert_eq!(status, 200, "{job}");
        if matches!(job["status"].as_str(), Some("succeeded" | "failed")) {
            break job;
        }
        assert!(Instant::now() < deadline, "{job}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(ended["status"], "failed", "{ended}");
    assert_eq!(
        ended["progress"],
        json!({"phase": "adoption", "tables": 2}),
        "{ended}"
    );
    assert!(
        ended["error"]
            .as_str()
            .is_some_and(|error| error.contains("stay applied: V2__loan_stage")),
        "{ended}"
    );
}

fn noted_service(migrations: &[SqlMigration], private: bool) -> Value {
    let notes = json!({"name": "loan_notes", "managed": !private, "columns": [
        {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false},
        {"name": "body", "column_type": "text", "primary_key": false, "nullable": false}
    ]});
    let mut manifest = loan_service(migrations, json!({"table": "schema_history"}));
    let list = if private { "private_tables" } else { "tables" };
    manifest[list] = manifest[list]
        .as_array()
        .into_iter()
        .flatten()
        .cloned()
        .chain([notes])
        .collect();
    manifest
}

#[tokio::test]
async fn a_table_taken_from_the_platform_into_adoption_waits_for_the_migrations() {
    let colours = migration(
        1,
        "colours",
        "CREATE TABLE loan_service.colours (name text PRIMARY KEY);\n",
    );
    let labels = migration(
        2,
        "loan_labels",
        "CREATE TABLE loan_service.loan_labels (id uuid PRIMARY KEY);\n",
    );
    let body = migration(
        2,
        "loan_note_body",
        "ALTER TABLE loan_service.loan_notes ADD COLUMN body text NOT NULL;\n",
    );
    let releases = [
        (false, labels.clone(), None),
        (true, body, None),
        (
            true,
            labels,
            Some("loan_service.loan_notes.body is missing"),
        ),
    ];
    for (drifted, pending, refusal) in releases {
        let Some(d) = start(&[]).await else {
            return;
        };
        let first = d
            .deployed(json!({"manifest": noted_service(std::slice::from_ref(&colours), false)}))
            .await["version"]
            .clone();
        if drifted {
            d.database
                .batch_execute("ALTER TABLE loan_service.loan_notes DROP COLUMN body")
                .await
                .expect("drift");
        }
        let both = [colours.clone(), pending.clone()];
        let (status, body) = d
            .deploy(json!({"manifest": noted_service(&both, true)}))
            .await;
        assert_eq!(recorded(&d.database, HISTORY).await, history_of(&both));
        match refusal {
            None => {
                assert_eq!(status, 200, "{}: {body}", pending.file_stem());
                assert_ne!(served_version(&d).await, first);
            }
            Some(finding) => {
                assert_eq!(status, 422, "{body}");
                let error = body["error"].as_str().expect("error");
                assert!(
                    error.contains(finding)
                        && error.contains(&format!("stay applied: {}", pending.file_stem())),
                    "{error}"
                );
                assert_eq!(served_version(&d).await, first);
            }
        }
    }
}

#[tokio::test]
async fn a_snapshot_without_records_settles_what_it_declared_unmanaged() {
    let Some(d) = start(&[]).await else {
        return;
    };
    let colours = migration(
        1,
        "colours",
        "CREATE TABLE loan_service.colours (name text PRIMARY KEY);\n\
         CREATE TABLE loan_service.loan_notes (id uuid PRIMARY KEY, body text NOT NULL);\n",
    );
    d.deployed(json!({"manifest": noted_service(std::slice::from_ref(&colours), true)}))
        .await;
    let served = served_version(&d).await;
    let rewritten =
        d.db.pool()
            .get()
            .await
            .expect("checkout")
            .execute(
                "UPDATE schema_snapshots SET schema_json = schema_json -> 'tables' \
             WHERE jsonb_typeof(schema_json) = 'object' AND service_id IN \
             (SELECT id FROM services WHERE tenant_id = $1 AND name = 'loan-service')",
                &[&d.tenant.id],
            )
            .await
            .expect("a snapshot without adoption records");
    assert_eq!(rewritten, 1);

    d.database
        .batch_execute("ALTER TABLE loan_service.loan_notes DROP COLUMN body")
        .await
        .expect("drift");
    let unrelated = [
        colours.clone(),
        migration(
            2,
            "loan_labels",
            "CREATE TABLE loan_service.loan_labels (id uuid PRIMARY KEY);\n",
        ),
    ];
    let (status, body) = d
        .deploy(json!({"manifest": noted_service(&unrelated, true)}))
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(
        error.contains("loan_service.loan_notes.body is missing")
            && !error.contains("stay applied"),
        "{error}"
    );
    assert_eq!(
        recorded(&d.database, HISTORY).await,
        history_of(std::slice::from_ref(&colours))
    );
    assert!(!exists(&d.database, "loan_service.loan_labels").await);
    assert_eq!(served_version(&d).await, served);
}
