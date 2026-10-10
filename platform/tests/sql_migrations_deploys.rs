use cufflink_testkit as common;

use base64::Engine;
use common::{legacy, Deployment};
use cufflink_migrate::{checksum, legacy_table_lock_key};
use cufflink_types::SqlMigration;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
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

    let waiting = {
        let url = format!("{}/api/services/deploy", d.platform.base_url);
        let auth = format!("ApiKey {}", d.tenant.api_key);
        let body = json!({"manifest": loan_service(&local, legacy_history())});
        let client = d.platform.http.clone();
        tokio::spawn(async move {
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
        })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!waiting.is_finished(), "the deploy waits for the lock");
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
