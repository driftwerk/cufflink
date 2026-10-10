use cufflink_testkit as common;

use common::infra::TestDb;
use cufflink_migrate::{checksum, legacy_table_lock_key, PlanError, QualifiedName};
use cufflink_platform::schema_manager::migrate_exec::{
    run, Action, MigrateError, MigrationRef, Report, Run, Timeouts,
};
use cufflink_types::{MigrationLock, SqlMigration};
use std::time::Duration;
use tokio_postgres::Client;

const SCHEMA: &str = "loan_service";

const TIMEOUTS: Timeouts = Timeouts {
    lock_wait: Duration::from_secs(30),
    statement: Duration::from_secs(60),
    lock: Duration::from_secs(10),
};

async fn connect(db: &TestDb) -> Client {
    let client = cufflink_db::connect_single(
        cufflink_db::parse_config(&db.url).expect("url"),
        &cufflink_db::PoolSettings::from_vars(1, |_| None).expect("defaults"),
    )
    .await
    .expect("connect");
    client
        .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        .await
        .expect("schema");
    client
}

fn file(version: u32, name: &str, sql: &str) -> SqlMigration {
    SqlMigration {
        version,
        name: name.into(),
        sql: sql.into(),
        no_transaction: SqlMigration::declares_no_transaction(sql),
    }
}

fn loan_service() -> Vec<SqlMigration> {
    vec![
        file(
            1,
            "initial_tables",
            "CREATE TABLE loans (id INT PRIMARY KEY, body TEXT);\n",
        ),
        file(
            6,
            "add_loan_kind",
            "ALTER TABLE loans ADD COLUMN kind TEXT;\r\n",
        ),
        file(
            7,
            "renewal_index",
            "CREATE INDEX loans_kind ON loans (kind);",
        ),
    ]
}

fn history() -> QualifiedName {
    QualifiedName::parse("schema_history", SCHEMA).expect("name")
}

fn apply(migrations: &[SqlMigration]) -> Run<'_> {
    Run {
        schema: SCHEMA,
        history: history(),
        lock: MigrationLock::Platform,
        baseline: None,
        migrations,
        action: Action::Apply,
        role: None,
        timeouts: TIMEOUTS,
    }
}

async fn recorded(client: &Client) -> Vec<(i32, String, String)> {
    client
        .query(
            "SELECT version, name, checksum FROM loan_service.schema_history ORDER BY version",
            &[],
        )
        .await
        .expect("history")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

async fn relation_exists(client: &Client, name: &str) -> bool {
    client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&name])
        .await
        .expect("regclass")
        .get(0)
}

async fn apply_with_refinery(client: &mut Client, migrations: &[SqlMigration]) {
    client
        .batch_execute("SET search_path TO loan_service, public")
        .await
        .expect("search_path");
    let migrations: Vec<refinery::Migration> = migrations
        .iter()
        .map(|m| refinery::Migration::unapplied(&m.file_stem(), &m.sql).expect("migration"))
        .collect();
    let mut runner = refinery::Runner::new(&migrations).set_grouped(false);
    runner.set_migration_table_name("loan_service.schema_history");
    runner.run_async(client).await.expect("refinery run");
    client
        .batch_execute("RESET search_path")
        .await
        .expect("reset");
}

fn refs(migrations: &[SqlMigration]) -> Vec<MigrationRef> {
    migrations.iter().map(MigrationRef::from).collect()
}

#[tokio::test]
async fn a_history_refinery_began_is_continued_and_refinery_reads_the_result() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    let local = loan_service();
    apply_with_refinery(&mut client, &local[..2]).await;

    let report = run(&mut client, &apply(&local)).await.expect("run");
    assert_eq!(
        report,
        Report {
            history: "loan_service.schema_history".into(),
            verified: 2,
            applied: refs(&local[2..]),
            ..Report::default()
        }
    );
    assert_eq!(
        recorded(&client).await,
        local
            .iter()
            .map(|m| (m.version as i32, m.name.clone(), checksum(m).to_string()))
            .collect::<Vec<_>>()
    );
    assert!(relation_exists(&client, "loan_service.loans_kind").await);

    let mut runner = refinery::Runner::new(&[]);
    runner.set_migration_table_name("loan_service.schema_history");
    let read_back = runner
        .get_applied_migrations_async(&mut client)
        .await
        .expect("refinery reads the history");
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

    let again = run(&mut client, &apply(&local)).await.expect("second run");
    assert_eq!((again.verified, again.applied.len()), (3, 0));
}

#[tokio::test]
async fn a_history_the_files_contradict_stops_the_run() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    let local = loan_service();
    run(&mut client, &apply(&local[..2])).await.expect("run");

    let mut edited = local.clone();
    edited[1].sql.push_str("-- edited\n");
    let error = run(&mut client, &apply(&edited))
        .await
        .expect_err("diverges");
    assert!(
        matches!(
            &error,
            MigrateError::Plan(PlanError::Divergent { version: 6, applied_checksum, local_checksum, .. })
                if *applied_checksum == checksum(&local[1]) && *local_checksum == checksum(&edited[1])
        ),
        "{error}"
    );
    assert!(!relation_exists(&client, "loan_service.loans_kind").await);

    client
        .batch_execute("UPDATE loan_service.schema_history SET checksum = 'abc' WHERE version = 6")
        .await
        .expect("corrupt");
    let error = run(&mut client, &apply(&local)).await.expect_err("bad row");
    assert!(
        matches!(
            &error,
            MigrateError::Plan(PlanError::BadHistoryRow { version: 6, .. })
        ),
        "{error}"
    );
    let held: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()",
            &[],
        )
        .await
        .expect("locks")
        .get(0);
    assert_eq!(held, 0, "a refused run releases the lock");
}

#[tokio::test]
async fn an_unverifiable_history_is_accepted_under_the_baseline() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    client
        .batch_execute(
            "CREATE TABLE public.schema_history_deferred(version INT4 PRIMARY KEY, \
             name VARCHAR(255), applied_on VARCHAR(255), checksum VARCHAR(255)); \
             INSERT INTO public.schema_history_deferred \
             SELECT v, 'deferred_' || v, '2024-05-01T09:30:00.000000000Z', (v * 7)::text \
             FROM generate_series(1, 15) v",
        )
        .await
        .expect("deferred history");
    let local = [file(
        16,
        "scoped_view",
        "CREATE VIEW scoped AS SELECT 1 AS one;",
    )];
    let deferred = |baseline| Run {
        history: QualifiedName::parse("public.schema_history_deferred", SCHEMA).expect("name"),
        baseline,
        ..apply(&local)
    };

    let error = run(&mut client, &deferred(None))
        .await
        .expect_err("missing");
    assert!(
        matches!(
            &error,
            MigrateError::Plan(PlanError::Missing { version: 1, .. })
        ),
        "{error}"
    );
    let report = run(&mut client, &deferred(Some(15))).await.expect("run");
    assert_eq!(report.baselined, (1..=15).collect::<Vec<i32>>());
    assert_eq!(report.applied, refs(&local));
    assert!(relation_exists(&client, "loan_service.scoped").await);
}

#[tokio::test]
async fn verifying_reports_what_is_pending_and_applies_nothing() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    let local = loan_service();
    run(&mut client, &apply(&local[..2])).await.expect("run");

    let verify = Run {
        action: Action::Verify,
        ..apply(&local)
    };
    let report = run(&mut client, &verify).await.expect("verify");
    assert_eq!(
        (report.verified, &report.applied, &report.pending),
        (2, &vec![], &refs(&local[2..]))
    );
    assert_eq!(recorded(&client).await.len(), 2);
    assert!(!relation_exists(&client, "loan_service.loans_kind").await);
}

#[tokio::test]
async fn the_legacy_lock_excludes_a_runner_holding_it() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    let legacy_runner = connect(&db).await;
    let key = legacy_table_lock_key("loan_service.schema_history");
    legacy_runner
        .execute("SELECT pg_advisory_lock($1)", &[&key])
        .await
        .expect("legacy lock");

    let local = loan_service();
    let contended = |lock_wait| Run {
        lock: MigrationLock::LegacyTableHash,
        timeouts: Timeouts {
            lock_wait,
            ..TIMEOUTS
        },
        ..apply(&local)
    };
    let error = run(&mut client, &contended(Duration::from_secs(2)))
        .await
        .expect_err("locked out");
    assert!(matches!(error, MigrateError::LockTimeout(..)), "{error}");
    assert!(!relation_exists(&client, "loan_service.loans").await);

    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        legacy_runner
            .execute("SELECT pg_advisory_unlock($1)", &[&key])
            .await
            .expect("unlock");
        legacy_runner
    });
    let report = run(&mut client, &contended(Duration::from_secs(30)))
        .await
        .expect("applies once the lock is free");
    assert_eq!(report.applied.len(), 3);
    release.await.expect("release task");
}

#[tokio::test]
async fn a_concurrent_index_needs_a_migration_outside_a_transaction() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    let concurrent = "CREATE INDEX CONCURRENTLY loans_body ON loans (body);";
    let mut local = loan_service();
    local.push(file(8, "body_index", concurrent));

    let error = run(&mut client, &apply(&local))
        .await
        .expect_err("in a transaction");
    assert!(
        matches!(
            &error,
            MigrateError::Apply { version: 8, applied, .. }
                if *applied == refs(&local[..3])
                    && error.to_string().starts_with("migration V8__body_index failed: ")
                    && error.to_string().contains("cannot run inside a transaction block")
        ),
        "{error}"
    );
    assert_eq!(recorded(&client).await.len(), 3, "earlier migrations stay");

    local[3] = file(
        8,
        "body_index",
        &format!("{}\n{concurrent}", SqlMigration::NO_TRANSACTION_MARKER),
    );
    let report = run(&mut client, &apply(&local)).await.expect("run");
    assert_eq!(report.applied, refs(&local[3..]));
    assert!(relation_exists(&client, "loan_service.loans_body").await);
    let search_path: String = client
        .query_one("SHOW search_path", &[])
        .await
        .expect("show")
        .get(0);
    assert!(!search_path.contains(SCHEMA), "session settings are reset");
}

#[tokio::test]
async fn an_interrupted_concurrent_build_is_dropped_and_rebuilt() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    let mut local = loan_service();
    run(&mut client, &apply(&local)).await.expect("run");
    client
        .batch_execute("INSERT INTO loan_service.loans (id, body) VALUES (1, 'same'), (2, 'same')")
        .await
        .expect("rows");
    client
        .batch_execute("CREATE UNIQUE INDEX CONCURRENTLY loans_body ON loan_service.loans (body)")
        .await
        .expect_err("a unique build over duplicates fails and leaves an invalid index");

    local.push(file(
        8,
        "body_index",
        &format!(
            "{}\nCREATE INDEX CONCURRENTLY loans_body ON loans (body);",
            SqlMigration::NO_TRANSACTION_MARKER
        ),
    ));
    let report = run(&mut client, &apply(&local)).await.expect("run");
    assert_eq!(report.recovered_indexes, ["loans_body"]);
    assert_eq!(report.applied, refs(&local[3..]));
    let valid: bool = client
        .query_one(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = 'loan_service.loans_body'::regclass",
            &[],
        )
        .await
        .expect("index")
        .get(0);
    assert!(valid);
    assert_eq!(recorded(&client).await.len(), 4);
}

#[tokio::test]
async fn migrations_run_as_the_role_and_the_history_is_the_sessions() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    let role = format!("svc_{}", db.name);
    client
        .batch_execute(&format!(
            "CREATE ROLE \"{role}\" NOLOGIN; GRANT \"{role}\" TO CURRENT_USER; \
             GRANT ALL ON SCHEMA loan_service TO \"{role}\""
        ))
        .await
        .expect("role");
    let local = [
        file(
            1,
            "initial_tables",
            "CREATE TABLE loans (id INT PRIMARY KEY);",
        ),
        file(
            2,
            "id_index",
            &format!(
                "{}\nCREATE INDEX CONCURRENTLY loans_id ON loans (id);",
                SqlMigration::NO_TRANSACTION_MARKER
            ),
        ),
    ];
    let as_role = Run {
        role: Some(&role),
        ..apply(&local)
    };
    let outcome = run(&mut client, &as_role).await;
    let owners: Vec<(String, String)> = client
        .query(
            "SELECT c.relname::text, pg_get_userbyid(c.relowner)::text FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'loan_service' AND c.relname IN ('loans', 'loans_id', 'schema_history') \
             ORDER BY 1",
            &[],
        )
        .await
        .expect("owners")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let session: String = client
        .query_one("SELECT current_user::text", &[])
        .await
        .expect("user")
        .get(0);
    client
        .batch_execute(&format!(
            "DROP SCHEMA loan_service CASCADE; DROP ROLE \"{role}\""
        ))
        .await
        .expect("clean up the role");
    assert_eq!(outcome.expect("run").applied, refs(&local));
    assert_eq!(
        owners,
        [
            ("loans".to_string(), role.clone()),
            ("loans_id".to_string(), role),
            ("schema_history".to_string(), session),
        ]
    );
}

#[tokio::test]
async fn a_migration_that_ends_its_transaction_is_refused_and_unrecorded() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    for after_commit in ["ALTER TABLE loans ADD COLUMN body TEXT;", "SELECT 1;"] {
        let local = [
            file(
                1,
                "initial_tables",
                "CREATE TABLE loans (id INT PRIMARY KEY);",
            ),
            file(
                2,
                "commits",
                &format!(
                    "ALTER TABLE loans ADD COLUMN IF NOT EXISTS kind TEXT; COMMIT; {after_commit}"
                ),
            ),
            file(3, "after", "CREATE TABLE after_it (id INT);"),
        ];
        let refused = run(&mut client, &apply(&local)).await.expect_err("refused");
        assert!(
            matches!(&refused, MigrateError::Escaped { version: 2, .. }),
            "{after_commit}: {refused}"
        );
        assert_eq!(
            refused.to_string().split(';').next(),
            Some("migration V2__commits ended the transaction it ran in")
        );
    }
    assert_eq!(
        recorded(&client)
            .await
            .into_iter()
            .map(|(version, _, _)| version)
            .collect::<Vec<_>>(),
        [1]
    );
    assert!(!relation_exists(&client, "loan_service.after_it").await);
    let kept: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = \
             'loan_service' AND table_name = 'loans' AND column_name = 'kind')",
            &[],
        )
        .await
        .expect("columns")
        .get(0);
    assert!(kept, "what the migration committed itself stays");
    let session: String = client
        .query_one("SELECT current_setting('search_path')", &[])
        .await
        .expect("path")
        .get(0);
    assert!(
        !session.contains("loan_service"),
        "the run leaves the connection as it found it: {session}"
    );
}

#[tokio::test]
async fn a_schema_with_capitals_or_a_keywords_name_is_the_one_postgres_has() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let mut client = connect(&db).await;
    for (schema, stored) in [("Order_Desk", "order_desk"), ("order", "order")] {
        client
            .batch_execute(&format!("CREATE SCHEMA \"{stored}\""))
            .await
            .expect("schema");
        let local = [file(
            1,
            "initial_tables",
            "CREATE TABLE lines (id INT PRIMARY KEY);",
        )];
        for round in 0..2 {
            let report = run(
                &mut client,
                &Run::of_service(schema, &local, None, false, None, TIMEOUTS).expect("run"),
            )
            .await
            .expect("run");
            assert_eq!(
                (report.applied.len(), report.verified),
                if round == 0 { (1, 0) } else { (0, 1) },
                "{schema}: applied once, then verified"
            );
            assert_eq!(report.history, format!("{stored}.refinery_schema_history"));
        }
        for relation in ["lines", "refinery_schema_history"] {
            assert!(
                relation_exists(&client, &format!("\"{stored}\".{relation}")).await,
                "{stored}.{relation}"
            );
        }
    }
}
