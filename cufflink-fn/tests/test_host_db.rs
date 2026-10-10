#![cfg(feature = "test-host")]

mod support;

use cufflink_fn::db;
use cufflink_fn::host::db::DbHost;
use cufflink_fn::testing::{
    HistoryMode, MigrationHistory, MigrationLock, PgHost, PgOptions, PgRole, Plan, Pooler, TestDb,
    TestHost, TestHostError,
};
use cufflink_pgcodec::pooler::pooler_refusal;
use cufflink_types::db::{DbErrorKind, DbRequest, DbResponse, Isolation, Param};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use support::{database, loan_migrations};

const BOOK: &str = "11111111-1111-4111-8111-111111111111";
const AUTHOR: &str = "22222222-2222-4222-8222-222222222222";

pub struct Scratch(PathBuf);

impl Scratch {
    pub fn with(files: &[(&str, &str)]) -> Self {
        let dir = std::env::temp_dir().join(format!("cufflink-fn-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("scratch directory");
        for (name, text) in files {
            std::fs::write(dir.join(name), text).expect("scratch file");
        }
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn legacy_history() -> MigrationHistory {
    MigrationHistory {
        table: "schema_history".into(),
        lock: MigrationLock::LegacyTableHash,
        ..MigrationHistory::default()
    }
}

fn loans() -> Option<(TestDb, TestHost)> {
    let db = database()?;
    db.migrate("loan_service", &loan_migrations(), &legacy_history())
        .expect("migrations");
    let host = loan_host(&db, Pooler::PgBouncerTransaction);
    Some((db, host))
}

fn loan_host(db: &TestDb, pooler: Pooler) -> TestHost {
    TestHost::builder()
        .tenant("harbor")
        .service("loan-service")
        .db(db)
        .pooler(pooler)
        .build()
        .expect("host")
}

fn insert(id: u32, memo: &str) -> String {
    format!(
        "INSERT INTO loans (id, book_id, memo, created_by, created_at, updated_at) \
         VALUES ('00000000-0000-4000-8000-{id:012}', '{BOOK}', '{memo}', '{AUTHOR}', \
         '2026-09-28 10:00:00', '2026-09-28 10:00:00')"
    )
}

fn count(db: &TestDb) -> i64 {
    db.query("SELECT count(*) AS loans FROM loan_service.loans")
        .expect("count")[0]["loans"]
        .as_i64()
        .expect("a count")
}

#[test]
fn a_database_is_created_empty_and_dropped_with_its_value() {
    let (Some(first), Some(second)) = (database(), database()) else {
        return;
    };
    assert_ne!(first.name(), second.name());
    assert!(first.name().starts_with("test_"));
    first
        .execute("CREATE TABLE seen (id int); INSERT INTO seen VALUES (1), (2)")
        .expect("seed");
    assert_eq!(
        first
            .query("SELECT id FROM seen ORDER BY id")
            .expect("rows"),
        [json!({"id": 1}), json!({"id": 2})]
    );
    assert!(
        second.query("SELECT id FROM seen").is_err(),
        "a database of its own"
    );
    let exists = |name: &str| {
        second
            .query(&format!(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{name}') AS there"
            ))
            .expect("pg_database")[0]["there"]
            == json!(true)
    };
    let name = first.name().to_string();
    assert!(exists(&name));
    drop(first);
    assert!(!exists(&name));
}

#[test]
fn a_file_of_statements_seeds_a_database() {
    let Some(db) = database() else {
        return;
    };
    let seed = Scratch::with(&[(
        "seed.sql",
        "CREATE SCHEMA catalog_service;\n\
         CREATE TABLE catalog_service.book (id uuid PRIMARY KEY, status varchar(50));\n\
         INSERT INTO catalog_service.book VALUES ('11111111-1111-4111-8111-111111111111', 'open');",
    )]);
    db.execute_file(&seed.path().join("seed.sql"))
        .expect("seed");
    assert_eq!(
        db.query("SELECT status FROM catalog_service.book")
            .expect("rows"),
        [json!({"status": "open"})]
    );
    let missing = db.execute_file(&seed.path().join("absent.sql"));
    assert!(
        matches!(&missing, Err(TestHostError::File { path, .. }) if path.ends_with("absent.sql")),
        "{missing:?}"
    );
    let broken = db.execute("INSERT INTO catalog_service.nowhere VALUES (1)");
    assert!(
        matches!(&broken, Err(TestHostError::Database(_))),
        "{broken:?}"
    );
    assert_eq!(
        broken.expect_err("refused").to_string(),
        "ERROR: relation \"catalog_service.nowhere\" does not exist"
    );
}

#[test]
fn migrations_continue_the_history_a_service_declares() {
    let Some(db) = database() else {
        return;
    };
    let versions = |plan: &Plan| -> Vec<u32> { plan.pending.iter().map(|m| m.version).collect() };
    let first = db
        .migrate("loan_service", &loan_migrations(), &legacy_history())
        .expect("first run");
    assert_eq!(versions(&first), [1, 2, 3]);
    assert_eq!((first.verified, first.baselined.len()), (0, 0));
    assert_eq!(
        db.query("SELECT version, name FROM loan_service.schema_history ORDER BY version")
            .expect("history"),
        [
            json!({"version": 1, "name": "initial_tables"}),
            json!({"version": 2, "name": "add_loan_kind"}),
            json!({"version": 3, "name": "renewal_index"}),
        ]
    );
    assert_eq!(
        db.query(
            "SELECT column_default FROM information_schema.columns \
             WHERE table_schema = 'loan_service' AND column_name = 'loan_kind'"
        )
        .expect("column"),
        [json!({"column_default": "'Standard'::character varying"})]
    );
    let again = db
        .migrate("loan_service", &loan_migrations(), &legacy_history())
        .expect("second run");
    assert!(again.pending.is_empty());
    assert_eq!(again.verified, 3);
}

#[test]
fn a_history_that_is_only_verified_has_nothing_applied() {
    let Some(db) = database() else {
        return;
    };
    let verify = MigrationHistory {
        mode: HistoryMode::VerifyOnly,
        ..legacy_history()
    };
    let planned = db
        .migrate("loan_service", &loan_migrations(), &verify)
        .expect("plan");
    assert_eq!(planned.pending.len(), 3);
    assert_eq!(
        db.query("SELECT count(*) AS applied FROM loan_service.schema_history")
            .expect("history"),
        [json!({"applied": 0})]
    );
    assert!(db.query("SELECT 1 FROM loan_service.loans").is_err());
}

#[test]
fn a_history_the_files_contradict_is_refused() {
    let Some(db) = database() else {
        return;
    };
    let applied = Scratch::with(&[
        ("V1__loans.sql", "CREATE TABLE loans (id int PRIMARY KEY);"),
        ("V2__kind.sql", "ALTER TABLE loans ADD COLUMN kind text;"),
    ]);
    db.migrate("loan_service", applied.path(), &MigrationHistory::default())
        .expect("applied");
    assert_eq!(
        db.query("SELECT count(*) AS applied FROM loan_service.refinery_schema_history")
            .expect("the default history"),
        [json!({"applied": 2})]
    );
    let refused = |files: &[(&str, &str)], history: &MigrationHistory| {
        let dir = Scratch::with(files);
        match db.migrate("loan_service", dir.path(), history) {
            Err(TestHostError::Migration(reason)) => reason,
            other => panic!("{other:?}"),
        }
    };
    let edited = refused(
        &[
            ("V1__loans.sql", "CREATE TABLE loans (id int PRIMARY KEY);"),
            (
                "V2__kind.sql",
                "ALTER TABLE loans ADD COLUMN kind varchar(9);",
            ),
        ],
        &MigrationHistory::default(),
    );
    assert!(edited.starts_with("migration V2 diverges"), "{edited}");
    let missing = refused(
        &[("V1__loans.sql", "CREATE TABLE loans (id int PRIMARY KEY);")],
        &MigrationHistory::default(),
    );
    assert_eq!(missing, "applied migration V2__kind has no local file");
    let named = refused(
        &[("v1_loans.sql", "SELECT 1;")],
        &MigrationHistory::default(),
    );
    assert!(named.contains("v1_loans.sql"), "{named}");
    let history = refused(
        &[],
        &MigrationHistory {
            table: "a.b.c".into(),
            ..MigrationHistory::default()
        },
    );
    assert!(history.starts_with("migration_history.table:"), "{history}");
}

#[test]
fn a_migration_runs_in_a_transaction_unless_it_says_otherwise() {
    let Some(db) = database() else {
        return;
    };
    let dir = Scratch::with(&[
        (
            "V1__loans.sql",
            "CREATE TABLE loans (id int PRIMARY KEY, memo text);",
        ),
        (
            "V2__memo_index.sql",
            "-- cufflink:no-transaction\nCREATE INDEX CONCURRENTLY loans_memo ON loans (memo);",
        ),
        (
            "V3__escapes.sql",
            "ALTER TABLE loans ADD COLUMN kind text;\nCOMMIT;\nALTER TABLE loans ADD COLUMN late text;",
        ),
    ]);
    let escaped = db.migrate("loan_service", dir.path(), &MigrationHistory::default());
    let reason = match escaped {
        Err(TestHostError::Migration(reason)) => reason,
        other => panic!("{other:?}"),
    };
    assert!(
        reason.starts_with("migration V3__escapes ended the transaction it ran in"),
        "{reason}"
    );
    assert_eq!(
        db.query("SELECT version FROM loan_service.refinery_schema_history ORDER BY version")
            .expect("history"),
        [json!({"version": 1}), json!({"version": 2})],
        "what ran before it stays applied, and it is not recorded"
    );
    assert_eq!(
        db.query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'loan_service' \
             AND indexname = 'loans_memo'"
        )
        .expect("index"),
        [json!({"indexname": "loans_memo"})]
    );
    let failing = Scratch::with(&[
        (
            "V1__loans.sql",
            "CREATE TABLE loans (id int PRIMARY KEY, memo text);",
        ),
        (
            "V2__memo_index.sql",
            "-- cufflink:no-transaction\nCREATE INDEX CONCURRENTLY loans_memo ON loans (memo);",
        ),
        (
            "V3__broken.sql",
            "ALTER TABLE nowhere ADD COLUMN kind text;",
        ),
    ]);
    let broken = db.migrate("loan_service", failing.path(), &MigrationHistory::default());
    assert_eq!(
        broken.expect_err("refused").to_string(),
        "migration V3__broken failed: relation \"nowhere\" does not exist"
    );
}

#[test]
fn legacy_statements_run_in_the_schema_of_the_service() {
    let Some((db, host)) = loans() else {
        return;
    };
    let (inserted, rows, path) = host.run(|| {
        (
            db::execute(&insert(1, "Due back from Ada on Monday")),
            db::query("SELECT id, memo, loan_kind, created_at FROM loans"),
            db::query_one("SELECT current_setting('search_path') AS path"),
        )
    });
    assert_eq!(inserted, 1);
    assert_eq!(
        rows,
        [json!({
            "id": "00000000-0000-4000-8000-000000000001",
            "memo": "Due back from Ada on Monday",
            "loan_kind": "Standard",
            "created_at": "2026-09-28 10:00:00",
        })]
    );
    assert_eq!(path, Some(json!({"path": "loan_service, public"})));
    assert_eq!(count(&db), 1);
}

#[test]
fn a_legacy_failure_is_no_rows_and_minus_one() {
    let Some((db, host)) = loans() else {
        return;
    };
    host.run(|| {
        assert!(db::query("SELECT * FROM nowhere").is_empty());
        assert_eq!(db::query_one("SELECT * FROM nowhere"), None);
        assert_eq!(db::execute("INSERT INTO nowhere VALUES (1)"), -1);
        assert_eq!(db::execute(&insert(1, "first")), 1);
        assert_eq!(db::execute(&insert(1, "again")), -1, "a duplicate key");
        assert_eq!(db::execute("UPDATE loans SET memo = 'x' WHERE false"), 0);
        assert_eq!(db::execute("SELECT 1"), 1);
        assert_eq!(
            db::execute("CREATE INDEX CONCURRENTLY loans_memo ON loans (memo)"),
            -1,
            "a transaction pooler cannot carry what runs outside a transaction"
        );
        assert_eq!(db::execute("SET search_path TO public"), -1);
    });
    loan_host(&db, Pooler::Direct).run(|| {
        assert_eq!(
            db::execute("CREATE INDEX CONCURRENTLY loans_memo ON loans (memo)"),
            0,
            "what cannot run in a transaction runs outside one"
        );
        assert_eq!(
            db::query_one("SELECT current_setting('search_path') AS path"),
            Some(json!({"path": "loan_service, public"}))
        );
    });
    assert_eq!(count(&db), 1);
}

#[test]
fn a_batch_commits_every_statement_or_none() {
    let Some((db, host)) = loans() else {
        return;
    };
    let (first, second, third) = (insert(1, "one"), insert(2, "two"), insert(1, "again"));
    host.run(|| {
        assert_eq!(db::execute_all(&[]), Ok(vec![]));
        assert_eq!(
            db::execute_all(&[&first, &second, "UPDATE loans SET loan_kind = 'Renewal'"]),
            Ok(vec![1, 1, 2])
        );
        assert_eq!(
            db::execute_all(&["DELETE FROM loans", &third, &third]),
            Err(
                "statement 3 of 3 failed, batch rolled back: ERROR: duplicate key value \
                 violates unique constraint \"loans_pkey\"\nDETAIL: Key (id)=\
                 (00000000-0000-4000-8000-000000000001) already exists."
                    .to_string()
            )
        );
        assert_eq!(
            db::execute_all(&["SELECT 1; SELECT 2"]).map_err(|e| e.contains("statement 1 of 1")),
            Err(true),
            "one statement per slot"
        );
    });
    assert_eq!(count(&db), 2, "the batch that failed deleted nothing");
}

#[test]
fn a_read_only_query_names_its_schema_and_writes_nothing() {
    let Some(db) = database() else {
        return;
    };
    db.migrate("loan_service", &loan_migrations(), &legacy_history())
        .expect("migrations");
    db.execute(
        "CREATE FUNCTION loan_service.purge() RETURNS bigint LANGUAGE sql SECURITY DEFINER \
         AS 'WITH gone AS (DELETE FROM loan_service.loans RETURNING 1) SELECT count(*) FROM gone'",
    )
    .expect("a function that writes");
    let granted = "GRANT USAGE ON SCHEMA loan_service TO reports_ro_test; \
                   GRANT SELECT ON loan_service.loans TO reports_ro_test";
    db.execute(&format!("CREATE ROLE reports_ro_test NOLOGIN; {granted}"))
        .or_else(|_| db.execute(granted))
        .expect("the read-only role, which another test's database may have created");
    let host = TestHost::builder()
        .service("loan-service")
        .db(&db)
        .read_only_role("reports_ro_test")
        .read_only_max_rows(2)
        .build()
        .expect("host");
    host.run(|| {
        (1..=3).for_each(|id| assert_eq!(db::execute(&insert(id, "loan")), 1));
        let found = db::query_read_only(
            "SELECT memo, current_user AS role FROM loan_service.loans ORDER BY id -- all",
        )
        .expect("rows");
        assert_eq!(
            found,
            json!({
                "ok": true,
                "row_count": 2,
                "truncated": true,
                "rows": [
                    {"memo": "loan", "role": "reports_ro_test"},
                    {"memo": "loan", "role": "reports_ro_test"},
                ],
            })
        );
        let exact = db::query_read_only("SELECT 1 AS one").expect("rows");
        assert_eq!(
            (&exact["row_count"], &exact["truncated"]),
            (&json!(1), &json!(false))
        );
        assert_eq!(
            db::query_read_only("SELECT * FROM loans"),
            Err("ERROR: relation \"loans\" does not exist".to_string()),
            "the service's schema is not on the path"
        );
        assert_eq!(
            db::query_read_only("DELETE FROM loan_service.loans RETURNING id"),
            Err("ERROR: syntax error at or near \"FROM\"".to_string()),
            "only what can be selected from"
        );
        assert_eq!(
            db::query_read_only(
                "WITH gone AS (DELETE FROM loan_service.loans RETURNING id) SELECT * FROM gone"
            ),
            Err(
                "ERROR: WITH clause containing a data-modifying statement must be at the top level"
                    .to_string()
            )
        );
        let written = db::query_read_only("SELECT loan_service.purge() AS gone")
            .expect_err("a write the wrapper could not see");
        assert!(written.ends_with("in a read-only transaction"), "{written}");
    });
    assert_eq!(count(&db), 3);
    let unknown = TestHost::builder()
        .db(&db)
        .read_only_role("nobody_at_all")
        .build()
        .map(drop)
        .expect_err("a role that is not there");
    assert_eq!(
        unknown.to_string(),
        "ERROR: role \"nobody_at_all\" does not exist"
    );
    let connection = TestHost::builder()
        .db(&db)
        .role(PgRole::Connection)
        .read_only_role("nobody_at_all")
        .build()
        .expect("host");
    assert_eq!(
        connection.run(|| db::query_read_only("SELECT 1")),
        Err("session setup failed: ERROR: role \"nobody_at_all\" does not exist".to_string())
    );
}

fn host_of(db: &TestDb) -> PgHost {
    PgHost::connect(db, "loan_service", PgOptions::default()).expect("host")
}

fn query(sql: &str, params: Vec<Param>, tx: Option<u32>) -> DbRequest {
    DbRequest::Query {
        sql: sql.into(),
        params,
        tx,
    }
}

fn execute(sql: &str, params: Vec<Param>, tx: Option<u32>) -> DbRequest {
    DbRequest::Execute {
        sql: sql.into(),
        params,
        tx,
    }
}

fn loan(id: u32, memo: &str) -> Vec<Param> {
    vec![
        Param::Uuid(format!("00000000-0000-4000-8000-{id:012}")),
        Param::Uuid(BOOK.into()),
        Param::Text(memo.into()),
        Param::Uuid(AUTHOR.into()),
        Param::NaiveTimestamp("2026-09-28T10:00:00".into()),
    ]
}

const INSERT: &str = "INSERT INTO loans (id, book_id, memo, created_by, created_at, updated_at) \
                      VALUES ($1, $2, $3, $4, $5, $5)";

fn rows(response: DbResponse) -> Vec<Value> {
    match response {
        DbResponse::Rows { rows, .. } => rows.into_iter().map(Value::Object).collect(),
        other => panic!("{other:?}"),
    }
}

fn kind(response: DbResponse) -> DbErrorKind {
    match response {
        DbResponse::Failed(error) => error.kind,
        other => panic!("{other:?}"),
    }
}

fn begin(host: &PgHost) -> u32 {
    match host.call(&DbRequest::Begin {
        isolation: Isolation::ReadCommitted,
        read_only: false,
    }) {
        DbResponse::Begun(tx) => tx,
        other => panic!("{other:?}"),
    }
}

#[test]
fn bound_parameters_take_the_types_the_statement_reports() {
    let Some((_db, test)) = loans() else {
        return;
    };
    let host = test.db.clone().expect("a database");
    assert_eq!(
        host.call(&execute(INSERT, loan(1, "it's bound"), None)),
        DbResponse::Affected(1)
    );
    let found = host.call(&query(
        "SELECT memo, loan_kind, created_at, 1.50::numeric AS price, \
         ARRAY['a', NULL] AS tags FROM loans WHERE id = $1 AND memo = $2",
        vec![
            Param::Uuid("00000000-0000-4000-8000-000000000001".into()),
            Param::Text("it's bound".into()),
        ],
        None,
    ));
    let DbResponse::Rows { rows, columns } = found else {
        panic!("{found:?}");
    };
    assert_eq!(
        rows.into_iter().map(Value::Object).collect::<Vec<_>>(),
        [json!({
            "memo": "it's bound",
            "loan_kind": "Standard",
            "created_at": "2026-09-28T10:00:00",
            "price": "1.50",
            "tags": ["a", null],
        })]
    );
    assert_eq!(
        columns
            .iter()
            .map(|column| (column.name.as_str(), column.type_name.as_str()))
            .collect::<Vec<_>>(),
        [
            ("memo", "text"),
            ("loan_kind", "varchar"),
            ("created_at", "timestamp"),
            ("price", "numeric"),
            ("tags", "_text"),
        ]
    );
    assert_eq!(
        kind(host.call(&query(
            "SELECT 1 FROM loans WHERE id = $1",
            vec![Param::Text("not a uuid".into())],
            None
        ))),
        DbErrorKind::InvalidParameter
    );
}

#[test]
fn a_server_error_carries_its_fields() {
    let Some((_db, test)) = loans() else {
        return;
    };
    let host = test.db.clone().expect("a database");
    assert_eq!(
        host.call(&execute(INSERT, loan(1, "first"), None)),
        DbResponse::Affected(1)
    );
    let DbResponse::Failed(duplicate) = host.call(&execute(INSERT, loan(1, "again"), None)) else {
        panic!("accepted");
    };
    assert_eq!(duplicate.kind, DbErrorKind::UniqueViolation);
    assert!(duplicate.is_constraint_violation() && !duplicate.is_retryable());
    assert_eq!(duplicate.code.as_deref(), Some("23505"));
    assert_eq!(duplicate.constraint.as_deref(), Some("loans_pkey"));
    assert_eq!(duplicate.table.as_deref(), Some("loans"));
    assert_eq!(duplicate.schema.as_deref(), Some("loan_service"));
    assert_eq!(
        duplicate.detail.as_deref(),
        Some("Key (id)=(00000000-0000-4000-8000-000000000001) already exists.")
    );
    assert_eq!(
        kind(host.call(&query("SELECT * FROM nowhere", vec![], None))),
        DbErrorKind::UndefinedTable
    );
    assert_eq!(
        kind(host.call(&query("SELEKT 1", vec![], None))),
        DbErrorKind::SyntaxError
    );
    assert_eq!(
        kind(host.call(&execute(
            "INSERT INTO loans (id) VALUES ($1)",
            vec![Param::Uuid(BOOK.into())],
            None
        ))),
        DbErrorKind::NotNullViolation
    );
}

#[test]
fn a_statement_may_not_control_the_transaction_it_runs_in() {
    let Some((db, test)) = loans() else {
        return;
    };
    let host = test.db.clone().expect("a database");
    for sql in [
        "COMMIT",
        "BEGIN",
        "ROLLBACK",
        "SAVEPOINT mine",
        "-- done\nEND",
    ] {
        let DbResponse::Failed(refused) = host.call(&execute(sql, vec![], None)) else {
            panic!("{sql} was run");
        };
        assert_eq!(refused.kind, DbErrorKind::NotSupported, "{sql}");
        assert!(refused
            .message
            .starts_with("transaction control is refused"));
    }
    let DbResponse::Failed(pooled) = host.call(&execute("VACUUM loans", vec![], None)) else {
        panic!("VACUUM was run behind a transaction pooler");
    };
    assert_eq!(pooled.kind, DbErrorKind::NotSupported);
    let direct = loan_host(&db, Pooler::Direct).db.expect("a database");
    assert_eq!(
        direct.call(&execute("VACUUM loans", vec![], None)),
        DbResponse::Affected(0)
    );
    let DbResponse::Failed(bound) =
        direct.call(&execute("VACUUM loans", vec![Param::I32(1)], None))
    else {
        panic!("run");
    };
    assert_eq!(
        (bound.kind, bound.message.as_str()),
        (
            DbErrorKind::NotSupported,
            "a statement that cannot run inside a transaction takes no parameters"
        )
    );
}

#[test]
fn a_transaction_is_seen_by_others_once_it_commits() {
    let Some((db, test)) = loans() else {
        return;
    };
    let host = test.db.clone().expect("a database");
    let tx = begin(&host);
    assert_eq!(tx, 1);
    assert_eq!(
        host.call(&execute(INSERT, loan(1, "held"), Some(tx))),
        DbResponse::Affected(1)
    );
    assert_eq!(
        rows(host.call(&query(
            "SELECT count(*) AS loans FROM loans",
            vec![],
            Some(tx)
        ))),
        [json!({"loans": 1})]
    );
    assert_eq!(count(&db), 0, "not yet committed");
    assert_eq!(host.open_transactions(), 1);
    assert_eq!(host.call(&DbRequest::Commit { tx }), DbResponse::Done);
    assert_eq!((count(&db), host.open_transactions()), (1, 0));
    assert_eq!(
        kind(host.call(&DbRequest::Commit { tx })),
        DbErrorKind::TransactionNotFound
    );

    let undone = begin(&host);
    assert_eq!(undone, 2);
    assert_eq!(
        host.call(&execute("DELETE FROM loans", vec![], Some(undone))),
        DbResponse::Affected(1)
    );
    assert_eq!(
        host.call(&DbRequest::Rollback { tx: undone }),
        DbResponse::Done
    );
    assert_eq!(count(&db), 1);
    assert_eq!(
        kind(host.call(&query("SELECT 1", vec![], Some(undone)))),
        DbErrorKind::TransactionNotFound
    );
}

#[test]
fn a_failed_transaction_admits_only_its_ways_out() {
    let Some((db, test)) = loans() else {
        return;
    };
    let host = test.db.clone().expect("a database");
    let tx = begin(&host);
    assert_eq!(
        host.call(&execute(INSERT, loan(1, "kept"), Some(tx))),
        DbResponse::Affected(1)
    );
    let refused = host.call(&query(
        "SELECT 1 FROM loans WHERE id = $1",
        vec![Param::Text("not a uuid".into())],
        Some(tx),
    ));
    assert_eq!(kind(refused), DbErrorKind::InvalidParameter);
    assert_eq!(
        rows(host.call(&query("SELECT 1 AS alive", vec![], Some(tx)))),
        [json!({"alive": 1})],
        "what the host refused never reached the server"
    );
    assert_eq!(
        kind(host.call(&execute(INSERT, loan(1, "again"), Some(tx)))),
        DbErrorKind::UniqueViolation
    );
    for request in [
        query("SELECT 1", vec![], Some(tx)),
        DbRequest::Savepoint {
            tx,
            name: "sp_0".into(),
        },
    ] {
        let DbResponse::Failed(aborted) = host.call(&request) else {
            panic!("{request:?} was run");
        };
        assert_eq!(aborted.kind, DbErrorKind::InFailedTransaction);
        assert!(aborted.message.ends_with("first"), "{aborted}");
    }
    let DbResponse::Failed(commit) = host.call(&DbRequest::Commit { tx }) else {
        panic!("committed");
    };
    assert_eq!(
        (commit.kind, commit.message.as_str()),
        (
            DbErrorKind::InFailedTransaction,
            "the transaction was aborted by an earlier error; COMMIT rolled it back"
        )
    );
    assert_eq!((count(&db), host.open_transactions()), (0, 0));
}

#[test]
fn a_savepoint_recovers_a_transaction() {
    let Some((db, test)) = loans() else {
        return;
    };
    let host = test.db.clone().expect("a database");
    let tx = begin(&host);
    let savepoint = |op: fn(u32, String) -> DbRequest, name: &str| host.call(&op(tx, name.into()));
    let create = |tx, name| DbRequest::Savepoint { tx, name };
    let release = |tx, name| DbRequest::Release { tx, name };
    let rollback_to = |tx, name| DbRequest::RollbackTo { tx, name };
    assert_eq!(
        host.call(&execute(INSERT, loan(1, "kept"), Some(tx))),
        DbResponse::Affected(1)
    );
    assert_eq!(savepoint(create, "sp_0"), DbResponse::Done);
    assert_eq!(
        kind(host.call(&execute(INSERT, loan(1, "again"), Some(tx)))),
        DbErrorKind::UniqueViolation
    );
    assert_eq!(
        kind(savepoint(release, "sp_0")),
        DbErrorKind::InFailedTransaction
    );
    assert_eq!(savepoint(rollback_to, "sp_0"), DbResponse::Done);
    assert_eq!(
        host.call(&execute(INSERT, loan(2, "after"), Some(tx))),
        DbResponse::Affected(1)
    );
    assert_eq!(savepoint(release, "sp_0"), DbResponse::Done);
    for (request, message) in [
        (
            release(tx, "sp_0".into()),
            "no savepoint sp_0 in transaction 1",
        ),
        (
            rollback_to(tx, "sp_7".into()),
            "no savepoint sp_7 in transaction 1",
        ),
        (create(tx, "mine".into()), "invalid savepoint name \"mine\""),
        (
            create(tx, "sp_1; DROP TABLE loans".into()),
            "invalid savepoint name \"sp_1; DROP TABLE loans\"",
        ),
    ] {
        let DbResponse::Failed(refused) = host.call(&request) else {
            panic!("{request:?} was run");
        };
        assert_eq!(
            (refused.kind, refused.message.as_str()),
            (DbErrorKind::InvalidParameter, message)
        );
    }
    assert_eq!(
        rows(host.call(&query("SELECT 1 AS alive", vec![], Some(tx)))),
        [json!({"alive": 1})],
        "a refused savepoint leaves the transaction as it was"
    );
    assert_eq!(host.call(&DbRequest::Commit { tx }), DbResponse::Done);
    assert_eq!(count(&db), 2);
}

#[test]
fn transactions_are_bounded_and_read_only_ones_refuse_writes() {
    let Some((db, _test)) = loans() else {
        return;
    };
    let host = host_of(&db);
    let (first, second) = (begin(&host), begin(&host));
    assert_eq!((first, second), (1, 2));
    let DbResponse::Failed(full) = host.call(&DbRequest::Begin {
        isolation: Isolation::Serializable,
        read_only: false,
    }) else {
        panic!("a third transaction");
    };
    assert_eq!(
        (full.kind, full.message.as_str()),
        (
            DbErrorKind::TransactionLimit,
            "at most 2 transactions may be open at once"
        )
    );
    assert_eq!(
        host.call(&DbRequest::Rollback { tx: first }),
        DbResponse::Done
    );
    let DbResponse::Begun(reader) = host.call(&DbRequest::Begin {
        isolation: Isolation::RepeatableRead,
        read_only: true,
    }) else {
        panic!("refused");
    };
    assert_eq!(reader, 3);
    assert_eq!(
        rows(host.call(&query(
            "SELECT current_setting('transaction_isolation') AS isolation, \
             current_setting('transaction_read_only') AS read_only, \
             current_setting('search_path') AS path",
            vec![],
            Some(reader)
        ))),
        [json!({
            "isolation": "repeatable read",
            "read_only": "on",
            "path": "loan_service, public",
        })]
    );
    let DbResponse::Failed(write) = host.call(&execute(INSERT, loan(1, "no"), Some(reader))) else {
        panic!("written");
    };
    assert_eq!(write.code.as_deref(), Some("25006"));
    assert_eq!(host.open_transactions(), 2);
    assert_eq!(host.abandon_open_transactions(), 2);
    assert_eq!((host.open_transactions(), count(&db)), (0, 0));
}

#[test]
fn what_a_handler_left_open_is_rolled_back_when_it_returns() {
    let Some((db, test)) = loans() else {
        return;
    };
    let host = test.db.clone().expect("a database");
    test.run(|| {
        let tx = begin(&host);
        assert_eq!(
            host.call(&execute(INSERT, loan(1, "leaked"), Some(tx))),
            DbResponse::Affected(1)
        );
        test.run(|| assert_eq!(host.open_transactions(), 1));
        assert_eq!(host.open_transactions(), 1, "an inner run ends nothing");
    });
    assert_eq!((host.open_transactions(), count(&db)), (0, 0));
}

#[test]
fn a_schema_is_named_as_the_platform_names_it() {
    let Some(db) = database() else {
        return;
    };
    db.execute("CREATE SCHEMA \"order\"; CREATE TABLE \"order\".lines (id int)")
        .expect("a schema that is a keyword");
    let host = TestHost::builder()
        .service("order-lines")
        .schema("Order")
        .db(&db)
        .build()
        .expect("host");
    assert_eq!(host.db.as_ref().map(PgHost::schema), Some("Order"));
    host.run(|| {
        assert_eq!(db::execute("INSERT INTO lines VALUES (1)"), 1);
        assert_eq!(db::query("SELECT id FROM lines"), [json!({"id": 1})]);
    });
}

#[test]
fn a_service_logs_in_as_its_own_role_and_reaches_nothing_else() {
    let Some((db, host)) = loans() else {
        return;
    };
    db.execute(
        "CREATE SCHEMA catalog_service; \
         CREATE TABLE catalog_service.book (id int); \
         INSERT INTO catalog_service.book VALUES (7)",
    )
    .expect("another service's table");
    let role = db.service_role("loan_service").expect("the role");
    host.run(|| {
        assert_eq!(
            db::query_one("SELECT current_user AS role, session_user AS login"),
            Some(json!({"role": role.name(), "login": role.name()}))
        );
        assert_eq!(db::execute(&insert(1, "mine")), 1);
        assert!(
            db::query("SELECT id FROM catalog_service.book").is_empty(),
            "another service's schema is not granted"
        );
        assert_eq!(db::execute("CREATE TABLE public.spill (id int)"), -1);
        assert_eq!(db::execute("CREATE TABLE extras (id int)"), 0);
    });
    let DbResponse::Failed(denied) = host.db.as_ref().expect("a database").call(&query(
        "SELECT id FROM catalog_service.book",
        vec![],
        None,
    )) else {
        panic!("another service's table was read");
    };
    assert_eq!(
        (denied.kind, denied.message.as_str()),
        (
            DbErrorKind::InsufficientPrivilege,
            "permission denied for schema catalog_service"
        )
    );
    TestHost::builder()
        .service("loan-service")
        .db(&db)
        .role(PgRole::Connection)
        .build()
        .expect("host")
        .run(|| {
            assert_eq!(
                db::query("SELECT id FROM catalog_service.book"),
                [json!({"id": 7})],
                "the server's user, as under db_roles: off"
            );
        });
}

#[test]
fn a_role_set_on_the_connection_is_the_one_statements_run_as() {
    let Some(db) = database() else {
        return;
    };
    db.migrate("loan_service", &loan_migrations(), &legacy_history())
        .expect("migrations");
    let role = db.service_role("loan_service").expect("the role");
    let host = TestHost::builder()
        .service("loan-service")
        .db(&db)
        .role(PgRole::Set(role.name().to_string()))
        .pooler(Pooler::Direct)
        .build()
        .expect("host");
    host.run(|| {
        assert_eq!(
            db::query_one("SELECT current_user AS role"),
            Some(json!({"role": role.name()}))
        );
        assert_eq!(
            db::execute("CREATE INDEX CONCURRENTLY loans_created ON loans (created_at)"),
            0,
            "outside a transaction the role is set for the statement and reset after it"
        );
        assert_ne!(
            db::query_one("SELECT session_user AS login"),
            Some(json!({"login": role.name()}))
        );
    });
    assert_eq!(
        db.query(
            "SELECT tableowner FROM pg_tables \
             WHERE schemaname = 'loan_service' AND tablename = 'loans'"
        )
        .expect("owner"),
        [json!({"tableowner": role.name()})]
    );
}

#[test]
fn a_transaction_pooler_refuses_what_the_platform_refuses() {
    let Some((_db, test)) = loans() else {
        return;
    };
    let host = test.db.clone().expect("a database");
    for sql in [
        "CREATE INDEX CONCURRENTLY loans_memo ON loans (memo)",
        "VACUUM loans",
        "SET search_path TO public",
        "RESET ALL",
        "SET ROLE postgres",
    ] {
        let refusal = pooler_refusal(Pooler::PgBouncerTransaction, sql)
            .expect("refused by the platform")
            .message();
        let DbResponse::Failed(refused) = host.call(&execute(sql, vec![], None)) else {
            panic!("{sql} was run");
        };
        assert_eq!(
            (refused.kind, refused.message.as_str()),
            (DbErrorKind::NotSupported, refusal),
            "{sql}"
        );
        test.run(|| {
            assert_eq!(db::execute(sql), -1, "{sql}");
            assert_eq!(
                db::execute_all(&["SELECT 1", sql]),
                Err(format!(
                    "statement 2 of 2 failed, batch rolled back: {refusal}"
                )),
                "{sql}"
            );
        });
    }
    for sql in ["SET LOCAL statement_timeout = 5", "SELECT 1"] {
        assert_eq!(pooler_refusal(Pooler::PgBouncerTransaction, sql), None);
        assert!(
            !matches!(
                host.call(&execute(sql, vec![], None)),
                DbResponse::Failed(_)
            ),
            "{sql}"
        );
    }
}

#[test]
fn migrations_run_as_the_service_role_and_may_not_leave_it() {
    let Some(db) = database() else {
        return;
    };
    let dir = Scratch::with(&[
        (
            "V1__loans.sql",
            "CREATE TABLE loans (id int PRIMARY KEY, memo text);",
        ),
        (
            "V2__escapes.sql",
            "RESET ROLE;\nCREATE TABLE stray (id int);",
        ),
    ]);
    let reason = match db.migrate("loan_service", dir.path(), &MigrationHistory::default()) {
        Err(TestHostError::Migration(reason)) => reason,
        other => panic!("{other:?}"),
    };
    assert!(
        reason.starts_with("migration V2__escapes changed the role it ran as"),
        "{reason}"
    );
    let role = db.service_role("loan_service").expect("the role");
    assert_eq!(
        db.query(
            "SELECT tablename, tableowner FROM pg_tables \
             WHERE schemaname = 'loan_service' ORDER BY tablename"
        )
        .expect("owners"),
        [
            json!({"tablename": "loans", "tableowner": role.name()}),
            json!({"tablename": "refinery_schema_history", "tableowner": role.name()}),
        ],
        "the history, made by the server's user, is the service's once it is provisioned"
    );
    assert_eq!(
        db.query("SELECT version FROM loan_service.refinery_schema_history")
            .expect("history"),
        [json!({"version": 1})]
    );
}

#[test]
fn an_interrupted_concurrent_build_is_dropped_before_it_runs_again() {
    let Some(db) = database() else {
        return;
    };
    let dir = Scratch::with(&[
        (
            "V1__loans.sql",
            "CREATE TABLE loans (id int PRIMARY KEY, memo text); \
             INSERT INTO loans VALUES (1, 'a'), (2, 'a');",
        ),
        (
            "V2__unique_memo.sql",
            "-- cufflink:no-transaction\nCREATE UNIQUE INDEX CONCURRENTLY loans_memo ON loans (memo);",
        ),
    ]);
    let failed = db.migrate("loan_service", dir.path(), &MigrationHistory::default());
    assert!(
        matches!(&failed, Err(TestHostError::Migration(reason)) if reason.starts_with("migration V2__unique_memo failed")),
        "{failed:?}"
    );
    let invalid = || {
        db.query(
            "SELECT c.relname AS index FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
             WHERE NOT i.indisvalid",
        )
        .expect("invalid indexes")
    };
    assert_eq!(invalid(), [json!({"index": "loans_memo"})]);
    db.execute("UPDATE loan_service.loans SET memo = 'b' WHERE id = 2")
        .expect("the duplicate fixed");
    db.migrate("loan_service", dir.path(), &MigrationHistory::default())
        .expect("the build again");
    assert!(invalid().is_empty());
}

#[test]
fn the_roles_of_a_database_go_with_it() {
    let (Some(db), Some(other)) = (database(), database()) else {
        return;
    };
    let role = db.service_role("loan_service").expect("the role");
    assert_eq!(
        db.service_role("Loan_Service").expect("the same role"),
        role,
        "one role for a service, named as the platform names its schema"
    );
    let roles = || {
        other
            .query(&format!(
                "SELECT count(*) AS roles FROM pg_roles WHERE rolname = '{}'",
                role.name()
            ))
            .expect("pg_roles")
    };
    assert_eq!(roles(), [json!({"roles": 1})]);
    drop(db);
    assert_eq!(roles(), [json!({"roles": 0})]);
}
