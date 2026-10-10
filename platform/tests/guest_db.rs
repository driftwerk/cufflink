use cufflink_testkit as common;

use cufflink_db::{PoolSettings, Recycling};
use cufflink_platform::tenant_sessions::STATEMENT_STOP_EVERY;
use cufflink_platform::wasm_runtime::host::{run_db_batch, run_guest_statement, GuestStatement};
use deadpool_postgres::Pool;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const BUDGET: Duration = Duration::from_secs(30);

async fn guest(pool: &Pool, schema: &str) -> cufflink_platform::tenant_sessions::Session {
    cufflink_platform::tenant_sessions::Session::over(pool.get().await.expect("checkout"), schema)
}

struct Session {
    pool: Pool,
    schema: &'static str,
    backend_pid: i32,
}

impl Session {
    async fn open(db: &common::TestDb, schema: &'static str) -> Self {
        let settings = PoolSettings {
            recycling: Recycling::Fast,
            ..PoolSettings::from_vars(1, |_| None).expect("defaults")
        };
        let pool = cufflink_db::build_pool_with(
            cufflink_db::parse_config(&db.url).expect("url"),
            settings,
        )
        .expect("one-connection pool");
        let client = pool.get().await.expect("checkout");
        client
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; \
                 CREATE TABLE {schema}.widgets (id int primary key, ord int);"
            ))
            .await
            .expect("create fixture schema");
        let backend_pid = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        drop(client);
        Self {
            pool,
            schema,
            backend_pid,
        }
    }

    async fn ids(&self) -> Vec<i32> {
        self.pool
            .get()
            .await
            .expect("checkout")
            .query(
                &format!("SELECT id FROM {}.widgets ORDER BY id", self.schema),
                &[],
            )
            .await
            .expect("select")
            .iter()
            .map(|r| r.get::<_, i32>(0))
            .collect()
    }

    async fn assert_clean(&self) {
        let client = self.pool.get().await.expect("checkout");
        let pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        assert_eq!(pid, self.backend_pid, "the pooled session was replaced");
        client
            .batch_execute("SET LOCAL statement_timeout = '17s'")
            .await
            .expect("probe");
        let timeout: String = client
            .query_one("SHOW statement_timeout", &[])
            .await
            .expect("show")
            .get(0);
        assert_ne!(timeout, "17s", "session left inside a transaction");
        assert_eq!(timeout, "30s", "statement_timeout leaked");
        let search_path: String = client
            .query_one("SHOW search_path", &[])
            .await
            .expect("show")
            .get(0);
        assert!(
            !search_path.contains(self.schema),
            "search_path leaked: {search_path}"
        );
    }
}

#[tokio::test]
async fn db_batch_commits_every_statement_or_none() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let session = Session::open(&db, "batch_commit").await;

    let counts = run_db_batch(
        &mut guest(&session.pool, session.schema).await,
        &[
            "INSERT INTO widgets (id, ord) VALUES (1, 10)".into(),
            "INSERT INTO widgets (id, ord) VALUES (2, 20)".into(),
            "UPDATE widgets SET ord = ord + 1".into(),
        ],
        BUDGET,
    )
    .await
    .expect("batch should commit");
    assert_eq!(counts, vec![1, 1, 2]);
    assert_eq!(session.ids().await, vec![1, 2]);

    let failure = run_db_batch(
        &mut guest(&session.pool, session.schema).await,
        &[
            "INSERT INTO widgets (id, ord) VALUES (3, 30)".into(),
            "INSERT INTO widgets (id, ord) VALUES (1, 20)".into(),
            "INSERT INTO widgets (id, ord) VALUES (4, 40)".into(),
        ],
        BUDGET,
    )
    .await
    .expect_err("duplicate key must abort the batch");
    assert_eq!(failure.index, Some(1));
    assert!(
        failure.error.contains("duplicate key"),
        "unexpected error: {}",
        failure.error
    );
    assert_eq!(
        session.ids().await,
        vec![1, 2],
        "the statement before the failure is rolled back too"
    );
    session.assert_clean().await;
}

#[tokio::test]
async fn db_batch_leaves_the_pooled_connection_usable_after_failures() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let session = Session::open(&db, "batch_hygiene").await;

    for _ in 0..8 {
        let _ = run_db_batch(
            &mut guest(&session.pool, session.schema).await,
            &["SELECT * FROM no_such_table".into()],
            BUDGET,
        )
        .await;
    }
    let status = session.pool.status();
    assert_eq!(
        status.available, status.size,
        "failed batches left a connection checked out of the pool"
    );
    session.assert_clean().await;
}

#[tokio::test]
async fn db_batch_holds_an_advisory_lock_across_statements() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let session = Session::open(&db, "batch_lock").await;
    let pool = cufflink_db::build_pool(&db.url, 4).expect("pool");

    let batch = |id: i32| {
        let pool = pool.clone();
        async move {
            run_db_batch(
                &mut guest(&pool, session.schema).await,
                &[
                    "SELECT pg_advisory_xact_lock(hashtext('item-42')::bigint)".into(),
                    format!(
                        "INSERT INTO widgets (id, ord) \
                         SELECT {id}, COALESCE((SELECT MAX(ord) + 1 FROM widgets), 0)"
                    ),
                ],
                BUDGET,
            )
            .await
        }
    };
    let (a, b) = tokio::join!(batch(1), batch(2));
    a.expect("first batch");
    b.expect("second batch");

    let orders: Vec<i32> = pool
        .get()
        .await
        .expect("checkout")
        .query(
            &format!("SELECT ord FROM {}.widgets ORDER BY ord", session.schema),
            &[],
        )
        .await
        .expect("select")
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(orders, vec![0, 1], "allocation was not serialized");
}

#[tokio::test]
async fn guest_statements_run_under_the_schema_and_budget_and_leave_nothing_behind() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let session = Session::open(&db, "guest_stmt").await;
    let fixture = &session;
    let run = |budget: Duration, stmt: GuestStatement<'static>| async move {
        run_guest_statement(
            &mut guest(&fixture.pool, fixture.schema).await,
            budget,
            stmt,
        )
        .await
    };

    let affected = run(
        Duration::from_secs(10),
        GuestStatement::Execute("INSERT INTO widgets (id, ord) VALUES (1, 1)"),
    )
    .await
    .expect("insert")
    .affected();
    assert_eq!(affected, 1);
    let rows = run(
        Duration::from_secs(10),
        GuestStatement::Query("SELECT current_setting('statement_timeout') AS t, id FROM widgets"),
    )
    .await
    .expect("select")
    .rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("t"), "9500ms");
    session.assert_clean().await;

    let returned = run(
        Duration::from_secs(10),
        GuestStatement::Query("INSERT INTO widgets (id, ord) VALUES (2, 2) RETURNING id, ord"),
    )
    .await
    .expect("insert returning")
    .rows();
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].get::<_, i32>("id"), 2);
    assert_eq!(session.ids().await, vec![1, 2]);

    let err = run(
        Duration::from_secs(10),
        GuestStatement::Query("SELECT * FROM no_such_table"),
    )
    .await
    .err()
    .expect("missing table errors");
    assert!(err.to_string().contains("no_such_table"), "{err}");
    session.assert_clean().await;
    let err = run(
        Duration::from_millis(100),
        GuestStatement::Execute("SELECT pg_sleep(5)"),
    )
    .await
    .err()
    .expect("statement timeout fires at the 1s floor");
    assert!(err.to_string().contains("statement timeout"), "{err}");
    session.assert_clean().await;

    run(
        Duration::from_secs(10),
        GuestStatement::Execute("VACUUM widgets"),
    )
    .await
    .expect("vacuum runs outside a transaction");
    session.assert_clean().await;
}

async fn sleeping(pool: &Pool) -> i64 {
    pool.get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM pg_stat_activity
              WHERE datname = current_database() AND state = 'active'
                AND query LIKE '%pg_sleep(30)%' AND pid <> pg_backend_pid()",
            &[],
        )
        .await
        .expect("activity")
        .get(0)
}

#[tokio::test]
async fn a_cancelled_executions_statement_is_stopped_by_the_server() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let pool = db.pool();
    let slow = || GuestStatement::Execute("SELECT pg_sleep(30)");

    for cancelled_after in [Some(Duration::from_millis(500)), None] {
        let cancelled = CancellationToken::new();
        match cancelled_after {
            Some(after) => {
                let cancelled = cancelled.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(after).await;
                    cancelled.cancel();
                });
            }
            None => cancelled.cancel(),
        }
        let mut session = guest(&pool, "public").await;
        let started = Instant::now();
        let stopped = session
            .stop()
            .stopping(
                &cancelled,
                run_guest_statement(&mut session, BUDGET, slow()),
            )
            .await
            .err()
            .expect("the statement is stopped");
        assert!(
            stopped.to_string().contains("canceling statement"),
            "{stopped}"
        );
        assert!(
            started.elapsed() < STATEMENT_STOP_EVERY * 4,
            "{cancelled_after:?}: stopped after {:?}",
            started.elapsed()
        );
        assert_eq!(sleeping(&pool).await, 0, "nothing runs on at the server");
        run_guest_statement(&mut session, BUDGET, GuestStatement::Query("SELECT 1"))
            .await
            .expect("the session is as the statement's end left it");
        session.release(&cancelled);
    }

    let mut session = guest(&pool, "public").await;
    run_guest_statement(&mut session, BUDGET, GuestStatement::Query("SELECT 1"))
        .await
        .expect("a session after those that were closed");
    let unhurried = session
        .stop()
        .stopping(
            &CancellationToken::new(),
            run_guest_statement(
                &mut session,
                BUDGET,
                GuestStatement::Execute("SELECT pg_sleep(2)"),
            ),
        )
        .await;
    assert!(unhurried.is_ok(), "nothing stops what nothing cancelled");
}

#[tokio::test]
async fn guest_transaction_control_cannot_leak_a_transaction() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let session = Session::open(&db, "guest_txn").await;

    for sql in ["COMMIT", "BEGIN", "ROLLBACK"] {
        let outcome = run_guest_statement(
            &mut guest(&session.pool, session.schema).await,
            Duration::from_secs(10),
            GuestStatement::Execute(sql),
        )
        .await;
        assert!(outcome.is_ok(), "{sql}: {:?}", outcome.err());
        session.assert_clean().await;
    }
    assert_eq!(
        run_guest_statement(
            &mut guest(&session.pool, session.schema).await,
            Duration::from_secs(10),
            GuestStatement::Execute("INSERT INTO widgets (id, ord) VALUES (9, 9)"),
        )
        .await
        .expect("the session still takes statements")
        .affected(),
        1
    );
    assert_eq!(session.ids().await, vec![9]);
}
