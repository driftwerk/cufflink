use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_platform::registry::ServiceIdentity;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_platform::tenant_sessions::{TenantSessions, STATEMENT_STOP_EVERY};
use cufflink_platform::wasm_runtime::budget::HostCaps;
use cufflink_platform::wasm_runtime::host::{db_v2_call, AbandonReason, DbSession, DbTarget};
use cufflink_types::db::{DbErrorKind, DbRequest, DbResponse, Isolation, Param, RowMap};
use deadpool_postgres::Pool;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

struct Fixture {
    _db: common::TestDb,
    pool: Pool,
    sessions: TenantSessions,
    identity: ServiceIdentity,
    schema: &'static str,
}

impl Fixture {
    async fn new(schema: &'static str, ddl: &str) -> Option<Self> {
        Self::with_pool_size(schema, ddl, 4).await
    }

    async fn with_pool_size(schema: &'static str, ddl: &str, size: usize) -> Option<Self> {
        let infra = common::infra().await?;
        let db = infra.create_database().await;
        let pool = cufflink_db::build_pool_with(
            cufflink_db::parse_config(&db.url).expect("url"),
            cufflink_db::PoolSettings::from_vars(size, |_| None).expect("defaults"),
        )
        .expect("pool");
        pool.get()
            .await
            .expect("checkout")
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; SET search_path TO {schema}; {ddl}"
            ))
            .await
            .expect("fixture schema");
        Some(Self {
            _db: db,
            sessions: TenantSessions::over_pool(pool.clone(), cufflink_db::Pooler::Direct),
            identity: ServiceIdentity {
                tenant_id: uuid::Uuid::nil(),
                tenant_slug: "acme".into(),
                service_id: uuid::Uuid::nil(),
                service_name: schema.replace('_', "-"),
                schema: schema.into(),
            },
            pool,
            schema,
        })
    }

    fn session(&self) -> Client<'_> {
        Client {
            fixture: self,
            session: DbSession::new(2),
            budget: Duration::from_secs(30),
            cancel: CancellationToken::new(),
            caps: HostCaps::default(),
        }
    }
}

struct Client<'a> {
    fixture: &'a Fixture,
    session: DbSession,
    budget: Duration,
    cancel: CancellationToken,
    caps: HostCaps,
}

impl Client<'_> {
    async fn call(&mut self, request: DbRequest) -> DbResponse {
        let target = DbTarget {
            sessions: &self.fixture.sessions,
            identity: &self.fixture.identity,
            deadline: Instant::now() + self.budget,
            cancel: &self.cancel,
        };
        db_v2_call(&mut self.session, &target, request, &self.caps).await
    }

    async fn query_in(&mut self, tx: Option<u32>, sql: &str, params: Vec<Param>) -> Vec<RowMap> {
        match self
            .call(DbRequest::Query {
                sql: sql.into(),
                params,
                tx,
            })
            .await
        {
            DbResponse::Rows { rows, .. } => rows,
            other => panic!("{sql}: {other:?}"),
        }
    }

    async fn query(&mut self, sql: &str, params: Vec<Param>) -> Vec<RowMap> {
        self.query_in(None, sql, params).await
    }

    async fn execute_in(&mut self, tx: Option<u32>, sql: &str, params: Vec<Param>) -> u64 {
        match self
            .call(DbRequest::Execute {
                sql: sql.into(),
                params,
                tx,
            })
            .await
        {
            DbResponse::Affected(n) => n,
            other => panic!("{sql}: {other:?}"),
        }
    }

    async fn failure(&mut self, request: DbRequest) -> cufflink_types::db::DbError {
        match self.call(request).await {
            DbResponse::Failed(error) => error,
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    async fn begin(&mut self) -> u32 {
        match self
            .call(DbRequest::Begin {
                isolation: Isolation::ReadCommitted,
                read_only: false,
            })
            .await
        {
            DbResponse::Begun(tx) => tx,
            other => panic!("begin: {other:?}"),
        }
    }

    async fn done(&mut self, request: DbRequest) {
        assert_eq!(self.call(request).await, DbResponse::Done);
    }
}

fn execute(sql: &str, params: Vec<Param>, tx: Option<u32>) -> DbRequest {
    DbRequest::Execute {
        sql: sql.into(),
        params,
        tx,
    }
}

fn query(sql: &str, params: Vec<Param>, tx: Option<u32>) -> DbRequest {
    DbRequest::Query {
        sql: sql.into(),
        params,
        tx,
    }
}

const KINDS_DDL: &str = "
    CREATE TYPE mood AS ENUM ('sad', 'happy');
    CREATE DOMAIN positive AS int CHECK (VALUE > 0);
    CREATE TABLE kinds (
        id uuid PRIMARY KEY, n smallint, i int, b bigint, f4 real, f8 double precision,
        amount numeric(30, 6), flag boolean, label text, code varchar(10), at timestamptz,
        local timestamp, day date, clock time, doc jsonb, raw bytea, tags text[], ids uuid[],
        counts int[], mood mood, pos positive, span interval, pt point
    );";

#[tokio::test]
async fn db_v2_params_and_types() {
    let Some(fixture) = Fixture::new("v2_types", KINDS_DDL).await else {
        return;
    };
    let mut client = fixture.session();
    let id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    let affected = client
        .execute_in(
            None,
            "INSERT INTO kinds (id, n, i, b, f4, f8, amount, flag, label, code, at, local, day, \
             clock, doc, raw, tags, ids, counts, mood, pos, span, pt) VALUES \
             ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, \
             $19, $20, $21, '1 year 2 mons 3 days 04:05:06.5', '(1,2)')",
            vec![
                Param::Uuid(id.into()),
                Param::I64(7),
                Param::Text("42".into()),
                Param::I64(i64::MAX),
                Param::F32(0.5),
                Param::F64(f64::INFINITY),
                Param::Numeric("12345678901234567890.123456".into()),
                Param::Bool(true),
                Param::Text("it's".into()),
                Param::Uuid("short".into()),
                Param::Timestamp("2026-03-04T05:06:07.123456+02:00".into()),
                Param::NaiveTimestamp("2026-03-04 05:06:07".into()),
                Param::Date("2026-03-04".into()),
                Param::Time("05:06:07".into()),
                Param::Json(json!({"a": [1, null]})),
                Param::Bytes(vec![0, 255]),
                Param::TextArray(vec![Some("a".into()), None]),
                Param::TextArray(vec![Some(id.into())]),
                Param::I64Array(vec![Some(1), None, Some(3)]),
                Param::Text("happy".into()),
                Param::I32(5),
            ],
        )
        .await;
    assert_eq!(affected, 1);

    let rows = client
        .query(
            "SELECT id, n, i, b, f4, f8, amount, flag, label, code, at, local, day, clock, doc, \
             raw, tags, ids, counts, mood, pos, span FROM kinds WHERE id = $1",
            vec![Param::Text(id.into())],
        )
        .await;
    assert_eq!(
        Value::Object(rows.into_iter().next().expect("the row")),
        json!({
            "id": id, "n": 7, "i": 42, "b": i64::MAX, "f4": 0.5, "f8": "Infinity",
            "amount": "12345678901234567890.123456", "flag": true, "label": "it's",
            "code": "short", "at": "2026-03-04T03:06:07.123456Z", "local": "2026-03-04T05:06:07",
            "day": "2026-03-04", "clock": "05:06:07", "doc": {"a": [1, null]}, "raw": "AP8=",
            "tags": ["a", null], "ids": [id], "counts": [1, null, 3], "mood": "happy", "pos": 5,
            "span": "P1Y2M3DT4H5M6.5S"
        })
    );

    match client
        .call(query(
            "SELECT id, amount FROM kinds WHERE false",
            vec![],
            None,
        ))
        .await
    {
        DbResponse::Rows { rows, columns } => {
            assert!(rows.is_empty());
            let described: Vec<(&str, &str)> = columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_name.as_str()))
                .collect();
            assert_eq!(described, vec![("id", "uuid"), ("amount", "numeric")]);
        }
        other => panic!("{other:?}"),
    }

    let unsupported = client
        .failure(query("SELECT pt FROM kinds", vec![], None))
        .await;
    assert_eq!(unsupported.kind, DbErrorKind::UnsupportedType);
    assert!(unsupported.message.contains("column pt"), "{unsupported}");

    let bad_enum = client
        .failure(execute(
            "UPDATE kinds SET mood = $1",
            vec![Param::Text("angry".into())],
            None,
        ))
        .await;
    assert_eq!(
        (bad_enum.kind, bad_enum.code.as_deref()),
        (DbErrorKind::InvalidTextRepresentation, Some("22P02")),
        "the server checks enum labels: {bad_enum}"
    );

    let count = client.failure(query("SELECT $1::int", vec![], None)).await;
    assert_eq!(count.kind, DbErrorKind::InvalidParameter);

    let untyped = client
        .query("SELECT $1 AS v", vec![Param::Text("x".into())])
        .await;
    assert_eq!(untyped[0]["v"], "x");

    let cast = client
        .query(
            "SELECT $1::text::point IS NOT NULL AS ok",
            vec![Param::Text("(3,4)".into())],
        )
        .await;
    assert_eq!(cast[0]["ok"], true);
}

#[tokio::test]
async fn db_v2_errors_are_structured() {
    let Some(fixture) = Fixture::new(
        "v2_errors",
        "CREATE TABLE parents (id int PRIMARY KEY);
         CREATE TABLE children (
             id int PRIMARY KEY,
             parent int REFERENCES parents (id),
             email text NOT NULL CONSTRAINT children_email_key UNIQUE,
             age int CONSTRAINT children_age_check CHECK (age >= 0)
         );
         INSERT INTO parents VALUES (1);
         INSERT INTO children VALUES (1, 1, 'a@x', 1);",
    )
    .await
    else {
        return;
    };
    let mut client = fixture.session();
    let insert = |id: i32, parent: i32, email: Option<&str>, age: i32| {
        execute(
            "INSERT INTO children (id, parent, email, age) VALUES ($1, $2, $3, $4)",
            vec![
                Param::I32(id),
                Param::I32(parent),
                email.map_or(Param::Null, |e| Param::Text(e.into())),
                Param::I32(age),
            ],
            None,
        )
    };
    for (request, kind, code, constraint) in [
        (
            insert(2, 1, Some("a@x"), 1),
            DbErrorKind::UniqueViolation,
            "23505",
            Some("children_email_key"),
        ),
        (
            insert(2, 9, Some("b@x"), 1),
            DbErrorKind::ForeignKeyViolation,
            "23503",
            Some("children_parent_fkey"),
        ),
        (
            insert(2, 1, Some("b@x"), -1),
            DbErrorKind::CheckViolation,
            "23514",
            Some("children_age_check"),
        ),
        (
            insert(2, 1, None, 1),
            DbErrorKind::NotNullViolation,
            "23502",
            None,
        ),
        (
            execute(
                "DO $$ BEGIN RAISE EXCEPTION 'not today'; END $$",
                vec![],
                None,
            ),
            DbErrorKind::RaiseException,
            "P0001",
            None,
        ),
        (
            query("SELECT * FROM missing", vec![], None),
            DbErrorKind::UndefinedTable,
            "42P01",
            None,
        ),
        (
            query("SELEC 1", vec![], None),
            DbErrorKind::SyntaxError,
            "42601",
            None,
        ),
    ] {
        let error = client.failure(request).await;
        assert_eq!(error.kind, kind, "{error}");
        assert_eq!(error.code.as_deref(), Some(code), "{error}");
        assert_eq!(error.constraint.as_deref(), constraint, "{error}");
    }
    let not_null = client.failure(insert(2, 1, None, 1)).await;
    assert_eq!(not_null.column.as_deref(), Some("email"));
    assert_eq!(not_null.table.as_deref(), Some("children"));
    assert_eq!(not_null.schema.as_deref(), Some("v2_errors"));

    client.budget = Duration::from_secs(2);
    let started = Instant::now();
    let timeout = client
        .failure(query("SELECT pg_sleep(5)", vec![], None))
        .await;
    assert_eq!(timeout.kind, DbErrorKind::QueryCanceled, "{timeout}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );

    let mut hold = cufflink_platform::wasm_runtime::budget::Hold::none();
    let caps = cufflink_platform::wasm_runtime::budget::HostCaps::default();
    let rows = cufflink_platform::wasm_runtime::host::RowText::legacy(&mut hold, &caps, "db_query");
    let legacy = cufflink_platform::wasm_runtime::host::run_guest_statement(
        &mut cufflink_platform::tenant_sessions::Session::over(
            fixture.pool.get().await.expect("checkout"),
            fixture.schema,
        ),
        Duration::from_secs(30),
        cufflink_platform::wasm_runtime::host::GuestStatement::Query(
            "SELECT * FROM missing",
            &rows,
        ),
    )
    .await
    .err()
    .expect("legacy error");
    assert_eq!(
        legacy.to_string(),
        "ERROR: relation \"missing\" does not exist"
    );
}

#[tokio::test]
async fn db_v2_transaction_read_decide_write() {
    let Some(fixture) = Fixture::new(
        "v2_tx",
        "CREATE TABLE accounts (id int PRIMARY KEY, balance int NOT NULL);
         INSERT INTO accounts VALUES (1, 100);
         CREATE TABLE items (id int PRIMARY KEY, owner text NOT NULL);
         INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'a');
         CREATE VIEW scoped AS SELECT * FROM items WHERE owner = current_setting('app.scope', true);",
    )
    .await
    else {
        return;
    };

    let withdraw = |amount: i32| {
        let fixture = &fixture;
        async move {
            let mut client = fixture.session();
            let tx = client.begin().await;
            let balance = client
                .query_in(
                    Some(tx),
                    "SELECT balance FROM accounts WHERE id = 1 FOR UPDATE",
                    vec![],
                )
                .await[0]["balance"]
                .as_i64()
                .expect("balance");
            tokio::time::sleep(Duration::from_millis(200)).await;
            let withdrew = balance >= i64::from(amount);
            if withdrew {
                client
                    .execute_in(
                        Some(tx),
                        "UPDATE accounts SET balance = balance - $1 WHERE id = 1",
                        vec![Param::I32(amount)],
                    )
                    .await;
            }
            client.done(DbRequest::Commit { tx }).await;
            withdrew
        }
    };
    let (a, b) = tokio::join!(withdraw(60), withdraw(60));
    assert!(a ^ b, "exactly one withdrawal: {a} {b}");
    let mut client = fixture.session();
    assert_eq!(
        client.query("SELECT balance FROM accounts", vec![]).await[0]["balance"],
        40
    );

    let tx = client.begin().await;
    client
        .query_in(
            Some(tx),
            "SELECT set_config('app.scope', $1, true)",
            vec![Param::Text("a".into())],
        )
        .await;
    let scoped = client
        .query_in(Some(tx), "SELECT id FROM scoped ORDER BY id", vec![])
        .await;
    assert_eq!(
        scoped.iter().map(|r| r["id"].clone()).collect::<Vec<_>>(),
        vec![json!(1), json!(3)]
    );
    client.done(DbRequest::Commit { tx }).await;
    for _ in 0..4 {
        let leaked = client
            .query("SELECT current_setting('app.scope', true) AS s", vec![])
            .await;
        assert!(
            leaked[0]["s"].is_null() || leaked[0]["s"] == "",
            "SET LOCAL leaked: {leaked:?}"
        );
    }

    let mut holder = fixture.session();
    let mut other = fixture.session();
    let held = holder.begin().await;
    holder
        .query_in(
            Some(held),
            "SELECT pg_advisory_xact_lock($1)",
            vec![Param::I64(42)],
        )
        .await;
    let waiting = other.begin().await;
    let try_lock = |tx| {
        query(
            "SELECT pg_try_advisory_xact_lock($1) AS got",
            vec![Param::I64(42)],
            Some(tx),
        )
    };
    let got = |response: DbResponse| match response {
        DbResponse::Rows { rows, .. } => rows[0]["got"].clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(got(other.call(try_lock(waiting)).await), json!(false));
    holder.done(DbRequest::Commit { tx: held }).await;
    assert_eq!(got(other.call(try_lock(waiting)).await), json!(true));
    other.done(DbRequest::Rollback { tx: waiting }).await;

    let tx = client.begin().await;
    client
        .execute_in(Some(tx), "INSERT INTO items VALUES (10, 'outer')", vec![])
        .await;
    client
        .done(DbRequest::Savepoint {
            tx,
            name: "sp_0".into(),
        })
        .await;
    let duplicate = client
        .failure(execute(
            "INSERT INTO items VALUES (10, 'dup')",
            vec![],
            Some(tx),
        ))
        .await;
    assert_eq!(duplicate.kind, DbErrorKind::UniqueViolation);
    let blocked = client.failure(query("SELECT 1", vec![], Some(tx))).await;
    assert_eq!(blocked.kind, DbErrorKind::InFailedTransaction);
    let unknown = client
        .failure(DbRequest::RollbackTo {
            tx,
            name: "sp_7".into(),
        })
        .await;
    assert_eq!(unknown.kind, DbErrorKind::InvalidParameter);
    client
        .done(DbRequest::RollbackTo {
            tx,
            name: "sp_0".into(),
        })
        .await;
    client
        .done(DbRequest::Release {
            tx,
            name: "sp_0".into(),
        })
        .await;
    client
        .execute_in(Some(tx), "INSERT INTO items VALUES (11, 'after')", vec![])
        .await;
    client.done(DbRequest::Commit { tx }).await;
    assert_eq!(
        client
            .query(
                "SELECT count(*) AS n FROM items WHERE id IN (10, 11)",
                vec![]
            )
            .await[0]["n"],
        2
    );

    let tx = client.begin().await;
    client
        .execute_in(Some(tx), "INSERT INTO items VALUES (12, 'doomed')", vec![])
        .await;
    client
        .failure(execute(
            "INSERT INTO items VALUES (12, 'dup')",
            vec![],
            Some(tx),
        ))
        .await;
    let commit = client.failure(DbRequest::Commit { tx }).await;
    assert_eq!(commit.kind, DbErrorKind::InFailedTransaction);
    assert_eq!(
        client
            .query("SELECT count(*) AS n FROM items WHERE id = 12", vec![])
            .await[0]["n"],
        0
    );
    let gone = client.failure(DbRequest::Rollback { tx }).await;
    assert_eq!(gone.kind, DbErrorKind::TransactionNotFound);

    let read_only = match client
        .call(DbRequest::Begin {
            isolation: Isolation::Serializable,
            read_only: true,
        })
        .await
    {
        DbResponse::Begun(tx) => tx,
        other => panic!("{other:?}"),
    };
    let refused = client
        .failure(execute("DELETE FROM items", vec![], Some(read_only)))
        .await;
    assert_eq!(refused.code.as_deref(), Some("25006"), "{refused}");
    client.done(DbRequest::Rollback { tx: read_only }).await;

    let tx = client.begin().await;
    client.budget = Duration::from_secs(2);
    let slow = client
        .failure(query("SELECT pg_sleep(5)", vec![], Some(tx)))
        .await;
    assert_eq!(slow.kind, DbErrorKind::QueryCanceled, "{slow}");
    client.budget = Duration::from_secs(30);
    client.done(DbRequest::Rollback { tx }).await;
}

#[tokio::test]
async fn db_v2_statements_of_a_cancelled_invocation_are_stopped_by_the_server() {
    let Some(fixture) = Fixture::new("v2_stop", "CREATE TABLE items (id int PRIMARY KEY);").await
    else {
        return;
    };
    let stopped_within = STATEMENT_STOP_EVERY * 4;
    let cancel_soon = |client: &Client<'_>| {
        let cancel = client.cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            cancel.cancel();
        });
    };

    let mut client = fixture.session();
    cancel_soon(&client);
    let started = Instant::now();
    let stopped = client
        .failure(query("SELECT pg_sleep(30)", vec![], None))
        .await;
    assert_eq!(stopped.kind, DbErrorKind::QueryCanceled, "{stopped}");
    assert_eq!(stopped.code.as_deref(), Some("57014"), "{stopped}");
    assert!(
        started.elapsed() < stopped_within,
        "{:?}",
        started.elapsed()
    );

    let mut client = fixture.session();
    let tx = client.begin().await;
    client
        .execute_in(Some(tx), "INSERT INTO items (id) VALUES (1)", vec![])
        .await;
    cancel_soon(&client);
    let started = Instant::now();
    let stopped = client
        .failure(query("SELECT pg_sleep(30)", vec![], Some(tx)))
        .await;
    assert_eq!(stopped.code.as_deref(), Some("57014"), "{stopped}");
    assert!(
        started.elapsed() < stopped_within,
        "{:?}",
        started.elapsed()
    );
    client.done(DbRequest::Rollback { tx }).await;

    let mut after = fixture.session();
    assert_eq!(
        after
            .query("SELECT count(*)::int AS n FROM items", vec![])
            .await[0]["n"],
        json!(0),
        "what the stopped transaction wrote is rolled back"
    );
    let sleeping = after
        .query(
            "SELECT count(*)::int AS n FROM pg_stat_activity
              WHERE datname = current_database() AND state = 'active'
                AND query LIKE '%pg_sleep(30)%' AND pid <> pg_backend_pid()",
            vec![],
        )
        .await;
    assert_eq!(sleeping[0]["n"], json!(0), "nothing runs on at the server");
}

#[tokio::test]
async fn db_v2_tx_limit_and_pool_pressure() {
    let Some(fixture) =
        Fixture::with_pool_size("v2_pressure", "CREATE TABLE items (id int PRIMARY KEY);", 1).await
    else {
        return;
    };
    let mut client = fixture.session();
    let held = client.begin().await;
    let started = Instant::now();
    client.budget = Duration::from_millis(1_500);
    let starved = client.failure(query("SELECT 1", vec![], None)).await;
    assert_eq!(starved.kind, DbErrorKind::PoolTimeout, "{starved}");
    let pool_wait = fixture.pool.timeouts().wait.expect("the pool's own wait");
    assert!(
        started.elapsed() < pool_wait,
        "the checkout outwaited the budget, to the pool's {pool_wait:?}: {:?}",
        started.elapsed()
    );
    let second = client
        .failure(DbRequest::Begin {
            isolation: Isolation::ReadCommitted,
            read_only: false,
        })
        .await;
    assert_eq!(second.kind, DbErrorKind::PoolTimeout);
    client.budget = Duration::from_secs(30);
    client
        .execute_in(Some(held), "INSERT INTO items VALUES (1)", vec![])
        .await;
    client.session.abandon_all(AbandonReason::Leaked);
    assert_eq!(client.session.open_count(), 0);
    assert!(
        client
            .query("SELECT id FROM items", vec![])
            .await
            .is_empty(),
        "the abandoned transaction's insert was rolled back"
    );

    let Some(roomy) = Fixture::new("v2_limit", "SELECT 1;").await else {
        return;
    };
    let mut client = roomy.session();
    let first = client.begin().await;
    client.begin().await;
    let third = client
        .failure(DbRequest::Begin {
            isolation: Isolation::ReadCommitted,
            read_only: false,
        })
        .await;
    assert_eq!(third.kind, DbErrorKind::TransactionLimit);
    client.done(DbRequest::Commit { tx: first }).await;
    assert!(matches!(
        client
            .call(DbRequest::Begin {
                isolation: Isolation::ReadCommitted,
                read_only: false
            })
            .await,
        DbResponse::Begun(_)
    ));
    let unknown = client.failure(DbRequest::Commit { tx: 99 }).await;
    assert_eq!(unknown.kind, DbErrorKind::TransactionNotFound);
    let not_supported = client
        .failure(execute(
            "CREATE INDEX CONCURRENTLY i ON items (id) WHERE id > $1",
            vec![Param::I32(1)],
            None,
        ))
        .await;
    assert_eq!(not_supported.kind, DbErrorKind::NotSupported);
    let bad_name = client
        .failure(DbRequest::Savepoint {
            tx: first + 1,
            name: "x; DROP TABLE items".into(),
        })
        .await;
    assert_eq!(bad_name.kind, DbErrorKind::InvalidParameter);
}

const ABANDONING_GUEST: &str = r#"
(module
  (import "env" "db_call" (func $db_call (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 4096))
  (data (i32.const 0) "{\"op\":\"begin\"}")
  (data (i32.const 64) "{\"op\":\"execute\",\"sql\":\"INSERT INTO items (title) VALUES ('abandoned')\",\"tx\":1}")
  (func (export "alloc") (param $n i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $heap))
    (global.set $heap (i32.add (local.get $p) (local.get $n)))
    (local.get $p))
  (func (export "abandon") (param i32 i32) (result i32)
    (drop (call $db_call (i32.const 0) (i32.const 14)))
    (drop (call $db_call (i32.const 64) (i32.const 78)))
    (loop $forever (br $forever))
    (unreachable))
)
"#;

#[tokio::test]
async fn db_v2_abandoned_tx_is_rolled_back() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("WASM_TEST_ROUTE_BUDGET_SECS", "1")],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy_module(
            &tenant,
            &common::wasm_manifest("abandon-svc", &[("abandon", "/abandon")]),
            &wat::parse_str(ABANDONING_GUEST).expect("wat"),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let (status, body) = platform.call_fn(&tenant, "abandon-svc", "abandon").await;
    assert_eq!(status, 500, "{body}");
    assert_eq!(body["error"], "WASM execution timed out after 1s");

    let tenant_db = common::with_dbname(&db.url, &TenantPoolManager::managed_db_name(&tenant.slug));
    let (tenant_client, connection) = tokio_postgres::connect(&tenant_db, tokio_postgres::NoTls)
        .await
        .expect("tenant database");
    tokio::spawn(connection);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let idle: i64 = tenant_client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND state LIKE 'idle in transaction%'",
                &[],
            )
            .await
            .expect("activity")
            .get(0);
        if idle == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "an idle-in-transaction session outlived the invocation"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let rows: i64 = tenant_client
        .query_one("SELECT count(*) FROM abandon_svc.items", &[])
        .await
        .expect("items")
        .get(0);
    assert_eq!(rows, 0, "the abandoned insert committed");
    assert_eq!(
        platform
            .metric("cufflink_db_tx_abandoned_total", &[("reason", "deadline")])
            .await,
        Some(1.0)
    );
    assert_eq!(platform.metric("cufflink_db_tx_open", &[]).await, Some(0.0));
}

#[tokio::test]
async fn db_v2_domains_bind_as_their_base_type() {
    let Some(fixture) = Fixture::new(
        "v2_domains",
        "CREATE DOMAIN positive AS int CHECK (VALUE > 0);
         CREATE DOMAIN doc AS jsonb;
         CREATE DOMAIN labels AS text[];
         CREATE DOMAIN small_doc AS doc CHECK (length(VALUE::text) < 20);
         CREATE TABLE things (n positive, body doc, tags labels, docs doc[], note small_doc);",
    )
    .await
    else {
        return;
    };
    let mut client = fixture.session();
    let inserted = client
        .execute_in(
            None,
            "INSERT INTO things VALUES ($1, $2, $3, $4, $5)",
            vec![
                Param::I32(5),
                Param::Json(json!({"a": [1, 2]})),
                Param::TextArray(vec![Some("x".into()), None]),
                Param::Json(json!([{"k": 1}, "s"])),
                Param::Text("short".into()),
            ],
        )
        .await;
    assert_eq!(inserted, 1);
    let rows = client
        .query(
            "SELECT n, body, tags, docs, note FROM things WHERE n = $1 AND body = $2 AND tags = $3",
            vec![
                Param::I64(5),
                Param::Json(json!({"a": [1, 2]})),
                Param::TextArray(vec![Some("x".into()), None]),
            ],
        )
        .await;
    assert_eq!(
        Value::Object(rows.into_iter().next().expect("the row")),
        json!({
            "n": 5, "body": {"a": [1, 2]}, "tags": ["x", null],
            "docs": [{"k": 1}, "s"], "note": "short"
        })
    );
    for (sql, value) in [
        ("INSERT INTO things (n) VALUES ($1)", Param::I32(-1)),
        (
            "INSERT INTO things (note) VALUES ($1)",
            Param::Text("far too long for the check".into()),
        ),
    ] {
        let refused = client.failure(execute(sql, vec![value], None)).await;
        assert_eq!(
            refused.kind,
            DbErrorKind::CheckViolation,
            "the value reached the server's check: {refused}"
        );
    }
}

#[tokio::test]
async fn db_v2_enum_labels_added_later_bind() {
    let Some(fixture) = Fixture::with_pool_size(
        "v2_enum_labels",
        "CREATE TYPE mood AS ENUM ('sad', 'happy'); CREATE TABLE moods (m mood);",
        1,
    )
    .await
    else {
        return;
    };
    let mut client = fixture.session();
    let insert = |label: &str| {
        execute(
            "INSERT INTO moods VALUES ($1)",
            vec![Param::Text(label.into())],
            None,
        )
    };
    assert_eq!(client.call(insert("sad")).await, DbResponse::Affected(1));
    assert_eq!(
        client
            .call(execute("ALTER TYPE mood ADD VALUE 'calm'", vec![], None))
            .await,
        DbResponse::Affected(0)
    );
    assert_eq!(
        client.call(insert("calm")).await,
        DbResponse::Affected(1),
        "the connection's cached labels predate 'calm'"
    );
}

#[tokio::test]
async fn db_v2_statements_the_host_owns_are_refused() {
    let Some(fixture) = Fixture::new(
        "v2_refused",
        "CREATE TABLE notes (id int PRIMARY KEY, at point);",
    )
    .await
    else {
        return;
    };
    let mut client = fixture.session();
    let tx = client.begin().await;
    for sql in [
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT mine",
        "RELEASE SAVEPOINT sp_1",
        "BEGIN",
        "START TRANSACTION",
        "END",
        "PREPARE TRANSACTION 'x'",
    ] {
        for tx in [Some(tx), None] {
            let refused = client.failure(execute(sql, vec![], tx)).await;
            assert_eq!(refused.kind, DbErrorKind::NotSupported, "{sql}: {refused}");
        }
    }
    assert_eq!(
        client
            .execute_in(Some(tx), "INSERT INTO notes (id) VALUES (1)", vec![])
            .await,
        1,
        "a refusal leaves the transaction open"
    );

    let unsupported = client
        .failure(query(
            "INSERT INTO notes VALUES (2, '(1,2)') RETURNING at",
            vec![],
            Some(tx),
        ))
        .await;
    assert_eq!(unsupported.kind, DbErrorKind::UnsupportedType);
    let duplicate = client
        .failure(query(
            "INSERT INTO notes (id) VALUES (3) RETURNING id, id",
            vec![],
            Some(tx),
        ))
        .await;
    assert_eq!(duplicate.kind, DbErrorKind::NotSupported, "{duplicate}");
    client.done(DbRequest::Commit { tx }).await;

    for sql in [
        "INSERT INTO notes VALUES (4, '(1,2)') RETURNING at",
        "INSERT INTO notes (id) VALUES (5) RETURNING id, id AS id",
    ] {
        assert!(matches!(
            client.failure(query(sql, vec![], None)).await.kind,
            DbErrorKind::UnsupportedType | DbErrorKind::NotSupported
        ));
    }
    assert_eq!(
        client
            .query("SELECT id FROM notes ORDER BY id", vec![])
            .await,
        vec![serde_json::Map::from_iter([("id".to_string(), json!(1))])],
        "nothing refused was run"
    );
    for sql in [
        "INSERT INTO notes VALUES (6, '(1,2)') RETURNING at",
        "INSERT INTO notes (id) VALUES (7) RETURNING id, id",
    ] {
        assert_eq!(
            client.execute_in(None, sql, vec![]).await,
            1,
            "an execute renders no rows, so its columns are not checked: {sql}"
        );
    }
    let aliased = client
        .query("SELECT id, id AS again FROM notes ORDER BY id", vec![])
        .await;
    assert_eq!(aliased[0]["again"], 1);
}

#[tokio::test]
async fn db_v2_float4_values_that_do_not_fit_are_refused() {
    let Some(fixture) = Fixture::new("v2_float4", "CREATE TABLE m (v real);").await else {
        return;
    };
    let mut client = fixture.session();
    for value in [
        Param::F64(1e300),
        Param::Text("1e400".into()),
        Param::I64((1 << 40) + 1),
    ] {
        let refused = client
            .failure(execute(
                "INSERT INTO m VALUES ($1)",
                vec![value.clone()],
                None,
            ))
            .await;
        assert_eq!(refused.kind, DbErrorKind::InvalidParameter, "{value:?}");
    }
    assert_eq!(
        client
            .execute_in(None, "INSERT INTO m VALUES ($1)", vec![Param::F64(0.1)])
            .await,
        1
    );
    let rows = client.query("SELECT v FROM m", vec![]).await;
    assert_eq!(rows[0]["v"], json!(0.1));
}

#[tokio::test]
async fn db_v2_a_result_past_the_response_cap_is_too_large_and_ends_its_transaction() {
    let Some(fixture) = Fixture::new("v2_capped", "CREATE TABLE t (id int)").await else {
        return;
    };
    let big = "SELECT generate_series(1, 10000000) AS g, repeat('x', 100) AS pad";
    let mut client = fixture.session();
    client.caps = HostCaps {
        mode: cufflink_platform::policy::PolicyMode::Enforce,
        response: 64 * 1024,
        ..HostCaps::default()
    };
    let too_large = |response: DbResponse| match response {
        DbResponse::Failed(error) => assert_eq!(error.kind, DbErrorKind::TooLarge, "{error:?}"),
        other => panic!("{other:?}"),
    };

    let started = Instant::now();
    too_large(
        client
            .call(DbRequest::Query {
                sql: big.into(),
                params: vec![],
                tx: None,
            })
            .await,
    );
    assert_eq!(client.query("SELECT 1 AS one", vec![]).await.len(), 1);

    let tx = client.begin().await;
    too_large(
        client
            .call(DbRequest::Query {
                sql: big.into(),
                params: vec![],
                tx: Some(tx),
            })
            .await,
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    match client.call(DbRequest::Commit { tx }).await {
        DbResponse::Failed(error) => assert_eq!(error.kind, DbErrorKind::TransactionNotFound),
        other => panic!("the transaction was abandoned: {other:?}"),
    }
}
