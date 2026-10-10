//! A handler's SQL against a real Postgres, run as the platform runs it:
//! the same session prelude, the same codec, the same rules for what a
//! statement and a transaction may do.

use super::super::error::TestHostError;
use super::{apart, locked, ServiceRole, TestDb};
use crate::host::db::{BatchError, DbHost, ReadOnlyRows};
use cufflink_pgcodec::pooler::{pooler_refusal, Pooler};
use cufflink_pgcodec::render::{legacy_rows, rows_v2};
use cufflink_pgcodec::session::{self, Scope};
use cufflink_pgcodec::statement::{controls_transaction, requires_autocommit, rows_as_json};
use cufflink_pgcodec::tx::{
    admit, in_failed_transaction, invalid_savepoint, isolation_sql, next,
    parameters_outside_transaction, rolled_back_by_commit, savepoint_sql, savepoints_after,
    transaction_control_refused, transaction_limit, transaction_not_found, unknown_savepoint,
    valid_savepoint, Next, Outcome, Phase, SavepointOp, SlotOp,
};
use cufflink_pgcodec::{bind, check_columns_v2, from_pg, server_message};
use cufflink_types::db::{DbError, DbErrorKind, DbRequest, DbResponse, Isolation, Param};
use postgres::types::ToSql;
use postgres::{Client, Config, GenericClient, NoTls};
use serde_json::Value;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

/// Who a service's statements run as, as the platform's `db_roles`
/// decides it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgRole {
    /// The role the connection logs in as: the platform's own, as under
    /// `db_roles: off`.
    Connection,
    /// The connection's role with this one set on every transaction
    /// (`SET LOCAL ROLE`), as the platform sets a service's role where it
    /// does not log in as it. A statement the role may not run fails; the
    /// platform's `warn` would run it again as itself and record it.
    Set(String),
    /// A connection logged in as the service's role, as `db_roles:
    /// enforce` runs a guest's statements: no statement can give it back
    /// the server's user.
    Login(ServiceRole),
}

/// What the platform decides for a guest, decided for a test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOptions {
    /// How long a statement may run; the platform's follows from what is
    /// left of the handler's budget.
    pub statement_timeout: Duration,
    /// Interactive transactions open at once (`WASM_MAX_OPEN_TX`).
    pub max_open_transactions: usize,
    pub role: PgRole,
    /// What sits between the platform and the database
    /// (`TENANT_DB_POOLER`): a transaction pooler refuses what it cannot
    /// carry, as the platform does.
    pub pooler: Pooler,
    /// The role `db::query_read_only` runs as: `DB_READ_ONLY_ROLE` under
    /// [`PgRole::Connection`] and [`PgRole::Set`], the read-only role of
    /// the service's binding under [`PgRole::Login`]. `None` runs it as the
    /// connection's own role, which under `Login` is the platform's too
    /// when the binding names none.
    pub read_only_role: Option<String>,
    /// The rows `db::query_read_only` answers at most
    /// (`DB_READ_ONLY_MAX_ROWS`).
    pub read_only_max_rows: i64,
}

impl Default for PgOptions {
    fn default() -> Self {
        Self {
            statement_timeout: Duration::from_secs(30),
            max_open_transactions: 2,
            role: PgRole::Connection,
            pooler: Pooler::Direct,
            read_only_role: None,
            read_only_max_rows: 10_000,
        }
    }
}

impl PgOptions {
    /// The role a session sets on its connection, where it sets one.
    fn imposed(&self) -> Option<&str> {
        match &self.role {
            PgRole::Set(role) => Some(role),
            PgRole::Connection | PgRole::Login(_) => None,
        }
    }

    /// How the service's connections reach `db`: as the service's role
    /// where they log in as it.
    fn connection(&self, db: &TestDb) -> Config {
        let mut config = db.config().clone();
        if let PgRole::Login(role) = &self.role {
            config.user(role.name()).password(role.password());
        }
        config
    }

    fn refused(&self, sql: &str) -> Option<&'static str> {
        pooler_refusal(self.pooler, sql).map(|refusal| refusal.message())
    }
}

struct Slot {
    client: Client,
    phase: Phase,
    savepoints: Vec<String>,
}

struct Slots {
    open: BTreeMap<u32, Slot>,
    next_id: u32,
}

impl Slots {
    fn slot(&mut self, tx: u32) -> Result<&mut Slot, DbError> {
        self.open
            .get_mut(&tx)
            .ok_or_else(|| transaction_not_found(tx))
    }
}

struct Inner {
    config: Config,
    schema: String,
    options: PgOptions,
    /// `None` until the first statement outside a transaction, and after
    /// the server closed the connection.
    autocommit: Mutex<Option<Client>>,
    slots: Mutex<Slots>,
}

/// The database of one service: statements outside a transaction share a
/// connection, and every interactive transaction has one of its own.
/// Clones share both. Every call runs its connection's work [`apart`], so
/// a host works inside an async runtime as outside one.
#[derive(Clone)]
pub struct PgHost {
    inner: Rc<Inner>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let autocommit = self
            .autocommit
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let open = std::mem::take(
            &mut self
                .slots
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .open,
        );
        apart(move || drop((autocommit, open)));
    }
}

/// A step of the per-statement wrapper, around the guest's own statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    Begin,
    Setup,
    Commit,
}

/// The wrapper itself failed rather than the guest's statement.
struct Wrapper {
    step: Step,
    source: postgres::Error,
}

impl Wrapper {
    fn at(step: Step) -> impl FnOnce(postgres::Error) -> Self {
        move |source| Self { step, source }
    }
}

/// A guest statement the per-statement wrapper runs.
trait Work {
    type Output;
    type Error: From<Wrapper>;

    fn sql(&self) -> &str;

    fn run(&self, client: &mut impl GenericClient) -> Result<Self::Output, Self::Error>;
}

/// What a legacy call failed with; the guest only ever sees the text.
struct Text(String);

impl From<postgres::Error> for Text {
    fn from(error: postgres::Error) -> Self {
        Self(server_message(&error))
    }
}

impl From<Wrapper> for Text {
    fn from(wrapper: Wrapper) -> Self {
        let step = match wrapper.step {
            Step::Begin => "BEGIN failed",
            Step::Setup => "session setup failed",
            Step::Commit => "COMMIT failed",
        };
        Self(format!("{step}: {}", server_message(&wrapper.source)))
    }
}

struct LegacyQuery<'a>(&'a str);

impl Work for LegacyQuery<'_> {
    type Output = Vec<Value>;
    type Error = Text;

    fn sql(&self) -> &str {
        self.0
    }

    fn run(&self, client: &mut impl GenericClient) -> Result<Vec<Value>, Text> {
        Ok(legacy_rows(&client.query(self.0, &[])?))
    }
}

struct LegacyExecute<'a>(&'a str);

impl Work for LegacyExecute<'_> {
    type Output = u64;
    type Error = Text;

    fn sql(&self) -> &str {
        self.0
    }

    fn run(&self, client: &mut impl GenericClient) -> Result<u64, Text> {
        Ok(client.execute(self.0, &[])?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Query,
    Execute,
}

/// Why a bound statement failed, and what that does to a transaction.
struct Failure {
    error: DbError,
    outcome: Outcome,
}

impl Failure {
    fn server(error: postgres::Error) -> Self {
        Self {
            error: from_pg(&error),
            outcome: Outcome::ServerError,
        }
    }

    fn refused(error: DbError) -> Self {
        Self {
            error,
            outcome: Outcome::Refused,
        }
    }
}

impl From<Wrapper> for Failure {
    fn from(wrapper: Wrapper) -> Self {
        Self::server(wrapper.source)
    }
}

struct Bound<'a> {
    kind: Kind,
    sql: &'a str,
    params: &'a [Param],
}

impl Work for Bound<'_> {
    type Output = DbResponse;
    type Error = Failure;

    fn sql(&self) -> &str {
        self.sql
    }

    fn run(&self, client: &mut impl GenericClient) -> Result<DbResponse, Failure> {
        let statement = client.prepare(self.sql).map_err(Failure::server)?;
        if self.kind == Kind::Query {
            check_columns_v2(statement.columns()).map_err(Failure::refused)?;
        }
        let bound = bind(statement.params(), self.params).map_err(Failure::refused)?;
        let args: Vec<&(dyn ToSql + Sync)> = bound
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        match self.kind {
            Kind::Query => {
                let rows = client.query(&statement, &args).map_err(Failure::server)?;
                rows_v2(statement.columns(), &rows).map_err(Failure::refused)
            }
            Kind::Execute => client
                .execute(&statement, &args)
                .map(DbResponse::Affected)
                .map_err(Failure::server),
        }
    }
}

impl PgHost {
    /// The host of the service whose schema is `schema`, in `db`.
    pub fn connect(db: &TestDb, schema: &str, options: PgOptions) -> Result<Self, TestHostError> {
        let config = options.connection(db);
        let autocommit = apart(|| config.connect(NoTls))?;
        Ok(Self {
            inner: Rc::new(Inner {
                config,
                schema: schema.to_string(),
                options,
                autocommit: Mutex::new(Some(autocommit)),
                slots: Mutex::new(Slots {
                    open: BTreeMap::new(),
                    next_id: 1,
                }),
            }),
        })
    }

    /// The schema a handler's unqualified names resolve in.
    pub fn schema(&self) -> &str {
        &self.inner.schema
    }

    /// Interactive transactions a handler has open.
    pub fn open_transactions(&self) -> usize {
        locked(&self.inner.slots).open.len()
    }

    /// Discard the transactions a handler left open, as the platform does
    /// when an invocation ends: the server rolls them back. Answers how
    /// many there were.
    pub fn abandon_open_transactions(&self) -> usize {
        let abandoned = std::mem::take(&mut locked(&self.inner.slots).open);
        let count = abandoned.len();
        apart(move || drop(abandoned));
        count
    }
}

impl Inner {
    fn prelude(&self, scope: Scope) -> String {
        session::prelude(
            &self.schema,
            self.options.imposed(),
            scope,
            self.options.statement_timeout,
        )
    }

    /// The connection statements outside a transaction share, opened again
    /// when the server closed it.
    fn with_client<T, E: From<postgres::Error>>(
        &self,
        work: impl FnOnce(&mut Client) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut held = locked(&self.autocommit);
        let mut client = match held.take().filter(|client| !client.is_closed()) {
            Some(client) => client,
            None => self.config.connect(NoTls)?,
        };
        let outcome = work(&mut client);
        *held = Some(client);
        outcome
    }

    /// Statements run inside their own transaction, so the prelude is
    /// `SET LOCAL` and reverts however the statement ends. The few
    /// statements Postgres refuses inside a transaction block run on the
    /// bare connection between a `SET` and a `RESET` instead.
    fn wrapped<W: Work>(&self, client: &mut Client, work: &W) -> Result<W::Output, W::Error> {
        if requires_autocommit(work.sql()) {
            client
                .batch_execute(&self.prelude(Scope::Session))
                .map_err(Wrapper::at(Step::Setup))?;
            let outcome = work.run(client);
            let _ = client.batch_execute(session::reset(self.options.imposed().is_some()));
            outcome
        } else {
            let mut tx = client.transaction().map_err(Wrapper::at(Step::Begin))?;
            let outcome = tx
                .batch_execute(&self.prelude(Scope::Transaction))
                .map_err(|e| Wrapper::at(Step::Setup)(e).into())
                .and_then(|()| work.run(&mut tx));
            match outcome {
                Ok(output) => {
                    tx.commit().map_err(Wrapper::at(Step::Commit))?;
                    Ok(output)
                }
                Err(e) => {
                    let _ = tx.rollback();
                    Err(e)
                }
            }
        }
    }

    fn legacy<W: Work<Error = Text>>(&self, work: &W) -> Result<W::Output, String> {
        if let Some(refusal) = self.options.refused(work.sql()) {
            return Err(refusal.to_string());
        }
        self.with_client(|client| self.wrapped(client, work))
            .map_err(|Text(message)| message)
    }

    fn batch(&self, client: &mut Client, statements: &[&str]) -> Result<Vec<i64>, BatchError> {
        let transaction = |what: &str, e: &postgres::Error| BatchError {
            index: None,
            message: format!("{what} failed: {}", server_message(e)),
        };
        let mut tx = client.transaction().map_err(|e| transaction("BEGIN", &e))?;
        if let Err(e) = tx.batch_execute(&self.prelude(Scope::Transaction)) {
            let _ = tx.rollback();
            return Err(transaction("session setup", &e));
        }
        let counts = statements
            .iter()
            .enumerate()
            .map(|(index, sql)| {
                tx.execute(*sql, &[])
                    .map(|count| i64::try_from(count).unwrap_or(i64::MAX))
                    .map_err(|e| BatchError {
                        index: Some(index),
                        message: server_message(&e),
                    })
            })
            .collect::<Result<Vec<_>, _>>();
        match counts {
            Ok(counts) => tx
                .commit()
                .map(|()| counts)
                .map_err(|e| transaction("COMMIT", &e)),
            Err(failure) => {
                let _ = tx.rollback();
                Err(failure)
            }
        }
    }

    /// One caller-supplied `SELECT` in a read-only transaction that is
    /// always rolled back, without the service's schema on the search path.
    fn read_only(&self, client: &mut Client, sql: &str) -> Result<ReadOnlyRows, String> {
        let options = &self.options;
        let max_rows = options.read_only_max_rows;
        let mut tx = client
            .transaction()
            .map_err(|e| format!("BEGIN failed: {}", server_message(&e)))?;
        let setup = std::iter::once("SET TRANSACTION READ ONLY".to_string())
            .chain(
                options
                    .read_only_role
                    .iter()
                    .map(|role| session::set_role(Scope::Transaction, role)),
            )
            .chain(std::iter::once(format!(
                "SET LOCAL statement_timeout = {}",
                options.statement_timeout.as_millis()
            )))
            .try_for_each(|statement| tx.execute(statement.as_str(), &[]).map(drop));
        if let Err(e) = setup {
            let _ = tx.rollback();
            return Err(format!("session setup failed: {}", server_message(&e)));
        }
        let found = tx.query(rows_as_json(sql, max_rows + 1).as_str(), &[]);
        let _ = tx.rollback();
        let rows = found.map_err(|e| server_message(&e))?;
        let kept = usize::try_from(max_rows).unwrap_or(usize::MAX);
        Ok(ReadOnlyRows {
            truncated: rows.len() > kept,
            rows: rows
                .iter()
                .take(kept)
                .map(|row| row.try_get::<_, Value>("row"))
                .collect::<Result<_, _>>()
                .map_err(|e| server_message(&e))?,
        })
    }

    fn statement(
        &self,
        tx: Option<u32>,
        kind: Kind,
        sql: &str,
        params: &[Param],
    ) -> Result<DbResponse, DbError> {
        if controls_transaction(sql) {
            return Err(transaction_control_refused());
        }
        if let Some(refusal) = self.options.refused(sql) {
            return Err(DbError::new(DbErrorKind::NotSupported, refusal));
        }
        let work = Bound { kind, sql, params };
        match tx {
            None if requires_autocommit(sql) && !params.is_empty() => {
                Err(parameters_outside_transaction())
            }
            None => self
                .with_client(|client| self.wrapped(client, &work))
                .map_err(|failure: Failure| failure.error),
            Some(tx) => {
                let mut slots = locked(&self.slots);
                let slot = slots.slot(tx)?;
                admit(slot.phase, SlotOp::Statement).map_err(in_failed_transaction)?;
                let result = work.run(&mut slot.client);
                let outcome = result
                    .as_ref()
                    .map_or_else(|failure| failure.outcome, |_| Outcome::Ok);
                if let Next::Stay(phase) = next(slot.phase, SlotOp::Statement, outcome) {
                    slot.phase = phase;
                }
                result.map_err(|failure| failure.error)
            }
        }
    }

    fn begin(&self, isolation: Isolation, read_only: bool) -> Result<u32, DbError> {
        let options = &self.options;
        let mut slots = locked(&self.slots);
        if slots.open.len() >= options.max_open_transactions {
            return Err(transaction_limit(options.max_open_transactions));
        }
        let setup = format!(
            "BEGIN ISOLATION LEVEL {}{}; {}; \
             SET LOCAL idle_in_transaction_session_timeout = {}",
            isolation_sql(isolation),
            if read_only { " READ ONLY" } else { "" },
            self.prelude(Scope::Transaction),
            (options.statement_timeout + Duration::from_secs(1)).as_millis(),
        );
        let mut client = self.config.connect(NoTls).map_err(|e| from_pg(&e))?;
        client.batch_execute(&setup).map_err(|e| from_pg(&e))?;
        let id = slots.next_id;
        slots.next_id = id.wrapping_add(1).max(1);
        slots.open.insert(
            id,
            Slot {
                client,
                phase: Phase::Open,
                savepoints: Vec::new(),
            },
        );
        Ok(id)
    }

    fn end(&self, tx: u32, op: SlotOp) -> Result<DbResponse, DbError> {
        let mut slot = locked(&self.slots)
            .open
            .remove(&tx)
            .ok_or_else(|| transaction_not_found(tx))?;
        let sql = match op {
            SlotOp::Commit => "COMMIT",
            _ => "ROLLBACK",
        };
        match slot.client.batch_execute(sql) {
            Ok(()) if slot.phase == Phase::Failed && op == SlotOp::Commit => {
                Err(rolled_back_by_commit())
            }
            Ok(()) => Ok(DbResponse::Done),
            Err(e) => Err(from_pg(&e)),
        }
    }

    fn savepoint(&self, tx: u32, op: SavepointOp, name: &str) -> Result<DbResponse, DbError> {
        if !valid_savepoint(name) {
            return Err(invalid_savepoint(name));
        }
        let mut slots = locked(&self.slots);
        let slot = slots.slot(tx)?;
        admit(slot.phase, op.slot_op()).map_err(in_failed_transaction)?;
        let known = slot.savepoints.iter().rposition(|held| held == name);
        let sql = savepoint_sql(op, name, known).ok_or_else(|| unknown_savepoint(name, tx))?;
        let result = slot.client.batch_execute(&sql);
        let outcome = match result {
            Ok(()) => Outcome::Ok,
            Err(_) => Outcome::ServerError,
        };
        if let Next::Stay(phase) = next(slot.phase, op.slot_op(), outcome) {
            slot.phase = phase;
        }
        if result.is_ok() {
            slot.savepoints = savepoints_after(std::mem::take(&mut slot.savepoints), op, name);
        }
        result.map(|()| DbResponse::Done).map_err(|e| from_pg(&e))
    }

    fn dispatch(&self, request: &DbRequest) -> Result<DbResponse, DbError> {
        match request {
            DbRequest::Query { sql, params, tx } => self.statement(*tx, Kind::Query, sql, params),
            DbRequest::Execute { sql, params, tx } => {
                self.statement(*tx, Kind::Execute, sql, params)
            }
            DbRequest::Begin {
                isolation,
                read_only,
            } => self.begin(*isolation, *read_only).map(DbResponse::Begun),
            DbRequest::Commit { tx } => self.end(*tx, SlotOp::Commit),
            DbRequest::Rollback { tx } => self.end(*tx, SlotOp::Rollback),
            DbRequest::Savepoint { tx, name } => self.savepoint(*tx, SavepointOp::Create, name),
            DbRequest::Release { tx, name } => self.savepoint(*tx, SavepointOp::Release, name),
            DbRequest::RollbackTo { tx, name } => {
                self.savepoint(*tx, SavepointOp::RollbackTo, name)
            }
        }
    }
}

/// A connection that could not be opened again, as a batch reports it.
impl From<postgres::Error> for BatchError {
    fn from(error: postgres::Error) -> Self {
        BatchError {
            index: None,
            message: server_message(&error),
        }
    }
}

impl From<postgres::Error> for Failure {
    fn from(error: postgres::Error) -> Self {
        Failure::server(error)
    }
}

/// The text of a read-only query's failure.
struct Message(String);

impl From<postgres::Error> for Message {
    fn from(error: postgres::Error) -> Self {
        Self(server_message(&error))
    }
}

impl DbHost for PgHost {
    fn query(&self, sql: &str) -> Result<Vec<Value>, String> {
        let inner = &*self.inner;
        apart(|| inner.legacy(&LegacyQuery(sql)))
    }

    fn execute(&self, sql: &str) -> Result<u64, String> {
        let inner = &*self.inner;
        apart(|| inner.legacy(&LegacyExecute(sql)))
    }

    fn execute_all(&self, statements: &[&str]) -> Result<Vec<i64>, BatchError> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let inner = &*self.inner;
        if let Some(refused) = statements.iter().enumerate().find_map(|(index, sql)| {
            inner.options.refused(sql).map(|refusal| BatchError {
                index: Some(index),
                message: refusal.to_string(),
            })
        }) {
            return Err(refused);
        }
        apart(|| inner.with_client(|client| inner.batch(client, statements)))
    }

    fn query_read_only(&self, sql: &str) -> Result<ReadOnlyRows, String> {
        let inner = &*self.inner;
        apart(|| {
            inner
                .with_client(|client| inner.read_only(client, sql).map_err(Message))
                .map_err(|Message(message)| message)
        })
    }

    fn call(&self, request: &DbRequest) -> DbResponse {
        let inner = &*self.inner;
        apart(|| inner.dispatch(request)).unwrap_or_else(DbResponse::Failed)
    }
}
