mod session;

use super::db::{
    controls_transaction, pooler_refusal, requires_autocommit, run_wrapped, statement_timeout_for,
    GuestWork, WrapperError,
};
use super::{block_on, read_guest_bytes, wrap, HostState};
use crate::db_error::{from_pg, from_pool, pool_timeout};
use crate::registry::ServiceIdentity;
use crate::row_json::check_columns_v2;
use crate::tenant_sessions::{
    AcquireError, Purpose, Scope, Session, StatementStop, Statements, TenantSessions,
};
use cufflink_pgcodec::bind;
use cufflink_pgcodec::render::rows_v2;
use cufflink_pgcodec::tx::{
    admit, in_failed_transaction, invalid_savepoint, isolation_sql, next,
    parameters_outside_transaction, rolled_back_by_commit, savepoint_sql, savepoints_after,
    transaction_control_refused, unknown_savepoint, valid_savepoint, Next, Outcome, Phase,
    SavepointOp, SlotOp,
};
use cufflink_types::db::{DbError, DbErrorKind, DbRequest, DbResponse, Isolation, Param};
use session::TxEnd;
use std::collections::HashSet;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio_postgres::types::ToSql;
use tokio_postgres::Statement;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use wasmtime::{Caller, Linker};

pub use session::{AbandonReason, DbSession};

const SET_STATEMENT_TIMEOUT: &str = "SELECT set_config('statement_timeout', $1, true)";

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "db_call",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            let bytes = host_try!(read_guest_bytes(&mut caller, "db_call", ptr, len));
            let response = match serde_json::from_slice::<DbRequest>(&bytes) {
                Ok(request) => handle(caller.data_mut(), request),
                Err(e) => {
                    record("invalid", Instant::now(), Some(DbErrorKind::Protocol));
                    DbResponse::Failed(DbError::new(
                        DbErrorKind::Protocol,
                        format!("malformed db_call request: {e}"),
                    ))
                }
            };
            caller.data_mut().respond_json("db_call", &response)
        },
    )
}

fn handle(state: &mut HostState, request: DbRequest) -> DbResponse {
    let app_state = state.app_state.clone();
    let target = DbTarget {
        sessions: &app_state.tenant_sessions,
        identity: &state.identity,
        deadline: state.deadline,
        cancel: &state.cancel,
    };
    block_on(call(&mut state.db, &target, request))
}

#[doc(hidden)]
pub struct DbTarget<'a> {
    pub sessions: &'a TenantSessions,
    pub identity: &'a ServiceIdentity,
    pub deadline: Instant,
    pub cancel: &'a CancellationToken,
}

impl DbTarget<'_> {
    async fn run<T>(
        &self,
        stop: StatementStop,
        work: impl Future<Output = T>,
    ) -> Result<T, DbError> {
        bounded(self.deadline, stop.stopping(self.cancel, work)).await
    }

    fn reusable(&self) -> bool {
        !self.cancel.is_cancelled()
    }

    async fn session(&self) -> Result<Session, DbError> {
        tokio::time::timeout(
            remaining(self.deadline),
            self.sessions.acquire(self.identity, Purpose::Guest, None),
        )
        .await
        .map_err(|_| pool_timeout())?
        .map_err(|e| match e {
            AcquireError::Checkout(e) => from_pool(&e),
            unavailable => DbError::new(
                DbErrorKind::Connection,
                format!("tenant database unavailable: {unavailable}"),
            ),
        })
    }
}

#[doc(hidden)]
pub async fn call(
    session: &mut DbSession,
    target: &DbTarget<'_>,
    request: DbRequest,
) -> DbResponse {
    let op = request.op();
    let started = Instant::now();
    let result = dispatch(session, target, request)
        .instrument(tracing::info_span!(
            "db",
            otel.name = %format!("db.v2.{op}"),
            otel.kind = "client",
        ))
        .await;
    record(op, started, result.as_ref().err().map(|e| e.kind));
    result.unwrap_or_else(DbResponse::Failed)
}

fn record(op: &'static str, started: Instant, failure: Option<DbErrorKind>) {
    let outcome = failure.map_or("ok", DbErrorKind::as_str);
    metrics::counter!("cufflink_db_v2_calls_total", "op" => op, "result" => outcome).increment(1);
    metrics::histogram!("cufflink_db_v2_call_duration_seconds", "op" => op)
        .record(started.elapsed().as_secs_f64());
}

async fn dispatch(
    session: &mut DbSession,
    target: &DbTarget<'_>,
    request: DbRequest,
) -> Result<DbResponse, DbError> {
    match request {
        DbRequest::Query { sql, params, tx } => {
            statement(session, target, tx, Kind::Query, &sql, &params).await
        }
        DbRequest::Execute { sql, params, tx } => {
            statement(session, target, tx, Kind::Execute, &sql, &params).await
        }
        DbRequest::Begin {
            isolation,
            read_only,
        } => begin(session, target, isolation, read_only)
            .await
            .map(DbResponse::Begun),
        DbRequest::Commit { tx } => end(session, target, tx, SlotOp::Commit).await,
        DbRequest::Rollback { tx } => end(session, target, tx, SlotOp::Rollback).await,
        DbRequest::Savepoint { tx, name } => {
            savepoint(session, target, tx, SavepointOp::Create, &name).await
        }
        DbRequest::Release { tx, name } => {
            savepoint(session, target, tx, SavepointOp::Release, &name).await
        }
        DbRequest::RollbackTo { tx, name } => {
            savepoint(session, target, tx, SavepointOp::RollbackTo, &name).await
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Query,
    Execute,
}

struct StatementFailure {
    error: DbError,
    aborted: bool,
}

impl StatementFailure {
    fn server(error: tokio_postgres::Error) -> Self {
        Self {
            error: from_pg(&error),
            aborted: true,
        }
    }

    fn refused(error: DbError) -> Self {
        Self {
            error,
            aborted: false,
        }
    }

    fn outcome(&self) -> Outcome {
        if self.aborted {
            Outcome::ServerError
        } else {
            Outcome::Refused
        }
    }
}

impl From<WrapperError> for StatementFailure {
    fn from(error: WrapperError) -> Self {
        Self::server(error.source)
    }
}

struct BoundStatement<'a> {
    kind: Kind,
    sql: &'a str,
    params: &'a [Param],
}

impl GuestWork for BoundStatement<'_> {
    type Output = DbResponse;
    type Error = StatementFailure;

    fn sql(&self) -> &str {
        self.sql
    }

    async fn run<C: Statements>(&self, client: &C) -> Result<DbResponse, StatementFailure> {
        let statement = client
            .prepare(self.sql)
            .await
            .map_err(StatementFailure::server)?;
        run_prepared(client, &statement, self.kind, self.params).await
    }
}

async fn run_prepared<C: Statements>(
    client: &C,
    statement: &Statement,
    kind: Kind,
    params: &[Param],
) -> Result<DbResponse, StatementFailure> {
    if kind == Kind::Query {
        check_columns_v2(statement.columns()).map_err(StatementFailure::refused)?;
    }
    let bound = bind(statement.params(), params).map_err(StatementFailure::refused)?;
    let args: Vec<&(dyn ToSql + Sync)> = bound.iter().map(|b| b as &(dyn ToSql + Sync)).collect();
    match kind {
        Kind::Query => {
            let rows = client
                .query(statement, &args)
                .await
                .map_err(StatementFailure::server)?;
            rows_v2(statement.columns(), &rows).map_err(StatementFailure::refused)
        }
        Kind::Execute => client
            .execute(statement, &args)
            .await
            .map(DbResponse::Affected)
            .map_err(StatementFailure::server),
    }
}

fn refused(target: &DbTarget<'_>, sql: &str) -> Result<(), DbError> {
    if controls_transaction(sql) {
        return Err(transaction_control_refused());
    }
    pooler_refusal(target.sessions.pooler(), sql).map_or(Ok(()), |refusal| {
        Err(DbError::new(DbErrorKind::NotSupported, refusal.message()))
    })
}

async fn statement(
    session: &mut DbSession,
    target: &DbTarget<'_>,
    tx: Option<u32>,
    kind: Kind,
    sql: &str,
    params: &[Param],
) -> Result<DbResponse, DbError> {
    refused(target, sql)?;
    match tx {
        None => autocommit(target, kind, sql, params).await,
        Some(tx) => in_transaction(session, target, tx, kind, sql, params).await,
    }
}

async fn autocommit(
    target: &DbTarget<'_>,
    kind: Kind,
    sql: &str,
    params: &[Param],
) -> Result<DbResponse, DbError> {
    if requires_autocommit(sql) && !params.is_empty() {
        return Err(parameters_outside_transaction());
    }
    let remaining = remaining(target.deadline);
    let mut session = target.session().await?;
    let work = BoundStatement { kind, sql, params };
    let outcome = target
        .run(
            session.stop(),
            run_wrapped(&mut session, statement_timeout_for(remaining), &work),
        )
        .await;
    match outcome {
        Ok(result) => {
            session.release(target.cancel);
            result.map_err(|failure| failure.error)
        }
        Err(elapsed) => {
            session.discard();
            Err(elapsed)
        }
    }
}

async fn in_transaction(
    session: &mut DbSession,
    target: &DbTarget<'_>,
    tx: u32,
    kind: Kind,
    sql: &str,
    params: &[Param],
) -> Result<DbResponse, DbError> {
    let slot = session.slot(tx)?;
    admit(slot.phase, SlotOp::Statement).map_err(in_failed_transaction)?;
    let timeout_ms = statement_timeout_for(remaining(target.deadline))
        .as_millis()
        .to_string();
    let client = slot.client()?;
    let outcome = target
        .run(client.stop(), async {
            let timeout_arg: [&(dyn ToSql + Sync); 1] = [&timeout_ms];
            let (_, statement) = tokio::try_join!(
                client.execute(&slot.set_timeout, &timeout_arg),
                client.prepare(sql),
            )
            .map_err(StatementFailure::server)?;
            run_prepared(client, &statement, kind, params).await
        })
        .await;
    match outcome {
        Ok(result) => {
            let outcome = result
                .as_ref()
                .map_or_else(StatementFailure::outcome, |_| Outcome::Ok);
            if let Next::Stay(phase) = next(slot.phase, SlotOp::Statement, outcome) {
                slot.phase = phase;
            }
            result.map_err(|failure| failure.error)
        }
        Err(elapsed) => {
            session.remove(tx)?.abandon(AbandonReason::Deadline);
            Err(elapsed)
        }
    }
}

async fn begin(
    session: &mut DbSession,
    target: &DbTarget<'_>,
    isolation: Isolation,
    read_only: bool,
) -> Result<u32, DbError> {
    session.has_capacity()?;
    let remaining = remaining(target.deadline);
    let client = target.session().await?;
    let setup = format!(
        "BEGIN ISOLATION LEVEL {}{}; {}; \
         SET LOCAL idle_in_transaction_session_timeout = {}",
        isolation_sql(isolation),
        if read_only { " READ ONLY" } else { "" },
        client.prelude(Scope::Transaction, statement_timeout_for(remaining)),
        (remaining + Duration::from_secs(1)).as_millis(),
    );
    let opened = target
        .run(client.stop(), async {
            tokio::try_join!(
                client.begin_by_hand(&setup),
                client.prepare(SET_STATEMENT_TIMEOUT)
            )
            .map_err(|e| from_pg(&e))
        })
        .await
        .and_then(|opened| opened);
    match opened {
        Ok(((), set_timeout)) => Ok(session.open(client, set_timeout)),
        Err(error) => {
            client.discard();
            Err(error)
        }
    }
}

async fn end(
    session: &mut DbSession,
    target: &DbTarget<'_>,
    tx: u32,
    op: SlotOp,
) -> Result<DbResponse, DbError> {
    let slot = session.remove(tx)?;
    let (sql, ended) = match op {
        SlotOp::Commit => ("COMMIT", TxEnd::Commit),
        _ => ("ROLLBACK", TxEnd::Rollback),
    };
    let aborted_earlier = slot.phase == Phase::Failed;
    let client = slot.client()?;
    let result = target.run(client.stop(), client.end_by_hand(sql)).await;
    match result {
        Err(elapsed) => {
            slot.abandon(AbandonReason::Deadline);
            Err(elapsed)
        }
        Ok(Ok(())) if aborted_earlier && op == SlotOp::Commit => {
            slot.finish(TxEnd::Rollback, target.reusable());
            Err(rolled_back_by_commit())
        }
        Ok(Ok(())) => {
            slot.finish(ended, target.reusable());
            Ok(DbResponse::Done)
        }
        Ok(Err(e)) => {
            let server_ended_it = op == SlotOp::Commit && e.as_db_error().is_some();
            slot.finish(TxEnd::Rollback, server_ended_it && target.reusable());
            Err(from_pg(&e))
        }
    }
}

async fn savepoint(
    session: &mut DbSession,
    target: &DbTarget<'_>,
    tx: u32,
    op: SavepointOp,
    name: &str,
) -> Result<DbResponse, DbError> {
    if !valid_savepoint(name) {
        return Err(invalid_savepoint(name));
    }
    let slot = session.slot(tx)?;
    admit(slot.phase, op.slot_op()).map_err(in_failed_transaction)?;
    let known = slot.savepoints.iter().rposition(|s| s == name);
    let sql = savepoint_sql(op, name, known).ok_or_else(|| unknown_savepoint(name, tx))?;
    let client = slot.client()?;
    let result = target.run(client.stop(), client.batch_execute(&sql)).await;
    match result {
        Ok(result) => {
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
        Err(elapsed) => {
            session.remove(tx)?.abandon(AbandonReason::Deadline);
            Err(elapsed)
        }
    }
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn bounded<T>(deadline: Instant, fut: impl Future<Output = T>) -> Result<T, DbError> {
    tokio::time::timeout(remaining(deadline), fut)
        .await
        .map_err(|_| {
            DbError::new(
                DbErrorKind::QueryCanceled,
                "the call did not finish before the invocation deadline",
            )
        })
}
