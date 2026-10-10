use super::{block_on, duration_from_env, read_guest_string, wrap, HostState};
use crate::tenant_sessions::{Purpose, Scope, Session, Statements};
use cufflink_pgcodec::error::server_message as pg_error_string;
pub(crate) use cufflink_pgcodec::pooler::pooler_refusal;
use cufflink_pgcodec::render::legacy_rows;
use cufflink_pgcodec::session as pg_session;
use cufflink_pgcodec::statement::rows_as_json;
pub(crate) use cufflink_pgcodec::statement::{controls_transaction, requires_autocommit};
use serde_json::json;
use std::collections::HashSet;
use std::ops::AsyncFnOnce;
use std::time::{Duration, Instant};
use wasmtime::{Caller, Linker};

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "db_query",
        |mut caller: Caller<'_, HostState>, sql_ptr: i32, sql_len: i32| -> i32 {
            let sql = host_try!(read_guest_string(&mut caller, "db_query", sql_ptr, sql_len));
            if pooler_refused(caller.data(), &sql) {
                return -1;
            }
            let _span = db_span("query", &sql);

            let result = block_on(on_guest_session(
                caller.data(),
                async |session: &mut Session, remaining| {
                    let rows = run_guest_statement(session, remaining, GuestStatement::Query(&sql))
                        .await?
                        .rows();
                    serde_json::to_string(&legacy_rows(&rows))
                        .map_err(|e| DbCallError(e.to_string()))
                },
            ));

            match result {
                Ok(json_str) => {
                    caller.data_mut().host_response = Some(json_str);
                    0
                }
                Err(e) => {
                    tracing::error!("[wasm] db_query error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "db_execute",
        |mut caller: Caller<'_, HostState>, sql_ptr: i32, sql_len: i32| -> i32 {
            let sql = host_try!(read_guest_string(
                &mut caller,
                "db_execute",
                sql_ptr,
                sql_len
            ));
            if pooler_refused(caller.data(), &sql) {
                return -1;
            }
            let _span = db_span("execute", &sql);

            let result = block_on(on_guest_session(
                caller.data(),
                async |session: &mut Session, remaining| {
                    run_guest_statement(session, remaining, GuestStatement::Execute(&sql))
                        .await
                        .map(GuestDbOutcome::affected)
                },
            ));

            match result {
                Ok(n) => n as i32,
                Err(e) => {
                    tracing::error!("[wasm] db_execute error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "db_execute_all",
        |mut caller: Caller<'_, HostState>, items_ptr: i32, items_len: i32| -> i32 {
            let items_json = host_try!(read_guest_string(
                &mut caller,
                "db_execute_all",
                items_ptr,
                items_len
            ));
            let statements: Vec<String> = match serde_json::from_str(&items_json) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("[wasm] db_execute_all: bad input: {}", e);
                    return -1;
                }
            };

            let _span = tracing::info_span!(
                "db",
                otel.name = "db.execute_all",
                db.statement_count = statements.len()
            )
            .entered();
            let pooler = caller.data().app_state.tenant_sessions.pooler();
            let refused = statements.iter().enumerate().find_map(|(i, sql)| {
                pooler_refusal(pooler, sql).map(|refusal| BatchFailure::at(i, refusal.message()))
            });
            let outcome = refused.map_or_else(
                || {
                    block_on(on_guest_session(
                        caller.data(),
                        async |session: &mut Session, remaining| {
                            run_db_batch(session, &statements, remaining).await
                        },
                    ))
                },
                Err,
            );

            let envelope = match outcome {
                Ok(counts) => json!({ "ok": true, "counts": counts }),
                Err(failure) => {
                    tracing::error!(
                        "[wasm] db_execute_all rolled back at statement {}: {}",
                        failure
                            .index
                            .map(|i| i.to_string())
                            .unwrap_or_else(|| "-".to_string()),
                        failure.error
                    );
                    json!({ "ok": false, "index": failure.index, "error": failure.error })
                }
            };

            match serde_json::to_string(&envelope) {
                Ok(json) => {
                    caller.data_mut().host_response = Some(json);
                    0
                }
                Err(e) => {
                    tracing::error!("[wasm] db_execute_all: encode failed: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "db_query_read_only",
        |mut caller: Caller<'_, HostState>, sql_ptr: i32, sql_len: i32| -> i32 {
            let sql = host_try!(read_guest_string(
                &mut caller,
                "db_query_read_only",
                sql_ptr,
                sql_len
            ));

            let role = read_only_role();
            let timeout =
                read_only_timeout().min(statement_timeout_for(caller.data().remaining_budget()));
            let max_rows = read_only_max_rows();

            let outcome = block_on(on_guest_session(
                caller.data(),
                async |session: &mut Session, _| {
                    run_read_only_query(session, &sql, &role, timeout, max_rows).await
                },
            ));

            let envelope = match outcome {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("[wasm] db_query_read_only failed: {}", e);
                    json!({ "ok": false, "error": e })
                }
            };

            match serde_json::to_string(&envelope) {
                Ok(json) => {
                    caller.data_mut().host_response = Some(json);
                    0
                }
                Err(e) => {
                    tracing::error!("[wasm] db_query_read_only: encode failed: {}", e);
                    -1
                }
            }
        },
    )?;

    Ok(())
}

async fn on_guest_session<T, E: From<DbCallError>>(
    state: &HostState,
    work: impl AsyncFnOnce(&mut Session, Duration) -> Result<T, E>,
) -> Result<T, E> {
    let mut session = state
        .app_state
        .tenant_sessions
        .acquire(&state.identity, Purpose::Guest, None)
        .await
        .map_err(|e| DbCallError(e.to_string()))?;
    let ended = session
        .stop()
        .stopping(&state.cancel, work(&mut session, state.remaining_budget()))
        .await;
    session.release(&state.cancel);
    ended
}

fn pooler_refused(state: &HostState, sql: &str) -> bool {
    let refusal = pooler_refusal(state.app_state.tenant_sessions.pooler(), sql);
    if let Some(refusal) = refusal {
        tracing::error!(
            tenant = %state.identity.tenant_slug,
            service = %state.identity.service_name,
            "[wasm] {}",
            refusal.message()
        );
    }
    refusal.is_some()
}

const STATEMENT_DEADLINE_MARGIN: Duration = Duration::from_millis(500);
const STATEMENT_TIMEOUT_FLOOR: Duration = Duration::from_secs(1);

pub(crate) fn statement_timeout_for(remaining: Duration) -> Duration {
    remaining
        .saturating_sub(STATEMENT_DEADLINE_MARGIN)
        .max(STATEMENT_TIMEOUT_FLOOR)
}

#[derive(Debug)]
#[doc(hidden)]
pub struct BatchFailure {
    pub index: Option<usize>,
    pub error: String,
}

impl BatchFailure {
    fn at(index: usize, error: &str) -> Self {
        Self {
            index: Some(index),
            error: error.to_string(),
        }
    }
    fn transaction(error: &str) -> Self {
        Self {
            index: None,
            error: error.to_string(),
        }
    }
}

impl From<DbCallError> for BatchFailure {
    fn from(error: DbCallError) -> Self {
        Self::transaction(&error.0)
    }
}

#[doc(hidden)]
pub async fn run_db_batch(
    session: &mut Session,
    statements: &[String],
    remaining: Duration,
) -> Result<Vec<i64>, BatchFailure> {
    if statements.is_empty() {
        return Ok(vec![]);
    }
    let started = Instant::now();

    let settings = session.prelude(Scope::Transaction, statement_timeout_for(remaining));
    let tx = session.transaction().await.map_err(|e| {
        BatchFailure::transaction(&format!("BEGIN failed: {}", pg_error_string(&e)))
    })?;
    if let Err(e) = tx.batch_execute(&settings).await {
        let _ = tx.rollback().await;
        return Err(BatchFailure::transaction(&format!(
            "session setup failed: {}",
            pg_error_string(&e)
        )));
    }

    let mut counts = Vec::with_capacity(statements.len());
    for (i, sql) in statements.iter().enumerate() {
        let result = tx.execute(sql.as_str(), &[]).await;
        match result {
            Ok(n) => counts.push(n as i64),
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(BatchFailure::at(i, &pg_error_string(&e)));
            }
        }
    }

    tx.commit().await.map_err(|e| {
        BatchFailure::transaction(&format!("COMMIT failed: {}", pg_error_string(&e)))
    })?;
    record_guest_statement("execute_all", started);
    Ok(counts)
}

fn record_guest_statement(op: &'static str, started: Instant) {
    metrics::histogram!("cufflink_db_guest_statement_duration_seconds", "op" => op)
        .record(started.elapsed().as_secs_f64());
}

#[doc(hidden)]
pub enum GuestStatement<'a> {
    Query(&'a str),
    Execute(&'a str),
}

impl GuestStatement<'_> {
    fn op(&self) -> &'static str {
        match self {
            Self::Query(_) => "query",
            Self::Execute(_) => "execute",
        }
    }
}

#[doc(hidden)]
pub enum GuestDbOutcome {
    Rows(Vec<tokio_postgres::Row>),
    Affected(u64),
}

impl GuestDbOutcome {
    pub fn rows(self) -> Vec<tokio_postgres::Row> {
        match self {
            Self::Rows(rows) => rows,
            Self::Affected(_) => Vec::new(),
        }
    }

    pub fn affected(self) -> u64 {
        match self {
            Self::Rows(rows) => rows.len() as u64,
            Self::Affected(n) => n,
        }
    }
}

#[derive(Debug)]
#[doc(hidden)]
pub struct DbCallError(pub String);

impl std::fmt::Display for DbCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<DbCallError> for String {
    fn from(error: DbCallError) -> Self {
        error.0
    }
}

impl From<tokio_postgres::Error> for DbCallError {
    fn from(err: tokio_postgres::Error) -> Self {
        Self(pg_error_string(&err))
    }
}

impl From<WrapperError> for DbCallError {
    fn from(err: WrapperError) -> Self {
        let step = match err.step {
            WrapperStep::Begin => "BEGIN failed",
            WrapperStep::Setup => "session setup failed",
            WrapperStep::Commit => "COMMIT failed",
        };
        Self(format!("{step}: {}", pg_error_string(&err.source)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WrapperStep {
    Begin,
    Setup,
    Commit,
}

#[derive(Debug)]
pub(crate) struct WrapperError {
    pub(crate) step: WrapperStep,
    pub(crate) source: tokio_postgres::Error,
}

impl WrapperError {
    fn at(step: WrapperStep) -> impl FnOnce(tokio_postgres::Error) -> Self {
        move |source| Self { step, source }
    }
}

pub(crate) trait GuestWork {
    type Output;
    type Error: From<WrapperError>;

    fn sql(&self) -> &str;

    async fn run<C: Statements>(&self, client: &C) -> Result<Self::Output, Self::Error>;
}

impl GuestWork for GuestStatement<'_> {
    type Output = GuestDbOutcome;
    type Error = DbCallError;

    fn sql(&self) -> &str {
        match self {
            Self::Query(sql) | Self::Execute(sql) => sql,
        }
    }

    async fn run<C: Statements>(&self, client: &C) -> Result<GuestDbOutcome, DbCallError> {
        match self {
            GuestStatement::Query(sql) => Ok(GuestDbOutcome::Rows(client.query(*sql, &[]).await?)),
            GuestStatement::Execute(sql) => {
                Ok(GuestDbOutcome::Affected(client.execute(*sql, &[]).await?))
            }
        }
    }
}

#[doc(hidden)]
pub async fn run_guest_statement(
    session: &mut Session,
    remaining: Duration,
    stmt: GuestStatement<'_>,
) -> Result<GuestDbOutcome, DbCallError> {
    let started = Instant::now();
    let outcome = run_wrapped(session, statement_timeout_for(remaining), &stmt).await;
    record_guest_statement(stmt.op(), started);
    outcome
}

pub(crate) async fn run_wrapped<W: GuestWork>(
    session: &mut Session,
    timeout: Duration,
    work: &W,
) -> Result<W::Output, W::Error> {
    if requires_autocommit(work.sql()) {
        run_autocommit(session, timeout, work).await
    } else {
        run_transactional(session, timeout, work).await
    }
}

async fn run_transactional<W: GuestWork>(
    session: &mut Session,
    timeout: Duration,
    work: &W,
) -> Result<W::Output, W::Error> {
    let settings = session.prelude(Scope::Transaction, timeout);
    let tx = session
        .transaction()
        .await
        .map_err(WrapperError::at(WrapperStep::Begin))?;
    let result = async {
        tx.batch_execute(&settings)
            .await
            .map_err(WrapperError::at(WrapperStep::Setup))?;
        work.run(&tx).await
    }
    .await;
    match result {
        Ok(outcome) => {
            tx.commit()
                .await
                .map_err(WrapperError::at(WrapperStep::Commit))?;
            Ok(outcome)
        }
        Err(e) => {
            let _ = tx.rollback().await;
            Err(e)
        }
    }
}

async fn run_autocommit<W: GuestWork>(
    session: &Session,
    timeout: Duration,
    work: &W,
) -> Result<W::Output, W::Error> {
    session
        .batch_execute(&session.prelude(Scope::Session, timeout))
        .await
        .map_err(WrapperError::at(WrapperStep::Setup))?;
    let result = work.run(session).await;
    if let Err(e) = session.batch_execute(session.reset()).await {
        tracing::warn!(error = %pg_error_string(&e), "RESET after an autocommit guest statement failed");
    }
    result
}

fn read_only_role() -> String {
    std::env::var("DB_READ_ONLY_ROLE").unwrap_or_else(|_| "reports_ro".to_string())
}

fn read_only_timeout() -> Duration {
    duration_from_env("DB_READ_ONLY_TIMEOUT_SECS", 30)
}

fn read_only_max_rows() -> i64 {
    std::env::var("DB_READ_ONLY_MAX_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(10_000)
}

pub(crate) async fn run_read_only_query(
    session: &mut Session,
    sql: &str,
    role: &str,
    timeout: Duration,
    max_rows: i64,
) -> Result<serde_json::Value, String> {
    let started = Instant::now();
    let role = session.read_only_role(role).to_string();
    let tx = session
        .transaction()
        .await
        .map_err(|e| format!("BEGIN failed: {}", pg_error_string(&e)))?;

    for stmt in [
        "SET TRANSACTION READ ONLY".to_string(),
        pg_session::set_role(pg_session::Scope::Transaction, &role),
        format!("SET LOCAL statement_timeout = {}", timeout.as_millis()),
    ] {
        if let Err(e) = tx.execute(stmt.as_str(), &[]).await {
            let _ = tx.rollback().await;
            return Err(format!("session setup failed: {}", pg_error_string(&e)));
        }
    }

    let wrapped = rows_as_json(sql, max_rows + 1);

    let rows = match tx.query(wrapped.as_str(), &[]).await {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.rollback().await;
            return Err(pg_error_string(&e));
        }
    };

    let _ = tx.rollback().await;
    record_guest_statement("read_only", started);

    let truncated = rows.len() as i64 > max_rows;
    let out: Vec<serde_json::Value> = rows
        .iter()
        .take(max_rows as usize)
        .map(|r| r.get::<_, serde_json::Value>("row"))
        .collect();

    Ok(json!({
        "ok": true,
        "row_count": out.len(),
        "truncated": truncated,
        "rows": out,
    }))
}

fn db_span(op: &str, sql: &str) -> tracing::span::EnteredSpan {
    let kind = sql
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    tracing::info_span!(
        "db",
        otel.name = %format!("db.{}", op),
        otel.kind = "client",
        db.operation = %kind
    )
    .entered()
}

#[cfg(test)]
mod guest_statement_tests {
    use super::statement_timeout_for;
    use std::time::Duration;

    #[test]
    fn statement_timeout_keeps_a_margin_and_a_floor() {
        assert_eq!(
            statement_timeout_for(Duration::from_secs(30)),
            Duration::from_millis(29_500)
        );
        assert_eq!(
            statement_timeout_for(Duration::from_millis(1_200)),
            Duration::from_secs(1)
        );
        assert_eq!(
            statement_timeout_for(Duration::ZERO),
            Duration::from_secs(1)
        );
    }
}
