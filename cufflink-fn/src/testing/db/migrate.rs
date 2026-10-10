//! A service's SQL migrations applied to a test database as a deploy
//! applies them: what to apply, what is sent and what a migration may not
//! do are `cufflink-migrate`'s and `cufflink-pgcodec`'s, the rules the
//! platform's executor follows; this runs them on the synchronous client.

use super::super::error::TestHostError;
use cufflink_migrate::{
    applied_on_now, applied_rows, checksum, escaped, history_ddl, history_version,
    interrupted_builds, lock_key, parse_applied, plan, record_statement, Applied, Escape, Plan,
    QualifiedName, Standing, Timeouts, INVALID_INDEXES, STANDING, TRY_LOCK, UNLOCK,
};
use cufflink_pgcodec::session::{
    migration_prelude, quote_ident, Scope, MIGRATION_RESET, ROLE_NONE,
};
use cufflink_types::manifest::validate::{folded, folded_ident};
use cufflink_types::{HistoryMode, MigrationHistory, SqlMigration};
use postgres::{Client, GenericClient};
use std::time::{Duration, Instant};

const LOCK_POLL: Duration = Duration::from_millis(100);

/// One run: the service's schema, the history it declares, and the role
/// its migrations run as.
pub(super) struct Run<'a> {
    pub schema: &'a str,
    pub declared: &'a MigrationHistory,
    pub migrations: &'a [SqlMigration],
    pub role: &'a str,
}

fn refused(reason: impl ToString) -> TestHostError {
    TestHostError::Migration(reason.to_string())
}

/// Bring the schema to what the run's migrations describe. The plan is
/// what the history left to apply; it was applied unless the history is
/// `verify_only`.
pub(super) fn run(client: &mut Client, run: &Run<'_>) -> Result<Plan, TestHostError> {
    let history = QualifiedName::parse(&run.declared.table, run.schema)
        .map_err(|e| refused(format!("migration_history.table: {e}")))?;
    let key = lock_key(run.declared.lock, &history);
    lock(client, key, &history)?;
    let outcome = locked(client, run, &history);
    let unlocked = client.execute(UNLOCK, &[&key]);
    let planned = outcome?;
    unlocked?;
    Ok(planned)
}

fn lock(client: &mut Client, key: i64, history: &QualifiedName) -> Result<(), TestHostError> {
    let wait = Timeouts::default().lock_wait;
    let deadline = Instant::now() + wait;
    loop {
        let held: bool = client.query_one(TRY_LOCK, &[&key])?.try_get(0)?;
        if held {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(refused(format!(
                "timed out after {wait:?} waiting for the migration lock of {}",
                history.dotted()
            )));
        }
        std::thread::sleep(LOCK_POLL);
    }
}

fn applied(client: &mut Client, history: &QualifiedName) -> Result<Vec<Applied>, TestHostError> {
    client.batch_execute(&history_ddl(history))?;
    client
        .query(&applied_rows(history), &[])?
        .iter()
        .map(|row| {
            parse_applied(
                row.try_get(0)?,
                row.try_get(1)?,
                row.try_get(2)?,
                row.try_get(3)?,
            )
            .map_err(refused)
        })
        .collect()
}

fn locked(
    client: &mut Client,
    run: &Run<'_>,
    history: &QualifiedName,
) -> Result<Plan, TestHostError> {
    let planned = plan(
        &applied(client, history)?,
        run.migrations,
        run.declared.baseline,
    )
    .map_err(refused)?;
    match run.declared.mode {
        HistoryMode::VerifyOnly => Ok(planned),
        HistoryMode::Apply => planned
            .pending
            .iter()
            .try_for_each(|migration| apply(client, run, history, migration))
            .map(|()| planned),
    }
}

enum Failure {
    Database(postgres::Error),
    Escaped(Escape),
}

impl From<postgres::Error> for Failure {
    fn from(error: postgres::Error) -> Self {
        Failure::Database(error)
    }
}

fn apply(
    client: &mut Client,
    run: &Run<'_>,
    history: &QualifiedName,
    migration: &SqlMigration,
) -> Result<(), TestHostError> {
    let applied = if migration.no_transaction {
        outside_transaction(client, run, history, migration).map_err(Failure::Database)
    } else {
        in_transaction(client, run, history, migration)
    };
    applied.map_err(|failure| match failure {
        Failure::Database(e) => refused(format!(
            "migration {} failed: {}",
            migration.file_stem(),
            cufflink_pgcodec::from_pg(&e).message
        )),
        Failure::Escaped(escape) => refused(escaped(migration, escape)),
    })
}

fn prelude(run: &Run<'_>, scope: Scope) -> String {
    let timeouts = Timeouts::default();
    migration_prelude(
        run.schema,
        Some(run.role),
        scope,
        timeouts.statement,
        timeouts.lock,
    )
}

fn standing(client: &mut impl GenericClient) -> Result<Standing, postgres::Error> {
    let row = client.query_one(STANDING, &[])?;
    Ok(Standing {
        transaction: row.try_get(0)?,
        role: row.try_get(1)?,
    })
}

/// The history is the platform's record, written as the session's own
/// role whatever role the migration ran as.
fn record(
    client: &mut impl GenericClient,
    history: &QualifiedName,
    migration: &SqlMigration,
) -> Result<(), postgres::Error> {
    client
        .execute(
            &record_statement(history),
            &[
                &history_version(migration.version),
                &migration.name,
                &applied_on_now(),
                &checksum(migration).to_string(),
            ],
        )
        .map(drop)
}

/// The migration and its history row commit together or not at all. A
/// migration that ends the transaction or changes the role has taken that
/// from the runner, and is refused rather than recorded.
fn in_transaction(
    client: &mut Client,
    run: &Run<'_>,
    history: &QualifiedName,
    migration: &SqlMigration,
) -> Result<(), Failure> {
    let mut tx = client.transaction()?;
    tx.batch_execute(&prelude(run, Scope::Transaction))?;
    let before = standing(&mut tx)?;
    let applied = tx.batch_execute(&migration.sql);
    let escape = standing(&mut tx)
        .ok()
        .and_then(|after| before.escape(&after, true));
    match (escape, applied) {
        (Some(escape), _) => return Err(Failure::Escaped(escape)),
        (None, Err(e)) => return Err(Failure::Database(e)),
        (None, Ok(())) => {}
    }
    tx.batch_execute(ROLE_NONE)?;
    record(&mut tx, history, migration)?;
    Ok(tx.commit()?)
}

fn outside_transaction(
    client: &mut Client,
    run: &Run<'_>,
    history: &QualifiedName,
    migration: &SqlMigration,
) -> Result<(), postgres::Error> {
    client.batch_execute(&prelude(run, Scope::Session))?;
    let applied = drop_invalid_indexes(client, run, migration)
        .and_then(|()| client.batch_execute(&migration.sql));
    let reset = client.batch_execute(MIGRATION_RESET);
    applied?;
    reset?;
    record(client, history, migration)
}

/// Drop the invalid indexes a concurrent build of this migration left
/// when it was interrupted, which would make this attempt fail.
fn drop_invalid_indexes(
    client: &mut Client,
    run: &Run<'_>,
    migration: &SqlMigration,
) -> Result<(), postgres::Error> {
    let invalid = client
        .query(INVALID_INDEXES, &[&folded(run.schema)])?
        .iter()
        .map(|row| row.try_get(0))
        .collect::<Result<Vec<String>, _>>()?;
    interrupted_builds(&invalid, &migration.sql)
        .into_iter()
        .try_for_each(|index| {
            client.batch_execute(&format!(
                "DROP INDEX CONCURRENTLY IF EXISTS {}.{}",
                folded_ident(run.schema),
                quote_ident(index)
            ))
        })
}
