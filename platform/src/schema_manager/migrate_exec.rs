use crate::errors::PlatformError;
use cufflink_db::quote_ident;
pub use cufflink_migrate::Timeouts;
use cufflink_migrate::{
    applied_on_now, applied_rows, checksum, escaped, history_ddl, history_version,
    interrupted_builds, lock_key, parse_applied, plan, record_statement, Applied, Escape, Plan,
    PlanError, QualifiedName, Standing, INVALID_INDEXES, STANDING, TRY_LOCK, UNLOCK,
};
use cufflink_pgcodec::session::{migration_prelude, Scope, MIGRATION_RESET, ROLE_NONE};
use cufflink_types::manifest::validate::{folded, folded_ident};
use cufflink_types::{HistoryMode, MigrationHistory, MigrationLock, SqlMigration};
use serde::Serialize;
use std::time::Duration;
use tokio_postgres::Client;
use tracing::{info, warn};

const LOCK_POLL: Duration = Duration::from_secs(1);

pub fn timeouts_from_env() -> eyre::Result<Timeouts> {
    let defaults = Timeouts::default();
    Ok(Timeouts {
        lock_wait: cufflink_db::env_secs(
            "SQL_MIGRATION_LOCK_WAIT_SECS",
            defaults.lock_wait.as_secs(),
        )?,
        statement: cufflink_db::env_secs(
            "SQL_MIGRATION_STATEMENT_TIMEOUT_SECS",
            defaults.statement.as_secs(),
        )?,
        lock: cufflink_db::env_secs("SQL_MIGRATION_LOCK_TIMEOUT_SECS", defaults.lock.as_secs())?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Apply,
    Verify,
}

impl Action {
    pub fn of(mode: HistoryMode, plan_only: bool) -> Self {
        match (mode, plan_only) {
            (HistoryMode::Apply, false) => Action::Apply,
            _ => Action::Verify,
        }
    }
}

pub struct Run<'a> {
    pub schema: &'a str,
    pub history: QualifiedName,
    pub lock: MigrationLock,
    pub baseline: Option<u32>,
    pub migrations: &'a [SqlMigration],
    pub action: Action,
    pub role: Option<&'a str>,
    pub timeouts: Timeouts,
}

impl<'a> Run<'a> {
    pub fn of_service(
        schema: &'a str,
        migrations: &'a [SqlMigration],
        history: Option<&MigrationHistory>,
        plan_only: bool,
        role: Option<&'a str>,
        timeouts: Timeouts,
    ) -> Result<Self, MigrateError> {
        let declared = history.cloned().unwrap_or_default();
        Ok(Self {
            schema,
            history: QualifiedName::parse(&declared.table, schema)
                .map_err(MigrateError::History)?,
            lock: declared.lock,
            baseline: declared.baseline,
            migrations,
            action: Action::of(declared.mode, plan_only),
            role,
            timeouts,
        })
    }

    pub(super) fn lock_key(&self) -> i64 {
        lock_key(self.lock, &self.history)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MigrationRef {
    pub version: u32,
    pub name: String,
    pub checksum: u64,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub no_transaction: bool,
}

impl From<&SqlMigration> for MigrationRef {
    fn from(migration: &SqlMigration) -> Self {
        Self {
            version: migration.version,
            name: migration.name.clone(),
            checksum: checksum(migration),
            no_transaction: migration.no_transaction,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Report {
    pub history: String,
    pub verified: usize,
    pub baselined: Vec<i32>,
    pub applied: Vec<MigrationRef>,
    pub pending: Vec<MigrationRef>,
    pub recovered_indexes: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error("migration_history.table: {0}")]
    History(String),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error("timed out after {0:?} waiting for the migration lock of {1}")]
    LockTimeout(Duration, String),
    #[error("migration V{version}__{name} failed: {}", crate::db_error::from_pg(source).message)]
    Apply {
        version: u32,
        name: String,
        applied: Vec<MigrationRef>,
        source: tokio_postgres::Error,
    },
    #[error("{reason}")]
    Escaped {
        version: u32,
        applied: Vec<MigrationRef>,
        reason: String,
    },
    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),
}

impl From<MigrateError> for PlatformError {
    fn from(error: MigrateError) -> Self {
        match error {
            MigrateError::History(_) | MigrateError::Plan(_) => {
                PlatformError::Validation(error.to_string())
            }
            MigrateError::LockTimeout(..) => PlatformError::Unavailable(error.to_string()),
            MigrateError::Apply { .. } | MigrateError::Escaped { .. } => {
                PlatformError::SchemaMigration(error.to_string())
            }
            MigrateError::Database(e) => PlatformError::Database(e),
        }
    }
}

pub async fn run(client: &mut Client, run: &Run<'_>) -> Result<Report, MigrateError> {
    let key = run.lock_key();
    lock(client, key, run).await?;
    let outcome = run_locked(client, run).await;
    let unlocked = client.execute(UNLOCK, &[&key]).await;
    let report = outcome?;
    unlocked?;
    Ok(report)
}

async fn lock(client: &Client, key: i64, run: &Run<'_>) -> Result<(), MigrateError> {
    let deadline = tokio::time::Instant::now() + run.timeouts.lock_wait;
    loop {
        let held: bool = client.query_one(TRY_LOCK, &[&key]).await?.get(0);
        if held {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(MigrateError::LockTimeout(
                run.timeouts.lock_wait,
                run.history.dotted(),
            ));
        }
        tokio::time::sleep(LOCK_POLL).await;
    }
}

async fn read_history(client: &Client, run: &Run<'_>) -> Result<Vec<Applied>, MigrateError> {
    client.batch_execute(&history_ddl(&run.history)).await?;
    recorded(client, run).await
}

pub(super) async fn recorded(
    client: &impl tokio_postgres::GenericClient,
    run: &Run<'_>,
) -> Result<Vec<Applied>, MigrateError> {
    client
        .query(&applied_rows(&run.history), &[])
        .await?
        .iter()
        .map(|row| {
            parse_applied(row.get(0), row.get(1), row.get(2), row.get(3))
                .map_err(MigrateError::from)
        })
        .collect()
}

fn planned(
    run: &Run<'_>,
    applied: &[Applied],
) -> Result<(Report, Vec<SqlMigration>), MigrateError> {
    let Plan {
        pending,
        verified,
        baselined,
    } = plan(applied, run.migrations, run.baseline)?;
    let report = Report {
        history: run.history.dotted(),
        verified,
        baselined,
        ..Report::default()
    };
    Ok((report, pending))
}

pub(super) fn verified(run: &Run<'_>, applied: &[Applied]) -> Result<Report, MigrateError> {
    planned(run, applied).map(|(report, pending)| Report {
        pending: pending.iter().map(MigrationRef::from).collect(),
        ..report
    })
}

async fn run_locked(client: &mut Client, run: &Run<'_>) -> Result<Report, MigrateError> {
    let applied = read_history(client, run).await?;
    match run.action {
        Action::Verify => verified(run, &applied),
        Action::Apply => {
            let (report, pending) = planned(run, &applied)?;
            apply_all(client, run, &pending, report).await
        }
    }
}

async fn apply_all(
    client: &mut Client,
    run: &Run<'_>,
    pending: &[SqlMigration],
    mut report: Report,
) -> Result<Report, MigrateError> {
    for migration in pending {
        let applied = if migration.no_transaction {
            apply_outside_transaction(client, run, migration)
                .await
                .map_err(Failure::Database)
        } else {
            apply_in_transaction(client, run, migration)
                .await
                .map(|()| Vec::new())
        };
        match applied {
            Ok(recovered) => {
                let done = MigrationRef::from(migration);
                info!(
                    event = "sql_migration_applied",
                    table = %report.history,
                    version = done.version,
                    name = %done.name,
                    checksum = done.checksum,
                    "SQL migration applied"
                );
                report.recovered_indexes.extend(recovered);
                report.applied.push(done);
            }
            Err(Failure::Database(source)) => {
                return Err(MigrateError::Apply {
                    version: migration.version,
                    name: migration.name.clone(),
                    applied: report.applied,
                    source,
                })
            }
            Err(Failure::Escaped(escape)) => {
                return Err(MigrateError::Escaped {
                    version: migration.version,
                    applied: report.applied,
                    reason: escaped(migration, escape),
                })
            }
        }
    }
    Ok(report)
}

enum Failure {
    Database(tokio_postgres::Error),
    Escaped(Escape),
}

impl From<tokio_postgres::Error> for Failure {
    fn from(error: tokio_postgres::Error) -> Self {
        Failure::Database(error)
    }
}

async fn standing(
    client: &impl tokio_postgres::GenericClient,
) -> Result<Standing, tokio_postgres::Error> {
    client.query_one(STANDING, &[]).await.map(|row| Standing {
        transaction: row.get(0),
        role: row.get(1),
    })
}

fn prelude(run: &Run<'_>, scope: Scope) -> String {
    migration_prelude(
        run.schema,
        run.role,
        scope,
        run.timeouts.statement,
        run.timeouts.lock,
    )
}

async fn record(
    client: &impl tokio_postgres::GenericClient,
    run: &Run<'_>,
    migration: &SqlMigration,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            &record_statement(&run.history),
            &[
                &history_version(migration.version),
                &migration.name,
                &applied_on_now(),
                &checksum(migration).to_string(),
            ],
        )
        .await
        .map(drop)
}

async fn apply_in_transaction(
    client: &mut Client,
    run: &Run<'_>,
    migration: &SqlMigration,
) -> Result<(), Failure> {
    let tx = client.transaction().await?;
    tx.batch_execute(&prelude(run, Scope::Transaction)).await?;
    let before = standing(&tx).await?;
    let applied = tx.batch_execute(&migration.sql).await;
    let escape = standing(&tx)
        .await
        .ok()
        .and_then(|after| before.escape(&after, run.role.is_some()));
    match (escape, applied) {
        (Some(escape), _) => return Err(Failure::Escaped(escape)),
        (None, Err(e)) => return Err(Failure::Database(e)),
        (None, Ok(())) => {}
    }
    tx.batch_execute(ROLE_NONE).await?;
    record(&tx, run, migration).await?;
    Ok(tx.commit().await?)
}

async fn apply_outside_transaction(
    client: &Client,
    run: &Run<'_>,
    migration: &SqlMigration,
) -> Result<Vec<String>, tokio_postgres::Error> {
    client.batch_execute(&prelude(run, Scope::Session)).await?;
    let applied = async {
        let recovered = drop_invalid_indexes(client, run, migration).await?;
        client.batch_execute(&migration.sql).await?;
        Ok(recovered)
    }
    .await;
    let reset = client.batch_execute(MIGRATION_RESET).await;
    let recovered = applied?;
    reset?;
    record(client, run, migration).await?;
    Ok(recovered)
}

async fn drop_invalid_indexes(
    client: &Client,
    run: &Run<'_>,
    migration: &SqlMigration,
) -> Result<Vec<String>, tokio_postgres::Error> {
    let invalid: Vec<String> = client
        .query(INVALID_INDEXES, &[&folded(run.schema)])
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let mut dropped = Vec::new();
    for index in interrupted_builds(&invalid, &migration.sql) {
        client
            .batch_execute(&format!(
                "DROP INDEX CONCURRENTLY IF EXISTS {}.{}",
                folded_ident(run.schema),
                quote_ident(index)
            ))
            .await?;
        warn!(
            schema = run.schema,
            index,
            version = migration.version,
            "Dropped the invalid index an interrupted concurrent build left"
        );
        dropped.push(index.to_string());
    }
    Ok(dropped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_migrate::{legacy_table_lock_key, platform_lock_key};

    const TIMEOUTS: Timeouts = Timeouts {
        lock_wait: Duration::from_secs(600),
        statement: Duration::from_secs(600),
        lock: Duration::from_secs(10),
    };

    fn service_run<'a>(history: Option<&MigrationHistory>, plan_only: bool) -> Run<'a> {
        Run::of_service("loan_service", &[], history, plan_only, None, TIMEOUTS).expect("a run")
    }

    #[test]
    fn a_service_without_a_declared_history_uses_refinerys_table_in_its_schema() {
        let run = service_run(None, false);
        assert_eq!(run.history.dotted(), "loan_service.refinery_schema_history");
        assert_eq!(run.action, Action::Apply);
        assert_eq!(
            run.lock_key(),
            platform_lock_key("loan_service.refinery_schema_history")
        );
    }

    #[test]
    fn a_declared_history_decides_the_table_the_lock_and_the_action() {
        let history = MigrationHistory {
            table: "schema_history".into(),
            baseline: Some(6),
            lock: MigrationLock::LegacyTableHash,
            mode: HistoryMode::VerifyOnly,
        };
        let run = service_run(Some(&history), false);
        assert_eq!(run.history.quoted(), "\"loan_service\".\"schema_history\"");
        assert_eq!(run.baseline, Some(6));
        assert_eq!(run.action, Action::Verify);
        assert_eq!(
            run.lock_key(),
            legacy_table_lock_key("loan_service.schema_history")
        );
        let qualified = MigrationHistory {
            table: "public.schema_history_deferred".into(),
            ..Default::default()
        };
        assert_eq!(
            service_run(Some(&qualified), true).history.dotted(),
            "public.schema_history_deferred"
        );
        let bad = MigrationHistory {
            table: "a.b.c".into(),
            ..Default::default()
        };
        let refused = Run::of_service("s", &[], Some(&bad), false, None, TIMEOUTS)
            .err()
            .map(PlatformError::from);
        assert!(
            matches!(&refused, Some(PlatformError::Validation(m)) if m.contains("'a.b.c'")),
            "{refused:?}"
        );
    }

    #[test]
    fn a_legacy_schema_is_the_one_postgres_folded_it_to() {
        let run = Run::of_service("Order_Desk_2", &[], None, false, None, TIMEOUTS).expect("run");
        assert_eq!(
            run.history.quoted(),
            "\"order_desk_2\".\"refinery_schema_history\""
        );
        assert!(prelude(&run, Scope::Transaction)
            .starts_with("SET LOCAL search_path TO \"order_desk_2\", public;"));
        let capitals = MigrationHistory {
            table: "Schema_History".into(),
            ..Default::default()
        };
        assert!(matches!(
            Run::of_service("s", &[], Some(&capitals), false, None, TIMEOUTS),
            Err(MigrateError::History(_))
        ));
    }

    #[test]
    fn an_escaped_migration_is_refused_with_the_runners_reason() {
        let migration = SqlMigration {
            version: 8,
            name: "commits".into(),
            sql: String::new(),
            no_transaction: false,
        };
        let escaped = MigrateError::Escaped {
            version: 8,
            applied: vec![],
            reason: escaped(&migration, Escape::Transaction),
        };
        assert!(matches!(
            PlatformError::from(escaped),
            PlatformError::SchemaMigration(m)
                if m.starts_with("migration V8__commits ended the transaction it ran in; it is not recorded")
        ));
    }

    #[test]
    fn only_an_applying_history_outside_a_plan_applies() {
        assert_eq!(Action::of(HistoryMode::Apply, false), Action::Apply);
        assert_eq!(Action::of(HistoryMode::Apply, true), Action::Verify);
        assert_eq!(Action::of(HistoryMode::VerifyOnly, false), Action::Verify);
        assert_eq!(Action::of(HistoryMode::VerifyOnly, true), Action::Verify);
    }

    #[test]
    fn the_prelude_sets_the_path_the_timeouts_and_the_role() {
        let run = service_run(None, false);
        assert_eq!(
            prelude(&run, Scope::Transaction),
            "SET LOCAL search_path TO \"loan_service\", public; \
             SET LOCAL statement_timeout = 600000; SET LOCAL lock_timeout = 10000; "
        );
        let as_role = Run {
            role: Some("loan_service_user"),
            ..run
        };
        assert_eq!(
            prelude(&as_role, Scope::Session),
            "SET search_path TO \"loan_service\", public; SET statement_timeout = 600000; \
             SET lock_timeout = 10000; SET ROLE \"loan_service_user\";"
        );
    }

    #[test]
    fn errors_map_to_the_status_a_deploy_answers() {
        let plan = MigrateError::from(PlanError::Duplicate { version: 3 });
        assert!(matches!(
            PlatformError::from(plan),
            PlatformError::Validation(m) if m == "migration version 3 is declared twice"
        ));
        let locked = MigrateError::LockTimeout(Duration::from_secs(600), "s.t".into());
        assert!(matches!(
            PlatformError::from(locked),
            PlatformError::Unavailable(m)
                if m == "timed out after 600s waiting for the migration lock of s.t"
        ));
    }

    #[test]
    fn timeouts_default_to_the_documented_values() {
        assert_eq!(timeouts_from_env().expect("defaults"), TIMEOUTS);
    }
}
