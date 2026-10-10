use crate::errors::PlatformError;
use crate::policy::{PolicyKey, PolicyMode, PolicySet};
use crate::schema_manager::migrate_exec::{self, Action, MigrateError, Report, Run};
use crate::tenant_sessions::{AcquireError, TenantSessions};
use crate::AppState;
use cufflink_migrate::QualifiedName;
use cufflink_types::{MigrationLock, SqlMigration};
use futures_util::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::error::SqlState;
use tracing::{error, info};
use uuid::Uuid;

pub const SCHEMA: &str = "_cufflink";
const HISTORY_TABLE: &str = "schema_history";

const EMBEDDED: [(u32, &str, &str); 2] = [
    (
        1,
        "outbox",
        include_str!("../../tenant_migrations/V1__outbox.sql"),
    ),
    (
        2,
        "service_enqueue",
        include_str!("../../tenant_migrations/V2__service_enqueue.sql"),
    ),
];

const SYNC_INTERVAL: Duration = Duration::from_secs(60);
const SYNC_CONCURRENCY: usize = 4;

pub fn migrations() -> Vec<SqlMigration> {
    EMBEDDED
        .iter()
        .map(|&(version, name, sql)| SqlMigration {
            version,
            name: name.to_string(),
            sql: sql.to_string(),
            no_transaction: SqlMigration::declares_no_transaction(sql),
        })
        .collect()
}

pub fn needed(policies: &PolicySet) -> bool {
    policies.mode(PolicyKey::Outbox) == PolicyMode::Enforce
        || policies.mode(PolicyKey::DbRoles) != PolicyMode::Off
}

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("the tenant's database cannot be reached: {}", .0.reason())]
    Connect(#[from] AcquireError),
    #[error("the schema {SCHEMA} cannot be created in the tenant's database: {}", crate::db_error::from_pg(.0).message)]
    Schema(#[source] tokio_postgres::Error),
    #[error(transparent)]
    Migrate(#[from] MigrateError),
    #[error("{0}")]
    Config(String),
}

impl BootstrapError {
    fn is_refusal(&self) -> bool {
        match self {
            BootstrapError::Schema(e)
            | BootstrapError::Migrate(MigrateError::Database(e))
            | BootstrapError::Migrate(MigrateError::Apply { source: e, .. }) => {
                e.code().is_some_and(|code| !passes(code))
            }
            BootstrapError::Migrate(MigrateError::LockTimeout(..)) | BootstrapError::Connect(_) => {
                false
            }
            BootstrapError::Migrate(_) | BootstrapError::Config(_) => true,
        }
    }
}

fn passes(code: &SqlState) -> bool {
    *code == SqlState::LOCK_NOT_AVAILABLE
        || matches!(code.code().get(..2), Some("08" | "40" | "53" | "57"))
}

impl From<BootstrapError> for PlatformError {
    fn from(error: BootstrapError) -> Self {
        if error.is_refusal() {
            PlatformError::Validation(error.to_string())
        } else {
            PlatformError::Unavailable(error.to_string())
        }
    }
}

pub fn is_missing(error: &tokio_postgres::Error) -> bool {
    matches!(
        error.code(),
        Some(&SqlState::INVALID_SCHEMA_NAME | &SqlState::UNDEFINED_FUNCTION)
    )
}

fn already_exists(error: &tokio_postgres::Error) -> bool {
    matches!(
        error.code(),
        Some(&SqlState::DUPLICATE_SCHEMA | &SqlState::UNIQUE_VIOLATION)
    )
}

async fn create_schema(client: &tokio_postgres::Client) -> Result<(), BootstrapError> {
    client
        .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        .await
        .or_else(|e| already_exists(&e).then_some(()).ok_or(e))
        .map_err(BootstrapError::Schema)
}

pub async fn ensure(sessions: &TenantSessions, tenant_id: Uuid) -> Result<Report, BootstrapError> {
    let migrations = migrations();
    let run = Run {
        schema: SCHEMA,
        history: QualifiedName::parse(HISTORY_TABLE, SCHEMA).map_err(MigrateError::History)?,
        lock: MigrationLock::Platform,
        baseline: None,
        migrations: &migrations,
        action: Action::Apply,
        role: None,
        timeouts: migrate_exec::timeouts_from_env()
            .map_err(|e| BootstrapError::Config(e.to_string()))?,
    };
    let mut client = sessions.direct(tenant_id).await?;
    create_schema(&client).await?;
    Ok(migrate_exec::run(&mut client, &run).await?)
}

fn pending(state: &AppState, done: &HashSet<Uuid>) -> Vec<(Uuid, String)> {
    state
        .tenants
        .entries()
        .into_iter()
        .filter(|entry| needed(&entry.policies) && !done.contains(&entry.tenant.id))
        .map(|entry| (entry.tenant.id, entry.tenant.slug.clone()))
        .collect()
}

async fn sync(state: &AppState, done: &tokio::sync::Mutex<HashSet<Uuid>>) {
    let mut done = done.lock().await;
    let ensured: Vec<Uuid> = futures_util::stream::iter(pending(state, &done))
        .map(|(tenant_id, slug)| async move {
            match ensure(&state.tenant_sessions, tenant_id).await {
                Ok(report) => {
                    info!(tenant = %slug, applied = report.applied.len(), "Platform schema ready in the tenant's database");
                    Some(tenant_id)
                }
                Err(e) => {
                    error!(tenant = %slug, error = %e, "Platform schema could not be made ready in the tenant's database; retrying");
                    None
                }
            }
        })
        .buffer_unordered(SYNC_CONCURRENCY)
        .filter_map(std::future::ready)
        .collect()
        .await;
    done.extend(ensured);
}

pub fn start(state: Arc<AppState>) {
    let done = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
    let (shutdown, nats) = (state.shutdown.clone(), state.nats.clone());
    let pass = move || {
        let (state, done) = (state.clone(), done.clone());
        async move { sync(&state, &done).await }
    };
    shutdown.spawn("platform_schema_sync", pass());
    crate::nats_conn::spawn_reload_loop(
        &shutdown,
        "platform_schema_changes",
        nats,
        crate::tenant_catalog::TENANTS_CHANGED_SUBJECT,
        SYNC_INTERVAL,
        pass,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant_catalog::tests::policy_row;
    use std::path::Path;

    #[test]
    fn every_file_of_the_directory_is_embedded_byte_for_byte() {
        let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("tenant_migrations");
        assert_eq!(
            cufflink_migrate::scan_dir(&directory).expect("the directory"),
            migrations()
        );
    }

    #[test]
    fn the_history_is_a_table_of_the_platforms_schema() {
        let history = QualifiedName::parse(HISTORY_TABLE, SCHEMA).expect("a name");
        assert_eq!(history.quoted(), "\"_cufflink\".\"schema_history\"");
        assert!(cufflink_types::manifest::validate::is_reserved_schema(
            SCHEMA
        ));
    }

    #[test]
    fn the_schema_is_needed_by_an_enforced_outbox_and_by_database_roles() {
        let with = |key, mode| {
            PolicySet::builtin_defaults().with_tenant_rows(&[policy_row(Uuid::nil(), key, mode)])
        };
        assert!(!needed(&PolicySet::builtin_defaults()));
        assert!(needed(&with(PolicyKey::Outbox, PolicyMode::Enforce)));
        assert!(needed(&with(PolicyKey::DbRoles, PolicyMode::Warn)));
        assert!(needed(&with(PolicyKey::DbRoles, PolicyMode::Enforce)));
        assert!(!needed(&with(PolicyKey::SchemaGuard, PolicyMode::Enforce)));
    }

    #[test]
    fn what_passes_with_time_is_told_from_what_the_database_refuses() {
        for passing in [
            SqlState::QUERY_CANCELED,
            SqlState::LOCK_NOT_AVAILABLE,
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CANNOT_CONNECT_NOW,
            SqlState::TOO_MANY_CONNECTIONS,
            SqlState::CONNECTION_FAILURE,
            SqlState::T_R_DEADLOCK_DETECTED,
        ] {
            assert!(passes(&passing), "{passing:?}");
        }
        for refusal in [
            SqlState::INSUFFICIENT_PRIVILEGE,
            SqlState::DUPLICATE_TABLE,
            SqlState::OBJECT_IN_USE,
            SqlState::SYNTAX_ERROR,
        ] {
            assert!(!passes(&refusal), "{refusal:?}");
        }
        let waited = PlatformError::from(BootstrapError::Migrate(MigrateError::LockTimeout(
            Duration::from_secs(600),
            "_cufflink".into(),
        )));
        assert!(
            matches!(&waited, PlatformError::Unavailable(m)
                if m == "timed out after 600s waiting for the migration lock of _cufflink"),
            "{waited}"
        );
    }

    #[test]
    fn what_the_database_refuses_is_the_callers_and_what_is_unreachable_is_not() {
        let unreachable = PlatformError::from(BootstrapError::Connect(AcquireError::Unavailable(
            "connection refused".into(),
        )));
        assert!(
            matches!(&unreachable, PlatformError::Unavailable(m)
                if m == "the tenant's database cannot be reached: connection refused"),
            "{unreachable}"
        );
        let divergent = PlatformError::from(BootstrapError::Migrate(MigrateError::Plan(
            cufflink_migrate::PlanError::Duplicate { version: 1 },
        )));
        assert!(
            matches!(&divergent, PlatformError::Validation(m)
                if m == "migration version 1 is declared twice"),
            "{divergent}"
        );
    }
}
