use super::adopt::{validate_adoption, AdoptedSchema, AdoptionReport};
use super::diff::{build_schema_diff, diff_schemas, SchemaChange};
use super::guard::{self, GuardReport, GuardResult};
use super::introspect::{self, Archives, Found, LiveSchema};
use super::migration::{archive_name, generate_sql_phase, Drops, MigrationPhase};
use crate::deployer::manifest::TableDefinition;
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::PolicyMode;
use crate::tenant_sessions::{ServiceSessions, Statements};
use cufflink_types::manifest::validate::folded;
use cufflink_types::{SchemaDiff, SchemaOwnership};
use std::collections::BTreeSet;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Guard {
    pub mode: PolicyMode,
    pub accept_drift: bool,
    pub purge: bool,
    pub accept_extra_columns: bool,
}

impl Guard {
    pub const OFF: Guard = Guard {
        mode: PolicyMode::Off,
        accept_drift: false,
        purge: false,
        accept_extra_columns: true,
    };

    fn enforced(&self) -> bool {
        self.mode == PolicyMode::Enforce
    }
}

#[derive(Debug, Clone, Default)]
pub struct Tables {
    pub managed: Vec<TableDefinition>,
    pub adopted: Vec<TableDefinition>,
    pub left: Vec<TableDefinition>,
    pub taken_over: Vec<String>,
}

impl Tables {
    pub fn of(
        ownership: SchemaOwnership,
        declared: &[TableDefinition],
        left: &[TableDefinition],
        left_adopted: &AdoptedSchema,
    ) -> Self {
        if ownership == SchemaOwnership::Adopted {
            return Self {
                adopted: declared.to_vec(),
                ..Self::default()
            };
        }
        let (managed, adopted): (Vec<_>, Vec<_>) =
            declared.iter().cloned().partition(|table| table.managed);
        let left = left
            .iter()
            .filter(|table| {
                table.managed
                    && left_adopted.table(&table.name).is_none()
                    && !adopted
                        .iter()
                        .any(|now| folded(&now.name) == folded(&table.name))
            })
            .cloned()
            .collect();
        let taken_over = managed
            .iter()
            .filter(|table| left_adopted.table(&table.name).is_some())
            .map(|table| table.name.clone())
            .collect();
        Self {
            managed,
            adopted,
            left,
            taken_over,
        }
    }

    pub fn noted(&self, prepared: PreparedSchemaChanges) -> PreparedSchemaChanges {
        let taken = self.taken_over.iter().map(|table| {
            format!(
                "table {table} was adopted by the version being left and is declared managed \
                 now: the platform makes it where it is not there, changes it as the manifest \
                 says, and drops it when a later manifest leaves it out"
            )
        });
        PreparedSchemaChanges {
            warnings: prepared.warnings.into_iter().chain(taken).collect(),
            ..prepared
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SchemaRequest<'a> {
    pub service: &'a str,
    pub ownership: SchemaOwnership,
    pub tables: &'a [TableDefinition],
    pub adopted: &'a [TableDefinition],
    pub old_tables: &'a [TableDefinition],
    pub leaving: i32,
    pub allow_destructive: bool,
    pub direction: Direction,
    pub guard: Guard,
}

#[derive(Debug, Default)]
pub struct PreparedSchemaChanges {
    pub additive_sql: Vec<String>,
    pub destructive_sql: Vec<String>,
    pub online_sql: Vec<String>,
    pub diff: SchemaDiff,
    pub guard: Option<GuardReport>,
    pub adoption: Option<AdoptionReport>,
    pub warnings: Vec<String>,
}

impl SchemaRequest<'_> {
    fn adopts(&self) -> bool {
        self.ownership == SchemaOwnership::Adopted || !self.adopted.is_empty()
    }
}

pub async fn ensure_schema(db: &ServiceSessions, ownership: SchemaOwnership) -> PlatformResult<()> {
    if ownership == SchemaOwnership::Adopted {
        return Ok(());
    }
    db.get()
        .await?
        .execute(
            &format!("CREATE SCHEMA IF NOT EXISTS {}", db.schema_ident()),
            &[],
        )
        .await?;
    Ok(())
}

#[derive(Debug, Default)]
pub struct Planned {
    pub changes: Vec<SchemaChange>,
    pub guarded: Option<GuardResult>,
    pub warnings: Vec<String>,
}

fn reshaped_indexes(changes: Vec<SchemaChange>, guard: &Guard) -> (Vec<SchemaChange>, Vec<String>) {
    let (kept, left): (Vec<_>, Vec<_>) = changes.into_iter().partition(|change| {
        guard.enforced() || !matches!(change, SchemaChange::ReplaceIndex { .. })
    });
    let warnings = left
        .iter()
        .map(|change| {
            format!(
                "{} is not run: an index declared in another shape under the name it has is \
                 replaced only where schema_guard is enforced. Declare it under another name, or \
                 replace it in an SQL migration",
                change.describe()
            )
        })
        .collect();
    (kept, warnings)
}

fn archive_drops(changes: Vec<SchemaChange>, version: i32) -> Vec<SchemaChange> {
    changes
        .into_iter()
        .map(|c| match c {
            SchemaChange::DropTable(name) => SchemaChange::ArchiveTable {
                archive_name: archive_name(version, &name),
                table_name: name,
            },
            SchemaChange::DropColumn { table, column_name } => SchemaChange::ArchiveColumn {
                archive_name: archive_name(version, &column_name),
                table,
                column_name,
            },
            other => other,
        })
        .collect()
}

fn restore_archived(changes: Vec<SchemaChange>, archives: &Archives) -> Vec<SchemaChange> {
    changes
        .into_iter()
        .map(|change| match change {
            SchemaChange::CreateTable(table) => match archives.latest(None, &table.name) {
                Some(archived) => {
                    info!(
                        "Restoring archived table {} as {}",
                        archived.archive_name, table.name
                    );
                    SchemaChange::RestoreTable {
                        archive_name: archived.archive_name.clone(),
                        restore_name: table.name.clone(),
                    }
                }
                None => SchemaChange::CreateTable(table),
            },
            SchemaChange::AddColumn { table, column } => {
                match archives.latest(Some(&table), &column.name) {
                    Some(archived) => {
                        info!(
                            "Restoring archived column {}.{} as {}",
                            table, archived.archive_name, column.name
                        );
                        SchemaChange::RestoreColumn {
                            archive_name: archived.archive_name.clone(),
                            table,
                            column,
                        }
                    }
                    None => SchemaChange::AddColumn { table, column },
                }
            }
            other => other,
        })
        .collect()
}

fn refuse_destructive(changes: &[SchemaChange]) -> PlatformResult<()> {
    let destructive: Vec<String> = changes
        .iter()
        .filter(|change| change.is_destructive())
        .map(SchemaChange::describe)
        .collect();
    if destructive.is_empty() {
        return Ok(());
    }
    Err(PlatformError::DestructiveChanges(format!(
        "Destructive changes detected (use --allow-destructive to force): {}",
        destructive.join(", ")
    )))
}

pub fn planned_changes(
    request: &SchemaRequest<'_>,
    archives: &Archives,
    live: Option<&LiveSchema>,
) -> PlatformResult<Planned> {
    let (diff, left) = reshaped_indexes(
        diff_schemas(request.old_tables, request.tables),
        &request.guard,
    );
    let changes = match request.direction {
        Direction::Rollback => archive_drops(diff, request.leaving),
        Direction::Forward => restore_archived(diff, archives),
    };
    let guarded =
        live.map(|live| guard::reconcile_with_live(changes.clone(), request.tables, live));
    let changes = match &guarded {
        Some(result) if request.guard.enforced() => result.run.clone(),
        _ => changes,
    };
    if !request.allow_destructive {
        refuse_destructive(&changes)?;
    }
    let warnings = left
        .into_iter()
        .chain(changes.iter().filter_map(SchemaChange::caution))
        .collect();
    Ok(Planned {
        changes,
        guarded,
        warnings,
    })
}

fn drops(request: &SchemaRequest<'_>) -> Drops {
    match (request.guard.enforced(), request.guard.purge) {
        (true, true) => Drops::Restrict,
        (true, false) => Drops::Archive {
            version: request.leaving,
        },
        (false, _) => Drops::Legacy,
    }
}

pub fn prepared(
    request: &SchemaRequest<'_>,
    schema: &str,
    planned: Planned,
    found: Vec<Found>,
) -> PlatformResult<PreparedSchemaChanges> {
    let report = planned.guarded.as_ref().map(|result| {
        let owned = guard::owned_by(request.old_tables.iter().chain(request.tables));
        GuardReport::of(
            request.guard.mode,
            result,
            guard::unmanaged(found, schema, &owned),
            request.guard.accept_extra_columns,
        )
    });
    if let Some(report) = &report {
        report.record();
        let accept_drift = request.guard.accept_drift || request.direction == Direction::Rollback;
        match report.refusal(accept_drift) {
            Some(refusal) if request.guard.enforced() => {
                warn!(
                    event = "schema_guard_blocked",
                    service = request.service,
                    dependents = report.dependents.len(),
                    drift = report.drift.len(),
                    "schema guard refused the deploy"
                );
                return Err(PlatformError::Validation(refusal));
            }
            Some(finding) => warn!(service = request.service, "{finding}"),
            None => {}
        }
    }
    if planned.changes.is_empty() {
        info!("No schema changes detected for {}", request.service);
    }
    let ident = cufflink_types::manifest::validate::folded_ident(schema);
    let phase = |phase| generate_sql_phase(&ident, &planned.changes, phase, drops(request));
    planned
        .warnings
        .iter()
        .for_each(|warning| warn!(service = request.service, "{warning}"));
    Ok(PreparedSchemaChanges {
        additive_sql: phase(MigrationPhase::Additive),
        destructive_sql: phase(MigrationPhase::Destructive),
        online_sql: phase(MigrationPhase::Online),
        diff: build_schema_diff(&planned.changes),
        guard: report,
        adoption: None,
        warnings: planned.warnings,
    })
}

async fn look(
    db: &ServiceSessions,
    request: &SchemaRequest<'_>,
) -> PlatformResult<(Option<LiveSchema>, Vec<String>)> {
    if request.guard.mode == PolicyMode::Off && !request.adopts() {
        return Ok((None, Vec::new()));
    }
    match introspect::live_schema(db).await {
        Ok(live) => Ok((Some(live), Vec::new())),
        Err(e) if request.guard.enforced() || request.adopts() => Err(e),
        Err(e) => Ok((
            None,
            vec![format!("schema_guard could not read the schema: {e}")],
        )),
    }
}

async fn dependents(
    db: &ServiceSessions,
    request: &SchemaRequest<'_>,
    planned: &Planned,
) -> PlatformResult<(Vec<Found>, Vec<String>)> {
    let targets = match planned.guarded {
        Some(_) => guard::drop_targets(&planned.changes),
        None => Default::default(),
    };
    match introspect::dependents(db, &targets).await {
        Ok(found) => Ok((found, Vec::new())),
        Err(e) if request.guard.enforced() => Err(e),
        Err(e) => Ok((
            Vec::new(),
            vec![format!(
                "schema_guard could not read what depends on the drops: {e}"
            )],
        )),
    }
}

async fn check_search_configs(
    db: &ServiceSessions,
    tables: &[&TableDefinition],
) -> PlatformResult<()> {
    let configs: BTreeSet<&str> = tables
        .iter()
        .filter_map(|table| table.search.as_ref())
        .map(|spec| spec.config.as_str())
        .collect();
    if configs.is_empty() {
        return Ok(());
    }
    let named: Vec<&str> = configs.iter().copied().collect();
    let known: BTreeSet<String> = db
        .get()
        .await?
        .query(
            "SELECT cfgname::text FROM pg_ts_config WHERE cfgname = ANY($1)",
            &[&named],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    match configs.iter().find(|config| !known.contains(**config)) {
        Some(missing) => Err(PlatformError::Validation(format!(
            "search.config: the database has no text search configuration '{missing}'"
        ))),
        None => Ok(()),
    }
}

fn may_restore(request: &SchemaRequest<'_>) -> bool {
    request.direction == Direction::Forward
        && diff_schemas(request.old_tables, request.tables)
            .iter()
            .any(|change| {
                matches!(
                    change,
                    SchemaChange::CreateTable(_) | SchemaChange::AddColumn { .. }
                )
            })
}

pub async fn prepare_schema_changes(
    db: &ServiceSessions,
    request: &SchemaRequest<'_>,
) -> PlatformResult<PreparedSchemaChanges> {
    check_search_configs(
        db,
        &request
            .tables
            .iter()
            .chain(request.adopted)
            .collect::<Vec<_>>(),
    )
    .await?;
    let (live, unread) = look(db, request).await?;
    let adoption = live
        .as_ref()
        .filter(|_| request.adopts())
        .map(|live| validate_adoption(db.schema(), request.adopted, live).accepted())
        .transpose()?;
    if request.ownership == SchemaOwnership::Adopted {
        return Ok(PreparedSchemaChanges {
            adoption,
            ..PreparedSchemaChanges::default()
        });
    }
    let archives = if may_restore(request) {
        introspect::archives(db).await?
    } else {
        Archives::default()
    };
    let guarded = live
        .as_ref()
        .filter(|_| request.guard.mode != PolicyMode::Off);
    let planned = planned_changes(request, &archives, guarded)?;
    let (found, unfound) = dependents(db, request, &planned).await?;
    let prepared = prepared(request, db.schema(), planned, found)?;
    Ok(PreparedSchemaChanges {
        adoption,
        warnings: unread
            .into_iter()
            .chain(unfound)
            .chain(prepared.warnings)
            .collect(),
        ..prepared
    })
}

pub async fn execute_phase(
    db: &ServiceSessions,
    statements: &[String],
    statement_timeout: Duration,
) -> PlatformResult<Vec<String>> {
    if statements.is_empty() {
        return Ok(vec![]);
    }

    let mut client = db.get().await?;
    let txn = client.transaction().await?;
    txn.execute("SET LOCAL lock_timeout = '5s'", &[]).await?;
    txn.execute(
        &format!(
            "SET LOCAL statement_timeout = {}",
            statement_timeout.as_millis()
        ),
        &[],
    )
    .await?;

    let mut executed = Vec::new();
    for stmt in statements {
        info!("Executing migration: {}", stmt);
        txn.execute(stmt.as_str(), &[]).await.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("lock timeout") || msg.contains("lock_timeout") {
                PlatformError::SchemaMigration(format!(
                    "Migration timed out waiting for table lock (table is in use by active connections). Retry or restart services. SQL: {}",
                    stmt
                ))
            } else {
                PlatformError::SchemaMigration(format!("Migration failed: {} — SQL: {}", e, stmt))
            }
        })?;
        executed.push(stmt.clone());
    }

    txn.commit().await?;
    Ok(executed)
}

pub async fn execute_online(
    client: &tokio_postgres::Client,
    statements: &[String],
    statement_timeout: Duration,
) -> PlatformResult<Vec<String>> {
    client
        .batch_execute(&format!(
            "SET statement_timeout = {}",
            statement_timeout.as_millis()
        ))
        .await?;
    for statement in statements {
        info!("Executing migration: {statement}");
        client.batch_execute(statement).await.map_err(|e| {
            PlatformError::SchemaMigration(format!("Migration failed: {e} — SQL: {statement}"))
        })?;
    }
    Ok(statements.to_vec())
}

pub async fn apply_schema(
    db: &ServiceSessions,
    prepared: &PreparedSchemaChanges,
    service: &str,
    statement_timeout: Duration,
) -> PlatformResult<Vec<String>> {
    let statements: Vec<String> = prepared
        .additive_sql
        .iter()
        .chain(&prepared.destructive_sql)
        .cloned()
        .collect();
    let executed = execute_phase(db, &statements, statement_timeout).await?;
    if !executed.is_empty() {
        info!("Applied {} schema changes for {}", executed.len(), service);
    }
    Ok(executed)
}

#[cfg(test)]
mod tests {
    use super::super::introspect::fixtures::{column, schema, table as live_table};
    use super::super::introspect::{Archived, DependentKind, DropTarget};
    use super::*;
    use crate::deployer::manifest::{ColumnDefinition, ColumnType};

    fn declared(name: &str, column_type: ColumnType) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type,
            ..Default::default()
        }
    }

    fn users(columns: &[&str]) -> TableDefinition {
        TableDefinition {
            name: "users".into(),
            columns: std::iter::once(ColumnDefinition {
                primary_key: true,
                ..declared("id", ColumnType::Uuid)
            })
            .chain(columns.iter().map(|name| declared(name, ColumnType::Text)))
            .collect(),
            ..Default::default()
        }
    }

    fn live_users(columns: &[&str]) -> LiveSchema {
        schema(vec![(
            "users",
            live_table(
                std::iter::once(column("id", "uuid"))
                    .chain(columns.iter().map(|name| column(name, "text")))
                    .collect(),
            ),
        )])
    }

    fn request<'a>(
        old_tables: &'a [TableDefinition],
        tables: &'a [TableDefinition],
        guard: Guard,
    ) -> SchemaRequest<'a> {
        SchemaRequest {
            service: "user-service",
            ownership: SchemaOwnership::Managed,
            tables,
            adopted: &[],
            old_tables,
            leaving: 7,
            allow_destructive: true,
            direction: Direction::Forward,
            guard,
        }
    }

    fn guard(mode: PolicyMode) -> Guard {
        Guard { mode, ..Guard::OFF }
    }

    fn statements(prepared: &PreparedSchemaChanges) -> Vec<&str> {
        prepared
            .additive_sql
            .iter()
            .chain(&prepared.destructive_sql)
            .map(String::as_str)
            .collect()
    }

    fn prepare(
        request: &SchemaRequest<'_>,
        live: Option<&LiveSchema>,
        found: Vec<Found>,
    ) -> PlatformResult<PreparedSchemaChanges> {
        planned_changes(request, &Archives::default(), live)
            .and_then(|planned| prepared(request, "user_service", planned, found))
    }

    fn view_on(column: &str) -> Found {
        Found {
            target: DropTarget::Column {
                table: "users".into(),
                column: column.into(),
            },
            kind: DependentKind::View,
            name: "reporting.customer_overview".into(),
            on_target: false,
            description: "rule _RETURN on view reporting.customer_overview".into(),
        }
    }

    #[test]
    fn a_destructive_change_is_refused_unless_allowed() {
        let (old, new) = ([users(&["email", "phone"])], [users(&["email"])]);
        let refused = SchemaRequest {
            allow_destructive: false,
            ..request(&old, &new, Guard::OFF)
        };
        let error = prepare(&refused, None, vec![]).expect_err("refused");
        assert!(matches!(error, PlatformError::DestructiveChanges(_)));
        assert_eq!(
            error.to_string(),
            "Destructive changes: Destructive changes detected (use --allow-destructive to \
             force): DROP COLUMN users.phone"
        );
    }

    #[test]
    fn without_a_guard_a_deploy_runs_the_statements_it_always_ran() {
        let (old, new) = ([users(&["email", "phone"])], [users(&["email", "name"])]);
        let prepared = prepare(&request(&old, &new, Guard::OFF), None, vec![]).expect("prepared");
        assert_eq!(
            statements(&prepared),
            [
                "ALTER TABLE \"user_service\".\"users\" ADD COLUMN IF NOT EXISTS \"name\" TEXT DEFAULT NULL",
                "ALTER TABLE \"user_service\".\"users\" ALTER COLUMN \"name\" SET NOT NULL",
                "ALTER TABLE \"user_service\".\"users\" DROP COLUMN IF EXISTS \"phone\"",
            ]
        );
        assert_eq!(prepared.guard, None);
        assert_eq!(
            prepared.diff.dropped_columns,
            [("users".to_string(), "phone".to_string())]
        );
    }

    #[test]
    fn a_guard_that_warns_reports_and_runs_the_statements_it_always_ran() {
        let (old, new) = ([users(&["email", "phone"])], [users(&["email", "name"])]);
        let live = live_users(&["email", "name"]);
        let unguarded = prepare(&request(&old, &new, Guard::OFF), None, vec![]).expect("off");
        let warned = prepare(
            &request(&old, &new, guard(PolicyMode::Warn)),
            Some(&live),
            vec![view_on("phone")],
        )
        .expect("warn");
        assert_eq!(statements(&warned), statements(&unguarded));
        assert_eq!(
            serde_json::to_value(&warned.diff).expect("diff"),
            serde_json::to_value(&unguarded.diff).expect("diff")
        );
        let report = warned.guard.expect("report");
        assert_eq!(report.mode, PolicyMode::Warn);
        assert_eq!(
            report.satisfied,
            ["ADD COLUMN users.name", "DROP COLUMN users.phone"]
        );
        assert_eq!(report.dependents.len(), 1);
        assert!(report.drift.is_empty());
    }

    #[test]
    fn a_guard_that_is_enforced_runs_what_is_left_and_archives_what_it_drops() {
        let (old, new) = (
            [users(&["email", "phone", "fax"])],
            [users(&["email", "name"])],
        );
        let live = live_users(&["email", "phone", "name"]);
        let enforced = request(&old, &new, guard(PolicyMode::Enforce));
        let prepared = prepare(&enforced, Some(&live), vec![]).expect("enforce");
        assert_eq!(
            statements(&prepared),
            [
                "ALTER TABLE \"user_service\".\"users\" RENAME COLUMN \"phone\" TO \"_archived_v7_phone\"",
                "ALTER TABLE \"user_service\".\"users\" ALTER COLUMN \"_archived_v7_phone\" DROP NOT NULL",
            ]
        );
        assert_eq!(
            prepared.diff.dropped_columns,
            [("users".to_string(), "phone".to_string())]
        );
        assert!(prepared.diff.added_columns.is_empty());

        let purged = SchemaRequest {
            guard: Guard {
                purge: true,
                ..enforced.guard
            },
            ..enforced
        };
        assert_eq!(
            statements(&prepare(&purged, Some(&live), vec![]).expect("purge")),
            ["ALTER TABLE \"user_service\".\"users\" DROP COLUMN IF EXISTS \"phone\" RESTRICT"]
        );
    }

    #[test]
    fn a_column_dropped_by_hand_needs_no_permission_under_an_enforced_guard() {
        let (old, new) = ([users(&["email", "phone"])], [users(&["email"])]);
        let enforced = SchemaRequest {
            allow_destructive: false,
            ..request(&old, &new, guard(PolicyMode::Enforce))
        };
        let prepared = prepare(&enforced, Some(&live_users(&["email"])), vec![]).expect("deploys");
        assert!(statements(&prepared).is_empty());
        assert!(prepared.diff.is_empty());
        let report = prepared.guard.expect("report");
        assert_eq!(report.satisfied, ["DROP COLUMN users.phone"]);
        assert!(report.drift.is_empty());

        let warned = SchemaRequest {
            guard: guard(PolicyMode::Warn),
            ..enforced
        };
        assert!(matches!(
            prepare(&warned, Some(&live_users(&["email"])), vec![]),
            Err(PlatformError::DestructiveChanges(_))
        ));
    }

    #[test]
    fn an_enforced_guard_refuses_a_drop_something_unmanaged_depends_on() {
        let (old, new) = ([users(&["email", "phone"])], [users(&["email"])]);
        let live = live_users(&["email", "phone"]);
        for purge in [false, true] {
            let enforced = SchemaRequest {
                guard: Guard {
                    mode: PolicyMode::Enforce,
                    accept_drift: true,
                    purge,
                    accept_extra_columns: true,
                },
                ..request(&old, &new, Guard::OFF)
            };
            let error =
                prepare(&enforced, Some(&live), vec![view_on("phone")]).expect_err("refused");
            assert!(matches!(error, PlatformError::Validation(_)));
            assert!(
                error.to_string().contains(
                    "view reporting.customer_overview depends on user_service.users.phone"
                ),
                "{error}"
            );
        }
    }

    #[test]
    fn an_enforced_guard_refuses_drift_unless_it_is_accepted() {
        let tables = [users(&["email", "phone"])];
        let live = live_users(&["email"]);
        let enforced = request(&tables, &tables, guard(PolicyMode::Enforce));
        let error = prepare(&enforced, Some(&live), vec![]).expect_err("refused");
        assert!(
            error.to_string().contains("column users.phone is missing"),
            "{error}"
        );
        assert!(error.to_string().contains("--accept-drift"), "{error}");

        let accepted = SchemaRequest {
            guard: Guard {
                accept_drift: true,
                ..enforced.guard
            },
            ..enforced
        };
        let prepared = prepare(&accepted, Some(&live), vec![]).expect("accepted");
        assert!(statements(&prepared).is_empty());
        assert_eq!(prepared.guard.expect("report").drift.len(), 1);

        let warned = request(&tables, &tables, guard(PolicyMode::Warn));
        let prepared = prepare(&warned, Some(&live), vec![]).expect("warned");
        assert_eq!(prepared.guard.expect("report").drift.len(), 1);
    }

    #[test]
    fn a_rollback_archives_what_it_leaves_behind_and_is_never_refused() {
        let (old, new) = (
            [users(&["email", "phone", "fax"])],
            [users(&["email", "name"])],
        );
        let rollback = |guard| SchemaRequest {
            allow_destructive: false,
            direction: Direction::Rollback,
            ..request(&old, &new, guard)
        };
        let archived = [
            "ALTER TABLE \"user_service\".\"users\" ADD COLUMN IF NOT EXISTS \"name\" TEXT DEFAULT NULL",
            "ALTER TABLE \"user_service\".\"users\" RENAME COLUMN \"phone\" TO \"_archived_v7_phone\"",
            "ALTER TABLE \"user_service\".\"users\" ALTER COLUMN \"_archived_v7_phone\" DROP NOT NULL",
            "ALTER TABLE \"user_service\".\"users\" RENAME COLUMN \"fax\" TO \"_archived_v7_fax\"",
            "ALTER TABLE \"user_service\".\"users\" ALTER COLUMN \"_archived_v7_fax\" DROP NOT NULL",
            "ALTER TABLE \"user_service\".\"users\" ALTER COLUMN \"name\" SET NOT NULL",
        ];
        let unguarded = prepare(&rollback(Guard::OFF), None, vec![]).expect("off");
        assert_eq!(statements(&unguarded), archived);
        assert!(unguarded.diff.dropped_columns.is_empty());

        let live = live_users(&["email", "phone"]);
        let warned = prepare(&rollback(guard(PolicyMode::Warn)), Some(&live), vec![]);
        assert_eq!(statements(&warned.expect("warn")), archived);

        let enforced =
            prepare(&rollback(guard(PolicyMode::Enforce)), Some(&live), vec![]).expect("enforce");
        assert_eq!(
            statements(&enforced),
            [archived[0], archived[1], archived[2], archived[5]]
        );
        assert_eq!(
            enforced.guard.expect("report").satisfied,
            ["ARCHIVE COLUMN users.fax AS _archived_v7_fax"]
        );

        let contradicted = live_users(&["phone"]);
        let prepared = prepare(
            &rollback(guard(PolicyMode::Enforce)),
            Some(&contradicted),
            vec![],
        )
        .expect("a rollback goes ahead");
        assert_eq!(
            prepared.guard.expect("report").drift,
            [guard::Drift::ColumnMissing {
                table: "users".into(),
                column: "email".into()
            }]
        );
    }

    #[test]
    fn what_comes_back_is_its_latest_archive() {
        let archives = Archives::new(
            [
                ("users", Some("_archived_v3_phone")),
                ("users", Some("_archived_v6_phone")),
                ("_archived_v5_orders", None),
            ]
            .into_iter()
            .filter_map(|(relation, column)| Archived::parse(relation, column, None))
            .collect(),
        );
        let orders = TableDefinition {
            name: "orders".into(),
            ..users(&[])
        };
        let (old, new) = ([users(&[])], [users(&["phone", "name"]), orders]);
        let forward = request(&old, &new, Guard::OFF);
        let planned = planned_changes(&forward, &archives, None).expect("planned");
        assert_eq!(
            planned
                .changes
                .iter()
                .map(SchemaChange::describe)
                .collect::<Vec<_>>(),
            [
                "RESTORE TABLE orders FROM _archived_v5_orders",
                "RESTORE COLUMN users.phone FROM _archived_v6_phone",
                "ADD COLUMN users.name",
            ]
        );
        assert_eq!(
            statements(&prepared(&forward, "user_service", planned, vec![]).expect("prepared")),
            [
                "ALTER TABLE \"user_service\".\"_archived_v5_orders\" RENAME TO \"orders\"",
                "ALTER TABLE \"user_service\".\"users\" RENAME COLUMN \"_archived_v6_phone\" TO \"phone\"",
                "ALTER TABLE \"user_service\".\"users\" ADD COLUMN IF NOT EXISTS \"name\" TEXT DEFAULT NULL",
                "ALTER TABLE \"user_service\".\"users\" ALTER COLUMN \"phone\" SET NOT NULL",
                "ALTER TABLE \"user_service\".\"users\" ALTER COLUMN \"name\" SET NOT NULL",
            ]
        );
    }

    #[test]
    fn a_table_that_is_or_was_adopted_is_never_the_platforms_to_change() {
        let unmanaged = |name: &str| TableDefinition {
            name: name.into(),
            managed: false,
            ..users(&[])
        };
        let named = |name: &str| TableDefinition {
            name: name.into(),
            ..users(&[])
        };
        let names =
            |tables: &[TableDefinition]| tables.iter().map(|t| t.name.clone()).collect::<Vec<_>>();
        let was_adopted: AdoptedSchema =
            serde_json::from_value(serde_json::json!({"book": {}})).expect("adopted");
        let declared = [named("users"), unmanaged("notes"), named("book")];
        let left = [named("users"), named("notes"), named("book"), named("gone")];

        let tables = Tables::of(SchemaOwnership::Managed, &declared, &left, &was_adopted);
        assert_eq!(names(&tables.managed), ["users", "book"]);
        assert_eq!(names(&tables.adopted), ["notes"]);
        assert_eq!(names(&tables.left), ["users", "gone"]);
        assert_eq!(tables.taken_over, ["book"]);
        assert_eq!(
            tables.noted(PreparedSchemaChanges::default()).warnings,
            [
                "table book was adopted by the version being left and is declared managed now: \
                 the platform makes it where it is not there, changes it as the manifest says, \
                 and drops it when a later manifest leaves it out"
            ]
        );
        let planned = planned_changes(
            &SchemaRequest {
                tables: &tables.managed,
                adopted: &tables.adopted,
                old_tables: &tables.left,
                ..request(&[], &[], Guard::OFF)
            },
            &Archives::default(),
            None,
        )
        .expect("planned");
        assert_eq!(
            planned
                .changes
                .iter()
                .map(SchemaChange::describe)
                .collect::<Vec<_>>(),
            ["CREATE TABLE book", "DROP TABLE gone"]
        );

        let adopted = Tables::of(SchemaOwnership::Adopted, &declared, &left, &was_adopted);
        assert!(adopted.managed.is_empty() && adopted.left.is_empty());
        assert!(adopted.taken_over.is_empty());
        assert_eq!(names(&adopted.adopted), ["users", "notes", "book"]);
    }

    fn indexed(unique: bool) -> TableDefinition {
        TableDefinition {
            indexes: vec![crate::deployer::manifest::IndexDefinition {
                name: "users_by_email".into(),
                columns: vec!["email".into()],
                unique,
                ..Default::default()
            }],
            ..users(&["email"])
        }
    }

    #[test]
    fn an_index_declared_in_another_shape_is_replaced_only_under_an_enforced_guard() {
        let (before, after) = ([indexed(false)], [indexed(true)]);
        for mode in [PolicyMode::Off, PolicyMode::Warn] {
            let live = (mode == PolicyMode::Warn).then(|| live_users(&["email"]));
            let prepared = prepare(
                &request(&before, &after, guard(mode)),
                live.as_ref(),
                vec![],
            )
            .expect("prepared");
            assert!(statements(&prepared).is_empty(), "{mode:?}");
            assert_eq!(
                prepared.warnings,
                [
                    "REPLACE INDEX users_by_email ON users is not run: an index declared in \
                  another shape under the name it has is replaced only where schema_guard is \
                  enforced. Declare it under another name, or replace it in an SQL migration"
                ],
                "{mode:?}"
            );
            let refused = SchemaRequest {
                allow_destructive: false,
                ..request(&before, &after, guard(mode))
            };
            assert!(
                prepare(&refused, live.as_ref(), vec![]).is_ok(),
                "{mode:?}: what is not run needs no permission"
            );
        }

        let enforced = request(&before, &after, guard(PolicyMode::Enforce));
        let prepared = prepare(&enforced, Some(&live_users(&["email"])), vec![]).expect("prepared");
        assert_eq!(
            statements(&prepared),
            [
                "DROP INDEX IF EXISTS \"user_service\".\"users_by_email\"",
                "CREATE UNIQUE INDEX IF NOT EXISTS \"users_by_email\" ON \
                 \"user_service\".\"users\" USING btree (\"email\")",
            ]
        );
        assert_eq!(
            prepared.warnings,
            [
                "REPLACE INDEX users_by_email ON users: the index is dropped and built again in \
              one transaction, and writes to users wait for it (SHARE). A table too large for \
              that replaces it in an SQL migration, CONCURRENTLY"
            ]
        );
        let refused = SchemaRequest {
            allow_destructive: false,
            ..enforced
        };
        assert!(matches!(
            prepare(&refused, Some(&live_users(&["email"])), vec![]),
            Err(PlatformError::DestructiveChanges(_))
        ));
    }

    #[test]
    fn a_search_added_to_a_table_that_is_there_says_what_it_holds_and_is_indexed_online() {
        let searched = TableDefinition {
            search: Some(cufflink_types::SearchSpec {
                columns: vec!["email".into()],
                ..Default::default()
            }),
            ..users(&["email"])
        };
        let (before, after) = ([users(&["email"])], [searched]);
        let prepared =
            prepare(&request(&before, &after, Guard::OFF), None, vec![]).expect("prepared");
        assert_eq!(prepared.additive_sql.len(), 2);
        assert!(prepared.destructive_sql.is_empty());
        assert_eq!(
            prepared.online_sql,
            [
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"users__search_gin\" ON \
              \"user_service\".\"users\" USING gin (\"_search\")"
            ]
        );
        assert_eq!(
            prepared.warnings,
            [
                "SET SEARCH ON users: the search vector is a stored generated column, and adding \
              it rewrites users while nothing else reads or writes it (ACCESS EXCLUSIVE); its \
              index is then built CONCURRENTLY. A table too large for that adds both in an SQL \
              migration"
            ]
        );

        let made = prepare(&request(&[], &after, Guard::OFF), None, vec![]).expect("prepared");
        assert!(
            made.online_sql.is_empty() && made.warnings.is_empty(),
            "a table that is made has no rows to rewrite"
        );
        assert_eq!(made.additive_sql.len(), 4);
    }

    #[test]
    fn archives_are_read_only_for_a_deploy_that_adds_what_one_may_hold() {
        let (one, two) = ([users(&["email"])], [users(&["email", "name"])]);
        assert!(!may_restore(&request(&one, &one, Guard::OFF)));
        assert!(may_restore(&request(&one, &two, Guard::OFF)));
        assert!(may_restore(&request(&[], &one, Guard::OFF)));
        assert!(!may_restore(&request(&two, &one, Guard::OFF)));
        assert!(!may_restore(&SchemaRequest {
            direction: Direction::Rollback,
            ..request(&one, &two, Guard::OFF)
        }));
    }

    #[test]
    fn nothing_to_change_is_nothing_to_run() {
        let tables = [users(&["email"])];
        let prepared =
            prepare(&request(&tables, &tables, Guard::OFF), None, vec![]).expect("prepared");
        assert!(statements(&prepared).is_empty());
        assert!(prepared.diff.is_empty());
        assert_eq!(prepared.guard, None);
    }
}
