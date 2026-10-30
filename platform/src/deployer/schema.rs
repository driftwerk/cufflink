use super::module::{load_wasm_module, UploadTracker};
use super::pipeline::DeployContext;
use crate::deployer::manifest::{ServiceManifest, TableDefinition};
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::config::SchemaGuardConfig;
use crate::policy::PolicyKey;
use crate::registry::RegisteredService;
use crate::repo::deployments::{Adoption, Snapshot};
use crate::repo::services::Service;
use crate::schema_manager::adopt::AdoptionReport;
use crate::schema_manager::executor::{
    apply_schema, ensure_schema, execute_online, execute_phase, prepare_schema_changes, Direction,
    Guard, PreparedSchemaChanges, SchemaRequest, Tables,
};
use crate::schema_manager::guard::GuardReport;
use crate::schema_manager::sql_migrations::{self, MigrationsReport};
use crate::tenant_sessions::{Purpose, ServiceSessions};
use crate::AppState;
use cufflink_types::manifest::validate::folded;
use cufflink_types::{HistoryMode, SchemaDiff};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub(crate) struct Options {
    pub allow_destructive: bool,
    pub accept_drift: bool,
    pub purge: bool,
    pub plan_only: bool,
    pub progress: Progress,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Progress(Option<Arc<Recorded>>);

#[derive(Debug)]
struct Recorded {
    pool: deadpool_postgres::Pool,
    job: Uuid,
}

impl Progress {
    pub(crate) fn recorded(pool: deadpool_postgres::Pool, job: Uuid) -> Self {
        Self(Some(Arc::new(Recorded { pool, job })))
    }

    pub(crate) async fn phase(&self, phase: &str, detail: Value) {
        let Some(recorded) = &self.0 else {
            return;
        };
        let mut progress = json!({ "phase": phase });
        if let (Some(fields), Some(details)) = (progress.as_object_mut(), detail.as_object()) {
            fields.extend(details.clone());
        }
        if let Err(e) =
            crate::repo::deploy_jobs::progress(&recorded.pool, recorded.job, &progress).await
        {
            tracing::warn!(job = %recorded.job, phase, error = %e, "deploy progress not recorded");
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaReport {
    pub schema_changes: Vec<String>,
    #[serde(flatten)]
    pub guard: Option<GuardReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adoption: Option<AdoptionReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_migrations: Option<MigrationsReport>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(flatten)]
    pub unserved: Unserved,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Unserved {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub private_tables: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub no_longer_served: Vec<String>,
}

impl Unserved {
    fn of(manifest: &ServiceManifest, running: Option<&ServiceManifest>) -> Self {
        let served: BTreeSet<String> = running
            .into_iter()
            .flat_map(|running| &running.tables)
            .map(|table| folded(&table.name))
            .collect();
        let private_tables: Vec<String> = manifest
            .private_tables
            .iter()
            .map(|table| table.name.clone())
            .collect();
        Self {
            no_longer_served: private_tables
                .iter()
                .filter(|table| served.contains(&folded(table)))
                .cloned()
                .collect(),
            private_tables,
        }
    }
}

pub(crate) fn declared(manifest: &ServiceManifest) -> Vec<TableDefinition> {
    manifest.declared_tables().cloned().collect()
}

fn tables_of(manifest: &ServiceManifest, left: &Snapshot) -> Tables {
    Tables::of(
        manifest.schema,
        &declared(manifest),
        &left.tables,
        &left.adopted,
    )
}

async fn running(state: &AppState, service_id: Uuid) -> PlatformResult<Option<ServiceManifest>> {
    Ok(
        crate::repo::deployments::current_manifest(&state.platform_db, service_id)
            .await?
            .and_then(|manifest| serde_json::from_value(manifest).ok()),
    )
}

impl SchemaReport {
    fn of(prepared: PreparedSchemaChanges, phases: Phases, unserved: Unserved) -> Self {
        let noted = phases
            .migrations
            .iter()
            .flat_map(MigrationsReport::warnings);
        Self {
            schema_changes: phases.executed,
            guard: prepared.guard,
            adoption: prepared.adoption,
            warnings: prepared.warnings.into_iter().chain(noted).collect(),
            sql_migrations: phases.migrations,
            unserved,
        }
    }

    pub(crate) fn adoption(&self) -> Adoption<'_> {
        self.adoption
            .as_ref()
            .map_or(Adoption::Nothing, |adoption| {
                Adoption::Read(&adoption.adopted)
            })
    }
}

async fn guard(state: &AppState, tenant_id: Uuid, options: &Options) -> PlatformResult<Guard> {
    let policies = state.tenants.policies_or_load(tenant_id).await?;
    Ok(Guard {
        mode: policies.mode(PolicyKey::SchemaGuard),
        accept_drift: options.accept_drift,
        purge: options.purge,
        accept_extra_columns: policies.config::<SchemaGuardConfig>().accept_extra_columns,
    })
}

async fn prepare(
    ctx: &DeployContext<'_>,
    tenant_pool: &ServiceSessions,
    leaving: i32,
    left: &Snapshot,
) -> PlatformResult<PreparedSchemaChanges> {
    let (state, manifest) = (ctx.state, ctx.manifest);
    let tables = tables_of(manifest, left);
    prepare_schema_changes(
        tenant_pool,
        &SchemaRequest {
            service: &manifest.name,
            ownership: manifest.schema,
            tables: &tables.managed,
            adopted: &tables.adopted,
            old_tables: &tables.left,
            leaving,
            allow_destructive: ctx.schema.allow_destructive,
            direction: Direction::Forward,
            guard: guard(state, ctx.identity.tenant_id, &ctx.schema).await?,
        },
    )
    .await
    .map(|prepared| tables.noted(prepared))
}

fn migrations_of<'a>(
    ctx: &'a DeployContext<'_>,
    left: &'a Snapshot,
    plan_only: bool,
) -> sql_migrations::Deploy<'a> {
    sql_migrations::Deploy {
        identity: &ctx.identity,
        manifest: ctx.manifest,
        left,
        plan_only,
    }
}

pub(crate) async fn plan(
    ctx: &DeployContext<'_>,
    leaving: i32,
    left: &Snapshot,
) -> PlatformResult<SchemaReport> {
    let state = ctx.state;
    let unserved = unserved(ctx).await?;
    let tenant_pool = state
        .tenant_sessions
        .service(&ctx.identity, Purpose::Ddl)
        .await?;
    let prepared = prepare(ctx, &tenant_pool, leaving, left).await?;
    let migrations = sql_migrations::run(state, &migrations_of(ctx, left, true)).await?;
    let executed = prepared
        .additive_sql
        .iter()
        .chain(&prepared.destructive_sql)
        .chain(&prepared.online_sql)
        .cloned()
        .collect();
    Ok(SchemaReport::of(
        prepared,
        Phases {
            executed,
            migrations,
        },
        unserved,
    ))
}

async fn unserved(ctx: &DeployContext<'_>) -> PlatformResult<Unserved> {
    if ctx.manifest.private_tables.is_empty() {
        return Ok(Unserved::default());
    }
    let running = running(ctx.state, ctx.identity.service_id).await?;
    Ok(Unserved::of(ctx.manifest, running.as_ref()))
}

pub(crate) async fn apply(
    ctx: &DeployContext<'_>,
    service: &Service,
    left: &Snapshot,
    new_version: i32,
    uploads: &UploadTracker,
) -> PlatformResult<SchemaReport> {
    let (state, manifest) = (ctx.state, ctx.manifest);
    let hook = match (manifest.on_migrate.as_deref(), ctx.module()) {
        (Some(_), None) => return Err(PlatformError::BadRequest(
            "Manifest declares 'on_migrate' but no wasm_base64 was bundled in the deploy request. Upgrade your CLI to a version that bundles WASM with deploy.".into(),
        )),
        (hook, _) => hook,
    };
    let leaving = service.current_version;
    if ctx.schema.plan_only {
        return plan(ctx, leaving, left).await;
    }
    let unserved = unserved(ctx).await?;
    let verified = Verified {
        left,
        report: sql_migrations::run(state, &migrations_of(ctx, left, true)).await?,
    };
    let tenant_pool = state
        .tenant_sessions
        .service(&ctx.identity, Purpose::Ddl)
        .await?;
    ensure_schema(&tenant_pool, manifest.schema).await?;
    let prepared = prepare(ctx, &tenant_pool, leaving, left).await?;
    ctx.schema
        .progress
        .phase(
            "schema",
            json!({ "statements": prepared.additive_sql.len() + prepared.destructive_sql.len() }),
        )
        .await;
    let handlers = manifest.declared_wasm_handlers();
    let load = |(bytes, sha256)| {
        uploads.mark(service.id, new_version);
        load_wasm_module(state, service.id, new_version, bytes, sha256, &handlers)
    };
    let run = match (hook, ctx.module()) {
        (Some(hook_name), Some(module @ (_, sha256))) => {
            load(module).await?;
            let hook = Hook {
                name: hook_name,
                service: RegisteredService {
                    service_id: service.id,
                    tenant_id: ctx.identity.tenant_id,
                    tenant_slug: ctx.identity.tenant_slug.clone(),
                    name: manifest.name.clone(),
                    manifest: manifest.clone(),
                    version: new_version,
                    wasm_hash: Some(sha256.to_string()),
                    commit_hash: None,
                },
            };
            run_phases(ctx, &tenant_pool, &prepared, verified, Some(&hook)).await?
        }
        (_, module) => {
            let phases = run_phases(ctx, &tenant_pool, &prepared, verified, None).await?;
            if let Some(module) = module {
                load(module).await?;
            }
            phases
        }
    };
    let online = online(state, ctx.identity.tenant_id, &prepared).await?;
    if !online.is_empty() {
        ctx.schema
            .progress
            .phase("online", json!({ "statements": online.len() }))
            .await;
    }
    let phases = Phases {
        executed: run.executed.into_iter().chain(online).collect(),
        ..run
    };
    Ok(SchemaReport::of(prepared, phases, unserved))
}

async fn online(
    state: &AppState,
    tenant_id: Uuid,
    prepared: &PreparedSchemaChanges,
) -> PlatformResult<Vec<String>> {
    if prepared.online_sql.is_empty() {
        return Ok(Vec::new());
    }
    let client = state.tenant_sessions.direct(tenant_id).await?;
    execute_online(
        &client,
        &prepared.online_sql,
        state.config.deploy_statement_timeout,
    )
    .await
}

fn left_applied(running: Option<&ServiceManifest>, target: &ServiceManifest) -> Option<String> {
    let left = running
        .map(|running| sql_migrations::left_applied(running, target))
        .unwrap_or_default();
    (!left.is_empty()).then(|| {
        format!(
            "SQL migrations are never rolled back: {} stay applied",
            left.join(", ")
        )
    })
}

pub(crate) async fn apply_rollback(
    state: &Arc<AppState>,
    service: &Service,
    tenant_slug: &str,
    manifest: &ServiceManifest,
    left: &Snapshot,
    allow_destructive: bool,
) -> PlatformResult<SchemaReport> {
    let running = running(state, service.id).await?;
    let tenant_pool = state
        .tenant_sessions
        .service(&service.identity(tenant_slug), Purpose::Ddl)
        .await?;
    ensure_schema(&tenant_pool, manifest.schema).await?;
    let tables = tables_of(manifest, left);
    let options = Options {
        allow_destructive,
        ..Options::default()
    };
    let prepared = prepare_schema_changes(
        &tenant_pool,
        &SchemaRequest {
            service: &manifest.name,
            ownership: manifest.schema,
            tables: &tables.managed,
            adopted: &tables.adopted,
            old_tables: &tables.left,
            leaving: service.current_version,
            allow_destructive,
            direction: Direction::Rollback,
            guard: guard(state, service.tenant_id, &options).await?,
        },
    )
    .await
    .map(|prepared| tables.noted(prepared))?;
    let executed = apply_schema(
        &tenant_pool,
        &prepared,
        &manifest.name,
        state.config.deploy_statement_timeout,
    )
    .await?
    .into_iter()
    .chain(online(state, service.tenant_id, &prepared).await?)
    .collect();
    let report = SchemaReport::of(
        prepared,
        Phases {
            executed,
            migrations: None,
        },
        Unserved::of(manifest, running.as_ref()),
    );
    Ok(SchemaReport {
        warnings: report
            .warnings
            .into_iter()
            .chain(left_applied(running.as_ref(), manifest))
            .collect(),
        ..report
    })
}

struct Hook<'a> {
    name: &'a str,
    service: RegisteredService,
}

struct Verified<'a> {
    left: &'a Snapshot,
    report: Option<MigrationsReport>,
}

struct Phases {
    executed: Vec<String>,
    migrations: Option<MigrationsReport>,
}

async fn run_phases(
    ctx: &DeployContext<'_>,
    tenant_pool: &ServiceSessions,
    prepared: &PreparedSchemaChanges,
    verified: Verified<'_>,
    hook: Option<&Hook<'_>>,
) -> PlatformResult<Phases> {
    let (state, manifest) = (ctx.state, ctx.manifest);
    let statement_timeout = state.config.deploy_statement_timeout;
    if hook.is_none() && manifest.migrations.is_empty() {
        return Ok(Phases {
            executed: apply_schema(tenant_pool, prepared, &manifest.name, statement_timeout)
                .await?,
            migrations: None,
        });
    }
    let progress = &ctx.schema.progress;
    let additive = execute_phase(tenant_pool, &prepared.additive_sql, statement_timeout).await?;
    progress
        .phase("additive", json!({ "statements": additive.len() }))
        .await;
    let migrations = match verified.report {
        Some(report) if report.mode == HistoryMode::Apply => {
            sql_migrations::run(state, &migrations_of(ctx, verified.left, false)).await?
        }
        report => report,
    };
    if let Some(report) = &migrations {
        progress
            .phase(
                "sql_migrations",
                json!({ "applied": report.run.applied.len(), "pending": report.run.pending.len() }),
            )
            .await;
    }
    if let Some(hook) = hook {
        progress.phase("on_migrate", json!({})).await;
        crate::router::crud::execute_hook_as(
            crate::wasm_runtime::InvocationKind::OnMigrate,
            crate::wasm_runtime::Class::Committed,
            state,
            &hook.service,
            hook.name,
            on_migrate_body(&prepared.diff)?,
            None,
            None,
        )
        .await
        .map_err(|e| {
            PlatformError::SchemaMigration(format!("on_migrate hook '{}' failed: {}", hook.name, e))
        })?;
    }
    let destructive =
        execute_phase(tenant_pool, &prepared.destructive_sql, statement_timeout).await?;
    progress
        .phase("destructive", json!({ "statements": destructive.len() }))
        .await;
    let executed: Vec<String> = additive.into_iter().chain(destructive).collect();
    info!(
        "Applied {} schema changes for {} in phases",
        executed.len(),
        manifest.name
    );
    Ok(Phases {
        executed,
        migrations,
    })
}

fn on_migrate_body(diff: &SchemaDiff) -> PlatformResult<Value> {
    Ok(serde_json::to_value(diff)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_text(diff: &SchemaDiff) -> String {
        let service = RegisteredService {
            service_id: uuid::Uuid::nil(),
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "acme".into(),
            name: "ferry-service".into(),
            manifest: ServiceManifest {
                name: "ferry-service".into(),
                ..Default::default()
            },
            version: 1,
            wasm_hash: None,
            commit_hash: None,
        };
        crate::router::crud::hook_request_text(
            on_migrate_body(diff).expect("body"),
            "handle_on_migrate",
            &service,
            None,
            None,
            &mut crate::wasm_runtime::budget::Hold::none(),
        )
        .expect("text")
    }

    #[test]
    fn on_migrate_reads_the_text_it_always_read() {
        assert_eq!(
            request_text(&SchemaDiff::default()),
            r#"{"auth":null,"body":{"added_columns":[],"added_tables":[],"dropped_columns":[],"dropped_tables":[],"nullability_changes":[],"type_changes":[]},"handler":"handle_on_migrate","headers":{},"method":"HOOK","service":"ferry-service","tenant":"acme"}"#
        );
        assert_eq!(
            request_text(&SchemaDiff {
                added_tables: vec!["batches".into()],
                dropped_tables: vec!["legacy_batches".into()],
                added_columns: vec![("sailings".into(), "min".into())],
                dropped_columns: vec![("sailings".into(), "midpoint".into())],
                type_changes: vec![(
                    "sailings".into(),
                    "weight".into(),
                    "integer".into(),
                    "double".into()
                )],
                nullability_changes: vec![("sailings".into(), "notes".into(), true)],
            }),
            r#"{"auth":null,"body":{"added_columns":[["sailings","min"]],"added_tables":["batches"],"dropped_columns":[["sailings","midpoint"]],"dropped_tables":["legacy_batches"],"nullability_changes":[["sailings","notes",true]],"type_changes":[["sailings","weight","integer","double"]]},"handler":"handle_on_migrate","headers":{},"method":"HOOK","service":"ferry-service","tenant":"acme"}"#
        );
    }

    use crate::repo::deployments::{parse_snapshot, stored};
    use crate::schema_manager::adopt::AdoptedSchema;
    use crate::schema_manager::diff::SchemaChange;
    use crate::schema_manager::executor::planned_changes;
    use crate::schema_manager::introspect::Archives;
    use cufflink_types::SchemaOwnership;

    fn notes(managed: bool) -> TableDefinition {
        serde_json::from_value(json!({
            "name": "Notes", "managed": managed,
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true},
                {"name": "body", "column_type": "text"}
            ]
        }))
        .expect("table")
    }

    fn labels() -> TableDefinition {
        serde_json::from_value(json!({
            "name": "labels",
            "columns": [{"name": "id", "column_type": "uuid", "primary_key": true}]
        }))
        .expect("table")
    }

    fn service(
        schema: SchemaOwnership,
        tables: Vec<TableDefinition>,
        private_tables: Vec<TableDefinition>,
    ) -> ServiceManifest {
        ServiceManifest {
            name: "loan-service".into(),
            schema,
            tables,
            private_tables,
            ..Default::default()
        }
    }

    fn kept(manifest: &ServiceManifest) -> Snapshot {
        let tables = declared(manifest);
        let adopted: AdoptedSchema = serde_json::from_value(Value::Object(
            tables
                .iter()
                .filter(|table| !manifest.schema.is_managed() || !table.managed)
                .map(|table| (folded(&table.name), json!({})))
                .collect(),
        ))
        .expect("adopted");
        parse_snapshot(
            &manifest.name,
            1,
            Some(stored(&tables, &adopted).expect("stored")),
        )
        .expect("snapshot")
    }

    fn planned(manifest: &ServiceManifest, left: &Snapshot, direction: Direction) -> Vec<String> {
        let tables = tables_of(manifest, left);
        planned_changes(
            &SchemaRequest {
                service: &manifest.name,
                ownership: manifest.schema,
                tables: &tables.managed,
                adopted: &tables.adopted,
                old_tables: &tables.left,
                leaving: 1,
                allow_destructive: false,
                direction,
                guard: Guard::OFF,
            },
            &Archives::default(),
            None,
        )
        .expect("nothing destructive")
        .changes
        .iter()
        .map(SchemaChange::describe)
        .collect()
    }

    fn adopted_both_ways() -> [(ServiceManifest, ServiceManifest); 2] {
        [
            (SchemaOwnership::Adopted, notes(true)),
            (SchemaOwnership::Managed, notes(false)),
        ]
        .map(|(schema, table)| {
            (
                service(schema, vec![labels(), table.clone()], vec![]),
                service(schema, vec![labels()], vec![table]),
            )
        })
    }

    const BOTH_WAYS: [Direction; 2] = [Direction::Forward, Direction::Rollback];

    #[test]
    fn moving_an_adopted_table_between_the_lists_plans_no_ddl() {
        for (served, private) in adopted_both_ways() {
            assert_eq!(
                tables_of(&private, &kept(&served)).adopted.len(),
                tables_of(&served, &kept(&private)).adopted.len(),
                "a private table gets the adoption check a served one does"
            );
            for (to, from) in [(&private, &served), (&served, &private)] {
                assert!(
                    planned(to, &kept(from), Direction::Forward).is_empty(),
                    "{:?}",
                    to.schema
                );
            }
        }
    }

    #[test]
    fn a_managed_table_made_private_and_unmanaged_plans_no_ddl() {
        let searched: TableDefinition = serde_json::from_value(json!({
            "name": "Notes",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true},
                {"name": "body", "column_type": "text"}
            ],
            "search": {"columns": ["body"]},
            "indexes": [{"name": "notes_by_body", "columns": ["body"]}]
        }))
        .expect("table");
        let served = service(SchemaOwnership::Managed, vec![labels(), searched], vec![]);
        let private = service(SchemaOwnership::Managed, vec![labels()], vec![notes(false)]);
        let left = kept(&served);
        assert!(left.adopted.is_empty() && left.tables[1].managed);
        assert!(planned(&private, &left, Direction::Forward).is_empty());
        let tables = tables_of(&private, &left);
        assert_eq!(tables.left.len(), 1, "only the table still managed is left");
        assert_eq!(tables.adopted[0].name, "Notes");

        let lowered = TableDefinition {
            name: "notes".into(),
            ..notes(false)
        };
        let respelled = service(SchemaOwnership::Managed, vec![labels()], vec![lowered]);
        assert!(
            planned(&respelled, &left, Direction::Forward).is_empty(),
            "the name a private table folds to is the table it adopts"
        );
        assert_eq!(
            tables_of(&respelled, &left)
                .left
                .iter()
                .map(|table| table.name.as_str())
                .collect::<Vec<_>>(),
            ["labels"]
        );
    }

    #[test]
    fn removing_a_private_table_plans_no_ddl() {
        for (_, private) in adopted_both_ways() {
            let without = service(private.schema, vec![labels()], vec![]);
            for direction in BOTH_WAYS {
                assert!(
                    planned(&without, &kept(&private), direction).is_empty(),
                    "{:?} {direction:?}",
                    private.schema
                );
            }
        }
    }

    #[test]
    fn the_snapshot_records_private_tables() {
        let names = |tables: &[TableDefinition]| -> Vec<String> {
            tables.iter().map(|table| table.name.clone()).collect()
        };
        for (served, private) in adopted_both_ways() {
            assert_eq!(names(&declared(&private)), ["labels", "Notes"]);
            let snapshot = kept(&private);
            assert_eq!(names(&snapshot.tables), ["labels", "Notes"]);
            assert!(snapshot.adopted.table("notes").is_some());
            let json = stored(&declared(&private), &snapshot.adopted).expect("stored");
            assert_eq!(
                json,
                stored(&declared(&served), &snapshot.adopted).expect("stored"),
                "one list, whichever list the manifest declares the table in"
            );
            assert_eq!(json.get("private_tables"), None);
        }
    }

    #[test]
    fn a_rollback_across_a_private_table_plans_no_ddl() {
        for (_, private) in adopted_both_ways() {
            let without = service(private.schema, vec![labels()], vec![]);
            for (target, left) in [
                (&private, &private),
                (&private, &without),
                (&without, &private),
            ] {
                assert!(
                    planned(target, &kept(left), Direction::Rollback).is_empty(),
                    "{:?}",
                    private.schema
                );
            }
        }
    }

    #[test]
    fn a_reader_without_the_key_plans_no_ddl() {
        for (_, private) in adopted_both_ways() {
            let mut wire = serde_json::to_value(&private).expect("wire");
            wire.as_object_mut()
                .expect("manifest")
                .remove("private_tables");
            let older: ServiceManifest = serde_json::from_value(wire).expect("older reader");
            assert_eq!(declared(&older).len(), 1);
            for direction in BOTH_WAYS {
                assert!(
                    planned(&older, &kept(&private), direction).is_empty(),
                    "{:?} {direction:?}",
                    private.schema
                );
            }
        }
    }

    #[test]
    fn a_plan_names_the_private_tables_and_those_no_longer_served() {
        let (served, private) = &adopted_both_ways()[0];
        assert_eq!(Unserved::of(served, None), Unserved::default());
        assert_eq!(Unserved::of(served, Some(private)), Unserved::default());
        assert_eq!(
            Unserved::of(private, None),
            Unserved {
                private_tables: vec!["Notes".into()],
                no_longer_served: vec![],
            }
        );
        assert!(Unserved::of(private, Some(private))
            .no_longer_served
            .is_empty());
        let mut lowercase = served.clone();
        lowercase.tables[1].name = "notes".into();
        let moved = Unserved::of(private, Some(&lowercase));
        assert_eq!(moved.no_longer_served, ["Notes"]);

        let report = |unserved| {
            serde_json::to_value(SchemaReport {
                unserved,
                ..SchemaReport::default()
            })
            .expect("report")
        };
        assert_eq!(
            report(Unserved::default()),
            json!({"schema_changes": []}),
            "a plan without private tables is the plan it was"
        );
        assert_eq!(
            report(moved),
            json!({
                "schema_changes": [],
                "private_tables": ["Notes"],
                "no_longer_served": ["Notes"]
            })
        );
    }
}
