use super::migrate_exec::{self, Action, MigrateError, Report, Run};
use crate::deployer::manifest::ServiceManifest;
use crate::errors::{PlatformError, PlatformResult};
use crate::registry::ServiceIdentity;
use crate::repo::deployments::Snapshot;
use crate::tenant_sessions::Purpose;
use crate::AppState;
use cufflink_migrate::Applied;
use cufflink_types::manifest::validate::folded;
use cufflink_types::{HistoryMode, SqlMigration};
use serde::Serialize;
use std::collections::BTreeSet;
use std::time::Instant;
use tokio_postgres::Client;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MigrationsReport {
    pub mode: HistoryMode,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub locked: bool,
    #[serde(flatten)]
    pub run: Report,
}

impl MigrationsReport {
    pub fn warnings(&self) -> Vec<String> {
        let pending: Vec<String> = self
            .run
            .pending
            .iter()
            .map(|m| format!("V{}__{}", m.version, m.name))
            .collect();
        let locked = self.locked.then(|| {
            format!(
                "the migration lock of {} is held by another runner: what it applies is not in \
                 what was read",
                self.run.history
            )
        });
        let left = (self.mode == HistoryMode::VerifyOnly && !pending.is_empty()).then(|| {
            format!(
                "migration_history.mode is verify_only: {} left to the legacy authority",
                pending.join(", ")
            )
        });
        let recovered = self.run.recovered_indexes.iter().map(|index| {
            format!("index {index} was left invalid by an interrupted build and was built again")
        });
        locked.into_iter().chain(left).chain(recovered).collect()
    }
}

#[derive(Clone, Copy)]
pub struct Deploy<'a> {
    pub identity: &'a ServiceIdentity,
    pub manifest: &'a ServiceManifest,
    pub left: &'a Snapshot,
    pub plan_only: bool,
}

impl Deploy<'_> {
    fn first(&self) -> bool {
        self.manifest.schema.is_managed()
            && self.left.tables.is_empty()
            && self.left.adopted.is_empty()
    }

    fn made(&self) -> Vec<String> {
        self.manifest
            .declared_tables()
            .filter(|table| self.first() && table.managed)
            .map(|table| folded(&table.name))
            .collect()
    }
}

fn record(identity: &ServiceIdentity, applied: usize, started: Instant) {
    if applied == 0 {
        return;
    }
    metrics::counter!(
        "cufflink_sql_migrations_applied_total",
        "tenant" => identity.tenant_slug.clone(),
        "service" => identity.service_name.clone(),
    )
    .increment(applied as u64);
    metrics::histogram!("cufflink_sql_migration_duration_seconds")
        .record(started.elapsed().as_secs_f64());
}

pub async fn run(
    state: &AppState,
    deploy: &Deploy<'_>,
) -> PlatformResult<Option<MigrationsReport>> {
    let (identity, manifest) = (deploy.identity, deploy.manifest);
    if manifest.migrations.is_empty() {
        return Ok(None);
    }
    let history = manifest.migration_history();
    let run = Run::of_service(
        &identity.schema,
        &manifest.migrations,
        Some(&history),
        deploy.plan_only,
        None,
        state.config.sql_migrations,
    )?;
    let sessions = &state.tenant_sessions;
    let mut client = match run.action {
        Action::Verify => sessions.direct(identity.tenant_id).await?,
        Action::Apply => sessions.direct_for(identity, Purpose::Guest).await?,
    };
    let started = Instant::now();
    let outcome = migrate(&mut client, &run, history.mode, deploy).await;
    let applied = match &outcome {
        Ok((report, _)) => report.applied.len(),
        Err(MigrateError::Apply { applied, .. } | MigrateError::Escaped { applied, .. }) => {
            applied.len()
        }
        Err(_) => 0,
    };
    record(identity, applied, started);
    outcome
        .map(|(run, locked)| {
            Some(MigrationsReport {
                mode: history.mode,
                locked,
                run,
            })
        })
        .map_err(|error| stayed_applied(error, applied))
}

async fn migrate(
    client: &mut Client,
    run: &Run<'_>,
    mode: HistoryMode,
    deploy: &Deploy<'_>,
) -> Result<(Report, bool), MigrateError> {
    let standing = Standing::read(client, run, &deploy.made()).await?;
    if let Some(refused) = standing.refusal(run, mode, deploy.first()) {
        return Err(MigrateError::History(refused));
    }
    match run.action {
        Action::Verify => {
            migrate_exec::verified(run, &standing.applied).map(|report| (report, standing.locked))
        }
        Action::Apply => migrate_exec::run(client, run)
            .await
            .map(|report| (report, false)),
    }
}

const HISTORY_EXISTS: &str = "SELECT to_regclass($1::text) IS NOT NULL";

const LOCK_HELD: &str = "\
SELECT EXISTS (
    SELECT 1 FROM pg_locks l
      JOIN pg_database d ON d.oid = l.database
     WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
       AND d.datname = current_database()
       AND ((l.classid::bigint << 32) | l.objid::bigint) = $1)";

const HELD: &str = "\
SELECT c.relname::text FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S', 'c')
   AND c.relname::text <> ALL($2)
UNION
SELECT p.proname::text FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = $1
UNION
SELECT t.typname::text FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace
 WHERE n.nspname = $1 AND t.typtype IN ('e', 'd', 'r')
 ORDER BY 1";

const NAMED: usize = 5;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Standing {
    recorded: bool,
    applied: Vec<Applied>,
    locked: bool,
    held: Vec<String>,
}

impl Standing {
    async fn read(
        client: &mut Client,
        run: &Run<'_>,
        made: &[String],
    ) -> Result<Self, MigrateError> {
        let tx = client.build_transaction().read_only(true).start().await?;
        let recorded: bool = tx
            .query_one(HISTORY_EXISTS, &[&run.history.quoted()])
            .await?
            .get(0);
        let (applied, locked) = if recorded {
            (
                migrate_exec::recorded(&tx, run).await?,
                tx.query_one(LOCK_HELD, &[&run.lock_key()]).await?.get(0),
            )
        } else {
            (Vec::new(), false)
        };
        let held = if applied.is_empty() {
            tx.query(HELD, &[&folded(run.schema), &made])
                .await?
                .iter()
                .map(|row| row.get(0))
                .collect()
        } else {
            Vec::new()
        };
        tx.rollback().await?;
        Ok(Self {
            recorded,
            applied,
            locked,
            held,
        })
    }

    fn refusal(&self, run: &Run<'_>, mode: HistoryMode, first: bool) -> Option<String> {
        if !self.applied.is_empty() {
            return None;
        }
        let blank = format!(
            "table '{}' {} schema '{}'",
            run.history.table(),
            if self.recorded {
                "holds nothing in"
            } else {
                "does not exist in"
            },
            run.history.schema()
        );
        let under: Vec<String> = run
            .migrations
            .iter()
            .filter(|migration| {
                run.baseline
                    .is_some_and(|baseline| migration.version <= baseline)
            })
            .map(SqlMigration::file_stem)
            .collect();
        match (mode, run.baseline) {
            (HistoryMode::VerifyOnly, _) if self.recorded => None,
            (HistoryMode::VerifyOnly, _) => Some(format!(
                "{blank}; mode verify_only verifies the history another runner keeps and \
                 begins none. Name the table the migrations are recorded in"
            )),
            (HistoryMode::Apply, Some(_)) if under.is_empty() => None,
            (HistoryMode::Apply, Some(baseline)) => Some(format!(
                "{blank}, and {} {} at or under the baseline {baseline}: a history begun here \
                 vouches for nothing applied before it, and what a baseline takes for applied \
                 is not applied again",
                under.join(", "),
                if under.len() == 1 { "is" } else { "are" }
            )),
            (HistoryMode::Apply, None) if first && self.held.is_empty() => None,
            (HistoryMode::Apply, None) => Some(format!(
                "{blank}, and {}: a history is begun by the first deploy of a service into a \
                 schema that holds nothing but its own tables, or under a declared baseline. \
                 Name the table the migrations are recorded in",
                self.holding(run.schema)
            )),
        }
    }

    fn holding(&self, schema: &str) -> String {
        let named: Vec<&str> = self.held.iter().take(NAMED).map(String::as_str).collect();
        match self.held.len().saturating_sub(NAMED) {
            _ if named.is_empty() => format!(
                "schema '{}' is adopted or was deployed to before",
                folded(schema)
            ),
            0 => format!("schema '{}' holds {}", folded(schema), named.join(", ")),
            more => format!(
                "schema '{}' holds {} and {more} more",
                folded(schema),
                named.join(", ")
            ),
        }
    }
}

fn stayed_applied(error: MigrateError, applied: usize) -> PlatformError {
    match (PlatformError::from(error), applied) {
        (PlatformError::SchemaMigration(message), applied) if applied > 0 => {
            PlatformError::SchemaMigration(format!(
                "{message}; the {applied} before it in this deploy stay applied"
            ))
        }
        (error, _) => error,
    }
}

pub fn left_applied(left: &ServiceManifest, target: &ServiceManifest) -> Vec<String> {
    let kept: BTreeSet<u32> = target.migrations.iter().map(|m| m.version).collect();
    left.migrations
        .iter()
        .filter(|migration| !kept.contains(&migration.version))
        .map(|migration| migration.file_stem())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use migrate_exec::MigrationRef;

    fn migration(version: u32, name: &str) -> SqlMigration {
        SqlMigration {
            version,
            name: name.into(),
            sql: "SELECT 1;".into(),
            no_transaction: false,
        }
    }

    fn manifest(migrations: Vec<SqlMigration>) -> ServiceManifest {
        ServiceManifest {
            name: "loan-service".into(),
            migrations,
            ..Default::default()
        }
    }

    fn pending(mode: HistoryMode) -> MigrationsReport {
        MigrationsReport {
            mode,
            locked: false,
            run: Report {
                history: "loan_service.schema_history".into(),
                verified: 2,
                pending: vec![MigrationRef::from(&migration(7, "renewal_index"))],
                recovered_indexes: vec!["idx_loans_renewals".into()],
                ..Report::default()
            },
        }
    }

    #[test]
    fn what_is_left_to_the_legacy_runner_is_said() {
        assert_eq!(
            pending(HistoryMode::VerifyOnly).warnings(),
            [
                "migration_history.mode is verify_only: V7__renewal_index left to the legacy \
                 authority",
                "index idx_loans_renewals was left invalid by an interrupted build and was \
                 built again",
            ]
        );
        assert_eq!(pending(HistoryMode::Apply).warnings().len(), 1);
        assert!(MigrationsReport::default().warnings().is_empty());
        let locked = MigrationsReport {
            locked: true,
            ..pending(HistoryMode::Apply)
        };
        assert_eq!(
            locked.warnings()[0],
            "the migration lock of loan_service.schema_history is held by another runner: what \
             it applies is not in what was read"
        );
        assert_eq!(serde_json::to_value(&locked).expect("json")["locked"], true);
    }

    #[test]
    fn the_report_is_the_runs_with_its_mode() {
        let json = serde_json::to_value(pending(HistoryMode::VerifyOnly)).expect("json");
        assert_eq!(json["mode"], "verify_only");
        assert_eq!(json["history"], "loan_service.schema_history");
        assert_eq!(json["verified"], 2);
        assert_eq!(json["pending"][0]["version"], 7);
        assert_eq!(json["pending"][0]["name"], "renewal_index");
        assert!(json["pending"][0]["checksum"].is_u64());
    }

    const TIMEOUTS: migrate_exec::Timeouts = migrate_exec::Timeouts {
        lock_wait: std::time::Duration::ZERO,
        statement: std::time::Duration::ZERO,
        lock: std::time::Duration::ZERO,
    };

    fn run<'a>(local: &'a [SqlMigration], table: &str, baseline: Option<u32>) -> Run<'a> {
        let declared = cufflink_types::MigrationHistory {
            table: table.into(),
            baseline,
            ..Default::default()
        };
        Run::of_service(
            "loan_service",
            local,
            Some(&declared),
            false,
            None,
            TIMEOUTS,
        )
        .expect("a run")
    }

    fn holding(recorded: bool, held: &[&str]) -> Standing {
        Standing {
            recorded,
            held: held.iter().map(|name| name.to_string()).collect(),
            ..Standing::default()
        }
    }

    fn applied(migration: &SqlMigration) -> Applied {
        Applied {
            version: migration.version as i32,
            name: migration.name.clone(),
            checksum: cufflink_migrate::checksum(migration),
            applied_on: "2026-09-28T10:30:00Z".into(),
        }
    }

    #[test]
    fn a_history_is_begun_only_where_nothing_was_applied_before_it() {
        let local = [
            migration(1, "initial_tables"),
            migration(6, "add_loan_kind"),
        ];
        let mistyped = run(&local, "schema_histroy", None);
        assert_eq!(
            holding(false, &[]).refusal(&mistyped, HistoryMode::Apply, true),
            None,
            "the first deploy of a service into a schema that holds nothing"
        );
        let refused = holding(false, &["loans", "loan_kind"])
            .refusal(&mistyped, HistoryMode::Apply, true)
            .expect("refused");
        assert!(
            refused.starts_with(
                "table 'schema_histroy' does not exist in schema 'loan_service', and schema \
                 'loan_service' holds loans, loan_kind: "
            ),
            "{refused}"
        );
        let deployed_before = holding(false, &[])
            .refusal(&mistyped, HistoryMode::Apply, false)
            .expect("refused");
        assert!(
            deployed_before.contains("is adopted or was deployed to before"),
            "{deployed_before}"
        );
        let emptied = holding(true, &["loans"])
            .refusal(&mistyped, HistoryMode::Apply, true)
            .expect("refused");
        assert!(
            emptied.starts_with("table 'schema_histroy' holds nothing in schema 'loan_service'"),
            "{emptied}"
        );
        let many: Vec<String> = (1..=8).map(|n| format!("t{n}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert!(holding(false, &many)
            .refusal(&mistyped, HistoryMode::Apply, false)
            .is_some_and(|refused| refused.contains("holds t1, t2, t3, t4, t5 and 3 more")));
    }

    #[test]
    fn a_history_another_runner_keeps_is_never_begun_for_it() {
        let local = [migration(1, "initial_tables")];
        let mistyped = run(&local, "public.schema_history_deferd", None);
        for (first, held) in [(true, vec![]), (false, vec!["loans"])] {
            let refused = holding(false, &held)
                .refusal(&mistyped, HistoryMode::VerifyOnly, first)
                .expect("refused");
            assert!(
                refused.starts_with(
                    "table 'schema_history_deferd' does not exist in schema 'public'; mode \
                     verify_only"
                ),
                "{refused}"
            );
        }
        assert_eq!(
            holding(true, &["loans"]).refusal(&mistyped, HistoryMode::VerifyOnly, false),
            None,
            "a history that runner has made and not yet written to"
        );
    }

    #[test]
    fn a_baseline_begins_a_history_and_applies_nothing_at_or_under_it() {
        let local = [
            migration(1, "initial_tables"),
            migration(6, "add_loan_kind"),
            migration(7, "renewal_index"),
        ];
        let held = holding(false, &["loans"]);
        assert_eq!(
            held.refusal(&run(&local, "history", Some(0)), HistoryMode::Apply, false),
            None
        );
        assert_eq!(
            held.refusal(
                &run(&local[2..], "history", Some(6)),
                HistoryMode::Apply,
                false
            ),
            None
        );
        let refused = held
            .refusal(&run(&local, "history", Some(6)), HistoryMode::Apply, false)
            .expect("refused");
        assert!(
            refused
                .contains("V1__initial_tables, V6__add_loan_kind are at or under the baseline 6"),
            "{refused}"
        );
        let one = held
            .refusal(
                &run(&local[1..], "history", Some(6)),
                HistoryMode::Apply,
                true,
            )
            .expect("refused");
        assert!(
            one.contains("V6__add_loan_kind is at or under the baseline 6"),
            "{one}"
        );
    }

    #[test]
    fn a_history_that_holds_what_was_applied_is_continued() {
        let local = [migration(1, "initial_tables")];
        let kept = Standing {
            recorded: true,
            applied: vec![applied(&local[0])],
            ..Standing::default()
        };
        for mode in [HistoryMode::Apply, HistoryMode::VerifyOnly] {
            assert_eq!(
                kept.refusal(&run(&local, "history", None), mode, false),
                None
            );
        }
    }

    #[test]
    fn the_first_deploy_of_a_managed_service_makes_its_own_tables() {
        let identity = ServiceIdentity {
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "acme".into(),
            service_id: uuid::Uuid::nil(),
            service_name: "loan-service".into(),
            schema: "loan_service".into(),
        };
        let table = |name: &str, managed| cufflink_types::TableDefinition {
            name: name.into(),
            managed,
            ..Default::default()
        };
        let managed = ServiceManifest {
            tables: vec![table("Loan_Tags", true), table("loans", false)],
            ..manifest(vec![migration(1, "initial_tables")])
        };
        let (nothing, tables) = (
            Snapshot::default(),
            Snapshot {
                tables: vec![table("loan_tags", true)],
                ..Snapshot::default()
            },
        );
        let deploy = |manifest, left| Deploy {
            identity: &identity,
            manifest,
            left,
            plan_only: false,
        };
        assert!(deploy(&managed, &nothing).first());
        assert_eq!(deploy(&managed, &nothing).made(), ["loan_tags"]);
        assert!(!deploy(&managed, &tables).first());
        assert!(deploy(&managed, &tables).made().is_empty());
        let adopted = ServiceManifest {
            schema: cufflink_types::SchemaOwnership::Adopted,
            ..managed.clone()
        };
        assert!(!deploy(&adopted, &nothing).first());
        assert!(deploy(&adopted, &nothing).made().is_empty());
    }

    #[test]
    fn a_rollback_names_the_migrations_it_leaves_applied() {
        let left = manifest(vec![
            migration(1, "initial_tables"),
            migration(6, "add_loan_kind"),
            migration(7, "renewal_index"),
        ]);
        let target = manifest(vec![migration(1, "initial_tables")]);
        assert_eq!(
            left_applied(&left, &target),
            ["V6__add_loan_kind", "V7__renewal_index"]
        );
        assert!(left_applied(&target, &left).is_empty());
        assert!(left_applied(&left, &left).is_empty());
    }

    #[test]
    fn a_failure_says_what_stays_applied() {
        let failed = || MigrateError::LockTimeout(std::time::Duration::from_secs(1), "h".into());
        assert!(matches!(
            stayed_applied(failed(), 2),
            PlatformError::Unavailable(_)
        ));
        let plan = MigrateError::History("bad".into());
        assert!(matches!(
            stayed_applied(plan, 0),
            PlatformError::Validation(_)
        ));
    }
}
