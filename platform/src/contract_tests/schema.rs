use crate::deployer::manifest::{ColumnDefinition, ColumnType, IndexDefinition, TableDefinition};
use crate::errors::PlatformError;
use crate::policy::{PolicyKey, PolicyMode, PolicySet};
use crate::schema_manager::executor::{
    planned_changes, prepared, Direction, Guard, PreparedSchemaChanges, SchemaRequest,
};
use crate::schema_manager::introspect::fixtures::{column, index, schema, table};
use crate::schema_manager::introspect::{
    Archives, DependentKind, DropTarget, Found, LiveColumn, LiveIndex, LiveRelation, LiveSchema,
};

fn declared(name: &str, column_type: ColumnType) -> ColumnDefinition {
    ColumnDefinition {
        name: name.into(),
        column_type,
        ..Default::default()
    }
}

fn queued_emails(columns: &[&str]) -> TableDefinition {
    TableDefinition {
        name: "queued_emails".into(),
        columns: [ColumnDefinition {
            primary_key: true,
            auto_generate: true,
            ..declared("id", ColumnType::Uuid)
        }]
        .into_iter()
        .chain(columns.iter().map(|name| declared(name, ColumnType::Text)))
        .collect(),
        indexes: vec![IndexDefinition {
            name: "queued_emails_ref_id".into(),
            columns: vec!["ref_id".into()],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn live(columns: &[&str]) -> LiveSchema {
    schema(vec![(
        "queued_emails",
        LiveRelation {
            indexes: vec![
                index("queued_emails_ref_id", &["ref_id"]),
                LiveIndex {
                    unique: true,
                    partial: true,
                    ..index(
                        "queued_emails_dedup",
                        &["ref_type", "ref_id", "template_type"],
                    )
                },
            ],
            ..table(
                [column("id", "uuid")]
                    .into_iter()
                    .chain(columns.iter().map(|name| LiveColumn {
                        has_default: *name == "status",
                        ..column(name, "text")
                    }))
                    .collect(),
            )
        },
    )])
}

fn request<'a>(
    old_tables: &'a [TableDefinition],
    tables: &'a [TableDefinition],
    mode: PolicyMode,
) -> SchemaRequest<'a> {
    SchemaRequest {
        service: "notification-service",
        ownership: cufflink_types::SchemaOwnership::Managed,
        tables,
        adopted: &[],
        old_tables,
        leaving: 41,
        allow_destructive: false,
        direction: Direction::Forward,
        guard: Guard { mode, ..Guard::OFF },
    }
}

fn prepare(
    request: &SchemaRequest<'_>,
    live: Option<&LiveSchema>,
    found: Vec<Found>,
) -> Result<PreparedSchemaChanges, PlatformError> {
    planned_changes(request, &Archives::default(), live)
        .and_then(|planned| prepared(request, "notification_service", planned, found))
}

fn statements(prepared: &PreparedSchemaChanges) -> Vec<&str> {
    prepared
        .additive_sql
        .iter()
        .chain(&prepared.destructive_sql)
        .map(String::as_str)
        .collect()
}

const COLUMNS: [&str; 4] = ["ref_type", "ref_id", "template_type", "status"];

#[test]
fn the_guard_is_off_unless_a_tenant_turns_it_on() {
    assert_eq!(
        PolicySet::builtin_defaults().mode(PolicyKey::SchemaGuard),
        PolicyMode::Off
    );
    assert_eq!(Guard::OFF.mode, PolicyMode::Off);
}

#[test]
fn a_deploy_runs_the_statements_v0_27_0_ran() {
    let old = [queued_emails(&COLUMNS)];
    let new = [queued_emails(&[
        "ref_type",
        "ref_id",
        "template_type",
        "status",
        "locale",
    ])];
    let ran = [
        "ALTER TABLE \"notification_service\".\"queued_emails\" ADD COLUMN IF NOT EXISTS \"locale\" TEXT DEFAULT NULL",
        "ALTER TABLE \"notification_service\".\"queued_emails\" ALTER COLUMN \"locale\" SET NOT NULL",
    ];
    let unguarded = prepare(&request(&old, &new, PolicyMode::Off), None, vec![]).expect("off");
    assert_eq!(statements(&unguarded), ran);
    assert_eq!(unguarded.guard, None);

    let warned = prepare(
        &request(&old, &new, PolicyMode::Warn),
        Some(&live(&COLUMNS)),
        vec![],
    )
    .expect("warn");
    assert_eq!(statements(&warned), ran);
    assert_eq!(
        serde_json::to_value(&warned.diff).expect("diff"),
        serde_json::to_value(&unguarded.diff).expect("diff")
    );
}

#[test]
fn a_first_deploy_creates_what_v0_27_0_created() {
    let new = [queued_emails(&["ref_id"])];
    for (mode, live) in [
        (PolicyMode::Off, None),
        (PolicyMode::Warn, Some(LiveSchema::default())),
    ] {
        let prepared = prepare(&request(&[], &new, mode), live.as_ref(), vec![]).expect("deploys");
        assert_eq!(
            statements(&prepared),
            [
                "CREATE TABLE IF NOT EXISTS \"notification_service\".\"queued_emails\" (\"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid(), \"ref_id\" TEXT NOT NULL)",
                "CREATE INDEX IF NOT EXISTS \"queued_emails_ref_id\" ON \"notification_service\".\"queued_emails\" USING btree (\"ref_id\")",
            ],
            "{mode}"
        );
    }
}

#[test]
fn a_destructive_change_is_refused_as_v0_27_0_refused_it() {
    let old = [queued_emails(&COLUMNS)];
    let new = [queued_emails(&["ref_type", "ref_id", "template_type"])];
    for (mode, live) in [
        (PolicyMode::Off, None),
        (PolicyMode::Warn, Some(live(&COLUMNS))),
    ] {
        let error =
            prepare(&request(&old, &new, mode), live.as_ref(), vec![]).expect_err("refused");
        assert_eq!(
            error.to_string(),
            "Destructive changes: Destructive changes detected (use --allow-destructive to \
             force): DROP COLUMN queued_emails.status",
            "{mode}"
        );
    }
}

#[test]
fn what_on_migrate_made_is_neither_drift_nor_the_platforms_to_drop() {
    let tables = [queued_emails(&COLUMNS)];
    for mode in [PolicyMode::Warn, PolicyMode::Enforce] {
        let prepared = prepare(
            &request(&tables, &tables, mode),
            Some(&live(&COLUMNS)),
            vec![],
        )
        .expect("deploys");
        assert!(statements(&prepared).is_empty(), "{mode}");
        let report = prepared.guard.expect("report");
        assert!(report.drift.is_empty(), "{mode}: {:?}", report.drift);
        assert!(report.extra_columns.is_empty(), "{mode}");
        assert_eq!(report.refusal(false), None, "{mode}");
    }

    let dropping = [queued_emails(&["ref_type", "ref_id", "template_type"])];
    let hand_made = |kind, name: &str| Found {
        target: DropTarget::Column {
            table: "queued_emails".into(),
            column: "status".into(),
        },
        kind,
        name: name.into(),
        on_target: true,
        description: format!("{} {name}", kind.as_str()),
    };
    let enforced = SchemaRequest {
        allow_destructive: true,
        ..request(&tables, &dropping, PolicyMode::Enforce)
    };
    let error = prepare(
        &enforced,
        Some(&live(&COLUMNS)),
        vec![
            hand_made(DependentKind::Index, "queued_emails_dedup"),
            hand_made(DependentKind::Constraint, "items_condition_check"),
        ],
    )
    .expect_err("refused");
    let error = error.to_string();
    assert!(error.contains("index queued_emails_dedup"), "{error}");
    assert!(
        error.contains("constraint items_condition_check"),
        "{error}"
    );
}

#[test]
fn a_column_dropped_by_hand_is_never_brought_back() {
    let old = [queued_emails(&COLUMNS)];
    let new = [queued_emails(&["ref_type", "ref_id", "template_type"])];
    let gone = live(&["ref_type", "ref_id", "template_type"]);
    let enforced = prepare(
        &request(&old, &new, PolicyMode::Enforce),
        Some(&gone),
        vec![],
    )
    .expect("deploys");
    assert!(statements(&enforced).is_empty());
    assert!(enforced.guard.expect("report").drift.is_empty());

    for mode in PolicyMode::ALL {
        let standing = prepare(
            &SchemaRequest {
                guard: Guard {
                    mode,
                    accept_drift: true,
                    ..Guard::OFF
                },
                ..request(&old, &old, mode)
            },
            (mode != PolicyMode::Off).then_some(&gone),
            vec![],
        )
        .expect("deploys");
        assert!(statements(&standing).is_empty(), "{mode}");
    }
}
