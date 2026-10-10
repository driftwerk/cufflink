use super::diff::{kept_search, SchemaChange};
use super::introspect::{
    DependentKind, DropTarget, Found, LiveIndex, LiveRelation, LiveSchema, RelationKind,
    ARCHIVE_PREFIX,
};
use super::migration::{object_name, SOFT_DELETE_COLUMN};
use crate::deployer::manifest::{
    CheckConstraint, ColumnDefinition, IndexDefinition, TableDefinition,
};
use crate::policy::PolicyMode;
use crate::router::query::search::{search_index_name, SEARCH_COLUMN};
use cufflink_types::manifest::validate::folded;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

const CHECK: &str = "c";

const SEARCH_TYPE: &str = "tsvector";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnedObjects {
    indexes: BTreeSet<String>,
    constraints: BTreeSet<String>,
    columns: BTreeSet<String>,
}

impl OwnedObjects {
    pub fn for_table(table: &TableDefinition) -> Self {
        let named = |label: &'static str, has: fn(&ColumnDefinition) -> bool| {
            table
                .columns
                .iter()
                .filter(move |column| has(column))
                .map(move |column| object_name(&table.name, Some(&column.name), label))
        };
        let primary_key = object_name(&table.name, None, "pkey");
        let unique: Vec<String> = named("key", |c| c.unique).collect();
        let search = kept_search(table).map(|_| folded(&search_index_name(&table.name)));
        Self {
            indexes: table
                .indexes
                .iter()
                .map(|index| folded(&index.name))
                .chain(unique.iter().cloned())
                .chain([primary_key.clone()])
                .chain(search)
                .collect(),
            constraints: named("fkey", |c| c.references.is_some())
                .chain(table.checks.iter().map(|check| folded(&check.name)))
                .chain(unique)
                .chain([primary_key])
                .collect(),
            columns: table
                .soft_delete
                .then(|| SOFT_DELETE_COLUMN.to_string())
                .into_iter()
                .chain(kept_search(table).map(|_| SEARCH_COLUMN.to_string()))
                .collect(),
        }
    }

    pub fn with(mut self, other: Self) -> Self {
        self.indexes.extend(other.indexes);
        self.constraints.extend(other.constraints);
        self.columns.extend(other.columns);
        self
    }

    pub fn column(&self, name: &str) -> bool {
        self.columns.contains(name)
    }

    fn owns(&self, found: &Found) -> bool {
        found.on_target
            && match found.kind {
                DependentKind::Index => self.indexes.contains(&found.name),
                DependentKind::Constraint => self.constraints.contains(&found.name),
                DependentKind::GeneratedColumn => self.columns.contains(&found.name),
                _ => false,
            }
    }
}

pub fn owned_by<'a>(
    definitions: impl IntoIterator<Item = &'a TableDefinition>,
) -> BTreeMap<String, OwnedObjects> {
    definitions
        .into_iter()
        .fold(BTreeMap::new(), |mut owned, table| {
            let objects = OwnedObjects::for_table(table);
            let merged = match owned.remove(&folded(&table.name)) {
                Some(known) => known.with(objects),
                None => objects,
            };
            owned.insert(folded(&table.name), merged);
            owned
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Drift {
    TableMissing {
        table: String,
    },
    NotATable {
        table: String,
    },
    ColumnMissing {
        table: String,
        column: String,
    },
    ColumnType {
        table: String,
        column: String,
        declared: String,
        live: String,
    },
    IndexDiffers {
        table: String,
        index: String,
        live: String,
    },
    ExtraColumn {
        table: String,
        column: String,
    },
    NotASearchVector {
        table: String,
        column: String,
        live: String,
    },
}

impl std::fmt::Display for Drift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Drift::TableMissing { table } => write!(f, "table {table} is missing"),
            Drift::NotATable { table } => write!(f, "{table} exists and is not a table"),
            Drift::ColumnMissing { table, column } => {
                write!(f, "column {table}.{column} is missing")
            }
            Drift::ColumnType {
                table,
                column,
                declared,
                live,
            } => write!(f, "column {table}.{column} is {live}, declared {declared}"),
            Drift::IndexDiffers { table, index, live } => write!(
                f,
                "index {index} of {table} is not the one declared: {live}"
            ),
            Drift::ExtraColumn { table, column } => {
                write!(f, "column {table}.{column} is not declared")
            }
            Drift::NotASearchVector {
                table,
                column,
                live,
            } => write!(
                f,
                "column {table}.{column} ({live}) is not the generated tsvector the platform \
                 keeps the search vector of {table} in"
            ),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GuardResult {
    pub run: Vec<SchemaChange>,
    pub satisfied: Vec<SchemaChange>,
    pub drift: Vec<Drift>,
    pub extra_columns: Vec<(String, String)>,
}

enum Verdict {
    Run,
    Satisfied,
    Drift(Drift),
}

fn held(relation: &LiveRelation, table: &str, column: &ColumnDefinition) -> Result<bool, Drift> {
    match relation.column(&column.name) {
        None => Ok(false),
        Some(found) if found.holds(&column.column_type) => Ok(true),
        Some(found) => Err(Drift::ColumnType {
            table: table.to_string(),
            column: column.name.clone(),
            declared: column.column_type.to_sql().into_owned(),
            live: found.type_name.clone(),
        }),
    }
}

fn column_missing(table: &str, column: &ColumnDefinition) -> Drift {
    Drift::ColumnMissing {
        table: table.to_string(),
        column: column.name.clone(),
    }
}

fn column_verdict(
    live: &LiveSchema,
    table: &str,
    column: &ColumnDefinition,
    absent: Verdict,
) -> Verdict {
    let Some(relation) = live.relation(table) else {
        return Verdict::Drift(Drift::TableMissing {
            table: table.to_string(),
        });
    };
    match held(relation, table, column) {
        Ok(true) => Verdict::Satisfied,
        Ok(false) => absent,
        Err(drift) => Verdict::Drift(drift),
    }
}

fn unquoted(name: &str) -> &str {
    name.strip_prefix('"')
        .and_then(|name| name.strip_suffix('"'))
        .unwrap_or(name)
}

fn same_index(declared: &IndexDefinition, table: &str, on: &str, live: &LiveIndex) -> bool {
    folded(table) == on
        && declared.unique == live.unique
        && declared.method.as_sql() == live.method
        && declared.predicate.is_some() == live.partial
        && declared.columns.iter().map(|column| folded(column)).eq(live
            .columns
            .iter()
            .map(|column| unquoted(column).to_string()))
}

fn index_verdict(live: &LiveSchema, table: &str, index: &IndexDefinition) -> Verdict {
    match live.index(&index.name) {
        None => Verdict::Run,
        Some((on, found)) if same_index(index, table, on, found) => Verdict::Satisfied,
        Some((on, found)) => Verdict::Drift(Drift::IndexDiffers {
            table: on.to_string(),
            index: index.name.clone(),
            live: found.definition.clone(),
        }),
    }
}

fn added_column(live: &LiveSchema, table: &str, column: &ColumnDefinition) -> Verdict {
    let unfinished = !column.nullable
        && live
            .relation(table)
            .and_then(|relation| relation.column(&column.name))
            .is_some_and(|found| !found.not_null);
    match column_verdict(live, table, column, Verdict::Run) {
        Verdict::Satisfied if unfinished => Verdict::Run,
        verdict => verdict,
    }
}

fn check_verdict(relation: &LiveRelation, check: &CheckConstraint) -> Verdict {
    let condition = check.expr.trim();
    let held = relation.constraint(&check.name).is_some_and(|found| {
        found.kind == CHECK
            && [
                format!("CHECK ({condition})"),
                format!("CHECK (({condition}))"),
            ]
            .contains(&found.definition)
    });
    absent(!held)
}

enum Vector {
    Absent,
    Kept { indexed: bool },
    Other(Drift),
}

impl Vector {
    fn of(relation: &LiveRelation, table: &str) -> Self {
        match relation.column(SEARCH_COLUMN) {
            None => Vector::Absent,
            Some(found) if found.generated && found.type_name == SEARCH_TYPE => Vector::Kept {
                indexed: relation
                    .index(&search_index_name(table))
                    .is_some_and(|index| index.valid),
            },
            Some(found) => Vector::Other(Drift::NotASearchVector {
                table: table.to_string(),
                column: SEARCH_COLUMN.to_string(),
                live: found.type_name.clone(),
            }),
        }
    }
}

fn search_verdict(relation: &LiveRelation, table: &str, kept: bool, changed: bool) -> Verdict {
    match Vector::of(relation, table) {
        Vector::Other(drift) => Verdict::Drift(drift),
        Vector::Absent => absent(kept),
        Vector::Kept { indexed } => absent(changed || !kept || !indexed),
    }
}

fn absent(present: bool) -> Verdict {
    if present {
        Verdict::Run
    } else {
        Verdict::Satisfied
    }
}

fn has_column(live: &LiveSchema, table: &str, column: &str) -> bool {
    live.relation(table)
        .is_some_and(|relation| relation.column(column).is_some())
}

fn declared_column<'a>(
    declared: &'a [TableDefinition],
    table: &str,
    column: &str,
) -> Option<&'a ColumnDefinition> {
    declared
        .iter()
        .find(|t| t.name == table)
        .and_then(|t| t.columns.iter().find(|c| c.name == column))
}

fn existing_table(live: &LiveRelation, table: &TableDefinition) -> Result<(), Drift> {
    if live.kind != RelationKind::Table {
        return Err(Drift::NotATable {
            table: table.name.clone(),
        });
    }
    table.columns.iter().try_for_each(|column| {
        held(live, &table.name, column)?
            .then_some(())
            .ok_or_else(|| column_missing(&table.name, column))
    })
}

fn verdicts(
    change: SchemaChange,
    declared: &[TableDefinition],
    live: &LiveSchema,
) -> Vec<(SchemaChange, Verdict)> {
    let verdict = match &change {
        SchemaChange::CreateTable(table) => match live.relation(&table.name) {
            None => Verdict::Run,
            Some(found) => match existing_table(found, table) {
                Err(drift) => Verdict::Drift(drift),
                Ok(()) => {
                    let indexes = table.indexes.iter().map(|index| {
                        let verdict = index_verdict(live, &table.name, index);
                        let create = SchemaChange::CreateIndex {
                            table: table.name.clone(),
                            index: index.clone(),
                        };
                        (create, verdict)
                    });
                    let checks = table.checks.iter().map(|check| {
                        let add = SchemaChange::AddCheck {
                            table: table.name.clone(),
                            check: check.clone(),
                        };
                        (add, check_verdict(found, check))
                    });
                    let search = kept_search(table).map(|spec| {
                        let set = SchemaChange::Search {
                            table: table.name.clone(),
                            spec: Some(spec.clone()),
                        };
                        (set, search_verdict(found, &table.name, true, false))
                    });
                    return std::iter::once((change.clone(), Verdict::Satisfied))
                        .chain(indexes)
                        .chain(checks)
                        .chain(search)
                        .collect();
                }
            },
        },
        SchemaChange::AddColumn { table, column } => added_column(live, table, column),
        SchemaChange::AlterColumnType {
            table, column_name, ..
        } => match live.relation(table).and_then(|r| r.column(column_name)) {
            None => Verdict::Drift(Drift::ColumnMissing {
                table: table.clone(),
                column: column_name.clone(),
            }),
            Some(found) => absent(
                !declared_column(declared, table, column_name)
                    .is_some_and(|column| found.holds(&column.column_type)),
            ),
        },
        SchemaChange::AlterNullability {
            table,
            column_name,
            nullable,
        } => match live.relation(table).and_then(|r| r.column(column_name)) {
            Some(found) if found.not_null != *nullable => Verdict::Satisfied,
            Some(_) => Verdict::Run,
            None => Verdict::Drift(Drift::ColumnMissing {
                table: table.clone(),
                column: column_name.clone(),
            }),
        },
        SchemaChange::CreateIndex { table, index } => index_verdict(live, table, index),
        SchemaChange::ReplaceIndex { table, index } => match live.index(&index.name) {
            Some((on, found)) if same_index(index, table, on, found) => Verdict::Satisfied,
            _ => Verdict::Run,
        },
        SchemaChange::AddCheck { table, check } => match live.relation(table) {
            Some(relation) => check_verdict(relation, check),
            None => Verdict::Drift(Drift::TableMissing {
                table: table.clone(),
            }),
        },
        SchemaChange::DropCheck { table, name } => absent(
            live.relation(table)
                .is_some_and(|relation| relation.constraint(name).is_some()),
        ),
        SchemaChange::DropTable(table)
        | SchemaChange::ArchiveTable {
            table_name: table, ..
        } => absent(live.relation(table).is_some()),
        SchemaChange::DropColumn { table, column_name }
        | SchemaChange::ArchiveColumn {
            table, column_name, ..
        } => absent(has_column(live, table, column_name)),
        SchemaChange::DropIndex { index_name, .. } => absent(live.index(index_name).is_some()),
        SchemaChange::Search { table, spec } => match live.relation(table) {
            Some(relation) => search_verdict(relation, table, spec.is_some(), true),
            None => Verdict::Drift(Drift::TableMissing {
                table: table.clone(),
            }),
        },
        SchemaChange::AlterForeignKey { .. }
        | SchemaChange::RestoreTable { .. }
        | SchemaChange::RestoreColumn { .. } => Verdict::Run,
    };
    vec![(change, verdict)]
}

#[derive(Default)]
struct Pending {
    tables: BTreeSet<String>,
    columns: BTreeSet<(String, String)>,
}

impl Pending {
    fn of(changes: &[SchemaChange]) -> Self {
        changes.iter().fold(Self::default(), |mut pending, change| {
            match change {
                SchemaChange::CreateTable(table) => {
                    pending.tables.insert(table.name.clone());
                }
                SchemaChange::RestoreTable { restore_name, .. } => {
                    pending.tables.insert(restore_name.clone());
                }
                SchemaChange::AddColumn { table, column }
                | SchemaChange::RestoreColumn { table, column, .. } => {
                    pending.columns.insert((table.clone(), column.name.clone()));
                }
                SchemaChange::AlterColumnType {
                    table, column_name, ..
                }
                | SchemaChange::DropColumn { table, column_name }
                | SchemaChange::ArchiveColumn {
                    table, column_name, ..
                } => {
                    pending.columns.insert((table.clone(), column_name.clone()));
                }
                _ => {}
            }
            pending
        })
    }

    fn column(&self, table: &str, column: &str) -> bool {
        self.columns
            .contains(&(table.to_string(), column.to_string()))
    }
}

fn standing(
    table: &TableDefinition,
    live: &LiveSchema,
    pending: &Pending,
) -> (Vec<Drift>, Vec<(String, String)>) {
    let Some(relation) = live.relation(&table.name) else {
        return (
            vec![Drift::TableMissing {
                table: table.name.clone(),
            }],
            Vec::new(),
        );
    };
    let drift = table
        .columns
        .iter()
        .filter(|column| !pending.column(&table.name, &column.name))
        .filter_map(|column| {
            let missing = Verdict::Drift(column_missing(&table.name, column));
            match column_verdict(live, &table.name, column, missing) {
                Verdict::Drift(drift) => Some(drift),
                Verdict::Run | Verdict::Satisfied => None,
            }
        })
        .collect();
    let owned = OwnedObjects::for_table(table);
    let declared: BTreeSet<String> = table.columns.iter().map(|c| folded(&c.name)).collect();
    let extra = relation
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .filter(|name| {
            !declared.contains(*name)
                && !owned.column(name)
                && !name.starts_with(ARCHIVE_PREFIX)
                && !pending.column(&table.name, name)
        })
        .map(|name| (table.name.clone(), name.to_string()))
        .collect();
    (drift, extra)
}

pub fn reconcile_with_live(
    changes: Vec<SchemaChange>,
    declared: &[TableDefinition],
    live: &LiveSchema,
) -> GuardResult {
    let pending = Pending::of(&changes);
    let (standing_drift, extra_columns): (Vec<_>, Vec<_>) = declared
        .iter()
        .filter(|table| !pending.tables.contains(&table.name))
        .map(|table| standing(table, live, &pending))
        .unzip();
    let changed = changes
        .into_iter()
        .flat_map(|change| verdicts(change, declared, live))
        .fold(GuardResult::default(), |mut result, (change, verdict)| {
            match verdict {
                Verdict::Run => result.run.push(change),
                Verdict::Satisfied => result.satisfied.push(change),
                Verdict::Drift(drift) => result.drift.push(drift),
            }
            result
        });
    GuardResult {
        drift: changed
            .drift
            .into_iter()
            .chain(standing_drift.into_iter().flatten())
            .fold(Vec::new(), |mut drift, found| {
                if !drift.contains(&found) {
                    drift.push(found);
                }
                drift
            }),
        extra_columns: extra_columns.into_iter().flatten().collect(),
        ..changed
    }
}

pub fn drop_targets(changes: &[SchemaChange]) -> BTreeSet<DropTarget> {
    changes
        .iter()
        .filter_map(|change| match change {
            SchemaChange::DropTable(table) => Some(DropTarget::Table(table.clone())),
            SchemaChange::DropColumn { table, column_name } => Some(DropTarget::Column {
                table: table.clone(),
                column: column_name.clone(),
            }),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Dependent {
    pub on: String,
    pub kind: DependentKind,
    pub name: String,
    pub description: String,
}

impl std::fmt::Display for Dependent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            DependentKind::View | DependentKind::MaterializedView => {
                write!(f, "{} {}", self.kind.as_str(), self.name)
            }
            _ => f.write_str(&self.description),
        }?;
        write!(f, " depends on {}", self.on)
    }
}

pub fn unmanaged(
    found: Vec<Found>,
    schema: &str,
    owned: &BTreeMap<String, OwnedObjects>,
) -> Vec<Dependent> {
    found
        .into_iter()
        .filter(|dependent| {
            !owned
                .get(dependent.target.table())
                .is_some_and(|objects| objects.owns(dependent))
        })
        .map(|dependent| Dependent {
            on: dependent.target.named_in(schema),
            kind: dependent.kind,
            name: dependent.name,
            description: dependent.description,
        })
        .collect()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct GuardReport {
    #[serde(rename = "schema_guard")]
    pub mode: PolicyMode,
    pub satisfied: Vec<String>,
    pub drift: Vec<Drift>,
    pub dependents: Vec<Dependent>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub extra_columns: Vec<String>,
}

impl GuardReport {
    pub fn of(
        mode: PolicyMode,
        result: &GuardResult,
        dependents: Vec<Dependent>,
        accept_extra_columns: bool,
    ) -> Self {
        let extra = result
            .extra_columns
            .iter()
            .map(|(table, column)| Drift::ExtraColumn {
                table: table.clone(),
                column: column.clone(),
            });
        Self {
            mode,
            satisfied: result
                .satisfied
                .iter()
                .map(SchemaChange::describe)
                .collect(),
            drift: result
                .drift
                .iter()
                .cloned()
                .chain(extra.filter(|_| !accept_extra_columns))
                .collect(),
            dependents,
            extra_columns: result
                .extra_columns
                .iter()
                .map(|(table, column)| format!("{table}.{column}"))
                .collect(),
        }
    }

    pub fn refusal(&self, accept_drift: bool) -> Option<String> {
        let listed = |items: Vec<String>| items.join("; ");
        if !self.dependents.is_empty() {
            return Some(format!(
                "schema_guard: the deploy would drop what objects outside the manifest depend on: \
                 {}. Drop or redefine them in an SQL migration first",
                listed(self.dependents.iter().map(ToString::to_string).collect())
            ));
        }
        (!self.drift.is_empty() && !accept_drift).then(|| {
            format!(
                "schema_guard: the database is not what the manifest expects: {}. \
                 Bring them together, or deploy with --accept-drift to leave what differs as it is",
                listed(self.drift.iter().map(ToString::to_string).collect())
            )
        })
    }

    pub fn record(&self) {
        [
            ("satisfied", self.satisfied.len()),
            ("drift", self.drift.len()),
            ("dependent", self.dependents.len()),
        ]
        .into_iter()
        .filter(|(_, found)| *found > 0)
        .for_each(|(kind, found)| {
            metrics::counter!("cufflink_schema_guard_findings_total", "kind" => kind)
                .increment(found as u64)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::super::introspect::fixtures::{column, index, schema, table as live_table};
    use super::super::introspect::{LiveColumn, LiveConstraint};
    use super::*;
    use crate::deployer::manifest::{ColumnType, IndexMethod};

    fn declared_column(name: &str, column_type: ColumnType) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type,
            ..Default::default()
        }
    }

    fn sailings() -> TableDefinition {
        TableDefinition {
            name: "sailings".into(),
            columns: vec![
                ColumnDefinition {
                    primary_key: true,
                    ..declared_column("id", ColumnType::Uuid)
                },
                declared_column("notes", ColumnType::Text),
            ],
            ..Default::default()
        }
    }

    fn live_sailings(extra: Vec<LiveColumn>) -> LiveSchema {
        schema(vec![(
            "sailings",
            live_table(
                [column("id", "uuid"), column("notes", "text")]
                    .into_iter()
                    .chain(extra)
                    .collect(),
            ),
        )])
    }

    fn reconcile(changes: Vec<SchemaChange>, live: &LiveSchema) -> GuardResult {
        reconcile_with_live(changes, &[sailings()], live)
    }

    fn described(changes: &[SchemaChange]) -> Vec<String> {
        changes.iter().map(SchemaChange::describe).collect()
    }

    fn drop_column(name: &str) -> SchemaChange {
        SchemaChange::DropColumn {
            table: "sailings".into(),
            column_name: name.into(),
        }
    }

    fn add_column(name: &str, column_type: ColumnType) -> SchemaChange {
        SchemaChange::AddColumn {
            table: "sailings".into(),
            column: declared_column(name, column_type),
        }
    }

    fn by_date() -> IndexDefinition {
        IndexDefinition {
            name: "sailings_by_date".into(),
            columns: vec!["notes".into(), "id".into()],
            ..Default::default()
        }
    }

    #[test]
    fn a_table_that_is_there_with_its_columns_is_satisfied_and_its_indexes_are_made() {
        let table = TableDefinition {
            indexes: vec![by_date()],
            ..sailings()
        };
        let create = || vec![SchemaChange::CreateTable(Box::new(table.clone()))];
        let absent = reconcile_with_live(
            create(),
            std::slice::from_ref(&table),
            &LiveSchema::default(),
        );
        assert_eq!(described(&absent.run), ["CREATE TABLE sailings"]);
        assert!(absent.drift.is_empty());

        let present = reconcile_with_live(
            create(),
            std::slice::from_ref(&table),
            &live_sailings(vec![]),
        );
        assert_eq!(described(&present.satisfied), ["CREATE TABLE sailings"]);
        assert_eq!(
            described(&present.run),
            ["CREATE INDEX sailings_by_date ON sailings"]
        );
        assert!(present.drift.is_empty());
    }

    #[test]
    fn a_table_that_is_there_without_a_declared_column_is_drift() {
        let live = schema(vec![("sailings", live_table(vec![column("id", "uuid")]))]);
        let result = reconcile(vec![SchemaChange::CreateTable(Box::new(sailings()))], &live);
        assert!(result.run.is_empty() && result.satisfied.is_empty());
        assert_eq!(
            result.drift,
            [Drift::ColumnMissing {
                table: "sailings".into(),
                column: "notes".into()
            }]
        );
        let view = schema(vec![("sailings", LiveRelation::of(RelationKind::View))]);
        assert_eq!(
            reconcile(vec![SchemaChange::CreateTable(Box::new(sailings()))], &view).drift,
            [Drift::NotATable {
                table: "sailings".into()
            }]
        );
    }

    #[test]
    fn a_column_that_is_there_is_satisfied_when_it_holds_the_declared_type() {
        let live = live_sailings(vec![column("weight", "integer")]);
        let same = reconcile(vec![add_column("weight", ColumnType::Integer)], &live);
        assert_eq!(described(&same.satisfied), ["ADD COLUMN sailings.weight"]);
        assert!(same.run.is_empty() && same.drift.is_empty());

        let other = reconcile(vec![add_column("weight", ColumnType::Double)], &live);
        assert!(other.run.is_empty() && other.satisfied.is_empty());
        assert_eq!(
            other.drift,
            [Drift::ColumnType {
                table: "sailings".into(),
                column: "weight".into(),
                declared: "DOUBLE PRECISION".into(),
                live: "integer".into()
            }]
        );

        let absent = reconcile(
            vec![add_column("weight", ColumnType::Integer)],
            &live_sailings(vec![]),
        );
        assert_eq!(described(&absent.run), ["ADD COLUMN sailings.weight"]);
    }

    #[test]
    fn a_column_dropped_by_hand_is_satisfied() {
        let result = reconcile(
            vec![drop_column("boarding_photo_urls")],
            &live_sailings(vec![]),
        );
        assert_eq!(
            described(&result.satisfied),
            ["DROP COLUMN sailings.boarding_photo_urls"]
        );
        assert!(result.run.is_empty());
        assert!(result.drift.is_empty());
        assert!(result.extra_columns.is_empty());
    }

    #[test]
    fn what_is_absent_is_not_dropped_and_what_is_there_is() {
        let live = schema(vec![(
            "sailings",
            LiveRelation {
                indexes: vec![index("sailings_by_date", &["notes", "id"])],
                ..live_table(vec![
                    column("id", "uuid"),
                    column("notes", "text"),
                    column("midpoint", "text"),
                ])
            },
        )]);
        let drop_index = |name: &str| SchemaChange::DropIndex {
            table: "sailings".into(),
            index_name: name.into(),
        };
        let archive = |name: &str| SchemaChange::ArchiveColumn {
            table: "sailings".into(),
            column_name: name.into(),
            archive_name: format!("_archived_v3_{name}"),
        };
        let result = reconcile(
            vec![
                drop_column("midpoint"),
                SchemaChange::DropTable("batches".into()),
                drop_index("sailings_by_date"),
                drop_index("sailings_by_zone"),
                archive("midpoint"),
                archive("gone"),
                SchemaChange::ArchiveTable {
                    table_name: "batches".into(),
                    archive_name: "_archived_v3_batches".into(),
                },
            ],
            &live,
        );
        assert_eq!(
            described(&result.run),
            [
                "DROP COLUMN sailings.midpoint",
                "DROP INDEX sailings_by_date",
                "ARCHIVE COLUMN sailings.midpoint AS _archived_v3_midpoint"
            ]
        );
        assert_eq!(
            described(&result.satisfied),
            [
                "DROP TABLE batches",
                "DROP INDEX sailings_by_zone",
                "ARCHIVE COLUMN sailings.gone AS _archived_v3_gone",
                "ARCHIVE TABLE batches AS _archived_v3_batches"
            ]
        );
        assert!(result.drift.is_empty());
        assert!(result.extra_columns.is_empty());
    }

    #[test]
    fn a_type_or_nullability_the_database_has_already_is_satisfied() {
        let table = TableDefinition {
            columns: vec![
                declared_column("id", ColumnType::Uuid),
                ColumnDefinition {
                    nullable: true,
                    ..declared_column("notes", ColumnType::Text)
                },
                declared_column("weight", ColumnType::Double),
            ],
            ..sailings()
        };
        let alter = || SchemaChange::AlterColumnType {
            table: "sailings".into(),
            column_name: "weight".into(),
            old_type: "INTEGER".into(),
            new_type: "DOUBLE PRECISION".into(),
        };
        let nullable = |nullable| SchemaChange::AlterNullability {
            table: "sailings".into(),
            column_name: "notes".into(),
            nullable,
        };
        let live = |weight: &str, not_null| {
            schema(vec![(
                "sailings",
                live_table(vec![
                    column("id", "uuid"),
                    LiveColumn {
                        not_null,
                        ..column("notes", "text")
                    },
                    column("weight", weight),
                ]),
            )])
        };
        let done = reconcile_with_live(
            vec![alter(), nullable(true)],
            std::slice::from_ref(&table),
            &live("double precision", false),
        );
        assert_eq!(done.satisfied.len(), 2);
        assert!(done.run.is_empty() && done.drift.is_empty());

        let due = reconcile_with_live(
            vec![alter(), nullable(true)],
            std::slice::from_ref(&table),
            &live("integer", true),
        );
        assert_eq!(due.run.len(), 2);
        assert!(due.satisfied.is_empty() && due.drift.is_empty());

        let tighten = reconcile_with_live(
            vec![nullable(false)],
            &[table],
            &live("double precision", true),
        );
        assert_eq!(
            described(&tighten.satisfied),
            ["SET NOT NULL sailings.notes"]
        );
    }

    #[test]
    fn an_index_of_the_declared_name_is_satisfied_only_when_it_is_the_declared_index() {
        let create = |declared: IndexDefinition| {
            vec![SchemaChange::CreateIndex {
                table: "sailings".into(),
                index: declared,
            }]
        };
        let with = |found| {
            schema(vec![(
                "sailings",
                LiveRelation {
                    indexes: vec![found],
                    ..live_table(vec![column("id", "uuid"), column("notes", "text")])
                },
            )])
        };
        let same = reconcile(
            create(by_date()),
            &with(index("sailings_by_date", &["notes", "id"])),
        );
        assert_eq!(same.satisfied.len(), 1);

        let other_columns = reconcile(
            create(by_date()),
            &with(LiveIndex {
                definition: "CREATE INDEX sailings_by_date ON sailings USING btree (id)".into(),
                ..index("sailings_by_date", &["id"])
            }),
        );
        assert_eq!(
            other_columns.drift,
            [Drift::IndexDiffers {
                table: "sailings".into(),
                index: "sailings_by_date".into(),
                live: "CREATE INDEX sailings_by_date ON sailings USING btree (id)".into()
            }]
        );

        let shaped = IndexDefinition {
            unique: true,
            method: IndexMethod::Btree,
            predicate: Some("notes IS NOT NULL".into()),
            columns: vec!["order".into()],
            ..by_date()
        };
        let quoted = LiveIndex {
            unique: true,
            partial: true,
            ..index("sailings_by_date", &["\"order\""])
        };
        assert_eq!(
            reconcile(create(shaped.clone()), &with(quoted.clone()))
                .satisfied
                .len(),
            1
        );
        for different in [
            LiveIndex {
                unique: false,
                ..quoted.clone()
            },
            LiveIndex {
                partial: false,
                ..quoted.clone()
            },
            LiveIndex {
                method: "gin".into(),
                ..quoted
            },
        ] {
            assert_eq!(
                reconcile(create(shaped.clone()), &with(different))
                    .drift
                    .len(),
                1
            );
        }
        assert_eq!(
            reconcile(create(by_date()), &live_sailings(vec![]))
                .run
                .len(),
            1
        );
    }

    fn live_check(name: &str, definition: &str) -> LiveConstraint {
        LiveConstraint {
            name: name.into(),
            kind: "c".into(),
            columns: vec!["weight".into()],
            definition: definition.into(),
        }
    }

    fn check(name: &str, expr: &str) -> CheckConstraint {
        CheckConstraint {
            name: name.into(),
            expr: expr.into(),
        }
    }

    fn add_check(name: &str, expr: &str) -> SchemaChange {
        SchemaChange::AddCheck {
            table: "sailings".into(),
            check: check(name, expr),
        }
    }

    #[test]
    fn a_check_is_there_by_its_condition_and_a_replaced_index_by_its_shape() {
        let live = schema(vec![(
            "sailings",
            LiveRelation {
                indexes: vec![index("sailings_by_date", &["notes", "id"])],
                constraints: vec![
                    live_check("sailings_weight_check", "CHECK ((weight > 0))"),
                    live_check("sailings_known_check", "CHECK (known(weight))"),
                    live_check("sailings_late_check", "CHECK ((weight > 0)) NOT VALID"),
                    primary_key_named("sailings_key_check"),
                ],
                ..live_table(vec![column("id", "uuid"), column("notes", "text")])
            },
        )]);
        let result = reconcile(
            vec![
                add_check("sailings_weight_check", " weight > 0 "),
                add_check("sailings_known_check", "known(weight)"),
                add_check("sailings_notes_check", "weight > 0"),
                SchemaChange::DropCheck {
                    table: "sailings".into(),
                    name: "sailings_weight_check".into(),
                },
                SchemaChange::DropCheck {
                    table: "sailings".into(),
                    name: "sailings_zone_check".into(),
                },
                SchemaChange::ReplaceIndex {
                    table: "sailings".into(),
                    index: by_date(),
                },
                SchemaChange::ReplaceIndex {
                    table: "sailings".into(),
                    index: IndexDefinition {
                        unique: true,
                        ..by_date()
                    },
                },
            ],
            &live,
        );
        assert_eq!(
            described(&result.satisfied),
            [
                "ADD CHECK sailings_weight_check ON sailings",
                "ADD CHECK sailings_known_check ON sailings",
                "DROP CHECK sailings_zone_check ON sailings",
                "REPLACE INDEX sailings_by_date ON sailings",
            ]
        );
        assert_eq!(
            described(&result.run),
            [
                "ADD CHECK sailings_notes_check ON sailings",
                "DROP CHECK sailings_weight_check ON sailings",
                "REPLACE INDEX sailings_by_date ON sailings",
            ]
        );
        assert!(result.drift.is_empty());

        let changed = reconcile(
            vec![
                add_check("sailings_weight_check", "weight > 10"),
                add_check("sailings_weight_check", "weight > 0 AND weight < 9"),
                add_check("sailings_late_check", "weight > 0"),
                add_check("sailings_key_check", "weight > 0"),
            ],
            &live,
        );
        assert_eq!(
            changed.run.len(),
            4,
            "another condition, one Postgres keeps in other words, one not validated and a \
             constraint that is no check are all added: {:?}",
            described(&changed.satisfied)
        );
        assert!(changed.satisfied.is_empty() && changed.drift.is_empty());
    }

    fn primary_key_named(name: &str) -> LiveConstraint {
        LiveConstraint {
            kind: "p".into(),
            ..live_check(name, "CHECK ((weight > 0))")
        }
    }

    #[test]
    fn a_column_that_is_there_and_not_yet_not_null_is_still_to_be_finished() {
        let nullable_live = || {
            live_sailings(vec![LiveColumn {
                not_null: false,
                ..column("weight", "integer")
            }])
        };
        let required = reconcile(
            vec![add_column("weight", ColumnType::Integer)],
            &nullable_live(),
        );
        assert_eq!(
            described(&required.run),
            ["ADD COLUMN sailings.weight"],
            "the deploy that failed after it added the column sets NOT NULL when it is run again"
        );
        assert!(required.satisfied.is_empty() && required.drift.is_empty());

        let optional = reconcile(
            vec![SchemaChange::AddColumn {
                table: "sailings".into(),
                column: ColumnDefinition {
                    nullable: true,
                    ..declared_column("weight", ColumnType::Integer)
                },
            }],
            &nullable_live(),
        );
        assert_eq!(
            described(&optional.satisfied),
            ["ADD COLUMN sailings.weight"]
        );

        let other = reconcile(
            vec![add_column("weight", ColumnType::Double)],
            &nullable_live(),
        );
        assert!(other.run.is_empty(), "a column of another type is drift");
        assert_eq!(other.drift.len(), 1);
    }

    fn searched() -> TableDefinition {
        TableDefinition {
            search: Some(cufflink_types::SearchSpec {
                columns: vec!["notes".into()],
                ..Default::default()
            }),
            ..sailings()
        }
    }

    fn vector(type_name: &str, generated: bool) -> LiveColumn {
        LiveColumn {
            generated,
            not_null: false,
            ..column("_search", type_name)
        }
    }

    fn live_searched(vector: Option<LiveColumn>, indexes: Vec<LiveIndex>) -> LiveSchema {
        schema(vec![(
            "sailings",
            LiveRelation {
                indexes,
                ..live_table(
                    [column("id", "uuid"), column("notes", "text")]
                        .into_iter()
                        .chain(vector)
                        .collect(),
                )
            },
        )])
    }

    fn search_index(valid: bool) -> LiveIndex {
        LiveIndex {
            valid,
            method: "gin".into(),
            ..index("sailings__search_gin", &["_search"])
        }
    }

    #[test]
    fn a_table_that_is_there_without_its_search_vector_is_given_one() {
        let table = searched();
        let create = |live: &LiveSchema| {
            reconcile_with_live(
                vec![SchemaChange::CreateTable(Box::new(table.clone()))],
                std::slice::from_ref(&table),
                live,
            )
        };
        for (live, why) in [
            (live_searched(None, vec![]), "no vector"),
            (
                live_searched(Some(vector("tsvector", true)), vec![]),
                "no index",
            ),
            (
                live_searched(Some(vector("tsvector", true)), vec![search_index(false)]),
                "an index whose build was interrupted",
            ),
        ] {
            let result = create(&live);
            assert_eq!(
                described(&result.satisfied),
                ["CREATE TABLE sailings"],
                "{why}"
            );
            assert_eq!(described(&result.run), ["SET SEARCH ON sailings"], "{why}");
            assert!(result.drift.is_empty(), "{why}");
        }
        let there = create(&live_searched(
            Some(vector("tsvector", true)),
            vec![search_index(true)],
        ));
        assert_eq!(
            described(&there.satisfied),
            ["CREATE TABLE sailings", "SET SEARCH ON sailings"]
        );
        assert!(there.run.is_empty() && there.drift.is_empty());
    }

    #[test]
    fn a_column_of_the_vectors_name_that_the_platform_did_not_make_is_drift() {
        let table = searched();
        let set = |spec: Option<cufflink_types::SearchSpec>| SchemaChange::Search {
            table: "sailings".into(),
            spec,
        };
        for (type_name, generated) in [("text", false), ("tsvector", false), ("text", true)] {
            let live = live_searched(Some(vector(type_name, generated)), vec![]);
            for change in [
                set(table.search.clone()),
                set(None),
                SchemaChange::CreateTable(Box::new(table.clone())),
            ] {
                let result = reconcile_with_live(vec![change], std::slice::from_ref(&table), &live);
                assert!(result.run.is_empty(), "{type_name} {generated}");
                assert_eq!(
                    result.drift,
                    [Drift::NotASearchVector {
                        table: "sailings".into(),
                        column: "_search".into(),
                        live: type_name.into(),
                    }],
                    "{type_name} {generated}"
                );
            }
        }
        assert_eq!(
            Drift::NotASearchVector {
                table: "sailings".into(),
                column: "_search".into(),
                live: "text".into(),
            }
            .to_string(),
            "column sailings._search (text) is not the generated tsvector the platform keeps the \
             search vector of sailings in"
        );
    }

    #[test]
    fn a_search_that_changes_is_remade_and_one_that_is_gone_already_is_not_dropped() {
        let table = searched();
        let kept = live_searched(Some(vector("tsvector", true)), vec![search_index(true)]);
        let none = live_searched(None, vec![]);
        let verdict = |spec: Option<cufflink_types::SearchSpec>, live: &LiveSchema| {
            let change = SchemaChange::Search {
                table: "sailings".into(),
                spec,
            };
            let result = reconcile_with_live(vec![change], std::slice::from_ref(&table), live);
            assert!(result.drift.is_empty());
            (described(&result.run), described(&result.satisfied))
        };
        let set = vec!["SET SEARCH ON sailings".to_string()];
        let dropped = vec!["DROP SEARCH ON sailings".to_string()];
        assert_eq!(verdict(table.search.clone(), &kept), (set.clone(), vec![]));
        assert_eq!(verdict(table.search.clone(), &none), (set, vec![]));
        assert_eq!(verdict(None, &kept), (dropped.clone(), vec![]));
        assert_eq!(verdict(None, &none), (vec![], dropped));
        let missing = reconcile_with_live(
            vec![SchemaChange::Search {
                table: "routes".into(),
                spec: None,
            }],
            &[],
            &none,
        );
        assert_eq!(
            missing.drift,
            [Drift::TableMissing {
                table: "routes".into()
            }]
        );
    }

    #[test]
    fn a_table_no_change_touches_is_compared_with_the_database() {
        let missing = reconcile(vec![], &LiveSchema::default());
        assert_eq!(
            missing.drift,
            [Drift::TableMissing {
                table: "sailings".into()
            }]
        );
        let live = schema(vec![(
            "sailings",
            live_table(vec![column("id", "uuid"), column("notes", "jsonb")]),
        )]);
        assert_eq!(
            reconcile(vec![], &live).drift,
            [Drift::ColumnType {
                table: "sailings".into(),
                column: "notes".into(),
                declared: "TEXT".into(),
                live: "jsonb".into()
            }]
        );
        let hand_dropped = schema(vec![("sailings", live_table(vec![column("id", "uuid")]))]);
        assert_eq!(
            reconcile(vec![], &hand_dropped).drift,
            [Drift::ColumnMissing {
                table: "sailings".into(),
                column: "notes".into()
            }]
        );
        assert!(reconcile(vec![], &live_sailings(vec![])).drift.is_empty());
    }

    #[test]
    fn a_column_the_manifest_does_not_declare_is_drift_only_where_it_is_not_accepted() {
        let soft = TableDefinition {
            soft_delete: true,
            ..sailings()
        };
        let live = live_sailings(vec![
            column("deleted_at", "timestamp with time zone"),
            column("_archived_v3_midpoint", "text"),
            column("legacy_ref", "text"),
            column("going", "text"),
        ]);
        let result = reconcile_with_live(vec![drop_column("going")], &[soft], &live);
        assert!(result.drift.is_empty());
        assert_eq!(
            result.extra_columns,
            [("sailings".to_string(), "legacy_ref".to_string())]
        );

        let accepted = GuardReport::of(PolicyMode::Enforce, &result, vec![], true);
        assert!(accepted.drift.is_empty());
        assert_eq!(accepted.extra_columns, ["sailings.legacy_ref"]);
        assert_eq!(accepted.refusal(false), None);

        let refused = GuardReport::of(PolicyMode::Enforce, &result, vec![], false);
        assert_eq!(
            refused.drift,
            [Drift::ExtraColumn {
                table: "sailings".into(),
                column: "legacy_ref".into()
            }]
        );
        assert!(refused.refusal(false).is_some());
        assert_eq!(refused.refusal(true), None);
    }

    fn found(kind: DependentKind, name: &str, on_target: bool, column: Option<&str>) -> Found {
        Found {
            target: match column {
                Some(column) => DropTarget::Column {
                    table: "sailings".into(),
                    column: column.into(),
                },
                None => DropTarget::Table("sailings".into()),
            },
            kind,
            name: name.into(),
            on_target,
            description: format!("{} {name}", kind.as_str()),
        }
    }

    #[test]
    fn only_what_the_manifest_declared_is_owned() {
        let table = TableDefinition {
            indexes: vec![by_date()],
            checks: vec![CheckConstraint {
                name: "sailings_weight_check".into(),
                expr: "weight > 0".into(),
            }],
            columns: vec![
                ColumnDefinition {
                    primary_key: true,
                    ..declared_column("id", ColumnType::Uuid)
                },
                ColumnDefinition {
                    unique: true,
                    ..declared_column("reference", ColumnType::Text)
                },
                ColumnDefinition {
                    references: Some("customers.id".into()),
                    ..declared_column("customer_id", ColumnType::Uuid)
                },
            ],
            ..sailings()
        };
        let owned = owned_by([&table]);
        let all = vec![
            found(DependentKind::Index, "sailings_by_date", true, None),
            found(DependentKind::Index, "sailings_pkey", true, None),
            found(DependentKind::Index, "sailings_reference_key", true, None),
            found(DependentKind::Constraint, "sailings_pkey", true, None),
            found(
                DependentKind::Constraint,
                "sailings_reference_key",
                true,
                None,
            ),
            found(
                DependentKind::Constraint,
                "sailings_customer_id_fkey",
                true,
                None,
            ),
            found(
                DependentKind::Constraint,
                "sailings_weight_check",
                true,
                None,
            ),
            found(DependentKind::Index, "scheduled_sailings_due", true, None),
            found(
                DependentKind::Constraint,
                "items_condition_check",
                true,
                None,
            ),
            found(
                DependentKind::Constraint,
                "sailings_customer_id_fkey",
                false,
                None,
            ),
            found(DependentKind::Trigger, "touch_sailings", true, None),
            found(
                DependentKind::View,
                "reporting.sailing_overview",
                false,
                Some("notes"),
            ),
            found(
                DependentKind::MaterializedView,
                "reporting.sailing_totals",
                false,
                None,
            ),
            found(DependentKind::Policy, "tenant_rows", true, None),
            found(DependentKind::Rule, "log_sailings", true, None),
            found(DependentKind::Sequence, "sailings_seq", false, None),
            found(DependentKind::Index, "sailings_by_date", false, None),
        ];
        let unmanaged: Vec<String> = unmanaged(all, "ferry_service", &owned)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            unmanaged,
            [
                "index scheduled_sailings_due depends on ferry_service.sailings",
                "constraint items_condition_check depends on ferry_service.sailings",
                "constraint sailings_customer_id_fkey depends on ferry_service.sailings",
                "trigger touch_sailings depends on ferry_service.sailings",
                "view reporting.sailing_overview depends on ferry_service.sailings.notes",
                "materialized view reporting.sailing_totals depends on ferry_service.sailings",
                "policy tenant_rows depends on ferry_service.sailings",
                "rule log_sailings depends on ferry_service.sailings",
                "sequence sailings_seq depends on ferry_service.sailings",
                "index sailings_by_date depends on ferry_service.sailings",
            ]
        );
    }

    #[test]
    fn a_search_vector_and_its_index_are_the_platforms() {
        let searched = searched();
        let owned = owned_by([&searched]);
        assert!(owned["sailings"].column("_search"));
        assert!(owned["sailings"].owns(&found(
            DependentKind::Index,
            "sailings__search_gin",
            true,
            None
        )));
        assert!(owned["sailings"].owns(&found(
            DependentKind::GeneratedColumn,
            "_search",
            true,
            Some("notes")
        )));
        let live = live_sailings(vec![column("_search", "tsvector")]);
        let result = reconcile_with_live(vec![], std::slice::from_ref(&searched), &live);
        assert!(result.extra_columns.is_empty());
        assert!(result.drift.is_empty());
    }

    #[test]
    fn a_table_defined_twice_owns_what_either_definition_does() {
        let before = TableDefinition {
            indexes: vec![by_date()],
            ..sailings()
        };
        let owned = owned_by([&before, &sailings()]);
        assert!(owned["sailings"].owns(&found(
            DependentKind::Index,
            "sailings_by_date",
            true,
            None
        )));
        assert!(owned_by([&TableDefinition {
            soft_delete: true,
            ..sailings()
        }])["sailings"]
            .column("deleted_at"));
    }

    #[test]
    fn only_drops_have_targets() {
        let targets = drop_targets(&[
            drop_column("midpoint"),
            SchemaChange::DropTable("batches".into()),
            add_column("weight", ColumnType::Integer),
            SchemaChange::ArchiveTable {
                table_name: "routes".into(),
                archive_name: "_archived_v3_routes".into(),
            },
        ]);
        assert_eq!(
            targets.into_iter().collect::<Vec<_>>(),
            [
                DropTarget::Table("batches".into()),
                DropTarget::Column {
                    table: "sailings".into(),
                    column: "midpoint".into()
                },
            ]
        );
    }

    #[test]
    fn a_dependent_refuses_a_deploy_whatever_it_accepts() {
        let report = GuardReport {
            mode: PolicyMode::Enforce,
            dependents: vec![Dependent {
                on: "catalog_service.book.status".into(),
                kind: DependentKind::View,
                name: "reporting.book_overview".into(),
                description: "rule _RETURN on view reporting.book_overview".into(),
            }],
            ..Default::default()
        };
        let refusal = report.refusal(true).expect("refused");
        assert!(
            refusal.contains("view reporting.book_overview depends on catalog_service.book.status"),
            "{refusal}"
        );
        assert!(refusal.contains("SQL migration"), "{refusal}");
        assert_eq!(GuardReport::default().refusal(false), None);
    }

    #[test]
    fn the_report_serialises_what_a_deploy_answers() {
        let report = GuardReport {
            mode: PolicyMode::Warn,
            satisfied: vec!["DROP COLUMN sailings.midpoint".into()],
            drift: vec![Drift::ColumnMissing {
                table: "sailings".into(),
                column: "notes".into(),
            }],
            dependents: vec![Dependent {
                on: "catalog_service.book.status".into(),
                kind: DependentKind::MaterializedView,
                name: "reporting.book_totals".into(),
                description: "rule _RETURN on materialized view reporting.book_totals".into(),
            }],
            extra_columns: vec![],
        };
        assert_eq!(
            serde_json::to_string(&report).expect("json"),
            r#"{"schema_guard":"warn","satisfied":["DROP COLUMN sailings.midpoint"],"drift":[{"kind":"column_missing","table":"sailings","column":"notes"}],"dependents":[{"on":"catalog_service.book.status","kind":"materialized_view","name":"reporting.book_totals","description":"rule _RETURN on materialized view reporting.book_totals"}]}"#
        );
    }
}
