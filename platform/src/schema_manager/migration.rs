use super::diff::{kept_search, SchemaChange};
use super::introspect::{Archived, ARCHIVE_COMMENT, ARCHIVE_PREFIX};
use crate::deployer::manifest::{
    CheckConstraint, ColumnDefinition, ForeignKeyAction, IndexDefinition, SearchSpec,
    TableDefinition,
};
use crate::router::query::search::{search_index_name, SEARCH_COLUMN};
use crate::wasm_runtime::sha256_hex;
use cufflink_types::manifest::validate::{folded, folded_ident as ident, IDENTIFIER_MAX};

pub(super) const SOFT_DELETE_COLUMN: &str = "deleted_at";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    Additive,
    Destructive,
    Online,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drops {
    Legacy,
    Restrict,
    Archive { version: i32 },
}

fn clipped(name: &str, bytes: usize) -> &str {
    let end = (0..=bytes.min(name.len()))
        .rev()
        .find(|at| name.is_char_boundary(*at))
        .unwrap_or(0);
    &name[..end]
}

const ARCHIVE_DIGEST: usize = 6;

pub fn archive_name(version: i32, name: &str) -> String {
    let name = folded(name);
    let prefix = format!("{ARCHIVE_PREFIX}{version}_");
    if prefix.len() + name.len() <= IDENTIFIER_MAX {
        return format!("{prefix}{name}");
    }
    let room = IDENTIFIER_MAX.saturating_sub(prefix.len() + ARCHIVE_DIGEST + 1);
    format!(
        "{prefix}{}_{}",
        clipped(&name, room),
        &sha256_hex(name.as_bytes())[..ARCHIVE_DIGEST]
    )
}

pub(super) fn object_name(relation: &str, column: Option<&str>, label: &str) -> String {
    let (relation, column) = (folded(relation), column.map(folded));
    let separators = 1 + usize::from(column.is_some());
    let available = IDENTIFIER_MAX.saturating_sub(label.len() + separators);
    let fit = |name: &str| clipped(name, available).len();
    let lengths = (fit(&relation), column.as_deref().map_or(0, fit));
    let excess = (lengths.0 + lengths.1).saturating_sub(available);
    let (first, second) = (0..excess).fold(lengths, |(first, second), _| {
        if first > second {
            (first - 1, second)
        } else {
            (first, second - 1)
        }
    });
    [
        Some(clipped(&relation, first)),
        column.as_deref().map(|column| clipped(column, second)),
        Some(label),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("_")
}

fn archive_comment(archive_name: &str, name: &str) -> Option<String> {
    let named = Archived::parse(archive_name, None, None)
        .is_some_and(|archived| archived.original == folded(name));
    (!named).then(|| format!("'{ARCHIVE_COMMENT}{}'", folded(name).replace('\'', "''")))
}

fn relation(schema: &str, table: &str) -> String {
    format!("{schema}.{}", ident(table))
}

fn archive_table_sql(schema: &str, table: &str, archive_name: &str) -> Vec<String> {
    std::iter::once(format!(
        "ALTER TABLE {} RENAME TO {}",
        relation(schema, table),
        ident(archive_name)
    ))
    .chain(archive_comment(archive_name, table).map(|comment| {
        format!(
            "COMMENT ON TABLE {} IS {comment}",
            relation(schema, archive_name)
        )
    }))
    .collect()
}

fn archive_column_sql(schema: &str, table: &str, column: &str, archive_name: &str) -> Vec<String> {
    let (table, archived) = (relation(schema, table), ident(archive_name));
    [
        format!(
            "ALTER TABLE {table} RENAME COLUMN {} TO {archived}",
            ident(column)
        ),
        format!("ALTER TABLE {table} ALTER COLUMN {archived} DROP NOT NULL"),
    ]
    .into_iter()
    .chain(
        archive_comment(archive_name, column)
            .map(|comment| format!("COMMENT ON COLUMN {table}.{archived} IS {comment}")),
    )
    .collect()
}

fn restore_table_sql(schema: &str, archive_name: &str, table: &str) -> Vec<String> {
    std::iter::once(format!(
        "ALTER TABLE {} RENAME TO {}",
        relation(schema, archive_name),
        ident(table)
    ))
    .chain(
        archive_comment(archive_name, table)
            .map(|_| format!("COMMENT ON TABLE {} IS NULL", relation(schema, table))),
    )
    .collect()
}

fn restore_column_sql(schema: &str, table: &str, archive_name: &str, column: &str) -> Vec<String> {
    let (table, restored) = (relation(schema, table), ident(column));
    std::iter::once(format!(
        "ALTER TABLE {table} RENAME COLUMN {} TO {restored}",
        ident(archive_name)
    ))
    .chain(
        archive_comment(archive_name, column)
            .map(|_| format!("COMMENT ON COLUMN {table}.{restored} IS NULL")),
    )
    .collect()
}

pub fn generate_sql_phase(
    schema: &str,
    changes: &[SchemaChange],
    phase: MigrationPhase,
    drops: Drops,
) -> Vec<String> {
    changes
        .iter()
        .flat_map(|change| match phase {
            MigrationPhase::Additive => additive_sql(schema, change),
            MigrationPhase::Destructive => destructive_sql(schema, change, drops),
            MigrationPhase::Online => online_sql(schema, change),
        })
        .collect()
}

fn additive_sql(schema: &str, change: &SchemaChange) -> Vec<String> {
    match change {
        SchemaChange::CreateTable(_)
        | SchemaChange::AddColumn { .. }
        | SchemaChange::CreateIndex { .. }
        | SchemaChange::AddCheck { .. }
        | SchemaChange::DropCheck { .. }
        | SchemaChange::Search { .. }
        | SchemaChange::AlterForeignKey { .. }
        | SchemaChange::ArchiveTable { .. }
        | SchemaChange::ArchiveColumn { .. }
        | SchemaChange::RestoreTable { .. }
        | SchemaChange::RestoreColumn { .. }
        | SchemaChange::AlterNullability { nullable: true, .. } => change_to_sql(schema, change),
        _ => vec![],
    }
}

fn online_sql(schema: &str, change: &SchemaChange) -> Vec<String> {
    match change {
        SchemaChange::Search {
            table,
            spec: Some(_),
        } => vec![search_index_sql(schema, table, Build::Concurrent)],
        _ => vec![],
    }
}

fn set_not_null(schema: &str, table: &str, column: &str) -> String {
    format!(
        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
        relation(schema, table),
        ident(column)
    )
}

fn destructive_sql(schema: &str, change: &SchemaChange, drops: Drops) -> Vec<String> {
    match (change, drops) {
        (SchemaChange::DropTable(table), Drops::Archive { version }) => {
            archive_table_sql(schema, table, &archive_name(version, table))
        }
        (SchemaChange::DropColumn { table, column_name }, Drops::Archive { version }) => {
            archive_column_sql(
                schema,
                table,
                column_name,
                &archive_name(version, column_name),
            )
        }
        (SchemaChange::DropTable(table), Drops::Restrict) => vec![format!(
            "DROP TABLE IF EXISTS {} RESTRICT",
            relation(schema, table)
        )],
        (SchemaChange::DropColumn { table, column_name }, Drops::Restrict) => vec![format!(
            "ALTER TABLE {} DROP COLUMN IF EXISTS {} RESTRICT",
            relation(schema, table),
            ident(column_name)
        )],
        (
            SchemaChange::DropTable(_)
            | SchemaChange::DropColumn { .. }
            | SchemaChange::AlterColumnType { .. }
            | SchemaChange::ReplaceIndex { .. }
            | SchemaChange::DropIndex { .. }
            | SchemaChange::AlterNullability {
                nullable: false, ..
            },
            _,
        ) => change_to_sql(schema, change),
        (
            SchemaChange::AddColumn { table, column }
            | SchemaChange::RestoreColumn { table, column, .. },
            _,
        ) if !column.nullable => vec![set_not_null(schema, table, &column.name)],
        _ => vec![],
    }
}

fn references_sql(schema: &str, column: &ColumnDefinition) -> String {
    column
        .references
        .as_deref()
        .and_then(|reference| reference.split_once('.'))
        .map(|(table, referenced)| {
            let on_delete = column
                .on_delete
                .as_ref()
                .map(|action| format!(" ON DELETE {}", fk_action_sql(action)))
                .unwrap_or_default();
            format!(
                " REFERENCES {}({}){on_delete} DEFERRABLE",
                relation(schema, table),
                ident(referenced)
            )
        })
        .unwrap_or_default()
}

fn add_check_sql(schema: &str, table: &str, check: &CheckConstraint) -> Vec<String> {
    let (table, name) = (relation(schema, table), ident(&check.name));
    vec![
        format!("ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}"),
        format!(
            "ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({}) NOT VALID",
            check.expr
        ),
        format!("ALTER TABLE {table} VALIDATE CONSTRAINT {name}"),
    ]
}

fn search_column_sql(schema: &str, table: &str, spec: Option<&SearchSpec>) -> Vec<String> {
    let (relation, column) = (relation(schema, table), ident(SEARCH_COLUMN));
    let text: Vec<String> = spec
        .iter()
        .flat_map(|spec| &spec.columns)
        .map(|c| format!("coalesce({}::text, '')", ident(c)))
        .collect();
    std::iter::once(format!(
        "ALTER TABLE {relation} DROP COLUMN IF EXISTS {column}"
    ))
    .chain(spec.map(|spec| {
        format!(
            "ALTER TABLE {relation} ADD COLUMN {column} tsvector GENERATED ALWAYS AS \
             (to_tsvector('{}'::regconfig, {})) STORED",
            spec.config,
            text.join(" || ' ' || ")
        )
    }))
    .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Build {
    Transactional,
    Concurrent,
}

fn search_index_sql(schema: &str, table: &str, build: Build) -> String {
    let concurrently = match build {
        Build::Transactional => "",
        Build::Concurrent => "CONCURRENTLY ",
    };
    format!(
        "CREATE INDEX {concurrently}IF NOT EXISTS {} ON {} USING gin ({})",
        ident(&search_index_name(table)),
        relation(schema, table),
        ident(SEARCH_COLUMN)
    )
}

fn change_to_sql(schema: &str, change: &SchemaChange) -> Vec<String> {
    let statement = match change {
        SchemaChange::ArchiveTable {
            table_name,
            archive_name,
        } => return archive_table_sql(schema, table_name, archive_name),
        SchemaChange::ArchiveColumn {
            table,
            column_name,
            archive_name,
        } => return archive_column_sql(schema, table, column_name, archive_name),
        SchemaChange::RestoreTable {
            archive_name,
            restore_name,
        } => return restore_table_sql(schema, archive_name, restore_name),
        SchemaChange::RestoreColumn {
            table,
            archive_name,
            column,
        } => return restore_column_sql(schema, table, archive_name, &column.name),
        SchemaChange::CreateTable(table) => {
            return std::iter::once(create_table_sql(schema, table))
                .chain(
                    table
                        .indexes
                        .iter()
                        .map(|index| create_index_sql(schema, &table.name, index)),
                )
                .chain(
                    table
                        .checks
                        .iter()
                        .flat_map(|check| add_check_sql(schema, &table.name, check)),
                )
                .chain(kept_search(table).into_iter().flat_map(|spec| {
                    search_column_sql(schema, &table.name, Some(spec))
                        .into_iter()
                        .chain([search_index_sql(schema, &table.name, Build::Transactional)])
                }))
                .collect()
        }
        SchemaChange::Search { table, spec } => {
            return search_column_sql(schema, table, spec.as_ref())
        }
        SchemaChange::AddCheck { table, check } => return add_check_sql(schema, table, check),
        SchemaChange::DropCheck { table, name } => format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
            relation(schema, table),
            ident(name)
        ),
        SchemaChange::DropTable(name) => {
            format!("DROP TABLE IF EXISTS {} CASCADE", relation(schema, name))
        }
        SchemaChange::AddColumn { table, column } => {
            let nullable = if column.nullable || column.default_value.is_some() {
                ""
            } else {
                " DEFAULT NULL"
            };
            let default = column
                .default_value
                .as_ref()
                .map(|d| format!(" DEFAULT {}", sql_default(d)))
                .unwrap_or_default();
            let unique = if column.unique { " UNIQUE" } else { "" };
            format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}{default}{nullable}{unique}{}",
                relation(schema, table),
                ident(&column.name),
                column.column_type.to_sql(),
                references_sql(schema, column),
            )
        }
        SchemaChange::DropColumn { table, column_name } => format!(
            "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
            relation(schema, table),
            ident(column_name)
        ),
        SchemaChange::AlterColumnType {
            table,
            column_name,
            new_type,
            ..
        } => {
            let column = ident(column_name);
            format!(
                "ALTER TABLE {} ALTER COLUMN {column} TYPE {new_type} USING {column}::{new_type}",
                relation(schema, table)
            )
        }
        SchemaChange::AlterNullability {
            table,
            column_name,
            nullable: true,
        } => format!(
            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
            relation(schema, table),
            ident(column_name)
        ),
        SchemaChange::AlterNullability {
            table,
            column_name,
            nullable: false,
        } => set_not_null(schema, table, column_name),
        SchemaChange::AlterForeignKey {
            table,
            column_name,
            references,
            on_delete,
        } => {
            let constraint = ident(&object_name(table, Some(column_name), "fkey"));
            let column = ColumnDefinition {
                name: column_name.clone(),
                references: Some(references.clone()),
                on_delete: on_delete.clone(),
                ..Default::default()
            };
            format!(
                "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {constraint}, \
                 ADD CONSTRAINT {constraint} FOREIGN KEY ({}){}",
                relation(schema, table),
                ident(column_name),
                references_sql(schema, &column)
            )
        }
        SchemaChange::CreateIndex { table, index } => create_index_sql(schema, table, index),
        SchemaChange::ReplaceIndex { table, index } => {
            return vec![
                drop_index_sql(schema, &index.name),
                create_index_sql(schema, table, index),
            ]
        }
        SchemaChange::DropIndex { index_name, .. } => drop_index_sql(schema, index_name),
    };
    vec![statement]
}

fn column_sql(schema: &str, column: &ColumnDefinition) -> String {
    let key = match (column.primary_key, column.auto_generate) {
        (true, true) => " PRIMARY KEY DEFAULT gen_random_uuid()",
        (true, false) => " PRIMARY KEY",
        (false, _) if !column.nullable => " NOT NULL",
        (false, _) => "",
    };
    let default = column
        .default_value
        .as_ref()
        .map(|default| format!(" DEFAULT {}", sql_default(default)))
        .unwrap_or_default();
    let generated = if column.auto_generate && !column.primary_key {
        " DEFAULT now()"
    } else {
        ""
    };
    let unique = if column.unique { " UNIQUE" } else { "" };
    format!(
        "{} {}{key}{default}{generated}{unique}{}",
        ident(&column.name),
        column.column_type.to_sql(),
        references_sql(schema, column)
    )
}

fn create_table_sql(schema: &str, table: &TableDefinition) -> String {
    let deleted_at = (table.soft_delete
        && !table.columns.iter().any(|c| c.name == SOFT_DELETE_COLUMN))
    .then(|| format!("{} TIMESTAMPTZ", ident(SOFT_DELETE_COLUMN)));
    let columns: Vec<String> = table
        .columns
        .iter()
        .map(|column| column_sql(schema, column))
        .chain(deleted_at)
        .collect();
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        relation(schema, &table.name),
        columns.join(", ")
    )
}

fn create_index_sql(schema: &str, table: &str, index: &IndexDefinition) -> String {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let columns: Vec<String> = index.columns.iter().map(|column| ident(column)).collect();
    let predicate = index
        .predicate
        .as_ref()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    format!(
        "CREATE {unique}INDEX IF NOT EXISTS {} ON {} USING {} ({}){predicate}",
        ident(&index.name),
        relation(schema, table),
        index.method.as_sql(),
        columns.join(", ")
    )
}

fn drop_index_sql(schema: &str, index: &str) -> String {
    format!("DROP INDEX IF EXISTS {}", relation(schema, index))
}

fn fk_action_sql(action: &ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::SetNull => "SET NULL",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::NoAction => "NO ACTION",
    }
}

fn sql_default(value: &str) -> String {
    if value.contains('(') || value == "true" || value == "false" || value == "NULL" {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployer::manifest::{ColumnType, IndexMethod};

    fn column(name: &str, column_type: ColumnType) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type,
            ..Default::default()
        }
    }

    fn id() -> ColumnDefinition {
        ColumnDefinition {
            primary_key: true,
            auto_generate: true,
            ..column("id", ColumnType::Uuid)
        }
    }

    fn table(name: &str, columns: Vec<ColumnDefinition>) -> TableDefinition {
        TableDefinition {
            name: name.into(),
            columns,
            ..Default::default()
        }
    }

    fn sql(change: SchemaChange) -> Vec<String> {
        change_to_sql("\"svc\"", &change)
    }

    fn phase(changes: &[SchemaChange], phase: MigrationPhase) -> Vec<String> {
        generate_sql_phase("\"svc\"", changes, phase, Drops::Legacy)
    }

    #[test]
    fn a_name_postgres_made_is_shortened_as_postgres_shortens_it() {
        assert_eq!(object_name("sailings", None, "pkey"), "sailings_pkey");
        assert_eq!(
            object_name("Sailings", Some("Email"), "key"),
            "sailings_email_key"
        );
        let long_table = "t".repeat(40);
        let long_column = "c".repeat(40);
        let name = object_name(&long_table, Some(&long_column), "fkey");
        assert_eq!(name, format!("{}_{}_fkey", "t".repeat(29), "c".repeat(28)));
        assert_eq!(name.len(), IDENTIFIER_MAX);
        assert_eq!(
            object_name(&"t".repeat(70), None, "pkey"),
            format!("{}_pkey", "t".repeat(58))
        );
        assert_eq!(
            object_name(&"é".repeat(40), None, "pkey"),
            format!("{}_pkey", "é".repeat(29))
        );
    }

    #[test]
    fn a_table_is_created_with_the_columns_every_release_gave_it() {
        let todos = table(
            "todos",
            vec![
                id(),
                column("title", ColumnType::Text),
                ColumnDefinition {
                    default_value: Some("false".into()),
                    ..column("completed", ColumnType::Boolean)
                },
                ColumnDefinition {
                    default_value: Some("it's new".into()),
                    nullable: true,
                    ..column("status", ColumnType::Text)
                },
                ColumnDefinition {
                    unique: true,
                    ..column("email", ColumnType::Text)
                },
                ColumnDefinition {
                    references: Some("users.id".into()),
                    on_delete: Some(ForeignKeyAction::SetNull),
                    nullable: true,
                    ..column("owner_id", ColumnType::Uuid)
                },
                ColumnDefinition {
                    auto_generate: true,
                    ..column("created_at", ColumnType::Timestamp)
                },
            ],
        );
        assert_eq!(
            create_table_sql("\"svc\"", &todos),
            "CREATE TABLE IF NOT EXISTS \"svc\".\"todos\" (\
             \"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid(), \
             \"title\" TEXT NOT NULL, \
             \"completed\" BOOLEAN NOT NULL DEFAULT false, \
             \"status\" TEXT DEFAULT 'it''s new', \
             \"email\" TEXT NOT NULL UNIQUE, \
             \"owner_id\" UUID REFERENCES \"svc\".\"users\"(\"id\") ON DELETE SET NULL DEFERRABLE, \
             \"created_at\" TIMESTAMPTZ NOT NULL DEFAULT now())"
        );
        let soft = TableDefinition {
            soft_delete: true,
            ..table("posts", vec![id()])
        };
        assert_eq!(
            create_table_sql("\"svc\"", &soft),
            "CREATE TABLE IF NOT EXISTS \"svc\".\"posts\" (\
             \"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid(), \"deleted_at\" TIMESTAMPTZ)"
        );
    }

    #[test]
    fn a_name_is_the_one_postgres_folds_it_to_and_a_keyword_is_a_name() {
        let orders = table(
            "Order",
            vec![
                id(),
                column("User", ColumnType::Text),
                column("group", ColumnType::Integer),
            ],
        );
        assert_eq!(
            create_table_sql("\"svc\"", &orders),
            "CREATE TABLE IF NOT EXISTS \"svc\".\"order\" (\
             \"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid(), \
             \"user\" TEXT NOT NULL, \"group\" INTEGER NOT NULL)"
        );
    }

    #[test]
    fn every_column_type_is_created_as_its_postgres_type() {
        let types = [
            (ColumnType::Uuid, "UUID"),
            (ColumnType::Text, "TEXT"),
            (ColumnType::Integer, "INTEGER"),
            (ColumnType::BigInteger, "BIGINT"),
            (ColumnType::Float, "REAL"),
            (ColumnType::Double, "DOUBLE PRECISION"),
            (ColumnType::Boolean, "BOOLEAN"),
            (ColumnType::Timestamp, "TIMESTAMPTZ"),
            (ColumnType::Date, "DATE"),
            (ColumnType::Jsonb, "JSONB"),
            (
                ColumnType::Numeric {
                    precision: Some(12),
                    scale: Some(2),
                },
                "NUMERIC(12,2)",
            ),
            (
                ColumnType::Numeric {
                    precision: Some(12),
                    scale: None,
                },
                "NUMERIC(12)",
            ),
            (
                ColumnType::Numeric {
                    precision: None,
                    scale: None,
                },
                "NUMERIC",
            ),
            (ColumnType::Varchar { length: 50 }, "VARCHAR(50)"),
            (ColumnType::TimestampNaive, "TIMESTAMP"),
            (ColumnType::Time, "TIME"),
            (ColumnType::Bytea, "BYTEA"),
            (ColumnType::TextArray, "TEXT[]"),
            (ColumnType::UuidArray, "UUID[]"),
            (ColumnType::IntegerArray, "INTEGER[]"),
            (ColumnType::BigIntegerArray, "BIGINT[]"),
            (ColumnType::JsonbArray, "JSONB[]"),
        ];
        for (column_type, postgres) in types {
            assert_eq!(
                column_sql("\"svc\"", &column("c", column_type.clone())),
                format!("\"c\" {postgres} NOT NULL"),
            );
            assert_eq!(
                sql(SchemaChange::AddColumn {
                    table: "t".into(),
                    column: ColumnDefinition {
                        nullable: true,
                        ..column("c", column_type)
                    },
                }),
                [format!(
                    "ALTER TABLE \"svc\".\"t\" ADD COLUMN IF NOT EXISTS \"c\" {postgres}"
                )]
            );
        }
    }

    #[test]
    fn an_index_is_created_in_the_shape_it_declares() {
        let index = |edit: fn(&mut IndexDefinition)| {
            let mut index = IndexDefinition {
                name: "idx_books".into(),
                columns: vec!["status".into()],
                ..Default::default()
            };
            edit(&mut index);
            create_index_sql("\"svc\"", "book", &index)
        };
        assert_eq!(
            index(|_| {}),
            "CREATE INDEX IF NOT EXISTS \"idx_books\" ON \"svc\".\"book\" USING btree (\"status\")"
        );
        assert_eq!(
            index(|i| {
                i.unique = true;
                i.columns = vec!["branch_id".into(), "email".into()];
                i.predicate = Some("deleted_at IS NULL".into());
            }),
            "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_books\" ON \"svc\".\"book\" USING btree \
             (\"branch_id\", \"email\") WHERE deleted_at IS NULL"
        );
        assert_eq!(
            index(|i| {
                i.method = IndexMethod::Gin;
                i.columns = vec!["services".into()];
            }),
            "CREATE INDEX IF NOT EXISTS \"idx_books\" ON \"svc\".\"book\" USING gin (\"services\")"
        );
        for (method, name) in [
            (IndexMethod::Gist, "gist"),
            (IndexMethod::Hash, "hash"),
            (IndexMethod::Brin, "brin"),
        ] {
            let index = IndexDefinition {
                name: "i".into(),
                columns: vec!["c".into()],
                method,
                ..Default::default()
            };
            assert!(create_index_sql("\"svc\"", "t", &index).contains(&format!("USING {name} ")));
        }
    }

    fn by_status(predicate: Option<&str>) -> IndexDefinition {
        IndexDefinition {
            name: "idx_books_status".into(),
            columns: vec!["status".into()],
            predicate: predicate.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn an_index_of_another_shape_is_replaced_in_the_destructive_phase() {
        let changes = [
            SchemaChange::ReplaceIndex {
                table: "book".into(),
                index: by_status(Some("status <> 'Closed'")),
            },
            SchemaChange::DropIndex {
                table: "book".into(),
                index_name: "idx_books_kind".into(),
            },
        ];
        assert!(phase(&changes, MigrationPhase::Additive).is_empty());
        assert_eq!(
            phase(&changes, MigrationPhase::Destructive),
            [
                "DROP INDEX IF EXISTS \"svc\".\"idx_books_status\"",
                "CREATE INDEX IF NOT EXISTS \"idx_books_status\" ON \"svc\".\"book\" USING btree \
                 (\"status\") WHERE status <> 'Closed'",
                "DROP INDEX IF EXISTS \"svc\".\"idx_books_kind\"",
            ]
        );
    }

    fn price_check(expr: &str) -> CheckConstraint {
        CheckConstraint {
            name: "price_nonneg".into(),
            expr: expr.into(),
        }
    }

    #[test]
    fn a_check_is_added_unvalidated_then_validated_and_dropped_when_it_is_gone() {
        let changes = [
            SchemaChange::AddCheck {
                table: "book".into(),
                check: price_check("price_cents >= 0"),
            },
            SchemaChange::DropCheck {
                table: "book".into(),
                name: "book_status_check".into(),
            },
        ];
        assert_eq!(
            phase(&changes, MigrationPhase::Additive),
            [
                "ALTER TABLE \"svc\".\"book\" DROP CONSTRAINT IF EXISTS \"price_nonneg\"",
                "ALTER TABLE \"svc\".\"book\" ADD CONSTRAINT \"price_nonneg\" CHECK (price_cents >= 0) NOT VALID",
                "ALTER TABLE \"svc\".\"book\" VALIDATE CONSTRAINT \"price_nonneg\"",
                "ALTER TABLE \"svc\".\"book\" DROP CONSTRAINT IF EXISTS \"book_status_check\"",
            ]
        );
        assert!(phase(&changes, MigrationPhase::Destructive).is_empty());
    }

    #[test]
    fn a_new_table_brings_its_indexes_and_checks() {
        let book = TableDefinition {
            indexes: vec![by_status(None)],
            checks: vec![price_check("price >= 0")],
            ..table("book", vec![id()])
        };
        let changes = [SchemaChange::CreateTable(Box::new(book))];
        assert_eq!(
            phase(&changes, MigrationPhase::Additive),
            [
                "CREATE TABLE IF NOT EXISTS \"svc\".\"book\" (\"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid())",
                "CREATE INDEX IF NOT EXISTS \"idx_books_status\" ON \"svc\".\"book\" USING btree (\"status\")",
                "ALTER TABLE \"svc\".\"book\" DROP CONSTRAINT IF EXISTS \"price_nonneg\"",
                "ALTER TABLE \"svc\".\"book\" ADD CONSTRAINT \"price_nonneg\" CHECK (price >= 0) NOT VALID",
                "ALTER TABLE \"svc\".\"book\" VALIDATE CONSTRAINT \"price_nonneg\"",
            ]
        );
        assert!(phase(&changes, MigrationPhase::Destructive).is_empty());
    }

    fn searched(columns: &[&str], config: &str) -> SearchSpec {
        SearchSpec {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            config: config.into(),
            vector_column: None,
        }
    }

    #[test]
    fn a_search_vector_is_kept_by_the_database_and_indexed() {
        let set = [SchemaChange::Search {
            table: "book".into(),
            spec: Some(searched(&["title", "Notes"], "english")),
        }];
        assert_eq!(
            phase(&set, MigrationPhase::Additive),
            [
                "ALTER TABLE \"svc\".\"book\" DROP COLUMN IF EXISTS \"_search\"",
                "ALTER TABLE \"svc\".\"book\" ADD COLUMN \"_search\" tsvector GENERATED ALWAYS AS \
                 (to_tsvector('english'::regconfig, coalesce(\"title\"::text, '') || ' ' || \
                 coalesce(\"notes\"::text, ''))) STORED",
            ]
        );
        assert!(phase(&set, MigrationPhase::Destructive).is_empty());
        assert_eq!(
            phase(&set, MigrationPhase::Online),
            [
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"book__search_gin\" ON \"svc\".\"book\" \
              USING gin (\"_search\")"
            ],
            "the index of a table that has rows is built without holding its writes"
        );
        let dropped = [SchemaChange::Search {
            table: "book".into(),
            spec: None,
        }];
        assert_eq!(
            phase(&dropped, MigrationPhase::Additive),
            ["ALTER TABLE \"svc\".\"book\" DROP COLUMN IF EXISTS \"_search\""]
        );
        assert!(phase(&dropped, MigrationPhase::Online).is_empty());
        let made = TableDefinition {
            search: Some(searched(&["title"], "simple")),
            ..table("book", vec![id(), column("title", ColumnType::Text)])
        };
        let made = [SchemaChange::CreateTable(Box::new(made))];
        let created = phase(&made, MigrationPhase::Additive);
        assert_eq!(created.len(), 4);
        assert!(created[2].contains("GENERATED ALWAYS AS"));
        assert_eq!(
            created[3],
            "CREATE INDEX IF NOT EXISTS \"book__search_gin\" ON \"svc\".\"book\" USING gin (\"_search\")",
            "the index of a table that is made is made with it"
        );
        assert!(phase(&made, MigrationPhase::Online).is_empty());
        let own = TableDefinition {
            search: Some(SearchSpec {
                vector_column: Some("document".into()),
                ..searched(&[], "simple")
            }),
            ..table("book", vec![id()])
        };
        assert_eq!(
            phase(
                &[SchemaChange::CreateTable(Box::new(own))],
                MigrationPhase::Additive
            )
            .len(),
            1,
            "a vector the table has already is not made"
        );
    }

    #[test]
    fn a_column_is_added_nullable_and_tightened_after_the_hook() {
        let add = |column| {
            [SchemaChange::AddColumn {
                table: "sailings".into(),
                column,
            }]
        };
        let required = add(column("min", ColumnType::Integer));
        assert_eq!(
            phase(&required, MigrationPhase::Additive),
            ["ALTER TABLE \"svc\".\"sailings\" ADD COLUMN IF NOT EXISTS \"min\" INTEGER DEFAULT NULL"]
        );
        assert_eq!(
            phase(&required, MigrationPhase::Destructive),
            ["ALTER TABLE \"svc\".\"sailings\" ALTER COLUMN \"min\" SET NOT NULL"]
        );
        let defaulted = add(ColumnDefinition {
            default_value: Some("0".into()),
            unique: true,
            references: Some("routes.id".into()),
            ..column("route", ColumnType::Uuid)
        });
        assert_eq!(
            phase(&defaulted, MigrationPhase::Additive),
            ["ALTER TABLE \"svc\".\"sailings\" ADD COLUMN IF NOT EXISTS \"route\" UUID DEFAULT '0' UNIQUE \
              REFERENCES \"svc\".\"routes\"(\"id\") DEFERRABLE"]
        );
        let optional = add(ColumnDefinition {
            nullable: true,
            ..column("notes", ColumnType::Text)
        });
        assert!(phase(&optional, MigrationPhase::Destructive).is_empty());
    }

    #[test]
    fn a_change_of_type_casts_the_column() {
        assert_eq!(
            sql(SchemaChange::AlterColumnType {
                table: "book".into(),
                column_name: "price".into(),
                old_type: "INTEGER".into(),
                new_type: "NUMERIC(12,2)".into(),
            }),
            [
                "ALTER TABLE \"svc\".\"book\" ALTER COLUMN \"price\" TYPE NUMERIC(12,2) \
              USING \"price\"::NUMERIC(12,2)"
            ]
        );
    }

    #[test]
    fn nullability_is_relaxed_before_the_hook_and_tightened_after_it() {
        let nullable = |nullable| {
            [SchemaChange::AlterNullability {
                table: "media".into(),
                column_name: "item_id".into(),
                nullable,
            }]
        };
        assert_eq!(
            phase(&nullable(true), MigrationPhase::Additive),
            ["ALTER TABLE \"svc\".\"media\" ALTER COLUMN \"item_id\" DROP NOT NULL"]
        );
        assert!(phase(&nullable(true), MigrationPhase::Destructive).is_empty());
        assert!(phase(&nullable(false), MigrationPhase::Additive).is_empty());
        assert_eq!(
            phase(&nullable(false), MigrationPhase::Destructive),
            ["ALTER TABLE \"svc\".\"media\" ALTER COLUMN \"item_id\" SET NOT NULL"]
        );
    }

    #[test]
    fn a_foreign_key_is_replaced_under_the_name_postgres_gave_it() {
        assert_eq!(
            sql(SchemaChange::AlterForeignKey {
                table: "items".into(),
                column_name: "category_id".into(),
                references: "categories.id".into(),
                on_delete: Some(ForeignKeyAction::SetNull),
            }),
            ["ALTER TABLE \"svc\".\"items\" DROP CONSTRAINT IF EXISTS \"items_category_id_fkey\", \
              ADD CONSTRAINT \"items_category_id_fkey\" FOREIGN KEY (\"category_id\") \
              REFERENCES \"svc\".\"categories\"(\"id\") ON DELETE SET NULL DEFERRABLE"]
        );
    }

    #[test]
    fn an_archive_name_fits_an_identifier_and_no_two_names_share_one() {
        assert_eq!(archive_name(3, "Orders"), "_archived_v3_orders");
        let fits = "c".repeat(IDENTIFIER_MAX - "_archived_v12_".len());
        assert_eq!(archive_name(12, &fits), format!("_archived_v12_{fits}"));

        let long = format!("{}_sent_at", "n".repeat(55));
        let longer = format!("{}_seen_at", "n".repeat(55));
        let (first, second) = (archive_name(12, &long), archive_name(12, &longer));
        assert_eq!(first.len(), IDENTIFIER_MAX);
        assert_eq!(second.len(), IDENTIFIER_MAX);
        assert_ne!(first, second);
        assert!(first.starts_with(&format!("_archived_v12_{}_", "n".repeat(42))));
        assert_eq!(archive_name(12, &long), first);

        let wide = "é".repeat(40);
        let archived = archive_name(7, &wide);
        assert!(archived.len() <= IDENTIFIER_MAX);
        assert!(archived.starts_with(&format!("_archived_v7_{}_", "é".repeat(21))));
    }

    #[test]
    fn what_is_archived_is_renamed_and_what_is_restored_is_renamed_back() {
        assert_eq!(
            sql(SchemaChange::ArchiveTable {
                table_name: "orders".into(),
                archive_name: "_archived_v3_orders".into(),
            }),
            ["ALTER TABLE \"svc\".\"orders\" RENAME TO \"_archived_v3_orders\""]
        );
        assert_eq!(
            sql(SchemaChange::ArchiveColumn {
                table: "users".into(),
                column_name: "email".into(),
                archive_name: "_archived_v5_email".into(),
            }),
            [
                "ALTER TABLE \"svc\".\"users\" RENAME COLUMN \"email\" TO \"_archived_v5_email\"",
                "ALTER TABLE \"svc\".\"users\" ALTER COLUMN \"_archived_v5_email\" DROP NOT NULL",
            ]
        );
        assert_eq!(
            sql(SchemaChange::RestoreTable {
                archive_name: "_archived_v3_orders".into(),
                restore_name: "orders".into(),
            }),
            ["ALTER TABLE \"svc\".\"_archived_v3_orders\" RENAME TO \"orders\""]
        );
        assert_eq!(
            sql(SchemaChange::RestoreColumn {
                table: "users".into(),
                archive_name: "_archived_v5_email".into(),
                column: column("email", ColumnType::Text),
            }),
            ["ALTER TABLE \"svc\".\"users\" RENAME COLUMN \"_archived_v5_email\" TO \"email\""]
        );
    }

    #[test]
    fn an_archive_whose_name_was_cut_says_what_it_was_and_is_restored_without_saying_it() {
        let long = format!("{}_sent_at", "n".repeat(55));
        let archived = archive_name(12, &long);
        assert_eq!(
            archive_column_sql("\"svc\"", "users", &long, &archived),
            [
                format!("ALTER TABLE \"svc\".\"users\" RENAME COLUMN \"{long}\" TO \"{archived}\""),
                format!("ALTER TABLE \"svc\".\"users\" ALTER COLUMN \"{archived}\" DROP NOT NULL"),
                format!(
                    "COMMENT ON COLUMN \"svc\".\"users\".\"{archived}\" IS 'cufflink:archived:{long}'"
                ),
            ]
        );
        assert_eq!(
            Archived::parse(
                "users",
                Some(&archived),
                Some(&format!("cufflink:archived:{long}"))
            )
            .map(|a| a.original),
            Some(long.clone())
        );
        assert_eq!(
            restore_column_sql("\"svc\"", "users", &archived, &long),
            [
                format!("ALTER TABLE \"svc\".\"users\" RENAME COLUMN \"{archived}\" TO \"{long}\""),
                format!("COMMENT ON COLUMN \"svc\".\"users\".\"{long}\" IS NULL"),
            ]
        );
        assert_eq!(
            archive_table_sql("\"svc\"", &long, &archived),
            [
                format!("ALTER TABLE \"svc\".\"{long}\" RENAME TO \"{archived}\""),
                format!("COMMENT ON TABLE \"svc\".\"{archived}\" IS 'cufflink:archived:{long}'"),
            ]
        );
        assert_eq!(
            restore_table_sql("\"svc\"", &archived, &long),
            [
                format!("ALTER TABLE \"svc\".\"{archived}\" RENAME TO \"{long}\""),
                format!("COMMENT ON TABLE \"svc\".\"{long}\" IS NULL"),
            ]
        );
    }

    fn drops() -> Vec<SchemaChange> {
        vec![
            SchemaChange::DropTable("orders".into()),
            SchemaChange::DropColumn {
                table: "users".into(),
                column_name: "email".into(),
            },
        ]
    }

    #[test]
    fn a_drop_is_what_the_deploy_makes_of_it() {
        let destructive = |drops_as| {
            generate_sql_phase("\"svc\"", &drops(), MigrationPhase::Destructive, drops_as)
        };
        assert_eq!(
            destructive(Drops::Legacy),
            [
                "DROP TABLE IF EXISTS \"svc\".\"orders\" CASCADE",
                "ALTER TABLE \"svc\".\"users\" DROP COLUMN IF EXISTS \"email\"",
            ]
        );
        assert_eq!(
            destructive(Drops::Restrict),
            [
                "DROP TABLE IF EXISTS \"svc\".\"orders\" RESTRICT",
                "ALTER TABLE \"svc\".\"users\" DROP COLUMN IF EXISTS \"email\" RESTRICT",
            ]
        );
        assert_eq!(
            destructive(Drops::Archive { version: 4 }),
            [
                "ALTER TABLE \"svc\".\"orders\" RENAME TO \"_archived_v4_orders\"",
                "ALTER TABLE \"svc\".\"users\" RENAME COLUMN \"email\" TO \"_archived_v4_email\"",
                "ALTER TABLE \"svc\".\"users\" ALTER COLUMN \"_archived_v4_email\" DROP NOT NULL",
            ]
        );
        for drops_as in [
            Drops::Legacy,
            Drops::Restrict,
            Drops::Archive { version: 4 },
        ] {
            assert!(
                generate_sql_phase("\"svc\"", &drops(), MigrationPhase::Additive, drops_as)
                    .is_empty()
            );
        }
    }

    #[test]
    fn a_column_restored_as_not_null_is_tightened_after_the_hook() {
        let restore = |nullable| {
            [SchemaChange::RestoreColumn {
                table: "users".into(),
                archive_name: "_archived_v5_email".into(),
                column: ColumnDefinition {
                    nullable,
                    ..column("email", ColumnType::Text)
                },
            }]
        };
        assert_eq!(
            phase(&restore(false), MigrationPhase::Destructive),
            ["ALTER TABLE \"svc\".\"users\" ALTER COLUMN \"email\" SET NOT NULL"]
        );
        assert!(phase(&restore(true), MigrationPhase::Destructive).is_empty());
    }

    #[test]
    fn the_phases_of_a_split_leave_the_old_column_for_the_hook() {
        let changes = [
            SchemaChange::AddColumn {
                table: "sailings".into(),
                column: column("min", ColumnType::Integer),
            },
            SchemaChange::AddColumn {
                table: "sailings".into(),
                column: column("max", ColumnType::Integer),
            },
            SchemaChange::DropColumn {
                table: "sailings".into(),
                column_name: "midpoint".into(),
            },
        ];
        assert_eq!(
            phase(&changes, MigrationPhase::Additive),
            [
                "ALTER TABLE \"svc\".\"sailings\" ADD COLUMN IF NOT EXISTS \"min\" INTEGER DEFAULT NULL",
                "ALTER TABLE \"svc\".\"sailings\" ADD COLUMN IF NOT EXISTS \"max\" INTEGER DEFAULT NULL",
            ]
        );
        assert_eq!(
            phase(&changes, MigrationPhase::Destructive),
            [
                "ALTER TABLE \"svc\".\"sailings\" ALTER COLUMN \"min\" SET NOT NULL",
                "ALTER TABLE \"svc\".\"sailings\" ALTER COLUMN \"max\" SET NOT NULL",
                "ALTER TABLE \"svc\".\"sailings\" DROP COLUMN IF EXISTS \"midpoint\"",
            ]
        );
    }
}
