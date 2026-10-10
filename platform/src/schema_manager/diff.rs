use crate::deployer::manifest::{
    CheckConstraint, ColumnDefinition, ForeignKeyAction, SearchSpec, TableDefinition,
};
use cufflink_types::SchemaDiff;

#[derive(Debug, Clone)]
pub enum SchemaChange {
    CreateTable(Box<TableDefinition>),
    DropTable(String),
    AddColumn {
        table: String,
        column: ColumnDefinition,
    },
    DropColumn {
        table: String,
        column_name: String,
    },
    AlterColumnType {
        table: String,
        column_name: String,
        old_type: String,
        new_type: String,
    },
    AlterNullability {
        table: String,
        column_name: String,
        nullable: bool,
    },
    CreateIndex {
        table: String,
        index: crate::deployer::manifest::IndexDefinition,
    },
    ReplaceIndex {
        table: String,
        index: crate::deployer::manifest::IndexDefinition,
    },
    Search {
        table: String,
        spec: Option<SearchSpec>,
    },
    AddCheck {
        table: String,
        check: CheckConstraint,
    },
    DropCheck {
        table: String,
        name: String,
    },
    AlterForeignKey {
        table: String,
        column_name: String,
        references: String,
        on_delete: Option<ForeignKeyAction>,
    },
    DropIndex {
        table: String,
        index_name: String,
    },
    ArchiveTable {
        table_name: String,
        archive_name: String,
    },
    ArchiveColumn {
        table: String,
        column_name: String,
        archive_name: String,
    },
    RestoreTable {
        archive_name: String,
        restore_name: String,
    },
    RestoreColumn {
        table: String,
        archive_name: String,
        column: ColumnDefinition,
    },
}

impl SchemaChange {
    pub fn is_destructive(&self) -> bool {
        match self {
            SchemaChange::DropTable(_)
            | SchemaChange::DropColumn { .. }
            | SchemaChange::AlterColumnType { .. }
            | SchemaChange::ReplaceIndex { .. }
            | SchemaChange::DropIndex { .. } => true,
            SchemaChange::AlterNullability { nullable, .. } => !nullable,
            SchemaChange::ArchiveTable { .. }
            | SchemaChange::ArchiveColumn { .. }
            | SchemaChange::RestoreTable { .. }
            | SchemaChange::RestoreColumn { .. }
            | SchemaChange::AlterForeignKey { .. } => false,
            _ => false,
        }
    }

    pub fn caution(&self) -> Option<String> {
        match self {
            SchemaChange::Search {
                table,
                spec: Some(_),
            } => Some(format!(
                "{}: the search vector is a stored generated column, and adding it rewrites \
                 {table} while nothing else reads or writes it (ACCESS EXCLUSIVE); its index is \
                 then built CONCURRENTLY. A table too large for that adds both in an SQL migration",
                self.describe()
            )),
            SchemaChange::ReplaceIndex { table, .. } => Some(format!(
                "{}: the index is dropped and built again in one transaction, and writes to \
                 {table} wait for it (SHARE). A table too large for that replaces it in an SQL \
                 migration, CONCURRENTLY",
                self.describe()
            )),
            _ => None,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            SchemaChange::CreateTable(table) => format!("CREATE TABLE {}", table.name),
            SchemaChange::DropTable(name) => format!("DROP TABLE {name}"),
            SchemaChange::AddColumn { table, column } => {
                format!("ADD COLUMN {table}.{}", column.name)
            }
            SchemaChange::DropColumn { table, column_name } => {
                format!("DROP COLUMN {table}.{column_name}")
            }
            SchemaChange::AlterColumnType {
                table,
                column_name,
                old_type,
                new_type,
            } => format!("ALTER COLUMN {table}.{column_name} from {old_type} to {new_type}"),
            SchemaChange::AlterNullability {
                table,
                column_name,
                nullable: true,
            } => format!("DROP NOT NULL {table}.{column_name}"),
            SchemaChange::AlterNullability {
                table,
                column_name,
                nullable: false,
            } => format!("SET NOT NULL {table}.{column_name}"),
            SchemaChange::CreateIndex { table, index } => {
                format!("CREATE INDEX {} ON {table}", index.name)
            }
            SchemaChange::ReplaceIndex { table, index } => {
                format!("REPLACE INDEX {} ON {table}", index.name)
            }
            SchemaChange::Search {
                table,
                spec: Some(_),
            } => format!("SET SEARCH ON {table}"),
            SchemaChange::Search { table, spec: None } => format!("DROP SEARCH ON {table}"),
            SchemaChange::AddCheck { table, check } => {
                format!("ADD CHECK {} ON {table}", check.name)
            }
            SchemaChange::DropCheck { table, name } => format!("DROP CHECK {name} ON {table}"),
            SchemaChange::AlterForeignKey {
                table, column_name, ..
            } => format!("ALTER FOREIGN KEY {table}.{column_name}"),
            SchemaChange::DropIndex { index_name, .. } => format!("DROP INDEX {index_name}"),
            SchemaChange::ArchiveTable {
                table_name,
                archive_name,
            } => format!("ARCHIVE TABLE {table_name} AS {archive_name}"),
            SchemaChange::ArchiveColumn {
                table,
                column_name,
                archive_name,
            } => format!("ARCHIVE COLUMN {table}.{column_name} AS {archive_name}"),
            SchemaChange::RestoreTable {
                archive_name,
                restore_name,
            } => format!("RESTORE TABLE {restore_name} FROM {archive_name}"),
            SchemaChange::RestoreColumn {
                table,
                archive_name,
                column,
            } => format!("RESTORE COLUMN {table}.{} FROM {archive_name}", column.name),
        }
    }
}

pub fn diff_schemas(
    old_tables: &[TableDefinition],
    new_tables: &[TableDefinition],
) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    for new_table in new_tables {
        if !old_tables.iter().any(|t| t.name == new_table.name) {
            changes.push(SchemaChange::CreateTable(Box::new(new_table.clone())));
        }
    }

    for old_table in old_tables {
        if !new_tables.iter().any(|t| t.name == old_table.name) {
            changes.push(SchemaChange::DropTable(old_table.name.clone()));
        }
    }

    for new_table in new_tables {
        if let Some(old_table) = old_tables.iter().find(|t| t.name == new_table.name) {
            for new_col in &new_table.columns {
                if let Some(old_col) = old_table.columns.iter().find(|c| c.name == new_col.name) {
                    if old_col.column_type != new_col.column_type {
                        changes.push(SchemaChange::AlterColumnType {
                            table: new_table.name.clone(),
                            column_name: new_col.name.clone(),
                            old_type: old_col.column_type.to_sql().to_string(),
                            new_type: new_col.column_type.to_sql().to_string(),
                        });
                    }
                    if old_col.nullable != new_col.nullable {
                        changes.push(SchemaChange::AlterNullability {
                            table: new_table.name.clone(),
                            column_name: new_col.name.clone(),
                            nullable: new_col.nullable,
                        });
                    }
                    let moved = new_col
                        .references
                        .as_ref()
                        .filter(|_| old_col.on_delete != new_col.on_delete);
                    if let Some(references) = moved {
                        changes.push(SchemaChange::AlterForeignKey {
                            table: new_table.name.clone(),
                            column_name: new_col.name.clone(),
                            references: references.clone(),
                            on_delete: new_col.on_delete.clone(),
                        });
                    }
                } else {
                    changes.push(SchemaChange::AddColumn {
                        table: new_table.name.clone(),
                        column: new_col.clone(),
                    });
                }
            }

            for old_col in &old_table.columns {
                if !new_table.columns.iter().any(|c| c.name == old_col.name) {
                    changes.push(SchemaChange::DropColumn {
                        table: new_table.name.clone(),
                        column_name: old_col.name.clone(),
                    });
                }
            }

            changes.extend(index_changes(old_table, new_table));
            changes.extend(check_changes(old_table, new_table));
            if kept_search(old_table) != kept_search(new_table) {
                changes.push(SchemaChange::Search {
                    table: new_table.name.clone(),
                    spec: kept_search(new_table).cloned(),
                });
            }
        }
    }

    changes
}

fn index_changes(old: &TableDefinition, new: &TableDefinition) -> Vec<SchemaChange> {
    let before = |name: &str| old.indexes.iter().find(|index| index.name == name);
    let made = new
        .indexes
        .iter()
        .filter_map(|index| match before(&index.name) {
            None => Some(SchemaChange::CreateIndex {
                table: new.name.clone(),
                index: index.clone(),
            }),
            Some(same) if same == index => None,
            Some(_) => Some(SchemaChange::ReplaceIndex {
                table: new.name.clone(),
                index: index.clone(),
            }),
        });
    let dropped = old
        .indexes
        .iter()
        .filter(|index| !new.indexes.iter().any(|now| now.name == index.name))
        .map(|index| SchemaChange::DropIndex {
            table: new.name.clone(),
            index_name: index.name.clone(),
        });
    made.chain(dropped).collect()
}

pub fn kept_search(table: &TableDefinition) -> Option<&SearchSpec> {
    table
        .search
        .as_ref()
        .filter(|spec| spec.vector_column.is_none())
}

fn check_changes(old: &TableDefinition, new: &TableDefinition) -> Vec<SchemaChange> {
    let added = new
        .checks
        .iter()
        .filter(|check| !old.checks.contains(check))
        .map(|check| SchemaChange::AddCheck {
            table: new.name.clone(),
            check: check.clone(),
        });
    let dropped = old
        .checks
        .iter()
        .filter(|check| !new.checks.iter().any(|now| now.name == check.name))
        .map(|check| SchemaChange::DropCheck {
            table: new.name.clone(),
            name: check.name.clone(),
        });
    added.chain(dropped).collect()
}

pub fn build_schema_diff(changes: &[SchemaChange]) -> SchemaDiff {
    let mut diff = SchemaDiff::default();
    for change in changes {
        match change {
            SchemaChange::CreateTable(td) => diff.added_tables.push(td.name.clone()),
            SchemaChange::DropTable(name) => diff.dropped_tables.push(name.clone()),
            SchemaChange::AddColumn { table, column } => diff
                .added_columns
                .push((table.clone(), column.name.clone())),
            SchemaChange::DropColumn { table, column_name } => diff
                .dropped_columns
                .push((table.clone(), column_name.clone())),
            SchemaChange::AlterColumnType {
                table,
                column_name,
                old_type,
                new_type,
            } => diff.type_changes.push((
                table.clone(),
                column_name.clone(),
                old_type.clone(),
                new_type.clone(),
            )),
            SchemaChange::AlterNullability {
                table,
                column_name,
                nullable,
            } => diff
                .nullability_changes
                .push((table.clone(), column_name.clone(), *nullable)),
            SchemaChange::CreateIndex { .. }
            | SchemaChange::ReplaceIndex { .. }
            | SchemaChange::DropIndex { .. }
            | SchemaChange::AddCheck { .. }
            | SchemaChange::DropCheck { .. }
            | SchemaChange::Search { .. }
            | SchemaChange::AlterForeignKey { .. }
            | SchemaChange::ArchiveTable { .. }
            | SchemaChange::ArchiveColumn { .. }
            | SchemaChange::RestoreTable { .. }
            | SchemaChange::RestoreColumn { .. } => {}
        }
    }
    diff
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployer::manifest::ColumnType;

    fn col(name: &str, ct: ColumnType, pk: bool) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type: ct,
            primary_key: pk,
            nullable: false,
            auto_generate: false,
            default_value: None,
            references: None,
            on_delete: None,
            unique: false,
            validations: vec![],
            ..Default::default()
        }
    }

    fn table(name: &str, columns: Vec<ColumnDefinition>) -> TableDefinition {
        TableDefinition {
            name: name.into(),
            columns,
            indexes: vec![],
            soft_delete: false,
            owner_field: None,
            auth_required: false,
            permission_area: None,
            owner_self_service: false,
            hooks: None,
            scoped_by: None,
            ..Default::default()
        }
    }

    #[test]
    fn test_new_table() {
        let old = vec![];
        let new = vec![table("todos", vec![col("id", ColumnType::Uuid, true)])];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], SchemaChange::CreateTable(t) if t.name == "todos"));
    }

    #[test]
    fn test_add_column() {
        let old = vec![table("todos", vec![col("id", ColumnType::Uuid, true)])];
        let new = vec![table(
            "todos",
            vec![
                col("id", ColumnType::Uuid, true),
                col("title", ColumnType::Text, false),
            ],
        )];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(
            matches!(&changes[0], SchemaChange::AddColumn { table, column } if table == "todos" && column.name == "title")
        );
    }

    #[test]
    fn test_drop_column_is_destructive() {
        let old = vec![table(
            "todos",
            vec![
                col("id", ColumnType::Uuid, true),
                col("title", ColumnType::Text, false),
            ],
        )];
        let new = vec![table("todos", vec![col("id", ColumnType::Uuid, true)])];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(changes[0].is_destructive());
    }

    #[test]
    fn test_drop_table() {
        let old = vec![table("todos", vec![col("id", ColumnType::Uuid, true)])];
        let new = vec![];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], SchemaChange::DropTable(name) if name == "todos"));
        assert!(changes[0].is_destructive());
    }

    #[test]
    fn test_alter_column_type() {
        let old = vec![table(
            "todos",
            vec![
                col("id", ColumnType::Uuid, true),
                col("count", ColumnType::Integer, false),
            ],
        )];
        let new = vec![table(
            "todos",
            vec![
                col("id", ColumnType::Uuid, true),
                col("count", ColumnType::BigInteger, false),
            ],
        )];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            SchemaChange::AlterColumnType { table, column_name, .. }
            if table == "todos" && column_name == "count"
        ));
    }

    #[test]
    fn test_no_changes() {
        let tables = vec![table(
            "todos",
            vec![
                col("id", ColumnType::Uuid, true),
                col("title", ColumnType::Text, false),
            ],
        )];

        let changes = diff_schemas(&tables, &tables);
        assert!(changes.is_empty());
    }

    #[test]
    fn test_multiple_tables_and_changes() {
        let old = vec![
            table("posts", vec![col("id", ColumnType::Uuid, true)]),
            table("old_table", vec![col("id", ColumnType::Uuid, true)]),
        ];
        let new = vec![
            table(
                "posts",
                vec![
                    col("id", ColumnType::Uuid, true),
                    col("title", ColumnType::Text, false),
                ],
            ),
            table("comments", vec![col("id", ColumnType::Uuid, true)]),
        ];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 3);
    }

    fn indexed(index: crate::deployer::manifest::IndexDefinition) -> TableDefinition {
        TableDefinition {
            indexes: vec![index],
            ..table("book", vec![col("id", ColumnType::Uuid, true)])
        }
    }

    fn described(changes: &[SchemaChange]) -> Vec<String> {
        changes.iter().map(SchemaChange::describe).collect()
    }

    #[test]
    fn an_index_of_the_same_name_and_another_shape_is_replaced() {
        use crate::deployer::manifest::{IndexDefinition, IndexMethod};
        let by_status = IndexDefinition {
            name: "idx_books_status".into(),
            columns: vec!["status".into()],
            ..Default::default()
        };
        let shapes = [
            IndexDefinition {
                unique: true,
                ..by_status.clone()
            },
            IndexDefinition {
                columns: vec!["status".into(), "kind".into()],
                ..by_status.clone()
            },
            IndexDefinition {
                predicate: Some("status <> 'Closed'".into()),
                ..by_status.clone()
            },
            IndexDefinition {
                method: IndexMethod::Hash,
                ..by_status.clone()
            },
        ];
        for shape in shapes {
            let changes = diff_schemas(&[indexed(by_status.clone())], &[indexed(shape)]);
            assert_eq!(
                described(&changes),
                ["REPLACE INDEX idx_books_status ON book"]
            );
            assert!(changes[0].is_destructive());
        }
        assert!(diff_schemas(&[indexed(by_status.clone())], &[indexed(by_status)]).is_empty());
    }

    #[test]
    fn an_index_stored_before_shapes_were_is_the_index_it_was() {
        let stored: TableDefinition = serde_json::from_value(serde_json::json!({
            "name": "book",
            "columns": [{"name": "id", "column_type": "uuid", "primary_key": true}],
            "indexes": [{"name": "idx_books_status", "columns": ["status", "kind"], "unique": true}]
        }))
        .expect("a snapshot v0.27.0 stored");
        let declared = indexed(crate::deployer::manifest::IndexDefinition {
            name: "idx_books_status".into(),
            columns: vec!["status".into(), "kind".into()],
            unique: true,
            ..Default::default()
        });
        assert!(diff_schemas(&[stored], &[declared]).is_empty());
    }

    #[test]
    fn a_check_is_added_replaced_and_dropped_by_its_name() {
        use crate::deployer::manifest::CheckConstraint;
        let checked = |checks: &[(&str, &str)]| TableDefinition {
            checks: checks
                .iter()
                .map(|(name, expr)| CheckConstraint {
                    name: name.to_string(),
                    expr: expr.to_string(),
                })
                .collect(),
            ..table("book", vec![col("id", ColumnType::Uuid, true)])
        };
        let old = [checked(&[
            ("price_nonneg", "price >= 0"),
            ("status_known", "status <> ''"),
            ("kind_known", "kind <> ''"),
        ])];
        let new = [checked(&[
            ("price_nonneg", "price >= 0"),
            ("status_known", "status IN ('New', 'Closed')"),
            ("services_known", "cardinality(services) > 0"),
        ])];
        let changes = diff_schemas(&old, &new);
        assert_eq!(
            described(&changes),
            [
                "ADD CHECK status_known ON book",
                "ADD CHECK services_known ON book",
                "DROP CHECK kind_known ON book",
            ]
        );
        assert!(changes.iter().all(|change| !change.is_destructive()));
        assert!(build_schema_diff(&changes).is_empty());
    }

    #[test]
    fn a_search_that_changes_is_kept_again_and_one_over_an_own_vector_is_not_the_platforms() {
        let searching = |spec: Option<SearchSpec>| TableDefinition {
            search: spec,
            ..table("book", vec![col("id", ColumnType::Uuid, true)])
        };
        let over = |columns: &[&str]| SearchSpec {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            ..Default::default()
        };
        let set = diff_schemas(&[searching(None)], &[searching(Some(over(&["title"])))]);
        assert_eq!(described(&set), ["SET SEARCH ON book"]);
        assert!(!set[0].is_destructive());
        let changed = diff_schemas(
            &[searching(Some(over(&["title"])))],
            &[searching(Some(over(&["title", "body"])))],
        );
        assert_eq!(described(&changed), ["SET SEARCH ON book"]);
        let same = diff_schemas(
            &[searching(Some(over(&["title"])))],
            &[searching(Some(over(&["title"])))],
        );
        assert!(same.is_empty());
        let gone = diff_schemas(&[searching(Some(over(&["title"])))], &[searching(None)]);
        assert_eq!(described(&gone), ["DROP SEARCH ON book"]);
        let own = SearchSpec {
            vector_column: Some("document".into()),
            ..Default::default()
        };
        assert!(diff_schemas(&[searching(None)], &[searching(Some(own.clone()))]).is_empty());
        assert_eq!(
            described(&diff_schemas(
                &[searching(Some(over(&["title"])))],
                &[searching(Some(own))]
            )),
            ["DROP SEARCH ON book"]
        );
    }

    #[test]
    fn test_index_added() {
        use crate::deployer::manifest::IndexDefinition;
        let old = vec![table("todos", vec![col("id", ColumnType::Uuid, true)])];
        let mut new_table = table("todos", vec![col("id", ColumnType::Uuid, true)]);
        new_table.indexes.push(IndexDefinition {
            name: "idx_todos_title".into(),
            columns: vec!["title".into()],
            unique: false,
            ..Default::default()
        });
        let new = vec![new_table];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(
            matches!(&changes[0], SchemaChange::CreateIndex { table, index } if table == "todos" && index.name == "idx_todos_title")
        );
    }

    #[test]
    fn test_index_dropped() {
        use crate::deployer::manifest::IndexDefinition;
        let mut old_table = table("todos", vec![col("id", ColumnType::Uuid, true)]);
        old_table.indexes.push(IndexDefinition {
            name: "idx_todos_title".into(),
            columns: vec!["title".into()],
            unique: false,
            ..Default::default()
        });
        let old = vec![old_table];
        let new = vec![table("todos", vec![col("id", ColumnType::Uuid, true)])];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(
            matches!(&changes[0], SchemaChange::DropIndex { table, index_name } if table == "todos" && index_name == "idx_todos_title")
        );
        assert!(changes[0].is_destructive());
    }

    #[test]
    fn test_alter_nullability_to_nullable() {
        let old = vec![table(
            "media",
            vec![
                col("id", ColumnType::Uuid, true),
                col("item_id", ColumnType::Uuid, false),
            ],
        )];
        let mut nullable_col = col("item_id", ColumnType::Uuid, false);
        nullable_col.nullable = true;
        let new = vec![table(
            "media",
            vec![col("id", ColumnType::Uuid, true), nullable_col],
        )];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            SchemaChange::AlterNullability { table, column_name, nullable }
            if table == "media" && column_name == "item_id" && *nullable
        ));
        assert!(!changes[0].is_destructive());
    }

    #[test]
    fn test_alter_fk_on_delete() {
        let mut old_col = col("category_id", ColumnType::Uuid, false);
        old_col.nullable = true;
        old_col.references = Some("categories.id".into());
        old_col.on_delete = None;

        let mut new_col = col("category_id", ColumnType::Uuid, false);
        new_col.nullable = true;
        new_col.references = Some("categories.id".into());
        new_col.on_delete = Some(ForeignKeyAction::SetNull);

        let old = vec![table(
            "items",
            vec![col("id", ColumnType::Uuid, true), old_col],
        )];
        let new = vec![table(
            "items",
            vec![col("id", ColumnType::Uuid, true), new_col],
        )];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            SchemaChange::AlterForeignKey { table, column_name, on_delete, .. }
            if table == "items" && column_name == "category_id" && *on_delete == Some(ForeignKeyAction::SetNull)
        ));
        assert!(!changes[0].is_destructive());
    }

    #[test]
    fn test_archive_table_is_not_destructive() {
        let change = SchemaChange::ArchiveTable {
            table_name: "orders".into(),
            archive_name: "_archived_v3_orders".into(),
        };
        assert!(!change.is_destructive());
    }

    #[test]
    fn test_archive_column_is_not_destructive() {
        let change = SchemaChange::ArchiveColumn {
            table: "users".into(),
            column_name: "email".into(),
            archive_name: "_archived_v3_email".into(),
        };
        assert!(!change.is_destructive());
    }

    #[test]
    fn test_restore_table_is_not_destructive() {
        let change = SchemaChange::RestoreTable {
            archive_name: "_archived_v3_orders".into(),
            restore_name: "orders".into(),
        };
        assert!(!change.is_destructive());
    }

    #[test]
    fn test_restore_column_is_not_destructive() {
        let change = SchemaChange::RestoreColumn {
            table: "users".into(),
            archive_name: "_archived_v3_email".into(),
            column: col("email", ColumnType::Text, false),
        };
        assert!(!change.is_destructive());
    }

    #[test]
    fn test_build_schema_diff_captures_all_kinds() {
        let changes = vec![
            SchemaChange::CreateTable(Box::new(table(
                "batches",
                vec![col("id", ColumnType::Uuid, true)],
            ))),
            SchemaChange::DropTable("legacy".into()),
            SchemaChange::AddColumn {
                table: "sailings".into(),
                column: col("min", ColumnType::Integer, false),
            },
            SchemaChange::DropColumn {
                table: "sailings".into(),
                column_name: "midpoint".into(),
            },
            SchemaChange::AlterColumnType {
                table: "items".into(),
                column_name: "price".into(),
                old_type: "INTEGER".into(),
                new_type: "BIGINT".into(),
            },
            SchemaChange::AlterNullability {
                table: "items".into(),
                column_name: "name".into(),
                nullable: true,
            },
        ];
        let diff = build_schema_diff(&changes);
        assert!(diff.added_table("batches"));
        assert!(diff.dropped_table("legacy"));
        assert!(diff.added_column("sailings", "min"));
        assert!(diff.dropped_column("sailings", "midpoint"));
        assert_eq!(diff.type_changes.len(), 1);
        assert_eq!(diff.nullability_changes.len(), 1);
    }

    #[test]
    fn test_build_schema_diff_skips_index_and_archive_variants() {
        use crate::deployer::manifest::IndexDefinition;
        let changes = vec![
            SchemaChange::CreateIndex {
                table: "items".into(),
                index: IndexDefinition {
                    name: "idx".into(),
                    columns: vec!["x".into()],
                    unique: false,
                    ..Default::default()
                },
            },
            SchemaChange::DropIndex {
                table: "items".into(),
                index_name: "idx".into(),
            },
            SchemaChange::ArchiveTable {
                table_name: "old".into(),
                archive_name: "_archived_v1_old".into(),
            },
            SchemaChange::ArchiveColumn {
                table: "items".into(),
                column_name: "old_col".into(),
                archive_name: "_archived_v1_old_col".into(),
            },
            SchemaChange::RestoreTable {
                archive_name: "_archived_v1_old".into(),
                restore_name: "old".into(),
            },
            SchemaChange::RestoreColumn {
                table: "items".into(),
                archive_name: "_archived_v1_old_col".into(),
                column: col("old_col", ColumnType::Text, false),
            },
        ];
        let diff = build_schema_diff(&changes);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_build_schema_diff_empty() {
        assert!(build_schema_diff(&[]).is_empty());
    }

    #[test]
    fn test_alter_nullability_to_not_null() {
        let mut nullable_col = col("item_id", ColumnType::Uuid, false);
        nullable_col.nullable = true;
        let old = vec![table(
            "media",
            vec![col("id", ColumnType::Uuid, true), nullable_col],
        )];
        let new = vec![table(
            "media",
            vec![
                col("id", ColumnType::Uuid, true),
                col("item_id", ColumnType::Uuid, false),
            ],
        )];

        let changes = diff_schemas(&old, &new);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            &changes[0],
            SchemaChange::AlterNullability { table, column_name, nullable }
            if table == "media" && column_name == "item_id" && !*nullable
        ));
        assert!(changes[0].is_destructive());
    }
}
