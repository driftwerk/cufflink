use super::is_safe_identifier;
#[cfg(test)]
use crate::deployer::manifest::ColumnDefinition;
use crate::deployer::manifest::{ColumnType, IndexDefinition, ServiceManifest, TableDefinition};
use crate::errors::PlatformError;
use crate::router::crud_pk::is_key_type;
use crate::router::query::keyset::Keys;
use crate::router::query::search::SEARCH_COLUMN;
use cufflink_types::manifest::validate::{
    folded, validate_audit_redact, validate_check_constraints, validate_column_access,
    validate_column_type, validate_index, validate_private_table, validate_search_spec,
};
use cufflink_types::{Pagination, SchemaOwnership};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reach {
    Served,
    Private,
}

pub(super) fn validate_all(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    let adopted = manifest.schema == SchemaOwnership::Adopted;
    if manifest.declared_tables().next().is_none() && !adopted {
        return Err(PlatformError::Validation(
            "Service must define at least one table".into(),
        ));
    }
    manifest
        .tables
        .iter()
        .try_for_each(|table| validate_owned(table, Reach::Served, adopted || !table.managed))?;
    manifest
        .private_tables
        .iter()
        .try_for_each(|table| validate_private(table, adopted))?;
    validate_private_names(manifest)
}

pub(super) fn refuse_private(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    if manifest.private_tables.is_empty() {
        Ok(())
    } else {
        Err(PlatformError::Validation(
            "private_tables: a web app declares no tables; only a service the platform runs does"
                .into(),
        ))
    }
}

fn validate_private(table: &TableDefinition, adopted: bool) -> Result<(), PlatformError> {
    if !adopted && table.managed {
        return Err(PlatformError::Validation(format!(
            "Private table '{}' would be made by the platform; declare it \
             #[table(managed = false)] and create it in an SQL migration",
            table.name
        )));
    }
    validate_private_table(table).map_err(PlatformError::Validation)?;
    validate_owned(table, Reach::Private, true)
}

fn validate_private_names(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    manifest
        .private_tables
        .iter()
        .enumerate()
        .find_map(|(index, table)| {
            manifest
                .tables
                .iter()
                .chain(&manifest.private_tables[..index])
                .find(|other| folded(&other.name) == folded(&table.name))
                .map(|other| (table, other))
        })
        .map_or(Ok(()), |(table, other)| {
            Err(PlatformError::Validation(format!(
                "Private table '{}' names the same table as '{}'; a table is declared once",
                table.name, other.name
            )))
        })
}

fn validate_adopted(table: &TableDefinition) -> Result<(), PlatformError> {
    let undeclarable = [
        (!table.indexes.is_empty()).then_some("indexes"),
        (!table.checks.is_empty()).then_some("checks"),
    ];
    match undeclarable.into_iter().flatten().next() {
        Some(what) => Err(PlatformError::Validation(format!(
            "Table '{}' is declared over a table that exists and declares {what}, which the \
             platform would never create; create them in an SQL migration",
            table.name
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
fn validate(table: &TableDefinition) -> Result<(), PlatformError> {
    validate_owned(table, Reach::Served, !table.managed)
}

fn validate_declarations(table: &TableDefinition) -> Result<(), String> {
    let path = format!("tables.{}", table.name);
    let declared = |what: &str, column: &str| {
        if table.columns.iter().any(|c| c.name == column) {
            Ok(())
        } else {
            Err(format!("{path}.{what}: column '{column}' is not declared"))
        }
    };
    table.columns.iter().try_for_each(|column| {
        validate_column_type(
            &format!("{path}.columns.{}", column.name),
            &column.column_type,
        )
    })?;
    table
        .indexes
        .iter()
        .filter(|index| IndexDefinition::is_v2(index))
        .try_for_each(|index| validate_index(&format!("{path}.indexes.{}", index.name), index))?;
    validate_check_constraints(&format!("{path}.checks"), &table.checks)?;
    validate_audit_redact(&format!("{path}.audit_redact"), &table.audit_redact)?;
    validate_column_access(&path, table)?;
    let Some(search) = &table.search else {
        return Ok(());
    };
    validate_search_spec(&format!("{path}.search"), search)?;
    search
        .columns
        .iter()
        .try_for_each(|column| declared("search", column))
}

fn validate_listing(table: &TableDefinition, adopted: bool) -> Result<(), String> {
    let path = format!("tables.{}", table.name);
    if let Some(vector) = table
        .search
        .as_ref()
        .and_then(|s| s.vector_column.as_deref())
    {
        if !adopted {
            return Err(format!(
                "{path}.search.vector_column: '{vector}' names a column of a table the platform \
                 makes; declare the columns to search and the platform keeps the vector"
            ));
        }
        if table.columns.iter().any(|c| c.name == vector) {
            return Err(format!(
                "{path}.search.vector_column: '{vector}' is a declared column; a search vector \
                 is read by the search alone"
            ));
        }
    }
    let keeps_vector = !adopted && table.search.is_some();
    if keeps_vector
        && table
            .columns
            .iter()
            .any(|c| folded(&c.name) == SEARCH_COLUMN)
    {
        return Err(format!(
            "{path}.columns.{SEARCH_COLUMN}: '{SEARCH_COLUMN}' is the column the platform keeps \
             the search vector of the table in; name the column otherwise"
        ));
    }
    match (table.pagination, Keys::default_nullable(table)) {
        (Pagination::Keyset, Some(column)) => Err(format!(
            "{path}.pagination: a keyset listing is ordered by '{}', which may be null",
            column.name
        )),
        _ => Ok(()),
    }
}

fn validate_owned(
    table: &TableDefinition,
    reach: Reach,
    adopted: bool,
) -> Result<(), PlatformError> {
    if table.name.is_empty() {
        return Err(PlatformError::Validation(
            "Table name cannot be empty".into(),
        ));
    }

    if !table.name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(PlatformError::Validation(format!(
            "Table name '{}' can only contain alphanumeric characters and underscores",
            table.name
        )));
    }

    if table.columns.is_empty() {
        return Err(PlatformError::Validation(format!(
            "Table '{}' must have at least one column",
            table.name
        )));
    }

    let mut primary_keys = table.columns.iter().filter(|c| c.primary_key);
    let addressed = match (primary_keys.next(), primary_keys.next(), reach) {
        (None, _, Reach::Served) => {
            return Err(PlatformError::Validation(format!(
                "Table '{}' must have a primary key column",
                table.name
            )))
        }
        (Some(_), Some(_), _) => {
            return Err(PlatformError::Validation(format!(
                "Table '{}' cannot have multiple primary keys",
                table.name
            )))
        }
        (pk, _, Reach::Served) => pk,
        (_, _, Reach::Private) => None,
    };

    if adopted {
        validate_adopted(table)?;
    }
    match addressed {
        Some(pk) if adopted && !is_key_type(&pk.column_type) => {
            return Err(PlatformError::Validation(format!(
                "Table '{}' primary key must be a UUID, a text, a varchar or an integer",
                table.name
            )))
        }
        Some(pk) if !adopted && pk.column_type != ColumnType::Uuid => {
            return Err(PlatformError::Validation(format!(
                "Table '{}' primary key must be UUID type",
                table.name
            )))
        }
        _ => {}
    }

    for col in &table.columns {
        if col.name.is_empty() {
            return Err(PlatformError::Validation(format!(
                "Column name cannot be empty in table '{}'",
                table.name
            )));
        }

        if !col.name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(PlatformError::Validation(format!(
                "Column name '{}' in table '{}' can only contain alphanumeric characters and underscores",
                col.name, table.name
            )));
        }

        if let Some(ref default) = col.default_value {
            if default.contains('(') {
                let lower = default.trim().to_lowercase();
                if !is_safe_default_function(&lower) {
                    return Err(PlatformError::Validation(format!(
                        "Default value '{}' for column '{}' in table '{}' uses an unsupported SQL function. \
                         Allowed: now(), gen_random_uuid(), current_timestamp",
                        default, col.name, table.name
                    )));
                }
            }
        }

        if let Some(ref fk_ref) = col.references {
            if let Some((ref_table, ref_col)) = fk_ref.split_once('.') {
                if !is_safe_identifier(ref_table) {
                    return Err(PlatformError::Validation(format!(
                        "Foreign key reference table '{}' in column '{}' of table '{}' is not a valid identifier",
                        ref_table, col.name, table.name
                    )));
                }
                if !is_safe_identifier(ref_col) {
                    return Err(PlatformError::Validation(format!(
                        "Foreign key reference column '{}' in column '{}' of table '{}' is not a valid identifier",
                        ref_col, col.name, table.name
                    )));
                }
            } else {
                return Err(PlatformError::Validation(format!(
                    "Foreign key reference '{}' in column '{}' must be in 'table.column' format",
                    fk_ref, col.name
                )));
            }
        }
    }

    if let Some(ref owner_field) = table.owner_field {
        if !table.columns.iter().any(|c| c.name == *owner_field) {
            return Err(PlatformError::Validation(format!(
                "owner_field '{}' in table '{}' does not match any column",
                owner_field, table.name
            )));
        }
    }

    if let Some(area) = table.scoped_by.as_deref() {
        validate_scoped_by(table, area)?;
    }

    validate_declarations(table).map_err(PlatformError::Validation)?;
    validate_listing(table, adopted).map_err(PlatformError::Validation)?;

    for index in &table.indexes {
        if !is_safe_identifier(&index.name) {
            return Err(PlatformError::Validation(format!(
                "Index name '{}' in table '{}' is not a valid identifier",
                index.name, table.name
            )));
        }
        for idx_col in &index.columns {
            if !is_safe_identifier(idx_col) {
                return Err(PlatformError::Validation(format!(
                    "Index column '{}' in index '{}' of table '{}' is not a valid identifier",
                    idx_col, index.name, table.name
                )));
            }
            if !table.columns.iter().any(|c| c.name == *idx_col) {
                return Err(PlatformError::Validation(format!(
                    "Index column '{}' in index '{}' does not exist in table '{}'",
                    idx_col, index.name, table.name
                )));
            }
        }
    }

    Ok(())
}

fn validate_scoped_by(table: &TableDefinition, area: &str) -> Result<(), PlatformError> {
    let invalid = |reason: String| {
        Err(PlatformError::Validation(format!(
            "Table '{}': scoped_by {reason}",
            table.name
        )))
    };
    if area.is_empty() {
        return invalid("cannot be empty".into());
    }
    if table.owner_field.is_none() {
        return invalid("requires owner_field (the column @own grants reach)".into());
    }
    if table.columns.iter().any(|c| c.name == "scope") {
        return invalid("reserves ?scope= for narrowing; rename the column 'scope'".into());
    }
    match table.permission_area.as_deref() {
        Some(permission_area) if permission_area != area => invalid(format!(
            "'{area}' differs from permission_area '{permission_area}'; omit permission_area or make them equal"
        )),
        _ => Ok(()),
    }
}

fn is_safe_default_function(lower: &str) -> bool {
    const ALLOWED: &[&str] = &[
        "now()",
        "gen_random_uuid()",
        "current_timestamp",
        "current_date",
        "current_time",
    ];
    ALLOWED.contains(&lower)
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{minimal_manifest, private_loans};
    use super::*;

    #[test]
    fn a_table_needs_exactly_one_uuid_primary_key() {
        let table = || minimal_manifest().tables.remove(0);
        assert!(validate(&table()).is_ok());

        let mut keyless = table();
        keyless.columns[0].primary_key = false;
        let err = validate(&keyless).expect_err("no key").to_string();
        assert!(err.contains("must have a primary key"), "{err}");

        let mut two_keys = table();
        let mut second = two_keys.columns[0].clone();
        second.name = "other_id".into();
        two_keys.columns.push(second);
        let err = validate(&two_keys).expect_err("two keys").to_string();
        assert!(err.contains("multiple primary keys"), "{err}");

        let mut text_key = table();
        text_key.columns[0].column_type = ColumnType::Text;
        let err = validate(&text_key).expect_err("text key").to_string();
        assert!(err.contains("must be UUID"), "{err}");
    }

    #[test]
    fn a_table_that_exists_may_have_the_key_it_has() {
        let keyed = |column_type: ColumnType, managed: bool| {
            let mut table = minimal_manifest().tables.remove(0);
            table.columns[0].column_type = column_type;
            table.managed = managed;
            table
        };
        for key in [
            ColumnType::Uuid,
            ColumnType::Text,
            ColumnType::Varchar { length: 50 },
            ColumnType::Integer,
            ColumnType::BigInteger,
        ] {
            assert!(validate(&keyed(key.clone(), false)).is_ok(), "{key:?}");
            let mut adopted = minimal_manifest();
            adopted.schema = SchemaOwnership::Adopted;
            adopted.tables = vec![keyed(key.clone(), true)];
            assert!(validate_all(&adopted).is_ok(), "{key:?}");
        }
        let err = validate(&keyed(ColumnType::Boolean, false))
            .expect_err("a boolean key")
            .to_string();
        assert!(
            err.contains("must be a UUID, a text, a varchar or an integer"),
            "{err}"
        );
        let err = validate(&keyed(ColumnType::Integer, true))
            .expect_err("a managed integer key")
            .to_string();
        assert!(err.contains("must be UUID"), "{err}");
    }

    #[test]
    fn a_table_that_exists_declares_nothing_the_platform_would_have_to_create() {
        let mut indexed = minimal_manifest().tables.remove(0);
        indexed.managed = false;
        indexed.indexes = vec![IndexDefinition {
            name: "items_by_id".into(),
            columns: vec!["id".into()],
            ..Default::default()
        }];
        let err = validate(&indexed).expect_err("indexes").to_string();
        assert!(err.contains("declares indexes"), "{err}");
        assert!(err.contains("SQL migration"), "{err}");

        let mut checked = minimal_manifest().tables.remove(0);
        checked.checks = vec![cufflink_types::CheckConstraint {
            name: "items_id_check".into(),
            expr: "id IS NOT NULL".into(),
        }];
        let mut adopted = minimal_manifest();
        adopted.schema = SchemaOwnership::Adopted;
        adopted.tables = vec![checked.clone()];
        let err = validate_all(&adopted).expect_err("checks").to_string();
        assert!(err.contains("declares checks"), "{err}");
        assert!(validate(&checked).is_ok(), "a managed table declares them");
    }

    #[test]
    fn a_search_vector_is_a_column_an_adopted_table_has_and_a_keyset_order_cannot_be_null() {
        use cufflink_types::SearchSpec;
        let vectored = |managed: bool| {
            let mut table = minimal_manifest().tables.remove(0);
            table.managed = managed;
            table.search = Some(SearchSpec {
                vector_column: Some("document".into()),
                ..Default::default()
            });
            table
        };
        let err = validate(&vectored(true)).expect_err("managed").to_string();
        assert!(err.contains("a table the platform makes"), "{err}");
        assert!(validate(&vectored(false)).is_ok());
        let mut declared = vectored(false);
        declared.columns.push(ColumnDefinition {
            name: "document".into(),
            column_type: ColumnType::Text,
            ..Default::default()
        });
        let err = validate(&declared).expect_err("declared").to_string();
        assert!(err.contains("is a declared column"), "{err}");

        let mut keyed = minimal_manifest().tables.remove(0);
        keyed.pagination = Pagination::Keyset;
        assert!(validate(&keyed).is_ok());
        keyed.columns.push(ColumnDefinition {
            name: "created_at".into(),
            column_type: ColumnType::Timestamp,
            nullable: true,
            ..Default::default()
        });
        let err = validate(&keyed).expect_err("nullable order").to_string();
        assert!(err.contains("'created_at', which may be null"), "{err}");
        keyed.columns[1].nullable = false;
        assert!(validate(&keyed).is_ok());
    }

    #[test]
    fn the_column_a_search_vector_is_kept_in_is_the_platforms_to_name() {
        use cufflink_types::SearchSpec;
        let named = |name: &str, search: bool, managed: bool| {
            let mut table = minimal_manifest().tables.remove(0);
            table.managed = managed;
            table.columns.push(ColumnDefinition {
                name: name.into(),
                column_type: ColumnType::Text,
                ..Default::default()
            });
            table.search = search.then(|| SearchSpec {
                columns: vec![name.into()],
                ..Default::default()
            });
            table
        };
        for name in ["_search", "_SEARCH"] {
            let err = validate(&named(name, true, true))
                .expect_err("reserved")
                .to_string();
            assert!(
                err.contains("is the column the platform keeps the search vector"),
                "{err}"
            );
        }
        assert!(validate(&named("title", true, true)).is_ok());
        assert!(
            validate(&named("_search", false, true)).is_ok(),
            "a table without a search keeps the column it always could have"
        );
        assert!(
            validate(&named("_search", true, false)).is_ok(),
            "no vector is kept for a table the platform did not make"
        );
    }

    #[test]
    fn scoped_by_needs_an_owner_field_and_a_matching_permission_area() {
        let table = || {
            let mut t = minimal_manifest().tables.remove(0);
            t.columns.push(ColumnDefinition {
                name: "user_id".into(),
                primary_key: false,
                auto_generate: false,
                ..t.columns[0].clone()
            });
            t.owner_field = Some("user_id".into());
            t.scoped_by = Some("items".into());
            t
        };
        assert!(validate(&table()).is_ok());
        assert!(validate(&TableDefinition {
            permission_area: Some("items".into()),
            ..table()
        })
        .is_ok());

        let reasons = [
            (
                TableDefinition {
                    owner_field: None,
                    ..table()
                },
                "requires owner_field",
            ),
            (
                TableDefinition {
                    permission_area: Some("other".into()),
                    ..table()
                },
                "differs from permission_area 'other'",
            ),
            (
                TableDefinition {
                    scoped_by: Some(String::new()),
                    ..table()
                },
                "cannot be empty",
            ),
            (
                {
                    let mut t = table();
                    t.columns.push(ColumnDefinition {
                        name: "scope".into(),
                        column_type: ColumnType::Text,
                        ..t.columns[1].clone()
                    });
                    t
                },
                "rename the column 'scope'",
            ),
        ];
        for (table, reason) in reasons {
            let err = validate(&table).expect_err(reason).to_string();
            assert!(err.contains(reason), "{err}");
        }
    }

    #[test]
    fn column_permissions_are_held_to_their_rules_at_deploy() {
        let restricted = |column: usize| {
            let mut table = minimal_manifest().tables.remove(0);
            table.permission_area = Some("items".into());
            table.columns.push(
                serde_json::from_value(serde_json::json!({
                    "name": "notes", "column_type": "text", "nullable": true
                }))
                .expect("column"),
            );
            table.columns[column].access = Some(cufflink_types::ColumnAccess {
                read: Some("internal".into()),
                write: None,
            });
            table
        };
        assert!(validate(&restricted(1)).is_ok());
        let err = validate(&restricted(0)).expect_err("key").to_string();
        assert!(
            err.contains("tables.items.columns.id.access: the primary key"),
            "{err}"
        );
        let private = TableDefinition {
            managed: false,
            permission_area: None,
            ..restricted(1)
        };
        let err = validate_private(&private, false)
            .expect_err("private")
            .to_string();
        assert!(
            err.contains("declares column permissions, which only data routes read"),
            "{err}"
        );
    }

    #[test]
    fn a_service_needs_a_table() {
        let mut manifest = minimal_manifest();
        assert!(validate_all(&manifest).is_ok());
        manifest.tables.clear();
        let err = validate_all(&manifest).expect_err("no tables").to_string();
        assert!(err.contains("at least one table"), "{err}");
        manifest.schema = SchemaOwnership::Adopted;
        assert!(validate_all(&manifest).is_ok());
    }

    #[test]
    fn declared_fields_are_validated_by_path() {
        use cufflink_types::{CheckConstraint, IndexMethod, SearchSpec};
        let table = || {
            let mut t = minimal_manifest().tables.remove(0);
            t.columns.push(ColumnDefinition {
                name: "price".into(),
                primary_key: false,
                auto_generate: false,
                column_type: ColumnType::Numeric {
                    precision: Some(12),
                    scale: Some(2),
                },
                ..t.columns[0].clone()
            });
            t.indexes.push(IndexDefinition {
                name: "price_positive".into(),
                columns: vec!["price".into()],
                predicate: Some("price > 0".into()),
                ..Default::default()
            });
            t.checks.push(CheckConstraint {
                name: "price_nonneg".into(),
                expr: "price >= 0".into(),
            });
            t.audit_redact = vec!["price".into(), "bank_*".into()];
            t
        };
        assert!(validate(&table()).is_ok());
        let mut searched = table();
        searched.search = Some(SearchSpec {
            columns: vec!["price".into()],
            ..Default::default()
        });
        assert!(validate(&searched).is_ok());

        let refusals: Vec<(TableDefinition, &str)> = vec![
            (
                {
                    let mut t = table();
                    t.columns[1].column_type = ColumnType::Numeric {
                        precision: Some(2),
                        scale: Some(5),
                    };
                    t
                },
                "tables.items.columns.price.scale",
            ),
            (
                {
                    let mut t = table();
                    t.indexes[0].predicate = Some("price > 0; DROP TABLE items".into());
                    t
                },
                "tables.items.indexes.price_positive.predicate",
            ),
            (
                {
                    let mut t = table();
                    t.indexes[0].unique = true;
                    t.indexes[0].method = IndexMethod::Gin;
                    t
                },
                "tables.items.indexes.price_positive.method",
            ),
            (
                {
                    let mut t = table();
                    t.checks[0].expr = "price >= 0 -- always".into();
                    t
                },
                "tables.items.checks[0].expr",
            ),
            (
                {
                    let mut t = table();
                    t.search = Some(SearchSpec::default());
                    t
                },
                "tables.items.search",
            ),
            (
                {
                    let mut t = table();
                    t.search = Some(SearchSpec {
                        columns: vec!["title".into()],
                        ..Default::default()
                    });
                    t
                },
                "tables.items.search: column 'title' is not declared",
            ),
            (
                {
                    let mut t = table();
                    t.audit_redact = vec!["*".into()];
                    t
                },
                "tables.items.audit_redact[0]",
            ),
        ];
        for (table, path) in refusals {
            let err = validate(&table).expect_err(path).to_string();
            assert!(err.contains(path), "'{err}' does not name {path}");
        }
    }

    #[test]
    fn an_index_accepted_before_is_still_accepted() {
        let mut table = minimal_manifest().tables.remove(0);
        table.indexes.push(IndexDefinition {
            name: "idx_items_a_name_longer_than_sixty_three_characters_is_still_accepted_here"
                .into(),
            columns: vec!["id".into()],
            ..Default::default()
        });
        assert!(validate(&table).is_ok());
    }

    fn declaring(private_tables: Vec<TableDefinition>) -> ServiceManifest {
        ServiceManifest {
            private_tables,
            ..minimal_manifest()
        }
    }

    #[test]
    fn a_private_table_declaring_hooks_is_refused() {
        use cufflink_types::{TableHooks, ValidationRule};
        type Declare = fn(&mut TableDefinition);
        let cases: [(&str, Declare); 2] = [
            ("hooks", |t| t.hooks = Some(TableHooks::default())),
            ("validations", |t| {
                t.columns[0].validations = vec![ValidationRule::MinLength { value: 1 }]
            }),
        ];
        for (name, declare) in cases {
            let mut table = private_loans();
            declare(&mut table);
            let refusal =
                cufflink_types::manifest::validate::validate_private_table(&table).expect_err(name);
            assert_eq!(
                refusal,
                format!("Private table 'loans' declares {name}, which only data routes read")
            );
            let served = ServiceManifest {
                tables: vec![table.clone()],
                ..minimal_manifest()
            };
            let as_served = validate_all(&served).map_err(|e| e.to_string());
            assert!(
                !as_served.is_err_and(|e| e.contains("Private table")),
                "{name}: the rule is a private table's alone"
            );
            let err = validate_all(&declaring(vec![table]))
                .expect_err(name)
                .to_string();
            assert!(err.contains(&refusal), "{err}");
        }
        let mut soft_deleted = private_loans();
        soft_deleted.soft_delete = true;
        assert!(validate_all(&declaring(vec![soft_deleted])).is_ok());
    }

    #[test]
    fn a_private_table_needs_no_primary_key() {
        let keyed = |keys: &[(&str, ColumnType)]| {
            let mut table = private_loans();
            for (name, column_type) in keys {
                let column = table
                    .columns
                    .iter_mut()
                    .find(|column| column.name == *name)
                    .expect("column");
                column.primary_key = true;
                column.column_type = column_type.clone();
            }
            table
        };
        assert!(validate_all(&declaring(vec![keyed(&[])])).is_ok());
        let err = validate(&keyed(&[])).expect_err("served").to_string();
        assert!(err.contains("must have a primary key"), "{err}");

        for key in [ColumnType::Boolean, ColumnType::Timestamp, ColumnType::Uuid] {
            let table = keyed(&[("owner", key.clone())]);
            assert!(validate_all(&declaring(vec![table])).is_ok(), "{key:?}");
        }
        let err = validate(&keyed(&[("owner", ColumnType::Boolean)]))
            .expect_err("served")
            .to_string();
        assert!(err.contains("must be a UUID, a text"), "{err}");

        let composite = keyed(&[("owner", ColumnType::Text), ("tag", ColumnType::Text)]);
        let err = validate_all(&declaring(vec![composite]))
            .expect_err("two flagged columns")
            .to_string();
        assert!(err.contains("cannot have multiple primary keys"), "{err}");
    }

    #[test]
    fn only_private_tables_is_a_valid_service() {
        let only = |schema| ServiceManifest {
            tables: vec![],
            schema,
            ..declaring(vec![private_loans()])
        };
        assert!(validate_all(&only(SchemaOwnership::Managed)).is_ok());
        assert!(validate_all(&only(SchemaOwnership::Adopted)).is_ok());
        let mut managed = private_loans();
        managed.managed = true;
        let adopted = ServiceManifest {
            tables: vec![],
            schema: SchemaOwnership::Adopted,
            ..declaring(vec![managed])
        };
        assert!(validate_all(&adopted).is_ok());
        let mut indexed = private_loans();
        indexed.indexes = vec![IndexDefinition {
            name: "loans_by_owner".into(),
            columns: vec!["owner".into()],
            ..Default::default()
        }];
        let err = validate_all(&declaring(vec![indexed]))
            .expect_err("an index the platform would never create")
            .to_string();
        assert!(err.contains("declares indexes"), "{err}");
    }

    #[test]
    fn owner_field_needs_scoped_by_on_a_private_table() {
        let mut owned = private_loans();
        owned.owner_field = Some("owner".into());
        let err = validate_all(&declaring(vec![owned.clone()]))
            .expect_err("owner_field alone")
            .to_string();
        assert!(
            err.contains("Private table 'loans' declares owner_field"),
            "{err}"
        );
        owned.scoped_by = Some("loans".into());
        assert!(validate_all(&declaring(vec![owned.clone()])).is_ok());
        owned.permission_area = Some("loans".into());
        assert!(validate_all(&declaring(vec![owned.clone()])).is_ok());
        owned.permission_area = Some("other".into());
        let err = validate_all(&declaring(vec![owned.clone()]))
            .expect_err("another area")
            .to_string();
        assert!(
            err.contains("Private table 'loans' declares permission_area"),
            "{err}"
        );
        owned.permission_area = None;
        owned.owner_field = None;
        let err = validate_all(&declaring(vec![owned]))
            .expect_err("scoped_by alone")
            .to_string();
        assert!(err.contains("scoped_by requires owner_field"), "{err}");
    }
}
