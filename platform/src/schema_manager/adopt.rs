use super::introspect::{LiveColumn, LiveRelation, LiveSchema, RelationKind, TypeKind};
use super::migration::SOFT_DELETE_COLUMN;
use crate::deployer::manifest::{ColumnDefinition, ColumnType, TableDefinition};
use crate::errors::{PlatformError, PlatformResult};
use cufflink_types::manifest::validate::folded;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, PoisonError, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdoptedColumn {
    #[serde(rename = "type")]
    pub type_name: String,
    pub typtype: TypeKind,
    pub nullable: bool,
    pub has_default: bool,
}

impl AdoptedColumn {
    fn of(live: &LiveColumn) -> Self {
        Self {
            type_name: live.type_name.clone(),
            typtype: live.kind,
            nullable: !live.not_null,
            has_default: live.has_default || live.generated,
        }
    }

    pub fn bound(&self, placeholder: &str) -> String {
        match self.typtype {
            TypeKind::Enum => format!("{placeholder}::text::{}", self.type_name),
            TypeKind::Domain | TypeKind::Base | TypeKind::Other => placeholder.to_string(),
        }
    }

    pub fn may_be_omitted(&self) -> bool {
        self.nullable || self.has_default
    }
}

pub type AdoptedTable = BTreeMap<String, AdoptedColumn>;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AdoptedSchema(BTreeMap<String, AdoptedTable>);

impl AdoptedSchema {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn table(&self, name: &str) -> Option<&AdoptedTable> {
        self.0.get(&folded(name))
    }

    pub fn column(&self, table: &str, column: &str) -> Option<&AdoptedColumn> {
        self.table(table)
            .and_then(|columns| columns.get(&folded(column)))
    }

    pub fn bound(&self, table: &str, column: &str, placeholder: String) -> String {
        match self.column(table, column) {
            Some(adopted) => adopted.bound(&placeholder),
            None => placeholder,
        }
    }

    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AdoptionReport {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub undeclared_columns: BTreeMap<String, usize>,
    #[serde(skip)]
    pub adopted: AdoptedSchema,
}

impl AdoptionReport {
    pub fn accepted(self) -> PlatformResult<Self> {
        if self.errors.is_empty() {
            Ok(self)
        } else {
            Err(PlatformError::Validation(format!(
                "the database does not hold what the manifest adopts: {}",
                self.errors.join("; ")
            )))
        }
    }
}

#[derive(Default)]
struct Findings {
    errors: Vec<String>,
    warnings: Vec<String>,
}

fn text_like(live: &LiveColumn) -> bool {
    live.holds(&ColumnType::Text) || live.holds(&ColumnType::Uuid)
}

fn column_findings(at: &str, declared: &ColumnDefinition, live: &LiveColumn) -> Findings {
    if !live.holds(&declared.column_type) {
        return Findings {
            errors: vec![format!(
                "{at} is {}, which a column declared {} cannot be",
                live.type_name,
                declared.column_type.to_sql()
            )],
            warnings: Vec::new(),
        };
    }
    let warning = match (declared.nullable, live.not_null) {
        (false, false) => Some(format!("{at} is declared NOT NULL and may be null")),
        (true, true) if !(live.has_default || live.generated) => Some(format!(
            "{at} is declared nullable, and is NOT NULL without a default: \
             creates that omit it will fail"
        )),
        _ => None,
    };
    Findings {
        errors: Vec::new(),
        warnings: warning.into_iter().collect(),
    }
}

fn key_error(at: &str, table: &TableDefinition, live: &LiveRelation) -> Option<String> {
    let declared = table.primary_key().map(|column| folded(&column.name));
    let key = live
        .constraints
        .iter()
        .find(|constraint| constraint.kind == "p")
        .map(|constraint| constraint.columns.as_slice());
    match (declared, key) {
        (None, _) => None,
        (Some(declared), None) => Some(format!(
            "{at} has no primary key; '{declared}' is declared as one"
        )),
        (Some(declared), Some([column])) if *column == declared => None,
        (Some(declared), Some([column])) => Some(format!(
            "{at} has the primary key '{column}', not the declared '{declared}'"
        )),
        (Some(declared), Some(columns)) => Some(format!(
            "{at} has the composite primary key ({}); '{declared}' alone is declared",
            columns.join(", ")
        )),
    }
}

fn owner_error(at: &str, table: &TableDefinition, live: &LiveRelation) -> Option<String> {
    let owner = table.owner_field.as_deref()?;
    let column = live.column(owner)?;
    (!text_like(column)).then(|| {
        format!(
            "{at}.{owner} is {}; an owner column is a uuid or a text",
            column.type_name
        )
    })
}

fn vector_error(at: &str, table: &TableDefinition, live: &LiveRelation) -> Option<String> {
    let vector = table.search.as_ref()?.vector_column.as_deref()?;
    match live.column(vector) {
        None => Some(format!(
            "{at}.{vector} is missing; search.vector_column names it"
        )),
        Some(column) if column.type_name != "tsvector" => Some(format!(
            "{at}.{vector} is {}; search.vector_column names a tsvector",
            column.type_name
        )),
        Some(_) => None,
    }
}

fn table_findings(
    schema: &str,
    table: &TableDefinition,
    live: &LiveRelation,
) -> (Findings, AdoptedTable) {
    let at = format!("{schema}.{}", table.name);
    let soft_delete = (table.soft_delete && live.column(SOFT_DELETE_COLUMN).is_none())
        .then(|| format!("{at} is declared soft_delete and has no column 'deleted_at'"));
    let start = Findings {
        errors: [
            key_error(&at, table, live),
            owner_error(&at, table, live),
            vector_error(&at, table, live),
            soft_delete,
        ]
        .into_iter()
        .flatten()
        .collect(),
        warnings: Vec::new(),
    };
    table.columns.iter().fold(
        (start, AdoptedTable::new()),
        |(mut findings, mut adopted), declared| {
            let at = format!("{at}.{}", declared.name);
            match live.column(&declared.name) {
                None => findings.errors.push(format!("{at} is missing")),
                Some(column) => {
                    let found = column_findings(&at, declared, column);
                    findings.errors.extend(found.errors);
                    findings.warnings.extend(found.warnings);
                    adopted.insert(folded(&declared.name), AdoptedColumn::of(column));
                }
            }
            (findings, adopted)
        },
    )
}

pub fn validate_adoption<'a>(
    schema: &str,
    tables: impl IntoIterator<Item = &'a TableDefinition>,
    live: &LiveSchema,
) -> AdoptionReport {
    if !live.exists {
        return AdoptionReport {
            errors: vec![format!("schema {schema} is missing")],
            ..AdoptionReport::default()
        };
    }
    tables
        .into_iter()
        .fold(AdoptionReport::default(), |mut report, table| {
            let at = format!("{schema}.{}", table.name);
            match live.relation(&table.name) {
                None => report.errors.push(format!("table {at} is missing")),
                Some(relation) if relation.kind != RelationKind::Table => {
                    report.errors.push(format!("{at} is not a table"))
                }
                Some(relation) => {
                    let (findings, adopted) = table_findings(schema, table, relation);
                    report.errors.extend(findings.errors);
                    report.warnings.extend(findings.warnings);
                    let undeclared = relation.columns.len().saturating_sub(adopted.len());
                    if undeclared > 0 {
                        report
                            .undeclared_columns
                            .insert(table.name.clone(), undeclared);
                    }
                    report.adopted.0.insert(folded(&table.name), adopted);
                }
            }
            report
        })
}

#[derive(Default)]
pub struct AdoptedSchemas {
    known: RwLock<HashMap<Uuid, (i32, Arc<AdoptedSchema>)>>,
}

impl AdoptedSchemas {
    fn cached(&self, service_id: Uuid, version: i32) -> Option<Arc<AdoptedSchema>> {
        self.known
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&service_id)
            .filter(|(known, _)| *known == version)
            .map(|(_, adopted)| Arc::clone(adopted))
    }

    pub async fn of(
        &self,
        pool: &deadpool_postgres::Pool,
        service: &str,
        service_id: Uuid,
        version: i32,
    ) -> PlatformResult<Arc<AdoptedSchema>> {
        if let Some(adopted) = self.cached(service_id, version) {
            return Ok(adopted);
        }
        let adopted = Arc::new(
            crate::repo::deployments::snapshot(pool, service, service_id, version)
                .await?
                .adopted,
        );
        self.known
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(service_id, (version, Arc::clone(&adopted)));
        Ok(adopted)
    }
}

#[cfg(test)]
mod tests {
    use super::super::introspect::fixtures::{column, primary_key, schema, table as live_table};
    use super::*;

    fn declared(name: &str, column_type: ColumnType) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type,
            ..Default::default()
        }
    }

    fn loans() -> TableDefinition {
        TableDefinition {
            name: "loans".into(),
            columns: vec![
                ColumnDefinition {
                    primary_key: true,
                    ..declared("id", ColumnType::Uuid)
                },
                declared("memo", ColumnType::Text),
                declared("loan_kind", ColumnType::Varchar { length: 50 }),
                ColumnDefinition {
                    nullable: true,
                    ..declared("created_at", ColumnType::TimestampNaive)
                },
            ],
            ..Default::default()
        }
    }

    fn live_loans(columns: Vec<LiveColumn>, key: &[&str]) -> LiveSchema {
        schema(vec![(
            "loans",
            LiveRelation {
                constraints: vec![primary_key("loans", key)],
                ..live_table(columns)
            },
        )])
    }

    fn as_a_legacy_schema_has_them() -> Vec<LiveColumn> {
        vec![
            column("id", "uuid"),
            column("book_id", "uuid"),
            column("memo", "text"),
            column("loan_kind", "character varying(50)"),
            LiveColumn {
                not_null: false,
                ..column("created_at", "timestamp without time zone")
            },
        ]
    }

    fn validate(table: &TableDefinition, live: &LiveSchema) -> AdoptionReport {
        validate_adoption("loan_service", [table], live)
    }

    #[test]
    fn a_table_the_database_has_as_declared_is_adopted_with_its_declared_columns() {
        let report = validate(
            &loans(),
            &live_loans(as_a_legacy_schema_has_them(), &["id"]),
        );
        assert_eq!(report.errors, Vec::<String>::new());
        assert_eq!(report.warnings, Vec::<String>::new());
        assert_eq!(report.undeclared_columns, [("loans".to_string(), 1)].into());
        assert_eq!(
            serde_json::to_value(&report.adopted).expect("json"),
            serde_json::json!({"loans": {
                "memo": {"type": "text", "typtype": "base", "nullable": false, "has_default": false},
                "created_at": {"type": "timestamp without time zone", "typtype": "base", "nullable": true, "has_default": false},
                "id": {"type": "uuid", "typtype": "base", "nullable": false, "has_default": false},
                "loan_kind": {"type": "character varying(50)", "typtype": "base", "nullable": false, "has_default": false}
            }})
        );
        assert_eq!(
            serde_json::to_value(&report).expect("json"),
            serde_json::json!({"errors": [], "warnings": [], "undeclared_columns": {"loans": 1}})
        );
        assert!(report.accepted().is_ok());
    }

    #[test]
    fn a_schema_a_table_or_a_column_that_is_missing_is_an_error() {
        let absent = validate(&loans(), &LiveSchema::default());
        assert_eq!(absent.errors, ["schema loan_service is missing"]);
        assert!(absent.adopted.is_empty());

        let empty = validate(&loans(), &schema(vec![]));
        assert_eq!(empty.errors, ["table loan_service.loans is missing"]);

        let view = schema(vec![("loans", LiveRelation::of(RelationKind::View))]);
        assert_eq!(
            validate(&loans(), &view).errors,
            ["loan_service.loans is not a table"]
        );

        let without_kind: Vec<LiveColumn> = as_a_legacy_schema_has_them()
            .into_iter()
            .filter(|c| c.name != "loan_kind")
            .collect();
        let report = validate(&loans(), &live_loans(without_kind, &["id"]));
        assert_eq!(report.errors, ["loan_service.loans.loan_kind is missing"]);
        let refused = report.accepted().expect_err("refused").to_string();
        assert!(
            refused.contains("loan_service.loans.loan_kind is missing"),
            "{refused}"
        );
    }

    #[test]
    fn a_column_of_another_type_is_an_error() {
        let mut columns = as_a_legacy_schema_has_them();
        columns[3] = column("loan_kind", "character varying(20)");
        columns[2] = column("memo", "jsonb");
        assert_eq!(
            validate(&loans(), &live_loans(columns, &["id"])).errors,
            [
                "loan_service.loans.memo is jsonb, which a column declared TEXT cannot be",
                "loan_service.loans.loan_kind is character varying(20), which a column declared \
                 VARCHAR(50) cannot be",
            ]
        );
    }

    #[test]
    fn text_is_declared_over_an_enum_or_a_domain_and_an_enum_is_bound_with_a_cast() {
        let table = TableDefinition {
            columns: vec![
                declared("kind", ColumnType::Text),
                declared("postcode", ColumnType::Text),
                declared("status", ColumnType::Text),
            ],
            ..loans()
        };
        let live = schema(vec![(
            "loans",
            live_table(vec![
                LiveColumn {
                    kind: TypeKind::Enum,
                    ..column("kind", "catalog_service.book_kind")
                },
                LiveColumn {
                    kind: TypeKind::Domain,
                    domain_base: Some("text".into()),
                    ..column("postcode", "catalog_service.postcode")
                },
                column("status", "character varying(50)"),
            ]),
        )]);
        let report = validate(&table, &live);
        assert_eq!(report.errors, Vec::<String>::new());
        let bound = |name| report.adopted.column("loans", name).map(|c| c.bound("$3"));
        assert_eq!(
            bound("kind").as_deref(),
            Some("$3::text::catalog_service.book_kind")
        );
        assert_eq!(bound("postcode").as_deref(), Some("$3"));
        assert_eq!(bound("status").as_deref(), Some("$3"));
        assert_eq!(bound("Kind"), bound("kind"));
        assert_eq!(bound("absent"), None);
    }

    #[test]
    fn the_declared_key_is_the_tables_own() {
        let keyed = |key: &[&str]| {
            validate(&loans(), &live_loans(as_a_legacy_schema_has_them(), key)).errors
        };
        assert_eq!(
            keyed(&["book_id"]),
            ["loan_service.loans has the primary key 'book_id', not the declared 'id'"]
        );
        assert_eq!(
            keyed(&["id", "book_id"]),
            ["loan_service.loans has the composite primary key (id, book_id); 'id' alone is declared"]
        );
        let unkeyed = schema(vec![("loans", live_table(as_a_legacy_schema_has_them()))]);
        assert_eq!(
            validate(&loans(), &unkeyed).errors,
            ["loan_service.loans has no primary key; 'id' is declared as one"]
        );
    }

    #[test]
    fn an_owner_column_is_a_uuid_or_a_text() {
        let owned = |owner: &str| TableDefinition {
            owner_field: Some(owner.into()),
            ..loans()
        };
        let live = |type_name: &str| {
            let mut columns = as_a_legacy_schema_has_them();
            columns.push(column("created_by", type_name));
            live_loans(columns, &["id"])
        };
        let with = |table: TableDefinition, type_name: &str| {
            let table = TableDefinition {
                columns: table
                    .columns
                    .into_iter()
                    .chain([declared(
                        "created_by",
                        match type_name {
                            "uuid" => ColumnType::Uuid,
                            "integer" => ColumnType::Integer,
                            _ => ColumnType::Text,
                        },
                    )])
                    .collect(),
                ..table
            };
            validate(&table, &live(type_name)).errors
        };
        assert!(with(owned("created_by"), "uuid").is_empty());
        assert!(with(owned("created_by"), "character varying(64)").is_empty());
        assert_eq!(
            with(owned("created_by"), "integer"),
            ["loan_service.loans.created_by is integer; an owner column is a uuid or a text"]
        );
    }

    #[test]
    fn nullability_that_differs_is_a_warning() {
        let mut columns = as_a_legacy_schema_has_them();
        columns[2].not_null = false;
        columns[4].not_null = true;
        let report = validate(&loans(), &live_loans(columns.clone(), &["id"]));
        assert_eq!(report.errors, Vec::<String>::new());
        assert_eq!(
            report.warnings,
            [
                "loan_service.loans.memo is declared NOT NULL and may be null",
                "loan_service.loans.created_at is declared nullable, and is NOT NULL without a \
                 default: creates that omit it will fail",
            ]
        );
        columns[4].has_default = true;
        assert_eq!(
            validate(&loans(), &live_loans(columns, &["id"])).warnings,
            ["loan_service.loans.memo is declared NOT NULL and may be null"]
        );
    }

    #[test]
    fn a_soft_delete_table_has_the_column_that_marks_its_rows() {
        let soft = TableDefinition {
            soft_delete: true,
            ..loans()
        };
        assert_eq!(
            validate(&soft, &live_loans(as_a_legacy_schema_has_them(), &["id"])).errors,
            ["loan_service.loans is declared soft_delete and has no column 'deleted_at'"]
        );
        let mut columns = as_a_legacy_schema_has_them();
        columns.push(column("deleted_at", "timestamp with time zone"));
        assert!(validate(&soft, &live_loans(columns, &["id"]))
            .errors
            .is_empty());
    }

    #[test]
    fn a_search_vector_is_a_tsvector_column_the_table_has() {
        let vectored = TableDefinition {
            search: Some(cufflink_types::SearchSpec {
                vector_column: Some("document".into()),
                ..Default::default()
            }),
            ..loans()
        };
        let with = |type_name: Option<&str>| {
            let mut columns = as_a_legacy_schema_has_them();
            columns.extend(type_name.map(|t| column("document", t)));
            validate(&vectored, &live_loans(columns, &["id"])).errors
        };
        assert!(with(Some("tsvector")).is_empty());
        assert_eq!(
            with(Some("text")),
            ["loan_service.loans.document is text; search.vector_column names a tsvector"]
        );
        assert_eq!(
            with(None),
            ["loan_service.loans.document is missing; search.vector_column names it"]
        );
    }

    #[test]
    fn a_column_may_be_omitted_when_the_database_fills_it() {
        let column = |nullable, has_default| AdoptedColumn {
            type_name: "integer".into(),
            typtype: TypeKind::Base,
            nullable,
            has_default,
        };
        assert!(column(true, false).may_be_omitted());
        assert!(column(false, true).may_be_omitted());
        assert!(!column(false, false).may_be_omitted());
        let generated = AdoptedColumn::of(&LiveColumn {
            generated: true,
            ..super::super::introspect::fixtures::column("total", "integer")
        });
        assert!(generated.may_be_omitted());
    }
}
