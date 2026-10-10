use cufflink::manifest::{
    CheckConstraint, ColumnType, IndexDefinition, IndexMethod, Pagination, SearchSpec,
    TableDefinition, ValidationRule,
};
use cufflink::prelude::*;
use serde_json::Value;

#[allow(dead_code)]
pub struct PgNumeric;
#[allow(dead_code)]
pub struct Decimal;

#[allow(dead_code)]
#[derive(Table)]
#[table(name = "everything")]
pub struct Everything {
    #[key]
    pub id: Uuid,
    pub text: String,
    #[column(varchar(50))]
    pub status: String,
    pub price: PgNumeric,
    pub cost: rust_decimal_like::Decimal,
    #[column(numeric(12, 2))]
    pub fee: PgNumeric,
    #[column(numeric(9))]
    pub weight: Decimal,
    #[column(numeric)]
    pub ratio: String,
    pub local_time: chrono::NaiveDateTime,
    pub day: chrono::NaiveDate,
    pub opens: chrono::NaiveTime,
    pub blob: Vec<u8>,
    pub tags: Vec<String>,
    pub owners: Vec<Uuid>,
    pub counts: Vec<i32>,
    pub totals: Vec<i64>,
    pub documents: Vec<Value>,
    pub maybe_tags: Option<Vec<String>>,
    pub json: Value,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}

mod rust_decimal_like {
    #[allow(dead_code)]
    pub struct Decimal;
}

fn column_type(table: &TableDefinition, name: &str) -> ColumnType {
    table
        .columns
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.column_type.clone())
        .unwrap_or_else(|| panic!("no column {name}"))
}

#[test]
fn rust_types_map_to_column_types() {
    let table = Everything::__cufflink_table_def();
    let numeric = |precision, scale| ColumnType::Numeric { precision, scale };
    for (column, expected) in [
        ("id", ColumnType::Uuid),
        ("text", ColumnType::Text),
        ("status", ColumnType::Varchar { length: 50 }),
        ("price", numeric(None, None)),
        ("cost", numeric(None, None)),
        ("fee", numeric(Some(12), Some(2))),
        ("weight", numeric(Some(9), None)),
        ("ratio", numeric(None, None)),
        ("local_time", ColumnType::TimestampNaive),
        ("day", ColumnType::Date),
        ("opens", ColumnType::Time),
        ("blob", ColumnType::Bytea),
        ("tags", ColumnType::TextArray),
        ("owners", ColumnType::UuidArray),
        ("counts", ColumnType::IntegerArray),
        ("totals", ColumnType::BigIntegerArray),
        ("documents", ColumnType::JsonbArray),
        ("maybe_tags", ColumnType::TextArray),
        ("json", ColumnType::Jsonb),
        ("created_at", ColumnType::Timestamp),
    ] {
        assert_eq!(column_type(&table, column), expected, "{column}");
    }
    let nullable = |name: &str| {
        table
            .columns
            .iter()
            .find(|c| c.name == name)
            .is_some_and(|c| c.nullable)
    };
    assert!(nullable("maybe_tags"));
    assert!(!nullable("tags"));
    assert_eq!(
        table.derived_features().collect::<Vec<_>>(),
        ["column_types_v2"]
    );
}

#[allow(dead_code)]
#[derive(Table)]
#[table(
    name = "plain",
    auth_required,
    permission_area = "plain",
    some_future_key = "x"
)]
pub struct Plain {
    #[key]
    pub id: Uuid,
    #[index]
    #[validate("min_length=1", "one_of=a,b")]
    pub sku: String,
    #[validate("regex=^[a-z]+$")]
    pub slug: String,
}

#[test]
fn a_table_without_the_new_keys_is_unchanged() {
    let table = Plain::__cufflink_table_def();
    assert_eq!(
        serde_json::to_value(&table).expect("serializes"),
        serde_json::json!({
            "name": "plain",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
                 "auto_generate": true, "default_value": null, "unique": false},
                {"name": "sku", "column_type": "text", "primary_key": false, "nullable": false,
                 "auto_generate": false, "default_value": null, "unique": false,
                 "validations": [{"rule": "min_length", "value": 1}]},
                {"name": "slug", "column_type": "text", "primary_key": false, "nullable": false,
                 "auto_generate": false, "default_value": null, "unique": false,
                 "validations": [{"rule": "regex", "pattern": "^[a-z]+$"}]}
            ],
            "indexes": [{"name": "idx_plain_sku", "columns": ["sku"], "unique": false}],
            "soft_delete": false,
            "auth_required": true,
            "permission_area": "plain",
            "owner_self_service": false
        })
    );
    assert_eq!(table.derived_features().count(), 0);
}

#[allow(dead_code)]
#[derive(Table)]
#[table(
    name = "book",
    managed = false,
    search = ["name", "email"],
    search_vector = "search_vector",
    pagination = keyset,
    audit_redact = ["dob", "bank_*"],
    sensitive,
    check(name = "price_nonneg", expr = "price_cents >= 0"),
    check(expr = "email <> ''", name = "email_present"),
    index(name = "uq_branch_email", columns = ["branch_id", "email"], unique, where = "deleted_at IS NULL", using = "btree"),
    index(name = "book_services_gin", columns = ["services"], using = "gin", unique = false)
)]
pub struct Book {
    #[key]
    pub id: Uuid,
    pub branch_id: Uuid,
    pub name: String,
    pub email: String,
    #[index]
    pub price_cents: i64,
    #[validate(one_of("new", "won", "lost"), "max_length=10")]
    pub status: String,
    pub services: Vec<String>,
    pub search_vector: String,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[test]
fn table_keys_reach_the_manifest() {
    let table = Book::__cufflink_table_def();
    assert!(!table.managed);
    assert!(table.sensitive);
    assert_eq!(table.pagination, Pagination::Keyset);
    assert_eq!(table.audit_redact, ["dob", "bank_*"]);
    assert_eq!(
        table.search,
        Some(SearchSpec {
            columns: vec!["name".into(), "email".into()],
            config: "simple".into(),
            vector_column: Some("search_vector".into()),
        })
    );
    assert_eq!(
        table.checks,
        [
            CheckConstraint {
                name: "price_nonneg".into(),
                expr: "price_cents >= 0".into()
            },
            CheckConstraint {
                name: "email_present".into(),
                expr: "email <> ''".into()
            },
        ]
    );
    assert_eq!(
        table.indexes,
        [
            IndexDefinition {
                name: "idx_book_price_cents".into(),
                columns: vec!["price_cents".into()],
                ..Default::default()
            },
            IndexDefinition {
                name: "uq_branch_email".into(),
                columns: vec!["branch_id".into(), "email".into()],
                unique: true,
                predicate: Some("deleted_at IS NULL".into()),
                method: IndexMethod::Btree,
            },
            IndexDefinition {
                name: "book_services_gin".into(),
                columns: vec!["services".into()],
                unique: false,
                predicate: None,
                method: IndexMethod::Gin,
            },
        ]
    );
    let status = table
        .columns
        .iter()
        .find(|c| c.name == "status")
        .expect("status");
    assert_eq!(
        status.validations,
        [
            ValidationRule::OneOf {
                values: vec!["new".into(), "won".into(), "lost".into()]
            },
            ValidationRule::MaxLength { value: 10 },
        ]
    );
    assert_eq!(
        table.derived_features().collect::<Vec<_>>(),
        [
            "schema_adopted",
            "column_types_v2",
            "audit_redaction",
            "keyset_lists"
        ]
    );
}
