use crate::features;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<IndexDefinition>,
    #[serde(default)]
    pub soft_delete: bool,
    /// Column name that stores the owner's user ID for row-level security.
    /// When set, CRUD operations are scoped to the authenticated user's records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_field: Option<String>,
    /// When true, all CRUD operations on this table require authentication.
    /// Unauthenticated requests receive a 401 Unauthorized response.
    #[serde(default)]
    pub auth_required: bool,
    /// Permission area that controls access to this table's CRUD operations.
    /// When set, platform checks "{area}:view" for GET, "{area}:create" for POST,
    /// "{area}:edit" for PUT, "{area}:delete" for DELETE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_area: Option<String>,
    /// Self-service ownership: when true (requires `owner_field`), owning a row
    /// authorizes access to it — the row-level scoping by `owner_field` IS the
    /// authorization, so the `permission_area` grant is NOT required for these
    /// (inherently owner-scoped) CRUD operations. Use for user-owned data a user
    /// manages themselves (their items, addresses, orders). Defaults false, so
    /// existing tables keep requiring `permission_area`; cross-owner/staff access
    /// stays gated by RBAC (typically via dedicated handlers, not owner-scoped CRUD).
    #[serde(default)]
    pub owner_self_service: bool,
    /// CRUD lifecycle hooks — WASM handler names called before/after operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<TableHooks>,
    /// Scoped permissions: the permission area whose grants' scopes
    /// (`@own`, `@field(column=claim)`, `@all`) select the rows a caller
    /// reaches. Requires `owner_field` (the `@own` column); `permission_area`
    /// defaults to it. Without it the table behaves as before scopes existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scoped_by: Option<String>,
    /// Whether the platform owns the table's DDL. An unmanaged table
    /// exists already: it is validated against the database, never created,
    /// altered or dropped.
    #[serde(
        default = "crate::serde_util::yes",
        skip_serializing_if = "crate::serde_util::is_yes"
    )]
    pub managed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<CheckConstraint>,
    /// What `?search=` matches; every text column by pattern when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchSpec>,
    /// Columns whose values audit records replace: a name, or `prefix*`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_redact: Vec<String>,
    /// Reads of the table are audited as well as writes.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sensitive: bool,
    #[serde(default, skip_serializing_if = "Pagination::is_offset")]
    pub pagination: Pagination,
}

impl Default for TableDefinition {
    fn default() -> Self {
        Self {
            name: String::new(),
            columns: Vec::new(),
            indexes: Vec::new(),
            soft_delete: false,
            owner_field: None,
            auth_required: false,
            permission_area: None,
            owner_self_service: false,
            hooks: None,
            scoped_by: None,
            managed: true,
            checks: Vec::new(),
            search: None,
            audit_redact: Vec::new(),
            sensitive: false,
            pagination: Pagination::default(),
        }
    }
}

/// How a list is paged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pagination {
    /// By page number.
    #[default]
    Offset,
    /// By a cursor over the sort keys.
    Keyset,
}

impl Pagination {
    pub fn is_offset(&self) -> bool {
        *self == Pagination::Offset
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchSpec {
    /// The columns searched.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// The text search configuration.
    #[serde(
        default = "SearchSpec::default_config",
        skip_serializing_if = "SearchSpec::is_default_config"
    )]
    pub config: String,
    /// An existing `tsvector` column of an unmanaged table, searched as
    /// it is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_column: Option<String>,
}

impl SearchSpec {
    pub const DEFAULT_CONFIG: &'static str = "simple";

    pub fn default_config() -> String {
        Self::DEFAULT_CONFIG.to_string()
    }

    pub fn is_default_config(config: &str) -> bool {
        config == Self::DEFAULT_CONFIG
    }
}

impl Default for SearchSpec {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            config: Self::default_config(),
            vector_column: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: String,
    /// The SQL condition every row satisfies.
    pub expr: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub column_type: ColumnType,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub auto_generate: bool,
    pub default_value: Option<String>,
    /// Foreign key reference in "table.column" format
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<String>,
    /// Whether to cascade deletes on the FK
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_delete: Option<ForeignKeyAction>,
    /// Whether this column has a unique constraint
    #[serde(default)]
    pub unique: bool,
    /// Validation rules for this column
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validations: Vec<ValidationRule>,
    /// The grants the data routes ask of a caller for the column's values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<ColumnAccess>,
}

/// The operations of the table's permission area a caller is granted to
/// read a column's values through the data routes, and to write them.
/// Owning the row grants neither.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnAccess {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<String>,
}

impl ColumnAccess {
    /// The access with each operation as the permission of `area` a role
    /// grants: `{area}:{operation}`.
    pub fn in_area(&self, area: &str) -> ColumnAccess {
        let permission = |operation: &Option<String>| {
            operation
                .as_ref()
                .map(|operation| format!("{area}:{operation}"))
        };
        ColumnAccess {
            read: permission(&self.read),
            write: permission(&self.write),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum ValidationRule {
    /// Regex pattern match
    Regex { pattern: String },
    /// Minimum numeric value
    Min { value: f64 },
    /// Maximum numeric value
    Max { value: f64 },
    /// Minimum string length
    MinLength { value: usize },
    /// Maximum string length
    MaxLength { value: usize },
    /// Must be one of the listed values
    OneOf { values: Vec<String> },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDefinition {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
    /// The SQL condition of a partial index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMethod::is_btree")]
    pub method: IndexMethod,
}

impl IndexDefinition {
    /// Whether the index has a shape only a `column_types_v2` platform
    /// builds. An index over several columns, unique or not, is not one:
    /// platforms built them before the feature existed.
    pub fn is_v2(&self) -> bool {
        self.predicate.is_some() || !self.method.is_btree()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexMethod {
    #[default]
    Btree,
    Gin,
    Gist,
    Hash,
    Brin,
}

impl IndexMethod {
    pub fn is_btree(&self) -> bool {
        *self == IndexMethod::Btree
    }

    pub fn as_sql(self) -> &'static str {
        match self {
            IndexMethod::Btree => "btree",
            IndexMethod::Gin => "gin",
            IndexMethod::Gist => "gist",
            IndexMethod::Hash => "hash",
            IndexMethod::Brin => "brin",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForeignKeyAction {
    Cascade,
    SetNull,
    Restrict,
    NoAction,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Uuid,
    #[default]
    Text,
    Integer,
    BigInteger,
    Float,
    Double,
    Boolean,
    Timestamp,
    Date,
    Jsonb,
    #[serde(rename = "numeric")]
    Numeric {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        precision: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scale: Option<u16>,
    },
    #[serde(rename = "varchar")]
    Varchar {
        length: u32,
    },
    #[serde(rename = "timestamp_naive")]
    TimestampNaive,
    #[serde(rename = "time")]
    Time,
    #[serde(rename = "bytea")]
    Bytea,
    #[serde(rename = "text_array")]
    TextArray,
    #[serde(rename = "uuid_array")]
    UuidArray,
    #[serde(rename = "integer_array")]
    IntegerArray,
    #[serde(rename = "biginteger_array")]
    BigIntegerArray,
    #[serde(rename = "jsonb_array")]
    JsonbArray,
}

impl ColumnType {
    pub const MAX_NUMERIC_PRECISION: u16 = 1000;
    pub const MAX_VARCHAR_LENGTH: u32 = 10_485_760;

    pub fn to_sql(&self) -> Cow<'static, str> {
        match self {
            ColumnType::Uuid => "UUID".into(),
            ColumnType::Text => "TEXT".into(),
            ColumnType::Integer => "INTEGER".into(),
            ColumnType::BigInteger => "BIGINT".into(),
            ColumnType::Float => "REAL".into(),
            ColumnType::Double => "DOUBLE PRECISION".into(),
            ColumnType::Boolean => "BOOLEAN".into(),
            ColumnType::Timestamp => "TIMESTAMPTZ".into(),
            ColumnType::Date => "DATE".into(),
            ColumnType::Jsonb => "JSONB".into(),
            ColumnType::Numeric {
                precision: Some(precision),
                scale: Some(scale),
            } => format!("NUMERIC({precision},{scale})").into(),
            ColumnType::Numeric {
                precision: Some(precision),
                scale: None,
            } => format!("NUMERIC({precision})").into(),
            ColumnType::Numeric {
                precision: None, ..
            } => "NUMERIC".into(),
            ColumnType::Varchar { length } => format!("VARCHAR({length})").into(),
            ColumnType::TimestampNaive => "TIMESTAMP".into(),
            ColumnType::Time => "TIME".into(),
            ColumnType::Bytea => "BYTEA".into(),
            ColumnType::TextArray => "TEXT[]".into(),
            ColumnType::UuidArray => "UUID[]".into(),
            ColumnType::IntegerArray => "INTEGER[]".into(),
            ColumnType::BigIntegerArray => "BIGINT[]".into(),
            ColumnType::JsonbArray => "JSONB[]".into(),
        }
    }

    /// Whether the type needs a `column_types_v2` platform.
    pub fn is_v2(&self) -> bool {
        !matches!(
            self,
            ColumnType::Uuid
                | ColumnType::Text
                | ColumnType::Integer
                | ColumnType::BigInteger
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::Boolean
                | ColumnType::Timestamp
                | ColumnType::Date
                | ColumnType::Jsonb
        )
    }

    /// The `format_type()` spellings of an existing column this type may be
    /// declared over, without their type modifiers (`numeric(12,2)` is
    /// `numeric`).
    pub fn pg_type_names(&self) -> &'static [&'static str] {
        match self {
            ColumnType::Uuid => &["uuid"],
            ColumnType::Text => &["text", "character varying", "character", "citext"],
            ColumnType::Integer => &["integer"],
            ColumnType::BigInteger => &["bigint"],
            ColumnType::Float => &["real"],
            ColumnType::Double => &["double precision"],
            ColumnType::Boolean => &["boolean"],
            ColumnType::Timestamp => &["timestamp with time zone"],
            ColumnType::Date => &["date"],
            ColumnType::Jsonb => &["jsonb"],
            ColumnType::Numeric { .. } => &["numeric"],
            ColumnType::Varchar { .. } => &["character varying"],
            ColumnType::TimestampNaive => &["timestamp without time zone"],
            ColumnType::Time => &["time without time zone"],
            ColumnType::Bytea => &["bytea"],
            ColumnType::TextArray => &["text[]", "character varying[]"],
            ColumnType::UuidArray => &["uuid[]"],
            ColumnType::IntegerArray => &["integer[]"],
            ColumnType::BigIntegerArray => &["bigint[]"],
            ColumnType::JsonbArray => &["jsonb[]"],
        }
    }

    /// The type's name in filter errors.
    pub fn filter_type(&self) -> &str {
        match self {
            ColumnType::Uuid => "uuid",
            ColumnType::Text | ColumnType::Varchar { .. } => "text",
            ColumnType::Integer | ColumnType::BigInteger => "integer",
            ColumnType::Float | ColumnType::Double => "double",
            ColumnType::Boolean => "boolean",
            ColumnType::Timestamp | ColumnType::TimestampNaive => "timestamp",
            ColumnType::Date => "date",
            ColumnType::Jsonb => "jsonb",
            ColumnType::Numeric { .. } => "numeric",
            ColumnType::Time => "time",
            ColumnType::Bytea => "bytea",
            ColumnType::TextArray
            | ColumnType::UuidArray
            | ColumnType::IntegerArray
            | ColumnType::BigIntegerArray
            | ColumnType::JsonbArray => "array",
        }
    }
}

/// CRUD lifecycle hooks for a table. Each field names a WASM handler function.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableHooks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_create: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_create: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_update: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_update: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_delete: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_delete: Option<String>,
}

impl TableDefinition {
    /// Get the primary key column
    pub fn primary_key(&self) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|c| c.primary_key)
    }

    /// Get all non-primary-key, non-auto columns (for INSERT/UPDATE)
    pub fn writable_columns(&self) -> Vec<&ColumnDefinition> {
        self.columns
            .iter()
            .filter(|c| !c.auto_generate || !c.primary_key)
            .collect()
    }

    /// The permission area whose operations the table's column permissions
    /// name.
    pub fn access_area(&self) -> Option<&str> {
        self.scoped_by
            .as_deref()
            .or(self.permission_area.as_deref())
    }

    /// The columns that ask a grant of a caller, each with what it asks.
    pub fn restricted_columns(&self) -> impl Iterator<Item = (&ColumnDefinition, &ColumnAccess)> {
        self.columns
            .iter()
            .filter_map(|column| column.access.as_ref().map(|access| (column, access)))
    }

    /// The permissions `column` asks of a caller, where it asks any.
    pub fn permissions_of(&self, column: &ColumnDefinition) -> Option<ColumnAccess> {
        column
            .access
            .as_ref()
            .zip(self.access_area())
            .map(|(access, area)| access.in_area(area))
    }

    /// Whether some callers are answered rows without `column`.
    pub fn withholds(&self, column: &ColumnDefinition) -> bool {
        self.permissions_of(column)
            .is_some_and(|permissions| permissions.read.is_some())
    }

    /// Platform features this table's declaration requires.
    pub fn derived_features(&self) -> impl Iterator<Item = &'static str> {
        let types_v2 = !self.checks.is_empty()
            || self.search.is_some()
            || self.indexes.iter().any(IndexDefinition::is_v2)
            || self.columns.iter().any(|c| c.column_type.is_v2());
        let audit_redaction = !self.audit_redact.is_empty() || self.sensitive;
        [
            self.scoped_by
                .is_some()
                .then_some(features::SCOPED_PERMISSIONS),
            (!self.managed).then_some(features::SCHEMA_ADOPTED),
            types_v2.then_some(features::COLUMN_TYPES_V2),
            audit_redaction.then_some(features::AUDIT_REDACTION),
            (!self.pagination.is_offset()).then_some(features::KEYSET_LISTS),
            self.restricted_columns()
                .next()
                .map(|_| features::COLUMN_PERMISSIONS),
        ]
        .into_iter()
        .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::fixtures::sample_manifest;

    #[test]
    fn test_primary_key() {
        let manifest = sample_manifest();
        let table = manifest.get_table("todos").unwrap();
        let pk = table.primary_key().unwrap();
        assert_eq!(pk.name, "id");
        assert!(pk.auto_generate);
    }

    #[test]
    fn test_column_type_to_sql() {
        assert_eq!(ColumnType::Uuid.to_sql(), "UUID");
        assert_eq!(ColumnType::Text.to_sql(), "TEXT");
        assert_eq!(ColumnType::Integer.to_sql(), "INTEGER");
        assert_eq!(ColumnType::BigInteger.to_sql(), "BIGINT");
        assert_eq!(ColumnType::Boolean.to_sql(), "BOOLEAN");
        assert_eq!(ColumnType::Timestamp.to_sql(), "TIMESTAMPTZ");
        assert_eq!(ColumnType::Date.to_sql(), "DATE");
        assert_eq!(ColumnType::Jsonb.to_sql(), "JSONB");
    }

    fn v2_types() -> Vec<(ColumnType, serde_json::Value, &'static str)> {
        use serde_json::json;
        vec![
            (
                ColumnType::Numeric {
                    precision: Some(12),
                    scale: Some(2),
                },
                json!({"numeric": {"precision": 12, "scale": 2}}),
                "NUMERIC(12,2)",
            ),
            (
                ColumnType::Numeric {
                    precision: Some(9),
                    scale: None,
                },
                json!({"numeric": {"precision": 9}}),
                "NUMERIC(9)",
            ),
            (
                ColumnType::Numeric {
                    precision: None,
                    scale: None,
                },
                json!({"numeric": {}}),
                "NUMERIC",
            ),
            (
                ColumnType::Varchar { length: 50 },
                json!({"varchar": {"length": 50}}),
                "VARCHAR(50)",
            ),
            (
                ColumnType::TimestampNaive,
                json!("timestamp_naive"),
                "TIMESTAMP",
            ),
            (ColumnType::Time, json!("time"), "TIME"),
            (ColumnType::Bytea, json!("bytea"), "BYTEA"),
            (ColumnType::TextArray, json!("text_array"), "TEXT[]"),
            (ColumnType::UuidArray, json!("uuid_array"), "UUID[]"),
            (
                ColumnType::IntegerArray,
                json!("integer_array"),
                "INTEGER[]",
            ),
            (
                ColumnType::BigIntegerArray,
                json!("biginteger_array"),
                "BIGINT[]",
            ),
            (ColumnType::JsonbArray, json!("jsonb_array"), "JSONB[]"),
        ]
    }

    #[test]
    fn v2_column_types_have_pinned_json_and_sql() {
        for (column_type, wire, sql) in v2_types() {
            assert_eq!(serde_json::to_value(&column_type).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<ColumnType>(wire).unwrap(),
                column_type
            );
            assert_eq!(column_type.to_sql(), sql);
            assert!(column_type.is_v2(), "{column_type:?}");
            assert!(!column_type.pg_type_names().is_empty());
        }
    }

    #[test]
    fn legacy_column_types_are_unchanged() {
        for (column_type, wire) in [
            (ColumnType::Uuid, "uuid"),
            (ColumnType::Text, "text"),
            (ColumnType::Integer, "integer"),
            (ColumnType::BigInteger, "biginteger"),
            (ColumnType::Float, "float"),
            (ColumnType::Double, "double"),
            (ColumnType::Boolean, "boolean"),
            (ColumnType::Timestamp, "timestamp"),
            (ColumnType::Date, "date"),
            (ColumnType::Jsonb, "jsonb"),
        ] {
            assert_eq!(
                serde_json::to_value(&column_type).unwrap(),
                serde_json::json!(wire)
            );
            assert!(!column_type.is_v2(), "{column_type:?}");
        }
        assert!(serde_json::from_str::<ColumnType>(r#""numeric""#).is_err());
        assert!(serde_json::from_str::<ColumnType>(r#""interval""#).is_err());
    }

    #[test]
    fn a_numeric_scale_needs_a_precision_to_reach_sql() {
        let scale_only = ColumnType::Numeric {
            precision: None,
            scale: Some(2),
        };
        assert_eq!(scale_only.to_sql(), "NUMERIC");
    }

    fn column(name: &str, column_type: ColumnType) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type,
            primary_key: false,
            nullable: true,
            auto_generate: false,
            default_value: None,
            references: None,
            on_delete: None,
            unique: false,
            validations: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn r3_table_fields_are_omitted_at_their_defaults() {
        let table = TableDefinition {
            name: "notes".into(),
            ..Default::default()
        };
        assert!(table.managed);
        assert_eq!(
            serde_json::to_value(&table).unwrap(),
            serde_json::json!({
                "name": "notes",
                "columns": [],
                "soft_delete": false,
                "auth_required": false,
                "owner_self_service": false
            })
        );
        let parsed: TableDefinition =
            serde_json::from_str(r#"{"name":"notes","columns":[]}"#).unwrap();
        assert!(parsed.managed);
        assert_eq!(parsed.pagination, Pagination::Offset);
        assert_eq!(table.derived_features().count(), 0);
    }

    #[test]
    fn r3_table_fields_round_trip() {
        let table = TableDefinition {
            name: "book".into(),
            indexes: vec![IndexDefinition {
                name: "uq_org_email".into(),
                columns: vec!["branch_id".into(), "email".into()],
                unique: true,
                predicate: Some("deleted_at IS NULL".into()),
                method: IndexMethod::Gin,
            }],
            managed: false,
            checks: vec![CheckConstraint {
                name: "price_nonneg".into(),
                expr: "price_cents >= 0".into(),
            }],
            search: Some(SearchSpec {
                columns: vec!["name".into(), "email".into()],
                config: "english".into(),
                vector_column: Some("search_vector".into()),
            }),
            audit_redact: vec!["dob".into(), "bank_*".into()],
            sensitive: true,
            pagination: Pagination::Keyset,
            ..Default::default()
        };
        let wire = serde_json::to_value(&table).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "name": "book",
                "columns": [],
                "indexes": [{
                    "name": "uq_org_email",
                    "columns": ["branch_id", "email"],
                    "unique": true,
                    "predicate": "deleted_at IS NULL",
                    "method": "gin"
                }],
                "soft_delete": false,
                "auth_required": false,
                "owner_self_service": false,
                "managed": false,
                "checks": [{"name": "price_nonneg", "expr": "price_cents >= 0"}],
                "search": {
                    "columns": ["name", "email"],
                    "config": "english",
                    "vector_column": "search_vector"
                },
                "audit_redact": ["dob", "bank_*"],
                "sensitive": true,
                "pagination": "keyset"
            })
        );
        let parsed: TableDefinition = serde_json::from_value(wire).unwrap();
        assert!(!parsed.managed);
        assert_eq!(parsed.indexes, table.indexes);
        assert_eq!(parsed.checks, table.checks);
        assert_eq!(parsed.search, table.search);
        assert_eq!(parsed.audit_redact, table.audit_redact);
        assert!(parsed.sensitive);
        assert_eq!(parsed.pagination, Pagination::Keyset);
    }

    #[test]
    fn a_default_search_config_is_omitted() {
        let search = SearchSpec {
            columns: vec!["name".into()],
            ..Default::default()
        };
        let wire = serde_json::json!({"columns": ["name"]});
        assert_eq!(serde_json::to_value(&search).unwrap(), wire);
        assert_eq!(serde_json::from_value::<SearchSpec>(wire).unwrap(), search);
    }

    #[test]
    fn a_legacy_index_is_unchanged() {
        let index = IndexDefinition {
            name: "idx_items_sku".into(),
            columns: vec!["sku".into()],
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&index).unwrap(),
            serde_json::json!({"name": "idx_items_sku", "columns": ["sku"], "unique": false})
        );
        assert!(!index.is_v2());
    }

    #[test]
    fn an_index_over_several_columns_is_as_legacy_as_one_over_one() {
        for unique in [false, true] {
            let index = IndexDefinition {
                name: "idx_items_branch_sku".into(),
                columns: vec!["branch_id".into(), "sku".into()],
                unique,
                ..Default::default()
            };
            assert!(!index.is_v2(), "unique={unique}");
            let table = TableDefinition {
                name: "items".into(),
                indexes: vec![index],
                ..Default::default()
            };
            assert_eq!(table.derived_features().count(), 0, "unique={unique}");
        }
    }

    #[test]
    fn each_r3_table_field_demands_its_feature() {
        let plain = || TableDefinition {
            name: "t".into(),
            ..Default::default()
        };
        let index = |index: IndexDefinition| TableDefinition {
            indexes: vec![index],
            ..plain()
        };
        let cases = [
            (
                TableDefinition {
                    managed: false,
                    ..plain()
                },
                features::SCHEMA_ADOPTED,
            ),
            (
                TableDefinition {
                    checks: vec![CheckConstraint {
                        name: "c".into(),
                        expr: "a > 0".into(),
                    }],
                    ..plain()
                },
                features::COLUMN_TYPES_V2,
            ),
            (
                TableDefinition {
                    search: Some(SearchSpec::default()),
                    ..plain()
                },
                features::COLUMN_TYPES_V2,
            ),
            (
                TableDefinition {
                    columns: vec![column("tags", ColumnType::TextArray)],
                    ..plain()
                },
                features::COLUMN_TYPES_V2,
            ),
            (
                index(IndexDefinition {
                    name: "i".into(),
                    columns: vec!["a".into()],
                    predicate: Some("a IS NOT NULL".into()),
                    ..Default::default()
                }),
                features::COLUMN_TYPES_V2,
            ),
            (
                index(IndexDefinition {
                    name: "i".into(),
                    columns: vec!["a".into()],
                    method: IndexMethod::Brin,
                    ..Default::default()
                }),
                features::COLUMN_TYPES_V2,
            ),
            (
                TableDefinition {
                    audit_redact: vec!["dob".into()],
                    ..plain()
                },
                features::AUDIT_REDACTION,
            ),
            (
                TableDefinition {
                    sensitive: true,
                    ..plain()
                },
                features::AUDIT_REDACTION,
            ),
            (
                TableDefinition {
                    pagination: Pagination::Keyset,
                    ..plain()
                },
                features::KEYSET_LISTS,
            ),
        ];
        for (table, feature) in cases {
            assert_eq!(
                table.derived_features().collect::<Vec<_>>(),
                [feature],
                "{table:?}"
            );
        }
    }

    #[test]
    fn test_fk_not_serialized_when_none() {
        let col = ColumnDefinition {
            name: "title".into(),
            column_type: ColumnType::Text,
            primary_key: false,
            nullable: false,
            auto_generate: false,
            default_value: None,
            references: None,
            on_delete: None,
            unique: false,
            validations: vec![],
            ..Default::default()
        };
        let json = serde_json::to_string(&col).unwrap();
        assert!(!json.contains("references"));
        assert!(!json.contains("on_delete"));
    }

    #[test]
    fn test_unique_column_serialization() {
        let col = ColumnDefinition {
            name: "email".into(),
            column_type: ColumnType::Text,
            primary_key: false,
            nullable: false,
            auto_generate: false,
            default_value: None,
            references: None,
            on_delete: None,
            unique: true,
            validations: vec![],
            ..Default::default()
        };
        let json = serde_json::to_string(&col).unwrap();
        assert!(json.contains("\"unique\":true"));
        let parsed: ColumnDefinition = serde_json::from_str(&json).unwrap();
        assert!(parsed.unique);
    }

    #[test]
    fn test_index_serialization() {
        let table = TableDefinition {
            name: "users".into(),
            columns: vec![],
            indexes: vec![IndexDefinition {
                name: "idx_users_email".into(),
                columns: vec!["email".into()],
                unique: true,
                ..Default::default()
            }],
            soft_delete: false,
            owner_field: None,
            auth_required: false,
            permission_area: None,
            owner_self_service: false,
            hooks: None,
            scoped_by: None,
            ..Default::default()
        };
        let json = serde_json::to_string(&table).unwrap();
        assert!(json.contains("idx_users_email"));
        let parsed: TableDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.indexes.len(), 1);
        assert!(parsed.indexes[0].unique);
    }

    #[test]
    fn test_indexes_not_serialized_when_empty() {
        let table = TableDefinition {
            name: "users".into(),
            columns: vec![],
            indexes: vec![],
            soft_delete: false,
            owner_field: None,
            auth_required: false,
            permission_area: None,
            owner_self_service: false,
            hooks: None,
            scoped_by: None,
            ..Default::default()
        };
        let json = serde_json::to_string(&table).unwrap();
        assert!(!json.contains("indexes"));
    }

    #[test]
    fn test_owner_field_serialization() {
        let table = TableDefinition {
            name: "notes".into(),
            columns: vec![],
            indexes: vec![],
            soft_delete: false,
            owner_field: Some("user_id".into()),
            auth_required: false,
            permission_area: None,
            owner_self_service: false,
            hooks: None,
            scoped_by: None,
            ..Default::default()
        };
        let json = serde_json::to_string(&table).unwrap();
        assert!(json.contains("\"owner_field\":\"user_id\""));
        let parsed: TableDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.owner_field.as_deref(), Some("user_id"));
    }

    #[test]
    fn test_owner_field_not_serialized_when_none() {
        let table = TableDefinition {
            name: "notes".into(),
            columns: vec![],
            indexes: vec![],
            soft_delete: false,
            owner_field: None,
            auth_required: false,
            permission_area: None,
            owner_self_service: false,
            hooks: None,
            scoped_by: None,
            ..Default::default()
        };
        let json = serde_json::to_string(&table).unwrap();
        assert!(!json.contains("owner_field"));
    }

    #[test]
    fn test_owner_field_defaults_to_none() {
        let json = r#"{"name":"notes","columns":[]}"#;
        let table: TableDefinition = serde_json::from_str(json).unwrap();
        assert!(table.owner_field.is_none());
    }

    #[test]
    fn test_auth_required_serialization() {
        let table = TableDefinition {
            name: "orders".into(),
            columns: vec![],
            indexes: vec![],
            soft_delete: false,
            owner_field: None,
            auth_required: true,
            permission_area: None,
            owner_self_service: false,
            hooks: None,
            scoped_by: None,
            ..Default::default()
        };
        let json = serde_json::to_string(&table).unwrap();
        assert!(json.contains("\"auth_required\":true"));
        let parsed: TableDefinition = serde_json::from_str(&json).unwrap();
        assert!(parsed.auth_required);
    }

    #[test]
    fn test_auth_required_defaults_to_false() {
        let json = r#"{"name":"orders","columns":[]}"#;
        let table: TableDefinition = serde_json::from_str(json).unwrap();
        assert!(!table.auth_required);
    }

    #[test]
    fn scoped_by_is_omitted_when_absent_and_derives_its_feature() {
        let table: TableDefinition =
            serde_json::from_str(r#"{"name":"books","columns":[],"owner_field":"user_id"}"#)
                .unwrap();
        assert!(table.scoped_by.is_none());
        assert!(!serde_json::to_string(&table).unwrap().contains("scoped_by"));
        assert_eq!(table.derived_features().count(), 0);

        let scoped = TableDefinition {
            scoped_by: Some("books".into()),
            ..table
        };
        let json = serde_json::to_string(&scoped).unwrap();
        assert!(json.contains(r#""scoped_by":"books""#), "{json}");
        assert_eq!(
            scoped.derived_features().collect::<Vec<_>>(),
            [crate::features::SCOPED_PERMISSIONS]
        );
    }
}
