use crate::{ColumnDefinition, ColumnType, TableDefinition};
use serde_json::{json, Map, Value};

/// The component names of `table`'s row, create-body and update-body schemas.
pub(super) fn schema_names(table: &TableDefinition) -> [String; 3] {
    let schema_name = to_pascal_case(&table.name);
    let create_schema_name = format!("{}Create", schema_name);
    let update_schema_name = format!("{}Update", schema_name);
    [schema_name, create_schema_name, update_schema_name]
}

/// How a nullable column's schema says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Nullable {
    /// OpenAPI 3.0's `nullable: true`, which the absolute-path spec keeps.
    Flag,
    /// JSON Schema's `type: [T, "null"]`, OpenAPI 3.1's form.
    TypeUnion,
}

impl Nullable {
    fn apply(self, mut prop: Value) -> Value {
        if let Some(obj) = prop.as_object_mut() {
            match self {
                Nullable::Flag => {
                    obj.insert("nullable".into(), json!(true));
                }
                Nullable::TypeUnion => {
                    if let Some(kind) = obj.remove("type") {
                        obj.insert("type".into(), json!([kind, "null"]));
                    }
                }
            }
        }
        prop
    }
}

/// `table`'s row, create-body and update-body component schemas.
pub(super) fn table_schemas(table: &TableDefinition, nullable: Nullable) -> [(String, Value); 3] {
    let [schema_name, create_schema_name, update_schema_name] = schema_names(table);
    [
        (schema_name, table_to_schema(table, nullable)),
        (create_schema_name, table_to_create_schema(table, nullable)),
        (update_schema_name, table_to_update_schema(table, nullable)),
    ]
}

/// The `Pagination` component every list response references.
pub(super) fn pagination() -> Value {
    json!({
        "type": "object",
        "properties": {
            "current_page": { "type": "integer" },
            "per_page": { "type": "integer" },
            "total_records": { "type": "integer" },
            "total_pages": { "type": "integer" }
        }
    })
}

pub(super) fn column_to_openapi_type(col_type: &ColumnType) -> Value {
    match col_type {
        ColumnType::Uuid => json!({"type": "string", "format": "uuid"}),
        ColumnType::Text => json!({"type": "string"}),
        ColumnType::Integer => json!({"type": "integer", "format": "int32"}),
        ColumnType::BigInteger => json!({"type": "integer", "format": "int64"}),
        ColumnType::Float => json!({"type": "number", "format": "float"}),
        ColumnType::Double => json!({"type": "number", "format": "double"}),
        ColumnType::Boolean => json!({"type": "boolean"}),
        ColumnType::Timestamp => json!({"type": "string", "format": "date-time"}),
        ColumnType::Date => json!({"type": "string", "format": "date"}),
        ColumnType::Jsonb => json!({}),
        ColumnType::Numeric { .. } => json!({"type": "string", "format": "decimal"}),
        ColumnType::Varchar { length } => json!({"type": "string", "maxLength": length}),
        ColumnType::TimestampNaive => json!({"type": "string", "format": "date-time"}),
        ColumnType::Time => json!({"type": "string", "format": "time"}),
        ColumnType::Bytea => json!({"type": "string", "format": "byte"}),
        ColumnType::TextArray => array_of(&ColumnType::Text),
        ColumnType::UuidArray => array_of(&ColumnType::Uuid),
        ColumnType::IntegerArray => array_of(&ColumnType::Integer),
        ColumnType::BigIntegerArray => array_of(&ColumnType::BigInteger),
        ColumnType::JsonbArray => array_of(&ColumnType::Jsonb),
    }
}

fn array_of(element: &ColumnType) -> Value {
    json!({"type": "array", "items": column_to_openapi_type(element)})
}

/// The permissions a column asks of a caller, as the spec carries them.
const ACCESS_EXTENSION: &str = "x-cufflink-access";

/// The schema of a column's values, with the permissions the column asks
/// of a caller where it asks any.
fn column_schema(table: &TableDefinition, col: &ColumnDefinition, nullable: Nullable) -> Value {
    let prop = column_to_openapi_type(&col.column_type);
    let mut prop = if col.nullable {
        nullable.apply(prop)
    } else {
        prop
    };
    if let (Some(permissions), Some(fields)) = (table.permissions_of(col), prop.as_object_mut()) {
        fields.insert(ACCESS_EXTENSION.to_string(), json!(permissions));
    }
    prop
}

/// A row as the data routes answer it.
fn table_to_schema(table: &TableDefinition, nullable: Nullable) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for col in &table.columns {
        properties.insert(col.name.clone(), column_schema(table, col, nullable));
        if !table.withholds(col) {
            required.push(json!(col.name));
        }
    }

    json!({
        "type": "object",
        "properties": Value::Object(properties),
        "required": required
    })
}

fn table_to_create_schema(table: &TableDefinition, nullable: Nullable) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for col in &table.columns {
        if col.auto_generate {
            continue;
        }
        properties.insert(col.name.clone(), column_schema(table, col, nullable));
        let owner = table.owner_field.as_deref() == Some(col.name.as_str());
        if !col.nullable && col.default_value.is_none() && !owner {
            required.push(json!(col.name));
        }
    }

    json!({
        "type": "object",
        "properties": Value::Object(properties),
        "required": required
    })
}

/// An update body, in which no column is required.
fn table_to_update_schema(table: &TableDefinition, nullable: Nullable) -> Value {
    let properties: Map<String, Value> = table
        .columns
        .iter()
        .filter(|col| !col.auto_generate)
        .map(|col| (col.name.clone(), column_schema(table, col, nullable)))
        .collect();
    json!({
        "type": "object",
        "properties": Value::Object(properties),
        "required": []
    })
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
                None => String::new(),
            }
        })
        .collect()
}
