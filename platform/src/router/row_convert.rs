use crate::deployer::manifest::{ColumnDefinition, ColumnType};
use crate::errors::{PlatformError, PlatformResult};
use crate::router::bind::{self, SqlValue};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use uuid::Uuid;

pub use cufflink_types::manifest::validate::folded_ident as manifest_ident;

pub fn row_to_json(
    row: &tokio_postgres::Row,
    columns: &[ColumnDefinition],
    requested_fields: Option<&[String]>,
) -> Value {
    let mut map = Map::new();

    for col in columns {
        if let Some(fields) = requested_fields {
            if !fields.is_empty() && !fields.contains(&col.name) {
                continue;
            }
        }

        let value: Value = match col.column_type {
            ColumnType::Uuid => match row.try_get::<_, Uuid>(&*col.name) {
                Ok(v) => json!(v.to_string()),
                Err(_) => Value::Null,
            },
            ColumnType::Text => match row.try_get::<_, String>(&*col.name) {
                Ok(v) => json!(v),
                Err(_) => Value::Null,
            },
            ColumnType::Integer => match row.try_get::<_, i32>(&*col.name) {
                Ok(v) => json!(v),
                Err(_) => Value::Null,
            },
            ColumnType::BigInteger => match row.try_get::<_, i64>(&*col.name) {
                Ok(v) => json!(v),
                Err(_) => Value::Null,
            },
            ColumnType::Float => match row.try_get::<_, f32>(&*col.name) {
                Ok(v) => json!(v),
                Err(_) => Value::Null,
            },
            ColumnType::Double => match row.try_get::<_, f64>(&*col.name) {
                Ok(v) => json!(v),
                Err(_) => Value::Null,
            },
            ColumnType::Boolean => match row.try_get::<_, bool>(&*col.name) {
                Ok(v) => json!(v),
                Err(_) => Value::Null,
            },
            ColumnType::Timestamp => match row.try_get::<_, DateTime<Utc>>(&*col.name) {
                Ok(v) => json!(v.to_rfc3339()),
                Err(_) => Value::Null,
            },
            ColumnType::Date => match row.try_get::<_, chrono::NaiveDate>(&*col.name) {
                Ok(v) => json!(v.to_string()),
                Err(_) => Value::Null,
            },
            ColumnType::Jsonb => match row.try_get::<_, Value>(&*col.name) {
                Ok(v) => v,
                Err(_) => Value::Null,
            },
            ColumnType::Numeric { .. }
            | ColumnType::Varchar { .. }
            | ColumnType::TimestampNaive
            | ColumnType::Time
            | ColumnType::Bytea
            | ColumnType::TextArray
            | ColumnType::UuidArray
            | ColumnType::IntegerArray
            | ColumnType::BigIntegerArray
            | ColumnType::JsonbArray => exact_json(row, &col.name),
        };

        map.insert(col.name.clone(), value);
    }

    Value::Object(map)
}

fn exact_json(row: &tokio_postgres::Row, name: &str) -> Value {
    row.columns()
        .iter()
        .position(|column| column.name() == name)
        .and_then(|index| {
            crate::row_json::column_to_json_v2(row, index, row.columns()[index].type_()).ok()
        })
        .unwrap_or(Value::Null)
}

fn refused(error: bind::BindError) -> PlatformError {
    PlatformError::BadRequest(error.to_string())
}

pub fn json_param(column: &ColumnDefinition, value: &Value) -> PlatformResult<SqlValue> {
    if column.column_type.is_v2() && !value.is_null() {
        bind::bind_column_value(column, value).map_err(refused)
    } else {
        Ok(json_to_sql_param(value, &column.column_type))
    }
}

pub fn string_param(column: &ColumnDefinition, value: &str) -> PlatformResult<SqlValue> {
    if !column.column_type.is_v2() {
        return Ok(string_to_sql_param(value, &column.column_type));
    }
    let array = matches!(
        column.column_type,
        ColumnType::TextArray
            | ColumnType::UuidArray
            | ColumnType::IntegerArray
            | ColumnType::BigIntegerArray
            | ColumnType::JsonbArray
    );
    let given = match serde_json::from_str::<Value>(value) {
        Ok(list @ Value::Array(_)) if array => list,
        _ => Value::String(value.to_string()),
    };
    bind::bind_column_value(column, &given).map_err(refused)
}

fn json_to_sql_param(
    val: &Value,
    col_type: &ColumnType,
) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
    if val.is_null() {
        return match col_type {
            ColumnType::Uuid => Box::new(None::<Uuid>),
            ColumnType::Boolean => Box::new(None::<bool>),
            ColumnType::Integer => Box::new(None::<i32>),
            ColumnType::BigInteger => Box::new(None::<i64>),
            ColumnType::Float => Box::new(None::<f32>),
            ColumnType::Double => Box::new(None::<f64>),
            ColumnType::Timestamp => Box::new(None::<DateTime<Utc>>),
            ColumnType::Date => Box::new(None::<chrono::NaiveDate>),
            ColumnType::Jsonb => Box::new(None::<Value>),
            ColumnType::Text | ColumnType::Varchar { .. } => Box::new(None::<String>),
            ColumnType::Numeric { .. } => Box::new(None::<crate::pg_numeric::PgNumeric>),
            ColumnType::TimestampNaive => Box::new(None::<chrono::NaiveDateTime>),
            ColumnType::Time => Box::new(None::<chrono::NaiveTime>),
            ColumnType::Bytea => Box::new(None::<Vec<u8>>),
            ColumnType::TextArray => Box::new(None::<Vec<String>>),
            ColumnType::UuidArray => Box::new(None::<Vec<Uuid>>),
            ColumnType::IntegerArray => Box::new(None::<Vec<i32>>),
            ColumnType::BigIntegerArray => Box::new(None::<Vec<i64>>),
            ColumnType::JsonbArray => Box::new(None::<Vec<Value>>),
        };
    }

    match col_type {
        ColumnType::Uuid => {
            let s = match val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            Box::new(Uuid::parse_str(&s).unwrap_or_else(|_| Uuid::new_v4()))
        }
        ColumnType::Boolean => {
            let b = match val {
                Value::Bool(b) => *b,
                Value::String(s) => s == "true",
                _ => false,
            };
            Box::new(b)
        }
        ColumnType::Integer => {
            let n = match val {
                Value::Number(n) => n.as_i64().unwrap_or(0) as i32,
                Value::String(s) => s.parse::<i32>().unwrap_or(0),
                _ => 0,
            };
            Box::new(n)
        }
        ColumnType::BigInteger => {
            let n = match val {
                Value::Number(n) => n.as_i64().unwrap_or(0),
                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                _ => 0,
            };
            Box::new(n)
        }
        ColumnType::Float => {
            let n = match val {
                Value::Number(n) => n.as_f64().unwrap_or(0.0) as f32,
                Value::String(s) => s.parse::<f32>().unwrap_or(0.0),
                _ => 0.0f32,
            };
            Box::new(n)
        }
        ColumnType::Double => {
            let n = match val {
                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                _ => 0.0f64,
            };
            Box::new(n)
        }
        ColumnType::Timestamp => {
            let s = match val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let dt = s.parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now());
            Box::new(dt)
        }
        ColumnType::Date => {
            let s = match val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let d = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .unwrap_or_else(|_| Utc::now().date_naive());
            Box::new(d)
        }
        ColumnType::Jsonb => Box::new(val.clone()),
        _ => {
            let s = match val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            Box::new(s)
        }
    }
}

fn string_to_sql_param(
    val: &str,
    col_type: &ColumnType,
) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
    match col_type {
        ColumnType::Uuid => Box::new(Uuid::parse_str(val).unwrap_or_else(|_| Uuid::nil())),
        ColumnType::Boolean => Box::new(val == "true" || val == "1" || val == "t"),
        ColumnType::Integer => Box::new(val.parse::<i32>().unwrap_or(0)),
        ColumnType::BigInteger => Box::new(val.parse::<i64>().unwrap_or(0)),
        ColumnType::Float => Box::new(val.parse::<f32>().unwrap_or(0.0)),
        ColumnType::Double => Box::new(val.parse::<f64>().unwrap_or(0.0)),
        ColumnType::Timestamp => {
            let dt = val.parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now());
            Box::new(dt)
        }
        ColumnType::Date => {
            let d = chrono::NaiveDate::parse_from_str(val, "%Y-%m-%d")
                .unwrap_or_else(|_| Utc::now().date_naive());
            Box::new(d)
        }
        _ => Box::new(val.to_string()),
    }
}

#[cfg(test)]
mod quote_tests {
    use super::*;
    use serde_json::json;

    fn column(column_type: ColumnType) -> ColumnDefinition {
        ColumnDefinition {
            name: "c".into(),
            column_type,
            ..Default::default()
        }
    }

    fn refusal(result: PlatformResult<SqlValue>) -> String {
        match result {
            Err(PlatformError::BadRequest(message)) => message,
            Err(other) => panic!("expected a bad request, got {other:?}"),
            Ok(value) => panic!("expected a bad request, got {value:?}"),
        }
    }

    #[test]
    fn a_column_of_a_newer_type_takes_its_type_and_nothing_else() {
        let price = column(ColumnType::Numeric {
            precision: Some(12),
            scale: Some(2),
        });
        for given in [json!("12.50"), json!(12.5), json!(7)] {
            assert!(json_param(&price, &given).is_ok(), "{given}");
        }
        assert_eq!(
            refusal(json_param(&price, &json!("twelve"))),
            "column 'c': expected a decimal number, got \"twelve\""
        );
        assert_eq!(
            format!("{:?}", json_param(&price, &Value::Null).expect("null")),
            "None"
        );
        let accepted = [
            (ColumnType::Varchar { length: 3 }, json!("abc")),
            (ColumnType::TimestampNaive, json!("2026-09-28T10:30:00")),
            (ColumnType::Time, json!("10:30:00")),
            (ColumnType::Bytea, json!("aGVsbG8=")),
            (ColumnType::TextArray, json!(["EPC", "Search"])),
            (
                ColumnType::UuidArray,
                json!(["0f8fad5b-d9cb-469f-a165-70867728950e"]),
            ),
            (ColumnType::IntegerArray, json!([1, 2])),
            (ColumnType::BigIntegerArray, json!([9007199254740993_i64])),
            (ColumnType::JsonbArray, json!([{"a": 1}, [2]])),
        ];
        for (column_type, given) in accepted {
            assert!(
                json_param(&column(column_type.clone()), &given).is_ok(),
                "{column_type:?}"
            );
        }
        let refused = [
            (ColumnType::Varchar { length: 3 }, json!("abcd")),
            (ColumnType::TimestampNaive, json!("2026-09-28T10:30:00Z")),
            (ColumnType::Time, json!("noon")),
            (ColumnType::Bytea, json!("not base64!")),
            (ColumnType::TextArray, json!("EPC")),
            (ColumnType::UuidArray, json!(["EPC"])),
            (ColumnType::IntegerArray, json!([1.5])),
        ];
        for (column_type, given) in refused {
            assert!(
                refusal(json_param(&column(column_type.clone()), &given)).starts_with("column 'c'"),
                "{column_type:?}"
            );
        }
    }

    #[test]
    fn a_column_of_a_type_every_release_had_takes_what_it_always_took() {
        for (column_type, given, bound) in [
            (ColumnType::Integer, json!("seven"), "0"),
            (ColumnType::Boolean, json!("yes"), "false"),
            (ColumnType::Text, json!(7), "\"7\""),
            (
                ColumnType::Uuid,
                json!("0f8fad5b-d9cb-469f-a165-70867728950e"),
                "0f8fad5b-d9cb-469f-a165-70867728950e",
            ),
        ] {
            let param = json_param(&column(column_type.clone()), &given).expect("coerced");
            assert_eq!(format!("{param:?}"), bound, "{column_type:?}");
        }
        let param = string_param(&column(ColumnType::Integer), "seven").expect("coerced");
        assert_eq!(format!("{param:?}"), "0");
    }

    #[test]
    fn a_value_in_a_query_string_is_text_or_an_array_of_json() {
        let bound = |column_type, given| string_param(&column(column_type), given);
        assert!(bound(ColumnType::Varchar { length: 50 }, "New").is_ok());
        assert!(bound(ColumnType::Varchar { length: 50 }, "[draft]").is_ok());
        assert_eq!(
            format!(
                "{:?}",
                bound(ColumnType::Varchar { length: 50 }, r#"["x"]"#).expect("text")
            ),
            format!("{:?}", String::from(r#"["x"]"#)),
            "the text of a column that holds no array is the text itself"
        );
        assert!(bound(
            ColumnType::Numeric {
                precision: None,
                scale: None
            },
            "12.50"
        )
        .is_ok());
        assert!(bound(ColumnType::TextArray, r#"["EPC","Search"]"#).is_ok());
        assert!(bound(ColumnType::IntegerArray, "[1,2]").is_ok());
        assert_eq!(
            refusal(bound(ColumnType::TextArray, "EPC")),
            "column 'c': expected an array of text, got \"EPC\""
        );
        assert!(refusal(bound(ColumnType::Time, "25:00:00")).contains("an HH:MM:SS time"));
    }

    #[test]
    fn manifest_ident_folds_like_an_unquoted_name_and_escapes_quotes() {
        assert_eq!(manifest_ident("items"), "\"items\"");
        assert_eq!(manifest_ident("OrderLines"), "\"orderlines\"");
        assert_eq!(
            manifest_ident("x\"; DROP TABLE users; --"),
            "\"x\"\"; drop table users; --\""
        );
    }
}
