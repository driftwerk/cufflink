use crate::deployer::manifest::{ColumnDefinition, ColumnType};
use crate::pg_numeric::PgNumeric;
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde_json::Value;
use std::error::Error;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};
use uuid::Uuid;

pub(crate) type SqlValue = Box<dyn ToSql + Sync + Send>;

pub struct AsBase(pub Box<dyn ToSql + Sync + Send>);

impl std::fmt::Debug for AsBase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl ToSql for AsBase {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.0.to_sql_checked(cufflink_pgcodec::base_type(ty), out)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("column '{column}': expected {expected}, got {got}")]
pub(crate) struct BindError {
    pub column: String,
    pub expected: &'static str,
    pub got: String,
}

const NAIVE_TIMESTAMP: &str = "%Y-%m-%dT%H:%M:%S%.f";
const NAIVE_TIMESTAMP_SPACED: &str = "%Y-%m-%d %H:%M:%S%.f";
const TIME: &str = "%H:%M:%S%.f";

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Typed {
    Uuid(Uuid),
    Text(String),
    Integer(i32),
    BigInteger(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    Date(NaiveDate),
    Jsonb(Value),
    Numeric(PgNumeric),
    NaiveTimestamp(NaiveDateTime),
    Time(NaiveTime),
    Bytes(Vec<u8>),
    TextArray(Vec<Option<String>>),
    UuidArray(Vec<Option<Uuid>>),
    IntegerArray(Vec<Option<i32>>),
    BigIntegerArray(Vec<Option<i64>>),
    JsonbArray(Vec<Option<Value>>),
}

fn array_json<T>(items: Vec<Option<T>>, render: impl Fn(T) -> Value) -> Value {
    items
        .into_iter()
        .map(|item| item.map_or(Value::Null, &render))
        .collect()
}

impl Typed {
    pub(crate) fn into_json(self) -> Value {
        match self {
            Typed::Uuid(v) => Value::String(v.to_string()),
            Typed::Text(v) => Value::String(v),
            Typed::Integer(v) => Value::from(v),
            Typed::BigInteger(v) => Value::from(v),
            Typed::Float(v) => Value::from(v),
            Typed::Double(v) => Value::from(v),
            Typed::Boolean(v) => Value::Bool(v),
            Typed::Timestamp(v) => Value::String(v.to_rfc3339()),
            Typed::Date(v) => Value::String(v.to_string()),
            Typed::Jsonb(v) => v,
            Typed::Numeric(v) => Value::String(v.to_string()),
            Typed::NaiveTimestamp(v) => Value::String(v.format(NAIVE_TIMESTAMP).to_string()),
            Typed::Time(v) => Value::String(v.format(TIME).to_string()),
            Typed::Bytes(v) => Value::String(STANDARD.encode(v)),
            Typed::TextArray(v) => array_json(v, Value::String),
            Typed::UuidArray(v) => array_json(v, |id| Value::String(id.to_string())),
            Typed::IntegerArray(v) => array_json(v, Value::from),
            Typed::BigIntegerArray(v) => array_json(v, Value::from),
            Typed::JsonbArray(v) => array_json(v, std::convert::identity),
        }
    }

    pub(crate) fn into_sql(self) -> SqlValue {
        match self {
            Typed::Uuid(v) => Box::new(v),
            Typed::Text(v) => Box::new(v),
            Typed::Integer(v) => Box::new(v),
            Typed::BigInteger(v) => Box::new(v),
            Typed::Float(v) => Box::new(v),
            Typed::Double(v) => Box::new(v),
            Typed::Boolean(v) => Box::new(v),
            Typed::Timestamp(v) => Box::new(v),
            Typed::Date(v) => Box::new(v),
            Typed::Jsonb(v) => Box::new(v),
            Typed::Numeric(v) => Box::new(v),
            Typed::NaiveTimestamp(v) => Box::new(v),
            Typed::Time(v) => Box::new(v),
            Typed::Bytes(v) => Box::new(v),
            Typed::TextArray(v) => Box::new(v),
            Typed::UuidArray(v) => Box::new(v),
            Typed::IntegerArray(v) => Box::new(v),
            Typed::BigIntegerArray(v) => Box::new(v),
            Typed::JsonbArray(v) => Box::new(v),
        }
    }

    fn same(&self, other: &Typed) -> bool {
        match (self, other) {
            (Typed::Numeric(a), Typed::Numeric(b)) => canonical_numeric(a) == canonical_numeric(b),
            (a, b) => a == b,
        }
    }
}

fn canonical_numeric(value: &PgNumeric) -> String {
    let text = value.to_string();
    let trimmed = match text.split_once('.') {
        Some((int, frac)) => match frac.trim_end_matches('0') {
            "" => int.to_string(),
            frac => format!("{int}.{frac}"),
        },
        None => text,
    };
    match trimmed.strip_prefix('-') {
        Some(magnitude) if magnitude.bytes().all(|b| b == b'0') => magnitude.to_string(),
        _ => trimmed,
    }
}

fn elements<T>(
    value: &Value,
    element: &ColumnType,
    unwrap: impl Fn(Typed) -> Option<T>,
) -> Option<Vec<Option<T>>> {
    value
        .as_array()?
        .iter()
        .map(|item| match item {
            Value::Null => Some(None),
            item => typed(element, item).and_then(&unwrap).map(Some),
        })
        .collect()
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn float(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok().filter(|f: &f64| f.is_finite()),
        _ => None,
    }
}

pub(crate) fn typed(column_type: &ColumnType, value: &Value) -> Option<Typed> {
    let text = || value.as_str();
    match column_type {
        ColumnType::Uuid => text()
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(Typed::Uuid),
        ColumnType::Text => scalar_text(value).map(Typed::Text),
        ColumnType::Integer => integer(value)
            .and_then(|n| i32::try_from(n).ok())
            .map(Typed::Integer),
        ColumnType::BigInteger => integer(value).map(Typed::BigInteger),
        ColumnType::Float => float(value)
            .map(|f| f as f32)
            .filter(|f| f.is_finite())
            .map(Typed::Float),
        ColumnType::Double => float(value).map(Typed::Double),
        ColumnType::Boolean => match value {
            Value::Bool(b) => Some(*b),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
        .map(Typed::Boolean),
        ColumnType::Timestamp => text()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| Typed::Timestamp(t.with_timezone(&Utc))),
        ColumnType::Date => text()
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
            .map(Typed::Date),
        ColumnType::Jsonb => (!value.is_null()).then(|| Typed::Jsonb(value.clone())),
        ColumnType::Numeric { .. } => match value {
            Value::String(s) => s.trim().parse().ok(),
            Value::Number(n) => n.to_string().parse().ok(),
            _ => None,
        }
        .map(Typed::Numeric),
        ColumnType::Varchar { length } => scalar_text(value)
            .filter(|s| u32::try_from(s.chars().count()).is_ok_and(|n| n <= *length))
            .map(Typed::Text),
        ColumnType::TimestampNaive => text()
            .and_then(|s| {
                [NAIVE_TIMESTAMP, NAIVE_TIMESTAMP_SPACED]
                    .iter()
                    .find_map(|format| NaiveDateTime::parse_from_str(s, format).ok())
            })
            .map(Typed::NaiveTimestamp),
        ColumnType::Time => text()
            .and_then(|s| NaiveTime::parse_from_str(s, TIME).ok())
            .map(Typed::Time),
        ColumnType::Bytea => text()
            .and_then(|s| STANDARD.decode(s).ok())
            .map(Typed::Bytes),
        ColumnType::TextArray => elements(value, &ColumnType::Text, |t| match t {
            Typed::Text(v) => Some(v),
            _ => None,
        })
        .map(Typed::TextArray),
        ColumnType::UuidArray => elements(value, &ColumnType::Uuid, |t| match t {
            Typed::Uuid(v) => Some(v),
            _ => None,
        })
        .map(Typed::UuidArray),
        ColumnType::IntegerArray => elements(value, &ColumnType::Integer, |t| match t {
            Typed::Integer(v) => Some(v),
            _ => None,
        })
        .map(Typed::IntegerArray),
        ColumnType::BigIntegerArray => elements(value, &ColumnType::BigInteger, |t| match t {
            Typed::BigInteger(v) => Some(v),
            _ => None,
        })
        .map(Typed::BigIntegerArray),
        ColumnType::JsonbArray => elements(value, &ColumnType::Jsonb, |t| match t {
            Typed::Jsonb(v) => Some(v),
            _ => None,
        })
        .map(Typed::JsonbArray),
    }
}

fn expected(column_type: &ColumnType) -> &'static str {
    match column_type {
        ColumnType::Uuid => "a uuid",
        ColumnType::Text => "text",
        ColumnType::Integer => "a 32-bit integer",
        ColumnType::BigInteger => "a 64-bit integer",
        ColumnType::Float | ColumnType::Double => "a number",
        ColumnType::Boolean => "a boolean",
        ColumnType::Timestamp => "an RFC 3339 timestamp",
        ColumnType::Date => "a YYYY-MM-DD date",
        ColumnType::Jsonb => "a JSON value",
        ColumnType::Numeric { .. } => "a decimal number",
        ColumnType::Varchar { .. } => "text within the column's length",
        ColumnType::TimestampNaive => "a timestamp without an offset",
        ColumnType::Time => "an HH:MM:SS time",
        ColumnType::Bytea => "base64 bytes",
        ColumnType::TextArray => "an array of text",
        ColumnType::UuidArray => "an array of uuids",
        ColumnType::IntegerArray => "an array of 32-bit integers",
        ColumnType::BigIntegerArray => "an array of 64-bit integers",
        ColumnType::JsonbArray => "an array of JSON values",
    }
}

pub(crate) fn bind_column_value(
    column: &ColumnDefinition,
    value: &Value,
) -> Result<SqlValue, BindError> {
    typed(&column.column_type, value)
        .map(Typed::into_sql)
        .ok_or_else(|| BindError {
            column: column.name.clone(),
            expected: expected(&column.column_type),
            got: value.to_string(),
        })
}

pub(crate) fn bind_column_array(column: &ColumnDefinition, values: &[Value]) -> Option<SqlValue> {
    let column_type = &column.column_type;
    let typed_values = values.iter().filter_map(|v| typed(column_type, v));
    macro_rules! array {
        ($variant:ident) => {
            Some(Box::new(
                typed_values
                    .filter_map(|t| match t {
                        Typed::$variant(v) => Some(v),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            ))
        };
    }
    match column_type {
        ColumnType::Uuid => array!(Uuid),
        ColumnType::Text | ColumnType::Varchar { .. } => array!(Text),
        ColumnType::Integer => array!(Integer),
        ColumnType::BigInteger => array!(BigInteger),
        ColumnType::Float => array!(Float),
        ColumnType::Double => array!(Double),
        ColumnType::Boolean => array!(Boolean),
        ColumnType::Timestamp => array!(Timestamp),
        ColumnType::Date => array!(Date),
        ColumnType::Jsonb => array!(Jsonb),
        ColumnType::Numeric { .. } => array!(Numeric),
        ColumnType::TimestampNaive => array!(NaiveTimestamp),
        ColumnType::Time => array!(Time),
        ColumnType::Bytea => array!(Bytes),
        ColumnType::TextArray
        | ColumnType::UuidArray
        | ColumnType::IntegerArray
        | ColumnType::BigIntegerArray
        | ColumnType::JsonbArray => None,
    }
}

pub(crate) fn same_value(column_type: &ColumnType, a: &Value, b: &Value) -> bool {
    matches!((typed(column_type, a), typed(column_type, b)), (Some(x), Some(y)) if x.same(&y))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn column(column_type: ColumnType) -> ColumnDefinition {
        ColumnDefinition {
            name: "c".into(),
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
    fn values_bind_losslessly_or_not_at_all() {
        let uuid = "0f8fad5b-d9cb-469f-a165-70867728950e";
        let cases: Vec<(ColumnType, Value, Option<Typed>)> = vec![
            (
                ColumnType::Uuid,
                json!(uuid.to_uppercase()),
                Some(Typed::Uuid(Uuid::parse_str(uuid).unwrap())),
            ),
            (ColumnType::Uuid, json!("abc"), None),
            (ColumnType::Uuid, json!(7), None),
            (ColumnType::Text, json!("a"), Some(Typed::Text("a".into()))),
            (ColumnType::Text, json!(42), Some(Typed::Text("42".into()))),
            (ColumnType::Text, json!({"a": 1}), None),
            (ColumnType::Integer, json!(7), Some(Typed::Integer(7))),
            (ColumnType::Integer, json!("7"), Some(Typed::Integer(7))),
            (ColumnType::Integer, json!(7.5), None),
            (ColumnType::Integer, json!(3_000_000_000_i64), None),
            (
                ColumnType::BigInteger,
                json!(3_000_000_000_i64),
                Some(Typed::BigInteger(3_000_000_000)),
            ),
            (ColumnType::Double, json!("2.5"), Some(Typed::Double(2.5))),
            (ColumnType::Double, json!("NaN"), None),
            (ColumnType::Float, json!(1e300), None),
            (
                ColumnType::Boolean,
                json!("true"),
                Some(Typed::Boolean(true)),
            ),
            (ColumnType::Boolean, json!(1), None),
            (
                ColumnType::Timestamp,
                json!("2026-01-01T00:00:00+02:00"),
                Some(Typed::Timestamp(
                    DateTime::parse_from_rfc3339("2025-12-31T22:00:00Z")
                        .unwrap()
                        .with_timezone(&Utc),
                )),
            ),
            (ColumnType::Timestamp, json!("yesterday"), None),
            (
                ColumnType::Date,
                json!("2026-02-28"),
                Some(Typed::Date(NaiveDate::from_ymd_opt(2026, 2, 28).unwrap())),
            ),
            (ColumnType::Date, json!("2026-02-30"), None),
            (
                ColumnType::Jsonb,
                json!([1]),
                Some(Typed::Jsonb(json!([1]))),
            ),
            (ColumnType::Jsonb, Value::Null, None),
            (ColumnType::Text, Value::Null, None),
        ];
        for (column_type, value, expected) in cases {
            assert_eq!(
                typed(&column_type, &value),
                expected,
                "{column_type:?} {value}"
            );
        }
    }

    #[test]
    fn a_bind_error_names_the_column_and_the_type() {
        let err =
            bind_column_value(&column(ColumnType::Uuid), &json!("abc")).expect_err("not a uuid");
        assert_eq!(err.to_string(), r#"column 'c': expected a uuid, got "abc""#);
        assert!(bind_column_value(&column(ColumnType::Integer), &json!(1)).is_ok());
    }

    #[test]
    fn arrays_keep_only_values_of_the_type() {
        let uuid = Uuid::new_v4();
        let rendered = format!(
            "{:?}",
            bind_column_array(
                &column(ColumnType::Uuid),
                &[json!(uuid.to_string()), json!("bad"), json!(1), Value::Null]
            )
            .expect("a scalar column")
        );
        assert_eq!(rendered, format!("[{uuid}]"));
        assert_eq!(
            format!(
                "{:?}",
                bind_column_array(
                    &column(ColumnType::Integer),
                    &[json!(1), json!("2"), json!("x")]
                )
                .expect("a scalar column")
            ),
            "[1, 2]"
        );
        assert_eq!(
            format!(
                "{:?}",
                bind_column_array(&column(ColumnType::Text), &[]).expect("a scalar column")
            ),
            "[]"
        );
    }

    #[test]
    fn a_list_cannot_be_compared_with_an_array_column() {
        for column_type in [
            ColumnType::TextArray,
            ColumnType::UuidArray,
            ColumnType::IntegerArray,
            ColumnType::BigIntegerArray,
            ColumnType::JsonbArray,
        ] {
            assert!(bind_column_array(&column(column_type), &[json!(["a"])]).is_none());
        }
        let numeric = ColumnType::Numeric {
            precision: Some(12),
            scale: Some(2),
        };
        assert_eq!(
            format!(
                "{:?}",
                bind_column_array(&column(numeric), &[json!("1.50"), json!("x"), json!(2)])
                    .expect("a scalar column")
                    .as_ref()
            )
            .matches("Finite")
            .count(),
            2
        );
    }

    #[test]
    fn v2_values_bind_losslessly_or_not_at_all() {
        let numeric = ColumnType::Numeric {
            precision: Some(12),
            scale: Some(2),
        };
        let uuid = Uuid::parse_str("0f8fad5b-d9cb-469f-a165-70867728950e").unwrap();
        let cases: Vec<(ColumnType, Value, Option<Value>)> = vec![
            (numeric.clone(), json!("12.50"), Some(json!("12.50"))),
            (numeric.clone(), json!(7), Some(json!("7"))),
            (numeric.clone(), json!(0.1), Some(json!("0.1"))),
            (
                numeric.clone(),
                json!("123456789012345678901234567890.12"),
                Some(json!("123456789012345678901234567890.12")),
            ),
            (numeric.clone(), json!("twelve"), None),
            (numeric.clone(), json!(true), None),
            (numeric, Value::Null, None),
            (
                ColumnType::Varchar { length: 3 },
                json!("abc"),
                Some(json!("abc")),
            ),
            (
                ColumnType::Varchar { length: 3 },
                json!("äöü"),
                Some(json!("äöü")),
            ),
            (ColumnType::Varchar { length: 3 }, json!("abcd"), None),
            (
                ColumnType::Varchar { length: 3 },
                json!(12),
                Some(json!("12")),
            ),
            (
                ColumnType::TimestampNaive,
                json!("2026-01-01T09:30:00"),
                Some(json!("2026-01-01T09:30:00")),
            ),
            (
                ColumnType::TimestampNaive,
                json!("2026-01-01 09:30:00.250"),
                Some(json!("2026-01-01T09:30:00.250")),
            ),
            (
                ColumnType::TimestampNaive,
                json!("2026-01-01T09:30:00Z"),
                None,
            ),
            (ColumnType::TimestampNaive, json!("2026-01-01"), None),
            (ColumnType::Time, json!("09:30:00"), Some(json!("09:30:00"))),
            (
                ColumnType::Time,
                json!("09:30:00.5"),
                Some(json!("09:30:00.500")),
            ),
            (ColumnType::Time, json!("25:00:00"), None),
            (ColumnType::Time, json!("morning"), None),
            (ColumnType::Bytea, json!("AP8="), Some(json!("AP8="))),
            (ColumnType::Bytea, json!(""), Some(json!(""))),
            (ColumnType::Bytea, json!("not base64!"), None),
            (ColumnType::Bytea, json!([0, 255]), None),
            (
                ColumnType::TextArray,
                json!(["a", null, 7]),
                Some(json!(["a", null, "7"])),
            ),
            (ColumnType::TextArray, json!([]), Some(json!([]))),
            (ColumnType::TextArray, json!("a"), None),
            (ColumnType::TextArray, json!([{"a": 1}]), None),
            (
                ColumnType::UuidArray,
                json!([uuid.to_string().to_uppercase()]),
                Some(json!([uuid.to_string()])),
            ),
            (ColumnType::UuidArray, json!(["abc"]), None),
            (
                ColumnType::IntegerArray,
                json!([1, "2", null]),
                Some(json!([1, 2, null])),
            ),
            (ColumnType::IntegerArray, json!([3_000_000_000_i64]), None),
            (
                ColumnType::BigIntegerArray,
                json!([3_000_000_000_i64]),
                Some(json!([3_000_000_000_i64])),
            ),
            (ColumnType::BigIntegerArray, json!([1.5]), None),
            (
                ColumnType::JsonbArray,
                json!([{"a": 1}, [2], "s", null]),
                Some(json!([{"a": 1}, [2], "s", null])),
            ),
            (ColumnType::JsonbArray, json!({"a": 1}), None),
        ];
        for (column_type, value, expected) in cases {
            assert_eq!(
                typed(&column_type, &value).map(Typed::into_json),
                expected,
                "{column_type:?} {value}"
            );
            assert_eq!(
                bind_column_value(&column(column_type.clone()), &value).is_ok(),
                expected.is_some(),
                "{column_type:?} {value}"
            );
        }
    }

    #[test]
    fn numerics_compare_by_value() {
        let numeric = ColumnType::Numeric {
            precision: None,
            scale: None,
        };
        for (a, b) in [
            (json!("1.50"), json!("1.5")),
            (json!("2"), json!(2)),
            (json!("2.000"), json!("2")),
            (json!("-0"), json!("0.00")),
            (json!("NaN"), json!("NaN")),
        ] {
            assert!(same_value(&numeric, &a, &b), "{a} {b}");
        }
        for (a, b) in [
            (json!("1.50"), json!("1.05")),
            (json!("10"), json!("1")),
            (json!("-1"), json!("1")),
            (json!("x"), json!("x")),
        ] {
            assert!(!same_value(&numeric, &a, &b), "{a} {b}");
        }
    }

    #[test]
    fn same_value_compares_as_the_column_type() {
        let uuid = "0f8fad5b-d9cb-469f-a165-70867728950e";
        assert!(same_value(
            &ColumnType::Uuid,
            &json!(uuid),
            &json!(uuid.to_uppercase())
        ));
        assert!(!same_value(&ColumnType::Text, &json!("a"), &json!("A")));
        assert!(same_value(&ColumnType::Integer, &json!(42), &json!("42")));
        assert!(same_value(&ColumnType::Text, &json!(42), &json!("42")));
        assert!(!same_value(&ColumnType::Uuid, &json!("x"), &json!("x")));
        assert!(!same_value(&ColumnType::Text, &Value::Null, &Value::Null));
    }
}
