use crate::numeric::PgNumeric;
use base64::Engine;
use cufflink_types::db::{DbError, DbErrorFields, DbErrorKind};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::error::Error;
use tokio_postgres::types::{FromSql, Kind, Type};
use tokio_postgres::{Column, Row};

/// Convert a single Postgres column into a `serde_json::Value`, mapping common
/// scalar types faithfully. Unknown types fall back to the column's text
/// representation. Used by both the SQL query endpoint (streaming rows to the
/// CLI) and the WASM runtime's host DB bridge.
pub fn column_to_json(row: &Row, idx: usize, col_type: &Type) -> Value {
    match *col_type {
        Type::UUID => match row.try_get::<_, Option<uuid::Uuid>>(idx) {
            Ok(Some(v)) => Value::String(v.to_string()),
            _ => Value::Null,
        },
        Type::TEXT | Type::VARCHAR | Type::NAME | Type::BPCHAR | Type::UNKNOWN => {
            match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(v)) => Value::String(v),
                _ => Value::Null,
            }
        }
        Type::INT2 => match row.try_get::<_, Option<i16>>(idx) {
            Ok(Some(v)) => json!(v),
            _ => Value::Null,
        },
        Type::INT4 => match row.try_get::<_, Option<i32>>(idx) {
            Ok(Some(v)) => json!(v),
            _ => Value::Null,
        },
        Type::INT8 => match row.try_get::<_, Option<i64>>(idx) {
            Ok(Some(v)) => json!(v),
            _ => Value::Null,
        },
        Type::FLOAT4 => match row.try_get::<_, Option<f32>>(idx) {
            Ok(Some(v)) => json!(v),
            _ => Value::Null,
        },
        Type::FLOAT8 => match row.try_get::<_, Option<f64>>(idx) {
            Ok(Some(v)) => json!(v),
            _ => Value::Null,
        },
        Type::BOOL => match row.try_get::<_, Option<bool>>(idx) {
            Ok(Some(v)) => json!(v),
            _ => Value::Null,
        },
        Type::DATE => match row.try_get::<_, Option<chrono::NaiveDate>>(idx) {
            Ok(Some(v)) => Value::String(v.to_string()),
            _ => Value::Null,
        },
        Type::TIMESTAMP => match row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
            Ok(Some(v)) => Value::String(v.to_string()),
            _ => Value::Null,
        },
        Type::TIMESTAMPTZ => match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
            Ok(Some(v)) => Value::String(v.to_rfc3339()),
            _ => Value::Null,
        },
        Type::JSON | Type::JSONB => match row.try_get::<_, Option<Value>>(idx) {
            Ok(Some(v)) => v,
            _ => Value::Null,
        },
        _ => match row.try_get::<_, Option<String>>(idx) {
            Ok(Some(v)) => Value::String(v),
            _ => Value::Null,
        },
    }
}

/// A column's value in the `db_call` v2 row envelope: every supported type
/// rendered exactly, and an error naming the column rather than a silent
/// `null` for anything else.
pub fn column_to_json_v2(row: &Row, idx: usize, ty: &Type) -> Result<Value, DbError> {
    let name = row.columns().get(idx).map_or("?", |c| c.name());
    let raw = row
        .try_get::<_, RawValue>(idx)
        .map_err(|e| DbError::new(DbErrorKind::Decode, format!("column {name}: {e}")))?;
    raw.0
        .map_or(Ok(Value::Null), |bytes| decode_v2(ty, bytes))
        .map_err(|failure| failure.for_column(name, ty))
}

/// Refuse, before a statement runs, result columns the v2 envelope cannot
/// carry: a type it does not render, or a name that appears twice (a row is
/// keyed by column name).
pub fn check_columns_v2(columns: &[Column]) -> Result<(), DbError> {
    let mut seen = HashSet::new();
    columns.iter().try_for_each(|column| {
        let (name, ty) = (column.name(), column.type_());
        if !renders_v2(ty) {
            return Err(
                DecodeFailure::Unsupported(format!("type {}", ty.name())).for_column(name, ty)
            );
        }
        if !seen.insert(name) {
            return Err(DbError::new(
                DbErrorKind::NotSupported,
                format!(
                    "column {name} appears more than once; give each column its own name (AS …)"
                ),
            ));
        }
        Ok(())
    })
}

/// A type with its domains looked through. tokio-postgres resolves a
/// domain's `typbasetype` once per connection, and a domain's base type
/// never changes, so the cached `Kind::Domain` is always current.
pub fn base_type(ty: &Type) -> &Type {
    match ty.kind() {
        Kind::Domain(inner) => base_type(inner),
        _ => ty,
    }
}

/// Whether the v2 envelope renders values of `ty`.
fn renders_v2(ty: &Type) -> bool {
    let ty = base_type(ty);
    match ty.kind() {
        Kind::Enum(_) => true,
        Kind::Array(element) => renders_v2(element),
        _ => scalar_decoder(ty).is_some(),
    }
}

/// A column's raw wire bytes, whatever its type; `None` for SQL `NULL`.
struct RawValue<'a>(Option<&'a [u8]>);

impl<'a> FromSql<'a> for RawValue<'a> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(RawValue(Some(raw)))
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(RawValue(None))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

enum DecodeFailure {
    Unsupported(String),
    Invalid(String),
}

impl DecodeFailure {
    fn for_column(self, name: &str, ty: &Type) -> DbError {
        match self {
            DecodeFailure::Unsupported(what) => DbErrorFields {
                hint: Some(format!("cast the column in SQL, e.g. {name}::text")),
                ..DbErrorFields::new(
                    DbErrorKind::UnsupportedType,
                    format!("column {name}: {what} is not supported"),
                )
            }
            .into(),
            DecodeFailure::Invalid(e) => DbError::new(
                DbErrorKind::Decode,
                format!("column {name} ({}): {e}", ty.name()),
            ),
        }
    }
}

fn decoded<'a, T: FromSql<'a>>(ty: &Type, raw: &'a [u8]) -> Result<T, DecodeFailure> {
    T::from_sql(ty, raw).map_err(|e| DecodeFailure::Invalid(e.to_string()))
}

fn float_json(value: f64, shortest: String) -> Value {
    match value {
        v if v.is_nan() => Value::String("NaN".into()),
        v if v.is_infinite() && v > 0.0 => Value::String("Infinity".into()),
        v if v.is_infinite() => Value::String("-Infinity".into()),
        _ => shortest
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map_or(Value::Null, Value::Number),
    }
}

/// `infinity` and `-infinity` timestamps and dates, which chrono cannot hold.
fn infinite<T: Copy + PartialEq + Bounded>(value: T) -> Option<Value> {
    match value {
        v if v == T::MAX => Some(Value::String("infinity".into())),
        v if v == T::MIN => Some(Value::String("-infinity".into())),
        _ => None,
    }
}

trait Bounded {
    const MAX: Self;
    const MIN: Self;
}

impl Bounded for i64 {
    const MAX: Self = i64::MAX;
    const MIN: Self = i64::MIN;
}

impl Bounded for i32 {
    const MAX: Self = i32::MAX;
    const MIN: Self = i32::MIN;
}

/// `24:00:00`, which Postgres accepts as a `time` and chrono wraps to midnight.
const END_OF_DAY_MICROS: i64 = 86_400_000_000;

fn decode_v2(ty: &Type, raw: &[u8]) -> Result<Value, DecodeFailure> {
    let ty = base_type(ty);
    match ty.kind() {
        Kind::Enum(_) => decoded::<&str>(&Type::TEXT, raw).map(Value::from),
        Kind::Array(element) => decode_array(element, raw),
        _ => scalar_decoder(ty)
            .ok_or_else(|| DecodeFailure::Unsupported(format!("type {}", ty.name())))
            .and_then(|decode| decode(ty, raw)),
    }
}

type Decoder = fn(&Type, &[u8]) -> Result<Value, DecodeFailure>;

/// How a scalar type's values render; `None` for a type the v2 envelope
/// does not carry.
fn scalar_decoder(ty: &Type) -> Option<Decoder> {
    Some(match *ty {
        Type::INT2 => |ty, raw| Ok(json!(decoded::<i16>(ty, raw)?)),
        Type::INT4 => |ty, raw| Ok(json!(decoded::<i32>(ty, raw)?)),
        Type::INT8 => |ty, raw| Ok(json!(decoded::<i64>(ty, raw)?)),
        Type::OID => |ty, raw| Ok(json!(decoded::<u32>(ty, raw)?)),
        Type::FLOAT4 => |ty, raw| {
            let v = decoded::<f32>(ty, raw)?;
            Ok(float_json(f64::from(v), v.to_string()))
        },
        Type::FLOAT8 => |ty, raw| {
            let v = decoded::<f64>(ty, raw)?;
            Ok(float_json(v, v.to_string()))
        },
        Type::NUMERIC => |ty, raw| Ok(Value::String(decoded::<PgNumeric>(ty, raw)?.to_string())),
        Type::BOOL => |ty, raw| Ok(Value::Bool(decoded(ty, raw)?)),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            |ty, raw| Ok(Value::String(decoded(ty, raw)?))
        }
        Type::UUID => |ty, raw| Ok(Value::String(decoded::<uuid::Uuid>(ty, raw)?.to_string())),
        Type::TIMESTAMPTZ => |ty, raw| {
            Ok(match infinite(decoded::<i64>(&Type::INT8, raw)?) {
                Some(v) => v,
                None => Value::String(
                    decoded::<chrono::DateTime<chrono::Utc>>(ty, raw)?
                        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                ),
            })
        },
        Type::TIMESTAMP => |ty, raw| {
            Ok(match infinite(decoded::<i64>(&Type::INT8, raw)?) {
                Some(v) => v,
                None => Value::String(
                    decoded::<chrono::NaiveDateTime>(ty, raw)?
                        .format("%Y-%m-%dT%H:%M:%S%.f")
                        .to_string(),
                ),
            })
        },
        Type::DATE => |ty, raw| {
            Ok(match infinite(decoded::<i32>(&Type::INT4, raw)?) {
                Some(v) => v,
                None => Value::String(
                    decoded::<chrono::NaiveDate>(ty, raw)?
                        .format("%Y-%m-%d")
                        .to_string(),
                ),
            })
        },
        Type::TIME => |ty, raw| {
            Ok(match decoded::<i64>(&Type::INT8, raw)? {
                END_OF_DAY_MICROS => Value::String("24:00:00".into()),
                _ => Value::String(
                    decoded::<chrono::NaiveTime>(ty, raw)?
                        .format("%H:%M:%S%.f")
                        .to_string(),
                ),
            })
        },
        Type::INTERVAL => |ty, raw| Ok(Value::String(decoded::<PgInterval>(ty, raw)?.to_iso8601())),
        Type::JSON | Type::JSONB => |ty, raw| decoded::<Value>(ty, raw),
        Type::VOID => |_, _| Ok(Value::Null),
        Type::BYTEA => |ty, raw| {
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(decoded::<&[u8]>(ty, raw)?),
            ))
        },
        _ if ty.name() == "citext" => |_, raw| {
            Ok(Value::String(
                decoded::<&str>(&Type::TEXT, raw)?.to_string(),
            ))
        },
        _ => return None,
    })
}

/// A one-dimensional array: `[ndim][flags][element oid]([len][lower bound])*`
/// then each element as `[len or -1][bytes]`.
fn decode_array(element: &Type, raw: &[u8]) -> Result<Value, DecodeFailure> {
    let truncated = || DecodeFailure::Invalid("truncated array value".into());
    let int = |at: usize| -> Result<i32, DecodeFailure> {
        raw.get(at..at + 4)
            .and_then(|b| b.try_into().ok())
            .map(i32::from_be_bytes)
            .ok_or_else(truncated)
    };
    let (count, mut at) = match int(0)? {
        0 => return Ok(Value::Array(Vec::new())),
        1 => (int(12)?, 20usize),
        dims => {
            return Err(DecodeFailure::Unsupported(format!(
                "a {dims}-dimensional array"
            )))
        }
    };
    (0..count)
        .map(|_| {
            let len = int(at)?;
            at += 4;
            match usize::try_from(len) {
                Err(_) => Ok(Value::Null),
                Ok(len) => {
                    let bytes = raw.get(at..at + len).ok_or_else(truncated)?;
                    at += len;
                    decode_v2(element, bytes)
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

/// A Postgres `interval`: months, days and microseconds kept apart, as the
/// server stores them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgInterval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl PgInterval {
    /// ISO 8601 as Postgres's `iso_8601` interval style prints it: each
    /// component carries its own sign, and a zero interval is `PT0S`.
    pub fn to_iso8601(&self) -> String {
        const MICROS_PER_SECOND: i64 = 1_000_000;
        let date: String = [
            (i64::from(self.months / 12), 'Y'),
            (i64::from(self.months % 12), 'M'),
            (i64::from(self.days), 'D'),
        ]
        .iter()
        .filter(|(n, _)| *n != 0)
        .map(|(n, unit)| format!("{n}{unit}"))
        .collect();
        let hours = self.micros / (3600 * MICROS_PER_SECOND);
        let minutes = self.micros / (60 * MICROS_PER_SECOND) % 60;
        let sub_minute = self.micros % (60 * MICROS_PER_SECOND);
        let seconds = match sub_minute {
            0 => String::new(),
            micros => {
                let sign = if micros < 0 { "-" } else { "" };
                let (whole, frac) = (
                    micros.unsigned_abs() / MICROS_PER_SECOND as u64,
                    micros.unsigned_abs() % MICROS_PER_SECOND as u64,
                );
                let frac = format!("{frac:06}");
                match frac.trim_end_matches('0') {
                    "" => format!("{sign}{whole}S"),
                    digits => format!("{sign}{whole}.{digits}S"),
                }
            }
        };
        let time: String = [(hours, 'H'), (minutes, 'M')]
            .iter()
            .filter(|(n, _)| *n != 0)
            .map(|(n, unit)| format!("{n}{unit}"))
            .chain(std::iter::once(seconds))
            .collect();
        match (date.is_empty(), time.is_empty()) {
            (true, true) => "PT0S".into(),
            (_, true) => format!("P{date}"),
            _ => format!("P{date}T{time}"),
        }
    }
}

impl<'a> FromSql<'a> for PgInterval {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err(format!("an interval is 16 bytes, got {}", raw.len()).into());
        }
        let (micros, rest) = raw.split_at(8);
        let (days, months) = rest.split_at(4);
        Ok(PgInterval {
            micros: i64::from_be_bytes(micros.try_into()?),
            days: i32::from_be_bytes(days.try_into()?),
            months: i32::from_be_bytes(months.try_into()?),
        })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INTERVAL
    }
}

#[cfg(test)]
mod v2_tests {
    use super::*;
    use crate::numeric::PgNumeric;
    use bytes::BytesMut;
    use tokio_postgres::types::ToSql;

    fn encode<T: ToSql>(ty: &Type, value: T) -> Vec<u8> {
        let mut out = BytesMut::new();
        value.to_sql(ty, &mut out).expect("encode");
        out.to_vec()
    }

    fn render<T: ToSql>(ty: &Type, value: T) -> Value {
        decode_v2(ty, &encode(ty, value)).unwrap_or_else(|_| panic!("decode {ty}"))
    }

    fn failure(ty: &Type, raw: &[u8]) -> DbError {
        match decode_v2(ty, raw) {
            Ok(v) => panic!("{ty} decoded to {v}"),
            Err(failure) => failure.for_column("c", ty),
        }
    }

    #[test]
    fn scalars_render_exactly() {
        let uuid = uuid::Uuid::parse_str("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11").expect("uuid");
        let tz = chrono::DateTime::parse_from_rfc3339("2026-03-04T05:06:07.120+02:00")
            .expect("timestamptz")
            .with_timezone(&chrono::Utc);
        let naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 4)
            .and_then(|d| d.and_hms_micro_opt(5, 6, 7, 890))
            .expect("timestamp");
        for (value, expected) in [
            (render(&Type::INT2, 7i16), json!(7)),
            (render(&Type::INT4, -7i32), json!(-7)),
            (render(&Type::INT8, i64::MAX), json!(i64::MAX)),
            (render(&Type::OID, 42u32), json!(42)),
            (render(&Type::FLOAT4, 0.1f32), json!(0.1)),
            (render(&Type::FLOAT8, 2.5f64), json!(2.5)),
            (render(&Type::FLOAT8, f64::NAN), json!("NaN")),
            (render(&Type::FLOAT8, f64::INFINITY), json!("Infinity")),
            (render(&Type::FLOAT4, f32::NEG_INFINITY), json!("-Infinity")),
            (
                render(
                    &Type::NUMERIC,
                    "12345678901234567890.000100"
                        .parse::<PgNumeric>()
                        .expect("n"),
                ),
                json!("12345678901234567890.000100"),
            ),
            (render(&Type::BOOL, true), json!(true)),
            (render(&Type::TEXT, "it's"), json!("it's")),
            (render(&Type::BPCHAR, "ab"), json!("ab")),
            (
                render(&Type::UUID, uuid),
                json!("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
            ),
            (
                render(&Type::TIMESTAMPTZ, tz),
                json!("2026-03-04T03:06:07.120Z"),
            ),
            (
                render(&Type::TIMESTAMP, naive),
                json!("2026-03-04T05:06:07.000890"),
            ),
            (
                render(
                    &Type::TIMESTAMP,
                    naive.date().and_hms_opt(1, 2, 3).expect("t"),
                ),
                json!("2026-03-04T01:02:03"),
            ),
            (render(&Type::DATE, naive.date()), json!("2026-03-04")),
            (render(&Type::TIME, naive.time()), json!("05:06:07.000890")),
            (
                render(&Type::JSONB, json!({"a": [1, null]})),
                json!({"a": [1, null]}),
            ),
            (render(&Type::JSON, json!("s")), json!("s")),
            (render(&Type::BYTEA, &b"\x00\xff"[..]), json!("AP8=")),
        ] {
            assert_eq!(value, expected);
        }
    }

    #[test]
    fn void_results_render_as_null() {
        assert_eq!(decode_v2(&Type::VOID, &[]).ok(), Some(Value::Null));
    }

    #[test]
    fn infinite_dates_and_end_of_day_render_as_postgres_prints_them() {
        assert_eq!(
            decode_v2(&Type::TIMESTAMPTZ, &encode(&Type::INT8, i64::MAX)).ok(),
            Some(json!("infinity"))
        );
        assert_eq!(
            decode_v2(&Type::TIMESTAMP, &encode(&Type::INT8, i64::MIN)).ok(),
            Some(json!("-infinity"))
        );
        assert_eq!(
            decode_v2(&Type::DATE, &encode(&Type::INT4, i32::MAX)).ok(),
            Some(json!("infinity"))
        );
        assert_eq!(
            decode_v2(&Type::TIME, &encode(&Type::INT8, END_OF_DAY_MICROS)).ok(),
            Some(json!("24:00:00"))
        );
    }

    #[test]
    fn intervals_render_as_iso_8601() {
        let interval = |months, days, micros| {
            PgInterval {
                months,
                days,
                micros,
            }
            .to_iso8601()
        };
        assert_eq!(interval(14, 3, 14_706_500_000), "P1Y2M3DT4H5M6.5S");
        assert_eq!(interval(0, 0, 0), "PT0S");
        assert_eq!(interval(0, 1, 0), "P1D");
        assert_eq!(interval(-14, 0, 0), "P-1Y-2M");
        assert_eq!(interval(0, 0, -1_500_000), "PT-1.5S");
        assert_eq!(interval(0, 0, 3_600_000_000), "PT1H");
        assert_eq!(interval(0, 0, 60_000_001), "PT1M0.000001S");
        let mut raw = Vec::new();
        raw.extend_from_slice(&14_706_500_000i64.to_be_bytes());
        raw.extend_from_slice(&3i32.to_be_bytes());
        raw.extend_from_slice(&14i32.to_be_bytes());
        assert_eq!(
            decode_v2(&Type::INTERVAL, &raw).ok(),
            Some(json!("P1Y2M3DT4H5M6.5S"))
        );
        assert_eq!(
            failure(&Type::INTERVAL, &raw[..15]).kind,
            DbErrorKind::Decode
        );
    }

    #[test]
    fn arrays_enums_and_domains_render_through_their_element_type() {
        assert_eq!(
            render(&Type::INT4_ARRAY, vec![Some(1i32), None, Some(3)]),
            json!([1, null, 3])
        );
        assert_eq!(
            render(&Type::TEXT_ARRAY, Vec::<Option<String>>::new()),
            json!([])
        );
        assert_eq!(
            render(
                &Type::NUMERIC_ARRAY,
                vec![Some("0.10".parse::<PgNumeric>().expect("n")), None]
            ),
            json!(["0.10", null])
        );
        let mood = Type::new(
            "mood".into(),
            90_001,
            Kind::Enum(vec!["sad".into(), "happy".into()]),
            "public".into(),
        );
        assert_eq!(decode_v2(&mood, b"happy").ok(), Some(json!("happy")));
        let moods = Type::new("_mood".into(), 90_002, Kind::Array(mood), "public".into());
        let mut raw = Vec::new();
        for word in [1i32, 1, 90_001, 2, 1] {
            raw.extend_from_slice(&word.to_be_bytes());
        }
        raw.extend_from_slice(&3i32.to_be_bytes());
        raw.extend_from_slice(b"sad");
        raw.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(decode_v2(&moods, &raw).ok(), Some(json!(["sad", null])));
        let positive = Type::new(
            "positive".into(),
            90_003,
            Kind::Domain(Type::INT4),
            "public".into(),
        );
        assert_eq!(
            decode_v2(&positive, &encode(&Type::INT4, 5i32)).ok(),
            Some(json!(5))
        );
    }

    #[test]
    fn unsupported_and_corrupt_values_are_errors_naming_the_column() {
        let point = failure(&Type::POINT, &[0; 16]);
        assert_eq!(point.kind, DbErrorKind::UnsupportedType);
        assert!(point.message.contains("column c"), "{}", point.message);
        assert!(point.message.contains("point"), "{}", point.message);
        assert!(point.hint.as_deref().is_some_and(|h| h.contains("c::text")));
        let mut matrix = Vec::new();
        for word in [2i32, 0, 23, 1, 1, 1, 1] {
            matrix.extend_from_slice(&word.to_be_bytes());
        }
        assert_eq!(
            failure(&Type::INT4_ARRAY, &matrix).kind,
            DbErrorKind::UnsupportedType
        );
        assert_eq!(failure(&Type::INT4, &[0, 1]).kind, DbErrorKind::Decode);
        assert_eq!(
            failure(&Type::INT4_ARRAY, &[0, 0, 0, 1]).kind,
            DbErrorKind::Decode
        );
    }
}
