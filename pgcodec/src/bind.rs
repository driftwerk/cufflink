//! Prepare-then-coerce binding: Postgres reports each parameter's type when
//! the statement is prepared, and every tagged guest parameter is converted
//! to exactly that type, losslessly, or refused with a message naming it.

use crate::numeric::PgNumeric;
use crate::row_json::base_type;
use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use cufflink_types::db::{DbError, DbErrorFields, DbErrorKind, Param};
use serde_json::Value;
use std::error::Error;
use std::str::FromStr;
use tokio_postgres::types::{to_sql_checked, IsNull, Kind, ToSql, Type};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    /// Sent as its text bytes: text-like types, enum labels, `unknown`.
    Text(String),
    Uuid(Uuid),
    Timestamptz(DateTime<Utc>),
    Timestamp(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    Json(Value),
    Numeric(PgNumeric),
    Bytes(Vec<u8>),
    TextArray(Vec<Option<String>>),
    UuidArray(Vec<Option<Uuid>>),
    I16Array(Vec<Option<i16>>),
    I32Array(Vec<Option<i32>>),
    I64Array(Vec<Option<i64>>),
    BoolArray(Vec<Option<bool>>),
    F64Array(Vec<Option<f64>>),
    NumericArray(Vec<Option<PgNumeric>>),
    JsonArray(Vec<Option<Value>>),
}

/// Every value is encoded as its type's base: a domain is received by its
/// base type's input function, and an array of domains carries the domain in
/// its header but base-encoded elements.
impl ToSql for Bound {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let ty = base_type(ty);
        match self {
            Bound::Null => Ok(IsNull::Yes),
            Bound::Bool(v) => v.to_sql(ty, out),
            Bound::I16(v) => v.to_sql(ty, out),
            Bound::I32(v) => v.to_sql(ty, out),
            Bound::I64(v) => v.to_sql(ty, out),
            Bound::F32(v) => v.to_sql(ty, out),
            Bound::F64(v) => v.to_sql(ty, out),
            Bound::Text(v) => {
                out.extend_from_slice(v.as_bytes());
                Ok(IsNull::No)
            }
            Bound::Uuid(v) => v.to_sql(ty, out),
            Bound::Timestamptz(v) => v.to_sql(ty, out),
            Bound::Timestamp(v) => v.to_sql(ty, out),
            Bound::Date(v) => v.to_sql(ty, out),
            Bound::Time(v) => v.to_sql(ty, out),
            Bound::Json(v) => v.to_sql(ty, out),
            Bound::Numeric(v) => v.to_sql(ty, out),
            Bound::Bytes(v) => v.to_sql(ty, out),
            Bound::TextArray(v) => array(v, ty, out),
            Bound::UuidArray(v) => array(v, ty, out),
            Bound::I16Array(v) => array(v, ty, out),
            Bound::I32Array(v) => array(v, ty, out),
            Bound::I64Array(v) => array(v, ty, out),
            Bound::BoolArray(v) => array(v, ty, out),
            Bound::F64Array(v) => array(v, ty, out),
            Bound::NumericArray(v) => array(v, ty, out),
            Bound::JsonArray(v) => array(v, ty, out),
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

fn array<T: ToSql>(
    values: &[Option<T>],
    ty: &Type,
    out: &mut BytesMut,
) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
    if !matches!(ty.kind(), Kind::Array(_)) {
        return Err(format!("an array cannot be sent as {}", ty.name()).into());
    }
    values
        .iter()
        .map(|value| value.as_ref().map(OnBase))
        .collect::<Vec<_>>()
        .to_sql(ty, out)
}

/// An array element, encoded as its type's base.
#[derive(Debug)]
struct OnBase<'a, T>(&'a T);

impl<T: ToSql> ToSql for OnBase<'_, T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.0.to_sql(base_type(ty), out)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// Coerce every parameter to the type the prepared statement reported.
pub fn bind(types: &[Type], params: &[Param]) -> Result<Vec<Bound>, DbError> {
    if types.len() != params.len() {
        return Err(DbError::new(
            DbErrorKind::InvalidParameter,
            format!(
                "the statement takes {} parameter(s), {} were given",
                types.len(),
                params.len()
            ),
        ));
    }
    params
        .iter()
        .zip(types)
        .enumerate()
        .map(|(i, (param, ty))| coerce(i + 1, param, ty))
        .collect()
}

/// Why a parameter does not fit its type, before the parameter is named.
enum Refusal {
    Mismatch(Option<String>),
    Unsupported,
}

type Coerced = Result<Bound, Refusal>;

fn mismatch() -> Refusal {
    Refusal::Mismatch(None)
}

fn invalid(reason: impl ToString) -> Refusal {
    Refusal::Mismatch(Some(reason.to_string()))
}

/// Convert parameter `$index` to `ty`, or say why not.
pub fn coerce(index: usize, param: &Param, ty: &Type) -> Result<Bound, DbError> {
    coerce_to(param, ty).map_err(|refusal| match refusal {
        Refusal::Mismatch(reason) => DbError::new(
            DbErrorKind::InvalidParameter,
            format!(
                "parameter ${index}: expected {}, got {}{}",
                ty.name(),
                describe(param),
                reason.map(|r| format!(" ({r})")).unwrap_or_default()
            ),
        ),
        Refusal::Unsupported => DbErrorFields {
            hint: Some(format!(
                "cast it in SQL, e.g. ${index}::text::{}",
                ty.name()
            )),
            ..DbErrorFields::new(
                DbErrorKind::UnsupportedType,
                format!("parameter ${index}: type {} is not supported", ty.name()),
            )
        }
        .into(),
    })
}

/// The parameter's tag, with a short preview of a scalar text value.
fn describe(param: &Param) -> String {
    let text = match param {
        Param::Text(s)
        | Param::Uuid(s)
        | Param::Timestamp(s)
        | Param::NaiveTimestamp(s)
        | Param::Date(s)
        | Param::Time(s)
        | Param::Numeric(s) => Some(s),
        _ => None,
    };
    match text {
        Some(s) if s.chars().count() > 40 => {
            format!(
                "{} '{}…'",
                param.tag(),
                s.chars().take(40).collect::<String>()
            )
        }
        Some(s) => format!("{} '{s}'", param.tag()),
        None => param.tag().to_string(),
    }
}

/// An enum label is sent as given: the server checks it against the labels
/// it has now, which a connection's type cache may predate.
fn coerce_to(param: &Param, ty: &Type) -> Coerced {
    let ty = base_type(ty);
    match ty.kind() {
        Kind::Array(element) => return coerce_array(param, element),
        Kind::Enum(_) => {
            return match param {
                Param::Null => Ok(Bound::Null),
                Param::Text(label) => Ok(Bound::Text(label.clone())),
                _ => Err(mismatch()),
            }
        }
        _ => {}
    }
    if *param == Param::Null {
        return Ok(Bound::Null);
    }
    match *ty {
        Type::BOOL => match param {
            Param::Bool(v) => Ok(Bound::Bool(*v)),
            _ => Err(mismatch()),
        },
        Type::INT2 => integer(param)
            .and_then(|v| i16::try_from(v).map_err(|_| invalid("out of range for int2")))
            .map(Bound::I16),
        Type::INT4 => integer(param)
            .and_then(|v| i32::try_from(v).map_err(|_| invalid("out of range for int4")))
            .map(Bound::I32),
        Type::INT8 => integer(param).map(Bound::I64),
        Type::FLOAT4 => float4(param).map(Bound::F32),
        Type::FLOAT8 => float8(param).map(Bound::F64),
        Type::NUMERIC => numeric(param).map(Bound::Numeric),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => text(param).map(Bound::Text),
        Type::UUID => uuid(param).map(Bound::Uuid),
        Type::TIMESTAMPTZ => match param {
            Param::Timestamp(s) | Param::Text(s) => rfc3339(s).map(Bound::Timestamptz),
            _ => Err(mismatch()),
        },
        Type::TIMESTAMP => match param {
            Param::NaiveTimestamp(s) | Param::Text(s) => naive_timestamp(s).map(Bound::Timestamp),
            Param::Timestamp(s) => rfc3339(s).map(|t| Bound::Timestamp(t.naive_utc())),
            _ => Err(mismatch()),
        },
        Type::DATE => match param {
            Param::Date(s) | Param::Text(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(Bound::Date)
                .map_err(invalid),
            _ => Err(mismatch()),
        },
        Type::TIME => match param {
            Param::Time(s) | Param::Text(s) => NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                .map(Bound::Time)
                .map_err(invalid),
            _ => Err(mismatch()),
        },
        Type::JSON | Type::JSONB => json(param).map(Bound::Json),
        Type::BYTEA => match param {
            Param::Bytes(v) => Ok(Bound::Bytes(v.clone())),
            _ => Err(mismatch()),
        },
        Type::UNKNOWN => match param {
            Param::Text(s) => Ok(Bound::Text(s.clone())),
            _ => Err(mismatch()),
        },
        _ if ty.name() == "citext" => text(param).map(Bound::Text),
        _ => Err(Refusal::Unsupported),
    }
}

fn coerce_array(param: &Param, element: &Type) -> Coerced {
    if *param == Param::Null {
        return Ok(Bound::Null);
    }
    let element = base_type(element);
    if let Kind::Enum(_) = element.kind() {
        return match param {
            Param::TextArray(values) => Ok(Bound::TextArray(values.clone())),
            _ => Err(mismatch()),
        };
    }
    match (element, param) {
        (&Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME, Param::TextArray(v))
        | (&Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME, Param::UuidArray(v)) => {
            Ok(Bound::TextArray(v.clone()))
        }
        (&Type::UUID, Param::UuidArray(v) | Param::TextArray(v)) => {
            each(v, |s| Uuid::parse_str(s).map_err(invalid)).map(Bound::UuidArray)
        }
        (&Type::INT2 | &Type::INT4 | &Type::INT8, Param::TextArray(v)) => {
            each(v, |s| integer(&Param::Text(s.clone())))
                .and_then(|integers| coerce_array(&Param::I64Array(integers), element))
        }
        (&Type::INT2, Param::I32Array(v)) => each(v, narrow::<i32, i16>).map(Bound::I16Array),
        (&Type::INT2, Param::I64Array(v)) => each(v, narrow::<i64, i16>).map(Bound::I16Array),
        (&Type::INT4, Param::I32Array(v)) => Ok(Bound::I32Array(v.clone())),
        (&Type::INT4, Param::I64Array(v)) => each(v, narrow::<i64, i32>).map(Bound::I32Array),
        (&Type::INT8, Param::I32Array(v)) => Ok(Bound::I64Array(
            v.iter().map(|x| x.map(i64::from)).collect(),
        )),
        (&Type::INT8, Param::I64Array(v)) => Ok(Bound::I64Array(v.clone())),
        (&Type::BOOL, Param::BoolArray(v)) => Ok(Bound::BoolArray(v.clone())),
        (&Type::FLOAT8, Param::F64Array(v)) => Ok(Bound::F64Array(v.clone())),
        (&Type::FLOAT8, Param::TextArray(v)) => {
            each(v, |s| float8(&Param::Text(s.clone()))).map(Bound::F64Array)
        }
        (&Type::NUMERIC, Param::NumericArray(v) | Param::TextArray(v)) => {
            each(v, |s| s.parse::<PgNumeric>().map_err(invalid)).map(Bound::NumericArray)
        }
        (&Type::JSON | &Type::JSONB, Param::Json(Value::Array(v))) => {
            Ok(Bound::JsonArray(v.iter().cloned().map(Some).collect()))
        }
        (
            &Type::TEXT
            | &Type::VARCHAR
            | &Type::BPCHAR
            | &Type::NAME
            | &Type::UUID
            | &Type::INT2
            | &Type::INT4
            | &Type::INT8
            | &Type::BOOL
            | &Type::FLOAT8
            | &Type::NUMERIC
            | &Type::JSON
            | &Type::JSONB,
            _,
        ) => Err(mismatch()),
        _ => Err(Refusal::Unsupported),
    }
}

fn each<T, U>(
    values: &[Option<T>],
    convert: impl Fn(&T) -> Result<U, Refusal>,
) -> Result<Vec<Option<U>>, Refusal> {
    values
        .iter()
        .map(|v| v.as_ref().map(&convert).transpose())
        .collect()
}

fn narrow<T: Copy, U: TryFrom<T>>(value: &T) -> Result<U, Refusal> {
    U::try_from(*value).map_err(|_| invalid("element out of range"))
}

fn integer(param: &Param) -> Result<i64, Refusal> {
    match param {
        Param::I16(v) => Ok(i64::from(*v)),
        Param::I32(v) => Ok(i64::from(*v)),
        Param::I64(v) => Ok(*v),
        Param::Text(s) => s.trim().parse().map_err(|_| invalid("not an integer")),
        Param::Numeric(s) => s
            .parse::<PgNumeric>()
            .ok()
            .and_then(|n| n.to_i64())
            .ok_or_else(|| invalid("not an integer in range")),
        _ => Err(mismatch()),
    }
}

/// A binary float type a parameter can be coerced to.
trait Float: FromStr + Copy + PartialEq {
    const ZERO: Self;
    fn is_finite(self) -> bool;
}

impl Float for f32 {
    const ZERO: Self = 0.0;
    fn is_finite(self) -> bool {
        f32::is_finite(self)
    }
}

impl Float for f64 {
    const ZERO: Self = 0.0;
    fn is_finite(self) -> bool {
        f64::is_finite(self)
    }
}

/// Decimal text at the target width, rounded to nearest. `NaN` and the
/// infinities only when spelled out; a value that overflows to an infinity
/// or underflows to zero is refused, as Postgres's own input refuses it.
fn parse_float<F: Float>(text: &str) -> Result<F, Refusal> {
    let text = text.trim();
    let value = text.parse::<F>().map_err(|_| invalid("not a number"))?;
    let spelled_out = matches!(
        text.trim_start_matches(['+', '-'])
            .to_ascii_lowercase()
            .as_str(),
        "nan" | "inf" | "infinity"
    );
    let mantissa_is_zero = text
        .split(['e', 'E'])
        .next()
        .is_none_or(|mantissa| !mantissa.bytes().any(|b| (b'1'..=b'9').contains(&b)));
    match value {
        v if !v.is_finite() && !spelled_out => Err(invalid("out of range")),
        v if v == F::ZERO && !mantissa_is_zero => Err(invalid("out of range")),
        v => Ok(v),
    }
}

/// An integer, when the float holds it exactly.
fn exact_integer<F: Float>(
    value: i64,
    to_float: fn(i64) -> F,
    to_int: fn(F) -> i128,
) -> Result<F, Refusal> {
    let float = to_float(value);
    (to_int(float) == i128::from(value))
        .then_some(float)
        .ok_or_else(|| invalid("not exactly representable as a float"))
}

fn float8(param: &Param) -> Result<f64, Refusal> {
    let exact = |v: i64| exact_integer(v, |v| v as f64, |f| f as i128);
    match param {
        Param::F32(v) => Ok(f64::from(*v)),
        Param::F64(v) => Ok(*v),
        Param::I16(v) => Ok(f64::from(*v)),
        Param::I32(v) => Ok(f64::from(*v)),
        Param::I64(v) => exact(*v),
        Param::Numeric(s) | Param::Text(s) => parse_float(s),
        _ => Err(mismatch()),
    }
}

/// A `float8` value is rounded to `float4` as Postgres's cast rounds it,
/// but one too large or too small for `float4` is refused.
fn float4(param: &Param) -> Result<f32, Refusal> {
    let exact = |v: i64| exact_integer(v, |v| v as f32, |f| f as i128);
    match param {
        Param::F32(v) => Ok(*v),
        Param::F64(v) => {
            let narrowed = *v as f32;
            let overflow = v.is_finite() && narrowed.is_infinite();
            let underflow = *v != 0.0 && narrowed == 0.0;
            (!overflow && !underflow)
                .then_some(narrowed)
                .ok_or_else(|| invalid("out of range for float4"))
        }
        Param::I16(v) => Ok(f32::from(*v)),
        Param::I32(v) => exact(i64::from(*v)),
        Param::I64(v) => exact(*v),
        Param::Numeric(s) | Param::Text(s) => parse_float(s),
        _ => Err(mismatch()),
    }
}

fn numeric(param: &Param) -> Result<PgNumeric, Refusal> {
    let finite = |v: f64, shortest: String| {
        v.is_finite()
            .then_some(shortest)
            .ok_or_else(|| invalid("not finite"))
    };
    let text = match param {
        Param::Numeric(s) | Param::Text(s) => s.clone(),
        Param::I16(v) => v.to_string(),
        Param::I32(v) => v.to_string(),
        Param::I64(v) => v.to_string(),
        Param::F64(v) => finite(*v, v.to_string())?,
        Param::F32(v) => finite(f64::from(*v), v.to_string())?,
        _ => return Err(mismatch()),
    };
    text.parse().map_err(invalid)
}

fn text(param: &Param) -> Result<String, Refusal> {
    match param {
        Param::Text(s) | Param::Uuid(s) => Ok(s.clone()),
        _ => Err(mismatch()),
    }
}

fn uuid(param: &Param) -> Result<Uuid, Refusal> {
    match param {
        Param::Uuid(s) | Param::Text(s) => Uuid::parse_str(s).map_err(|_| invalid("invalid uuid")),
        _ => Err(mismatch()),
    }
}

fn rfc3339(s: &str) -> Result<DateTime<Utc>, Refusal> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| invalid("not an RFC 3339 timestamp with an offset"))
}

fn naive_timestamp(s: &str) -> Result<NaiveDateTime, Refusal> {
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
        .iter()
        .find_map(|format| NaiveDateTime::parse_from_str(s, format).ok())
        .ok_or_else(|| invalid("not a timestamp without an offset"))
}

fn json(param: &Param) -> Result<Value, Refusal> {
    Ok(match param {
        Param::Json(v) => v.clone(),
        Param::Bool(v) => Value::Bool(*v),
        Param::I16(v) => Value::from(*v),
        Param::I32(v) => Value::from(*v),
        Param::I64(v) => Value::from(*v),
        Param::F32(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(Value::Number)
            .ok_or_else(|| invalid("not finite"))?,
        Param::F64(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .ok_or_else(|| invalid("not finite"))?,
        Param::Text(s)
        | Param::Uuid(s)
        | Param::Timestamp(s)
        | Param::NaiveTimestamp(s)
        | Param::Date(s)
        | Param::Time(s)
        | Param::Numeric(s) => Value::String(s.clone()),
        _ => return Err(mismatch()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ok(param: Param, ty: &Type) -> Bound {
        coerce(1, &param, ty).unwrap_or_else(|e| panic!("{ty} from {param:?}: {e}"))
    }

    fn err(param: Param, ty: &Type) -> DbError {
        match coerce(3, &param, ty) {
            Ok(bound) => panic!("{ty} accepted {param:?} as {bound:?}"),
            Err(e) => e,
        }
    }

    fn enum_type() -> Type {
        Type::new(
            "mood".into(),
            90_001,
            Kind::Enum(vec!["sad".into(), "happy".into()]),
            "public".into(),
        )
    }

    fn domain_over(base: Type) -> Type {
        Type::new(
            format!("{}_domain", base.name()),
            90_002,
            Kind::Domain(base),
            "public".into(),
        )
    }

    #[test]
    fn integers_are_range_checked_and_parsed_exactly() {
        assert_eq!(ok(Param::I64(7), &Type::INT2), Bound::I16(7));
        assert_eq!(ok(Param::I16(7), &Type::INT8), Bound::I64(7));
        assert_eq!(ok(Param::Text(" 42 ".into()), &Type::INT4), Bound::I32(42));
        assert_eq!(
            ok(Param::Numeric("42.000".into()), &Type::INT8),
            Bound::I64(42)
        );
        let range = err(Param::I64(70_000), &Type::INT2);
        assert_eq!(range.kind, DbErrorKind::InvalidParameter);
        assert_eq!(
            range.message,
            "parameter $3: expected int2, got i64 (out of range for int2)"
        );
        err(Param::I64(i64::from(i32::MAX) + 1), &Type::INT4);
        err(Param::Numeric("1.5".into()), &Type::INT4);
        err(Param::Text("1.5".into()), &Type::INT4);
        err(Param::F64(1.0), &Type::INT4);
        err(Param::Bool(true), &Type::INT4);
    }

    #[test]
    fn floats_take_numbers_that_fit_exactly() {
        assert_eq!(ok(Param::F64(1.5), &Type::FLOAT8), Bound::F64(1.5));
        assert_eq!(ok(Param::F64(0.5), &Type::FLOAT4), Bound::F32(0.5));
        assert_eq!(
            ok(Param::I64(1 << 53), &Type::FLOAT8),
            Bound::F64(9_007_199_254_740_992.0)
        );
        assert_eq!(
            ok(Param::I64(1 << 60), &Type::FLOAT8),
            Bound::F64(1_152_921_504_606_846_976.0),
            "a power of two is exact"
        );
        err(Param::I64((1 << 53) + 1), &Type::FLOAT8);
        err(Param::I64(i64::MAX), &Type::FLOAT8);
        assert_eq!(
            ok(Param::Text("2.25".into()), &Type::FLOAT8),
            Bound::F64(2.25)
        );
        assert!(
            matches!(ok(Param::Numeric("NaN".into()), &Type::FLOAT8), Bound::F64(v) if v.is_nan())
        );
        err(Param::Text("two".into()), &Type::FLOAT8);
    }

    #[test]
    fn float4_refuses_overflow_underflow_and_inexact_integers() {
        assert_eq!(ok(Param::F64(0.1), &Type::FLOAT4), Bound::F32(0.1));
        assert_eq!(ok(Param::F64(-0.0), &Type::FLOAT4), Bound::F32(-0.0));
        assert_eq!(
            ok(Param::Text("0.1".into()), &Type::FLOAT4),
            ok(Param::F64(0.1), &Type::FLOAT4),
            "text and float8 round alike"
        );
        for too_far in [1e300, -1e300, 1e-300, f64::from(f32::MAX) * 2.0] {
            let error = err(Param::F64(too_far), &Type::FLOAT4);
            assert!(error.message.contains("out of range"), "{too_far}: {error}");
        }
        for explicit in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                matches!(ok(Param::F64(explicit), &Type::FLOAT4), Bound::F32(v) if v.is_nan() || v.is_infinite())
            );
        }
        assert_eq!(
            ok(Param::I32(1 << 24), &Type::FLOAT4),
            Bound::F32(16_777_216.0)
        );
        err(Param::I32((1 << 24) + 1), &Type::FLOAT4);
        assert!(err(Param::I64((1 << 40) + 1), &Type::FLOAT4)
            .message
            .contains("not exactly representable"));
        assert_eq!(
            ok(Param::I64(1 << 40), &Type::FLOAT4),
            Bound::F32(1_099_511_627_776.0)
        );
        assert_eq!(ok(Param::I16(-7), &Type::FLOAT4), Bound::F32(-7.0));
    }

    #[test]
    fn float_text_is_finite_unless_spelled_out() {
        for (text, ty) in [
            ("1e400", Type::FLOAT8),
            ("-1e400", Type::FLOAT8),
            ("1e-400", Type::FLOAT8),
            ("1e40", Type::FLOAT4),
            ("1e-50", Type::FLOAT4),
        ] {
            let error = err(Param::Text(text.into()), &ty);
            assert!(error.message.contains("out of range"), "{text}: {error}");
        }
        for (text, ty) in [
            ("0", Type::FLOAT4),
            ("0.000e-999", Type::FLOAT8),
            ("-0", Type::FLOAT8),
        ] {
            assert!(
                matches!(ok(Param::Text(text.into()), &ty), Bound::F32(v) if v == 0.0)
                    || matches!(ok(Param::Text(text.into()), &ty), Bound::F64(v) if v == 0.0),
                "{text}"
            );
        }
        for spelled in [
            "Infinity",
            "-Infinity",
            "inf",
            "+INF",
            "NaN",
            "nan",
            " infinity ",
        ] {
            assert!(
                matches!(ok(Param::Text(spelled.into()), &Type::FLOAT8), Bound::F64(v) if !v.is_finite()),
                "{spelled}"
            );
            assert!(
                matches!(ok(Param::Numeric(spelled.into()), &Type::FLOAT4), Bound::F32(v) if !v.is_finite()),
                "{spelled}"
            );
        }
    }

    #[test]
    fn numerics_stay_exact() {
        let n = |s: &str| Bound::Numeric(s.parse().expect("numeric"));
        assert_eq!(ok(Param::Numeric("0.1".into()), &Type::NUMERIC), n("0.1"));
        assert_eq!(ok(Param::Text("12.50".into()), &Type::NUMERIC), n("12.50"));
        assert_eq!(ok(Param::I64(-3), &Type::NUMERIC), n("-3"));
        assert_eq!(ok(Param::F64(0.1), &Type::NUMERIC), n("0.1"));
        err(Param::F64(f64::NAN), &Type::NUMERIC);
        err(Param::Text("1,5".into()), &Type::NUMERIC);
        err(Param::Bool(true), &Type::NUMERIC);
    }

    #[test]
    fn text_uuid_and_time_types_parse_their_grammar() {
        let id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        assert_eq!(
            ok(Param::Text("x".into()), &Type::VARCHAR),
            Bound::Text("x".into())
        );
        assert_eq!(
            ok(Param::Uuid(id.into()), &Type::TEXT),
            Bound::Text(id.into())
        );
        err(Param::I32(1), &Type::TEXT);
        let parsed = Uuid::parse_str(id).expect("uuid");
        assert_eq!(ok(Param::Uuid(id.into()), &Type::UUID), Bound::Uuid(parsed));
        assert_eq!(ok(Param::Text(id.into()), &Type::UUID), Bound::Uuid(parsed));
        assert_eq!(
            err(Param::Text("abc".into()), &Type::UUID).message,
            "parameter $3: expected uuid, got text 'abc' (invalid uuid)"
        );
        let utc = DateTime::parse_from_rfc3339("2026-01-01T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        assert_eq!(
            ok(
                Param::Timestamp("2026-01-01T12:00:00+02:00".into()),
                &Type::TIMESTAMPTZ
            ),
            Bound::Timestamptz(utc)
        );
        err(
            Param::Timestamp("2026-01-01T12:00:00".into()),
            &Type::TIMESTAMPTZ,
        );
        assert_eq!(
            ok(
                Param::Timestamp("2026-01-01T12:00:00+02:00".into()),
                &Type::TIMESTAMP
            ),
            Bound::Timestamp(utc.naive_utc())
        );
        for naive in [
            "2026-01-01T10:00:00",
            "2026-01-01 10:00:00",
            "2026-01-01 10:00:00.000",
        ] {
            assert_eq!(
                ok(Param::NaiveTimestamp(naive.into()), &Type::TIMESTAMP),
                Bound::Timestamp(utc.naive_utc()),
                "{naive}"
            );
        }
        assert_eq!(
            ok(Param::Date("2026-02-03".into()), &Type::DATE),
            Bound::Date(NaiveDate::from_ymd_opt(2026, 2, 3).expect("date"))
        );
        err(Param::Date("2026-02-30".into()), &Type::DATE);
        assert_eq!(
            ok(Param::Time("04:05:06.5".into()), &Type::TIME),
            Bound::Time(NaiveTime::from_hms_milli_opt(4, 5, 6, 500).expect("time"))
        );
    }

    #[test]
    fn json_wraps_scalars_and_bytes_need_bytes() {
        assert_eq!(
            ok(Param::Json(json!({"a": 1})), &Type::JSONB),
            Bound::Json(json!({"a": 1}))
        );
        assert_eq!(ok(Param::I32(1), &Type::JSON), Bound::Json(json!(1)));
        assert_eq!(
            ok(Param::Text("s".into()), &Type::JSONB),
            Bound::Json(json!("s"))
        );
        err(Param::F64(f64::INFINITY), &Type::JSONB);
        assert_eq!(
            ok(Param::Bytes(vec![1]), &Type::BYTEA),
            Bound::Bytes(vec![1])
        );
        err(Param::Text("AQ==".into()), &Type::BYTEA);
    }

    #[test]
    fn nulls_bind_to_anything_supported() {
        for ty in [
            Type::INT4,
            Type::TEXT,
            Type::JSONB,
            Type::UUID_ARRAY,
            enum_type(),
        ] {
            assert_eq!(ok(Param::Null, &ty), Bound::Null, "{ty}");
        }
    }

    #[test]
    fn enums_domains_and_unknown_follow_their_rules() {
        assert_eq!(
            ok(Param::Text("happy".into()), &enum_type()),
            Bound::Text("happy".into())
        );
        assert_eq!(
            ok(Param::Text("added_later".into()), &enum_type()),
            Bound::Text("added_later".into()),
            "labels are the server's to check"
        );
        err(Param::I32(1), &enum_type());
        let domain = domain_over(Type::INT4);
        assert_eq!(ok(Param::I64(5), &domain), Bound::I32(5));
        let nested = Type::new(
            "small_positive".into(),
            90_005,
            Kind::Domain(domain),
            "public".into(),
        );
        assert_eq!(ok(Param::I64(5), &nested), Bound::I32(5));
        assert_eq!(
            ok(Param::Text("sad".into()), &domain_over(enum_type())),
            Bound::Text("sad".into())
        );
        assert_eq!(
            ok(Param::Text("x".into()), &Type::UNKNOWN),
            Bound::Text("x".into())
        );
        err(Param::I32(1), &Type::UNKNOWN);
    }

    #[test]
    fn arrays_bind_element_by_element() {
        let id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        assert_eq!(
            ok(
                Param::TextArray(vec![Some("a".into()), None]),
                &Type::TEXT_ARRAY
            ),
            Bound::TextArray(vec![Some("a".into()), None])
        );
        assert_eq!(
            ok(
                Param::TextArray(vec![Some(id.into()), None]),
                &Type::UUID_ARRAY
            ),
            Bound::UuidArray(vec![Some(Uuid::parse_str(id).expect("uuid")), None])
        );
        err(
            Param::TextArray(vec![Some("nope".into())]),
            &Type::UUID_ARRAY,
        );
        assert_eq!(
            ok(Param::I32Array(vec![Some(1), None]), &Type::INT8_ARRAY),
            Bound::I64Array(vec![Some(1), None])
        );
        assert_eq!(
            ok(Param::I64Array(vec![Some(2)]), &Type::INT2_ARRAY),
            Bound::I16Array(vec![Some(2)])
        );
        err(Param::I64Array(vec![Some(i64::MAX)]), &Type::INT4_ARRAY);
        assert_eq!(
            ok(
                Param::TextArray(vec![Some(" 7".into()), None]),
                &Type::INT4_ARRAY
            ),
            Bound::I32Array(vec![Some(7), None])
        );
        err(
            Param::TextArray(vec![Some("seven".into())]),
            &Type::INT8_ARRAY,
        );
        err(
            Param::TextArray(vec![Some("70000".into())]),
            &Type::INT2_ARRAY,
        );
        assert_eq!(
            ok(
                Param::TextArray(vec![Some("2.5".into())]),
                &Type::FLOAT8_ARRAY
            ),
            Bound::F64Array(vec![Some(2.5)])
        );
        assert_eq!(
            ok(
                Param::TextArray(vec![Some("1.10".into())]),
                &Type::NUMERIC_ARRAY
            ),
            Bound::NumericArray(vec![Some("1.10".parse().expect("numeric"))])
        );
        assert_eq!(
            ok(Param::BoolArray(vec![None]), &Type::BOOL_ARRAY),
            Bound::BoolArray(vec![None])
        );
        assert_eq!(
            ok(Param::F64Array(vec![Some(1.5)]), &Type::FLOAT8_ARRAY),
            Bound::F64Array(vec![Some(1.5)])
        );
        assert_eq!(
            ok(
                Param::NumericArray(vec![Some("1.10".into())]),
                &Type::NUMERIC_ARRAY
            ),
            Bound::NumericArray(vec![Some("1.10".parse().expect("numeric"))])
        );
        assert_eq!(
            ok(Param::Json(json!([1, "a"])), &Type::JSONB_ARRAY),
            Bound::JsonArray(vec![Some(json!(1)), Some(json!("a"))])
        );
        let moods = Type::new(
            "_mood".into(),
            90_003,
            Kind::Array(enum_type()),
            "public".into(),
        );
        assert_eq!(
            ok(Param::TextArray(vec![Some("sad".into())]), &moods),
            Bound::TextArray(vec![Some("sad".into())])
        );
        assert_eq!(
            ok(Param::TextArray(vec![Some("added".into())]), &moods),
            Bound::TextArray(vec![Some("added".into())]),
            "labels are the server's to check"
        );
        err(Param::I32Array(vec![Some(1)]), &Type::TEXT_ARRAY);
        assert_eq!(
            err(Param::TextArray(vec![]), &Type::POINT_ARRAY).kind,
            DbErrorKind::UnsupportedType
        );
    }

    #[test]
    fn unsupported_types_suggest_a_cast() {
        let error = err(Param::Text("(1,2)".into()), &Type::POINT);
        assert_eq!(error.kind, DbErrorKind::UnsupportedType);
        assert_eq!(error.message, "parameter $3: type point is not supported");
        assert_eq!(
            error.hint.as_deref(),
            Some("cast it in SQL, e.g. $3::text::point")
        );
    }

    #[test]
    fn the_count_must_match_the_statement() {
        let error = bind(&[Type::INT4], &[]).expect_err("count");
        assert_eq!(error.kind, DbErrorKind::InvalidParameter);
        assert_eq!(
            error.message,
            "the statement takes 1 parameter(s), 0 were given"
        );
        assert_eq!(
            bind(
                &[Type::INT4, Type::TEXT],
                &[Param::I32(1), Param::Text("a".into())]
            )
            .expect("bound"),
            vec![Bound::I32(1), Bound::Text("a".into())]
        );
    }

    #[test]
    fn long_values_are_previewed() {
        let error = err(Param::Text("x".repeat(100)), &Type::UUID);
        assert!(
            error.message.contains(&format!("'{}…'", "x".repeat(40))),
            "{}",
            error.message
        );
    }

    #[test]
    fn bound_values_encode_as_their_type() {
        let encode = |bound: Bound, ty: Type| {
            let mut out = BytesMut::new();
            let null = bound.to_sql_checked(&ty, &mut out).expect("encode");
            (matches!(null, IsNull::Yes), out.to_vec())
        };
        assert_eq!(encode(Bound::Null, Type::INT4), (true, vec![]));
        assert_eq!(encode(Bound::I32(1), Type::INT4), (false, vec![0, 0, 0, 1]));
        assert_eq!(
            encode(Bound::Text("happy".into()), enum_type()),
            (false, b"happy".to_vec())
        );
        assert_eq!(
            encode(Bound::Json(json!(1)), Type::JSONB),
            (false, b"\x011".to_vec())
        );
    }

    #[test]
    fn domains_encode_as_their_base_type() {
        let encode = |bound: Bound, ty: &Type| {
            let mut out = BytesMut::new();
            bound.to_sql_checked(ty, &mut out).expect("encode");
            out.to_vec()
        };
        let jsonb = domain_over(Type::JSONB);
        assert_eq!(
            encode(Bound::Json(json!(1)), &jsonb),
            encode(Bound::Json(json!(1)), &Type::JSONB),
            "jsonb's version byte"
        );
        let tags = domain_over(Type::TEXT_ARRAY);
        assert_eq!(
            encode(Bound::TextArray(vec![Some("a".into())]), &tags),
            encode(Bound::TextArray(vec![Some("a".into())]), &Type::TEXT_ARRAY)
        );
        let int = domain_over(Type::INT4);
        assert_eq!(encode(Bound::I32(5), &int), vec![0, 0, 0, 5]);

        let docs = Type::new(
            "_jsonb_domain".into(),
            90_004,
            Kind::Array(jsonb.clone()),
            "public".into(),
        );
        let encoded = encode(Bound::JsonArray(vec![Some(json!(1)), None]), &docs);
        let element_oid = u32::from_be_bytes(encoded[8..12].try_into().expect("oid"));
        assert_eq!(element_oid, jsonb.oid(), "the header names the domain");
        assert_eq!(&encoded[20..24], &2i32.to_be_bytes(), "element length");
        assert_eq!(&encoded[24..26], b"\x011", "a base-encoded jsonb element");
        assert_eq!(&encoded[26..30], &(-1i32).to_be_bytes(), "a null element");

        let mut out = BytesMut::new();
        assert!(
            Bound::TextArray(vec![])
                .to_sql_checked(&Type::TEXT, &mut out)
                .is_err(),
            "an array sent as a scalar is an error, not a panic"
        );
    }
}
