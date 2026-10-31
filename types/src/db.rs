//! The `db_call` wire contract shared by the guest SDK and the platform:
//! requests, responses, bound parameters and structured database errors.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt;

/// A bound parameter. Serializes as `{"t": <tag>, "v": <value>}`; `Null`
/// carries no `v`, `Bytes` is base64 and non-finite floats are the strings
/// `"NaN"`, `"Infinity"` and `"-Infinity"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
pub enum Param {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(#[serde(with = "float")] f32),
    F64(#[serde(with = "float")] f64),
    Text(String),
    Uuid(String),
    /// RFC 3339 with an offset.
    Timestamp(String),
    NaiveTimestamp(String),
    Date(String),
    Time(String),
    Json(Value),
    /// Exact decimal text.
    Numeric(String),
    Bytes(#[serde(with = "crate::serde_util::base64_bytes")] Vec<u8>),
    TextArray(Vec<Option<String>>),
    UuidArray(Vec<Option<String>>),
    I32Array(Vec<Option<i32>>),
    I64Array(Vec<Option<i64>>),
    BoolArray(Vec<Option<bool>>),
    F64Array(#[serde(with = "float_array")] Vec<Option<f64>>),
    NumericArray(Vec<Option<String>>),
}

impl Param {
    /// The wire tag, which names the parameter's kind in errors.
    pub fn tag(&self) -> &'static str {
        match self {
            Param::Null => "null",
            Param::Bool(_) => "bool",
            Param::I16(_) => "i16",
            Param::I32(_) => "i32",
            Param::I64(_) => "i64",
            Param::F32(_) => "f32",
            Param::F64(_) => "f64",
            Param::Text(_) => "text",
            Param::Uuid(_) => "uuid",
            Param::Timestamp(_) => "timestamp",
            Param::NaiveTimestamp(_) => "naive_timestamp",
            Param::Date(_) => "date",
            Param::Time(_) => "time",
            Param::Json(_) => "json",
            Param::Numeric(_) => "numeric",
            Param::Bytes(_) => "bytes",
            Param::TextArray(_) => "text_array",
            Param::UuidArray(_) => "uuid_array",
            Param::I32Array(_) => "i32_array",
            Param::I64Array(_) => "i64_array",
            Param::BoolArray(_) => "bool_array",
            Param::F64Array(_) => "f64_array",
            Param::NumericArray(_) => "numeric_array",
        }
    }
}

macro_rules! param_from {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(impl From<$ty> for Param {
            fn from(value: $ty) -> Self {
                Param::$variant(value.into())
            }
        })*
    };
}

param_from! {
    bool => Bool,
    i16 => I16,
    i32 => I32,
    i64 => I64,
    f32 => F32,
    f64 => F64,
    String => Text,
    &str => Text,
    Value => Json,
    Vec<u8> => Bytes,
    &[u8] => Bytes,
}

impl From<Vec<String>> for Param {
    fn from(values: Vec<String>) -> Self {
        Param::TextArray(values.into_iter().map(Some).collect())
    }
}

impl From<Vec<&str>> for Param {
    fn from(values: Vec<&str>) -> Self {
        Param::TextArray(values.into_iter().map(|v| Some(v.to_string())).collect())
    }
}

impl From<Vec<i32>> for Param {
    fn from(values: Vec<i32>) -> Self {
        Param::I32Array(values.into_iter().map(Some).collect())
    }
}

impl From<Vec<i64>> for Param {
    fn from(values: Vec<i64>) -> Self {
        Param::I64Array(values.into_iter().map(Some).collect())
    }
}

impl<T: Into<Param>> From<Option<T>> for Param {
    fn from(value: Option<T>) -> Self {
        value.map_or(Param::Null, Into::into)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// One `db_call` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DbRequest {
    Query {
        sql: String,
        #[serde(default)]
        params: Vec<Param>,
        #[serde(default)]
        tx: Option<u32>,
    },
    Execute {
        sql: String,
        #[serde(default)]
        params: Vec<Param>,
        #[serde(default)]
        tx: Option<u32>,
    },
    Begin {
        #[serde(default)]
        isolation: Isolation,
        #[serde(default)]
        read_only: bool,
    },
    Commit {
        tx: u32,
    },
    Rollback {
        tx: u32,
    },
    Savepoint {
        tx: u32,
        name: String,
    },
    Release {
        tx: u32,
        name: String,
    },
    RollbackTo {
        tx: u32,
        name: String,
    },
}

impl DbRequest {
    /// The `op` tag, which is also the metric label.
    pub fn op(&self) -> &'static str {
        match self {
            DbRequest::Query { .. } => "query",
            DbRequest::Execute { .. } => "execute",
            DbRequest::Begin { .. } => "begin",
            DbRequest::Commit { .. } => "commit",
            DbRequest::Rollback { .. } => "rollback",
            DbRequest::Savepoint { .. } => "savepoint",
            DbRequest::Release { .. } => "release",
            DbRequest::RollbackTo { .. } => "rollback_to",
        }
    }
}

/// A result column: its name and SQL type name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}

pub type RowMap = Map<String, Value>;

/// One `db_call` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "WireResponse", try_from = "WireResponse")]
pub enum DbResponse {
    Rows {
        rows: Vec<RowMap>,
        columns: Vec<Column>,
    },
    Affected(u64),
    Begun(u32),
    Done,
    Failed(DbError),
}

#[derive(Serialize, Deserialize)]
struct WireResponse {
    ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rows: Option<Vec<RowMap>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<Column>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    affected: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tx: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<DbError>,
}

impl WireResponse {
    fn ok() -> Self {
        Self {
            ok: true,
            rows: None,
            columns: None,
            affected: None,
            tx: None,
            error: None,
        }
    }
}

impl From<DbResponse> for WireResponse {
    fn from(response: DbResponse) -> Self {
        match response {
            DbResponse::Rows { rows, columns } => Self {
                rows: Some(rows),
                columns: Some(columns),
                ..Self::ok()
            },
            DbResponse::Affected(affected) => Self {
                affected: Some(affected),
                ..Self::ok()
            },
            DbResponse::Begun(tx) => Self {
                tx: Some(tx),
                ..Self::ok()
            },
            DbResponse::Done => Self::ok(),
            DbResponse::Failed(error) => Self {
                ok: false,
                error: Some(error),
                ..Self::ok()
            },
        }
    }
}

impl TryFrom<WireResponse> for DbResponse {
    type Error = String;

    fn try_from(wire: WireResponse) -> Result<Self, String> {
        match wire {
            WireResponse {
                ok: false,
                error: Some(error),
                ..
            } => Ok(DbResponse::Failed(error)),
            WireResponse { ok: false, .. } => Err("a failed response carries no error".into()),
            WireResponse {
                rows: Some(rows),
                columns,
                ..
            } => Ok(DbResponse::Rows {
                rows,
                columns: columns.unwrap_or_default(),
            }),
            WireResponse {
                affected: Some(affected),
                ..
            } => Ok(DbResponse::Affected(affected)),
            WireResponse { tx: Some(tx), .. } => Ok(DbResponse::Begun(tx)),
            WireResponse { .. } => Ok(DbResponse::Done),
        }
    }
}

/// A database failure as the guest sees it: the Postgres error fields when
/// the server reported one, and a [`DbErrorKind`] either way. Boxed so a
/// `Result<_, DbError>` stays a pointer wide; the fields read through
/// `Deref`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DbError(Box<DbErrorFields>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbErrorFields {
    pub code: Option<String>,
    pub kind: DbErrorKind,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub constraint: Option<String>,
    pub table: Option<String>,
    pub column: Option<String>,
    pub schema: Option<String>,
    pub position: Option<u32>,
}

impl DbErrorFields {
    /// An error raised outside the server, with no Postgres fields.
    pub fn new(kind: DbErrorKind, message: impl Into<String>) -> Self {
        Self {
            code: None,
            kind,
            message: message.into(),
            detail: None,
            hint: None,
            constraint: None,
            table: None,
            column: None,
            schema: None,
            position: None,
        }
    }
}

impl From<DbErrorFields> for DbError {
    fn from(fields: DbErrorFields) -> Self {
        Self(Box::new(fields))
    }
}

impl std::ops::Deref for DbError {
    type Target = DbErrorFields;

    fn deref(&self) -> &DbErrorFields {
        &self.0
    }
}

impl std::ops::DerefMut for DbError {
    fn deref_mut(&mut self) -> &mut DbErrorFields {
        &mut self.0
    }
}

impl DbError {
    /// An error raised outside the server, with no Postgres fields.
    pub fn new(kind: DbErrorKind, message: impl Into<String>) -> Self {
        DbErrorFields::new(kind, message).into()
    }

    /// What every database call returns where there is no platform to run it.
    pub fn unavailable() -> Self {
        Self::new(
            DbErrorKind::Unavailable,
            "the database is only available inside the Cufflink platform",
        )
    }

    /// A serialization failure or deadlock: the transaction may succeed if
    /// run again.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.kind,
            DbErrorKind::SerializationFailure | DbErrorKind::DeadlockDetected
        )
    }

    pub fn is_constraint_violation(&self) -> bool {
        matches!(
            self.kind,
            DbErrorKind::UniqueViolation
                | DbErrorKind::ForeignKeyViolation
                | DbErrorKind::CheckViolation
                | DbErrorKind::NotNullViolation
                | DbErrorKind::ExclusionViolation
        )
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)?;
        match &self.code {
            Some(code) => write!(f, " (SQLSTATE {code})"),
            None => Ok(()),
        }
    }
}

impl std::error::Error for DbError {}

macro_rules! error_kinds {
    ($($variant:ident => $label:literal),* $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum DbErrorKind {
            $($variant,)*
            /// A kind this SDK does not know, from a newer platform.
            #[serde(other)]
            Other,
        }

        impl DbErrorKind {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(DbErrorKind::$variant => $label,)*
                    DbErrorKind::Other => "other",
                }
            }
        }
    };
}

error_kinds! {
    UniqueViolation => "unique_violation",
    ForeignKeyViolation => "foreign_key_violation",
    CheckViolation => "check_violation",
    NotNullViolation => "not_null_violation",
    ExclusionViolation => "exclusion_violation",
    SerializationFailure => "serialization_failure",
    DeadlockDetected => "deadlock_detected",
    QueryCanceled => "query_canceled",
    LockNotAvailable => "lock_not_available",
    InvalidTextRepresentation => "invalid_text_representation",
    NumericOutOfRange => "numeric_out_of_range",
    DatetimeFieldOverflow => "datetime_field_overflow",
    DataException => "data_exception",
    SyntaxError => "syntax_error",
    UndefinedTable => "undefined_table",
    UndefinedColumn => "undefined_column",
    InsufficientPrivilege => "insufficient_privilege",
    RaiseException => "raise_exception",
    InFailedTransaction => "in_failed_transaction",
    Connection => "connection",
    PoolTimeout => "pool_timeout",
    InvalidParameter => "invalid_parameter",
    UnsupportedType => "unsupported_type",
    TooManyRows => "too_many_rows",
    ColumnNotFound => "column_not_found",
    Decode => "decode",
    TransactionNotFound => "transaction_not_found",
    TransactionLimit => "transaction_limit",
    NotSupported => "not_supported",
    Protocol => "protocol",
    Unavailable => "unavailable",
    TooLarge => "too_large",
}

impl DbErrorKind {
    /// The kind of a Postgres SQLSTATE: the exact codes first, then the
    /// data-exception and connection classes.
    pub fn from_sqlstate(code: &str) -> Self {
        match code {
            "23505" => Self::UniqueViolation,
            "23503" => Self::ForeignKeyViolation,
            "23514" => Self::CheckViolation,
            "23502" => Self::NotNullViolation,
            "23P01" => Self::ExclusionViolation,
            "40001" => Self::SerializationFailure,
            "40P01" => Self::DeadlockDetected,
            "57014" => Self::QueryCanceled,
            "55P03" => Self::LockNotAvailable,
            "22P02" => Self::InvalidTextRepresentation,
            "22003" => Self::NumericOutOfRange,
            "22008" => Self::DatetimeFieldOverflow,
            "42601" => Self::SyntaxError,
            "42P01" => Self::UndefinedTable,
            "42703" => Self::UndefinedColumn,
            "42501" => Self::InsufficientPrivilege,
            "P0001" => Self::RaiseException,
            "25P02" => Self::InFailedTransaction,
            _ if code.starts_with("22") => Self::DataException,
            _ if code.starts_with("08") => Self::Connection,
            _ => Self::Other,
        }
    }
}

impl fmt::Display for DbErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// JSON has no NaN or infinities; they travel as the strings Postgres uses.
mod float {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    pub(super) enum Repr {
        Number(f64),
        Special(String),
    }

    impl Repr {
        pub(super) fn from_f64(value: f64) -> Self {
            match value {
                v if v.is_finite() => Repr::Number(v),
                v if v.is_nan() => Repr::Special("NaN".into()),
                v if v > 0.0 => Repr::Special("Infinity".into()),
                _ => Repr::Special("-Infinity".into()),
            }
        }

        pub(super) fn into_f64<E: serde::de::Error>(self) -> Result<f64, E> {
            match self {
                Repr::Number(v) => Ok(v),
                Repr::Special(s) => match s.as_str() {
                    "NaN" => Ok(f64::NAN),
                    "Infinity" => Ok(f64::INFINITY),
                    "-Infinity" => Ok(f64::NEG_INFINITY),
                    other => Err(E::custom(format!("{other:?} is not a number"))),
                },
            }
        }
    }

    pub(super) fn serialize<T: Copy + Into<f64>, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        Repr::from_f64((*value).into()).serialize(serializer)
    }

    pub(super) fn deserialize<'de, T: FromF64, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        Repr::deserialize(deserializer)?.into_f64().map(T::from_f64)
    }

    pub(super) trait FromF64 {
        fn from_f64(value: f64) -> Self;
    }

    impl FromF64 for f64 {
        fn from_f64(value: f64) -> Self {
            value
        }
    }

    impl FromF64 for f32 {
        fn from_f64(value: f64) -> Self {
            value as f32
        }
    }
}

mod float_array {
    use super::float::Repr;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        values: &[Option<f64>],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        values
            .iter()
            .map(|v| v.map(Repr::from_f64))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Option<f64>>, D::Error> {
        Vec::<Option<Repr>>::deserialize(deserializer)?
            .into_iter()
            .map(|v| v.map(Repr::into_f64).transpose())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip<T: Serialize + for<'de> Deserialize<'de> + PartialEq + fmt::Debug>(
        value: &T,
        wire: Value,
    ) {
        assert_eq!(serde_json::to_value(value).expect("serialize"), wire);
        assert_eq!(
            &serde_json::from_value::<T>(wire).expect("deserialize"),
            value
        );
    }

    #[test]
    fn params_carry_their_tag_and_value() {
        for (param, wire) in [
            (Param::Null, json!({"t": "null"})),
            (Param::Bool(true), json!({"t": "bool", "v": true})),
            (Param::I16(-3), json!({"t": "i16", "v": -3})),
            (Param::I64(i64::MAX), json!({"t": "i64", "v": i64::MAX})),
            (Param::F64(1.5), json!({"t": "f64", "v": 1.5})),
            (Param::F32(0.5), json!({"t": "f32", "v": 0.5})),
            (
                Param::F64(f64::INFINITY),
                json!({"t": "f64", "v": "Infinity"}),
            ),
            (
                Param::F64(f64::NEG_INFINITY),
                json!({"t": "f64", "v": "-Infinity"}),
            ),
            (
                Param::Text("it's".into()),
                json!({"t": "text", "v": "it's"}),
            ),
            (Param::Uuid("a".into()), json!({"t": "uuid", "v": "a"})),
            (
                Param::Timestamp("2026-01-01T00:00:00Z".into()),
                json!({"t": "timestamp", "v": "2026-01-01T00:00:00Z"}),
            ),
            (
                Param::NaiveTimestamp("2026-01-01 00:00:00".into()),
                json!({"t": "naive_timestamp", "v": "2026-01-01 00:00:00"}),
            ),
            (
                Param::Date("2026-01-01".into()),
                json!({"t": "date", "v": "2026-01-01"}),
            ),
            (
                Param::Time("12:00:00".into()),
                json!({"t": "time", "v": "12:00:00"}),
            ),
            (
                Param::Json(json!({"a": [1]})),
                json!({"t": "json", "v": {"a": [1]}}),
            ),
            (
                Param::Numeric("1.50".into()),
                json!({"t": "numeric", "v": "1.50"}),
            ),
            (
                Param::Bytes(vec![0, 255]),
                json!({"t": "bytes", "v": "AP8="}),
            ),
            (
                Param::TextArray(vec![Some("a".into()), None]),
                json!({"t": "text_array", "v": ["a", null]}),
            ),
            (
                Param::UuidArray(vec![None]),
                json!({"t": "uuid_array", "v": [null]}),
            ),
            (
                Param::I32Array(vec![Some(1)]),
                json!({"t": "i32_array", "v": [1]}),
            ),
            (Param::I64Array(vec![]), json!({"t": "i64_array", "v": []})),
            (
                Param::BoolArray(vec![Some(false), None]),
                json!({"t": "bool_array", "v": [false, null]}),
            ),
            (
                Param::F64Array(vec![Some(2.5), None, Some(f64::INFINITY)]),
                json!({"t": "f64_array", "v": [2.5, null, "Infinity"]}),
            ),
            (
                Param::NumericArray(vec![Some("0.1".into())]),
                json!({"t": "numeric_array", "v": ["0.1"]}),
            ),
        ] {
            assert_eq!(wire["t"], param.tag());
            round_trip(&param, wire);
        }
    }

    #[test]
    fn nan_travels_as_a_string() {
        let wire = serde_json::to_value(Param::F64(f64::NAN)).expect("serialize");
        assert_eq!(wire, json!({"t": "f64", "v": "NaN"}));
        match serde_json::from_value::<Param>(wire).expect("deserialize") {
            Param::F64(v) => assert!(v.is_nan()),
            other => panic!("{other:?}"),
        }
        assert!(serde_json::from_value::<Param>(json!({"t": "f64", "v": "many"})).is_err());
        assert!(serde_json::from_value::<Param>(json!({"t": "bytes", "v": "%%"})).is_err());
    }

    #[test]
    fn params_convert_from_rust_values() {
        assert_eq!(Param::from("x"), Param::Text("x".into()));
        assert_eq!(Param::from(7i64), Param::I64(7));
        assert_eq!(Param::from(None::<i32>), Param::Null);
        assert_eq!(Param::from(Some(true)), Param::Bool(true));
        assert_eq!(Param::from(&b"ab"[..]), Param::Bytes(b"ab".to_vec()));
        assert_eq!(
            Param::from(vec!["a", "b"]),
            Param::TextArray(vec![Some("a".into()), Some("b".into())])
        );
        assert_eq!(Param::from(vec![1i32]), Param::I32Array(vec![Some(1)]));
        assert_eq!(Param::from(json!(1)), Param::Json(json!(1)));
    }

    #[test]
    fn requests_are_tagged_by_op() {
        for (request, wire) in [
            (
                DbRequest::Query {
                    sql: "SELECT $1".into(),
                    params: vec![Param::I32(1)],
                    tx: None,
                },
                json!({"op": "query", "sql": "SELECT $1", "params": [{"t": "i32", "v": 1}], "tx": null}),
            ),
            (
                DbRequest::Execute {
                    sql: "DELETE FROM t".into(),
                    params: vec![],
                    tx: Some(1),
                },
                json!({"op": "execute", "sql": "DELETE FROM t", "params": [], "tx": 1}),
            ),
            (
                DbRequest::Begin {
                    isolation: Isolation::Serializable,
                    read_only: true,
                },
                json!({"op": "begin", "isolation": "serializable", "read_only": true}),
            ),
            (
                DbRequest::Commit { tx: 1 },
                json!({"op": "commit", "tx": 1}),
            ),
            (
                DbRequest::Rollback { tx: 2 },
                json!({"op": "rollback", "tx": 2}),
            ),
            (
                DbRequest::Savepoint {
                    tx: 1,
                    name: "sp_0".into(),
                },
                json!({"op": "savepoint", "tx": 1, "name": "sp_0"}),
            ),
            (
                DbRequest::Release {
                    tx: 1,
                    name: "sp_0".into(),
                },
                json!({"op": "release", "tx": 1, "name": "sp_0"}),
            ),
            (
                DbRequest::RollbackTo {
                    tx: 1,
                    name: "sp_0".into(),
                },
                json!({"op": "rollback_to", "tx": 1, "name": "sp_0"}),
            ),
        ] {
            assert_eq!(wire["op"], request.op());
            round_trip(&request, wire);
        }
        assert_eq!(
            serde_json::from_value::<DbRequest>(json!({"op": "query", "sql": "SELECT 1"}))
                .expect("defaults"),
            DbRequest::Query {
                sql: "SELECT 1".into(),
                params: vec![],
                tx: None
            }
        );
        assert_eq!(
            serde_json::from_value::<DbRequest>(json!({"op": "begin"})).expect("defaults"),
            DbRequest::Begin {
                isolation: Isolation::ReadCommitted,
                read_only: false
            }
        );
        assert!(serde_json::from_value::<DbRequest>(json!({"op": "vacuum"})).is_err());
    }

    #[test]
    fn responses_match_the_wire_shapes() {
        let unique = DbError::from(DbErrorFields {
            code: Some("23505".into()),
            constraint: Some("books_email_key".into()),
            table: Some("books".into()),
            schema: Some("catalog_service".into()),
            detail: Some("Key (email)=(a) already exists.".into()),
            ..DbErrorFields::new(DbErrorKind::UniqueViolation, "duplicate key")
        });
        for (response, wire) in [
            (
                DbResponse::Rows {
                    rows: vec![json!({"id": 1}).as_object().cloned().expect("object")],
                    columns: vec![Column {
                        name: "id".into(),
                        type_name: "int4".into(),
                    }],
                },
                json!({"ok": true, "rows": [{"id": 1}], "columns": [{"name": "id", "type": "int4"}]}),
            ),
            (DbResponse::Affected(3), json!({"ok": true, "affected": 3})),
            (DbResponse::Begun(1), json!({"ok": true, "tx": 1})),
            (DbResponse::Done, json!({"ok": true})),
            (
                DbResponse::Failed(unique),
                json!({"ok": false, "error": {
                    "code": "23505", "kind": "unique_violation", "message": "duplicate key",
                    "detail": "Key (email)=(a) already exists.", "hint": null,
                    "constraint": "books_email_key", "table": "books", "column": null,
                    "schema": "catalog_service", "position": null
                }}),
            ),
        ] {
            round_trip(&response, wire);
        }
        assert_eq!(
            serde_json::from_value::<DbResponse>(json!({"ok": true, "rows": []}))
                .expect("columns default"),
            DbResponse::Rows {
                rows: vec![],
                columns: vec![]
            }
        );
        assert!(serde_json::from_value::<DbResponse>(json!({"ok": false})).is_err());
    }

    #[test]
    fn unknown_error_kinds_decode_as_other() {
        let error: DbError = serde_json::from_value(json!({
            "code": null, "kind": "quantum_decoherence", "message": "m", "detail": null,
            "hint": null, "constraint": null, "table": null, "column": null,
            "schema": null, "position": null
        }))
        .expect("decode");
        assert_eq!(error.kind, DbErrorKind::Other);
    }

    #[test]
    fn kind_labels_match_their_serde_names() {
        for kind in [
            DbErrorKind::UniqueViolation,
            DbErrorKind::RaiseException,
            DbErrorKind::InFailedTransaction,
            DbErrorKind::PoolTimeout,
            DbErrorKind::TransactionLimit,
            DbErrorKind::Unavailable,
            DbErrorKind::TooLarge,
            DbErrorKind::Other,
        ] {
            assert_eq!(
                serde_json::to_value(kind).expect("kind"),
                json!(kind.as_str())
            );
        }
    }

    #[test]
    fn sqlstates_map_to_kinds() {
        for (code, kind) in [
            ("23505", DbErrorKind::UniqueViolation),
            ("23503", DbErrorKind::ForeignKeyViolation),
            ("23514", DbErrorKind::CheckViolation),
            ("23502", DbErrorKind::NotNullViolation),
            ("23P01", DbErrorKind::ExclusionViolation),
            ("40001", DbErrorKind::SerializationFailure),
            ("40P01", DbErrorKind::DeadlockDetected),
            ("57014", DbErrorKind::QueryCanceled),
            ("55P03", DbErrorKind::LockNotAvailable),
            ("22P02", DbErrorKind::InvalidTextRepresentation),
            ("22003", DbErrorKind::NumericOutOfRange),
            ("22008", DbErrorKind::DatetimeFieldOverflow),
            ("22012", DbErrorKind::DataException),
            ("22001", DbErrorKind::DataException),
            ("42601", DbErrorKind::SyntaxError),
            ("42P01", DbErrorKind::UndefinedTable),
            ("42703", DbErrorKind::UndefinedColumn),
            ("42501", DbErrorKind::InsufficientPrivilege),
            ("P0001", DbErrorKind::RaiseException),
            ("25P02", DbErrorKind::InFailedTransaction),
            ("08006", DbErrorKind::Connection),
            ("08P01", DbErrorKind::Connection),
            ("23000", DbErrorKind::Other),
            ("XX000", DbErrorKind::Other),
            ("", DbErrorKind::Other),
        ] {
            assert_eq!(DbErrorKind::from_sqlstate(code), kind, "{code}");
        }
    }

    #[test]
    fn errors_classify_and_display() {
        let error = |kind| DbError::new(kind, "m");
        assert!(error(DbErrorKind::SerializationFailure).is_retryable());
        assert!(error(DbErrorKind::DeadlockDetected).is_retryable());
        assert!(!error(DbErrorKind::UniqueViolation).is_retryable());
        for kind in [
            DbErrorKind::UniqueViolation,
            DbErrorKind::ForeignKeyViolation,
            DbErrorKind::CheckViolation,
            DbErrorKind::NotNullViolation,
            DbErrorKind::ExclusionViolation,
        ] {
            assert!(error(kind).is_constraint_violation(), "{kind}");
        }
        assert!(!error(DbErrorKind::RaiseException).is_constraint_violation());
        assert_eq!(
            DbError::from(DbErrorFields {
                code: Some("57014".into()),
                ..DbErrorFields::new(DbErrorKind::QueryCanceled, "canceling statement")
            })
            .to_string(),
            "query_canceled: canceling statement (SQLSTATE 57014)"
        );
        assert_eq!(DbError::unavailable().kind, DbErrorKind::Unavailable);
    }
}
