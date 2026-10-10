//! Rust values as bound parameters.

use cufflink_types::db::Param;
use serde_json::Value;

/// A value that binds as a [`Param`]; what [`params!`](crate::params)
/// accepts. `None` binds as `NULL`.
pub trait IntoParam {
    fn into_param(self) -> Param;
}

impl IntoParam for Param {
    fn into_param(self) -> Param {
        self
    }
}

macro_rules! via_from {
    ($($ty:ty),* $(,)?) => {
        $(impl IntoParam for $ty {
            fn into_param(self) -> Param {
                Param::from(self)
            }
        })*
    };
}

via_from!(
    bool,
    i16,
    i32,
    i64,
    f32,
    f64,
    String,
    &str,
    Value,
    Vec<u8>,
    &[u8],
    Vec<String>,
    Vec<&str>,
    Vec<i32>,
    Vec<i64>,
);

impl IntoParam for &String {
    fn into_param(self) -> Param {
        Param::Text(self.clone())
    }
}

impl<T: IntoParam> IntoParam for Option<T> {
    fn into_param(self) -> Param {
        self.map_or(Param::Null, IntoParam::into_param)
    }
}

impl IntoParam for chrono::DateTime<chrono::Utc> {
    fn into_param(self) -> Param {
        Param::Timestamp(self.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
    }
}

impl IntoParam for chrono::NaiveDateTime {
    fn into_param(self) -> Param {
        Param::NaiveTimestamp(self.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
    }
}

impl IntoParam for chrono::NaiveDate {
    fn into_param(self) -> Param {
        Param::Date(self.format("%Y-%m-%d").to_string())
    }
}

impl IntoParam for chrono::NaiveTime {
    fn into_param(self) -> Param {
        Param::Time(self.format("%H:%M:%S%.f").to_string())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl IntoParam for uuid::Uuid {
    fn into_param(self) -> Param {
        Param::Uuid(self.to_string())
    }
}

/// Bound parameters for [`sql::query`](crate::sql::query) and friends,
/// from any mix of [`IntoParam`] values.
///
/// ```rust,ignore
/// sql::query("SELECT * FROM orders WHERE customer = $1 AND total > $2", &params![id, 100])?;
/// ```
#[macro_export]
macro_rules! params {
    ($($p:expr),* $(,)?) => {
        [$($crate::sql::IntoParam::into_param($p)),*]
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn rust_values_bind_as_their_tags() {
        let date = NaiveDate::from_ymd_opt(2026, 3, 4).expect("date");
        let at = date.and_hms_micro_opt(5, 6, 7, 120).expect("time");
        let id = uuid::Uuid::nil();
        let owned = String::from("owned");
        let bound = crate::params![
            1i32,
            "a",
            &owned,
            None::<i64>,
            Some(true),
            json!({"k": 1}),
            Utc.from_utc_datetime(&at),
            at,
            date,
            at.time(),
            id,
            vec![1i64, 2],
            &b"\x00"[..],
            Param::Numeric("1.5".into()),
        ];
        assert_eq!(
            bound.to_vec(),
            vec![
                Param::I32(1),
                Param::Text("a".into()),
                Param::Text("owned".into()),
                Param::Null,
                Param::Bool(true),
                Param::Json(json!({"k": 1})),
                Param::Timestamp("2026-03-04T05:06:07.000120Z".into()),
                Param::NaiveTimestamp("2026-03-04T05:06:07.000120".into()),
                Param::Date("2026-03-04".into()),
                Param::Time("05:06:07.000120".into()),
                Param::Uuid("00000000-0000-0000-0000-000000000000".into()),
                Param::I64Array(vec![Some(1), Some(2)]),
                Param::Bytes(vec![0]),
                Param::Numeric("1.5".into()),
            ]
        );
        let none: [Param; 0] = crate::params![];
        assert!(none.is_empty());
    }
}
