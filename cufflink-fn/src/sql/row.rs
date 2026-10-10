//! A result row: column values by name, decoded on demand.

use cufflink_types::db::{Column, DbError, DbErrorKind, RowMap};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::rc::Rc;

/// One row of a `query` result. Values are the platform's exact JSON
/// renderings: `numeric` and timestamps are strings, `bytea` is base64.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    values: RowMap,
    columns: Rc<[Column]>,
}

pub(crate) fn rows(rows: Vec<RowMap>, columns: Vec<Column>) -> Vec<Row> {
    let columns: Rc<[Column]> = columns.into();
    rows.into_iter()
        .map(|values| Row {
            values,
            columns: Rc::clone(&columns),
        })
        .collect()
}

impl Row {
    /// The column's value as `T`; a missing column is `ColumnNotFound` and a
    /// value that is not a `T` (including `NULL` for a non-`Option`) is
    /// `Decode`, naming the column and its SQL type.
    pub fn get<T: DeserializeOwned>(&self, column: &str) -> Result<T, DbError> {
        let value = self.values.get(column).ok_or_else(|| {
            DbError::new(
                DbErrorKind::ColumnNotFound,
                format!("no column {column:?} in the row"),
            )
        })?;
        T::deserialize(value).map_err(|e| {
            DbError::new(
                DbErrorKind::Decode,
                format!("column {column}{}: {e}", self.sql_type(column)),
            )
        })
    }

    /// Like [`get`](Self::get), with SQL `NULL` as `None`.
    pub fn try_get<T: DeserializeOwned>(&self, column: &str) -> Result<Option<T>, DbError> {
        self.get::<Option<T>>(column)
    }

    /// The whole row as a struct whose fields are the column names.
    pub fn decode<T: DeserializeOwned>(self) -> Result<T, DbError> {
        T::deserialize(Value::Object(self.values))
            .map_err(|e| DbError::new(DbErrorKind::Decode, format!("row: {e}")))
    }

    pub fn as_map(&self) -> &RowMap {
        &self.values
    }

    pub fn into_value(self) -> Value {
        Value::Object(self.values)
    }

    /// The result's columns with their SQL type names.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    fn sql_type(&self, column: &str) -> String {
        self.columns
            .iter()
            .find(|c| c.name == column)
            .map(|c| format!(" ({})", c.type_name))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    fn row() -> Row {
        let values = json!({"id": 7, "name": "a", "note": null, "amount": "1.50"});
        rows(
            vec![values.as_object().cloned().expect("object")],
            [
                ("id", "int4"),
                ("name", "text"),
                ("note", "text"),
                ("amount", "numeric"),
            ]
            .iter()
            .map(|(name, type_name)| Column {
                name: name.to_string(),
                type_name: type_name.to_string(),
            })
            .collect(),
        )
        .remove(0)
    }

    #[test]
    fn get_reads_typed_values_and_names_what_went_wrong() {
        let row = row();
        assert_eq!(row.get::<i64>("id").ok(), Some(7));
        assert_eq!(row.get::<String>("amount").ok(), Some("1.50".into()));
        let missing = row.get::<i64>("nope").expect_err("missing");
        assert_eq!(missing.kind, DbErrorKind::ColumnNotFound);
        let wrong = row.get::<i64>("name").expect_err("type");
        assert_eq!(wrong.kind, DbErrorKind::Decode);
        assert!(
            wrong.message.starts_with("column name (text):"),
            "{}",
            wrong.message
        );
        let null = row.get::<String>("note").expect_err("null");
        assert_eq!(null.kind, DbErrorKind::Decode);
    }

    #[test]
    fn try_get_maps_null_to_none() {
        let row = row();
        assert_eq!(row.try_get::<String>("note").ok(), Some(None));
        assert_eq!(row.try_get::<String>("name").ok(), Some(Some("a".into())));
        assert_eq!(
            row.try_get::<String>("nope").err().map(|e| e.kind),
            Some(DbErrorKind::ColumnNotFound)
        );
    }

    #[test]
    fn decode_fills_a_struct_and_rows_share_their_columns() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Item {
            id: i32,
            name: String,
            note: Option<String>,
        }
        let row = row();
        assert_eq!(row.columns().len(), 4);
        assert_eq!(row.as_map()["name"], "a");
        assert_eq!(
            row.clone().decode::<Item>().ok(),
            Some(Item {
                id: 7,
                name: "a".into(),
                note: None
            })
        );
        #[derive(Deserialize, Debug)]
        struct Wrong {
            _absent: i32,
        }
        assert_eq!(
            row.clone().decode::<Wrong>().err().map(|e| e.kind),
            Some(DbErrorKind::Decode)
        );
        assert_eq!(row.into_value()["id"], 7);
    }
}
