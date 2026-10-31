//! A statement's rows as a guest receives them.

use crate::row_json::{column_to_json, column_to_json_v2};
use cufflink_types::db::{Column, DbError, DbResponse, RowMap};
use serde_json::Value;
use tokio_postgres::Row;

/// The rows of a legacy query: an object per row, each column under its
/// name in the legacy encoding.
pub fn legacy_rows(rows: &[Row]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            Value::Object(
                row.columns()
                    .iter()
                    .enumerate()
                    .map(|(i, column)| {
                        (
                            column.name().to_string(),
                            column_to_json(row, i, column.type_()),
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

/// The rows of a `db_call` query with the columns its statement reports;
/// a value the exact encoding cannot carry is the error.
pub fn rows_v2(columns: &[tokio_postgres::Column], rows: &[Row]) -> Result<DbResponse, DbError> {
    let rows = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(i, column)| {
                    column_to_json_v2(row, i, column.type_())
                        .map(|value| (column.name().to_string(), value))
                })
                .collect::<Result<RowMap, DbError>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DbResponse::Rows {
        rows,
        columns: columns_v2(columns),
    })
}

/// The order a statement's columns render in when each row becomes an
/// object: by name, as a JSON map orders them, and of two columns with the
/// same name the last wins.
pub struct ColumnOrder(Vec<(String, usize)>);

impl ColumnOrder {
    pub fn of(columns: &[tokio_postgres::Column]) -> Self {
        let last: std::collections::BTreeMap<&str, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, column)| (column.name(), i))
            .collect();
        Self(
            last.into_iter()
                .map(|(name, i)| (Value::from(name).to_string(), i))
                .collect(),
        )
    }

    /// One row of a legacy query, written as `serde_json::to_string` writes
    /// the row's object in `legacy_rows`.
    pub fn legacy_row(&self, row: &Row, out: &mut String) {
        self.write(out, |i| column_to_json(row, i, row.columns()[i].type_()));
    }

    /// One row of a `db_call` query, written as the row's map is written in
    /// the `rows` of a `DbResponse::Rows`.
    pub fn row_v2(&self, row: &Row, out: &mut String) -> Result<(), DbError> {
        let mut values = row
            .columns()
            .iter()
            .enumerate()
            .map(|(i, column)| column_to_json_v2(row, i, column.type_()).map(Some))
            .collect::<Result<Vec<Option<Value>>, DbError>>()?;
        self.write(out, |i| values[i].take().unwrap_or(Value::Null));
        Ok(())
    }

    fn write(&self, out: &mut String, mut value: impl FnMut(usize) -> Value) {
        out.push('{');
        for (n, (key, i)) in self.0.iter().enumerate() {
            if n > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push(':');
            out.push_str(&value(*i).to_string());
        }
        out.push('}');
    }
}

/// The columns of a `db_call` query as its `DbResponse::Rows` writes them.
pub fn columns_v2(columns: &[tokio_postgres::Column]) -> Vec<Column> {
    columns
        .iter()
        .map(|column| Column {
            name: column.name().to_string(),
            type_name: column.type_().name().to_string(),
        })
        .collect()
}
