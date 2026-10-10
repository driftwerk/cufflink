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
        columns: columns
            .iter()
            .map(|column| Column {
                name: column.name().to_string(),
                type_name: column.type_().name().to_string(),
            })
            .collect(),
    })
}
