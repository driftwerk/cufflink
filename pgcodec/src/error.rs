//! A Postgres failure as a guest receives it: the structured [`DbError`] of
//! `db_call`, and the text of the legacy calls.

use cufflink_types::db::{DbError, DbErrorFields, DbErrorKind};
use std::error::Error;
use tokio_postgres::error::ErrorPosition;

/// The server's error fields when Postgres reported one; otherwise the
/// failure is the connection's (closed socket, I/O, protocol).
pub fn from_pg(error: &tokio_postgres::Error) -> DbError {
    match error.as_db_error() {
        Some(db) => DbErrorFields {
            code: Some(db.code().code().to_string()),
            kind: DbErrorKind::from_sqlstate(db.code().code()),
            message: db.message().to_string(),
            detail: db.detail().map(str::to_string),
            hint: db.hint().map(str::to_string),
            constraint: db.constraint().map(str::to_string),
            table: db.table().map(str::to_string),
            column: db.column().map(str::to_string),
            schema: db.schema().map(str::to_string),
            position: db.position().and_then(|position| match position {
                ErrorPosition::Original(at) => Some(*at),
                ErrorPosition::Internal { .. } => None,
            }),
        }
        .into(),
        None => DbError::new(DbErrorKind::Connection, flatten(error)),
    }
}

fn flatten(error: &tokio_postgres::Error) -> String {
    match error.source() {
        Some(source) => format!("{error}: {source}"),
        None => error.to_string(),
    }
}

/// `tokio_postgres::Error` renders as the bare string "db error": what is
/// worth reading (the constraint name, the syntax error, the detail line)
/// hangs off its source, which is what the legacy calls hand a guest.
pub fn server_message(error: &tokio_postgres::Error) -> String {
    match error.source() {
        Some(source) => source.to_string(),
        None => error.to_string(),
    }
}
