//! The service's database.

use cufflink_types::db::{DbRequest, DbResponse};
use serde_json::Value;

/// Why a batch rolled back: the statement that failed, counted from zero,
/// or none when the transaction itself did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchError {
    pub index: Option<usize>,
    pub message: String,
}

/// What a read-only query answered; `truncated` when the row cap cut it
/// short.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReadOnlyRows {
    pub rows: Vec<Value>,
    pub truncated: bool,
}

/// What answers `db::*` and `sql::*`. The legacy functions read a failure
/// as no rows, `-1` or the message, as they do on the platform.
pub trait DbHost {
    /// Rows in the legacy encoding.
    fn query(&self, sql: &str) -> Result<Vec<Value>, String>;
    /// The rows affected.
    fn execute(&self, sql: &str) -> Result<u64, String>;
    /// The rows each statement affected, in one transaction.
    fn execute_all(&self, statements: &[&str]) -> Result<Vec<i64>, BatchError>;
    fn query_read_only(&self, sql: &str) -> Result<ReadOnlyRows, String>;
    #[cfg_attr(not(feature = "db-v2"), allow(dead_code))]
    fn call(&self, request: &DbRequest) -> DbResponse;
}
