//! PostgreSQL's parser.

use cufflink_types::sql_parse::ParseResponse;

/// What answers `sql::parse`.
pub trait SqlParseHost {
    #[cfg_attr(not(feature = "sql-parse"), allow(dead_code))]
    fn parse(&self, sql: &str) -> ParseResponse;
}
