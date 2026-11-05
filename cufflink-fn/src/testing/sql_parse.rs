//! PostgreSQL's parser, in the test's own process.

use crate::host::sql_parse::SqlParseHost;
use cufflink_types::sql_parse::ParseResponse;

/// The response cap a platform enforces unless configured otherwise.
const RESPONSE_CAP: usize = 64 * 1024 * 1024;

/// Parses as the platform's parser processes do: the same bounds, the same
/// `pg_query` release and the same tree, under the platform's default
/// response cap. It parses in the test's process, one statement at a time,
/// and while it does `pg_query` holds the process's stderr.
#[derive(Debug, Clone, Copy, Default)]
pub struct PgQueryParser;

impl SqlParseHost for PgQueryParser {
    fn parse(&self, sql: &str) -> ParseResponse {
        let (before, after) = cufflink_sql_parse::answer_around();
        cufflink_sql_parse::parsed(sql, RESPONSE_CAP - before.len() - after.len()).into()
    }
}
