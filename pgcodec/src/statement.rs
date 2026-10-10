//! What a guest's statement is, judged by its leading keywords: whatever
//! runs a guest's SQL decides the same way how it may run.

/// Statements Postgres refuses inside a transaction block, judged by their
/// leading keywords after any comments: `CREATE`/`DROP INDEX CONCURRENTLY`,
/// `REINDEX … CONCURRENTLY`, `REINDEX DATABASE`/`SYSTEM`, `VACUUM`,
/// `CREATE`/`DROP DATABASE`, `CREATE`/`DROP TABLESPACE`, `ALTER SYSTEM`,
/// `CLUSTER`, `DISCARD ALL`.
pub fn requires_autocommit(sql: &str) -> bool {
    let words = leading_keywords(sql, 8);
    let word = |i: usize| words.get(i).map_or("", String::as_str);
    let position = |keyword: &str| words.iter().position(|w| w == keyword);
    match word(0) {
        "VACUUM" | "CLUSTER" => true,
        "DISCARD" => word(1) == "ALL",
        "ALTER" => word(1) == "SYSTEM",
        "CREATE" | "DROP" => {
            matches!(word(1), "DATABASE" | "TABLESPACE")
                || position("INDEX").is_some_and(|i| i <= 2 && word(i + 1) == "CONCURRENTLY")
        }
        "REINDEX" => words
            .iter()
            .position(|w| {
                matches!(
                    w.as_str(),
                    "INDEX" | "TABLE" | "SCHEMA" | "DATABASE" | "SYSTEM"
                )
            })
            .is_some_and(|kind| {
                matches!(word(kind), "DATABASE" | "SYSTEM")
                    || words[..=(kind + 1).min(words.len() - 1)]
                        .iter()
                        .any(|w| w == "CONCURRENTLY")
            }),
        _ => false,
    }
}

/// Statements that change session state rather than the transaction's:
/// `SET` other than `SET LOCAL`, `SET TRANSACTION` and `SET CONSTRAINTS`,
/// and `RESET`.
pub fn changes_session(sql: &str) -> bool {
    let words = leading_keywords(sql, 2);
    match words.first().map(String::as_str) {
        Some("RESET") => true,
        Some("SET") => !matches!(
            words.get(1).map(String::as_str),
            Some("LOCAL" | "TRANSACTION" | "CONSTRAINTS")
        ),
        _ => false,
    }
}

/// Transaction control, which inside a host-managed transaction would end
/// or split it behind the host's back.
pub fn controls_transaction(sql: &str) -> bool {
    let words = leading_keywords(sql, 2);
    match words.first().map(String::as_str) {
        Some(
            "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "ABORT" | "SAVEPOINT" | "RELEASE",
        ) => true,
        Some("PREPARE") => words.get(1).is_some_and(|w| w == "TRANSACTION"),
        _ => false,
    }
}

/// A caller's `SELECT` wrapped to answer each row as JSON, `limit` rows at
/// most. The SQL is wrapped, never concatenated onto another statement, and
/// the newline after it keeps a query that ends in a `--` comment from
/// swallowing the limit.
pub fn rows_as_json(sql: &str, limit: i64) -> String {
    format!("SELECT to_jsonb(t) AS row FROM (\n{sql}\n) t LIMIT {limit}")
}

/// The first `n` words of a statement, upper-cased, with leading `--` and
/// `/* */` comments skipped and punctuation split off.
fn leading_keywords(sql: &str, n: usize) -> Vec<String> {
    let mut rest = sql;
    loop {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map_or("", |(_, r)| r);
        } else if let Some(after) = rest.strip_prefix("/*") {
            rest = after.split_once("*/").map_or("", |(_, r)| r);
        } else {
            break;
        }
    }
    rest.split(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | ',' | ';'))
        .filter(|t| !t.is_empty())
        .take(n)
        .map(|t| t.to_ascii_uppercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trailing_comment_cannot_swallow_the_limit() {
        assert_eq!(
            rows_as_json("SELECT 1 -- all of them", 11),
            "SELECT to_jsonb(t) AS row FROM (\nSELECT 1 -- all of them\n) t LIMIT 11"
        );
    }

    #[test]
    fn session_settings_are_told_from_transaction_ones() {
        for sql in [
            "SET search_path TO other",
            "set app.x = '1'",
            "SET SESSION statement_timeout = 0",
            "SET ROLE admin",
            "SET SESSION AUTHORIZATION admin",
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
            "/* x */ SET TIME ZONE 'UTC'",
            "RESET ALL",
            "reset role",
        ] {
            assert!(changes_session(sql), "{sql}");
        }
        for sql in [
            "SET LOCAL app.x = '1'",
            "set local role reporter",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            "SET CONSTRAINTS ALL DEFERRED",
            "SELECT set_config('app.x', '1', true)",
            "UPDATE t SET a = 1",
            "",
        ] {
            assert!(!changes_session(sql), "{sql}");
        }
    }

    #[test]
    fn transaction_control_is_recognised_by_leading_keywords() {
        for sql in [
            "BEGIN",
            "begin isolation level serializable",
            "START TRANSACTION",
            "COMMIT",
            "commit and chain",
            "END",
            "ROLLBACK",
            "ROLLBACK TO SAVEPOINT sp_1",
            "ABORT",
            "SAVEPOINT x",
            "RELEASE SAVEPOINT x",
            "PREPARE TRANSACTION 'x'",
            "COMMIT PREPARED 'x'",
            "-- done\nCOMMIT;",
        ] {
            assert!(controls_transaction(sql), "{sql}");
        }
        for sql in [
            "SELECT 1",
            "PREPARE q AS SELECT 1",
            "UPDATE t SET committed = true",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "",
        ] {
            assert!(!controls_transaction(sql), "{sql}");
        }
    }

    #[test]
    fn autocommit_statements_are_recognised_by_leading_keywords() {
        for sql in [
            "CREATE INDEX CONCURRENTLY idx ON t (c)",
            "create unique index concurrently if not exists idx on t (c)",
            "DROP INDEX CONCURRENTLY IF EXISTS idx",
            "REINDEX INDEX CONCURRENTLY idx",
            "REINDEX (VERBOSE) TABLE CONCURRENTLY t",
            "REINDEX (CONCURRENTLY) TABLE t",
            "REINDEX DATABASE mydb",
            "reindex system",
            "CREATE TABLESPACE fast LOCATION '/mnt/fast'",
            "DROP TABLESPACE fast",
            "  -- rebuild\n  VACUUM ANALYZE t",
            "/* nightly */ VACUUM",
            "CREATE DATABASE other",
            "DROP DATABASE other",
            "ALTER SYSTEM SET work_mem = '64MB'",
            "CLUSTER t USING idx",
            "DISCARD ALL",
        ] {
            assert!(requires_autocommit(sql), "{sql}");
        }
        for sql in [
            "SELECT 1",
            "INSERT INTO t VALUES (1)",
            "CREATE INDEX idx ON t (c)",
            "CREATE INDEX i ON t (concurrently)",
            "REINDEX TABLE t",
            "CREATE TABLE concurrently_log (id int)",
            "ALTER TABLE t ADD COLUMN c int",
            "-- CREATE INDEX CONCURRENTLY in a comment only\nSELECT 1",
            "DISCARD TEMP",
            "",
        ] {
            assert!(!requires_autocommit(sql), "{sql}");
        }
    }
}
