//! Database access — run SQL queries against your service's tables.
//!
//! All queries run in the tenant's schema automatically. You don't need
//! to qualify table names with a schema prefix.

#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};
use serde_json::Value;

/// Run a SELECT query and return all rows as a `Vec<Value>`.
///
/// Each row is a JSON object with column names as keys.
///
/// ```rust,ignore
/// let users = db::query("SELECT id, name, email FROM users WHERE active = true");
/// for user in &users {
///     log::info(&format!("User: {}", user["name"]));
/// }
/// ```
pub fn query(sql: &str) -> Vec<Value> {
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = sql.as_bytes();
        let result = unsafe { ffi::db_query(bytes.as_ptr() as i32, bytes.len() as i32) };
        if result < 0 {
            return vec![];
        }
        host_buffer::read_host_json_lossy::<Vec<Value>>().unwrap_or_default()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::db()
            .and_then(|host| host.query(sql).ok())
            .unwrap_or_default()
    }
}

/// Run a SELECT query and return the first row, or `None` if empty.
///
/// ```rust,ignore
/// if let Some(user) = db::query_one("SELECT * FROM users WHERE id = 'abc'") {
///     log::info(&format!("Found user: {}", user["name"]));
/// }
/// ```
pub fn query_one(sql: &str) -> Option<Value> {
    query(sql).into_iter().next()
}

/// Run an INSERT, UPDATE, or DELETE statement.
///
/// Returns the number of affected rows, or -1 on error.
///
/// ```rust,ignore
/// let affected = db::execute("UPDATE orders SET status = 'shipped' WHERE id = 'abc'");
/// log::info(&format!("Updated {} rows", affected));
/// ```
pub fn execute(sql: &str) -> i32 {
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = sql.as_bytes();
        unsafe { ffi::db_execute(bytes.as_ptr() as i32, bytes.len() as i32) }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::db().map_or(0, |host| {
            host.execute(sql)
                .map_or(-1, |affected| i32::try_from(affected).unwrap_or(i32::MAX))
        })
    }
}

/// Run several statements in order inside **one transaction on one
/// connection** — all commit, or none do.
///
/// Every other `db` call takes a fresh connection out of the tenant pool,
/// runs its statement as its own implicit transaction, and hands the
/// connection back. So `SELECT … FOR UPDATE` buys nothing across two
/// [`query`] calls and an advisory lock cannot be held between them. This is
/// the primitive that gives you a transaction; reach for it when a sequence
/// of writes must be all-or-nothing, or when a lock has to span them.
///
/// On success returns the affected-row count per statement, positionally.
/// A count of `0` is a normal result, not a failure — failure is the `Err`.
/// On failure **nothing is committed** and the message names the 1-based
/// statement that failed.
///
/// ```rust,ignore
/// // Serialize display_order allocation: the advisory lock is taken and
/// // released by the same transaction that does the insert.
/// db::execute_all(&[
///     &format!("SELECT pg_advisory_xact_lock(hashtext('{}')::bigint)", item_id),
///     &insert_sql,
/// ])?;
/// ```
///
/// Constraints:
///
/// * One statement per slot — the host runs each through the extended query
///   protocol, which rejects several commands in one string.
/// * Rows are not returned. Use `RETURNING` for its side effects only, and
///   read the row back with [`query_one`] after the batch commits.
/// * [`nats::publish`](crate::nats::publish) is a separate host call and
///   cannot join the transaction — this is not a transactional outbox.
/// * The batch holds one pooled connection for its whole duration; keep it
///   short.
pub fn execute_all(statements: &[&str]) -> Result<Vec<i32>, String> {
    #[cfg(target_arch = "wasm32")]
    {
        if statements.is_empty() {
            return Ok(vec![]);
        }
        let payload = serde_json::to_string(statements)
            .map_err(|e| format!("could not encode statements: {}", e))?;
        let bytes = payload.as_bytes();
        let rc = unsafe { ffi::db_execute_all(bytes.as_ptr() as i32, bytes.len() as i32) };
        if rc < 0 {
            return Err("db_execute_all host call failed".to_string());
        }
        let envelope: Value = match host_buffer::read_host_json::<Value>() {
            Some(v) => v,
            None => return Err("malformed host response".to_string()),
        };
        if envelope["ok"].as_bool().unwrap_or(false) {
            return Ok(envelope["counts"]
                .as_array()
                .map(|counts| {
                    counts
                        .iter()
                        .map(|c| c.as_i64().unwrap_or(0) as i32)
                        .collect()
                })
                .unwrap_or_default());
        }
        Err(batch_failure(
            envelope["index"]
                .as_u64()
                .and_then(|index| usize::try_from(index).ok()),
            statements.len(),
            envelope["error"].as_str().unwrap_or("unknown error"),
        ))
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        match crate::host::db() {
            Some(host) if !statements.is_empty() => host
                .execute_all(statements)
                .map(|counts| {
                    counts
                        .into_iter()
                        .map(|count| i32::try_from(count).unwrap_or(i32::MAX))
                        .collect()
                })
                .map_err(|e| batch_failure(e.index, statements.len(), &e.message)),
            _ => Ok(statements.iter().map(|_| 0).collect()),
        }
    }
}

/// How a rolled-back batch of `total` statements is reported: by the
/// statement that failed, counted from one, or as the transaction's own
/// failure.
fn batch_failure(index: Option<usize>, total: usize, error: &str) -> String {
    match index {
        Some(index) => format!(
            "statement {} of {total} failed, batch rolled back: {error}",
            index + 1
        ),
        None => format!("transaction failed, batch rolled back: {error}"),
    }
}

/// Run ONE caller-supplied `SELECT` and get its rows back as JSON.
///
/// For SQL your service did **not** write — the case this exists for is an
/// assistant turning a user's question into a query. The host, not the
/// guest, fixes every safety property:
///
/// * a `READ ONLY` transaction (writes fail even if one gets through)
/// * `SET LOCAL ROLE` to a least-privilege role, configured per install
/// * a `statement_timeout`, so a runaway query can't pin a connection
/// * a row cap, with `truncated` telling you the answer was cut short
///
/// None of those are arguments, deliberately: a caller that could choose
/// them could disable them. The transaction is always rolled back.
///
/// The search_path is NOT set to your service schema, so name schemas
/// explicitly (`SELECT … FROM reporting.item_overview`). Your own tables are
/// not reachable by bare name through this door — use [`query`] for those.
///
/// You should still validate the SQL before calling (single statement,
/// starts with SELECT/WITH) — not for safety, which is handled above, but so
/// the caller gets a clear message instead of a Postgres error.
///
/// ```rust,ignore
/// let out = db::query_read_only("SELECT count(*) FROM reporting.item_overview")?;
/// let rows = out["rows"].as_array().cloned().unwrap_or_default();
/// if out["truncated"].as_bool().unwrap_or(false) { /* say so */ }
/// ```
///
/// Returns `Err` with the database's message when the query is invalid —
/// that is a normal outcome here, not a host failure.
pub fn query_read_only(sql: &str) -> Result<Value, String> {
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = sql.as_bytes();
        let rc = unsafe { ffi::db_query_read_only(bytes.as_ptr() as i32, bytes.len() as i32) };
        if rc < 0 {
            return Err("db_query_read_only host call failed".to_string());
        }
        let envelope: Value = match host_buffer::read_host_json::<Value>() {
            Some(v) => v,
            None => return Err("malformed host response".to_string()),
        };
        if envelope["ok"].as_bool().unwrap_or(false) {
            return Ok(envelope);
        }
        Err(envelope["error"]
            .as_str()
            .unwrap_or("unknown error")
            .to_string())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let found = crate::host::db().map_or_else(
            || Ok(crate::host::db::ReadOnlyRows::default()),
            |host| host.query_read_only(sql),
        )?;
        Ok(serde_json::json!({
            "ok": true,
            "row_count": found.rows.len(),
            "truncated": found.truncated,
            "rows": found.rows,
        }))
    }
}

/// Escape a string for safe inclusion inside a single-quoted SQL string
/// literal, by doubling embedded single quotes (`'` → `''`).
///
/// Queries are built as plain SQL — there is no bound-parameter API — so
/// every user-supplied string interpolated into one must pass through this:
///
/// ```rust,ignore
/// db::query(&format!("SELECT * FROM users WHERE name = '{}'", db::sql_escape(name)));
/// ```
///
/// Pure (no host call): identical on wasm and native.
#[cfg_attr(
    feature = "db-v2",
    deprecated(note = "use cufflink_fn::sql with bound parameters")
)]
pub fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use crate::db;

    #[test]
    #[allow(deprecated)]
    fn sql_escape_doubles_single_quotes() {
        assert_eq!(db::sql_escape("it's"), "it''s");
        assert_eq!(db::sql_escape("o'hara's"), "o''hara''s");
        assert_eq!(db::sql_escape("plain"), "plain");
        assert_eq!(db::sql_escape(""), "");
    }

    #[test]
    fn a_rolled_back_batch_names_the_statement_that_failed() {
        assert_eq!(
            db::batch_failure(Some(1), 3, "duplicate key"),
            "statement 2 of 3 failed, batch rolled back: duplicate key"
        );
        assert_eq!(
            db::batch_failure(None, 3, "COMMIT failed: deadlock"),
            "transaction failed, batch rolled back: COMMIT failed: deadlock"
        );
    }

    #[test]
    fn without_a_host_a_batch_and_a_read_only_query_are_inert() {
        assert_eq!(db::execute_all(&["SELECT 1", "SELECT 2"]), Ok(vec![0, 0]));
        assert_eq!(db::execute_all(&[]), Ok(vec![]));
        assert_eq!(
            db::query_read_only("SELECT 1"),
            Ok(serde_json::json!({"ok": true, "row_count": 0, "truncated": false, "rows": []}))
        );
    }

    #[test]
    fn test_db_query_noop_on_native() {
        let rows = db::query("SELECT 1");
        assert!(rows.is_empty());
    }

    #[test]
    fn test_db_query_one_noop_on_native() {
        let row = db::query_one("SELECT 1");
        assert!(row.is_none());
    }

    #[test]
    fn test_db_execute_noop_on_native() {
        let affected = db::execute("INSERT INTO x VALUES (1)");
        assert_eq!(affected, 0);
    }
}
