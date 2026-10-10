//! SQL with bound parameters, structured errors, exact row typing and
//! interactive transactions (`db_call`). Needs a platform that supports
//! `db_v2`; the `db-v2` feature declares that in the module.
//!
//! ```rust,ignore
//! use cufflink_fn::prelude::*;
//!
//! let open = sql::query("SELECT id, total FROM orders WHERE customer = $1", &params![customer])?;
//! sql::transaction(|tx| {
//!     let row = tx.query_one("SELECT stock FROM items WHERE id = $1 FOR UPDATE", &params![item])?;
//!     let stock: i64 = row.map(|r| r.get("stock")).transpose()?.unwrap_or(0);
//!     if stock > 0 {
//!         tx.execute("UPDATE items SET stock = stock - 1 WHERE id = $1", &params![item])?;
//!     }
//!     Ok(stock > 0)
//! })?;
//! ```
//!
//! Outside the platform every call is `Err` with kind `Unavailable`, unless
//! a test host answers it.

mod param;
mod row;
mod transport;
mod tx;

pub use cufflink_types::db::{Column, DbError, DbErrorKind, Isolation, Param};
pub use param::IntoParam;
pub use row::Row;
pub use tx::{retry_transaction, transaction, transaction_with, Tx, TxOptions};

use cufflink_types::db::DbRequest;
use serde::de::DeserializeOwned;
use transport::call;

pub fn query(sql: &str, params: &[Param]) -> Result<Vec<Row>, DbError> {
    call(&query_request(sql, params, None))?.rows()
}

/// The only row, `None` for no rows; more than one is `TooManyRows`.
pub fn query_one(sql: &str, params: &[Param]) -> Result<Option<Row>, DbError> {
    at_most_one(query(sql, params)?)
}

/// Every row decoded as a `T` whose fields are the column names.
pub fn query_as<T: DeserializeOwned>(sql: &str, params: &[Param]) -> Result<Vec<T>, DbError> {
    query(sql, params)?.into_iter().map(Row::decode).collect()
}

/// Run a statement and return the number of rows it affected.
pub fn execute(sql: &str, params: &[Param]) -> Result<u64, DbError> {
    call(&DbRequest::Execute {
        sql: sql.into(),
        params: params.to_vec(),
        tx: None,
    })?
    .affected()
}

fn query_request(sql: &str, params: &[Param], tx: Option<u32>) -> DbRequest {
    DbRequest::Query {
        sql: sql.into(),
        params: params.to_vec(),
        tx,
    }
}

fn at_most_one(rows: Vec<Row>) -> Result<Option<Row>, DbError> {
    match rows.len() {
        0 | 1 => Ok(rows.into_iter().next()),
        n => Err(DbError::new(
            DbErrorKind::TooManyRows,
            format!("expected at most one row, got {n}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::transport::mock;
    use super::*;
    use cufflink_types::db::{DbResponse, RowMap};
    use serde_json::json;

    fn rows(values: &[serde_json::Value]) -> DbResponse {
        DbResponse::Rows {
            rows: values
                .iter()
                .map(|v| v.as_object().cloned().unwrap_or_else(RowMap::new))
                .collect(),
            columns: vec![Column {
                name: "id".into(),
                type_name: "int4".into(),
            }],
        }
    }

    fn failure(kind: DbErrorKind) -> DbResponse {
        DbResponse::Failed(DbError::new(kind, "scripted"))
    }

    fn ops(requests: &[DbRequest]) -> Vec<&'static str> {
        requests.iter().map(DbRequest::op).collect()
    }

    #[test]
    fn outside_the_platform_every_call_is_unavailable() {
        assert_eq!(
            query("SELECT 1", &[]).err().map(|e| e.kind),
            Some(DbErrorKind::Unavailable)
        );
        assert_eq!(
            transaction(|_| Ok(())).err().map(|e| e.kind),
            Some(DbErrorKind::Unavailable)
        );
    }

    #[test]
    fn queries_send_their_parameters_and_decode_rows() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Item {
            id: i64,
        }
        let (result, seen) = mock::with(
            |_| rows(&[json!({"id": 1}), json!({"id": 2})]),
            || query_as::<Item>("SELECT id FROM items WHERE a = $1", &crate::params![5]),
        );
        assert_eq!(result.ok(), Some(vec![Item { id: 1 }, Item { id: 2 }]));
        assert_eq!(
            seen,
            vec![DbRequest::Query {
                sql: "SELECT id FROM items WHERE a = $1".into(),
                params: vec![Param::I32(5)],
                tx: None
            }]
        );
        let (one, _) = mock::with(
            |_| rows(&[json!({"id": 1}), json!({"id": 2})]),
            || query_one("SELECT id FROM items", &[]),
        );
        assert_eq!(one.err().map(|e| e.kind), Some(DbErrorKind::TooManyRows));
        let (none, _) = mock::with(|_| rows(&[]), || query_one("SELECT 1", &[]));
        assert_eq!(none.ok(), Some(None));
        let (affected, _) = mock::with(
            |_| DbResponse::Affected(3),
            || execute("DELETE FROM t", &[]),
        );
        assert_eq!(affected.ok(), Some(3));
        let (confused, _) = mock::with(|_| DbResponse::Done, || execute("DELETE FROM t", &[]));
        assert_eq!(confused.err().map(|e| e.kind), Some(DbErrorKind::Protocol));
        let (failed, _) = mock::with(
            |_| failure(DbErrorKind::UniqueViolation),
            || execute("INSERT INTO t VALUES (1)", &[]),
        );
        assert_eq!(
            failed.err().map(|e| e.kind),
            Some(DbErrorKind::UniqueViolation)
        );
    }

    fn platform(request: &DbRequest) -> DbResponse {
        match request {
            DbRequest::Begin { .. } => DbResponse::Begun(4),
            DbRequest::Query { sql, .. } if sql.contains("try_advisory") => {
                rows(&[json!({"locked": true})])
            }
            DbRequest::Query { .. } => rows(&[json!({"id": 1})]),
            DbRequest::Execute { .. } => DbResponse::Affected(1),
            _ => DbResponse::Done,
        }
    }

    #[test]
    fn a_transaction_commits_when_the_closure_succeeds() {
        let (result, seen) = mock::with(platform, || {
            transaction_with(
                TxOptions {
                    isolation: Isolation::Serializable,
                    read_only: false,
                },
                |tx| {
                    tx.set_local("app.scope", "a")?;
                    tx.advisory_xact_lock(7)?;
                    let locked = tx.try_advisory_xact_lock(8)?;
                    tx.execute("UPDATE t SET a = 1", &[])?;
                    Ok(locked)
                },
            )
        });
        assert_eq!(result.ok(), Some(true));
        assert_eq!(
            ops(&seen),
            vec!["begin", "query", "query", "query", "execute", "commit"]
        );
        assert_eq!(
            seen[0],
            DbRequest::Begin {
                isolation: Isolation::Serializable,
                read_only: false
            }
        );
        assert_eq!(
            seen[1],
            DbRequest::Query {
                sql: "SELECT set_config($1, $2, true)".into(),
                params: vec![Param::Text("app.scope".into()), Param::Text("a".into())],
                tx: Some(4)
            }
        );
        assert_eq!(seen[5], DbRequest::Commit { tx: 4 });
    }

    #[test]
    fn a_failing_closure_rolls_back_and_keeps_its_own_error() {
        let (result, seen) = mock::with(
            |request| match request {
                DbRequest::Rollback { .. } => failure(DbErrorKind::Connection),
                other => platform(other),
            },
            || {
                transaction(|tx| -> Result<(), DbError> {
                    tx.execute("UPDATE t SET a = 1", &[])?;
                    Err(DbError::new(DbErrorKind::CheckViolation, "mine"))
                })
            },
        );
        let error = result.expect_err("closure error");
        assert_eq!(
            (error.kind, error.message.as_str()),
            (DbErrorKind::CheckViolation, "mine")
        );
        assert_eq!(ops(&seen), vec!["begin", "execute", "rollback"]);
    }

    #[test]
    fn a_failed_commit_is_the_error() {
        let (result, _) = mock::with(
            |request| match request {
                DbRequest::Commit { .. } => failure(DbErrorKind::SerializationFailure),
                other => platform(other),
            },
            || transaction(|_| Ok(1)),
        );
        assert_eq!(
            result.err().map(|e| e.kind),
            Some(DbErrorKind::SerializationFailure)
        );
    }

    #[test]
    fn savepoints_release_on_success_and_roll_back_on_error() {
        let (result, seen) = mock::with(platform, || {
            transaction(|tx| {
                tx.savepoint(|inner| inner.savepoint(|_| Ok(())))?;
                let failed = tx.savepoint(|inner| -> Result<(), DbError> {
                    inner.execute("INSERT INTO t VALUES (1)", &[])?;
                    Err(DbError::new(DbErrorKind::UniqueViolation, "dup"))
                });
                assert_eq!(
                    failed.err().map(|e| e.kind),
                    Some(DbErrorKind::UniqueViolation)
                );
                Ok(())
            })
        });
        assert!(result.is_ok());
        let names: Vec<(&str, String)> = seen
            .iter()
            .filter_map(|r| match r {
                DbRequest::Savepoint { name, .. } => Some(("savepoint", name.clone())),
                DbRequest::Release { name, .. } => Some(("release", name.clone())),
                DbRequest::RollbackTo { name, .. } => Some(("rollback_to", name.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            names,
            [
                ("savepoint", "sp_0"),
                ("savepoint", "sp_1"),
                ("release", "sp_1"),
                ("release", "sp_0"),
                ("savepoint", "sp_0"),
                ("rollback_to", "sp_0"),
                ("release", "sp_0"),
            ]
            .map(|(op, name)| (op, name.to_string()))
        );
    }

    #[test]
    fn savepoints_nest_no_deeper_than_the_platform_accepts() {
        fn nest(tx: &mut Tx, depth: u32) -> Result<u32, DbError> {
            tx.savepoint(|inner| match depth {
                0 => Ok(0),
                d => nest(inner, d - 1).map(|n| n + 1),
            })
        }
        let (deep, _) = mock::with(platform, || transaction(|tx| nest(tx, 999)));
        assert_eq!(deep.ok(), Some(999));
        let (too_deep, _) = mock::with(platform, || transaction(|tx| nest(tx, 1_000)));
        assert_eq!(
            too_deep.err().map(|e| e.kind),
            Some(DbErrorKind::InvalidParameter)
        );
    }

    #[test]
    fn retryable_failures_run_the_transaction_again() {
        let attempts = std::rc::Rc::new(std::cell::Cell::new(0));
        let counter = std::rc::Rc::clone(&attempts);
        let (result, _) = mock::with(
            move |request| match request {
                DbRequest::Commit { .. } if counter.get() < 3 => {
                    counter.set(counter.get() + 1);
                    failure(DbErrorKind::SerializationFailure)
                }
                other => platform(other),
            },
            || retry_transaction(5, TxOptions::default(), |_| Ok("done")),
        );
        assert_eq!(result.ok(), Some("done"));
        assert_eq!(attempts.get(), 3);
        let (exhausted, seen) = mock::with(
            |request| match request {
                DbRequest::Commit { .. } => failure(DbErrorKind::DeadlockDetected),
                other => platform(other),
            },
            || retry_transaction(2, TxOptions::default(), |_| Ok(())),
        );
        assert_eq!(
            exhausted.err().map(|e| e.kind),
            Some(DbErrorKind::DeadlockDetected)
        );
        assert_eq!(ops(&seen).iter().filter(|op| **op == "begin").count(), 2);
        let (final_error, seen) = mock::with(
            |request| match request {
                DbRequest::Commit { .. } => failure(DbErrorKind::UniqueViolation),
                other => platform(other),
            },
            || retry_transaction(5, TxOptions::default(), |_| Ok(())),
        );
        assert_eq!(
            final_error.err().map(|e| e.kind),
            Some(DbErrorKind::UniqueViolation)
        );
        assert_eq!(ops(&seen).iter().filter(|op| **op == "begin").count(), 1);
    }
}
