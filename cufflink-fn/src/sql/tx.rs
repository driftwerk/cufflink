//! Interactive transactions: a [`Tx`] exists only inside the closure that
//! [`transaction`] runs, which always ends in a commit or a rollback.

use super::transport::call;
use super::{query_request, Row};
use cufflink_types::db::{DbError, DbErrorKind, DbRequest, Isolation, Param};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

/// How a transaction runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxOptions {
    pub isolation: Isolation,
    pub read_only: bool,
}

/// The platform accepts savepoint names `sp_0` to `sp_999`.
const MAX_SAVEPOINT_DEPTH: u32 = 1_000;

/// An open transaction. Statements through it run on the transaction's own
/// connection; it cannot outlive the closure it was handed to.
pub struct Tx {
    id: u32,
    depth: u32,
    _not_send: PhantomData<*const ()>,
}

/// Run `f` in a transaction: `Ok` commits (a failed commit is the `Err`),
/// `Err` rolls back and returns the closure's error.
pub fn transaction<T>(f: impl FnOnce(&mut Tx) -> Result<T, DbError>) -> Result<T, DbError> {
    transaction_with(TxOptions::default(), f)
}

/// [`transaction`] with an isolation level or read-only mode.
pub fn transaction_with<T>(
    options: TxOptions,
    f: impl FnOnce(&mut Tx) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let id = call(&DbRequest::Begin {
        isolation: options.isolation,
        read_only: options.read_only,
    })?
    .begun()?;
    let mut tx = Tx::new(id, 0);
    match f(&mut tx) {
        Ok(value) => call(&DbRequest::Commit { tx: id })?.done().map(|()| value),
        Err(error) => {
            if let Err(rollback) = call(&DbRequest::Rollback { tx: id }) {
                crate::log::warn(&format!(
                    "rolling back transaction {id} after an error failed too: {rollback}"
                ));
            }
            Err(error)
        }
    }
}

/// [`transaction_with`], run again while it fails with a serialization
/// failure or a deadlock, up to `max_attempts` runs in all.
pub fn retry_transaction<T>(
    max_attempts: u32,
    options: TxOptions,
    mut f: impl FnMut(&mut Tx) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let mut attempt = 1;
    loop {
        match transaction_with(options, &mut f) {
            Err(error) if error.is_retryable() && attempt < max_attempts => attempt += 1,
            outcome => return outcome,
        }
    }
}

impl Tx {
    fn new(id: u32, depth: u32) -> Self {
        Self {
            id,
            depth,
            _not_send: PhantomData,
        }
    }

    pub fn query(&mut self, sql: &str, params: &[Param]) -> Result<Vec<Row>, DbError> {
        call(&query_request(sql, params, Some(self.id)))?.rows()
    }

    /// The only row, `None` for no rows; more than one is `TooManyRows`.
    pub fn query_one(&mut self, sql: &str, params: &[Param]) -> Result<Option<Row>, DbError> {
        super::at_most_one(self.query(sql, params)?)
    }

    pub fn query_as<T: DeserializeOwned>(
        &mut self,
        sql: &str,
        params: &[Param],
    ) -> Result<Vec<T>, DbError> {
        self.query(sql, params)?
            .into_iter()
            .map(Row::decode)
            .collect()
    }

    pub fn execute(&mut self, sql: &str, params: &[Param]) -> Result<u64, DbError> {
        call(&DbRequest::Execute {
            sql: sql.into(),
            params: params.to_vec(),
            tx: Some(self.id),
        })?
        .affected()
    }

    /// `SET LOCAL setting = value`, reverted when the transaction ends.
    pub fn set_local(&mut self, setting: &str, value: &str) -> Result<(), DbError> {
        self.query(
            "SELECT set_config($1, $2, true)",
            &[Param::Text(setting.into()), Param::Text(value.into())],
        )
        .map(drop)
    }

    /// Wait for the transaction-scoped advisory lock `key`.
    pub fn advisory_xact_lock(&mut self, key: i64) -> Result<(), DbError> {
        self.query("SELECT pg_advisory_xact_lock($1)", &[Param::I64(key)])
            .map(drop)
    }

    /// Take the transaction-scoped advisory lock `key` if it is free.
    pub fn try_advisory_xact_lock(&mut self, key: i64) -> Result<bool, DbError> {
        self.query(
            "SELECT pg_try_advisory_xact_lock($1) AS locked",
            &[Param::I64(key)],
        )?
        .first()
        .ok_or_else(|| super::transport::protocol("the lock query returned no row"))?
        .get("locked")
    }

    /// Run `f` inside a savepoint: `Ok` releases it, `Err` rolls back to it
    /// (keeping the work before it) and returns the closure's error.
    pub fn savepoint<T>(
        &mut self,
        f: impl FnOnce(&mut Tx) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        if self.depth >= MAX_SAVEPOINT_DEPTH {
            return Err(DbError::new(
                DbErrorKind::InvalidParameter,
                format!("savepoints nest at most {MAX_SAVEPOINT_DEPTH} deep"),
            ));
        }
        let name = format!("sp_{}", self.depth);
        let request = |make: fn(u32, String) -> DbRequest| make(self.id, name.clone());
        call(&request(|tx, name| DbRequest::Savepoint { tx, name }))?.done()?;
        match f(&mut Tx::new(self.id, self.depth + 1)) {
            Ok(value) => call(&request(|tx, name| DbRequest::Release { tx, name }))?
                .done()
                .map(|()| value),
            Err(error) => {
                let recovered = call(&request(|tx, name| DbRequest::RollbackTo { tx, name }))
                    .and_then(|_| call(&request(|tx, name| DbRequest::Release { tx, name })));
                if let Err(rollback) = recovered {
                    crate::log::warn(&format!(
                        "rolling back to {name} after an error failed too: {rollback}"
                    ));
                }
                Err(error)
            }
        }
    }
}
