//! What sits between a pool and Postgres, and the guest statements a
//! transaction pooler cannot carry.

use crate::statement::{changes_session, requires_autocommit};
use std::str::FromStr;

/// What sits between a pool and Postgres.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Pooler {
    #[default]
    Direct,
    /// PgBouncer in `pool_mode=transaction`: a server connection is held
    /// only for the length of a transaction, so no session state may be
    /// relied on across statements, and startup parameters are refused.
    PgBouncerTransaction,
}

impl FromStr for Pooler {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "direct" => Ok(Pooler::Direct),
            "pgbouncer_transaction" => Ok(Pooler::PgBouncerTransaction),
            other => Err(format!(
                "'{other}' is not one of direct, pgbouncer_transaction"
            )),
        }
    }
}

impl Pooler {
    /// Whether consecutive statements outside a transaction may land on
    /// different server connections.
    pub fn is_transaction_pooler(self) -> bool {
        self == Pooler::PgBouncerTransaction
    }
}

/// Why a transaction pooler refuses a guest statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolerRefusal {
    /// It cannot run in a transaction, so its `SET` and `RESET` would land
    /// on other server connections; such DDL belongs in deploy-time
    /// migrations.
    Autocommit,
    /// It sets session state, which would outlive the transaction on a
    /// server connection other clients share. `set_config(…, false)` and
    /// `DO` blocks cannot be recognised this way.
    SessionState,
}

impl PoolerRefusal {
    pub fn message(self) -> &'static str {
        match self {
            PoolerRefusal::Autocommit => {
                "a statement that cannot run inside a transaction is refused under \
                 TENANT_DB_POOLER=pgbouncer_transaction; run it in a deploy-time migration"
            }
            PoolerRefusal::SessionState => {
                "SET and RESET are refused under TENANT_DB_POOLER=pgbouncer_transaction; \
                 use SET LOCAL inside a transaction"
            }
        }
    }
}

/// Why `pooler` refuses the guest statement `sql`, if it does.
pub fn pooler_refusal(pooler: Pooler, sql: &str) -> Option<PoolerRefusal> {
    if !pooler.is_transaction_pooler() {
        None
    } else if requires_autocommit(sql) {
        Some(PoolerRefusal::Autocommit)
    } else if changes_session(sql) {
        Some(PoolerRefusal::SessionState)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pooler_is_named_in_any_case() {
        assert_eq!(
            "PgBouncer_Transaction".parse::<Pooler>(),
            Ok(Pooler::PgBouncerTransaction)
        );
        assert_eq!(" direct ".parse::<Pooler>(), Ok(Pooler::Direct));
        assert!("session".parse::<Pooler>().is_err());
        assert!(Pooler::PgBouncerTransaction.is_transaction_pooler());
        assert!(!Pooler::Direct.is_transaction_pooler());
        assert_eq!(Pooler::default(), Pooler::Direct);
    }

    #[test]
    fn a_transaction_pooler_refuses_autocommit_and_session_statements_only() {
        let pooled = |sql| pooler_refusal(Pooler::PgBouncerTransaction, sql);
        let concurrently = "CREATE INDEX CONCURRENTLY i ON t (c)";
        assert_eq!(pooled(concurrently), Some(PoolerRefusal::Autocommit));
        assert_eq!(pooled("SET ROLE other"), Some(PoolerRefusal::SessionState));
        assert_eq!(pooled("SELECT 1"), None);
        assert_eq!(pooled("SET LOCAL app.x = '1'"), None);
        for sql in [concurrently, "SET ROLE other", "RESET ALL"] {
            assert_eq!(pooler_refusal(Pooler::Direct, sql), None, "{sql}");
        }
    }
}
