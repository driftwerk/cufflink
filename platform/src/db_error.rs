pub use cufflink_pgcodec::error::from_pg;
use cufflink_types::db::{DbError, DbErrorKind};

pub fn from_pool(error: &deadpool_postgres::PoolError) -> DbError {
    use deadpool_postgres::PoolError;
    match error {
        PoolError::Timeout(_) => pool_timeout(),
        PoolError::Backend(e) => from_pg(e),
        other => DbError::new(
            DbErrorKind::Connection,
            format!("database connection unavailable: {other}"),
        ),
    }
}

pub fn pool_timeout() -> DbError {
    DbError::new(
        DbErrorKind::PoolTimeout,
        "timed out waiting for a database connection",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_timeouts_and_closed_pools_are_classified() {
        use deadpool_postgres::{PoolError, TimeoutType};
        assert_eq!(
            from_pool(&PoolError::Timeout(TimeoutType::Wait)).kind,
            DbErrorKind::PoolTimeout
        );
        let closed = from_pool(&PoolError::Closed);
        assert_eq!(closed.kind, DbErrorKind::Connection);
        assert!(closed.code.is_none());
    }
}
