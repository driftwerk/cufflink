use crate::errors::{PlatformError, PlatformResult};
use deadpool_postgres::{Client, Object, Pool};
use tracing::warn;

pub struct AdvisoryLock {
    client: Option<Client>,
    key: i64,
    held: bool,
    dropped: &'static str,
}

impl AdvisoryLock {
    async fn session(pool: &Pool, key: i64, dropped: &'static str) -> PlatformResult<Self> {
        Ok(Self {
            client: Some(pool.get().await?),
            key,
            held: false,
            dropped,
        })
    }

    pub async fn try_acquire(
        pool: &Pool,
        key: i64,
        dropped: &'static str,
    ) -> PlatformResult<Option<Self>> {
        let mut lock = Self::session(pool, key, dropped).await?;
        lock.held = lock
            .client()?
            .query_one("SELECT pg_try_advisory_lock($1)", &[&key])
            .await
            .map_err(|e| PlatformError::Internal(format!("Failed to acquire the lock: {e}")))?
            .get(0);
        match lock.held {
            true => Ok(Some(lock)),
            false => {
                drop(lock.client.take());
                Ok(None)
            }
        }
    }

    pub async fn acquire(pool: &Pool, key: i64, dropped: &'static str) -> PlatformResult<Self> {
        let mut lock = Self::session(pool, key, dropped).await?;
        lock.client()?
            .execute("SELECT pg_advisory_lock($1)", &[&key])
            .await
            .map_err(|e| {
                PlatformError::Unavailable(format!(
                    "the lock was not free in time: {}",
                    crate::db_error::from_pg(&e).message
                ))
            })?;
        lock.held = true;
        Ok(lock)
    }

    pub fn client(&self) -> PlatformResult<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| PlatformError::Internal("the lock's session is gone".into()))
    }

    pub async fn release(mut self) {
        let Some(client) = self.client.as_ref() else {
            return;
        };
        match client
            .execute("SELECT pg_advisory_unlock($1)", &[&self.key])
            .await
        {
            Ok(_) => drop(self.client.take()),
            Err(e) => warn!(error = %e, "advisory unlock failed; closing the session"),
        }
    }
}

impl Drop for AdvisoryLock {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if self.held {
                metrics::counter!(self.dropped).increment(1);
            }
            drop(Object::take(client));
        }
    }
}
