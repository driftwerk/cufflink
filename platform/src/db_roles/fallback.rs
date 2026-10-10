use crate::policy::record::{LogThrottle, LOG_THROTTLE};
use crate::policy::{self, PolicyKey, PolicyMode, Violation};
use cufflink_pgcodec::session::{set_role, Scope};
use deadpool_postgres::GenericClient;
use futures_util::FutureExt;
use std::future::Future;
use std::sync::LazyLock;
use std::time::Instant;
use tokio_postgres::error::SqlState;
use tokio_postgres::Error;

const SAVEPOINT: &str = "SAVEPOINT cufflink_role";
const RELEASE: &str = "RELEASE SAVEPOINT cufflink_role";
const ROLLBACK_TO: &str = "ROLLBACK TO SAVEPOINT cufflink_role";

pub const REFUSED: &str = "role_refused";

static LOGGED: LazyLock<LogThrottle<(String, String)>> =
    LazyLock::new(|| LogThrottle::new(LOG_THROTTLE));

pub fn refused(error: &Error) -> bool {
    error.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE)
}

fn outside_only(error: &Error) -> bool {
    error.code() == Some(&SqlState::ACTIVE_SQL_TRANSACTION)
}

pub fn message(refusal: &Error) -> &str {
    refusal
        .as_db_error()
        .map_or("permission denied", |e| e.message())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fallback<'a> {
    pub role: &'a str,
    pub tenant: &'a str,
    pub service: &'a str,
}

struct Alone<'a, C: GenericClient> {
    client: &'a C,
    open: bool,
}

impl<C: GenericClient> Drop for Alone<'_, C> {
    fn drop(&mut self) {
        if self.open {
            let _ = self.client.batch_execute("ROLLBACK").now_or_never();
        }
    }
}

impl Fallback<'_> {
    pub async fn in_transaction<C, T, F, Fut>(&self, client: &C, run: &F) -> Result<T, Error>
    where
        C: GenericClient + Sync,
        F: Fn() -> Fut + Sync,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let (savepoint, outcome) = tokio::join!(client.batch_execute(SAVEPOINT), run());
        match savepoint {
            Ok(()) => self.settle(client, outcome, run).await,
            Err(_) => outcome,
        }
    }

    pub async fn in_session<C, T, F, Fut>(&self, client: &C, run: &F) -> Result<T, Error>
    where
        C: GenericClient + Sync,
        F: Fn() -> Fut + Sync,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let refusal = match run().await {
            Err(e) if refused(&e) => e,
            outcome => return outcome,
        };
        if !self.is_current(client).await? {
            return Err(refusal);
        }
        self.record(&refusal);
        client.batch_execute("SET ROLE NONE").await?;
        let outcome = run().await;
        client
            .batch_execute(&set_role(Scope::Session, self.role))
            .await?;
        outcome
    }

    pub async fn alone<C, T, F, Fut>(&self, client: &C, run: &F) -> Result<T, Error>
    where
        C: GenericClient + Sync,
        F: Fn() -> Fut + Sync,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let mut own = Alone { client, open: true };
        let begin = format!("BEGIN; {}", set_role(Scope::Transaction, self.role));
        let (begun, outcome) = tokio::join!(client.batch_execute(&begin), run());
        let end = match (&begun, &outcome) {
            (Ok(()), Ok(_)) => "COMMIT",
            _ => "ROLLBACK",
        };
        client.batch_execute(end).await?;
        own.open = false;
        match (begun, outcome) {
            (Err(e), _) => Err(e),
            (Ok(()), Err(e)) if refused(&e) => {
                self.record(&e);
                run().await
            }
            (Ok(()), outcome) => outcome,
        }
    }

    async fn settle<C, T, F, Fut>(
        &self,
        client: &C,
        outcome: Result<T, Error>,
        run: &F,
    ) -> Result<T, Error>
    where
        C: GenericClient + Sync,
        F: Fn() -> Fut + Sync,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let refusal = match outcome {
            Ok(value) => return client.batch_execute(RELEASE).await.map(|()| value),
            Err(e) if refused(&e) => e,
            Err(e) if outside_only(&e) => {
                client
                    .batch_execute(&format!("{ROLLBACK_TO}; {RELEASE}"))
                    .await?;
                return run().await;
            }
            Err(e) => return Err(e),
        };
        client.batch_execute(ROLLBACK_TO).await?;
        if !self.is_current(client).await? {
            return run().await;
        }
        self.record(&refusal);
        client.batch_execute("SET LOCAL ROLE NONE").await?;
        let value = run().await?;
        client
            .batch_execute(&format!(
                "{}; {RELEASE}",
                set_role(Scope::Transaction, self.role)
            ))
            .await?;
        Ok(value)
    }

    async fn is_current<C: GenericClient + Sync>(&self, client: &C) -> Result<bool, Error> {
        client
            .query_one("SELECT current_user::text = $1::text", &[&self.role])
            .await
            .map(|row| row.get(0))
    }

    fn record(&self, refusal: &Error) {
        let message = message(refusal);
        metrics::counter!(
            "cufflink_db_role_fallback_total",
            "tenant" => self.tenant.to_string(),
            "service" => self.service.to_string(),
        )
        .increment(1);
        let _ = policy::apply(
            PolicyMode::Warn,
            PolicyKey::DbRoles,
            self.tenant,
            Some(Violation::new(REFUSED, message)),
        );
        if LOGGED.should_log(
            (self.tenant.to_string(), self.service.to_string()),
            Instant::now(),
        ) {
            tracing::warn!(
                event = "db_role_fallback",
                tenant = %self.tenant,
                service = %self.service,
                role = %self.role,
                refusal = %message,
                "A statement the service's role was refused ran as the platform"
            );
        }
    }
}
