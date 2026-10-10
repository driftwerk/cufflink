use crate::errors::PlatformError;
use crate::nats_conn::Backoff;
use crate::shutdown::Shutdown;
use eyre::eyre;
use std::error::Error;
use std::future::Future;
use tokio::task::JoinHandle;
use tracing::{error, warn, Level};

#[derive(Debug)]
pub enum Abort {
    Shutdown,
    Fatal(eyre::Report),
}

pub trait Permanence: std::fmt::Display {
    fn is_permanent(&self) -> bool;
}

impl Permanence for eyre::Report {
    fn is_permanent(&self) -> bool {
        self.chain().any(permanent_cause)
    }
}

impl Permanence for PlatformError {
    fn is_permanent(&self) -> bool {
        std::iter::successors(Some(self as &(dyn Error + 'static)), |e| (*e).source())
            .any(permanent_cause)
    }
}

fn permanent_cause(error: &(dyn Error + 'static)) -> bool {
    if let Some(migration) = error.downcast_ref::<refinery::Error>() {
        return matches!(
            migration.kind(),
            refinery::error::Kind::DivergentVersion(..) | refinery::error::Kind::MissingVersion(..)
        );
    }
    error
        .downcast_ref::<tokio_postgres::Error>()
        .and_then(tokio_postgres::Error::code)
        .is_some_and(|state| permanent_sqlstate(state.code()))
}

fn permanent_sqlstate(code: &str) -> bool {
    code.starts_with("28") || matches!(code, "3D000" | "42501")
}

pub async fn until_ready<T, E, Fut>(
    shutdown: &Shutdown,
    level: Level,
    what: &str,
    mut op: impl FnMut() -> Fut,
) -> Result<T, Abort>
where
    Fut: Future<Output = Result<T, E>>,
    E: Permanence,
{
    let mut backoff = Backoff::new();
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) if e.is_permanent() => {
                return Err(Abort::Fatal(eyre!("{what} failed permanently: {e}")))
            }
            Err(e) => {
                let retry_in = backoff.next_delay();
                if level == Level::ERROR {
                    error!(error = %e, ?retry_in, "{what} not ready; retrying");
                } else {
                    warn!(error = %e, ?retry_in, "{what} not ready; retrying");
                }
                tokio::select! {
                    _ = shutdown.cancelled() => return Err(Abort::Shutdown),
                    _ = tokio::time::sleep(retry_in) => {}
                }
            }
        }
    }
}

pub(super) async fn bootstrap_failure(bootstrap: JoinHandle<eyre::Result<()>>) -> eyre::Report {
    match bootstrap.await {
        Ok(Ok(())) => std::future::pending().await,
        Ok(Err(error)) => error,
        Err(join) => eyre!("the bootstrap task failed: {join}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn credentials_missing_databases_and_privileges_are_permanent() {
        for code in ["28P01", "28000", "3D000", "42501"] {
            assert!(permanent_sqlstate(code), "{code}");
        }
        for code in [
            "42P01", "42703", "42704", "42883", "57P03", "53300", "08006", "40001", "3F000",
        ] {
            assert!(!permanent_sqlstate(code), "{code}");
        }
    }

    #[test]
    fn errors_without_a_permanent_cause_are_retried() {
        assert!(!eyre!("connection refused").is_permanent());
        assert!(!PlatformError::Internal("pool timed out".into()).is_permanent());
    }

    #[tokio::test]
    async fn a_permanent_error_ends_the_wait_at_once() {
        let shutdown = Shutdown::new(Duration::from_secs(1));
        let mut calls = 0;
        let outcome = until_ready(&shutdown, Level::ERROR, "step", || {
            calls += 1;
            std::future::ready(Err::<(), _>(FixedPermanence(true)))
        })
        .await;
        assert!(
            matches!(outcome, Err(Abort::Fatal(e)) if e.to_string() == "step failed permanently: fixed")
        );
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn a_transient_error_waits_until_shutdown() {
        let shutdown = Shutdown::new(Duration::from_secs(1));
        shutdown.trigger();
        let outcome = until_ready(&shutdown, Level::WARN, "step", || {
            std::future::ready(Err::<(), _>(FixedPermanence(false)))
        })
        .await;
        assert!(matches!(outcome, Err(Abort::Shutdown)));
    }

    #[tokio::test]
    async fn a_failed_or_panicked_bootstrap_is_reported_and_a_finished_one_is_not() {
        let failed = tokio::spawn(async { Err(eyre!("divergent")) });
        assert_eq!(bootstrap_failure(failed).await.to_string(), "divergent");

        let panicked = tokio::spawn(async { panic!("boom") });
        let reason = bootstrap_failure(panicked).await.to_string();
        assert!(reason.starts_with("the bootstrap task failed"), "{reason}");

        let finished = tokio::spawn(async { Ok(()) });
        let pending =
            tokio::time::timeout(Duration::from_millis(50), bootstrap_failure(finished)).await;
        assert!(pending.is_err());
    }

    struct FixedPermanence(bool);

    impl std::fmt::Display for FixedPermanence {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("fixed")
        }
    }

    impl Permanence for FixedPermanence {
        fn is_permanent(&self) -> bool {
            self.0
        }
    }
}
