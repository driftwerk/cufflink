use crate::errors::{PlatformError, PlatformResult};
use crate::repo::deploy_jobs::{self, Ending, Finished, Heartbeat, JobStatus};
use crate::repo::deploy_lock::DeployLock;
use crate::shutdown::{Shutdown, KILL_MARGIN};
use crate::AppState;
use deadpool_postgres::Pool;
use futures_util::FutureExt;
use serde_json::Value;
use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;
use uuid::Uuid;

const HEARTBEAT: Duration = Duration::from_secs(10);
const LOST_AFTER: Duration = Duration::from_secs(60);
const SWEEP: Duration = Duration::from_secs(30);
const INTERRUPTED: &str = "interrupted by shutdown";

pub async fn detached<T: Send + 'static>(
    shutdown: &Shutdown,
    name: &'static str,
    work: impl Future<Output = PlatformResult<T>> + Send + 'static,
) -> PlatformResult<T> {
    shutdown
        .spawn(name, work)
        .await
        .map_err(|e| PlatformError::Internal(format!("the {name} did not run to its end: {e}")))?
}

pub fn replica_name() -> String {
    format!(
        "{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "platform".into()),
        std::process::id()
    )
}

fn panicked(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("no message")
}

#[derive(Clone)]
pub struct Job {
    pub pool: Pool,
    pub shutdown: Shutdown,
    pub id: Uuid,
}

impl Job {
    pub async fn run<F>(self, lock: DeployLock, deploy: impl FnOnce(DeployLock) -> F)
    where
        F: Future<Output = PlatformResult<Value>>,
    {
        let (ending, last) = match deploy_jobs::start(&self.pool, self.id).await {
            Ok(first) => {
                let last = Mutex::new(first);
                let ending = self.follow(deploy(lock), &last).await;
                let last = last.into_inner().unwrap_or_else(PoisonError::into_inner);
                (ending, Some(last))
            }
            Err(e) => {
                lock.release().await;
                (Ending::Failed(format!("not started: {e}")), None)
            }
        };
        match deploy_jobs::finish(&self.pool, self.id, &ending, last).await {
            Ok(Finished::Recorded) => {}
            Ok(Finished::Corrected) => tracing::warn!(
                job = %self.id,
                "deploy taken for lost while its replica ran it; its ending is recorded"
            ),
            Ok(Finished::Kept) => {
                tracing::warn!(job = %self.id, "deploy job had ended before its deploy did")
            }
            Err(e) => tracing::error!(job = %self.id, error = %e, "deploy job ending not recorded"),
        }
    }

    async fn follow(
        &self,
        deploy: impl Future<Output = PlatformResult<Value>>,
        last: &Mutex<Heartbeat>,
    ) -> Ending {
        tokio::select! {
            done = AssertUnwindSafe(deploy).catch_unwind() => match done {
                Ok(Ok(result)) => Ending::Succeeded(result),
                Ok(Err(error)) => Ending::Failed(error.to_string()),
                Err(payload) => {
                    let reason = panicked(payload.as_ref());
                    tracing::error!(job = %self.id, reason, "deploy panicked");
                    Ending::Failed(format!("the deploy panicked: {reason}"))
                }
            },
            interrupted = self.heartbeats(last) => interrupted,
        }
    }

    async fn heartbeats(&self, last: &Mutex<Heartbeat>) -> Ending {
        let mut beat = tokio::time::interval(HEARTBEAT);
        beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let deadline = async {
            self.shutdown.cancelled().await;
            tokio::time::sleep(self.shutdown.remaining().saturating_sub(KILL_MARGIN)).await;
        };
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                _ = &mut deadline => return Ending::Failed(INTERRUPTED.into()),
                _ = beat.tick() => {
                    match deploy_jobs::heartbeat(&self.pool, self.id).await {
                        Ok(Some(beat)) => {
                            *last.lock().unwrap_or_else(PoisonError::into_inner) = beat;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(job = %self.id, error = %e, "deploy heartbeat not recorded")
                        }
                    }
                }
            }
        }
    }
}

pub fn start_sweep(state: &AppState) {
    let (pool, shutdown) = (state.platform_db.clone(), state.shutdown.clone());
    let retention = state.config.deploy_job_retention;
    state.shutdown.spawn("deploy_sweep", async move {
        let mut every = tokio::time::interval(SWEEP);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = every.tick() => sweep(&pool, retention).await,
                _ = shutdown.cancelled() => return,
            }
        }
    });
}

async fn sweep(pool: &Pool, retention: Duration) {
    match deploy_jobs::sweep_lost(pool, LOST_AFTER).await {
        Ok(lost) => lost
            .iter()
            .for_each(|id| tracing::warn!(job = %id, "deploy failed: replica lost")),
        Err(e) => tracing::warn!(error = %e, "deploy jobs not swept"),
    }
    if let Err(e) = deploy_jobs::prune(pool, retention).await {
        tracing::warn!(error = %e, "ended deploy jobs not pruned");
    }
    if let Ok(counts) = deploy_jobs::counts(pool).await {
        JobStatus::ALL.into_iter().for_each(|status| {
            let count = counts
                .iter()
                .find(|(known, _)| *known == status)
                .map_or(0, |(_, count)| *count);
            metrics::gauge!("cufflink_deploy_jobs", "status" => status.as_str()).set(count as f64);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_is_recorded_by_what_it_said() {
        let said: Box<dyn Any + Send> = Box::new("index out of bounds");
        assert_eq!(panicked(said.as_ref()), "index out of bounds");
        let formatted: Box<dyn Any + Send> = Box::new(String::from("no table 'notes'"));
        assert_eq!(panicked(formatted.as_ref()), "no table 'notes'");
        let other: Box<dyn Any + Send> = Box::new(7_u8);
        assert_eq!(panicked(other.as_ref()), "no message");
    }

    #[test]
    fn the_replica_is_named_by_its_host_and_process() {
        assert!(replica_name().ends_with(&format!(":{}", std::process::id())));
    }
}
