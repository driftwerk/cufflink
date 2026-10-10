use cufflink_platform::errors::PlatformResult;
use cufflink_platform::nats_manager::events::LONG_JOBS_STREAM;
use cufflink_platform::repo::wasm_jobs::{self, JobStatus, WasmJob};
use cufflink_platform::router::jobs::{self, dead_letter};
use cufflink_platform::AppState;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

const BATCH: i64 = 200;
const RETENTION_EVERY: Duration = Duration::from_secs(3600);
const RETENTION_BATCH: u32 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    pub every: Duration,
    pub republish_after: Duration,
}

impl Settings {
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let vars = cufflink_db::Vars(var);
        Ok(Self {
            every: Duration::from_secs(vars.positive("JOB_RECLAIM_INTERVAL_SECS", 30)?),
            republish_after: Duration::from_secs(vars.positive("JOB_REPUBLISH_AFTER_SECS", 300)?),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reclaimed {
    Unpublished,
    LeaseExpired,
    Exhausted,
}

impl Reclaimed {
    fn of(job: &WasmJob) -> Self {
        match job.status {
            _ if job.out_of_attempts() => Reclaimed::Exhausted,
            JobStatus::Running => Reclaimed::LeaseExpired,
            _ => Reclaimed::Unpublished,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Reclaimed::Unpublished => "unpublished",
            Reclaimed::LeaseExpired => "lease_expired",
            Reclaimed::Exhausted => "exhausted",
        }
    }
}

fn republish_after(configured: Duration, window: Duration) -> Duration {
    match window.is_zero() {
        true => configured,
        false => configured.min(window),
    }
}

fn emptied<'a>(before: &'a HashSet<String>, now: &HashSet<String>) -> Vec<&'a String> {
    before
        .iter()
        .filter(|tenant| !now.contains(*tenant))
        .collect()
}

struct Reclaimer {
    app_state: Arc<AppState>,
    settings: Settings,
    republish_after: Duration,
    reported: HashSet<String>,
}

impl Reclaimer {
    fn pool(&self) -> &deadpool_postgres::Pool {
        &self.app_state.platform_db
    }

    async fn publish(&self, job: &WasmJob) -> PlatformResult<()> {
        match jobs::publish(&self.app_state, job).await {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!(job_id = %job.id, error = %e, "Could not publish a reclaimed job");
                wasm_jobs::set_published(self.pool(), job.id, false).await
            }
        }
    }

    async fn republish_after(&mut self) -> Duration {
        match self.app_state.jetstream.get_stream(LONG_JOBS_STREAM).await {
            Ok(stream) => {
                let window = stream.cached_info().config.duplicate_window;
                let after = republish_after(self.settings.republish_after, window);
                if after != self.republish_after {
                    info!(
                        republish_after = ?after,
                        duplicate_window = ?window,
                        "A published job waits this long for a worker"
                    );
                    self.republish_after = after;
                }
            }
            Err(e) => warn!(
                error = %e,
                republish_after = ?self.republish_after,
                "Could not read the long-jobs stream's duplicate window; a published job waits as on the last pass"
            ),
        }
        self.republish_after
    }

    async fn reclaim(&mut self) -> PlatformResult<()> {
        let republish_after = self.republish_after().await;
        let due = wasm_jobs::reclaim(self.pool(), republish_after, BATCH).await?;
        for job in &due {
            let reclaimed = Reclaimed::of(job);
            metrics::counter!("cufflink_job_reclaimed_total", "reason" => reclaimed.as_str())
                .increment(1);
            let done = match reclaimed {
                Reclaimed::Exhausted => jobs::fail_lost(&self.app_state, job).await.map(drop),
                Reclaimed::Unpublished | Reclaimed::LeaseExpired => self.publish(job).await,
            };
            if let Err(e) = done {
                warn!(job_id = %job.id, error = %e, "Could not reclaim a job");
            }
        }
        Ok(())
    }

    async fn file_dead_letters(&self) -> PlatformResult<()> {
        for job in wasm_jobs::undelivered_dead_letters(self.pool(), BATCH).await? {
            dead_letter::file(&self.app_state, &job).await;
        }
        Ok(())
    }

    async fn report_backlog(&mut self) -> PlatformResult<()> {
        let backlog = wasm_jobs::unfinished_by_tenant(self.pool()).await?;
        let gauge =
            |tenant: &String| metrics::gauge!("cufflink_job_backlog", "tenant" => tenant.clone());
        let tenants: HashSet<String> = backlog
            .into_iter()
            .map(|(tenant, unfinished)| {
                gauge(&tenant).set(unfinished as f64);
                tenant
            })
            .collect();
        emptied(&self.reported, &tenants)
            .into_iter()
            .for_each(|tenant| gauge(tenant).set(0.0));
        self.reported = tenants;
        Ok(())
    }

    async fn delete_expired(&self) -> PlatformResult<()> {
        let mut deleted = 0;
        loop {
            let batch = wasm_jobs::delete_expired(self.pool(), i64::from(RETENTION_BATCH)).await?;
            deleted += batch;
            if batch < u64::from(RETENTION_BATCH) || self.app_state.shutdown.is_triggered() {
                break;
            }
        }
        if deleted > 0 {
            info!(deleted, "Deleted the ended jobs kept long enough");
        }
        Ok(())
    }

    async fn pass(&mut self, retention_due: bool) {
        let steps = [
            ("reclaim", self.reclaim().await),
            ("dead letters", self.file_dead_letters().await),
            ("backlog", self.report_backlog().await),
            (
                "retention",
                match retention_due {
                    true => self.delete_expired().await,
                    false => Ok(()),
                },
            ),
        ];
        for (step, done) in steps {
            if let Err(e) = done {
                warn!(step, error = %e, "A reclaimer step failed; the next pass does it");
            }
        }
    }
}

pub async fn run(app_state: Arc<AppState>, settings: Settings) -> eyre::Result<()> {
    let shutdown = app_state.shutdown.clone();
    let mut reclaimer = Reclaimer {
        app_state,
        settings,
        republish_after: settings.republish_after,
        reported: HashSet::new(),
    };
    info!(
        every = ?settings.every,
        republish_after = ?settings.republish_after,
        "Job reclaimer started"
    );
    let mut passes = tokio::time::interval(settings.every);
    passes.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut retention_at = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = passes.tick() => {}
        }
        let retention_due = Instant::now() >= retention_at;
        reclaimer.pass(retention_due).await;
        if retention_due {
            retention_at = Instant::now() + RETENTION_EVERY;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn settings(vars: &[(&str, &str)]) -> eyre::Result<Settings> {
        let vars: HashMap<&str, &str> = vars.iter().copied().collect();
        Settings::from_vars(|name| vars.get(name).map(|value| value.to_string()))
    }

    #[test]
    fn a_pass_is_every_half_minute_and_a_job_waits_five_for_a_worker() {
        assert_eq!(
            settings(&[]).expect("defaults"),
            Settings {
                every: Duration::from_secs(30),
                republish_after: Duration::from_secs(300),
            }
        );
        assert_eq!(
            settings(&[
                ("JOB_RECLAIM_INTERVAL_SECS", "1"),
                ("JOB_REPUBLISH_AFTER_SECS", " 45 ")
            ])
            .expect("set"),
            Settings {
                every: Duration::from_secs(1),
                republish_after: Duration::from_secs(45),
            }
        );
        for (name, unusable) in [
            ("JOB_RECLAIM_INTERVAL_SECS", "0"),
            ("JOB_REPUBLISH_AFTER_SECS", "soon"),
            ("JOB_REPUBLISH_AFTER_SECS", "-5"),
        ] {
            let refused = settings(&[(name, unusable)]).expect_err(unusable);
            assert!(
                refused.to_string().starts_with(name),
                "{name}={unusable:?}: {refused}"
            );
        }
    }

    #[test]
    fn a_published_job_waits_no_longer_than_the_stream_remembers_its_message() {
        let secs = Duration::from_secs;
        for (configured, window, waits) in [
            (300, 600, 300),
            (300, 120, 120),
            (300, 300, 300),
            (1, 600, 1),
            (300, 0, 300),
        ] {
            assert_eq!(
                republish_after(secs(configured), secs(window)),
                secs(waits),
                "{configured} s configured, a window of {window} s"
            );
        }
    }

    #[test]
    fn a_tenant_without_a_backlog_any_more_is_reported_empty() {
        let set = |tenants: &[&str]| -> HashSet<String> {
            tenants.iter().map(|tenant| tenant.to_string()).collect()
        };
        assert_eq!(
            emptied(&set(&["acme", "default"]), &set(&["default", "harbor"])),
            [&"acme".to_string()]
        );
        assert!(emptied(&set(&[]), &set(&["default"])).is_empty());
    }
}
