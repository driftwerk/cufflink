use crate::apps::{Apps, Ran};
use crate::coalescer::Coalescer;
use crate::source::Unread;
use cufflink_supervisor::runner_api::key_refused;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tracing::{info, warn};

type AppKey = (String, String);

const COLD_START_BACKOFF: Duration = Duration::from_secs(30);
const UNKNOWN_APP_TTL: Duration = Duration::from_secs(10);
const MAX_REMEMBERED: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Failed,
    Unknown,
}

impl Outcome {
    fn wait(self) -> Duration {
        match self {
            Outcome::Failed => COLD_START_BACKOFF,
            Outcome::Unknown => UNKNOWN_APP_TTL,
        }
    }
}

#[derive(Clone, Default)]
struct Recent(Arc<Mutex<HashMap<AppKey, (Instant, Outcome)>>>);

impl Recent {
    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<AppKey, (Instant, Outcome)>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn holding(&self, key: &AppKey, now: Instant) -> Option<(Outcome, Duration)> {
        self.entries().get(key).and_then(|(at, outcome)| {
            let left = outcome
                .wait()
                .saturating_sub(now.saturating_duration_since(*at));
            (!left.is_zero()).then_some((*outcome, left))
        })
    }

    fn record(&self, key: AppKey, result: &Result<(), WarmError>, now: Instant) {
        let mut entries = self.entries();
        let outcome = match result {
            Ok(()) => {
                entries.remove(&key);
                return;
            }
            Err(WarmError::WarmupFailed(_)) => Outcome::Failed,
            Err(WarmError::NotFound { .. }) => Outcome::Unknown,
            Err(WarmError::KeyRefused) => return,
        };
        if entries.len() >= MAX_REMEMBERED {
            entries.retain(|_, (at, kept)| now.saturating_duration_since(*at) < kept.wait());
        }
        if entries.len() >= MAX_REMEMBERED {
            entries.retain(|_, (_, kept)| *kept == Outcome::Failed);
        }
        entries.insert(key, (now, outcome));
    }
}

async fn recorded(
    recent: Recent,
    key: AppKey,
    work: impl Future<Output = Result<(), WarmError>>,
) -> Result<(), WarmError> {
    let result = work.await;
    recent.record(key, &result, Instant::now());
    result
}

#[derive(Debug, Clone)]
pub enum WarmError {
    NotFound { tenant: String, service: String },
    WarmupFailed(String),
    KeyRefused,
}

impl From<eyre::Report> for WarmError {
    fn from(error: eyre::Report) -> Self {
        match key_refused(&error) {
            true => WarmError::KeyRefused,
            false => WarmError::WarmupFailed(format!("{error:#}")),
        }
    }
}

impl std::fmt::Display for WarmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WarmError::NotFound { tenant, service } => {
                write!(f, "no active deployment for {tenant}/{service}")
            }
            WarmError::WarmupFailed(msg) => write!(f, "warmup failed: {msg}"),
            WarmError::KeyRefused => write!(f, "the platform refuses this runner's key"),
        }
    }
}

impl std::error::Error for WarmError {}

pub struct LazyWarmer {
    apps: Arc<Apps>,
    coalescer: Coalescer<AppKey, Result<(), WarmError>>,
    recent: Recent,
}

impl LazyWarmer {
    pub fn new(apps: Arc<Apps>) -> Self {
        Self {
            apps,
            coalescer: Coalescer::new(),
            recent: Recent::default(),
        }
    }

    pub fn serves(&self, tenant: &str) -> bool {
        self.apps.source.serves(tenant)
    }

    pub async fn ensure_warm(&self, tenant: &str, service: &str) -> Result<(), WarmError> {
        let process_manager = &self.apps.process_manager;
        if process_manager.get_port(tenant, service).is_some() {
            return Ok(());
        }
        if self.apps.source.key_refused() {
            return Err(WarmError::KeyRefused);
        }
        if process_manager.is_crash_looping(tenant, service) {
            return Err(WarmError::WarmupFailed(format!(
                "{tenant}/{service} keeps exiting right after it starts; it restarts with its next deploy"
            )));
        }
        let key = (tenant.to_string(), service.to_string());
        match self.recent.holding(&key, Instant::now()) {
            Some((Outcome::Failed, wait)) => {
                return Err(WarmError::WarmupFailed(format!(
                    "the last cold start of {tenant}/{service} failed; retrying in {}s",
                    wait.as_secs().max(1)
                )))
            }
            Some((Outcome::Unknown, _)) => {
                return Err(WarmError::NotFound {
                    tenant: tenant.to_string(),
                    service: service.to_string(),
                })
            }
            None => {}
        }
        let (apps, recent) = (self.apps.clone(), self.recent.clone());
        let (tenant_owned, service_owned) = (tenant.to_string(), service.to_string());

        self.coalescer
            .run(key.clone(), move || {
                recorded(recent, key, async move {
                    do_warmup(&apps, &tenant_owned, &service_owned).await
                })
            })
            .await
            .unwrap_or_else(|| {
                Err(WarmError::WarmupFailed(
                    "warmup task ended without reporting a result".into(),
                ))
            })
    }
}

async fn do_warmup(apps: &Arc<Apps>, tenant: &str, service: &str) -> Result<(), WarmError> {
    let deployment = match apps.source.current(tenant, service).await {
        Ok(Some(deployment)) => deployment,
        Ok(None) => {
            return Err(WarmError::NotFound {
                tenant: tenant.to_string(),
                service: service.to_string(),
            })
        }
        Err(Unread::KeyRefused) => return Err(WarmError::KeyRefused),
        Err(Unread::Failed(e)) => return Err(e.wrap_err("lookup").into()),
    };
    match apps.run(&deployment).await? {
        Ran::AlreadyRunning => Ok(()),
        Ran::Started { port } => {
            info!(
                tenant = %tenant,
                service = %service,
                version = deployment.version,
                port = port,
                "Cold start: process ready"
            );
            Ok(())
        }
        Ran::Unhealthy { error, .. } => {
            warn!(
                tenant = %tenant,
                service = %service,
                error = %error,
                "Cold start: health check failed"
            );
            Err(WarmError::WarmupFailed(format!("health check: {error}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(service: &str) -> AppKey {
        ("acme".into(), service.into())
    }

    fn failed() -> Result<(), WarmError> {
        Err(WarmError::WarmupFailed("broken".into()))
    }

    fn unknown() -> Result<(), WarmError> {
        Err(WarmError::NotFound {
            tenant: "acme".into(),
            service: "ghost".into(),
        })
    }

    #[test]
    fn failed_and_unknown_cold_starts_are_remembered_for_their_wait() {
        let recent = Recent::default();
        let now = Instant::now();
        recent.record(key("web"), &failed(), now);
        recent.record(key("ghost"), &unknown(), now);
        let later = now + Duration::from_secs(5);
        assert_eq!(
            recent.holding(&key("web"), later),
            Some((Outcome::Failed, Duration::from_secs(25)))
        );
        assert_eq!(
            recent.holding(&key("ghost"), later),
            Some((Outcome::Unknown, Duration::from_secs(5)))
        );
        assert_eq!(recent.holding(&key("ghost"), now + UNKNOWN_APP_TTL), None);
        assert_eq!(recent.holding(&key("web"), now + COLD_START_BACKOFF), None);
        recent.record(key("web"), &Ok(()), later);
        assert_eq!(recent.holding(&key("web"), later), None);
    }

    #[test]
    fn a_refused_key_is_never_held_against_an_app() {
        let recent = Recent::default();
        let now = Instant::now();
        recent.record(key("web"), &Err(WarmError::KeyRefused), now);
        assert_eq!(
            recent.holding(&key("web"), now),
            None,
            "the first request once a key is accepted again starts it"
        );
    }

    #[test]
    fn a_flood_of_unknown_hosts_keeps_a_broken_app_backed_off() {
        let recent = Recent::default();
        let now = Instant::now();
        recent.record(key("web"), &failed(), now);
        for n in 0..MAX_REMEMBERED * 2 {
            recent.record(key(&format!("ghost-{n}")), &unknown(), now);
        }
        assert!(matches!(
            recent.holding(&key("web"), now),
            Some((Outcome::Failed, _))
        ));
        assert!(recent.entries().len() <= MAX_REMEMBERED);
    }

    #[tokio::test]
    async fn the_backoff_is_recorded_when_every_requester_has_gone() {
        let coalescer: Coalescer<AppKey, Result<(), WarmError>> = Coalescer::new();
        let recent = Recent::default();
        let requester = {
            let (coalescer, recent) = (coalescer.clone(), recent.clone());
            tokio::spawn(async move {
                coalescer
                    .run(key("web"), move || {
                        recorded(recent, key("web"), async {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            failed()
                        })
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        requester.abort();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(matches!(
            recent.holding(&key("web"), Instant::now()),
            Some((Outcome::Failed, _))
        ));
    }
}
