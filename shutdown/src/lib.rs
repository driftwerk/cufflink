use std::fmt;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn, Instrument};

pub const KILL_MARGIN: Duration = Duration::from_secs(2);
pub const DEPENDENCY_FLUSH_CAP: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct Shutdown {
    token: CancellationToken,
    tracker: TaskTracker,
    triggered_at: Arc<OnceLock<Instant>>,
    grace: Duration,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    Completed,
    TimedOut { still_running: usize },
}

impl DrainOutcome {
    pub fn merge(self, other: DrainOutcome) -> DrainOutcome {
        match (self, other) {
            (DrainOutcome::Completed, DrainOutcome::Completed) => DrainOutcome::Completed,
            (a, b) => DrainOutcome::TimedOut {
                still_running: a.still_running() + b.still_running(),
            },
        }
    }

    fn still_running(&self) -> usize {
        match self {
            DrainOutcome::Completed => 0,
            DrainOutcome::TimedOut { still_running } => *still_running,
        }
    }
}

impl Shutdown {
    pub fn new(grace: Duration) -> Self {
        Self {
            token: CancellationToken::new(),
            tracker: TaskTracker::new(),
            triggered_at: Arc::new(OnceLock::new()),
            grace,
        }
    }

    pub fn trigger(&self) {
        self.triggered_at.set(Instant::now()).ok();
        self.token.cancel();
    }

    pub fn is_triggered(&self) -> bool {
        self.triggered_at.get().is_some()
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn cancelled(&self) -> WaitForCancellationFutureOwned {
        self.token.clone().cancelled_owned()
    }

    pub fn remaining(&self) -> Duration {
        self.triggered_at
            .get()
            .map_or(self.grace, |at| self.grace.saturating_sub(at.elapsed()))
    }

    pub fn hard_deadline_reached(&self) -> bool {
        self.is_triggered() && self.remaining() <= KILL_MARGIN
    }

    pub fn spawn<F>(&self, name: &'static str, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tracker
            .spawn(fut.instrument(tracing::info_span!("task", task = name)))
    }

    pub async fn drain_tasks(&self) -> DrainOutcome {
        self.tracker.close();
        match tokio::time::timeout(self.remaining(), self.tracker.wait()).await {
            Ok(()) => DrainOutcome::Completed,
            Err(_) => DrainOutcome::TimedOut {
                still_running: self.tracker.len(),
            },
        }
    }
}

pub async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => return,
                    _ = ctrl_c() => return,
                }
            }
            Err(e) => warn!(error = %e, "SIGTERM handler not installed; listening for ctrl-c only"),
        }
    }
    ctrl_c().await
}

async fn ctrl_c() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!(error = %e, "ctrl-c handler not installed; the process will not stop on a signal");
        std::future::pending::<()>().await
    }
}

pub async fn phase<F: Future<Output = T>, T>(
    shutdown: &Shutdown,
    name: &'static str,
    fut: F,
) -> Option<T> {
    phase_within(shutdown, name, Duration::MAX, fut).await
}

pub async fn phase_within<F: Future<Output = T>, T>(
    shutdown: &Shutdown,
    name: &'static str,
    cap: Duration,
    fut: F,
) -> Option<T> {
    let bound = shutdown.remaining().min(cap);
    let started = Instant::now();
    let outcome = tokio::time::timeout(bound, fut).await.ok();
    let elapsed = started.elapsed();
    metrics::histogram!("cufflink_shutdown_phase_seconds", "phase" => name)
        .record(elapsed.as_secs_f64());
    if outcome.is_some() {
        info!(phase = name, ?elapsed, "Shutdown phase complete");
    } else {
        warn!(phase = name, ?elapsed, ?bound, "Shutdown phase timed out");
    }
    outcome
}

pub fn drained(tasks: Option<DrainOutcome>) -> bool {
    match tasks {
        Some(DrainOutcome::Completed) => true,
        Some(DrainOutcome::TimedOut { still_running }) => {
            warn!(
                still_running,
                "Background tasks did not finish within the grace period"
            );
            false
        }
        None => false,
    }
}

pub async fn finish<N, E>(
    shutdown: &Shutdown,
    work: &[(&'static str, bool)],
    nats: N,
) -> eyre::Result<()>
where
    N: Future<Output = Result<(), E>>,
    E: fmt::Display,
{
    match phase_within(shutdown, "nats", DEPENDENCY_FLUSH_CAP, nats).await {
        Some(Err(e)) => warn!(error = %e, "NATS drain failed"),
        None => warn!("NATS did not flush within its cap"),
        Some(Ok(())) => {}
    }
    phase_within(
        shutdown,
        "telemetry",
        DEPENDENCY_FLUSH_CAP,
        cufflink_telemetry::shutdown(DEPENDENCY_FLUSH_CAP),
    )
    .await;
    let overran: Vec<&str> = work
        .iter()
        .filter(|(_, completed)| !completed)
        .map(|(name, _)| *name)
        .collect();
    if overran.is_empty() {
        info!("Shutdown complete");
        Ok(())
    } else {
        Err(eyre::eyre!(
            "shutdown overran the grace period in phases: {}",
            overran.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_is_the_full_grace_until_triggered() {
        let shutdown = Shutdown::new(Duration::from_secs(45));
        assert_eq!(shutdown.remaining(), Duration::from_secs(45));
        assert!(!shutdown.is_triggered());
        assert!(!shutdown.hard_deadline_reached());
        shutdown.trigger();
        assert!(shutdown.is_triggered());
        assert!(shutdown.remaining() <= Duration::from_secs(45));
        assert!(shutdown.remaining() > Duration::from_secs(44));
    }

    #[test]
    fn trigger_is_idempotent() {
        let shutdown = Shutdown::new(Duration::from_secs(10));
        shutdown.trigger();
        let first = shutdown.remaining();
        std::thread::sleep(Duration::from_millis(20));
        shutdown.trigger();
        assert!(
            shutdown.remaining() < first,
            "a second trigger must not reset the clock"
        );
        assert!(shutdown.token().is_cancelled());
    }

    #[tokio::test]
    async fn hard_deadline_is_reached_at_grace_minus_margin() {
        let shutdown = Shutdown::new(KILL_MARGIN + Duration::from_millis(60));
        shutdown.trigger();
        assert!(!shutdown.hard_deadline_reached());
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(shutdown.hard_deadline_reached());
    }

    #[tokio::test]
    async fn drain_completes_for_tasks_that_honour_the_token() {
        let shutdown = Shutdown::new(Duration::from_secs(5));
        let token = shutdown.token();
        shutdown.spawn("polite", async move { token.cancelled().await });
        shutdown.trigger();
        assert_eq!(shutdown.drain_tasks().await, DrainOutcome::Completed);
    }

    #[tokio::test]
    async fn drain_times_out_on_a_task_that_ignores_the_token() {
        let shutdown = Shutdown::new(Duration::from_millis(100));
        shutdown.spawn("stubborn", std::future::pending::<()>());
        shutdown.trigger();
        assert_eq!(
            shutdown.drain_tasks().await,
            DrainOutcome::TimedOut { still_running: 1 }
        );
    }

    #[test]
    fn drain_outcomes_merge_by_adding_stragglers() {
        assert_eq!(
            DrainOutcome::Completed.merge(DrainOutcome::Completed),
            DrainOutcome::Completed
        );
        assert_eq!(
            DrainOutcome::TimedOut { still_running: 2 }.merge(DrainOutcome::Completed),
            DrainOutcome::TimedOut { still_running: 2 }
        );
        assert_eq!(
            DrainOutcome::TimedOut { still_running: 2 }
                .merge(DrainOutcome::TimedOut { still_running: 3 }),
            DrainOutcome::TimedOut { still_running: 5 }
        );
    }

    #[tokio::test]
    async fn spawned_task_output_is_returned() {
        let shutdown = Shutdown::new(Duration::from_secs(1));
        let handle = shutdown.spawn("answer", async { 42 });
        assert_eq!(handle.await.ok(), Some(42));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn phase_bounds_a_never_ending_future_and_records_a_metric() {
        let recorder = cufflink_telemetry::metrics_builder()
            .expect("builder")
            .build_recorder();
        let handle = recorder.handle();
        let shutdown = Shutdown::new(Duration::from_millis(100));
        shutdown.trigger();
        let runtime = tokio::runtime::Handle::current();
        let outcome = metrics::with_local_recorder(&recorder, || {
            tokio::task::block_in_place(|| {
                runtime.block_on(async {
                    let never = phase(&shutdown, "never", std::future::pending::<()>()).await;
                    let done = phase(&shutdown, "done", async { 7 }).await;
                    (never, done)
                })
            })
        });
        assert_eq!(outcome, (None, Some(7)));
        let rendered = handle.render();
        assert!(
            rendered.contains("cufflink_shutdown_phase_seconds")
                && rendered.contains("phase=\"never\""),
            "phase metric missing: {rendered}"
        );
    }

    #[test]
    fn only_a_completed_drain_counts_as_drained() {
        assert!(drained(Some(DrainOutcome::Completed)));
        assert!(!drained(Some(DrainOutcome::TimedOut { still_running: 1 })));
        assert!(!drained(None));
    }

    #[tokio::test]
    async fn finish_fails_only_on_overrun_work_phases() {
        let shutdown = Shutdown::new(Duration::from_millis(200));
        shutdown.trigger();
        let flushed = async { Ok::<(), &str>(()) };
        assert!(
            finish(&shutdown, &[("http", true), ("tasks", true)], flushed)
                .await
                .is_ok()
        );
        let wedged = std::future::pending::<Result<(), &str>>();
        assert!(finish(&shutdown, &[("tasks", true)], wedged).await.is_ok());
        let failed = async { Err::<(), _>("connection lost") };
        let err = finish(&shutdown, &[("http", false), ("tasks", false)], failed)
            .await
            .expect_err("overran work phases");
        assert_eq!(
            err.to_string(),
            "shutdown overran the grace period in phases: http, tasks"
        );
    }

    #[tokio::test]
    async fn phase_within_caps_below_the_remaining_grace() {
        let shutdown = Shutdown::new(Duration::from_secs(30));
        let started = Instant::now();
        let outcome = phase_within(
            &shutdown,
            "capped",
            Duration::from_millis(50),
            std::future::pending::<()>(),
        )
        .await;
        assert_eq!(outcome, None);
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
