use crate::nats_conn::Backoff;
use crate::shutdown::Shutdown;
use crate::AppState;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::warn;

const RECONNECT_DEBOUNCE: Duration = Duration::from_secs(2);

type StreamSetup = for<'a> fn(&'a AppState) -> BoxFuture<'a, eyre::Result<()>>;

struct StreamSet {
    task: &'static str,
    ensuring: AtomicBool,
    streams: &'static [(&'static str, StreamSetup)],
}

fn backup_stream(state: &AppState) -> BoxFuture<'_, eyre::Result<()>> {
    super::events::setup_backup_stream(&state.jetstream).boxed()
}

fn long_jobs_stream(state: &AppState) -> BoxFuture<'_, eyre::Result<()>> {
    super::events::setup_long_jobs_stream(&state.jetstream).boxed()
}

fn schedules_stream(state: &AppState) -> BoxFuture<'_, eyre::Result<()>> {
    super::events::setup_schedules_stream(&state.jetstream).boxed()
}

fn tick_stream(state: &AppState) -> BoxFuture<'_, eyre::Result<()>> {
    super::tick::setup_stream(&state.jetstream).boxed()
}

fn dead_letter_stream(state: &AppState) -> BoxFuture<'_, eyre::Result<()>> {
    super::events::setup_dlq_stream(&state.jetstream, state.config.dlq_max_age).boxed()
}

fn audit_stream(state: &AppState) -> BoxFuture<'_, eyre::Result<()>> {
    super::events::setup_audit_stream(&state.jetstream, &state.config.audit).boxed()
}

fn webhooks_stream(state: &AppState) -> BoxFuture<'_, eyre::Result<()>> {
    super::events::webhooks_stream(&state.jetstream)
        .map(|made| made.map(drop))
        .boxed()
}

static PLATFORM_STREAMS: StreamSet = StreamSet {
    task: "platform_streams",
    ensuring: AtomicBool::new(false),
    streams: &[
        (super::events::BACKUP_STREAM, backup_stream),
        (super::events::LONG_JOBS_STREAM, long_jobs_stream),
        (super::events::SCHEDULES_STREAM, schedules_stream),
        ("CUFFLINK_TICKS", tick_stream),
        (super::dlq::DLQ_STREAM, dead_letter_stream),
    ],
};

static DEAD_LETTER_STREAM: StreamSet = StreamSet {
    task: "dead_letter_stream",
    ensuring: AtomicBool::new(false),
    streams: &[(super::dlq::DLQ_STREAM, dead_letter_stream)],
};

static OUTBOX_STREAMS: StreamSet = StreamSet {
    task: "outbox_streams",
    ensuring: AtomicBool::new(false),
    streams: &[
        (crate::outbox::route::AUDIT_STREAM, audit_stream),
        (crate::outbox::route::WEBHOOKS_STREAM, webhooks_stream),
    ],
};

static AUDIT_STREAM: StreamSet = StreamSet {
    task: "audit_stream",
    ensuring: AtomicBool::new(false),
    streams: &[(crate::outbox::route::AUDIT_STREAM, audit_stream)],
};

pub fn spawn_ensure_outbox_streams(state: &Arc<AppState>) {
    spawn_ensure(state, &OUTBOX_STREAMS);
}

pub async fn ensure_outbox_streams(state: &AppState) -> eyre::Result<()> {
    for (_, setup) in OUTBOX_STREAMS.streams {
        setup(state).await?;
    }
    Ok(())
}

fn spawn_ensure_audit_stream(state: &Arc<AppState>) {
    if state.config.audit.durable_all {
        spawn_ensure(state, &AUDIT_STREAM);
    }
}

pub fn spawn_ensure_dead_letter_stream(state: &Arc<AppState>) {
    spawn_ensure(state, &DEAD_LETTER_STREAM);
}

pub fn spawn(state: Arc<AppState>, reconnected: Arc<Notify>) {
    spawn_ensure(&state, &PLATFORM_STREAMS);
    state.reconciler.start(state.clone());
    crate::schedules::scheduler::start(state.clone());
    state
        .stream_index
        .start_sync(&state.shutdown, &state.nats, &state.jetstream);
    spawn_ensure_audit_stream(&state);
    state.shutdown.spawn("nats_reconnect", {
        let state = state.clone();
        async move {
            let shutdown = state.shutdown.clone();
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = reconnected.notified() => {}
                }
                if !sleep_unless_cancelled(&shutdown, RECONNECT_DEBOUNCE).await {
                    return;
                }
                state.reconciler.trigger();
                spawn_ensure(&state, &PLATFORM_STREAMS);
                spawn_ensure_audit_stream(&state);
            }
        }
    });
}

async fn unless_cancelled<T>(shutdown: &Shutdown, fut: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        _ = shutdown.cancelled() => None,
        value = fut => Some(value),
    }
}

async fn sleep_unless_cancelled(shutdown: &Shutdown, delay: Duration) -> bool {
    unless_cancelled(shutdown, tokio::time::sleep(delay))
        .await
        .is_some()
}

fn spawn_ensure(state: &Arc<AppState>, set: &'static StreamSet) {
    if set
        .ensuring
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    let state = state.clone();
    state.shutdown.clone().spawn(set.task, async move {
        let _running = Ensuring(set);
        ensure_streams(&state, set).await;
    });
}

struct Ensuring(&'static StreamSet);

impl Drop for Ensuring {
    fn drop(&mut self) {
        self.0.ensuring.store(false, Ordering::SeqCst);
    }
}

async fn ensure_streams(state: &Arc<AppState>, set: &StreamSet) {
    let mut backoff = Backoff::new();
    loop {
        let Some(missing) = unless_cancelled(&state.shutdown, async {
            let mut missing = 0usize;
            for (name, setup) in set.streams {
                if let Err(e) = setup(state).await {
                    missing += 1;
                    metrics::counter!("cufflink_platform_stream_unavailable_total", "stream" => *name)
                        .increment(1);
                    warn!(stream = name, error = %e, "JetStream platform stream not ready; retrying");
                }
            }
            missing
        })
        .await
        else {
            return;
        };
        if missing == 0 || !sleep_unless_cancelled(&state.shutdown, backoff.next_delay()).await {
            return;
        }
    }
}
