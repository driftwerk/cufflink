use crate::shutdown::Shutdown;
use async_nats::{Client, Event, HeaderMap};
use axum::body::Bytes;
use futures_util::{FutureExt, Stream, StreamExt};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::Notify;

pub const PUBLISH_TIMEOUT_ENV: &str = "NATS_PUBLISH_TIMEOUT_MS";
pub const STREAM_REPLICAS_ENV: &str = "NATS_STREAM_REPLICAS";

pub const FETCH_EXPIRES: Duration = Duration::from_secs(30);
pub const FETCH_STALL_AFTER: Duration = Duration::from_secs(33);
const FETCH_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

const DEFAULT_PUBLISH_TIMEOUT_MS: u64 = 2_000;
const MAX_STREAM_REPLICAS: usize = 5;
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

pub use cufflink_nats::{connect, parse_server_list, redact_urls, NatsAuth};

pub async fn connect_with_reconnect_signal(
    raw_urls: &str,
    client_name: &str,
    auth: &NatsAuth,
) -> eyre::Result<(Client, Arc<Notify>)> {
    let servers = parse_server_list(raw_urls)?;
    let connected = Arc::new(Notify::new());
    let signal = connected.clone();
    let client = cufflink_nats::connect_options(client_name, auth)
        .await?
        .event_callback(move |event| {
            let signal = signal.clone();
            async move {
                if matches!(event, Event::Connected) {
                    signal.notify_one();
                }
            }
        })
        .connect(servers)
        .await?;
    Ok((client, connected))
}

pub fn is_connected(client: &Client) -> bool {
    client.connection_state() == async_nats::connection::State::Connected
}

pub fn publish_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT
        .get_or_init(|| publish_timeout_from(std::env::var(PUBLISH_TIMEOUT_ENV).ok().as_deref()))
}

fn publish_timeout_from(raw: Option<&str>) -> Duration {
    let ms = raw
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(DEFAULT_PUBLISH_TIMEOUT_MS);
    Duration::from_millis(ms)
}

pub fn stream_replicas() -> usize {
    static REPLICAS: OnceLock<usize> = OnceLock::new();
    *REPLICAS
        .get_or_init(|| stream_replicas_from(std::env::var(STREAM_REPLICAS_ENV).ok().as_deref()))
}

fn stream_replicas_from(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .map(|n| n.min(MAX_STREAM_REPLICAS))
        .unwrap_or(1)
}

pub fn effective_replicas(target: usize, existing: Option<usize>) -> usize {
    target.max(existing.unwrap_or(0)).max(1)
}

pub fn needs_replica_upgrade(existing: usize, target: usize) -> bool {
    target > existing.max(1)
}

#[derive(Debug)]
pub enum PublishError {
    TimedOut(Duration),
    Failed(String),
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut(after) => {
                write!(f, "publish timed out after {:?} (NATS unavailable?)", after)
            }
            Self::Failed(e) => write!(f, "publish failed: {}", e),
        }
    }
}

impl std::error::Error for PublishError {}

pub async fn publish_bounded(
    client: &Client,
    subject: String,
    headers: Option<HeaderMap>,
    payload: Bytes,
) -> Result<(), PublishError> {
    let timeout = publish_timeout();
    let send = async {
        match headers {
            Some(headers) => client.publish_with_headers(subject, headers, payload).await,
            None => client.publish(subject, payload).await,
        }
    };
    match tokio::time::timeout(timeout, send).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(count_publish_failure(
            "core",
            PublishError::Failed(e.to_string()),
        )),
        Err(_) => Err(count_publish_failure(
            "core",
            PublishError::TimedOut(timeout),
        )),
    }
}

fn count_publish_failure(kind: &'static str, error: PublishError) -> PublishError {
    let reason = match error {
        PublishError::TimedOut(_) => "timeout",
        PublishError::Failed(_) => "error",
    };
    metrics::counter!(
        "cufflink_nats_publish_failed_total",
        "kind" => kind,
        "reason" => reason,
    )
    .increment(1);
    error
}

pub fn spawn_publish(
    client: Client,
    subject: String,
    headers: Option<HeaderMap>,
    payload: Bytes,
    what: &'static str,
) {
    tokio::spawn(async move {
        if let Err(e) = publish_bounded(&client, subject.clone(), headers, payload).await {
            tracing::warn!(subject = %subject, error = %e, "Dropped {}", what);
        }
    });
}

pub async fn jetstream_publish_bounded(
    js: &async_nats::jetstream::Context,
    subject: String,
    headers: Option<HeaderMap>,
    payload: Bytes,
) -> Result<(), PublishError> {
    let timeout = publish_timeout();
    let send = async {
        match headers {
            Some(headers) => js.publish_with_headers(subject, headers, payload).await,
            None => js.publish(subject, payload).await,
        }
    };
    let ack = match tokio::time::timeout(timeout, send).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(e)) => {
            return Err(count_publish_failure(
                "jetstream",
                PublishError::Failed(e.to_string()),
            ))
        }
        Err(_) => {
            return Err(count_publish_failure(
                "jetstream",
                PublishError::TimedOut(timeout),
            ))
        }
    };
    ack.await.map(|_| ()).map_err(|e| {
        count_publish_failure("jetstream", PublishError::Failed(format!("ack: {}", e)))
    })
}

pub async fn flush_and_drain(nats: &Client) -> Result<(), async_nats::client::DrainError> {
    if is_connected(nats) {
        if let Err(e) = nats.flush().await {
            tracing::warn!(error = %e, "NATS flush before drain failed");
        }
    }
    nats.drain().await
}

pub const NAK_DRAIN_BUDGET: Duration = Duration::from_secs(1);

pub trait HandBack: Send {
    fn hand_back(&self) -> impl Future<Output = bool> + Send;
}

impl HandBack for async_nats::jetstream::Message {
    async fn hand_back(&self) -> bool {
        self.ack_with(async_nats::jetstream::AckKind::Nak(None))
            .await
            .inspect_err(|e| tracing::warn!(error = %e, "Failed to nak a message on shutdown"))
            .is_ok()
    }
}

pub async fn nak_rest<M, S, E>(first: M, batch: &mut S) -> usize
where
    M: HandBack,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + NAK_DRAIN_BUDGET;
    usize::from(first.hand_back().await) + nak_arrivals(batch, deadline).await
}

pub async fn nak_pending<M, S, E>(batch: &mut S) -> usize
where
    M: HandBack,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    nak_arrivals(batch, tokio::time::Instant::now() + NAK_DRAIN_BUDGET).await
}

async fn nak_arrivals<M, S, E>(batch: &mut S, deadline: tokio::time::Instant) -> usize
where
    M: HandBack,
    S: Stream<Item = Result<M, E>> + Unpin,
{
    let mut naked = 0;
    while let Ok(Some(Ok(msg))) = tokio::time::timeout_at(deadline, batch.next()).await {
        naked += usize::from(msg.hand_back().await);
    }
    naked
}

#[derive(Debug)]
pub enum BatchStep<M = async_nats::jetstream::Message> {
    Message(Box<M>),
    HandedBack(usize),
    End,
    Failed(String),
}

pub async fn next_or_hand_back<M, S, E>(batch: &mut S, shutdown: &Shutdown) -> BatchStep<M>
where
    M: HandBack,
    S: Stream<Item = Result<M, E>> + Unpin,
    E: fmt::Display,
{
    let next = if shutdown.is_triggered() {
        None
    } else {
        tokio::select! {
            biased;
            next = next_in_batch(batch) => Some(next),
            _ = shutdown.cancelled() => None,
        }
    };
    match next {
        None => BatchStep::HandedBack(nak_pending(batch).await),
        Some(Ok(Some(Ok(msg)))) if shutdown.is_triggered() => {
            BatchStep::HandedBack(nak_rest(msg, batch).await)
        }
        Some(Ok(Some(Ok(msg)))) => BatchStep::Message(Box::new(msg)),
        Some(Ok(Some(Err(e)))) => BatchStep::Failed(e.to_string()),
        Some(Ok(None)) => BatchStep::End,
        Some(Err(e)) => BatchStep::Failed(e.to_string()),
    }
}

#[derive(Debug)]
pub struct FetchError(pub String);

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FetchError {}

pub async fn start_fetch<F, T, E>(request: F) -> Result<T, FetchError>
where
    F: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    match tokio::time::timeout(FETCH_REQUEST_TIMEOUT, request).await {
        Ok(Ok(batch)) => Ok(batch),
        Ok(Err(e)) => Err(FetchError(e.to_string())),
        Err(_) => Err(FetchError(format!(
            "fetch request timed out after {:?}",
            FETCH_REQUEST_TIMEOUT
        ))),
    }
}

pub async fn next_in_batch<S>(batch: &mut S) -> Result<Option<S::Item>, FetchError>
where
    S: Stream + Unpin,
{
    next_within(batch, FETCH_STALL_AFTER).await
}

async fn next_within<S>(batch: &mut S, stall_after: Duration) -> Result<Option<S::Item>, FetchError>
where
    S: Stream + Unpin,
{
    tokio::time::timeout(stall_after, batch.next())
        .await
        .map_err(|_| {
            FetchError(format!(
                "fetch batch stalled: no message or terminal status for {:?} (NATS server gone?)",
                stall_after
            ))
        })
}

#[derive(Debug)]
pub struct Backoff {
    next: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

impl Backoff {
    pub fn new() -> Self {
        Self {
            next: BACKOFF_INITIAL,
        }
    }

    pub fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = (self.next * 2).min(BACKOFF_MAX);
        delay
    }

    pub fn reset(&mut self) {
        self.next = BACKOFF_INITIAL;
    }
}

pub async fn publish_change(client: &Client, subject: &'static str) {
    if let Err(e) = publish_bounded(client, subject.to_string(), None, Bytes::new()).await {
        tracing::warn!(subject, error = %e, "Failed to publish change notification");
    }
}

fn drain_ready<S: Stream + Unpin>(notifications: &mut S) -> usize {
    std::iter::from_fn(|| notifications.next().now_or_never().flatten()).count()
}

pub fn spawn_reload_loop<F, Fut>(
    shutdown: &Shutdown,
    task: &'static str,
    nats: Client,
    subject: &'static str,
    interval: Duration,
    reload: F,
) where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let subscribe = move || {
        let nats = nats.clone();
        async move { nats.subscribe(subject).await }
    };
    shutdown.spawn(
        task,
        reload_loop(
            shutdown.token(),
            ReloadSource {
                task,
                subject,
                interval,
            },
            subscribe,
            reload,
        ),
    );
}

struct ReloadSource {
    task: &'static str,
    subject: &'static str,
    interval: Duration,
}

enum Wake {
    Notified,
    SubscriptionEnded,
    Resubscribe,
    Tick,
}

async fn reload_loop<S, E, Sub, SubFut, F, Fut>(
    token: tokio_util::sync::CancellationToken,
    source: ReloadSource,
    mut subscribe: Sub,
    mut reload: F,
) where
    S: Stream + Unpin,
    E: fmt::Display,
    Sub: FnMut() -> SubFut,
    SubFut: Future<Output = Result<S, E>>,
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let ReloadSource {
        task,
        subject,
        interval,
    } = source;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut backoff = Backoff::new();
    let mut subscriber: Option<S> = None;
    let mut resubscribe_at = tokio::time::Instant::now();
    loop {
        if subscriber.is_none() && tokio::time::Instant::now() >= resubscribe_at {
            match subscribe().await {
                Ok(s) => {
                    tracing::info!(task, subject, "Change listener started");
                    backoff.reset();
                    subscriber = Some(s);
                }
                Err(e) => {
                    let retry_in = backoff.next_delay();
                    tracing::warn!(task, subject, error = %e, ?retry_in, "Change subscription failed; retrying");
                    resubscribe_at = tokio::time::Instant::now() + retry_in;
                }
            }
        }
        let listening = subscriber.is_some();
        let wake = tokio::select! {
            _ = token.cancelled() => return,
            msg = async { subscriber.as_mut()?.next().await }, if listening => match msg {
                Some(_) => Wake::Notified,
                None => Wake::SubscriptionEnded,
            },
            _ = tokio::time::sleep_until(resubscribe_at), if !listening => Wake::Resubscribe,
            _ = ticker.tick() => Wake::Tick,
        };
        match wake {
            Wake::Resubscribe => continue,
            Wake::SubscriptionEnded => {
                tracing::warn!(task, subject, "Change subscription ended; resubscribing");
                subscriber = None;
                resubscribe_at = tokio::time::Instant::now();
                continue;
            }
            Wake::Notified => tracing::info!(task, "Change notification received, reloading"),
            Wake::Tick => {}
        }
        if let Some(subscriber) = subscriber.as_mut() {
            drain_ready(subscriber);
        }
        reload().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_reload_timer_runs_while_subscribing_keeps_failing() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let (attempts, reloads) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let token = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(reload_loop(
            token.clone(),
            ReloadSource {
                task: "test",
                subject: "test.changed",
                interval: Duration::from_millis(50),
            },
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Err::<futures_util::stream::Pending<()>, _>("no NATS") }
                }
            },
            {
                let reloads = Arc::clone(&reloads);
                move || {
                    reloads.fetch_add(1, Ordering::SeqCst);
                    async {}
                }
            },
        ));
        tokio::time::sleep(Duration::from_millis(400)).await;
        token.cancel();
        task.await.expect("loop ends on cancel");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "resubscribing backs off"
        );
        assert!(
            reloads.load(Ordering::SeqCst) >= 3,
            "the timer keeps reloading: {}",
            reloads.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn publish_timeout_defaults_and_ignores_garbage() {
        assert_eq!(publish_timeout_from(None), Duration::from_millis(2_000));
        assert_eq!(
            publish_timeout_from(Some("abc")),
            Duration::from_millis(2_000)
        );
        assert_eq!(
            publish_timeout_from(Some("0")),
            Duration::from_millis(2_000)
        );
        assert_eq!(
            publish_timeout_from(Some(" 750 ")),
            Duration::from_millis(750)
        );
    }

    #[test]
    fn stream_replicas_default_to_one_and_cap_at_five() {
        assert_eq!(stream_replicas_from(None), 1);
        assert_eq!(stream_replicas_from(Some("0")), 1);
        assert_eq!(stream_replicas_from(Some("x")), 1);
        assert_eq!(stream_replicas_from(Some("3")), 3);
        assert_eq!(stream_replicas_from(Some("9")), 5);
    }

    #[test]
    fn effective_replicas_never_downscales() {
        assert_eq!(effective_replicas(3, None), 3);
        assert_eq!(effective_replicas(3, Some(1)), 3);
        assert_eq!(effective_replicas(1, Some(3)), 3);
        assert_eq!(effective_replicas(1, Some(0)), 1);
    }

    #[test]
    fn replica_upgrade_only_when_target_exceeds_existing() {
        assert!(needs_replica_upgrade(1, 3));
        assert!(needs_replica_upgrade(0, 3));
        assert!(!needs_replica_upgrade(3, 3));
        assert!(!needs_replica_upgrade(3, 1));
        assert!(!needs_replica_upgrade(0, 1));
    }

    #[test]
    fn fetch_stall_fires_before_async_nats_quietly_ends_the_batch() {
        let async_nats_batch_end = FETCH_EXPIRES + Duration::from_secs(5);
        assert!(FETCH_STALL_AFTER > FETCH_EXPIRES);
        assert!(FETCH_STALL_AFTER < async_nats_batch_end);
    }

    type Batch = futures_util::stream::BoxStream<
        'static,
        Result<async_nats::jetstream::Message, &'static str>,
    >;

    #[tokio::test]
    async fn a_batch_ends_or_fails_as_the_stream_says() {
        let shutdown = Shutdown::new(Duration::from_secs(5));
        let mut ended: Batch = futures_util::stream::empty().boxed();
        assert!(matches!(
            next_or_hand_back(&mut ended, &shutdown).await,
            BatchStep::End
        ));
        let mut failing: Batch = futures_util::stream::iter([Err("consumer deleted")]).boxed();
        assert!(matches!(
            next_or_hand_back(&mut failing, &shutdown).await,
            BatchStep::Failed(e) if e == "consumer deleted"
        ));
    }

    #[tokio::test]
    async fn a_trigger_stops_waiting_on_the_batch() {
        let shutdown = Shutdown::new(Duration::from_secs(5));
        let trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.trigger();
        });
        let started = tokio::time::Instant::now();
        let mut silent: Batch = futures_util::stream::pending().boxed();
        assert!(matches!(
            next_or_hand_back(&mut silent, &shutdown).await,
            BatchStep::HandedBack(0)
        ));
        assert!(started.elapsed() < FETCH_EXPIRES);
        assert!(matches!(
            next_or_hand_back(&mut silent, &shutdown).await,
            BatchStep::HandedBack(0)
        ));
    }

    #[tokio::test]
    async fn a_batch_that_never_yields_is_reported_as_stalled() {
        let mut silent = futures_util::stream::pending::<u8>();
        let result = next_within(&mut silent, Duration::from_millis(20)).await;
        assert!(result.is_err());

        let mut finished = futures_util::stream::iter(vec![1u8]);
        assert_eq!(
            next_within(&mut finished, Duration::from_millis(20))
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            next_within(&mut finished, Duration::from_millis(20))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn fetch_request_errors_are_reported() {
        let failed = start_fetch(async { Err::<(), _>("consumer not found") }).await;
        assert_eq!(failed.unwrap_err().to_string(), "consumer not found");
        assert!(start_fetch(async { Ok::<_, String>(5) }).await.is_ok());
    }

    #[test]
    fn queued_notifications_are_drained_at_once() {
        let mut burst =
            futures_util::stream::iter([(), (), ()]).chain(futures_util::stream::pending());
        assert_eq!(drain_ready(&mut burst), 3);
        assert_eq!(drain_ready(&mut burst), 0);
        let mut ended = futures_util::stream::iter([()]);
        assert_eq!(drain_ready(&mut ended), 1);
        assert_eq!(drain_ready(&mut ended), 0);
    }

    #[test]
    fn backoff_doubles_to_a_cap_and_resets() {
        let mut backoff = Backoff::new();
        let delays: Vec<u64> = (0..7).map(|_| backoff.next_delay().as_secs()).collect();
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30]);
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }
}
