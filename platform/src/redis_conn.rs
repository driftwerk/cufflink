use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub const RESPONSE_TIMEOUT_ENV: &str = "REDIS_RESPONSE_TIMEOUT_MS";
pub const CONNECT_TIMEOUT_ENV: &str = "REDIS_CONNECT_TIMEOUT_MS";

const DEFAULT_RESPONSE_TIMEOUT_MS: u64 = 500;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 1_000;
const OUTER_TIMEOUT_MARGIN: Duration = Duration::from_millis(100);
const RECONNECT_RETRIES: usize = 3;
const RECONNECT_FACTOR_MS: u64 = 100;
const RECONNECT_MAX_DELAY_MS: u64 = 2_000;
const INITIAL_CONNECT_RETRY: Duration = Duration::from_millis(500);
const INITIAL_CONNECT_RETRY_MAX: Duration = Duration::from_secs(30);
const BREAKER_TRIP_AFTER: u32 = 3;
const BREAKER_COOL_DOWN: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisTimeouts {
    pub response: Duration,
    pub connect: Duration,
}

impl RedisTimeouts {
    pub fn from_env() -> Self {
        Self::from_values(
            std::env::var(RESPONSE_TIMEOUT_ENV).ok().as_deref(),
            std::env::var(CONNECT_TIMEOUT_ENV).ok().as_deref(),
        )
    }

    fn from_values(response: Option<&str>, connect: Option<&str>) -> Self {
        Self {
            response: parse_ms(response, DEFAULT_RESPONSE_TIMEOUT_MS),
            connect: parse_ms(connect, DEFAULT_CONNECT_TIMEOUT_MS),
        }
    }
}

fn parse_ms(raw: Option<&str>, default_ms: u64) -> Duration {
    let ms = raw
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(default_ms);
    Duration::from_millis(ms)
}

#[derive(Debug, thiserror::Error)]
pub enum RedisCallError {
    #[error("Redis not configured")]
    NotConfigured,
    #[error("Redis not connected")]
    Unavailable,
    #[error("Redis command timed out after {0:?}")]
    TimedOut(Duration),
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
}

impl RedisCallError {
    pub fn is_unavailable(&self) -> bool {
        match self {
            Self::NotConfigured | Self::Unavailable | Self::TimedOut(_) => true,
            Self::Redis(e) => {
                e.is_timeout()
                    || e.is_io_error()
                    || e.is_connection_dropped()
                    || e.is_connection_refusal()
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct RedisHandle {
    inner: Option<Arc<Inner>>,
}

struct Inner {
    client: redis::Client,
    conn: RwLock<Option<ConnectionManager>>,
    timeouts: RedisTimeouts,
    breaker: Mutex<Breaker>,
    connecting: AtomicBool,
}

impl RedisHandle {
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub fn connect_in_background(url: &str) -> eyre::Result<Self> {
        let client = redis::Client::open(url)?;
        let inner = Arc::new(Inner {
            client,
            conn: RwLock::new(None),
            timeouts: RedisTimeouts::from_env(),
            breaker: Mutex::new(Breaker::default()),
            connecting: AtomicBool::new(false),
        });
        spawn_connect(&inner);
        Ok(Self { inner: Some(inner) })
    }

    pub fn is_configured(&self) -> bool {
        self.inner.is_some()
    }

    pub fn is_connected(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.current().is_some())
    }

    pub async fn call<T, F, Fut>(&self, op: F) -> Result<T, RedisCallError>
    where
        F: FnOnce(ConnectionManager) -> Fut,
        Fut: Future<Output = redis::RedisResult<T>>,
    {
        let inner = self.inner.as_ref().ok_or(RedisCallError::NotConfigured)?;
        if inner.breaker_open() {
            return Err(RedisCallError::Unavailable);
        }
        let conn = inner.current().ok_or(RedisCallError::Unavailable)?;
        let response_timeout = inner.timeouts.response;
        let result =
            match tokio::time::timeout(response_timeout + OUTER_TIMEOUT_MARGIN, op(conn)).await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(e)) => Err(RedisCallError::Redis(e)),
                Err(_) => Err(RedisCallError::TimedOut(response_timeout)),
            };
        match &result {
            Err(e) if e.is_unavailable() => on_connectivity_failure(inner),
            _ => inner.record_success(),
        }
        result
    }

    pub fn status(&self) -> &'static str {
        if !self.is_configured() {
            "disabled"
        } else if self.is_connected() {
            "ok"
        } else {
            "unavailable"
        }
    }
}

impl Inner {
    fn current(&self) -> Option<ConnectionManager> {
        self.conn.read().ok().and_then(|conn| conn.clone())
    }

    fn breaker_open(&self) -> bool {
        self.breaker
            .lock()
            .map(|b| b.is_open(Instant::now()))
            .unwrap_or(false)
    }

    fn record_success(&self) {
        if let Ok(mut breaker) = self.breaker.lock() {
            breaker.record_success();
        }
    }
}

fn on_connectivity_failure(inner: &Arc<Inner>) {
    let tripped = inner
        .breaker
        .lock()
        .map(|mut b| b.record_failure(Instant::now()))
        .unwrap_or(false);
    if !tripped {
        return;
    }
    warn!(
        cool_down = ?BREAKER_COOL_DOWN,
        "Redis failing repeatedly; dropping the connection and reconnecting"
    );
    if let Ok(mut slot) = inner.conn.write() {
        *slot = None;
    }
    spawn_connect(inner);
}

fn spawn_connect(inner: &Arc<Inner>) {
    if inner
        .connecting
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        tokio::spawn(connect_until_ready(inner.clone()));
    }
}

#[derive(Debug, Default)]
struct Breaker {
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

impl Breaker {
    fn is_open(&self, now: Instant) -> bool {
        self.open_until.is_some_and(|until| now < until)
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    fn record_failure(&mut self, now: Instant) -> bool {
        if self.is_open(now) {
            return false;
        }
        self.consecutive_failures += 1;
        if self.consecutive_failures < BREAKER_TRIP_AFTER {
            return false;
        }
        self.consecutive_failures = 0;
        self.open_until = Some(now + BREAKER_COOL_DOWN);
        true
    }
}

fn manager_config(timeouts: RedisTimeouts) -> ConnectionManagerConfig {
    ConnectionManagerConfig::new()
        .set_response_timeout(timeouts.response)
        .set_connection_timeout(timeouts.connect)
        .set_number_of_retries(RECONNECT_RETRIES)
        .set_factor(RECONNECT_FACTOR_MS)
        .set_max_delay(RECONNECT_MAX_DELAY_MS)
}

async fn connect_until_ready(inner: Arc<Inner>) {
    let timeouts = inner.timeouts;
    let attempt_budget = timeouts.connect * (RECONNECT_RETRIES as u32 + 2);
    let mut retry_in = INITIAL_CONNECT_RETRY;
    loop {
        let attempt = tokio::time::timeout(
            attempt_budget,
            ConnectionManager::new_with_config(inner.client.clone(), manager_config(timeouts)),
        )
        .await;
        match attempt {
            Ok(Ok(conn)) => {
                inner.connecting.store(false, Ordering::SeqCst);
                if let Ok(mut slot) = inner.conn.write() {
                    *slot = Some(conn);
                }
                inner.record_success();
                info!("Connected to Redis");
                return;
            }
            Ok(Err(e)) => warn!(
                error = %e,
                retry_in = ?retry_in,
                "Redis unreachable; running without it and retrying in the background"
            ),
            Err(_) => warn!(
                retry_in = ?retry_in,
                "Redis connect attempt timed out; running without it and retrying in the background"
            ),
        }
        tokio::time::sleep(retry_in).await;
        retry_in = (retry_in * 2).min(INITIAL_CONNECT_RETRY_MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeouts_default_and_ignore_garbage() {
        let defaults = RedisTimeouts::from_values(None, Some("nope"));
        assert_eq!(defaults.response, Duration::from_millis(500));
        assert_eq!(defaults.connect, Duration::from_millis(1_000));

        let zero = RedisTimeouts::from_values(Some("0"), Some("0"));
        assert_eq!(zero, defaults);

        let custom = RedisTimeouts::from_values(Some("250"), Some(" 3000 "));
        assert_eq!(custom.response, Duration::from_millis(250));
        assert_eq!(custom.connect, Duration::from_millis(3_000));
    }

    #[test]
    fn unhealthy_errors_are_flagged() {
        assert!(RedisCallError::NotConfigured.is_unavailable());
        assert!(RedisCallError::Unavailable.is_unavailable());
        assert!(RedisCallError::TimedOut(Duration::from_millis(1)).is_unavailable());
        let protocol = redis::RedisError::from((redis::ErrorKind::TypeError, "bad type"));
        assert!(!RedisCallError::Redis(protocol).is_unavailable());
        let io = redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out",
        ));
        assert!(RedisCallError::Redis(io).is_unavailable());
    }

    #[test]
    fn breaker_trips_after_consecutive_failures_and_cools_down() {
        let start = Instant::now();
        let mut breaker = Breaker::default();
        assert!(!breaker.record_failure(start));
        assert!(!breaker.record_failure(start));
        assert!(breaker.record_failure(start), "third failure trips");
        assert!(breaker.is_open(start));
        assert!(breaker.is_open(start + BREAKER_COOL_DOWN - Duration::from_millis(1)));
        assert!(!breaker.is_open(start + BREAKER_COOL_DOWN));
        assert!(!breaker.record_failure(start + Duration::from_secs(1)));
    }

    #[test]
    fn success_resets_the_failure_count() {
        let now = Instant::now();
        let mut breaker = Breaker::default();
        breaker.record_failure(now);
        breaker.record_failure(now);
        breaker.record_success();
        assert!(!breaker.record_failure(now));
        assert!(!breaker.record_failure(now));
        assert!(breaker.record_failure(now));
    }

    #[tokio::test]
    async fn disabled_handle_fails_fast() {
        let handle = RedisHandle::disabled();
        assert!(!handle.is_configured());
        assert!(!handle.is_connected());
        let result = handle
            .call(
                |mut conn| async move { redis::cmd("PING").query_async::<String>(&mut conn).await },
            )
            .await;
        assert!(matches!(result, Err(RedisCallError::NotConfigured)));
        assert_eq!(handle.status(), "disabled");
    }

    #[tokio::test]
    async fn unreachable_redis_starts_disconnected_and_calls_fail_fast() {
        let handle = RedisHandle::connect_in_background("redis://127.0.0.1:1").unwrap();
        assert!(handle.is_configured());
        let started = Instant::now();
        let result = handle
            .call(
                |mut conn| async move { redis::cmd("PING").query_async::<String>(&mut conn).await },
            )
            .await;
        assert!(matches!(result, Err(RedisCallError::Unavailable)));
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(handle.status(), "unavailable");
    }
}
