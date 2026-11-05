//! The parser processes behind `sql_parse` (`docs/design-notes/sql-parse.md`).
//!
//! PostgreSQL's parser is C code, and a fault in it ends the process it
//! runs in, so it runs in `cufflink-sql-parser` processes, never here. A
//! process that faults, stops answering or answers out of turn costs its
//! call (`Unavailable`) and is replaced.

use cufflink_db::positive_var;
use cufflink_sql_parse::bounds;
use cufflink_sql_parse::wire::{self, Answer, Hello, Request, MAX_CONTROL_FRAME};
use cufflink_sql_parse::{PARSER, PROBE_FINGERPRINT};
use cufflink_types::sql_parse::ParseError;
use eyre::{eyre, Result};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

const BINARY: &str = "cufflink-sql-parser";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_TENANT_COOLDOWN_MS: u64 = 60_000;
/// Processes a tenant's parses may lose within its cooldown before its
/// parses are refused for that long.
const LOSSES_BEFORE_COOLDOWN: usize = 3;
const REAP_EVERY: Duration = Duration::from_secs(1);

/// The least of an invocation's budget a parse is started with.
pub const MIN_PARSE_BUDGET: Duration = Duration::from_millis(100);

/// Failed starts in a row, with no process left, after which `sql_parse`
/// is no longer advertised.
const UNREADY_AFTER: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlParseSettings {
    /// `SQL_PARSER_BIN`: by default `cufflink-sql-parser` beside this
    /// process's own binary.
    pub binary: PathBuf,
    /// `SQL_PARSE_PROCESSES`: parses at once.
    pub processes: usize,
    /// `SQL_PARSE_PER_TENANT`: parses of one tenant at once, by default
    /// half the processes and at least one.
    pub per_tenant: usize,
    /// `SQL_PARSE_MEMORY_MB`: each process's address space, on Linux.
    pub memory_mb: usize,
    /// `SQL_PARSE_TIMEOUT_MS`: the longest one parse may take.
    pub timeout: Duration,
    /// `SQL_PARSE_TENANT_COOLDOWN_MS`: how long a tenant whose parses lost
    /// three processes within it has its parses refused.
    pub tenant_cooldown: Duration,
}

impl Default for SqlParseSettings {
    fn default() -> Self {
        let processes = 2;
        Self {
            binary: std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(|dir| dir.join(BINARY)))
                .unwrap_or_else(|| PathBuf::from(BINARY)),
            processes,
            per_tenant: half(processes),
            memory_mb: 1024,
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            tenant_cooldown: Duration::from_millis(DEFAULT_TENANT_COOLDOWN_MS),
        }
    }
}

fn half(processes: usize) -> usize {
    (processes / 2).max(1)
}

impl SqlParseSettings {
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let defaults = Self::default();
        let positive = |name: &str, default: usize| positive_var(name, var(name), default);
        let millis = |name: &str, default: u64| {
            positive_var(name, var(name), default).map(Duration::from_millis)
        };
        let processes = positive("SQL_PARSE_PROCESSES", defaults.processes)?;
        Ok(Self {
            binary: var("SQL_PARSER_BIN").map_or(defaults.binary, PathBuf::from),
            processes,
            per_tenant: positive("SQL_PARSE_PER_TENANT", half(processes))?,
            memory_mb: positive("SQL_PARSE_MEMORY_MB", defaults.memory_mb)?,
            timeout: millis("SQL_PARSE_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?,
            tenant_cooldown: millis("SQL_PARSE_TENANT_COOLDOWN_MS", DEFAULT_TENANT_COOLDOWN_MS)?,
        })
    }
}

/// Why a parse has no tree: the parser's refusal, or the caller's own
/// refusal to hold the tree.
#[derive(Debug)]
pub enum ParseFailure<E> {
    Refused(ParseError),
    Unheld(E),
}

impl<E> From<ParseError> for ParseFailure<E> {
    fn from(error: ParseError) -> Self {
        Self::Refused(error)
    }
}

pub struct SqlParser {
    pool: Arc<Pool>,
}

/// The processes and who holds them. A parse runs in a task of its own
/// that owns its process from the request to the answer's last byte, so a
/// caller that gives up costs no process: the task finishes the exchange
/// within `SQL_PARSE_TIMEOUT_MS` and returns the process. Replacements are
/// started by a task of their own, never under a caller's deadline.
struct Pool {
    settings: SqlParseSettings,
    ready: AtomicBool,
    idle: Mutex<Vec<Process>>,
    /// One permit for each process in `idle`.
    available: Semaphore,
    tenants: Mutex<HashMap<Uuid, Arc<Semaphore>>>,
    losses: Mutex<HashMap<Uuid, Losses>>,
    health: Mutex<Health>,
    wanted: mpsc::UnboundedSender<()>,
    started: Mutex<Option<mpsc::UnboundedReceiver<()>>>,
}

#[derive(Default)]
struct Health {
    live: usize,
    /// Processes lost and starts failed since a process last answered.
    failures: u32,
    /// Starts failed in a row.
    failed_starts: u32,
}

/// The processes one tenant's parses lost within its cooldown, and until
/// when its parses are refused.
#[derive(Default)]
struct Losses {
    at: Vec<Instant>,
    refused_until: Option<Instant>,
}

impl Losses {
    fn refused(&self, now: Instant) -> bool {
        self.refused_until.is_some_and(|until| now < until)
    }

    fn recent(&self, now: Instant, window: Duration) -> bool {
        self.refused(now) || self.at.iter().any(|at| now.duration_since(*at) < window)
    }
}

impl Health {
    fn backoff(&self) -> Duration {
        match self.failures {
            0 => Duration::ZERO,
            n => FIRST_BACKOFF
                .saturating_mul(1 << (n - 1).min(16))
                .min(MAX_BACKOFF),
        }
    }
}

impl SqlParser {
    pub fn new(settings: SqlParseSettings) -> Self {
        let (wanted, requests) = mpsc::unbounded_channel();
        Self {
            pool: Arc::new(Pool {
                available: Semaphore::new(0),
                ready: AtomicBool::new(false),
                idle: Mutex::new(Vec::new()),
                tenants: Mutex::new(HashMap::new()),
                losses: Mutex::new(HashMap::new()),
                health: Mutex::new(Health::default()),
                wanted,
                started: Mutex::new(Some(requests)),
                settings,
            }),
        }
    }

    /// Starts the processes in the background, and answers whether the
    /// first was ready within its handshake's time. Until one has answered
    /// with this release's parser and fingerprint, `sql_parse` is not
    /// advertised and every call is `Unavailable`.
    pub async fn start(&self) -> bool {
        let Some(requests) = lock(&self.pool.started).take() else {
            return self.is_ready();
        };
        let (first, answered) = oneshot::channel();
        tokio::spawn(supervise(Arc::downgrade(&self.pool), requests, first));
        tokio::spawn(reap_idle(Arc::downgrade(&self.pool)));
        (0..self.pool.settings.processes).for_each(|_| self.pool.want());
        let ready = tokio::time::timeout(HANDSHAKE_TIMEOUT, answered)
            .await
            .is_ok_and(|started| started.unwrap_or(false));
        match ready {
            true => tracing::info!(
                parser = PARSER,
                processes = self.pool.settings.processes,
                "SQL parser ready"
            ),
            false => tracing::warn!(
                "SQL parser not ready; sql_parse is not advertised until a process starts"
            ),
        }
        ready
    }

    pub fn is_ready(&self) -> bool {
        self.pool.ready.load(Ordering::Acquire)
    }

    /// The canonical text of `sql`'s tree, written after what `hold` gives
    /// back for the tree's length, at most `max_output` bytes. `hold` runs
    /// before the tree is read, so it can charge for it first. `tenant`'s
    /// parses at once are limited to `SQL_PARSE_PER_TENANT`, and a tenant
    /// whose parses keep losing processes is refused for a cooldown.
    pub async fn parse<E>(
        &self,
        tenant: Uuid,
        sql: &str,
        max_output: usize,
        within: Duration,
        hold: impl FnOnce(usize) -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, ParseFailure<E>> {
        let started = Instant::now();
        let outcome = self
            .parse_timed(tenant, sql, max_output, within, hold)
            .await;
        let result = match &outcome {
            Ok(_) => "ok",
            Err(ParseFailure::Refused(ParseError::Syntax(_))) => "syntax",
            Err(ParseFailure::Refused(ParseError::TooLarge(_))) => "too_large",
            Err(ParseFailure::Refused(ParseError::Unavailable(_))) => "unavailable",
            Err(ParseFailure::Unheld(_)) => "unheld",
        };
        metrics::counter!("cufflink_sql_parse_calls_total", "result" => result).increment(1);
        metrics::histogram!("cufflink_sql_parse_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        outcome
    }

    async fn parse_timed<E>(
        &self,
        tenant: Uuid,
        sql: &str,
        max_output: usize,
        within: Duration,
        hold: impl FnOnce(usize) -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, ParseFailure<E>> {
        bounds::check(sql)?;
        if !self.is_ready() || within < MIN_PARSE_BUDGET || self.pool.refuses(tenant) {
            return Err(ParseError::unavailable().into());
        }
        let deadline = tokio::time::Instant::now() + within.min(self.pool.settings.timeout);
        let turn = before(deadline, TenantTurn::take(&self.pool, tenant))
            .await
            .flatten()
            .ok_or_else(ParseError::unavailable)?;
        let process = before(deadline, self.pool.take())
            .await
            .flatten()
            .ok_or_else(ParseError::unavailable)?;
        let mut frames = Vec::with_capacity(sql.len() + 64);
        wire::write_message(
            &mut frames,
            &Request {
                max_output: u64::try_from(max_output).unwrap_or(u64::MAX),
            },
        )
        .and_then(|()| wire::write_frame(&mut frames, sql.as_bytes()))
        .map_err(|_| ParseError::unavailable())?;
        let (header, headed) = oneshot::channel();
        let (buffer, given) = oneshot::channel();
        let (tree, read) = oneshot::channel();
        tokio::spawn(serve(
            Arc::clone(&self.pool),
            process,
            turn,
            Job {
                frames,
                max_output,
                header,
                given,
                tree,
            },
        ));
        match before(deadline, headed).await.and_then(Result::ok) {
            None => Err(ParseError::unavailable().into()),
            Some(Header::Refused(error)) => Err(error.into()),
            Some(Header::Tree(len)) => match hold(len) {
                Err(unheld) => {
                    let _ = buffer.send(None);
                    Err(ParseFailure::Unheld(unheld))
                }
                Ok(out) => {
                    let _ = buffer.send(Some(out));
                    before(deadline, read)
                        .await
                        .and_then(Result::ok)
                        .ok_or_else(|| ParseError::unavailable().into())
                }
            },
        }
    }
}

async fn before<T>(deadline: tokio::time::Instant, work: impl Future<Output = T>) -> Option<T> {
    tokio::time::timeout_at(deadline, work).await.ok()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Pool {
    fn want(&self) {
        let _ = self.wanted.send(());
    }

    /// A live idle process, waiting for one to come back or be started.
    async fn take(&self) -> Option<Process> {
        loop {
            self.available.acquire().await.ok()?.forget();
            if let Some(mut process) = lock(&self.idle).pop() {
                match process.is_alive() {
                    true => return Some(process),
                    false => self.lost("an idle parser process ended", &"exited", "idle"),
                }
            }
        }
    }

    /// Idle processes that ended, killed by the kernel for instance, are
    /// replaced without waiting for a call to find them.
    fn reap(&self) {
        let ended = lock(&self.idle)
            .extract_if(.., |process| !process.is_alive())
            .count();
        for _ in 0..ended {
            if let Ok(permit) = self.available.try_acquire() {
                permit.forget();
            }
            self.lost("an idle parser process ended", &"exited", "idle");
        }
    }

    fn refuses(&self, tenant: Uuid) -> bool {
        lock(&self.losses)
            .get(&tenant)
            .is_some_and(|losses| losses.refused(Instant::now()))
    }

    /// A process lost to `tenant`'s parse, which past
    /// `LOSSES_BEFORE_COOLDOWN` within the cooldown refuses its parses for
    /// the cooldown, so a tenant whose statements keep costing processes
    /// does not keep the others waiting on their replacements.
    fn lost_to(&self, tenant: Uuid) {
        let now = Instant::now();
        let window = self.settings.tenant_cooldown;
        let mut all = lock(&self.losses);
        all.retain(|_, losses| losses.recent(now, window));
        let losses = all.entry(tenant).or_default();
        losses.at.retain(|at| now.duration_since(*at) < window);
        losses.at.push(now);
        if losses.at.len() >= LOSSES_BEFORE_COOLDOWN {
            losses.at.clear();
            losses.refused_until = Some(now + window);
            metrics::counter!("cufflink_sql_parse_tenant_cooldowns_total").increment(1);
            tracing::warn!(
                %tenant,
                cooldown = ?window,
                "a tenant's parses keep losing parser processes; its parses are refused for the cooldown"
            );
        }
    }

    fn give_back(&self, process: Process) {
        lock(&self.health).failures = 0;
        lock(&self.idle).push(process);
        self.available.add_permits(1);
    }

    /// A process that failed is dropped, which kills it, and another is
    /// started in its place.
    fn lost(&self, what: &str, error: &dyn std::fmt::Display, stage: &'static str) {
        metrics::counter!("cufflink_sql_parser_failures_total", "stage" => stage).increment(1);
        tracing::warn!(%error, "{what}");
        {
            let mut health = lock(&self.health);
            health.live = health.live.saturating_sub(1);
            health.failures = health.failures.saturating_add(1);
            metrics::gauge!("cufflink_sql_parser_processes").set(health.live as f64);
        }
        self.want();
    }

    fn started(&self, process: Process) {
        {
            let mut health = lock(&self.health);
            health.live += 1;
            health.failed_starts = 0;
            metrics::gauge!("cufflink_sql_parser_processes").set(health.live as f64);
        }
        if !self.ready.swap(true, Ordering::AcqRel) {
            tracing::info!(
                parser = PARSER,
                "a SQL parser process answered; sql_parse is advertised"
            );
        }
        lock(&self.idle).push(process);
        self.available.add_permits(1);
    }

    fn failed_to_start(&self, error: &eyre::Report) {
        metrics::counter!("cufflink_sql_parser_failures_total", "stage" => "start").increment(1);
        let mut health = lock(&self.health);
        health.failures = health.failures.saturating_add(1);
        health.failed_starts = health.failed_starts.saturating_add(1);
        let delay = health.backoff();
        tracing::warn!(%error, ?delay, "a SQL parser process did not start");
        if health.live == 0
            && health.failed_starts >= UNREADY_AFTER
            && self.ready.swap(false, Ordering::AcqRel)
        {
            tracing::warn!("no SQL parser process can start; sql_parse is no longer advertised");
        }
    }
}

/// Starts a process each time one is wanted, one at a time, after the
/// backoff the failures since a process last answered call for.
async fn supervise(
    pool: Weak<Pool>,
    mut requests: mpsc::UnboundedReceiver<()>,
    first: oneshot::Sender<bool>,
) {
    let mut first = Some(first);
    while requests.recv().await.is_some() {
        loop {
            let Some((settings, delay)) = pool
                .upgrade()
                .map(|pool| (pool.settings.clone(), lock(&pool.health).backoff()))
            else {
                return;
            };
            tokio::time::sleep(delay).await;
            let spawned = Process::spawn(&settings).await;
            let Some(pool) = pool.upgrade() else {
                return;
            };
            let ready = spawned.is_ok();
            match spawned {
                Ok(process) => pool.started(process),
                Err(error) => pool.failed_to_start(&error),
            }
            if let Some(first) = first.take() {
                let _ = first.send(ready);
            }
            if ready {
                break;
            }
        }
    }
}

async fn reap_idle(pool: Weak<Pool>) {
    let mut every = tokio::time::interval(REAP_EVERY);
    loop {
        every.tick().await;
        match pool.upgrade() {
            Some(pool) => pool.reap(),
            None => return,
        }
    }
}

/// A tenant's hold on one of its parses at once, released when the
/// exchange it was taken for ends, whether or not its caller waited. A
/// tenant's calls past its share wait for one of its own turns, never for
/// another tenant's.
struct TenantTurn {
    pool: Arc<Pool>,
    tenant: Uuid,
    permit: Option<OwnedSemaphorePermit>,
}

impl TenantTurn {
    async fn take(pool: &Arc<Pool>, tenant: Uuid) -> Option<Self> {
        let turns = Arc::clone(
            lock(&pool.tenants)
                .entry(tenant)
                .or_insert_with(|| Arc::new(Semaphore::new(pool.settings.per_tenant))),
        );
        let permit = turns.acquire_owned().await.ok()?;
        Some(Self {
            pool: Arc::clone(pool),
            tenant,
            permit: Some(permit),
        })
    }
}

impl Drop for TenantTurn {
    fn drop(&mut self) {
        drop(self.permit.take());
        let mut tenants = lock(&self.pool.tenants);
        if tenants
            .get(&self.tenant)
            .is_some_and(|turns| Arc::strong_count(turns) == 1)
        {
            tenants.remove(&self.tenant);
        }
    }
}

enum Header {
    Tree(usize),
    Refused(ParseError),
}

/// One request and the caller's side of its answer: the header first, then
/// the buffer the caller charged for, or none, then the tree read into it.
struct Job {
    frames: Vec<u8>,
    max_output: usize,
    header: oneshot::Sender<Header>,
    given: oneshot::Receiver<Option<Vec<u8>>>,
    tree: oneshot::Sender<Vec<u8>>,
}

async fn serve(pool: Arc<Pool>, mut process: Process, turn: TenantTurn, job: Job) {
    let timeout = pool.settings.timeout;
    match tokio::time::timeout(timeout, process.exchange(job)).await {
        Ok(Ok(())) => pool.give_back(process),
        Ok(Err(fault)) => {
            pool.lost_to(turn.tenant);
            pool.lost("the parser process failed", &fault, "parse");
        }
        Err(_) => {
            pool.lost_to(turn.tenant);
            pool.lost(
                "the parser process did not answer in time",
                &"timed out",
                "parse",
            );
        }
    }
    drop(turn);
}

/// One parser process. Dropping it kills it; tokio reaps it.
struct Process {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Process {
    async fn spawn(settings: &SqlParseSettings) -> Result<Self> {
        let mut child = Command::new(&settings.binary)
            .env_clear()
            .env("SQL_PARSE_MEMORY_MB", settings.memory_mb.to_string())
            .env("MALLOC_ARENA_MAX", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| eyre!("cannot start {}: {e}", settings.binary.display()))?;
        let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            return Err(eyre!("the parser process has no pipes"));
        };
        let mut process = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        };
        let hello: Hello = tokio::time::timeout(HANDSHAKE_TIMEOUT, process.message())
            .await
            .map_err(|_| {
                eyre!("the parser process did not greet within {HANDSHAKE_TIMEOUT:?}")
            })??;
        metrics::counter!("cufflink_sql_parser_spawns_total").increment(1);
        match hello.parser == PARSER && hello.fingerprint == PROBE_FINGERPRINT {
            true => Ok(process),
            false => Err(eyre!(
                "the parser process is pg_query {} with fingerprint {}; this platform needs {PARSER} with {PROBE_FINGERPRINT}",
                hello.parser,
                hello.fingerprint
            )),
        }
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// The whole exchange of `job`, to the answer's last byte, whether or
    /// not its caller is still waiting.
    async fn exchange(&mut self, job: Job) -> io::Result<()> {
        self.stdin.write_all(&job.frames).await?;
        self.stdin.flush().await?;
        match self.message().await? {
            Answer::Error(error) => {
                let _ = job.header.send(Header::Refused(error));
                Ok(())
            }
            Answer::Tree => {
                let len = self.frame_len(job.max_output).await?;
                let given = match job.header.send(Header::Tree(len)) {
                    Ok(()) => job.given.await.ok().flatten(),
                    Err(_) => None,
                };
                match given {
                    Some(mut out) => {
                        let start = out.len();
                        out.resize(start + len, 0);
                        self.stdout.read_exact(&mut out[start..]).await?;
                        let _ = job.tree.send(out);
                        Ok(())
                    }
                    None => self.skip(len).await,
                }
            }
        }
    }

    async fn skip(&mut self, len: usize) -> io::Result<()> {
        let skip = u64::try_from(len).unwrap_or(u64::MAX);
        let skipped =
            tokio::io::copy(&mut (&mut self.stdout).take(skip), &mut tokio::io::sink()).await?;
        match skipped == skip {
            true => Ok(()),
            false => Err(io::ErrorKind::UnexpectedEof.into()),
        }
    }

    async fn frame_len(&mut self, max: usize) -> io::Result<usize> {
        let mut header = [0u8; 4];
        self.stdout.read_exact(&mut header).await?;
        wire::frame_len(header, max)
    }

    async fn message<T: for<'de> serde::Deserialize<'de>>(&mut self) -> io::Result<T> {
        let len = self.frame_len(MAX_CONTROL_FRAME).await?;
        let mut bytes = vec![0u8; len];
        self.stdout.read_exact(&mut bytes).await?;
        serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_come_from_the_environment_or_their_defaults() {
        let defaults = SqlParseSettings::from_vars(|_| None).expect("defaults");
        assert_eq!(defaults, SqlParseSettings::default());
        assert!(defaults.binary.ends_with(BINARY));
        let set = SqlParseSettings::from_vars(|name| {
            match name {
                "SQL_PARSER_BIN" => Some("/opt/parser"),
                "SQL_PARSE_PROCESSES" => Some("4"),
                "SQL_PARSE_PER_TENANT" => Some("3"),
                "SQL_PARSE_MEMORY_MB" => Some("512"),
                "SQL_PARSE_TIMEOUT_MS" => Some("250"),
                "SQL_PARSE_TENANT_COOLDOWN_MS" => Some("1500"),
                _ => None,
            }
            .map(String::from)
        })
        .expect("set");
        assert_eq!(
            set,
            SqlParseSettings {
                binary: PathBuf::from("/opt/parser"),
                processes: 4,
                per_tenant: 3,
                memory_mb: 512,
                timeout: Duration::from_millis(250),
                tenant_cooldown: Duration::from_millis(1500),
            }
        );
        assert_eq!(
            SqlParseSettings::from_vars(|name| {
                (name == "SQL_PARSE_PROCESSES").then(|| "5".to_string())
            })
            .expect("processes")
            .per_tenant,
            2,
            "a tenant may hold half the processes by default"
        );
        for name in [
            "SQL_PARSE_PROCESSES",
            "SQL_PARSE_PER_TENANT",
            "SQL_PARSE_MEMORY_MB",
            "SQL_PARSE_TIMEOUT_MS",
            "SQL_PARSE_TENANT_COOLDOWN_MS",
        ] {
            assert!(
                SqlParseSettings::from_vars(|n| (n == name).then(|| "0".to_string())).is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn starts_back_off_with_every_loss_and_failure_since_a_process_last_answered() {
        let delays: Vec<_> = [0, 1, 2, 3, 7, 30]
            .into_iter()
            .map(|failures| {
                Health {
                    failures,
                    ..Health::default()
                }
                .backoff()
            })
            .collect();
        assert_eq!(
            delays,
            [0, 100, 200, 400, 5_000, 5_000].map(Duration::from_millis)
        );
    }

    #[tokio::test]
    async fn a_parser_that_never_started_refuses_every_call_as_unavailable() {
        let parser = SqlParser::new(SqlParseSettings {
            binary: PathBuf::from("/nonexistent/cufflink-sql-parser"),
            ..SqlParseSettings::default()
        });
        assert!(!parser.start().await);
        assert!(!parser.is_ready());
        let refused = parser
            .parse(
                Uuid::nil(),
                "SELECT 1",
                1024,
                Duration::from_secs(1),
                |_| Ok::<_, ()>(Vec::new()),
            )
            .await;
        assert!(matches!(
            refused,
            Err(ParseFailure::Refused(ParseError::Unavailable(_)))
        ));
        let bounded = parser
            .parse(
                Uuid::nil(),
                &"x".repeat(bounds::MAX_INPUT_BYTES + 1),
                1024,
                Duration::from_secs(1),
                |_| Ok::<_, ()>(Vec::new()),
            )
            .await;
        assert!(
            matches!(bounded, Err(ParseFailure::Refused(ParseError::TooLarge(_)))),
            "the bounds answer before any process is asked"
        );
    }
}
