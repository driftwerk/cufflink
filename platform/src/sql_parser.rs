//! The parser processes behind `sql_parse` (`docs/design-notes/sql-parse.md`).
//!
//! PostgreSQL's parser is C code, and a fault in it ends the process it
//! runs in, so it runs in `cufflink-sql-parser` processes, never here. A
//! process that faults, stops answering or answers out of turn costs its
//! call (`Unavailable`) and is replaced. A process serves one tenant: one
//! that served another is replaced before it serves the next.

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
use tokio::sync::{mpsc, oneshot, Semaphore};
use uuid::Uuid;

const BINARY: &str = "cufflink-sql-parser";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_TENANT_COOLDOWN_MS: u64 = 60_000;
const DEFAULT_TENANT_BUSY_MS: u64 = 10_000;
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
    /// `SQL_PARSE_TENANT_BUSY_MS`: the process time one tenant's parses may
    /// take at once, refilled at that much every `tenant_cooldown`.
    pub tenant_busy: Duration,
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
            tenant_busy: Duration::from_millis(DEFAULT_TENANT_BUSY_MS),
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
            tenant_busy: millis("SQL_PARSE_TENANT_BUSY_MS", DEFAULT_TENANT_BUSY_MS)?,
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

/// What a caller charges for its parse's memory: the request's copy of the
/// statement, then the buffer the tree is read into. It goes with the
/// exchange and comes back with the tree, so each charge lasts as long as
/// the bytes it pays for, also when the caller stops waiting.
pub trait Charge: Send + 'static {
    type Refused: Send + 'static;

    /// Charges for the request's `bytes`, before they are copied.
    fn request(&mut self, bytes: usize) -> Result<(), Self::Refused>;

    /// The buffer a tree of `len` bytes is read onto the end of, charged
    /// for before it is made.
    fn tree(&mut self, len: usize) -> Result<Vec<u8>, Self::Refused>;
}

/// A caller with nothing to charge for the request, whose closure makes the
/// tree's buffer.
impl<F, E> Charge for F
where
    F: FnMut(usize) -> Result<Vec<u8>, E> + Send + 'static,
    E: Send + 'static,
{
    type Refused = E;

    fn request(&mut self, _: usize) -> Result<(), E> {
        Ok(())
    }

    fn tree(&mut self, len: usize) -> Result<Vec<u8>, E> {
        self(len)
    }
}

/// A tree's text and the charge for it.
pub type Charged<C> = (Vec<u8>, C);

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
    turns: Mutex<HashMap<Uuid, Arc<Semaphore>>>,
    standings: Mutex<HashMap<Uuid, Standing>>,
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

/// One tenant's standing: the processes its parses lost within its
/// cooldown, until when its parses are refused for them, and the process
/// time it has left. The time is a bucket of `tenant_busy` that refills at
/// that much every `tenant_cooldown`; each exchange is charged what it took
/// once it ends, and a tenant with nothing left is refused until it refills.
struct Standing {
    losses: Vec<Instant>,
    refused_until: Option<Instant>,
    /// Seconds of process time left, below zero after a long exchange.
    busy_left: f64,
    as_of: Instant,
}

impl Standing {
    fn new(now: Instant, settings: &SqlParseSettings) -> Self {
        Self {
            losses: Vec::new(),
            refused_until: None,
            busy_left: settings.tenant_busy.as_secs_f64(),
            as_of: now,
        }
    }

    fn refill(&mut self, now: Instant, settings: &SqlParseSettings) {
        let full = settings.tenant_busy.as_secs_f64();
        let rate = full / settings.tenant_cooldown.as_secs_f64();
        let since = now.saturating_duration_since(self.as_of).as_secs_f64();
        self.busy_left = (self.busy_left + since * rate).min(full);
        self.as_of = now;
    }

    fn cooling(&self, now: Instant) -> bool {
        self.refused_until.is_some_and(|until| now < until)
    }

    fn refused(&mut self, now: Instant, settings: &SqlParseSettings) -> bool {
        self.refill(now, settings);
        self.cooling(now) || self.busy_left <= 0.0
    }

    /// Nothing to remember: no loss within the cooldown, no refusal, and
    /// the whole busy time left.
    fn settled(&mut self, now: Instant, settings: &SqlParseSettings) -> bool {
        self.refill(now, settings);
        let window = settings.tenant_cooldown;
        !self.cooling(now)
            && self
                .losses
                .iter()
                .all(|at| now.duration_since(*at) >= window)
            && self.busy_left >= settings.tenant_busy.as_secs_f64()
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
                turns: Mutex::new(HashMap::new()),
                standings: Mutex::new(HashMap::new()),
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

    /// The canonical text of `sql`'s tree, written onto the end of the
    /// buffer `charge` makes for the tree's length, at most `max_output`
    /// bytes, with `charge` itself. `tenant`'s parses at once are limited
    /// to `SQL_PARSE_PER_TENANT`, and a tenant whose parses keep losing
    /// processes, or have spent their process time, is refused for a
    /// while.
    pub async fn parse<C: Charge>(
        &self,
        tenant: Uuid,
        sql: &str,
        max_output: usize,
        within: Duration,
        charge: C,
    ) -> Result<Charged<C>, ParseFailure<C::Refused>> {
        let started = Instant::now();
        let outcome = self
            .parse_timed(tenant, sql, max_output, within, charge)
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

    async fn parse_timed<C: Charge>(
        &self,
        tenant: Uuid,
        sql: &str,
        max_output: usize,
        within: Duration,
        mut charge: C,
    ) -> Result<Charged<C>, ParseFailure<C::Refused>> {
        bounds::check(sql)?;
        if !self.is_ready() || within < MIN_PARSE_BUDGET || self.pool.refuses(tenant) {
            return Err(ParseError::unavailable().into());
        }
        let frames = request(sql, max_output, &mut charge)?;
        let deadline = tokio::time::Instant::now() + within.min(self.pool.settings.timeout);
        let turn = before(deadline, TenantTurn::take(&self.pool, tenant))
            .await
            .flatten()
            .ok_or_else(ParseError::unavailable)?;
        let process = before(deadline, self.pool.take(tenant))
            .await
            .flatten()
            .ok_or_else(ParseError::unavailable)?;
        let (answer, answered) = oneshot::channel();
        tokio::spawn(serve(
            Arc::clone(&self.pool),
            process,
            turn,
            Job {
                frames,
                max_output,
                charge,
                answer,
            },
        ));
        before(deadline, answered)
            .await
            .and_then(Result::ok)
            .unwrap_or_else(|| Err(ParseError::unavailable().into()))
    }
}

/// The request's frames, charged for before they are made.
fn request<C: Charge>(
    sql: &str,
    max_output: usize,
    charge: &mut C,
) -> Result<Vec<u8>, ParseFailure<C::Refused>> {
    let message = serde_json::to_vec(&Request {
        max_output: u64::try_from(max_output).unwrap_or(u64::MAX),
    })
    .map_err(|_| ParseError::unavailable())?;
    let len = 2 * size_of::<u32>() + message.len() + sql.len();
    charge.request(len).map_err(ParseFailure::Unheld)?;
    let mut frames = Vec::with_capacity(len);
    wire::write_frame(&mut frames, &message)
        .and_then(|()| wire::write_frame(&mut frames, sql.as_bytes()))
        .map_err(|_| ParseError::unavailable())?;
    Ok(frames)
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

    /// A live idle process that has served no tenant but `tenant`, waiting
    /// for one to come back or be started. When only other tenants'
    /// processes are idle, the one idle longest is replaced, and the call
    /// waits for the next process.
    async fn take(&self, tenant: Uuid) -> Option<Process> {
        loop {
            self.available.acquire().await.ok()?.forget();
            let Some(mut process) = self.pick(tenant) else {
                continue;
            };
            if !process.is_alive() {
                self.lost("an idle parser process ended", &"exited", "idle");
                continue;
            }
            match process.tenant {
                Some(served) if served != tenant => self.recycle(process),
                _ => {
                    process.tenant = Some(tenant);
                    return Some(process);
                }
            }
        }
    }

    /// `tenant`'s own idle process, else one that has served no tenant,
    /// else the one idle longest.
    fn pick(&self, tenant: Uuid) -> Option<Process> {
        let mut idle = lock(&self.idle);
        let at = idle
            .iter()
            .position(|process| process.tenant == Some(tenant))
            .or_else(|| idle.iter().position(|process| process.tenant.is_none()))
            .or_else(|| (!idle.is_empty()).then_some(0))?;
        Some(idle.remove(at))
    }

    /// A process that served another tenant is dropped, which kills it, and
    /// a fresh one is started in its place. It is no failure, so it adds
    /// nothing to the backoff.
    fn recycle(&self, process: Process) {
        drop(process);
        metrics::counter!("cufflink_sql_parser_recycled_total").increment(1);
        {
            let mut health = lock(&self.health);
            health.live = health.live.saturating_sub(1);
            metrics::gauge!("cufflink_sql_parser_processes").set(health.live as f64);
        }
        self.want();
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
        let now = Instant::now();
        lock(&self.standings).retain(|_, standing| !standing.settled(now, &self.settings));
    }

    fn refuses(&self, tenant: Uuid) -> bool {
        lock(&self.standings)
            .get_mut(&tenant)
            .is_some_and(|standing| standing.refused(Instant::now(), &self.settings))
    }

    /// An exchange of `tenant`'s took `took` of a process's time.
    fn spent(&self, tenant: Uuid, took: Duration) {
        let now = Instant::now();
        let mut standings = lock(&self.standings);
        let standing = standings
            .entry(tenant)
            .or_insert_with(|| Standing::new(now, &self.settings));
        standing.refill(now, &self.settings);
        let had = standing.busy_left;
        standing.busy_left -= took.as_secs_f64();
        if had > 0.0 && standing.busy_left <= 0.0 {
            metrics::counter!("cufflink_sql_parse_tenant_busy_spent_total").increment(1);
            tracing::warn!(
                %tenant,
                busy = ?self.settings.tenant_busy,
                "a tenant's parses have taken all their process time; its parses are refused until it refills"
            );
        }
    }

    /// A process lost to `tenant`'s parse, which past
    /// `LOSSES_BEFORE_COOLDOWN` within the cooldown refuses its parses for
    /// the cooldown, so a tenant whose statements keep costing processes
    /// does not keep the others waiting on their replacements.
    fn lost_to(&self, tenant: Uuid) {
        let now = Instant::now();
        let window = self.settings.tenant_cooldown;
        let mut standings = lock(&self.standings);
        let standing = standings
            .entry(tenant)
            .or_insert_with(|| Standing::new(now, &self.settings));
        standing
            .losses
            .retain(|at| now.duration_since(*at) < window);
        standing.losses.push(now);
        if standing.losses.len() >= LOSSES_BEFORE_COOLDOWN {
            standing.losses.clear();
            standing.refused_until = Some(now + window);
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
/// another tenant's. The last of a tenant's turns, held or waited for,
/// removes its entry, also when its caller stopped waiting.
struct TenantTurn {
    pool: Arc<Pool>,
    tenant: Uuid,
    turns: Arc<Semaphore>,
    held: bool,
}

impl TenantTurn {
    async fn take(pool: &Arc<Pool>, tenant: Uuid) -> Option<Self> {
        let turns = Arc::clone(
            lock(&pool.turns)
                .entry(tenant)
                .or_insert_with(|| Arc::new(Semaphore::new(pool.settings.per_tenant))),
        );
        let mut turn = Self {
            pool: Arc::clone(pool),
            tenant,
            turns,
            held: false,
        };
        turn.turns.acquire().await.ok()?.forget();
        turn.held = true;
        Some(turn)
    }
}

impl Drop for TenantTurn {
    fn drop(&mut self) {
        if self.held {
            self.turns.add_permits(1);
        }
        let mut turns = lock(&self.pool.turns);
        if turns.get(&self.tenant).is_some_and(|shared| {
            Arc::ptr_eq(shared, &self.turns) && Arc::strong_count(shared) == 2
        }) {
            turns.remove(&self.tenant);
        }
    }
}

/// What a caller is answered: the tree with its charge, or why not.
type Reply<C> = Result<Charged<C>, ParseFailure<<C as Charge>::Refused>>;

/// One request, and the caller's side of its answer.
struct Job<C: Charge> {
    frames: Vec<u8>,
    max_output: usize,
    charge: C,
    answer: oneshot::Sender<Reply<C>>,
}

/// The exchange of `job` on `process`. Its time is charged to the tenant,
/// and the process given back or replaced, before the caller is answered,
/// so the caller's next call sees both.
async fn serve<C: Charge>(pool: Arc<Pool>, mut process: Process, turn: TenantTurn, job: Job<C>) {
    let Job {
        frames,
        max_output,
        charge,
        answer,
    } = job;
    let timeout = pool.settings.timeout;
    let started = Instant::now();
    let exchanged = tokio::time::timeout(
        timeout,
        process.exchange(frames, max_output, charge, &answer),
    )
    .await;
    pool.spent(turn.tenant, started.elapsed());
    match exchanged {
        Ok(Ok(reply)) => {
            pool.give_back(process);
            if let Some(reply) = reply {
                let _ = answer.send(reply);
            }
        }
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
    /// The tenant it has served, if any.
    tenant: Option<Uuid>,
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
            tenant: None,
        };
        let hello: Hello = tokio::time::timeout(HANDSHAKE_TIMEOUT, process.message())
            .await
            .map_err(|_| {
                eyre!("the parser process did not greet within {HANDSHAKE_TIMEOUT:?}")
            })??;
        metrics::counter!("cufflink_sql_parser_spawns_total").increment(1);
        let wanted = Hello::of_this_build(PROBE_FINGERPRINT);
        match hello == wanted {
            true => Ok(process),
            false => Err(eyre!(
                "the parser process greeted with {hello:?}; this platform needs {wanted:?}"
            )),
        }
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// One exchange, to the answer's last byte, whether or not its caller
    /// is still waiting, and what the caller is to be answered. A caller
    /// that stopped waiting is charged nothing for the tree, which is read
    /// and discarded.
    async fn exchange<C: Charge>(
        &mut self,
        frames: Vec<u8>,
        max_output: usize,
        mut charge: C,
        answer: &oneshot::Sender<Reply<C>>,
    ) -> io::Result<Option<Reply<C>>> {
        self.stdin.write_all(&frames).await?;
        self.stdin.flush().await?;
        drop(frames);
        match self.message().await? {
            Answer::Error(error) => Ok(Some(Err(ParseFailure::Refused(error)))),
            Answer::Tree => {
                let len = self.frame_len(max_output).await?;
                if answer.is_closed() {
                    return self.skip(len).await.map(|()| None);
                }
                match charge.tree(len) {
                    Err(refused) => self
                        .skip(len)
                        .await
                        .map(|()| Some(Err(ParseFailure::Unheld(refused)))),
                    Ok(mut out) => {
                        let start = out.len();
                        out.resize(start + len, 0);
                        self.stdout.read_exact(&mut out[start..]).await?;
                        match is_one_object(&out[start..]) {
                            true => Ok(Some(Ok((out, charge)))),
                            false => Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "the parser process answered a tree that is not one JSON object",
                            )),
                        }
                    }
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

/// Whether `tree` is UTF-8 holding one well-formed JSON object and nothing
/// more, so that what a process answers cannot reach past the tree in the
/// answer the platform writes around it.
fn is_one_object(tree: &[u8]) -> bool {
    tree.first() == Some(&b'{')
        && std::str::from_utf8(tree)
            .is_ok_and(|text| serde_json::from_str::<serde::de::IgnoredAny>(text).is_ok())
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
                "SQL_PARSE_TENANT_BUSY_MS" => Some("700"),
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
                tenant_busy: Duration::from_millis(700),
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
            "SQL_PARSE_TENANT_BUSY_MS",
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

    #[test]
    fn only_one_json_object_is_taken_as_a_tree() {
        assert!(is_one_object(br#"{"stmts":[],"version":170007}"#));
        assert!(is_one_object(br#"{"a":"}\"{"}"#));
        for forged in [
            &br#"{}},"injected":{"#[..],
            br#"{"a":1}{"b":2}"#,
            br#"{"a":1"#,
            br#"[{"a":1}]"#,
            br#""tree""#,
            br#" {"a":1}"#,
            b"{\"a\":\"\xff\"}",
            b"",
        ] {
            assert!(
                !is_one_object(forged),
                "{}",
                String::from_utf8_lossy(forged)
            );
        }
    }

    #[test]
    fn a_tenants_process_time_is_spent_and_refills_at_its_rate() {
        let settings = SqlParseSettings {
            tenant_busy: Duration::from_secs(10),
            tenant_cooldown: Duration::from_secs(60),
            ..SqlParseSettings::default()
        };
        let start = Instant::now();
        let mut standing = Standing::new(start, &settings);
        assert!(!standing.refused(start, &settings));
        assert!(standing.settled(start, &settings));
        standing.busy_left -= 12.0;
        assert!(standing.refused(start, &settings));
        assert!(!standing.settled(start, &settings));
        let refilled = start + Duration::from_secs(13);
        assert!(
            !standing.refused(refilled, &settings),
            "13 s refill 13 * 10 / 60 s"
        );
        let full = start + Duration::from_secs(600);
        assert!(standing.settled(full, &settings));
        assert!(
            standing.busy_left <= settings.tenant_busy.as_secs_f64(),
            "the bucket holds no more than its size"
        );
    }

    #[tokio::test]
    async fn a_tenant_turn_waited_for_and_given_up_leaves_no_entry() {
        let parser = SqlParser::new(SqlParseSettings {
            per_tenant: 1,
            ..SqlParseSettings::default()
        });
        let pool = &parser.pool;
        let tenant = Uuid::from_u128(7);
        let held = TenantTurn::take(pool, tenant).await.expect("a turn");
        let mut waiting = Box::pin(TenantTurn::take(pool, tenant));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err(),
            "the tenant's only turn is held"
        );
        drop(held);
        assert_eq!(lock(&pool.turns).len(), 1, "a turn is still waited for");
        drop(waiting);
        assert!(
            lock(&pool.turns).is_empty(),
            "the waiter that gave up removed it"
        );
        let again = TenantTurn::take(pool, tenant)
            .await
            .expect("the turn is free");
        drop(again);
        assert!(lock(&pool.turns).is_empty());
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
