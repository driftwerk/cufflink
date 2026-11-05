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
use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Semaphore;

const BINARY: &str = "cufflink-sql-parser";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlParseSettings {
    /// `SQL_PARSER_BIN`: by default `cufflink-sql-parser` beside this
    /// process's own binary.
    pub binary: PathBuf,
    /// `SQL_PARSE_PROCESSES`: parses at once.
    pub processes: usize,
    /// `SQL_PARSE_MEMORY_MB`: each process's address space, on Linux.
    pub memory_mb: usize,
    /// `SQL_PARSE_TIMEOUT_MS`: the longest one parse may take.
    pub timeout: Duration,
}

impl Default for SqlParseSettings {
    fn default() -> Self {
        Self {
            binary: std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(|dir| dir.join(BINARY)))
                .unwrap_or_else(|| PathBuf::from(BINARY)),
            processes: 2,
            memory_mb: 1024,
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
        }
    }
}

impl SqlParseSettings {
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let defaults = Self::default();
        let positive = |name: &str, default: usize| positive_var(name, var(name), default);
        Ok(Self {
            binary: var("SQL_PARSER_BIN").map_or(defaults.binary, PathBuf::from),
            processes: positive("SQL_PARSE_PROCESSES", defaults.processes)?,
            memory_mb: positive("SQL_PARSE_MEMORY_MB", defaults.memory_mb)?,
            timeout: positive_var(
                "SQL_PARSE_TIMEOUT_MS",
                var("SQL_PARSE_TIMEOUT_MS"),
                DEFAULT_TIMEOUT_MS,
            )
            .map(Duration::from_millis)?,
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
    settings: SqlParseSettings,
    ready: AtomicBool,
    idle: Mutex<Vec<Process>>,
    turns: Semaphore,
    backoff: Mutex<Backoff>,
}

#[derive(Default)]
struct Backoff {
    failures: u32,
    until: Option<Instant>,
}

impl SqlParser {
    pub fn new(settings: SqlParseSettings) -> Self {
        Self {
            turns: Semaphore::new(settings.processes),
            settings,
            ready: AtomicBool::new(false),
            idle: Mutex::new(Vec::new()),
            backoff: Mutex::new(Backoff::default()),
        }
    }

    /// Starts the first process, and answers whether it is ready. Until
    /// one has answered with this release's parser and fingerprint,
    /// `sql_parse` is not advertised and every call is `Unavailable`.
    pub async fn start(&self) -> bool {
        match Process::spawn(&self.settings).await {
            Ok(process) => {
                self.lock_idle().push(process);
                self.ready.store(true, Ordering::Release);
                tracing::info!(
                    parser = PARSER,
                    processes = self.settings.processes,
                    "SQL parser ready"
                );
                true
            }
            Err(error) => {
                tracing::warn!(%error, "SQL parser unavailable; sql_parse is not advertised");
                false
            }
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// The canonical text of `sql`'s tree, written after what `hold` gives
    /// back for the tree's length, at most `max_output` bytes. `hold` runs
    /// before the tree is read, so it can charge for it first.
    pub async fn parse<E>(
        &self,
        sql: &str,
        max_output: usize,
        within: Duration,
        hold: impl FnOnce(usize) -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, ParseFailure<E>> {
        let started = Instant::now();
        let outcome = self.parse_timed(sql, max_output, within, hold).await;
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
        sql: &str,
        max_output: usize,
        within: Duration,
        hold: impl FnOnce(usize) -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, ParseFailure<E>> {
        bounds::check(sql)?;
        if !self.is_ready() {
            return Err(ParseError::unavailable().into());
        }
        let deadline = tokio::time::Instant::now() + within.min(self.settings.timeout);
        let _turn = tokio::time::timeout_at(deadline, self.turns.acquire())
            .await
            .ok()
            .and_then(Result::ok)
            .ok_or_else(ParseError::unavailable)?;
        let mut process = tokio::time::timeout_at(deadline, self.take_process())
            .await
            .map_err(|_| ParseError::unavailable())??;
        match tokio::time::timeout_at(deadline, process.exchange(sql, max_output, hold)).await {
            Ok(Ok(exchanged)) => {
                self.lock_idle().push(process);
                match exchanged {
                    Exchanged::Tree(tree) => Ok(tree),
                    Exchanged::Refused(error) => Err(error.into()),
                    Exchanged::Unheld(unheld) => Err(ParseFailure::Unheld(unheld)),
                }
            }
            Ok(Err(fault)) => Err(lost("the parser process failed", &fault).into()),
            Err(_) => Err(lost("the parser process did not answer in time", &"timed out").into()),
        }
    }

    /// A live idle process, or a new one unless the last one to start
    /// failed less than its backoff ago.
    async fn take_process(&self) -> Result<Process, ParseError> {
        let idle = std::iter::from_fn(|| self.lock_idle().pop()).find_map(|mut process| {
            process.is_alive().then_some(process).or_else(|| {
                lost("an idle parser process ended", &"exited");
                None
            })
        });
        if let Some(process) = idle {
            return Ok(process);
        }
        if self
            .lock_backoff()
            .until
            .is_some_and(|until| Instant::now() < until)
        {
            return Err(ParseError::unavailable());
        }
        match Process::spawn(&self.settings).await {
            Ok(process) => {
                *self.lock_backoff() = Backoff::default();
                Ok(process)
            }
            Err(error) => {
                let mut backoff = self.lock_backoff();
                let delay = FIRST_BACKOFF
                    .saturating_mul(1 << backoff.failures.min(16))
                    .min(MAX_BACKOFF);
                backoff.failures = backoff.failures.saturating_add(1);
                backoff.until = Some(Instant::now() + delay);
                metrics::counter!("cufflink_sql_parser_failures_total", "stage" => "start")
                    .increment(1);
                tracing::warn!(%error, ?delay, "a SQL parser process did not start");
                Err(ParseError::unavailable())
            }
        }
    }

    fn lock_idle(&self) -> std::sync::MutexGuard<'_, Vec<Process>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_backoff(&self) -> std::sync::MutexGuard<'_, Backoff> {
        self.backoff.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A process that failed is dropped, which kills it; the next call starts
/// another.
fn lost(what: &str, error: &dyn std::fmt::Display) -> ParseError {
    metrics::counter!("cufflink_sql_parser_failures_total", "stage" => "parse").increment(1);
    tracing::warn!(%error, "{what}");
    ParseError::unavailable()
}

enum Exchanged<E> {
    Tree(Vec<u8>),
    Refused(ParseError),
    Unheld(E),
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

    async fn exchange<E>(
        &mut self,
        sql: &str,
        max_output: usize,
        hold: impl FnOnce(usize) -> Result<Vec<u8>, E>,
    ) -> io::Result<Exchanged<E>> {
        let request = Request {
            max_output: u64::try_from(max_output).unwrap_or(u64::MAX),
        };
        let mut frames = Vec::with_capacity(sql.len() + 64);
        wire::write_message(&mut frames, &request)?;
        wire::write_frame(&mut frames, sql.as_bytes())?;
        self.stdin.write_all(&frames).await?;
        self.stdin.flush().await?;
        match self.message().await? {
            Answer::Error(error) => Ok(Exchanged::Refused(error)),
            Answer::Tree => {
                let len = self.frame_len(max_output).await?;
                let mut out = match hold(len) {
                    Ok(out) => out,
                    Err(unheld) => {
                        let skip = u64::try_from(len).unwrap_or(u64::MAX);
                        let skipped = tokio::io::copy(
                            &mut (&mut self.stdout).take(skip),
                            &mut tokio::io::sink(),
                        )
                        .await?;
                        return match skipped == skip {
                            true => Ok(Exchanged::Unheld(unheld)),
                            false => Err(io::ErrorKind::UnexpectedEof.into()),
                        };
                    }
                };
                let start = out.len();
                out.resize(start + len, 0);
                self.stdout.read_exact(&mut out[start..]).await?;
                Ok(Exchanged::Tree(out))
            }
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
                "SQL_PARSE_MEMORY_MB" => Some("512"),
                "SQL_PARSE_TIMEOUT_MS" => Some("250"),
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
                memory_mb: 512,
                timeout: Duration::from_millis(250),
            }
        );
        for name in [
            "SQL_PARSE_PROCESSES",
            "SQL_PARSE_MEMORY_MB",
            "SQL_PARSE_TIMEOUT_MS",
        ] {
            assert!(
                SqlParseSettings::from_vars(|n| (n == name).then(|| "0".to_string())).is_err(),
                "{name}"
            );
        }
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
            .parse("SELECT 1", 1024, Duration::from_secs(1), |_| {
                Ok::<_, ()>(Vec::new())
            })
            .await;
        assert!(matches!(
            refused,
            Err(ParseFailure::Refused(ParseError::Unavailable(_)))
        ));
        let bounded = parser
            .parse(
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
