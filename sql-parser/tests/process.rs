use cufflink_sql_parse::bounds::{
    MAX_BRACKET_DEPTH, MAX_CHAIN_DEPTH, MAX_INPUT_BYTES, MAX_WARNED_FORMS,
};
use cufflink_sql_parse::wire::{read_frame, read_message, write_frame, write_message};
use cufflink_sql_parse::wire::{Answer, Hello, Request};
use cufflink_sql_parse::{golden, parse, PROBE, PROBE_FINGERPRINT};
use std::io::Write;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

struct Parser {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
}

impl Parser {
    fn start() -> Self {
        Self::start_with(Stdio::null())
    }

    fn start_with(stderr: Stdio) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_cufflink-sql-parser"))
            .env_clear()
            .env("SQL_PARSE_MEMORY_MB", "1024")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .expect("spawned");
        let input = child.stdin.take().expect("stdin");
        let output = child.stdout.take().expect("stdout");
        Self {
            child,
            input,
            output,
        }
    }

    fn hello(&mut self) -> Hello {
        read_message(&mut self.output)
            .expect("read")
            .expect("hello")
    }

    fn answer(&mut self, sql: &str) -> Result<String, cufflink_types::sql_parse::ParseError> {
        write_message(
            &mut self.input,
            &Request {
                max_output: u64::MAX,
            },
        )
        .expect("request");
        write_frame(&mut self.input, sql.as_bytes()).expect("statement");
        self.input.flush().expect("flushed");
        match read_message(&mut self.output)
            .expect("read")
            .expect("answer")
        {
            Answer::Tree => Ok(String::from_utf8(
                read_frame(&mut self.output, usize::MAX)
                    .expect("read")
                    .expect("tree"),
            )
            .expect("text")),
            Answer::Error(error) => Err(error),
        }
    }

    #[cfg(target_os = "linux")]
    fn proc_field(&self, file: &str, name: &str) -> Option<String> {
        let text = std::fs::read_to_string(format!("/proc/{}/{file}", self.child.id()))
            .expect("proc file");
        text.lines()
            .find_map(|line| line.strip_prefix(name))
            .map(|value| value.trim().to_string())
    }

    #[cfg(target_os = "linux")]
    fn assert_confined(&self) {
        assert_eq!(self.proc_field("status", "Seccomp:").as_deref(), Some("2"));
        assert_eq!(
            self.proc_field("status", "NoNewPrivs:").as_deref(),
            Some("1")
        );
        assert_eq!(
            self.proc_field("oom_score_adj", "").as_deref(),
            Some("1000")
        );
    }

    fn end(mut self) {
        drop(self.input);
        assert!(self.child.wait().expect("waited").success());
    }
}

#[test]
fn the_built_process_greets_answers_and_ends_with_its_input() {
    let mut parser = Parser::start();
    assert_eq!(parser.hello(), Hello::of_this_build(PROBE_FINGERPRINT));
    assert_eq!(parser.answer("SELECT 1"), parse("SELECT 1", usize::MAX));
    parser.end();
}

/// Everything a parse can do, in the confined process: the corpus, every
/// refusal, the deepest inputs the bounds let through, and the golden
/// corpus's committed answers. A system call
/// missing from the allowlist ends the process, and the test with it.
#[test]
fn the_confined_process_answers_as_the_library_does() {
    let mut parser = Parser::start();
    parser.hello();
    #[cfg(target_os = "linux")]
    parser.assert_confined();
    let brackets = MAX_BRACKET_DEPTH as usize;
    let links = MAX_CHAIN_DEPTH as usize;
    let statements = PROBE
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_string)
        .chain([
            "SELEC 1".to_string(),
            "SELECT 'open".to_string(),
            format!("SELECT 1{}", "+1".repeat(40)),
            format!("SELECT 1{}", "+1".repeat(links)),
            format!("SELECT 1{}", "+1".repeat(links / 2 - 2)),
            format!(
                "SELECT {}1{}",
                "f(".repeat(brackets - 1),
                ")".repeat(brackets - 1)
            ),
            filled("SELECT 1 WHERE x IN (1", ",1", ")"),
        ]);
    for sql in statements {
        assert_eq!(
            parser.answer(&sql),
            parse(&sql, usize::MAX),
            "{}",
            &sql[..20.min(sql.len())]
        );
    }
    for (sql, expected) in golden::cases() {
        assert_eq!(
            golden::answer(&parser.answer(sql)),
            expected,
            "{}",
            &sql[..20.min(sql.len())]
        );
    }
    parser.end();
}

/// A confined process may still report on its stderr: a request it cannot
/// read ends it with its own failure, not with the allowlist's kill.
#[test]
fn the_confined_process_reports_a_broken_request_on_stderr_and_fails() {
    let mut parser = Parser::start_with(Stdio::piped());
    let mut stderr = parser.child.stderr.take().expect("stderr");
    parser.hello();
    #[cfg(target_os = "linux")]
    parser.assert_confined();
    parser
        .input
        .write_all(&u32::MAX.to_be_bytes())
        .expect("header");
    drop(parser.input);
    let status = parser.child.wait().expect("waited");
    let mut report = String::new();
    std::io::Read::read_to_string(&mut stderr, &mut report).expect("stderr");
    assert_eq!(status.code(), Some(1), "{status}");
    assert!(report.starts_with("cufflink-sql-parser: "), "{report}");
}

/// `prefix`, as many `unit`s as fit, and `suffix`: a statement of
/// [`MAX_INPUT_BYTES`], less at most one unit.
fn filled(prefix: &str, unit: &str, suffix: &str) -> String {
    let units = (MAX_INPUT_BYTES - prefix.len() - suffix.len()) / unit.len();
    format!("{prefix}{}{suffix}", unit.repeat(units))
}

/// The shapes whose parse costs most for their length, each at the input
/// bound.
fn costliest() -> Vec<(&'static str, String)> {
    let warned = "CREATE GLOBAL TEMP TABLE t();".repeat(MAX_WARNED_FORMS as usize);
    vec![
        ("columns", filled("SELECT a", ",a", "")),
        ("stars", filled("SELECT *", ",*", "")),
        ("ordering", filled("SELECT 1 ORDER BY a", ",a", "")),
        ("arguments", filled("SELECT f(a", ",a", ")")),
        ("subscripts", filled("SELECT a[1]", ",a[1]", "")),
        ("in list", filled("SELECT 1 WHERE x IN (1", ",1", ")")),
        ("conjunction", filled("SELECT 1 WHERE x=1", " AND x=1", "")),
        ("rows", filled("VALUES (1)", ",(1)", "")),
        ("statements", filled("", "SELECT 1;", "")),
        (
            "column definitions",
            filled("CREATE TABLE t (a int", ",a int", ")"),
        ),
        (
            "warnings at the end",
            filled("-- ", "x", &format!("\n{warned}")),
        ),
    ]
}

/// What a platform allows one parse by default; the costliest statements
/// must answer well within it.
const PARSE_TIMEOUT: Duration = Duration::from_secs(5);

/// The costliest statements the bounds let through, in the confined
/// process: each answers its tree, as the library does, within half the
/// platform's parse timeout, and on Linux the process's resident peak stays
/// under half its address space; a parse past that space would end it.
#[test]
fn the_costliest_statements_within_the_bounds_answer_in_time_and_memory() {
    let mut parser = Parser::start();
    parser.hello();
    #[cfg(target_os = "linux")]
    parser.assert_confined();
    for (name, sql) in costliest() {
        assert!(
            (MAX_INPUT_BYTES - 64..=MAX_INPUT_BYTES).contains(&sql.len()),
            "{name}: {}",
            sql.len()
        );
        let started = Instant::now();
        let answer = parser.answer(&sql);
        let took = started.elapsed();
        assert!(answer.is_ok(), "{name}: {answer:?}");
        assert!(took < PARSE_TIMEOUT / 2, "{name}: {took:?}");
        assert!(answer == parse(&sql, usize::MAX), "{name}");
    }
    let past = "CREATE GLOBAL TEMP TABLE t();".repeat(MAX_WARNED_FORMS as usize + 1);
    assert!(matches!(
        parser.answer(&past),
        Err(cufflink_types::sql_parse::ParseError::TooLarge(_))
    ));
    #[cfg(target_os = "linux")]
    {
        let peak = parser.proc_field("status", "VmHWM:").expect("VmHWM");
        let peak_kib: u64 = peak
            .trim_end_matches("kB")
            .trim()
            .parse()
            .expect("a size in kB");
        assert!(
            peak_kib < 512 * 1024,
            "the parser process reached {peak_kib} KiB, over half of SQL_PARSE_MEMORY_MB"
        );
    }
    parser.end();
}

/// A descriptor the process is started with, here the write end of a pipe
/// the test holds the read end of, is closed before the process greets: the
/// pipe ends as soon as the test closes its own copy.
#[cfg(target_os = "linux")]
#[test]
fn the_process_closes_every_descriptor_it_inherits() {
    let mut ends = [0; 2];
    // SAFETY: pipe2 writes two descriptors into the array it is given.
    assert_eq!(
        unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) },
        0
    );
    let [read, write] = ends;
    // SAFETY: F_SETFD on a descriptor this test owns.
    assert_eq!(unsafe { libc::fcntl(write, libc::F_SETFD, 0) }, 0);
    let mut parser = Parser::start();
    parser.hello();
    // SAFETY: closing the test's own copy of the write end.
    assert_eq!(unsafe { libc::close(write) }, 0);
    let mut ready = libc::pollfd {
        fd: read,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: poll reads and writes the one pollfd it is given.
    let polled = unsafe { libc::poll(&mut ready, 1, 5_000) };
    let mut byte = 0u8;
    // SAFETY: read writes at most one byte into `byte`.
    let read_len = unsafe { libc::read(read, (&mut byte as *mut u8).cast(), 1) };
    // SAFETY: closing the test's own read end.
    unsafe { libc::close(read) };
    assert_eq!(polled, 1, "the pipe is still open in the parser process");
    assert_eq!(read_len, 0);
    parser.end();
}
