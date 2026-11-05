use cufflink_sql_parse::bounds::{MAX_BRACKET_DEPTH, MAX_CHAIN_DEPTH};
use cufflink_sql_parse::wire::{read_frame, read_message, write_frame, write_message};
use cufflink_sql_parse::wire::{Answer, Hello, Request};
use cufflink_sql_parse::{parse, PARSER, PROBE, PROBE_FINGERPRINT};
use std::io::Write;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

struct Parser {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
}

impl Parser {
    fn start() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_cufflink-sql-parser"))
            .env_clear()
            .env("SQL_PARSE_MEMORY_MB", "1024")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
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

    fn end(mut self) {
        drop(self.input);
        assert!(self.child.wait().expect("waited").success());
    }
}

#[test]
fn the_built_process_greets_answers_and_ends_with_its_input() {
    let mut parser = Parser::start();
    assert_eq!(
        parser.hello(),
        Hello {
            parser: PARSER.to_string(),
            fingerprint: PROBE_FINGERPRINT.to_string(),
        }
    );
    assert_eq!(parser.answer("SELECT 1"), parse("SELECT 1", usize::MAX));
    parser.end();
}

/// Everything a parse can do, in the confined process: the corpus, every
/// refusal, and the deepest inputs the bounds let through. A system call
/// missing from the allowlist ends the process, and the test with it.
#[test]
fn the_confined_process_answers_as_the_library_does() {
    let mut parser = Parser::start();
    parser.hello();
    #[cfg(target_os = "linux")]
    {
        let status =
            std::fs::read_to_string(format!("/proc/{}/status", parser.child.id())).expect("status");
        let field = |name: &str| {
            status
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .map(str::trim)
                .map(str::to_string)
        };
        assert_eq!(field("Seccomp:").as_deref(), Some("2"), "{status}");
        assert_eq!(field("NoNewPrivs:").as_deref(), Some("1"), "{status}");
    }
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
            format!("SELECT 1 WHERE x IN (1{})", ",1".repeat(100_000)),
        ]);
    for sql in statements {
        assert_eq!(
            parser.answer(&sql),
            parse(&sql, usize::MAX),
            "{}",
            &sql[..20.min(sql.len())]
        );
    }
    parser.end();
}
