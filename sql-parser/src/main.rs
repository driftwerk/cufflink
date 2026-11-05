//! A parser process: it answers a platform's parse requests one at a time,
//! over stdin and stdout (`cufflink_sql_parse::wire`), so that a fault in
//! the parser ends this process and not the platform.
//!
//! It confines itself before it greets ([`sandbox`]): a platform never
//! sends a statement to a process that could not.

mod sandbox;

use cufflink_sql_parse::bounds::MAX_INPUT_BYTES;
use cufflink_sql_parse::wire::{read_frame, read_message, write_frame, write_message};
use cufflink_sql_parse::wire::{Answer, Hello, Request};
use cufflink_sql_parse::{fingerprint_with, parse_on_this_thread, PARSER, STACK_BYTES};
use cufflink_types::sql_parse::ParseError;
use std::io::{self, BufWriter, Read, Write};
use std::process::ExitCode;
use std::sync::mpsc;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("cufflink-sql-parser: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> io::Result<()> {
    sandbox::confine()?;
    let thread = ParseThread::spawn()?;
    let hello = Hello {
        parser: PARSER.to_string(),
        fingerprint: thread.warm_up()?,
    };
    sandbox::enter()?;
    serve(
        &hello,
        |sql, max_output| thread.parse(sql, max_output),
        &mut io::stdin().lock(),
        &mut BufWriter::new(io::stdout().lock()),
    )
}

/// Greets with `hello`, then answers each request in turn with `parse`
/// until the input ends.
fn serve(
    hello: &Hello,
    mut parse: impl FnMut(String, usize) -> io::Result<Result<String, ParseError>>,
    input: &mut impl Read,
    out: &mut impl Write,
) -> io::Result<()> {
    write_message(out, hello)?;
    out.flush()?;
    while let Some(request) = read_message::<Request>(input)? {
        let sql = read_frame(input, MAX_INPUT_BYTES)?
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let max_output = usize::try_from(request.max_output).unwrap_or(usize::MAX);
        let answer = match String::from_utf8(sql) {
            Ok(sql) => parse(sql, max_output)?,
            Err(_) => Err(ParseError::syntax()),
        };
        match answer {
            Ok(text) => {
                write_message(out, &Answer::Tree)?;
                write_frame(out, text.as_bytes())?;
            }
            Err(error) => write_message(out, &Answer::Error(error))?,
        }
        out.flush()?;
        sandbox::release_memory();
    }
    Ok(())
}

/// The one thread every parse runs on, started before the process
/// confines itself, so no thread is started after.
struct ParseThread {
    requests: mpsc::Sender<(String, usize)>,
    answers: mpsc::Receiver<Result<String, ParseError>>,
}

impl ParseThread {
    fn spawn() -> io::Result<Self> {
        let (requests, jobs) = mpsc::channel::<(String, usize)>();
        let (answer, answers) = mpsc::channel();
        std::thread::Builder::new()
            .name("sql-parse".into())
            .stack_size(STACK_BYTES)
            .spawn(move || {
                for (sql, max_output) in jobs {
                    if answer.send(parse_on_this_thread(&sql, max_output)).is_err() {
                        return;
                    }
                }
            })?;
        Ok(Self { requests, answers })
    }

    /// The fingerprint, parsed on this thread, and a refused statement:
    /// whatever the first parse, the first refusal and the first wait for
    /// an answer set up, both threads set up before the process confines
    /// itself.
    fn warm_up(&self) -> io::Result<String> {
        let mut failed = None;
        let fingerprint = fingerprint_with(|sql| match self.parse(sql.into(), usize::MAX) {
            Ok(answer) => answer,
            Err(e) => {
                failed = Some(e);
                Err(ParseError::unavailable())
            }
        });
        if let Some(e) = failed {
            return Err(e);
        }
        match self.parse("SELEC".into(), usize::MAX)? {
            Err(ParseError::Syntax(_)) => fingerprint.map_err(io::Error::other),
            _ => Err(io::Error::other(
                "a statement that is not SQL was not refused",
            )),
        }
    }

    fn parse(&self, sql: String, max_output: usize) -> io::Result<Result<String, ParseError>> {
        let ended = || io::Error::other("the parse thread ended");
        self.requests.send((sql, max_output)).map_err(|_| ended())?;
        self.answers.recv().map_err(|_| ended())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_sql_parse::parse;

    fn request(sql: &[u8], max_output: u64) -> Vec<u8> {
        let mut wire = Vec::new();
        write_message(&mut wire, &Request { max_output }).expect("request");
        write_frame(&mut wire, sql).expect("statement");
        wire
    }

    fn hello() -> Hello {
        Hello {
            parser: PARSER.to_string(),
            fingerprint: cufflink_sql_parse::fingerprint()
                .expect("fingerprint")
                .to_string(),
        }
    }

    fn served(input: &[u8]) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        serve(
            &hello(),
            |sql, max_output| Ok(parse(&sql, max_output)),
            &mut &input[..],
            &mut out,
        )
        .map(|()| out)
    }

    #[test]
    fn it_greets_then_answers_each_request_in_turn() {
        let input = [
            request(b"SELECT 1", u64::MAX),
            request(b"SELEC 1", u64::MAX),
            request(&[b'S', 0xff], u64::MAX),
            request(b"SELECT 1", 2),
        ]
        .concat();
        let out = served(&input).expect("served");
        let mut answers = out.as_slice();
        let greeting: Hello = read_message(&mut answers).expect("read").expect("hello");
        assert_eq!(greeting, hello());
        assert_eq!(
            read_message(&mut answers).expect("read"),
            Some(Answer::Tree)
        );
        let tree = read_frame(&mut answers, usize::MAX)
            .expect("read")
            .expect("tree");
        assert_eq!(
            Ok(String::from_utf8(tree).expect("text")),
            parse("SELECT 1", usize::MAX)
        );
        for expected in [
            Answer::Error(ParseError::syntax()),
            Answer::Error(ParseError::syntax()),
        ] {
            assert_eq!(read_message(&mut answers).expect("read"), Some(expected));
        }
        assert!(matches!(
            read_message(&mut answers).expect("read"),
            Some(Answer::Error(ParseError::TooLarge(_)))
        ));
        assert_eq!(read_message::<Answer>(&mut answers).expect("end"), None);
    }

    #[test]
    fn a_statement_past_the_input_limit_ends_the_process() {
        let mut wire = Vec::new();
        write_message(&mut wire, &Request { max_output: 1 }).expect("request");
        wire.extend_from_slice(
            &u32::try_from(MAX_INPUT_BYTES + 1)
                .expect("fits")
                .to_be_bytes(),
        );
        assert!(served(&wire).is_err());
        let cut = request(b"SELECT 1", 1);
        assert!(served(&cut[..cut.len() - 1]).is_err());
    }

    #[test]
    fn the_parse_thread_answers_in_turn_as_a_parse_does() {
        let thread = ParseThread::spawn().expect("spawned");
        assert_eq!(
            thread.warm_up().expect("warmed up"),
            cufflink_sql_parse::PROBE_FINGERPRINT
        );
        for sql in ["SELECT 1", "SELEC 1", "SELECT 2"] {
            assert_eq!(
                thread.parse(sql.into(), usize::MAX).expect("answered"),
                parse(sql, usize::MAX)
            );
        }
    }
}
