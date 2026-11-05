//! A parser process: it answers a platform's parse requests one at a time,
//! over stdin and stdout (`cufflink_sql_parse::wire`), so that a fault in
//! the parser ends this process and not the platform.
//!
//! On Linux it caps its own address space at `SQL_PARSE_MEMORY_MB` and
//! asks to be the first process the kernel kills when memory runs out.

use cufflink_sql_parse::bounds::MAX_INPUT_BYTES;
use cufflink_sql_parse::wire::{read_frame, read_message, write_frame, write_message};
use cufflink_sql_parse::wire::{Answer, Hello, Request};
use cufflink_sql_parse::{fingerprint, parse, PARSER};
use cufflink_types::sql_parse::ParseError;
use std::io::{self, BufWriter, Read, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    if let Err(e) = confine() {
        eprintln!("cufflink-sql-parser: {e}");
        return ExitCode::FAILURE;
    }
    match serve(
        &mut io::stdin().lock(),
        &mut BufWriter::new(io::stdout().lock()),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("cufflink-sql-parser: {e}");
            ExitCode::FAILURE
        }
    }
}

fn serve(input: &mut impl Read, out: &mut impl Write) -> io::Result<()> {
    let hello = Hello {
        parser: PARSER.to_string(),
        fingerprint: fingerprint().map_err(io::Error::other)?.to_string(),
    };
    write_message(out, &hello)?;
    out.flush()?;
    while let Some(request) = read_message::<Request>(input)? {
        let sql = read_frame(input, MAX_INPUT_BYTES)?
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let max_output = usize::try_from(request.max_output).unwrap_or(usize::MAX);
        let answer = std::str::from_utf8(&sql)
            .map_err(|_| ParseError::syntax())
            .and_then(|sql| parse(sql, max_output));
        match answer {
            Ok(text) => {
                write_message(out, &Answer::Tree)?;
                write_frame(out, text.as_bytes())?;
            }
            Err(error) => write_message(out, &Answer::Error(error))?,
        }
        out.flush()?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn confine() -> io::Result<()> {
    if let Some(mb) = memory_limit_mb()? {
        let bytes =
            libc::rlim_t::try_from(mb.saturating_mul(1024 * 1024)).unwrap_or(libc::RLIM_INFINITY);
        let limit = libc::rlimit {
            rlim_cur: bytes,
            rlim_max: bytes,
        };
        // SAFETY: setrlimit reads the struct it is given and nothing else.
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    std::fs::write("/proc/self/oom_score_adj", "1000")
}

#[cfg(not(target_os = "linux"))]
fn confine() -> io::Result<()> {
    memory_limit_mb().map(drop)
}

fn memory_limit_mb() -> io::Result<Option<u64>> {
    match std::env::var("SQL_PARSE_MEMORY_MB") {
        Ok(mb) => mb
            .parse::<u64>()
            .ok()
            .filter(|mb| *mb > 0)
            .map(Some)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SQL_PARSE_MEMORY_MB is not a positive number",
                )
            }),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(sql: &[u8], max_output: u64) -> Vec<u8> {
        let mut wire = Vec::new();
        write_message(&mut wire, &Request { max_output }).expect("request");
        write_frame(&mut wire, sql).expect("statement");
        wire
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
        let mut out = Vec::new();
        serve(&mut input.as_slice(), &mut out).expect("served");
        let mut answers = out.as_slice();
        let hello: Hello = read_message(&mut answers).expect("read").expect("hello");
        assert_eq!(hello.parser, PARSER);
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
        assert!(serve(&mut wire.as_slice(), &mut Vec::new()).is_err());
        let cut = request(b"SELECT 1", 1);
        assert!(serve(&mut &cut[..cut.len() - 1], &mut Vec::new()).is_err());
    }
}
