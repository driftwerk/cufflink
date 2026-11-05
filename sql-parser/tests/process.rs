use cufflink_sql_parse::wire::{read_frame, read_message, write_frame, write_message};
use cufflink_sql_parse::wire::{Answer, Hello, Request};
use cufflink_sql_parse::{parse, PARSER, PROBE_FINGERPRINT};
use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn the_built_process_greets_answers_and_ends_with_its_input() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_cufflink-sql-parser"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawned");
    let mut input = child.stdin.take().expect("stdin");
    let mut output = child.stdout.take().expect("stdout");

    let hello: Hello = read_message(&mut output).expect("read").expect("hello");
    assert_eq!(
        hello,
        Hello {
            parser: PARSER.to_string(),
            fingerprint: PROBE_FINGERPRINT.to_string(),
        }
    );

    write_message(
        &mut input,
        &Request {
            max_output: u64::MAX,
        },
    )
    .expect("request");
    write_frame(&mut input, b"SELECT 1").expect("statement");
    input.flush().expect("flushed");
    assert_eq!(read_message(&mut output).expect("read"), Some(Answer::Tree));
    let tree = read_frame(&mut output, usize::MAX)
        .expect("read")
        .expect("tree");
    assert_eq!(
        Ok(String::from_utf8(tree).expect("text")),
        parse("SELECT 1", usize::MAX)
    );

    drop(input);
    assert!(child.wait().expect("waited").success());
}
