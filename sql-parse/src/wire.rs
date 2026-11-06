//! Between the platform and a parser process, over the process's stdin and
//! stdout: frames of a 4-byte big-endian length and that many bytes.
//!
//! - The process starts by sending a [`Hello`], which the platform takes
//!   only if it is [`Hello::of_this_build`] with the published fingerprint.
//! - Each request is a [`Request`] frame, then a frame of the statement.
//! - Each answer is an [`Answer`] frame, and after [`Answer::Tree`] a frame
//!   of the tree's canonical text.

use cufflink_types::sql_parse::ParseError;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

/// The most a [`Hello`], [`Request`] or [`Answer`] frame holds.
pub const MAX_CONTROL_FRAME: usize = 64 * 1024;

/// The version of this exchange: its frames and messages, the bounds both
/// sides apply and the tree's canonical text. It changes whenever any of
/// them does, so a platform never uses a process built to other terms.
pub const PROTOCOL: u32 = 1;

/// What a process greets with: the terms it was built to, and its parser's
/// identity, which it computes by parsing the probe corpus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol: u32,
    /// The `cufflink-sql-parse` release the process was built with.
    pub build: String,
    pub parser: String,
    pub fingerprint: String,
}

impl Hello {
    /// The greeting of a process built with this crate whose parser reads
    /// the probe corpus as `fingerprint`.
    pub fn of_this_build(fingerprint: impl Into<String>) -> Self {
        Self {
            protocol: PROTOCOL,
            build: env!("CARGO_PKG_VERSION").to_string(),
            parser: crate::PARSER.to_string(),
            fingerprint: fingerprint.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    /// The longest tree text the platform takes; a longer one is
    /// `TooLarge`.
    pub max_output: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Answer {
    Tree,
    Error(ParseError),
}

pub fn write_frame(out: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "a frame is over 4 GiB"))?;
    out.write_all(&len.to_be_bytes())?;
    out.write_all(bytes)
}

pub fn write_message(out: &mut impl Write, message: &impl Serialize) -> io::Result<()> {
    let bytes = serde_json::to_vec(message).map_err(io::Error::other)?;
    write_frame(out, &bytes)
}

/// The next frame, `None` at a clean end of input, and an error for a
/// frame past `max` bytes, which is never read.
pub fn read_frame(input: &mut impl Read, max: usize) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; 4];
    match input.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = frame_len(header, max)?;
    let mut bytes = vec![0u8; len];
    input.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

/// The length a frame's header announces, refused past `max`.
pub fn frame_len(header: [u8; 4], max: usize) -> io::Result<usize> {
    usize::try_from(u32::from_be_bytes(header))
        .ok()
        .filter(|len| *len <= max)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "a frame is over its limit"))
}

pub fn read_message<T: for<'de> Deserialize<'de>>(input: &mut impl Read) -> io::Result<Option<T>> {
    read_frame(input, MAX_CONTROL_FRAME)?
        .map(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_and_refuse_what_is_past_their_limit() {
        let mut wire = Vec::new();
        write_message(&mut wire, &Request { max_output: 9 }).expect("request");
        write_frame(&mut wire, b"SELECT 1").expect("statement");
        write_message(&mut wire, &Answer::Error(ParseError::syntax())).expect("answer");
        let mut input = wire.as_slice();
        assert_eq!(
            read_message::<Request>(&mut input).expect("read"),
            Some(Request { max_output: 9 })
        );
        assert_eq!(
            read_frame(&mut input, 8).expect("read").as_deref(),
            Some(&b"SELECT 1"[..])
        );
        assert_eq!(
            read_message::<Answer>(&mut input).expect("read"),
            Some(Answer::Error(ParseError::syntax()))
        );
        assert_eq!(read_frame(&mut input, 8).expect("end"), None);

        let mut long = Vec::new();
        write_frame(&mut long, b"SELECT 10").expect("statement");
        let refused = read_frame(&mut long.as_slice(), 8).expect_err("past the limit");
        assert_eq!(refused.kind(), io::ErrorKind::InvalidData);
        let cut = read_frame(&mut &long[..6], 16).expect_err("cut short");
        assert_eq!(cut.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn a_greeting_names_its_terms_and_one_without_them_is_not_read() {
        let hello = Hello::of_this_build("f");
        assert_eq!(
            serde_json::to_value(&hello).expect("encode"),
            serde_json::json!({
                "protocol": PROTOCOL,
                "build": env!("CARGO_PKG_VERSION"),
                "parser": crate::PARSER,
                "fingerprint": "f",
            })
        );
        let earlier = br#"{"parser":"6.2.1","fingerprint":"f"}"#;
        let mut wire = Vec::new();
        write_frame(&mut wire, earlier).expect("hello");
        assert!(read_message::<Hello>(&mut wire.as_slice()).is_err());
    }

    #[test]
    fn answers_are_small_and_tagged() {
        assert_eq!(
            serde_json::to_string(&Answer::Tree).expect("encode"),
            r#""tree""#
        );
        assert_eq!(
            serde_json::to_value(Answer::Error(ParseError::TooLarge("x".into()))).expect("encode"),
            serde_json::json!({"error": {"kind": "too_large", "message": "x"}})
        );
    }
}
