//! Between the platform and a parser process, over the process's stdin and
//! stdout: frames of a 4-byte big-endian length and that many bytes.
//!
//! - The process starts by sending a [`Hello`].
//! - Each request is a [`Request`] frame, then a frame of the statement.
//! - Each answer is an [`Answer`] frame, and after [`Answer::Tree`] a frame
//!   of the tree's canonical text.

use cufflink_types::sql_parse::ParseError;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

/// The most a [`Hello`], [`Request`] or [`Answer`] frame holds.
pub const MAX_CONTROL_FRAME: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub parser: String,
    pub fingerprint: String,
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
