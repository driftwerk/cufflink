//! PostgreSQL's own parser for Cufflink guests that accept SQL they did not
//! write.
//!
//! - [`bounds`]: what is refused before the parser runs.
//! - [`wire`]: how the platform talks to a parser process.
//! - With the `parser` feature, the parse itself (`pg_query`), which the
//!   parser process and the native test host run.

pub mod bounds;
#[cfg(feature = "parser")]
mod parse;
pub mod wire;

#[cfg(feature = "parser")]
pub use parse::{fingerprint, parse, parsed, STACK_BYTES};

/// The `pg_query` release that parses, which fixes PostgreSQL's grammar.
pub const PARSER: &str = "6.2.1";

/// The probe corpus: one statement a line.
pub const PROBE: &str = include_str!("../probe.sql");

/// The SHA-256 of [`PROBE`]'s trees as [`PARSER`] reads them: for each
/// statement in order, its tree's canonical text and a `\n`. A parser
/// process whose fingerprint differs is not used.
pub const PROBE_FINGERPRINT: &str =
    "31ade59b7c0a7ba53a2ee8c99473df4cc65c2c4d0112996d791bb19b89f0c9bc";

/// What surrounds a tree's canonical text in the `sql_parse` host
/// function's answer, a `ParseResponse::Ok`, so that the tree is written
/// into the answer as it is read and never parsed again on the way.
pub fn answer_around() -> (String, &'static str) {
    (
        format!(r#"{{"ok":{{"fingerprint":"{PROBE_FINGERPRINT}","parser":"{PARSER}","tree":"#),
        "}}",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_types::sql_parse::{ParseResponse, Parsed};

    #[test]
    fn an_answer_around_a_tree_is_the_ok_response() {
        let (before, after) = answer_around();
        let answer = format!(r#"{before}{{"stmts":[],"version":170007}}{after}"#);
        assert_eq!(
            serde_json::from_str::<ParseResponse>(&answer).expect("decodes"),
            ParseResponse::Ok(Parsed {
                tree: serde_json::json!({"stmts": [], "version": 170007}),
                parser: PARSER.to_string(),
                fingerprint: PROBE_FINGERPRINT.to_string(),
            })
        );
    }
}
