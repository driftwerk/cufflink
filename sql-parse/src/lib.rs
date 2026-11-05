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
pub use parse::{fingerprint, fingerprint_with, parse, parse_on_this_thread, parsed, STACK_BYTES};

/// The `pg_query` release that parses, which fixes PostgreSQL's grammar.
pub const PARSER: &str = "6.2.1";

/// The probe corpus: one statement a line.
pub const PROBE: &str = include_str!("../probe.sql");

/// The SHA-256 of [`PROBE`]'s trees as [`PARSER`] reads them: for each
/// statement in order, its tree's canonical text and a `\n`. A parser
/// process whose fingerprint differs is not used.
pub const PROBE_FINGERPRINT: &str =
    "98e4c9b72f500c15ea6a9d2c34124cc5a5aedf1b1d5a33e94754473b1a1c2030";

/// The deepest a tree's text may nest, in objects and arrays. A guest
/// reads the answer with `serde_json`, which stops at 128 levels, and the
/// answer wraps the tree in two more; a deeper tree is `TooLarge`.
pub const MAX_TREE_NESTING: usize = 120;

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

    fn nested(levels: usize) -> String {
        format!("{}{}", "[".repeat(levels), "]".repeat(levels))
    }

    #[test]
    fn a_tree_at_the_nesting_limit_is_read_back_and_a_deeper_one_is_not() {
        let (before, after) = answer_around();
        let decodes =
            |tree: &str| serde_json::from_str::<ParseResponse>(&format!("{before}{tree}{after}"));
        assert!(decodes(&nested(MAX_TREE_NESTING)).is_ok());
        assert!(decodes(&nested(MAX_TREE_NESTING + 8)).is_err());
    }
}
