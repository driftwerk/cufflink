//! The parse itself: `pg_query` over a statement that passed the bounds, on
//! a thread with a stack sized for them, one at a time in the process.

use crate::{bounds, canonical, MAX_TREE_NESTING, PARSER, PROBE, PROBE_FINGERPRINT};
use cufflink_types::sql_parse::{ParseError, Parsed};
use sha2::{Digest, Sha256};
use std::sync::{Mutex, OnceLock, PoisonError};

/// The stack each parse runs on. The bounds keep a tree to a depth whose
/// parse needs a small part of it.
pub const STACK_BYTES: usize = 64 * 1024 * 1024;

/// Every parse redirects the process's stderr for its duration, so no two
/// may run at once.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// The tree of `sql` as canonical text: `serde_json::to_value` of
/// `pg_query`'s protobuf, written with sorted keys and no whitespace
/// ([`canonical`]). Refuses input past the bounds, a text past
/// `max_output` bytes, and one nesting deeper than [`MAX_TREE_NESTING`].
/// Runs on a thread of its own with [`STACK_BYTES`] of stack.
pub fn parse(sql: &str, max_output: usize) -> Result<String, ParseError> {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("sql-parse".into())
            .stack_size(STACK_BYTES)
            .spawn_scoped(scope, || parse_on_this_thread(sql, max_output))
            .map_err(|_| ParseError::unavailable())?
            .join()
            .map_err(|_| ParseError::unavailable())?
    })
}

/// [`parse`] on the calling thread, which must have [`STACK_BYTES`] of
/// stack, for a process that keeps one such thread for every parse. The
/// text is written as the tree is serialised, and stops at the first byte
/// or level past either limit.
pub fn parse_on_this_thread(sql: &str, max_output: usize) -> Result<String, ParseError> {
    bounds::check(sql)?;
    let parsed = {
        let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(PoisonError::into_inner);
        pg_query::parse(sql).map_err(refusal)?
    };
    canonical::to_text(&parsed.protobuf, max_output, MAX_TREE_NESTING).map_err(|refused| {
        match refused {
            canonical::Refusal::TooLong => {
                ParseError::TooLarge(format!("the parse tree is over {max_output} bytes"))
            }
            canonical::Refusal::TooDeep => ParseError::TooLarge(format!(
                "the parse tree nests deeper than {MAX_TREE_NESTING} levels"
            )),
            canonical::Refusal::Unsupported => ParseError::unavailable(),
        }
    })
}

/// [`parse`], read back as the tree a guest is given, with the release's
/// identity. A build whose parser reads the probe corpus differently from
/// the release is `Unavailable`, never answered with the release's name.
pub fn parsed(sql: &str, max_output: usize) -> Result<Parsed, ParseError> {
    if fingerprint()? != PROBE_FINGERPRINT {
        return Err(ParseError::Unavailable(
            "this build's parser reads the probe corpus differently from the release".into(),
        ));
    }
    let text = parse(sql, max_output)?;
    let tree = serde_json::from_str(&text).map_err(|_| ParseError::unavailable())?;
    Ok(Parsed {
        tree,
        parser: PARSER.to_string(),
        fingerprint: PROBE_FINGERPRINT.to_string(),
    })
}

/// The probe corpus's fingerprint as this process's parser computes it.
pub fn fingerprint() -> Result<&'static str, ParseError> {
    static COMPUTED: OnceLock<Result<String, ParseError>> = OnceLock::new();
    COMPUTED
        .get_or_init(|| fingerprint_with(|sql| parse(sql, usize::MAX)))
        .as_deref()
        .map_err(Clone::clone)
}

/// The probe corpus's fingerprint with each statement's tree from `tree`.
pub fn fingerprint_with(
    mut tree: impl FnMut(&str) -> Result<String, ParseError>,
) -> Result<String, ParseError> {
    PROBE
        .lines()
        .filter(|line| !line.trim().is_empty())
        .try_fold(Sha256::new(), |hash, sql| {
            tree(sql).map(|text| hash.chain_update(text).chain_update(b"\n"))
        })
        .map(|hash| hex::encode(hash.finalize()))
}

/// No message of the parser's reaches the guest: they quote the input. A
/// parse that ran out of memory, which PostgreSQL reports as an error of
/// its own, is too large rather than wrong.
fn refusal(error: pg_query::Error) -> ParseError {
    match error {
        pg_query::Error::Parse(message) if message.starts_with("out of memory") => {
            ParseError::TooLarge("the statement needs more memory than the parser has".into())
        }
        pg_query::Error::Parse(_) | pg_query::Error::Conversion(_) => ParseError::syntax(),
        pg_query::Error::Decode(_) => ParseError::TooLarge(
            "the statement nests deeper than the parser's tree decoder allows".into(),
        ),
        _ => ParseError::unavailable(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// The deepest the objects and arrays of JSON `text` nest.
    fn nesting(text: &str) -> usize {
        let mut depth = 0usize;
        let mut deepest = 0usize;
        let mut bytes = text.bytes();
        while let Some(byte) = bytes.next() {
            match byte {
                b'"' => {
                    while let Some(inner) = bytes.next() {
                        match inner {
                            b'\\' => {
                                bytes.next();
                            }
                            b'"' => break,
                            _ => {}
                        }
                    }
                }
                b'{' | b'[' => {
                    depth += 1;
                    deepest = deepest.max(depth);
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        deepest
    }

    fn own(sql: &str) -> Option<String> {
        pg_query::parse(sql).ok().map(|parsed| {
            serde_json::to_string(&serde_json::to_value(&parsed.protobuf).expect("tree"))
                .expect("text")
        })
    }

    #[test]
    fn the_tree_is_pg_querys_own_in_canonical_form() {
        let differing: Vec<_> = PROBE
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter(|sql| match (parse(sql, usize::MAX), own(sql)) {
                (Ok(text), Some(own)) => text != own,
                _ => true,
            })
            .collect();
        assert!(differing.is_empty(), "{differing:#?}");
        let tree: serde_json::Value =
            serde_json::from_str(&parse("SELECT 1", usize::MAX).expect("parses")).expect("json");
        assert_eq!(tree["version"], 170_007);
    }

    #[test]
    fn every_golden_statement_answers_its_committed_tree_or_refusal() {
        let differing: Vec<_> = crate::golden::cases()
            .filter(|(sql, expected)| crate::golden::answer(&parse(sql, usize::MAX)) != *expected)
            .map(|(sql, _)| &sql[..sql.len().min(60)])
            .collect();
        assert!(differing.is_empty(), "{differing:#?}");
    }

    /// Rewrites `golden/answers.txt` from this build's parser, for a release
    /// that changes the parser on purpose.
    #[test]
    #[ignore = "rewrites the committed golden answers"]
    fn bless_golden_answers() {
        let answers: String = crate::golden::statements()
            .map(|sql| crate::golden::answer(&parse(sql, usize::MAX)) + "\n")
            .collect();
        std::fs::write(
            concat!(env!("CARGO_MANIFEST_DIR"), "/golden/answers.txt"),
            answers,
        )
        .expect("written");
    }

    #[test]
    fn the_fingerprint_is_the_published_one() {
        assert_eq!(fingerprint(), Ok(PROBE_FINGERPRINT));
    }

    #[test]
    fn refused_statements_carry_no_part_of_the_input() {
        let sentinel = "zq_sentinel_9f";
        for sql in [
            format!("SELEC {sentinel}"),
            format!("SELECT '{sentinel}"),
            format!("SELECT $q${sentinel}"),
            format!("SELECT \"{sentinel}"),
            format!("SELECT 1 /* {sentinel}"),
            format!("SELECT {sentinel}\0"),
            format!("SELECT {}{sentinel}", "(".repeat(100_000)),
            format!("SELECT {sentinel}{}", "+1".repeat(60)),
        ] {
            let refused = parse(&sql, usize::MAX).expect_err("refused");
            assert!(!refused.to_string().contains(sentinel), "{refused}");
            assert!(
                matches!(refused, ParseError::Syntax(_) | ParseError::TooLarge(_)),
                "{refused}"
            );
        }
    }

    #[test]
    fn a_tree_past_the_output_limit_is_too_large() {
        let text = parse("SELECT 1", usize::MAX).expect("parses");
        assert_eq!(parse("SELECT 1", text.len()), Ok(text.clone()));
        assert!(matches!(
            parse("SELECT 1", text.len() - 1),
            Err(ParseError::TooLarge(_))
        ));
    }

    /// The deepest inputs each bound lets through, parsed on the stack every
    /// parse gets: each answers, in time.
    #[test]
    fn the_deepest_inputs_within_the_bounds_fit_the_stack() {
        let brackets = bounds::MAX_BRACKET_DEPTH as usize;
        let links = bounds::MAX_CHAIN_DEPTH as usize;
        let deepest = [
            format!("SELECT 1{}", "+1".repeat(links / 2 - 2)),
            format!("SELECT 1{}", "::int".repeat(links / 3 - 2)),
            format!("SELECT 1{}", " UNION SELECT 1".repeat(links / 4 - 2)),
            format!("SELECT 1 FROM a{}", " JOIN a ON true".repeat(links / 5 - 2)),
            format!(
                "SELECT 1{}",
                " BETWEEN 1 AND 1 IS TRUE".repeat(links / 6 - 2)
            ),
            format!("SELECT 1{}", " BETWEEN 1 AND NOT 1".repeat(links / 5 - 2)),
            format!("SELECT {}true", "NOT ".repeat(links - 3)),
            format!("SELECT {}1", "- ".repeat(links - 3)),
            format!(
                "SELECT {}1{}",
                "(SELECT ".repeat(brackets - 1),
                ")".repeat(brackets - 1)
            ),
            format!(
                "SELECT ARRAY{}1{}",
                "[".repeat(brackets - 1),
                "]".repeat(brackets - 1)
            ),
            format!(
                "SELECT {}1{}",
                "CASE WHEN true THEN ".repeat(brackets - 1),
                " END".repeat(brackets - 1)
            ),
            format!(
                "SELECT {}1{}",
                "f(".repeat(brackets - 1),
                ")".repeat(brackets - 1)
            ),
            format!(
                "SELECT {}1{}{}",
                "(".repeat(brackets / 2),
                "+1".repeat(links / 4),
                ")".repeat(brackets / 2)
            ),
        ];
        for sql in deepest {
            assert!(bounds::check(&sql).is_ok(), "{}", &sql[..30]);
            let started = Instant::now();
            let answer = parse(&sql, usize::MAX);
            assert!(
                matches!(
                    answer,
                    Ok(_) | Err(ParseError::Syntax(_) | ParseError::TooLarge(_))
                ),
                "{}: {answer:?}",
                &sql[..30]
            );
            assert!(
                started.elapsed() < Duration::from_millis(2_500),
                "{}: {:?}",
                &sql[..30],
                started.elapsed()
            );
        }
    }

    #[test]
    fn a_tree_nesting_past_the_limit_is_too_large() {
        let within = format!("SELECT 1{}", "+1".repeat(35));
        let text = parse(&within, usize::MAX).expect("parses");
        assert_eq!(nesting(&text), MAX_TREE_NESTING);
        let deeper = format!("SELECT 1{}", "+1".repeat(36));
        assert!(matches!(
            parse(&deeper, usize::MAX),
            Err(ParseError::TooLarge(message)) if message.contains("levels")
        ));
    }

    #[test]
    fn a_parse_out_of_memory_is_too_large_and_quotes_nothing() {
        let refused = refusal(pg_query::Error::Parse(
            "out of memory: Failed on request of size 1024 zq_sentinel_9f".into(),
        ));
        assert!(matches!(&refused, ParseError::TooLarge(_)), "{refused}");
        assert!(!refused.to_string().contains("zq_sentinel_9f"));
        assert_eq!(
            refusal(pg_query::Error::Parse(
                "syntax error at or near \"x\"".into()
            )),
            ParseError::syntax()
        );
    }

    #[test]
    fn nesting_counts_brackets_outside_strings() {
        assert_eq!(nesting("1"), 0);
        assert_eq!(nesting(r#"{"a":[{"b":"[[[\"{{"}]}"#), 3);
        assert_eq!(nesting(r#"["\\",["x"]]"#), 2);
    }

    #[test]
    fn parses_take_turns_across_threads() {
        let answers: Vec<_> = std::thread::scope(|scope| {
            (0..8)
                .map(|n| scope.spawn(move || parse(&format!("SELECT {n}"), usize::MAX)))
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| handle.join().expect("joined"))
                .collect()
        });
        assert!(answers.iter().all(Result::is_ok));
    }
}
