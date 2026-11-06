#![cfg(all(feature = "test-host", feature = "sql-parse"))]

use cufflink_fn::sql::{self, ParseError};
use cufflink_fn::testing::TestHost;
use serde_json::Value;

fn host() -> TestHost {
    TestHost::builder().build().expect("host")
}

#[test]
fn the_test_host_answers_the_platforms_tree_and_identity() {
    let host = host();
    let sql = "SELECT id, total FROM orders WHERE total > $1 ORDER BY id; DELETE FROM carts";
    let parsed = host.run(|| sql::parse(sql)).expect("parses");
    let own: Value =
        serde_json::from_str(&cufflink_sql_parse::parse(sql, usize::MAX).expect("pg_query parses"))
            .expect("json");
    assert_eq!(parsed.tree, own);
    assert_eq!(parsed.tree["stmts"].as_array().map(Vec::len), Some(2));
    assert_eq!(parsed.parser, cufflink_sql_parse::PARSER);
    assert_eq!(parsed.fingerprint, cufflink_sql_parse::PROBE_FINGERPRINT);
}

/// Every statement of the golden corpus, the probe corpus among them,
/// answers the committed tree or refusal, as the platform's parser process
/// does in its own tests.
#[test]
fn the_test_host_answers_the_golden_corpus() {
    let host = host();
    let differing: Vec<_> = cufflink_sql_parse::golden::cases()
        .filter(|(sql, expected)| {
            let answered = host.run(|| sql::parse(sql));
            match (answered, expected.strip_prefix('!')) {
                (Ok(parsed), None) => {
                    serde_json::from_str::<Value>(expected).ok() != Some(parsed.tree)
                        || parsed.parser != cufflink_sql_parse::PARSER
                        || parsed.fingerprint != cufflink_sql_parse::PROBE_FINGERPRINT
                }
                (Err(error), Some(kind)) => cufflink_sql_parse::golden::kind(&error) != kind,
                _ => true,
            }
        })
        .map(|(sql, _)| &sql[..sql.len().min(60)])
        .collect();
    assert!(differing.is_empty(), "{differing:#?}");
}

#[test]
fn the_test_host_refuses_as_the_platform_does_and_quotes_nothing() {
    let host = host();
    let sentinel = "zq_sentinel_9f";
    host.run(|| {
        let syntax = sql::parse(&format!("SELEC {sentinel}")).expect_err("not SQL");
        assert_eq!(syntax, ParseError::syntax());
        let large = sql::parse(&format!(
            "SELECT '{sentinel}{}'",
            "x".repeat(cufflink_sql_parse::bounds::MAX_INPUT_BYTES)
        ))
        .expect_err("past the input bound");
        assert!(matches!(large, ParseError::TooLarge(_)), "{large}");
        let deep = sql::parse(&format!("SELECT {}{sentinel}", "(".repeat(5_000)))
            .expect_err("past the nesting bound");
        assert!(matches!(deep, ParseError::TooLarge(_)), "{deep}");
        for refused in [syntax, large, deep] {
            assert!(!refused.to_string().contains(sentinel), "{refused}");
        }
    });
}

#[test]
fn a_tree_too_deep_for_a_guest_to_read_is_too_large_as_on_the_platform() {
    let host = host();
    host.run(|| {
        let within = sql::parse(&format!("SELECT 1{}", "+1".repeat(35))).expect("at the limit");
        assert_eq!(within.tree["version"], 170_007);
        let deeper = sql::parse(&format!("SELECT 1{}", "+1".repeat(40))).expect_err("too deep");
        assert!(matches!(deeper, ParseError::TooLarge(_)), "{deeper}");
    });
}

#[test]
fn without_a_host_the_parser_is_unavailable() {
    assert!(matches!(
        sql::parse("SELECT 1"),
        Err(ParseError::Unavailable(_))
    ));
}
