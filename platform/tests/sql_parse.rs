use common::{infra, PlatformProcess, Tenant};
use cufflink_platform::sql_parser::{ParseFailure, SqlParseSettings, SqlParser};
use cufflink_sql_parse::{bounds, PROBE};
use cufflink_test_guests::Profile;
use cufflink_testkit as common;
use cufflink_types::sql_parse::{ParseError, ParseResponse};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const SERVICE: &str = "catalog-service";
const SENTINEL: &str = "zq_sentinel_9f";
const WITHIN: Duration = Duration::from_secs(20);

fn settings(binary: PathBuf, processes: usize) -> SqlParseSettings {
    SqlParseSettings {
        binary,
        processes,
        memory_mb: 1024,
        timeout: WITHIN,
    }
}

/// The parser binary behind a path of the test's own, so that the test
/// tells its processes from those of the tests running beside it.
struct Binary {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl Binary {
    fn new() -> Option<Self> {
        let built = infra::require_binary("cufflink-sql-parser")?;
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("cufflink-sql-parser");
        std::os::unix::fs::symlink(built, &path).expect("a link to the parser");
        Some(Self { _dir: dir, path })
    }

    fn processes(&self) -> Vec<u32> {
        let listed = std::process::Command::new("pgrep")
            .args(["-f", &self.path.to_string_lossy()])
            .output()
            .expect("pgrep runs");
        String::from_utf8_lossy(&listed.stdout)
            .lines()
            .filter_map(|pid| pid.trim().parse().ok())
            .collect()
    }
}

async fn started(processes: usize) -> Option<(Binary, SqlParser)> {
    let binary = Binary::new()?;
    let parser = SqlParser::new(settings(binary.path.clone(), processes));
    assert!(parser.start().await, "the parser process greets");
    Some((binary, parser))
}

async fn tree(parser: &SqlParser, sql: &str) -> Result<String, ParseError> {
    tree_within(parser, sql, WITHIN).await
}

async fn tree_within(
    parser: &SqlParser,
    sql: &str,
    within: Duration,
) -> Result<String, ParseError> {
    parser
        .parse(sql, usize::MAX, within, |_| Ok::<_, ()>(Vec::new()))
        .await
        .map(|bytes| String::from_utf8(bytes).expect("text"))
        .map_err(|failure| match failure {
            ParseFailure::Refused(error) => error,
            ParseFailure::Unheld(()) => unreachable!("every tree is held"),
        })
}

fn own(sql: &str) -> Result<String, ParseError> {
    cufflink_sql_parse::parse(sql, usize::MAX)
}

#[tokio::test]
async fn the_parser_process_answers_pg_querys_own_tree() {
    let Some((_binary, parser)) = started(2).await else {
        return;
    };
    for sql in PROBE.lines().filter(|line| !line.trim().is_empty()) {
        assert_eq!(tree(&parser, sql).await, own(sql), "{sql}");
    }
}

#[tokio::test]
async fn refusals_through_the_process_quote_nothing_and_cost_no_process() {
    let Some((binary, parser)) = started(1).await else {
        return;
    };
    let before = binary.processes();
    for (sql, expected) in [
        (format!("SELEC {SENTINEL}"), "syntax"),
        (format!("SELECT '{SENTINEL}"), "syntax"),
        (format!("SELECT {SENTINEL}\0"), "syntax"),
        (
            format!("SELECT {}{SENTINEL}", "(".repeat(100_000)),
            "too_large",
        ),
        (
            format!(
                "SELECT {SENTINEL}{}",
                "+1".repeat(bounds::MAX_CHAIN_DEPTH as usize)
            ),
            "too_large",
        ),
        (
            format!("SELECT '{SENTINEL}{}'", "x".repeat(bounds::MAX_INPUT_BYTES)),
            "too_large",
        ),
    ] {
        let refused = tree(&parser, &sql).await.expect_err("refused");
        let kind = serde_json::to_value(&refused).expect("encode")["kind"].clone();
        assert_eq!(kind, expected, "{refused}");
        assert!(!refused.to_string().contains(SENTINEL), "{refused}");
    }
    assert_eq!(binary.processes(), before, "the same process answered each");
    assert!(tree(&parser, "SELECT 1").await.is_ok());
}

#[tokio::test]
async fn a_tree_past_the_callers_limit_or_hold_costs_no_process() {
    let Some((binary, parser)) = started(1).await else {
        return;
    };
    let text = own("SELECT 1").expect("parses");
    let short = parser
        .parse("SELECT 1", text.len() - 1, WITHIN, |_| {
            Ok::<_, ()>(Vec::new())
        })
        .await;
    assert!(matches!(
        short,
        Err(ParseFailure::Refused(ParseError::TooLarge(_)))
    ));
    let unheld = parser
        .parse("SELECT 1", usize::MAX, WITHIN, Err::<Vec<u8>, _>)
        .await;
    assert!(matches!(unheld, Err(ParseFailure::Unheld(len)) if len == text.len()));
    let held = parser
        .parse("SELECT 1", usize::MAX, WITHIN, |len| {
            let mut answer = Vec::with_capacity(len + 2);
            answer.extend_from_slice(b"<<");
            Ok::<_, ()>(answer)
        })
        .await
        .expect("held");
    assert_eq!(held, [b"<<".as_slice(), text.as_bytes()].concat());
    assert_eq!(binary.processes().len(), 1);
}

#[tokio::test]
async fn a_process_killed_while_idle_is_replaced_without_failing_a_call() {
    let Some((binary, parser)) = started(1).await else {
        return;
    };
    let [first] = binary.processes()[..] else {
        panic!("one parser process: {:?}", binary.processes());
    };
    std::process::Command::new("kill")
        .args(["-9", &first.to_string()])
        .status()
        .expect("kill runs");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(tree(&parser, "SELECT 1").await, own("SELECT 1"));
    let replaced = binary.processes();
    assert_eq!(replaced.len(), 1);
    assert_ne!(replaced, [first]);
}

#[tokio::test]
async fn a_process_past_its_time_is_killed_and_the_next_call_succeeds() {
    let Some((binary, parser)) = started(1).await else {
        return;
    };
    let before = binary.processes();
    let slow = format!("SELECT 1 WHERE {}", vec!["x = 1"; 50_000].join(" AND "));
    assert!(matches!(
        tree_within(&parser, &slow, Duration::from_millis(20)).await,
        Err(ParseError::Unavailable(_))
    ));
    assert_eq!(tree(&parser, "SELECT 1").await, own("SELECT 1"));
    let after = binary.processes();
    assert_eq!(after.len(), 1);
    assert_ne!(after, before, "the late process was killed and replaced");
}

#[tokio::test]
async fn parses_run_at_once_across_processes() {
    let Some((binary, parser)) = started(4).await else {
        return;
    };
    let parser = Arc::new(parser);
    let calls: Vec<_> = (0..32)
        .map(|n| {
            let parser = Arc::clone(&parser);
            tokio::spawn(async move {
                let sql = format!("SELECT {n} AS n FROM orders WHERE id = {n}");
                (tree(&parser, &sql).await, own(&sql))
            })
        })
        .collect();
    for call in calls {
        let (through, own) = call.await.expect("joined");
        assert_eq!(through, own);
    }
    assert!((1..=4).contains(&binary.processes().len()));
}

#[tokio::test]
async fn a_process_that_does_not_greet_is_never_used() {
    let parser = SqlParser::new(settings(PathBuf::from("/usr/bin/true"), 1));
    assert!(!parser.start().await);
    assert!(!parser.is_ready());
    assert!(matches!(
        tree(&parser, "SELECT 1").await,
        Err(ParseError::Unavailable(_))
    ));
}

async fn call(platform: &PlatformProcess, tenant: &Tenant, sql: &str) -> Value {
    let response = platform
        .http
        .post(platform.fn_url(tenant, SERVICE, "parse"))
        .json(&json!({ "sql": sql }))
        .send()
        .await
        .expect("fn request");
    assert_eq!(response.status().as_u16(), 200);
    response.json().await.expect("json body")
}

fn manifest() -> Value {
    let mut manifest = common::wasm_manifest(SERVICE, &[]);
    manifest["custom_routes"] = json!([{
        "method": "POST",
        "path": "/parse",
        "handler": "parse",
        "posture": {"kind": "public", "reason": "integration test"},
    }]);
    manifest
}

async fn advertises(platform: &PlatformProcess) -> bool {
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    version["supports"]
        .as_array()
        .is_some_and(|supports| supports.contains(&json!("sql_parse")))
}

#[tokio::test]
async fn a_guest_parses_through_the_platform_as_the_test_host_parses() {
    let Some(wasm) = common::sdk::sdk_guest("sql-parse", Profile::Debug) else {
        return;
    };
    if infra::require_binary("cufflink-sql-parser").is_none() {
        return;
    }
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    assert!(advertises(&platform).await);
    let tenant = db.mint_tenant().await;
    let (status, body) = platform.deploy_module(&tenant, &manifest(), &wasm).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    for sql in PROBE.lines().filter(|line| !line.trim().is_empty()) {
        let answer = call(&platform, &tenant, sql).await;
        let expected: Value = serde_json::from_str(&own(sql).expect("parses")).expect("json");
        assert_eq!(answer["ok"]["tree"], expected, "{sql}");
        assert_eq!(answer["ok"]["parser"], cufflink_sql_parse::PARSER);
        assert_eq!(
            answer["ok"]["fingerprint"],
            cufflink_sql_parse::PROBE_FINGERPRINT
        );
    }
    for (sql, refused) in [
        (format!("SELEC {SENTINEL}"), ParseError::syntax()),
        (
            format!("SELECT {}{SENTINEL}", "(".repeat(5_000)),
            ParseError::TooLarge(
                bounds::check(&format!("SELECT {}", "(".repeat(5_000)))
                    .expect_err("too deep")
                    .to_string()
                    .trim_start_matches("too large: ")
                    .to_string(),
            ),
        ),
    ] {
        let answer = call(&platform, &tenant, &sql).await;
        assert_eq!(
            serde_json::from_value::<ParseResponse>(answer.clone()).expect("an answer"),
            ParseResponse::Error(refused)
        );
        assert!(!answer.to_string().contains(SENTINEL), "{answer}");
    }
}

#[tokio::test]
async fn a_guest_that_parses_is_refused_where_no_parser_answers() {
    let Some(wasm) = common::sdk::sdk_guest("sql-parse", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("SQL_PARSER_BIN", "/nonexistent/cufflink-sql-parser")],
    )
    .await;
    platform.wait_ready().await;
    assert!(!advertises(&platform).await);
    let tenant = db.mint_tenant().await;
    let (status, body) = platform.deploy_module(&tenant, &manifest(), &wasm).await;
    assert_ne!(status, 200, "{body}");
    assert!(
        body.to_string().contains("'sql_parse'"),
        "the refusal names the feature: {body}"
    );
}
