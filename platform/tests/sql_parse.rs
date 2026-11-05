use common::{infra, PlatformProcess, Tenant};
use cufflink_platform::sql_parser::{ParseFailure, SqlParseSettings, SqlParser, MIN_PARSE_BUDGET};
use cufflink_sql_parse::{bounds, PROBE};
use cufflink_test_guests::Profile;
use cufflink_testkit as common;
use cufflink_types::sql_parse::{ParseError, ParseResponse};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const SERVICE: &str = "catalog-service";
const SENTINEL: &str = "zq_sentinel_9f";
const WITHIN: Duration = Duration::from_secs(20);

fn settings(binary: PathBuf, processes: usize) -> SqlParseSettings {
    SqlParseSettings {
        binary,
        processes,
        per_tenant: processes,
        memory_mb: 1024,
        timeout: WITHIN,
    }
}

/// A statement whose parse takes well over [`MIN_PARSE_BUDGET`] and well
/// under [`WITHIN`].
fn slow() -> String {
    format!("SELECT 1 WHERE {}", vec!["x = 1"; 50_000].join(" AND "))
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
    started_with(processes, |settings| settings).await
}

async fn started_with(
    processes: usize,
    adjust: impl FnOnce(SqlParseSettings) -> SqlParseSettings,
) -> Option<(Binary, SqlParser)> {
    let binary = Binary::new()?;
    let parser = SqlParser::new(adjust(settings(binary.path.clone(), processes)));
    assert!(parser.start().await, "the parser process greets");
    wait_for(|| binary.processes().len() == processes).await;
    Some((binary, parser))
}

async fn wait_for(done: impl Fn() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < Duration::from_secs(10), "timed out");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn tree(parser: &SqlParser, sql: &str) -> Result<String, ParseError> {
    tree_within(parser, Uuid::nil(), sql, WITHIN).await
}

async fn tree_within(
    parser: &SqlParser,
    tenant: Uuid,
    sql: &str,
    within: Duration,
) -> Result<String, ParseError> {
    parser
        .parse(tenant, sql, usize::MAX, within, |_| Ok::<_, ()>(Vec::new()))
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
        .parse(Uuid::nil(), "SELECT 1", text.len() - 1, WITHIN, |_| {
            Ok::<_, ()>(Vec::new())
        })
        .await;
    assert!(matches!(
        short,
        Err(ParseFailure::Refused(ParseError::TooLarge(_)))
    ));
    let unheld = parser
        .parse(
            Uuid::nil(),
            "SELECT 1",
            usize::MAX,
            WITHIN,
            Err::<Vec<u8>, _>,
        )
        .await;
    assert!(matches!(unheld, Err(ParseFailure::Unheld(len)) if len == text.len()));
    let held = parser
        .parse(Uuid::nil(), "SELECT 1", usize::MAX, WITHIN, |len| {
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
async fn a_call_with_too_little_budget_left_costs_no_process() {
    let Some((binary, parser)) = started(1).await else {
        return;
    };
    let before = binary.processes();
    let short = MIN_PARSE_BUDGET - Duration::from_millis(1);
    assert!(matches!(
        tree_within(&parser, Uuid::nil(), "SELECT 1", short).await,
        Err(ParseError::Unavailable(_))
    ));
    assert_eq!(tree(&parser, "SELECT 1").await, own("SELECT 1"));
    assert_eq!(binary.processes(), before);
}

#[tokio::test]
async fn a_caller_that_stops_waiting_leaves_its_process_to_finish_and_serve_on() {
    let Some((binary, parser)) = started(1).await else {
        return;
    };
    let before = binary.processes();
    let started = Instant::now();
    assert!(matches!(
        tree_within(&parser, Uuid::nil(), &slow(), MIN_PARSE_BUDGET).await,
        Err(ParseError::Unavailable(_))
    ));
    assert!(
        started.elapsed() < MIN_PARSE_BUDGET * 3,
        "{:?}",
        started.elapsed()
    );
    assert_eq!(tree(&parser, "SELECT 1").await, own("SELECT 1"));
    assert_eq!(
        binary.processes(),
        before,
        "the process finished and was kept"
    );
}

#[tokio::test]
async fn a_process_past_the_parse_timeout_is_killed_and_replaced_in_the_background() {
    let Some((binary, parser)) = started_with(1, |settings| SqlParseSettings {
        timeout: MIN_PARSE_BUDGET,
        ..settings
    })
    .await
    else {
        return;
    };
    let before = binary.processes();
    assert!(matches!(
        tree_within(&parser, Uuid::nil(), &slow(), WITHIN).await,
        Err(ParseError::Unavailable(_))
    ));
    wait_for(|| {
        let now = binary.processes();
        now.len() == 1 && now != before
    })
    .await;
    assert_eq!(tree(&parser, "SELECT 1").await, own("SELECT 1"));
}

#[tokio::test]
async fn one_tenant_waits_for_its_own_share_of_the_processes() {
    let Some((_binary, parser)) = started_with(2, |settings| SqlParseSettings {
        per_tenant: 1,
        ..settings
    })
    .await
    else {
        return;
    };
    let busy = Uuid::from_u128(1);
    let slow = slow();
    let started = Instant::now();
    let own_slow = own(&slow);
    let took = started.elapsed();
    assert!(
        took > MIN_PARSE_BUDGET * 5,
        "the slow statement takes {took:?}"
    );
    assert!(matches!(
        tree_within(&parser, busy, &slow, MIN_PARSE_BUDGET).await,
        Err(ParseError::Unavailable(_))
    ));
    assert!(
        matches!(
            tree_within(&parser, busy, "SELECT 1", MIN_PARSE_BUDGET).await,
            Err(ParseError::Unavailable(_))
        ),
        "the parse its caller left still holds the tenant's turn"
    );
    assert_eq!(
        tree_within(
            &parser,
            Uuid::from_u128(2),
            "SELECT 1",
            MIN_PARSE_BUDGET * 2
        )
        .await,
        own("SELECT 1"),
        "another tenant is served meanwhile"
    );
    assert_eq!(
        tree_within(&parser, busy, "SELECT 1", WITHIN).await,
        own("SELECT 1"),
        "the tenant's next call waits for its turn"
    );
    assert!(own_slow.is_ok());
}

#[tokio::test]
async fn readiness_follows_whether_a_process_can_start() {
    let Some(built) = infra::require_binary("cufflink-sql-parser") else {
        return;
    };
    let dir = tempfile::tempdir().expect("a directory");
    let path = dir.path().join("cufflink-sql-parser");
    let point = |target: &std::path::Path| {
        let staged = dir.path().join("staged");
        std::os::unix::fs::symlink(target, &staged).expect("a link");
        std::fs::rename(&staged, &path).expect("the link replaced");
    };
    point(&built);
    let binary = Binary {
        path: path.clone(),
        _dir: tempfile::tempdir().expect("a directory"),
    };
    let parser = SqlParser::new(settings(path.clone(), 1));
    assert!(parser.start().await);
    point(std::path::Path::new("/usr/bin/true"));
    for pid in binary.processes() {
        std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status()
            .expect("kill runs");
    }
    wait_for(|| !parser.is_ready()).await;
    assert!(matches!(
        tree(&parser, "SELECT 1").await,
        Err(ParseError::Unavailable(_))
    ));
    point(&built);
    wait_for(|| parser.is_ready()).await;
    assert_eq!(tree(&parser, "SELECT 1").await, own("SELECT 1"));
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

async fn comes_to_advertise(platform: &PlatformProcess) -> bool {
    let started = Instant::now();
    while !advertises(platform).await {
        if started.elapsed() > Duration::from_secs(10) {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    true
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
    assert!(comes_to_advertise(&platform).await);
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
    let nested = format!("SELECT 1{}", "+1".repeat(35));
    let answer = call(&platform, &tenant, &nested).await;
    let expected: Value = serde_json::from_str(&own(&nested).expect("parses")).expect("json");
    assert_eq!(
        answer["ok"]["tree"], expected,
        "a tree at the nesting limit"
    );
    let deeper = format!("SELECT {SENTINEL}{}", "+1".repeat(40));
    for (sql, refused) in [
        (deeper.clone(), own(&deeper).expect_err("too deep a tree")),
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
