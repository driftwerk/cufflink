use common::{infra, PlatformProcess, Tenant};
use cufflink_platform::sql_parser::{
    Charge, ParseFailure, SqlParseSettings, SqlParser, MIN_PARSE_BUDGET,
};
use cufflink_sql_parse::{bounds, golden, PROBE, PROBE_FINGERPRINT};
use cufflink_test_guests::Profile;
use cufflink_testkit as common;
use cufflink_types::sql_parse::{ParseError, ParseResponse};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
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
        tenant_cooldown: WITHIN,
        tenant_busy: WITHIN * 30,
    }
}

/// A statement whose parse takes well over [`MIN_PARSE_BUDGET`] and well
/// under [`WITHIN`]: the costliest shape at the input bound.
fn slow() -> String {
    format!("SELECT a{}", ",a".repeat(bounds::MAX_INPUT_BYTES / 2 - 5))
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
        .map(|(bytes, _)| String::from_utf8(bytes).expect("text"))
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
async fn the_parser_process_answers_the_golden_corpus() {
    let Some((_binary, parser)) = started(2).await else {
        return;
    };
    for (sql, expected) in golden::cases() {
        assert_eq!(
            golden::answer(&tree(&parser, sql).await),
            expected,
            "{}",
            &sql[..sql.len().min(60)]
        );
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
        .expect("held")
        .0;
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
async fn a_tenant_whose_parses_keep_losing_processes_is_refused_for_its_cooldown() {
    let cooldown = Duration::from_secs(2);
    let Some((binary, parser)) = started_with(1, |settings| SqlParseSettings {
        timeout: MIN_PARSE_BUDGET,
        tenant_cooldown: cooldown,
        ..settings
    })
    .await
    else {
        return;
    };
    let losing = Uuid::from_u128(1);
    let slow = slow();
    for _ in 0..3 {
        let before = binary.processes();
        assert!(matches!(
            tree_within(&parser, losing, &slow, WITHIN).await,
            Err(ParseError::Unavailable(_))
        ));
        wait_for(|| {
            let now = binary.processes();
            now.len() == 1 && now != before
        })
        .await;
    }
    let cooled = Instant::now();
    let before = binary.processes();
    assert!(
        matches!(
            tree_within(&parser, losing, "SELECT 1", WITHIN).await,
            Err(ParseError::Unavailable(_))
        ),
        "the tenant is refused during its cooldown"
    );
    assert_eq!(
        tree_within(&parser, Uuid::from_u128(2), "SELECT 1", WITHIN).await,
        own("SELECT 1"),
        "another tenant is served meanwhile"
    );
    assert_eq!(binary.processes(), before, "a refusal costs no process");
    tokio::time::sleep(cooldown.saturating_sub(cooled.elapsed())).await;
    assert_eq!(
        tree_within(&parser, losing, "SELECT 1", WITHIN).await,
        own("SELECT 1"),
        "the tenant is served once its cooldown ends"
    );
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
        took > MIN_PARSE_BUDGET * 2,
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
async fn a_process_serves_one_tenant_and_is_replaced_before_it_serves_another() {
    let Some((binary, parser)) = started(1).await else {
        return;
    };
    let (first, second) = (Uuid::from_u128(1), Uuid::from_u128(2));
    let fresh = binary.processes();
    assert_eq!(
        tree_within(&parser, first, "SELECT 1", WITHIN).await,
        own("SELECT 1")
    );
    assert_eq!(
        tree_within(&parser, first, "SELECT 2", WITHIN).await,
        own("SELECT 2")
    );
    assert_eq!(binary.processes(), fresh, "a tenant keeps its own process");
    assert_eq!(
        tree_within(&parser, second, "SELECT 3", WITHIN).await,
        own("SELECT 3")
    );
    wait_for(|| {
        let now = binary.processes();
        now.len() == 1 && now != fresh
    })
    .await;
    let replaced = binary.processes();
    assert_eq!(
        tree_within(&parser, second, "SELECT 4", WITHIN).await,
        own("SELECT 4")
    );
    assert_eq!(binary.processes(), replaced);
}

#[tokio::test]
async fn tenants_taking_turns_on_fewer_processes_are_each_served_in_time() {
    let Some((_binary, parser)) = started_with(2, |settings| SqlParseSettings {
        per_tenant: 1,
        ..settings
    })
    .await
    else {
        return;
    };
    let parser = Arc::new(parser);
    let budget = Duration::from_secs(5);
    let tenants: Vec<_> = (1..=3)
        .map(|tenant| {
            let parser = Arc::clone(&parser);
            tokio::spawn(async move {
                let mut answers = Vec::new();
                for call in 0..8 {
                    let sql = format!("SELECT {tenant} AS tenant, {call} AS call");
                    let started = Instant::now();
                    let answer = tree_within(&parser, Uuid::from_u128(tenant), &sql, budget).await;
                    answers.push((answer == own(&sql), started.elapsed()));
                }
                answers
            })
        })
        .collect();
    for tenant in tenants {
        for (same, took) in tenant.await.expect("joined") {
            assert!(same, "every call is answered its tree");
            assert!(took < budget, "{took:?}");
        }
    }
}

/// The reviewer's inputs from before the input bound came down: each is
/// refused by its length alone, at once, and asks no process.
#[tokio::test]
async fn statements_past_the_input_bound_are_refused_at_once_without_a_process() {
    let Some((binary, parser)) = started(1).await else {
        return;
    };
    let before = binary.processes();
    let mebibyte = 1024 * 1024;
    let fill = |prefix: &str, unit: &str, suffix: &str| {
        let units = (mebibyte - prefix.len() - suffix.len()) / unit.len();
        format!("{prefix}{}{suffix}", unit.repeat(units))
    };
    for sql in [
        fill("SELECT a", ",a", ""),
        fill("SELECT *", ",*", ""),
        fill("SELECT 1 WHERE x IN (1", ",1", ")"),
        fill("SELECT 1 WHERE x = 1", " AND x = 1", ""),
        "CREATE GLOBAL TEMP TABLE t();".repeat(35_000),
        format!("SELECT '{}'", "x".repeat(bounds::MAX_INPUT_BYTES - 9)) + " ",
    ] {
        let started = Instant::now();
        let refused = tree(&parser, &sql).await;
        assert!(
            matches!(&refused, Err(ParseError::TooLarge(message)) if message.contains("bytes")),
            "{refused:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "{:?}",
            started.elapsed()
        );
    }
    assert_eq!(binary.processes(), before);
}

#[tokio::test]
async fn a_tenant_whose_parses_spent_their_process_time_is_refused_until_it_refills() {
    let Some((binary, parser)) = started_with(2, |settings| SqlParseSettings {
        tenant_busy: Duration::from_millis(50),
        tenant_cooldown: Duration::from_millis(200),
        ..settings
    })
    .await
    else {
        return;
    };
    let busy = Uuid::from_u128(1);
    let slow = slow();
    assert_eq!(tree_within(&parser, busy, &slow, WITHIN).await, own(&slow));
    let before = binary.processes();
    assert!(
        matches!(
            tree_within(&parser, busy, "SELECT 1", WITHIN).await,
            Err(ParseError::Unavailable(_))
        ),
        "the tenant has taken more process time than it has"
    );
    assert_eq!(
        tree_within(&parser, Uuid::from_u128(2), "SELECT 1", WITHIN).await,
        own("SELECT 1"),
        "another tenant is served meanwhile"
    );
    let refilling = Instant::now();
    while tree_within(&parser, busy, "SELECT 1", WITHIN)
        .await
        .is_err()
    {
        assert!(
            refilling.elapsed() < Duration::from_secs(10),
            "never refilled"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(binary.processes().len() <= 2 && before.len() == 2);
}

/// Counts what a parse charges, and whether the charge is still held.
#[derive(Clone, Default)]
struct Ledger {
    requested: Arc<AtomicUsize>,
    trees: Arc<AtomicUsize>,
    held: Arc<AtomicUsize>,
}

struct Counted(Ledger);

impl Counted {
    fn new(ledger: &Ledger) -> Self {
        ledger.held.fetch_add(1, Ordering::SeqCst);
        Self(ledger.clone())
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.held.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Charge for Counted {
    type Refused = ();

    fn request(&mut self, bytes: usize) -> Result<(), ()> {
        self.0.requested.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn tree(&mut self, len: usize) -> Result<Vec<u8>, ()> {
        self.0.trees.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::with_capacity(len))
    }
}

#[tokio::test]
async fn a_charge_lasts_as_long_as_the_exchange_that_holds_its_bytes() {
    let Some((_binary, parser)) = started(1).await else {
        return;
    };
    let ledger = Ledger::default();
    let slow = slow();
    let gave_up = parser
        .parse(
            Uuid::nil(),
            &slow,
            usize::MAX,
            MIN_PARSE_BUDGET,
            Counted::new(&ledger),
        )
        .await;
    assert!(matches!(
        gave_up,
        Err(ParseFailure::Refused(ParseError::Unavailable(_)))
    ));
    assert!(ledger.requested.load(Ordering::SeqCst) > slow.len());
    assert_eq!(
        ledger.held.load(Ordering::SeqCst),
        1,
        "the exchange still holds the request's charge"
    );
    wait_for(|| ledger.held.load(Ordering::SeqCst) == 0).await;
    assert_eq!(
        ledger.trees.load(Ordering::SeqCst),
        0,
        "no tree is charged to a caller that stopped waiting"
    );
    let (text, charge) = parser
        .parse(
            Uuid::nil(),
            "SELECT 1",
            usize::MAX,
            WITHIN,
            Counted::new(&ledger),
        )
        .await
        .expect("parsed");
    assert_eq!(Ok(String::from_utf8(text).expect("text")), own("SELECT 1"));
    assert_eq!(ledger.trees.load(Ordering::SeqCst), 1);
    assert_eq!(ledger.held.load(Ordering::SeqCst), 1, "the caller holds it");
    drop(charge);
    assert_eq!(ledger.held.load(Ordering::SeqCst), 0);
}

/// A shell script that greets as this build's parser does, then answers
/// whatever it is asked with `tree` as the tree's text.
fn answering(dir: &std::path::Path, tree: &[u8]) -> PathBuf {
    let octal = |bytes: &[u8]| {
        bytes
            .iter()
            .map(|b| format!("\\{b:03o}"))
            .collect::<String>()
    };
    let frame = |bytes: &[u8]| {
        let len = u32::try_from(bytes.len()).expect("small");
        octal(&[len.to_be_bytes().as_slice(), bytes].concat())
    };
    let hello = serde_json::to_vec(&cufflink_sql_parse::wire::Hello::of_this_build(
        PROBE_FINGERPRINT,
    ))
    .expect("hello");
    let script = format!(
        "#!/bin/sh\nprintf '{}'\nprintf '{}{}'\nexec /bin/sleep 30\n",
        frame(&hello),
        frame(br#""tree""#),
        frame(tree)
    );
    let path = dir.join("cufflink-sql-parser");
    std::fs::write(&path, script).expect("script");
    let mut permissions = std::fs::metadata(&path).expect("script").permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    std::fs::set_permissions(&path, permissions).expect("executable");
    path
}

#[tokio::test]
async fn a_tree_that_is_not_one_json_object_costs_the_process_and_never_reaches_the_caller() {
    let (forging, answering_honestly) = (
        tempfile::tempdir().expect("a directory"),
        tempfile::tempdir().expect("a directory"),
    );
    let forged = answering(forging.path(), br#"{}},"injected":{"#);
    let honest = answering(
        answering_honestly.path(),
        br#"{"stmts":[],"version":170007}"#,
    );
    let parser = SqlParser::new(settings(forged, 1));
    assert!(parser.start().await, "the script greets as the parser does");
    assert!(matches!(
        tree(&parser, "SELECT 1").await,
        Err(ParseError::Unavailable(_))
    ));
    let parser = SqlParser::new(settings(honest, 1));
    assert!(parser.start().await);
    assert_eq!(
        tree(&parser, "SELECT 1").await.as_deref(),
        Ok(r#"{"stmts":[],"version":170007}"#)
    );
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
