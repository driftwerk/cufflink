use cufflink_testkit as common;

use common::{PlatformProcess, PLATFORM_LAUNCHER};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

const IGNORED: &str =
    "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a memory limit; run by the memory-limit CI job";
const GIB: u64 = 1024 * 1024 * 1024;
const MAX_EVENTS_BOUND: u64 = 10_000;

struct Limited {
    platform: PlatformProcess,
    cgroup: PathBuf,
    _db: common::TestDb,
    tenant: common::Tenant,
}

fn read(cgroup: &Path, file: &str) -> String {
    std::fs::read_to_string(cgroup.join(file))
        .unwrap_or_else(|e| panic!("{}: {e}", cgroup.join(file).display()))
}

fn event(cgroup: &Path, name: &str) -> u64 {
    read(cgroup, "memory.events")
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .and_then(|count| count.trim().parse().ok())
        .unwrap_or_else(|| panic!("memory.events has no {name}"))
}

async fn limited(routes: &[(&str, &str)]) -> Limited {
    assert!(
        std::env::var(PLATFORM_LAUNCHER).is_ok_and(|launcher| !launcher.trim().is_empty()),
        "{PLATFORM_LAUNCHER} is not set: {IGNORED}"
    );
    let infra = common::infra().await.expect("the CI services");
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("WASM_MEMORY_BUDGET_MODE", "enforce")],
    )
    .await;
    platform.wait_ready().await;
    let cgroup = platform
        .scope()
        .and_then(|scope| scope.cgroup())
        .expect("the platform runs in its own scope");
    let max: u64 = read(&cgroup, "memory.max")
        .trim()
        .parse()
        .expect("memory.max is a number, not max");
    assert!(max <= GIB, "memory.max {max} is above 1 GiB");
    assert_eq!(read(&cgroup, "memory.swap.max").trim(), "0");
    assert!(
        !read(&cgroup, "cgroup.procs").trim().is_empty(),
        "the platform is in the scope"
    );
    let tenant = db.mint_tenant().await;
    let manifest = common::wasm_manifest("catalog-service", routes);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    Limited {
        platform,
        cgroup,
        _db: db,
        tenant,
    }
}

impl Limited {
    async fn post(&self, handler: &str, body: &Value) -> (u16, Value) {
        let response = self
            .platform
            .http
            .post(
                self.platform
                    .fn_url(&self.tenant, "catalog-service", handler),
            )
            .json(body)
            .send()
            .await
            .expect("request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn survived(&self) {
        assert_eq!(self.platform.status("/health").await, Some(200));
        assert_eq!(event(&self.cgroup, "oom_kill"), 0, "nothing was OOM-killed");
        let swap = std::fs::read_to_string(self.cgroup.join("memory.swap.peak"))
            .or_else(|_| std::fs::read_to_string(self.cgroup.join("memory.swap.current")))
            .expect("swap usage");
        assert_eq!(swap.trim(), "0", "nothing was swapped");
        let max = event(&self.cgroup, "max");
        assert!(max < MAX_EVENTS_BOUND, "{max} max events");
    }
}

#[tokio::test]
#[ignore = "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a memory limit; run by the memory-limit CI job"]
async fn a_linear_burst_stays_inside_the_limit() {
    let limited = limited(&[("hold_48", "/hold_48")]).await;
    let empty = json!({});
    let answers =
        futures_util::future::join_all((0..20).map(|_| limited.post("hold_48", &empty))).await;
    assert!(answers.iter().any(|(status, _)| *status == 200));
    for (status, body) in &answers {
        assert!(matches!(status, 200 | 503), "{status}: {body}");
    }
    limited.survived().await;
}

#[tokio::test]
#[ignore = "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a memory limit; run by the memory-limit CI job"]
async fn large_bodies_and_an_oversized_query_stay_inside_the_limit() {
    let limited = limited(&[("echo", "/echo"), ("big_query", "/big_query")]).await;
    let body = json!({ "pad": "x".repeat(16 * 1024 * 1024) });
    let echoes = futures_util::future::join_all((0..4).map(|_| limited.post("echo", &body))).await;
    for (status, answer) in &echoes {
        assert!(matches!(status, 200 | 503), "{status}: {answer}");
    }
    let empty = json!({});
    let queries =
        futures_util::future::join_all((0..3).map(|_| limited.post("big_query", &empty))).await;
    for (status, answer) in &queries {
        assert!(matches!(status, 200 | 503), "{status}: {answer}");
        if *status == 200 {
            assert_eq!(answer, &json!({"rc": -1}));
        }
    }
    limited.survived().await;
}

#[tokio::test]
#[ignore = "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a memory limit; run by the memory-limit CI job"]
async fn teardown_stops_the_scope() {
    let limited = limited(&[("echo", "/echo")]).await;
    let scope = limited.platform.scope().cloned().expect("a scope");
    assert!(scope.active());
    drop(limited);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!scope.active(), "the scope outlived its platform");
}
