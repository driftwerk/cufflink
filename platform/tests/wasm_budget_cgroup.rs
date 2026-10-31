use cufflink_testkit as common;

use common::{PlatformProcess, PLATFORM_LAUNCHER};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

const IGNORED: &str =
    "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a memory limit; run by the memory-limit CI job";
const MIB: usize = 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;
const SOON: Duration = Duration::from_secs(10);

struct Limited {
    platform: PlatformProcess,
    cgroup: PathBuf,
    db: common::TestDb,
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

async fn limited(manifest: Value, extra: &[(&str, &str)]) -> Limited {
    assert!(
        std::env::var(PLATFORM_LAUNCHER).is_ok_and(|launcher| !launcher.trim().is_empty()),
        "{PLATFORM_LAUNCHER} is not set: {IGNORED}"
    );
    let infra = common::infra().await.expect("the CI services");
    let mut db = infra.create_database().await;
    let env: Vec<(&str, &str)> = [("WASM_MEMORY_BUDGET_MODE", "enforce")]
        .into_iter()
        .chain(extra.iter().copied())
        .collect();
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
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
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    Limited {
        platform,
        cgroup,
        db,
        tenant,
    }
}

impl Limited {
    fn post(
        &self,
        handler: &str,
        body: Value,
    ) -> impl std::future::Future<Output = (u16, Value)> + use<> {
        let request = self
            .platform
            .http
            .post(
                self.platform
                    .fn_url(&self.tenant, "catalog-service", handler),
            )
            .json(&body);
        async move {
            let response = request.send().await.expect("request");
            let status = response.status().as_u16();
            (status, response.json().await.unwrap_or(Value::Null))
        }
    }

    async fn survived(&self) {
        assert_eq!(self.platform.status("/health").await, Some(200));
        assert_eq!(event(&self.cgroup, "oom_kill"), 0, "nothing was OOM-killed");
        let swap = std::fs::read_to_string(self.cgroup.join("memory.swap.peak"))
            .or_else(|_| std::fs::read_to_string(self.cgroup.join("memory.swap.current")))
            .expect("swap usage");
        assert_eq!(swap.trim(), "0", "nothing was swapped");
        assert_eq!(
            event(&self.cgroup, "max"),
            0,
            "the scope never reached memory.max (peak {})",
            read(&self.cgroup, "memory.peak").trim()
        );
    }
}

#[tokio::test]
#[ignore = "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a memory limit; run by the memory-limit CI job"]
async fn a_linear_burst_stays_inside_the_limit() {
    const BURST: usize = 20;
    let limited = limited(
        common::wasm_manifest("catalog-service", &[("hold_gated_56", "/hold")]),
        &[],
    )
    .await;
    let gate = common::Gate::closed(&limited.db.tenant_url(&limited.tenant.slug)).await;
    let calls: Vec<_> = (0..BURST)
        .map(|_| tokio::spawn(limited.post("hold_gated_56", json!({}))))
        .collect();
    let mut held = 0;
    common::until("the burst is admitted or refused", SOON, async || {
        held = gate.waiting().await;
        let answered = calls.iter().filter(|call| call.is_finished()).count();
        usize::try_from(held).is_ok_and(|held| held + answered == BURST)
    })
    .await
    .expect("in time");
    gate.open().await;
    let mut statuses = Vec::new();
    for call in calls {
        let (status, body) = call.await.expect("join");
        assert!(matches!(status, 200 | 503), "{status}: {body}");
        statuses.push(status);
    }
    let ran = statuses.iter().filter(|status| **status == 200).count();
    assert!(held >= 1, "some of the burst was admitted");
    assert_eq!(
        i64::try_from(ran).ok(),
        Some(held),
        "what held memory at the gate ran, and the rest was refused: {statuses:?}"
    );
    assert!(ran < BURST, "the burst passed the budget: {statuses:?}");
    limited.survived().await;
}

#[tokio::test]
#[ignore = "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a memory limit; run by the memory-limit CI job"]
async fn large_bodies_and_an_oversized_query_stay_inside_the_limit() {
    let mut manifest = common::wasm_manifest(
        "catalog-service",
        &[("echo", "/echo"), ("big_query", "/big_query")],
    );
    manifest["custom_routes"][0]["body_limit_bytes"] = json!(32 * MIB);
    let limited = limited(manifest, &[]).await;
    let body = json!({ "pad": "x".repeat(16 * MIB) });
    let echoes =
        futures_util::future::join_all((0..4).map(|_| limited.post("echo", body.clone()))).await;
    for (status, answer) in &echoes {
        assert!(matches!(status, 200 | 503), "{status}: {answer}");
    }
    let queries =
        futures_util::future::join_all((0..3).map(|_| limited.post("big_query", json!({})))).await;
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
async fn large_redis_values_are_reserved_before_they_are_read() {
    let infra = common::infra().await.expect("the CI services");
    let redis_url = infra.redis_url().expect("the CI Redis");
    let limited = limited(
        common::wasm_manifest(
            "catalog-service",
            &[
                ("redis_get_big", "/redis_get_big"),
                ("redis_mget_big", "/redis_mget_big"),
            ],
        ),
        &[("REDIS_URL", redis_url)],
    )
    .await;
    let mut redis = redis::Client::open(redis_url)
        .expect("a Redis url")
        .get_multiplexed_async_connection()
        .await
        .expect("Redis");
    redis::AsyncCommands::set::<_, _, ()>(
        &mut redis,
        format!("t:{}:big", limited.tenant.id),
        "x".repeat(60 * MIB),
    )
    .await
    .expect("a 60 MiB value");

    let gets =
        futures_util::future::join_all((0..20).map(|_| limited.post("redis_get_big", json!({}))))
            .await;
    for (status, answer) in &gets {
        assert!(matches!(status, 200 | 503), "{status}: {answer}");
    }
    assert!(
        gets.iter().any(|answer| *answer == (200, json!({"rc": 0}))),
        "a value within the budget is read: {gets:?}"
    );
    let (status, answer) = limited.post("redis_mget_big", json!({})).await;
    assert_eq!(
        (status, answer),
        (200, json!({"rc": -1})),
        "three of them pass the response cap and are not read"
    );
    limited.survived().await;
}

#[tokio::test]
#[ignore = "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a memory limit; run by the memory-limit CI job"]
async fn teardown_stops_the_scope() {
    let limited = limited(
        common::wasm_manifest("catalog-service", &[("echo", "/echo")]),
        &[],
    )
    .await;
    let scope = limited.platform.scope().cloned().expect("a scope");
    assert!(scope.active());
    drop(limited);
    common::until("the scope stops", SOON, async || !scope.active())
        .await
        .expect("the scope outlived its platform");
}
