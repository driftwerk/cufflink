use cufflink_testkit as common;

use axum::extract::Path;
use axum::routing::get;
use axum::Router;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{PlatformProcess, Tenant};
use cufflink_test_guests::Profile;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const CATALOG: &str = "catalog-service";
const LOANS: &str = "loan-service";
const SOON: Duration = Duration::from_secs(10);

struct Upstream {
    base: String,
    arrived: Arc<AtomicUsize>,
    open: watch::Sender<bool>,
}

impl Upstream {
    async fn start() -> Self {
        let arrived = Arc::new(AtomicUsize::new(0));
        let (open, opened) = watch::channel(false);
        let app = Router::new().route(
            "/held/{name}",
            get({
                let arrived = Arc::clone(&arrived);
                move |Path(name): Path<String>| {
                    let mut opened = opened.clone();
                    arrived.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let _ = opened.wait_for(|open| *open).await;
                        name
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            base,
            arrived,
            open,
        }
    }

    fn arrived(&self) -> usize {
        self.arrived.load(Ordering::SeqCst)
    }

    async fn waiting(&self, count: usize) {
        common::until("calls held upstream", SOON, async || {
            self.arrived() >= count
        })
        .await
        .expect("in time");
    }

    fn open(&self) {
        let _ = self.open.send(true);
    }

    fn fetch(&self, name: &str, wait: Option<&str>) -> Value {
        let mut request = json!({"method": "GET", "url": format!("{}/held/{name}", self.base)});
        if let Some(wait) = wait {
            request["wait"] = json!(wait);
        }
        json!({"host": "http_fetch_v2", "request": request})
    }
}

struct Stack {
    platform: PlatformProcess,
    tenant: Tenant,
    _db: common::TestDb,
}

const ENFORCED: &[(&str, &str)] = &[
    ("WASM_MEMORY_BUDGET_MODE", "enforce"),
    ("WASM_MEMORY_BUDGET_MB", "512"),
    ("WASM_MAX_CONCURRENT_PER_SERVICE", "2"),
    ("WASM_ADMISSION_WAIT_MS", "200"),
];

fn manifest(service: &str) -> Value {
    let mut manifest = common::wasm_manifest(
        service,
        &[
            ("relay", "/relay"),
            ("relay", "/relay_briefly"),
            ("compute", "/compute"),
        ],
    );
    manifest["routing"] = json!("strict");
    manifest["custom_routes"][1]["timeout_secs"] = json!(2);
    manifest
}

async fn stack(extra: &[(&str, &str)], services: &[&str]) -> Option<Stack> {
    let wasm = common::sdk::sdk_guest("wait-release", Profile::Debug)?;
    let infra = common::infra().await?;
    let mut db = infra.create_database().await;
    let env: Vec<(&str, &str)> = ENFORCED
        .iter()
        .filter(|(name, _)| extra.iter().all(|(set, _)| set != name))
        .chain(extra)
        .copied()
        .collect();
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    for service in services {
        let (status, body) = platform
            .deploy_module(&tenant, &manifest(service), &wasm)
            .await;
        assert_eq!(status, 200, "deploy failed: {body}");
    }
    Some(Stack {
        platform,
        tenant,
        _db: db,
    })
}

struct Answer {
    status: u16,
    body: Value,
}

impl Stack {
    fn url(&self, service: &str, path: &str) -> String {
        format!(
            "{}/svc/{}/{service}/{path}",
            self.platform.base_url, self.tenant.slug
        )
    }

    fn send(&self, service: &str, path: &str, body: Value) -> JoinHandle<Answer> {
        let request = self.platform.http.post(self.url(service, path)).json(&body);
        tokio::spawn(async move {
            let response = request.send().await.expect("request");
            Answer {
                status: response.status().as_u16(),
                body: response.json().await.unwrap_or(Value::Null),
            }
        })
    }

    async fn compute(&self, service: &str) -> Answer {
        self.send(service, "compute", json!({}))
            .await
            .expect("joined")
    }

    async fn metric(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.platform.metric(name, labels).await.unwrap_or_default()
    }

    async fn released(&self, result: &str) -> f64 {
        self.metric("cufflink_wasm_wait_release_total", &[("result", result)])
            .await
    }

    async fn parked_now(&self) -> f64 {
        self.metric("cufflink_wasm_parked{", &[("kind", "route")])
            .await
    }

    async fn settles_parked_at(&self, parked: f64) {
        common::until("the parked gauge settles", SOON, async || {
            self.parked_now().await == parked
        })
        .await
        .expect("in time");
    }
}

async fn answered(calls: Vec<JoinHandle<Answer>>) -> Vec<Answer> {
    futures_util::future::try_join_all(calls)
        .await
        .expect("joined")
}

fn fetched(answer: &Answer, name: &str) {
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(answer.body["result"], "ok", "{}", answer.body);
    assert_eq!(answer.body["status"], 200, "{}", answer.body);
    let body = answer.body["body_b64"].as_str().unwrap_or_default();
    let decoded = STANDARD.decode(body).expect("base64");
    assert_eq!(decoded, name.as_bytes());
}

#[tokio::test]
async fn a_parked_proxy_lets_its_service_keep_computing() {
    let Some(stack) = stack(&[], &[CATALOG]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let proxies = ["first", "second"]
        .map(|name| stack.send(CATALOG, "relay", upstream.fetch(name, Some("release"))));
    upstream.waiting(2).await;
    stack.settles_parked_at(2.0).await;

    let computed = stack.compute(CATALOG).await;
    assert_eq!(
        computed.status, 200,
        "both of the service's slots were given back: {}",
        computed.body
    );

    upstream.open();
    let answers = answered(proxies.into()).await;
    fetched(&answers[0], "first");
    fetched(&answers[1], "second");
    assert_eq!(upstream.arrived(), 2, "each call was sent once");
    stack.settles_parked_at(0.0).await;
    assert_eq!(stack.released("parked").await, 2.0);
    assert_eq!(
        stack
            .metric("cufflink_wasm_parked_seconds_count", &[("kind", "route")])
            .await,
        2.0
    );
    assert_eq!(
        stack
            .metric(
                "cufflink_wasm_resume_wait_seconds_count",
                &[("kind", "route"), ("result", "resumed")]
            )
            .await,
        2.0
    );
}

#[tokio::test]
async fn a_held_proxy_still_holds_its_slots() {
    let Some(stack) = stack(&[], &[CATALOG]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let proxies =
        ["first", "second"].map(|name| stack.send(CATALOG, "relay", upstream.fetch(name, None)));
    upstream.waiting(2).await;

    let computed = stack.compute(CATALOG).await;
    assert_eq!(computed.status, 503, "{}", computed.body);

    upstream.open();
    for answer in answered(proxies.into()).await {
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
    assert_eq!(stack.released("parked").await, 0.0);
}

#[tokio::test]
async fn wait_release_off_holds_every_call() {
    let Some(stack) = stack(&[("WASM_WAIT_RELEASE", "off")], &[CATALOG]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let proxies = ["first", "second"]
        .map(|name| stack.send(CATALOG, "relay", upstream.fetch(name, Some("release"))));
    upstream.waiting(2).await;

    let computed = stack.compute(CATALOG).await;
    assert_eq!(computed.status, 503, "{}", computed.body);

    upstream.open();
    for answer in answered(proxies.into()).await {
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
    assert_eq!(stack.released("held_disabled").await, 2.0);
    assert_eq!(stack.released("parked").await, 0.0);
}

#[tokio::test]
async fn a_parked_proxy_does_not_starve_another_service() {
    let Some(stack) = stack(&[("WASM_MAX_CONCURRENT", "2")], &[CATALOG, LOANS]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let proxies = ["first", "second"]
        .map(|name| stack.send(CATALOG, "relay", upstream.fetch(name, Some("release"))));
    upstream.waiting(2).await;
    stack.settles_parked_at(2.0).await;

    let computed = stack.compute(LOANS).await;
    assert_eq!(
        computed.status, 200,
        "every global slot was given back: {}",
        computed.body
    );

    upstream.open();
    for answer in answered(proxies.into()).await {
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
}

#[tokio::test]
async fn a_full_wait_pool_runs_the_call_held() {
    let Some(stack) = stack(&[("WASM_MAX_PARKED", "1")], &[CATALOG]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let proxies = ["first", "second"]
        .map(|name| stack.send(CATALOG, "relay", upstream.fetch(name, Some("release"))));
    upstream.waiting(2).await;
    common::until("the second call is held", SOON, async || {
        stack.released("held_pool_full").await == 1.0
    })
    .await
    .expect("in time");
    assert_eq!(stack.parked_now().await, 1.0);

    upstream.open();
    for answer in answered(proxies.into()).await {
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
    assert_eq!(stack.released("parked").await, 1.0);
}

#[tokio::test]
async fn a_full_service_wait_pool_holds_that_service_alone() {
    let Some(stack) = stack(&[("WASM_MAX_PARKED_PER_SERVICE", "1")], &[CATALOG, LOANS]).await
    else {
        return;
    };
    let upstream = Upstream::start().await;
    let proxies = [
        stack.send(CATALOG, "relay", upstream.fetch("first", Some("release"))),
        stack.send(CATALOG, "relay", upstream.fetch("second", Some("release"))),
        stack.send(LOANS, "relay", upstream.fetch("third", Some("release"))),
    ];
    upstream.waiting(3).await;
    common::until("one catalog call is held", SOON, async || {
        stack.released("held_service_pool_full").await == 1.0
    })
    .await
    .expect("in time");
    stack.settles_parked_at(2.0).await;

    upstream.open();
    for answer in answered(proxies.into()).await {
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
    assert_eq!(stack.released("parked").await, 2.0);
}

#[tokio::test]
async fn no_deadlock_when_every_slot_is_parked() {
    let Some(stack) = stack(
        &[
            ("WASM_MAX_CONCURRENT", "2"),
            ("WASM_MAX_PARKED", "6"),
            ("WASM_ADMISSION_WAIT_MS", "5000"),
        ],
        &[CATALOG, LOANS],
    )
    .await
    else {
        return;
    };
    let upstream = Upstream::start().await;
    let names = ["a", "b", "c", "d", "e", "f"];
    let proxies = names
        .iter()
        .enumerate()
        .map(|(n, name)| {
            let service = [CATALOG, LOANS][n % 2];
            stack.send(service, "relay", upstream.fetch(name, Some("release")))
        })
        .collect::<Vec<_>>();
    upstream.waiting(names.len()).await;
    stack.settles_parked_at(6.0).await;

    upstream.open();
    let answers = tokio::time::timeout(SOON, answered(proxies))
        .await
        .expect("every parked guest resumes and answers");
    answers
        .iter()
        .zip(names)
        .for_each(|(answer, name)| fetched(answer, name));
    stack.settles_parked_at(0.0).await;
    assert_eq!(stack.compute(CATALOG).await.status, 200);
}

#[tokio::test]
async fn a_call_that_times_out_while_parked_resumes_with_its_error() {
    let Some(stack) = stack(&[], &[CATALOG]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let mut call = upstream.fetch("slow", Some("release"));
    call["request"]["timeout_ms"] = json!(500);
    let answer = stack.send(CATALOG, "relay", call).await.expect("joined");
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(answer.body["result"], "error", "{}", answer.body);
    assert_eq!(answer.body["kind"], "timeout", "{}", answer.body);
    assert_eq!(stack.released("parked").await, 1.0);
    stack.settles_parked_at(0.0).await;
    let both = answered(vec![
        stack.send(CATALOG, "compute", json!({})),
        stack.send(CATALOG, "compute", json!({})),
    ])
    .await;
    assert!(both.iter().all(|answer| answer.status == 200));
}

#[tokio::test]
async fn a_guest_that_cannot_resume_before_its_deadline_ends_deadline_exceeded() {
    let Some(stack) = stack(&[("WASM_MAX_CONCURRENT_PER_SERVICE", "1")], &[CATALOG]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let late = Upstream::start().await;
    let proxy = stack.send(
        CATALOG,
        "relay_briefly",
        upstream.fetch("proxy", Some("release")),
    );
    upstream.waiting(1).await;
    stack.settles_parked_at(1.0).await;
    let holder = stack.send(CATALOG, "relay", late.fetch("holder", None));
    late.waiting(1).await;

    upstream.open();
    let ended = proxy.await.expect("joined");
    assert_eq!(ended.status, 500, "{}", ended.body);
    assert_eq!(upstream.arrived(), 1, "the upstream call was made once");
    assert_eq!(
        stack
            .metric(
                "cufflink_wasm_resume_wait_seconds_count",
                &[("kind", "route"), ("result", "deadline")]
            )
            .await,
        1.0
    );
    stack.settles_parked_at(0.0).await;

    late.open();
    let held = holder.await.expect("joined");
    assert_eq!(held.status, 200, "{}", held.body);
    assert_eq!(stack.compute(CATALOG).await.status, 200);
}

#[tokio::test]
async fn a_guest_that_traps_after_resuming_gives_every_slot_back() {
    let Some(stack) = stack(&[], &[CATALOG]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let mut call = upstream.fetch("trap", Some("release"));
    call["then"] = json!("trap");
    let proxy = stack.send(CATALOG, "relay", call);
    upstream.waiting(1).await;
    stack.settles_parked_at(1.0).await;
    upstream.open();
    let trapped = proxy.await.expect("joined");
    assert_eq!(trapped.status, 500, "{}", trapped.body);
    stack.settles_parked_at(0.0).await;
    let both = answered(vec![
        stack.send(CATALOG, "compute", json!({})),
        stack.send(CATALOG, "compute", json!({})),
    ])
    .await;
    assert!(
        both.iter().all(|answer| answer.status == 200),
        "both of the service's slots are free again"
    );
}

#[tokio::test]
async fn a_service_call_parks_its_caller_and_a_nested_callee_never_parks() {
    let Some(stack) = stack(&[], &[CATALOG, LOANS]).await else {
        return;
    };
    let upstream = Upstream::start().await;
    let callee = upstream.fetch("nested", Some("release"));
    let call = json!({
        "host": "service_call",
        "request": {
            "service": LOANS,
            "method": "POST",
            "path": "/relay",
            "body_b64": STANDARD.encode(callee.to_string()),
            "headers": {"content-type": "application/json"},
            "wait": "release",
        },
    });
    let caller = stack.send(CATALOG, "relay", call);
    upstream.waiting(1).await;
    stack.settles_parked_at(1.0).await;
    common::until("the callee's wait is held", SOON, async || {
        stack.released("held_not_parkable").await == 1.0
    })
    .await
    .expect("in time");

    upstream.open();
    let answer = caller.await.expect("joined");
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(answer.body["ok"], true, "{}", answer.body);
    assert_eq!(answer.body["status"], 200, "{}", answer.body);
    assert_eq!(stack.released("parked").await, 1.0);
}
