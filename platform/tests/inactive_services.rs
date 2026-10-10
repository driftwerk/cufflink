use cufflink_testkit as common;

use async_nats::jetstream::{self, consumer, stream};
use base64::Engine;
use common::jetstream::tenant_stream;
use common::{infra, until, until_loops, PlatformProcess, Tenant, TestDb};
use cufflink_platform::tenant_db::TenantPoolManager;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const SETTINGS: &[(&str, &str)] = &[
    ("CONSUMER_RECONCILE_INTERVAL_SECS", "1"),
    ("REGISTRY_RELOAD_SECS", "3600"),
];
const QUIET: &str = "quiet-service";
const CALLS: &str = "loan-service";
const SCHEDULES: &str = "CUFFLINK_SCHEDULES";
const WATCHED: Duration = Duration::from_secs(5);
const PERIOD_SECS: u64 = 10;
const A_FIRE_WAS_DUE: Duration = Duration::from_secs(PERIOD_SECS + 3);

struct World {
    platform: PlatformProcess,
    tenant: Tenant,
    js: jetstream::Context,
    nats_url: &'static str,
    db: TestDb,
}

async fn answers(http: &reqwest::Client, url: &str) -> bool {
    let answer = http.post(url).json(&json!({})).send().await;
    answer.expect("route").status() != reqwest::StatusCode::NOT_FOUND
}

async fn answered_during<T>(
    platform: &PlatformProcess,
    route: &str,
    doing: impl std::future::Future<Output = T>,
) -> (T, bool) {
    let done = tokio_util::sync::CancellationToken::new();
    let callers: Vec<_> = (0..4)
        .map(|_| {
            let (http, url, done) = (platform.http.clone(), route.to_string(), done.clone());
            tokio::spawn(async move {
                let mut answered = false;
                while !(answered || done.is_cancelled()) {
                    answered = answers(&http, &url).await;
                }
                answered
            })
        })
        .collect();
    let did = doing.await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    done.cancel();
    let answered = futures_util::future::join_all(callers)
        .await
        .into_iter()
        .any(|caller| caller.expect("caller"));
    (did, answered)
}

async fn world() -> Option<World> {
    let infra = common::infra().await?;
    let nats_url = infra.require_nats()?;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), SETTINGS).await;
    platform.wait_ready().await;
    Some(World {
        tenant: db.mint_tenant().await,
        js: jetstream::new(async_nats::connect(nats_url).await.expect("nats")),
        platform,
        nats_url,
        db,
    })
}

fn module(n: u8) -> Vec<u8> {
    let mut module = common::guest::module_bytes();
    module.extend([0, 4, 2, b'g', b'9', n]);
    module
}

fn quiet_service(extras: Value) -> Value {
    let mut manifest = common::wasm_manifest(QUIET, &[("echo", "/echo")]);
    manifest["subscriptions"] = json!([{
        "subject": "dw.{tenant}.quiet.ping", "handler": "hook_query", "handler_type": "wasm",
    }]);
    for (key, value) in extras.as_object().into_iter().flatten() {
        manifest[key] = value.clone();
    }
    manifest
}

fn cascade(handler: &str) -> Value {
    json!({
        "subject": format!("dw.{{tenant}}.books.{handler}"),
        "handler": handler,
        "handler_type": "delete_cascade",
        "config": {"table": "loans", "foreign_key": "book_id"},
    })
}

fn loan_service(version: &str, subscriptions: Vec<Value>) -> Value {
    calls_of(CALLS, version, subscriptions)
}

fn calls_of(name: &str, version: &str, subscriptions: Vec<Value>) -> Value {
    json!({
        "name": name,
        "version": version,
        "mode": "crud",
        "tables": [{"name": "loans", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "book_id", "column_type": "uuid", "primary_key": false, "nullable": false,
             "auto_generate": false},
        ]}],
        "subscriptions": subscriptions,
        "publishes": [],
    })
}

fn key(tenant: &Tenant) -> String {
    format!("ApiKey {}", tenant.api_key)
}

fn service_path(deployed: &Value) -> String {
    format!(
        "/api/services/{}",
        deployed["service_id"].as_str().expect("service id")
    )
}

fn durable_of(deployed: &Value, schedule: bool) -> String {
    deployed["plan"]["consumers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|op| op["durable"].as_str())
        .find(|durable| durable.starts_with("sched_") == schedule)
        .unwrap_or_else(|| panic!("no such durable in {deployed}"))
        .to_string()
}

async fn routes(replica: &PlatformProcess, rows: &str) -> bool {
    reqwest::get(format!("{}{rows}", replica.base_url))
        .await
        .expect("request")
        .status()
        .as_u16()
        != 404
}

async fn loops(platform: &PlatformProcess) -> Option<f64> {
    platform.metric("cufflink_consumer_loops", &[]).await
}

async fn set_status(platform: &PlatformProcess, tenant: &Tenant, deployed: &Value, status: &str) {
    let answer = platform
        .send(
            Method::PATCH,
            &format!("{}/status", service_path(deployed)),
            &key(tenant),
            Some(&json!({"status": status})),
        )
        .await;
    assert_eq!(
        answer,
        (200, json!({"status": status, "name": deployed["name"]})),
        "the status route answers as it always has"
    );
}

async fn consumer_info(js: &jetstream::Context, stream: &str, durable: &str) -> consumer::Info {
    js.get_stream(stream)
        .await
        .expect("stream")
        .consumer_info(durable)
        .await
        .unwrap_or_else(|e| panic!("{durable} on {stream}: {e}"))
}

async fn publish(js: &jetstream::Context, subject: String, messages: u64) {
    for n in 1..=messages {
        js.publish(subject.clone(), format!(r#"{{"id":{n}}}"#).into())
            .await
            .expect("publish")
            .await
            .expect("stored");
    }
}

struct Untouched<'a> {
    stream: &'a str,
    durable: &'a str,
    delivered: u64,
    pending: u64,
}

fn request(manifest: &Value, wasm: &[u8]) -> Value {
    json!({
        "manifest": manifest,
        "wasm_base64": base64::engine::general_purpose::STANDARD.encode(wasm),
        "api_version": 1,
    })
}

async fn deploy_as(
    platform: &PlatformProcess,
    tenant: &Tenant,
    query: &str,
    request: &Value,
) -> (u16, Value) {
    let path = format!("/api/services/deploy{query}");
    platform
        .send(Method::POST, &path, &key(tenant), Some(request))
        .await
}

async fn listed(platform: &PlatformProcess, tenant: &Tenant, name: &str) -> Option<Value> {
    let (status, listing) = platform.api_get(tenant, "/api/services").await;
    assert_eq!(status, 200, "{listing}");
    listing["services"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|service| service["name"] == name)
        .cloned()
}

fn refusal(text: &str) -> Value {
    json!({ "error": text })
}

impl World {
    async fn deploy_as(&self, query: &str, manifest: &Value, wasm: &[u8]) -> (u16, Value) {
        deploy_as(
            &self.platform,
            &self.tenant,
            query,
            &request(manifest, wasm),
        )
        .await
    }

    async fn deploy(&self, manifest: &Value, wasm: &[u8]) -> Value {
        let (status, body) = self
            .platform
            .deploy_module(&self.tenant, manifest, wasm)
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn manage(&self, method: Method, path: &str, body: Option<Value>) -> Value {
        let (status, answer) = self
            .platform
            .send(method, path, &key(&self.tenant), body.as_ref())
            .await;
        assert_eq!(status, 200, "{path}: {answer}");
        answer
    }

    async fn untouched<'a>(&self, stream: &'a str, durable: &'a str) -> Untouched<'a> {
        let info = consumer_info(&self.js, stream, durable).await;
        Untouched {
            stream,
            durable,
            delivered: info.delivered.consumer_sequence,
            pending: info.num_pending,
        }
    }

    async fn runs_nothing(&self, durables: &[&Untouched<'_>], watched: Duration, when: &str) {
        let until = Instant::now() + watched;
        while Instant::now() < until {
            for witness in durables {
                let info = consumer_info(&self.js, witness.stream, witness.durable).await;
                assert_eq!(
                    (
                        info.num_waiting,
                        info.num_ack_pending,
                        info.delivered.consumer_sequence,
                        info.num_pending,
                    ),
                    (0, 0, witness.delivered, witness.pending),
                    "{when}: {} was pulled from",
                    witness.durable
                );
            }
            assert_eq!(loops(&self.platform).await, Some(0.0), "{when}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn route(&self) -> u16 {
        self.platform.call_fn(&self.tenant, QUIET, "echo").await.0
    }

    async fn unrouted_during<T>(
        &self,
        doing: impl std::future::Future<Output = T>,
        what: &str,
    ) -> T {
        let route = self.platform.fn_url(&self.tenant, QUIET, "echo");
        let (did, answered) = answered_during(&self.platform, &route, doing).await;
        assert!(!answered, "the inactive service answered during {what}");
        did
    }

    async fn tenant_db(&self) -> deadpool_postgres::Object {
        let infra = common::infra().await.expect("infra");
        cufflink_db::build_pool(
            &infra::with_dbname(
                &infra.database_url,
                &TenantPoolManager::managed_db_name(&self.tenant.slug),
            ),
            1,
        )
        .expect("tenant pool")
        .get()
        .await
        .expect("tenant database")
    }

    async fn migrations_hooked(&self) -> i64 {
        self.tenant_db()
            .await
            .query_one(
                "SELECT count(*) FROM quiet_service.items WHERE title = 'hook'",
                &[],
            )
            .await
            .expect("items")
            .get(0)
    }

    async fn fires(&self) -> f64 {
        let text = self.platform.get_text("/metrics").await.unwrap_or_default();
        ["ok", "error", "coalesced", "enqueued"]
            .iter()
            .filter_map(|result| {
                infra::metric_value(
                    &text,
                    "cufflink_schedule_runs_total",
                    &[("tenant", self.tenant.slug.as_str()), ("result", result)],
                )
            })
            .sum()
    }

    async fn current(&self, deployed: &Value) -> (i64, String) {
        let service = self
            .manage(Method::GET, &service_path(deployed), None)
            .await;
        (
            service["current_version"].as_i64().expect("version"),
            service["status"].as_str().expect("status").to_string(),
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g9_deploy_of_an_inactive_service_runs_nothing() {
    let Some(w) = world().await else {
        return;
    };
    let manifest = quiet_service(json!({
        "schedules": [{"name": "tick", "handler": "hook_query", "spec": {"kind": "every", "secs": PERIOD_SECS}}],
        "on_migrate": "insert_slow",
    }));
    let deployed = w.deploy(&manifest, &module(1)).await;
    let version = deployed["version"].as_i64().expect("version");
    let stream = tenant_stream(&w.tenant);
    let (consumed, fired) = (durable_of(&deployed, false), durable_of(&deployed, true));
    until_loops(&[&w.platform], 2.0).await.expect("in time");
    assert_eq!(w.route().await, 200);
    assert_eq!(w.migrations_hooked().await, 1);
    until("the schedule fired", Duration::from_secs(30), || async {
        w.fires().await >= 1.0
    })
    .await
    .expect("in time");

    set_status(&w.platform, &w.tenant, &deployed, "inactive").await;
    until_loops(&[&w.platform], 0.0).await.expect("in time");
    until("its pulls expired", Duration::from_secs(40), || async {
        consumer_info(&w.js, &stream, &consumed).await.num_waiting == 0
            && consumer_info(&w.js, SCHEDULES, &fired).await.num_waiting == 0
    })
    .await
    .expect("in time");
    assert_eq!(w.route().await, 404);
    publish(&w.js, format!("dw.{}.quiet.ping", w.tenant.slug), 3).await;
    let fires = w.fires().await;
    let ping = w.untouched(&stream, &consumed).await;
    let tick = w.untouched(SCHEDULES, &fired).await;
    assert_eq!(ping.pending, 3);

    let redeployed = w
        .unrouted_during(w.deploy(&manifest, &module(2)), "its deploy")
        .await;
    assert_eq!(
        (&redeployed["version"], redeployed.get("skipped")),
        (&json!(version + 1), None),
        "{redeployed}"
    );
    w.runs_nothing(&[&ping, &tick], A_FIRE_WAS_DUE, "after a deploy")
        .await;
    assert_eq!(w.route().await, 404);
    assert_eq!(w.fires().await, fires, "the schedule did not fire");
    assert_eq!(
        w.migrations_hooked().await,
        2,
        "on_migrate ran, as on every deploy that is not skipped"
    );
    assert_eq!(
        w.current(&deployed).await,
        (version + 1, "inactive".to_string())
    );

    set_status(&w.platform, &w.tenant, &deployed, "active").await;
    until_loops(&[&w.platform], 2.0).await.expect("in time");
    until(
        "the version deployed while it was inactive runs",
        Duration::from_secs(40),
        || async {
            let info = consumer_info(&w.js, &stream, &consumed).await;
            w.route().await == 200
                && info.num_pending == 0
                && info.num_ack_pending == 0
                && w.fires().await > fires
        },
    )
    .await
    .expect("in time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g9_rollback_and_upload_keep_a_service_inactive() {
    let Some(w) = world().await else {
        return;
    };
    let manifest = quiet_service(json!({}));
    let first = w.deploy(&manifest, &module(1)).await;
    let version = first["version"].as_i64().expect("version");
    w.deploy(&manifest, &module(2)).await;
    let stream = tenant_stream(&w.tenant);
    let consumed = durable_of(&first, false);
    until_loops(&[&w.platform], 1.0).await.expect("in time");

    set_status(&w.platform, &w.tenant, &first, "inactive").await;
    until_loops(&[&w.platform], 0.0).await.expect("in time");
    until("its pulls expired", Duration::from_secs(40), || async {
        consumer_info(&w.js, &stream, &consumed).await.num_waiting == 0
    })
    .await
    .expect("in time");
    publish(&w.js, format!("dw.{}.quiet.ping", w.tenant.slug), 2).await;
    let ping = w.untouched(&stream, &consumed).await;
    assert_eq!(ping.pending, 2);

    let (rollback, upload) = (
        format!("{}/rollback", service_path(&first)),
        format!("{}/wasm", service_path(&first)),
    );
    let rolling_back = w.manage(
        Method::POST,
        &rollback,
        Some(json!({"target_version": version})),
    );
    let rolled_back = w.unrouted_during(rolling_back, "its rollback").await;
    assert_eq!(rolled_back["new_version"], version + 2, "{rolled_back}");
    w.runs_nothing(&[&ping], WATCHED, "after a rollback").await;
    assert_eq!(w.route().await, 404);

    let uploading = w.manage(
        Method::POST,
        &upload,
        Some(json!({
            "wasm_base64": base64::engine::general_purpose::STANDARD.encode(module(3)),
        })),
    );
    let uploaded = w.unrouted_during(uploading, "its module upload").await;
    assert_eq!(uploaded["version"], version + 3, "{uploaded}");
    w.runs_nothing(&[&ping], WATCHED, "after a module upload")
        .await;
    assert_eq!(w.route().await, 404);
    assert_eq!(
        w.current(&first).await,
        (version + 3, "inactive".to_string())
    );

    set_status(&w.platform, &w.tenant, &first, "active").await;
    until_loops(&[&w.platform], 1.0).await.expect("in time");
    until(
        "the uploaded version runs",
        Duration::from_secs(40),
        || async {
            let info = consumer_info(&w.js, &stream, &consumed).await;
            w.route().await == 200 && info.num_pending == 0 && info.num_ack_pending == 0
        },
    )
    .await
    .expect("in time");
}

#[tokio::test]
async fn g9_an_inactive_service_stays_unrouted_where_no_reload_follows() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let settings: Vec<(&str, &str)> = SETTINGS
        .iter()
        .copied()
        .chain([("NATS_PUBLISH_TIMEOUT_MS", "300")])
        .collect();
    let platform = PlatformProcess::start(&db.url, None, &settings).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let manifest = common::wasm_manifest(QUIET, &[("echo", "/echo")]);
    let deploy = |n: u8| {
        let (platform, tenant, manifest) = (&platform, &tenant, &manifest);
        async move {
            let (status, body) = platform.deploy_module(tenant, manifest, &module(n)).await;
            assert_eq!(status, 200, "{body}");
            body
        }
    };
    let manage = |method: Method, path: String, body: Value| {
        let (platform, tenant) = (&platform, &tenant);
        async move {
            let (status, answer) = platform
                .send(method, &path, &key(tenant), Some(&body))
                .await;
            assert_eq!(status, 200, "{path}: {answer}");
            answer
        }
    };
    let route = || async { platform.call_fn(&tenant, QUIET, "echo").await.0 };

    let first = deploy(1).await;
    let version = first["version"].as_i64().expect("version");
    deploy(2).await;
    assert_eq!(route().await, 200);
    set_status(&platform, &tenant, &first, "inactive").await;
    assert_eq!(route().await, 404);

    assert_eq!(deploy(3).await["version"], version + 2);
    assert_eq!(route().await, 404, "after a deploy");
    let rolled_back = manage(
        Method::POST,
        format!("{}/rollback", service_path(&first)),
        json!({"target_version": version}),
    )
    .await;
    assert_eq!(rolled_back["new_version"], version + 3, "{rolled_back}");
    assert_eq!(route().await, 404, "after a rollback");
    let uploaded = manage(
        Method::POST,
        format!("{}/wasm", service_path(&first)),
        json!({"wasm_base64": base64::engine::general_purpose::STANDARD.encode(module(4))}),
    )
    .await;
    assert_eq!(uploaded["version"], version + 4, "{uploaded}");
    assert_eq!(route().await, 404, "after a module upload");

    set_status(&platform, &tenant, &first, "active").await;
    assert_eq!(route().await, 200, "enabling registers the one service");
    let health = platform
        .get_json(&format!("/svc/{}/{QUIET}/_meta/health", tenant.slug))
        .await
        .expect("health");
    assert_eq!(
        (health.0, &health.1["version"]),
        (200, &json!(version + 4)),
        "at the version it was last given: {health:?}"
    );
}

#[tokio::test]
async fn g9_skipped_deploy_of_an_inactive_service_starts_nothing() {
    let Some(w) = world().await else {
        return;
    };
    let stream_name = tenant_stream(&w.tenant);
    let bound = format!("bound-{}", w.tenant.slug);
    let mut binds = cascade("merged");
    binds["consumer"] = json!({"durable_name": bound});
    let manifest = loan_service("0.1.0", vec![cascade("deleted"), binds]);
    let deployed = w.deploy(&manifest, &module(1)).await;
    until_loops(&[&w.platform], 2.0).await.expect("in time");

    set_status(&w.platform, &w.tenant, &deployed, "inactive").await;
    until_loops(&[&w.platform], 0.0).await.expect("in time");
    until("its pulls expired", Duration::from_secs(40), || async {
        consumer_info(&w.js, &stream_name, "loan_service_deleted")
            .await
            .num_waiting
            == 0
    })
    .await
    .expect("in time");
    w.js.get_stream(&stream_name)
        .await
        .expect("stream")
        .delete_consumer(&bound)
        .await
        .expect("the bound durable is lost");

    let again = w.deploy(&manifest, &module(1)).await;
    assert_eq!(again["skipped"], true, "{again}");
    let healed = w.untouched(&stream_name, &bound).await;
    let managed = w.untouched(&stream_name, "loan_service_deleted").await;
    publish(&w.js, format!("dw.{}.books.merged", w.tenant.slug), 1).await;
    let healed = Untouched {
        pending: 1,
        ..healed
    };
    w.runs_nothing(&[&healed, &managed], WATCHED, "after a skipped deploy")
        .await;
    assert_eq!(
        w.current(&deployed).await,
        (
            deployed["version"].as_i64().expect("version"),
            "inactive".to_string()
        )
    );
}

#[tokio::test]
async fn g9_an_inactive_deploy_creates_a_managed_durable_on_an_interest_stream() {
    let Some(w) = world().await else {
        return;
    };
    let stream_name = tenant_stream(&w.tenant);
    let mut stream =
        w.js.create_stream(stream::Config {
            name: stream_name.clone(),
            subjects: vec![format!("dw.{}.>", w.tenant.slug)],
            retention: stream::RetentionPolicy::Interest,
            ..Default::default()
        })
        .await
        .expect("the tenant's stream");
    let deployed = w
        .deploy(&loan_service("0.1.0", vec![cascade("deleted")]), &module(1))
        .await;
    until_loops(&[&w.platform], 1.0).await.expect("in time");
    set_status(&w.platform, &w.tenant, &deployed, "inactive").await;
    until_loops(&[&w.platform], 0.0).await.expect("in time");

    let widened = w
        .deploy(
            &loan_service("0.2.0", vec![cascade("deleted"), cascade("merged")]),
            &module(1),
        )
        .await;
    let created = widened["plan"]["consumers"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|op| op["durable"] == "loan_service_merged")
        .unwrap_or_else(|| panic!("no operation on the new durable: {widened}"));
    assert_eq!(created["op"], "create", "{widened}");
    let merged = w.untouched(&stream_name, "loan_service_merged").await;

    publish(&w.js, format!("dw.{}.books.merged", w.tenant.slug), 2).await;
    publish(&w.js, format!("dw.{}.books.unwanted", w.tenant.slug), 1).await;
    let merged = Untouched {
        pending: 2,
        ..merged
    };
    w.runs_nothing(&[&merged], WATCHED, "after a deploy").await;
    assert_eq!(
        stream.info().await.expect("stream").state.messages,
        2,
        "an Interest stream keeps what the new durable has yet to take, and nothing else"
    );

    set_status(&w.platform, &w.tenant, &deployed, "active").await;
    until_loops(&[&w.platform], 2.0).await.expect("in time");
    until(
        "what it held was delivered",
        Duration::from_secs(40),
        || async {
            consumer_info(&w.js, &stream_name, "loan_service_merged")
                .await
                .delivered
                .consumer_sequence
                >= 2
        },
    )
    .await
    .expect("in time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g9_two_replicas() {
    const REPETITIONS: usize = 50;
    let Some(w) = world().await else {
        return;
    };
    let a = &w.platform;
    let b = PlatformProcess::start(&w.db.url, Some(w.nats_url), SETTINGS).await;
    b.wait_ready().await;
    let stream = tenant_stream(&w.tenant);
    let manifest = |n: usize| loan_service(&format!("0.1.{n}"), vec![cascade("deleted")]);
    let deployed = w.deploy(&manifest(0), &module(1)).await;
    let rows = format!("/svc/{}/{CALLS}/loans", w.tenant.slug);
    let routed = |replica| routes(replica, &rows);

    let mut deploys = 0;
    for repetition in 1..=REPETITIONS {
        until_loops(&[a, &b], 1.0).await.expect("in time");
        until(
            "both replicas route it",
            Duration::from_secs(40),
            || async { routed(a).await && routed(&b).await },
        )
        .await
        .expect("in time");

        let stagger = Duration::from_millis([0, 2, 10, 40][(repetition / 2) % 4]);
        let (disable_after, deploy_after) = if repetition % 2 == 0 {
            (Duration::ZERO, stagger)
        } else {
            (stagger, Duration::ZERO)
        };
        let disable = async {
            tokio::time::sleep(disable_after).await;
            set_status(a, &w.tenant, &deployed, "inactive").await;
        };
        let deploy = async {
            tokio::time::sleep(deploy_after).await;
            b.deploy(&w.tenant, &manifest(repetition)).await
        };
        let ((), (status, body)) = tokio::join!(disable, deploy);
        assert!(
            status == 200 || status == 409,
            "repetition {repetition}: {status} {body}"
        );
        deploys += usize::from(status == 200);

        until(
            "neither replica runs the disabled service",
            Duration::from_secs(40),
            || async {
                !routed(a).await
                    && !routed(&b).await
                    && loops(a).await == Some(0.0)
                    && loops(&b).await == Some(0.0)
                    && consumer_info(&w.js, &stream, "loan_service_deleted")
                        .await
                        .num_waiting
                        == 0
            },
        )
        .await
        .expect("in time");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !routed(a).await && !routed(&b).await,
            "repetition {repetition}: a replica routes the disabled service again"
        );
        assert_eq!(
            (loops(a).await, loops(&b).await),
            (Some(0.0), Some(0.0)),
            "repetition {repetition}"
        );
        assert_eq!(
            w.current(&deployed).await.1,
            "inactive",
            "repetition {repetition}"
        );

        let enabler = if repetition % 2 == 0 { a } else { &b };
        set_status(enabler, &w.tenant, &deployed, "active").await;
    }
    assert!(
        deploys >= REPETITIONS / 2,
        "only {deploys} of {REPETITIONS} deploys ran beside the disable"
    );
}

const ACTIVE_REFUSAL: &str = "Service 'loan-service' is active; a deploy never stops a running \
                              service. Disable it first, or deploy without --inactive";
const WEB_REFUSAL: &str = "a web app is started by its runner whatever its service's status; \
                           deploy it without --inactive";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g9_first_deploy_inactive() {
    let Some(w) = world().await else {
        return;
    };
    let stream_name = tenant_stream(&w.tenant);
    let subject = format!("dw.{}.books.merged", w.tenant.slug);
    let stream =
        w.js.create_stream(stream::Config {
            name: stream_name.clone(),
            subjects: vec![format!("dw.{}.>", w.tenant.slug)],
            ..Default::default()
        })
        .await
        .expect("the tenant's stream");
    let bound = format!("bound-{}", w.tenant.slug);
    let durable = stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(bound.clone()),
            filter_subject: subject.clone(),
            ack_policy: consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .expect("the durable of the consumer it replaces");
    let legacy = common::jetstream::Foreign::pulling(&durable)
        .await
        .expect("a pull of the durable");
    let mut binds = cascade("merged");
    binds["consumer"] = json!({"durable_name": bound});
    let manifest = loan_service("0.1.0", vec![binds]);
    let kept = json!([{"op": "keep", "stream": stream_name, "durable": bound}]);
    let rows = format!("/svc/{}/{CALLS}/loans", w.tenant.slug);
    let routed = || async {
        w.platform
            .get_json(&rows)
            .await
            .is_some_and(|(status, _)| status != 404)
    };

    let (deployed, waiting) = common::jetstream::most_waiting(&durable, async {
        let (status, plan) = w
            .deploy_as("?plan=true&inactive=true", &manifest, &module(1))
            .await;
        assert_eq!(
            (
                status,
                &plan["status"],
                &plan["new"],
                &plan["plan"]["consumers"]
            ),
            (200, &json!("inactive"), &json!(true), &kept),
            "{plan}"
        );
        assert_eq!(
            listed(&w.platform, &w.tenant, CALLS).await,
            None,
            "a plan creates nothing"
        );

        let (status, deployed) = w.deploy_as("?inactive=true", &manifest, &module(1)).await;
        assert_eq!(
            (status, &deployed["status"], &deployed["plan"]["consumers"]),
            (200, &json!("inactive"), &kept),
            "{deployed}"
        );
        assert_eq!(w.current(&deployed).await.1, "inactive");
        assert!(!routed().await);

        publish(&w.js, subject.clone(), 3).await;
        until(
            "the consumer it replaces took what was published",
            Duration::from_secs(20),
            || async { legacy.taken() == 3 },
        )
        .await
        .expect("in time");
        let watched = Instant::now() + WATCHED;
        while Instant::now() < watched {
            assert_eq!(loops(&w.platform).await, Some(0.0));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        deployed
    })
    .await
    .expect("the durable's state");
    assert_eq!(waiting, 1, "the durable never had a second subscriber");

    assert_eq!(legacy.stop().await, 3);
    set_status(&w.platform, &w.tenant, &deployed, "active").await;
    until_loops(&[&w.platform], 1.0).await.expect("in time");
    assert!(routed().await);
    publish(&w.js, subject, 2).await;
    until(
        "the service consumes from the durable it was bound to",
        Duration::from_secs(40),
        || async {
            let info = consumer_info(&w.js, &stream_name, &bound).await;
            (info.ack_floor.consumer_sequence, info.num_ack_pending) == (5, 0)
        },
    )
    .await
    .expect("in time");
}

#[tokio::test]
async fn g9_inactive_refused_for_an_active_service() {
    let Some(w) = world().await else {
        return;
    };
    let manifest = |n: u8| loan_service(&format!("0.1.{n}"), vec![cascade("deleted")]);
    let deployed = w.deploy(&manifest(0), &module(1)).await;
    assert_eq!(deployed["status"], "active", "{deployed}");
    let version = deployed["version"].as_i64().expect("version");
    until_loops(&[&w.platform], 1.0).await.expect("in time");
    let jobs = || async {
        let recorded: i64 =
            w.db.pool()
                .get()
                .await
                .expect("platform database")
                .query_one("SELECT count(*) FROM deploy_jobs", &[])
                .await
                .expect("deploy jobs")
                .get(0);
        recorded
    };

    for query in [
        "?inactive=true",
        "?plan=true&inactive=true",
        "?async=true&inactive=true",
    ] {
        assert_eq!(
            w.deploy_as(query, &manifest(1), &module(1)).await,
            (409, refusal(ACTIVE_REFUSAL)),
            "{query}"
        );
    }
    assert_eq!(jobs().await, 0, "a refused deploy is no job");
    assert_eq!(w.current(&deployed).await, (version, "active".to_string()));
    assert_eq!(loops(&w.platform).await, Some(1.0), "it still runs");
    let (status, plan) = w.deploy_as("?plan=true", &manifest(1), &module(1)).await;
    assert_eq!((status, &plan["status"]), (200, &json!("active")), "{plan}");

    set_status(&w.platform, &w.tenant, &deployed, "inactive").await;
    until_loops(&[&w.platform], 0.0).await.expect("in time");
    let (status, plan) = w
        .deploy_as("?plan=true&inactive=true", &manifest(1), &module(1))
        .await;
    assert_eq!(
        (status, &plan["status"], &plan["new"]),
        (200, &json!("inactive"), &json!(false)),
        "{plan}"
    );
    let (status, begun) = w
        .deploy_as("?async=true&inactive=true", &manifest(1), &module(1))
        .await;
    assert_eq!(
        (status, &begun["status"]),
        (202, &json!("queued")),
        "{begun}"
    );
    let job = format!(
        "/api/deploys/{}",
        begun["deploy_id"].as_str().expect("deploy id")
    );
    until("the deploy ended", Duration::from_secs(60), || async {
        w.manage(Method::GET, &job, None).await["status"] == "succeeded"
    })
    .await
    .expect("in time");
    let ended = w.manage(Method::GET, &job, None).await;
    assert_eq!(
        (
            &ended["request"]["status"],
            &ended["result"]["status"],
            &ended["result"]["version"]
        ),
        (&json!("inactive"), &json!("inactive"), &json!(version + 1)),
        "{ended}"
    );
    assert_eq!(jobs().await, 1);
    assert_eq!(loops(&w.platform).await, Some(0.0));

    let (status, plain) = w.deploy_as("", &manifest(2), &module(1)).await;
    assert_eq!(
        (status, &plain["status"], &plain["version"]),
        (200, &json!("inactive"), &json!(version + 2)),
        "a deploy that does not ask leaves it inactive too: {plain}"
    );
    let (status, skipped) = w
        .deploy_as("?inactive=true", &manifest(2), &module(1))
        .await;
    assert_eq!(
        (status, &skipped["status"], &skipped["skipped"]),
        (200, &json!("inactive"), &json!(true)),
        "{skipped}"
    );
}

#[tokio::test]
async fn g9_inactive_is_refused_for_a_web_app() {
    let Some(w) = world().await else {
        return;
    };
    let site =
        json!({"manifest": {"name": "site", "mode": "web", "version": "0.1.0", "tables": []}});
    let deploy = |query: &'static str| deploy_as(&w.platform, &w.tenant, query, &site);
    for query in ["?inactive=true", "?plan=true&inactive=true"] {
        assert_eq!(deploy(query).await, (422, refusal(WEB_REFUSAL)), "{query}");
    }
    assert_eq!(
        listed(&w.platform, &w.tenant, "site").await,
        None,
        "a refused deploy creates nothing"
    );

    let (status, deployed) = deploy("").await;
    assert_eq!(
        (status, &deployed["status"]),
        (200, &json!("active")),
        "{deployed}"
    );
    set_status(&w.platform, &w.tenant, &deployed, "inactive").await;
    assert_eq!(
        deploy("?inactive=true").await,
        (422, refusal(WEB_REFUSAL)),
        "whatever its status"
    );
}

#[tokio::test]
async fn g9_a_failed_first_inactive_deploy_leaves_the_service_inactive() {
    let Some(w) = world().await else {
        return;
    };
    let broken = common::wasm_manifest(QUIET, &[("no_such_handler", "/echo")]);
    let manifest = common::wasm_manifest(QUIET, &[("echo", "/echo")]);
    let (status, failed) = w.deploy_as("?inactive=true", &broken, &module(1)).await;
    assert!(status >= 400, "{status} {failed}");
    let created = listed(&w.platform, &w.tenant, QUIET)
        .await
        .expect("the row its first deploy created");
    assert_eq!(
        (&created["status"], &created["mode"]),
        (&json!("inactive"), &json!("")),
        "inactive, and nothing deployed: {created}"
    );
    let service = json!({"service_id": created["id"], "name": QUIET});
    let undeployed = created["current_version"].as_i64().expect("version");

    let (status, plan) = w.deploy_as("?plan=true", &manifest, &module(1)).await;
    assert_eq!(
        (status, &plan["status"], &plan["new"]),
        (200, &json!("inactive"), &json!(false)),
        "{plan}"
    );
    let retried = w
        .unrouted_during(w.deploy_as("", &manifest, &module(1)), "its first version")
        .await;
    assert_eq!(
        (retried.0, &retried.1["status"], &retried.1["version"]),
        (200, &json!("inactive"), &json!(undeployed + 1)),
        "{retried:?}"
    );
    assert_eq!(w.route().await, 404);
    set_status(&w.platform, &w.tenant, &service, "active").await;
    assert_eq!(w.route().await, 200);

    let other = "quiet-too";
    let (status, failed) = w
        .deploy_as(
            "?inactive=true",
            &common::wasm_manifest(other, &[("no_such_handler", "/echo")]),
            &module(1),
        )
        .await;
    assert!(status >= 400, "{status} {failed}");
    let created = listed(&w.platform, &w.tenant, other)
        .await
        .expect("the row its first deploy created");
    let service = json!({"service_id": created["id"], "name": other});
    set_status(&w.platform, &w.tenant, &service, "active").await;
    assert_eq!(w.platform.call_fn(&w.tenant, other, "echo").await.0, 404);
    let (status, deployed) = w
        .deploy_as(
            "",
            &common::wasm_manifest(other, &[("echo", "/echo")]),
            &module(1),
        )
        .await;
    assert_eq!(
        (status, &deployed["status"]),
        (200, &json!("active")),
        "{deployed}"
    );
    assert_eq!(w.platform.call_fn(&w.tenant, other, "echo").await.0, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g9_enable_racing_a_first_inactive_deploy() {
    const REPETITIONS: usize = 16;
    let Some(w) = world().await else {
        return;
    };
    let a = &w.platform;
    let b = PlatformProcess::start(&w.db.url, Some(w.nats_url), SETTINGS).await;
    b.wait_ready().await;

    let mut raced = 0;
    for repetition in 1..=REPETITIONS {
        let name = format!("loans-{repetition}");
        let request = request(
            &calls_of(&name, "0.1.0", vec![cascade("deleted")]),
            &module(1),
        );
        let rows = format!("/svc/{}/{name}/loans", w.tenant.slug);
        let routed = |replica| routes(replica, &rows);
        let stagger = Duration::from_millis([0, 5, 20, 80][repetition % 4]);
        let deploy = deploy_as(&b, &w.tenant, "?inactive=true", &request);
        let enable = async {
            let created = loop {
                match listed(a, &w.tenant, &name).await {
                    Some(created) => break created,
                    None => tokio::time::sleep(Duration::from_millis(2)).await,
                }
            };
            tokio::time::sleep(stagger).await;
            let service = json!({"service_id": created["id"], "name": name});
            set_status(a, &w.tenant, &service, "active").await;
            (service, listed(a, &w.tenant, &name).await)
        };
        let ((status, deployed), (service, enabled)) = tokio::join!(deploy, enable);
        assert_eq!(status, 200, "repetition {repetition}: {deployed}");
        assert!(
            deployed["status"] == "inactive" || deployed["status"] == "active",
            "repetition {repetition}: {deployed}"
        );
        raced += usize::from(enabled.is_some_and(|service| service["mode"] == ""));

        until(
            "both replicas run the enabled service",
            Duration::from_secs(40),
            || async {
                routed(a).await
                    && routed(&b).await
                    && loops(a).await == Some(repetition as f64)
                    && loops(&b).await == Some(repetition as f64)
            },
        )
        .await
        .expect("in time");
        assert_eq!(
            w.current(&service).await,
            (
                deployed["version"].as_i64().expect("version"),
                "active".to_string()
            ),
            "repetition {repetition}"
        );
    }
    assert!(
        raced >= REPETITIONS / 4,
        "only {raced} of {REPETITIONS} enables landed while the deploy ran"
    );
}
