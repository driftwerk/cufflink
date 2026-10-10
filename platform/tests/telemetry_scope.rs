use cufflink_testkit as common;

use base64::Engine as _;
use common::guest::SAID;
use common::otlp::{Capture, Signal};
use common::{infra, until, until_loops, PlatformProcess, Tenant, TestDb};
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

const NOTES: &str = "notes";
const PERIOD_SECS: u64 = 10;
const SETTINGS: &[(&str, &str)] = &[("CONSUMER_RECONCILE_INTERVAL_SECS", "1")];
const PUBLIC: &str = r#"{"kind": "public", "reason": "integration test"}"#;

struct World {
    capture: Capture,
    platform: PlatformProcess,
    tenant: Tenant,
    nats_url: &'static str,
    _db: TestDb,
}

async fn world() -> Option<World> {
    let infra = infra().await?;
    let nats_url = infra.require_nats()?;
    let capture = Capture::start().await.expect("capture");
    let mut db = infra.create_database().await;
    let settings: Vec<_> = SETTINGS.iter().copied().chain(capture.env()).collect();
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &settings).await;
    platform.wait_ready().await;
    Some(World {
        tenant: db.mint_tenant().await,
        capture,
        platform,
        nats_url,
        _db: db,
    })
}

fn route(path: &str, handler: &str) -> Value {
    json!({
        "method": "POST",
        "path": path,
        "handler": handler,
        "posture": serde_json::from_str::<Value>(PUBLIC).expect("posture"),
    })
}

fn notes() -> Value {
    let mut manifest = common::wasm_manifest(NOTES, &[]);
    manifest["routing"] = json!("strict");
    manifest["custom_routes"] = json!([route("/speak/{to}", "say")]);
    manifest["tables"][0]["hooks"] = json!({"before_create": "say"});
    manifest["subscriptions"] = json!([{
        "subject": "dw.{tenant}.notes.ping", "handler": "say", "handler_type": "wasm",
    }]);
    manifest["schedules"] = json!([{
        "name": "tick", "handler": "say", "spec": {"kind": "every", "secs": PERIOD_SECS},
    }]);
    manifest["on_migrate"] = json!("say");
    manifest
}

impl World {
    async fn deploy(&self, manifest: &Value, wasm: &[u8]) -> String {
        let (status, body) = self
            .platform
            .deploy_module(&self.tenant, manifest, wasm)
            .await;
        assert_eq!(status, 200, "{body}");
        format!("v{}", body["version"].as_i64().expect("version"))
    }

    async fn send(&self, method: Method, path: &str, body: Option<Value>) -> u16 {
        self.platform
            .send(
                method,
                &format!("/svc/{}{path}", self.tenant.slug),
                &format!("ApiKey {}", self.tenant.api_key),
                body.as_ref(),
            )
            .await
            .0
    }

    fn scope<'a>(
        &'a self,
        of: (&'a str, &'a str),
        handler: &'a str,
        kind: &'a str,
    ) -> Vec<(&'a str, &'a str)> {
        vec![
            ("cufflink.handler", handler),
            ("cufflink.kind", kind),
            ("cufflink.service", of.0),
            ("cufflink.tenant", &self.tenant.slug),
            ("cufflink.version", of.1),
        ]
    }

    async fn said(&self, kind: &str) -> (Signal, Signal) {
        let line = format!("[wasm] {SAID}");
        let log = self
            .capture
            .log(&format!("the guest's line as a {kind}"), |record| {
                record.text == line && record.attribute("cufflink.kind") == Some(kind)
            })
            .await
            .expect("exported");
        let handler = self
            .capture
            .span(&format!("the handler span of the {kind}"), |span| {
                span.span_id == log.span_id
            })
            .await
            .expect("exported");
        (log, handler)
    }

    async fn span(&self, name: &str, kind: Option<&str>) -> Signal {
        self.capture
            .span(&format!("a span named {name} of kind {kind:?}"), |span| {
                span.text == name && span.attribute("cufflink.kind") == kind
            })
            .await
            .expect("exported")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf3_each_execution_exports_its_scope() {
    let Some(w) = world().await else {
        return;
    };
    let slug = w.tenant.slug.as_str();
    let named = |leaf: &str| format!("{slug}/{NOTES}/{leaf}");
    let version = w.deploy(&notes(), &common::guest::module_bytes()).await;
    let notes = (NOTES, version.as_str());
    until_loops(&[&w.platform], 2.0).await.expect("in time");

    let (log, handler) = w.said("on_migrate").await;
    assert_eq!(log.scope(), w.scope(notes, "say", "on_migrate"));
    assert_eq!(handler.scope(), log.scope());
    assert_eq!(handler.text, "wasm_handler");

    assert_eq!(
        w.send(Method::POST, "/notes/_fn/say", Some(json!({})))
            .await,
        200
    );
    let (log, handler) = w.said("route").await;
    assert_eq!(log.scope(), w.scope(notes, "say", "route"));
    assert_eq!(handler.scope(), log.scope());
    let request = w.span(&named("say"), Some("route")).await;
    assert_eq!(request.scope(), w.scope(notes, "say", "route"));
    assert_eq!(request.trace_id, log.trace_id);
    assert_eq!(handler.parent_span_id, request.span_id);

    assert_eq!(
        w.send(Method::POST, "/notes/speak/everyone", Some(json!({})))
            .await,
        200
    );
    let declared = w.span(&named("speak"), Some("route")).await;
    assert_eq!(declared.scope(), w.scope(notes, "say", "route"));

    assert_eq!(
        w.send(
            Method::POST,
            "/notes/items",
            Some(json!({"title": "first"}))
        )
        .await,
        200
    );
    let (log, hook) = w.said("hook").await;
    assert_eq!(log.scope(), w.scope(notes, "say", "hook"));
    assert_eq!(hook.scope(), log.scope());
    let crud = w.span(&named("items"), Some("crud")).await;
    assert_eq!(
        crud.scope(),
        [
            ("cufflink.kind", "crud"),
            ("cufflink.service", NOTES),
            ("cufflink.tenant", slug),
            ("cufflink.version", notes.1),
        ]
    );
    assert_eq!(hook.parent_span_id, crud.span_id);

    let js = async_nats::jetstream::new(async_nats::connect(w.nats_url).await.expect("nats"));
    js.publish(format!("dw.{slug}.notes.ping"), "{}".into())
        .await
        .expect("publish")
        .await
        .expect("stored");
    let (log, handler) = w.said("subscription").await;
    assert_eq!(log.scope(), w.scope(notes, "say", "subscription"));
    assert_eq!(handler.scope(), log.scope());
    let delivery = w.span(&named("say"), Some("subscription")).await;
    assert_eq!(delivery.scope(), log.scope());
    assert_eq!(delivery.parent_span_id, "", "a message is its own trace");
    assert_eq!(handler.parent_span_id, delivery.span_id);

    let (log, handler) = w.said("schedule").await;
    assert_eq!(log.scope(), w.scope(notes, "say", "schedule"));
    assert_eq!(handler.scope(), log.scope());
    let fire = w.span(&named("say"), Some("schedule")).await;
    assert_eq!(fire.scope(), log.scope());
    assert_eq!(fire.parent_span_id, "", "a fire is its own trace");

    let service = [
        ("cufflink.service", NOTES),
        ("cufflink.tenant", slug),
        ("cufflink.version", notes.1),
    ];
    w.send(Method::GET, "/notes/_meta/health", None).await;
    let meta = w.span(&named("_meta/health"), None).await;
    assert_eq!(meta.scope(), service);
    let query = json!({"query": "{ __typename }"});
    w.send(Method::POST, "/notes/graphql", Some(query)).await;
    let graphql = w.span(&named("graphql"), Some("crud")).await;
    assert_eq!(graphql.scope()[0], ("cufflink.kind", "crud"));
    assert_eq!(graphql.scope()[1..], service);

    let job = uuid::Uuid::new_v4();
    assert_eq!(
        w.send(Method::GET, &format!("/_jobs/{job}"), None).await,
        404
    );
    let poll = w.span(&format!("{slug}/_jobs"), None).await;
    assert_eq!(poll.scope(), [("cufflink.tenant", slug)]);

    for signal in w.capture.spans().iter().chain(&w.capture.logs()) {
        assert_eq!(
            signal.resource.get("service.name").map(String::as_str),
            Some("cufflink-platform"),
            "the process's resource is the binary's: {signal:?}"
        );
        assert_eq!(
            signal
                .resource
                .get("cufflink.component")
                .map(String::as_str),
            Some("platform")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf3_a_guest_log_line_and_its_host_calls_carry_scope_and_trace() {
    let Some(w) = world().await else {
        return;
    };
    let manifest = common::wasm_manifest(NOTES, &[("say", "/say")]);
    let version = w.deploy(&manifest, &common::guest::module_bytes()).await;
    let notes = (NOTES, version.as_str());
    assert_eq!(
        w.send(Method::POST, "/notes/_fn/say", Some(json!({})))
            .await,
        200
    );

    let (log, handler) = w.said("route").await;
    let request = w
        .span(&format!("{}/{NOTES}/say", w.tenant.slug), Some("route"))
        .await;
    assert_eq!(
        (log.trace_id.as_str(), log.span_id.as_str()),
        (request.trace_id.as_str(), handler.span_id.as_str()),
        "the line names the trace of its request and the span it was said in"
    );
    assert_eq!(log.trace_id.len(), 32);
    assert_ne!(log.trace_id, "0".repeat(32));

    let query = w
        .capture
        .span("the guest's query", |span| {
            span.text.starts_with("db.") && span.parent_span_id == handler.span_id
        })
        .await
        .expect("exported");
    assert_eq!(query.scope(), w.scope(notes, "say", "route"));
    assert_eq!(query.trace_id, request.trace_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf3_a_propagated_call_keeps_one_trace() {
    let Some(wasm) = common::sdk::sdk_guest("internal", Profile::Debug) else {
        return;
    };
    let Some(w) = world().await else {
        return;
    };
    let strict = |name: &str, routes: Value| {
        let mut manifest = common::wasm_manifest(name, &[]);
        manifest["routing"] = json!("strict");
        manifest["custom_routes"] = routes;
        manifest
    };
    let front = w
        .deploy(&strict("front", json!([route("/hop", "hop")])), &wasm)
        .await;
    let front = ("front", front.as_str());
    let mut asked = route("/whoami", "whoami");
    asked["posture"] = json!({"kind": "internal", "callers": ["front"]});
    let back = w.deploy(&strict("back", json!([asked])), &wasm).await;
    let back = ("back", back.as_str());

    assert_eq!(
        w.send(
            Method::POST,
            "/front/hop",
            Some(json!({"service": "back", "path": "/whoami", "body": {}})),
        )
        .await,
        200
    );

    let slug = w.tenant.slug.as_str();
    let caller = w.span(&format!("{slug}/front/hop"), Some("route")).await;
    let called = w.span(&format!("{slug}/back/whoami"), Some("route")).await;
    assert_eq!(caller.scope(), w.scope(front, "hop", "route"));
    assert_eq!(called.scope(), w.scope(back, "whoami", "route"));
    assert_eq!(
        called.trace_id, caller.trace_id,
        "one trace across the call"
    );

    let call = w
        .capture
        .span("the call's client span", |span| span.text == "service_call")
        .await
        .expect("exported");
    assert_eq!(call.scope(), w.scope(front, "hop", "route"));
    assert_eq!(called.parent_span_id, call.span_id);

    let log = w
        .capture
        .log("the called guest's line", |record| {
            record.text == "[wasm] asked who calls"
        })
        .await
        .expect("exported");
    assert_eq!(log.scope(), w.scope(back, "whoami", "route"));
    assert_eq!(
        log.trace_id, caller.trace_id,
        "the callee logs in the caller's trace"
    );

    assert_eq!(
        w.send(
            Method::POST,
            "/front/hop",
            Some(json!({"service": "back", "path": "/items", "body": {"title": "kept"}})),
        )
        .await,
        200
    );
    let table = w.span(&format!("{slug}/back/items"), Some("crud")).await;
    assert_eq!(
        table.scope(),
        [
            ("cufflink.kind", "crud"),
            ("cufflink.service", "back"),
            ("cufflink.tenant", slug),
            ("cufflink.version", back.1),
        ],
        "a called table's request carries nothing of its caller's handler"
    );
    let hopped = w
        .capture
        .span("the second call's client span", |span| {
            span.text == "service_call" && span.span_id == table.parent_span_id
        })
        .await
        .expect("exported");
    assert_eq!(hopped.scope(), w.scope(front, "hop", "route"));

    let mut failing = strict("flaky", json!([route("/fail", "fail_always")]));
    failing["custom_routes"][0]["posture"] = json!({"kind": "internal", "callers": ["front"]});
    w.deploy(&failing, &common::guest::module_bytes()).await;
    assert_eq!(
        w.send(
            Method::POST,
            "/front/hop",
            Some(json!({"service": "flaky", "path": "/fail", "body": {}})),
        )
        .await,
        200
    );
    let failed = w.span(&format!("{slug}/flaky/fail"), Some("route")).await;
    assert_eq!(
        failed.attribute("http.response.status_code"),
        Some("503"),
        "a call answered in process records its answer as one over HTTP does"
    );
}

fn junk(n: usize) -> String {
    format!("zz{n}x{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf3_unresolved_paths_export_no_scope_and_name_nothing() {
    const PATHS: usize = 1000;
    let Some(w) = world().await else {
        return;
    };
    let manifest = common::wasm_manifest(NOTES, &[("say", "/say")]);
    w.deploy(&manifest, &common::guest::module_bytes()).await;
    let slug = w.tenant.slug.as_str();

    let paths: Vec<String> = (0..PATHS)
        .map(|n| match n % 5 {
            0 => format!("/svc/{}/{}/{}", junk(n), junk(n), junk(n)),
            1 => format!("/svc/{slug}/{}/_fn/{}", junk(n), junk(n)),
            2 => format!("/svc/{slug}/{NOTES}/{}", junk(n)),
            3 => format!("/svc/{slug}/{NOTES}/_fn/{}", junk(n)),
            _ => format!("/svc/{}/_jobs/{}", junk(n), uuid::Uuid::new_v4()),
        })
        .collect();
    for batch in paths.chunks(50) {
        let answers = futures_util::future::join_all(batch.iter().map(|path| {
            w.platform
                .http
                .get(format!("{}{path}", w.platform.base_url))
                .send()
        }))
        .await;
        for answer in answers {
            let status = answer.expect("answered").status().as_u16();
            assert!(matches!(status, 401 | 404 | 405), "{status}");
        }
    }
    assert_eq!(
        w.send(Method::POST, "/notes/_fn/say", Some(json!({})))
            .await,
        200
    );
    w.said("route").await;
    until(
        "every request's span was exported",
        Duration::from_secs(30),
        async || {
            w.capture
                .spans()
                .iter()
                .filter(|span| span.text == "GET unmatched")
                .count()
                >= PATHS * 4 / 5
        },
    )
    .await
    .expect("in time");

    let (spans, logs) = (w.capture.spans(), w.capture.logs());
    let named_from_a_path: Vec<_> = spans
        .iter()
        .filter(|span| span.text.contains("zz"))
        .map(|span| span.text.as_str())
        .collect();
    assert_eq!(named_from_a_path, Vec::<&str>::new());
    let scoped: BTreeSet<(Option<&str>, Option<&str>)> = spans
        .iter()
        .chain(&logs)
        .filter(|signal| !signal.scope().is_empty())
        .map(|signal| {
            (
                signal.attribute("cufflink.tenant"),
                signal.attribute("cufflink.service"),
            )
        })
        .collect();
    assert_eq!(scoped, BTreeSet::from([(Some(slug), Some(NOTES))]));

    let metrics = w.platform.get_text("/metrics").await.expect("metrics");
    let routes: BTreeSet<&str> = metrics
        .lines()
        .filter(|line| line.starts_with("cufflink_http_request_duration_seconds_count"))
        .filter_map(|line| line.split_once("route=\"")?.1.split_once('"'))
        .map(|(route, _)| route)
        .filter(|route| route.contains("unmatched") || route.contains(slug) || route.contains("zz"))
        .collect();
    assert_eq!(
        routes,
        BTreeSet::from([
            "GET unmatched",
            format!("{slug}/{NOTES}/:id").as_str(),
            format!("{slug}/{NOTES}/say").as_str(),
        ])
    );
    assert!(
        !metrics.contains("zz"),
        "no metric is labelled from a path that resolved nothing"
    );
}

async fn service_info(w: &World) -> Vec<String> {
    w.platform
        .get_text("/metrics")
        .await
        .expect("metrics")
        .lines()
        .filter(|line| {
            line.starts_with("cufflink_service_info{")
                && line.contains(&format!("tenant=\"{}\"", w.tenant.slug))
        })
        .map(str::to_string)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf5_service_info_follows_the_registry() {
    let Some(w) = world().await else {
        return;
    };
    let wasm = base64::engine::general_purpose::STANDARD.encode(common::guest::module_bytes());
    let deploy = async |title: &str, commit: &str| {
        let mut manifest = common::wasm_manifest(NOTES, &[("say", "/say")]);
        manifest["tables"][0]["columns"][1]["nullable"] = json!(title == "nullable");
        let (status, body) = w
            .platform
            .send(
                Method::POST,
                "/api/services/deploy",
                &format!("ApiKey {}", w.tenant.api_key),
                Some(&json!({
                    "manifest": manifest,
                    "wasm_base64": wasm,
                    "api_version": 1,
                    "commit_hash": commit,
                })),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        body
    };
    let series = |version: &Value, commit: &str| {
        vec![format!(
            "cufflink_service_info{{tenant=\"{}\",service=\"{NOTES}\",version=\"v{version}\",commit=\"{commit}\",mode=\"wasm\"}} 1",
            w.tenant.slug
        )]
    };
    assert_eq!(service_info(&w).await, Vec::<String>::new());

    let first = deploy("required", "0a1b2c3").await;
    assert_eq!(service_info(&w).await, series(&first["version"], "0a1b2c3"));

    let second = deploy("nullable", "4d5e6f7").await;
    assert_ne!(second["version"], first["version"]);
    assert_eq!(
        service_info(&w).await,
        series(&second["version"], "4d5e6f7"),
        "the version a redeploy replaced is printed no more"
    );

    let skipped = deploy("nullable", "8a9b0c1").await;
    assert_eq!(skipped["version"], second["version"], "{skipped}");
    assert_eq!(
        service_info(&w).await,
        series(&second["version"], "8a9b0c1"),
        "a skipped deploy from a newer commit moves the commit"
    );

    let (status, body) = w
        .platform
        .send(
            Method::PATCH,
            &format!(
                "/api/services/{}/status",
                second["service_id"].as_str().expect("service id")
            ),
            &format!("ApiKey {}", w.tenant.api_key),
            Some(&json!({"status": "inactive"})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(service_info(&w).await, Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf3_a_platform_that_exports_stops_cleanly() {
    let Some(mut w) = world().await else {
        return;
    };
    w.capture
        .log("a line of the platform's start", |record| {
            record.scope().is_empty()
        })
        .await
        .expect("exported");
    w.platform.sigterm();
    let exited = w.platform.wait_exit(Duration::from_secs(60)).await;
    assert!(
        exited.is_some_and(|status| status.success()),
        "{exited:?}: {}",
        w.platform.output_tail()
    );
}
