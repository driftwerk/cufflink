use cufflink_testkit as common;

use common::guest::SAID;
use common::monitoring::{self, Query, Source};
use common::{infra, ChildProcess, PlatformProcess, Tenant, TestDb};
use cufflink_test_guests::Profile;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const NOTES: &str = "notes";
const FRONT: &str = "front";
const ON_A_WORKER: &str = "say_on_a_worker";
const PUBLIC: &str = r#"{"kind": "public", "reason": "integration test"}"#;
const SHOWN_WITHIN: Duration = Duration::from_secs(120);
const SETTLES_IN: Duration = Duration::from_secs(3);
const PLATFORM: &str = "cufflink-platform";
const RUNNER: &str = "cufflink-ext-runner";
const VARIABLES: [&str; 5] = [
    "CUFFLINK_TEST_OTLP_ENDPOINT",
    "CUFFLINK_TEST_PINNED_OTLP_ENDPOINT",
    "CUFFLINK_TEST_LOKI_URL",
    "CUFFLINK_TEST_TEMPO_URL",
    "CUFFLINK_TEST_PROMETHEUS_URL",
];

struct Stack {
    otlp: String,
    pinned: String,
    loki: String,
    tempo: String,
    prometheus: String,
}

impl Stack {
    fn named() -> Option<Self> {
        match VARIABLES.map(|name| std::env::var(name).ok()) {
            [Some(otlp), Some(pinned), Some(loki), Some(tempo), Some(prometheus)] => Some(Self {
                otlp,
                pinned,
                loki,
                tempo,
                prometheus,
            }),
            _ if std::env::var_os("CUFFLINK_TEST_REQUIRE_OBSERVABILITY").is_some() => {
                panic!("CUFFLINK_TEST_REQUIRE_OBSERVABILITY is set but not each of {VARIABLES:?}")
            }
            _ => {
                eprintln!("skipped: {VARIABLES:?} are not all set (the observability tier)");
                None
            }
        }
    }
}

struct Trace(String);

impl Trace {
    fn new() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }

    fn parent(&self) -> String {
        format!("00-{}-{}-01", self.0, &self.0[..16])
    }
}

struct Deployed {
    tenant: Tenant,
    notes_id: String,
    notes_version: String,
    front_version: String,
    direct: Trace,
    through_front: Trace,
    created: Trace,
}

struct World {
    stack: Stack,
    environment: String,
    platform: PlatformProcess,
    acme: Deployed,
    globex: Deployed,
    _worker: ChildProcess,
    storage_dir: PathBuf,
    _db: TestDb,
}

impl Drop for World {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.storage_dir);
    }
}

fn route(path: &str, handler: &str) -> Value {
    json!({
        "method": "POST",
        "path": path,
        "handler": handler,
        "posture": serde_json::from_str::<Value>(PUBLIC).expect("posture"),
    })
}

fn strict(name: &str, routes: Value) -> Value {
    let mut manifest = common::wasm_manifest(name, &[]);
    manifest["routing"] = json!("strict");
    manifest["custom_routes"] = routes;
    manifest
}

fn notes() -> Value {
    let mut manifest = strict(NOTES, json!([route("/speak", "say")]));
    manifest["tables"][0]["hooks"] = json!({"after_create": ON_A_WORKER});
    manifest
}

async fn ask(platform: &PlatformProcess, tenant: &Tenant, trace: &Trace, path: &str, body: Value) {
    let answer = platform
        .http
        .post(format!("{}/svc/{}{path}", platform.base_url, tenant.slug))
        .header("authorization", format!("ApiKey {}", tenant.api_key))
        .header("traceparent", trace.parent())
        .json(&body)
        .send()
        .await
        .expect("answered");
    let status = answer.status();
    let answer: Value = answer.json().await.unwrap_or_default();
    assert!(status.is_success(), "{path}: {status} {answer}");
    assert_ne!(answer["status"], 404, "{path}: {answer}");
}

async fn deployed(platform: &PlatformProcess, tenant: Tenant, front: &[u8]) -> Deployed {
    let (status, body) = platform
        .deploy_module(&tenant, &notes(), &common::guest::module_bytes())
        .await;
    assert_eq!(status, 200, "{body}");
    let called = strict(FRONT, json!([route("/hop", "hop")]));
    let (status, front) = platform.deploy_module(&tenant, &called, front).await;
    assert_eq!(status, 200, "{front}");

    let (direct, through_front, created) = (Trace::new(), Trace::new(), Trace::new());
    ask(platform, &tenant, &direct, "/notes/speak", json!({})).await;
    let hop = json!({"service": NOTES, "path": "/speak", "body": {}});
    ask(platform, &tenant, &through_front, "/front/hop", hop).await;
    let item = json!({"title": "first"});
    ask(platform, &tenant, &created, "/notes/items", item).await;
    let (status, _) = platform
        .api_get(
            &tenant,
            &format!("/svc/{}/_jobs/{}", tenant.slug, uuid::Uuid::new_v4()),
        )
        .await;
    assert_eq!(status, 404, "a job nobody enqueued");
    let version = |deployed: &Value| format!("v{}", deployed["version"].as_i64().expect("version"));
    Deployed {
        notes_id: body["service_id"].as_str().expect("service id").to_string(),
        notes_version: version(&body),
        front_version: version(&front),
        tenant,
        direct,
        through_front,
        created,
    }
}

async fn world() -> Option<World> {
    let stack = Stack::named()?;
    let front = common::sdk::sdk_guest("internal", Profile::Debug)?;
    let infra = infra().await?;
    let nats_url = infra.require_nats()?;
    let worker = infra::require_binary("cufflink-worker")?;
    let mut db = infra.create_database().await;
    let environment = format!("run-{}", uuid::Uuid::new_v4().simple());
    let resource =
        format!("deployment.environment.name={environment},k8s.pod.name=pod-{environment}");
    let storage_dir = std::env::temp_dir().join(format!("cufflink-observed-{environment}"));
    let storage = storage_dir.to_string_lossy().into_owned();
    let exporting = [
        ("OTEL_EXPORTER_OTLP_ENDPOINT", stack.otlp.as_str()),
        ("OTEL_RESOURCE_ATTRIBUTES", resource.as_str()),
        ("OTEL_BSP_SCHEDULE_DELAY", "200"),
        ("OTEL_BLRP_SCHEDULE_DELAY", "200"),
        ("STORAGE_BACKEND", "local"),
        ("STORAGE_LOCAL_DIR", storage.as_str()),
    ];
    let routed = json!({ NOTES: [ON_A_WORKER] }).to_string();
    let querying = [
        ("LOKI_URL", stack.loki.as_str()),
        ("LOKI_LABEL_MODEL", "scoped"),
        ("WORKER_ROUTES", routed.as_str()),
    ];
    let settings: Vec<_> = exporting.into_iter().chain(querying).collect();
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &settings).await;
    platform.wait_ready().await;
    let running = [
        ("DATABASE_URL", db.url.as_str()),
        ("NATS_URL", nats_url),
        ("RUST_LOG", "info"),
    ];
    let settings: Vec<_> = running.into_iter().chain(exporting).collect();
    let worker = ChildProcess::spawn(&worker, "WORKER_HTTP_PORT", &settings).await;
    worker.wait_ready().await;
    let acme = deployed(&platform, db.mint_tenant().await, &front).await;
    let globex = deployed(&platform, db.mint_tenant().await, &front).await;
    Some(World {
        stack,
        environment,
        platform,
        acme,
        globex,
        _worker: worker,
        storage_dir,
        _db: db,
    })
}

impl Deployed {
    fn slug(&self) -> &str {
        &self.tenant.slug
    }

    fn traces(&self) -> BTreeSet<&str> {
        [&self.direct, &self.through_front, &self.created]
            .map(|trace| trace.0.as_str())
            .into()
    }

    fn stream(&self) -> String {
        format!(
            r#"{{service_namespace="{}", service_name="{NOTES}"}}"#,
            self.slug()
        )
    }

    fn spans(&self) -> String {
        format!(
            r#"{{resource.service.namespace="{}" && resource.service.name="{NOTES}"}}"#,
            self.slug()
        )
    }
}

fn lately() -> [(&'static str, String); 2] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock past 1970")
        .as_secs();
    [
        ("start", (now - 3600).to_string()),
        ("end", (now + 60).to_string()),
    ]
}

impl World {
    async fn get(&self, url: String, query: &[(&str, &str)]) -> (u16, Value) {
        let answer = self
            .platform
            .http
            .get(url)
            .query(query)
            .query(&lately())
            .send()
            .await
            .expect("answered");
        let status = answer.status().as_u16();
        let text = answer.text().await.unwrap_or_default();
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn shown<T>(
        &self,
        what: &str,
        url: String,
        query: &[(&str, &str)],
        found: impl Fn(&Value) -> Option<T>,
    ) -> T {
        let deadline = Instant::now() + SHOWN_WITHIN;
        loop {
            let (status, answer) = self.get(url.clone(), query).await;
            match Some(&answer).filter(|_| status == 200).and_then(&found) {
                Some(found) => return found,
                None if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(500)).await
                }
                None => panic!("{what} was never shown; last answer {status}: {answer}"),
            }
        }
    }

    async fn settled<T>(
        &self,
        what: &str,
        url: String,
        query: &[(&str, &str)],
        found: impl Fn(&Value) -> Option<T>,
    ) -> T {
        self.shown(what, url.clone(), query, &found).await;
        tokio::time::sleep(SETTLES_IN).await;
        self.shown(what, url, query, found).await
    }

    fn loki(&self, path: &str) -> String {
        format!("{}/loki/api/v1/{path}", self.stack.loki)
    }

    fn tempo(&self, path: &str) -> String {
        format!("{}/api/{path}", self.stack.tempo)
    }

    fn prometheus(&self, path: &str) -> String {
        format!("{}/api/v1/{path}", self.stack.prometheus)
    }

    async fn streams(&self, query: &str, traces: &BTreeSet<&str>) -> Vec<Value> {
        self.shown(
            &format!("a line of each of {traces:?} under {query}"),
            self.loki("query_range"),
            &[("query", query), ("limit", "1000")],
            |answer| {
                let streams = answer["data"]["result"].as_array()?;
                let held: BTreeSet<&str> = streams
                    .iter()
                    .filter_map(|stream| stream["stream"]["trace_id"].as_str())
                    .collect();
                (!streams.is_empty() && held.is_superset(traces)).then(|| streams.clone())
            },
        )
        .await
    }

    async fn series(&self, query: &str, enough: usize) -> Vec<Value> {
        self.settled(
            &format!("{enough} series of {query}"),
            self.prometheus("query"),
            &[("query", query)],
            |answer| {
                answer["data"]["result"]
                    .as_array()
                    .filter(|series| series.len() >= enough)
                    .cloned()
            },
        )
        .await
    }
}

fn label<'a>(stream: &'a Value, name: &str) -> Option<&'a str> {
    stream["stream"][name].as_str()
}

fn whole(id: &str) -> String {
    format!("{id:0>32}")
}

fn resources(trace: &Value) -> BTreeSet<BTreeMap<String, String>> {
    trace["batches"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|batch| {
            batch["resource"]["attributes"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|attribute| {
                    Some((
                        attribute["key"].as_str()?.to_string(),
                        attribute["value"]["stringValue"].as_str()?.to_string(),
                    ))
                })
                .filter(|(name, _)| name.starts_with("service.") || name.starts_with("cufflink."))
                .collect()
        })
        .collect()
}

async fn loki_holds_a_tenants_lines_under_its_service(
    w: &World,
    tenant: &Deployed,
    other: &Deployed,
) {
    let said = format!("{} |= {:?}", tenant.stream(), SAID);
    let streams = w.streams(&said, &tenant.traces()).await;
    let lines: BTreeSet<_> = streams
        .iter()
        .map(|stream| {
            (
                label(stream, "trace_id").map(str::to_string),
                label(stream, "cufflink_component"),
                label(stream, "cufflink_kind"),
                label(stream, "cufflink_handler"),
            )
        })
        .collect();
    let line = |trace: &'_ Trace, component, kind, handler| {
        (
            Some(trace.0.clone()),
            Some(component),
            Some(kind),
            Some(handler),
        )
    };
    assert_eq!(
        lines,
        BTreeSet::from([
            line(&tenant.direct, "platform", "route", "say"),
            line(&tenant.through_front, "platform", "route", "say"),
            line(&tenant.created, "worker", "hook", ON_A_WORKER),
        ]),
        "the guest's lines, each in its trace, and none of {}",
        other.slug()
    );
    for stream in &streams {
        assert_eq!(
            (
                label(stream, "service_namespace"),
                label(stream, "service_name"),
                label(stream, "service_version"),
                label(stream, "deployment_environment_name"),
                label(stream, "detected_level"),
            ),
            (
                Some(tenant.slug()),
                Some(NOTES),
                Some(tenant.notes_version.as_str()),
                Some(w.environment.as_str()),
                Some("info"),
            )
        );
        assert_eq!(
            stream["values"][0][1],
            format!("[wasm] {SAID}"),
            "the line is the message alone"
        );
    }

    let (status, labels) = w.get(w.loki("labels"), &[]).await;
    assert_eq!(status, 200, "{labels}");
    assert_eq!(
        labels["data"],
        json!([
            "cufflink_component",
            "deployment_environment_name",
            "service_name",
            "service_namespace"
        ]),
        "only the resource is indexed"
    );
}

async fn what_belongs_to_no_service_stays_the_binarys(w: &World) {
    let own = format!(
        r#"{{service_name="cufflink-platform", deployment_environment_name="{}"}}"#,
        w.environment
    );
    let streams = w.streams(&own, &BTreeSet::new()).await;
    assert!(!streams.is_empty(), "the platform's start is logged");
    for stream in &streams {
        assert_eq!(label(stream, "service_namespace"), None, "{stream}");
        assert_eq!(label(stream, "cufflink_component"), Some("platform"));
    }

    let scoped = format!(
        r#"{{service_namespace=~".+", deployment_environment_name="{}"}}"#,
        w.environment
    );
    let (status, series) = w.get(w.loki("series"), &[("match[]", &scoped)]).await;
    assert_eq!(status, 200, "{series}");
    let services: BTreeSet<_> = series["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|labels| {
            (
                labels["service_namespace"].as_str(),
                labels["service_name"].as_str(),
            )
        })
        .collect();
    let expected: BTreeSet<_> = [&w.acme, &w.globex]
        .into_iter()
        .flat_map(|tenant| [NOTES, FRONT].map(|service| (Some(tenant.slug()), Some(service))))
        .collect();
    assert_eq!(services, expected);
}

async fn tempo_holds_a_tenants_traces_under_its_service(
    w: &World,
    tenant: &Deployed,
    other: &Deployed,
) {
    let named = |service: &'static str, version: &'_ String| {
        [
            ("cufflink.component", "platform"),
            ("service.namespace", tenant.slug()),
            ("service.name", service),
            ("service.version", version.as_str()),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .into_iter()
        .collect::<BTreeMap<_, _>>()
    };
    let trace = w
        .settled(
            "the trace of the call through front",
            w.tempo(&format!("traces/{}", tenant.through_front.0)),
            &[],
            |trace| (resources(trace).len() >= 2).then(|| trace.clone()),
        )
        .await;
    assert_eq!(
        resources(&trace),
        BTreeSet::from([
            named(FRONT, &tenant.front_version),
            named(NOTES, &tenant.notes_version)
        ]),
        "one trace across the call, each span under the service it ran for"
    );

    let wanted = tenant.traces();
    let found = w
        .shown(
            "the tenant's traces by its service",
            w.tempo("search"),
            &[("q", tenant.spans().as_str()), ("limit", "100")],
            |answer| {
                let found: BTreeSet<String> = answer["traces"]
                    .as_array()?
                    .iter()
                    .filter_map(|trace| trace["traceID"].as_str())
                    .map(whole)
                    .collect();
                wanted
                    .iter()
                    .all(|trace| found.contains(*trace))
                    .then_some(found)
            },
        )
        .await;
    assert_eq!(
        found.iter().map(String::as_str).collect::<BTreeSet<_>>(),
        wanted,
        "and none of {}",
        other.slug()
    );
}

async fn prometheus_holds_a_tenants_span_metrics(w: &World, tenant: &Deployed) {
    let slug = tenant.slug();
    let calls = format!(
        r#"traces_spanmetrics_calls_total{{service_namespace="{slug}", span_kind="SPAN_KIND_SERVER"}}"#
    );
    let entered: BTreeSet<_> = w
        .series(&calls, 3)
        .await
        .iter()
        .map(|series| {
            let label = |name: &str| {
                series["metric"][name]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            };
            (
                label("service"),
                label("span_name"),
                label("service_version"),
                label("cufflink_kind"),
            )
        })
        .collect();
    let request = |service: &str, leaf: &str, kind: &str| {
        let version = match service {
            FRONT => &tenant.front_version,
            _ => &tenant.notes_version,
        };
        (
            service.to_string(),
            format!("{slug}/{service}/{leaf}"),
            version.clone(),
            kind.to_string(),
        )
    };
    assert_eq!(
        entered,
        BTreeSet::from([
            request(FRONT, "hop", "route"),
            request(NOTES, "items", "crud"),
            request(NOTES, "speak", "route"),
        ])
    );

    let latency = format!(r#"traces_spanmetrics_latency_bucket{{service_namespace="{slug}"}}"#);
    let wanted = tenant.traces();
    let exemplars = w
        .shown(
            "an exemplar of the tenant's latency",
            w.prometheus("query_exemplars"),
            &[("query", latency.as_str())],
            |answer| {
                let traces: BTreeSet<String> = answer["data"]
                    .as_array()?
                    .iter()
                    .flat_map(|series| series["exemplars"].as_array().into_iter().flatten())
                    .filter_map(|exemplar| exemplar["labels"]["traceID"].as_str())
                    .map(whole)
                    .collect();
                Some(traces).filter(|traces| !traces.is_empty())
            },
        )
        .await;
    assert!(
        exemplars
            .iter()
            .all(|trace| wanted.contains(trace.as_str())),
        "{exemplars:?} are traces of the tenant's requests, {wanted:?}"
    );

    let poll = format!(r#"traces_spanmetrics_calls_total{{span_name="{slug}/_jobs"}}"#);
    let tenant_only: Vec<_> = w
        .series(&poll, 1)
        .await
        .iter()
        .map(|series| {
            (
                series["metric"]["service"].clone(),
                series["metric"]["service_namespace"].clone(),
            )
        })
        .collect();
    assert_eq!(
        tenant_only,
        [(json!("cufflink-platform"), Value::Null)],
        "a span scoped to a tenant alone stays the platform's"
    );
}

async fn the_drilldowns_have_what_they_ask_for(w: &World, tenant: &Deployed) {
    let namespace = format!(r#"{{service_namespace="{}"}}"#, tenant.slug());
    let volume = w
        .shown(
            "the tenant's log volume",
            w.loki("index/volume"),
            &[("query", namespace.as_str())],
            |answer| answer["data"]["result"].as_array().cloned(),
        )
        .await;
    assert_eq!(volume.len(), 1, "{volume:?}");
    assert_eq!(
        volume[0]["metric"],
        json!({"service_namespace": tenant.slug()})
    );

    let (status, patterns) = w
        .get(w.loki("patterns"), &[("query", namespace.as_str())])
        .await;
    assert_eq!(
        (status, &patterns["status"]),
        (200, &json!("success")),
        "{patterns}"
    );

    let rate = format!(
        r#"{{resource.service.namespace="{}"}} | rate()"#,
        tenant.slug()
    );
    let series = w
        .shown(
            "a TraceQL rate of the tenant's spans",
            w.tempo("metrics/query_range"),
            &[("q", rate.as_str()), ("step", "60")],
            |answer| {
                answer["series"]
                    .as_array()
                    .filter(|series| !series.is_empty())
                    .cloned()
            },
        )
        .await;
    assert_eq!(series.len(), 1, "{series:?}");
}

async fn the_logs_api_answers_with_level_and_fields(w: &World, tenant: &Deployed) {
    let path = format!(
        "/api/services/{}/logs?level=INFO&search={}",
        tenant.notes_id,
        SAID.replace(' ', "%20")
    );
    let (status, answer) = w.platform.api_get(&tenant.tenant, &path).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(
        answer["label_model"],
        json!("scoped"),
        "the answer says once which model its entries were read under: {answer}"
    );
    let logged_at: Vec<u64> = answer["logs"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|entry| {
            let timestamp = entry["timestamp"].as_str().unwrap_or_default();
            timestamp
                .parse()
                .unwrap_or_else(|_| panic!("{entry}: a timestamp in nanoseconds"))
        })
        .collect();
    assert!(
        logged_at.is_sorted(),
        "the lines of every trace together, oldest first: {logged_at:?}"
    );
    let entries: BTreeSet<_> = answer["logs"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|entry| {
            json!({"line": entry["line"], "level": entry["level"], "fields": entry["fields"]})
                .to_string()
        })
        .collect();
    let entry = |trace: &Trace, kind: &str, handler: &str| {
        json!({
            "line": format!("[wasm] {SAID}"),
            "level": "info",
            "fields": {"cufflink_handler": handler, "cufflink_kind": kind, "trace_id": trace.0},
        })
        .to_string()
    };
    assert_eq!(
        entries,
        BTreeSet::from([
            entry(&tenant.direct, "route", "say"),
            entry(&tenant.through_front, "route", "say"),
            entry(&tenant.created, "hook", ON_A_WORKER),
        ])
    );

    let path = format!("/api/services/{}/logs?level=error", tenant.notes_id);
    let (status, answer) = w.platform.api_get(&tenant.tenant, &path).await;
    assert_eq!(
        (status, &answer),
        (200, &json!({"label_model": "scoped", "logs": []}))
    );
}

type Attributes<'a> = &'a [(&'a str, &'a str)];

struct Forged<'a> {
    what: &'a str,
    resource: Attributes<'a>,
    own: Attributes<'a>,
    stored_as: &'a str,
}

fn attributes(pairs: Attributes) -> Value {
    pairs
        .iter()
        .map(|(key, value)| json!({"key": key, "value": {"stringValue": value}}))
        .collect()
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock past 1970")
        .as_nanos()
}

fn line_of(resource: Attributes, record: Attributes, body: &str) -> Value {
    json!({"resourceLogs": [{
        "resource": {"attributes": attributes(resource)},
        "scopeLogs": [{"logRecords": [{
            "timeUnixNano": now_ns().to_string(),
            "severityText": "INFO",
            "body": {"stringValue": body},
            "attributes": attributes(record),
        }]}],
    }]})
}

fn span_of(resource: Attributes, span: Attributes, name: &str) -> Value {
    let (id, ended) = (Trace::new().0, now_ns());
    json!({"resourceSpans": [{
        "resource": {"attributes": attributes(resource)},
        "scopeSpans": [{"spans": [{
            "traceId": id,
            "spanId": id[..16],
            "name": name,
            "kind": 2,
            "startTimeUnixNano": (ended - 1_000_000).to_string(),
            "endTimeUnixNano": ended.to_string(),
            "attributes": attributes(span),
        }]}],
    }]})
}

async fn a_runners_collector_pins_whatever_it_is_sent(w: &World, victim: &Deployed) {
    let slug = victim.slug();
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let said =
        |what: &str| format!(r#"{{"service":"{NOTES}","tenant":"{slug}","by":"{what}-{tag}"}}"#);
    let plainly = [("tenant", slug), ("service", NOTES)];
    let renamed = format!("runner-{PLATFORM}");
    let lines = [
        Forged {
            what: "a namespace given twice",
            resource: &[
                ("service.namespace", "elsewhere"),
                ("service.namespace", slug),
                ("service.name", NOTES),
            ],
            own: &[],
            stored_as: NOTES,
        },
        Forged {
            what: "a scope on the record",
            resource: &[("service.name", RUNNER)],
            own: &[("cufflink.tenant", slug), ("cufflink.service", NOTES)],
            stored_as: NOTES,
        },
        Forged {
            what: "the platform's name",
            resource: &[("service.name", PLATFORM)],
            own: &plainly,
            stored_as: &renamed,
        },
        Forged {
            what: "the platform's name as a scope",
            resource: &[("service.name", RUNNER)],
            own: &[
                ("cufflink.service", PLATFORM),
                ("tenant", slug),
                ("service", NOTES),
            ],
            stored_as: &renamed,
        },
        Forged {
            what: "a name given twice",
            resource: &[("service.name", "elsewhere"), ("service.name", PLATFORM)],
            own: &plainly,
            stored_as: "elsewhere",
        },
    ];
    let spans = [
        Forged {
            what: "namespaced",
            resource: &[("service.name", NOTES)],
            own: &[("service.namespace", slug)],
            stored_as: NOTES,
        },
        Forged {
            what: "scoped",
            resource: &[("service.name", RUNNER)],
            own: &[("cufflink.tenant", slug), ("cufflink.service", PLATFORM)],
            stored_as: &renamed,
        },
    ];
    let span_name = |what: &str| format!("{what}-{tag}");
    let requests = lines
        .iter()
        .map(|line| ("logs", line_of(line.resource, line.own, &said(line.what))))
        .chain(spans.iter().map(|span| {
            (
                "traces",
                span_of(span.resource, span.own, &span_name(span.what)),
            )
        }));
    for (signal, request) in requests {
        let answer = w
            .platform
            .http
            .post(format!("{}/v1/{signal}", w.stack.pinned))
            .json(&request)
            .send()
            .await
            .expect("the runner's collector answered");
        assert_eq!(answer.status(), 200, "{request}");
    }

    let anywhere = format!(r#"{{service_name=~".+"}} |= "{tag}""#);
    let stored: BTreeMap<String, (Value, Value)> = w
        .settled(
            "every line the runner's collector was sent",
            w.loki("query_range"),
            &[("query", anywhere.as_str()), ("limit", "1000")],
            |answer| {
                let stored: BTreeMap<_, _> = answer["data"]["result"]
                    .as_array()?
                    .iter()
                    .flat_map(|stream| {
                        let labels = &stream["stream"];
                        let under = (
                            labels["service_namespace"].clone(),
                            labels["service_name"].clone(),
                        );
                        stream["values"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(|value| value[1].as_str())
                            .map(move |line| (line.to_string(), under.clone()))
                    })
                    .collect();
                (stored.len() >= lines.len()).then_some(stored)
            },
        )
        .await;
    let pinned = stored
        .values()
        .next()
        .and_then(|(namespace, _)| namespace.as_str())
        .expect("a line under a tenant");
    assert!(
        ![slug, w.acme.slug(), w.globex.slug()].contains(&pinned),
        "the runner's collector has a tenant of its own, not {pinned}"
    );
    assert_eq!(
        stored,
        lines
            .iter()
            .map(|line| (said(line.what), (json!(pinned), json!(line.stored_as))))
            .collect(),
        "each line under the collector's tenant"
    );

    let legacy = format!(
        r#"{{service_name="{PLATFORM}"}} |= {:?} |= {:?} |= "{tag}""#,
        format!(r#""service":"{NOTES}""#),
        format!(r#""tenant":"{slug}""#),
    );
    for elsewhere in [
        format!(r#"{{service_namespace="{slug}"}} |= "{tag}""#),
        format!(r#"{{service_name="{PLATFORM}"}} |= "{tag}""#),
        format!(r#"{{service_name=~".+", service_namespace=""}} |= "{tag}""#),
        legacy,
    ] {
        let (status, answer) = w
            .get(w.loki("query_range"), &[("query", elsewhere.as_str())])
            .await;
        assert_eq!(
            (status, &answer["data"]["result"]),
            (200, &json!([])),
            "{elsewhere}: {answer}"
        );
    }
    let path = format!("/api/services/{}/logs?search={tag}", victim.notes_id);
    let (status, answer) = w.platform.api_get(&victim.tenant, &path).await;
    assert_eq!(
        (status, &answer),
        (200, &json!({"label_model": "scoped", "logs": []}))
    );

    let counted = format!(r#"traces_spanmetrics_calls_total{{span_name=~".+-{tag}"}}"#);
    let counted: BTreeMap<String, (Value, Value)> = w
        .series(&counted, spans.len())
        .await
        .iter()
        .map(|series| {
            let metric = &series["metric"];
            (
                metric["span_name"].as_str().unwrap_or_default().to_string(),
                (
                    metric["service_namespace"].clone(),
                    metric["service"].clone(),
                ),
            )
        })
        .collect();
    assert_eq!(
        counted,
        spans
            .iter()
            .map(|span| (span_name(span.what), (json!(pinned), json!(span.stored_as))))
            .collect(),
        "each span counted under the collector's tenant, and under no other"
    );
}

fn filled(query: &Query, tenant: &Deployed) -> String {
    [
        ("${__data.fields.service}", NOTES),
        ("$service", NOTES),
        ("$tenant", tenant.slug()),
        ("$__rate_interval", "1m"),
        ("$__range", "1h"),
        ("$__auto", "1m"),
    ]
    .into_iter()
    .fold(query.text.clone(), |text, (variable, value)| {
        text.replace(variable, value)
    })
}

async fn the_dashboards_ask_what_their_datasources_accept(w: &World, tenant: &Deployed) {
    let dashboards = monitoring::dashboards().expect("the dashboards parse");
    let mut listed = BTreeMap::new();
    for query in dashboards.iter().flat_map(|dashboard| dashboard.queries()) {
        let text = filled(&query, tenant);
        let (status, answer) = match (query.source, query.label_values()) {
            (Source::Loki, Some((selector, label))) => {
                let selector = selector.replace("$tenant", tenant.slug());
                let asked = w
                    .get(
                        w.loki(&format!("label/{label}/values")),
                        &[("query", selector.as_str())],
                    )
                    .await;
                listed.insert(query.text.clone(), asked.1["data"].clone());
                asked
            }
            (Source::Loki, None) => {
                w.get(
                    w.loki("query_range"),
                    &[("query", text.as_str()), ("limit", "1")],
                )
                .await
            }
            (Source::Prometheus, _) => {
                w.get(w.prometheus("query"), &[("query", text.as_str())])
                    .await
            }
            (Source::Tempo, _) => {
                w.get(w.tempo("search"), &[("q", text.as_str()), ("limit", "1")])
                    .await
            }
        };
        assert_eq!(status, 200, "{:?} refused {text}: {answer}", query.source);
    }

    let tenants: BTreeSet<&str> =
        listed[r#"label_values({service_namespace=~".+"}, service_namespace)"#]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
    assert!(
        tenants.is_superset(&[w.acme.slug(), w.globex.slug()].into()),
        "{tenants:?}"
    );
    assert_eq!(
        listed[r#"label_values({service_namespace="$tenant"}, service_name)"#],
        json!([FRONT, NOTES])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf10_a_tenants_telemetry_is_found_under_its_service_and_nowhere_else() {
    let Some(w) = world().await else {
        return;
    };
    for (tenant, other) in [(&w.acme, &w.globex), (&w.globex, &w.acme)] {
        loki_holds_a_tenants_lines_under_its_service(&w, tenant, other).await;
        tempo_holds_a_tenants_traces_under_its_service(&w, tenant, other).await;
        prometheus_holds_a_tenants_span_metrics(&w, tenant).await;
        the_drilldowns_have_what_they_ask_for(&w, tenant).await;
        the_logs_api_answers_with_level_and_fields(&w, tenant).await;
    }
    what_belongs_to_no_service_stays_the_binarys(&w).await;
    a_runners_collector_pins_whatever_it_is_sent(&w, &w.acme).await;
    the_dashboards_ask_what_their_datasources_accept(&w, &w.acme).await;
}
