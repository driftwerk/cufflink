use cufflink_testkit as common;

use async_nats::jetstream::{self, consumer};
use common::{infra, until, PlatformProcess, Tenant, TestDb};
use cufflink_platform::nats_manager::events;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

const SERVICE: &str = "ledger-service";
const STREAM: &str = "CUFFLINK_SCHEDULES";
const RUNS: &str = "cufflink_schedule_runs_total";

fn column(name: &str, column_type: &str) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": true,
           "auto_generate": false})
}

fn every(name: &str, handler: &str, secs: u32) -> Value {
    json!({"name": name, "handler": handler, "spec": {"kind": "every", "secs": secs}})
}

fn with(schedule: Value, key: &str, value: Value) -> Value {
    let mut schedule = schedule;
    schedule[key] = value;
    schedule
}

fn manifest(version: &str, schedules: Vec<Value>) -> Value {
    json!({
        "name": SERVICE,
        "version": version,
        "mode": "wasm",
        "tables": [{"name": "runs", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            column("schedule", "text"), column("handler", "text"), column("fire_at", "text"),
            column("catch_up", "boolean"), column("attempt", "biginteger"),
            column("started_at", "timestamp"), column("ended_at", "timestamp"),
        ]}],
        "subscriptions": [],
        "publishes": [],
        "schedules": schedules,
    })
}

struct Ledger {
    replicas: Vec<PlatformProcess>,
    environment: Vec<(String, String)>,
    nats_url: String,
    tenant: Tenant,
    wasm: Vec<u8>,
    js: jetstream::Context,
    _modules: tempfile::TempDir,
    db: TestDb,
}

async fn replica(db: &TestDb, nats_url: &str, environment: &[(String, String)]) -> PlatformProcess {
    let environment: Vec<(&str, &str)> = environment
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &environment).await;
    platform.wait_ready().await;
    platform
}

async fn ledger(replicas: usize, extra: &[(&str, &str)]) -> Option<Ledger> {
    let infra = common::infra().await?;
    let nats_url = infra.require_nats()?.to_string();
    let wasm = common::sdk::sdk_guest("schedules", Profile::Debug)?;
    let mut db = infra.create_database().await;
    let modules = tempfile::tempdir().expect("module store");
    let environment: Vec<(String, String)> = [
        ("CONSUMER_RECONCILE_INTERVAL_SECS", "2"),
        ("STORAGE_BACKEND", "local"),
        ("STORAGE_LOCAL_DIR", &modules.path().to_string_lossy()),
    ]
    .iter()
    .chain(extra)
    .map(|(name, value)| (name.to_string(), value.to_string()))
    .collect();
    let mut running = Vec::with_capacity(replicas);
    for _ in 0..replicas {
        running.push(replica(&db, &nats_url, &environment).await);
    }
    Some(Ledger {
        replicas: running,
        tenant: db.mint_tenant().await,
        js: jetstream::new(async_nats::connect(&nats_url).await.expect("nats")),
        environment,
        nats_url,
        wasm,
        _modules: modules,
        db,
    })
}

impl Ledger {
    async fn deploy(&self, manifest: &Value) -> Value {
        let (status, body) = self.replicas[0]
            .deploy_module(&self.tenant, manifest, &self.wasm)
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn runs(&self) -> deadpool_postgres::Object {
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

    async fn platform_db(&self) -> deadpool_postgres::Object {
        self.db.pool().get().await.expect("platform database")
    }

    fn subject(&self, schedule: &str) -> String {
        format!(
            "cufflink.schedule.{}.{SERVICE}.{schedule}",
            self.tenant.slug
        )
    }

    fn durable(&self, deployed: &Value, schedule: &str) -> String {
        let prefix = format!("sched_{}_{SERVICE}_{schedule}_", self.tenant.slug);
        deployed["plan"]["consumers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|op| op["durable"].as_str())
            .find(|durable| durable.starts_with(&prefix))
            .unwrap_or_else(|| panic!("no durable of '{schedule}' in {deployed}"))
            .to_string()
    }

    async fn durable_info(&self, name: &str) -> Option<consumer::Info> {
        self.js
            .get_stream(STREAM)
            .await
            .expect("stream")
            .consumer_info(name)
            .await
            .ok()
    }

    async fn counted(&self, schedule: &str, result: &str) -> f64 {
        let mut counted = 0.0;
        for replica in &self.replicas {
            counted += replica
                .metric(
                    RUNS,
                    &[
                        ("tenant", self.tenant.slug.as_str()),
                        ("schedule", schedule),
                        ("result", result),
                    ],
                )
                .await
                .unwrap_or_default();
        }
        counted
    }

    async fn until_loops(&self, loops: f64) {
        common::until_loops(&self.replicas.iter().collect::<Vec<_>>(), loops)
            .await
            .expect("in time");
    }
}

async fn count(client: &deadpool_postgres::Object, query: &str) -> i64 {
    client
        .query_one(query, &[])
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .get(0)
}

async fn ran(client: &deadpool_postgres::Object, schedule: &str) -> Vec<(String, bool)> {
    client
        .query(
            "SELECT fire_at, catch_up FROM ledger_service.runs WHERE schedule = $1 \
             ORDER BY started_at",
            &[&schedule],
        )
        .await
        .expect("runs")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn unix_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

fn rfc3339(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .expect("instant")
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[tokio::test]
async fn the_streams_of_the_platforms_work_remember_what_they_took_for_ten_minutes() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    events::setup_schedules_stream(&js).await.expect("fires");
    events::setup_long_jobs_stream(&js).await.expect("jobs");
    for (stream, subjects) in [
        (STREAM, "cufflink.schedule.>"),
        (events::LONG_JOBS_STREAM, "cufflink.jobs.>"),
    ] {
        let config = js
            .get_stream(stream)
            .await
            .expect("stream")
            .get_info()
            .await
            .expect("info")
            .config;
        assert_eq!(config.subjects, [subjects], "{stream}");
        assert_eq!(
            (config.retention, config.max_age, config.duplicate_window),
            (
                jetstream::stream::RetentionPolicy::WorkQueue,
                Duration::from_secs(24 * 60 * 60),
                Duration::from_secs(600)
            ),
            "{stream}"
        );
    }
}

#[tokio::test]
async fn a_schedule_is_deployed_run_and_removed() {
    let Some(ledger) = ledger(1, &[]).await else {
        return;
    };
    let deployed = ledger
        .deploy(&manifest("0.1.0", vec![every("tick", "quick", 10)]))
        .await;
    let durable = ledger.durable(&deployed, "tick");
    let created = deployed["plan"]["consumers"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|op| op["durable"] == durable.as_str())
        .expect("op");
    assert_eq!(
        (&created["op"], &created["stream"], &created["handler"]),
        (&json!("create"), &json!(STREAM), &json!("quick")),
        "{deployed}"
    );

    let info = ledger.durable_info(&durable).await.expect("durable");
    assert_eq!(info.config.filter_subject, ledger.subject("tick"));
    assert_eq!(
        (
            info.config.max_ack_pending,
            info.config.max_deliver,
            info.config.ack_policy,
            info.config.deliver_policy,
        ),
        (
            1,
            3,
            consumer::AckPolicy::Explicit,
            consumer::DeliverPolicy::All
        )
    );
    assert_eq!(
        info.config
            .metadata
            .get("cufflink.kind")
            .map(String::as_str),
        Some("schedule")
    );

    let platform_db = ledger.platform_db().await;
    let introduced_at = unix_secs();
    let timetable = "SELECT count(*) FROM schedule_state WHERE name = 'tick' \
                     AND service_id = (SELECT id FROM services WHERE name = 'ledger-service' \
                                       AND tenant_id = (SELECT id FROM tenants WHERE slug = '{slug}'))"
        .replace("{slug}", &ledger.tenant.slug);
    assert_eq!(count(&platform_db, &timetable).await, 1);
    ledger.until_loops(1.0).await;

    let runs = ledger.runs().await;
    until(
        "the schedule's first run",
        Duration::from_secs(40),
        || async {
            count(
                &runs,
                "SELECT count(*) FROM ledger_service.runs WHERE ended_at IS NOT NULL",
            )
            .await
                >= 1
        },
    )
    .await
    .expect("in time");
    let row = runs
        .query_one(
            "SELECT schedule, handler, fire_at, catch_up, attempt FROM ledger_service.runs \
             ORDER BY started_at LIMIT 1",
            &[],
        )
        .await
        .expect("run");
    let (schedule, handler, fire_at, catch_up, attempt): (String, String, String, bool, i64) =
        (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
    assert_eq!(
        (schedule.as_str(), handler.as_str(), catch_up, attempt),
        ("tick", "quick", false, 1)
    );
    let fired = chrono::DateTime::parse_from_rfc3339(&fire_at)
        .expect("fire_at")
        .timestamp();
    assert_eq!(fired % 10, 0, "{fire_at} is a multiple of the interval");
    assert!(
        fired > introduced_at - 10,
        "{fire_at}: a schedule fires for no instant before it was introduced"
    );
    assert!(ledger.counted("tick", "ok").await >= 1.0);

    let (status, listed) = ledger.replicas[0]
        .send(
            Method::GET,
            &format!(
                "/api/services/{}/consumers",
                deployed["service_id"].as_str().expect("service id")
            ),
            &format!("ApiKey {}", ledger.tenant.api_key),
            None,
        )
        .await;
    assert_eq!(status, 200, "{listed}");
    let consumer = &listed["consumers"][0];
    assert_eq!(
        (
            &consumer["durable"],
            &consumer["stream"],
            &consumer["handler"],
            &consumer["running_here"],
            &consumer["server"]["max_ack_pending"],
        ),
        (
            &json!(durable),
            &json!(STREAM),
            &json!("quick"),
            &json!(true),
            &json!(1)
        ),
        "{listed}"
    );

    let removed = ledger.deploy(&manifest("0.2.0", vec![])).await;
    assert!(
        removed["plan"]["consumers"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|op| op["op"] == "delete" && op["durable"] == durable.as_str()),
        "{removed}"
    );
    assert!(ledger.durable_info(&durable).await.is_none());
    assert_eq!(count(&platform_db, &timetable).await, 0);
    ledger.until_loops(0.0).await;
}

#[tokio::test]
async fn schedule_single_runner_across_replicas() {
    let Some(ledger) = ledger(2, &[]).await else {
        return;
    };
    ledger
        .deploy(&manifest("0.1.0", vec![every("settle", "slow", 10)]))
        .await;
    ledger.until_loops(1.0).await;

    let runs = ledger.runs().await;
    until(
        "three runs of fifteen seconds",
        Duration::from_secs(150),
        || async {
            count(
                &runs,
                "SELECT count(*) FROM ledger_service.runs WHERE ended_at IS NOT NULL",
            )
            .await
                >= 3
        },
    )
    .await
    .expect("in time");
    until("a fire passed over", Duration::from_secs(40), || async {
        ledger.counted("settle", "coalesced").await >= 1.0
    })
    .await
    .expect("in time");

    let at_once = count(
        &runs,
        "SELECT count(*) FROM ledger_service.runs a JOIN ledger_service.runs b ON a.id < b.id \
         WHERE a.started_at < coalesce(b.ended_at, 'infinity') \
           AND b.started_at < coalesce(a.ended_at, 'infinity')",
    )
    .await;
    assert_eq!(at_once, 0, "two runs at once");
    let again = count(
        &runs,
        "SELECT count(*) FROM (SELECT fire_at FROM ledger_service.runs GROUP BY fire_at \
         HAVING count(*) > 1) twice",
    )
    .await;
    assert_eq!(again, 0, "a fire run twice");
    let ended = count(
        &runs,
        "SELECT count(*) FROM ledger_service.runs WHERE ended_at IS NOT NULL",
    )
    .await;
    assert!(
        ledger.counted("settle", "ok").await >= ended as f64,
        "the replicas ran what ended, between them"
    );
    let published = count(
        &runs,
        "SELECT count(*) FROM ledger_service.runs WHERE catch_up OR attempt <> 1",
    )
    .await;
    assert_eq!(published, 0, "every fire on time and delivered once");
}

fn quiet_interval(now: i64) -> (u32, i64) {
    [3600, 3000, 2100]
        .into_iter()
        .map(|secs| (secs, now.rem_euclid(i64::from(secs))))
        .find(|(secs, since)| *since >= 30 && i64::from(*secs) - since >= 600)
        .map(|(secs, since)| (secs, now - since))
        .expect("one of three intervals is quiet at any instant")
}

#[tokio::test]
async fn schedule_catch_up_after_outage() {
    let Some(mut ledger) = ledger(1, &[("SCHEDULE_ON_TIME_GRACE_SECS", "5")]).await else {
        return;
    };
    let (secs, last) = quiet_interval(unix_secs());
    ledger
        .deploy(&manifest(
            "0.1.0",
            vec![
                every("newest", "quick", secs),
                with(every("none", "quick", secs), "catch_up", json!("none")),
                with(every("every", "quick", secs), "catch_up", json!("all")),
            ],
        ))
        .await;
    ledger.until_loops(3.0).await;

    ledger.replicas.clear();
    let platform_db = ledger.platform_db().await;
    let set_back = platform_db
        .execute(
            "UPDATE schedule_state SET last_fired_at = to_timestamp($1::float8) \
             WHERE service_id = (SELECT s.id FROM services s JOIN tenants t ON t.id = s.tenant_id \
                                 WHERE s.name = $2 AND t.slug = $3)",
            &[
                &((last - 3 * i64::from(secs)) as f64),
                &SERVICE,
                &ledger.tenant.slug,
            ],
        )
        .await
        .expect("set back");
    assert_eq!(set_back, 3);
    let restarted = replica(&ledger.db, &ledger.nats_url, &ledger.environment).await;
    ledger.replicas.push(restarted);

    let runs = ledger.runs().await;
    until(
        "what was missed is caught up",
        Duration::from_secs(90),
        || async { ran(&runs, "every").await.len() >= 3 && !ran(&runs, "newest").await.is_empty() },
    )
    .await
    .expect("in time");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let missed: Vec<(String, bool)> = [2, 1, 0]
        .into_iter()
        .map(|ago| (rfc3339(last - ago * i64::from(secs)), true))
        .collect();
    assert_eq!(ran(&runs, "every").await, missed, "each, the oldest first");
    assert_eq!(ran(&runs, "newest").await, missed[2..]);
    assert_eq!(ran(&runs, "none").await, []);
    assert_eq!(
        ledger.replicas[0]
            .metric(
                "cufflink_schedule_fires_published_total",
                &[
                    ("tenant", ledger.tenant.slug.as_str()),
                    ("schedule", "every"),
                    ("catch_up", "true")
                ],
            )
            .await,
        Some(3.0)
    );
    let read_to: Vec<i64> = platform_db
        .query(
            "SELECT extract(epoch FROM last_fired_at)::bigint FROM schedule_state \
             WHERE service_id = (SELECT s.id FROM services s JOIN tenants t ON t.id = s.tenant_id \
                                 WHERE s.name = $1 AND t.slug = $2)",
            &[&SERVICE, &ledger.tenant.slug],
        )
        .await
        .expect("timetables")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(read_to, [last; 3], "what was passed over is not due again");
}

#[tokio::test]
async fn schedule_job_idempotent() {
    let Some(ledger) = ledger(2, &[]).await else {
        return;
    };
    let job = json!({"timeout_secs": 60, "max_attempts": 3});
    let deployed = ledger
        .deploy(&manifest(
            "0.1.0",
            vec![
                with(
                    with(every("post", "quick", 10), "job", job.clone()),
                    "overlap",
                    json!("allow"),
                ),
                with(every("post_one", "quick", 10), "job", job),
            ],
        ))
        .await;
    ledger.until_loops(2.0).await;

    let platform_db = ledger.platform_db().await;
    let jobs = |schedule: &str, select: &str| {
        format!(
            "SELECT {select} FROM wasm_jobs WHERE schedule_name = '{schedule}' \
             AND tenant_slug = '{}'",
            ledger.tenant.slug
        )
    };
    until("two fires enqueued", Duration::from_secs(60), || async {
        count(&platform_db, &jobs("post", "count(*)")).await >= 2
    })
    .await
    .expect("in time");

    let key: String = platform_db
        .query_one(&jobs("post", "min(idempotency_key)"), &[])
        .await
        .expect("key")
        .get(0);
    let fired: i64 = key
        .strip_prefix("sched:post:")
        .and_then(|secs| secs.parse().ok())
        .unwrap_or_else(|| panic!("'{key}' is not the key of a fire of 'post'"));
    assert_eq!(fired % 10, 0, "{key}");

    let again = ledger
        .js
        .publish(
            ledger.subject("post"),
            json!({"schedule": "post", "fire_at": rfc3339(fired), "catch_up": false})
                .to_string()
                .into(),
        )
        .await
        .expect("publish")
        .await
        .expect("stored");
    let durable = ledger.durable(&deployed, "post");
    until(
        "the fire handled again",
        Duration::from_secs(30),
        || async {
            ledger
                .durable_info(&durable)
                .await
                .is_some_and(|info| info.ack_floor.stream_sequence >= again.sequence)
        },
    )
    .await
    .expect("in time");
    assert_eq!(
        count(
            &platform_db,
            &format!("{} AND idempotency_key = '{key}'", jobs("post", "count(*)"))
        )
        .await,
        1,
        "a fire handled twice enqueues one job"
    );
    assert_eq!(
        count(&platform_db, &jobs("post", "count(*)")).await,
        count(
            &platform_db,
            &jobs("post", "count(DISTINCT idempotency_key)")
        )
        .await,
        "a fire every replica published enqueues one job"
    );

    until("a fire passed over", Duration::from_secs(40), || async {
        ledger.counted("post_one", "coalesced").await >= 1.0
    })
    .await
    .expect("in time");
    assert_eq!(
        count(&platform_db, &jobs("post_one", "count(*)")).await,
        1,
        "no job is enqueued while the last one has yet to end"
    );
    let job = platform_db
        .query_one(
            &jobs("post_one", "handler, status, request_json->>'method'"),
            &[],
        )
        .await
        .expect("job");
    assert_eq!(
        (
            job.get::<_, String>(0),
            job.get::<_, String>(1),
            job.get::<_, String>(2)
        ),
        ("quick".into(), "pending".into(), "SCHEDULE".into())
    );
}

#[tokio::test]
async fn a_fire_its_handler_keeps_failing_is_dead_lettered() {
    let Some(ledger) = ledger(1, &[]).await else {
        return;
    };
    ledger
        .deploy(&manifest(
            "0.1.0",
            vec![with(
                every("close", "failing", 10),
                "overlap",
                json!("allow"),
            )],
        ))
        .await;
    ledger.until_loops(1.0).await;

    let key = format!("ApiKey {}", ledger.tenant.api_key);
    let queue = format!("/api/tenants/{}/dlq", ledger.tenant.slug);
    let letters = || async {
        let (status, body) = ledger.replicas[0]
            .send(Method::GET, &queue, &key, None)
            .await;
        assert_eq!(status, 200, "{body}");
        body["letters"].as_array().cloned().unwrap_or_default()
    };
    until("the fire given up", Duration::from_secs(120), || async {
        !letters().await.is_empty()
    })
    .await
    .expect("in time");
    let letter = letters().await.pop().expect("the first letter");
    assert_eq!(
        (
            &letter["service"],
            &letter["kind"],
            &letter["handler"],
            &letter["deliveries"],
            &letter["stream"],
            &letter["payload"]["schedule"],
        ),
        (
            &json!(SERVICE),
            &json!("sched"),
            &json!("failing"),
            &json!(3),
            &json!(STREAM),
            &json!("close")
        ),
        "{letter}"
    );
    assert_eq!(letter["original_subject"], ledger.subject("close"));
    let fire_at = letter["payload"]["fire_at"].as_str().expect("fire_at");
    let runs = ledger.runs().await;
    let attempts: Vec<i64> = runs
        .query(
            "SELECT attempt FROM ledger_service.runs WHERE fire_at = $1 ORDER BY started_at",
            &[&fire_at],
        )
        .await
        .expect("attempts")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(attempts, [1, 2, 3]);
    assert!(ledger.counted("close", "error").await >= 3.0);

    let (status, body) = ledger.replicas[0]
        .send(
            Method::POST,
            &format!("{queue}/{}/replay", letter["sequence"]),
            &key,
            None,
        )
        .await;
    assert_eq!(
        status, 409,
        "the stream remembers the fire for ten minutes: {body}"
    );
}

#[tokio::test]
async fn a_failing_schedule_that_fires_faster_than_its_retries_is_never_dead_lettered() {
    let Some(ledger) = ledger(1, &[]).await else {
        return;
    };
    ledger
        .deploy(&manifest("0.1.0", vec![every("close", "failing", 10)]))
        .await;
    ledger.until_loops(1.0).await;
    let attempts = || async {
        ledger
            .runs()
            .await
            .query(
                "SELECT max(attempt) FROM ledger_service.runs GROUP BY fire_at",
                &[],
            )
            .await
            .expect("attempts")
            .iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>()
    };
    until("three fires run", Duration::from_secs(90), || async {
        attempts().await.len() >= 3
    })
    .await
    .expect("in time");
    let attempts = attempts().await;
    assert!(
        attempts.iter().all(|attempt| *attempt < 3),
        "a fire behind a newer one is not run a third time: {attempts:?}"
    );
    assert!(ledger.counted("close", "error").await >= 3.0);
    let (status, body) = ledger.replicas[0]
        .send(
            Method::GET,
            &format!("/api/tenants/{}/dlq", ledger.tenant.slug),
            &format!("ApiKey {}", ledger.tenant.api_key),
            None,
        )
        .await;
    assert_eq!((status, &body["letters"]), (200, &json!([])), "{body}");
}
