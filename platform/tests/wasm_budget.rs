use cufflink_testkit as common;

use common::PlatformProcess;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SERVICE: &str = "loan-service";

struct Answer {
    status: u16,
    retry_after: Option<String>,
    body: Value,
    took: Duration,
}

async fn call(platform: &PlatformProcess, url: &str, after: Duration) -> Answer {
    tokio::time::sleep(after).await;
    answered(platform.http.post(url).json(&json!({}))).await
}

async fn answered(request: reqwest::RequestBuilder) -> Answer {
    let started = Instant::now();
    let response = request.send().await.expect("fn request");
    let status = response.status().as_u16();
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    Answer {
        status,
        retry_after,
        body: response.json().await.unwrap_or(Value::Null),
        took: started.elapsed(),
    }
}

#[tokio::test]
async fn a_route_runs_max_concurrent_calls_at_once_and_refuses_the_rest_unrun() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("route_concurrency"))),
        "{version}"
    );

    let mut manifest = common::wasm_manifest(SERVICE, &[("sleep2", "/sleep2"), ("say", "/say")]);
    manifest["custom_routes"][0]["max_concurrent"] = json!(2);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let limited = platform.fn_url(&tenant, SERVICE, "sleep2");
    let free = platform.fn_url(&tenant, SERVICE, "say");
    let (first, second, third, other) = tokio::join!(
        call(&platform, &limited, Duration::ZERO),
        call(&platform, &limited, Duration::ZERO),
        call(&platform, &limited, Duration::from_millis(400)),
        call(&platform, &free, Duration::from_millis(400)),
    );

    assert_eq!((first.status, second.status), (200, 200));
    assert_eq!(
        other.status, 200,
        "another route has no limit: {}",
        other.body
    );
    assert_eq!(third.status, 503, "{}", third.body);
    assert_eq!(third.retry_after.as_deref(), Some("2"));
    assert_eq!(
        third.body,
        json!({"error": "guest capacity exhausted; the request was not run"})
    );
    assert!(
        third.took >= Duration::from_secs(1),
        "the third call waits WASM_ADMISSION_WAIT_MS for a slot: {:?}",
        third.took
    );
    assert!(
        third.took + Duration::from_millis(400) < first.took.min(second.took),
        "and is refused while the handlers holding the slots still run: {:?} {:?} {:?}",
        third.took,
        first.took,
        second.took
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_wasm_admission_refused_total",
                &[
                    ("tenant", tenant.slug.as_str()),
                    ("service", SERVICE),
                    ("reason", "route_concurrency"),
                ],
            )
            .await,
        Some(1.0)
    );

    let again = call(&platform, &limited, Duration::ZERO).await;
    assert_eq!(again.status, 200, "slots are freed: {}", again.body);
}

const MIB: u64 = 1024 * 1024;

async fn budgeted(mode: &str) -> Option<(PlatformProcess, common::TestDb)> {
    let infra = common::infra().await?;
    let db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("WASM_MEMORY_BUDGET_MODE", mode),
            ("WASM_MEMORY_BUDGET_MB", "128"),
        ],
    )
    .await;
    platform.wait_ready().await;
    Some((platform, db))
}

#[tokio::test]
async fn a_burst_under_enforce_is_answered_503_with_retry_after_and_the_process_survives() {
    let Some((platform, mut db)) = budgeted("enforce").await else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let manifest = common::wasm_manifest("catalog-service", &[("hold_48", "/hold_48")]);
    let (status, body) = platform.deploy(&catalog, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let url = platform.fn_url(&catalog, "catalog-service", "hold_48");
    let health = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        platform.status("/health").await
    };
    let (answers, healthy) = tokio::join!(
        futures_util::future::join_all((0..12).map(|_| call(&platform, &url, Duration::ZERO))),
        health,
    );
    assert_eq!(healthy, Some(200), "health answers during the burst");
    let ok = answers.iter().filter(|a| a.status == 200).count();
    assert!(ok >= 1, "some calls run");
    for answer in answers.iter().filter(|a| a.status != 200) {
        assert_eq!(answer.status, 503, "never a 500: {}", answer.body);
        assert_eq!(answer.retry_after.as_deref(), Some("2"));
        assert_eq!(
            answer.body,
            json!({"error": "guest capacity exhausted; the request was not run"})
        );
    }
    assert!(
        ok < 12,
        "a 128 MiB budget cannot run twelve 48 MiB guests at once"
    );
    assert_eq!(
        platform.status("/health").await,
        Some(200),
        "the process that took the burst still answers"
    );
    assert_eq!(
        platform
            .metric("cufflink_wasm_memory_budget_bytes", &[],)
            .await,
        Some((128 * MIB) as f64)
    );
}

#[tokio::test]
async fn under_warn_the_same_burst_runs_and_counts_what_would_have_waited() {
    let Some((platform, mut db)) = budgeted("warn").await else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let manifest = common::wasm_manifest("catalog-service", &[("hold_48", "/hold_48")]);
    let (status, body) = platform.deploy(&catalog, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let url = platform.fn_url(&catalog, "catalog-service", "hold_48");
    let answers =
        futures_util::future::join_all((0..6).map(|_| call(&platform, &url, Duration::ZERO))).await;
    assert!(
        answers.iter().all(|answer| answer.status == 200),
        "warn refuses nothing"
    );
    let counted = platform
        .metric(
            "cufflink_wasm_admission_refused_total",
            &[("mode", "warn"), ("tenant", catalog.slug.as_str())],
        )
        .await
        .unwrap_or_default();
    assert!(counted >= 1.0, "would-be refusals are counted: {counted}");
}

const SMALL: &[(&str, &str)] = &[
    ("WASM_MEMORY_BUDGET_MODE", "enforce"),
    ("WASM_MEMORY_BUDGET_MB", "64"),
    ("WASM_MEMORY_ADMIT_PERCENT", "100"),
    ("WASM_MEMORY_COLD_ESTIMATE_MB", "8"),
    ("WASM_THREAD_STACK_KB", "1024"),
    ("WASM_ADMISSION_WAIT_MS", "100"),
];

async fn small(extra: &[(&str, &str)]) -> Option<(PlatformProcess, common::TestDb)> {
    let infra = common::infra().await?;
    let db = infra.create_database().await;
    let env: Vec<(&str, &str)> = SMALL.iter().chain(extra).copied().collect();
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
    platform.wait_ready().await;
    Some((platform, db))
}

async fn deployed(platform: &PlatformProcess, tenant: &common::Tenant, manifest: &Value) {
    let (status, body) = platform.deploy(tenant, manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");
}

async fn tenant_db(db: &common::TestDb, tenant: &common::Tenant) -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect(&db.tenant_url(&tenant.slug), tokio_postgres::NoTls)
            .await
            .expect("the tenant database");
    tokio::spawn(connection);
    client
}

async fn hooked(client: &tokio_postgres::Client, schema: &str) -> i64 {
    client
        .query_one(
            &format!("SELECT count(*) FROM {schema}.items WHERE title = 'hooked'"),
            &[],
        )
        .await
        .expect("items")
        .get(0)
}

const SOON: Duration = Duration::from_secs(10);

async fn eventually(what: &str, reached: impl AsyncFnMut() -> bool) {
    common::until(what, SOON, reached).await.expect("in time");
}

async fn gauge(platform: &PlatformProcess, name: &str, labels: &[(&str, &str)]) -> f64 {
    platform.metric(name, labels).await.unwrap_or_default()
}

struct Held {
    gate: common::Gate,
    call: tokio::task::JoinHandle<Answer>,
}

async fn held(
    platform: &PlatformProcess,
    db: &common::TestDb,
    tenant: &common::Tenant,
    service: &str,
    handler: &str,
) -> Held {
    let gate = common::Gate::closed(&db.tenant_url(&tenant.slug)).await;
    let call = tokio::spawn(answered(
        platform
            .http
            .post(platform.fn_url(tenant, service, handler))
            .json(&json!({})),
    ));
    eventually("held at the gate", async || gate.waiting().await == 1).await;
    Held { gate, call }
}

impl Held {
    async fn release(self) {
        self.gate.open().await;
        let answer = self.call.await.expect("the holder");
        assert_eq!(answer.status, 200, "{}", answer.body);
    }
}

#[tokio::test]
async fn a_db_query_past_the_response_cap_fails_the_call_and_the_guest_answers() {
    let Some((platform, mut db)) = small(&[("WASM_MAX_HOST_RESPONSE_MB", "1")]).await else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let manifest = common::wasm_manifest(
        "catalog-service",
        &[("query_past_a_mib", "/query_past_a_mib")],
    );
    deployed(&platform, &catalog, &manifest).await;
    let url = platform.fn_url(&catalog, "catalog-service", "query_past_a_mib");
    let answer = call(&platform, &url, Duration::ZERO).await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(
        answer.body,
        json!({"rc": -1}),
        "1.1 MB of rows past a 1 MiB cap"
    );
    assert_eq!(
        gauge(
            &platform,
            "cufflink_wasm_memory_service_bytes",
            &[("tenant", catalog.slug.as_str())]
        )
        .await,
        0.0,
        "the rows' charge went back"
    );
}

#[tokio::test]
async fn a_light_service_keeps_answering_while_another_holds_its_share() {
    let Some((platform, mut db)) = small(&[("WASM_MEMORY_SERVICE_SHARE_PERCENT", "40")]).await
    else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let loans = db.mint_tenant().await;
    let heavy = common::wasm_manifest("catalog-service", &[("hold_gated_24", "/hold")]);
    deployed(&platform, &catalog, &heavy).await;
    deployed(
        &platform,
        &loans,
        &common::wasm_manifest("loan-service", &[("echo", "/echo")]),
    )
    .await;
    let holding = held(&platform, &db, &catalog, "catalog-service", "hold_gated_24").await;

    let again = call(
        &platform,
        &platform.fn_url(&catalog, "catalog-service", "hold_gated_24"),
        Duration::ZERO,
    )
    .await;
    assert_eq!(again.status, 503, "past its 25.6 MiB share: {}", again.body);
    assert_eq!(
        gauge(
            &platform,
            "cufflink_wasm_admission_refused_total",
            &[
                ("tenant", catalog.slug.as_str()),
                ("reason", "service_share")
            ]
        )
        .await,
        1.0
    );
    let light = call(
        &platform,
        &platform.fn_url(&loans, "loan-service", "echo"),
        Duration::ZERO,
    )
    .await;
    assert_eq!(light.status, 200, "{}", light.body);
    holding.release().await;
}

#[tokio::test]
async fn an_after_hook_started_with_the_budget_full_runs_once_room_frees() {
    let Some((platform, mut db)) = small(&[("WASM_MEMORY_SERVICE_SHARE_PERCENT", "100")]).await
    else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let loans = db.mint_tenant().await;
    let mut hooked_manifest = common::wasm_manifest("catalog-service", &[]);
    hooked_manifest["tables"][0]["hooks"] = json!({"after_create": "insert_hooked"});
    deployed(&platform, &catalog, &hooked_manifest).await;
    deployed(
        &platform,
        &loans,
        &common::wasm_manifest("loan-service", &[("hold_gated_56", "/hold")]),
    )
    .await;
    let rows = tenant_db(&db, &catalog).await;
    let holding = held(&platform, &db, &loans, "loan-service", "hold_gated_56").await;

    let (status, body) = platform
        .send(
            reqwest::Method::POST,
            &format!("/svc/{}/catalog-service/items", catalog.slug),
            &format!("ApiKey {}", catalog.api_key),
            Some(&json!({"title": "written"})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    eventually("the hook waits for memory", async || {
        gauge(
            &platform,
            "cufflink_wasm_admission_waiters",
            &[("class", "committed")],
        )
        .await
            >= 1.0
    })
    .await;
    assert_eq!(hooked(&rows, "catalog_service").await, 0);

    holding.release().await;
    eventually("the hook ran", async || {
        hooked(&rows, "catalog_service").await == 1
    })
    .await;
}

#[tokio::test]
async fn an_after_hook_of_a_service_at_its_share_is_kept_and_runs_once_room_frees() {
    let Some((platform, mut db)) = small(&[("WASM_MEMORY_SERVICE_SHARE_PERCENT", "40")]).await
    else {
        return;
    };
    let share: usize = 64 * 1024 * 1024 / 100 * 40;
    let catalog = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("catalog-service", &[("hold_gated_24", "/hold")]);
    manifest["tables"][0]["hooks"] = json!({"after_create": "insert_hooked"});
    deployed(&platform, &catalog, &manifest).await;
    let rows = tenant_db(&db, &catalog).await;
    let holding = held(&platform, &db, &catalog, "catalog-service", "hold_gated_24").await;
    let charged = gauge(
        &platform,
        "cufflink_wasm_memory_service_bytes",
        &[("tenant", catalog.slug.as_str())],
    )
    .await as usize;
    let past_what_is_left = share.saturating_sub(charged) + 64 * 1024;

    let (status, body) = platform
        .send(
            reqwest::Method::POST,
            &format!("/svc/{}/catalog-service/items", catalog.slug),
            &format!("ApiKey {}", catalog.api_key),
            Some(&json!({"title": "x".repeat(past_what_is_left)})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    eventually("the hook waits for its share", async || {
        gauge(
            &platform,
            "cufflink_wasm_admission_waiters",
            &[("class", "committed")],
        )
        .await
            >= 1.0
    })
    .await;
    assert_eq!(hooked(&rows, "catalog_service").await, 0);

    holding.gate.open().await;
    let holder = holding.call.await.expect("the holder");
    assert_eq!(
        holder.status, 503,
        "the kept text took the service past its share, so its answer was refused: {}",
        holder.body
    );
    eventually("the hook ran", async || {
        hooked(&rows, "catalog_service").await == 1
    })
    .await;
}

#[tokio::test]
async fn a_bulk_writes_after_hooks_hold_at_most_half_of_their_services_slots() {
    let Some((platform, mut db)) = small(&[
        ("WASM_MAX_CONCURRENT_PER_SERVICE", "4"),
        ("WASM_MEMORY_COLD_ESTIMATE_MB", "2"),
    ])
    .await
    else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("catalog-service", &[("echo", "/echo")]);
    manifest["tables"][0]["hooks"] = json!({"after_create": "gate_then_insert"});
    deployed(&platform, &catalog, &manifest).await;
    let rows = tenant_db(&db, &catalog).await;
    let gate = common::Gate::closed(&db.tenant_url(&catalog.slug)).await;

    let operations: Vec<Value> = (0..6)
        .map(|n| json!({"action": "create", "data": {"title": format!("row {n}")}}))
        .collect();
    let (status, body) = platform
        .send(
            reqwest::Method::POST,
            &format!("/svc/{}/catalog-service/items/batch", catalog.slug),
            &format!("ApiKey {}", catalog.api_key),
            Some(&json!({"operations": operations})),
        )
        .await;
    assert!(matches!(status, 200 | 201), "{status}: {body}");
    eventually("two hooks take their slots", async || {
        gate.waiting().await == 2
    })
    .await;
    for _ in 0..5 {
        assert_eq!(gate.waiting().await, 2, "half of four slots");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let route = call(
        &platform,
        &platform.fn_url(&catalog, "catalog-service", "echo"),
        Duration::ZERO,
    )
    .await;
    assert_eq!(
        route.status, 200,
        "a route keeps the other half: {}",
        route.body
    );

    gate.open().await;
    eventually("every hook ran", async || {
        hooked(&rows, "catalog_service").await == 6
    })
    .await;
}

#[tokio::test]
async fn an_on_migrate_started_with_the_budget_full_runs_once_room_frees() {
    let Some((platform, mut db)) = small(&[("WASM_MEMORY_SERVICE_SHARE_PERCENT", "100")]).await
    else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let loans = db.mint_tenant().await;
    deployed(
        &platform,
        &loans,
        &common::wasm_manifest("loan-service", &[("hold_gated_56", "/hold")]),
    )
    .await;
    let holding = held(&platform, &db, &loans, "loan-service", "hold_gated_56").await;

    let mut migrating = common::wasm_manifest("catalog-service", &[]);
    migrating["on_migrate"] = json!("insert_hooked");
    let deploy = tokio::spawn({
        let (http, base, key) = (
            platform.http.clone(),
            platform.base_url.clone(),
            catalog.api_key.clone(),
        );
        async move {
            use base64::Engine;
            http.post(format!("{base}/api/services/deploy"))
                .header("authorization", format!("ApiKey {key}"))
                .json(&json!({
                    "manifest": migrating,
                    "wasm_base64": base64::engine::general_purpose::STANDARD
                        .encode(common::guest::module_bytes()),
                    "api_version": 1,
                }))
                .send()
                .await
                .expect("deploy")
                .status()
                .as_u16()
        }
    });
    eventually("on_migrate waits for memory", async || {
        gauge(
            &platform,
            "cufflink_wasm_admission_waiters",
            &[("class", "committed")],
        )
        .await
            >= 1.0
    })
    .await;
    assert!(!deploy.is_finished(), "the deploy waits for its migration");

    holding.release().await;
    assert_eq!(deploy.await.expect("deploy"), 200);
    assert_eq!(
        hooked(&tenant_db(&db, &catalog).await, "catalog_service").await,
        1
    );
}

#[tokio::test]
async fn a_chunked_body_holds_its_limit_until_read_then_its_length() {
    let Some((platform, mut db)) = small(&[]).await else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("catalog-service", &[("gate", "/gate")]);
    manifest["custom_routes"][0]["body_limit_bytes"] = json!(16 * MIB);
    deployed(&platform, &catalog, &manifest).await;
    let gate = common::Gate::closed(&db.tenant_url(&catalog.slug)).await;
    let held = async || {
        gauge(
            &platform,
            "cufflink_wasm_memory_service_bytes",
            &[("tenant", catalog.slug.as_str())],
        )
        .await
    };

    let mut upload = tokio::net::TcpStream::connect(("127.0.0.1", platform.port))
        .await
        .expect("connect");
    let chunk = |data: &[u8]| [format!("{:x}\r\n", data.len()).as_bytes(), data, b"\r\n"].concat();
    let head = format!(
        "POST /svc/{}/catalog-service/_fn/gate HTTP/1.1\r\nhost: 127.0.0.1\r\n\
         content-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n",
        catalog.slug
    );
    upload
        .write_all(&[head.as_bytes(), &chunk(br#"{"pad":""#)].concat())
        .await
        .expect("the head and a first chunk");
    eventually("the body limit is reserved", async || {
        held().await >= (16 * MIB) as f64
    })
    .await;

    upload
        .write_all(&[chunk(b"\"}"), b"0\r\n\r\n".to_vec()].concat())
        .await
        .expect("the last chunk");
    eventually("the guest runs", async || gate.waiting().await == 1).await;
    let running = held().await;
    assert!(
        running < (16 * MIB) as f64,
        "the reservation shrank to the body read: {running}"
    );
    gate.open().await;
    let mut answer = vec![0; 64];
    let read = upload.read(&mut answer).await.expect("the answer");
    assert!(
        answer[..read].starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&answer[..read])
    );
    eventually("the charge went back", async || held().await == 0.0).await;
}

#[tokio::test]
async fn a_handler_calling_itself_with_its_slots_full_is_answered_503_and_never_hangs() {
    let wasm = match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => {
            common::skip(reason);
            return;
        }
        None => cufflink_test_guests::wasm("service-call", cufflink_test_guests::Profile::Debug)
            .expect("built guest"),
    };
    let Some((platform, mut db)) = budgeted("enforce").await else {
        return;
    };
    let catalog = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("catalog-service", &[("relay", "/relay")]);
    manifest["custom_routes"][0]["max_concurrent"] = json!(1);
    let (status, body) = platform.deploy_module(&catalog, &manifest, &wasm).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let started = Instant::now();
    let response = platform
        .http
        .post(platform.fn_url(&catalog, "catalog-service", "relay"))
        .json(&json!({
            "service": "catalog-service",
            "method": "POST",
            "path": "/_fn/relay",
            "body": {},
        }))
        .send()
        .await
        .expect("relay request");
    assert_eq!(response.status().as_u16(), 200);
    let answer: Value = response.json().await.expect("json body");
    assert_eq!(answer["status"], json!(503), "{answer}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the nested call did not wait on its caller's slot: {:?}",
        started.elapsed()
    );
}
