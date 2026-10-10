use cufflink_testkit as common;

use common::{wasm_manifest, PlatformProcess, Tenant};
use cufflink_platform::repo::wasm_jobs::{self, JobStatus, WasmJob};
use serde_json::{json, Value};
use uuid::Uuid;

struct World {
    platform: PlatformProcess,
    tenant: Tenant,
    pool: deadpool_postgres::Pool,
    _db: common::TestDb,
}

impl World {
    async fn start() -> Option<Self> {
        Self::start_with(&[]).await
    }

    async fn start_with(env: &[(&str, &str)]) -> Option<Self> {
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let env: Vec<(&str, &str)> = [("NATS_PUBLISH_TIMEOUT_MS", "300")]
            .into_iter()
            .chain(env.iter().copied())
            .collect();
        let platform = PlatformProcess::start(&db.url, None, &env).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let mut manifest = wasm_manifest("job-svc", &[("echo", "/echo")]);
        manifest["custom_routes"][0]["job"] = json!({"timeout_secs": 900, "max_attempts": 2});
        let (status, body) = platform.deploy(&tenant, &manifest).await;
        assert_eq!(status, 200, "deploy: {body}");
        Some(Self {
            pool: db.pool(),
            platform,
            tenant,
            _db: db,
        })
    }

    async fn call(&self, headers: &[(&str, &str)], body: &Value) -> (u16, Value) {
        let request = headers.iter().fold(
            self.platform
                .http
                .post(self.platform.fn_url(&self.tenant, "job-svc", "echo"))
                .json(body),
            |request, (name, value)| request.header(*name, *value),
        );
        let response = request.send().await.expect("job route");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn job(&self, accepted: &Value) -> WasmJob {
        let id = accepted["job_id"]
            .as_str()
            .and_then(|id| Uuid::parse_str(id).ok())
            .expect("job id");
        wasm_jobs::get(&self.pool, id)
            .await
            .expect("lookup")
            .expect("job row")
    }
}

#[tokio::test]
async fn job_publish_failure_still_accepted() {
    let Some(world) = World::start().await else {
        return;
    };
    let (status, accepted) = world.call(&[], &json!({"id": "item-1"})).await;
    assert_eq!(status, 202, "{accepted}");
    assert_eq!(accepted["status"], "pending");
    assert_eq!(
        accepted.as_object().map(|keys| keys.len()),
        Some(2),
        "{accepted}"
    );

    let job = world.job(&accepted).await;
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(
        job.published_at, None,
        "the reclaimer is to publish it: no stream took it"
    );
    assert_eq!((&job.error, job.attempts), (&None, 0));
    assert_eq!((job.timeout_secs, job.max_attempts), (900, 2));
    assert_eq!(job.retry_on, vec![]);
}

#[tokio::test]
async fn job_request_json_has_no_credentials() {
    let Some(world) = World::start().await else {
        return;
    };
    let api_key = format!("ApiKey {}", world.tenant.api_key);
    let (status, accepted) = world
        .call(
            &[
                ("authorization", api_key.as_str()),
                ("cookie", "session=1"),
                ("proxy-authorization", "Basic dTpw"),
                ("x-api-key", "cfp_live_1"),
                ("x-sweep-secret", "kept: not a credential of the platform's"),
            ],
            &json!({"id": "item-1"}),
        )
        .await;
    assert_eq!(status, 202, "{accepted}");

    let job = world.job(&accepted).await;
    let headers = job.request_json["headers"]
        .as_object()
        .expect("the request's headers");
    for credential in [
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-api-key",
    ] {
        assert!(
            !headers.contains_key(credential),
            "{credential}: {headers:?}"
        );
    }
    assert_eq!(
        headers["x-sweep-secret"],
        "kept: not a credential of the platform's"
    );
    assert_eq!(headers["content-type"], "application/json");
    let stored = job.request_json.to_string();
    for secret in [
        world.tenant.api_key.as_str(),
        "session=1",
        "dTpw",
        "cfp_live_1",
    ] {
        assert!(!stored.contains(secret), "{secret} in {stored}");
    }

    assert_eq!(job.request_json["body"], json!({"id": "item-1"}));
    assert_eq!(job.request_json["handler"], "echo");
    assert!(
        job.request_json["auth"]["sub"].is_string(),
        "the handler runs as its caller: {}",
        job.request_json["auth"]
    );
    assert_eq!(job.auth_claims, Some(job.request_json["auth"].clone()));
    assert_eq!(
        job.auth_subject.as_deref(),
        job.request_json["auth"]["sub"].as_str()
    );
}

#[tokio::test]
async fn a_tenant_with_a_full_backlog_is_answered_429() {
    let Some(world) = World::start().await else {
        return;
    };
    let (status, accepted) = world.call(&[], &json!({})).await;
    assert_eq!(status, 202, "{accepted}");
    let first = world.job(&accepted).await;
    world
        .pool
        .get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO wasm_jobs (tenant_id, tenant_slug, service_id, service_slug,
                                    service_version, handler, status, request_json,
                                    timeout_secs, expires_at)
             SELECT tenant_id, tenant_slug, service_id, service_slug, service_version, handler,
                    CASE WHEN n % 2 = 0 THEN 'pending' ELSE 'running' END, '{}'::jsonb, 60,
                    now() + interval '1 day'
               FROM wasm_jobs, generate_series(1, 999) AS n
              WHERE id = $1",
            &[&first.id],
        )
        .await
        .expect("a backlog of 1000");

    let (status, refused) = world.call(&[], &json!({})).await;
    assert_eq!(status, 429, "{refused}");
    assert_eq!(
        refused,
        json!({"error": "job backlog limit reached (1000 pending or running)"})
    );

    world
        .pool
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE wasm_jobs SET status = 'completed' WHERE id = $1",
            &[&first.id],
        )
        .await
        .expect("one ends");
    let (status, accepted) = world.call(&[], &json!({})).await;
    assert_eq!(status, 202, "{accepted}");
}

#[tokio::test]
async fn job_trace_context_parented() {
    use opentelemetry::trace::TraceContextExt;
    let nowhere = format!("http://127.0.0.1:{}", common::free_port());
    let Some(world) = World::start_with(&[("OTEL_EXPORTER_OTLP_ENDPOINT", nowhere.as_str())]).await
    else {
        return;
    };
    let (status, accepted) = world
        .call(
            &[(
                "traceparent",
                "00-97c9e5c13f4a4d0fa53a2273c4be3aad-00f067aa0ba902b7-01",
            )],
            &json!({}),
        )
        .await;
    assert_eq!(status, 202, "{accepted}");
    let job = world.job(&accepted).await;
    let traceparent = job
        .trace_context
        .as_ref()
        .and_then(|kept| kept["traceparent"].as_str())
        .expect("the trace it was enqueued under");
    assert!(
        traceparent.starts_with("00-97c9e5c13f4a4d0fa53a2273c4be3aad-"),
        "the caller's trace: {traceparent}"
    );
    assert!(
        !traceparent.contains("00f067aa0ba902b7"),
        "under the platform's span of the request, not the caller's: {traceparent}"
    );

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let parent = cufflink_platform::router::jobs::enqueued_under(&job).expect("a parent");
    let parent = parent.span().span_context().clone();
    assert_eq!(
        parent.trace_id().to_string(),
        "97c9e5c13f4a4d0fa53a2273c4be3aad"
    );
    assert!(traceparent.contains(&parent.span_id().to_string()));
}
