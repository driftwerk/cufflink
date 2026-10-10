use cufflink_testkit as common;

use common::jobs::{Jobs, LEASE, RETENTION};
use common::keycloak::MockKeycloak;
use common::{PlatformProcess, Tenant};
use cufflink_platform::repo::wasm_jobs::{self, Failure, JobStatus, NewJob, WasmJob};
use reqwest::Method;
use serde_json::{json, Value};
use uuid::Uuid;

const UPSTREAM: Failure<'static> = Failure {
    error: "Handler answered 503",
    class: "handler_error",
};

struct World {
    platform: PlatformProcess,
    jobs: Jobs,
}

impl World {
    async fn start() -> Option<Self> {
        Some(Self::start_on(Jobs::new().await?, &[]).await)
    }

    async fn start_on(jobs: Jobs, env: &[(&str, &str)]) -> Self {
        let env: Vec<(&str, &str)> = [("NATS_PUBLISH_TIMEOUT_MS", "300")]
            .into_iter()
            .chain(env.iter().copied())
            .collect();
        let platform = PlatformProcess::start(&jobs.database_url(), None, &env).await;
        platform.wait_ready().await;
        Self { platform, jobs }
    }

    async fn failed(&self, max_attempts: u32) -> WasmJob {
        self.failed_of(&self.jobs.tenant, max_attempts).await
    }

    async fn failed_of(&self, tenant: &Tenant, max_attempts: u32) -> WasmJob {
        let config = self.jobs.config(max_attempts);
        let job = self
            .jobs
            .create(NewJob {
                tenant_id: tenant.id,
                tenant_slug: &tenant.slug,
                ..self.jobs.new_job(&config)
            })
            .await
            .job()
            .clone();
        let fence = self.jobs.claimed(&job, "worker-1", LEASE).await.fence;
        let failed = wasm_jobs::fail(&self.jobs.pool, &fence, UPSTREAM, RETENTION)
            .await
            .expect("fail")
            .expect("failed");
        wasm_jobs::set_dead_lettered(&self.jobs.pool, failed.id)
            .await
            .expect("dead-lettered");
        self.jobs.row(&failed).await
    }

    async fn retry_as(&self, tenant_slug: &str, authorization: &str, job_id: Uuid) -> (u16, Value) {
        self.platform
            .send(
                Method::POST,
                &format!("/svc/{tenant_slug}/_jobs/{job_id}/retry"),
                authorization,
                None,
            )
            .await
    }

    async fn retry_naming(
        &self,
        tenant: &Tenant,
        named: Option<&Tenant>,
        authorization: &str,
        job_id: Uuid,
    ) -> (u16, Value) {
        let request = self
            .platform
            .http
            .post(format!(
                "{}/svc/{}/_jobs/{job_id}/retry",
                self.platform.base_url, tenant.slug
            ))
            .header("authorization", authorization);
        let response = named
            .into_iter()
            .fold(request, |request, named| {
                request.header("x-tenant-slug", &named.slug)
            })
            .send()
            .await
            .expect("retry");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn retry(&self, job_id: Uuid) -> (u16, Value) {
        let tenant = &self.jobs.tenant;
        self.retry_as(&tenant.slug, &format!("ApiKey {}", tenant.api_key), job_id)
            .await
    }
}

#[tokio::test]
async fn a_failed_job_is_given_one_more_attempt() {
    let Some(world) = World::start().await else {
        return;
    };
    let failed = world.failed(3).await;
    assert!(failed.dead_lettered_at.is_some());

    let (status, answered) = world.retry(failed.id).await;
    assert_eq!(status, 200, "{answered}");
    let retried = world.jobs.row(&failed).await;
    assert_eq!(
        answered,
        json!({
            "job_id": failed.id,
            "status": "pending",
            "handler": "handle_run_report",
            "service": "stock-service",
            "attempts": 1,
            "max_attempts": 2,
            "result": null,
            "error": null,
            "created_at": retried.created_at,
            "started_at": retried.started_at,
            "finished_at": null,
        })
    );
    assert_eq!(retried.status, JobStatus::Pending);
    assert_eq!((retried.attempts, retried.max_attempts), (1, 2));
    assert_eq!(
        retried.last_error.as_deref(),
        Some("Handler answered 503"),
        "its next attempt is told what the last failed of"
    );
    assert_eq!(
        (
            &retried.error,
            retried.finished_at,
            retried.dead_lettered_at
        ),
        (&None, None, None)
    );
    assert_eq!(
        retried.published_at, None,
        "no stream took it: the reclaimer publishes it"
    );
    assert!(retried.next_attempt_at <= chrono::Utc::now());

    let again = world.jobs.claimed(&retried, "worker-2", LEASE).await;
    assert_eq!(again.job.attempts, 2);

    let audited: i64 = world
        .jobs
        .pool
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM audit_log
              WHERE action = 'job.retry' AND resource_type = 'job' AND resource_id = $1
                AND tenant_id = $2",
            &[&failed.id.to_string(), &world.jobs.tenant.id],
        )
        .await
        .expect("audit")
        .get(0);
    assert_eq!(audited, 1);
}

#[tokio::test]
async fn only_a_failed_job_is_retried() {
    let Some(world) = World::start().await else {
        return;
    };
    let pending = world.jobs.enqueue(3).await;
    let running = world.jobs.enqueue(3).await;
    world.jobs.claimed(&running, "worker-1", LEASE).await;
    let completed = world.jobs.enqueue(3).await;
    let fence = world
        .jobs
        .claimed(&completed, "worker-1", LEASE)
        .await
        .fence;
    wasm_jobs::complete(&world.jobs.pool, &fence, &json!({"ok": true}), RETENTION)
        .await
        .expect("complete");

    for (job, status) in [
        (&pending, "pending"),
        (&running, "running"),
        (&completed, "completed"),
    ] {
        let before = world.jobs.row(job).await;
        let (code, refused) = world.retry(job.id).await;
        assert_eq!(code, 409, "{refused}");
        assert_eq!(
            refused,
            json!({"error": format!("only a failed job is retried; this one is {status}")})
        );
        assert_eq!(world.jobs.row(job).await, before);
    }

    let (status, missing) = world.retry(Uuid::new_v4()).await;
    assert_eq!(status, 404, "{missing}");
    assert_eq!(missing, json!({"error": "Job not found"}));
}

#[tokio::test]
async fn a_job_is_retried_by_its_own_tenants_deployers_alone() {
    let Some(mut world) = World::start().await else {
        return;
    };
    let failed = world.failed(3).await;
    let other = world.jobs.another_tenant().await;
    let own = world.jobs.tenant.slug.clone();

    let others_key = format!("ApiKey {}", other.api_key);
    for slug in [other.slug.as_str(), own.as_str()] {
        let (status, refused) = world.retry_as(slug, &others_key, failed.id).await;
        assert_eq!(status, 404, "{slug}: {refused}");
        assert_eq!(refused, json!({"error": "Job not found"}));
    }
    let own_key = format!("ApiKey {}", world.jobs.tenant.api_key);
    let (status, refused) = world.retry_as(&other.slug, &own_key, failed.id).await;
    assert_eq!(status, 404, "{refused}");

    let (status, _) = world.retry_as(&own, "ApiKey cfk_nobody", failed.id).await;
    assert_eq!(status, 401);
    let unauthenticated = world
        .platform
        .http
        .post(format!(
            "{}/svc/{own}/_jobs/{}/retry",
            world.platform.base_url, failed.id
        ))
        .send()
        .await
        .expect("request");
    assert_eq!(unauthenticated.status().as_u16(), 401);

    assert_eq!(
        world.jobs.row(&failed).await,
        failed,
        "nobody but its tenant's deployer changed the job"
    );
    let get = world
        .platform
        .send(
            Method::GET,
            &format!("/svc/{own}/_jobs/{}/retry", failed.id),
            &own_key,
            None,
        )
        .await;
    assert_eq!(get.0, 405, "a job is retried with POST: {}", get.1);
}

#[tokio::test]
async fn a_job_is_retried_by_a_deployer_bound_to_its_tenant() {
    let Some(mut jobs) = Jobs::new().await else {
        return;
    };
    let keycloak = MockKeycloak::start().await;
    let realm = format!("orchard-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let preview = jobs.another_tenant().await;
    let elsewhere = jobs.another_tenant().await;
    jobs.pool
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE tenants SET keycloak_url = $1, keycloak_realm = $2 WHERE id = ANY($3)",
            &[&keycloak.url, &realm, &vec![jobs.tenant.id, preview.id]],
        )
        .await
        .expect("the source and its preview share a realm");
    let admin = format!("ApiKey {}", jobs.mint_admin_key().await);
    let world = World::start_on(
        jobs,
        &[
            ("KEYCLOAK_URL", keycloak.url.as_str()),
            ("KEYCLOAK_ISSUER", keycloak.issuer_base.as_str()),
        ],
    )
    .await;
    let bearer = |sub: &str, roles: &[&str]| {
        format!(
            "Bearer {}",
            keycloak.token(
                &realm,
                json!({"sub": sub, "realm_access": {"roles": roles}})
            )
        )
    };
    let retried = |job: &WasmJob| {
        let (pool, id) = (world.jobs.pool.clone(), job.id);
        async move {
            wasm_jobs::get(&pool, id)
                .await
                .expect("lookup")
                .map(|job| job.status)
                == Some(JobStatus::Pending)
        }
    };

    let failed = world.failed_of(&preview, 3).await;
    let (status, refused) = world
        .retry_naming(&preview, None, &bearer("staff-1", &[]), failed.id)
        .await;
    assert_eq!(
        status, 403,
        "a user of the realm deploys nothing: {refused}"
    );
    assert!(!retried(&failed).await);
    let (status, answered) = world
        .retry_naming(
            &preview,
            None,
            &bearer("deployer-1", &["cufflink-deploy"]),
            failed.id,
        )
        .await;
    assert_eq!(status, 200, "{answered}");
    assert!(retried(&failed).await);

    let failed = world.failed_of(&preview, 3).await;
    for named in [&elsewhere, &world.jobs.tenant] {
        let (status, refused) = world
            .retry_naming(&preview, Some(named), &admin, failed.id)
            .await;
        assert_eq!(status, 404, "naming {}: {refused}", named.slug);
        assert_eq!(refused, json!({"error": "Job not found"}));
    }
    assert!(!retried(&failed).await);
    let (status, answered) = world
        .retry_naming(&preview, Some(&preview), &admin, failed.id)
        .await;
    assert_eq!(status, 200, "{answered}");
    assert!(retried(&failed).await);
}
