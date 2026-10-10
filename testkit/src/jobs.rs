use crate::infra::{infra, Tenant, TestDb};
use cufflink_platform::repo::wasm_jobs::{self, Claim, Enqueued, NewJob, WasmJob};
use cufflink_types::JobConfig;
use serde_json::{json, Value};
use std::sync::LazyLock;
use std::time::Duration;
use uuid::Uuid;

pub const LEASE: Duration = Duration::from_secs(60);
pub const NO_LEASE: Duration = Duration::ZERO;
pub const RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

static REQUEST: LazyLock<Value> =
    LazyLock::new(|| json!({"method": "POST", "handler": "handle_run_report", "body": null}));

pub struct Jobs {
    pub pool: deadpool_postgres::Pool,
    pub tenant: Tenant,
    pub service_id: Uuid,
    db: TestDb,
}

impl Jobs {
    pub async fn new() -> Option<Self> {
        let infra = infra().await?;
        let mut db = infra.create_migrated_database().await;
        let tenant = db.mint_tenant().await;
        let pool = db.pool();
        let service =
            crate::infra::active_service(&pool, tenant.id, "stock-service", "stock_service")
                .await
                .expect("service");
        Some(Self {
            pool,
            tenant,
            service_id: service.id,
            db,
        })
    }

    pub fn database_url(&self) -> String {
        self.db.url.clone()
    }

    pub async fn another_tenant(&mut self) -> Tenant {
        self.db.mint_tenant().await
    }

    pub async fn mint_admin_key(&self) -> String {
        self.db.mint_admin_key().await
    }

    pub fn config(&self, max_attempts: u32) -> JobConfig {
        JobConfig {
            timeout_secs: 900,
            max_attempts,
            ..JobConfig::default()
        }
    }

    pub fn new_job<'a>(&'a self, config: &'a JobConfig) -> NewJob<'a> {
        NewJob {
            tenant_id: self.tenant.id,
            tenant_slug: &self.tenant.slug,
            service_id: self.service_id,
            service_slug: "stock-service",
            service_version: 1,
            handler: "handle_run_report",
            request_json: &REQUEST,
            auth_subject: None,
            auth_claims: None,
            max_attempts: i32::try_from(config.max_attempts).expect("attempts"),
            timeout_secs: i32::try_from(config.timeout_secs).expect("timeout"),
            retry_on: &config.retry_on,
            backoff: config.backoff,
            trace_context: None,
            idempotency_key: None,
            schedule_name: None,
            retain_for: RETENTION,
        }
    }

    pub async fn create(&self, job: NewJob<'_>) -> Enqueued {
        wasm_jobs::create(&self.pool, job).await.expect("job row")
    }

    pub async fn enqueue_as(&self, config: &JobConfig) -> WasmJob {
        match self.create(self.new_job(config)).await {
            Enqueued::Created(job) => job,
            Enqueued::Existing(job) => panic!("a job without a key was enqueued before: {job:?}"),
        }
    }

    pub async fn enqueue(&self, max_attempts: u32) -> WasmJob {
        self.enqueue_as(&self.config(max_attempts)).await
    }

    pub async fn claim(&self, job: &WasmJob, owner: &str, lease: Duration) -> Option<Claim> {
        wasm_jobs::claim(&self.pool, job.id, owner, lease)
            .await
            .expect("claim")
    }

    pub async fn claimed(&self, job: &WasmJob, owner: &str, lease: Duration) -> Claim {
        self.claim(job, owner, lease).await.expect("claimed")
    }

    pub async fn row(&self, job: &WasmJob) -> WasmJob {
        wasm_jobs::get(&self.pool, job.id)
            .await
            .expect("lookup")
            .expect("row")
    }

    pub async fn execute(&self, sql: &str, job: &WasmJob) {
        self.pool
            .get()
            .await
            .expect("checkout")
            .execute(sql, &[&job.id])
            .await
            .expect("statement");
    }
}
