use async_nats::jetstream::consumer::pull::Config as PullConfig;
use cufflink_platform::nats_manager::events::{
    self, ASYNC_JOBS_STREAM, BACKUP_STREAM, LONG_JOBS_STREAM,
};
use cufflink_platform::repo::wasm_jobs::{self, JobStatus, NewJob};
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_testkit::infra::ChildProcess;
use cufflink_testkit::otlp::{Capture, Signal};
use cufflink_testkit::{guest, infra};
use cufflink_types::{BackoffSpec, JobConfig, RetryOn};
use futures_util::StreamExt;
use serde_json::json;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

static JETSTREAM: Mutex<()> = Mutex::const_new(());

async fn exclusive_jetstream(js: &async_nats::jetstream::Context) -> MutexGuard<'static, ()> {
    let turn = JETSTREAM.lock().await;
    for name in [LONG_JOBS_STREAM, ASYNC_JOBS_STREAM, BACKUP_STREAM] {
        if let Ok(stream) = js.get_stream(name).await {
            stream.purge().await.expect("purge stream");
        }
    }
    turn
}

async fn remember_for(js: &async_nats::jetstream::Context, window: Duration) {
    let stream = js.get_stream(LONG_JOBS_STREAM).await.expect("stream");
    js.update_stream(async_nats::jetstream::stream::Config {
        duplicate_window: window,
        ..stream.cached_info().config.clone()
    })
    .await
    .expect("duplicate window");
}

async fn only_message(
    js: &async_nats::jetstream::Context,
    stream: &str,
    consumer: &str,
) -> async_nats::jetstream::Message {
    let consumer = js
        .get_stream(stream)
        .await
        .expect("stream")
        .get_consumer::<PullConfig>(consumer)
        .await
        .expect("durable consumer");
    let mut batch = consumer
        .fetch()
        .max_messages(2)
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .expect("fetch");
    let message = batch
        .next()
        .await
        .expect("a message is waiting")
        .expect("message");
    assert!(
        batch.next().await.is_none(),
        "exactly one message is left on {consumer:?}"
    );
    message
}

async fn record_deployment(
    pool: &deadpool_postgres::Pool,
    service: &cufflink_platform::repo::services::Service,
    module: &[u8],
) {
    let wasm_hash = cufflink_platform::wasm_runtime::sha256_hex(module);
    cufflink_platform::repo::deployments::activate_version(
        pool,
        cufflink_platform::repo::deployments::ActivateVersion {
            service_id: service.id,
            version: service.current_version,
            manifest_hash: &service.name,
            manifest: &serde_json::from_value(
                json!({"name": service.name, "mode": "wasm", "tables": [], "publishes": []}),
            )
            .expect("a manifest"),
            wasm_hash: Some(&wasm_hash),
            commit_hash: None,
            schema_json: None,
            schema_hash: "",
            native: None,
        },
    )
    .await
    .expect("deployment row");
}

struct HookService {
    tenant: infra::Tenant,
    service: cufflink_platform::repo::services::Service,
    tenant_db: deadpool_postgres::Pool,
    storage_dir: PathBuf,
}

impl HookService {
    async fn deploy(infra: &infra::Infra, db: &mut infra::TestDb) -> Self {
        let tenant = db.mint_tenant().await;
        let service =
            cufflink_testkit::active_service(&db.pool(), tenant.id, "hook-svc", "hook_svc")
                .await
                .expect("service");
        let schema = service.schema();
        let tenant_db = cufflink_db::build_pool(
            &infra::with_dbname(
                &infra.database_url,
                &TenantPoolManager::managed_db_name(&tenant.slug),
            ),
            1,
        )
        .expect("tenant pool");
        tenant_db
            .get()
            .await
            .expect("tenant checkout")
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; CREATE TABLE {schema}.items (id serial PRIMARY KEY, title text)"
            ))
            .await
            .expect("service table");
        let module = guest::module_bytes();
        record_deployment(&db.pool(), &service, &module).await;
        let storage_dir = std::env::temp_dir().join(format!("cufflink-worker-{}", Uuid::new_v4()));
        cufflink_storage::local_dir(&storage_dir, "cufflink")
            .expect("local storage")
            .upload(
                &format!("wasm/{}/v{}.wasm", service.id, service.current_version),
                module,
                "application/wasm",
            )
            .await
            .expect("upload module");
        Self {
            tenant,
            service,
            tenant_db,
            storage_dir,
        }
    }

    fn job(
        &self,
        handler: &str,
        request_json: &str,
    ) -> cufflink_platform::worker_dispatch::WasmJob {
        cufflink_platform::worker_dispatch::WasmJob {
            service_id: self.service.id,
            version: self.service.current_version,
            handler: handler.into(),
            request_json: request_json.into(),
            tenant_id: self.tenant.id,
            tenant_slug: self.tenant.slug.clone(),
            service_schema: self.service.schema(),
            service_name: Some("hook-svc".into()),
            publishes: None,
        }
    }

    async fn publish(
        &self,
        js: &async_nats::jetstream::Context,
        job: cufflink_platform::worker_dispatch::WasmJob,
    ) {
        events::async_jobs_stream(js)
            .await
            .expect("async-jobs stream");
        cufflink_platform::worker_dispatch::publish_async_job(js, "hook-svc", job)
            .await
            .expect("publish after-hook");
    }

    async fn titles(&self) -> Vec<String> {
        self.tenant_db
            .get()
            .await
            .expect("tenant checkout")
            .query(
                &format!(
                    "SELECT title FROM {}.items ORDER BY id",
                    self.service.schema()
                ),
                &[],
            )
            .await
            .expect("titles")
            .iter()
            .map(|row| row.get(0))
            .collect()
    }

    async fn until_titles(&self, count: usize) -> Vec<String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let titles = self.titles().await;
            if titles.len() >= count {
                return titles;
            }
            assert!(
                Instant::now() < deadline,
                "only {titles:?} of {count} hooks ran"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn dead_letter(
    js: &async_nats::jetstream::Context,
    filter: String,
    within: Duration,
) -> Option<async_nats::jetstream::Message> {
    js.get_stream(cufflink_platform::nats_manager::dlq::DLQ_STREAM)
        .await
        .expect("dead-letter stream")
        .create_consumer(PullConfig {
            filter_subject: filter,
            ..Default::default()
        })
        .await
        .expect("reader")
        .batch()
        .max_messages(1)
        .expires(within)
        .messages()
        .await
        .expect("fetch")
        .next()
        .await
        .map(|letter| letter.expect("message"))
}

struct Worker {
    process: ChildProcess,
    storage_dir: PathBuf,
}

impl Worker {
    async fn start(db: &infra::TestDb, nats_url: &str, extra: &[(&str, &str)]) -> Self {
        let storage_dir = std::env::temp_dir().join(format!("cufflink-worker-{}", Uuid::new_v4()));
        Self::start_in(storage_dir, db, nats_url, extra).await
    }

    async fn start_in(
        storage_dir: PathBuf,
        db: &infra::TestDb,
        nats_url: &str,
        extra: &[(&str, &str)],
    ) -> Self {
        let dir = storage_dir.to_string_lossy().into_owned();
        let env: Vec<(&str, &str)> = [
            ("DATABASE_URL", db.url.as_str()),
            ("NATS_URL", nats_url),
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", dir.as_str()),
            ("RUST_LOG", "info"),
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .collect();
        let process = ChildProcess::spawn(
            &infra::workspace_binary("cufflink-worker"),
            "WORKER_HTTP_PORT",
            &env,
        )
        .await;
        Self {
            process,
            storage_dir,
        }
    }

    fn storage(&self) -> std::sync::Arc<dyn cufflink_storage::ObjectStorage> {
        cufflink_storage::local_dir(&self.storage_dir, "cufflink").expect("local storage")
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.storage_dir);
    }
}

#[tokio::test]
async fn a_secret_given_as_a_variable_and_a_file_stops_the_worker() {
    let mut worker = ChildProcess::launch_unchecked(
        &infra::workspace_binary("cufflink-worker"),
        "WORKER_HTTP_PORT",
        &[
            (
                "DATABASE_URL",
                "postgres://nobody:s3cret@127.0.0.1:1/nowhere",
            ),
            ("DATABASE_URL_FILE", "/run/secrets/absent"),
        ],
    );
    let status = worker
        .wait_exit(Duration::from_secs(30))
        .await
        .expect("the worker exits");
    assert!(!status.success());
    let said = worker.output_tail();
    assert!(
        said.contains("DATABASE_URL and DATABASE_URL_FILE are both set; set one"),
        "{said}"
    );
    assert!(!said.contains("s3cret"), "no secret is echoed: {said}");
}

#[tokio::test]
async fn a_key_ring_that_does_not_parse_stops_the_worker() {
    let mut worker = ChildProcess::launch_unchecked(
        &infra::workspace_binary("cufflink-worker"),
        "WORKER_HTTP_PORT",
        &[
            ("DATABASE_URL", "postgres://nobody@127.0.0.1:1/nowhere"),
            ("CUFFLINK_SECRETS_KEK", "not base64 !"),
            ("CUFFLINK_SECRETS_KEK_ID", "k1"),
        ],
    );
    let status = worker
        .wait_exit(Duration::from_secs(30))
        .await
        .expect("the worker exits");
    assert!(!status.success());
    let said = worker.output_tail();
    assert!(said.contains("CUFFLINK_SECRETS_KEK"), "{said}");
    assert!(
        !said.contains("not base64 !"),
        "the key is never echoed: {said}"
    );
}

#[tokio::test]
async fn worker_ready_and_metrics() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;
    events::setup_long_jobs_stream(&js)
        .await
        .expect("long-jobs stream");

    let worker = Worker::start(&db, nats_url, &[]).await;
    let (status, body) = worker.process.get_json("/health").await.expect("/health");
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ok");
    assert!(body["version"].as_str().is_some_and(|v| !v.is_empty()));

    let ready = worker.process.wait_ready().await;
    assert_eq!(ready["consumers"]["up"], 3, "{ready}");
    assert_eq!(ready["nats"], "connected");

    let metrics = worker.process.get_text("/metrics").await.expect("/metrics");
    for kind in ["sync", "async", "long"] {
        assert_eq!(
            infra::metric_value(&metrics, "cufflink_worker_inflight", &[("kind", kind)]),
            Some(0.0),
            "{metrics}"
        );
    }
    assert!(metrics.contains("cufflink_db_pool_max"), "{metrics}");
}

#[tokio::test]
async fn a_job_is_answered_on_an_inbox_alone() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;
    let hooks = HookService::deploy(infra, &mut db).await;
    let worker = Worker::start_in(hooks.storage_dir.clone(), &db, nats_url, &[]).await;
    worker.process.wait_ready().await;

    let client = async_nats::connect(nats_url).await.expect("nats");
    let subject = "cufflink.wasm.hook-svc.insert_ordered";
    let job = |title: &str| serde_json::to_vec(&hooks.job("insert_ordered", title)).expect("a job");
    let mut elsewhere = client
        .subscribe(format!("dw.{}.results", hooks.tenant.slug))
        .await
        .expect("subscribe");
    client
        .publish_with_reply(
            subject,
            format!("dw.{}.results", hooks.tenant.slug),
            job("4").into(),
        )
        .await
        .expect("publish");
    let answered = client
        .request(subject, job("8").into())
        .await
        .expect("an answer on the inbox");
    let answered: serde_json::Value = serde_json::from_slice(&answered.payload).expect("json");
    assert_eq!(answered["ok"], true, "{answered}");

    let deadline = Instant::now() + Duration::from_secs(10);
    while worker
        .process
        .metric("cufflink_worker_reply_refused_total", &[])
        .await
        != Some(1.0)
    {
        assert!(Instant::now() < deadline, "the refusal is counted");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(hooks.titles().await, ["8"], "the refused job ran nothing");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), elsewhere.next())
            .await
            .is_err(),
        "nothing is published on the subject it named"
    );
}

#[tokio::test]
async fn worker_waits_for_the_platform_schema() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let db = infra.create_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;
    events::setup_long_jobs_stream(&js)
        .await
        .expect("long-jobs stream");

    let worker = Worker::start(&db, nats_url, &[]).await;
    let (status, body) = worker.process.get_json("/ready").await.expect("/ready");
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["state"], "starting");
    assert_eq!(body["catalog"], "loading");
    assert_eq!(body["consumers"]["up"], 0, "no jobs before the catalog");

    cufflink_platform::repo::migrations::run(&cufflink_platform::repo::migrations::Target {
        config: cufflink_db::parse_config(&db.url).expect("url"),
        settings: cufflink_db::PoolSettings::from_vars(1, |_| None).expect("defaults"),
    })
    .await
    .expect("migrations");
    let ready = worker.process.wait_ready().await;
    assert_eq!(ready["catalog"], "loaded");
    assert_eq!(ready["consumers"]["up"], 3, "{ready}");
}

struct JobService {
    pool: deadpool_postgres::Pool,
    js: async_nats::jetstream::Context,
    nats_url: &'static str,
    slug: String,
    tenant_id: Uuid,
    service: cufflink_platform::repo::services::Service,
    worker: Worker,
    _turn: MutexGuard<'static, ()>,
    db: infra::TestDb,
}

#[derive(Clone)]
struct Job {
    handler: &'static str,
    config: JobConfig,
}

fn job(handler: &'static str, timeout_secs: u32, max_attempts: u32) -> Job {
    Job {
        handler,
        config: JobConfig {
            timeout_secs,
            max_attempts,
            ..JobConfig::default()
        },
    }
}

impl Job {
    fn retrying(self, failure: RetryOn) -> Self {
        Self {
            config: JobConfig {
                retry_on: vec![failure],
                backoff: Some(BackoffSpec {
                    base_secs: 1,
                    max_secs: 1,
                }),
                ..self.config
            },
            ..self
        }
    }
}

async fn consumer_of_an_earlier_release(js: &async_nats::jetstream::Context) {
    js.get_stream(LONG_JOBS_STREAM)
        .await
        .expect("stream")
        .create_consumer(PullConfig {
            durable_name: Some("cufflink-long-jobs".into()),
            filter_subject: "cufflink.jobs.>".into(),
            ack_wait: Duration::from_secs(3600),
            ..Default::default()
        })
        .await
        .expect("consumer");
}

impl JobService {
    async fn start(worker_env: &[(&str, &str)]) -> Option<Self> {
        Self::start_beside(worker_env, false).await
    }

    async fn start_beside(worker_env: &[(&str, &str)], upgraded: bool) -> Option<Self> {
        let infra = infra::infra().await?;
        let nats_url: &'static str = infra.require_nats()?;
        let mut db = infra.create_migrated_database().await;
        let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
        let turn = exclusive_jetstream(&js).await;
        events::setup_long_jobs_stream(&js)
            .await
            .expect("long-jobs stream");
        remember_for(&js, Duration::from_secs(600)).await;
        if upgraded {
            consumer_of_an_earlier_release(&js).await;
        }

        let pool = db.pool();
        let tenant = db.mint_tenant().await;
        let service = cufflink_testkit::active_service(&pool, tenant.id, "spin-svc", "spin_svc")
            .await
            .expect("service");
        let module = guest::module_bytes();
        record_deployment(&pool, &service, &module).await;

        let worker = Worker::start(&db, nats_url, worker_env).await;
        worker
            .storage()
            .upload(
                &format!("wasm/{}/v{}.wasm", service.id, service.current_version),
                module,
                "application/wasm",
            )
            .await
            .expect("upload module");
        worker.process.wait_ready().await;
        Some(Self {
            pool,
            js,
            nats_url,
            slug: tenant.slug,
            tenant_id: tenant.id,
            service,
            worker,
            _turn: turn,
            db,
        })
    }

    async fn running(&self, sql: &str) -> i64 {
        let tenant_db = infra::with_dbname(
            &self.db.url,
            &TenantPoolManager::managed_db_name(&self.slug),
        );
        let (client, connection) = tokio_postgres::connect(&tenant_db, tokio_postgres::NoTls)
            .await
            .expect("tenant database");
        tokio::spawn(connection);
        client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity
                  WHERE datname = current_database() AND state = 'active'
                    AND pid <> pg_backend_pid() AND position($1 in query) > 0",
                &[&sql],
            )
            .await
            .expect("activity")
            .get(0)
    }

    async fn take_lease(&self, job_id: Uuid) -> wasm_jobs::WasmJob {
        self.pool
            .get()
            .await
            .expect("checkout")
            .execute(
                "UPDATE wasm_jobs SET lease_owner = 'another-worker', attempts = 2,
                        lease_until = now() + interval '1 hour'
                  WHERE id = $1",
                &[&job_id],
            )
            .await
            .expect("take the lease");
        self.row(job_id).await
    }

    async fn idle(&self, within: Duration) {
        let deadline = Instant::now() + within;
        while self
            .metric("cufflink_worker_inflight", &[("kind", "long")])
            .await
            != Some(0.0)
        {
            assert!(
                Instant::now() < deadline,
                "the handler was not stopped: {}",
                self.worker.process.output_tail()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn replace_worker(&mut self, worker_env: &[(&str, &str)]) {
        self.worker.process.kill();
        let storage_dir = std::mem::take(&mut self.worker.storage_dir);
        self.worker = Worker::start_in(storage_dir, &self.db, self.nats_url, worker_env).await;
        self.worker.process.wait_ready().await;
    }

    async fn record(&self, job: &Job) -> Uuid {
        let Job { handler, config } = job;
        let enqueued = wasm_jobs::create(
            &self.pool,
            NewJob {
                tenant_id: self.tenant_id,
                tenant_slug: &self.slug,
                service_id: self.service.id,
                service_slug: "spin-svc",
                service_version: self.service.current_version,
                handler,
                request_json: &json!({ "method": "POST", "handler": handler, "body": null }),
                auth_subject: None,
                auth_claims: None,
                max_attempts: i32::try_from(config.max_attempts).expect("attempts"),
                timeout_secs: i32::try_from(config.timeout_secs).expect("timeout"),
                retry_on: &config.retry_on,
                backoff: config.backoff,
                trace_context: None,
                idempotency_key: None,
                schedule_name: None,
                retain_for: Duration::from_secs(3600),
            },
        )
        .await
        .expect("job row");
        enqueued.job().id
    }

    async fn publish(&self, job_id: Uuid) {
        let job = self.row(job_id).await;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            "Nats-Msg-Id",
            cufflink_platform::router::jobs::message_id(&job).as_str(),
        );
        self.js
            .publish_with_headers(
                format!("cufflink.jobs.{}.spin-svc.{}", self.slug, job.handler),
                headers,
                json!({ "job_id": job_id }).to_string().into(),
            )
            .await
            .expect("publish")
            .await
            .expect("stored");
    }

    async fn enqueue(&self, job: &Job) -> Uuid {
        let job_id = self.record(job).await;
        self.publish(job_id).await;
        wasm_jobs::set_published(&self.pool, job_id, true)
            .await
            .expect("published");
        job_id
    }

    async fn messages(&self) -> u64 {
        self.js
            .get_stream(LONG_JOBS_STREAM)
            .await
            .expect("stream")
            .info()
            .await
            .expect("stream info")
            .state
            .messages
    }

    async fn row(&self, job_id: Uuid) -> wasm_jobs::WasmJob {
        wasm_jobs::get(&self.pool, job_id)
            .await
            .expect("job lookup")
            .expect("job row")
    }

    async fn until(
        &self,
        job_id: Uuid,
        what: &str,
        reached: impl Fn(&wasm_jobs::WasmJob) -> bool,
    ) -> wasm_jobs::WasmJob {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let row = self.row(job_id).await;
            if reached(&row) {
                return row;
            }
            assert!(Instant::now() < deadline, "the job never {what}: {row:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn metric(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        self.worker.process.metric(name, labels).await
    }

    async fn consumer(&self) -> async_nats::jetstream::consumer::Info {
        self.js
            .get_stream(LONG_JOBS_STREAM)
            .await
            .expect("stream")
            .get_consumer::<PullConfig>("cufflink-long-jobs")
            .await
            .expect("durable consumer")
            .info()
            .await
            .expect("consumer info")
            .clone()
    }

    async fn settled(&self, why: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let unsettled = self.messages().await;
            if unsettled == 0 {
                return;
            }
            assert!(Instant::now() < deadline, "{unsettled} unsettled: {why}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn worker_nak_on_shutdown_redelivers_immediately() {
    let Some(mut jobs) = JobService::start(&[("SHUTDOWN_GRACE_SECS", "5")]).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("spin", 60, 3)).await;

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        jobs.metric("cufflink_worker_inflight", &[("kind", "long")])
            .await,
        Some(1.0),
        "the job is running"
    );
    jobs.worker.process.sigterm();
    let exit = jobs
        .worker
        .process
        .wait_exit(Duration::from_secs(6))
        .await
        .expect("worker exits within the grace period");
    assert!(exit.success(), "exit status: {exit}");

    let redelivered = only_message(&jobs.js, LONG_JOBS_STREAM, "cufflink-long-jobs").await;
    assert_eq!(
        redelivered.info().expect("info").delivered,
        2,
        "second delivery of the naked message"
    );
    redelivered.ack().await.expect("ack");

    let row = jobs.row(job_id).await;
    assert_eq!(row.status, JobStatus::Pending, "the job is given up");
    assert_eq!(
        row.attempts, 0,
        "an attempt the shutdown stopped is not counted"
    );
    assert_eq!(row.lease_owner, None);
}

#[tokio::test]
async fn a_long_job_completes_with_what_its_handler_answered() {
    let Some(jobs) = JobService::start(&[]).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("echo", 60, 3)).await;
    let row = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!(row.status, JobStatus::Completed);
    assert_eq!((row.attempts, &row.error), (1, &None));
    assert_eq!(
        row.result_json,
        Some(json!({
            "__status": 200,
            "__body": {
                "_job": {"id": job_id.to_string(), "attempt": 1, "max_attempts": 3},
                "method": "POST",
                "handler": "echo",
                "body": null,
            },
        }))
    );
    assert_eq!((&row.lease_owner, row.lease_until), (&None, None));
    jobs.settled("its message is acknowledged").await;
    assert_eq!(
        jobs.metric(
            "cufflink_worker_jobs_total",
            &[("kind", "long"), ("result", "ok")]
        )
        .await,
        Some(1.0)
    );
    assert_eq!(
        jobs.metric("cufflink_job_transitions_total", &[("to", "completed")])
            .await,
        Some(1.0)
    );
}

#[tokio::test]
async fn a_long_job_that_times_out_fails_without_another_attempt() {
    let Some(jobs) = JobService::start(&[("JOB_LEASE_SECS", "3")]).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("spin", 5, 3)).await;
    let row = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.error.as_deref(), Some("Handler exceeded timeout (5s)"));
    assert_eq!(row.error_class.as_deref(), Some("timeout"));
    assert_eq!(
        row.attempts, 1,
        "a lease of 3 s was kept for the 5 s the handler ran"
    );
    jobs.settled("its message is acknowledged").await;
    assert_eq!(
        jobs.metric("cufflink_job_lease_lost_total", &[]).await,
        None
    );
}

#[tokio::test]
async fn a_worker_that_loses_its_lease_stops_the_job_and_writes_nothing() {
    let Some(jobs) = JobService::start(&[("JOB_LEASE_SECS", "3")]).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("spin", 60, 3)).await;
    jobs.until(job_id, "started", |row| row.status == JobStatus::Running)
        .await;
    let taken = jobs.take_lease(job_id).await;

    jobs.idle(Duration::from_secs(15)).await;
    assert_eq!(
        jobs.metric("cufflink_job_lease_lost_total", &[]).await,
        Some(1.0)
    );
    assert_eq!(
        jobs.metric("cufflink_job_fence_lost_total", &[("op", "execute")])
            .await,
        Some(1.0)
    );
    assert_eq!(
        jobs.row(job_id).await,
        taken,
        "the job is as its holder has it"
    );
    jobs.settled("the job's holder has its own message").await;
}

#[tokio::test]
async fn a_worker_that_loses_its_lease_stops_the_statement_its_handler_runs() {
    const SLEEP: &str = "pg_sleep(600)";
    let Some(jobs) = JobService::start(&[("JOB_LEASE_SECS", "3")]).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("slow_sql", 300, 3)).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while jobs.running(SLEEP).await == 0 {
        assert!(
            Instant::now() < deadline,
            "the statement never ran: {}",
            jobs.worker.process.output_tail()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let taken = jobs.take_lease(job_id).await;
    let lost_at = Instant::now();

    jobs.idle(Duration::from_secs(15)).await;
    assert!(
        lost_at.elapsed() < Duration::from_secs(10),
        "stopped {:?} after the lease was taken, not at the job's timeout",
        lost_at.elapsed()
    );
    assert_eq!(jobs.running(SLEEP).await, 0, "the server stopped it");
    assert_eq!(
        jobs.metric("cufflink_job_lease_lost_total", &[]).await,
        Some(1.0)
    );
    assert_eq!(
        jobs.row(job_id).await,
        taken,
        "the job is as its holder has it"
    );
    jobs.settled("the job's holder has its own message").await;
}

#[tokio::test]
async fn a_job_the_stream_never_took_is_published_by_the_reclaimer() {
    let Some(jobs) = JobService::start(&[("JOB_RECLAIM_INTERVAL_SECS", "1")]).await else {
        return;
    };
    let job_id = jobs.record(&job("echo", 60, 3)).await;
    let row = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!((row.status, row.attempts), (JobStatus::Completed, 1));
    assert!(row.published_at.is_some());
    assert_eq!(
        jobs.metric("cufflink_job_reclaimed_total", &[("reason", "unpublished")])
            .await,
        Some(1.0)
    );
}

#[tokio::test]
async fn the_reclaimer_reports_a_tenants_backlog() {
    let Some(jobs) = JobService::start(&[("JOB_RECLAIM_INTERVAL_SECS", "1")]).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("spin", 60, 1)).await;
    jobs.until(job_id, "started", |row| row.status == JobStatus::Running)
        .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    while jobs
        .metric("cufflink_job_backlog", &[("tenant", jobs.slug.as_str())])
        .await
        != Some(1.0)
    {
        assert!(
            Instant::now() < deadline,
            "the running job was never reported as the tenant's backlog: {}",
            jobs.worker.process.output_tail()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn a_job_whose_message_waits_for_a_worker_is_not_stored_again() {
    let Some(jobs) = JobService::start(&[
        ("MAX_CONCURRENT_JOBS", "1"),
        ("JOB_RECLAIM_INTERVAL_SECS", "1"),
        ("JOB_REPUBLISH_AFTER_SECS", "1"),
    ])
    .await
    else {
        return;
    };
    let running = jobs.enqueue(&job("spin", 60, 1)).await;
    jobs.until(running, "started", |row| row.status == JobStatus::Running)
        .await;
    let waiting = jobs.enqueue(&job("echo", 60, 1)).await;

    let deadline = Instant::now() + Duration::from_secs(30);
    while jobs
        .metric("cufflink_job_reclaimed_total", &[("reason", "unpublished")])
        .await
        < Some(3.0)
    {
        assert!(
            Instant::now() < deadline,
            "the waiting job was not published again: {}",
            jobs.worker.process.output_tail()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        jobs.messages().await,
        2,
        "the message of the job that runs and the one of the job that waits"
    );
    let waited = jobs.row(waiting).await;
    assert_eq!((waited.status, waited.attempts), (JobStatus::Pending, 0));
}

#[tokio::test]
async fn a_published_job_waits_no_longer_than_the_duplicate_window_of_its_stream() {
    let Some(jobs) = JobService::start(&[("JOB_RECLAIM_INTERVAL_SECS", "1")]).await else {
        return;
    };
    remember_for(&jobs.js, Duration::from_secs(1)).await;
    let job_id = jobs.record(&job("echo", 60, 3)).await;
    wasm_jobs::set_published(&jobs.pool, job_id, true)
        .await
        .expect("published");

    let row = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!((row.status, row.attempts), (JobStatus::Completed, 1));
    assert_eq!(
        jobs.metric("cufflink_job_reclaimed_total", &[("reason", "unpublished")])
            .await,
        Some(1.0),
        "published again after a second, not the five minutes of the default"
    );
}

const LOST_WORKER: [(&str, &str); 2] =
    [("JOB_LEASE_SECS", "3"), ("JOB_RECLAIM_INTERVAL_SECS", "1")];

#[tokio::test]
async fn job_lease_reclaim_after_worker_kill() {
    let Some(mut jobs) = JobService::start(&LOST_WORKER).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("spin", 8, 2)).await;
    let first = jobs
        .until(job_id, "started", |row| row.status == JobStatus::Running)
        .await;
    jobs.replace_worker(&LOST_WORKER).await;

    let second = jobs
        .until(job_id, "was taken over", |row| row.attempts == 2)
        .await;
    assert_eq!(second.status, JobStatus::Running);
    assert_ne!(second.lease_owner, first.lease_owner);
    assert_eq!(
        (&second.result_json, &second.error),
        (&None, &None),
        "the worker that was killed wrote nothing"
    );
    assert_eq!(
        jobs.metric(
            "cufflink_job_reclaimed_total",
            &[("reason", "lease_expired")]
        )
        .await,
        Some(1.0)
    );

    let ended = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!((ended.status, ended.attempts), (JobStatus::Failed, 2));
    assert_eq!(
        ended.error.as_deref(),
        Some("Handler exceeded timeout (8s)")
    );
}

#[tokio::test]
async fn a_job_whose_worker_died_on_its_last_attempt_is_failed_and_dead_lettered() {
    let Some(mut jobs) = JobService::start(&LOST_WORKER).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("spin", 60, 1)).await;
    jobs.until(job_id, "started", |row| row.status == JobStatus::Running)
        .await;
    jobs.replace_worker(&LOST_WORKER).await;

    let failed = jobs
        .until(job_id, "was dead-lettered", |row| {
            row.dead_lettered_at.is_some()
        })
        .await;
    assert_eq!((failed.status, failed.attempts), (JobStatus::Failed, 1));
    assert_eq!(
        failed.error.as_deref(),
        Some("worker lost after 1 attempts")
    );
    assert_eq!(failed.error_class.as_deref(), Some("exhausted"));
    assert_eq!(
        jobs.metric("cufflink_job_reclaimed_total", &[("reason", "exhausted")])
            .await,
        Some(1.0)
    );
    let letter = dead_letter(
        &jobs.js,
        format!("cufflink.dlq.{}.spin-svc.job.spin", jobs.slug),
        Duration::from_secs(5),
    )
    .await
    .expect("the job's dead letter");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&letter.payload).expect("json"),
        json!({
            "job_id": job_id.to_string(),
            "handler": "spin",
            "attempts": 1,
            "error": "worker lost after 1 attempts",
            "error_class": "exhausted",
        })
    );
}

#[tokio::test]
async fn a_failed_job_is_dead_lettered_under_its_service() {
    use cufflink_platform::nats_manager::dlq::{REASON_HEADER, SUBJECT_HEADER};
    let Some(jobs) = JobService::start(&[("JOB_RECLAIM_INTERVAL_SECS", "1")]).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("spin", 2, 3)).await;
    let failed = jobs
        .until(job_id, "was dead-lettered", |row| {
            row.dead_lettered_at.is_some()
        })
        .await;
    assert_eq!((failed.status, failed.attempts), (JobStatus::Failed, 1));

    let letter = dead_letter(
        &jobs.js,
        format!("cufflink.dlq.{}.spin-svc.job.spin", jobs.slug),
        Duration::from_secs(5),
    )
    .await
    .expect("the job's dead letter");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&letter.payload).expect("json"),
        json!({
            "job_id": job_id.to_string(),
            "handler": "spin",
            "attempts": 1,
            "error": "Handler exceeded timeout (2s)",
            "error_class": "timeout",
        })
    );
    let header = |name: &str| {
        letter
            .headers
            .as_ref()
            .and_then(|headers| headers.get(name))
            .map(|value| value.to_string())
    };
    assert_eq!(
        header(REASON_HEADER).as_deref(),
        Some("Handler exceeded timeout (2s)")
    );
    assert_eq!(
        header(SUBJECT_HEADER),
        Some(format!("cufflink.jobs.{}.spin-svc.spin", jobs.slug))
    );
    assert_eq!(
        header("Nats-Msg-Id"),
        Some(format!("dlq:CUFFLINK_LONG_JOBS:{job_id}:1"))
    );
}

#[tokio::test]
async fn a_job_that_retries_timeouts_is_attempted_until_out_of_attempts() {
    let Some(jobs) = JobService::start(&[("JOB_RECLAIM_INTERVAL_SECS", "1")]).await else {
        return;
    };
    let job_id = jobs
        .enqueue(&job("spin", 2, 2).retrying(RetryOn::Timeout))
        .await;
    let waiting = jobs
        .until(job_id, "was given another attempt", |row| {
            row.status == JobStatus::Pending && row.attempts == 1
        })
        .await;
    assert_eq!(
        waiting.last_error.as_deref(),
        Some("Handler exceeded timeout (2s)")
    );
    assert_eq!(waiting.error_class.as_deref(), Some("timeout"));
    assert_eq!(waiting.error, None);
    assert!(waiting.published_at.is_some(), "its message is handed back");

    let ended = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!((ended.status, ended.attempts), (JobStatus::Failed, 2));
    assert_eq!(
        ended.error.as_deref(),
        Some("Handler exceeded timeout (2s)")
    );
    assert_eq!(
        jobs.metric("cufflink_job_transitions_total", &[("to", "retry")])
            .await,
        Some(1.0)
    );
    assert_eq!(
        jobs.metric("cufflink_job_reclaimed_total", &[("reason", "unpublished")])
            .await,
        None,
        "the stream delivered its message again"
    );
}

#[tokio::test]
async fn job_retry_on_error_opt_in() {
    let Some(jobs) = JobService::start(&[("JOB_RECLAIM_INTERVAL_SECS", "1")]).await else {
        return;
    };
    let opted_in = jobs
        .enqueue(&job("fail_first", 60, 3).retrying(RetryOn::Error))
        .await;
    let as_always = jobs.enqueue(&job("fail_first", 60, 3)).await;

    let retried = jobs
        .until(opted_in, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!(
        (retried.status, retried.attempts),
        (JobStatus::Completed, 2)
    );
    assert_eq!(retried.result_json, Some(json!({"ok": true})));
    assert_eq!(retried.last_error.as_deref(), Some("Handler answered 503"));
    assert_eq!(retried.error, None);

    let completed = jobs
        .until(as_always, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!(
        (completed.status, completed.attempts),
        (JobStatus::Completed, 1)
    );
    assert_eq!(
        completed.result_json,
        Some(json!({"__status": 503, "__body": {"error": "upstream"}}))
    );

    let exhausted = jobs
        .enqueue(&job("fail_always", 60, 2).retrying(RetryOn::Error))
        .await;
    let failed = jobs
        .until(exhausted, "was dead-lettered", |row| {
            row.dead_lettered_at.is_some()
        })
        .await;
    assert_eq!((failed.status, failed.attempts), (JobStatus::Failed, 2));
    assert_eq!(failed.error.as_deref(), Some("Handler answered 503"));
    assert_eq!(failed.error_class.as_deref(), Some("handler_error"));
    jobs.settled("every attempt's message is acknowledged")
        .await;
}

#[tokio::test]
async fn a_job_longer_than_the_ack_wait_is_delivered_once() {
    let Some(jobs) = JobService::start(&[("LONG_JOBS_ACK_WAIT_SECS", "3")]).await else {
        return;
    };
    let delivered_before = jobs.consumer().await.delivered.consumer_sequence;
    let job_id = jobs.enqueue(&job("spin", 8, 3)).await;
    let ended = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!((ended.status, ended.attempts), (JobStatus::Failed, 1));
    jobs.settled("its message is acknowledged").await;
    let consumer = jobs.consumer().await;
    assert_eq!(consumer.config.ack_wait, Duration::from_secs(3));
    assert_eq!(
        (
            consumer.delivered.consumer_sequence - delivered_before,
            consumer.num_redelivered
        ),
        (1, 0),
        "{consumer:?}"
    );
    assert!(
        jobs.metric("cufflink_pump_heartbeats_total", &[("kind", "job")])
            .await
            .is_some_and(|heartbeats| heartbeats >= 2.0),
        "the worker told the stream it still had the message"
    );
}

#[tokio::test]
async fn the_consumer_of_an_earlier_release_is_brought_to_a_minute() {
    let Some(jobs) = JobService::start_beside(&[], true).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("echo", 60, 3)).await;
    let ended = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!(ended.status, JobStatus::Completed);
    let consumer = jobs.consumer().await;
    assert_eq!(consumer.config.ack_wait, Duration::from_secs(60));
    assert_eq!(consumer.config.max_deliver, -1);
    assert_eq!(consumer.config.filter_subject, "cufflink.jobs.>");
}

#[tokio::test]
async fn a_job_that_waits_longer_than_its_message_may_is_published_when_due() {
    let Some(jobs) = JobService::start(&[
        ("JOB_RECLAIM_INTERVAL_SECS", "1"),
        ("JOB_NAK_MAX_DELAY_SECS", "0"),
    ])
    .await
    else {
        return;
    };
    let job_id = jobs
        .enqueue(&job("spin", 2, 2).retrying(RetryOn::Timeout))
        .await;
    let ended = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!((ended.status, ended.attempts), (JobStatus::Failed, 2));
    assert_eq!(
        jobs.metric("cufflink_job_reclaimed_total", &[("reason", "unpublished")])
            .await,
        Some(1.0),
        "the reclaimer published its second attempt"
    );
    jobs.settled("every attempt's message is acknowledged")
        .await;
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_finishes_the_after_hook_in_hand_and_leaves_the_next_in_its_stream() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;

    let hooks = HookService::deploy(infra, &mut db).await;
    for _ in 0..2 {
        hooks.publish(&js, hooks.job("insert_slow", "{}")).await;
    }
    let mut worker = Worker::start_in(
        hooks.storage_dir.clone(),
        &db,
        nats_url,
        &[("SHUTDOWN_GRACE_SECS", "10")],
    )
    .await;
    worker.process.wait_ready().await;

    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        worker
            .process
            .metric("cufflink_worker_inflight", &[("kind", "async")])
            .await,
        Some(1.0),
        "the first hook is running"
    );
    worker.process.sigterm();

    let deadline = Instant::now() + Duration::from_secs(1);
    let (status, ready) = loop {
        match worker.process.get_json("/ready").await {
            Some((status, body)) if body["state"] == "shutting_down" => break (status, body),
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(20)).await,
            other => panic!("/ready never reported shutting_down: {other:?}"),
        }
    };
    assert_eq!(status, 503, "{ready}");
    assert_eq!(worker.process.status("/health").await, Some(200));

    let exit = worker
        .process
        .wait_exit(Duration::from_secs(10))
        .await
        .expect("worker exits within the grace period");
    assert!(exit.success(), "exit status: {exit}");

    assert_eq!(
        hooks.titles().await,
        ["hook"],
        "the in-flight hook ran exactly once"
    );

    let waiting = only_message(&js, ASYNC_JOBS_STREAM, "wasm-worker").await;
    assert_eq!(
        waiting.info().expect("info").delivered,
        1,
        "the second hook was never taken: a worker fetches what it has a slot for"
    );
    waiting.ack().await.expect("ack");
}

#[cfg(unix)]
#[tokio::test]
async fn an_after_hook_the_shutdown_stops_is_delivered_again_at_once() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;

    let hooks = HookService::deploy(infra, &mut db).await;
    hooks.publish(&js, hooks.job("spin", "{}")).await;
    let mut worker = Worker::start_in(
        hooks.storage_dir.clone(),
        &db,
        nats_url,
        &[("SHUTDOWN_GRACE_SECS", "5")],
    )
    .await;
    worker.process.wait_ready().await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while worker
        .process
        .metric("cufflink_worker_inflight", &[("kind", "async")])
        .await
        != Some(1.0)
    {
        assert!(Instant::now() < deadline, "the hook never started");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    worker.process.sigterm();
    let exit = worker
        .process
        .wait_exit(Duration::from_secs(6))
        .await
        .expect("worker exits within the grace period");
    assert!(exit.success(), "exit status: {exit}");

    let handed_back = Instant::now();
    let again = only_message(&js, ASYNC_JOBS_STREAM, "wasm-worker").await;
    assert!(
        handed_back.elapsed() < Duration::from_secs(5),
        "well inside the consumer's ack_wait of 60 s"
    );
    assert_eq!(again.info().expect("info").delivered, 2);
    again.ack().await.expect("ack");
}

#[tokio::test]
async fn a_forged_after_hook_is_dead_lettered_under_no_tenant() {
    use cufflink_platform::nats_manager::dlq::{REASON_HEADER, SUBJECT_HEADER};

    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;

    let hooks = HookService::deploy(infra, &mut db).await;
    let claimed = format!("forged-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let handler = format!("forged_{}", &Uuid::new_v4().simple().to_string()[..8]);
    hooks
        .publish(
            &js,
            cufflink_platform::worker_dispatch::WasmJob {
                tenant_slug: claimed.clone(),
                ..hooks.job(&handler, "{}")
            },
        )
        .await;

    let worker = Worker::start_in(hooks.storage_dir.clone(), &db, nats_url, &[]).await;
    worker.process.wait_ready().await;

    let letter = dead_letter(
        &js,
        format!("cufflink.dlq.*.hook-svc.hook.{handler}"),
        Duration::from_secs(20),
    )
    .await
    .expect("the hook is dead-lettered");
    assert_eq!(
        letter.subject.as_str(),
        format!("cufflink.dlq._.hook-svc.hook.{handler}"),
        "neither the tenant it claimed nor the one it named a deployment of"
    );
    let headers = letter.headers.as_ref().expect("headers");
    assert_eq!(
        headers.get(REASON_HEADER).map(|v| v.as_str()),
        Some("job identity does not match its deployment")
    );
    assert_eq!(
        headers.get(SUBJECT_HEADER).map(|v| v.as_str()),
        Some(format!("cufflink.wasm.async.hook-svc.{handler}").as_str())
    );
    let job: serde_json::Value = serde_json::from_slice(&letter.payload).expect("the job");
    assert_eq!(job["handler"], handler.as_str());

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut jobs = js.get_stream(ASYNC_JOBS_STREAM).await.expect("stream");
    while jobs.info().await.expect("info").state.messages > 0 {
        assert!(
            Instant::now() < deadline,
            "a dead-lettered hook is not delivered again"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        worker
            .process
            .metric(
                "cufflink_worker_jobs_total",
                &[("kind", "async"), ("result", "error")]
            )
            .await,
        Some(1.0)
    );
}

#[tokio::test]
async fn an_after_hook_out_of_deliveries_is_dead_lettered_under_its_deployment() {
    use cufflink_platform::nats_manager::dlq::{CONSUMER_HEADER, DELIVERIES_HEADER};

    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;

    let hooks = HookService::deploy(infra, &mut db).await;
    hooks.publish(&js, hooks.job("no_such_export", "{}")).await;
    let worker = Worker::start_in(hooks.storage_dir.clone(), &db, nats_url, &[]).await;
    worker.process.wait_ready().await;

    let letter = dead_letter(
        &js,
        format!("cufflink.dlq.{}.>", hooks.tenant.slug),
        Duration::from_secs(60),
    )
    .await
    .expect("the hook is dead-lettered once its deliveries run out");
    assert_eq!(
        letter.subject.as_str(),
        format!(
            "cufflink.dlq.{}.hook-svc.hook.no_such_export",
            hooks.tenant.slug
        )
    );
    let headers = letter.headers.as_ref().expect("headers");
    assert_eq!(
        headers.get(DELIVERIES_HEADER).map(|v| v.as_str()),
        Some("3")
    );
    assert_eq!(
        headers.get(CONSUMER_HEADER).map(|v| v.as_str()),
        Some("wasm-worker")
    );
}

#[tokio::test]
async fn after_hooks_run_in_order_unless_the_operator_opts_out() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;

    let hooks = HookService::deploy(infra, &mut db).await;
    let publish_four = || async {
        for request in ["1", "2", "3", "4"] {
            hooks
                .publish(&js, hooks.job("insert_ordered", request))
                .await;
        }
    };

    publish_four().await;
    let mut in_order = Worker::start_in(hooks.storage_dir.clone(), &db, nats_url, &[]).await;
    in_order.process.wait_ready().await;
    assert_eq!(hooks.until_titles(4).await, ["1", "2", "3", "4"]);
    in_order.process.sigterm();
    in_order
        .process
        .wait_exit(Duration::from_secs(10))
        .await
        .expect("worker exits");

    publish_four().await;
    let at_once = Worker::start_in(
        hooks.storage_dir.clone(),
        &db,
        nats_url,
        &[("WORKER_AFTER_HOOK_CONCURRENCY", "4")],
    )
    .await;
    at_once.process.wait_ready().await;
    let mut at_once = hooks.until_titles(8).await.split_off(4);
    assert_ne!(
        at_once[0], "1",
        "run at once, a quicker hook finishes before the first: {at_once:?}"
    );
    at_once.sort();
    assert_eq!(at_once, ["1", "2", "3", "4"]);
}

#[tokio::test]
async fn after_hooks_run_without_a_dead_letter_queue() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_spare_nats().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    js.get_or_create_stream(async_nats::jetstream::stream::Config {
        name: "SQUATTER".into(),
        subjects: vec!["cufflink.dlq.>".into()],
        no_ack: true,
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await
    .expect("squatter stream");
    events::setup_long_jobs_stream(&js)
        .await
        .expect("long-jobs stream");

    let hooks = HookService::deploy(infra, &mut db).await;
    hooks.publish(&js, hooks.job("insert_ordered", "7")).await;
    let handler = format!("forged_{}", &Uuid::new_v4().simple().to_string()[..8]);
    hooks
        .publish(
            &js,
            cufflink_platform::worker_dispatch::WasmJob {
                tenant_slug: "forged".into(),
                ..hooks.job(&handler, "{}")
            },
        )
        .await;
    let worker = Worker::start_in(hooks.storage_dir.clone(), &db, nats_url, &[]).await;
    let ready = worker.process.wait_ready().await;
    assert_eq!(ready["consumers"]["up"], 3, "{ready}");
    assert_eq!(hooks.until_titles(1).await, ["7"]);

    let deadline = Instant::now() + Duration::from_secs(20);
    while worker
        .process
        .metric("cufflink_dlq_publish_failed_total", &[("kind", "hook")])
        .await
        .is_none()
    {
        assert!(Instant::now() < deadline, "the hook was never given up on");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        worker
            .process
            .metric(
                "cufflink_platform_stream_unavailable_total",
                &[("stream", "CUFFLINK_DLQ")]
            )
            .await
            .is_some_and(|attempts| attempts >= 1.0),
        "the worker keeps trying to create the queue"
    );
    let mut consumer = js
        .get_stream(ASYNC_JOBS_STREAM)
        .await
        .expect("stream")
        .get_consumer::<PullConfig>("wasm-worker")
        .await
        .expect("durable consumer");
    let info = consumer.info().await.expect("info");
    assert_eq!(
        (info.num_ack_pending, info.ack_floor.stream_sequence),
        (1, 1),
        "the hook that ran is acknowledged; the one given up on is neither ended nor lost: {info:?}"
    );
}

struct Backups {
    js: async_nats::jetstream::Context,
    pool: deadpool_postgres::Pool,
    tenant: infra::Tenant,
    service: cufflink_platform::repo::services::Service,
    tenant_db: deadpool_postgres::Object,
    _worker: Worker,
    _turn: MutexGuard<'static, ()>,
    _db: infra::TestDb,
}

impl Backups {
    async fn start(manifest: serde_json::Value, schema: &str, fixture: &str) -> Option<Self> {
        Self::start_with(manifest, schema, fixture, &[]).await
    }

    async fn start_with(
        manifest: serde_json::Value,
        schema: &str,
        fixture: &str,
        worker_env: &[(&str, &str)],
    ) -> Option<Self> {
        let infra = infra::infra().await?;
        let nats_url = infra.require_nats()?;
        let mut db = infra.create_migrated_database().await;
        let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
        let turn = exclusive_jetstream(&js).await;
        events::setup_long_jobs_stream(&js)
            .await
            .expect("long-jobs stream");
        events::backup_stream(&js).await.expect("backup stream");

        let tenant = db.mint_tenant().await;
        let pool = db.pool();
        let name = manifest["name"].as_str().expect("a service name");
        let service = cufflink_testkit::active_service(&pool, tenant.id, name, schema)
            .await
            .expect("service");
        cufflink_platform::repo::deployments::activate_version(
            &pool,
            cufflink_platform::repo::deployments::ActivateVersion {
                service_id: service.id,
                version: service.current_version,
                manifest_hash: name,
                manifest: &serde_json::from_value(manifest.clone()).expect("a manifest"),
                wasm_hash: None,
                commit_hash: None,
                schema_json: None,
                schema_hash: "",
                native: None,
            },
        )
        .await
        .expect("deployment row");
        let tenant_db = cufflink_db::build_pool(
            &infra::with_dbname(
                &infra.database_url,
                &TenantPoolManager::managed_db_name(&tenant.slug),
            ),
            1,
        )
        .expect("tenant pool")
        .get()
        .await
        .expect("tenant connection");
        tenant_db
            .batch_execute(fixture)
            .await
            .expect("the tables as deploy DDL creates them");

        let worker = Worker::start(&db, nats_url, worker_env).await;
        worker.process.wait_ready().await;
        Some(Self {
            js,
            pool,
            tenant,
            service,
            tenant_db,
            _worker: worker,
            _turn: turn,
            _db: db,
        })
    }

    async fn publish(&self, job_id: Uuid, service_id: Uuid, job_type: &str) {
        let message = json!({
            "job_id": job_id,
            "service_id": service_id,
            "tenant_id": self.tenant.id,
            "tenant_slug": self.tenant.slug,
            "service_name": self.service.name,
            "job_type": job_type,
        });
        self.js
            .publish(
                format!("cufflink.backup.{}", self.service.id),
                message.to_string().into(),
            )
            .await
            .expect("publish")
            .await
            .expect("stored");
    }

    async fn queued(
        &self,
        job_type: &str,
        s3_key: Option<String>,
    ) -> cufflink_platform::repo::backup_jobs::BackupJob {
        cufflink_platform::repo::backup_jobs::create(
            &self.pool,
            self.service.id,
            self.tenant.id,
            job_type,
            None,
            true,
            s3_key.as_deref(),
        )
        .await
        .expect("job row")
    }

    async fn job(&self, job_id: Uuid) -> cufflink_platform::repo::backup_jobs::BackupJob {
        cufflink_platform::repo::backup_jobs::get_by_id(&self.pool, job_id)
            .await
            .expect("job")
            .expect("job row")
    }

    async fn finished(&self, job_id: Uuid) -> cufflink_platform::repo::backup_jobs::BackupJob {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let job = self.job(job_id).await;
            match job.status.as_str() {
                "completed" => return job,
                "failed" => panic!("{} failed: {:?}", job.job_type, job.error_message),
                _ if Instant::now() > deadline => panic!("{} never finished", job.job_type),
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    }

    async fn run(
        &self,
        job_type: &str,
        s3_key: Option<String>,
    ) -> cufflink_platform::repo::backup_jobs::BackupJob {
        let job = self.queued(job_type, s3_key).await;
        self.publish(job.id, self.service.id, job_type).await;
        self.finished(job.id).await
    }

    async fn texts(&self, sql: &str) -> Vec<String> {
        let mut texts: Vec<String> = self
            .tenant_db
            .query(sql, &[])
            .await
            .expect(sql)
            .iter()
            .map(|row| row.get(0))
            .collect();
        texts.sort_unstable();
        texts
    }
}

#[tokio::test]
async fn backup_round_trips_a_mixed_case_table() {
    let Some(backups) = Backups::start(
        json!({
            "name": "Order",
            "mode": "crud",
            "tables": [{
                "name": "OrderLines",
                "columns": [
                    {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                    {"name": "Title", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}
                ]
            }],
            "publishes": [],
        }),
        "Order",
        "CREATE SCHEMA \"order\";
         CREATE TABLE \"order\".OrderLines (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), Title TEXT);
         INSERT INTO \"order\".OrderLines (Title) VALUES ('first'), ('second');",
    )
    .await
    else {
        return;
    };

    let export = backups.run("export", None).await;
    backups
        .tenant_db
        .batch_execute("DELETE FROM \"order\".OrderLines")
        .await
        .expect("lose the rows");
    let forged = backups.queued("export", None).await;
    backups
        .publish(export.id, backups.service.id, "export")
        .await;
    backups.publish(forged.id, Uuid::new_v4(), "export").await;
    backups.run("restore", export.s3_key.clone()).await;
    assert_eq!(
        backups
            .texts("SELECT Title FROM \"order\".OrderLines")
            .await,
        ["first", "second"]
    );

    let replayed = backups.job(export.id).await;
    assert_eq!(
        (replayed.status.as_str(), replayed.completed_at),
        ("completed", export.completed_at),
        "a finished job is not run again by its message"
    );
    let refused = backups.job(forged.id).await;
    assert_eq!(
        (refused.status.as_str(), refused.started_at),
        ("queued", None),
        "a message naming another service runs nothing and leaves the row alone"
    );
}

#[tokio::test]
async fn g1_backups_leave_private_tables_alone() {
    let columns = json!([
        {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
        {"name": "body", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}
    ]);
    let Some(backups) = Backups::start(
        json!({
            "name": "shop",
            "mode": "crud",
            "tables": [{"name": "open", "columns": columns}],
            "private_tables": [{"name": "closed", "columns": columns, "managed": false}],
            "publishes": [],
        }),
        "shop",
        "CREATE SCHEMA shop;
         CREATE TABLE shop.open (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), body TEXT);
         CREATE TABLE shop.closed (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), body TEXT);
         INSERT INTO shop.open (body) VALUES ('first'), ('second');
         INSERT INTO shop.closed (body) VALUES ('before the export'), ('lost after it');",
    )
    .await
    else {
        return;
    };

    let export = backups.run("export", None).await;
    backups
        .tenant_db
        .batch_execute(
            "DELETE FROM shop.open;
             DELETE FROM shop.closed WHERE body = 'lost after it';
             INSERT INTO shop.closed (body) VALUES ('written after it');",
        )
        .await
        .expect("change both tables");
    backups.run("restore", export.s3_key.clone()).await;

    assert_eq!(
        backups.texts("SELECT body FROM shop.open").await,
        ["first", "second"],
        "the served table is restored"
    );
    assert_eq!(
        backups.texts("SELECT body FROM shop.closed").await,
        ["before the export", "written after it"],
        "no row of the private table was exported, and none was cleared"
    );
}

#[tokio::test]
async fn a_running_backup_job_is_claimed_again_by_a_redelivery_alone() {
    use cufflink_platform::repo::backup_jobs;

    let Some(infra) = infra::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let pool = db.pool();
    let service = cufflink_testkit::active_service(&pool, tenant.id, "ledger", "ledger")
        .await
        .expect("service");
    let job = backup_jobs::create(&pool, service.id, tenant.id, "export", None, false, None)
        .await
        .expect("job row");
    let claim = |redelivered| backup_jobs::claim(&pool, &job, redelivered);
    assert!(
        claim(false).await.expect("claim"),
        "a queued job is claimed"
    );
    assert!(
        !claim(false).await.expect("claim"),
        "a running job is not claimed by a fresh message"
    );
    assert!(
        claim(true).await.expect("claim"),
        "its redelivery claims it again"
    );
    backup_jobs::update_status(&pool, job.id, "completed", None)
        .await
        .expect("completed");
    for redelivered in [false, true] {
        assert!(
            !claim(redelivered).await.expect("claim"),
            "a finished job is claimed by nothing"
        );
    }
    let foreign = backup_jobs::BackupJob {
        tenant_id: Uuid::new_v4(),
        ..job.clone()
    };
    backup_jobs::update_status(&pool, job.id, "running", None)
        .await
        .expect("running");
    assert!(
        !backup_jobs::claim(&pool, &foreign, true)
            .await
            .expect("claim"),
        "nor one whose row names another tenant"
    );
}

#[tokio::test]
async fn a_worker_guest_calls_an_internal_route_through_the_platform() {
    const KEYS: &str = "k1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let Some(platform_binary) = infra::require_binary("cufflink-platform") else {
        return;
    };
    if let Some(reason) = cufflink_test_guests::UNAVAILABLE {
        infra::skip(reason);
        return;
    }
    let wasm = cufflink_test_guests::wasm("internal", cufflink_test_guests::Profile::Debug)
        .expect("built guest");
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;

    let storage_dir = std::env::temp_dir().join(format!("cufflink-worker-{}", Uuid::new_v4()));
    let dir = storage_dir.to_string_lossy().into_owned();
    let platform = ChildProcess::spawn(
        &platform_binary,
        "PLATFORM_PORT",
        &[
            ("DATABASE_URL", db.url.as_str()),
            ("NATS_URL", nats_url),
            ("PLATFORM_HOST", "127.0.0.1"),
            ("KEYCLOAK_URL", ""),
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", dir.as_str()),
            ("CUFFLINK_SERVICE_IDENTITY_KEYS", KEYS),
            ("WORKER_ROUTES", r#"{"front": ["hop"]}"#),
            ("RUST_LOG", "info"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let manifest = |name: &str, routes: serde_json::Value| {
        json!({
            "name": name,
            "version": "0.1.0",
            "mode": "wasm",
            "routing": "strict",
            "tables": [{
                "name": "items",
                "columns": [{"name": "id", "column_type": "uuid", "primary_key": true,
                             "nullable": false, "auto_generate": true}]
            }],
            "custom_routes": routes,
            "publishes": [],
        })
    };
    for (name, routes) in [
        (
            "front",
            json!([{"method": "POST", "path": "/hop", "handler": "hop",
                    "posture": {"kind": "public", "reason": "integration test"}}]),
        ),
        (
            "back",
            json!([{"method": "POST", "path": "/files/{id}", "handler": "whoami",
                    "posture": {"kind": "internal", "callers": ["front"]}}]),
        ),
    ] {
        use base64::Engine;
        let response = platform
            .http
            .post(format!("{}/api/services/deploy", platform.base_url))
            .header("authorization", format!("ApiKey {}", tenant.api_key))
            .json(&json!({
                "manifest": manifest(name, routes),
                "wasm_base64": base64::engine::general_purpose::STANDARD.encode(&wasm),
                "api_version": 1,
            }))
            .send()
            .await
            .expect("deploy");
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        assert_eq!(status, 200, "deploy of {name}: {body}");
    }

    let worker = Worker::start_in(
        storage_dir,
        &db,
        nats_url,
        &[
            ("CUFFLINK_SERVICE_IDENTITY_KEYS", KEYS),
            ("PLATFORM_INTERNAL_URL", platform.base_url.as_str()),
        ],
    )
    .await;
    worker.process.wait_ready().await;
    let hop = |path: &str| {
        platform
            .http
            .post(format!(
                "{}/svc/{}/front/hop",
                platform.base_url, tenant.slug
            ))
            .json(&json!({"service": "back", "path": path, "body": {}}))
            .send()
    };
    for path in ["/files/7", "/files/{draft}"] {
        let response = hop(path).await.expect("hop");
        let status = response.status().as_u16();
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(status, 200, "{path}: {body}");
        assert_eq!(
            body,
            json!({"status": 200, "body": {
                "caller": {"service": "front", "on_behalf_of": null, "depth": 1},
                "authenticated": false,
                "identity_header": null,
            }}),
            "{path}"
        );
    }
}

#[tokio::test]
async fn a_job_enqueued_while_the_stream_was_down_runs_once_a_worker_finds_it() {
    let Some(platform_binary) = infra::require_binary("cufflink-platform") else {
        return;
    };
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;
    events::setup_long_jobs_stream(&js)
        .await
        .expect("long-jobs stream");

    let storage_dir = std::env::temp_dir().join(format!("cufflink-worker-{}", Uuid::new_v4()));
    let dir = storage_dir.to_string_lossy().into_owned();
    let nowhere = format!("nats://127.0.0.1:{}", infra::free_port());
    let platform = ChildProcess::spawn(
        &platform_binary,
        "PLATFORM_PORT",
        &[
            ("DATABASE_URL", db.url.as_str()),
            ("NATS_URL", nowhere.as_str()),
            ("NATS_PUBLISH_TIMEOUT_MS", "300"),
            ("PLATFORM_HOST", "127.0.0.1"),
            ("KEYCLOAK_URL", ""),
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", dir.as_str()),
            ("RUST_LOG", "info"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    use base64::Engine;
    let deployed = platform
        .http
        .post(format!("{}/api/services/deploy", platform.base_url))
        .header("authorization", format!("ApiKey {}", tenant.api_key))
        .json(&json!({
            "manifest": {
                "name": "job-svc",
                "version": "0.1.0",
                "mode": "wasm",
                "tables": [{
                    "name": "items",
                    "columns": [{"name": "id", "column_type": "uuid", "primary_key": true,
                                 "nullable": false, "auto_generate": true}]
                }],
                "custom_routes": [{
                    "method": "POST", "path": "/echo", "handler": "echo",
                    "posture": {"kind": "public", "reason": "integration test"},
                    "job": {"timeout_secs": 60, "max_attempts": 2},
                }],
                "publishes": [],
            },
            "wasm_base64": base64::engine::general_purpose::STANDARD.encode(guest::module_bytes()),
            "api_version": 1,
        }))
        .send()
        .await
        .expect("deploy");
    assert_eq!(deployed.status().as_u16(), 200);

    let accepted = platform
        .http
        .post(format!(
            "{}/svc/{}/job-svc/_fn/echo",
            platform.base_url, tenant.slug
        ))
        .json(&json!({"id": "item-1"}))
        .send()
        .await
        .expect("job route");
    assert_eq!(accepted.status().as_u16(), 202);
    let accepted: serde_json::Value = accepted.json().await.expect("json");
    let job_id = accepted["job_id"]
        .as_str()
        .and_then(|id| Uuid::parse_str(id).ok())
        .expect("job id");

    let pool = db.pool();
    let row = || async {
        wasm_jobs::get(&pool, job_id)
            .await
            .expect("lookup")
            .expect("job row")
    };
    let unpublished = row().await;
    assert_eq!(
        (unpublished.status, unpublished.published_at),
        (JobStatus::Pending, None)
    );

    let worker = Worker::start_in(
        storage_dir,
        &db,
        nats_url,
        &[("JOB_RECLAIM_INTERVAL_SECS", "1")],
    )
    .await;
    worker.process.wait_ready().await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let ended = loop {
        let row = row().await;
        if row.status.is_terminal() {
            break row;
        }
        assert!(Instant::now() < deadline, "the job never ran: {row:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!((ended.status, ended.attempts), (JobStatus::Completed, 1));
    assert_eq!(
        ended
            .result_json
            .as_ref()
            .map(|result| &result["__body"]["body"]),
        Some(&json!({"id": "item-1"}))
    );
}

#[tokio::test]
async fn a_job_route_retries_what_it_declares_and_what_its_handler_asks() {
    let Some(platform_binary) = infra::require_binary("cufflink-platform") else {
        return;
    };
    if let Some(reason) = cufflink_test_guests::UNAVAILABLE {
        infra::skip(reason);
        return;
    }
    let wasm = cufflink_test_guests::wasm("jobs", cufflink_test_guests::Profile::Debug)
        .expect("built guest");
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;

    let storage_dir = std::env::temp_dir().join(format!("cufflink-worker-{}", Uuid::new_v4()));
    let dir = storage_dir.to_string_lossy().into_owned();
    let platform = ChildProcess::spawn(
        &platform_binary,
        "PLATFORM_PORT",
        &[
            ("DATABASE_URL", db.url.as_str()),
            ("NATS_URL", nats_url),
            ("PLATFORM_HOST", "127.0.0.1"),
            ("KEYCLOAK_URL", ""),
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", dir.as_str()),
            ("RUST_LOG", "info"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let (status, version) = platform.get_json("/api/version").await.expect("version");
    assert_eq!(status, 200);
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("jobs_v2"))),
        "{version}"
    );

    let route = |handler: &str, job: serde_json::Value| {
        json!({
            "method": "POST",
            "path": format!("/{handler}"),
            "handler": handler,
            "posture": {"kind": "public", "reason": "integration test"},
            "job": job,
        })
    };
    use base64::Engine;
    let deployed = platform
        .http
        .post(format!("{}/api/services/deploy", platform.base_url))
        .header("authorization", format!("ApiKey {}", tenant.api_key))
        .json(&json!({
            "manifest": {
                "name": "job-svc",
                "version": "0.1.0",
                "mode": "wasm",
                "tables": [{
                    "name": "items",
                    "columns": [{"name": "id", "column_type": "uuid", "primary_key": true,
                                 "nullable": false, "auto_generate": true}]
                }],
                "custom_routes": [
                    route("job_fail_first", json!({
                        "timeout_secs": 60, "max_attempts": 3,
                        "retry_on": ["error"], "backoff": {"base_secs": 1, "max_secs": 1},
                    })),
                    route("job_retry_later", json!({"timeout_secs": 60, "max_attempts": 3})),
                    route("job_give_up", json!({"timeout_secs": 60, "max_attempts": 3})),
                ],
                "publishes": [],
            },
            "wasm_base64": base64::engine::general_purpose::STANDARD.encode(&wasm),
            "api_version": 1,
        }))
        .send()
        .await
        .expect("deploy");
    let status = deployed.status().as_u16();
    let body = deployed.text().await.unwrap_or_default();
    assert_eq!(status, 200, "deploy: {body}");

    let worker = Worker::start_in(
        storage_dir,
        &db,
        nats_url,
        &[("JOB_RECLAIM_INTERVAL_SECS", "1")],
    )
    .await;
    worker.process.wait_ready().await;

    let pool = db.pool();
    let enqueue = |handler: &'static str| {
        let request = platform
            .http
            .post(format!(
                "{}/svc/{}/job-svc/_fn/{handler}",
                platform.base_url, tenant.slug
            ))
            .header("authorization", format!("ApiKey {}", tenant.api_key))
            .json(&json!({"id": "item-1"}));
        async move {
            let accepted = request.send().await.expect("job route");
            assert_eq!(accepted.status().as_u16(), 202, "{handler}");
            let accepted: serde_json::Value = accepted.json().await.expect("json");
            accepted["job_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
                .expect("job id")
        }
    };
    let ended = |job_id: Uuid| {
        let pool = pool.clone();
        async move {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let row = wasm_jobs::get(&pool, job_id)
                    .await
                    .expect("lookup")
                    .expect("job row");
                if row.status.is_terminal() {
                    return row;
                }
                assert!(Instant::now() < deadline, "the job never ended: {row:?}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    let (fail_first, retry_later, give_up) = (
        enqueue("job_fail_first").await,
        enqueue("job_retry_later").await,
        enqueue("job_give_up").await,
    );

    let retried = ended(fail_first).await;
    assert_eq!(
        (retried.status, retried.attempts),
        (JobStatus::Completed, 2)
    );
    assert_eq!(retried.retry_on, vec![RetryOn::Error]);
    let told = retried.result_json.expect("result");
    assert_eq!(told["attempt"], 2, "{told}");
    assert_eq!(told["max_attempts"], 3);
    assert_eq!(told["last_error"], "Handler answered 503");
    assert_eq!(
        told["authorization"],
        serde_json::Value::Null,
        "the job was not kept with its caller's key"
    );
    assert!(
        told["subject"].is_string(),
        "and runs as its caller all the same: {told}"
    );

    let asked = ended(retry_later).await;
    assert_eq!((asked.status, asked.attempts), (JobStatus::Completed, 2));
    assert_eq!(asked.retry_on, vec![]);
    assert_eq!(
        asked.result_json.expect("result")["last_error"],
        "Handler asked for another attempt (503)"
    );
    assert_eq!(asked.error_class.as_deref(), Some("explicit"));

    let given_up = ended(give_up).await;
    assert_eq!((given_up.status, given_up.attempts), (JobStatus::Failed, 1));
    assert_eq!(given_up.error.as_deref(), Some("unknown item"));
    assert_eq!(given_up.error_class.as_deref(), Some("terminal"));
    let letter = dead_letter(
        &js,
        format!("cufflink.dlq.{}.job-svc.job.job_give_up", tenant.slug),
        Duration::from_secs(10),
    )
    .await
    .expect("the job's dead letter");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&letter.payload).expect("json"),
        json!({
            "job_id": give_up.to_string(),
            "handler": "job_give_up",
            "attempts": 1,
            "error": "unknown item",
            "error_class": "terminal",
        })
    );
}

#[tokio::test]
async fn a_job_calls_an_extension_its_tenant_registers() {
    use cufflink_types::ext::{call_subject, ExtReply, ExtRequest, QUEUE_GROUP};

    let Some(platform_binary) = infra::require_binary("cufflink-platform") else {
        return;
    };
    if let Some(reason) = cufflink_test_guests::UNAVAILABLE {
        infra::skip(reason);
        return;
    }
    let wasm = cufflink_test_guests::wasm("ext-call", cufflink_test_guests::Profile::Debug)
        .expect("built guest");
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;

    let storage_dir = std::env::temp_dir().join(format!("cufflink-worker-{}", Uuid::new_v4()));
    let dir = storage_dir.to_string_lossy().into_owned();
    let platform = ChildProcess::spawn(
        &platform_binary,
        "PLATFORM_PORT",
        &[
            ("DATABASE_URL", db.url.as_str()),
            ("NATS_URL", nats_url),
            ("PLATFORM_HOST", "127.0.0.1"),
            ("KEYCLOAK_URL", ""),
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", dir.as_str()),
            ("CUFFLINK_EXTENSIONS", "true"),
            ("RUST_LOG", "info"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let admin = db.mint_admin_key().await;

    let registered = platform
        .http
        .put(format!(
            "{}/api/tenants/{}/extensions/test.echo",
            platform.base_url, tenant.slug
        ))
        .header("authorization", format!("ApiKey {admin}"))
        .json(&json!({"callers": ["job-svc"]}))
        .send()
        .await
        .expect("register");
    assert_eq!(registered.status().as_u16(), 200);

    use base64::Engine;
    let deployed = platform
        .http
        .post(format!("{}/api/services/deploy", platform.base_url))
        .header("authorization", format!("ApiKey {}", tenant.api_key))
        .json(&json!({
            "manifest": {
                "name": "job-svc",
                "version": "0.1.0",
                "mode": "wasm",
                "tables": [{
                    "name": "items",
                    "columns": [{"name": "id", "column_type": "uuid", "primary_key": true,
                                 "nullable": false, "auto_generate": true}]
                }],
                "custom_routes": [{
                    "method": "POST", "path": "/call", "handler": "call",
                    "posture": {"kind": "public", "reason": "integration test"},
                    "job": {"timeout_secs": 60, "max_attempts": 1},
                }],
                "publishes": [],
                "extensions": ["test.echo"],
            },
            "wasm_base64": base64::engine::general_purpose::STANDARD.encode(&wasm),
            "api_version": 1,
        }))
        .send()
        .await
        .expect("deploy");
    let status = deployed.status().as_u16();
    let deployed: serde_json::Value = deployed.json().await.expect("json");
    assert_eq!(status, 200, "deploy: {deployed}");

    let provider = async_nats::connect(nats_url).await.expect("provider");
    let mut calls = provider
        .queue_subscribe(call_subject(&tenant.slug, "test.echo"), QUEUE_GROUP.into())
        .await
        .expect("subscribe");
    provider.flush().await.expect("flush");
    tokio::spawn(async move {
        while let Some(message) = calls.next().await {
            let request: ExtRequest =
                serde_json::from_slice(&message.payload).expect("an ExtRequest");
            let reply = ExtReply::Ok(json!({
                "context": request.context,
                "payload": request.payload,
            }));
            if let Some(to) = message.reply {
                provider
                    .publish(to, serde_json::to_vec(&reply).expect("encode").into())
                    .await
                    .expect("reply");
            }
        }
    });

    let worker = Worker::start_in(storage_dir, &db, nats_url, &[]).await;
    worker.process.wait_ready().await;

    let accepted = platform
        .http
        .post(format!(
            "{}/svc/{}/job-svc/_fn/call",
            platform.base_url, tenant.slug
        ))
        .json(&json!({"extension": "test.echo", "payload": {"from": "a job"}}))
        .send()
        .await
        .expect("job route");
    assert_eq!(accepted.status().as_u16(), 202);
    let accepted: serde_json::Value = accepted.json().await.expect("json");
    let job_id = accepted["job_id"]
        .as_str()
        .and_then(|id| Uuid::parse_str(id).ok())
        .expect("job id");

    let pool = db.pool();
    let deadline = Instant::now() + Duration::from_secs(30);
    let ended = loop {
        let row = wasm_jobs::get(&pool, job_id)
            .await
            .expect("lookup")
            .expect("job row");
        if row.status.is_terminal() {
            break row;
        }
        assert!(Instant::now() < deadline, "the job never ran: {row:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(ended.status, JobStatus::Completed, "{ended:?}");
    let answered = ended
        .result_json
        .as_ref()
        .map(|result| result["ok"].clone())
        .unwrap_or_default();
    assert_eq!(answered["payload"], json!({"from": "a job"}), "{answered}");
    assert_eq!(answered["context"]["tenant"], json!(tenant.slug));
    assert_eq!(
        answered["context"]["service"],
        json!({"name": "job-svc", "version": deployed["version"]})
    );
}

fn scope_of<'a>(
    slug: &'a str,
    service: (&'a str, &'a str),
    handler: &'a str,
    kind: &'a str,
) -> Vec<(&'a str, &'a str)> {
    vec![
        ("cufflink.handler", handler),
        ("cufflink.kind", kind),
        ("cufflink.service", service.0),
        ("cufflink.tenant", slug),
        ("cufflink.version", service.1),
    ]
}

async fn said_as(capture: &Capture, kind: &str) -> (Signal, Signal, Signal) {
    let line = format!("[wasm] {}", guest::SAID);
    let log = capture
        .log(&format!("the guest's line as a {kind}"), |record| {
            record.text == line && record.attribute("cufflink.kind") == Some(kind)
        })
        .await
        .expect("exported");
    let handler = capture
        .span("the handler's span", |span| span.span_id == log.span_id)
        .await
        .expect("exported");
    let job = capture
        .span("the job's span", |span| {
            span.span_id == handler.parent_span_id
        })
        .await
        .expect("exported");
    (log, handler, job)
}

#[tokio::test]
async fn cf3_each_worker_job_exports_its_scope() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let capture = Capture::start().await.expect("capture");
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let turn = exclusive_jetstream(&js).await;
    let hooks = HookService::deploy(infra, &mut db).await;
    let worker = Worker::start_in(hooks.storage_dir.clone(), &db, nats_url, &capture.env()).await;
    worker.process.wait_ready().await;
    let version = format!("v{}", hooks.service.current_version);
    let scope = |kind| {
        scope_of(
            &hooks.tenant.slug,
            ("hook-svc", version.as_str()),
            "say",
            kind,
        )
    };

    let answered = async_nats::connect(nats_url)
        .await
        .expect("nats")
        .request(
            "cufflink.wasm.hook-svc.say",
            serde_json::to_vec(&hooks.job("say", "{}"))
                .expect("a job")
                .into(),
        )
        .await
        .expect("an answer");
    let answered: serde_json::Value = serde_json::from_slice(&answered.payload).expect("json");
    assert_eq!(answered["ok"], true, "{answered}");
    let (log, handler, ran) = said_as(&capture, "worker_route").await;
    assert_eq!(log.scope(), scope("worker_route"));
    assert_eq!(handler.scope(), log.scope());
    assert_eq!(
        (ran.text.as_str(), ran.scope()),
        ("worker/sync", scope("worker_route"))
    );
    assert_eq!(ran.parent_span_id, "", "a job is its own trace");
    assert_eq!(log.trace_id, ran.trace_id);

    hooks.publish(&js, hooks.job("say", "{}")).await;
    let (log, handler, ran) = said_as(&capture, "hook").await;
    assert_eq!(log.scope(), scope("hook"));
    assert_eq!(handler.scope(), log.scope());
    assert_eq!(
        (ran.text.as_str(), ran.scope()),
        ("worker/async", scope("hook"))
    );

    for signal in [&log, &handler, &ran] {
        assert_eq!(
            (
                signal.resource.get("service.name").map(String::as_str),
                signal
                    .resource
                    .get("cufflink.component")
                    .map(String::as_str)
            ),
            (Some("cufflink-worker"), Some("worker"))
        );
    }
    drop((worker, turn));

    let Some(jobs) = JobService::start(&capture.env()).await else {
        return;
    };
    let job_id = jobs.enqueue(&job("say", 60, 1)).await;
    let row = jobs
        .until(job_id, "ended", |row| row.status.is_terminal())
        .await;
    assert_eq!(row.status, JobStatus::Completed);
    let version = format!("v{}", jobs.service.current_version);
    let (log, handler, ran) = said_as(&capture, "job").await;
    let long = scope_of(&jobs.slug, ("spin-svc", version.as_str()), "say", "job");
    assert_eq!(log.scope(), long);
    assert_eq!(handler.scope(), long);
    assert_eq!((ran.text.as_str(), ran.scope()), ("worker/long", long));
}

#[tokio::test]
async fn cf3_a_refused_worker_job_exports_no_scope() {
    let Some(infra) = infra::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let capture = Capture::start().await.expect("capture");
    let mut db = infra.create_migrated_database().await;
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let _turn = exclusive_jetstream(&js).await;
    let hooks = HookService::deploy(infra, &mut db).await;
    let worker = Worker::start_in(hooks.storage_dir.clone(), &db, nats_url, &capture.env()).await;
    worker.process.wait_ready().await;

    let forged = cufflink_platform::worker_dispatch::WasmJob {
        tenant_slug: "forged-tenant".into(),
        ..hooks.job("say", "{}")
    };
    let answered = async_nats::connect(nats_url)
        .await
        .expect("nats")
        .request(
            "cufflink.wasm.hook-svc.say",
            serde_json::to_vec(&forged).expect("a job").into(),
        )
        .await
        .expect("an answer");
    let answered: serde_json::Value = serde_json::from_slice(&answered.payload).expect("json");
    assert_ne!(answered["ok"], true, "{answered}");

    let refused = capture
        .span("the refused job's span", |span| span.text == "worker/sync")
        .await
        .expect("exported");
    assert_eq!(refused.scope(), []);
    capture
        .log("the refusal", |record| {
            record.text == "Rejected a WASM job whose identity does not match its deployment"
        })
        .await
        .expect("exported");
    let scoped: Vec<_> = capture
        .spans()
        .into_iter()
        .chain(capture.logs())
        .filter(|signal| !signal.scope().is_empty())
        .collect();
    assert_eq!(scoped, []);
}

#[tokio::test]
async fn cf3_a_backup_job_exports_its_scope() {
    let capture = Capture::start().await.expect("capture");
    let Some(backups) = Backups::start_with(
        json!({
            "name": "ledger",
            "mode": "crud",
            "tables": [{
                "name": "entries",
                "columns": [
                    {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                    {"name": "title", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}
                ]
            }],
            "publishes": [],
        }),
        "ledger",
        "CREATE SCHEMA ledger;
         CREATE TABLE ledger.entries (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title TEXT);
         INSERT INTO ledger.entries (title) VALUES ('first');",
        &capture.env(),
    )
    .await
    else {
        return;
    };

    let export = backups.run("export", None).await;
    assert_eq!(export.status, "completed");
    let span = capture
        .span("the backup job's span", |span| span.text == "backup_job")
        .await
        .expect("exported");
    assert_eq!(
        span.scope(),
        [
            ("cufflink.kind", "job"),
            ("cufflink.service", "ledger"),
            ("cufflink.tenant", backups.tenant.slug.as_str()),
        ]
    );
    assert_eq!(span.parent_span_id, "", "a backup job is its own trace");
    assert_eq!(
        span.attribute("job_id"),
        Some(export.id.to_string().as_str())
    );
}
