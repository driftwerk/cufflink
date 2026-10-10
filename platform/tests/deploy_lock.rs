use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::deployer::job::{detached, Job};
use cufflink_platform::errors::{PlatformError, PlatformResult};
use cufflink_platform::repo::deploy_jobs;
use cufflink_platform::repo::deploy_lock::DeployLock;
use cufflink_platform::shutdown::{Shutdown, KILL_MARGIN};
use deadpool_postgres::Pool;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use uuid::Uuid;

const SERVICE: &str = "lock-svc";

const SECOND_MODULE: &str = r#"
(module
  (memory (export "memory") 1)
  (data (i32.const 0) "\07\00\00\00{\"v\":2}")
  (func (export "alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "echo") (param i32 i32) (result i32) (i32.const 0)))
"#;

fn manifest(columns: &[&str]) -> Value {
    let mut manifest = common::wasm_manifest(SERVICE, &[("echo", "/echo")]);
    if let Some(declared) = manifest["tables"][0]["columns"].as_array_mut() {
        declared.extend(columns.iter().map(|name| {
            json!({"name": name, "column_type": "text", "primary_key": false, "nullable": true})
        }));
    }
    manifest
}

fn encoded(module: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(module)
}

fn panics() -> PlatformResult<Value> {
    panic!("the deploy body panicked")
}

async fn eventually<F, Fut>(what: &str, within: Duration, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    while !probe().await {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

struct Deployed {
    _db: TestDb,
    platform: PlatformProcess,
    tenant: Tenant,
    pool: Pool,
    locks: Pool,
    service: Uuid,
}

impl Deployed {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let (status, body) = platform.deploy(&tenant, &manifest(&[])).await;
        assert_eq!(status, 200, "{body}");
        let service = body["service_id"]
            .as_str()
            .and_then(|id| id.parse().ok())
            .expect("service id");
        Some(Self {
            pool: db.pool(),
            locks: db.pool(),
            _db: db,
            platform,
            tenant,
            service,
        })
    }

    async fn scalar<T>(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> T
    where
        T: for<'a> tokio_postgres::types::FromSql<'a>,
    {
        self.pool
            .get()
            .await
            .expect("checkout")
            .query_one(sql, params)
            .await
            .expect("query")
            .get(0)
    }

    async fn holders(&self) -> i64 {
        self.scalar(
            "SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database \
             WHERE l.locktype = 'advisory' AND l.granted AND d.datname = current_database() \
             AND ((l.classid::bigint << 32) | l.objid::bigint) = $1",
            &[&DeployLock::key_for(self.service)],
        )
        .await
    }

    async fn inside(
        &self,
        request: &mut JoinHandle<String>,
        blocking: &deadpool_postgres::Object,
        like: &str,
    ) {
        let blocker: i32 = blocking
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("the blocking session's pid")
            .get(0);
        let deadline = Instant::now() + Duration::from_secs(20);
        while !self
            .scalar::<bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                 WHERE $1 = ANY(pg_blocking_pids(pid)) AND query LIKE $2)",
                &[&blocker, &like],
            )
            .await
        {
            if request.is_finished() {
                panic!(
                    "the request ended before it was cancelled: {}",
                    request.await.expect("the request")
                );
            }
            assert!(
                Instant::now() < deadline,
                "the request was not waiting inside its locked work within 20s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(self.holders().await, 1);
    }

    async fn released(&self) {
        eventually(
            "the deploy lock's release",
            Duration::from_secs(2),
            || async { self.holders().await == 0 },
        )
        .await;
    }

    async fn dropped(&self) -> f64 {
        self.platform
            .metric("cufflink_deploy_lock_dropped_total", &[])
            .await
            .unwrap_or(0.0)
    }

    async fn blocking(&self, table: &str) -> deadpool_postgres::Object {
        let session = self.pool.get().await.expect("checkout");
        session
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
            ))
            .await
            .expect("table lock");
        session
    }

    fn request(&self, path: &str, body: Value) -> JoinHandle<String> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .expect("client");
        let url = format!("{}{path}", self.platform.base_url);
        let authorization = format!("ApiKey {}", self.tenant.api_key);
        tokio::spawn(async move {
            match client
                .post(url)
                .header("authorization", authorization)
                .json(&body)
                .send()
                .await
            {
                Ok(response) => format!(
                    "{} {}",
                    response.status(),
                    response.text().await.unwrap_or_default()
                ),
                Err(e) => e.to_string(),
            }
        })
    }

    async fn deploys(&self, columns: &[&str]) -> (u16, Value) {
        self.platform.deploy(&self.tenant, &manifest(columns)).await
    }

    async fn lock(&self) -> DeployLock {
        DeployLock::try_acquire(&self.locks, self.service)
            .await
            .expect("the lock's session")
            .expect("the lock is free")
    }

    async fn job(&self, shutdown: &Shutdown) -> Job {
        let row = deploy_jobs::create(&self.pool, self.service, "test:1", &json!({}), "tester")
            .await
            .expect("a job");
        Job {
            pool: self.pool.clone(),
            shutdown: shutdown.clone(),
            id: row.id,
        }
    }

    async fn standing(&self, job: Uuid) -> String {
        self.scalar(
            "SELECT status || coalesce(': ' || error, '') FROM deploy_jobs WHERE id = $1",
            &[&job],
        )
        .await
    }
}

#[tokio::test]
async fn a_module_upload_that_is_cancelled_releases_the_deploy_lock() {
    let Some(d) = Deployed::start().await else {
        return;
    };
    let blocking = d.blocking("deployments").await;
    let replacement = wat::parse_str(SECOND_MODULE).expect("wat");
    let mut upload = d.request(
        &format!("/api/services/{}/wasm", d.service),
        json!({"wasm_base64": encoded(&replacement)}),
    );
    d.inside(
        &mut upload,
        &blocking,
        "%WHERE d.service_id = $1 ORDER BY d.version DESC",
    )
    .await;
    upload.abort();
    let _ = upload.await;

    d.released().await;
    assert_eq!(d.dropped().await, 1.0);
    blocking
        .batch_execute("ROLLBACK")
        .await
        .expect("the table is let go");
    let (status, body) = d.deploys(&["colour"]).await;
    assert_eq!(status, 200, "the deploy that follows is not a 409: {body}");
}

#[tokio::test]
async fn a_request_dropped_before_its_deploy_is_spawned_releases_the_deploy_lock() {
    let Some(d) = Deployed::start().await else {
        return;
    };
    let blocking = d.blocking("deploy_jobs").await;
    let mut deploy = d.request(
        "/api/services/deploy?async=true",
        json!({
            "manifest": manifest(&["colour"]),
            "wasm_base64": encoded(&common::guest::module_bytes()),
            "api_version": 1,
        }),
    );
    d.inside(&mut deploy, &blocking, "%INSERT INTO deploy_jobs%")
        .await;
    deploy.abort();
    let _ = deploy.await;

    d.released().await;
    assert_eq!(d.dropped().await, 1.0);
    blocking
        .batch_execute("ROLLBACK")
        .await
        .expect("the table is let go");
    let (status, body) = d.deploys(&["colour"]).await;
    assert_eq!(status, 200, "the deploy that follows is not a 409: {body}");
}

#[tokio::test]
async fn a_deploy_that_panics_releases_the_deploy_lock() {
    let Some(d) = Deployed::start().await else {
        return;
    };
    let shutdown = Shutdown::new(Duration::from_secs(30));

    let lock = d.lock().await;
    let (status, body) = d.deploys(&["colour"]).await;
    assert_eq!(status, 409, "the lock is the one a deploy takes: {body}");
    let ended = detached(&shutdown, "deploy", async move {
        let _held = lock;
        panics()
    })
    .await;
    assert!(
        matches!(&ended, Err(PlatformError::Internal(e)) if e.contains("did not run to its end")),
        "{ended:?}"
    );
    d.released().await;

    let lock = d.lock().await;
    let job = d.job(&shutdown).await;
    let id = job.id;
    job.run(lock, |lock| async move {
        let _held = lock;
        panics()
    })
    .await;
    assert_eq!(
        d.standing(id).await,
        "failed: the deploy panicked: the deploy body panicked"
    );
    d.released().await;
    let (status, body) = d.deploys(&["colour"]).await;
    assert_eq!(status, 200, "the deploy that follows is not a 409: {body}");
}

#[tokio::test]
async fn a_deploy_the_hard_deadline_ends_releases_the_deploy_lock() {
    let Some(d) = Deployed::start().await else {
        return;
    };
    let shutdown = Shutdown::new(KILL_MARGIN + Duration::from_millis(500));
    let lock = d.lock().await;
    let job = d.job(&shutdown).await;
    let id = job.id;
    let running = tokio::spawn(job.run(lock, |lock| async move {
        let _held = lock;
        std::future::pending::<PlatformResult<Value>>().await
    }));
    eventually("the deploy running", Duration::from_secs(10), || async {
        d.standing(id).await == "running"
    })
    .await;
    assert_eq!(d.holders().await, 1);

    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("the deploy ends at the hard deadline")
        .expect("the task");
    assert_eq!(d.standing(id).await, "failed: interrupted by shutdown");
    d.released().await;
    let (status, body) = d.deploys(&["colour"]).await;
    assert_eq!(status, 200, "the deploy that follows is not a 409: {body}");
}

#[tokio::test]
async fn a_task_that_is_aborted_releases_the_deploy_lock() {
    let Some(d) = Deployed::start().await else {
        return;
    };
    let lock = d.lock().await;
    let holding = tokio::spawn(async move {
        let _held = lock;
        std::future::pending::<()>().await
    });
    assert_eq!(d.holders().await, 1);
    assert!(
        DeployLock::try_acquire(&d.pool, d.service)
            .await
            .expect("a session")
            .is_none(),
        "the lock is held"
    );
    holding.abort();
    let _ = holding.await;
    d.released().await;
    d.lock().await.release().await;
    assert_eq!(d.holders().await, 0, "a lock that is released is free");
}
