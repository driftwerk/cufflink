use cufflink_testkit as common;

use common::{legacy, Deployment, PlatformProcess};
use cufflink_platform::repo::deploy_jobs;
use cufflink_types::SqlMigration;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn migration(version: u32, name: &str, sql: &str) -> SqlMigration {
    SqlMigration {
        version,
        name: name.into(),
        sql: sql.into(),
        no_transaction: false,
    }
}

fn loans(columns: &[&str], migrations: &[SqlMigration]) -> Value {
    service("loan-service", columns, migrations)
}

fn service(name: &str, columns: &[&str], migrations: &[SqlMigration]) -> Value {
    let id = json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true});
    let declared: Vec<Value> = std::iter::once(id)
        .chain(columns.iter().map(
            |c| json!({"name": c, "column_type": "text", "primary_key": false, "nullable": true}),
        ))
        .collect();
    json!({
        "name": name, "version": "0.1.0", "mode": "crud", "publishes": [],
        "tables": [{"name": "loan_tags", "columns": declared}],
        "migrations": migrations,
        "migration_history": {"table": "schema_history"},
    })
}

struct Deploys(Deployment);

impl std::ops::Deref for Deploys {
    type Target = Deployment;

    fn deref(&self) -> &Deployment {
        &self.0
    }
}

impl std::ops::DerefMut for Deploys {
    fn deref_mut(&mut self) -> &mut Deployment {
        &mut self.0
    }
}

impl Deploys {
    async fn start() -> Option<Self> {
        Self::start_with(&[]).await
    }

    async fn start_with(settings: &[(&str, &str)]) -> Option<Self> {
        let d = Deployment::start(settings).await?;
        legacy::apply(&d.database, &[legacy::BOOTSTRAP]).await;
        Some(Self(d))
    }

    async fn send(
        &self,
        platform: &PlatformProcess,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        platform
            .send(
                method,
                path,
                &format!("ApiKey {}", self.tenant.api_key),
                body.as_ref(),
            )
            .await
    }

    async fn deploy(&self, query: &str, manifest: Value) -> (u16, Value) {
        self.send(
            &self.platform,
            Method::POST,
            &format!("/api/services/deploy{query}"),
            Some(json!({"manifest": manifest})),
        )
        .await
    }

    async fn status(&self, platform: &PlatformProcess, id: &str) -> (u16, Value) {
        self.send(platform, Method::GET, &format!("/api/deploys/{id}"), None)
            .await
    }

    async fn ended(&self, platform: &PlatformProcess, id: &str, within: Duration) -> Value {
        let deadline = Instant::now() + within;
        loop {
            let (status, job) = self.status(platform, id).await;
            assert_eq!(status, 200, "{job}");
            if matches!(job["status"].as_str(), Some("succeeded" | "failed")) {
                return job;
            }
            assert!(Instant::now() < deadline, "{job}");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn ended_as(&self, id: &str, status: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let (code, job) = self.status(&self.platform, id).await;
            assert_eq!(code, 200, "{job}");
            if job["status"] == status {
                return job;
            }
            assert!(Instant::now() < deadline, "{job}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn running(&self, id: &str, phase: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let (status, job) = self.status(&self.platform, id).await;
            assert_eq!(status, 200, "{job}");
            if job["status"] == "running" && job["progress"]["phase"] == phase {
                return job;
            }
            assert!(Instant::now() < deadline, "{job}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn scalar<T>(&self, sql: &str) -> T
    where
        T: for<'a> tokio_postgres::types::FromSql<'a>,
    {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(sql, &[])
            .await
            .expect("query")
            .get(0)
    }

    async fn tenant_scalar<T>(&self, sql: &str) -> T
    where
        T: for<'a> tokio_postgres::types::FromSql<'a>,
    {
        self.database
            .query_one(sql, &[])
            .await
            .expect("query")
            .get(0)
    }

    async fn sessions(&self, counted: &str, sql: &str) -> i64 {
        self.database
            .query_one(
                &format!(
                    "SELECT count({counted}) FROM pg_stat_activity \
                     WHERE datname = current_database() AND pid <> pg_backend_pid() \
                     AND state = 'active' AND query LIKE '%' || $1 || '%'"
                ),
                &[&sql],
            )
            .await
            .expect("sessions")
            .get(0)
    }

    async fn runs(&self, sql: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while self.sessions("*", sql).await == 0 {
            assert!(Instant::now() < deadline, "{sql} never ran");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn end_statements(&self, sql: &str) -> i64 {
        self.sessions("pg_terminate_backend(pid)", sql).await
    }
}

#[tokio::test]
async fn a_plan_says_what_a_deploy_would_do_and_does_nothing() {
    let Some(d) = Deploys::start().await else {
        return;
    };
    legacy::record_ddl(&d.database).await;
    let local = legacy::loan_migrations();

    let (status, body) = d.deploy("?plan=true", loans(&["tag"], &local)).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["new"], true, "{body}");
    assert_eq!(body["version"], 0, "{body}");
    assert_eq!(body["service_id"], Value::Null, "{body}");
    assert_eq!(
        body["plan"]["schema_changes"],
        json!(["CREATE TABLE IF NOT EXISTS \"loan_service\".\"loan_tags\" (\"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid(), \"tag\" TEXT)"]),
        "{body}"
    );
    let migrations = &body["plan"]["sql_migrations"];
    assert_eq!(
        migrations["history"], "loan_service.schema_history",
        "{body}"
    );
    assert_eq!(
        migrations["pending"].as_array().map(|pending| pending
            .iter()
            .filter_map(|m| m["version"].as_u64())
            .collect::<Vec<_>>()),
        Some(vec![1, 2, 3]),
        "{body}"
    );
    assert_eq!(migrations["applied"], json!([]), "{body}");
    let services: i64 = d.scalar("SELECT count(*) FROM services").await;
    assert_eq!(services, 0, "a plan leaves no service behind");
    assert_eq!(
        legacy::recorded_ddl(&d.database, "loan_service").await,
        Vec::<String>::new(),
        "a plan makes nothing, the history it would verify included"
    );

    let (status, body) = d
        .deploy("?plan=true", service("tag-service", &["tag"], &local))
        .await;
    assert_eq!(status, 200, "a plan of a schema that is not there: {body}");
    assert_eq!(
        body["plan"]["sql_migrations"]["history"], "tag_service.schema_history",
        "{body}"
    );
    assert_eq!(
        body["plan"]["sql_migrations"]["pending"]
            .as_array()
            .map(Vec::len),
        Some(3),
        "{body}"
    );
    let schemas: i64 = d
        .tenant_scalar("SELECT count(*) FROM pg_namespace WHERE nspname = 'tag_service'")
        .await;
    assert_eq!(schemas, 0, "a plan makes no schema");

    let (status, deployed) = d.deploy("", loans(&["tag"], &local[..2])).await;
    assert_eq!(status, 200, "{deployed}");
    let recorded = legacy::recorded_ddl(&d.database, "loan_service").await;
    assert!(!recorded.is_empty(), "a deploy makes what it declares");

    let (status, body) = d
        .deploy("?plan=true", loans(&["tag", "colour"], &local))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["new"], false, "{body}");
    assert_eq!(body["version"], deployed["version"], "{body}");
    assert_eq!(
        body["plan"]["schema_changes"],
        json!([
            "ALTER TABLE \"loan_service\".\"loan_tags\" ADD COLUMN IF NOT EXISTS \"colour\" TEXT"
        ]),
        "{body}"
    );
    assert_eq!(body["plan"]["sql_migrations"]["verified"], 2, "{body}");
    assert_eq!(
        body["plan"]["sql_migrations"]["pending"][0]["version"], 3,
        "{body}"
    );
    assert_eq!(
        legacy::recorded_ddl(&d.database, "loan_service").await,
        recorded,
        "a plan runs no DDL"
    );
    let versions: i64 = d.scalar("SELECT count(*) FROM deployments").await;
    assert_eq!(versions, 1, "a plan activates nothing");
    let history: i64 = d
        .tenant_scalar("SELECT count(*) FROM loan_service.schema_history")
        .await;
    assert_eq!(history, 2, "a plan applies no migration");

    let (status, body) = d.deploy("?plan=true", loans(&["tag"], &local[..2])).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["skipped"], true, "{body}");

    let (status, body) = d.deploy("?plan=true", loans(&[], &local[..2])).await;
    assert_eq!(
        status, 422,
        "a plan the deploy would be refused for: {body}"
    );
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("Destructive changes")),
        "{body}"
    );
}

#[tokio::test]
async fn async_deploy_progress_and_replica_loss() {
    let Some(mut d) = Deploys::start().await else {
        return;
    };
    let slow = [migration(
        1,
        "slow",
        "CREATE TABLE loan_service.first (id int); SELECT pg_sleep(3);\n",
    )];
    let (status, accepted) = d.deploy("?async=true", loans(&["tag"], &slow)).await;
    assert_eq!(status, 202, "{accepted}");
    assert_eq!(accepted["status"], "queued", "{accepted}");
    let id = accepted["deploy_id"]
        .as_str()
        .expect("deploy_id")
        .to_string();

    let running = d.running(&id, "additive").await;
    assert_eq!(running["result"], Value::Null, "{running}");
    for query in ["?async=true", ""] {
        let (status, refused) = d.deploy(query, loans(&["tag"], &slow)).await;
        assert_eq!(status, 409, "one deploy of a service at a time: {refused}");
    }
    let jobs: i64 = d.scalar("SELECT count(*) FROM deploy_jobs").await;
    assert_eq!(jobs, 1, "a deploy that is refused is not followed");

    let job = d.ended(&d.platform, &id, Duration::from_secs(60)).await;
    assert_eq!(job["status"], "succeeded", "{job}");
    assert_eq!(job["progress"]["phase"], "activated", "{job}");
    let result = &job["result"];
    assert_eq!(job.get("plan"), None, "the plan is the result's: {job}");
    assert_eq!(result["name"], "loan-service", "{job}");
    assert_eq!(
        result["plan"]["sql_migrations"]["applied"][0]["version"], 1,
        "{job}"
    );
    assert_eq!(job["request"]["service"], "loan-service", "{job}");
    assert!(job["finished_at"].is_string(), "{job}");
    let (status, health) = d
        .platform
        .get_json(&format!("/svc/{}/loan-service/_meta/health", d.tenant.slug))
        .await
        .expect("health");
    assert_eq!(status, 200, "{health}");
    assert_eq!(health["version"], result["version"], "{health}");

    let other = d.db.mint_tenant().await;
    let (status, _) = d
        .platform
        .send(
            Method::GET,
            &format!("/api/deploys/{id}"),
            &format!("ApiKey {}", other.api_key),
            None,
        )
        .await;
    assert_eq!(status, 404, "another tenant's deploy is not there");

    let stuck = [
        slow[0].clone(),
        migration(2, "stuck", "SELECT pg_sleep(120);\n"),
    ];
    let (status, accepted) = d.deploy("?async=true", loans(&["tag"], &stuck)).await;
    assert_eq!(status, 202, "{accepted}");
    let lost = accepted["deploy_id"]
        .as_str()
        .expect("deploy_id")
        .to_string();
    d.running(&lost, "additive").await;
    d.runs("pg_sleep(120)").await;

    d.platform.kill();
    assert_eq!(d.end_statements("pg_sleep(120)").await, 1);
    d.db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE deploy_jobs SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1",
            &[&uuid::Uuid::parse_str(&lost).expect("uuid")],
        )
        .await
        .expect("a heartbeat as stale as the sweep would find it after a minute");
    let infra = common::infra().await.expect("infra");
    let second = PlatformProcess::start(&d.db.url, infra.nats_url(), &[]).await;
    second.wait_ready().await;
    let job = d.ended(&second, &lost, Duration::from_secs(90)).await;
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "replica lost", "{job}");
    let counted = Instant::now() + Duration::from_secs(10);
    while !second
        .metric("cufflink_deploy_jobs", &[("status", "failed")])
        .await
        .is_some_and(|failed| failed >= 1.0)
    {
        assert!(
            Instant::now() < counted,
            "the sweep that ended the deploy counts it"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (status, body) = d
        .send(
            &second,
            Method::POST,
            "/api/services/deploy",
            Some(json!({"manifest": loans(&["tag", "colour"], &slow)})),
        )
        .await;
    assert_eq!(status, 200, "the lock went with the lost replica: {body}");
    let (status, health) = second
        .get_json(&format!("/svc/{}/loan-service/_meta/health", d.tenant.slug))
        .await
        .expect("health");
    assert_eq!(status, 200, "{health}");
    assert_eq!(health["version"], body["version"], "{health}");
}

#[tokio::test]
async fn a_deploy_that_fails_beyond_its_request_says_why() {
    let Some(d) = Deploys::start().await else {
        return;
    };
    let broken = [migration(
        1,
        "broken",
        "ALTER TABLE loan_service.no_such_table ADD COLUMN x int;\n",
    )];
    let (status, accepted) = d.deploy("?async=true", loans(&["tag"], &broken)).await;
    assert_eq!(status, 202, "{accepted}");
    let id = accepted["deploy_id"]
        .as_str()
        .expect("deploy_id")
        .to_string();
    let job = d.ended(&d.platform, &id, Duration::from_secs(60)).await;
    assert_eq!(job["status"], "failed", "{job}");
    assert!(
        job["error"]
            .as_str()
            .is_some_and(|e| e.contains("V1__broken")),
        "{job}"
    );
    assert_eq!(job["result"], Value::Null, "{job}");
    let (status, _) = d
        .platform
        .get_json(&format!("/svc/{}/loan-service/_meta/health", d.tenant.slug))
        .await
        .expect("health");
    assert_ne!(status, 200, "nothing was activated");
    let (status, body) = d.status(&d.platform, "not-a-uuid").await;
    assert_eq!(status, 400, "{body}");
}

#[cfg(unix)]
#[tokio::test]
async fn a_deploy_the_shutdown_cuts_short_is_recorded_as_failed() {
    let Some(mut d) = Deploys::start_with(&[("SHUTDOWN_GRACE_SECS", "6")]).await else {
        return;
    };
    let stuck = [migration(1, "stuck", "SELECT pg_sleep(90);\n")];
    let (status, accepted) = d.deploy("?async=true", loans(&["tag"], &stuck)).await;
    assert_eq!(status, 202, "{accepted}");
    d.running(
        accepted["deploy_id"].as_str().expect("deploy_id"),
        "additive",
    )
    .await;
    d.runs("pg_sleep(90)").await;

    d.platform.sigterm();
    assert!(
        d.platform
            .wait_exit(Duration::from_secs(30))
            .await
            .is_some(),
        "the platform stops within its grace"
    );
    assert_eq!(d.end_statements("pg_sleep(90)").await, 1);
    let ending: String = d
        .scalar("SELECT status || ': ' || error FROM deploy_jobs")
        .await;
    assert_eq!(ending, "failed: interrupted by shutdown");
    let versions: i64 = d.scalar("SELECT count(*) FROM deployments").await;
    assert_eq!(versions, 0, "nothing was activated");
}

#[tokio::test]
async fn a_deploy_taken_for_lost_while_it_runs_records_how_it_ended() {
    let Some(d) = Deploys::start().await else {
        return;
    };
    let slow = [migration(1, "slow", "SELECT pg_sleep(3);\n")];
    let (status, accepted) = d.deploy("?async=true", loans(&["tag"], &slow)).await;
    assert_eq!(status, 202, "{accepted}");
    let id = accepted["deploy_id"]
        .as_str()
        .expect("deploy_id")
        .to_string();
    d.running(&id, "additive").await;

    let swept = deploy_jobs::sweep_lost(&d.db.pool(), Duration::ZERO)
        .await
        .expect("sweep");
    assert_eq!(swept, [uuid::Uuid::parse_str(&id).expect("uuid")]);
    let (_, job) = d.status(&d.platform, &id).await;
    assert_eq!(job["error"], deploy_jobs::LOST, "{job}");

    let job = d.ended_as(&id, "succeeded").await;
    assert_eq!(job["error"], Value::Null, "{job}");
    assert_eq!(job["progress"]["phase"], "activated", "{job}");
    assert_eq!(job["result"]["name"], "loan-service", "{job}");
}

#[tokio::test]
async fn a_verdict_of_loss_is_replaced_only_by_the_replica_it_judged_and_ended_deploys_are_pruned()
{
    let Some(d) = Deploys::start().await else {
        return;
    };
    let body = d.deploy("", loans(&["tag"], &[])).await.1;
    let service =
        uuid::Uuid::parse_str(body["service_id"].as_str().expect("service_id")).expect("uuid");
    let pool = d.db.pool();
    let job = |created: deploy_jobs::DeployJob| created.id;
    let ran = job(
        deploy_jobs::create(&pool, service, "replica:1", &json!({}), "test")
            .await
            .expect("job"),
    );
    let first = deploy_jobs::start(&pool, ran).await.expect("started");
    let last = deploy_jobs::heartbeat(&pool, ran)
        .await
        .expect("heartbeat")
        .expect("running");
    assert!(last > first);
    assert_eq!(
        deploy_jobs::sweep_lost(&pool, Duration::ZERO)
            .await
            .expect("sweep"),
        [ran]
    );
    assert_eq!(
        deploy_jobs::heartbeat(&pool, ran).await.expect("heartbeat"),
        None,
        "a deploy that ended beats no more"
    );
    deploy_jobs::progress(&pool, ran, &json!({"phase": "destructive"}))
        .await
        .expect("progress");
    let succeeded = deploy_jobs::Ending::Succeeded(json!({"version": 2}));
    for other in [Some(first), None] {
        assert_eq!(
            deploy_jobs::finish(&pool, ran, &succeeded, other)
                .await
                .expect("finish"),
            deploy_jobs::Finished::Kept,
            "{other:?}: a verdict made on another heartbeat stands"
        );
    }
    assert_eq!(
        deploy_jobs::finish(&pool, ran, &succeeded, Some(last))
            .await
            .expect("finish"),
        deploy_jobs::Finished::Corrected
    );
    let (status, error, progress): (String, Option<String>, Value) = {
        let row = pool
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT status, error, progress FROM deploy_jobs WHERE id = $1",
                &[&ran],
            )
            .await
            .expect("row");
        (row.get(0), row.get(1), row.get(2))
    };
    assert_eq!((status.as_str(), error), ("succeeded", None));
    assert_eq!(
        progress,
        json!({"phase": "destructive"}),
        "a deploy taken for lost records how far it goes on"
    );
    deploy_jobs::progress(&pool, ran, &json!({"phase": "activated"}))
        .await
        .expect("progress");
    assert_eq!(
        deploy_jobs::finish(&pool, ran, &succeeded, Some(last))
            .await
            .expect("finish"),
        deploy_jobs::Finished::Kept,
        "a deploy ends once"
    );
    let progress: Value = pool
        .get()
        .await
        .expect("checkout")
        .query_one("SELECT progress FROM deploy_jobs WHERE id = $1", &[&ran])
        .await
        .expect("row")
        .get(0);
    assert_eq!(
        progress,
        json!({"phase": "destructive"}),
        "a deploy that ended records no progress"
    );

    let recent = job(
        deploy_jobs::create(&pool, service, "replica:1", &json!({}), "test")
            .await
            .expect("job"),
    );
    let failed = deploy_jobs::Ending::Failed("broken".into());
    assert_eq!(
        deploy_jobs::finish(&pool, recent, &failed, None)
            .await
            .expect("finish"),
        deploy_jobs::Finished::Recorded
    );
    let running = job(
        deploy_jobs::create(&pool, service, "replica:1", &json!({}), "test")
            .await
            .expect("job"),
    );
    pool.get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE deploy_jobs SET created_at = now() - interval '40 days', \
             finished_at = CASE WHEN id = $1 THEN now() - interval '31 days' END \
             WHERE id IN ($1, $2)",
            &[&ran, &running],
        )
        .await
        .expect("an old deploy");
    let retention = Duration::from_secs(30 * 24 * 60 * 60);
    assert_eq!(
        deploy_jobs::prune(&pool, retention).await.expect("prune"),
        1
    );
    let left: Vec<uuid::Uuid> = pool
        .get()
        .await
        .expect("checkout")
        .query("SELECT id FROM deploy_jobs ORDER BY created_at", &[])
        .await
        .expect("jobs")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        left,
        [running, recent],
        "a deploy that has not ended, and one that ended lately, are kept"
    );
}
