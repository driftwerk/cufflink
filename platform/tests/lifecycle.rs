use cufflink_testkit as common;

use common::jetstream::acknowledged;
use common::PlatformProcess;
use std::time::{Duration, Instant};

#[tokio::test]
async fn ready_transitions() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;

    let black_hole = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind black hole");
    let unreachable_url = format!(
        "postgres://cufflink:cufflink@127.0.0.1:{}/cufflink",
        black_hole.local_addr().expect("addr").port()
    );
    let stuck = PlatformProcess::start(&unreachable_url, None, &[]).await;
    assert_eq!(stuck.status("/health").await, Some(200));
    let (status, body) = stuck.get_json("/ready").await.expect("/ready answers");
    assert_eq!(status, 503);
    assert_eq!(body["state"], "starting");
    assert_eq!(body["db"], "error");
    drop(stuck);
    drop(black_hole);

    db.pool()
        .get()
        .await
        .expect("checkout")
        .batch_execute(include_str!("../migrations/V1__baseline.sql"))
        .await
        .expect("legacy DDL");

    let platform = PlatformProcess::start(&db.url, None, &[]).await;
    let body = platform.wait_ready().await;
    assert_eq!(body["modules"]["pending"], 0);
    assert!(body["registry_version"].as_u64().unwrap_or(0) >= 1);
    drop(platform);
    assert_eq!(applied_migrations(&db).await, all_migrations());

    let again = PlatformProcess::start(&db.url, None, &[]).await;
    again.wait_ready().await;
    assert_eq!(applied_migrations(&db).await, all_migrations());
    let constraints: i64 = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM pg_constraint WHERE conname = 'deployments_wasm_hash_required'",
            &[],
        )
        .await
        .expect("pg_constraint")
        .get(0);
    assert_eq!(constraints, 1);
    drop(again);

    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO refinery_schema_history (version, name, applied_on, checksum)
             SELECT 99, 'from_a_newer_binary', applied_on, checksum
             FROM refinery_schema_history WHERE version = 2",
            &[],
        )
        .await
        .expect("record a newer migration");
    PlatformProcess::start(&db.url, None, &[])
        .await
        .wait_ready()
        .await;
}

#[tokio::test]
async fn r2_migration_is_additive_and_reapplies_cleanly() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = db.pool().get().await.expect("checkout");
    client
        .batch_execute(concat!(
            include_str!("../migrations/V1__baseline.sql"),
            include_str!("../migrations/V2__r1a_policy_and_isolation.sql"),
        ))
        .await
        .expect("R1 schema");
    client
        .batch_execute(
            "INSERT INTO tenants (id, keycloak_user_id, name, slug)
             VALUES ('00000000-0000-0000-0000-00000000000a', 'kc', 'acme', 'acme');
             INSERT INTO api_keys (tenant_id, key_hash, name, is_platform_admin, expires_at) VALUES
               ('00000000-0000-0000-0000-00000000000a', 'admin', 'admin', true, NULL),
               ('00000000-0000-0000-0000-00000000000a', 'admin-dated', 'admin-dated', true,
                '2030-01-01T00:00:00Z'),
               ('00000000-0000-0000-0000-00000000000a', 'tenant', 'tenant', false, NULL);",
        )
        .await
        .expect("R1 rows");

    let expiries = || async {
        client
            .query(
                "SELECT key_hash, expires_at::text FROM api_keys ORDER BY key_hash",
                &[],
            )
            .await
            .expect("api keys")
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, Option<String>>(1)))
            .collect::<Vec<_>>()
    };
    let before = expiries().await;
    assert_eq!(
        before.iter().filter(|(_, expiry)| expiry.is_none()).count(),
        2,
        "{before:?}"
    );
    for _ in 0..2 {
        client
            .batch_execute(include_str!("../migrations/V3__r2.sql"))
            .await
            .expect("V3 applies");
        assert_eq!(expiries().await, before, "V3 never dates an API key");
    }

    let columns: Vec<String> = client
        .query(
            "SELECT table_name || '.' || column_name FROM information_schema.columns
             WHERE table_schema = 'public'
               AND (table_name IN ('partner_keys', 'module_artifacts')
                    OR (table_name = 'api_keys' AND column_name = 'last_used_at'))
             ORDER BY 1",
            &[],
        )
        .await
        .expect("columns")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        columns,
        [
            "api_keys.last_used_at",
            "module_artifacts.created_at",
            "module_artifacts.engine_fingerprint",
            "module_artifacts.service_id",
            "module_artifacts.sha256",
            "module_artifacts.size_bytes",
            "module_artifacts.version",
            "module_artifacts.wasm_sha256",
            "partner_keys.created_at",
            "partner_keys.created_by",
            "partner_keys.expires_at",
            "partner_keys.id",
            "partner_keys.key_prefix",
            "partner_keys.last_used_at",
            "partner_keys.name",
            "partner_keys.revoked_at",
            "partner_keys.scopes",
            "partner_keys.secret_hash",
            "partner_keys.tenant_id",
        ]
    );
}

#[tokio::test]
async fn v4_applies_twice_over_an_r2_database_and_keeps_its_rows() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = db.pool().get().await.expect("checkout");
    client
        .batch_execute(concat!(
            include_str!("../migrations/V1__baseline.sql"),
            include_str!("../migrations/V2__r1a_policy_and_isolation.sql"),
            include_str!("../migrations/V3__r2.sql"),
        ))
        .await
        .expect("R2 schema");
    client
        .batch_execute(
            "INSERT INTO tenants (id, keycloak_user_id, name, slug)
             VALUES ('00000000-0000-0000-0000-00000000000a', 'kc', 'acme', 'acme');
             INSERT INTO services (id, tenant_id, name) VALUES
               ('00000000-0000-0000-0000-0000000000a1',
                '00000000-0000-0000-0000-00000000000a', 'loan-service'),
               ('00000000-0000-0000-0000-0000000000a2',
                '00000000-0000-0000-0000-00000000000a', 'catalog-service');
             INSERT INTO wasm_jobs (tenant_id, tenant_slug, service_id, service_slug,
                                    service_version, handler, status, request_json,
                                    timeout_secs, expires_at)
             SELECT '00000000-0000-0000-0000-00000000000a', 'acme',
                    '00000000-0000-0000-0000-0000000000a1', 'loan-service', 1, status, status,
                    '{}'::jsonb, 60, now() + interval '1 day'
             FROM unnest(ARRAY['pending', 'running', 'completed', 'failed']) AS status;",
        )
        .await
        .expect("R2 rows");

    for _ in 0..2 {
        client
            .batch_execute(include_str!("../migrations/V4__r3.sql"))
            .await
            .expect("V4 applies");
        cufflink_platform::repo::migrations::run_online(&client)
            .await
            .expect("what V4 leaves to be done online");
    }
    let indexes: Vec<(String, bool)> = client
        .query(
            "SELECT c.relname::text, i.indisvalid FROM pg_index i
             JOIN pg_class c ON c.oid = i.indexrelid
             WHERE c.relname IN ('wasm_jobs_idempotency_idx', 'wasm_jobs_due_idx',
                                 'wasm_jobs_lease_idx', 'wasm_jobs_schedule_idx')
             ORDER BY 1",
            &[],
        )
        .await
        .expect("indexes")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        indexes,
        [
            ("wasm_jobs_due_idx".to_string(), true),
            ("wasm_jobs_idempotency_idx".to_string(), true),
            ("wasm_jobs_lease_idx".to_string(), true),
            ("wasm_jobs_schedule_idx".to_string(), true),
        ]
    );

    let published: Vec<(String, bool)> = client
        .query(
            "SELECT status, published_at IS NOT NULL FROM wasm_jobs ORDER BY status",
            &[],
        )
        .await
        .expect("jobs")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        published,
        [
            ("completed".to_string(), true),
            ("failed".to_string(), true),
            ("pending".to_string(), true),
            ("running".to_string(), true),
        ]
    );
    let enqueued_since: bool = client
        .query_one(
            "INSERT INTO wasm_jobs (tenant_id, tenant_slug, service_id, service_slug,
                                    service_version, handler, status, request_json,
                                    timeout_secs, expires_at)
             VALUES ('00000000-0000-0000-0000-00000000000a', 'acme',
                     '00000000-0000-0000-0000-0000000000a1', 'loan-service', 1, 'later',
                     'pending', '{}'::jsonb, 60, now() + interval '1 day')
             RETURNING published_at IS NULL",
            &[],
        )
        .await
        .expect("a job enqueued after V4")
        .get(0);
    assert!(
        enqueued_since,
        "a job enqueued since is published when it is"
    );
    let same_schema = client
        .execute(
            "INSERT INTO services (tenant_id, name, schema_name)
             VALUES ('00000000-0000-0000-0000-00000000000a', 'loans', 'loan_service')",
            &[],
        )
        .await
        .expect_err("two services of a tenant cannot share a schema");
    assert_eq!(
        same_schema.as_db_error().and_then(|e| e.constraint()),
        Some("services_schema_idx")
    );
    let tables: i64 = client
        .query_one(
            "SELECT count(*) FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name IN (
               'tenant_nats_claims', 'tenant_streams', 'consumer_handovers',
               'outbox_relay_leases', 'schedule_state', 'webhook_deliveries', 'deploy_jobs',
               'service_db_roles', 'tenant_data_keys')",
            &[],
        )
        .await
        .expect("tables")
        .get(0);
    assert_eq!(tables, 9);
}

#[tokio::test]
async fn v5_applies_twice_over_an_r3_database_and_holds_registrations_to_their_backend() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = db.pool().get().await.expect("checkout");
    client
        .batch_execute(concat!(
            include_str!("../migrations/V1__baseline.sql"),
            include_str!("../migrations/V2__r1a_policy_and_isolation.sql"),
            include_str!("../migrations/V3__r2.sql"),
            include_str!("../migrations/V4__r3.sql"),
        ))
        .await
        .expect("R3 schema");
    client
        .batch_execute(
            "INSERT INTO tenants (id, keycloak_user_id, name, slug)
             VALUES ('00000000-0000-0000-0000-00000000000a', 'kc', 'acme', 'acme');
             INSERT INTO services (id, tenant_id, name) VALUES
               ('00000000-0000-0000-0000-0000000000a1',
                '00000000-0000-0000-0000-00000000000a', 'ext-google');",
        )
        .await
        .expect("R3 rows");
    for _ in 0..2 {
        client
            .batch_execute(include_str!("../migrations/V5__extensions.sql"))
            .await
            .expect("V5 applies");
    }
    let register = |name: &str, backend: &str, service: Option<&str>, callers: &str| {
        format!(
            "INSERT INTO tenant_extensions (tenant_id, name, backend, service_id, callers, created_by)
             VALUES ('00000000-0000-0000-0000-00000000000a', '{name}', '{backend}', {}, '{callers}',
                     'test')",
            service.map_or("NULL".to_string(), |id| format!("'{id}'"))
        )
    };
    let service = Some("00000000-0000-0000-0000-0000000000a1");
    client
        .batch_execute(&register("google.token", "native", service, "{reports}"))
        .await
        .expect("a native service's extension");
    client
        .batch_execute(&register("pdf.render", "nats", None, "{*}"))
        .await
        .expect("a NATS provider");
    for refused in [
        register("a.native", "native", None, "{*}"),
        register("a.provider", "nats", service, "{*}"),
        register("a.callerless", "nats", None, "{}"),
        register("a.grpc", "grpc", None, "{*}"),
        register("pdf.render", "nats", None, "{*}"),
    ] {
        assert!(client.batch_execute(&refused).await.is_err(), "{refused}");
    }
    let names = || async {
        client
            .query("SELECT name FROM tenant_extensions ORDER BY name", &[])
            .await
            .expect("registrations")
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
    };
    assert_eq!(names().await, ["google.token", "pdf.render"]);
    client
        .batch_execute("DELETE FROM services WHERE id = '00000000-0000-0000-0000-0000000000a1'")
        .await
        .expect("delete the service");
    assert_eq!(
        names().await,
        ["pdf.render"],
        "a service takes its own along"
    );
    client
        .batch_execute("DELETE FROM tenants WHERE id = '00000000-0000-0000-0000-00000000000a'")
        .await
        .expect("delete the tenant");
    assert!(names().await.is_empty(), "a tenant takes its own along");
}

#[tokio::test]
async fn v6_applies_twice_and_runner_keys_go_with_their_tenant() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = db.pool().get().await.expect("checkout");
    client
        .batch_execute(concat!(
            include_str!("../migrations/V1__baseline.sql"),
            include_str!("../migrations/V2__r1a_policy_and_isolation.sql"),
            include_str!("../migrations/V3__r2.sql"),
            include_str!("../migrations/V4__r3.sql"),
            include_str!("../migrations/V5__extensions.sql"),
        ))
        .await
        .expect("V1 to V5");
    for _ in 0..2 {
        client
            .batch_execute(include_str!("../migrations/V6__runner_keys.sql"))
            .await
            .expect("V6 applies");
    }
    let key = |prefix: &str| {
        format!(
            "INSERT INTO runner_keys (tenant_id, name, runs, key_prefix, secret_hash, created_by)
             VALUES ('00000000-0000-0000-0000-00000000000a', 'runner', 'native', '{prefix}', 'h', 'test')"
        )
    };
    client
        .batch_execute(
            "INSERT INTO tenants (id, keycloak_user_id, name, slug)
             VALUES ('00000000-0000-0000-0000-00000000000a', 'kc', 'acme', 'acme')",
        )
        .await
        .expect("a tenant");
    client
        .batch_execute(&key("AAAAAAAAAAAA"))
        .await
        .expect("a runner key");
    assert!(
        client.batch_execute(&key("AAAAAAAAAAAA")).await.is_err(),
        "a prefix names one key"
    );
    assert!(
        client
            .batch_execute(&key("BBBBBBBBBBBB").replace("'native'", "'wasm'"))
            .await
            .is_err(),
        "a key runs native services or web apps"
    );
    client
        .batch_execute("DELETE FROM tenants WHERE id = '00000000-0000-0000-0000-00000000000a'")
        .await
        .expect("delete the tenant");
    let left: i64 = client
        .query_one("SELECT count(*) FROM runner_keys", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(left, 0, "a tenant takes its runner keys along");
}

#[tokio::test]
async fn a_runner_of_an_earlier_release_runs_against_a_database_it_does_not_know() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_migrated_database().await;
    assert_eq!(applied_migrations(&db).await, all_migrations());
    let mut client = cufflink_db::connect_single(
        cufflink_db::parse_config(&db.url).expect("url"),
        &cufflink_db::PoolSettings::from_vars(1, |_| None).expect("defaults"),
    )
    .await
    .expect("connect");
    let migrations = [
        (
            "V1__baseline",
            include_str!("../migrations/V1__baseline.sql"),
        ),
        (
            "V2__r1a_policy_and_isolation",
            include_str!("../migrations/V2__r1a_policy_and_isolation.sql"),
        ),
        ("V3__r2", include_str!("../migrations/V3__r2.sql")),
        ("V4__r3", include_str!("../migrations/V4__r3.sql")),
    ];
    for known in [3, 4] {
        let earlier: Vec<refinery::Migration> = migrations[..known]
            .iter()
            .map(|(name, sql)| refinery::Migration::unapplied(name, sql).expect("migration"))
            .collect();
        let report = refinery::Runner::new(&earlier)
            .set_grouped(false)
            .set_abort_missing(false)
            .run_async(&mut client)
            .await
            .expect("the earlier release's runner accepts the later history");
        assert!(report.applied_migrations().is_empty(), "up to V{known}");
        assert_eq!(applied_migrations(&db).await, all_migrations());
    }
}

#[tokio::test]
async fn v4_applies_where_two_services_already_share_a_schema() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = db.pool().get().await.expect("checkout");
    client
        .batch_execute(concat!(
            include_str!("../migrations/V1__baseline.sql"),
            include_str!("../migrations/V2__r1a_policy_and_isolation.sql"),
            include_str!("../migrations/V3__r2.sql"),
        ))
        .await
        .expect("R2 schema");
    client
        .batch_execute(
            "INSERT INTO tenants (id, keycloak_user_id, name, slug)
             VALUES ('00000000-0000-0000-0000-00000000000a', 'kc', 'acme', 'acme');
             INSERT INTO services (tenant_id, name) VALUES
               ('00000000-0000-0000-0000-00000000000a', 'loan-service'),
               ('00000000-0000-0000-0000-00000000000a', 'loan_service');",
        )
        .await
        .expect("colliding services");
    client
        .batch_execute(include_str!("../migrations/V4__r3.sql"))
        .await
        .expect("V4 applies");
    let indexed: bool = client
        .query_one("SELECT to_regclass('services_schema_idx') IS NOT NULL", &[])
        .await
        .expect("index")
        .get(0);
    assert!(!indexed);
}

#[tokio::test]
async fn a_permanent_startup_failure_exits_with_an_error() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;

    let (server, _) = db.url.rsplit_once('/').expect("a database URL");
    let missing = format!(
        "{server}/cufflink_missing_{}",
        uuid::Uuid::new_v4().simple()
    );
    assert_exits_with(
        PlatformProcess::launch(&missing, None, &[]),
        "platform database failed permanently",
    )
    .await;

    let platform = PlatformProcess::start(&db.url, None, &[]).await;
    platform.wait_ready().await;
    drop(platform);
    db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE refinery_schema_history SET checksum = '0' WHERE version = 2",
            &[],
        )
        .await
        .expect("diverge V2");
    assert_exits_with(
        PlatformProcess::launch(&db.url, None, &[]),
        "platform migrations failed permanently",
    )
    .await;
}

#[tokio::test]
async fn a_startup_deadline_ends_a_startup_that_never_completes() {
    let black_hole = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind black hole");
    let unreachable_url = format!(
        "postgres://cufflink:cufflink@127.0.0.1:{}/cufflink",
        black_hole.local_addr().expect("addr").port()
    );
    assert_exits_with(
        PlatformProcess::launch(&unreachable_url, None, &[("STARTUP_DEADLINE_SECS", "1")]),
        "startup did not complete within 1s",
    )
    .await;
}

async fn assert_exits_with(mut platform: PlatformProcess, reason: &str) {
    let status = platform.wait_exit(Duration::from_secs(30)).await;
    let output = platform.output_tail();
    let status = status.unwrap_or_else(|| panic!("the platform is still running: {output}"));
    assert!(!status.success(), "{status}: {output}");
    assert!(output.contains(reason), "{output}");
}

fn all_migrations() -> Vec<(i32, String)> {
    vec![
        (1, "baseline".to_string()),
        (2, "r1a_policy_and_isolation".to_string()),
        (3, "r2".to_string()),
        (4, "r3".to_string()),
        (5, "extensions".to_string()),
        (6, "runner_keys".to_string()),
    ]
}

async fn applied_migrations(db: &common::TestDb) -> Vec<(i32, String)> {
    db.pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT version, name FROM refinery_schema_history ORDER BY version",
            &[],
        )
        .await
        .expect("refinery_schema_history")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_drains_within_grace() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let mut platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("SHUTDOWN_GRACE_SECS", "10"),
            ("SHUTDOWN_UNREADY_DELAY_SECS", "1"),
        ],
    )
    .await;
    platform.wait_ready().await;

    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy(
            &tenant,
            &common::wasm_manifest("lifecycle-svc", &[("sleep2", "/sleep2")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let mut sse = platform
        .http
        .get(format!(
            "{}/svc/{}/lifecycle-svc/items/stream",
            platform.base_url, tenant.slug
        ))
        .send()
        .await
        .expect("open SSE stream");
    assert_eq!(sse.status().as_u16(), 200);
    let first = sse
        .chunk()
        .await
        .expect("first SSE chunk")
        .expect("connected event");
    assert!(
        String::from_utf8_lossy(&first).contains("event: connected"),
        "unexpected first SSE chunk"
    );

    let slow = {
        let client = platform.http.clone();
        let url = platform.fn_url(&tenant, "lifecycle-svc", "sleep2");
        tokio::spawn(async move { client.post(url).json(&serde_json::json!({})).send().await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;

    let signalled_at = Instant::now();
    platform.sigterm();

    let (status, body) = platform
        .get_json("/ready")
        .await
        .expect("/ready during unready delay");
    assert_eq!(status, 503);
    assert_eq!(body["state"], "shutting_down");

    let response = slow
        .await
        .expect("slow request task")
        .expect("slow request completes");
    assert_eq!(
        response.status().as_u16(),
        200,
        "in-flight request must finish"
    );

    let mut tail = Vec::new();
    while let Ok(Some(chunk)) = sse.chunk().await {
        tail.extend_from_slice(&chunk);
    }
    assert!(
        String::from_utf8_lossy(&tail).contains("event: shutdown"),
        "SSE stream must end with a shutdown event; got: {}",
        String::from_utf8_lossy(&tail)
    );

    let exit = platform
        .wait_exit(Duration::from_secs(10))
        .await
        .expect("platform exits within the grace period");
    assert!(exit.success(), "exit status: {exit}");
    assert!(signalled_at.elapsed() < Duration::from_secs(10));
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_finishes_the_message_in_hand_and_leaves_the_next_in_its_stream() {
    use futures_util::StreamExt;

    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let mut platform =
        PlatformProcess::start(&db.url, Some(nats_url), &[("SHUTDOWN_GRACE_SECS", "10")]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("sub-svc", &[]);
    manifest["subscriptions"] = serde_json::json!([{
        "subject": "dw.{tenant}.jobs.run",
        "handler": "sleep2",
        "handler_type": "wasm",
    }]);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let nats = async_nats::connect(nats_url).await.expect("nats client");
    let js = async_nats::jetstream::new(nats);
    let subject = format!("dw.{}.jobs.run", tenant.slug);
    let publish = |n: u32| {
        let js = js.clone();
        let subject = subject.clone();
        async move {
            js.publish(subject, format!(r#"{{"n":{n}}}"#).into())
                .await
                .expect("publish")
                .await
                .expect("stored");
        }
    };
    let mut stream = js
        .get_stream(format!(
            "DW_{}",
            tenant.slug.to_uppercase().replace('-', "_")
        ))
        .await
        .expect("tenant stream");

    publish(0).await;
    durable_after(&js, &tenant, "sub_svc_sleep2", 1).await;
    publish(1).await;
    let mut consumer = durable_after(&js, &tenant, "sub_svc_sleep2", 2).await;
    publish(2).await;
    platform.sigterm();
    let exit = platform
        .wait_exit(Duration::from_secs(10))
        .await
        .expect("platform exits within the grace period");
    assert!(exit.success(), "exit status: {exit}");

    let info = acknowledged(&mut consumer, 2, SETTLED_WITHIN).await;
    assert_eq!(
        info.ack_floor.stream_sequence, 2,
        "m0 and the in-flight m1 were acked: {info:?}"
    );
    assert_eq!(
        info.delivered.stream_sequence, 2,
        "m2 was never taken: a subscription fetches what it has a slot for: {info:?}"
    );

    let mut waiting = consumer
        .fetch()
        .max_messages(10)
        .expires(Duration::from_secs(5))
        .messages()
        .await
        .expect("fetch");
    let message = waiting
        .next()
        .await
        .expect("the message left in the stream is there for the next consumer")
        .expect("message");
    assert_eq!(&message.payload[..], br#"{"n":2}"#);
    assert_eq!(message.info().expect("jetstream metadata").delivered, 1);
    message.double_ack().await.expect("ack");
    assert!(waiting.next().await.is_none(), "nothing else was waiting");
    let state = stream.info().await.expect("stream info").state.clone();
    assert_eq!(state.messages, 0, "every message settled: {state:?}");
}

const SETTLED_WITHIN: Duration = Duration::from_secs(20);

async fn handling(platform: &PlatformProcess, count: u32) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while platform
        .metric("cufflink_pump_inflight", &[("kind", "subscription")])
        .await
        < Some(f64::from(count))
    {
        assert!(
            Instant::now() < deadline,
            "the platform never handled {count} messages"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(unix)]
async fn durable_after(
    js: &async_nats::jetstream::Context,
    tenant: &common::Tenant,
    durable: &str,
    count: u64,
) -> async_nats::jetstream::consumer::PullConsumer {
    use async_nats::jetstream::consumer::pull;

    let mut consumer = js
        .get_stream(format!(
            "DW_{}",
            tenant.slug.to_uppercase().replace('-', "_")
        ))
        .await
        .expect("tenant stream")
        .get_consumer::<pull::Config>(durable)
        .await
        .expect("durable consumer");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let info = consumer.info().await.expect("consumer info");
        if info.delivered.consumer_sequence >= count {
            return consumer;
        }
        assert!(
            Instant::now() < deadline,
            "the platform never took {count} deliveries: {info:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn a_subscription_handler_the_shutdown_stops_is_acknowledged_and_answered() {
    use futures_util::StreamExt;

    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let mut platform =
        PlatformProcess::start(&db.url, Some(nats_url), &[("SHUTDOWN_GRACE_SECS", "5")]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("cut-svc", &[]);
    manifest["subscriptions"] = serde_json::json!([{
        "subject": "dw.{tenant}.jobs.spin",
        "handler": "spin",
        "handler_type": "wasm",
    }]);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let nats = async_nats::connect(nats_url).await.expect("nats client");
    let js = async_nats::jetstream::new(nats.clone());
    let inbox = nats.new_inbox();
    let mut replies = nats.subscribe(inbox.clone()).await.expect("inbox");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Cufflink-Reply", inbox.as_str());
    js.publish_with_headers(
        format!("dw.{}.jobs.spin", tenant.slug),
        headers,
        "{}".into(),
    )
    .await
    .expect("publish")
    .await
    .expect("stored");
    let mut consumer = durable_after(&js, &tenant, "cut_svc_spin", 1).await;
    handling(&platform, 1).await;

    platform.sigterm();
    let exit = platform
        .wait_exit(Duration::from_secs(10))
        .await
        .expect("platform exits within the grace period");
    assert!(exit.success(), "exit status: {exit}");

    let info = acknowledged(&mut consumer, 1, SETTLED_WITHIN).await;
    assert_eq!(
        (
            info.ack_floor.stream_sequence,
            info.num_ack_pending,
            info.num_redelivered
        ),
        (1, 0, 0),
        "the interrupted message was acknowledged, not handed back: {info:?}"
    );
    let reply = tokio::time::timeout(Duration::from_secs(2), replies.next())
        .await
        .expect("the requester is answered")
        .expect("reply");
    let reply: serde_json::Value = serde_json::from_slice(&reply.payload).expect("json");
    assert!(
        reply["error"]
            .as_str()
            .is_some_and(|error| error.starts_with("WASM ")),
        "{reply}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn an_idle_subscription_leaves_no_fetch_behind_to_swallow_the_next_message() {
    use futures_util::StreamExt;

    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let mut platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("idle-svc", &[]);
    manifest["subscriptions"] = serde_json::json!([{
        "subject": "dw.{tenant}.jobs.run",
        "handler": "echo",
        "handler_type": "wasm",
    }]);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    let subject = format!("dw.{}.jobs.run", tenant.slug);
    let publish = |payload: &'static str| {
        let (js, subject) = (js.clone(), subject.clone());
        async move {
            js.publish(subject, payload.into())
                .await
                .expect("publish")
                .await
                .expect("stored");
        }
    };
    publish(r#"{"n":0}"#).await;
    let consumer = durable_after(&js, &tenant, "idle_svc_echo", 1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    platform.sigterm();
    let exit = platform
        .wait_exit(Duration::from_secs(10))
        .await
        .expect("platform exits within the grace period");
    assert!(exit.success(), "exit status: {exit}");

    publish(r#"{"n":1}"#).await;
    let published_at = Instant::now();
    let message = consumer
        .fetch()
        .max_messages(1)
        .expires(Duration::from_secs(5))
        .messages()
        .await
        .expect("fetch")
        .next()
        .await
        .expect("the message reaches the next consumer")
        .expect("message");
    assert!(published_at.elapsed() < Duration::from_secs(5));
    assert_eq!(&message.payload[..], br#"{"n":1}"#);
    assert_eq!(message.info().expect("jetstream metadata").delivered, 1);
    message.double_ack().await.expect("ack");
}
