use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_platform::policy::PolicyMode;
use cufflink_platform::redis_conn::RedisHandle;
use cufflink_platform::repo::deployments::{self, ActivateVersion};
use cufflink_platform::repo::tenants::NewTenant;
use cufflink_platform::wasm_runtime::redis_scope::{self, KeyPlan, Reading};
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn a_tampered_module_is_refused_after_a_restart() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let storage_dir = std::env::temp_dir().join(format!("cufflink-isolation-{}", Uuid::new_v4()));
    let dir = storage_dir.to_string_lossy().to_string();
    let env = [
        ("STORAGE_BACKEND", "local"),
        ("STORAGE_LOCAL_DIR", dir.as_str()),
        ("S3_BUCKET", "cufflink"),
    ];
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform
        .deploy(
            &tenant,
            &common::wasm_manifest("guarded", &[("echo", "/echo")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    assert_eq!(platform.call_fn(&tenant, "guarded", "echo").await.0, 200);
    drop(platform);

    let service_id = body["service_id"].as_str().expect("service id");
    let version = body["version"].as_i64().expect("version");
    let replacement = wat::parse_str(
        r#"(module (memory (export "memory") 1)
             (func (export "alloc") (param i32) (result i32) (i32.const 0))
             (func (export "echo") (param i32 i32) (result i32) (i32.const 0)))"#,
    )
    .expect("wat");
    let storage = cufflink_storage::local_dir(&storage_dir, "cufflink").expect("local storage");
    storage
        .upload(
            &format!("wasm/{service_id}/v{version}.wasm"),
            replacement,
            "application/wasm",
        )
        .await
        .expect("overwrite the module");
    let precompiled: Vec<String> = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT engine_fingerprint, sha256 FROM module_artifacts \
             WHERE service_id = $1::text::uuid AND version = $2",
            &[&service_id, &i32::try_from(version).expect("version")],
        )
        .await
        .expect("artifacts")
        .iter()
        .map(|row| {
            format!(
                "wasm/{service_id}/v{version}/{}/{}.cwasm",
                row.get::<_, String>(0),
                row.get::<_, String>(1)
            )
        })
        .collect();
    assert!(
        !precompiled.is_empty(),
        "the deploy stored a precompiled module"
    );

    let restarted = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
    restarted.wait_ready().await;
    let (status, body) = restarted.call_fn(&tenant, "guarded", "echo").await;
    assert_eq!(
        (status, &body["handler"]),
        (200, &json!("echo")),
        "the verified precompiled module still serves the deployed code: {body}"
    );
    assert_eq!(
        restarted
            .metric(
                "cufflink_wasm_hash_mismatch_total",
                &[("service_id", service_id)]
            )
            .await,
        None,
        "the tampered .wasm is never read"
    );
    drop(restarted);

    for key in precompiled {
        storage
            .upload(&key, b"tampered".to_vec(), "application/octet-stream")
            .await
            .expect("overwrite the precompiled module");
    }

    let restarted = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
    restarted.wait_ready().await;
    let (status, body) = restarted.call_fn(&tenant, "guarded", "echo").await;
    assert_eq!(status, 500, "{body}");
    assert!(restarted
        .metric(
            "cufflink_wasm_hash_mismatch_total",
            &[("service_id", service_id)]
        )
        .await
        .is_some_and(|n| n >= 1.0));
    drop(restarted);
    let _ = std::fs::remove_dir_all(storage_dir);
}

#[tokio::test]
async fn redis_warn_dual_read_serves_pre_migration_keys() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(url) = infra.require_redis() else {
        return;
    };
    let redis = RedisHandle::connect_in_background(url).expect("redis url");
    for reading in [Reading::Plain, Reading::Whole, Reading::Upto(1024)] {
        dual_read(&redis, reading).await;
    }
}

async fn dual_read(redis: &RedisHandle, reading: Reading) {
    let redis = redis.clone();
    let tenant = Uuid::new_v4();
    let legacy_key = format!("feature_flags:{tenant}:checkout");
    let plan = |mode| redis_scope::plan(mode, tenant, &legacy_key).expect("plan");
    for _ in 0..50 {
        if redis.is_connected() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    redis_scope::set(&redis, &plan(PolicyMode::Off), "legacy", None)
        .await
        .expect("pre-migration write");
    let read = redis_scope::get(&redis, &plan(PolicyMode::Warn), reading)
        .await
        .expect("read");
    assert_eq!(read.value.as_deref(), Some("legacy"));
    assert!(read.from_fallback, "served from the unprefixed key");

    let other = format!("feature_flags:{tenant}:missing");
    let plans: Vec<KeyPlan> = [legacy_key.as_str(), other.as_str()]
        .iter()
        .map(|key| redis_scope::plan(PolicyMode::Warn, tenant, key).expect("plan"))
        .collect();
    let many = redis_scope::mget(&redis, &plans, reading)
        .await
        .expect("mget");
    assert_eq!(many.value, vec![Some("legacy".to_string()), None]);
    assert!(many.from_fallback);

    redis_scope::set(&redis, &plan(PolicyMode::Warn), "rewritten", Some(60))
        .await
        .expect("write");
    let read = redis_scope::get(&redis, &plan(PolicyMode::Warn), reading)
        .await
        .expect("read");
    assert_eq!(read.value.as_deref(), Some("rewritten"));
    assert!(!read.from_fallback, "the tenant key wins once written");
    let enforced = redis_scope::get(&redis, &plan(PolicyMode::Enforce), reading)
        .await
        .expect("read");
    assert_eq!(enforced.value.as_deref(), Some("rewritten"));
    assert_eq!(
        redis_scope::get(&redis, &plan(PolicyMode::Off), reading)
            .await
            .expect("read")
            .value
            .as_deref(),
        Some("legacy"),
        "warn never overwrote the unprefixed key"
    );

    redis_scope::del(&redis, &plan(PolicyMode::Warn))
        .await
        .expect("delete");
    assert_eq!(
        redis_scope::get(&redis, &plan(PolicyMode::Enforce), reading)
            .await
            .expect("read")
            .value,
        None,
        "a warn-mode delete removes the tenant's key"
    );
    assert_eq!(
        redis_scope::get(&redis, &plan(PolicyMode::Off), reading)
            .await
            .expect("read")
            .value
            .as_deref(),
        Some("legacy"),
        "and never the unprefixed key, which may be another tenant's"
    );
    let read = redis_scope::get(&redis, &plan(PolicyMode::Warn), reading)
        .await
        .expect("read");
    assert_eq!(
        read,
        redis_scope::Read {
            value: None,
            from_fallback: false
        },
        "a deleted key does not come back from the unprefixed one"
    );
    let many = redis_scope::mget(&redis, &plans, reading)
        .await
        .expect("mget");
    assert_eq!(many.value, vec![None, None]);
    assert!(!many.from_fallback);
    let primary = plan(PolicyMode::Warn).primary().to_string();
    let lifetime = |key: String| {
        let redis = &redis;
        async move {
            redis
                .call(|mut conn| async move {
                    redis::AsyncCommands::pttl::<_, i64>(&mut conn, key).await
                })
                .await
                .expect("pttl")
        }
    };
    assert_eq!(
        lifetime(primary.clone()).await,
        -1,
        "the tombstone lives as long as the unprefixed key, which does not expire"
    );
    redis_scope::set(&redis, &plan(PolicyMode::Warn), "again", None)
        .await
        .expect("write");
    assert_eq!(
        redis_scope::get(&redis, &plan(PolicyMode::Warn), reading)
            .await
            .expect("read")
            .value
            .as_deref(),
        Some("again"),
        "a write takes the tombstone's place"
    );

    redis_scope::set(&redis, &plan(PolicyMode::Off), "legacy", Some(60))
        .await
        .expect("an expiring unprefixed key");
    redis_scope::del(&redis, &plan(PolicyMode::Warn))
        .await
        .expect("delete");
    assert!(
        (1..=60_000).contains(&lifetime(primary.clone()).await),
        "the tombstone expires with the unprefixed key"
    );
    redis_scope::del(&redis, &plan(PolicyMode::Off))
        .await
        .expect("clean up");
    redis_scope::del(&redis, &plan(PolicyMode::Warn))
        .await
        .expect("delete");
    assert_eq!(
        lifetime(primary).await,
        -2,
        "with no unprefixed key, a delete leaves nothing"
    );
}

#[tokio::test]
async fn execution_context_joins_tenant_and_deployment() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_migrated_database().await;
    let pool = db.pool();
    let tenant = cufflink_platform::repo::tenants::create_tenant(
        &pool.get().await.expect("checkout"),
        &NewTenant {
            name: "ctx",
            slug: "ctx",
            ..Default::default()
        },
    )
    .await
    .expect("tenant");
    let service = cufflink_testkit::active_service(&pool, tenant.id, "catalog", "catalog")
        .await
        .expect("service");
    pool.get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO tenant_extensions (tenant_id, name, backend, callers, created_by)
             VALUES ($1, 'google.token', 'nats', '{catalog}', 'admin')",
            &[&tenant.id],
        )
        .await
        .expect("the extension the version calls");
    deployments::activate_version(
        &pool,
        ActivateVersion {
            service_id: service.id,
            version: 3,
            manifest_hash: "m",
            manifest: &serde_json::from_value(json!({"name": "catalog", "schema_name": "catalog_service", "tables": [], "publishes": ["harbor.books.>"], "extensions": ["google.token"]}))
                .expect("a manifest"),
            wasm_hash: Some("abc"),
            commit_hash: None,
            schema_json: None,
            schema_hash: "",
            native: None,
        },
    )
    .await
    .expect("deployment");

    let context = deployments::execution_context(&pool, service.id, 3)
        .await
        .expect("lookup")
        .expect("context");
    assert_eq!(
        context,
        deployments::ExecutionContext {
            tenant_id: tenant.id,
            tenant_slug: "ctx".into(),
            service_id: service.id,
            service_name: "catalog".into(),
            schema: "catalog_service".into(),
            wasm_hash: Some("abc".into()),
            publishes: Some(vec!["harbor.books.>".into()]),
            extensions: vec!["google.token".into()],
        }
    );
    assert!(deployments::execution_context(&pool, service.id, 4)
        .await
        .expect("lookup")
        .is_none());
}

#[tokio::test]
async fn subscriptions_outside_the_tenant_are_refused_at_deploy() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let with_subscription = |subject: &str, stream: Option<&str>| {
        let mut manifest = common::wasm_manifest("listener", &[("echo", "/echo")]);
        manifest["subscriptions"] = json!([{
            "subject": subject,
            "handler": "echo",
            "handler_type": "wasm",
            "stream": stream,
        }]);
        manifest
    };
    for (subject, stream) in [
        ("dw.victim.logs", None),
        ("dw.*.items.created", None),
        ("dw.{tenant}.items.created", Some("DW_VICTIM")),
    ] {
        let (status, body) = platform
            .deploy(&tenant, &with_subscription(subject, stream))
            .await;
        assert_eq!(status, 422, "{subject}: {body}");
    }
    let (status, body) = platform
        .deploy(
            &tenant,
            &with_subscription("cufflink.tick.5min", Some("CUFFLINK_TICKS")),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    let (_, version) = platform.get_json("/api/version").await.expect("version");
    let supports: Vec<&str> = version["supports"]
        .as_array()
        .expect("supports")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    for feature in ["tenant_policies", "forward_auth", "graphql_opt_in"] {
        assert!(supports.contains(&feature), "{feature} in {supports:?}");
    }
}

#[tokio::test]
async fn backup_restores_only_read_backups_the_caller_controls() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let js = async_nats::jetstream::new(async_nats::connect(nats_url).await.expect("nats"));
    js.get_or_create_stream(async_nats::jetstream::stream::Config {
        name: "CUFFLINK_BACKUP".into(),
        subjects: vec!["cufflink.backup.>".into()],
        retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await
    .expect("backup stream");
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let other = db.mint_tenant().await;
    let mut ids = Vec::new();
    for (owner, name) in [
        (&tenant, "orders"),
        (&tenant, "archive"),
        (&other, "elsewhere"),
    ] {
        let (status, body) = platform
            .deploy(owner, &common::wasm_manifest(name, &[("echo", "/echo")]))
            .await;
        assert_eq!(status, 200, "deploy failed: {body}");
        ids.push(body["service_id"].as_str().expect("id").to_string());
    }
    let key = format!("ApiKey {}", tenant.api_key);
    let restore = |body: Value| {
        let path = format!("/api/services/{}/backup/restore", ids[0]);
        let key = key.clone();
        let platform = &platform;
        async move {
            platform
                .send(reqwest::Method::POST, &path, &key, Some(&body))
                .await
        }
    };

    let cases = [
        (json!({"s3_key": format!("wasm/{}/v1.wasm", ids[0])}), 400),
        (
            json!({"s3_key": format!("backups/{}/../{}/x.jsonl", ids[0], ids[2])}),
            400,
        ),
        (
            json!({"s3_key": format!("backups/{}/job/export.jsonl", ids[2])}),
            404,
        ),
        (
            json!({"s3_key": format!("backups/{}/job/export.jsonl", ids[0]), "tables": ["items", "pg_authid"]}),
            422,
        ),
        (
            json!({"s3_key": format!("backups/{}/job/export.jsonl", ids[1])}),
            200,
        ),
        (
            json!({"s3_key": format!("backups/{}/job/export.jsonl", ids[0]), "tables": ["items"]}),
            200,
        ),
    ];
    for (body, expected) in cases {
        let (status, response) = restore(body.clone()).await;
        assert_eq!(status, expected, "{body}: {response}");
    }
    let (status, _) = platform
        .send(
            reqwest::Method::POST,
            &format!("/api/services/{}/backup/export", ids[0]),
            &key,
            Some(&json!({"tables": ["items; DROP TABLE items"]})),
        )
        .await;
    assert_eq!(status, 422);
}

#[tokio::test]
async fn bounded_redis_reads_answer_as_get_and_mget_do() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(url) = infra.require_redis() else {
        return;
    };
    let redis = RedisHandle::connect_in_background(url).expect("redis url");
    for _ in 0..50 {
        if redis.is_connected() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let tenant = Uuid::new_v4();
    let key = |name: &str| format!("bounded:{tenant}:{name}");
    let plan = |name: &str| redis_scope::plan(PolicyMode::Off, tenant, &key(name)).expect("plan");
    redis
        .call(|mut conn| {
            let (empty, list, at_cap, past_cap) =
                (key("empty"), key("list"), key("at_cap"), key("past_cap"));
            async move {
                use redis::AsyncCommands;
                conn.set::<_, _, ()>(empty, "").await?;
                conn.rpush::<_, _, ()>(list, "x").await?;
                conn.set::<_, _, ()>(at_cap, "a".repeat(16)).await?;
                conn.set::<_, _, ()>(past_cap, "b".repeat(17)).await
            }
        })
        .await
        .expect("fixtures");

    let names = ["missing", "empty", "list", "at_cap", "past_cap"];
    let plans: Vec<KeyPlan> = names.iter().map(|name| plan(name)).collect();
    let plain = redis_scope::mget(&redis, &plans, Reading::Plain)
        .await
        .expect("mget");
    for reading in [Reading::Whole, Reading::Upto(16)] {
        let bounded = redis_scope::mget(&redis, &plans, reading)
            .await
            .expect("bounded mget");
        for (i, name) in names.iter().enumerate() {
            let expected = match (reading, *name) {
                (Reading::Upto(_), "past_cap") => Some("b".repeat(17)),
                _ => plain.value[i].clone(),
            };
            assert_eq!(bounded.value[i], expected, "{reading:?} mget {name}");
        }
        for name in names {
            let single = plan(name);
            let get = |reading| redis_scope::get(&redis, &single, reading);
            match (get(Reading::Plain).await, get(reading).await) {
                (Ok(plain), Ok(bounded)) if name == "past_cap" => {
                    assert_eq!(plain.value.as_deref().map(str::len), Some(17));
                    assert_eq!(
                        bounded.value.as_deref().map(str::len),
                        Some(17),
                        "{reading:?}"
                    );
                }
                (Ok(plain), Ok(bounded)) => assert_eq!(plain, bounded, "{reading:?} get {name}"),
                (Err(plain), Err(bounded)) => {
                    assert_eq!(plain.to_string(), bounded.to_string(), "{reading:?} {name}")
                }
                other => panic!("{reading:?} get {name}: {other:?}"),
            }
        }
    }
    assert_eq!(plain.value[0], None);
    assert_eq!(plain.value[1].as_deref(), Some(""));
    assert_eq!(plain.value[2], None, "MGET answers nil for a wrong type");
}
