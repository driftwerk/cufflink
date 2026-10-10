use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_platform::repo::api_keys;
use reqwest::Method;
use serde_json::{json, Value};

type Expiry = Option<chrono::DateTime<chrono::Utc>>;

async fn expiry(pool: &deadpool_postgres::Pool, name: &str) -> Expiry {
    pool.get()
        .await
        .expect("checkout")
        .query_one("SELECT expires_at FROM api_keys WHERE name = $1", &[&name])
        .await
        .expect("key")
        .get(0)
}

fn days_left(expiry: Expiry) -> i64 {
    (expiry.expect("dated") - chrono::Utc::now()).num_days()
}

#[tokio::test]
async fn admin_keys_are_dated_at_startup_by_the_ttl() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_migrated_database().await;
    let admin = db.mint_admin_key().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CUFFLINK_ADMIN_API_KEY_TTL_DAYS", "30")],
    )
    .await;
    platform.wait_ready().await;
    let pool = db.pool();
    let dated = expiry(&pool, "integration-admin").await;
    assert!((29..=30).contains(&days_left(dated)), "{dated:?}");
    assert_eq!(
        api_keys::expire_unbounded(
            &pool,
            api_keys::KeyClass::PlatformAdmin,
            chrono::Duration::days(1),
        )
        .await
        .expect("again"),
        0,
        "idempotent"
    );
    assert_eq!(expiry(&pool, "integration-admin").await, dated);

    let (status, minted) = platform
        .send(
            Method::POST,
            "/api/platform/api-keys",
            &format!("ApiKey {admin}"),
            Some(&json!({"name": "ci-preview"})),
        )
        .await;
    assert_eq!(status, 200, "{minted}");
    assert!((29..=30).contains(&days_left(expiry(&pool, "ci-preview").await)));
}

#[tokio::test]
async fn a_zero_ttl_leaves_admin_keys_unbounded() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_migrated_database().await;
    let admin = db.mint_admin_key().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CUFFLINK_ADMIN_API_KEY_TTL_DAYS", "0")],
    )
    .await;
    platform.wait_ready().await;
    let pool = db.pool();
    assert_eq!(expiry(&pool, "integration-admin").await, None);
    let (status, minted) = platform
        .send(
            Method::POST,
            "/api/platform/api-keys",
            &format!("ApiKey {admin}"),
            Some(&json!({"name": "forever"})),
        )
        .await;
    assert_eq!((status, minted["expires_at"].clone()), (200, Value::Null));
}

#[tokio::test]
async fn api_key_rotation() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let admin = db.mint_admin_key().await;
    let old = format!("ApiKey {}", tenant.api_key);

    let (status, listed) = platform
        .send(Method::GET, "/api/auth/api-keys", &old, None)
        .await;
    assert_eq!(status, 200, "{listed}");
    let key = &listed["keys"][0];
    let id = key["id"].as_str().expect("id").to_string();
    assert!(
        key["last_used_at"].is_string(),
        "using a key records it: {key}"
    );

    let rotate = |path: String, authorization: String| {
        let platform = &platform;
        async move {
            platform
                .send(Method::POST, &path, &authorization, None)
                .await
        }
    };
    let (status, _) = rotate(
        format!("/api/auth/api-keys/{id}/rotate?grace_hours=169"),
        old.clone(),
    )
    .await;
    assert_eq!(status, 422, "a week at most");
    let (status, _) = rotate(
        format!("/api/auth/api-keys/{}/rotate", uuid::Uuid::new_v4()),
        old.clone(),
    )
    .await;
    assert_eq!(status, 404);

    let (status, rotated) = rotate(
        format!("/api/auth/api-keys/{id}/rotate?grace_hours=0"),
        old.clone(),
    )
    .await;
    assert_eq!(status, 200, "{rotated}");
    assert_eq!(rotated["replaces"], json!(id));
    let rotated_id = rotated["id"].clone();
    assert!((89..=90).contains(&days_left(
        rotated["expires_at"].as_str().and_then(|t| t.parse().ok())
    )));
    let new = format!("ApiKey {}", rotated["key"].as_str().expect("key"));
    let (status, listed) = platform
        .send(Method::GET, "/api/auth/api-keys", &new, None)
        .await;
    assert_eq!(status, 200, "the new key works: {listed}");
    assert_eq!(
        listed["keys"]
            .as_array()
            .map(|keys| keys.iter().filter(|k| k["name"] == "integration").count()),
        Some(2),
        "same name, both listed"
    );
    let (status, body) = platform
        .send(Method::GET, "/api/auth/api-keys", &old, None)
        .await;
    assert_eq!(status, 401);
    assert_eq!(body["error"], "API key expired");
    let (status, _) = rotate(format!("/api/auth/api-keys/{id}/rotate"), new.clone()).await;
    assert_eq!(status, 404, "an expired key is not rotated again");
    let (_, body) = platform
        .send(Method::GET, "/api/auth/api-keys", "ApiKey ck_nope", None)
        .await;
    assert_eq!(body["error"], "Invalid API key");
    assert!(platform
        .metric("cufflink_api_key_rejected_total", &[("reason", "expired")])
        .await
        .is_some_and(|n| n >= 1.0));

    let admin_auth = format!("ApiKey {admin}");
    let (_, keys) = platform
        .send(Method::GET, "/api/platform/api-keys", &admin_auth, None)
        .await;
    let admin_id = keys["keys"][0]["id"]
        .as_str()
        .expect("admin key")
        .to_string();
    let (status, _) = rotate(format!("/api/auth/api-keys/{admin_id}/rotate"), new.clone()).await;
    assert_eq!(status, 404, "a tenant cannot rotate an admin key");
    let (status, rotated) = rotate(
        format!("/api/platform/api-keys/{admin_id}/rotate"),
        admin_auth.clone(),
    )
    .await;
    assert_eq!(status, 200, "{rotated}");
    let (status, _) = platform
        .send(Method::GET, "/api/platform/api-keys", &admin_auth, None)
        .await;
    assert_eq!(status, 200, "the old admin key works through the grace");
    let replaced: chrono::DateTime<chrono::Utc> = rotated["replaced_key_expires_at"]
        .as_str()
        .and_then(|t| t.parse().ok())
        .expect("grace end");
    assert_eq!((replaced - chrono::Utc::now()).num_hours(), 23);
    let new_admin = format!("ApiKey {}", rotated["key"].as_str().expect("key"));
    let (status, _) = platform
        .send(Method::GET, "/api/platform/api-keys", &new_admin, None)
        .await;
    assert_eq!(status, 200);

    let audited: Vec<(Option<uuid::Uuid>, String, Value)> = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT tenant_id, resource_id, details FROM audit_log
             WHERE action = 'api_key.rotate' ORDER BY created_at",
            &[],
        )
        .await
        .expect("audit rows")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(
        audited,
        vec![
            (
                Some(tenant.id),
                id.clone(),
                json!({"new_key_id": rotated_id})
            ),
            (None, admin_id, json!({"new_key_id": rotated["id"]})),
        ],
        "both rotation routes are audited"
    );
}

#[tokio::test]
async fn a_key_changed_mid_rotation_is_not_rotated_twice() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_migrated_database().await;
    db.mint_admin_key().await;
    let pool = db.pool();
    let mut holder_client = pool.get().await.expect("checkout");
    let id: uuid::Uuid = holder_client
        .query_one(
            "SELECT id FROM api_keys WHERE name = 'integration-admin'",
            &[],
        )
        .await
        .expect("admin key")
        .get(0);
    let holder = holder_client.transaction().await.expect("begin");
    holder
        .execute(
            "UPDATE api_keys SET expires_at = now() + interval '1 hour' WHERE id = $1",
            &[&id],
        )
        .await
        .expect("a concurrent rotation's retiring update");

    let rotation = tokio::spawn({
        let pool = pool.clone();
        async move {
            api_keys::rotate(
                &pool,
                api_keys::KeyOwner::Platform,
                id,
                None,
                chrono::Duration::hours(1),
            )
            .await
        }
    });
    let watcher = pool.get().await.expect("checkout");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let waiting: i64 = watcher
            .query_one(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE datname = current_database() AND wait_event_type = 'Lock'",
                &[],
            )
            .await
            .expect("activity")
            .get(0);
        if waiting > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the rotation never waited on the key"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    holder.commit().await.expect("commit");

    let outcome = rotation.await.expect("join");
    assert!(
        matches!(
            outcome,
            Err(cufflink_platform::errors::PlatformError::Conflict(_))
        ),
        "{:?}",
        outcome.map(|rotated| rotated.map(|r| r.minted.id))
    );
    let keys: i64 = watcher
        .query_one("SELECT count(*) FROM api_keys WHERE is_platform_admin", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(keys, 1, "no second key was minted");
}
