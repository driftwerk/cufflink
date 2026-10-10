use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

async fn create_key(
    platform: &PlatformProcess,
    tenant: &Tenant,
    scopes: &[&str],
) -> (String, String) {
    let (status, body) = platform
        .send(
            Method::POST,
            &format!("/api/tenants/{}/partner-keys", tenant.slug),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({"name": "acme-ingest", "scopes": scopes})),
        )
        .await;
    assert_eq!(status, 201, "{body}");
    (
        body["id"].as_str().expect("id").to_string(),
        body["key"].as_str().expect("key").to_string(),
    )
}

async fn call(
    platform: &PlatformProcess,
    tenant: &Tenant,
    handler: &str,
    header: Option<(&str, &str)>,
) -> (u16, Value) {
    let request = platform
        .http
        .post(platform.fn_url(tenant, "ingest", handler))
        .json(&json!({"book": 1}));
    let request = match header {
        Some((name, value)) => request.header(name, value),
        None => request,
    };
    let response = request.send().await.expect("request");
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or(Value::Null))
}

#[tokio::test]
async fn api_key_posture() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let (tenant, other) = (db.mint_tenant().await, db.mint_tenant().await);
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|s| s.contains(&json!("api_key_posture"))),
        "{version}"
    );

    let mut manifest = common::wasm_manifest("ingest", &[]);
    manifest["custom_routes"] = json!([
        {"method": "POST", "path": "/ingest", "handler": "echo",
            "posture": {"kind": "api_key", "scope": "books:ingest"}},
        {"method": "POST", "path": "/export", "handler": "echo_forwarded",
            "posture": {"kind": "api_key", "scope": "books:export"}},
    ]);
    for owner in [&tenant, &other] {
        let (status, body) = platform.deploy(owner, &manifest).await;
        assert_eq!(status, 200, "deploy failed: {body}");
    }

    let (ingest_id, ingest) = create_key(&platform, &tenant, &["books:ingest"]).await;
    let (wide_id, wide) = create_key(&platform, &tenant, &["books:*"]).await;
    let (_, foreign) = create_key(&platform, &other, &["books:*"]).await;

    let (status, body) = call(&platform, &tenant, "echo", Some(("x-api-key", &ingest))).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["api_key"],
        json!({"id": ingest_id, "name": "acme-ingest", "scopes": ["books:ingest"]})
    );
    assert_eq!(body["auth"], Value::Null);
    assert!(body["headers"].get("x-api-key").is_none(), "{body}");

    let (status, _) = call(
        &platform,
        &tenant,
        "echo_forwarded",
        Some(("x-api-key", &wide)),
    )
    .await;
    assert_eq!(status, 200, "resource:* covers every action");

    let (_, expired) = create_key(&platform, &tenant, &["books:*"]).await;
    let expired_prefix = expired.split('_').nth(1).expect("prefix").to_string();
    db.pool()
        .get()
        .await
        .expect("client")
        .execute(
            "UPDATE partner_keys SET expires_at = now() - interval '1 minute' WHERE key_prefix = $1",
            &[&expired_prefix],
        )
        .await
        .expect("expire");

    let bearer = format!("ApiKey {}", tenant.api_key);
    for (handler, header, reason) in [
        (
            "echo_forwarded",
            Some(("x-api-key", ingest.as_str())),
            "a scope that does not cover the route",
        ),
        (
            "echo",
            Some(("x-api-key", foreign.as_str())),
            "another tenant's key",
        ),
        (
            "echo",
            Some(("x-api-key", expired.as_str())),
            "an expired key",
        ),
        (
            "echo",
            Some(("x-api-key", "cfp_not_a_key")),
            "a malformed key",
        ),
        (
            "echo",
            Some(("authorization", bearer.as_str())),
            "a deploy API key",
        ),
        ("echo", None, "no key"),
    ] {
        let (status, body) = call(&platform, &tenant, handler, header).await;
        assert_eq!(
            (status, &body),
            (401, &json!({"error": "Invalid API key"})),
            "{reason}"
        );
    }

    let (status, body) = platform
        .send(
            Method::DELETE,
            &format!("/api/tenants/{}/partner-keys/{wide_id}", tenant.slug),
            &bearer,
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let revoked = async {
        while call(&platform, &tenant, "echo", Some(("x-api-key", &wide)))
            .await
            .0
            != 401
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), revoked)
        .await
        .expect("a revoked key is refused once the invalidation arrives");

    for reason in ["scope", "tenant", "expired", "malformed"] {
        assert!(
            platform
                .metric("cufflink_partner_key_rejected_total", &[("reason", reason)])
                .await
                .is_some_and(|count| count >= 1.0),
            "{reason}"
        );
    }
}
