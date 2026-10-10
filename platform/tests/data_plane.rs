use cufflink_testkit as common;

use common::keycloak::MockKeycloak;
use common::PlatformProcess;
use cufflink_platform::repo::tenants::NewTenant;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

fn manifest(name: &str, graphql: bool) -> Value {
    let id = json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true});
    let created = json!({"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true});
    let text = |name: &str| json!({"name": name, "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false});
    json!({
        "name": name,
        "version": "0.1.0",
        "mode": "crud",
        "graphql": graphql,
        "tables": [
            {"name": "items", "columns": [id, text("title"), created], "auth_required": true, "permission_area": "items"},
            {"name": "notes", "columns": [id, text("title"), created], "auth_required": true},
            {
                "name": "notifications",
                "columns": [id, text("customer_id"), text("title"), created],
                "auth_required": true,
                "permission_area": "notifications",
                "owner_field": "customer_id",
                "owner_self_service": true
            }
        ],
        "publishes": [],
    })
}

async fn stream_contains(response: &mut reqwest::Response, needle: &str, within: Duration) -> bool {
    let mut seen = String::new();
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, response.chunk()).await {
            Ok(Ok(Some(chunk))) => {
                seen.push_str(&String::from_utf8_lossy(&chunk));
                if seen.contains(needle) {
                    return true;
                }
            }
            _ => return false,
        }
    }
}

#[tokio::test]
async fn graphql_is_opt_in_and_respects_table_permissions() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let key = format!("ApiKey {}", tenant.api_key);
    for (name, graphql) in [("dark", false), ("lit", true)] {
        let (status, body) = platform.deploy(&tenant, &manifest(name, graphql)).await;
        assert_eq!(status, 200, "deploy failed: {body}");
    }
    let query = |service: &str, q: &str| {
        let path = format!("/svc/{}/{service}/graphql", tenant.slug);
        let body = json!({ "query": q });
        let key = key.clone();
        let platform = &platform;
        async move { platform.send(Method::POST, &path, &key, Some(&body)).await }
    };

    let (status, body) = query("dark", "{ notes { id } }").await;
    assert_eq!(status, 404, "{body}");
    assert_eq!(body["error"], "GraphQL is not enabled for this service");
    let (status, _) = platform
        .send(
            Method::GET,
            &format!("/svc/{}/dark/graphql/schema", tenant.slug),
            &key,
            None,
        )
        .await;
    assert_eq!(status, 404);

    let (status, body) = query("lit", "{ notes { id title } }").await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = query("lit", "{ items { id } }").await;
    assert_eq!(
        status, 403,
        "items needs items:view, which the key's tenant user lacks"
    );
    let anonymous = platform
        .http
        .post(format!(
            "{}/svc/{}/lit/graphql",
            platform.base_url, tenant.slug
        ))
        .json(&json!({"query": "{ notes { id } }"}))
        .send()
        .await
        .expect("request");
    assert_eq!(anonymous.status().as_u16(), 401);
}

#[tokio::test]
async fn sse_streams_are_capped_permission_gated_and_owner_filtered() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let keycloak = MockKeycloak::start().await;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        Some(nats_url),
        &[
            ("KEYCLOAK_URL", keycloak.url.as_str()),
            ("KEYCLOAK_ISSUER", keycloak.issuer_base.as_str()),
            ("SSE_MAX_STREAMS_PER_PRINCIPAL", "2"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let realm = format!("realm-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let tenant = db
        .mint_tenant_with(NewTenant {
            keycloak_url: Some(&keycloak.url),
            keycloak_realm: Some(&realm),
            ..Default::default()
        })
        .await;
    let service = "feed";
    let (status, body) = platform.deploy(&tenant, &manifest(service, false)).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let bearer = |sub: &str| format!("Bearer {}", keycloak.token(&realm, json!({"sub": sub})));
    let open = |table: &str, authorization: String| {
        platform
            .http
            .get(format!(
                "{}/svc/{}/{service}/{table}/stream",
                platform.base_url, tenant.slug
            ))
            .header("authorization", authorization)
            .send()
    };

    let denied = open("items", bearer("alice")).await.expect("stream");
    assert_eq!(denied.status().as_u16(), 403, "items needs items:view");

    let first = open("notes", bearer("alice")).await.expect("stream");
    let second = open("notes", bearer("alice")).await.expect("stream");
    assert_eq!(
        (first.status().as_u16(), second.status().as_u16()),
        (200, 200)
    );
    let third = open("notes", bearer("alice")).await.expect("stream");
    assert_eq!(third.status().as_u16(), 429);
    assert_eq!(
        open("notes", bearer("bob"))
            .await
            .expect("stream")
            .status()
            .as_u16(),
        200
    );
    drop(first);
    drop(second);

    let mut alice = None;
    for _ in 0..50 {
        let response = open("notifications", bearer("alice"))
            .await
            .expect("stream");
        if response.status().as_u16() == 200 {
            alice = Some(response);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut alice = alice.expect("closing streams frees their slots");
    let mut bob = open("notifications", bearer("bob")).await.expect("stream");
    assert!(stream_contains(&mut alice, "event: connected", Duration::from_secs(5)).await);
    assert!(stream_contains(&mut bob, "event: connected", Duration::from_secs(5)).await);
    let (status, body) = platform
        .send(
            Method::POST,
            &format!("/svc/{}/{service}/notifications", tenant.slug),
            &bearer("alice"),
            Some(&json!({"title": "your order shipped"})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert!(
        stream_contains(&mut alice, "your order shipped", Duration::from_secs(5)).await,
        "the owner sees their own row"
    );
    assert!(
        !stream_contains(&mut bob, "your order shipped", Duration::from_secs(2)).await,
        "another customer never does"
    );
}

#[tokio::test]
async fn every_data_entry_point_checks_grants_and_binding() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let stranger = db.mint_tenant().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let (status, body) = platform.deploy(&tenant, &manifest("lit", true)).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let (status, body) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/policies", tenant.slug),
            &admin,
            Some(&json!({"policies": {"tenant_binding": {"mode": "enforce"}}})),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    let base = format!("/svc/{}/lit", tenant.slug);
    let entry_points = |table: &str| {
        let row = format!("{base}/{table}/{}", uuid::Uuid::new_v4());
        let title = json!({"title": "t"});
        vec![
            ("list", Method::GET, format!("{base}/{table}"), None),
            ("get", Method::GET, row.clone(), None),
            (
                "create",
                Method::POST,
                format!("{base}/{table}"),
                Some(title.clone()),
            ),
            ("update", Method::PUT, row.clone(), Some(title.clone())),
            ("delete", Method::DELETE, row, None),
            (
                "batch",
                Method::POST,
                format!("{base}/{table}/batch"),
                Some(json!({"operations": [{"action": "create", "data": title}]})),
            ),
            (
                "graphql",
                Method::POST,
                format!("{base}/graphql"),
                Some(json!({"query": format!("{{ {table} {{ id }} }}")})),
            ),
            ("sse", Method::GET, format!("{base}/{table}/stream"), None),
        ]
    };

    let own = format!("ApiKey {}", tenant.api_key);
    for (name, method, path, body) in entry_points("items") {
        let (status, response) = platform.send(method, &path, &own, body.as_ref()).await;
        assert_eq!(status, 403, "{name} needs an items grant: {response}");
        assert!(
            response["error"]
                .as_str()
                .is_some_and(|e| e.starts_with("Missing permission: items:")),
            "{name}: {response}"
        );
    }
    let foreign = format!("ApiKey {}", stranger.api_key);
    for (name, method, path, body) in entry_points("notes") {
        let (status, response) = platform.send(method, &path, &foreign, body.as_ref()).await;
        assert_eq!(status, 403, "{name} binds the principal: {response}");
        assert_eq!(
            response["error"], "tenant_binding: api_key_foreign_tenant",
            "{name}"
        );
    }
}
