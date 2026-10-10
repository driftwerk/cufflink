use cufflink_testkit as common;

use axum::http::HeaderMap;
use axum::routing::post;
use axum::Router;
use common::keycloak::MockKeycloak;
use common::PlatformProcess;
use cufflink_platform::repo::tenants::NewTenant;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::mpsc;

async fn receiver() -> (String, mpsc::UnboundedReceiver<HeaderMap>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/hook",
        post(move |headers: HeaderMap| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(headers);
                "ok"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}/hook", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (url, rx)
}

fn manifest(name: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{
            "name": "orders",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "title", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false},
                {"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true}
            ]
        }],
        "publishes": [],
    })
}

#[tokio::test]
async fn webhooks_are_guarded_signed_and_egress_checked() {
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
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let key = format!("ApiKey {}", tenant.api_key);
    let mut service_ids = Vec::new();
    for name in ["shop", "other"] {
        let (status, body) = platform.deploy(&tenant, &manifest(name)).await;
        assert_eq!(status, 200, "deploy failed: {body}");
        service_ids.push(body["service_id"].as_str().expect("id").to_string());
    }
    let hooks = format!("/api/services/{}/webhooks", service_ids[0]);
    let (url, mut deliveries) = receiver().await;
    let register = |target: &str| json!({"table_name": "orders", "event": "create", "url": target, "secret": "whsec"});

    let user = format!("Bearer {}", keycloak.token(&realm, json!({"sub": "clerk"})));
    let (status, _) = platform
        .send(Method::POST, &hooks, &user, Some(&register(&url)))
        .await;
    assert_eq!(
        status, 403,
        "a user without webhooks:manage cannot register"
    );
    let (status, body) = platform
        .send(
            Method::POST,
            &hooks,
            &key,
            Some(&register("http://169.254.169.254/latest/")),
        )
        .await;
    assert_eq!(status, 400, "{body}");
    let (status, webhook) = platform
        .send(Method::POST, &hooks, &key, Some(&register(&url)))
        .await;
    assert_eq!(status, 200, "{webhook}");
    let webhook_id = webhook["id"].as_str().expect("id");
    let (status, _) = platform
        .send(
            Method::PUT,
            &format!("/api/services/{}/webhooks/{webhook_id}", service_ids[1]),
            &key,
            Some(&json!({"active": false})),
        )
        .await;
    assert_eq!(
        status, 404,
        "a webhook cannot be toggled through another service"
    );
    let (status, _) = platform
        .send(
            Method::DELETE,
            &format!("/api/services/{}/webhooks/{webhook_id}", service_ids[1]),
            &key,
            None,
        )
        .await;
    assert_eq!(
        status, 404,
        "a webhook cannot be deleted through another service"
    );

    let orders = format!("/svc/{}/shop/orders", tenant.slug);
    let order = json!({"title": "o"});
    let create = || platform.send(Method::POST, &orders, &key, Some(&order));
    assert_eq!(create().await.0, 200);
    let delivered = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await
        .expect("delivered under warn")
        .expect("headers");
    let v2 = delivered["x-cufflink-signature-256"].to_str().expect("v2");
    assert!(v2.starts_with("t=") && v2.contains(",v1="), "{v2}");
    assert!(delivered["x-cufflink-signature"]
        .to_str()
        .expect("v1")
        .starts_with("sha256="));
    assert!(uuid::Uuid::parse_str(delivered["x-cufflink-delivery"].to_str().expect("id")).is_ok());
    assert_eq!(delivered["x-cufflink-event"], "create");

    let policies = format!("/api/tenants/{}/policies", tenant.slug);
    let (status, _) = platform
        .send(
            Method::PUT,
            &policies,
            &admin,
            Some(&json!({"policies": {"egress": {"mode": "enforce"}}})),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(create().await.0, 200);
    assert!(
        tokio::time::timeout(Duration::from_secs(3), deliveries.recv())
            .await
            .is_err(),
        "enforce refuses a loopback receiver"
    );

    let allow = json!({"policies": {"egress": {"mode": "enforce", "config": {"allow_hosts": ["127.0.0.1"]}}}});
    let (status, _) = platform
        .send(Method::PUT, &policies, &admin, Some(&allow))
        .await;
    assert_eq!(status, 200);
    assert_eq!(create().await.0, 200);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
            .await
            .is_ok(),
        "an allowlisted host is delivered to"
    );
}
