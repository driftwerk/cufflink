use cufflink_testkit as common;

use common::PlatformProcess;
use reqwest::Method;
use serde_json::{json, Value};

const SECRET: &str = "whsec_never_recorded";

fn shop() -> Value {
    json!({
        "name": "shop",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "orders", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
        ]}],
        "publishes": [],
    })
}

#[tokio::test]
async fn what_changes_the_control_plane_is_recorded_with_who_changed_it() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let key = format!("ApiKey {}", tenant.api_key);
    let (status, deployed) = platform.deploy(&tenant, &shop()).await;
    assert_eq!(status, 200, "{deployed}");
    let service = deployed["service_id"].as_str().expect("service id");

    let ok = |method: Method, path: String, by: &str, body: Option<Value>| {
        let (platform, by) = (&platform, by.to_string());
        async move {
            let (status, answer) = platform
                .send(method.clone(), &path, &by, body.as_ref())
                .await;
            assert_eq!(status, 200, "{method} {path}: {answer}");
            answer
        }
    };
    let policies = format!("/api/tenants/{}/policies", tenant.slug);
    ok(
        Method::PUT,
        policies.clone(),
        &admin,
        Some(json!({"policies": {"graphql": {"mode": "warn"}}})),
    )
    .await;
    ok(Method::DELETE, format!("{policies}/graphql"), &admin, None).await;
    ok(
        Method::PUT,
        format!("/api/tenants/{}/nats-claims", tenant.slug),
        &admin,
        Some(json!({"subjects": [format!("audited-{}.>", tenant.slug)], "streams": []})),
    )
    .await;
    let config = format!("/api/services/{service}/config");
    ok(
        Method::PUT,
        config.clone(),
        &key,
        Some(json!({"key": "STRIPE_KEY", "value": SECRET, "is_secret": true})),
    )
    .await;
    ok(Method::DELETE, format!("{config}/STRIPE_KEY"), &key, None).await;
    let minted = ok(
        Method::POST,
        "/api/auth/api-keys".to_string(),
        &key,
        Some(json!({"name": "ci"})),
    )
    .await;
    let minted_id = minted["id"].as_str().expect("key id").to_string();
    ok(
        Method::DELETE,
        format!("/api/auth/api-keys/{minted_id}"),
        &key,
        None,
    )
    .await;
    let webhooks = format!("/api/services/{service}/webhooks");
    let webhook = ok(
        Method::POST,
        webhooks.clone(),
        &key,
        Some(json!({"table_name": "orders", "event": "create",
                    "url": "http://127.0.0.1:9/hook", "secret": SECRET})),
    )
    .await;
    let webhook_id = webhook["id"].as_str().expect("webhook id").to_string();
    for active in [false, true] {
        ok(
            Method::PUT,
            format!("{webhooks}/{webhook_id}"),
            &key,
            Some(json!({"active": active})),
        )
        .await;
    }
    ok(
        Method::DELETE,
        format!("{webhooks}/{webhook_id}"),
        &key,
        None,
    )
    .await;

    let changes = [
        "policy.set",
        "policy.delete",
        "nats_claims.set",
        "config.set",
        "config.delete",
        "api_key.create",
        "api_key.revoke",
        "webhook.create",
        "webhook.deactivate",
        "webhook.activate",
        "webhook.delete",
    ];
    let actions: Vec<String> = changes.iter().map(|action| action.to_string()).collect();
    let recorded = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT action, actor, resource_type, resource_id, COALESCE(details::text, '')
             FROM audit_log WHERE tenant_id = $1 AND action = ANY($2) ORDER BY created_at",
            &[&tenant.id, &actions],
        )
        .await
        .expect("the audit log");
    let owner: String = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT keycloak_user_id FROM tenants WHERE id = $1",
            &[&tenant.id],
        )
        .await
        .expect("the tenant")
        .get(0);
    let of = |row: &tokio_postgres::Row, column: usize| row.get::<_, Option<String>>(column);
    let found: Vec<(String, String, String, String)> = recorded
        .iter()
        .map(|row| {
            (
                of(row, 0).unwrap_or_default(),
                of(row, 1).unwrap_or_default(),
                of(row, 2).unwrap_or_default(),
                of(row, 3).unwrap_or_default(),
            )
        })
        .collect();
    let by_admin = found[0].1.clone();
    assert!(
        by_admin.starts_with("apikey:"),
        "an admin's key is known by its name: {by_admin}"
    );
    let change = |action: &str, actor: &str, resource_type: &str, resource_id: &str| {
        (
            action.to_string(),
            actor.to_string(),
            resource_type.to_string(),
            resource_id.to_string(),
        )
    };
    assert_eq!(
        found,
        [
            change("policy.set", &by_admin, "policy", "graphql"),
            change("policy.delete", &by_admin, "policy", "graphql"),
            change("nats_claims.set", &by_admin, "tenant", &tenant.slug),
            change("config.set", &owner, "service_config", service),
            change("config.delete", &owner, "service_config", service),
            change("api_key.create", &owner, "api_key", &minted_id),
            change("api_key.revoke", &owner, "api_key", &minted_id),
            change("webhook.create", &owner, "webhook", &webhook_id),
            change("webhook.deactivate", &owner, "webhook", &webhook_id),
            change("webhook.activate", &owner, "webhook", &webhook_id),
            change("webhook.delete", &owner, "webhook", &webhook_id),
        ]
    );
    for row in &recorded {
        let details = of(row, 4).unwrap_or_default();
        assert!(
            !details.contains(SECRET)
                && !details.contains(minted["key"].as_str().unwrap_or(SECRET)),
            "{details}"
        );
    }
    let details = |index: usize| -> Value {
        serde_json::from_str(&of(&recorded[index], 4).unwrap_or_default()).expect("json")
    };
    let (key_id, key_name): (uuid::Uuid, String) = {
        let row = db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT id, name FROM api_keys WHERE key_hash = $1",
                &[&cufflink_platform::repo::api_keys::hash(&tenant.api_key)],
            )
            .await
            .expect("the tenant's key");
        (row.get(0), row.get(1))
    };
    assert_eq!(
        details(3),
        json!({"key": "STRIPE_KEY", "is_secret": true, "credential": {
            "method": "api_key", "key_id": key_id, "key_name": key_name}}),
        "an entry made with a key names the key, where its actor is the tenant's owner"
    );
    let by_admin_key = &details(0)["credential"];
    assert_eq!(
        (&by_admin_key["method"], &by_admin_key["key_name"]),
        (
            &json!("api_key"),
            &json!(by_admin.trim_start_matches("apikey:"))
        )
    );
    assert!(by_admin_key["key_id"]
        .as_str()
        .is_some_and(|id| id.parse::<uuid::Uuid>().is_ok()));
    let created = details(7);
    assert_eq!(created["host"], "127.0.0.1", "{created}");
    assert!(created.get("url").is_none(), "{created}");
}
