use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use cufflink_test_guests::Profile;
use serde_json::{json, Value};

fn guest() -> Option<Vec<u8>> {
    match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => {
            common::skip(reason);
            None
        }
        None => {
            Some(cufflink_test_guests::wasm("service-call", Profile::Debug).expect("built guest"))
        }
    }
}

async fn relay(platform: &PlatformProcess, tenant: &Tenant, call: Value) -> Value {
    let response = platform
        .http
        .post(platform.fn_url(tenant, "caller-svc", "relay"))
        .json(&call)
        .send()
        .await
        .expect("relay request");
    assert_eq!(response.status().as_u16(), 200);
    response.json().await.expect("json body")
}

#[tokio::test]
async fn service_calls_run_in_process_within_the_tenant() {
    let Some(wasm) = guest() else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let (tenant, other) = (db.mint_tenant().await, db.mint_tenant().await);
    let routes = [("relay", "/relay"), ("echo", "/echo")];
    for (owner, service) in [
        (&tenant, "caller-svc"),
        (&tenant, "target-svc"),
        (&other, "foreign-svc"),
    ] {
        let (status, body) = platform
            .deploy_module(owner, &common::wasm_manifest(service, &routes), &wasm)
            .await;
        assert_eq!(status, 200, "deploy failed: {body}");
    }

    assert_eq!(
        relay(
            &platform,
            &tenant,
            json!({"service": "target-svc", "method": "POST", "path": "/_fn/echo?x=1", "body": {"n": 1}}),
        )
        .await,
        json!({
            "status": 200,
            "content_type": "application/json",
            "body": {"method": "POST", "body": {"n": 1, "params": {"x": "1"}}, "service": "target-svc"}
        })
    );

    for service in ["foreign-svc", "no-such-svc"] {
        assert_eq!(
            relay(
                &platform,
                &tenant,
                json!({"service": service, "method": "POST", "path": "/_fn/echo"}),
            )
            .await["error"],
            "unknown_service",
            "{service}"
        );
    }
    assert_eq!(
        relay(
            &platform,
            &tenant,
            json!({"service": "target-svc", "method": "TRACE", "path": "/_fn/echo"}),
        )
        .await["error"],
        "denied"
    );

    for path in [
        "/../foreign-svc/_fn/echo",
        "/%2e%2e/%2e%2e/api/tenants",
        "/%252e%252e/target-svc/_fn/echo",
        "/_fn\\..\\echo",
        "//_fn/echo",
    ] {
        assert_eq!(
            relay(
                &platform,
                &tenant,
                json!({"service": "target-svc", "method": "GET", "path": path}),
            )
            .await["error"],
            "denied",
            "{path}"
        );
    }

    let large_title = "t".repeat(3 * 1024 * 1024);
    let created = relay(
        &platform,
        &tenant,
        json!({
            "service": "target-svc", "method": "POST", "path": "/items",
            "body": {"title": large_title}
        }),
    )
    .await;
    assert_eq!(
        created["status"], 200,
        "a CRUD body past axum's default limit is accepted in process"
    );
    let id = created["body"]["id"].as_str().expect("created id");
    let fetched = relay(
        &platform,
        &tenant,
        json!({"service": "target-svc", "method": "GET", "path": format!("/items/{id}")}),
    )
    .await;
    assert_eq!(fetched["status"], 200);
    assert_eq!(
        fetched["body"]["title"].as_str().map(str::len),
        Some(large_title.len())
    );

    let oversized = relay(
        &platform,
        &tenant,
        json!({
            "service": "target-svc", "method": "POST", "path": "/_fn/echo",
            "pad_bytes": 5 * 1024 * 1024
        }),
    )
    .await;
    assert_eq!(
        oversized["status"], 413,
        "route-level body limits still apply in process"
    );

    assert_eq!(
        platform
            .metric(
                "cufflink_service_calls_total",
                &[
                    ("caller", "caller-svc"),
                    ("target", "target-svc"),
                    ("result", "ok")
                ]
            )
            .await,
        Some(3.0)
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_service_calls_total",
                &[
                    ("caller", "caller-svc"),
                    ("target", "-"),
                    ("result", "unknown_service")
                ]
            )
            .await,
        Some(2.0)
    );
}

#[tokio::test]
async fn hook_recursion_through_crud_stops_at_the_maximum_depth() {
    let Some(wasm) = guest() else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let mut manifest = common::wasm_manifest("recursive-svc", &[]);
    manifest["tables"][0]["hooks"] = json!({"before_create": "recurse"});
    let (status, body) = platform.deploy_module(&tenant, &manifest, &wasm).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let response = platform
        .http
        .post(format!(
            "{}/svc/{}/recursive-svc/items",
            platform.base_url, tenant.slug
        ))
        .json(&json!({"title": "top"}))
        .send()
        .await
        .expect("create");
    assert_eq!(response.status().as_u16(), 200);

    let listed: Value = platform
        .http
        .get(format!(
            "{}/svc/{}/recursive-svc/items?sort=title",
            platform.base_url, tenant.slug
        ))
        .send()
        .await
        .expect("list")
        .json()
        .await
        .expect("json");
    let mut titles: Vec<&str> = listed["results"]
        .as_array()
        .unwrap_or_else(|| panic!("no results: {listed}"))
        .iter()
        .filter_map(|row| row["title"].as_str())
        .collect();
    titles.sort_unstable();
    assert_eq!(
        titles,
        ["depth_exceeded", "nested", "nested", "nested", "top"],
        "one hook per depth from 0 to the maximum of 4"
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_service_calls_total",
                &[("caller", "recursive-svc"), ("result", "depth")]
            )
            .await,
        Some(1.0)
    );
}
