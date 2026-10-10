use cufflink_testkit as common;

use common::PlatformProcess;
use reqwest::Method;
use serde_json::{json, Value};

fn strict_manifest(name: &str, routes: &[(&str, &str, &str)]) -> Value {
    let mut manifest = common::wasm_manifest(name, &[]);
    manifest["routing"] = json!("strict");
    manifest["custom_routes"] = routes
        .iter()
        .map(|(method, path, handler)| {
            json!({
                "method": method,
                "path": path,
                "handler": handler,
                "posture": {"kind": "public", "reason": "integration test"},
            })
        })
        .collect();
    manifest
}

async fn call(platform: &PlatformProcess, method: Method, path: &str) -> (u16, String, Value) {
    let response = platform
        .http
        .request(method, format!("{}{path}", platform.base_url))
        .send()
        .await
        .expect("request");
    let status = response.status().as_u16();
    let allow = response
        .headers()
        .get("allow")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = response.json().await.unwrap_or(Value::Null);
    (status, allow, body)
}

#[tokio::test]
async fn strict_routing_end_to_end() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;

    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|s| s.contains(&json!("route_matching"))),
        "{version}"
    );

    let (status, body) = platform
        .deploy(
            &tenant,
            &strict_manifest(
                "widgets",
                &[
                    ("GET", "/widgets/{widget_id}", "echo"),
                    ("PUT", "/widgets/{widget_id}", "echo_forwarded"),
                    ("POST", "/widgets/{widget_id}/files/{*rest}", "echo"),
                ],
            ),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let svc = format!("/svc/{}/widgets", tenant.slug);

    let (status, _, body) = call(
        &platform,
        Method::GET,
        &format!("{svc}/widgets/w%201?page=2"),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["handler"], "echo");
    assert_eq!(body["method"], "GET");
    assert_eq!(body["path"], "/widgets/w%201");
    assert_eq!(body["path_params"], json!([["widget_id", "w 1"]]));
    assert_eq!(body["body"], json!({"params": {"page": "2"}}));

    let (status, _, body) = call(&platform, Method::PUT, &format!("{svc}/widgets/w1")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["handler"], "echo_forwarded");

    let (status, _, body) = call(
        &platform,
        Method::POST,
        &format!("{svc}/widgets/w1/files/a/b.pdf"),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["path_params"],
        json!([["widget_id", "w1"], ["rest", "a/b.pdf"]])
    );

    let head = platform
        .http
        .head(format!("{}{svc}/widgets/w1", platform.base_url))
        .send()
        .await
        .expect("head");
    assert_eq!(head.status().as_u16(), 200);
    assert!(head.bytes().await.expect("body").is_empty());

    let (status, allow, _) = call(&platform, Method::DELETE, &format!("{svc}/widgets/w1")).await;
    assert_eq!((status, allow.as_str()), (405, "GET,HEAD,PUT"));

    let (status, _, body) = call(&platform, Method::GET, &format!("{svc}/nothing/here")).await;
    assert_eq!((status, body), (404, json!({"error": "Not found"})));

    let (status, _, body) = call(&platform, Method::GET, &format!("{svc}/widgets/%FF")).await;
    assert_eq!(status, 400, "{body}");

    let (status, _, body) = call(&platform, Method::POST, &format!("{svc}/_fn/echo")).await;
    assert_eq!(
        status, 200,
        "the _fn alias still dispatches by name: {body}"
    );
    assert_eq!(body["handler"], "echo");
    assert!(body.get("path").is_none(), "{body}");

    let created = platform
        .http
        .post(format!("{}{svc}/items", platform.base_url))
        .json(&json!({"title": "still crud"}))
        .send()
        .await
        .expect("create");
    assert_eq!(created.status().as_u16(), 200, "CRUD stays reachable");
    let created: Value = created.json().await.expect("row");
    let id = created["id"].as_str().expect("id");
    let (status, _, body) = call(&platform, Method::GET, &format!("{svc}/items/{id}")).await;
    assert_eq!(
        (status, &body["title"]),
        (200, &json!("still crud")),
        "{body}"
    );

    let (status, _, body) = call(&platform, Method::GET, &format!("{svc}/_meta/health")).await;
    assert_eq!((status, &body["name"]), (200, &json!("widgets")));

    let (status, body) = platform
        .deploy(
            &tenant,
            &strict_manifest("clash", &[("GET", "/items/{id}", "echo")]),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("shadows the CRUD routes of table 'items'")),
        "{body}"
    );

    assert_eq!(
        platform
            .metric(
                "cufflink_strict_route_matches_total",
                &[("result", "matched")]
            )
            .await,
        Some(4.0)
    );
    assert!(
        platform
            .metric(
                "cufflink_custom_route_duration_seconds_count",
                &[("route", "/widgets/{widget_id}"), ("status_class", "2xx")],
            )
            .await
            .is_some_and(|count| count >= 2.0),
        "strict calls are measured under their declared path"
    );
}
