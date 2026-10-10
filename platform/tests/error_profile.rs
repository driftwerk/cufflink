use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};

fn route(method: &str, path: &str, handler: &str, posture: Value, extra: Value) -> Value {
    let mut route = json!({"method": method, "path": path, "handler": handler, "posture": posture});
    if let (Some(route), Some(extra)) = (route.as_object_mut(), extra.as_object()) {
        route.extend(extra.clone());
    }
    route
}

#[tokio::test]
async fn error_profile_renders_platform_errors() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(wasm) = common::sdk::sdk_guest("responses", Profile::Debug) else {
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
            .is_some_and(|s| s.contains(&json!("error_profile"))),
        "{version}"
    );

    let public = json!({"kind": "public", "reason": "integration test"});
    let mut manifest = common::wasm_manifest("notes", &[]);
    manifest["routing"] = json!("strict");
    manifest["errors"] = json!({"key": "error_message"});
    manifest["custom_routes"] = json!([
        route(
            "GET",
            "/notes/gone",
            "legacy_error",
            public.clone(),
            json!({})
        ),
        route(
            "POST",
            "/notes",
            "created",
            json!({"kind": "permission", "area": "notes", "operation": "edit"}),
            json!({}),
        ),
        route(
            "POST",
            "/notes/broken",
            "invalid_status",
            public.clone(),
            json!({})
        ),
        route(
            "POST",
            "/notes/upload",
            "no_content",
            public,
            json!({"body_limit_bytes": 8})
        ),
    ]);
    let (status, body) = platform.deploy_module(&tenant, &manifest, &wasm).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let (status, body) = platform
        .deploy(
            &tenant,
            &common::wasm_manifest("plain", &[("echo", "/echo")]),
        )
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let svc = format!("/svc/{}/notes", tenant.slug);
    let call = |method: Method, path: String, authorization: Option<String>, body: &'static str| {
        let mut request = platform
            .http
            .request(method, format!("{}{path}", platform.base_url))
            .body(body);
        if let Some(authorization) = authorization {
            request = request.header("authorization", authorization);
        }
        async move {
            let response = request.send().await.expect("request");
            let status = response.status().as_u16();
            let allow = response
                .headers()
                .get("allow")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            (
                status,
                allow,
                response.json::<Value>().await.unwrap_or(Value::Null),
            )
        }
    };
    let key = Some(format!("ApiKey {}", tenant.api_key));

    for (method, path, authorization, body, status, message) in [
        (
            Method::POST,
            format!("{svc}/notes"),
            None,
            "{}",
            401,
            "Authentication required for this route",
        ),
        (
            Method::POST,
            format!("{svc}/notes"),
            key.clone(),
            "{}",
            403,
            "Missing permission: notes:edit",
        ),
        (
            Method::GET,
            format!("{svc}/nothing/here"),
            None,
            "",
            404,
            "Not found",
        ),
        (
            Method::DELETE,
            format!("{svc}/notes/gone"),
            None,
            "",
            405,
            "Method Not Allowed",
        ),
        (
            Method::POST,
            format!("{svc}/notes/upload"),
            None,
            "123456789",
            413,
            "Failed to buffer the request body: length limit exceeded",
        ),
        (
            Method::POST,
            format!("{svc}/notes/broken"),
            None,
            "",
            500,
            "WASM execution error: invalid response status",
        ),
        (
            Method::GET,
            format!("{svc}/items/{}", uuid::Uuid::new_v4()),
            None,
            "",
            404,
            "items not found",
        ),
    ] {
        let (got, allow, body) = call(method.clone(), path.clone(), authorization, body).await;
        assert_eq!(got, status, "{method} {path}: {body}");
        assert_eq!(body, json!({"error_message": message}), "{method} {path}");
        if status == 405 {
            assert_eq!(allow.as_deref(), Some("GET,HEAD"));
        }
    }

    let (status, _, body) = call(Method::GET, format!("{svc}/notes/gone"), None, "").await;
    assert_eq!(
        (status, body),
        (404, json!({"error": "gone"})),
        "the guest's own body"
    );

    let (status, _, body) = call(
        Method::POST,
        format!("/svc/{}/plain/_fn/missing", tenant.slug),
        None,
        "",
    )
    .await;
    assert_eq!(
        (status, body),
        (404, json!({"error": "Not found"})),
        "a service without a profile keeps error"
    );
}
