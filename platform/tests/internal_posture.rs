use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use cufflink_platform::auth::service_identity::{self, Claims, ServiceIdentityKeys};
use cufflink_test_guests::Profile;
use serde_json::{json, Value};

const KEYS: &str = "k1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

fn route(method: &str, path: &str, handler: &str, posture: Value) -> Value {
    json!({"method": method, "path": path, "handler": handler, "posture": posture})
}

fn manifest(name: &str, routes: Value) -> Value {
    let mut manifest = common::wasm_manifest(name, &[]);
    manifest["routing"] = json!("strict");
    manifest["custom_routes"] = routes;
    manifest
}

async fn post(
    platform: &PlatformProcess,
    tenant: &Tenant,
    service: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Value,
) -> (u16, Value) {
    let request = headers.iter().fold(
        platform
            .http
            .post(format!(
                "{}/svc/{}/{service}{path}",
                platform.base_url, tenant.slug
            ))
            .json(&body),
        |request, (name, value)| request.header(*name, value),
    );
    let response = request.send().await.expect("request");
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or(Value::Null))
}

fn token(tenant_id: uuid::Uuid, path: &str, depth: u8) -> String {
    token_for(tenant_id, path, depth, 30)
}

fn token_for(tenant_id: uuid::Uuid, path: &str, depth: u8, lifetime: i64) -> String {
    let now = chrono::Utc::now().timestamp();
    service_identity::mint(
        &ServiceIdentityKeys::from_setting(Some(KEYS)).expect("keys"),
        &Claims {
            tenant_id,
            caller_service: "front".into(),
            caller_service_id: uuid::Uuid::new_v4(),
            caller_version: 1,
            target_service: "back".into(),
            method: "POST".into(),
            path: path.into(),
            issued_at: now,
            expires_at: now + lifetime,
            jti: uuid::Uuid::new_v4(),
            depth,
            on_behalf_of: None,
        },
    )
    .expect("token")
}

#[tokio::test]
async fn internal_posture_and_service_call() {
    let Some(wasm) = common::sdk::sdk_guest("internal", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("CUFFLINK_SERVICE_IDENTITY_KEYS", KEYS)],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|s| s.contains(&json!("internal_posture"))),
        "{version}"
    );

    let public = json!({"kind": "public", "reason": "integration test"});
    for (name, routes) in [
        (
            "front",
            json!([
                route("POST", "/hop", "hop", public.clone()),
                route(
                    "POST",
                    "/deep",
                    "hop",
                    json!({"kind": "internal", "callers": ["front"]})
                ),
            ]),
        ),
        (
            "back",
            json!([
                route(
                    "POST",
                    "/whoami",
                    "whoami",
                    json!({"kind": "internal", "callers": ["front"]})
                ),
                route(
                    "POST",
                    "/others",
                    "whoami",
                    json!({"kind": "internal", "callers": ["nobody"]})
                ),
                route(
                    "POST",
                    "/spin",
                    "spin",
                    json!({"kind": "internal", "callers": ["front"]})
                ),
            ]),
        ),
    ] {
        let (status, body) = platform
            .deploy_module(&tenant, &manifest(name, routes), &wasm)
            .await;
        assert_eq!(status, 200, "deploy of {name} failed: {body}");
    }

    let (status, body) = post(
        &platform,
        &tenant,
        "front",
        "/hop",
        &[],
        json!({"service": "back", "path": "/whoami", "body": {}}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({"status": 200, "body": {
            "caller": {"service": "front", "on_behalf_of": null, "depth": 1},
            "authenticated": false,
            "identity_header": null,
        }})
    );

    let (_, body) = post(
        &platform,
        &tenant,
        "front",
        "/hop",
        &[],
        json!({"service": "back", "path": "/others", "body": {}}),
    )
    .await;
    assert_eq!(
        body["status"], 403,
        "a caller the route does not list: {body}"
    );

    let chain = |path: &str| {
        (0..8).fold(
            json!({}),
            |inner, _| json!({"service": "front", "path": path, "body": inner}),
        )
    };
    for (path, reason) in [
        ("/deep", "a chain of internal routes"),
        ("/hop", "a self-call through a public route"),
    ] {
        let (status, body) = post(&platform, &tenant, "front", "/hop", &[], chain(path)).await;
        assert_eq!(status, 200, "{reason}: {body}");
        let completed = std::iter::successors(Some(&body), |hop| {
            (hop["status"] == 200).then(|| &hop["body"])
        })
        .take_while(|hop| hop["status"] == 200)
        .count();
        let refused = (0..completed).fold(&body, |hop, _| &hop["body"]);
        assert_eq!(
            (completed, &refused["error"]),
            (4, &json!("depth_exceeded")),
            "{reason} stops at depth {}: {body}",
            service_identity::MAX_CALL_DEPTH
        );
    }

    let identity =
        |path: &str, depth: u8| vec![(service_identity::HEADER, token(tenant.id, path, depth))];
    let (status, body) = post(
        &platform,
        &tenant,
        "back",
        "/whoami",
        &identity("/whoami", 2),
        json!({}),
    )
    .await;
    assert_eq!(
        status, 200,
        "an identity minted with the shared keys (as a worker's is): {body}"
    );
    assert_eq!(body["caller"]["depth"], 2);
    for (headers, status) in [
        (identity("/others", 1), 401),
        (identity("/whoami", 5), 401),
        (
            vec![(service_identity::HEADER, "cfsi1.k1.forged.sig".to_string())],
            401,
        ),
        (
            vec![("authorization", format!("ApiKey {}", tenant.api_key))],
            401,
        ),
        (vec![], 401),
    ] {
        let (got, body) = post(&platform, &tenant, "back", "/whoami", &headers, json!({})).await;
        assert_eq!(got, status, "{headers:?}: {body}");
    }
    let started = std::time::Instant::now();
    let (status, body) = post(
        &platform,
        &tenant,
        "back",
        "/spin",
        &[(
            service_identity::HEADER,
            token_for(tenant.id, "/spin", 1, 2),
        )],
        json!({}),
    )
    .await;
    assert_eq!(status, 500, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("timed out")),
        "{body}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the callee runs within its caller's remaining budget, not its own 30 s: {:?}",
        started.elapsed()
    );

    assert!(platform
        .metric(
            "cufflink_service_identity_rejected_total",
            &[("reason", "path")]
        )
        .await
        .is_some_and(|count| count >= 1.0));
}
