use cufflink_testkit as common;

use common::PlatformProcess;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn route(method: &str, handler: &str, posture: Value, extra: Value) -> Value {
    let mut route = json!({
        "method": method,
        "path": format!("/{handler}"),
        "handler": handler,
        "posture": posture,
    });
    if let (Some(route), Some(extra)) = (route.as_object_mut(), extra.as_object()) {
        route.extend(extra.clone());
    }
    route
}

fn public() -> Value {
    json!({"kind": "public", "reason": "integration test"})
}

async fn post(
    platform: &PlatformProcess,
    url: String,
    content_type: &str,
    body: Vec<u8>,
) -> (u16, Value) {
    let response = platform
        .http
        .post(url)
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .expect("request");
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or(Value::Null))
}

#[tokio::test]
async fn route_limits() {
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
            .is_some_and(|s| s.contains(&json!("route_limits"))),
        "{version}"
    );

    let mut manifest = common::wasm_manifest("limits", &[]);
    manifest["custom_routes"] = json!([
        route(
            "POST",
            "neg_len",
            public(),
            json!({"body_limit_bytes": 12 * 1024 * 1024})
        ),
        route("POST", "insert_slow", public(), json!({})),
        route("POST", "spin", public(), json!({"timeout_secs": 2})),
        route("POST", "echo", public(), json!({"multipart": true})),
        route(
            "POST",
            "hook_query",
            public(),
            json!({"multipart": true, "body_limit_bytes": 12 * 1024 * 1024})
        ),
        route(
            "POST",
            "echo_forwarded",
            json!({"kind": "permission", "area": "items", "operation": "edit"}),
            json!({}),
        ),
    ]);
    let (status, body) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let (status, body) = post(
        &platform,
        platform.fn_url(&tenant, "limits", "neg_len"),
        "application/octet-stream",
        vec![b'x'; 10 * 1024 * 1024],
    )
    .await;
    assert_eq!((status, body), (200, json!({"rc": -1})));

    let (status, _) = post(
        &platform,
        platform.fn_url(&tenant, "limits", "insert_slow"),
        "application/octet-stream",
        vec![b'x'; 4 * 1024 * 1024 + 1],
    )
    .await;
    assert_eq!(status, 413, "the default route keeps the 4 MiB cap");

    let started = Instant::now();
    let (status, body) = post(
        &platform,
        platform.fn_url(&tenant, "limits", "spin"),
        "application/json",
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(status, 500, "{body}");
    assert_eq!(body["error"], "WASM execution timed out after 2s");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );

    let form = "--X\r\nContent-Disposition: form-data; name=\"title\"\r\n\r\nQ3\r\n\
        --X\r\nContent-Disposition: form-data; name=\"file\"; filename=\"r.csv\"\r\n\
        Content-Type: text/csv\r\n\r\na,b\r\n--X--\r\n";
    let (status, body) = post(
        &platform,
        format!("{}?tag=a&tag=b", platform.fn_url(&tenant, "limits", "echo")),
        "multipart/form-data; boundary=X",
        form.as_bytes().to_vec(),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["multipart"],
        json!([
            {"name": "title", "filename": null, "content_type": null, "data_b64": "UTM="},
            {"name": "file", "filename": "r.csv", "content_type": "text/csv", "data_b64": "YSxi"}
        ])
    );
    assert_eq!(body["query"], json!({"tag": ["a", "b"]}));
    assert_eq!(
        body["body"]["params"],
        json!({"tag": "b"}),
        "legacy params keep today's last-wins merge"
    );
    assert_eq!(body["client_ip"], "127.0.0.1");

    let (status, body) = post(
        &platform,
        platform.fn_url(&tenant, "limits", "echo"),
        "application/json",
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.starts_with("multipart:")),
        "{body}"
    );

    let upload = [
        b"--X\r\nContent-Disposition: form-data; name=\"file\"; filename=\"photo.jpg\"\r\n\
          Content-Type: image/jpeg\r\n\r\n"
            .to_vec(),
        vec![0xAB; 10 * 1024 * 1024],
        b"\r\n--X--\r\n".to_vec(),
    ]
    .concat();
    let (status, body) = post(
        &platform,
        platform.fn_url(&tenant, "limits", "hook_query"),
        "multipart/form-data; boundary=X",
        upload,
    )
    .await;
    assert_eq!(
        (status, &body),
        (200, &json!({"ok": true})),
        "a 10 MiB upload fits the default guest memory"
    );

    let fifty_mib = 50 * 1024 * 1024;
    let head = format!(
        "POST /svc/{}/limits/_fn/echo_forwarded HTTP/1.1\r\nHost: localhost\r\n\
         Content-Type: application/json\r\nContent-Length: {fifty_mib}\r\n\r\n",
        tenant.slug
    );
    assert_eq!(
        platform.status_line(head.as_bytes()).await,
        "HTTP/1.1 401 Unauthorized",
        "an unauthenticated caller is refused before its body is read"
    );
}
