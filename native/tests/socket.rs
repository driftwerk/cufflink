mod common;

use common::{hold, in_ms, request, Serving};
use cufflink_native::prelude::*;
use cufflink_types::ext::PROTOCOL_VERSION;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

fn echo(ctx: &CallContext, payload: Value) -> Result<Value> {
    Ok(json!({ "service": ctx.service.name, "payload": payload }))
}

fn directory() -> TempDir {
    tempfile::Builder::new()
        .prefix("cn")
        .tempdir_in("/tmp")
        .expect("socket directory")
}

fn call(extension: &str, payload: Value) -> String {
    String::from_utf8(request("harbor", extension, payload, in_ms(30_000))).expect("utf-8")
}

async fn send(socket: &Path, method: &str, target: &str, body: &str) -> (u16, Value) {
    send_bearing(socket, None, method, target, body).await
}

async fn send_bearing(
    socket: &Path,
    token: Option<&str>,
    method: &str,
    target: &str,
    body: &str,
) -> (u16, Value) {
    let mut stream = UnixStream::connect(socket).await.expect("connect");
    let authorization = token.map_or_else(String::new, |token| {
        format!("Authorization: Bearer {token}\r\n")
    });
    let request = format!(
        "{method} {target} HTTP/1.1\r\nHost: ext\r\nConnection: close\r\n{authorization}\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read response");
    let (head, body) = response.split_once("\r\n\r\n").expect("a response");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

async fn echo_server(socket: &Path) -> Serving {
    let server = cufflink_native::service("ext-echo")
        .extension("test.echo", echo, ["*"])
        .extension(
            "test.fail",
            |_: &CallContext, _: Value| -> Result<Value> {
                Err(ExtError::new("quota", "daily quota exhausted"))
            },
            ["reports-service"],
        )
        .bind_socket(socket)
        .await
        .expect("bound");
    Serving::start(|stopped| server.serve(stopped))
}

#[tokio::test]
async fn ready_lists_the_extensions_and_calls_are_answered() {
    let dir = directory();
    let socket = dir.path().join("s.sock");
    let serving = echo_server(&socket).await;

    assert_eq!(
        send(&socket, "GET", "/ready", "").await,
        (
            200,
            json!({"protocol": PROTOCOL_VERSION, "extensions": ["test.echo", "test.fail"]})
        )
    );
    assert_eq!(
        send(
            &socket,
            "POST",
            "/call",
            &call("test.echo", json!({"n": 1}))
        )
        .await,
        (
            200,
            json!({"ok": {"service": "reports-service", "payload": {"n": 1}}})
        )
    );
    assert_eq!(
        send(&socket, "POST", "/call", &call("test.fail", json!(null))).await,
        (
            200,
            json!({"error": {"code": "quota", "message": "daily quota exhausted"}})
        )
    );
    assert_eq!(
        send(&socket, "POST", "/call", &call("test.gone", json!(null))).await,
        (
            200,
            json!({"error": {"code": "not_found", "message": "'test.gone' is not served here"}})
        )
    );
    serving.stop().await;
    assert!(!socket.exists(), "the socket is removed once stopped");
}

#[tokio::test]
async fn a_server_requiring_a_token_answers_only_its_bearer() {
    let dir = directory();
    let socket = dir.path().join("s.sock");
    let server = cufflink_native::service("ext-echo")
        .extension("test.echo", echo, ["*"])
        .bind_socket(&socket)
        .await
        .expect("bound")
        .require_token("s3cret");
    let serving = Serving::start(|stopped| server.serve(stopped));

    let echoed = call("test.echo", json!({"n": 1}));
    for (method, target, body) in [("GET", "/ready", ""), ("POST", "/call", echoed.as_str())] {
        for refused in [None, Some("guess"), Some("s3cre"), Some("s3crets")] {
            assert_eq!(
                send_bearing(&socket, refused, method, target, body).await,
                (401, Value::Null),
                "{method} {target} bearing {refused:?}"
            );
        }
        assert_eq!(
            send_bearing(&socket, Some("s3cret"), method, target, body)
                .await
                .0,
            200,
            "{method} {target}"
        );
    }
    assert_eq!(
        send(&socket, "GET", "/elsewhere", "").await.0,
        401,
        "nothing is told without the token"
    );
    serving.stop().await;
}

#[tokio::test]
async fn a_malformed_call_is_a_bad_request_and_anything_else_is_not_found() {
    let dir = directory();
    let socket = dir.path().join("s.sock");
    let serving = echo_server(&socket).await;

    for body in ["not json", r#"{"v": 1, "extension": "test.echo"}"#] {
        let (status, refused) = send(&socket, "POST", "/call", body).await;
        assert_eq!(status, 400, "{body}");
        assert_eq!(refused["error"]["code"], "invalid", "{refused}");
    }
    for (method, target) in [
        ("GET", "/"),
        ("GET", "/call"),
        ("POST", "/ready"),
        ("DELETE", "/call"),
        ("GET", "/ready/now"),
    ] {
        assert_eq!(
            send(&socket, method, target, "").await.0,
            404,
            "{method} {target}"
        );
    }
    serving.stop().await;
}

#[tokio::test]
async fn the_socket_replaces_a_stale_one_and_is_its_owners_alone() {
    let dir = directory();
    let socket = dir.path().join("s.sock");
    drop(std::os::unix::net::UnixListener::bind(&socket).expect("a stale socket"));
    assert!(socket.exists());
    let serving = echo_server(&socket).await;
    let mode = std::fs::metadata(&socket)
        .expect("socket")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "{mode:o}");
    assert_eq!(send(&socket, "GET", "/ready", "").await.0, 200);
    serving.stop().await;

    let file = dir.path().join("f.sock");
    std::fs::write(&file, "not a socket").expect("a file");
    let refused = cufflink_native::service("ext-echo")
        .extension("test.echo", echo, ["*"])
        .bind_socket(&file)
        .await
        .err()
        .expect("refused");
    assert!(
        refused
            .to_string()
            .ends_with("f.sock exists and is not a socket"),
        "{refused}"
    );
    assert_eq!(
        std::fs::read_to_string(&file).expect("the file stays"),
        "not a socket"
    );
}

#[tokio::test]
async fn an_invalid_service_binds_nothing() {
    let dir = directory();
    let socket = dir.path().join("s.sock");
    let refused = cufflink_native::service("ext-echo")
        .bind_socket(&socket)
        .await
        .err()
        .expect("refused");
    assert_eq!(
        refused.to_string(),
        "provides: a native service provides at least one extension"
    );
    assert!(!socket.exists());
}

#[tokio::test]
async fn shutdown_stops_accepting_and_drains_the_calls_in_flight() {
    let dir = directory();
    let socket = dir.path().join("s.sock");
    let (hold, held) = hold();
    let server = cufflink_native::service("ext-hold")
        .extension("test.hold", hold, ["*"])
        .bind_socket(&socket)
        .await
        .expect("bound");
    let serving = Serving::start(|stopped| server.serve(stopped));
    let in_flight = tokio::spawn({
        let socket = socket.clone();
        async move { send(&socket, "POST", "/call", &call("test.hold", json!("held"))).await }
    });
    let release = held.started().await;

    serving.stop.send(()).expect("still serving");
    let refused = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match UnixStream::connect(&socket).await {
                Err(e) => break e,
                Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("new connections are refused");
    assert!(
        matches!(
            refused.kind(),
            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::ConnectionReset
        ),
        "a new connection is not served: {refused:?}"
    );
    assert!(
        socket.exists(),
        "the socket stays while a call is in flight"
    );
    assert!(!serving.served.is_finished());

    release.send(()).expect("the held call waits");
    assert_eq!(
        in_flight.await.expect("in-flight call"),
        (200, json!({"ok": "held"}))
    );
    serving.served.await.expect("serve task").expect("served");
    assert!(!socket.exists());
}
