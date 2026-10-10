//! Calls to another service of the same tenant, carrying a platform-minted
//! identity the target can verify (`internal` routes accept nothing else).
//! Needs a platform that supports `service_call`; the `service-call`
//! feature declares that in the module.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt;

/// The target's answer, whatever its status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceResponse {
    pub status: u16,
    /// Header names are lowercase.
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl ServiceResponse {
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, CallError> {
        serde_json::from_slice(&self.body).map_err(|e| {
            CallError::Transport(format!("the response is not the expected JSON: {e}"))
        })
    }

    pub fn text(&self) -> Result<&str, CallError> {
        std::str::from_utf8(&self.body)
            .map_err(|e| CallError::Transport(format!("the response is not UTF-8: {e}")))
    }

    /// A 2xx status.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    Timeout,
    /// The chain of service calls is already as deep as the platform allows.
    DepthExceeded,
    UnknownService(String),
    Transport(String),
    /// The platform refused to send the call (method, path or headers).
    Denied(String),
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Timeout => f.write_str("the service call timed out"),
            CallError::DepthExceeded => f.write_str("service calls nest too deep"),
            CallError::UnknownService(message)
            | CallError::Transport(message)
            | CallError::Denied(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for CallError {}

/// Call `method path` on `service` (a route path such as `/books/7/renew`
/// or `/_fn/handler`, with an optional `?query`). A body without a
/// `content-type` header is sent as `application/json`.
///
/// ```rust,ignore
/// let response = http::call_service("pricing", "POST", "/_fn/price", &[], Some(br#"{"sku":"a"}"#))?;
/// let price: Value = response.json()?;
/// ```
///
/// Outside the platform every call is `Err(CallError::Transport(..))`, unless
/// a test host answers it.
pub fn call_service(
    service: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> Result<ServiceResponse, CallError> {
    let request = json!({
        "service": service,
        "method": method,
        "path": path,
        "headers": headers.iter().copied().collect::<HashMap<&str, &str>>(),
        "body_b64": body.map(|body| STANDARD.encode(body)),
    });
    decode(&exchange(&request)?)
}

#[derive(Debug, Deserialize)]
struct Reply {
    ok: bool,
    #[serde(default)]
    status: Option<u16>,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    body_b64: Option<String>,
    #[serde(default)]
    error: Option<Failure>,
}

#[derive(Debug, Deserialize)]
struct Failure {
    kind: String,
    message: String,
}

fn decode(bytes: &[u8]) -> Result<ServiceResponse, CallError> {
    let reply: Reply = serde_json::from_slice(bytes)
        .map_err(|e| CallError::Transport(format!("malformed response: {e}")))?;
    match reply {
        Reply {
            ok: true,
            status: Some(status),
            headers,
            body_b64,
            ..
        } => Ok(ServiceResponse {
            status,
            headers,
            body: body_b64
                .map(|b64| STANDARD.decode(b64))
                .transpose()
                .map_err(|e| CallError::Transport(format!("malformed response body: {e}")))?
                .unwrap_or_default(),
        }),
        Reply {
            ok: false,
            error: Some(Failure { kind, message }),
            ..
        } => Err(match kind.as_str() {
            "timeout" => CallError::Timeout,
            "depth_exceeded" => CallError::DepthExceeded,
            "unknown_service" => CallError::UnknownService(message),
            "denied" => CallError::Denied(message),
            _ => CallError::Transport(message),
        }),
        _ => Err(CallError::Transport("malformed response".into())),
    }
}

#[cfg(target_arch = "wasm32")]
fn exchange(request: &Value) -> Result<Vec<u8>, CallError> {
    #[link(wasm_import_module = "env")]
    extern "C" {
        fn service_call(ptr: i32, len: i32) -> i32;
    }
    let payload = request.to_string();
    let rc = unsafe { service_call(payload.as_ptr() as i32, payload.len() as i32) };
    if rc < 0 {
        return Err(CallError::Transport(
            "the platform could not read the request (it may exceed the size limit)".into(),
        ));
    }
    crate::host_buffer::read_host_bytes()
        .ok_or_else(|| CallError::Transport("the platform returned no response".into()))
}

#[cfg(not(target_arch = "wasm32"))]
fn exchange(request: &Value) -> Result<Vec<u8>, CallError> {
    let host = crate::host::service_call().ok_or_else(|| {
        CallError::Transport("service calls are only available inside the Cufflink platform".into())
    })?;
    serde_json::to_vec(&host.call(request))
        .map_err(|e| CallError::Transport(format!("malformed response: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(wire: Value) -> Result<ServiceResponse, CallError> {
        decode(wire.to_string().as_bytes())
    }

    #[test]
    fn outside_the_platform_calls_fail_as_transport_errors() {
        assert!(matches!(
            call_service("s", "GET", "/x", &[], None),
            Err(CallError::Transport(_))
        ));
    }

    #[test]
    fn responses_decode_with_status_headers_and_body() {
        let response = reply(json!({
            "ok": true, "status": 404, "headers": {"content-type": "application/json"},
            "body_b64": STANDARD.encode(br#"{"error":"missing"}"#)
        }))
        .expect("response");
        assert_eq!(response.status, 404);
        assert!(!response.is_success());
        assert_eq!(response.headers["content-type"], "application/json");
        assert_eq!(
            response.json::<Value>().ok(),
            Some(json!({"error": "missing"}))
        );
        assert_eq!(response.text().ok(), Some(r#"{"error":"missing"}"#));
        let empty = reply(json!({"ok": true, "status": 204})).expect("empty");
        assert!(empty.is_success() && empty.body.is_empty());
        assert!(matches!(
            empty.json::<Value>(),
            Err(CallError::Transport(_))
        ));
        let binary = ServiceResponse {
            status: 200,
            headers: HashMap::new(),
            body: vec![0xff],
        };
        assert!(matches!(binary.text(), Err(CallError::Transport(_))));
    }

    #[test]
    fn failures_decode_into_their_kinds() {
        let failure =
            |kind: &str| reply(json!({"ok": false, "error": {"kind": kind, "message": "m"}}));
        assert_eq!(failure("timeout"), Err(CallError::Timeout));
        assert_eq!(failure("depth_exceeded"), Err(CallError::DepthExceeded));
        assert_eq!(
            failure("unknown_service"),
            Err(CallError::UnknownService("m".into()))
        );
        assert_eq!(failure("denied"), Err(CallError::Denied("m".into())));
        assert_eq!(failure("transport"), Err(CallError::Transport("m".into())));
        assert_eq!(failure("novel"), Err(CallError::Transport("m".into())));
        assert!(matches!(
            reply(json!({"ok": true})),
            Err(CallError::Transport(_))
        ));
        assert!(matches!(
            reply(json!({"ok": true, "status": 200, "body_b64": "%%"})),
            Err(CallError::Transport(_))
        ));
    }
}
