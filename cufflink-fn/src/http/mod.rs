//! Make HTTP requests from inside your WASM handler.
//!
//! Use this to call external APIs (Keycloak admin, third-party services, etc.)
//! from your handler code.

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(feature = "http-v2")]
mod v2;

#[cfg(feature = "service-call")]
pub use crate::call::{call_service, CallError, ServiceResponse};
#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};
use serde_json::Value;
use std::collections::HashMap;
#[cfg(feature = "http-v2")]
pub use v2::{fetch_bytes, multipart, HttpError, HttpErrorKind, HttpRequest, HttpResponse, Part};

/// Response from an HTTP request.
#[derive(Debug, Clone)]
pub struct FetchResponse {
    /// HTTP status code (e.g., 200, 404, 500).
    pub status: i32,
    /// Response body as a string (may be base64-encoded for binary content).
    pub body: String,
    /// Body encoding: "utf8" for text, "base64" for binary content.
    pub body_encoding: String,
    /// Response headers.
    pub headers: HashMap<String, String>,
}

impl FetchResponse {
    /// Parse the response body as JSON.
    pub fn json(&self) -> Option<Value> {
        serde_json::from_str(&self.body).ok()
    }

    /// Check if the response status indicates success (2xx).
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Check if the body is base64-encoded (binary content).
    pub fn is_base64(&self) -> bool {
        self.body_encoding == "base64"
    }
}

/// Make an HTTP request.
///
/// ```rust,ignore
/// let resp = http::fetch("GET", "https://api.example.com/data", &[], None);
/// if let Some(resp) = resp {
///     if resp.is_success() {
///         log::info(&format!("Got: {}", resp.body));
///     }
/// }
/// ```
pub fn fetch(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> Option<FetchResponse> {
    #[cfg(target_arch = "wasm32")]
    {
        let method_bytes = method.as_bytes();
        let url_bytes = url.as_bytes();
        let headers_map: HashMap<&str, &str> = headers.iter().copied().collect();
        let headers_json = serde_json::to_string(&headers_map).unwrap_or_default();
        let headers_bytes = headers_json.as_bytes();
        let body_bytes = body.unwrap_or("").as_bytes();
        let body_len = body.map(|b| b.len()).unwrap_or(0);

        let result = unsafe {
            ffi::http_fetch(
                method_bytes.as_ptr() as i32,
                method_bytes.len() as i32,
                url_bytes.as_ptr() as i32,
                url_bytes.len() as i32,
                headers_bytes.as_ptr() as i32,
                headers_bytes.len() as i32,
                body_bytes.as_ptr() as i32,
                body_len as i32,
            )
        };

        if result < 0 {
            return None;
        }

        host_buffer::read_host_json_lossy::<Value>().map(|v| fetch_response_from_json(&v))
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::fetch(method, url, headers, body, None).and_then(Result::ok)
    }
}

/// Make an HTTP request with an explicit request budget in milliseconds.
///
/// [`fetch`]'s default budget (~28s) is tuned for route handlers, which
/// the platform kills at 30s. Inside a `job(timeout = …)` handler there is
/// far more room — pass the budget a slow upstream actually needs. The
/// host clamps the value to the
/// handler's remaining execution budget, so asking for more than the
/// caller has left degrades to "as much time as remains", never a request
/// that outlives its handler. A value of 0 means the host default.
///
/// Requires a platform with the `http_fetch_timeout` host fn (cufflink
/// v0.25+) — roll the platform before deploying a service that calls this,
/// or the module fails to instantiate on the missing import.
pub fn fetch_with_timeout(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
    timeout_ms: u64,
) -> Option<FetchResponse> {
    #[cfg(target_arch = "wasm32")]
    {
        let method_bytes = method.as_bytes();
        let url_bytes = url.as_bytes();
        let headers_map: HashMap<&str, &str> = headers.iter().copied().collect();
        let headers_json = serde_json::to_string(&headers_map).unwrap_or_default();
        let headers_bytes = headers_json.as_bytes();
        let body_bytes = body.unwrap_or("").as_bytes();
        let body_len = body.map(|b| b.len()).unwrap_or(0);

        let result = unsafe {
            ffi::http_fetch_timeout(
                method_bytes.as_ptr() as i32,
                method_bytes.len() as i32,
                url_bytes.as_ptr() as i32,
                url_bytes.len() as i32,
                headers_bytes.as_ptr() as i32,
                headers_bytes.len() as i32,
                body_bytes.as_ptr() as i32,
                body_len as i32,
                timeout_ms as i64,
            )
        };

        if result < 0 {
            return None;
        }

        host_buffer::read_host_json_lossy::<Value>().map(|v| fetch_response_from_json(&v))
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::fetch(method, url, headers, body, Some(timeout_ms)).and_then(Result::ok)
    }
}

/// Make a POST request with an explicit request budget — see
/// [`fetch_with_timeout`].
pub fn post_with_timeout(
    url: &str,
    headers: &[(&str, &str)],
    body: &str,
    timeout_ms: u64,
) -> Option<FetchResponse> {
    fetch_with_timeout("POST", url, headers, Some(body), timeout_ms)
}

/// Make a GET request.
pub fn get(url: &str, headers: &[(&str, &str)]) -> Option<FetchResponse> {
    fetch("GET", url, headers, None)
}

/// Make a POST request with a body.
pub fn post(url: &str, headers: &[(&str, &str)], body: &str) -> Option<FetchResponse> {
    fetch("POST", url, headers, Some(body))
}

/// Make a PUT request with a body.
pub fn put(url: &str, headers: &[(&str, &str)], body: &str) -> Option<FetchResponse> {
    fetch("PUT", url, headers, Some(body))
}

/// Make a DELETE request.
pub fn delete(url: &str, headers: &[(&str, &str)]) -> Option<FetchResponse> {
    fetch("DELETE", url, headers, None)
}

/// Make a PATCH request with a body.
pub fn patch(url: &str, headers: &[(&str, &str)], body: &str) -> Option<FetchResponse> {
    fetch("PATCH", url, headers, Some(body))
}

/// A single request inside a [`fetch_many`] batch.
#[derive(Debug, Clone)]
pub struct FetchRequest<'a> {
    pub method: &'a str,
    pub url: &'a str,
    pub headers: &'a [(&'a str, &'a str)],
    pub body: Option<&'a str>,
}

/// Make N HTTP requests concurrently from the host. Per-item errors are
/// reported in the returned slot (other slots still succeed). Use this
/// when you'd otherwise call [`fetch`] in a loop — sequential blocking
/// fetches inside WASM are linear, the host fan-out is parallel.
///
/// The host bounds concurrency (default 32) so an unbounded list won't
/// exhaust connections.
///
/// ```rust,ignore
/// let results = http::fetch_many(&[
///     http::FetchRequest { method: "GET", url: "https://a", headers: &[], body: None },
///     http::FetchRequest { method: "GET", url: "https://b", headers: &[], body: None },
/// ]);
/// ```
pub fn fetch_many(requests: &[FetchRequest<'_>]) -> Vec<Result<FetchResponse, String>> {
    #[cfg(target_arch = "wasm32")]
    {
        let items: Vec<Value> = requests
            .iter()
            .map(|r| {
                let h: HashMap<&str, &str> = r.headers.iter().copied().collect();
                serde_json::json!({
                    "method": r.method,
                    "url": r.url,
                    "headers": h,
                    "body": r.body,
                })
            })
            .collect();
        let payload = serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string());
        let bytes = payload.as_bytes();
        let rc = unsafe { ffi::http_fetch_many(bytes.as_ptr() as i32, bytes.len() as i32) };
        if rc < 0 {
            return requests
                .iter()
                .map(|_| Err("http_fetch_many host call failed".to_string()))
                .collect();
        }
        host_buffer::read_host_json::<Vec<Value>>()
            .map(|v| v.into_iter().map(parse_fetch_slot).collect())
            .unwrap_or_else(|| {
                requests
                    .iter()
                    .map(|_| Err("malformed host response".to_string()))
                    .collect()
            })
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        requests
            .iter()
            .filter_map(|r| native::fetch(r.method, r.url, r.headers, r.body, None))
            .collect()
    }
}

#[cfg(target_arch = "wasm32")]
fn fetch_response_from_json(v: &Value) -> FetchResponse {
    FetchResponse {
        status: v["status"].as_i64().unwrap_or(0) as i32,
        body: v["body"].as_str().unwrap_or("").to_string(),
        body_encoding: v["body_encoding"].as_str().unwrap_or("utf8").to_string(),
        headers: v["headers"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

#[cfg(target_arch = "wasm32")]
fn parse_fetch_slot(slot: Value) -> Result<FetchResponse, String> {
    if !slot["ok"].as_bool().unwrap_or(false) {
        return Err(slot["error"]
            .as_str()
            .unwrap_or("unknown error")
            .to_string());
    }
    Ok(fetch_response_from_json(&slot))
}

#[cfg(test)]
mod tests {
    use crate::http;
    use std::collections::HashMap;

    #[test]
    fn test_http_fetch_noop_on_native() {
        let resp = http::fetch("GET", "https://example.com", &[], None);
        assert!(resp.is_none());
    }

    #[test]
    fn test_http_fetch_with_timeout_noop_on_native() {
        let resp = http::fetch_with_timeout("GET", "https://example.com", &[], None, 120_000);
        assert!(resp.is_none());
        let resp = http::post_with_timeout("https://example.com", &[], "{}", 120_000);
        assert!(resp.is_none());
    }

    #[test]
    fn test_http_get_noop_on_native() {
        let resp = http::get("https://example.com", &[]);
        assert!(resp.is_none());
    }

    #[test]
    fn test_http_post_noop_on_native() {
        let resp = http::post("https://example.com", &[], "{}");
        assert!(resp.is_none());
    }

    #[test]
    fn test_http_fetch_response_helpers() {
        let resp = http::FetchResponse {
            status: 200,
            body: r#"{"key": "value"}"#.to_string(),
            body_encoding: "utf8".to_string(),
            headers: HashMap::new(),
        };
        assert!(resp.is_success());
        assert!(!resp.is_base64());
        let json = resp.json().unwrap();
        assert_eq!(json["key"], "value");

        let err_resp = http::FetchResponse {
            status: 404,
            body: "not found".to_string(),
            body_encoding: "utf8".to_string(),
            headers: HashMap::new(),
        };
        assert!(!err_resp.is_success());

        let binary_resp = http::FetchResponse {
            status: 200,
            body: "aW1hZ2VkYXRh".to_string(),
            body_encoding: "base64".to_string(),
            headers: HashMap::new(),
        };
        assert!(binary_resp.is_base64());
    }
}
