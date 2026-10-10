#[cfg(feature = "response-v2")]
mod v2;

use cufflink_types::delivery::DeliveryControl;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::BTreeMap;

#[cfg(feature = "response-v2")]
pub use v2::{Disposition, Redirect, S3Object};

/// An HTTP response to return from your handler.
#[derive(Debug, Clone)]
pub struct Response {
    repr: Repr,
}

#[derive(Debug, Clone)]
enum Repr {
    /// A JSON body, bare at 200 and in the `__status`/`__body` envelope
    /// otherwise: what every platform understands.
    Legacy { data: String, status: u16 },
    #[cfg(feature = "response-v2")]
    V2(Box<v2::V2>),
}

impl Response {
    fn legacy(value: &Value, status: u16) -> Self {
        Self {
            repr: Repr::Legacy {
                data: serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string()),
                status,
            },
        }
    }

    /// Return a JSON response with HTTP 200.
    pub fn json(value: &Value) -> Self {
        Self::legacy(value, 200)
    }

    /// A JSON response with `status`.
    pub fn status_json(status: u16, value: &Value) -> Self {
        Self::legacy(value, status)
    }

    /// A 201 Created JSON response.
    pub fn created(value: &Value) -> Self {
        Self::legacy(value, 201)
    }

    /// `{"error_message": message}` with `status`: the error body of services
    /// using the `error_message` error profile.
    pub fn error_message(status: u16, message: &str) -> Self {
        Self::legacy(&serde_json::json!({ "error_message": message }), status)
    }

    /// A 422 `{"validation_errors": {field: [messages]}}`.
    pub fn validation_errors(errors: &BTreeMap<String, Vec<String>>) -> Self {
        Self::legacy(&serde_json::json!({ "validation_errors": errors }), 422)
    }

    /// Return a plain text response (wrapped in a JSON string).
    pub fn text(s: &str) -> Self {
        Self::json(&Value::String(s.to_string()))
    }

    /// Return an error response with HTTP 400.
    pub fn error(message: &str) -> Self {
        Self::legacy(&serde_json::json!({"error": message}), 400)
    }

    /// Return a 404 Not Found error.
    pub fn not_found(message: &str) -> Self {
        Self::legacy(&serde_json::json!({"error": message}), 404)
    }

    /// Return a 401 Unauthorized error. Use this (not `Response::json`) for a
    /// missing/invalid auth failure so the HTTP status is a real 401 — edge
    /// rate-limiters, WAFs, and client token-refresh-on-401 all key on the
    /// status line, not a `status` field in the body.
    pub fn unauthorized(message: &str) -> Self {
        Self::legacy(&serde_json::json!({"error": message}), 401)
    }

    /// Return a 403 Forbidden error.
    pub fn forbidden(message: &str) -> Self {
        Self::legacy(&serde_json::json!({"error": message}), 403)
    }

    /// Return an empty success response.
    pub fn empty() -> Self {
        Self::json(&serde_json::json!({"ok": true}))
    }

    /// Set a custom HTTP status code on the response.
    pub fn with_status(self, status: u16) -> Self {
        let repr = match self.repr {
            Repr::Legacy { data, .. } => Repr::Legacy { data, status },
            #[cfg(feature = "response-v2")]
            Repr::V2(v2) => Repr::V2(Box::new(v2.with_status(status))),
        };
        Self { repr }
    }

    /// The HTTP status the response carries.
    pub fn status(&self) -> u16 {
        match &self.repr {
            Repr::Legacy { status, .. } => *status,
            #[cfg(feature = "response-v2")]
            Repr::V2(v2) => v2.status(),
        }
    }

    /// The headers the response sets, by lowercase name.
    pub fn headers(&self) -> BTreeMap<String, String> {
        match &self.repr {
            Repr::Legacy { .. } => BTreeMap::new(),
            #[cfg(feature = "response-v2")]
            Repr::V2(v2) => v2.headers().clone(),
        }
    }

    /// The body as the client receives it: JSON text, the bytes given, or
    /// nothing for a body the platform produces itself (a stored object,
    /// an event stream).
    pub fn body_bytes(&self) -> Vec<u8> {
        match &self.repr {
            Repr::Legacy { data, .. } => data.clone().into_bytes(),
            #[cfg(feature = "response-v2")]
            Repr::V2(v2) => v2.body_bytes(),
        }
    }

    /// The JSON body as a `T`.
    pub fn json_as<T: DeserializeOwned>(&self) -> serde_json::Result<T> {
        serde_json::from_slice(&self.body_bytes())
    }

    /// How the platform is asked to settle the message or job the handler
    /// ran for; `None` leaves it to the status.
    pub fn delivery(&self) -> Option<&DeliveryControl> {
        match &self.repr {
            Repr::Legacy { .. } => None,
            #[cfg(feature = "response-v2")]
            Repr::V2(v2) => v2.delivery(),
        }
    }

    /// Get the raw response string.
    /// Encodes the status code into the response so the platform can extract it.
    pub fn into_data(self) -> String {
        match self.repr {
            Repr::Legacy { data, status } => legacy_data(data, status),
            #[cfg(feature = "response-v2")]
            Repr::V2(v2) => v2.envelope().to_string(),
        }
    }
}

/// A 200 as the bare body, which every platform reads; any other status in
/// the `__status`/`__body` envelope.
fn legacy_data(data: String, status: u16) -> String {
    if status == 200 {
        data
    } else {
        serde_json::json!({
            "__status": status,
            "__body": serde_json::from_str::<Value>(&data).unwrap_or(Value::String(data)),
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_response_can_be_read_back() {
        let created = Response::created(&json!({"id": 7, "name": "a"}));
        assert_eq!(created.status(), 201);
        assert!(created.headers().is_empty());
        assert_eq!(created.body_bytes(), br#"{"id":7,"name":"a"}"#);
        assert_eq!(
            created.json_as::<Value>().expect("json"),
            json!({"id": 7, "name": "a"})
        );
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Created {
            id: u32,
        }
        assert_eq!(
            created.json_as::<Created>().expect("typed"),
            Created { id: 7 }
        );
        assert!(created.delivery().is_none());
        assert_eq!(Response::json(&json!(null)).status(), 200);
        assert_eq!(Response::not_found("x").with_status(410).status(), 410);
        assert!(Response::text("hi").json_as::<u32>().is_err());
    }

    #[test]
    fn test_response_json() {
        let resp = Response::json(&json!({"status": "ok", "count": 42}));
        let data = resp.into_data();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["status"], "ok");
        assert_eq!(parsed["count"], 42);
    }

    #[test]
    fn test_response_error() {
        let resp = Response::error("something went wrong");
        let data = resp.into_data();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["__status"], 400);
        assert_eq!(parsed["__body"]["error"], "something went wrong");
    }

    #[test]
    fn test_response_not_found() {
        let resp = Response::not_found("item not found");
        let data = resp.into_data();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["__status"], 404);
        assert_eq!(parsed["__body"]["error"], "item not found");
    }

    #[test]
    fn test_response_with_status() {
        let resp = Response::json(&serde_json::json!({"ok": true})).with_status(201);
        let data = resp.into_data();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["__status"], 201);
        assert_eq!(parsed["__body"]["ok"], true);
    }

    #[test]
    fn test_response_200_no_wrapper() {
        let resp = Response::json(&serde_json::json!({"data": "test"}));
        let data = resp.into_data();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["data"], "test");
        assert!(parsed.get("__status").is_none());
    }

    #[test]
    fn test_response_empty() {
        let resp = Response::empty();
        let data = resp.into_data();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["ok"], true);
    }

    #[test]
    fn legacy_constructors_keep_their_exact_bytes() {
        for (response, data) in [
            (Response::json(&json!({"a": 1})), r#"{"a":1}"#),
            (Response::text("hi"), r#""hi""#),
            (Response::empty(), r#"{"ok":true}"#),
            (
                Response::error("bad"),
                r#"{"__body":{"error":"bad"},"__status":400}"#,
            ),
            (
                Response::not_found("x"),
                r#"{"__body":{"error":"x"},"__status":404}"#,
            ),
            (
                Response::unauthorized("x"),
                r#"{"__body":{"error":"x"},"__status":401}"#,
            ),
            (
                Response::forbidden("x"),
                r#"{"__body":{"error":"x"},"__status":403}"#,
            ),
            (
                Response::json(&json!([1])).with_status(202),
                r#"{"__body":[1],"__status":202}"#,
            ),
        ] {
            assert_eq!(response.into_data(), data);
        }
    }

    #[test]
    fn plain_constructors_use_the_legacy_envelope() {
        assert_eq!(
            Response::created(&json!({"id": 1})).into_data(),
            r#"{"__body":{"id":1},"__status":201}"#
        );
        assert_eq!(
            Response::status_json(202, &json!({"queued": true})).into_data(),
            r#"{"__body":{"queued":true},"__status":202}"#
        );
        assert_eq!(
            Response::error_message(404, "Loan not found").into_data(),
            r#"{"__body":{"error_message":"Loan not found"},"__status":404}"#
        );
        let errors = BTreeMap::from([(
            "content".to_string(),
            vec!["Content cannot be empty".to_string()],
        )]);
        assert_eq!(
            Response::validation_errors(&errors).into_data(),
            r#"{"__body":{"validation_errors":{"content":["Content cannot be empty"]}},"__status":422}"#
        );
    }

    #[test]
    fn test_response_text() {
        let resp = Response::text("hello world");
        let data = resp.into_data();
        let parsed: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed, "hello world");
    }
}
