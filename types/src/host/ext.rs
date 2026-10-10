//! `ext_call`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallRequest {
    pub extension: String,
    pub payload: Value,
    /// How long to wait for the reply; the platform's default when unset,
    /// and never past what is left of the handler's own budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// `{"ok": true, "value": …}` or `{"ok": false, "error": {…}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ResponseWire", into = "ResponseWire")]
pub enum CallResponse {
    Ok(Value),
    Error(CallErrorEnvelope),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallErrorEnvelope {
    pub kind: CallErrorKind,
    /// The extension's own code, carried by [`CallErrorKind::Failed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallErrorKind {
    /// The service's manifest does not list the extension.
    NotDeclared,
    /// No extension of the name is registered for the tenant.
    NotFound,
    /// The registration does not list the calling service.
    Denied,
    /// Nothing serves the extension, or what does answered nonsense.
    Unavailable,
    Timeout,
    TooLarge,
    /// The extension answered with an error of its own.
    Failed,
    /// The request is malformed.
    Invalid,
}

impl CallErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CallErrorKind::NotDeclared => "not_declared",
            CallErrorKind::NotFound => "not_found",
            CallErrorKind::Denied => "denied",
            CallErrorKind::Unavailable => "unavailable",
            CallErrorKind::Timeout => "timeout",
            CallErrorKind::TooLarge => "too_large",
            CallErrorKind::Failed => "failed",
            CallErrorKind::Invalid => "invalid",
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseWire {
    ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<CallErrorEnvelope>,
}

impl TryFrom<ResponseWire> for CallResponse {
    type Error = &'static str;

    fn try_from(wire: ResponseWire) -> Result<Self, &'static str> {
        match wire {
            ResponseWire {
                ok: true,
                value,
                error: None,
            } => Ok(CallResponse::Ok(value.unwrap_or_default())),
            ResponseWire {
                ok: false,
                value: None,
                error: Some(error),
            } => Ok(CallResponse::Error(error)),
            ResponseWire { ok: true, .. } => Err("a response that is ok carries no error"),
            ResponseWire { ok: false, .. } => {
                Err("a response that is not ok carries an error and no value")
            }
        }
    }
}

impl From<CallResponse> for ResponseWire {
    fn from(response: CallResponse) -> Self {
        match response {
            CallResponse::Ok(value) => ResponseWire {
                ok: true,
                value: Some(value),
                error: None,
            },
            CallResponse::Error(error) => ResponseWire {
                ok: false,
                value: None,
                error: Some(error),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::tests::{refused, round_trip};
    use serde_json::json;

    const KINDS: [CallErrorKind; 8] = [
        CallErrorKind::NotDeclared,
        CallErrorKind::NotFound,
        CallErrorKind::Denied,
        CallErrorKind::Unavailable,
        CallErrorKind::Timeout,
        CallErrorKind::TooLarge,
        CallErrorKind::Failed,
        CallErrorKind::Invalid,
    ];

    #[test]
    fn a_request_carries_only_what_it_sets() {
        round_trip(
            &CallRequest {
                extension: "google.token".into(),
                payload: json!({"scope": "analytics.readonly"}),
                timeout_ms: None,
            },
            json!({"extension": "google.token", "payload": {"scope": "analytics.readonly"}}),
        );
        round_trip(
            &CallRequest {
                extension: "pdf.render".into(),
                payload: Value::Null,
                timeout_ms: Some(5_000),
            },
            json!({"extension": "pdf.render", "payload": null, "timeout_ms": 5000}),
        );
    }

    #[test]
    fn a_response_says_whether_it_is_ok() {
        round_trip(
            &CallResponse::Ok(json!({"token": "ya29"})),
            json!({"ok": true, "value": {"token": "ya29"}}),
        );
        round_trip(
            &CallResponse::Ok(Value::Null),
            json!({"ok": true, "value": null}),
        );
        assert_eq!(
            serde_json::from_value::<CallResponse>(json!({"ok": true})).expect("parse"),
            CallResponse::Ok(Value::Null)
        );
        round_trip(
            &CallResponse::Error(CallErrorEnvelope {
                kind: CallErrorKind::Failed,
                code: Some("quota".into()),
                message: "daily quota exhausted".into(),
            }),
            json!({
                "ok": false,
                "error": {"kind": "failed", "code": "quota", "message": "daily quota exhausted"}
            }),
        );
    }

    #[test]
    fn errors_name_their_kind() {
        for kind in KINDS {
            round_trip(
                &CallResponse::Error(CallErrorEnvelope {
                    kind,
                    code: None,
                    message: "why".into(),
                }),
                json!({"ok": false, "error": {"kind": kind.as_str(), "message": "why"}}),
            );
        }
    }

    #[test]
    fn contradictions_and_unknown_kinds_are_refused() {
        let error = json!({"kind": "denied", "message": "m"});
        refused::<CallResponse>(json!({"ok": true, "error": error}));
        refused::<CallResponse>(json!({"ok": true, "value": 1, "error": error}));
        refused::<CallResponse>(json!({"ok": false}));
        refused::<CallResponse>(json!({"ok": false, "value": 1, "error": error}));
        refused::<CallResponse>(json!({"value": 1}));
        refused::<CallResponse>(json!({"ok": "true", "value": 1}));
        refused::<CallResponse>(json!({"ok": true, "value": 1, "trace": "x"}));
        refused::<CallResponse>(
            json!({"ok": false, "error": {"kind": "exploded", "message": "m"}}),
        );
        refused::<CallResponse>(
            json!({"ok": false, "error": {"kind": "denied", "message": "m", "extra": 1}}),
        );
        refused::<CallRequest>(json!({"extension": "a"}));
        refused::<CallRequest>(json!({"payload": 1}));
        refused::<CallRequest>(json!({"extension": "a", "payload": 1, "retries": 2}));
        refused::<CallRequest>(json!({"extension": "a", "payload": 1, "timeout_ms": -1}));
    }
}
