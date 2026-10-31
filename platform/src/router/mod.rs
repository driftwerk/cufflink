pub(crate) mod authz;
pub mod bind;
pub mod crud;
pub mod crud_pk;
pub mod custom;
pub(crate) mod error_profile;
pub mod filtering;
pub mod graphql;
pub mod guest_response;
pub mod jobs;
pub mod meta;
pub(crate) mod postures;
pub(crate) mod query;
pub mod row_convert;
pub mod sse;
pub(crate) mod sse_bridge;
pub(crate) mod strict;

use crate::errors::{PlatformError, PlatformResult};
use crate::wasm_runtime::budget::{ChargedJson, ChargedJsonError, Hold};
use crate::wasm_runtime::ExecutionError;
use cufflink_types::host::request::GuestRequest;
use serde_json::Value;

pub(crate) const HOP_BY_HOP_HEADERS: [&str; 7] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub(crate) fn guest_json(request: &GuestRequest) -> PlatformResult<Value> {
    request
        .to_value()
        .map_err(|e| PlatformError::Internal(format!("guest request: {e}")))
}

pub(crate) fn guest_text(request: &GuestRequest) -> PlatformResult<String> {
    charged_guest_text(request, &mut Hold::none())
        .map_err(|e| PlatformError::Internal(format!("guest request: {e}")))
}

pub(crate) fn charged_guest_text(
    request: &GuestRequest,
    hold: &mut Hold,
) -> Result<String, ChargedJsonError> {
    let mut tree = hold.sibling();
    let parsed = ChargedJson::to_value(request, &mut tree)?;
    ChargedJson::write(&parsed.value, hold)
}

pub(crate) fn request_refused(
    state: &crate::AppState,
    subject: &str,
    error: ChargedJsonError,
) -> PlatformError {
    match (error, state.wasm_engine.as_ref()) {
        (ChargedJsonError::Capacity(refused), Some(engine)) => {
            map_execution_error(subject, engine.refusal(refused, false))
        }
        (error, _) => PlatformError::Internal(format!("guest request: {error}")),
    }
}

pub(crate) fn retry_later(after: std::time::Duration, ran: bool) -> PlatformError {
    PlatformError::RetryLater {
        message: match ran {
            false => "guest capacity exhausted; the request was not run",
            true => {
                "guest capacity exhausted while the request ran; it may have been partly applied"
            }
        }
        .to_string(),
        after,
    }
}

pub(crate) fn retry_uncommitted(after: std::time::Duration) -> PlatformError {
    PlatformError::RetryLater {
        message: "guest capacity exhausted after the request's before-hook ran; nothing from \
                  the write was committed"
            .to_string(),
        after,
    }
}

pub(crate) fn map_execution_error(subject: &str, err: ExecutionError) -> PlatformError {
    match err {
        ExecutionError::DeadlineExceeded { budget } => PlatformError::Internal(format!(
            "{subject} execution timed out after {}s",
            budget.as_secs()
        )),
        ExecutionError::ShuttingDown => PlatformError::Internal(format!(
            "{subject} execution aborted: platform is shutting down"
        )),
        ExecutionError::NotStarted => PlatformError::Internal(format!(
            "{subject} execution not started: platform is shutting down"
        )),
        ExecutionError::Overloaded {
            retry_after, ran, ..
        } => retry_later(retry_after, ran),
        other => PlatformError::Internal(format!("{subject} execution error: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn execution_errors_keep_the_client_visible_strings() {
        let timeout = map_execution_error(
            "WASM",
            ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(30),
            },
        );
        assert_eq!(
            timeout.to_string(),
            "Internal error: WASM execution timed out after 30s"
        );
        let hook = map_execution_error(
            "Hook",
            ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(30),
            },
        );
        assert_eq!(
            hook.to_string(),
            "Internal error: Hook execution timed out after 30s"
        );
        let trap = map_execution_error("WASM", ExecutionError::Trap("unreachable".into()));
        assert_eq!(
            trap.to_string(),
            "Internal error: WASM execution error: WASM trap: unreachable"
        );
        assert!(map_execution_error("WASM", ExecutionError::ShuttingDown)
            .to_string()
            .contains("shutting down"));
        assert_eq!(
            map_execution_error("WASM", ExecutionError::Cancelled).to_string(),
            "Internal error: WASM execution error: WASM execution cancelled"
        );
    }

    #[test]
    fn capacity_is_a_retryable_503_that_says_whether_the_handler_ran() {
        use axum::response::IntoResponse;
        let answer = |ran| {
            map_execution_error(
                "WASM",
                ExecutionError::Overloaded {
                    reason: crate::wasm_runtime::budget::Reason::RouteConcurrency,
                    retry_after: Duration::from_secs(2),
                    ran,
                },
            )
            .into_response()
        };
        for (ran, message) in [
            (false, "guest capacity exhausted; the request was not run"),
            (
                true,
                "guest capacity exhausted while the request ran; it may have been partly applied",
            ),
        ] {
            let response = answer(ran);
            assert_eq!(
                response.status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(
                response
                    .headers()
                    .get(axum::http::header::RETRY_AFTER)
                    .map(|v| v.as_bytes()),
                Some(&b"2"[..])
            );
            assert_eq!(
                response
                    .extensions()
                    .get::<crate::errors::PlatformErrorBody>()
                    .map(|body| body.message.as_str()),
                Some(message)
            );
        }
        assert_eq!(
            map_execution_error(
                "WASM",
                ExecutionError::MemoryLimit {
                    requested: 2,
                    limit: 1
                }
            )
            .into_response()
            .status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn a_guest_reads_its_request_with_its_keys_in_order() {
        let request = GuestRequest {
            method: "POST".into(),
            handler: "h".into(),
            tenant: "acme".into(),
            service: "svc".into(),
            body: serde_json::json!({"z": 1, "a": {"y": 2, "b": 3}}),
            auth: Some(Value::Null),
            ..GuestRequest::default()
        };
        assert_eq!(
            guest_text(&request).expect("text"),
            r#"{"auth":null,"body":{"a":{"b":3,"y":2},"z":1},"handler":"h","method":"POST","service":"svc","tenant":"acme"}"#
        );
        assert_eq!(
            guest_json(&request).expect("value").to_string(),
            guest_text(&request).expect("text")
        );
    }

    #[test]
    fn a_charged_request_text_keeps_only_the_text_charged() {
        use crate::wasm_runtime::budget::testing::{self, MIB};
        let request = GuestRequest {
            method: "POST".into(),
            handler: "h".into(),
            body: serde_json::json!({"rows": vec![[0]; 200_000]}),
            body_raw_b64: Some("QUJD".repeat(MIB)),
            ..GuestRequest::default()
        };
        let (ledger, mut hold) = testing::hold(64 * MIB);
        let text = charged_guest_text(&request, &mut hold).expect("fits");
        assert_eq!(
            text,
            serde_json::to_value(&request).expect("value").to_string()
        );
        assert_eq!(hold.bytes(), ledger.in_use());
        assert!(hold.bytes() >= text.len() && hold.bytes() < text.len() + 2 * MIB);

        let (ledger, mut hold) = testing::hold(4 * MIB);
        assert!(matches!(
            charged_guest_text(&request, &mut hold),
            Err(ChargedJsonError::Capacity(_))
        ));
        assert_eq!(ledger.in_use(), 0);
    }
}
