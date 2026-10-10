//! Calls to the extensions of the tenant: capabilities a project adds to
//! Cufflink without a platform release, served by a native service or a
//! NATS provider. Needs a platform that supports `ext_call`; the `ext-call`
//! feature declares that in the module, and the service lists every
//! extension it calls in `service! { extensions: [...] }`.
//!
//! ```rust,ignore
//! let token = ext::call("google.token", &json!({ "scope": "analytics.readonly" }))?;
//! let pdf = ext::call_with_timeout("pdf.render", &doc, 5_000)?;
//! ```

pub use cufflink_types::host::ext::CallErrorKind;
use cufflink_types::host::ext::{CallErrorEnvelope, CallRequest, CallResponse};
use serde_json::Value;
use std::fmt;

/// Why an extension call brought no value back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtError {
    /// The service's manifest does not list the extension.
    NotDeclared,
    /// No extension of the name is registered for the tenant.
    NotFound,
    /// The extension's registration does not list this service.
    Denied,
    /// Nothing serves the extension now, or what does answered nonsense.
    Unavailable(String),
    Timeout,
    /// The call or its reply is larger than the platform carries.
    TooLarge,
    /// The extension answered with an error of its own.
    Failed {
        code: String,
        message: String,
    },
    Invalid(String),
}

impl fmt::Display for ExtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtError::NotDeclared => f.write_str(
                "the service does not declare the extension; list it in service! { extensions: [...] }",
            ),
            ExtError::NotFound => f.write_str("no extension of the name is registered for the tenant"),
            ExtError::Denied => {
                f.write_str("the extension's registration does not list this service as a caller")
            }
            ExtError::Unavailable(message) => write!(f, "the extension is unavailable: {message}"),
            ExtError::Timeout => f.write_str("the extension did not answer in time"),
            ExtError::TooLarge => {
                f.write_str("the call or its reply is larger than the platform carries")
            }
            ExtError::Failed { code, message } => write!(f, "the extension failed ({code}): {message}"),
            ExtError::Invalid(message) => write!(f, "the call is invalid: {message}"),
        }
    }
}

impl std::error::Error for ExtError {}

/// Call the extension `name` with `payload`, waiting as long as the
/// platform's default allows.
pub fn call(name: &str, payload: &Value) -> Result<Value, ExtError> {
    request(name, payload, None)
}

/// [`call`], waiting at most `timeout_ms`, and never past what is left of
/// the handler's own budget.
pub fn call_with_timeout(name: &str, payload: &Value, timeout_ms: u64) -> Result<Value, ExtError> {
    request(name, payload, Some(timeout_ms))
}

fn request(name: &str, payload: &Value, timeout_ms: Option<u64>) -> Result<Value, ExtError> {
    let request = CallRequest {
        extension: name.to_string(),
        payload: payload.clone(),
        timeout_ms,
    };
    match exchange(&request)? {
        CallResponse::Ok(value) => Ok(value),
        CallResponse::Error(envelope) => Err(refusal(envelope)),
    }
}

/// A `failed` without the extension's code carries `failed` as its code.
fn refusal(
    CallErrorEnvelope {
        kind,
        code,
        message,
    }: CallErrorEnvelope,
) -> ExtError {
    match kind {
        CallErrorKind::NotDeclared => ExtError::NotDeclared,
        CallErrorKind::NotFound => ExtError::NotFound,
        CallErrorKind::Denied => ExtError::Denied,
        CallErrorKind::Unavailable => ExtError::Unavailable(message),
        CallErrorKind::Timeout => ExtError::Timeout,
        CallErrorKind::TooLarge => ExtError::TooLarge,
        CallErrorKind::Failed => ExtError::Failed {
            code: code.unwrap_or_else(|| kind.as_str().to_string()),
            message,
        },
        CallErrorKind::Invalid => ExtError::Invalid(message),
    }
}

#[cfg(target_arch = "wasm32")]
fn exchange(request: &CallRequest) -> Result<CallResponse, ExtError> {
    use crate::envelope::{call, Failure};

    #[link(wasm_import_module = "env")]
    extern "C" {
        fn ext_call(ptr: i32, len: i32) -> i32;
    }
    call(ext_call, request).map_err(|failure| match failure {
        Failure::Encode(_) => ExtError::Invalid(failure.to_string()),
        Failure::Refused => ExtError::TooLarge,
        Failure::Malformed(_) => ExtError::Unavailable(failure.to_string()),
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn exchange(request: &CallRequest) -> Result<CallResponse, ExtError> {
    crate::host::ext()
        .map(|host| host.call(request))
        .ok_or_else(|| {
            ExtError::Unavailable(
                "extensions are only available inside the Cufflink platform".into(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn envelope(kind: CallErrorKind, code: Option<&str>) -> CallErrorEnvelope {
        CallErrorEnvelope {
            kind,
            code: code.map(str::to_string),
            message: "why".into(),
        }
    }

    #[test]
    fn outside_the_platform_extensions_are_unavailable() {
        let unavailable = ExtError::Unavailable(
            "extensions are only available inside the Cufflink platform".into(),
        );
        assert_eq!(call("google.token", &json!({})), Err(unavailable.clone()));
        assert_eq!(
            call_with_timeout("google.token", &json!({}), 5_000),
            Err(unavailable.clone())
        );
        assert_eq!(
            unavailable.to_string(),
            "the extension is unavailable: extensions are only available inside the Cufflink \
             platform"
        );
    }

    #[test]
    fn every_refusal_reaches_the_handler_as_its_kind() {
        for (kind, error) in [
            (CallErrorKind::NotDeclared, ExtError::NotDeclared),
            (CallErrorKind::NotFound, ExtError::NotFound),
            (CallErrorKind::Denied, ExtError::Denied),
            (
                CallErrorKind::Unavailable,
                ExtError::Unavailable("why".into()),
            ),
            (CallErrorKind::Timeout, ExtError::Timeout),
            (CallErrorKind::TooLarge, ExtError::TooLarge),
            (
                CallErrorKind::Failed,
                ExtError::Failed {
                    code: "failed".into(),
                    message: "why".into(),
                },
            ),
            (CallErrorKind::Invalid, ExtError::Invalid("why".into())),
        ] {
            assert_eq!(refusal(envelope(kind, None)), error, "{}", kind.as_str());
        }
        assert_eq!(
            refusal(envelope(CallErrorKind::Failed, Some("quota"))),
            ExtError::Failed {
                code: "quota".into(),
                message: "why".into(),
            }
        );
    }

    #[test]
    fn errors_read_as_sentences() {
        for (error, text) in [
            (
                ExtError::NotDeclared,
                "the service does not declare the extension; list it in service! { extensions: \
                 [...] }",
            ),
            (
                ExtError::NotFound,
                "no extension of the name is registered for the tenant",
            ),
            (
                ExtError::Denied,
                "the extension's registration does not list this service as a caller",
            ),
            (ExtError::Timeout, "the extension did not answer in time"),
            (
                ExtError::TooLarge,
                "the call or its reply is larger than the platform carries",
            ),
            (
                ExtError::Failed {
                    code: "quota".into(),
                    message: "daily quota exhausted".into(),
                },
                "the extension failed (quota): daily quota exhausted",
            ),
            (ExtError::Invalid("bad".into()), "the call is invalid: bad"),
        ] {
            assert_eq!(error.to_string(), text);
        }
    }
}
