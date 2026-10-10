//! Publish messages to NATS for event-driven communication.
//!
//! Use this to notify other services, trigger subscriptions, or emit
//! domain events. [`publish`] and [`request`] are core NATS: sent once,
//! kept by no stream unless one captures the subject. With the
//! `nats-publish-v2` feature, [`publish_with`], [`publish_json`] and
//! [`publish_core`] say what became of the message and carry headers and a
//! message id.

#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};
#[cfg(feature = "nats-publish-v2")]
pub use cufflink_types::host::nats::PublishErrorKind;
#[cfg(feature = "nats-publish-v2")]
use cufflink_types::host::nats::{PublishMode, PublishRequest, PublishResponse};
#[cfg(feature = "nats-publish-v2")]
use std::fmt;

/// Publish a message to a NATS subject.
///
/// Returns `true` on success, `false` on failure.
///
/// ```rust,ignore
/// nats::publish(
///     "dw.acme.order-service.orders.created",
///     &serde_json::json!({"order_id": "abc", "total": 4500}).to_string(),
/// );
/// ```
pub fn publish(subject: &str, payload: &str) -> bool {
    #[cfg(target_arch = "wasm32")]
    {
        let subj_bytes = subject.as_bytes();
        let payload_bytes = payload.as_bytes();
        let result = unsafe {
            ffi::nats_publish(
                subj_bytes.as_ptr() as i32,
                subj_bytes.len() as i32,
                payload_bytes.as_ptr() as i32,
                payload_bytes.len() as i32,
            )
        };
        result == 0
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::nats().is_none_or(|host| {
            !matches!(
                host.publish(&cufflink_types::host::nats::PublishRequest {
                    subject: subject.to_string(),
                    payload: payload.as_bytes().to_vec(),
                    mode: cufflink_types::host::nats::PublishMode::Core,
                    ..Default::default()
                }),
                cufflink_types::host::nats::PublishResponse::Error { .. }
            )
        })
    }
}

/// Send a NATS request and wait for a reply (synchronous request-reply).
///
/// Returns the reply payload as a string, or `None` on timeout/failure.
///
/// ```rust,ignore
/// let reply = nats::request(
///     "dw.acme.user-service.users.lookup",
///     &serde_json::json!({"customer_id": "abc"}).to_string(),
///     5000, // timeout in ms
/// );
/// ```
pub fn request(subject: &str, payload: &str, timeout_ms: i32) -> Option<String> {
    #[cfg(target_arch = "wasm32")]
    {
        let subj_bytes = subject.as_bytes();
        let payload_bytes = payload.as_bytes();
        let result = unsafe {
            ffi::nats_request(
                subj_bytes.as_ptr() as i32,
                subj_bytes.len() as i32,
                payload_bytes.as_ptr() as i32,
                payload_bytes.len() as i32,
                timeout_ms,
            )
        };
        if result != 0 {
            return None;
        }
        host_buffer::read_host_string()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let reply = crate::host::nats()?.request(
            subject,
            payload.as_bytes(),
            u32::try_from(timeout_ms).unwrap_or_default(),
        )?;
        String::from_utf8(reply).ok()
    }
}

/// How a message is published: its headers, its id and how long to wait
/// for the stream's acknowledgement.
#[cfg(feature = "nats-publish-v2")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublishOptions {
    headers: Vec<(String, String)>,
    msg_id: Option<String>,
    timeout_ms: Option<u32>,
}

#[cfg(feature = "nats-publish-v2")]
impl PublishOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a header; one added twice is sent twice. A header the server
    /// or the platform sets (`Nats-*`, `Cufflink-*`) is refused when the
    /// message is published.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// Sent as `Nats-Msg-Id`: the stream stores a message once for each
    /// id inside its duplicate window and answers the repeat with
    /// `duplicate`. Derive it from what the message is for
    /// (`format!("{}:{}", sub.stream(), sub.sequence())`), so a redelivery
    /// publishes once.
    pub fn msg_id(mut self, id: &str) -> Self {
        self.msg_id = Some(id.to_string());
        self
    }

    /// How long to wait for the stream's acknowledgement; the platform's
    /// default when unset or `0`, and never past what is left of the
    /// handler's own budget.
    pub fn timeout_ms(mut self, ms: u32) -> Self {
        self.timeout_ms = Some(ms);
        self
    }
}

/// The stream's acknowledgement.
#[cfg(feature = "nats-publish-v2")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubAck {
    pub stream: String,
    pub sequence: u64,
    /// The stream had a message of this id already and kept that one.
    pub duplicate: bool,
}

/// Why the message was not published.
#[cfg(feature = "nats-publish-v2")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishError {
    pub kind: PublishErrorKind,
    pub message: String,
}

#[cfg(feature = "nats-publish-v2")]
impl PublishError {
    fn new(kind: PublishErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// What every call returns where there is no platform to publish
    /// through.
    #[cfg(not(target_arch = "wasm32"))]
    fn unavailable() -> Self {
        Self::new(
            PublishErrorKind::Unavailable,
            "NATS is only available inside the Cufflink platform",
        )
    }
}

#[cfg(feature = "nats-publish-v2")]
impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

#[cfg(feature = "nats-publish-v2")]
impl std::error::Error for PublishError {}

/// Publish `payload` on `subject` and wait for the stream that captures
/// it to acknowledge. A subject no stream captures fails with
/// [`PublishErrorKind::NoStream`] without being sent: use
/// [`publish_core`] for one only core subscribers listen on.
///
/// ```rust,ignore
/// let ack = nats::publish_with(
///     "books.created",
///     &serde_json::to_vec(&book)?,
///     &PublishOptions::new().msg_id(&format!("book:{}", book.id)),
/// )?;
/// ```
#[cfg(feature = "nats-publish-v2")]
pub fn publish_with(
    subject: &str,
    payload: &[u8],
    opts: &PublishOptions,
) -> Result<PubAck, PublishError> {
    match exchange(&request_of(subject, payload, opts, PublishMode::JetStream))? {
        PublishResponse::Ack {
            stream,
            sequence,
            duplicate,
        } => Ok(PubAck {
            stream,
            sequence,
            duplicate,
        }),
        PublishResponse::Sent => Err(PublishError::new(
            PublishErrorKind::Unavailable,
            "the platform sent the message without a stream's acknowledgement",
        )),
        PublishResponse::Error { kind, message } => Err(PublishError { kind, message }),
    }
}

/// [`publish_with`] with `value` as JSON.
#[cfg(feature = "nats-publish-v2")]
pub fn publish_json<T: serde::Serialize>(
    subject: &str,
    value: &T,
    opts: &PublishOptions,
) -> Result<PubAck, PublishError> {
    let payload = serde_json::to_vec(value).map_err(|e| {
        PublishError::new(
            PublishErrorKind::Invalid,
            format!("the payload could not be encoded as JSON: {e}"),
        )
    })?;
    publish_with(subject, &payload, opts)
}

/// Send `payload` on `subject` to whoever listens now, without waiting
/// for any stream. `Ok` says it was sent, not that anyone received it.
#[cfg(feature = "nats-publish-v2")]
pub fn publish_core(
    subject: &str,
    payload: &[u8],
    headers: &[(&str, &str)],
) -> Result<(), PublishError> {
    let opts = headers
        .iter()
        .fold(PublishOptions::new(), |opts, (name, value)| {
            opts.header(name, value)
        });
    match exchange(&request_of(subject, payload, &opts, PublishMode::Core))? {
        PublishResponse::Sent | PublishResponse::Ack { .. } => Ok(()),
        PublishResponse::Error { kind, message } => Err(PublishError { kind, message }),
    }
}

#[cfg(feature = "nats-publish-v2")]
fn request_of(
    subject: &str,
    payload: &[u8],
    opts: &PublishOptions,
    mode: PublishMode,
) -> PublishRequest {
    PublishRequest {
        subject: subject.to_string(),
        payload: payload.to_vec(),
        headers: opts.headers.clone(),
        msg_id: opts.msg_id.clone(),
        mode,
        timeout_ms: opts.timeout_ms,
    }
}

#[cfg(all(feature = "nats-publish-v2", target_arch = "wasm32"))]
fn exchange(request: &PublishRequest) -> Result<PublishResponse, PublishError> {
    use crate::envelope::{call, Failure};

    #[link(wasm_import_module = "env")]
    extern "C" {
        fn nats_publish_v2(ptr: i32, len: i32) -> i32;
    }
    call(nats_publish_v2, request).map_err(|failure| {
        let kind = match failure {
            Failure::Encode(_) => PublishErrorKind::Invalid,
            Failure::Refused => PublishErrorKind::TooLarge,
            Failure::Malformed(_) => PublishErrorKind::Unavailable,
        };
        PublishError::new(kind, failure.to_string())
    })
}

#[cfg(all(feature = "nats-publish-v2", not(target_arch = "wasm32")))]
fn exchange(request: &PublishRequest) -> Result<PublishResponse, PublishError> {
    crate::host::nats()
        .map(|host| host.publish(request))
        .ok_or_else(PublishError::unavailable)
}

#[cfg(test)]
mod tests {
    use crate::nats;

    #[test]
    fn test_nats_publish_noop_on_native() {
        let ok = nats::publish("test.subject", "payload");
        assert!(ok);
        assert_eq!(nats::request("test.subject", "payload", 100), None);
    }

    #[cfg(feature = "nats-publish-v2")]
    mod v2 {
        use super::super::*;
        use serde_json::json;

        #[test]
        fn a_request_is_the_envelope_the_platform_reads() {
            let opts = PublishOptions::new()
                .header("x-source", "a")
                .header("x-source", "b")
                .msg_id("book-7")
                .timeout_ms(1500);
            assert_eq!(
                serde_json::to_value(request_of(
                    "books.created",
                    &[0, 255],
                    &opts,
                    PublishMode::JetStream
                ))
                .expect("json"),
                json!({
                    "subject": "books.created",
                    "payload_b64": "AP8=",
                    "headers": [["x-source", "a"], ["x-source", "b"]],
                    "msg_id": "book-7",
                    "timeout_ms": 1500,
                })
            );
            assert_eq!(
                serde_json::to_value(request_of(
                    "books.created",
                    b"{}",
                    &PublishOptions::new(),
                    PublishMode::Core
                ))
                .expect("json"),
                json!({"subject": "books.created", "payload_b64": "e30=", "mode": "core"})
            );
        }

        #[test]
        fn outside_the_platform_nats_is_unavailable() {
            for error in [
                publish_with("books.created", b"{}", &PublishOptions::new()).unwrap_err(),
                publish_json("books.created", &json!({}), &PublishOptions::new()).unwrap_err(),
                publish_core("books.created", b"{}", &[]).unwrap_err(),
            ] {
                assert_eq!(error.kind, PublishErrorKind::Unavailable);
                assert_eq!(
                    error.to_string(),
                    "unavailable: NATS is only available inside the Cufflink platform"
                );
            }
        }

        #[test]
        fn a_value_that_is_not_json_is_refused_before_anything_is_sent() {
            let mut unencodable = std::collections::HashMap::new();
            unencodable.insert(vec![1], 1);
            assert_eq!(
                publish_json("books.created", &unencodable, &PublishOptions::new())
                    .unwrap_err()
                    .kind,
                PublishErrorKind::Invalid
            );
        }
    }
}
