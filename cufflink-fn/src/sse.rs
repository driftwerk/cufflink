//! Server-sent event streams bridged from NATS. A route declared with
//! `sse(subject = "dw.{tenant}.…", reducer = "…")` returns
//! `Response::sse(SseSpec { … })` (feature `response-v2`) with its initial
//! events; the platform then streams the subject's messages, each through the
//! reducer when one is declared.
//!
//! ```rust,ignore
//! handler!(receipt_progress_reduce, |req: Request| sse::reduce(req, |state, message| {
//!     let done = state["done"].as_u64().unwrap_or(0) + 1;
//!     Reduction {
//!         events: vec![SseEvent::data(message.data)],
//!         close: done == state["total"].as_u64().unwrap_or(0),
//!         state: json!({ "done": done, "total": state["total"] }),
//!     }
//! }));
//! ```

use crate::{Request, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// The method of the request a reducer receives.
pub const REDUCER_METHOD: &str = cufflink_types::SseRouteDef::REDUCER_METHOD;

/// One event: `data` is sent as-is when it is a string and as JSON
/// otherwise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SseEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl SseEvent {
    /// A data-only event.
    pub fn data(data: Value) -> Self {
        Self {
            event: None,
            data,
            id: None,
        }
    }

    /// An event of type `event`.
    pub fn named(event: &str, data: Value) -> Self {
        Self {
            event: Some(event.to_string()),
            ..Self::data(data)
        }
    }
}

/// An event sent every `interval_secs`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SsePing {
    pub interval_secs: u32,
    pub event: SseEvent,
}

/// How the platform runs the stream.
#[derive(Debug, Clone, PartialEq)]
pub struct SseSpec {
    /// Sent first: the snapshot the handler built.
    pub initial: Vec<SseEvent>,
    /// The reducer's state before the first message.
    pub state: Value,
    /// Seconds between keep-alive comments.
    pub keep_alive_secs: u32,
    pub ping: Option<SsePing>,
    /// Seconds after which the stream closes, 1 to 3600.
    pub timeout_secs: u32,
    /// Response headers, such as `cache-control`.
    pub headers: Vec<(String, String)>,
}

impl Default for SseSpec {
    fn default() -> Self {
        Self {
            initial: Vec::new(),
            state: Value::Null,
            keep_alive_secs: 15,
            ping: None,
            timeout_secs: 300,
            headers: Vec::new(),
        }
    }
}

impl SseSpec {
    /// The `__sse` member of the response envelope.
    #[cfg(feature = "response-v2")]
    pub(crate) fn envelope(&self) -> Value {
        serde_json::json!({
            "initial": self.initial,
            "state": self.state,
            "keep_alive_secs": self.keep_alive_secs,
            "ping": self.ping,
            "timeout_secs": self.timeout_secs,
        })
    }
}

/// A message from the bridged subject.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SseMessage {
    pub subject: String,
    /// The payload parsed as JSON, or as a string when it is not JSON.
    pub data: Value,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// What a reducer makes of a message: the events to send, the state for the
/// next one, and whether the stream ends after these events.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Reduction {
    pub events: Vec<SseEvent>,
    pub state: Value,
    pub close: bool,
}

/// Run a reducer: `f` receives the stream's state and the message. A
/// request that is not a reducer call is answered 400.
pub fn reduce(req: Request, f: impl FnOnce(Value, SseMessage) -> Reduction) -> Response {
    if req.method() != REDUCER_METHOD {
        return Response::error("not an event-stream reducer call");
    }
    let body = req.body();
    match serde_json::from_value::<SseMessage>(body["message"].clone()) {
        Ok(message) => Response::json(
            &serde_json::to_value(f(body["state"].clone(), message)).unwrap_or(Value::Null),
        ),
        Err(e) => Response::error(&format!("invalid event-stream message: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reducer_request(state: Value, message: Value) -> Request {
        Request::from_json(
            &json!({"method": REDUCER_METHOD, "body": {"state": state, "message": message}})
                .to_string(),
        )
        .expect("request")
    }

    #[test]
    fn reduce_hands_the_state_and_message_to_the_reducer() {
        let response = reduce(
            reducer_request(
                json!({"done": 1, "total": 2}),
                json!({"subject": "dw.acme.q.7", "data": {"receipt_id": "q2"}, "headers": {"k": "v"}}),
            ),
            |state, message| {
                assert_eq!(message.subject, "dw.acme.q.7");
                assert_eq!(message.headers["k"], "v");
                Reduction {
                    events: vec![SseEvent::named("receipt", message.data)],
                    close: state["done"] == 1,
                    state: json!({"done": 2}),
                }
            },
        );
        assert_eq!(
            serde_json::from_str::<Value>(&response.into_data()).expect("json"),
            json!({"events": [{"event": "receipt", "data": {"receipt_id": "q2"}}], "state": {"done": 2}, "close": true})
        );
    }

    #[test]
    fn reduce_refuses_calls_that_are_not_reductions() {
        let not_a_reducer = Request::from_json(r#"{"method": "GET"}"#).expect("request");
        let response = reduce(not_a_reducer, |_, _| unreachable!("not called"));
        assert!(response.into_data().contains("\"__status\":400"));
        let malformed = reduce(reducer_request(Value::Null, json!("no")), |_, _| {
            unreachable!("not called")
        });
        assert!(malformed
            .into_data()
            .contains("invalid event-stream message"));
    }

    #[cfg(feature = "response-v2")]
    #[test]
    fn specs_default_their_timings() {
        let spec = SseSpec {
            initial: vec![SseEvent::data(json!({"type": "snapshot"}))],
            ping: Some(SsePing {
                interval_secs: 30,
                event: SseEvent::data(json!({"type": "ping"})),
            }),
            ..SseSpec::default()
        };
        assert_eq!(
            spec.envelope(),
            json!({
                "initial": [{"data": {"type": "snapshot"}}],
                "state": null,
                "keep_alive_secs": 15,
                "ping": {"interval_secs": 30, "event": {"data": {"type": "ping"}}},
                "timeout_secs": 300,
            })
        );
    }
}
