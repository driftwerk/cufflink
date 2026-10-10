//! `nats_publish_v2`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishRequest {
    pub subject: String,
    #[serde(rename = "payload_b64", with = "crate::serde_util::base64_bytes")]
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    /// Sent as `Nats-Msg-Id`: the stream drops a repeat inside its
    /// duplicate window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<String>,
    #[serde(default, skip_serializing_if = "PublishMode::is_jetstream")]
    pub mode: PublishMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u32>,
}

/// Header names a guest may not set: the server's and the platform's.
pub const RESERVED_HEADER_PREFIXES: [&str; 2] = ["nats-", "cufflink-"];

/// The header a stream deduplicates a message by.
pub const MSG_ID_HEADER: &str = "Nats-Msg-Id";

/// Whether `text`, a header's name or value, names [`MSG_ID_HEADER`].
/// Beside a message id it would hide the id from NATS servers before
/// 2.12.4, which take a header to be where its name first appears among
/// the headers, and then find none.
pub fn names_msg_id(text: &str) -> bool {
    text.contains(MSG_ID_HEADER)
}

/// Whether `name` starts with `prefix`, whatever the case of either.
pub fn header_has_prefix(name: &str, prefix: &str) -> bool {
    name.get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

pub fn is_reserved_header(name: &str) -> bool {
    RESERVED_HEADER_PREFIXES
        .iter()
        .any(|prefix| header_has_prefix(name, prefix))
}

fn value_violation(what: &str, value: &str) -> Option<String> {
    value
        .chars()
        .any(char::is_control)
        .then(|| format!("{what} has a control character in its value; a value is one line"))
}

/// Why a header cannot travel, if it cannot: a name is one or more visible
/// ASCII characters and no `:`, a value one line.
pub fn header_violation(name: &str, value: &str) -> Option<String> {
    if name.is_empty() {
        return Some("a header has no name".to_string());
    }
    if name.chars().any(|c| !c.is_ascii_graphic() || c == ':') {
        return Some(format!(
            "header {name:?} is not a header name (visible ASCII characters, no ':')"
        ));
    }
    value_violation(&format!("header '{name}'"), value)
}

impl PublishRequest {
    /// Why the platform refuses the request's headers or message id, if
    /// it does.
    pub fn violation(&self) -> Option<String> {
        self.headers
            .iter()
            .find_map(|(name, value)| {
                header_violation(name, value).or_else(|| {
                    is_reserved_header(name).then(|| {
                        format!("header '{name}' is the server's or the platform's to set")
                    })
                })
            })
            .or_else(|| {
                self.msg_id
                    .as_deref()
                    .and_then(|id| value_violation("the message id", id))
            })
            .or_else(|| self.hiding_the_msg_id())
    }

    /// Why a header would hide the request's message id, if one would.
    fn hiding_the_msg_id(&self) -> Option<String> {
        self.msg_id.as_deref().filter(|id| !id.is_empty())?;
        self.headers
            .iter()
            .find(|(name, value)| names_msg_id(name) || names_msg_id(value))
            .map(|(name, _)| {
                format!(
                    "header '{name}' names {MSG_ID_HEADER}, which would hide the message id from \
                     the server"
                )
            })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishMode {
    /// Acknowledged by the stream that captures the subject.
    #[default]
    #[serde(rename = "jetstream")]
    JetStream,
    Core,
}

impl PublishMode {
    pub fn is_jetstream(&self) -> bool {
        *self == PublishMode::JetStream
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublishResponse {
    Ack {
        stream: String,
        sequence: u64,
        duplicate: bool,
    },
    Sent,
    Error {
        kind: PublishErrorKind,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishErrorKind {
    Denied,
    NoStream,
    Timeout,
    Unavailable,
    Invalid,
    TooLarge,
}

impl PublishErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PublishErrorKind::Denied => "denied",
            PublishErrorKind::NoStream => "no_stream",
            PublishErrorKind::Timeout => "timeout",
            PublishErrorKind::Unavailable => "unavailable",
            PublishErrorKind::Invalid => "invalid",
            PublishErrorKind::TooLarge => "too_large",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::tests::{refused, round_trip};
    use serde_json::json;

    #[test]
    fn a_minimal_request_carries_only_what_it_sets() {
        round_trip(
            &PublishRequest {
                subject: "books.created".into(),
                payload: b"{}".to_vec(),
                ..Default::default()
            },
            json!({"subject": "books.created", "payload_b64": "e30="}),
        );
    }

    #[test]
    fn a_full_request_round_trips() {
        round_trip(
            &PublishRequest {
                subject: "books.created".into(),
                payload: vec![0, 255],
                headers: vec![
                    ("x-source".into(), "a".into()),
                    ("x-source".into(), "b".into()),
                ],
                msg_id: Some("book-7".into()),
                mode: PublishMode::Core,
                timeout_ms: Some(2500),
            },
            json!({
                "subject": "books.created",
                "payload_b64": "AP8=",
                "headers": [["x-source", "a"], ["x-source", "b"]],
                "msg_id": "book-7",
                "mode": "core",
                "timeout_ms": 2500
            }),
        );
        assert_eq!(
            serde_json::to_value(PublishMode::JetStream).expect("serialize"),
            json!("jetstream")
        );
    }

    #[test]
    fn responses_are_tagged_by_result() {
        round_trip(
            &PublishResponse::Ack {
                stream: "BOOKS".into(),
                sequence: 81234,
                duplicate: false,
            },
            json!({"result": "ack", "stream": "BOOKS", "sequence": 81234, "duplicate": false}),
        );
        round_trip(&PublishResponse::Sent, json!({"result": "sent"}));
        for kind in [
            PublishErrorKind::Denied,
            PublishErrorKind::NoStream,
            PublishErrorKind::Timeout,
            PublishErrorKind::Unavailable,
            PublishErrorKind::Invalid,
            PublishErrorKind::TooLarge,
        ] {
            round_trip(
                &PublishResponse::Error {
                    kind,
                    message: "why".into(),
                },
                json!({"result": "error", "kind": kind.as_str(), "message": "why"}),
            );
        }
    }

    fn carrying(headers: &[(&str, &str)], msg_id: Option<&str>) -> PublishRequest {
        PublishRequest {
            subject: "books.created".into(),
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            msg_id: msg_id.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn a_header_that_cannot_travel_is_refused_by_name() {
        for name in [
            "", "x bad", "x:bad", "x\rbad", "x\nbad", "x\0bad", "x\tbad", "naïve", "x-ok ",
            " x-ok", "\u{7f}",
        ] {
            let refused = carrying(&[("x-fine", "1"), (name, "v")], None).violation();
            assert!(refused.is_some(), "{name:?}");
            assert!(
                refused
                    .iter()
                    .all(|reason| !reason.contains(['\r', '\n', '\0'])),
                "{refused:?} is one line"
            );
        }
        for value in [
            "a\r\nNats-Expected-Stream: OTHER",
            "a\nb",
            "a\rb",
            "a\0b",
            "\u{1b}[0m",
        ] {
            assert_eq!(
                carrying(&[("x-source", value)], None)
                    .violation()
                    .as_deref(),
                Some("header 'x-source' has a control character in its value; a value is one line"),
                "{value:?}"
            );
            assert_eq!(
                carrying(&[], Some(value)).violation().as_deref(),
                Some("the message id has a control character in its value; a value is one line"),
                "{value:?}"
            );
        }
        for name in [
            "Nats-Msg-Id",
            "nats-expected-stream",
            "Cufflink-Reply",
            "CUFFLINK-DLQ-REASON",
        ] {
            assert_eq!(
                carrying(&[(name, "x")], None).violation(),
                Some(format!(
                    "header '{name}' is the server's or the platform's to set"
                ))
            );
        }
    }

    #[test]
    fn what_can_travel_is_not_refused() {
        assert_eq!(
            carrying(
                &[
                    ("x-source", "a"),
                    ("x-source", ""),
                    ("X_Any.Visible!Name", "naïve value, with spaces"),
                    ("x-nats-like", "1"),
                    ("traceparent", "00-abc-def-01"),
                ],
                Some("book 7: naïve"),
            )
            .violation(),
            None
        );
        assert_eq!(carrying(&[], Some("")).violation(), None);
        assert!(header_has_prefix("Nats-Msg-Id", "nats-"));
        assert!(!header_has_prefix("nat", "nats-"));
        assert!(
            !header_has_prefix("näts-x", "nats-"),
            "no boundary inside a character"
        );
    }

    #[test]
    fn a_header_that_names_the_message_id_is_refused_beside_one() {
        for (name, value) in [
            ("X-Original-Nats-Msg-Id", "book-7"),
            ("x-note", "copied from Nats-Msg-Id"),
            ("x-note", "Nats-Msg-Id: book-7"),
        ] {
            assert_eq!(
                carrying(
                    &[("traceparent", "00-abc-def-01"), (name, value)],
                    Some("book-8")
                )
                .violation(),
                Some(format!(
                    "header '{name}' names Nats-Msg-Id, which would hide the message id from the \
                     server"
                ))
            );
            for msg_id in [None, Some("")] {
                assert_eq!(carrying(&[(name, value)], msg_id).violation(), None);
            }
        }
        assert_eq!(
            carrying(&[("x-note", "nats-msg-id")], Some("book-8")).violation(),
            None,
            "the server matches a header's name as it is spelt"
        );
    }

    #[test]
    fn unknown_tags_kinds_modes_and_payloads_are_refused() {
        refused::<PublishResponse>(json!({"result": "queued"}));
        refused::<PublishResponse>(json!({"result": "error", "kind": "exploded", "message": ""}));
        refused::<PublishResponse>(json!({"stream": "BOOKS", "sequence": 1, "duplicate": false}));
        refused::<PublishRequest>(json!({"subject": "a", "payload_b64": "e30=", "mode": "queue"}));
        refused::<PublishRequest>(json!({"subject": "a", "payload_b64": "not base64!"}));
        refused::<PublishRequest>(json!({"subject": "a", "payload_b64": "e30=", "retries": 3}));
        refused::<PublishRequest>(json!({"subject": "a"}));
    }
}
