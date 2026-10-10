//! A NATS that records what is published and answers as told.

use crate::host::nats::NatsHost;
use cufflink_types::host::nats::{PublishErrorKind, PublishMode, PublishRequest, PublishResponse};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

/// The stream that acknowledges a publish when no stream is scripted.
pub const DEFAULT_STREAM: &str = "TEST";

/// NATS subject matching: `*` matches one token, a final `>` one or more.
fn subject_matches(pattern: &str, subject: &str) -> bool {
    let pattern: Vec<&str> = pattern.split('.').collect();
    let subject: Vec<&str> = subject.split('.').collect();
    pattern.iter().enumerate().all(|(i, token)| match *token {
        ">" => i + 1 == pattern.len() && i < subject.len(),
        "*" => i < subject.len(),
        literal => subject.get(i) == Some(&literal),
    }) && (pattern.len() == subject.len() || pattern.last() == Some(&">"))
}

#[derive(Default)]
struct Recorded {
    /// Which stream captures which subjects; every subject when empty.
    streams: Vec<(String, String)>,
    replies: Vec<(String, Vec<u8>)>,
    published: Vec<PublishRequest>,
    requests: Vec<(String, Vec<u8>)>,
    sequences: BTreeMap<String, u64>,
    /// The sequence each message id was stored at, by stream.
    ids: BTreeMap<String, BTreeMap<String, u64>>,
}

impl Recorded {
    fn capturing(&self, subject: &str) -> Option<String> {
        if self.streams.is_empty() {
            return Some(DEFAULT_STREAM.to_string());
        }
        self.streams
            .iter()
            .find(|(_, pattern)| subject_matches(pattern, subject))
            .map(|(stream, _)| stream.clone())
    }
}

/// Records every publish and request. A publish whose headers or message
/// id the platform refuses is refused. A JetStream publish is acknowledged
/// by the stream that captures its subject, [`DEFAULT_STREAM`] until
/// [`captures`](Self::captures) says which streams there are, and a
/// message id seen before on that stream is a duplicate, answered with
/// the sequence the first was stored at. A request is
/// answered with what [`reply`](Self::reply) scripted for its subject, or
/// not at all. Clones share one record.
#[derive(Clone, Default)]
pub struct NatsRecorder {
    recorded: Rc<RefCell<Recorded>>,
}

impl NatsRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Have `stream` capture `pattern`. Once any stream is declared, a
    /// JetStream publish on a subject none captures fails with `NoStream`,
    /// as it does on the platform.
    pub fn captures(&self, stream: &str, pattern: &str) -> &Self {
        self.recorded
            .borrow_mut()
            .streams
            .push((stream.to_string(), pattern.to_string()));
        self
    }

    /// Answer every request on `pattern` with `reply`.
    pub fn reply(&self, pattern: &str, reply: impl Into<Vec<u8>>) -> &Self {
        self.recorded
            .borrow_mut()
            .replies
            .push((pattern.to_string(), reply.into()));
        self
    }

    /// Every publish, in order, whether or not it was acknowledged.
    pub fn published(&self) -> Vec<PublishRequest> {
        self.recorded.borrow().published.clone()
    }

    /// The publishes on subjects `pattern` matches.
    pub fn published_to(&self, pattern: &str) -> Vec<PublishRequest> {
        self.published()
            .into_iter()
            .filter(|request| subject_matches(pattern, &request.subject))
            .collect()
    }

    /// Every request, in order, as subject and payload.
    pub fn requests(&self) -> Vec<(String, Vec<u8>)> {
        self.recorded.borrow().requests.clone()
    }
}

impl NatsHost for NatsRecorder {
    fn publish(&self, request: &PublishRequest) -> PublishResponse {
        let mut recorded = self.recorded.borrow_mut();
        recorded.published.push(request.clone());
        if let Some(message) = request.violation() {
            return PublishResponse::Error {
                kind: PublishErrorKind::Invalid,
                message,
            };
        }
        if request.mode == PublishMode::Core {
            return PublishResponse::Sent;
        }
        let Some(stream) = recorded.capturing(&request.subject) else {
            return PublishResponse::Error {
                kind: PublishErrorKind::NoStream,
                message: format!("no stream captures '{}'", request.subject),
            };
        };
        let id = request.msg_id.as_deref().filter(|id| !id.is_empty());
        let stored_before = id.and_then(|id| recorded.ids.get(&stream)?.get(id).copied());
        let sequence = stored_before.unwrap_or_else(|| {
            let last = recorded.sequences.entry(stream.clone()).or_default();
            *last += 1;
            *last
        });
        if let Some(id) = id {
            recorded
                .ids
                .entry(stream.clone())
                .or_default()
                .entry(id.to_string())
                .or_insert(sequence);
        }
        PublishResponse::Ack {
            stream,
            sequence,
            duplicate: stored_before.is_some(),
        }
    }

    fn request(&self, subject: &str, payload: &[u8], _timeout_ms: u32) -> Option<Vec<u8>> {
        let mut recorded = self.recorded.borrow_mut();
        recorded
            .requests
            .push((subject.to_string(), payload.to_vec()));
        recorded
            .replies
            .iter()
            .find(|(pattern, _)| subject_matches(pattern, subject))
            .map(|(_, reply)| reply.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publish(recorder: &NatsRecorder, subject: &str, msg_id: Option<&str>) -> PublishResponse {
        recorder.publish(&PublishRequest {
            subject: subject.into(),
            payload: b"{}".to_vec(),
            msg_id: msg_id.map(str::to_string),
            ..Default::default()
        })
    }

    #[test]
    fn every_publish_is_acknowledged_and_counted_until_streams_are_declared() {
        let recorder = NatsRecorder::new();
        assert_eq!(
            publish(&recorder, "books.created", None),
            PublishResponse::Ack {
                stream: DEFAULT_STREAM.into(),
                sequence: 1,
                duplicate: false
            }
        );
        assert_eq!(
            publish(&recorder, "anything.at.all", None),
            PublishResponse::Ack {
                stream: DEFAULT_STREAM.into(),
                sequence: 2,
                duplicate: false
            }
        );
        assert_eq!(recorder.published().len(), 2);
        assert_eq!(recorder.published_to("books.*").len(), 1);
    }

    #[test]
    fn a_declared_stream_captures_its_subjects_and_a_message_id_publishes_once() {
        let recorder = NatsRecorder::new();
        recorder.captures("BOOKS", "books.>");
        assert_eq!(
            publish(&recorder, "books.created", Some("book-7")),
            PublishResponse::Ack {
                stream: "BOOKS".into(),
                sequence: 1,
                duplicate: false
            }
        );
        assert_eq!(
            publish(&recorder, "books.created", Some("book-7")),
            PublishResponse::Ack {
                stream: "BOOKS".into(),
                sequence: 1,
                duplicate: true
            }
        );
        assert!(matches!(
            publish(&recorder, "receipts.created", None),
            PublishResponse::Error {
                kind: PublishErrorKind::NoStream,
                ..
            }
        ));
        assert_eq!(
            recorder.published().len(),
            3,
            "recorded whether or not acknowledged"
        );
    }

    #[test]
    fn a_duplicate_is_answered_with_the_sequence_of_the_message_it_repeats() {
        let recorder = NatsRecorder::new();
        let sequences: Vec<(u64, bool)> = [Some("book-7"), Some("book-8"), None, Some("book-7")]
            .into_iter()
            .map(|id| match publish(&recorder, "books.created", id) {
                PublishResponse::Ack {
                    sequence,
                    duplicate,
                    ..
                } => (sequence, duplicate),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(sequences, [(1, false), (2, false), (3, false), (1, true)]);
        assert!(
            matches!(
                publish(&recorder, "books.created", Some("")),
                PublishResponse::Ack {
                    sequence: 4,
                    duplicate: false,
                    ..
                }
            ),
            "an empty id is none"
        );
    }

    #[test]
    fn what_the_platform_refuses_of_a_publish_is_refused() {
        let recorder = NatsRecorder::new();
        let refused = |headers: &[(&str, &str)], msg_id: Option<&str>, mode| {
            matches!(
                recorder.publish(&PublishRequest {
                    subject: "books.created".into(),
                    payload: b"{}".to_vec(),
                    headers: headers
                        .iter()
                        .map(|(name, value)| (name.to_string(), value.to_string()))
                        .collect(),
                    msg_id: msg_id.map(str::to_string),
                    mode,
                    ..Default::default()
                }),
                PublishResponse::Error {
                    kind: PublishErrorKind::Invalid,
                    ..
                }
            )
        };
        for mode in [PublishMode::JetStream, PublishMode::Core] {
            assert!(refused(&[("Nats-Expected-Stream", "OTHER")], None, mode));
            assert!(refused(&[("Cufflink-Reply", "_INBOX.x")], None, mode));
            assert!(refused(&[("x bad", "v")], None, mode));
            assert!(refused(&[("x-source", "a\r\nb")], None, mode));
            assert!(refused(&[], Some("a\nb"), mode));
            assert!(!refused(&[("x-source", "a")], Some("book-7"), mode));
        }
        assert_eq!(recorder.published().len(), 12, "recorded all the same");
    }

    #[test]
    fn core_publishes_are_sent_and_requests_answered_as_scripted() {
        let recorder = NatsRecorder::new();
        recorder.reply("users.lookup", br#"{"name":"Ada"}"#.to_vec());
        assert_eq!(
            recorder.publish(&PublishRequest {
                subject: "books.created".into(),
                payload: b"{}".to_vec(),
                mode: PublishMode::Core,
                ..Default::default()
            }),
            PublishResponse::Sent
        );
        assert_eq!(
            recorder.request("users.lookup", b"{}", 100).as_deref(),
            Some(&br#"{"name":"Ada"}"#[..])
        );
        assert_eq!(recorder.request("users.other", b"{}", 100), None);
        assert_eq!(recorder.requests().len(), 2);
    }

    #[test]
    fn subjects_match_as_nats_matches_them() {
        assert!(subject_matches("books.>", "books.a.b"));
        assert!(subject_matches("books.*", "books.a"));
        assert!(!subject_matches("books.*", "books.a.b"));
        assert!(!subject_matches("books.>", "books"));
        assert!(subject_matches("books.created", "books.created"));
        assert!(!subject_matches("books.created", "books.deleted"));
    }
}
