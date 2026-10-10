use super::pump::MessageMeta;
use crate::nats_conn::{jetstream_publish_bounded, PublishError};
use async_nats::jetstream;
use async_nats::HeaderMap;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use cufflink_types::host::nats::{header_has_prefix as has_prefix, names_msg_id, MSG_ID_HEADER};
use serde_json::{Map, Value};

pub const DLQ_STREAM: &str = "CUFFLINK_DLQ";
pub const DLQ_SUBJECT_PREFIX: &str = "cufflink.dlq.";

pub const REASON_HEADER: &str = "Cufflink-Dlq-Reason";
pub const STREAM_HEADER: &str = "Cufflink-Dlq-Stream";
pub const SUBJECT_HEADER: &str = "Cufflink-Dlq-Subject";
pub const SEQUENCE_HEADER: &str = "Cufflink-Dlq-Sequence";
pub const DELIVERIES_HEADER: &str = "Cufflink-Dlq-Deliveries";
pub const CONSUMER_HEADER: &str = "Cufflink-Dlq-Consumer";
pub const AT_HEADER: &str = "Cufflink-Dlq-At";
pub const WITHHELD_HEADER: &str = "Cufflink-Dlq-Headers";

pub const UNOWNED_TENANT: &str = "_";

const INTERNAL_HEADER_PREFIX: &str = "cufflink-";
const SERVER_HEADER_PREFIX: &str = "nats-";
const REASON_KEPT_BYTES: usize = 1024;

pub(crate) fn cut(text: &str, bytes: usize) -> &str {
    &text[..text.floor_char_boundary(bytes)]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqKind {
    Subscription,
    Schedule,
    Job,
    Hook,
    Webhook,
    Outbox,
}

impl DlqKind {
    pub fn as_str(self) -> &'static str {
        match self {
            DlqKind::Subscription => "sub",
            DlqKind::Schedule => "sched",
            DlqKind::Job => "job",
            DlqKind::Hook => "hook",
            DlqKind::Webhook => "webhook",
            DlqKind::Outbox => "outbox",
        }
    }
}

pub fn token(name: &str) -> String {
    let mapped = super::consumers::naming::token(name);
    if mapped.is_empty() {
        UNOWNED_TENANT.to_string()
    } else {
        mapped
    }
}

pub fn dlq_subject(tenant_slug: &str, service_name: &str, kind: DlqKind, handler: &str) -> String {
    format!(
        "{DLQ_SUBJECT_PREFIX}{}.{}.{}.{}",
        token(tenant_slug),
        token(service_name),
        kind.as_str(),
        token(handler)
    )
}

pub fn tenant_of(dlq_subject: &str) -> Option<&str> {
    dlq_subject
        .strip_prefix(DLQ_SUBJECT_PREFIX)
        .and_then(|rest| rest.split('.').next())
        .filter(|tenant| !tenant.is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    pub subject: String,
    pub payload: Bytes,
    pub headers: Vec<(String, Vec<String>)>,
    pub reason: String,
    pub stream: String,
    pub original_subject: String,
    pub sequence: u64,
    pub deliveries: u64,
    pub consumer: String,
    pub at: DateTime<Utc>,
}

impl DeadLetter {
    pub fn of(
        subject: String,
        payload: Bytes,
        meta: &MessageMeta,
        reason: &str,
        at: DateTime<Utc>,
    ) -> Self {
        Self {
            subject,
            payload,
            headers: meta.headers.clone(),
            reason: reason.to_string(),
            stream: meta.stream.clone(),
            original_subject: meta.subject.clone(),
            sequence: meta.stream_sequence,
            deliveries: meta.delivered,
            consumer: meta.consumer.clone(),
            at,
        }
    }

    pub fn now(subject: String, payload: Bytes, meta: &MessageMeta, reason: &str) -> Self {
        Self::of(subject, payload, meta, reason, Utc::now())
    }

    pub fn msg_id(&self) -> String {
        format!("dlq:{}:{}:{}", self.stream, self.consumer, self.sequence)
    }

    fn withheld(&self) -> Option<String> {
        let withheld: Map<String, Value> = self
            .headers
            .iter()
            .filter(|(name, values)| is_withheld(name, values))
            .map(|(name, values)| (name.clone(), Value::from(values.clone())))
            .collect();
        (!withheld.is_empty()).then(|| withheld_record(withheld))
    }

    pub fn publish_headers(&self) -> HeaderMap {
        let carried = self
            .headers
            .iter()
            .filter(|(name, values)| {
                !has_prefix(name, INTERNAL_HEADER_PREFIX) && !is_withheld(name, values)
            })
            .flat_map(|(name, values)| {
                values
                    .iter()
                    .map(move |value| (name.clone(), value.clone()))
            });
        let withheld = self
            .withheld()
            .map(|headers| (WITHHELD_HEADER.to_string(), headers));
        let recorded = [
            (
                REASON_HEADER,
                header_value(cut(&self.reason, REASON_KEPT_BYTES)),
            ),
            (STREAM_HEADER, self.stream.clone()),
            (SUBJECT_HEADER, self.original_subject.clone()),
            (SEQUENCE_HEADER, self.sequence.to_string()),
            (DELIVERIES_HEADER, self.deliveries.to_string()),
            (CONSUMER_HEADER, self.consumer.clone()),
            (
                AT_HEADER,
                self.at.to_rfc3339_opts(SecondsFormat::Millis, true),
            ),
            (MSG_ID_HEADER, self.msg_id()),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value));
        carried.chain(withheld).chain(recorded).fold(
            HeaderMap::new(),
            |mut headers, (name, value)| {
                headers.append(name.as_str(), value.as_str());
                headers
            },
        )
    }
}

fn header_value(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>()
        .replace(MSG_ID_HEADER, &MSG_ID_HEADER.to_ascii_lowercase())
}

fn is_withheld(name: &str, values: &[String]) -> bool {
    has_prefix(name, SERVER_HEADER_PREFIX)
        || names_msg_id(name)
        || values.iter().any(|value| names_msg_id(value))
}

pub fn withheld_record(headers: Map<String, Value>) -> String {
    base64::engine::general_purpose::STANDARD.encode(Value::Object(headers).to_string())
}

pub fn withheld_headers(record: &str) -> Option<Map<String, Value>> {
    base64::engine::general_purpose::STANDARD
        .decode(record)
        .ok()
        .and_then(|json| serde_json::from_slice(&json).ok())
}

pub async fn publish_dead_letter(
    js: &jetstream::Context,
    letter: &DeadLetter,
) -> Result<(), PublishError> {
    let published = jetstream_publish_bounded(
        js,
        letter.subject.clone(),
        Some(letter.publish_headers()),
        letter.payload.clone(),
    )
    .await;
    match &published {
        Ok(()) => {
            metrics::counter!(
                "cufflink_dlq_messages_total",
                "tenant" => tenant_of(&letter.subject).unwrap_or_default().to_string(),
                "kind" => kind_of(&letter.subject).to_string(),
            )
            .increment(1);
        }
        Err(e) => {
            metrics::counter!(
                "cufflink_dlq_publish_failed_total",
                "kind" => kind_of(&letter.subject).to_string(),
            )
            .increment(1);
            tracing::error!(
                event = "dlq_publish_failed",
                stream = %letter.stream,
                sequence = letter.sequence,
                error = %e,
                "Could not store a dead letter; the message stays in its stream"
            );
        }
    }
    published
}

fn kind_of(dlq_subject: &str) -> &str {
    dlq_subject
        .strip_prefix(DLQ_SUBJECT_PREFIX)
        .and_then(|rest| rest.split('.').nth(2))
        .unwrap_or("unknown")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> MessageMeta {
        MessageMeta {
            subject: "books.due_date.set".into(),
            stream: "BOOKS".into(),
            consumer: "loans-service-book-consumer".into(),
            stream_sequence: 81_234,
            consumer_sequence: 17,
            delivered: 3,
            pending: 0,
            published: DateTime::UNIX_EPOCH,
            headers: vec![
                ("traceparent".into(), vec!["00-abc-def-01".into()]),
                ("Nats-Msg-Id".into(), vec!["book-42".into()]),
                ("Nats-Expected-Stream".into(), vec!["BOOKS".into()]),
                ("nats-rollup".into(), vec!["all".into()]),
                ("Cufflink-Dlq-Subject".into(), vec!["dw.victim.x".into()]),
                ("Cufflink-Reply".into(), vec!["_INBOX.x".into()]),
                ("cufflink-budget-ms".into(), vec!["500".into()]),
                ("X-Source".into(), vec!["boss".into(), "import".into()]),
                ("X-Original-Nats-Msg-Id".into(), vec!["book-41".into()]),
                (
                    "x-loan".into(),
                    vec!["a".into(), "copied from Nats-Msg-Id".into()],
                ),
            ],
        }
    }

    fn letter() -> DeadLetter {
        DeadLetter::of(
            dlq_subject(
                "harbor",
                "loan-service",
                DlqKind::Subscription,
                "on_book_event",
            ),
            Bytes::from_static(b"{\"id\":42}"),
            &meta(),
            "handler answered 503\nafter 3 deliveries",
            DateTime::UNIX_EPOCH + chrono::Duration::milliseconds(1_500),
        )
    }

    #[test]
    fn a_subject_names_the_tenant_the_service_the_kind_and_the_handler() {
        assert_eq!(
            letter().subject,
            "cufflink.dlq.harbor.loan-service.sub.on_book_event"
        );
        for (kind, name) in [
            (DlqKind::Schedule, "sched"),
            (DlqKind::Job, "job"),
            (DlqKind::Hook, "hook"),
            (DlqKind::Webhook, "webhook"),
            (DlqKind::Outbox, "outbox"),
        ] {
            assert_eq!(
                dlq_subject("acme", "svc", kind, "h"),
                format!("cufflink.dlq.acme.svc.{name}.h")
            );
        }
        assert_eq!(
            dlq_subject("acme", "my.svc", DlqKind::Hook, "after *create>"),
            "cufflink.dlq.acme.my_svc.hook.after__create_"
        );
        assert_eq!(
            dlq_subject("acme", "", DlqKind::Job, ""),
            "cufflink.dlq.acme._.job._"
        );
        assert_eq!(tenant_of(&letter().subject), Some("harbor"));
        assert_eq!(kind_of(&letter().subject), "sub");
        assert_eq!(tenant_of("dw.harbor.loans.created"), None);
        assert_eq!(tenant_of("cufflink.dlq."), None);
    }

    #[test]
    fn a_reason_is_carried_no_longer_than_its_bound_and_cut_where_a_character_ends() {
        let long = DeadLetter {
            reason: format!("{}é and more", "x".repeat(REASON_KEPT_BYTES - 1)),
            ..letter()
        };
        let headers = long.publish_headers();
        let reason = headers.get(REASON_HEADER).expect("reason").as_str();
        assert_eq!(reason, "x".repeat(REASON_KEPT_BYTES - 1));
        assert_eq!(cut("héllo", 2), "h");
        assert_eq!(cut("héllo", 3), "hé");
        assert_eq!(cut("hi", 10), "hi");
    }

    #[test]
    fn a_dead_letter_carries_the_messages_headers_and_what_happened_to_it() {
        let headers = letter().publish_headers();
        let all = |name: &str| -> Vec<String> {
            headers
                .get_all(name)
                .map(|value| value.as_str().to_string())
                .collect()
        };
        assert_eq!(all("traceparent"), ["00-abc-def-01"]);
        assert_eq!(all("X-Source"), ["boss", "import"]);
        assert!(all("Cufflink-Reply").is_empty());
        assert!(all("cufflink-budget-ms").is_empty());
        assert_eq!(
            all("Nats-Msg-Id"),
            ["dlq:BOOKS:loans-service-book-consumer:81234"],
            "the message's own id would make the queue drop it as a duplicate of nothing"
        );
        assert_eq!(
            headers
                .iter()
                .map(|(name, _)| name.to_string())
                .filter(|name| has_prefix(name, SERVER_HEADER_PREFIX))
                .collect::<Vec<_>>(),
            ["Nats-Msg-Id"],
            "a header the server acts on would steer or fail the publish"
        );
        assert!(all("x-loan").is_empty());
        assert!(all("X-Original-Nats-Msg-Id").is_empty());
        assert!(
            crate::nats_manager::headers::msg_id_unhidden(&headers),
            "a server that takes a header to be where its name first appears finds the queue's id"
        );
        assert_eq!(
            all(WITHHELD_HEADER)
                .iter()
                .map(|record| withheld_headers(record).map(Value::Object))
                .collect::<Vec<_>>(),
            [Some(serde_json::json!({
                "Nats-Expected-Stream": ["BOOKS"],
                "Nats-Msg-Id": ["book-42"],
                "nats-rollup": ["all"],
                "X-Original-Nats-Msg-Id": ["book-41"],
                "x-loan": ["a", "copied from Nats-Msg-Id"],
            }))]
        );
        assert_eq!(
            all(REASON_HEADER),
            ["handler answered 503 after 3 deliveries"]
        );
        assert_eq!(all(STREAM_HEADER), ["BOOKS"]);
        assert_eq!(
            all(SUBJECT_HEADER),
            ["books.due_date.set"],
            "what the queue records is never what the message claimed"
        );
        assert_eq!(all(SEQUENCE_HEADER), ["81234"]);
        assert_eq!(all(DELIVERIES_HEADER), ["3"]);
        assert_eq!(all(CONSUMER_HEADER), ["loans-service-book-consumer"]);
        assert_eq!(all(AT_HEADER), ["1970-01-01T00:00:01.500Z"]);
    }

    #[test]
    fn a_reason_that_names_the_message_id_does_not_hide_the_queues() {
        let headers = DeadLetter {
            reason: "no Nats-Msg-Id\non the message".into(),
            ..letter()
        }
        .publish_headers();
        assert_eq!(
            headers.get(REASON_HEADER).map(|reason| reason.as_str()),
            Some("no nats-msg-id on the message")
        );
        assert!(crate::nats_manager::headers::msg_id_unhidden(&headers));
    }

    #[test]
    fn a_withheld_record_is_read_back_and_what_is_not_one_is_none() {
        let headers: Map<String, Value> = serde_json::from_value(
            serde_json::json!({"Nats-Msg-Id": ["book-42"], "x-loan": ["a"]}),
        )
        .expect("an object");
        let record = withheld_record(headers.clone());
        assert!(!names_msg_id(&record));
        assert_eq!(withheld_headers(&record), Some(headers));
        assert_eq!(withheld_headers(r#"{"Nats-Msg-Id":["book-42"]}"#), None);
        assert_eq!(withheld_headers("bm90IGpzb24="), None);
    }

    #[test]
    fn a_message_without_server_headers_withholds_nothing() {
        let plain = DeadLetter {
            headers: vec![("traceparent".into(), vec!["00-abc-def-01".into()])],
            ..letter()
        };
        assert!(plain.publish_headers().get(WITHHELD_HEADER).is_none());
    }

    #[test]
    fn publishing_again_uses_the_same_message_id() {
        let (first, again) = (letter(), letter());
        assert_eq!(first.msg_id(), again.msg_id());
        assert_ne!(
            first.msg_id(),
            DeadLetter {
                sequence: 81_235,
                ..letter()
            }
            .msg_id()
        );
        assert_ne!(
            first.msg_id(),
            DeadLetter {
                consumer: "audit-service-book-consumer".into(),
                ..letter()
            }
            .msg_id(),
            "each consumer that gives a message up has its own dead letter"
        );
    }
}
