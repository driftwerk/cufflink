use crate::auth::DeployAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::nats_conn::{publish_timeout, PublishError};
use crate::nats_manager::dlq::{
    self, DlqKind, AT_HEADER, CONSUMER_HEADER, DELIVERIES_HEADER, DLQ_STREAM, DLQ_SUBJECT_PREFIX,
    REASON_HEADER, SEQUENCE_HEADER, STREAM_HEADER, SUBJECT_HEADER, UNOWNED_TENANT, WITHHELD_HEADER,
};
use crate::nats_manager::headers;
use crate::nats_manager::streams::publish_acked;
use crate::nats_manager::subjects::Namespace;
use crate::AppState;
use async_nats::jetstream::consumer::{pull, AckPolicy, Consumer, DeliverPolicy};
use async_nats::jetstream::message::StreamMessage;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::stream::{RawMessageErrorKind, Stream};
use async_nats::HeaderMap;
use axum::extract::{Path, Query, State};
use axum::Json;
use base64::Engine;
use cufflink_types::host::nats::header_has_prefix;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub const LIST_MAX: usize = 200;
const LIST_DEFAULT: usize = 50;
const READ_THROUGH: u64 = 1_000;
const READ_AT_ONCE: u64 = 500;
const READ_WAIT: Duration = Duration::from_millis(500);
const READER_IDLE: Duration = Duration::from_secs(30);
pub const REPLAYED_FROM_HEADER: &str = "Cufflink-Replayed-From";
const RECORD_HEADER_PREFIX: &str = "cufflink-dlq-";
const SERVER_HEADER_PREFIX: &str = "nats-";

#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    pub service: Option<String>,
    pub limit: Option<usize>,
}

fn tenant_token(user: &crate::auth::AuthUser, slug: &str) -> PlatformResult<String> {
    if slug == UNOWNED_TENANT && user.is_platform_admin {
        return Ok(UNOWNED_TENANT.to_string());
    }
    if slug == user.tenant_slug {
        return Ok(slug.to_string());
    }
    Err(PlatformError::Forbidden(
        "the dead-letter queue of another tenant is not yours to see".into(),
    ))
}

fn filter_of(tenant: &str, service: Option<&str>) -> String {
    match service {
        Some(service) => format!("{DLQ_SUBJECT_PREFIX}{tenant}.{}.>", dlq::token(service)),
        None => format!("{DLQ_SUBJECT_PREFIX}{tenant}.>"),
    }
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).map(|value| value.as_str().to_string())
}

fn is_queues(name: &str) -> bool {
    header_has_prefix(name, RECORD_HEADER_PREFIX) || header_has_prefix(name, SERVER_HEADER_PREFIX)
}

pub fn original_headers(headers: &HeaderMap) -> HeaderMap {
    let withheld: Map<String, Value> = header(headers, WITHHELD_HEADER)
        .and_then(|record| dlq::withheld_headers(&record))
        .unwrap_or_default();
    let carried = headers
        .iter()
        .filter(|(name, _)| !is_queues(name.as_ref()))
        .flat_map(|(name, values)| {
            values
                .iter()
                .map(move |value| (name.to_string(), value.to_string()))
        });
    let restored = withheld.into_iter().flat_map(|(name, values)| {
        values
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(str::to_string))
            .map(move |value| (name.clone(), value))
            .collect::<Vec<_>>()
    });
    carried
        .chain(restored)
        .filter_map(|(name, value)| headers::parsed(&name, &value).ok())
        .fold(HeaderMap::new(), |mut original, (name, value)| {
            original.append(name, value);
            original
        })
}

fn letter(message: &StreamMessage) -> Value {
    let headers = &message.headers;
    let subject = message.subject.as_str();
    let mut tokens = subject
        .strip_prefix(DLQ_SUBJECT_PREFIX)
        .unwrap_or_default()
        .split('.');
    let (tenant, service, kind, handler) =
        (tokens.next(), tokens.next(), tokens.next(), tokens.next());
    let (payload, payload_b64) = match std::str::from_utf8(&message.payload) {
        Ok(text) => (
            serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!(text)),
            Value::Null,
        ),
        Err(_) => (
            Value::Null,
            json!(base64::engine::general_purpose::STANDARD.encode(&message.payload)),
        ),
    };
    let original: Map<String, Value> = original_headers(headers)
        .iter()
        .map(|(name, values)| {
            (
                name.to_string(),
                json!(values.iter().map(|v| v.as_str()).collect::<Vec<_>>()),
            )
        })
        .collect();
    json!({
        "sequence": message.sequence,
        "subject": subject,
        "tenant": tenant,
        "service": service,
        "kind": kind,
        "handler": handler,
        "reason": header(headers, REASON_HEADER),
        "stream": header(headers, STREAM_HEADER),
        "original_subject": header(headers, SUBJECT_HEADER),
        "original_sequence": header(headers, SEQUENCE_HEADER).and_then(|s| s.parse::<u64>().ok()),
        "deliveries": header(headers, DELIVERIES_HEADER).and_then(|s| s.parse::<u64>().ok()),
        "consumer": header(headers, CONSUMER_HEADER),
        "at": header(headers, AT_HEADER),
        "payload": payload,
        "payload_b64": payload_b64,
        "headers": original,
    })
}

fn unavailable(e: impl std::fmt::Display) -> PlatformError {
    PlatformError::Unavailable(format!("the dead-letter queue is not reachable: {e}"))
}

async fn queue(state: &AppState) -> PlatformResult<Stream> {
    state
        .jetstream
        .get_stream(DLQ_STREAM)
        .await
        .map_err(unavailable)
}

struct Reader<'a> {
    queue: &'a Stream,
    consumer: Consumer<pull::Config>,
    pending: u64,
}

impl<'a> Reader<'a> {
    async fn open(
        queue: &'a Stream,
        filter: &str,
        from: DeliverPolicy,
    ) -> PlatformResult<Reader<'a>> {
        let consumer = queue
            .create_consumer(pull::Config {
                filter_subject: filter.to_string(),
                deliver_policy: from,
                ack_policy: AckPolicy::None,
                inactive_threshold: READER_IDLE,
                num_replicas: 1,
                memory_storage: true,
                ..Default::default()
            })
            .await
            .map_err(unavailable)?;
        let pending = consumer.cached_info().num_pending;
        Ok(Self {
            queue,
            consumer,
            pending,
        })
    }

    async fn close(self) {
        let _ = self
            .queue
            .delete_consumer(&self.consumer.cached_info().name)
            .await;
    }

    async fn newest(&self, limit: usize) -> PlatformResult<Vec<StreamMessage>> {
        let mut newest = VecDeque::with_capacity(limit);
        let mut ahead = self.pending;
        while ahead > 0 {
            let mut batch = self
                .consumer
                .fetch()
                .max_messages(usize::try_from(ahead.min(READ_AT_ONCE)).unwrap_or(usize::MAX))
                .expires(READ_WAIT)
                .messages()
                .await
                .map_err(unavailable)?;
            let mut received = 0u64;
            while let Some(message) = batch.next().await {
                let message = message.map_err(unavailable)?;
                let info = message.info().map_err(unavailable)?;
                if newest.len() == limit {
                    newest.pop_front();
                }
                newest.push_back(StreamMessage {
                    subject: message.subject.clone(),
                    sequence: info.stream_sequence,
                    headers: message.headers.clone().unwrap_or_default(),
                    payload: message.payload.clone(),
                    time: info.published,
                });
                received += 1;
            }
            if received == 0 {
                break;
            }
            ahead = ahead.saturating_sub(received);
        }
        Ok(newest.into_iter().rev().collect())
    }
}

async fn highest<F, Fut>(low: u64, high: u64, enough: F) -> PlatformResult<u64>
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = PlatformResult<bool>>,
{
    let (mut low, mut high) = (low, high);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if enough(middle).await? {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(low)
}

async fn newest_from(queue: &Stream, filter: &str, wanted: u64) -> PlatformResult<u64> {
    let held = queue.get_info().await.map_err(unavailable)?.state;
    highest(held.first_sequence, held.last_sequence, |from| async move {
        let reader = Reader::open(
            queue,
            filter,
            DeliverPolicy::ByStartSequence {
                start_sequence: from,
            },
        )
        .await?;
        let ahead = reader.pending;
        reader.close().await;
        Ok(ahead >= wanted)
    })
    .await
}

async fn read(state: &AppState, filter: &str, limit: usize) -> PlatformResult<Vec<StreamMessage>> {
    let queue = queue(state).await?;
    let whole = Reader::open(&queue, filter, DeliverPolicy::All).await?;
    let reader = if whole.pending <= READ_THROUGH {
        whole
    } else {
        whole.close().await;
        let from = newest_from(&queue, filter, limit as u64).await?;
        Reader::open(
            &queue,
            filter,
            DeliverPolicy::ByStartSequence {
                start_sequence: from,
            },
        )
        .await?
    };
    let newest = reader.newest(limit).await;
    reader.close().await;
    newest
}

pub async fn list(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(slug): Path<String>,
    Query(query): Query<ListQuery>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenant_token(&user, &slug)?;
    let limit = query.limit.unwrap_or(LIST_DEFAULT).clamp(1, LIST_MAX);
    let letters = read(&state, &filter_of(&tenant, query.service.as_deref()), limit).await?;
    Ok(Json(json!({
        "tenant": tenant,
        "letters": letters.iter().map(letter).collect::<Vec<_>>(),
        "limit": limit,
    })))
}

async fn owned(
    state: &AppState,
    tenant: &str,
    sequence: u64,
) -> PlatformResult<(Stream, StreamMessage)> {
    let not_found = || PlatformError::NotFound(format!("no dead letter {sequence}"));
    let queue = queue(state).await?;
    let message = match queue.get_raw_message(sequence).await {
        Ok(message) => message,
        Err(e) if e.kind() == RawMessageErrorKind::NoMessageFound => return Err(not_found()),
        Err(e) => return Err(unavailable(e)),
    };
    if dlq::tenant_of(message.subject.as_str()) != Some(tenant) {
        return Err(not_found());
    }
    Ok((queue, message))
}

async fn audit(state: &AppState, user: &crate::auth::AuthUser, action: &str, letter: &Value) {
    let sequence = letter["sequence"].as_u64().map(|s| s.to_string());
    let recorded = async {
        crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
            client: &state.platform_db.get().await?,
            tenant_id: (letter["tenant"] != UNOWNED_TENANT).then_some(user.tenant_id),
            actor: &user.subject,
            action,
            resource_type: "dead_letter",
            resource_id: sequence.as_deref(),
            details: Some(json!({
                "subject": letter["subject"],
                "original_subject": letter["original_subject"],
                "reason": letter["reason"],
            })),
            ip_address: None,
        })
        .await
    }
    .await;
    if let Err(e) = recorded {
        warn!(
            action,
            actor = %user.subject,
            sequence = sequence.as_deref().unwrap_or("?"),
            error = %e,
            "A dead letter's audit record could not be written"
        );
    }
}

fn still_held(ack: &PublishAck, recorded: &HeaderMap) -> bool {
    ack.duplicate
        && header(recorded, STREAM_HEADER).as_deref() == Some(ack.stream.as_str())
        && header(recorded, SEQUENCE_HEADER).and_then(|s| s.parse::<u64>().ok())
            == Some(ack.sequence)
}

fn is_fire_of(tenant: &str, subject: &str) -> bool {
    subject
        .strip_prefix(crate::schedules::SUBJECT_PREFIX)
        .and_then(|rest| rest.split('.').next())
        .is_some_and(|owner| owner == dlq::token(tenant))
}

pub async fn replay(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((slug, sequence)): Path<(String, u64)>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenant_token(&user, &slug)?;
    let (queue, message) = owned(&state, &tenant, sequence).await?;
    let shown = letter(&message);
    if shown["kind"] == DlqKind::Job.as_str() {
        return Err(PlatformError::Validation(format!(
            "the letter is of job {}, which its message would not run again; retry the job \
             (POST /svc/{tenant}/_jobs/{{id}}/retry) and drop the letter",
            shown["consumer"].as_str().unwrap_or_default()
        )));
    }
    let subject = header(&message.headers, SUBJECT_HEADER).ok_or_else(|| {
        PlatformError::Validation("the letter does not say where it came from".into())
    })?;
    let namespace = state
        .tenants
        .get_or_load(user.tenant_id)
        .await?
        .map_or_else(
            || Namespace::own(&user.tenant_slug),
            |entry| entry.namespace.clone(),
        );
    if tenant == UNOWNED_TENANT || !(namespace.contains(&subject) || is_fire_of(&tenant, &subject))
    {
        return Err(PlatformError::Validation(format!(
            "'{subject}' is not a subject of tenant '{}'; the letter can be dropped, not replayed",
            user.tenant_slug
        )));
    }
    let mut headers = original_headers(&message.headers);
    headers.insert(REPLAYED_FROM_HEADER, format!("dlq:{sequence}").as_str());
    let delivered = match state.stream_index.capturing(&subject) {
        Some(_) => {
            let ack = publish_acked(
                &state.jetstream,
                subject,
                headers,
                message.payload.clone(),
                publish_timeout(),
            )
            .await
            .map_err(|e| match e {
                PublishError::TimedOut(_) => {
                    PlatformError::GatewayTimeout("the stream did not acknowledge in time".into())
                }
                PublishError::Failed(e) => {
                    PlatformError::Unavailable(format!("not republished: {e}"))
                }
            })?;
            if still_held(&ack, &message.headers) {
                return Err(PlatformError::Conflict(format!(
                    "stream '{}' still holds the message under its id, as sequence {}, and \
                     would deliver nothing again; the letter is kept: replay it once the \
                     stream's duplicate window has passed",
                    ack.stream, ack.sequence
                )));
            }
            json!({"stream": ack.stream, "sequence": ack.sequence, "duplicate": ack.duplicate})
        }
        None => {
            crate::nats_conn::publish_bounded(
                &state.nats,
                subject,
                Some(headers),
                message.payload.clone(),
            )
            .await
            .map_err(|e| PlatformError::Unavailable(format!("not republished: {e}")))?;
            json!({"core": true})
        }
    };
    queue.delete_message(sequence).await.map_err(|e| {
        PlatformError::Unavailable(format!("republished, but the letter stays: {e}"))
    })?;
    audit(&state, &user, "dlq.replay", &shown).await;
    Ok(Json(json!({
        "replayed": true,
        "sequence": sequence,
        "subject": shown["original_subject"],
        "delivered": delivered,
    })))
}

pub async fn drop(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((slug, sequence)): Path<(String, u64)>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenant_token(&user, &slug)?;
    let (queue, message) = owned(&state, &tenant, sequence).await?;
    let shown = letter(&message);
    queue
        .delete_message(sequence)
        .await
        .map_err(|e| PlatformError::Unavailable(format!("the letter stays: {e}")))?;
    audit(&state, &user, "dlq.drop", &shown).await;
    Ok(Json(json!({"dropped": true, "sequence": sequence})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::extractor::AuthMethod;
    use cufflink_types::host::nats::MSG_ID_HEADER;
    use uuid::Uuid;

    fn withheld(headers: Value) -> String {
        dlq::withheld_record(serde_json::from_value(headers).expect("an object"))
    }

    fn user(tenant: &str, admin: bool) -> crate::auth::AuthUser {
        crate::auth::AuthUser {
            subject: "u".into(),
            name: None,
            email: None,
            tenant_id: Uuid::nil(),
            tenant_slug: tenant.into(),
            auth_method: AuthMethod::ApiKey {
                id: Uuid::nil(),
                user_sub: None,
            },
            token_info: None,
            is_service_account: false,
            is_platform_admin: admin,
            has_deploy_authority: true,
            realm: None,
        }
    }

    #[test]
    fn a_tenant_reads_its_own_queue_and_an_admin_the_unowned_one() {
        assert_eq!(
            tenant_token(&user("acme", false), "acme").ok(),
            Some("acme".into())
        );
        assert!(tenant_token(&user("acme", false), "other").is_err());
        assert!(tenant_token(&user("acme", false), "_").is_err());
        assert_eq!(
            tenant_token(&user("acme", true), "_").ok(),
            Some("_".into())
        );
        assert!(tenant_token(&user("acme", true), "other").is_err());
    }

    #[test]
    fn a_listing_is_filtered_by_tenant_and_service() {
        assert_eq!(filter_of("acme", None), "cufflink.dlq.acme.>");
        assert_eq!(
            filter_of("acme", Some("mail.service")),
            "cufflink.dlq.acme.mail_service.>"
        );
    }

    fn stored(headers: &[(&str, &str)], payload: &[u8]) -> StreamMessage {
        StreamMessage {
            subject: "cufflink.dlq.acme.mail-service.sub.fail".into(),
            sequence: 42,
            headers: headers
                .iter()
                .fold(HeaderMap::new(), |mut map, (name, value)| {
                    map.append(*name, *value);
                    map
                }),
            payload: payload.to_vec().into(),
            time: async_nats::datetime::now(),
        }
    }

    #[test]
    fn a_letter_is_shown_with_what_was_recorded_and_its_own_headers() {
        let withheld = withheld(json!({"Nats-Msg-Id": ["mail-7"], "x-note": ["Nats-Msg-Id"]}));
        let message = stored(
            &[
                (REASON_HEADER, "handler answered 503"),
                (STREAM_HEADER, "DW_ACME"),
                (SUBJECT_HEADER, "dw.acme.mail.fail"),
                (SEQUENCE_HEADER, "7"),
                (DELIVERIES_HEADER, "3"),
                (CONSUMER_HEADER, "mail_service_fail"),
                (AT_HEADER, "2026-09-28T10:00:00.000Z"),
                (WITHHELD_HEADER, &withheld),
                ("x-source", "a"),
                ("x-source", "b"),
                ("Nats-Msg-Id", "dlq:DW_ACME:mail_service_fail:7"),
            ],
            br#"{"to":"a@example.com"}"#,
        );
        let shown = letter(&message);
        assert_eq!(shown["sequence"], 42);
        assert_eq!(shown["tenant"], "acme");
        assert_eq!(shown["service"], "mail-service");
        assert_eq!(shown["kind"], "sub");
        assert_eq!(shown["handler"], "fail");
        assert_eq!(shown["reason"], "handler answered 503");
        assert_eq!(shown["original_subject"], "dw.acme.mail.fail");
        assert_eq!(shown["original_sequence"], 7);
        assert_eq!(shown["deliveries"], 3);
        assert_eq!(shown["consumer"], "mail_service_fail");
        assert_eq!(shown["payload"], json!({"to": "a@example.com"}));
        assert_eq!(
            shown["payload_b64"],
            Value::Null,
            "the payload is shown once"
        );
        assert_eq!(
            shown["headers"],
            json!({"x-source": ["a", "b"], "Nats-Msg-Id": ["mail-7"], "x-note": ["Nats-Msg-Id"]}),
            "the message's own id is back and the queue's own is gone"
        );
        let text = letter(&stored(&[], b"not json"));
        assert_eq!(
            (&text["payload"], &text["payload_b64"]),
            (&json!("not json"), &Value::Null)
        );
        let binary = letter(&stored(&[], &[0xff, 0xfe]));
        assert_eq!(binary["payload"], Value::Null);
        assert_eq!(binary["payload_b64"], "//4=");
    }

    fn ack(stream: &str, sequence: u64, duplicate: bool) -> PublishAck {
        serde_json::from_value(json!({"stream": stream, "seq": sequence, "duplicate": duplicate}))
            .expect("ack")
    }

    #[test]
    fn a_replay_answered_with_the_message_itself_keeps_the_letter() {
        let recorded = stored(&[(STREAM_HEADER, "DW_ACME"), (SEQUENCE_HEADER, "7")], b"{}").headers;
        assert!(still_held(&ack("DW_ACME", 7, true), &recorded));
        assert!(
            !still_held(&ack("DW_ACME", 8, false), &recorded),
            "stored anew"
        );
        assert!(
            !still_held(&ack("DW_ACME", 9, true), &recorded),
            "an earlier replay of it, on its way"
        );
        assert!(!still_held(&ack("OTHER", 7, true), &recorded));
        assert!(!still_held(
            &ack("DW_ACME", 7, true),
            &stored(&[], b"{}").headers
        ));
    }

    #[tokio::test]
    async fn the_newest_are_read_from_the_highest_sequence_enough_follow() {
        let ahead = |from: u64| (from..=100).filter(|sequence| sequence % 3 == 0).count() as u64;
        for wanted in [1, 5, 33] {
            let from = highest(1, 100, |from| async move { Ok(ahead(from) >= wanted) })
                .await
                .expect("found");
            assert!(ahead(from) >= wanted, "{wanted} follow {from}");
            assert!(ahead(from + 1) < wanted, "and no sequence later");
        }
        assert_eq!(highest(4, 4, |_| async { Ok(false) }).await.ok(), Some(4));
        assert!(highest(1, 9, |_| async {
            Err(PlatformError::Unavailable("the queue is down".into()))
        })
        .await
        .is_err());
    }

    #[test]
    fn a_fire_is_replayed_to_the_schedule_of_its_tenant_alone() {
        assert!(is_fire_of(
            "acme",
            "cufflink.schedule.acme.mail-service.digest"
        ));
        assert!(is_fire_of(
            "acme.eu",
            "cufflink.schedule.acme_eu.mail-service.digest"
        ));
        for another in [
            "cufflink.schedule.other.mail-service.digest",
            "cufflink.schedule.acme-eu.mail-service.digest",
            "cufflink.tick.5min",
            "cufflink.schedule.",
        ] {
            assert!(!is_fire_of("acme", another), "{another}");
        }
    }

    #[test]
    fn replayed_headers_are_the_messages_own() {
        let withheld = withheld(json!({"Nats-Expected-Stream": ["BOOKS"]}));
        let restored = original_headers(
            &stored(
                &[
                    (REASON_HEADER, "x"),
                    (WITHHELD_HEADER, &withheld),
                    ("traceparent", "00-abc-01"),
                    (MSG_ID_HEADER, "dlq:BOOKS:c:7"),
                ],
                b"{}",
            )
            .headers,
        );
        let names: Vec<String> = restored.iter().map(|(name, _)| name.to_string()).collect();
        assert!(names.contains(&"traceparent".to_string()));
        assert!(names.contains(&"Nats-Expected-Stream".to_string()));
        assert!(!names
            .iter()
            .any(|name| header_has_prefix(name, RECORD_HEADER_PREFIX)));
        assert!(
            !names.contains(&MSG_ID_HEADER.to_string()),
            "the queue's own id does not make the replay a duplicate of the letter"
        );
    }

    #[test]
    fn a_withheld_header_that_cannot_travel_is_left_out() {
        let withheld = withheld(json!({
            "Nats-Expected-Stream": ["BOOKS", "a\r\nNats-Rollup: all", 7],
            "Nats bad": ["x"],
            "Nats:bad": ["x"],
            "": ["x"],
            "Nats-Naïve": ["x"],
            "Nats-Nul": ["a\u{0}b"],
            "Nats-Msg-Id": "not a list",
        }));
        let restored = original_headers(
            &stored(&[(WITHHELD_HEADER, &withheld), ("x-source", "a")], b"{}").headers,
        );
        let mut found: Vec<(String, Vec<String>)> = restored
            .iter()
            .map(|(name, values)| {
                (
                    name.to_string(),
                    values.iter().map(|value| value.to_string()).collect(),
                )
            })
            .collect();
        found.sort();
        assert_eq!(
            found,
            [
                (
                    "Nats-Expected-Stream".to_string(),
                    vec!["BOOKS".to_string()]
                ),
                ("x-source".to_string(), vec!["a".to_string()]),
            ]
        );
        for record in [r#"{"Nats-Msg-Id":["mail-7"]}"#, "e25vdCBqc29u"] {
            assert_eq!(
                letter(&stored(&[(WITHHELD_HEADER, record)], b"{}"))["headers"],
                json!({}),
                "{record}"
            );
        }
    }
}
