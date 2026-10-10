use super::route::{Outcome, PendingRow, Route};
use crate::nats_conn::{publish_bounded, publish_timeout};
use async_nats::jetstream;
use async_nats::HeaderMap;
use async_trait::async_trait;
use cufflink_types::host::nats::{is_reserved_header, names_msg_id, MSG_ID_HEADER};
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesOrdered;
use futures_util::{FutureExt, StreamExt};
use uuid::Uuid;

pub type Ack = BoxFuture<'static, Result<(), String>>;

#[async_trait]
pub trait Transport: Sync {
    async fn send(&self, row: &PendingRow, route: &Route) -> Result<Ack, String>;
}

pub async fn publish_in_order<T: Transport>(
    transport: &T,
    rows: &[(PendingRow, Route)],
    concurrency: usize,
) -> Vec<Outcome> {
    let limit = concurrency.max(1);
    let mut in_flight: FuturesOrdered<BoxFuture<'static, Outcome>> = FuturesOrdered::new();
    let mut outcomes = Vec::with_capacity(rows.len());
    for (row, route) in rows {
        while let Some(outcome) = match in_flight.len() >= limit {
            true => in_flight.next().await,
            false => in_flight.next().now_or_never().flatten(),
        } {
            outcomes.push(outcome);
        }
        if outcomes.iter().any(Outcome::is_failure) {
            break;
        }
        let sent = match route {
            Route::Reject(reason) => Err(Outcome::Rejected(reason.clone())),
            Route::Unauthenticated(reason) => Err(Outcome::Unauthenticated(reason.clone())),
            Route::JetStream(_) | Route::Core => {
                transport.send(row, route).await.map_err(Outcome::Failed)
            }
        };
        match sent {
            Ok(ack) => in_flight.push_back(
                ack.map(|acked| acked.map_or_else(Outcome::Failed, |()| Outcome::Published))
                    .boxed(),
            ),
            Err(outcome) => {
                let failed = outcome.is_failure();
                in_flight.push_back(std::future::ready(outcome).boxed());
                if failed {
                    break;
                }
            }
        }
    }
    outcomes.extend(in_flight.collect::<Vec<_>>().await);
    outcomes
}

pub fn publish_headers(headers: &[(String, String)], msg_id: Uuid) -> HeaderMap {
    let mut headers = headers
        .iter()
        .filter(|(name, value)| {
            !is_reserved_header(name) && !names_msg_id(name) && !names_msg_id(value)
        })
        .filter_map(|(name, value)| {
            Some((
                name.parse::<async_nats::HeaderName>().ok()?,
                value.parse::<async_nats::HeaderValue>().ok()?,
            ))
        })
        .fold(HeaderMap::new(), |mut headers, (name, value)| {
            headers.append(name, value);
            headers
        });
    headers.insert(MSG_ID_HEADER, msg_id.to_string().as_str());
    headers
}

pub fn wire_size(payload_len: usize, headers: &HeaderMap) -> usize {
    let block = headers
        .iter()
        .flat_map(|(name, values)| {
            values.iter().map(move |value| {
                AsRef::<str>::as_ref(name).len()
                    + ": ".len()
                    + AsRef::<str>::as_ref(value).len()
                    + "\r\n".len()
            })
        })
        .sum::<usize>();
    "NATS/1.0\r\n".len() + block + "\r\n".len() + payload_len
}

pub fn max_payload(nats: &async_nats::Client) -> Option<usize> {
    nats.try_server_info()
        .map(|info| info.max_payload)
        .filter(|max| *max > 0)
}

pub struct NatsTransport<'a> {
    pub nats: &'a async_nats::Client,
    pub jetstream: &'a jetstream::Context,
}

#[async_trait]
impl Transport for NatsTransport<'_> {
    async fn send(&self, row: &PendingRow, route: &Route) -> Result<Ack, String> {
        let (subject, headers, payload) = (
            row.subject.clone(),
            row.publish_headers(),
            bytes::Bytes::from(row.payload.clone()),
        );
        match route {
            Route::JetStream(_) => {
                let timeout = publish_timeout();
                let sent = self
                    .jetstream
                    .publish_with_headers(subject, headers, payload);
                let ack = tokio::time::timeout(timeout, sent)
                    .await
                    .map_err(|_| format!("publish timed out after {timeout:?}"))?
                    .map_err(|e| format!("publish failed: {e}"))?;
                Ok(async move {
                    ack.await
                        .map(drop)
                        .map_err(|e| format!("not acknowledged: {e}"))
                }
                .boxed())
            }
            Route::Core => publish_bounded(self.nats, subject, Some(headers), payload)
                .await
                .map(|()| std::future::ready(Ok(())).boxed())
                .map_err(|e| e.to_string()),
            Route::Reject(reason) | Route::Unauthenticated(reason) => Err(reason.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::route::tests::row;
    use super::super::writer::OutboxKind;
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default)]
    struct Scripted {
        sent: Mutex<Vec<i64>>,
        unsendable: Vec<i64>,
        acks: HashMap<i64, (Duration, Result<(), String>)>,
    }

    #[async_trait]
    impl Transport for Scripted {
        async fn send(&self, row: &PendingRow, _: &Route) -> Result<Ack, String> {
            self.sent.lock().expect("sent").push(row.id);
            if self.unsendable.contains(&row.id) {
                return Err("not connected".into());
            }
            let (after, acked) = self
                .acks
                .get(&row.id)
                .cloned()
                .unwrap_or((Duration::ZERO, Ok(())));
            Ok(async move {
                tokio::time::sleep(after).await;
                acked
            }
            .boxed())
        }
    }

    impl Scripted {
        fn sent(&self) -> Vec<i64> {
            self.sent.lock().expect("sent").clone()
        }
    }

    fn batch(ids: std::ops::RangeInclusive<i64>) -> Vec<(PendingRow, Route)> {
        ids.map(|id| (row(id, OutboxKind::Event, "books.created"), Route::Core))
            .collect()
    }

    fn failed(reason: &str) -> Outcome {
        Outcome::Failed(reason.into())
    }

    #[tokio::test]
    async fn rows_are_sent_in_their_order_and_settled_in_it() {
        let transport = Scripted {
            acks: [(1, (Duration::from_millis(30), Ok(())))].into(),
            ..Default::default()
        };
        let outcomes = publish_in_order(&transport, &batch(1..=5), 64).await;
        assert_eq!(transport.sent(), [1, 2, 3, 4, 5]);
        assert_eq!(outcomes, vec![Outcome::Published; 5]);
    }

    #[tokio::test]
    async fn nothing_is_sent_after_a_row_that_could_not_be() {
        let transport = Scripted {
            unsendable: vec![3],
            ..Default::default()
        };
        let outcomes = publish_in_order(&transport, &batch(1..=6), 64).await;
        assert_eq!(transport.sent(), [1, 2, 3]);
        assert_eq!(
            outcomes,
            [
                Outcome::Published,
                Outcome::Published,
                failed("not connected")
            ]
        );
    }

    #[tokio::test]
    async fn nothing_is_sent_once_an_acknowledgement_is_known_to_have_failed() {
        let transport = Scripted {
            acks: [(2, (Duration::ZERO, Err("no stream".to_string())))].into(),
            ..Default::default()
        };
        let outcomes = publish_in_order(&transport, &batch(1..=6), 1).await;
        assert_eq!(transport.sent(), [1, 2]);
        assert_eq!(outcomes, [Outcome::Published, failed("no stream")]);
    }

    #[tokio::test]
    async fn what_was_in_flight_when_a_row_failed_is_settled_by_its_own_answer() {
        let transport = Scripted {
            acks: [
                (1, (Duration::from_millis(50), Err("timed out".to_string()))),
                (3, (Duration::from_millis(10), Ok(()))),
            ]
            .into(),
            ..Default::default()
        };
        let outcomes = publish_in_order(&transport, &batch(1..=9), 3).await;
        assert_eq!(transport.sent(), [1, 2, 3]);
        assert_eq!(
            outcomes,
            [failed("timed out"), Outcome::Published, Outcome::Published]
        );
    }

    #[tokio::test]
    async fn no_more_acknowledgements_are_outstanding_than_allowed() {
        let slow = |id| (id, (Duration::from_millis(400), Ok(())));
        let transport = Scripted {
            acks: (1..=4).map(slow).collect(),
            ..Default::default()
        };
        let rows = batch(1..=4);
        let publishing = publish_in_order(&transport, &rows, 2);
        tokio::pin!(publishing);
        let waited = tokio::time::timeout(Duration::from_millis(100), &mut publishing).await;
        assert!(waited.is_err());
        assert_eq!(transport.sent(), [1, 2]);
        assert_eq!(publishing.await, vec![Outcome::Published; 4]);
        assert_eq!(transport.sent(), [1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn a_rejected_row_is_not_sent_and_holds_nothing_up() {
        let transport = Scripted::default();
        let mut rows = batch(1..=3);
        rows[1].1 = Route::Reject("not the tenant's".into());
        let outcomes = publish_in_order(&transport, &rows, 64).await;
        assert_eq!(transport.sent(), [1, 3]);
        assert_eq!(
            outcomes,
            [
                Outcome::Published,
                Outcome::Rejected("not the tenant's".into()),
                Outcome::Published
            ]
        );
    }

    #[test]
    fn a_row_is_published_under_its_id_and_never_steers_the_server_or_the_platform() {
        let forged = PendingRow {
            headers: vec![
                ("traceparent".into(), "00-1-2-01".into()),
                ("Nats-Rollup".into(), "all".into()),
                ("nats-expected-stream".into(), "BOOKS".into()),
                ("Nats-Msg-Id".into(), "mine".into()),
                ("bad name".into(), "x".into()),
                ("X-Kind".into(), "a".into()),
                ("X-Kind".into(), "b".into()),
                ("cufflink-outbox-mac".into(), "00".into()),
                ("Cufflink-Reply".into(), "_INBOX.stolen".into()),
                ("CUFFLINK-DLQ-REASON".into(), "forged".into()),
                ("Cufflink-Replayed-From".into(), "7".into()),
                ("X-Original-Nats-Msg-Id".into(), "book-7".into()),
                ("X-Note".into(), "copied from Nats-Msg-Id".into()),
            ],
            ..row(4, OutboxKind::Event, "books.created")
        };
        let headers = forged.publish_headers();
        let values = |name: &str| -> Vec<String> {
            headers
                .get_all(name)
                .map(|value| value.to_string())
                .collect()
        };
        assert_eq!(values("traceparent"), ["00-1-2-01"]);
        assert_eq!(values("X-Kind"), ["a", "b"]);
        assert_eq!(values("Nats-Msg-Id"), [forged.msg_id.to_string()]);
        assert_eq!(headers.len(), 3);
        assert!(crate::nats_manager::headers::msg_id_unhidden(&headers));
    }

    #[test]
    fn a_message_takes_its_header_block_and_its_payload_on_the_wire() {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", "00-1-2-01");
        headers.append("X-Kind", "a");
        headers.append("X-Kind", "b");
        let block = "NATS/1.0\r\ntraceparent: 00-1-2-01\r\nX-Kind: a\r\nX-Kind: b\r\n\r\n";
        assert_eq!(wire_size(10, &headers), block.len() + 10);
        assert_eq!(
            wire_size(10, &HeaderMap::new()),
            "NATS/1.0\r\n\r\n".len() + 10
        );
        let sized = row(1, OutboxKind::Event, "books.created");
        assert_eq!(
            sized.wire_size(),
            wire_size(sized.payload.len(), &sized.publish_headers())
        );
    }
}
