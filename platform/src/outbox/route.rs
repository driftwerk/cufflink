use super::publish::{publish_headers, wire_size};
use super::seal::{authenticate, OutboxSeal};
use super::writer::{headers_of, OutboxKind};
use crate::nats_manager::stream_index::StreamSnapshot;
use crate::nats_manager::subjects::Namespace;
use async_nats::HeaderMap;
use cufflink_types::backoff::Backoff;
use cufflink_types::nats_names::subject_violation;
use std::time::Duration;
use uuid::Uuid;

pub const WEBHOOKS_STREAM: &str = "CUFFLINK_WEBHOOKS";
pub const WEBHOOK_SUBJECT_PREFIX: &str = "cufflink.webhook.";
pub const AUDIT_STREAM: &str = "CUFFLINK_AUDIT";
pub const AUDIT_SUBJECT_PREFIX: &str = "cufflink.audit.";

pub const REJECTED: &str = "rejected";
pub const UNAUTHENTICATED: &str = "unauthenticated";

pub fn legacy_audit_subject(tenant_slug: &str) -> String {
    format!("dw.{tenant_slug}.logs")
}

pub fn audit_subject(tenant_slug: &str, service_name: &str) -> String {
    format!("{AUDIT_SUBJECT_PREFIX}{tenant_slug}.{service_name}")
}

pub fn webhook_subject(tenant_slug: &str, webhook_id: Uuid) -> String {
    format!("{WEBHOOK_SUBJECT_PREFIX}{tenant_slug}.{webhook_id}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRow {
    pub id: i64,
    pub msg_id: Uuid,
    pub kind: OutboxKind,
    pub service: String,
    pub source_role: String,
    pub subject: String,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub attempts: i32,
}

impl PendingRow {
    pub fn of(row: &tokio_postgres::Row) -> Result<Self, String> {
        let kind: String = row.get("kind");
        Ok(Self {
            id: row.get("id"),
            msg_id: row.get("msg_id"),
            kind: kind.parse()?,
            service: row.get("service"),
            source_role: row.get("source_role"),
            subject: row.get("subject"),
            payload: row.get("payload"),
            headers: headers_of(&row.get("headers")),
            attempts: row.get("attempts"),
        })
    }

    pub fn publish_headers(&self) -> HeaderMap {
        publish_headers(&self.headers, self.msg_id)
    }

    pub fn wire_size(&self) -> usize {
        wire_size(self.payload.len(), &self.publish_headers())
    }

    fn failures(&self) -> u32 {
        u32::try_from(self.attempts)
            .unwrap_or_default()
            .saturating_add(1)
    }
}

#[derive(Debug, Clone)]
pub struct Routing<'a> {
    pub tenant_id: Uuid,
    pub seal: Option<&'a OutboxSeal>,
    pub namespace: &'a Namespace,
    pub streams: &'a StreamSnapshot,
    pub max_payload: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    JetStream(String),
    Core,
    Reject(String),
    Unauthenticated(String),
}

impl Route {
    pub fn label(&self) -> &'static str {
        match self {
            Route::JetStream(_) => "jetstream",
            Route::Core => "core",
            Route::Reject(_) => "reject",
            Route::Unauthenticated(_) => UNAUTHENTICATED,
        }
    }
}

fn is_under(prefix: &str, tenant_slug: &str, subject: &str) -> bool {
    subject
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix(tenant_slug))
        .and_then(|rest| rest.strip_prefix('.'))
        .is_some_and(|rest| !rest.is_empty())
}

pub fn classify(row: &PendingRow, routing: &Routing<'_>) -> Route {
    let Routing {
        tenant_id,
        seal,
        namespace,
        streams,
        max_payload,
    } = routing;
    let tenant = namespace.tenant_slug();
    let subject = row.subject.as_str();
    let captured = || {
        streams
            .capturing(subject)
            .map_or(Route::Core, |stream| Route::JetStream(stream.name))
    };
    if let Some(reason) = subject_violation(subject) {
        return Route::Reject(reason);
    }
    let size = row.wire_size();
    if let Some(max_payload) = max_payload.filter(|max_payload| size > *max_payload) {
        return Route::Reject(format!(
            "{size} bytes on the wire is more than the {max_payload} bytes the server carries in one message"
        ));
    }
    if let Err(why) = authenticate(*seal, *tenant_id, row) {
        return Route::Unauthenticated(why.to_string());
    }
    match row.kind {
        OutboxKind::Webhook if is_under(WEBHOOK_SUBJECT_PREFIX, tenant, subject) => {
            Route::JetStream(WEBHOOKS_STREAM.to_string())
        }
        OutboxKind::Audit if is_under(AUDIT_SUBJECT_PREFIX, tenant, subject) => {
            Route::JetStream(AUDIT_STREAM.to_string())
        }
        OutboxKind::Audit if subject == legacy_audit_subject(tenant) => captured(),
        OutboxKind::Event if namespace.contains(subject) => captured(),
        kind => Route::Reject(format!(
            "'{subject}' is not where the tenant '{tenant}' publishes {} rows",
            kind.as_str()
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Published,
    Failed(String),
    Rejected(String),
    Unauthenticated(String),
}

impl Outcome {
    pub fn is_failure(&self) -> bool {
        matches!(self, Outcome::Failed(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GivenUp {
    Rejected,
    Unauthenticated,
    Exhausted,
}

impl GivenUp {
    pub fn as_str(self) -> &'static str {
        match self {
            GivenUp::Rejected => REJECTED,
            GivenUp::Unauthenticated => UNAUTHENTICATED,
            GivenUp::Exhausted => "exhausted",
        }
    }

    pub fn handler(self, kind: OutboxKind) -> &'static str {
        match self {
            GivenUp::Rejected | GivenUp::Unauthenticated => self.as_str(),
            GivenUp::Exhausted => kind.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    Delete,
    DeadLetter { why: GivenUp, reason: String },
    Retry { delay: Duration, error: String },
}

pub fn retry_delay(backoff: &Backoff, row: &PendingRow) -> Duration {
    backoff.delay(row.failures(), row.id.unsigned_abs())
}

pub fn dispose(
    row: &PendingRow,
    outcome: Outcome,
    max_attempts: u32,
    backoff: &Backoff,
) -> Disposition {
    match outcome {
        Outcome::Published => Disposition::Delete,
        Outcome::Rejected(reason) => Disposition::DeadLetter {
            why: GivenUp::Rejected,
            reason,
        },
        Outcome::Unauthenticated(reason) => Disposition::DeadLetter {
            why: GivenUp::Unauthenticated,
            reason,
        },
        Outcome::Failed(reason) if row.failures() >= max_attempts => Disposition::DeadLetter {
            why: GivenUp::Exhausted,
            reason,
        },
        Outcome::Failed(error) => Disposition::Retry {
            delay: retry_delay(backoff, row),
            error,
        },
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::nats_manager::stream_index::StreamEntry;
    use crate::nats_manager::subjects::Claim;
    use crate::outbox::seal::tests as seal;
    use async_nats::jetstream::stream::RetentionPolicy;
    use cufflink_types::nats_names::SUBJECT_MAX;

    pub(crate) fn row(id: i64, kind: OutboxKind, subject: &str) -> PendingRow {
        PendingRow {
            id,
            msg_id: Uuid::from_u128(id.unsigned_abs().into()),
            kind,
            service: "loans".into(),
            source_role: "cufflink".into(),
            subject: subject.into(),
            payload: format!("{{\"n\":{id}}}").into_bytes(),
            headers: vec![("traceparent".into(), "00-1-2-01".into())],
            attempts: 0,
        }
    }

    fn streams() -> StreamSnapshot {
        let stream = |name: &str, subjects: &[&str], retention| StreamEntry {
            name: name.into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            retention,
        };
        StreamSnapshot::new(vec![
            stream("DW_HARBOR", &["dw.harbor.>"], RetentionPolicy::Interest),
            stream(
                "BOOKS",
                &["books.created", "books.updated"],
                RetentionPolicy::Limits,
            ),
        ])
    }

    fn harbor() -> Namespace {
        Namespace::new(
            "harbor",
            [Claim::subject("books.>"), Claim::stream("BOOKS")],
        )
    }

    fn routing<'a>(
        namespace: &'a Namespace,
        streams: &'a StreamSnapshot,
        max_payload: Option<usize>,
    ) -> Routing<'a> {
        static SEAL: std::sync::LazyLock<OutboxSeal> = std::sync::LazyLock::new(seal::seal);
        Routing {
            tenant_id: seal::tenant(),
            seal: Some(&SEAL),
            namespace,
            streams,
            max_payload,
        }
    }

    fn route(kind: OutboxKind, subject: &str) -> Route {
        classify(
            &seal::sealed(row(1, kind, subject)),
            &routing(&harbor(), &streams(), None),
        )
    }

    fn rejected(route: Route) -> bool {
        matches!(route, Route::Reject(_))
    }

    #[test]
    fn a_subject_longer_than_the_server_reads_is_rejected() {
        let under = "dw.harbor.loans.";
        let longest = format!("{under}{}", "x".repeat(SUBJECT_MAX - under.len()));
        assert_eq!(
            route(OutboxKind::Event, &longest),
            Route::JetStream("DW_HARBOR".into())
        );
        assert_eq!(
            route(OutboxKind::Event, &format!("{longest}x")),
            Route::Reject(format!(
                "a subject of {} bytes is longer than the {SUBJECT_MAX} bytes a message is published on",
                SUBJECT_MAX + 1
            ))
        );
    }

    #[test]
    fn a_row_the_server_cannot_carry_in_one_message_is_rejected() {
        let sized = row(1, OutboxKind::Event, "books.created");
        let size = sized.wire_size();
        assert!(size > sized.payload.len());
        let with = |max_payload| classify(&sized, &routing(&harbor(), &streams(), max_payload));
        assert_eq!(with(Some(size)), Route::JetStream("BOOKS".into()));
        assert_eq!(with(None), Route::JetStream("BOOKS".into()));
        assert_eq!(
            with(Some(size - 1)),
            Route::Reject(format!(
                "{size} bytes on the wire is more than the {} bytes the server carries in one message",
                size - 1
            ))
        );
    }

    #[test]
    fn an_event_goes_to_the_stream_that_captures_it_or_to_whoever_listens() {
        assert_eq!(
            route(OutboxKind::Event, "dw.harbor.loans.loans.created"),
            Route::JetStream("DW_HARBOR".into())
        );
        assert_eq!(
            route(OutboxKind::Event, "books.created"),
            Route::JetStream("BOOKS".into())
        );
        assert_eq!(route(OutboxKind::Event, "books.deleted"), Route::Core);
        assert_eq!(
            classify(
                &row(1, OutboxKind::Event, "dw.harbor.loans.loans.created"),
                &routing(&harbor(), &StreamSnapshot::default(), None)
            ),
            Route::Core
        );
    }

    #[test]
    fn an_event_outside_the_tenants_namespace_is_rejected() {
        for subject in [
            "dw.default.loans.loans.created",
            "receipts.created",
            "cufflink.audit.harbor.loans",
            "cufflink.webhook.harbor.1",
            "cufflink.tick.5min",
            "$JS.API.STREAM.PURGE.DW_HARBOR",
        ] {
            assert_eq!(
                route(OutboxKind::Event, subject),
                Route::Reject(format!(
                    "'{subject}' is not where the tenant 'harbor' publishes event rows"
                )),
            );
        }
    }

    #[test]
    fn what_is_not_a_subject_is_rejected_whatever_its_kind() {
        for kind in [OutboxKind::Event, OutboxKind::Audit, OutboxKind::Webhook] {
            for subject in [
                "",
                "books.>",
                "books.*",
                "dw.harbor..logs",
                "books. created",
            ] {
                assert_eq!(
                    route(kind, subject),
                    Route::Reject(format!("'{subject}' is not a subject")),
                    "{kind:?}"
                );
            }
        }
    }

    #[test]
    fn audit_rows_go_to_the_tenants_feed_and_to_the_audit_stream() {
        assert_eq!(
            route(OutboxKind::Audit, &legacy_audit_subject("harbor")),
            Route::JetStream("DW_HARBOR".into())
        );
        assert_eq!(
            route(OutboxKind::Audit, &audit_subject("harbor", "loan-service")),
            Route::JetStream(AUDIT_STREAM.into())
        );
        for subject in [
            "dw.default.logs",
            "cufflink.audit.default.loans",
            "cufflink.audit.harbor",
            "cufflink.audit.harbor.",
            "cufflink.audit.harbor-two.loans",
            "dw.harbor.loans.loans.created",
            "books.created",
        ] {
            assert!(rejected(route(OutboxKind::Audit, subject)), "{subject}");
        }
    }

    #[test]
    fn webhook_rows_go_to_the_webhook_stream_of_their_tenant_alone() {
        let id = Uuid::from_u128(7);
        assert_eq!(
            route(OutboxKind::Webhook, &webhook_subject("harbor", id)),
            Route::JetStream(WEBHOOKS_STREAM.into())
        );
        for subject in [
            webhook_subject("default", id),
            "cufflink.webhook.harbor".to_string(),
            "dw.harbor.loans.loans.created".to_string(),
        ] {
            assert!(rejected(route(OutboxKind::Webhook, &subject)), "{subject}");
        }
    }

    #[test]
    fn an_audit_or_webhook_row_the_platform_did_not_write_is_never_published() {
        let unsealed = |kind, subject: &str| {
            classify(
                &row(1, kind, subject),
                &routing(&harbor(), &streams(), None),
            )
        };
        let webhook = webhook_subject("harbor", Uuid::from_u128(7));
        for (kind, subject) in [
            (OutboxKind::Audit, legacy_audit_subject("harbor")),
            (OutboxKind::Audit, audit_subject("harbor", "loans")),
            (OutboxKind::Webhook, webhook.clone()),
        ] {
            assert!(
                matches!(unsealed(kind, &subject), Route::Unauthenticated(why) if why.contains("carries no MAC")),
                "{kind:?} {subject}"
            );
        }
        assert_eq!(
            unsealed(OutboxKind::Event, "books.created"),
            Route::JetStream("BOOKS".into())
        );
        assert_eq!(
            unsealed(OutboxKind::Webhook, "cufflink.webhook.harbor.>"),
            Route::Reject("'cufflink.webhook.harbor.>' is not a subject".into()),
            "what is no subject is refused as such"
        );
        let (namespace, streams) = (harbor(), streams());
        let keyless = Routing {
            seal: None,
            ..routing(&namespace, &streams, None)
        };
        assert!(matches!(
            classify(&seal::sealed(row(1, OutboxKind::Webhook, &webhook)), &keyless),
            Route::Unauthenticated(why) if why.contains("CUFFLINK_CACHE_SIGNING_KEY")
        ));
        let another_tenant = Routing {
            tenant_id: Uuid::from_u128(1),
            ..routing(&namespace, &streams, None)
        };
        assert!(matches!(
            classify(&seal::sealed(row(1, OutboxKind::Webhook, &webhook)), &another_tenant),
            Route::Unauthenticated(why) if why.contains("does not match")
        ));
    }

    #[test]
    fn a_row_is_kept_until_it_is_published_or_given_up() {
        let backoff = Backoff::exponential(Duration::from_secs(1), Duration::from_secs(300));
        let failed = |attempts| PendingRow {
            attempts,
            ..row(9, OutboxKind::Event, "books.created")
        };
        let dispose = |row: &PendingRow, outcome| dispose(row, outcome, 20, &backoff);
        assert_eq!(dispose(&failed(0), Outcome::Published), Disposition::Delete);
        assert_eq!(
            dispose(&failed(0), Outcome::Unauthenticated("forged".into())),
            Disposition::DeadLetter {
                why: GivenUp::Unauthenticated,
                reason: "forged".into()
            }
        );
        assert_eq!(
            dispose(&failed(0), Outcome::Rejected("not the tenant's".into())),
            Disposition::DeadLetter {
                why: GivenUp::Rejected,
                reason: "not the tenant's".into()
            }
        );
        assert_eq!(
            dispose(&failed(0), Outcome::Failed("timed out".into())),
            Disposition::Retry {
                delay: backoff.delay(1, 9),
                error: "timed out".into()
            }
        );
        assert_eq!(
            dispose(&failed(18), Outcome::Failed("timed out".into())),
            Disposition::Retry {
                delay: backoff.delay(19, 9),
                error: "timed out".into()
            }
        );
        assert_eq!(
            dispose(&failed(19), Outcome::Failed("timed out".into())),
            Disposition::DeadLetter {
                why: GivenUp::Exhausted,
                reason: "timed out".into()
            }
        );
        assert!(matches!(
            dispose(&failed(-3), Outcome::Failed("timed out".into())),
            Disposition::Retry { .. }
        ));
    }

    #[test]
    fn a_row_given_up_is_filed_by_why() {
        assert_eq!(GivenUp::Rejected.handler(OutboxKind::Event), "rejected");
        assert_eq!(
            GivenUp::Unauthenticated.handler(OutboxKind::Audit),
            "unauthenticated"
        );
        assert_eq!(GivenUp::Exhausted.handler(OutboxKind::Event), "event");
        assert_eq!(GivenUp::Exhausted.handler(OutboxKind::Audit), "audit");
        assert_eq!(GivenUp::Exhausted.handler(OutboxKind::Webhook), "webhook");
    }
}
