use super::bootstrap;
use super::publish::max_payload;
use super::route::{audit_subject, legacy_audit_subject, webhook_subject};
use super::seal::{OutboxSeal, NO_KEY};
use super::writer::{OutboxKind, OutboxRow, OutboxWriter};
use crate::audit::{self, Actor, AuditRecord, Read};
use crate::auth::scope::EffectiveScope;
use crate::auth::AuthUser;
use crate::errors::{PlatformError, PlatformResult};
use crate::nats_conn::{publish_bounded, spawn_publish};
use crate::policy::{PolicyKey, PolicyMode};
use crate::registry::RegisteredService;
use crate::tenant_catalog::TenantEntry;
use crate::tenant_sessions::{Purpose, Statements};
use crate::AppState;
use async_nats::HeaderMap;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const RETRY_MADE: Duration = Duration::from_secs(1);
const RETRY_NOT_MADE: Duration = Duration::from_secs(30);
const READ: &str = "read";
const RECORD_WITHIN: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Created,
    Updated,
    Deleted,
}

impl ChangeKind {
    pub fn event(self) -> &'static str {
        match self {
            ChangeKind::Created => "created",
            ChangeKind::Updated => "updated",
            ChangeKind::Deleted => "deleted",
        }
    }

    pub fn action(self) -> &'static str {
        match self {
            ChangeKind::Created => "create",
            ChangeKind::Updated => "update",
            ChangeKind::Deleted => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Target<'a> {
    pub tenant_slug: &'a str,
    pub tenant_id: Uuid,
    pub service: &'a str,
    pub table: &'a str,
    pub caller: &'a Option<AuthUser>,
    pub calling: Option<&'a str>,
    pub request_id: Option<&'a str>,
    pub redact: &'a [String],
}

#[derive(Debug, Clone, Copy)]
pub struct Change<'a> {
    pub kind: ChangeKind,
    pub row: &'a Value,
    pub record_id: &'a str,
    pub scope: Option<&'a EffectiveScope>,
    pub before: Option<&'a Value>,
}

impl<'a> Target<'a> {
    pub fn of(
        tenant_slug: &'a str,
        tenant_id: Uuid,
        service: &'a str,
        table: &'a str,
        caller: &'a Option<AuthUser>,
    ) -> Self {
        Self {
            tenant_slug,
            tenant_id,
            service,
            table,
            caller,
            calling: None,
            request_id: None,
            redact: &[],
        }
    }

    pub fn event_subject(&self, kind: ChangeKind) -> String {
        format!(
            "dw.{}.{}.{}.{}",
            self.tenant_slug,
            self.service,
            self.table,
            kind.event()
        )
    }

    pub fn event(&self, change: &Change<'_>) -> Value {
        json!({
            "action": change.kind.event(),
            "service": self.service,
            "table": self.table,
            "data": change.row,
            "tenant_id": self.tenant_id,
        })
    }

    fn actor(&self) -> Actor {
        self.calling
            .map_or_else(|| Actor::of(self.caller), Actor::service)
    }

    fn record(&self, action: &str, at: DateTime<Utc>) -> AuditRecord {
        let (user_id, user_name) = audit::named(self.caller);
        AuditRecord {
            subject: audit::subject_of(self.caller),
            timestamp: at,
            tenant: self.tenant_slug.to_string(),
            service: self.service.to_string(),
            table: Some(self.table.to_string()),
            action: action.to_string(),
            user_id,
            user_name,
            record_id: None,
            data: None,
            previous: None,
            before: None,
            scope: None,
            actor: self.actor(),
            request_id: self.request_id.map(str::to_string),
            redacted: Vec::new(),
        }
    }

    pub fn audit(&self, change: &Change<'_>, at: DateTime<Utc>) -> AuditRecord {
        let row = Some(change.row.clone());
        let (data, previous) = match change.kind {
            ChangeKind::Created | ChangeKind::Updated => (row, None),
            ChangeKind::Deleted => (None, row),
        };
        AuditRecord {
            record_id: Some(change.record_id.to_string()),
            data,
            previous,
            before: change.before.cloned(),
            scope: change.scope.map(EffectiveScope::audit_kind),
            ..self.record(change.kind.action(), at)
        }
        .redact(self.redact)
    }

    pub fn emission(&self, change: &Change<'_>, at: DateTime<Utc>) -> Emission {
        let record = self.audit(change, at);
        Emission {
            kind: change.kind,
            event: Message {
                subject: self.event_subject(change.kind),
                payload: self.event(change).to_string().into_bytes(),
            },
            audit: Message {
                subject: legacy_audit_subject(self.tenant_slug),
                payload: record.legacy_payload().to_string().into_bytes(),
            },
            durable: self.durable(&record),
        }
    }

    fn durable(&self, record: &AuditRecord) -> Message {
        Message {
            subject: audit_subject(self.tenant_slug, self.service),
            payload: record.durable_payload().to_string().into_bytes(),
        }
    }

    pub fn read(&self, read: &Read, scope: Option<&EffectiveScope>, at: DateTime<Utc>) -> Message {
        self.durable(&AuditRecord {
            data: Some(read.data()),
            scope: scope.map(EffectiveScope::audit_kind),
            ..self.record(READ, at)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub subject: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Emission {
    pub kind: ChangeKind,
    pub event: Message,
    pub audit: Message,
    pub durable: Message,
}

impl Emission {
    fn rows(&self, service: &str, headers: &[(String, String)]) -> [OutboxRow; 3] {
        let row = |kind, message: &Message| {
            OutboxRow::new(
                kind,
                service,
                message.subject.clone(),
                message.payload.clone(),
                headers.to_vec(),
            )
        };
        [
            row(OutboxKind::Event, &self.event),
            row(OutboxKind::Audit, &self.audit),
            row(OutboxKind::Audit, &self.durable),
        ]
    }

    async fn publish_event(&self, nats: &async_nats::Client, headers: HeaderMap) {
        let published = publish_bounded(
            nats,
            self.event.subject.clone(),
            Some(headers),
            self.event.payload.clone().into(),
        )
        .await;
        if let Err(e) = published {
            tracing::warn!("Failed to publish NATS {} event: {}", self.kind.event(), e);
        }
    }
}

fn carried(rows: &[OutboxRow], max_payload: usize) -> PlatformResult<()> {
    rows.iter()
        .map(|row| (row, row.wire_size()))
        .find(|(_, size)| *size > max_payload)
        .map_or(Ok(()), |(row, size)| {
            Err(PlatformError::PayloadTooLarge(format!(
                "the {} of this write is {size} bytes on the wire, more than the {max_payload} \
                 bytes the NATS server carries in one message (its max_payload)",
                row.kind.as_str()
            )))
        })
}

fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .flat_map(|(name, values)| {
            values
                .iter()
                .map(move |value| (name.to_string(), value.to_string()))
        })
        .collect()
}

#[derive(Clone, Copy)]
pub struct Sink<'a> {
    state: &'a Arc<AppState>,
    tenant_slug: &'a str,
    service: &'a RegisteredService,
    enforced: bool,
}

impl<'a> Sink<'a> {
    pub fn of(
        state: &'a Arc<AppState>,
        tenant: &'a TenantEntry,
        service: &'a RegisteredService,
    ) -> Self {
        Self {
            state,
            tenant_slug: &tenant.tenant.slug,
            service,
            enforced: tenant.mode(PolicyKey::Outbox) == PolicyMode::Enforce,
        }
    }

    fn durable(&self) -> bool {
        self.enforced || self.state.config.audit.durable_all
    }

    fn count(&self, action: &str) {
        audit::count(self.tenant_slug, action);
    }

    fn row(&self, kind: OutboxKind, message: Message, headers: &[(String, String)]) -> OutboxRow {
        OutboxRow::new(
            kind,
            &self.service.name,
            message.subject,
            message.payload,
            headers.to_vec(),
        )
    }

    async fn not_ready(&self) -> PlatformError {
        let tenant = self.tenant_slug;
        match bootstrap::ensure(&self.state.tenant_sessions, self.service.tenant_id).await {
            Ok(report) => {
                tracing::info!(
                    tenant,
                    applied = report.applied.len(),
                    "Platform schema asked for by a write that found none"
                );
                PlatformError::RetryLater {
                    message: "the tenant's outbox was not ready: nothing was written, and \
                              the request can be made again"
                        .into(),
                    after: RETRY_MADE,
                }
            }
            Err(e) => {
                tracing::error!(tenant, error = %e, "Platform schema could not be made ready for a write that found none");
                PlatformError::RetryLater {
                    message: format!(
                        "the tenant's outbox is not ready, and nothing was written: {e}"
                    ),
                    after: RETRY_NOT_MADE,
                }
            }
        }
    }

    fn seal(state: &AppState) -> PlatformResult<&OutboxSeal> {
        state
            .outbox_seal
            .as_ref()
            .ok_or_else(|| PlatformError::Unavailable(NO_KEY.into()))
    }

    async fn write<C: Statements>(&self, tx: &C, rows: &[OutboxRow]) -> PlatformResult<()> {
        if let Some(max_payload) = max_payload(&self.state.nats) {
            carried(rows, max_payload)?;
        }
        let writer = OutboxWriter::new(
            tx,
            self.service.tenant_id,
            self.tenant_slug,
            Self::seal(self.state)?,
        );
        match writer.write(rows).await {
            Err(e) if bootstrap::is_missing(&e) => Err(self.not_ready().await),
            written => Ok(written?),
        }
    }

    pub fn record(&self, record: &AuditRecord) {
        self.count(&record.action);
        let legacy = Message {
            subject: legacy_audit_subject(self.tenant_slug),
            payload: record.legacy_payload().to_string().into_bytes(),
        };
        let durable = Message {
            subject: audit_subject(self.tenant_slug, &self.service.name),
            payload: record.durable_payload().to_string().into_bytes(),
        };
        if !self.enforced {
            spawn_publish(
                self.state.nats.clone(),
                legacy.subject,
                None,
                legacy.payload.into(),
                "audit event",
            );
            if self.durable() {
                audit::publish_durable(self.state, durable.subject, durable.payload);
            }
            return;
        }
        let headers = header_pairs(&crate::telemetry::traced_nats_headers());
        let rows = [legacy, durable].map(|message| self.row(OutboxKind::Audit, message, &headers));
        let (state, identity, tenant_slug) = (
            Arc::clone(self.state),
            self.service.identity(),
            self.tenant_slug.to_string(),
        );
        let carries = max_payload(&self.state.nats);
        self.state.shutdown.spawn("audit_outbox", async move {
            let written = tokio::time::timeout(RECORD_WITHIN, async {
                carries.map_or(Ok(()), |max_payload| carried(&rows, max_payload))?;
                let seal = Self::seal(&state)?;
                let sessions = state
                    .tenant_sessions
                    .service(&identity, Purpose::Crud)
                    .await?;
                let client = sessions.get().await?;
                Ok::<_, PlatformError>(
                    OutboxWriter::new(&client, identity.tenant_id, &tenant_slug, seal)
                        .write(&rows)
                        .await?,
                )
            })
            .await
            .unwrap_or_else(|_| {
                Err(PlatformError::GatewayTimeout(format!(
                    "the outbox took longer than {RECORD_WITHIN:?}"
                )))
            });
            if let Err(e) = written {
                audit::count_failed("outbox");
                tracing::warn!(tenant = %tenant_slug, error = %e, "Dropped audit record");
            }
        });
    }
}

pub struct Outlet<'a> {
    sink: Sink<'a>,
    target: Target<'a>,
}

impl<'a> Outlet<'a> {
    pub fn of(
        state: &'a Arc<AppState>,
        tenant: &'a TenantEntry,
        service: &'a RegisteredService,
        target: Target<'a>,
    ) -> Self {
        Self {
            sink: Sink::of(state, tenant, service),
            target,
        }
    }

    pub fn emission(&self, change: &Change<'_>) -> Emission {
        self.target.emission(change, Utc::now())
    }

    pub fn enforced(&self) -> bool {
        self.sink.enforced
    }

    pub fn durable(&self) -> bool {
        self.sink.durable()
    }

    async fn deliveries(
        &self,
        emission: &Emission,
        headers: &[(String, String)],
    ) -> PlatformResult<Vec<OutboxRow>> {
        let Sink {
            state,
            tenant_slug,
            service,
            ..
        } = self.sink;
        let webhooks = state
            .webhooks
            .index
            .matching(
                &state.platform_db,
                service.service_id,
                self.target.table,
                emission.kind.action(),
            )
            .await?;
        Ok(webhooks
            .into_iter()
            .map(|webhook| {
                OutboxRow::new(
                    OutboxKind::Webhook,
                    &service.name,
                    webhook_subject(tenant_slug, webhook),
                    emission.event.payload.clone(),
                    headers.to_vec(),
                )
            })
            .collect())
    }

    pub async fn enqueue<C: Statements>(
        &self,
        tx: &C,
        emissions: &[Emission],
    ) -> PlatformResult<()> {
        if !self.sink.enforced {
            return Ok(());
        }
        let headers = header_pairs(&crate::telemetry::traced_nats_headers());
        let mut rows = Vec::with_capacity(emissions.len() * 3);
        for emission in emissions {
            rows.extend(emission.rows(&self.sink.service.name, &headers));
            rows.extend(self.deliveries(emission, &headers).await?);
        }
        self.sink.write(tx, &rows).await?;
        emissions
            .iter()
            .for_each(|emission| self.sink.count(emission.kind.action()));
        Ok(())
    }

    pub async fn publish(&self, emission: Emission) {
        let Sink {
            state, enforced, ..
        } = self.sink;
        if enforced {
            return;
        }
        self.sink.count(emission.kind.action());
        emission
            .publish_event(&state.nats, crate::telemetry::traced_nats_headers())
            .await;
        spawn_publish(
            state.nats.clone(),
            emission.audit.subject,
            Some(crate::telemetry::traced_nats_headers()),
            emission.audit.payload.into(),
            "audit event",
        );
        if self.sink.durable() {
            audit::publish_durable(state, emission.durable.subject, emission.durable.payload);
        }
    }

    pub fn publish_detached(&self, emissions: Vec<Emission>) {
        let Sink {
            state, enforced, ..
        } = self.sink;
        if enforced || emissions.is_empty() {
            return;
        }
        emissions
            .iter()
            .for_each(|emission| self.sink.count(emission.kind.action()));
        let (nats, jetstream) = (state.nats.clone(), state.jetstream.clone());
        let headers = crate::telemetry::traced_nats_headers();
        let durable = self.sink.durable();
        state.shutdown.spawn("batch_events", async move {
            for emission in emissions {
                emission.publish_event(&nats, headers.clone()).await;
                let audited = publish_bounded(
                    &nats,
                    emission.audit.subject,
                    Some(headers.clone()),
                    emission.audit.payload.into(),
                )
                .await;
                if let Err(e) = audited {
                    audit::count_failed("legacy");
                    tracing::warn!("Dropped audit event: {}", e);
                }
                if durable {
                    let kept = crate::nats_conn::jetstream_publish_bounded(
                        &jetstream,
                        emission.durable.subject,
                        Some(headers.clone()),
                        emission.durable.payload.into(),
                    )
                    .await;
                    if let Err(e) = kept {
                        audit::count_failed("durable");
                        tracing::warn!(error = %e, "Dropped durable audit record");
                    }
                }
            }
        });
    }

    pub async fn read<C: Statements>(
        &self,
        client: &C,
        read: Read,
        scope: Option<&EffectiveScope>,
    ) -> PlatformResult<()> {
        if !self.sink.durable() {
            return Ok(());
        }
        let Target {
            tenant_slug,
            service,
            table,
            ..
        } = self.target;
        let kind = (
            tenant_slug,
            service,
            table,
            self.target.actor(),
            read.data().to_string(),
            scope.map(EffectiveScope::audit_kind),
        );
        let read = match self.sink.state.audit_reads.admit(kind, read) {
            Ok(read) => read,
            Err(unrecorded) => {
                metrics::counter!(
                    "cufflink_audit_reads_unrecorded_total",
                    "tenant" => tenant_slug.to_string(),
                    "reason" => unrecorded.as_str(),
                )
                .increment(1);
                return Ok(());
            }
        };
        let record = self.target.read(&read, scope, Utc::now());
        if self.sink.enforced {
            let headers = header_pairs(&crate::telemetry::traced_nats_headers());
            let row = self.sink.row(OutboxKind::Audit, record, &headers);
            self.sink.write(client, std::slice::from_ref(&row)).await?;
        } else {
            audit::publish_durable(self.sink.state, record.subject, record.payload);
        }
        self.sink.count(READ);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn emission_of(target: &Target<'_>, before: Option<&Value>) -> Emission {
        let row = json!({"id": "7", "body": "kept", "dob": "1990-01-01"});
        let kind = match before {
            Some(_) => ChangeKind::Updated,
            None => ChangeKind::Deleted,
        };
        target.emission(
            &Change {
                kind,
                row: &row,
                record_id: "7",
                scope: None,
                before,
            },
            Utc::now(),
        )
    }

    fn emission() -> Emission {
        let nobody = None;
        emission_of(
            &Target::of("harbor", Uuid::nil(), "loans", "loans", &nobody),
            None,
        )
    }

    fn json_of(message: &Message) -> Value {
        serde_json::from_slice(&message.payload).expect("json")
    }

    #[test]
    fn a_written_row_is_an_event_row_and_then_its_audit_rows() {
        let headers = vec![("traceparent".to_string(), "00-1-2-01".to_string())];
        let emission = emission();
        let [event, audit, durable] = emission.rows("loan-service", &headers);
        assert_eq!(
            (event.kind, event.subject.as_str(), &event.payload),
            (
                OutboxKind::Event,
                "dw.harbor.loans.loans.deleted",
                &emission.event.payload
            )
        );
        assert_eq!(
            (audit.kind, audit.subject.as_str(), &audit.payload),
            (OutboxKind::Audit, "dw.harbor.logs", &emission.audit.payload)
        );
        assert_eq!(
            (durable.kind, durable.subject.as_str(), &durable.payload),
            (
                OutboxKind::Audit,
                "cufflink.audit.harbor.loans",
                &emission.durable.payload
            )
        );
        for row in [&event, &audit, &durable] {
            assert_eq!(row.service, "loan-service");
            assert_eq!(row.headers, headers);
        }
        let ids = [event.msg_id, audit.msg_id, durable.msg_id];
        assert!(ids[0] != ids[1] && ids[1] != ids[2] && ids[0] != ids[2]);
    }

    #[test]
    fn a_deleted_row_is_the_events_data_and_the_records_previous() {
        let emission = emission();
        let row = json!({"id": "7", "body": "kept", "dob": "1990-01-01"});
        let (event, audit, durable) = (
            json_of(&emission.event),
            json_of(&emission.audit),
            json_of(&emission.durable),
        );
        assert_eq!(event["data"], row);
        for record in [&audit, &durable] {
            assert_eq!(record["previous"], row);
            assert_eq!(record["data"], Value::Null);
            assert_eq!(record["action"], "delete");
        }
        assert_eq!(event["action"], "deleted");
        assert_eq!(
            durable["actor"],
            json!({"sub": "anonymous", "kind": "anonymous", "key_id": null, "azp": null})
        );
        assert_eq!(durable["redacted"], json!([]));
    }

    #[test]
    fn what_a_table_redacts_is_in_no_record_and_in_the_event() {
        let (nobody, redact) = (None, vec!["dob".to_string()]);
        let target = Target {
            calling: Some("books"),
            request_id: Some("req-7"),
            redact: &redact,
            ..Target::of("harbor", Uuid::nil(), "loans", "loans", &nobody)
        };
        let before = json!({"id": "7", "body": "was", "dob": "1989-12-31"});
        let emission = emission_of(&target, Some(&before));
        assert_eq!(json_of(&emission.event)["data"]["dob"], "1990-01-01");
        let (audit, durable) = (json_of(&emission.audit), json_of(&emission.durable));
        for record in [&audit, &durable] {
            assert_eq!(record["data"]["dob"], audit::REDACTED, "{record}");
            assert_eq!(record["data"]["body"], "kept");
        }
        assert_eq!(audit["previous"], Value::Null, "as an update's always was");
        assert!(audit.get("redacted").is_none() && audit.get("actor").is_none());
        assert_eq!(
            durable["previous"],
            json!({"id": "7", "body": "was", "dob": audit::REDACTED})
        );
        assert_eq!(durable["redacted"], json!(["dob"]));
        assert_eq!(durable["request_id"], "req-7");
        assert_eq!(
            durable["actor"],
            json!({"sub": "books", "kind": "service", "key_id": null, "azp": null})
        );
    }

    #[test]
    fn a_read_is_recorded_durably_and_names_no_row_of_its_own() {
        let nobody = None;
        let target = Target {
            request_id: Some("req-8"),
            ..Target::of("harbor", Uuid::nil(), "loans", "clients", &nobody)
        };
        let read = Read {
            record_ids: vec!["7".into(), "8".into()],
            returned: 2,
            filter_columns: vec!["status".into()],
            search: false,
            sort: Some("name".into()),
            ..Read::default()
        };
        let message = target.read(&read, Some(&EffectiveScope::All), Utc::now());
        assert_eq!(message.subject, "cufflink.audit.harbor.loans");
        let record = json_of(&message);
        assert_eq!(
            (&record["action"], &record["table"], &record["record_id"]),
            (&json!("read"), &json!("clients"), &Value::Null)
        );
        assert_eq!(record["data"], read.data());
        assert_eq!(record["scope"], "all");
        assert_eq!(record["request_id"], "req-8");
        assert_eq!(record["v"], 2);
    }

    #[test]
    fn a_row_the_server_cannot_carry_refuses_the_write_before_it_is_written() {
        let rows = emission().rows("loans", &[]);
        let largest = rows
            .iter()
            .map(OutboxRow::wire_size)
            .max()
            .unwrap_or_default();
        assert!(carried(&rows, largest).is_ok());
        let refused = carried(&rows, largest - 1).expect_err("one row is larger");
        assert!(
            matches!(&refused, PlatformError::PayloadTooLarge(m)
                if m.contains(&format!("{largest} bytes on the wire"))
                    && m.contains(&format!("more than the {} bytes", largest - 1))),
            "{refused}"
        );
    }

    #[test]
    fn the_headers_of_a_request_are_kept_as_pairs() {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", "00-1-2-01");
        headers.append("tracestate", "a=1");
        headers.append("tracestate", "b=2");
        let mut pairs = header_pairs(&headers);
        pairs.sort();
        assert_eq!(
            pairs,
            [
                ("traceparent".to_string(), "00-1-2-01".to_string()),
                ("tracestate".to_string(), "a=1".to_string()),
                ("tracestate".to_string(), "b=2".to_string()),
            ]
        );
        assert!(header_pairs(&HeaderMap::new()).is_empty());
    }
}
