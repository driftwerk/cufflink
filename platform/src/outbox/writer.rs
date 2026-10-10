use super::seal::OutboxSeal;
use crate::tenant_sessions::{RoleMode, Statements};
use serde_json::Value;
use std::str::FromStr;
use uuid::Uuid;

const ENQUEUE_MANY: &str = "SELECT _cufflink.outbox_enqueue_many(\
     $1::text[], $2::text[], $3::text[], $4::bytea[], $5::jsonb[], $6::uuid[])";
const ENQUEUE_OWN: &str = "SELECT _cufflink.outbox_enqueue_own(\
     $1::text[], $2::text[], $3::bytea[], $4::jsonb[], $5::uuid[])";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutboxKind {
    Event,
    Audit,
    Webhook,
}

impl OutboxKind {
    pub fn as_str(self) -> &'static str {
        match self {
            OutboxKind::Event => "event",
            OutboxKind::Audit => "audit",
            OutboxKind::Webhook => "webhook",
        }
    }
}

impl FromStr for OutboxKind {
    type Err = String;

    fn from_str(kind: &str) -> Result<Self, Self::Err> {
        [OutboxKind::Event, OutboxKind::Audit, OutboxKind::Webhook]
            .into_iter()
            .find(|known| known.as_str() == kind)
            .ok_or_else(|| format!("'{kind}' is not an outbox kind"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRow {
    pub kind: OutboxKind,
    pub service: String,
    pub subject: String,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub msg_id: Uuid,
}

impl OutboxRow {
    pub fn new(
        kind: OutboxKind,
        service: &str,
        subject: String,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Self {
        Self {
            kind,
            service: service.to_string(),
            subject,
            payload,
            headers,
            msg_id: Uuid::new_v4(),
        }
    }

    pub fn wire_size(&self) -> usize {
        super::publish::wire_size(
            self.payload.len(),
            &super::publish::publish_headers(&self.headers, self.msg_id),
        )
    }
}

pub fn headers_json(headers: &[(String, String)]) -> Value {
    headers
        .iter()
        .map(|(name, value)| Value::from(vec![name.as_str(), value.as_str()]))
        .collect()
}

pub fn headers_of(column: &Value) -> Vec<(String, String)> {
    column
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|pair| match pair.as_array()?.as_slice() {
            [name, value] => Some((name.as_str()?.to_string(), value.as_str()?.to_string())),
            _ => None,
        })
        .collect()
}

pub struct OutboxWriter<'a, C> {
    tx: &'a C,
    tenant_id: Uuid,
    tenant_slug: &'a str,
    seal: &'a OutboxSeal,
}

impl<'a, C: Statements> OutboxWriter<'a, C> {
    pub fn new(tx: &'a C, tenant_id: Uuid, tenant_slug: &'a str, seal: &'a OutboxSeal) -> Self {
        Self {
            tx,
            tenant_id,
            tenant_slug,
            seal,
        }
    }

    pub async fn write(&self, rows: &[OutboxRow]) -> Result<(), tokio_postgres::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let column = |text: fn(&OutboxRow) -> &str| rows.iter().map(text).collect::<Vec<_>>();
        let kinds = column(|row| row.kind.as_str());
        let subjects = column(|row| &row.subject);
        let payloads: Vec<&[u8]> = rows.iter().map(|row| row.payload.as_slice()).collect();
        let headers: Vec<Value> = rows
            .iter()
            .map(|row| headers_json(&self.seal.headers(self.tenant_id, row)))
            .collect();
        let msg_ids: Vec<Uuid> = rows.iter().map(|row| row.msg_id).collect();
        match self.tx.role_mode() {
            RoleMode::Platform => {
                let services = column(|row| &row.service);
                self.tx
                    .execute(
                        ENQUEUE_MANY,
                        &[&kinds, &services, &subjects, &payloads, &headers, &msg_ids],
                    )
                    .await
            }
            RoleMode::SetLocal { .. } | RoleMode::Login { .. } => {
                self.tx
                    .execute(
                        ENQUEUE_OWN,
                        &[&kinds, &subjects, &payloads, &headers, &msg_ids],
                    )
                    .await
            }
        }?;
        rows.iter().for_each(|row| {
            metrics::counter!(
                "cufflink_outbox_rows_written_total",
                "tenant" => self.tenant_slug.to_string(),
                "kind" => row.kind.as_str(),
            )
            .increment(1)
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_kind_is_the_text_the_table_checks() {
        for kind in [OutboxKind::Event, OutboxKind::Audit, OutboxKind::Webhook] {
            assert_eq!(kind.as_str().parse::<OutboxKind>(), Ok(kind));
        }
        assert_eq!(
            "job".parse::<OutboxKind>(),
            Err("'job' is not an outbox kind".to_string())
        );
    }

    #[test]
    fn headers_keep_their_order_and_their_repeats() {
        let headers = vec![
            ("traceparent".to_string(), "00-1-2-01".to_string()),
            ("X-Kind".to_string(), "a".to_string()),
            ("X-Kind".to_string(), "b".to_string()),
        ];
        let column = headers_json(&headers);
        assert_eq!(
            column,
            json!([
                ["traceparent", "00-1-2-01"],
                ["X-Kind", "a"],
                ["X-Kind", "b"]
            ])
        );
        assert_eq!(headers_of(&column), headers);
        assert_eq!(headers_json(&[]), json!([]));
    }

    #[test]
    fn a_column_the_platform_did_not_write_yields_what_it_can() {
        assert!(headers_of(&json!({"traceparent": "x"})).is_empty());
        assert_eq!(
            headers_of(&json!([["a", "1"], ["b"], ["c", 3], "d", ["e", "5", "6"]])),
            [("a".to_string(), "1".to_string())]
        );
    }

    #[test]
    fn rows_are_published_under_ids_of_their_own() {
        let row = |subject: &str| {
            OutboxRow::new(
                OutboxKind::Event,
                "notes",
                subject.to_string(),
                b"{}".to_vec(),
                Vec::new(),
            )
        };
        assert_ne!(row("a").msg_id, row("a").msg_id);
    }
}
