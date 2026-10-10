use crate::infra::{Tenant, TestDb};
use crate::platform::SIGNING_KEY;
use async_nats::jetstream;
use cufflink_platform::outbox::writer::headers_json;
use cufflink_platform::outbox::{OutboxRow, OutboxSeal};
use cufflink_platform::policy::PolicySet;
use cufflink_platform::registry::ServiceIdentity;
use cufflink_platform::secrets::KeyRing;
use cufflink_platform::tenant_catalog::TenantCatalog;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_platform::tenant_sessions::{Purpose, Statements, TenantSessions};
use futures_util::StreamExt;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

pub fn seal() -> OutboxSeal {
    OutboxSeal::of(&KeyRing::default(), Some(SIGNING_KEY.as_bytes())).expect("a seal")
}

pub fn sessions(db: &TestDb, nats: &async_nats::Client) -> TenantSessions {
    TenantSessions::new(
        TenantPoolManager::new(&db.url, 8, 2).expect("tenant pools"),
        TenantCatalog::new(db.pool(), nats.clone(), PolicySet::builtin_defaults()),
    )
}

pub fn identity(tenant: &Tenant, service: &str) -> ServiceIdentity {
    ServiceIdentity {
        tenant_id: tenant.id,
        tenant_slug: tenant.slug.clone(),
        service_id: Uuid::new_v4(),
        service_name: service.into(),
        schema: service.into(),
    }
}

pub async fn forge(sessions: &TenantSessions, tenant: &Tenant, service: &str, rows: &[OutboxRow]) {
    let mut session = sessions
        .acquire(&identity(tenant, service), Purpose::Guest, None)
        .await
        .expect("a guest's session");
    let tx = session.transaction().await.expect("begin");
    let texts = |text: fn(&OutboxRow) -> &str| rows.iter().map(text).collect::<Vec<&str>>();
    let payloads: Vec<&[u8]> = rows.iter().map(|row| row.payload.as_slice()).collect();
    let headers: Vec<Value> = rows.iter().map(|row| headers_json(&row.headers)).collect();
    let msg_ids: Vec<Uuid> = rows.iter().map(|row| row.msg_id).collect();
    Statements::execute(
        &tx,
        "SELECT _cufflink.outbox_enqueue_many(\
             $1::text[], $2::text[], $3::text[], $4::bytea[], $5::jsonb[], $6::uuid[])",
        &[
            &texts(|row| row.kind.as_str()),
            &texts(|row| &row.service),
            &texts(|row| &row.subject),
            &payloads,
            &headers,
            &msg_ids,
        ],
    )
    .await
    .expect("a guest writes to the outbox");
    tx.commit().await.expect("commit");
}

pub struct Filed {
    pub subject: String,
    pub original: String,
    pub stream: String,
    pub reason: String,
    pub payload: Value,
}

async fn stored(
    js: &jetstream::Context,
    stream: &str,
    filter: String,
    count: usize,
    only: bool,
) -> Vec<jetstream::Message> {
    let consumer = js
        .get_stream(stream)
        .await
        .unwrap_or_else(|e| panic!("the stream {stream}: {e}"))
        .create_consumer(jetstream::consumer::pull::Config {
            filter_subject: filter,
            ..Default::default()
        })
        .await
        .expect("consumer");
    let mut messages = consumer.messages().await.expect("messages");
    let mut found = Vec::with_capacity(count);
    while found.len() < count {
        let message = tokio::time::timeout(Duration::from_secs(10), messages.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} of {count} messages of {stream} within 10 s",
                    found.len()
                )
            })
            .expect("the stream of messages")
            .expect("a message");
        message.ack().await.expect("ack");
        found.push(message);
    }
    if only {
        let more = tokio::time::timeout(Duration::from_millis(500), messages.next()).await;
        assert!(more.is_err(), "no message of {stream} but those");
    }
    found
}

pub async fn dead_letters(js: &jetstream::Context, tenant: &Tenant, count: usize) -> Vec<Filed> {
    stored(
        js,
        "CUFFLINK_DLQ",
        format!("cufflink.dlq.{}.>", tenant.slug),
        count,
        false,
    )
    .await
    .into_iter()
    .map(|letter| {
        let header = |name: &str| {
            letter
                .headers
                .as_ref()
                .and_then(|headers| headers.get(name))
                .map(|value| value.to_string())
                .unwrap_or_default()
        };
        Filed {
            subject: letter.subject.to_string(),
            original: header("Cufflink-Dlq-Subject"),
            stream: header("Cufflink-Dlq-Stream"),
            reason: header("Cufflink-Dlq-Reason"),
            payload: serde_json::from_slice(&letter.payload).expect("json"),
        }
    })
    .collect()
}

pub async fn audit_records(js: &jetstream::Context, tenant: &Tenant, count: usize) -> Vec<Value> {
    stored(
        js,
        "CUFFLINK_AUDIT",
        format!("cufflink.audit.{}.>", tenant.slug),
        count,
        true,
    )
    .await
    .iter()
    .map(|record| serde_json::from_slice(&record.payload).expect("json"))
    .collect()
}
