use crate::secrets::{SecretBox, SecretError, SecretString, Stored, WEBHOOK_SECRET};
use crate::webhooks::index::Registered;
use deadpool_postgres::GenericClient;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct WebhookRecord {
    pub id: Uuid,
    #[serde(skip)]
    pub tenant_id: Uuid,
    pub service_id: Uuid,
    #[serde(skip)]
    pub service_name: String,
    pub table_name: String,
    pub event: String,
    pub url: String,
    #[serde(skip)]
    pub secret: SecretString,
    pub active: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

const SELECT: &str =
    "SELECT w.id, w.service_id, w.table_name, w.event, w.url, w.secret, w.secret_enc,
                             w.active, w.created_at, s.tenant_id, s.name AS service_name
                      FROM webhooks w JOIN services s ON s.id = w.service_id";

async fn record(
    secrets: &SecretBox,
    client: &impl GenericClient,
    row: &tokio_postgres::Row,
) -> Result<WebhookRecord, SecretError> {
    let id: Uuid = row.get("id");
    let stored = Stored::new(row.get("secret"), row.get("secret_enc"));
    let aad = WEBHOOK_SECRET.aad([id.to_string()]);
    let secret = secrets
        .read(client, row.get("tenant_id"), &aad, &stored)
        .await?
        .ok_or(SecretError::Absent {
            table: aad.table,
            column: aad.column,
        })?;
    Ok(WebhookRecord {
        id,
        tenant_id: row.get("tenant_id"),
        service_id: row.get("service_id"),
        service_name: row.get("service_name"),
        table_name: row.get("table_name"),
        event: row.get("event"),
        url: row.get("url"),
        secret,
        active: row.get("active"),
        created_at: row.get("created_at"),
    })
}

async fn records(
    secrets: &SecretBox,
    client: &impl GenericClient,
    rows: &[tokio_postgres::Row],
) -> Result<Vec<WebhookRecord>, SecretError> {
    let mut found = Vec::with_capacity(rows.len());
    for row in rows {
        found.push(record(secrets, client, row).await?);
    }
    Ok(found)
}

pub async fn list_by_service(
    secrets: &SecretBox,
    client: &impl GenericClient,
    service_id: Uuid,
) -> Result<Vec<WebhookRecord>, SecretError> {
    let rows = client
        .query(
            &format!("{SELECT} WHERE w.service_id = $1 ORDER BY w.created_at DESC"),
            &[&service_id],
        )
        .await?;
    records(secrets, client, &rows).await
}

pub async fn find(
    secrets: &SecretBox,
    client: &impl GenericClient,
    id: Uuid,
) -> Result<Option<WebhookRecord>, SecretError> {
    let row = client
        .query_opt(&format!("{SELECT} WHERE w.id = $1"), &[&id])
        .await?;
    match row {
        Some(row) => record(secrets, client, &row).await.map(Some),
        None => Ok(None),
    }
}

pub async fn exists(
    client: &impl GenericClient,
    id: Uuid,
    service_id: Uuid,
) -> Result<bool, tokio_postgres::Error> {
    client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM webhooks WHERE id = $1 AND service_id = $2)",
            &[&id, &service_id],
        )
        .await
        .map(|row| row.get(0))
}

pub async fn list_active_targets(
    client: &impl GenericClient,
) -> Result<Vec<Registered>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT id, service_id, table_name, event FROM webhooks WHERE active
             ORDER BY created_at, id",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| Registered {
            id: row.get("id"),
            service_id: row.get("service_id"),
            table: row.get("table_name"),
            event: row.get("event"),
        })
        .collect())
}

pub async fn list_active_for_event(
    secrets: &SecretBox,
    client: &impl GenericClient,
    service_id: Uuid,
    table_name: &str,
    event: &str,
) -> Result<Vec<WebhookRecord>, SecretError> {
    let rows = client
        .query(
            &format!(
                "{SELECT} WHERE w.service_id = $1 AND w.table_name = $2 AND w.event = $3 AND w.active"
            ),
            &[&service_id, &table_name, &event],
        )
        .await?;
    records(secrets, client, &rows).await
}

pub struct NewWebhook<'a> {
    pub tenant_id: Uuid,
    pub service_id: Uuid,
    pub service_name: &'a str,
    pub table_name: &'a str,
    pub event: &'a str,
    pub url: &'a str,
    pub secret: &'a str,
}

pub async fn create(
    secrets: &SecretBox,
    client: &impl GenericClient,
    new: &NewWebhook<'_>,
) -> Result<WebhookRecord, SecretError> {
    let id = Uuid::new_v4();
    let stored = secrets
        .store(
            client,
            new.tenant_id,
            &WEBHOOK_SECRET.aad([id.to_string()]),
            new.secret,
        )
        .await?;
    let row = client
        .query_one(
            "INSERT INTO webhooks (id, service_id, table_name, event, url, secret, secret_enc)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             RETURNING active, created_at",
            &[
                &id,
                &new.service_id,
                &new.table_name,
                &new.event,
                &new.url,
                &stored.plain(),
                &stored.sealed(),
            ],
        )
        .await?;
    Ok(WebhookRecord {
        id,
        tenant_id: new.tenant_id,
        service_id: new.service_id,
        service_name: new.service_name.to_string(),
        table_name: new.table_name.to_string(),
        event: new.event.to_string(),
        url: new.url.to_string(),
        secret: SecretString::new(new.secret),
        active: row.get("active"),
        created_at: row.get("created_at"),
    })
}

pub async fn delete(
    client: &deadpool_postgres::Object,
    webhook_id: Uuid,
    service_id: Uuid,
) -> eyre::Result<bool> {
    let count = client
        .execute(
            "DELETE FROM webhooks WHERE id = $1 AND service_id = $2",
            &[&webhook_id, &service_id],
        )
        .await?;
    Ok(count > 0)
}

pub async fn set_active(
    client: &deadpool_postgres::Object,
    webhook_id: Uuid,
    service_id: Uuid,
    active: bool,
) -> eyre::Result<bool> {
    let count = client
        .execute(
            "UPDATE webhooks SET active = $1 WHERE id = $2 AND service_id = $3",
            &[&active, &webhook_id, &service_id],
        )
        .await?;
    Ok(count > 0)
}
