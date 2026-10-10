use crate::secrets::{configs, SecretBox, SecretError, SecretString, Stored, SERVICE_CONFIG_VALUE};
use chrono::{DateTime, Utc};
use deadpool_postgres::{GenericClient, Transaction};
use uuid::Uuid;

pub struct ConfigEntry {
    pub key: String,
    pub value: Option<SecretString>,
    pub is_secret: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Opens {
    AllButSecrets,
    All,
}

async fn value(
    secrets: &SecretBox,
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
    row: &tokio_postgres::Row,
    opens: Opens,
) -> Result<Option<SecretString>, SecretError> {
    let key: &str = row.get("key");
    if opens == Opens::AllButSecrets && row.get("is_secret") {
        return Ok(None);
    }
    let stored = Stored::new(row.get("value"), row.get("value_enc"));
    match secrets
        .read(client, tenant_id, &aad(service_id, key), &stored)
        .await
    {
        Err(unreadable) if opens == Opens::All && !unreadable.is_transient() => {
            tracing::warn!(
                event = "config_value_unreadable",
                service_id = %service_id,
                key,
                error = %unreadable,
                "A stored config value does not open; an apply that names its key writes it again"
            );
            Ok(None)
        }
        read => read,
    }
}

async fn entries(
    secrets: &SecretBox,
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
    opens: Opens,
) -> Result<Vec<ConfigEntry>, SecretError> {
    let rows = client
        .query(
            "SELECT key, value, value_enc, is_secret, updated_at
             FROM service_configs WHERE service_id = $1 ORDER BY key",
            &[&service_id],
        )
        .await?;
    let mut entries = Vec::with_capacity(rows.len());
    for row in &rows {
        entries.push(ConfigEntry {
            key: row.get("key"),
            value: value(secrets, client, tenant_id, service_id, row, opens).await?,
            is_secret: row.get("is_secret"),
            updated_at: row.get("updated_at"),
        });
    }
    Ok(entries)
}

fn aad(service_id: Uuid, key: &str) -> crate::secrets::SecretAad {
    SERVICE_CONFIG_VALUE.aad([service_id.to_string(), key.to_string()])
}

pub async fn list(
    secrets: &SecretBox,
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
) -> Result<Vec<ConfigEntry>, SecretError> {
    entries(secrets, client, tenant_id, service_id, Opens::AllButSecrets).await
}

pub async fn opened(
    secrets: &SecretBox,
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
) -> Result<Vec<ConfigEntry>, SecretError> {
    entries(secrets, client, tenant_id, service_id, Opens::All).await
}

pub async fn lock_apply(
    tx: &Transaction<'_>,
    service_id: Uuid,
) -> Result<(), tokio_postgres::Error> {
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended('cufflink.config_apply:' || $1, 0))",
        &[&service_id.to_string()],
    )
    .await
    .map(drop)
}

pub async fn get(
    secrets: &SecretBox,
    client: &impl GenericClient,
    service_id: Uuid,
    key: &str,
) -> Result<Option<SecretString>, SecretError> {
    configs::get(secrets, client, service_id, key).await
}

pub async fn set(
    secrets: &SecretBox,
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
    key: &str,
    value: &str,
    is_secret: bool,
) -> Result<(), SecretError> {
    let stored = secrets
        .store(client, tenant_id, &aad(service_id, key), value)
        .await?;
    upsert(client, service_id, key, &stored, is_secret).await
}

pub async fn set_within(
    secrets: &SecretBox,
    tx: &Transaction<'_>,
    tenant_id: Uuid,
    service_id: Uuid,
    key: &str,
    value: &str,
    is_secret: bool,
) -> Result<(), SecretError> {
    let stored = secrets
        .store_within(tx, tenant_id, &aad(service_id, key), value)
        .await?;
    upsert(tx, service_id, key, &stored, is_secret).await
}

async fn upsert(
    client: &impl GenericClient,
    service_id: Uuid,
    key: &str,
    stored: &Stored,
    is_secret: bool,
) -> Result<(), SecretError> {
    client
        .execute(
            "INSERT INTO service_configs (service_id, key, value, value_enc, is_secret)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (service_id, key)
             DO UPDATE SET value = $3, value_enc = $4, is_secret = $5, updated_at = now()",
            &[
                &service_id,
                &key,
                &stored.plain(),
                &stored.sealed(),
                &is_secret,
            ],
        )
        .await?;
    Ok(())
}

pub async fn delete(
    client: &impl GenericClient,
    service_id: Uuid,
    key: &str,
) -> Result<bool, tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM service_configs WHERE service_id = $1 AND key = $2",
            &[&service_id, &key],
        )
        .await
        .map(|deleted| deleted > 0)
}
