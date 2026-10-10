use crate::columns::SERVICE_CONFIG_VALUE;
use crate::{SecretBox, SecretError, SecretString, Stored};
use deadpool_postgres::GenericClient;
use std::collections::HashMap;
use uuid::Uuid;

const SELECT: &str = "SELECT c.key, c.value, c.value_enc, s.tenant_id
                      FROM service_configs c JOIN services s ON s.id = c.service_id
                      WHERE c.service_id = $1";

async fn opened(
    secrets: &SecretBox,
    client: &impl GenericClient,
    service_id: Uuid,
    row: &tokio_postgres::Row,
) -> Result<Option<(String, SecretString)>, SecretError> {
    let key: String = row.get("key");
    let stored = Stored::new(row.get("value"), row.get("value_enc"));
    let aad = SERVICE_CONFIG_VALUE.aad([service_id.to_string(), key.clone()]);
    Ok(secrets
        .read(client, row.get("tenant_id"), &aad, &stored)
        .await?
        .map(|value| (key, value)))
}

pub async fn all(
    secrets: &SecretBox,
    client: &impl GenericClient,
    service_id: Uuid,
) -> Result<Vec<(String, SecretString)>, SecretError> {
    let rows = client
        .query(&format!("{SELECT} ORDER BY c.key"), &[&service_id])
        .await?;
    let mut values = Vec::with_capacity(rows.len());
    for row in &rows {
        values.extend(opened(secrets, client, service_id, row).await?);
    }
    Ok(values)
}

pub async fn environment(
    secrets: &SecretBox,
    client: &impl GenericClient,
    service_id: Uuid,
) -> Result<HashMap<String, String>, SecretError> {
    Ok(all(secrets, client, service_id)
        .await?
        .into_iter()
        .map(|(key, value)| (key, value.expose().to_string()))
        .collect())
}

pub async fn get(
    secrets: &SecretBox,
    client: &impl GenericClient,
    service_id: Uuid,
    key: &str,
) -> Result<Option<SecretString>, SecretError> {
    match client
        .query_opt(&format!("{SELECT} AND c.key = $2"), &[&service_id, &key])
        .await?
    {
        Some(row) => Ok(opened(secrets, client, service_id, &row)
            .await?
            .map(|(_, value)| value)),
        None => Ok(None),
    }
}
