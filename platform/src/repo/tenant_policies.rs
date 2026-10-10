use crate::errors::{PlatformError, PlatformResult};
use crate::policy::{PolicyKey, PolicyMode};
use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PolicyRow {
    pub tenant_id: Uuid,
    pub key: String,
    pub mode: PolicyMode,
    pub config: Value,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}

const COLUMNS: &str = "tenant_id, key, mode, config, updated_at, updated_by";

fn row_to_policy(row: &tokio_postgres::Row) -> PlatformResult<PolicyRow> {
    let mode: String = row.get("mode");
    Ok(PolicyRow {
        tenant_id: row.get("tenant_id"),
        key: row.get("key"),
        mode: mode.parse().map_err(PlatformError::Internal)?,
        config: row.get("config"),
        updated_at: row.get("updated_at"),
        updated_by: row.get("updated_by"),
    })
}

fn rows_to_policies(rows: &[tokio_postgres::Row]) -> PlatformResult<Vec<PolicyRow>> {
    rows.iter().map(row_to_policy).collect()
}

pub async fn list_all(pool: &deadpool_postgres::Pool) -> PlatformResult<Vec<PolicyRow>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!("SELECT {COLUMNS} FROM tenant_policies ORDER BY tenant_id, key"),
            &[],
        )
        .await?;
    rows_to_policies(&rows)
}

pub async fn list_for_tenant(
    client: &impl GenericClient,
    tenant_id: Uuid,
) -> PlatformResult<Vec<PolicyRow>> {
    let rows = client
        .query(
            &format!("SELECT {COLUMNS} FROM tenant_policies WHERE tenant_id = $1 ORDER BY key"),
            &[&tenant_id],
        )
        .await?;
    rows_to_policies(&rows)
}

pub async fn upsert(
    client: &impl GenericClient,
    tenant_id: Uuid,
    key: PolicyKey,
    mode: PolicyMode,
    config: &Value,
    updated_by: &str,
) -> PlatformResult<PolicyRow> {
    let row = client
        .query_one(
            &format!(
                "INSERT INTO tenant_policies (tenant_id, key, mode, config, updated_by)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (tenant_id, key) DO UPDATE
                 SET mode = EXCLUDED.mode, config = EXCLUDED.config,
                     updated_by = EXCLUDED.updated_by, updated_at = now()
                 RETURNING {COLUMNS}"
            ),
            &[
                &tenant_id,
                &key.as_str(),
                &mode.as_str(),
                config,
                &updated_by,
            ],
        )
        .await?;
    row_to_policy(&row)
}

pub async fn delete(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    key: PolicyKey,
) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let count = client
        .execute(
            "DELETE FROM tenant_policies WHERE tenant_id = $1 AND key = $2",
            &[&tenant_id, &key.as_str()],
        )
        .await?;
    Ok(count > 0)
}

pub async fn clone_from(
    client: &impl GenericClient,
    source: Uuid,
    target: Uuid,
) -> PlatformResult<u64> {
    Ok(client
        .execute(
            "INSERT INTO tenant_policies (tenant_id, key, mode, config, updated_by)
             SELECT $2, key, mode, config, updated_by FROM tenant_policies WHERE tenant_id = $1
             ON CONFLICT (tenant_id, key) DO NOTHING",
            &[&source, &target],
        )
        .await?)
}
