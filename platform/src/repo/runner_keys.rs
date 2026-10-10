use crate::auth::secret_keys::secret_hash;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::runner_services::Runs;
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunnerKey {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub runs: Runs,
    pub prefix: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRunnerKey {
    pub key: RunnerKey,
    pub secret_hash: String,
    pub tenant_slug: String,
}

const COLUMNS: &str = "k.id, k.tenant_id, k.name, k.runs, k.key_prefix, k.created_by, \
                       k.created_at, k.revoked_at, k.last_used_at";

fn row_to_key(row: &tokio_postgres::Row) -> PlatformResult<RunnerKey> {
    let id: Uuid = row.try_get("id")?;
    let runs: &str = row.try_get("runs")?;
    Ok(RunnerKey {
        id,
        tenant_id: row.try_get("tenant_id")?,
        name: row.try_get("name")?,
        runs: Runs::parse(runs).ok_or_else(|| {
            PlatformError::Internal(format!("runner key {id} runs the unknown kind '{runs}'"))
        })?,
        prefix: row.try_get("key_prefix")?,
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
        last_used_at: row.try_get("last_used_at")?,
    })
}

pub struct NewRunnerKey<'a> {
    pub tenant_id: Uuid,
    pub name: &'a str,
    pub runs: Runs,
    pub prefix: &'a str,
    pub secret: &'a str,
    pub created_by: &'a str,
}

pub async fn create(
    pool: &deadpool_postgres::Pool,
    new: &NewRunnerKey<'_>,
) -> PlatformResult<Option<RunnerKey>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!(
                "INSERT INTO runner_keys AS k
                   (tenant_id, name, runs, key_prefix, secret_hash, created_by)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (key_prefix) DO NOTHING
                 RETURNING {COLUMNS}"
            ),
            &[
                &new.tenant_id,
                &new.name,
                &new.runs.as_str(),
                &new.prefix,
                &secret_hash(new.secret),
                &new.created_by,
            ],
        )
        .await?;
    row.as_ref().map(row_to_key).transpose()
}

pub async fn find_by_prefix(
    pool: &deadpool_postgres::Pool,
    prefix: &str,
) -> PlatformResult<Option<StoredRunnerKey>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!(
                "SELECT {COLUMNS}, k.secret_hash, t.slug
                 FROM runner_keys k JOIN tenants t ON t.id = k.tenant_id
                 WHERE k.key_prefix = $1"
            ),
            &[&prefix],
        )
        .await?;
    row.map(|row| {
        Ok(StoredRunnerKey {
            key: row_to_key(&row)?,
            secret_hash: row.try_get("secret_hash")?,
            tenant_slug: row.try_get("slug")?,
        })
    })
    .transpose()
}

pub async fn list(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
) -> PlatformResult<Vec<RunnerKey>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "SELECT {COLUMNS} FROM runner_keys k WHERE k.tenant_id = $1
                 ORDER BY k.created_at DESC"
            ),
            &[&tenant_id],
        )
        .await?;
    rows.iter().map(row_to_key).collect()
}

pub async fn revoke(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    id: Uuid,
) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let count = client
        .execute(
            "UPDATE runner_keys SET revoked_at = now()
             WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL",
            &[&id, &tenant_id],
        )
        .await?;
    Ok(count > 0)
}

pub async fn touch(pool: &deadpool_postgres::Pool, id: Uuid) -> PlatformResult<()> {
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE runner_keys SET last_used_at = now() WHERE id = $1",
            &[&id],
        )
        .await?;
    Ok(())
}
