use crate::errors::PlatformResult;
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartnerKey {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub prefix: String,
    pub scopes: Vec<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
}

pub use crate::auth::secret_keys::secret_hash;

const COLUMNS: &str = "id, tenant_id, name, key_prefix, scopes, created_by, created_at, \
                       expires_at, revoked_at, last_used_at";

fn row_to_key(row: &tokio_postgres::Row) -> PartnerKey {
    PartnerKey {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        prefix: row.get("key_prefix"),
        scopes: row.get("scopes"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        expires_at: row.get("expires_at"),
        revoked_at: row.get("revoked_at"),
        last_used_at: row.get("last_used_at"),
    }
}

pub struct NewPartnerKey<'a> {
    pub tenant_id: Uuid,
    pub name: &'a str,
    pub prefix: &'a str,
    pub secret: &'a str,
    pub scopes: &'a [String],
    pub ttl: chrono::Duration,
    pub created_by: &'a str,
}

pub async fn create(
    pool: &deadpool_postgres::Pool,
    new: &NewPartnerKey<'_>,
) -> PlatformResult<Option<PartnerKey>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!(
                "INSERT INTO partner_keys
                   (tenant_id, name, key_prefix, secret_hash, scopes, created_by, expires_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (key_prefix) DO NOTHING
                 RETURNING {COLUMNS}"
            ),
            &[
                &new.tenant_id,
                &new.name,
                &new.prefix,
                &secret_hash(new.secret),
                &new.scopes,
                &new.created_by,
                &(Utc::now() + new.ttl),
            ],
        )
        .await?;
    Ok(row.as_ref().map(row_to_key))
}

pub async fn find_by_prefix(
    pool: &deadpool_postgres::Pool,
    prefix: &str,
) -> PlatformResult<Option<(PartnerKey, String)>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!("SELECT {COLUMNS}, secret_hash FROM partner_keys WHERE key_prefix = $1"),
            &[&prefix],
        )
        .await?;
    Ok(row.map(|row| (row_to_key(&row), row.get("secret_hash"))))
}

pub async fn list(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
) -> PlatformResult<Vec<PartnerKey>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "SELECT {COLUMNS} FROM partner_keys WHERE tenant_id = $1 ORDER BY created_at DESC"
            ),
            &[&tenant_id],
        )
        .await?;
    Ok(rows.iter().map(row_to_key).collect())
}

pub async fn revoke(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    id: Uuid,
) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let count = client
        .execute(
            "UPDATE partner_keys SET revoked_at = now()
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
            "UPDATE partner_keys SET last_used_at = now() WHERE id = $1",
            &[&id],
        )
        .await?;
    Ok(())
}

pub async fn count_active(pool: &deadpool_postgres::Pool) -> PlatformResult<i64> {
    let client = pool.get().await?;
    Ok(client
        .query_one(
            "SELECT count(*) FROM partner_keys WHERE revoked_at IS NULL AND expires_at > now()",
            &[],
        )
        .await?
        .get(0))
}
