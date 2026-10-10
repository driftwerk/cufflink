use crate::errors::{PlatformError, PlatformResult};
use crate::policy::record::LogThrottle;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyAuth {
    pub id: Uuid,
    pub tenant_id: Option<Uuid>,
    pub is_platform_admin: bool,
    pub name: String,
    pub user_sub: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiKeyRow {
    pub id: Uuid,
    pub name: String,
    pub user_sub: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyLookup {
    Active(ApiKeyAuth),
    Expired,
    Unknown,
}

impl KeyLookup {
    pub fn active(self) -> Result<ApiKeyAuth, crate::errors::PlatformError> {
        let rejected = |reason: &'static str, message: &str| {
            metrics::counter!("cufflink_api_key_rejected_total", "reason" => reason).increment(1);
            Err(crate::errors::PlatformError::Unauthorized(message.into()))
        };
        match self {
            KeyLookup::Active(key) => Ok(key),
            KeyLookup::Expired => rejected("expired", "API key expired"),
            KeyLookup::Unknown => rejected("unknown", "Invalid API key"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MintedKey {
    pub id: Uuid,
    pub raw: String,
    pub expires_at: Option<DateTime<Utc>>,
}

pub fn hash(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.trim().as_bytes()))
}

fn generate(prefix: &str) -> String {
    format!("{prefix}{}", Uuid::new_v4().simple())
}

const KEY_COLUMNS: &str = "id, name, user_sub, created_at, expires_at, last_used_at";

fn row_to_key(row: &tokio_postgres::Row) -> ApiKeyRow {
    ApiKeyRow {
        id: row.get("id"),
        name: row.get("name"),
        user_sub: row.get("user_sub"),
        created_at: row.get("created_at"),
        expires_at: row.get("expires_at"),
        last_used_at: row.get("last_used_at"),
    }
}

pub async fn find(pool: &deadpool_postgres::Pool, key_hash: &str) -> PlatformResult<KeyLookup> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT id, tenant_id, is_platform_admin, name, user_sub,
                    expires_at IS NOT NULL AND expires_at <= now() AS expired
             FROM api_keys WHERE key_hash = $1",
            &[&key_hash],
        )
        .await?;
    let Some(row) = row else {
        return Ok(KeyLookup::Unknown);
    };
    if row.get::<_, bool>("expired") {
        return Ok(KeyLookup::Expired);
    }
    let key = ApiKeyAuth {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        is_platform_admin: row.get("is_platform_admin"),
        name: row.get("name"),
        user_sub: row.get("user_sub"),
    };
    if LAST_USED.should_log(key.id, Instant::now()) {
        if let Err(e) = client
            .execute(
                "UPDATE api_keys SET last_used_at = now() WHERE id = $1",
                &[&key.id],
            )
            .await
        {
            tracing::warn!(key_id = %key.id, error = %e, "Could not record an API key's last use");
        }
    }
    Ok(KeyLookup::Active(key))
}

const LAST_USED_EVERY: Duration = Duration::from_secs(60);

static LAST_USED: LazyLock<LogThrottle<Uuid>> = LazyLock::new(|| LogThrottle::new(LAST_USED_EVERY));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyClass {
    Tenant,
    PlatformAdmin,
}

pub async fn expire_unbounded(
    pool: &deadpool_postgres::Pool,
    class: KeyClass,
    ttl: chrono::Duration,
) -> PlatformResult<u64> {
    let client = pool.get().await?;
    Ok(client
        .execute(
            "UPDATE api_keys SET expires_at = $1
             WHERE expires_at IS NULL AND is_platform_admin = $2",
            &[&(Utc::now() + ttl), &(class == KeyClass::PlatformAdmin)],
        )
        .await?)
}

pub async fn create_tenant_key(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    name: &str,
    user_sub: Option<&str>,
    ttl: chrono::Duration,
) -> PlatformResult<MintedKey> {
    let raw = generate("ck_");
    let client = pool.get().await?;
    let row = client
        .query_one(
            "INSERT INTO api_keys (tenant_id, key_hash, name, user_sub, expires_at)
             VALUES ($1, $2, $3, $4, now() + $5 * interval '1 second')
             RETURNING id, expires_at",
            &[
                &tenant_id,
                &hash(&raw),
                &name,
                &user_sub,
                &(ttl.num_seconds() as f64),
            ],
        )
        .await?;
    Ok(MintedKey {
        id: row.get("id"),
        raw,
        expires_at: row.get("expires_at"),
    })
}

pub async fn create_platform_key(
    pool: &deadpool_postgres::Pool,
    name: &str,
    ttl: Option<chrono::Duration>,
) -> PlatformResult<MintedKey> {
    let raw = generate("ck_platform_");
    let client = pool.get().await?;
    let row = client
        .query_one(
            "INSERT INTO api_keys (key_hash, name, is_platform_admin, expires_at)
             VALUES ($1, $2, true, $3) RETURNING id, expires_at",
            &[&hash(&raw), &name, &ttl.map(|ttl| Utc::now() + ttl)],
        )
        .await?;
    Ok(MintedKey {
        id: row.get("id"),
        raw,
        expires_at: row.get("expires_at"),
    })
}

#[derive(Debug, Clone)]
pub struct Rotated {
    pub minted: MintedKey,
    pub retired_until: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOwner {
    Tenant(Uuid),
    Platform,
}

pub async fn rotate(
    pool: &deadpool_postgres::Pool,
    owner: KeyOwner,
    id: Uuid,
    ttl: Option<chrono::Duration>,
    grace: chrono::Duration,
) -> PlatformResult<Option<Rotated>> {
    let (tenant_id, is_platform_admin) = match owner {
        KeyOwner::Tenant(tenant_id) => (Some(tenant_id), false),
        KeyOwner::Platform => (None, true),
    };
    let raw = generate(if is_platform_admin {
        "ck_platform_"
    } else {
        "ck_"
    });
    let now = Utc::now();
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let Some(seen) = txn
        .query_opt(
            "SELECT xmin::text FROM api_keys
             WHERE id = $1 AND tenant_id IS NOT DISTINCT FROM $2 AND is_platform_admin = $3
               AND (expires_at IS NULL OR expires_at > now())",
            &[&id, &tenant_id, &is_platform_admin],
        )
        .await?
    else {
        return Ok(None);
    };
    let version: String = seen.get(0);
    let retired = txn
        .query_opt(
            "UPDATE api_keys SET expires_at = LEAST(COALESCE(expires_at, 'infinity'), $2)
             WHERE id = $1 AND xmin::text = $3
             RETURNING name, user_sub, expires_at",
            &[&id, &(now + grace), &version],
        )
        .await?
        .ok_or_else(|| {
            PlatformError::Conflict(
                "the key changed while it was being rotated (rotated concurrently?); \
                 list the keys and retry"
                    .into(),
            )
        })?;
    let name: String = retired.get("name");
    let user_sub: Option<String> = retired.get("user_sub");
    let row = txn
        .query_one(
            "INSERT INTO api_keys (tenant_id, key_hash, name, user_sub, is_platform_admin, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, expires_at",
            &[
                &tenant_id,
                &hash(&raw),
                &name,
                &user_sub,
                &is_platform_admin,
                &ttl.map(|ttl| now + ttl),
            ],
        )
        .await?;
    txn.commit().await?;
    Ok(Some(Rotated {
        minted: MintedKey {
            id: row.get("id"),
            raw,
            expires_at: row.get("expires_at"),
        },
        retired_until: retired.get("expires_at"),
    }))
}

pub async fn list_for_tenant(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
) -> PlatformResult<Vec<ApiKeyRow>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "SELECT {KEY_COLUMNS} FROM api_keys
                 WHERE tenant_id = $1 AND is_platform_admin = false
                 ORDER BY created_at DESC"
            ),
            &[&tenant_id],
        )
        .await?;
    Ok(rows.iter().map(row_to_key).collect())
}

pub async fn list_platform(pool: &deadpool_postgres::Pool) -> PlatformResult<Vec<ApiKeyRow>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "SELECT {KEY_COLUMNS} FROM api_keys
                 WHERE is_platform_admin = true
                 ORDER BY created_at DESC"
            ),
            &[],
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
            "DELETE FROM api_keys WHERE id = $1 AND tenant_id = $2 AND is_platform_admin = false",
            &[&id, &tenant_id],
        )
        .await?;
    Ok(count > 0)
}

pub async fn revoke_platform(pool: &deadpool_postgres::Pool, id: Uuid) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let count = client
        .execute(
            "DELETE FROM api_keys WHERE id = $1 AND is_platform_admin = true",
            &[&id],
        )
        .await?;
    Ok(count > 0)
}

pub async fn revoke_for_subject(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    user_sub: &str,
) -> PlatformResult<u64> {
    let client = pool.get().await?;
    Ok(client
        .execute(
            "DELETE FROM api_keys WHERE tenant_id = $1 AND user_sub = $2",
            &[&tenant_id, &user_sub],
        )
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_sha256_hex_of_the_trimmed_key() {
        assert_eq!(hash(" ck_abc \n"), hex::encode(Sha256::digest(b"ck_abc")));
        assert_eq!(hash("ck_abc").len(), 64);
    }

    #[test]
    fn a_lookup_answers_expired_and_unknown_keys_apart() {
        let message = |lookup: KeyLookup| lookup.active().map_err(|e| e.to_string());
        assert_eq!(
            message(KeyLookup::Expired),
            Err("Unauthorized: API key expired".into())
        );
        assert_eq!(
            message(KeyLookup::Unknown),
            Err("Unauthorized: Invalid API key".into())
        );
    }

    #[test]
    fn generated_keys_carry_their_prefix_and_differ() {
        let (a, b) = (generate("ck_"), generate("ck_"));
        assert!(a.starts_with("ck_") && a.len() == 35, "{a}");
        assert_ne!(a, b);
    }
}
