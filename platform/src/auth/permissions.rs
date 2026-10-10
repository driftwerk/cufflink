use crate::errors::PlatformError;
use crate::AppState;
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

const PERMS_CACHE_TTL_SECS: u64 = 300;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedPermissions {
    pub permissions: Vec<String>,
    pub role_names: Vec<String>,
}

impl ResolvedPermissions {
    pub fn empty() -> Self {
        Self {
            permissions: vec![],
            role_names: vec![],
        }
    }
}

pub struct CacheSigner {
    key: hmac::Key,
}

#[derive(Serialize, Deserialize)]
struct Sealed {
    v: u8,
    sig: String,
    payload: String,
}

const SEAL_VERSION: u8 = 1;

impl CacheSigner {
    pub fn from_config(key: Option<&[u8]>) -> Self {
        let material = key.map(<[u8]>::to_vec).unwrap_or_else(|| {
            tracing::warn!(
                "CUFFLINK_CACHE_SIGNING_KEY is unset: the permission cache is per-replica until every replica shares a key"
            );
            [Uuid::new_v4(), Uuid::new_v4()]
                .iter()
                .flat_map(|u| u.into_bytes())
                .collect()
        });
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, &Sha256::digest(&material)),
        }
    }

    fn signed(cache_key: &str, payload: &str) -> Vec<u8> {
        [cache_key.as_bytes(), &[0], payload.as_bytes()].concat()
    }

    fn seal(&self, cache_key: &str, resolved: &ResolvedPermissions) -> Option<String> {
        let payload = serde_json::to_string(resolved).ok()?;
        let sig = hex::encode(hmac::sign(&self.key, &Self::signed(cache_key, &payload)));
        serde_json::to_string(&Sealed {
            v: SEAL_VERSION,
            sig,
            payload,
        })
        .ok()
    }

    fn open(&self, cache_key: &str, sealed: &str) -> Option<ResolvedPermissions> {
        let sealed: Sealed = serde_json::from_str(sealed).ok()?;
        let sig = hex::decode(&sealed.sig).ok()?;
        (sealed.v == SEAL_VERSION).then_some(())?;
        hmac::verify(&self.key, &Self::signed(cache_key, &sealed.payload), &sig).ok()?;
        serde_json::from_str(&sealed.payload).ok()
    }
}

pub fn cache_key(tenant_id: Uuid, subject: &str) -> String {
    format!("auth:perms:{tenant_id}:{subject}")
}

pub async fn resolve_permissions_cached(
    state: &Arc<AppState>,
    tenant_id: Uuid,
    user_subject: &str,
) -> Result<ResolvedPermissions, PlatformError> {
    if !state.redis.is_configured() {
        return resolve_permissions(&state.platform_db, tenant_id, user_subject).await;
    }

    use redis::AsyncCommands;
    let cache_key = cache_key(tenant_id, user_subject);

    let lookup_key = cache_key.clone();
    let cached = state
        .redis
        .call(|mut conn| async move { conn.get::<_, Option<String>>(&lookup_key).await })
        .await;
    let redis_healthy = !matches!(&cached, Err(e) if e.is_unavailable());
    if let Some(resolved) = cached
        .ok()
        .flatten()
        .and_then(|sealed| state.cache_signer.open(&cache_key, &sealed))
    {
        return Ok(resolved);
    }

    let resolved = resolve_permissions(&state.platform_db, tenant_id, user_subject).await?;
    if let Some(sealed) = redis_healthy
        .then(|| state.cache_signer.seal(&cache_key, &resolved))
        .flatten()
    {
        let _ = state
            .redis
            .call(|mut conn| async move {
                conn.set_ex::<_, _, ()>(&cache_key, &sealed, PERMS_CACHE_TTL_SECS)
                    .await
            })
            .await;
    }
    Ok(resolved)
}

pub async fn invalidate_user(state: &Arc<AppState>, tenant_id: Uuid, subject: &str) {
    if !state.redis.is_configured() {
        return;
    }
    use redis::AsyncCommands;
    let key = cache_key(tenant_id, subject);
    match state
        .redis
        .call(|mut conn| async move { conn.del::<_, u64>(&key).await })
        .await
    {
        Ok(_) => metrics::counter!("cufflink_perm_cache_invalidations_total", "scope" => "user")
            .increment(1),
        Err(e) => {
            tracing::warn!(%tenant_id, subject, error = %e, "Permission cache invalidation failed; the entry expires within its TTL")
        }
    }
}

const INVALIDATE_MAX_SCANS: usize = 1_000;

pub async fn invalidate_tenant(state: &Arc<AppState>, tenant_id: Uuid) {
    if !state.redis.is_configured() {
        return;
    }
    let pattern = format!("auth:perms:{tenant_id}:*");
    let mut cursor: u64 = 0;
    for _ in 0..INVALIDATE_MAX_SCANS {
        let pattern = pattern.clone();
        let step = state
            .redis
            .call(|mut conn| async move {
                let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(500)
                    .query_async(&mut conn)
                    .await?;
                if !keys.is_empty() {
                    redis::cmd("DEL")
                        .arg(&keys)
                        .query_async::<()>(&mut conn)
                        .await?;
                }
                Ok(next)
            })
            .await;
        match step {
            Ok(0) => {
                metrics::counter!("cufflink_perm_cache_invalidations_total", "scope" => "tenant")
                    .increment(1);
                return;
            }
            Ok(next) => cursor = next,
            Err(e) => {
                tracing::warn!(%tenant_id, error = %e, "Permission cache invalidation failed; entries expire within their TTL");
                return;
            }
        }
    }
    tracing::warn!(%tenant_id, "Permission cache invalidation stopped at its scan bound; remaining entries expire within their TTL");
}

pub async fn resolve_permissions(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    user_subject: &str,
) -> Result<ResolvedPermissions, PlatformError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT r.name, r.permissions
             FROM user_roles ur
             JOIN roles r ON ur.role_id = r.id
             WHERE ur.tenant_id = $1 AND ur.user_id = $2",
            &[&tenant_id, &user_subject],
        )
        .await?;

    let mut all_permissions = HashSet::new();
    let mut role_names = Vec::new();

    for row in &rows {
        let role_name: String = row.get(0);
        let permissions: Value = row.get(1);

        role_names.push(role_name);
        if let Some(perms) = permissions.as_array() {
            for p in perms {
                if let Some(s) = p.as_str() {
                    all_permissions.insert(s.to_string());
                }
            }
        }
    }

    let mut permissions: Vec<String> = all_permissions.into_iter().collect();
    permissions.sort();

    Ok(ResolvedPermissions {
        permissions,
        role_names,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(perms: &[&str]) -> ResolvedPermissions {
        ResolvedPermissions {
            permissions: perms.iter().map(|p| p.to_string()).collect(),
            role_names: vec!["staff".into()],
        }
    }

    fn signer() -> CacheSigner {
        CacheSigner::from_config(Some(b"0123456789abcdef-shared"))
    }

    #[test]
    fn a_value_is_sealed_as_every_release_seals_it() {
        let sealed = signer()
            .seal(&cache_key(Uuid::nil(), "alice"), &resolved(&["items:view"]))
            .expect("seal");
        let sealed: Value = serde_json::from_str(&sealed).expect("json");
        assert_eq!(
            sealed["sig"],
            "2e661cf445bbb79c209c321d458c56d7409c74a99e8fc9a0a0bed1a39068f2f1"
        );
    }

    #[test]
    fn seal_open_round_trip() {
        let key = cache_key(Uuid::nil(), "alice");
        let sealed = signer()
            .seal(&key, &resolved(&["items:view"]))
            .expect("seal");
        let opened = signer().open(&key, &sealed).expect("open");
        assert_eq!(opened.permissions, vec!["items:view".to_string()]);
    }

    #[test]
    fn open_rejects_tampered_payload() {
        let key = cache_key(Uuid::nil(), "alice");
        let sealed = signer()
            .seal(&key, &resolved(&["items:view"]))
            .expect("seal");
        let tampered = sealed.replace("items:view", "*");
        assert!(signer().open(&key, &tampered).is_none());
    }

    #[test]
    fn open_rejects_value_moved_between_keys() {
        let admin = cache_key(Uuid::nil(), "admin");
        let sealed = signer().seal(&admin, &resolved(&["*"])).expect("seal");
        assert!(signer()
            .open(&cache_key(Uuid::nil(), "mallory"), &sealed)
            .is_none());
    }

    #[test]
    fn open_rejects_unsigned_legacy_value_and_other_keys() {
        let key = cache_key(Uuid::nil(), "alice");
        let legacy = serde_json::to_string(&resolved(&["*"])).expect("json");
        assert!(signer().open(&key, &legacy).is_none());
        let sealed = signer().seal(&key, &resolved(&["*"])).expect("seal");
        assert!(CacheSigner::from_config(None).open(&key, &sealed).is_none());
    }

    #[test]
    fn cache_key_shape() {
        assert_eq!(
            cache_key(Uuid::nil(), "sub-1"),
            "auth:perms:00000000-0000-0000-0000-000000000000:sub-1"
        );
    }
}
