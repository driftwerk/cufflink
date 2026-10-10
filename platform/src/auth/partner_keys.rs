use super::secret_keys::{self, KeyKind};
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::record::LogThrottle;
use crate::repo::partner_keys::{self, PartnerKey};
use crate::shutdown::Shutdown;
use crate::AppState;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub(crate) const FEATURES: &[&str] = &["partner_keys"];

pub const CHANGED_SUBJECT: &str = "cufflink.partner_keys.changed";

pub const KIND: KeyKind = KeyKind::new("cfp_");
const CACHE_TTL: Duration = Duration::from_secs(30);
const CACHE_MAX_KEYS: usize = 10_000;
const CACHE_MAX_UNKNOWN: usize = 1_000;
const TOUCH_EVERY: Duration = Duration::from_secs(60);
pub const MAX_SCOPES: usize = 100;

pub fn valid_scope(scope: &str) -> bool {
    let name = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_.-".contains(&b))
    };
    match scope.split_once(':') {
        None => name(scope),
        Some((resource, action)) => name(resource) && (action == "*" || name(action)),
    }
}

pub fn scope_matches(granted: &str, required: &str) -> bool {
    granted == required
        || granted.strip_suffix(":*").is_some_and(|resource| {
            required
                .split_once(':')
                .is_some_and(|(name, _)| name == resource)
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Malformed,
    Unknown,
    Revoked,
    Expired,
    Tenant,
    Scope,
}

impl Rejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Rejection::Malformed => "malformed",
            Rejection::Unknown => "unknown",
            Rejection::Revoked => "revoked",
            Rejection::Expired => "expired",
            Rejection::Tenant => "tenant",
            Rejection::Scope => "scope",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartnerPrincipal {
    pub id: Uuid,
    pub name: String,
    pub scopes: Vec<String>,
}

impl PartnerPrincipal {
    pub fn to_json(&self) -> Value {
        json!({"id": self.id, "name": self.name, "scopes": self.scopes})
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredKey {
    pub key: PartnerKey,
    pub secret_hash: String,
}

pub fn check(
    stored: &StoredKey,
    secret: &str,
    tenant_id: Uuid,
    scope: &str,
    now: DateTime<Utc>,
) -> Result<PartnerPrincipal, Rejection> {
    let key = &stored.key;
    if !secret_keys::proves(secret, &stored.secret_hash) {
        return Err(Rejection::Unknown);
    }
    if key.revoked_at.is_some() {
        return Err(Rejection::Revoked);
    }
    if key.expires_at <= now {
        return Err(Rejection::Expired);
    }
    if key.tenant_id != tenant_id {
        return Err(Rejection::Tenant);
    }
    if !key
        .scopes
        .iter()
        .any(|granted| scope_matches(granted, scope))
    {
        return Err(Rejection::Scope);
    }
    Ok(PartnerPrincipal {
        id: key.id,
        name: key.name.clone(),
        scopes: key.scopes.clone(),
    })
}

struct Expiring<V> {
    capacity: usize,
    entries: HashMap<String, (Instant, V)>,
}

impl<V: Clone> Expiring<V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
        }
    }

    fn get(&self, prefix: &str, now: Instant) -> Option<V> {
        self.entries
            .get(prefix)
            .filter(|(at, _)| now.saturating_duration_since(*at) < CACHE_TTL)
            .map(|(_, value)| value.clone())
    }

    fn insert(&mut self, prefix: &str, value: V, now: Instant) {
        if self.entries.len() >= self.capacity {
            self.entries
                .retain(|_, (at, _)| now.saturating_duration_since(*at) < CACHE_TTL);
        }
        if self.entries.len() >= self.capacity {
            self.entries.clear();
        }
        self.entries.insert(prefix.to_string(), (now, value));
    }
}

struct Entries {
    generation: u64,
    keys: Expiring<Arc<StoredKey>>,
    unknown: Expiring<()>,
}

impl Default for Entries {
    fn default() -> Self {
        Self {
            generation: 0,
            keys: Expiring::new(CACHE_MAX_KEYS),
            unknown: Expiring::new(CACHE_MAX_UNKNOWN),
        }
    }
}

#[derive(Clone, Default)]
pub struct PartnerKeyCache {
    entries: Arc<Mutex<Entries>>,
}

static TOUCHED: LazyLock<LogThrottle<Uuid>> = LazyLock::new(|| LogThrottle::new(TOUCH_EVERY));

impl PartnerKeyCache {
    fn entries(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn cached(&self, prefix: &str, now: Instant) -> (u64, Option<Option<Arc<StoredKey>>>) {
        let entries = self.entries();
        let cached = entries
            .keys
            .get(prefix, now)
            .map(Some)
            .or_else(|| entries.unknown.get(prefix, now).map(|()| None));
        (entries.generation, cached)
    }

    fn remember(
        &self,
        generation: u64,
        prefix: &str,
        stored: Option<Arc<StoredKey>>,
        now: Instant,
    ) {
        let mut entries = self.entries();
        if entries.generation != generation {
            return;
        }
        match stored {
            Some(stored) => entries.keys.insert(prefix, stored, now),
            None => entries.unknown.insert(prefix, (), now),
        }
    }

    pub fn clear(&self) {
        let mut entries = self.entries();
        *entries = Entries {
            generation: entries.generation.wrapping_add(1),
            ..Entries::default()
        };
    }

    async fn lookup(
        &self,
        pool: &deadpool_postgres::Pool,
        prefix: &str,
    ) -> PlatformResult<Option<Arc<StoredKey>>> {
        let now = Instant::now();
        let (generation, cached) = self.cached(prefix, now);
        if let Some(stored) = cached {
            return Ok(stored);
        }
        let stored = partner_keys::find_by_prefix(pool, prefix)
            .await?
            .map(|(key, secret_hash)| Arc::new(StoredKey { key, secret_hash }));
        self.remember(generation, prefix, stored.clone(), now);
        Ok(stored)
    }

    pub fn start_invalidation_listener(&self, nats: async_nats::Client, shutdown: &Shutdown) {
        let cache = self.clone();
        crate::nats_conn::spawn_reload_loop(
            shutdown,
            "partner_key_invalidation",
            nats,
            CHANGED_SUBJECT,
            CACHE_TTL,
            move || {
                cache.clear();
                std::future::ready(())
            },
        );
    }
}

pub async fn authenticate(
    state: &Arc<AppState>,
    presented: Option<&str>,
    tenant_id: Uuid,
    scope: &str,
) -> PlatformResult<PartnerPrincipal> {
    let refused = |reason: Rejection| {
        metrics::counter!("cufflink_partner_key_rejected_total", "reason" => reason.as_str())
            .increment(1);
        PlatformError::Unauthorized("Invalid API key".into())
    };
    let (prefix, secret) = presented
        .and_then(|presented| KIND.parse(presented))
        .ok_or_else(|| refused(Rejection::Malformed))?;
    let stored = state
        .partner_keys
        .lookup(&state.platform_db, prefix)
        .await?
        .ok_or_else(|| refused(Rejection::Unknown))?;
    let principal = check(&stored, secret, tenant_id, scope, Utc::now()).map_err(refused)?;
    if TOUCHED.should_log(principal.id, Instant::now()) {
        if let Err(e) = partner_keys::touch(&state.platform_db, principal.id).await {
            tracing::warn!(key_id = %principal.id, error = %e, "Could not record a partner key's last use");
        }
    }
    Ok(principal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(tenant_id: Uuid, scopes: &[&str], secret: &str) -> StoredKey {
        let now = Utc::now();
        StoredKey {
            key: PartnerKey {
                id: Uuid::new_v4(),
                tenant_id,
                name: "acme-ingest".into(),
                prefix: "AAAAAAAAAAAA".into(),
                scopes: scopes.iter().map(|s| s.to_string()).collect(),
                created_by: "admin".into(),
                created_at: now,
                expires_at: now + chrono::Duration::days(1),
                revoked_at: None,
                last_used_at: None,
            },
            secret_hash: secret_keys::secret_hash(secret),
        }
    }

    #[test]
    fn partner_keys_are_cfp_keys() {
        let (prefix, secret) = secret_keys::generate().expect("randomness");
        let raw = KIND.format(&prefix, &secret);
        assert!(raw.starts_with("cfp_"), "{raw}");
        assert_eq!(KIND.parse(&raw), Some((prefix.as_str(), secret.as_str())));
    }

    #[test]
    fn scopes_match_exactly_or_by_resource_wildcard() {
        assert!(scope_matches("books:ingest", "books:ingest"));
        assert!(scope_matches("books:*", "books:ingest"));
        assert!(!scope_matches("books:*", "receipts:ingest"));
        assert!(!scope_matches("books:ingest", "books:read"));
        assert!(!scope_matches("books", "books:ingest"));
        assert!(scope_matches("books", "books"));
        for valid in ["books", "books:ingest", "books.v2:*", "a-b_c:x.y-z"] {
            assert!(valid_scope(valid), "{valid}");
        }
        for invalid in [
            "",
            "Books",
            "books:",
            ":x",
            "books:ingest:more",
            "le ads",
            "*",
            "books:in*",
            "books:*x",
            "le*:ingest",
        ] {
            assert!(!valid_scope(invalid), "{invalid}");
        }
    }

    #[test]
    fn a_key_is_checked_for_secret_state_tenant_and_scope() {
        let tenant = Uuid::new_v4();
        let key = stored(tenant, &["books:*"], "s3cret");
        let now = Utc::now();
        assert_eq!(
            check(&key, "s3cret", tenant, "books:ingest", now).map(|p| p.name),
            Ok("acme-ingest".to_string())
        );
        assert_eq!(
            check(&key, "guess", tenant, "books:ingest", now),
            Err(Rejection::Unknown)
        );
        assert_eq!(
            check(&key, "s3cret", Uuid::new_v4(), "books:ingest", now),
            Err(Rejection::Tenant)
        );
        assert_eq!(
            check(&key, "s3cret", tenant, "receipts:read", now),
            Err(Rejection::Scope)
        );
        let later = now + chrono::Duration::days(2);
        assert_eq!(
            check(&key, "s3cret", tenant, "books:ingest", later),
            Err(Rejection::Expired)
        );
        let mut revoked = key.clone();
        revoked.key.revoked_at = Some(now);
        assert_eq!(
            check(&revoked, "s3cret", tenant, "books:ingest", now),
            Err(Rejection::Revoked)
        );
        assert_eq!(
            PartnerPrincipal {
                id: Uuid::nil(),
                name: "n".into(),
                scopes: vec!["a:b".into()],
            }
            .to_json(),
            json!({"id": Uuid::nil(), "name": "n", "scopes": ["a:b"]})
        );
    }

    #[test]
    fn the_cache_expires_entries_and_clears_on_demand() {
        let cache = PartnerKeyCache::default();
        let key = Arc::new(stored(Uuid::nil(), &["a"], "s"));
        let now = Instant::now();
        let (generation, cached) = cache.cached("p", now);
        assert_eq!(cached, None);
        cache.remember(generation, "p", Some(key.clone()), now);
        cache.remember(generation, "unknown", None, now);
        assert_eq!(cache.cached("p", now).1, Some(Some(key)));
        assert_eq!(
            cache.cached("unknown", now).1,
            Some(None),
            "misses are cached"
        );
        assert_eq!(cache.cached("p", now + CACHE_TTL).1, None, "expired");
        cache.clear();
        assert_eq!(cache.cached("unknown", now).1, None);
        assert_eq!(cache.cached("p", now).1, None);
    }

    #[test]
    fn a_read_that_raced_a_revocation_is_not_cached() {
        let cache = PartnerKeyCache::default();
        let now = Instant::now();
        let (before, _) = cache.cached("p", now);
        cache.clear();
        cache.remember(
            before,
            "p",
            Some(Arc::new(stored(Uuid::nil(), &["a"], "s"))),
            now,
        );
        assert_eq!(
            cache.cached("p", now).1,
            None,
            "the key read before the revoke is dropped"
        );
        let (after, _) = cache.cached("p", now);
        cache.remember(after, "p", None, now);
        assert_eq!(cache.cached("p", now).1, Some(None));
    }

    #[test]
    fn a_flood_of_unknown_prefixes_never_evicts_a_key() {
        let cache = PartnerKeyCache::default();
        let key = Arc::new(stored(Uuid::nil(), &["a"], "s"));
        let now = Instant::now();
        let (generation, _) = cache.cached("real", now);
        cache.remember(generation, "real", Some(key.clone()), now);
        for n in 0..CACHE_MAX_UNKNOWN * 3 {
            cache.remember(generation, &format!("made-up-{n}"), None, now);
        }
        assert_eq!(cache.cached("real", now).1, Some(Some(key)));
        let entries = cache.entries();
        assert!(entries.unknown.entries.len() <= CACHE_MAX_UNKNOWN);
    }

    #[tokio::test]
    async fn authentication_refuses_every_way_alike_and_admits_a_scoped_key() {
        use crate::shutdown::Shutdown;
        use crate::wasm_runtime::{WasmEngine, WasmLimits};
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        let state = AppState::minimal(engine).await;
        let tenant = Uuid::new_v4();
        let (prefix, secret) = secret_keys::generate().expect("randomness");
        let now = Instant::now();
        let (generation, _) = state.partner_keys.cached(&prefix, now);
        state.partner_keys.remember(
            generation,
            &prefix,
            Some(Arc::new(stored(tenant, &["books:*"], &secret))),
            now,
        );
        let (unknown, _) = secret_keys::generate().expect("randomness");
        state.partner_keys.remember(generation, &unknown, None, now);
        let key = KIND.format(&prefix, &secret);
        let admitted = authenticate(&state, Some(&key), tenant, "books:ingest")
            .await
            .expect("admitted");
        assert_eq!(admitted.scopes, vec!["books:*".to_string()]);
        for (presented, scope) in [
            (None, "books:ingest"),
            (Some("not-a-key".to_string()), "books:ingest"),
            (Some(KIND.format(&unknown, &secret)), "books:ingest"),
            (Some(KIND.format(&prefix, "wrong")), "books:ingest"),
            (Some(key.clone()), "receipts:read"),
        ] {
            let err = authenticate(&state, presented.as_deref(), tenant, scope)
                .await
                .expect_err("refused");
            assert_eq!(
                err.to_string(),
                "Unauthorized: Invalid API key",
                "{presented:?}"
            );
        }
    }
}
