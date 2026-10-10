use crate::nats_conn::Backoff;
use eyre::Result;
use jsonwebtoken::{
    decode, decode_header,
    jwk::{Jwk, JwkSet},
    Algorithm, DecodingKey, Validation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

const JWKS_CACHE_TTL: Duration = Duration::from_secs(60 * 60);

const JWKS_REFETCH_COOLDOWN: Duration = Duration::from_secs(10);

const JWKS_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

pub const JWKS_MAX_STALE_DEFAULT: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone)]
struct CachedJwks {
    jwks: JwkSet,
    fetched_at: Instant,
}

#[derive(Debug, Default)]
struct DiscoveryRetry {
    backoff: Backoff,
    not_before: Option<Instant>,
}

#[derive(Clone)]
pub struct KeycloakAuth {
    keycloak_url: String,
    realm: String,
    jwks_url: String,
    issuer_cache: Arc<RwLock<Option<String>>>,
    discovery_retry: Arc<Mutex<DiscoveryRetry>>,
    jwks_cache: Arc<RwLock<Option<CachedJwks>>>,
    refetch_lock: Arc<Mutex<()>>,
    last_fetch_attempt: Arc<RwLock<Option<Instant>>>,
    http: reqwest::Client,
    ttl: Duration,
    max_stale: Duration,
}

#[derive(Debug)]
enum RefreshError {
    RateLimited,
    Fetch(eyre::Report),
}

impl std::fmt::Display for RefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited => write!(
                f,
                "JWKS fetch rate-limited (cooldown {JWKS_REFETCH_COOLDOWN:?})"
            ),
            Self::Fetch(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for RefreshError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub sub: String,
    pub preferred_username: Option<String>,
    pub name: Option<String>,
    pub email: Option<String>,
    #[serde(default)]
    pub realm_roles: Vec<String>,
    #[serde(default)]
    pub claims: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct RealmAccess {
    #[serde(default)]
    roles: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    preferred_username: Option<String>,
    name: Option<String>,
    email: Option<String>,
    realm_access: Option<RealmAccess>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    issuer: String,
}

impl From<Claims> for TokenInfo {
    fn from(c: Claims) -> Self {
        Self {
            sub: c.sub,
            preferred_username: c.preferred_username,
            name: c.name,
            email: c.email,
            realm_roles: c.realm_access.map(|ra| ra.roles).unwrap_or_default(),
            claims: c.extra,
        }
    }
}

fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(JWKS_HTTP_TIMEOUT)
        .build()
        .unwrap_or_default()
}

fn find_jwk<'a>(jwks: &'a JwkSet, kid: &str) -> Option<&'a Jwk> {
    jwks.keys
        .iter()
        .find(|k| k.common.key_id.as_deref() == Some(kid))
}

impl KeycloakAuth {
    pub fn new(
        keycloak_url: &str,
        realm: &str,
        issuer_override: Option<&str>,
        max_stale: Duration,
    ) -> Self {
        let issuer = issuer_override.map(|base| {
            let issuer = format!("{}/realms/{}", base.trim_end_matches('/'), realm);
            debug!("Using issuer override: {}", issuer);
            issuer
        });
        Self::build(keycloak_url, realm, issuer, max_stale)
    }

    pub fn with_issuer(
        keycloak_url: &str,
        realm: &str,
        issuer_exact: &str,
        max_stale: Duration,
    ) -> Self {
        debug!(
            "Using exact issuer: {} (JWKS from {})",
            issuer_exact, keycloak_url
        );
        Self::build(
            keycloak_url,
            realm,
            Some(issuer_exact.to_string()),
            max_stale,
        )
    }

    fn build(keycloak_url: &str, realm: &str, issuer: Option<String>, max_stale: Duration) -> Self {
        Self {
            keycloak_url: keycloak_url.to_string(),
            realm: realm.to_string(),
            jwks_url: format!(
                "{}/realms/{}/protocol/openid-connect/certs",
                keycloak_url, realm
            ),
            issuer_cache: Arc::new(RwLock::new(issuer)),
            discovery_retry: Arc::default(),
            jwks_cache: Arc::new(RwLock::new(None)),
            refetch_lock: Arc::new(Mutex::new(())),
            last_fetch_attempt: Arc::new(RwLock::new(None)),
            http: build_http_client(),
            ttl: JWKS_CACHE_TTL,
            max_stale,
        }
    }

    async fn cached_jwks_if_fresh(&self) -> Option<JwkSet> {
        self.cached_jwks_younger_than(self.ttl).await
    }

    async fn cached_jwks_younger_than(&self, age: Duration) -> Option<JwkSet> {
        self.jwks_cache
            .read()
            .await
            .as_ref()
            .and_then(|c| (c.fetched_at.elapsed() < age).then(|| c.jwks.clone()))
    }

    async fn cached_issuer(&self) -> Option<String> {
        self.issuer_cache.read().await.clone()
    }

    async fn get_issuer(&self) -> Result<String> {
        if let Some(issuer) = self.cached_issuer().await {
            return Ok(issuer);
        }
        let mut retry = self.discovery_retry.lock().await;
        if let Some(issuer) = self.cached_issuer().await {
            return Ok(issuer);
        }
        let now = Instant::now();
        if let Some(not_before) = retry.not_before.filter(|at| now < *at) {
            return Err(eyre::eyre!(
                "OIDC discovery for realm {} failed; retrying in {:?}",
                self.realm,
                not_before - now
            ));
        }
        match self.discover_issuer().await {
            Ok(issuer) => {
                debug!("Discovered issuer={}", issuer);
                *self.issuer_cache.write().await = Some(issuer.clone());
                *retry = DiscoveryRetry::default();
                Ok(issuer)
            }
            Err(e) => {
                let delay = retry.backoff.next_delay();
                retry.not_before = Some(now + delay);
                warn!(realm = %self.realm, error = %e, retry_in = ?delay, "OIDC discovery failed");
                Err(e)
            }
        }
    }

    async fn discover_issuer(&self) -> Result<String> {
        let discovery_url = format!(
            "{}/realms/{}/.well-known/openid-configuration",
            self.keycloak_url, self.realm
        );
        debug!("Fetching OIDC discovery from {}", discovery_url);
        let discovery = self
            .http
            .get(&discovery_url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| eyre::eyre!("OIDC discovery fetch failed ({discovery_url}): {e}"))?
            .json::<OidcDiscovery>()
            .await
            .map_err(|e| eyre::eyre!("OIDC discovery response unreadable: {e}"))?;
        Ok(discovery.issuer)
    }

    async fn get_jwks(&self) -> Result<JwkSet> {
        if let Some(jwks) = self.cached_jwks_if_fresh().await {
            return Ok(jwks);
        }
        match self.refresh_jwks(false).await {
            Ok(jwks) => Ok(jwks),
            Err(error) => self.serve_stale(error).await,
        }
    }

    async fn serve_stale(&self, error: RefreshError) -> Result<JwkSet> {
        let Some(jwks) = self.cached_jwks_younger_than(self.max_stale).await else {
            return Err(eyre::eyre!(
                "{error}; no JWKS younger than {:?} cached",
                self.max_stale
            ));
        };
        metrics::counter!("cufflink_jwks_stale_served_total", "realm" => self.realm.clone())
            .increment(1);
        match error {
            RefreshError::Fetch(e) => warn!(
                realm = %self.realm,
                error = %e,
                "JWKS refresh failed; serving the cached key set"
            ),
            RefreshError::RateLimited => {
                debug!(realm = %self.realm, "Serving the cached JWKS within the refetch cooldown")
            }
        }
        Ok(jwks)
    }

    async fn refresh_jwks(&self, force: bool) -> Result<JwkSet, RefreshError> {
        let _guard = self.refetch_lock.lock().await;

        if !force {
            if let Some(jwks) = self.cached_jwks_if_fresh().await {
                return Ok(jwks);
            }
        }

        let now = Instant::now();
        let cooldown_active = self
            .last_fetch_attempt
            .read()
            .await
            .map(|t| now.saturating_duration_since(t) < JWKS_REFETCH_COOLDOWN)
            .unwrap_or(false);
        if cooldown_active {
            return Err(RefreshError::RateLimited);
        }

        *self.last_fetch_attempt.write().await = Some(now);
        self.fetch_and_cache_jwks()
            .await
            .map_err(RefreshError::Fetch)
    }

    async fn fetch_and_cache_jwks(&self) -> Result<JwkSet> {
        debug!("Fetching JWKS from {}", self.jwks_url);
        let resp = self
            .http
            .get(&self.jwks_url)
            .send()
            .await
            .map_err(|e| eyre::eyre!("JWKS fetch failed ({}): {}", self.jwks_url, e))?;

        let status = resp.status();
        if !status.is_success() {
            return Err(eyre::eyre!(
                "JWKS fetch returned HTTP {} from {}",
                status,
                self.jwks_url
            ));
        }

        let jwks: JwkSet = resp
            .json()
            .await
            .map_err(|e| eyre::eyre!("JWKS response not valid JWKS JSON: {}", e))?;

        let kid_list = jwks
            .keys
            .iter()
            .filter_map(|k| k.common.key_id.as_deref())
            .collect::<Vec<_>>()
            .join(", ");
        debug!(
            "JWKS fetched: {} key(s), kids=[{}]",
            jwks.keys.len(),
            kid_list
        );

        *self.jwks_cache.write().await = Some(CachedJwks {
            jwks: jwks.clone(),
            fetched_at: Instant::now(),
        });
        Ok(jwks)
    }

    pub async fn invalidate_cache(&self) {
        *self.jwks_cache.write().await = None;
    }

    pub async fn validate_token(&self, token: &str) -> Result<TokenInfo> {
        let header = decode_header(token)?;
        let kid = header
            .kid
            .ok_or_else(|| eyre::eyre!("JWT missing kid header"))?;

        let issuer = self.get_issuer().await?;
        let jwks = self.get_jwks().await?;

        let decoding_key = match find_jwk(&jwks, &kid) {
            Some(jwk) => DecodingKey::from_jwk(jwk)?,
            None => {
                warn!("kid={} not in cached JWKS, forcing refresh", kid);
                let fresh = self.refresh_jwks(true).await?;
                let jwk = find_jwk(&fresh, &kid).ok_or_else(|| {
                    eyre::eyre!("No matching JWK for kid={} after JWKS refresh", kid)
                })?;
                info!("kid={} recovered after JWKS refresh", kid);
                DecodingKey::from_jwk(jwk)?
            }
        };

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&issuer]);
        validation.validate_exp = true;
        validation.validate_aud = false;

        Ok(decode::<Claims>(token, &decoding_key, &validation)?
            .claims
            .into())
    }
}

#[cfg(test)]
impl KeycloakAuth {
    pub(crate) async fn seed_jwks_for_test(&self, jwks: JwkSet) {
        *self.jwks_cache.write().await = Some(CachedJwks {
            jwks,
            fetched_at: Instant::now(),
        });
    }

    pub(crate) async fn cached_jwks_for_test(&self) -> Option<JwkSet> {
        self.cached_jwks_younger_than(Duration::MAX).await
    }

    pub(crate) fn with_zero_ttl(mut self) -> Self {
        self.ttl = Duration::ZERO;
        self
    }

    pub(crate) async fn retry_discovery_now_for_test(&self) {
        self.discovery_retry.lock().await.not_before = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_auth() -> KeycloakAuth {
        KeycloakAuth::new(
            "http://localhost:0",
            "test-realm",
            None,
            JWKS_MAX_STALE_DEFAULT,
        )
    }

    async fn seed_cooldown(auth: &KeycloakAuth) {
        *auth.last_fetch_attempt.write().await = Some(Instant::now());
    }

    fn one_key() -> JwkSet {
        serde_json::from_value(serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "kid": "k1",
                "alg": "RS256",
                "use": "sig",
                "n": "AQAB",
                "e": "AQAB"
            }]
        }))
        .expect("jwk set")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_jwks_are_served_while_the_refresh_fails_and_counted() {
        let recorder = cufflink_telemetry::metrics_builder()
            .expect("builder")
            .build_recorder();
        let handle = recorder.handle();
        let auth = new_auth().with_zero_ttl();
        auth.seed_jwks_for_test(one_key()).await;

        let runtime = tokio::runtime::Handle::current();
        let served = metrics::with_local_recorder(&recorder, || {
            tokio::task::block_in_place(|| runtime.block_on(auth.get_jwks()))
        })
        .expect("the stale set is served while the refresh fails");
        assert_eq!(served.keys.len(), 1);
        let rendered = handle.render();
        assert!(
            rendered.contains("cufflink_jwks_stale_served_total{realm=\"test-realm\"} 1"),
            "{rendered}"
        );

        assert_eq!(auth.get_jwks().await.expect("stale again").keys.len(), 1);
    }

    #[tokio::test]
    async fn stale_jwks_are_refused_past_max_stale_or_with_an_empty_cache() {
        let too_old = KeycloakAuth::new("http://localhost:0", "test-realm", None, Duration::ZERO)
            .with_zero_ttl();
        too_old.seed_jwks_for_test(one_key()).await;
        let err = too_old.get_jwks().await.expect_err("older than max stale");
        assert!(err.to_string().contains("no JWKS younger than"), "{err}");

        let empty = new_auth().with_zero_ttl();
        assert!(empty.get_jwks().await.is_err());
    }

    #[tokio::test]
    async fn a_failed_discovery_is_retried_with_backoff_and_never_pins_a_guess() {
        use axum::http::StatusCode;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let (up, hits) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicUsize::new(0)),
        );
        let app = axum::Router::new().route(
            "/realms/{realm}/.well-known/openid-configuration",
            axum::routing::get({
                let (up, hits) = (Arc::clone(&up), Arc::clone(&hits));
                move || async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    if up.load(Ordering::SeqCst) {
                        Ok(axum::Json(serde_json::json!({
                            "issuer": "https://auth.example.test/realms/test-realm"
                        })))
                    } else {
                        Err(StatusCode::SERVICE_UNAVAILABLE)
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let auth = KeycloakAuth::new(&url, "test-realm", None, JWKS_MAX_STALE_DEFAULT);

        assert!(auth.get_issuer().await.is_err(), "discovery is down");
        assert_eq!(auth.cached_issuer().await, None, "no fallback is cached");
        up.store(true, Ordering::SeqCst);
        let err = auth.get_issuer().await.expect_err("inside the backoff");
        assert!(err.to_string().contains("retrying in"), "{err}");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "the backoff gates discovery"
        );

        auth.retry_discovery_now_for_test().await;
        assert_eq!(
            auth.get_issuer().await.expect("discovered"),
            "https://auth.example.test/realms/test-realm"
        );
        up.store(false, Ordering::SeqCst);
        assert!(
            auth.get_issuer().await.is_ok(),
            "a discovered issuer stays cached"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidate_cache_clears_stored_jwks() {
        let auth = new_auth();
        auth.seed_jwks_for_test(JwkSet { keys: vec![] }).await;

        auth.invalidate_cache().await;

        assert!(auth.jwks_cache.read().await.is_none());
    }

    #[tokio::test]
    async fn get_jwks_returns_fresh_cache_without_fetching() {
        let auth = new_auth();
        auth.seed_jwks_for_test(JwkSet { keys: vec![] }).await;

        let jwks = auth
            .get_jwks()
            .await
            .expect("fresh cache returns instantly");
        assert_eq!(jwks.keys.len(), 0);
    }

    #[tokio::test]
    async fn an_unknown_kid_refresh_under_cooldown_is_rate_limited_not_served_stale() {
        let auth = new_auth();
        auth.seed_jwks_for_test(JwkSet { keys: vec![] }).await;
        seed_cooldown(&auth).await;

        let err = auth
            .refresh_jwks(true)
            .await
            .expect_err("cooldown blocks a forced refresh");
        assert!(matches!(err, RefreshError::RateLimited), "{err}");
        assert!(err.to_string().contains("rate-limited"), "{err}");
    }

    #[tokio::test]
    async fn refresh_records_attempt_so_cooldown_gates_retries() {
        let auth = new_auth();

        let first = auth.refresh_jwks(false).await;
        assert!(first.is_err(), "unreachable URL should produce an error");
        assert!(
            auth.last_fetch_attempt.read().await.is_some(),
            "a failed fetch must still record an attempt for cooldown"
        );

        let second = auth.refresh_jwks(false).await.expect_err("cooldown blocks");
        assert!(matches!(second, RefreshError::RateLimited), "{second}");
    }
}
