use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use super::keycloak::TokenInfo;
use super::KeycloakAuth;

#[derive(Clone)]
pub struct KeycloakAuthRegistry {
    entries: Arc<RwLock<HashMap<AuthKey, KeycloakAuth>>>,
    jwks_max_stale: Duration,
}

type AuthKey = (String, String, Option<String>);

impl KeycloakAuthRegistry {
    pub fn new(jwks_max_stale: Duration) -> Self {
        Self {
            entries: Arc::default(),
            jwks_max_stale,
        }
    }

    pub async fn validate(
        &self,
        keycloak_url: &str,
        realm: &str,
        issuer_base: Option<&str>,
        token: &str,
    ) -> eyre::Result<TokenInfo> {
        self.auth_for(keycloak_url, realm, issuer_base)
            .await
            .validate_token(token)
            .await
    }

    async fn auth_for(
        &self,
        keycloak_url: &str,
        realm: &str,
        issuer_base: Option<&str>,
    ) -> KeycloakAuth {
        let key = (
            keycloak_url.to_string(),
            realm.to_string(),
            issuer_base.map(str::to_string),
        );

        if let Some(auth) = self.entries.read().await.get(&key).cloned() {
            return auth;
        }

        self.entries
            .write()
            .await
            .entry(key)
            .or_insert_with(|| {
                KeycloakAuth::new(keycloak_url, realm, issuer_base, self.jwks_max_stale)
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::join_all;
    use jsonwebtoken::jwk::JwkSet;

    const URL_A: &str = "http://kc-a.local";
    const URL_B: &str = "http://kc-b.local";
    const REALM_A: &str = "realm-a";
    const REALM_B: &str = "realm-b";
    const ISSUER: Option<&str> = Some("https://auth.a.test");

    #[tokio::test]
    async fn auth_for_returns_cached_instance_for_same_key() {
        let registry = KeycloakAuthRegistry::new(crate::auth::keycloak::JWKS_MAX_STALE_DEFAULT);

        let first = registry.auth_for(URL_A, REALM_A, ISSUER).await;
        first.seed_jwks_for_test(JwkSet { keys: vec![] }).await;

        let second = registry.auth_for(URL_A, REALM_A, ISSUER).await;

        assert!(
            second.cached_jwks_for_test().await.is_some(),
            "second lookup must see the JWKS seeded on the first instance"
        );
    }

    #[tokio::test]
    async fn auth_for_separates_entries_by_realm() {
        let registry = KeycloakAuthRegistry::new(crate::auth::keycloak::JWKS_MAX_STALE_DEFAULT);

        let a = registry.auth_for(URL_A, REALM_A, ISSUER).await;
        a.seed_jwks_for_test(JwkSet { keys: vec![] }).await;

        let b = registry.auth_for(URL_A, REALM_B, ISSUER).await;

        assert!(b.cached_jwks_for_test().await.is_none());
    }

    #[tokio::test]
    async fn auth_for_separates_entries_by_url() {
        let registry = KeycloakAuthRegistry::new(crate::auth::keycloak::JWKS_MAX_STALE_DEFAULT);

        let a = registry.auth_for(URL_A, REALM_A, ISSUER).await;
        a.seed_jwks_for_test(JwkSet { keys: vec![] }).await;

        let b = registry.auth_for(URL_B, REALM_A, ISSUER).await;

        assert!(b.cached_jwks_for_test().await.is_none());
    }

    #[tokio::test]
    async fn concurrent_auth_for_constructs_only_once() {
        let registry = KeycloakAuthRegistry::new(crate::auth::keycloak::JWKS_MAX_STALE_DEFAULT);

        let calls = (0..32).map(|_| {
            let r = registry.clone();
            tokio::spawn(async move { r.auth_for(URL_A, REALM_A, ISSUER).await })
        });
        join_all(calls).await;

        assert_eq!(registry.entries.read().await.len(), 1);
    }

    #[tokio::test]
    async fn auth_for_separates_entries_by_issuer_base() {
        let registry = KeycloakAuthRegistry::new(crate::auth::keycloak::JWKS_MAX_STALE_DEFAULT);

        let a = registry.auth_for(URL_A, REALM_A, ISSUER).await;
        a.seed_jwks_for_test(JwkSet { keys: vec![] }).await;

        let discovered = registry.auth_for(URL_A, REALM_A, None).await;

        assert!(discovered.cached_jwks_for_test().await.is_none());
        assert_eq!(registry.entries.read().await.len(), 2);
    }

    #[tokio::test]
    async fn validate_rejects_a_token_without_building_state_from_it() {
        let registry = KeycloakAuthRegistry::new(crate::auth::keycloak::JWKS_MAX_STALE_DEFAULT);
        let err = registry
            .validate(URL_A, REALM_A, ISSUER, "not-a-jwt")
            .await
            .unwrap_err();
        assert!(!err.to_string().is_empty());
        let entries = registry.entries.read().await;
        assert_eq!(
            entries.keys().cloned().collect::<Vec<_>>(),
            vec![(
                URL_A.to_string(),
                REALM_A.to_string(),
                ISSUER.map(str::to_string)
            )]
        );
    }
}
