use crate::error::TestkitError;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Map, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const PRIVATE_KEY_PEM: &str = include_str!("keycloak_test_key.pem");
const KEY_ID: &str = "cufflink-test-key";
const MODULUS: &str = "uf9adoR8mCm90oRMXyFa7NL2jTKFkEVcvLlmV_Q_An69IyQJrN5t4vjytHDvUr56dgPsSk0MnXwXUrsse1pPejhvxES4LKf4ZOvQeldC7f2f2Fhdx25Mc8DXr6aQiuaOU626ocWclSTy6QWifXVY1P2FlOLoj9DMkw0GuNcjejBgzr6xKi_9xPYtqn0dJCtrlI3nkamA71Ht6MgoL3Uj4mVPB7tTGyGOs4vjX8SbWZIc3vXzW7qEgtk0-tKAL_hoMwA4xofUdc2GNIAshQmgpfVgEjyG7GkM78U3IM-6SH13j-gbdN0JOwjMWZ98qr_-Mutnj9QFFWt1NqpGxvUdvw";
const LIFETIME_SECS: u64 = 600;

pub struct TestIssuer {
    url: String,
    issuer_base: String,
    discovery_up: Arc<AtomicBool>,
}

impl TestIssuer {
    pub async fn start() -> Result<Self, TestkitError> {
        Self::serve(None).await
    }

    pub async fn start_behind(issuer_base: &str) -> Result<Self, TestkitError> {
        Self::serve(Some(issuer_base.to_string())).await
    }

    async fn serve(issuer_base: Option<String>) -> Result<Self, TestkitError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let issuer_base = issuer_base.unwrap_or_else(|| url.clone());
        let discovery_up = Arc::new(AtomicBool::new(true));
        let (discovery_issuer, up) = (issuer_base.clone(), Arc::clone(&discovery_up));
        let app = Router::new()
            .route(
                "/realms/{realm}/protocol/openid-connect/certs",
                get(|| async { Json(jwks()) }),
            )
            .route(
                "/realms/{realm}/.well-known/openid-configuration",
                get(move |Path(realm): Path<String>| {
                    let issuer = format!("{discovery_issuer}/realms/{realm}");
                    let up = up.load(Ordering::SeqCst);
                    async move {
                        up.then(|| Json(json!({ "issuer": issuer })))
                            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
                    }
                }),
            );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            url,
            issuer_base,
            discovery_up,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn issuer_base(&self) -> &str {
        &self.issuer_base
    }

    pub fn set_discovery(&self, up: bool) {
        self.discovery_up.store(up, Ordering::SeqCst);
    }

    pub fn token(&self, realm: &str) -> TokenBuilder {
        TokenBuilder {
            issuer: format!("{}/realms/{realm}", self.issuer_base),
            lifetime_secs: LIFETIME_SECS,
            claims: Map::from_iter([
                ("sub".to_string(), json!("user-1")),
                ("azp".to_string(), json!("portal")),
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TokenBuilder {
    issuer: String,
    lifetime_secs: u64,
    claims: Map<String, Value>,
}

impl TokenBuilder {
    pub fn subject(self, subject: &str) -> Self {
        self.claim("sub", subject)
    }

    pub fn roles(self, roles: &[&str]) -> Self {
        self.claim("realm_access", json!({ "roles": roles }))
    }

    pub fn client(self, client: &str) -> Self {
        self.claim("azp", client)
    }

    pub fn claim(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.claims.insert(name.to_string(), value.into());
        self
    }

    pub fn claims(mut self, claims: Map<String, Value>) -> Self {
        self.claims.extend(claims);
        self
    }

    pub fn lifetime_secs(self, lifetime_secs: u64) -> Self {
        Self {
            lifetime_secs,
            ..self
        }
    }

    pub fn names(&self) -> Option<&str> {
        self.claims.get("sub").and_then(Value::as_str)
    }

    pub fn sign(self) -> Result<String, TestkitError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| TestkitError::Platform(format!("the clock is before 1970: {e}")))?
            .as_secs();
        let claims: Map<String, Value> = Map::from_iter([
            ("iss".to_string(), json!(self.issuer)),
            ("iat".to_string(), json!(now)),
            ("exp".to_string(), json!(now + self.lifetime_secs)),
        ])
        .into_iter()
        .chain(self.claims)
        .collect();
        let header = Header {
            kid: Some(KEY_ID.into()),
            ..Header::new(Algorithm::RS256)
        };
        Ok(encode(&header, &claims, signing_key()?)?)
    }
}

fn signing_key() -> Result<&'static EncodingKey, TestkitError> {
    static KEY: OnceLock<Result<EncodingKey, String>> = OnceLock::new();
    KEY.get_or_init(|| {
        EncodingKey::from_rsa_pem(PRIVATE_KEY_PEM.as_bytes()).map_err(|e| e.to_string())
    })
    .as_ref()
    .map_err(|reason| TestkitError::Platform(format!("the testkit's key does not parse: {reason}")))
}

fn jwks() -> Value {
    json!({
        "keys": [{
            "kty": "RSA",
            "kid": KEY_ID,
            "alg": "RS256",
            "use": "sig",
            "n": MODULUS,
            "e": "AQAB",
        }]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    fn claims_of(token: &str) -> Value {
        let payload = token.split('.').nth(1).expect("a payload");
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).expect("base64")).expect("json")
    }

    #[tokio::test]
    async fn a_token_names_its_realm_and_what_it_was_given() {
        let issuer = TestIssuer::start().await.expect("issuer");
        let builder = issuer
            .token("harbor")
            .subject("ops-1")
            .roles(&["cufflink-deploy"])
            .client("harbor-ops")
            .claim("branch_id", "org-7");
        assert_eq!(builder.names(), Some("ops-1"));
        let claims = claims_of(&builder.sign().expect("token"));
        assert_eq!(claims["iss"], format!("{}/realms/harbor", issuer.url()));
        assert_eq!(claims["sub"], "ops-1");
        assert_eq!(claims["azp"], "harbor-ops");
        assert_eq!(claims["realm_access"]["roles"], json!(["cufflink-deploy"]));
        assert_eq!(claims["branch_id"], "org-7");
        assert_eq!(
            claims["exp"]
                .as_u64()
                .zip(claims["iat"].as_u64())
                .map(|(exp, iat)| exp - iat),
            Some(600)
        );
        let defaults = claims_of(&issuer.token("harbor").sign().expect("token"));
        assert_eq!(
            (&defaults["sub"], &defaults["azp"]),
            (&json!("user-1"), &json!("portal"))
        );
        assert!(defaults.get("realm_access").is_none());
    }

    #[tokio::test]
    async fn claims_given_whole_override_the_tokens_own() {
        let issuer = TestIssuer::start_behind("https://auth.cufflink.test")
            .await
            .expect("issuer");
        assert_eq!(issuer.issuer_base(), "https://auth.cufflink.test");
        assert_ne!(issuer.url(), issuer.issuer_base());
        let expired = claims_of(
            &issuer
                .token("acme")
                .claims(Map::from_iter([
                    ("exp".to_string(), json!(1)),
                    ("iss".to_string(), json!("https://elsewhere/realms/acme")),
                    ("sub".to_string(), json!("u-2")),
                ]))
                .sign()
                .expect("token"),
        );
        assert_eq!(expired["exp"], 1);
        assert_eq!(expired["iss"], "https://elsewhere/realms/acme");
        assert_eq!(expired["sub"], "u-2");
        let short = claims_of(&issuer.token("acme").lifetime_secs(5).sign().expect("token"));
        assert_eq!(short["iss"], "https://auth.cufflink.test/realms/acme");
        assert_eq!(
            short["exp"]
                .as_u64()
                .zip(short["iat"].as_u64())
                .map(|(exp, iat)| exp - iat),
            Some(5)
        );
    }

    #[tokio::test]
    async fn discovery_and_keys_are_served_for_every_realm() {
        let issuer = TestIssuer::start().await.expect("issuer");
        let get = |path: String| async move { reqwest::get(path).await.expect("answer") };
        let discovery = get(format!(
            "{}/realms/harbor/.well-known/openid-configuration",
            issuer.url()
        ))
        .await;
        assert_eq!(discovery.status(), 200);
        assert_eq!(
            discovery.json::<Value>().await.expect("json")["issuer"],
            format!("{}/realms/harbor", issuer.url())
        );
        let keys = get(format!(
            "{}/realms/any/protocol/openid-connect/certs",
            issuer.url()
        ))
        .await
        .json::<Value>()
        .await
        .expect("json");
        assert_eq!(keys["keys"][0]["kid"], KEY_ID);
        issuer.set_discovery(false);
        let down = get(format!(
            "{}/realms/harbor/.well-known/openid-configuration",
            issuer.url()
        ))
        .await;
        assert_eq!(down.status(), 503);
    }
}
