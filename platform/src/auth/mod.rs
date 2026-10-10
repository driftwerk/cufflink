pub mod audience;
pub mod binding;
pub(crate) mod columns;
pub mod extractor;
pub(crate) mod grants;
pub mod issuer;
pub mod keycloak;
pub mod keycloak_admin;
pub mod partner_keys;
pub mod permissions;
pub mod registry;
pub mod runner_keys;
pub(crate) mod scope;
pub mod secret_keys;
pub mod service_accounts;
pub mod service_identity;

pub(crate) use extractor::{authenticate, require_deploy_authority, AuthOutcome};
pub use extractor::{AuthUser, DeployAuth, OptionalAuth, PlatformAdmin, RbacAdmin};
pub use keycloak::KeycloakAuth;
pub use registry::KeycloakAuthRegistry;

pub fn peek_jwt_issuer(token: &str) -> Option<String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, parts[1]).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    claims["iss"].as_str().map(String::from)
}

pub fn peek_jwt_realm(token: &str) -> Option<String> {
    peek_jwt_issuer(token)?
        .rsplit_once("/realms/")
        .map(|(_, realm)| realm.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::json;

    fn fake_token(payload: serde_json::Value) -> String {
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = engine.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload_b64 = engine.encode(payload.to_string());
        format!("{}.{}.FAKE_SIG", header, payload_b64)
    }

    #[test]
    fn peek_issuer_extracts_iss_claim() {
        let token = fake_token(json!({ "iss": "https://auth.example.com/realms/foo" }));
        assert_eq!(
            peek_jwt_issuer(&token),
            Some("https://auth.example.com/realms/foo".to_string())
        );
    }

    #[test]
    fn peek_issuer_rejects_malformed_tokens() {
        assert_eq!(peek_jwt_issuer(""), None);
        assert_eq!(peek_jwt_issuer("only-one-part"), None);
        assert_eq!(peek_jwt_issuer("header.not-base64-$$.sig"), None);
    }

    #[test]
    fn peek_realm_extracts_from_keycloak_issuer() {
        let token = fake_token(json!({ "iss": "https://auth.example.com/realms/my-realm" }));
        assert_eq!(peek_jwt_realm(&token), Some("my-realm".to_string()));
    }

    #[test]
    fn peek_realm_returns_none_when_issuer_is_not_a_realm_url() {
        let token = fake_token(json!({ "iss": "https://plain.example.com/oauth" }));
        assert_eq!(peek_jwt_realm(&token), None);
    }

    #[test]
    fn peek_realm_uses_segment_after_last_realms_path() {
        let token = fake_token(json!({ "iss": "https://kc.example/foo/realms/bar" }));
        assert_eq!(peek_jwt_realm(&token), Some("bar".to_string()));
    }
}
