use crate::errors::{PlatformError, PlatformResult};
use crate::secrets::{SecretString, Stored, TENANT_OPS_SECRET};
use crate::AppState;
use serde::Deserialize;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

const ADMIN_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const ADMIN_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

fn admin_http() -> PlatformResult<&'static reqwest::Client> {
    static CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(ADMIN_HTTP_TIMEOUT)
                .connect_timeout(ADMIN_CONNECT_TIMEOUT)
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|e| PlatformError::Internal(format!("Keycloak admin HTTP client: {e}")))
}

#[derive(Debug, Deserialize)]
pub struct KeycloakUser {
    pub id: String,
    pub username: Option<String>,
    pub email: Option<String>,
    #[serde(rename = "firstName")]
    pub first_name: Option<String>,
    #[serde(rename = "lastName")]
    pub last_name: Option<String>,
    pub enabled: Option<bool>,
}

async fn get_admin_token(
    keycloak_url: &str,
    realm: &str,
    client_id: &str,
    client_secret: &str,
) -> PlatformResult<String> {
    let token_url = format!(
        "{}/realms/{}/protocol/openid-connect/token",
        keycloak_url.trim_end_matches('/'),
        realm
    );

    let resp = admin_http()?
        .post(&token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ])
        .send()
        .await
        .map_err(|e| {
            PlatformError::Internal(format!(
                "Failed to connect to Keycloak for admin token: {e}"
            ))
        })?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(PlatformError::Internal(format!(
            "Keycloak admin token request failed: {body}"
        )));
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| {
        PlatformError::Internal(format!("Failed to parse Keycloak token response: {e}"))
    })?;

    body["access_token"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| {
            PlatformError::Internal("Keycloak token response missing access_token".into())
        })
}

pub async fn search_users(
    keycloak_url: &str,
    realm: &str,
    admin_token: &str,
    query: &str,
) -> PlatformResult<Vec<KeycloakUser>> {
    let url = format!(
        "{}/admin/realms/{}/users",
        keycloak_url.trim_end_matches('/'),
        realm
    );

    let resp = admin_http()?
        .get(&url)
        .bearer_auth(admin_token)
        .query(&[("search", query), ("max", "20")])
        .send()
        .await
        .map_err(|e| {
            PlatformError::Internal(format!(
                "Failed to connect to Keycloak for user search: {e}"
            ))
        })?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(PlatformError::Internal(format!(
            "Keycloak user search failed: {body}"
        )));
    }

    resp.json().await.map_err(|e| {
        PlatformError::Internal(format!(
            "Failed to parse Keycloak user search response: {e}"
        ))
    })
}

pub async fn logout_user(
    keycloak_url: &str,
    realm: &str,
    admin_token: &str,
    user_id: &str,
) -> PlatformResult<()> {
    let url = format!(
        "{}/admin/realms/{}/users/{}/logout",
        keycloak_url.trim_end_matches('/'),
        realm,
        user_id
    );

    let resp = admin_http()?
        .post(&url)
        .bearer_auth(admin_token)
        .send()
        .await
        .map_err(|e| {
            PlatformError::Internal(format!(
                "Failed to connect to Keycloak for user logout: {e}"
            ))
        })?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(PlatformError::Internal(format!(
            "Keycloak user logout failed: {body}"
        )));
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsAccess {
    pub realm: String,
    pub client_id: String,
    pub client_secret: SecretString,
}

impl OpsAccess {
    pub async fn of(state: &AppState, tenant_id: Uuid) -> PlatformResult<Self> {
        let client = state.platform_db.get().await?;
        let row = client
            .query_opt(
                "SELECT keycloak_realm, keycloak_ops_client_id, keycloak_ops_client_secret,
                        keycloak_ops_client_secret_enc
                 FROM tenants WHERE id = $1",
                &[&tenant_id],
            )
            .await?
            .ok_or_else(|| PlatformError::NotFound("Tenant not found".into()))?;
        let realm = row.get::<_, Option<String>>(0).ok_or_else(|| {
            PlatformError::BadRequest("Tenant has no Keycloak realm configured".into())
        })?;
        let stored = Stored::new(row.get(2), row.get(3));
        let tenant_secret = state
            .secrets
            .read(
                &client,
                tenant_id,
                &TENANT_OPS_SECRET.aad([tenant_id.to_string()]),
                &stored,
            )
            .await?;
        drop(client);
        let (client_id, client_secret) = resolve_ops_credentials(
            tenant_id,
            row.get(1),
            tenant_secret,
            state.config.default_keycloak_ops_client_id.as_deref(),
            state.config.default_keycloak_ops_client_secret.as_deref(),
        )?;
        Ok(Self {
            realm,
            client_id,
            client_secret,
        })
    }

    pub async fn admin_token(&self, keycloak_url: &str) -> PlatformResult<String> {
        get_admin_token(
            keycloak_url,
            &self.realm,
            &self.client_id,
            self.client_secret.expose(),
        )
        .await
    }
}

fn resolve_ops_credentials(
    tenant_id: Uuid,
    tenant_client_id: Option<String>,
    tenant_client_secret: Option<SecretString>,
    default_client_id: Option<&str>,
    default_client_secret: Option<&str>,
) -> PlatformResult<(String, SecretString)> {
    let used_default_id = tenant_client_id.is_none();
    let id = tenant_client_id.or_else(|| default_client_id.map(str::to_string));
    let secret = tenant_client_secret.or_else(|| default_client_secret.map(SecretString::new));

    let (Some(id), Some(secret)) = (id, secret) else {
        return Err(PlatformError::BadRequest(
            "Keycloak ops credentials not configured. Set CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID + \
             CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET on the platform, or override per-tenant via \
             `cufflink tenants update <slug> --keycloak-ops-client-id <id> --keycloak-ops-client-secret <secret>`."
                .into(),
        ));
    };

    if used_default_id {
        info!(
            tenant_id = %tenant_id,
            "Keycloak ops credentials resolved from platform default (no per-tenant override)"
        );
    }
    Ok((id, secret))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid() -> Uuid {
        Uuid::nil()
    }

    #[test]
    fn resolves_from_tenant_override() {
        let (id, secret) = resolve_ops_credentials(
            tid(),
            Some("tenant-id".into()),
            Some("tenant-secret".into()),
            Some("default-id"),
            Some("default-secret"),
        )
        .unwrap();
        assert_eq!(id, "tenant-id");
        assert_eq!(secret.expose(), "tenant-secret");
    }

    #[test]
    fn falls_back_to_platform_default_when_tenant_unset() {
        let (id, secret) = resolve_ops_credentials(
            tid(),
            None,
            None,
            Some("default-id"),
            Some("default-secret"),
        )
        .unwrap();
        assert_eq!(id, "default-id");
        assert_eq!(secret.expose(), "default-secret");
    }

    #[test]
    fn errors_when_neither_source_supplies_credentials() {
        let err = resolve_ops_credentials(tid(), None, None, None, None).unwrap_err();
        assert!(
            matches!(err, PlatformError::BadRequest(_)),
            "expected BadRequest, got {err:?}"
        );
    }

    #[test]
    fn mixes_tenant_id_with_default_secret() {
        let (id, secret) = resolve_ops_credentials(
            tid(),
            Some("tenant-id".into()),
            None,
            Some("default-id"),
            Some("default-secret"),
        )
        .unwrap();
        assert_eq!(id, "tenant-id");
        assert_eq!(secret.expose(), "default-secret");
    }
}
