use crate::auth::{AuthUser, DeployAuth, PlatformAdmin};
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::api_keys;
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
}

#[derive(Serialize)]
pub struct CreateApiKeyResponse {
    pub id: String,
    pub name: String,
    pub key: String,
    pub tenant_slug: String,
    pub expires_at: Option<DateTime<Utc>>,
}

pub async fn create_api_key(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Json(req): Json<CreateApiKeyRequest>,
) -> PlatformResult<Json<CreateApiKeyResponse>> {
    if req.name.is_empty() {
        return Err(PlatformError::Validation(
            "API key name cannot be empty".into(),
        ));
    }

    let minted = api_keys::create_tenant_key(
        &state.platform_db,
        user.tenant_id,
        &req.name,
        None,
        state.config.api_key_ttl,
    )
    .await?;
    info!(
        tenant = %user.tenant_slug,
        key_id = %minted.id,
        created_by = %user.subject,
        "Created tenant API key '{}'",
        req.name
    );
    audit_key(
        &state,
        &user,
        "api_key.create",
        minted.id,
        json!({"name": req.name}),
    )
    .await;

    Ok(Json(CreateApiKeyResponse {
        id: minted.id.to_string(),
        name: req.name,
        key: minted.raw,
        tenant_slug: user.tenant_slug,
        expires_at: minted.expires_at,
    }))
}

pub async fn list_api_keys(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
) -> PlatformResult<Json<Value>> {
    let keys = api_keys::list_for_tenant(&state.platform_db, user.tenant_id).await?;
    Ok(Json(json!({ "keys": keys })))
}

pub async fn revoke_api_key(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    if !api_keys::revoke(&state.platform_db, user.tenant_id, id).await? {
        return Err(PlatformError::NotFound("API key not found".into()));
    }
    metrics::counter!("cufflink_api_keys_revoked_total", "reason" => "explicit").increment(1);
    info!(tenant = %user.tenant_slug, key_id = %id, revoked_by = %user.subject, "Revoked tenant API key");
    audit_key(&state, &user, "api_key.revoke", id, json!({})).await;
    Ok(Json(json!({ "revoked": true })))
}

#[derive(Debug, Default, Deserialize)]
pub struct RotateQuery {
    pub grace_hours: Option<i64>,
}

const DEFAULT_GRACE_HOURS: i64 = 24;
const MAX_GRACE_HOURS: i64 = 168;

impl RotateQuery {
    fn grace(&self) -> PlatformResult<chrono::Duration> {
        match self.grace_hours.unwrap_or(DEFAULT_GRACE_HOURS) {
            hours @ 0..=MAX_GRACE_HOURS => Ok(chrono::Duration::hours(hours)),
            hours => Err(PlatformError::Validation(format!(
                "grace_hours must be between 0 and {MAX_GRACE_HOURS}, not {hours}"
            ))),
        }
    }
}

#[derive(Serialize)]
pub struct RotatedApiKey {
    pub id: Uuid,
    pub key: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub replaces: Uuid,
    pub replaced_key_expires_at: DateTime<Utc>,
}

async fn rotate(
    state: &AppState,
    owner: api_keys::KeyOwner,
    id: Uuid,
    ttl: Option<chrono::Duration>,
    query: &RotateQuery,
) -> PlatformResult<RotatedApiKey> {
    let grace = query.grace()?;
    let api_keys::Rotated {
        minted,
        retired_until,
    } = api_keys::rotate(&state.platform_db, owner, id, ttl, grace)
        .await?
        .ok_or_else(|| PlatformError::NotFound("API key not found".into()))?;
    Ok(RotatedApiKey {
        id: minted.id,
        key: minted.raw,
        expires_at: minted.expires_at,
        replaces: id,
        replaced_key_expires_at: retired_until,
    })
}

async fn audit_key(state: &AppState, user: &AuthUser, action: &str, id: Uuid, details: Value) {
    crate::repo::audit::record(
        &state.platform_db,
        crate::repo::audit::Change {
            tenant_id: Some(user.tenant_id),
            actor: crate::audit::subject(user),
            credential: user.credential(),
            action,
            resource_type: "api_key",
            resource_id: Some(&id.to_string()),
            details: Some(details),
        },
    )
    .await;
}

async fn audit_rotation(
    state: &AppState,
    tenant_id: Option<Uuid>,
    actor: &str,
    rotated: &RotatedApiKey,
) {
    let recorded = async {
        let client = state.platform_db.get().await?;
        crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
            client: &client,
            tenant_id,
            actor,
            action: "api_key.rotate",
            resource_type: "api_key",
            resource_id: Some(&rotated.replaces.to_string()),
            details: Some(json!({"new_key_id": rotated.id})),
            ip_address: None,
        })
        .await
    }
    .await;
    if let Err(e) = recorded {
        tracing::warn!(key_id = %rotated.replaces, error = %e, "Could not audit an API key rotation");
    }
}

pub async fn rotate_api_key(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<Uuid>,
    Query(query): Query<RotateQuery>,
) -> PlatformResult<Json<RotatedApiKey>> {
    let rotated = rotate(
        &state,
        api_keys::KeyOwner::Tenant(user.tenant_id),
        id,
        Some(state.config.api_key_ttl),
        &query,
    )
    .await?;
    info!(tenant = %user.tenant_slug, key_id = %id, new_key_id = %rotated.id, rotated_by = %user.subject, "Rotated tenant API key");
    audit_rotation(&state, Some(user.tenant_id), &user.subject, &rotated).await;
    Ok(Json(rotated))
}

pub async fn rotate_platform_api_key(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path(id): Path<Uuid>,
    Query(query): Query<RotateQuery>,
) -> PlatformResult<Json<RotatedApiKey>> {
    let rotated = rotate(
        &state,
        api_keys::KeyOwner::Platform,
        id,
        state.config.admin_api_key_ttl,
        &query,
    )
    .await?;
    info!(key_id = %id, new_key_id = %rotated.id, rotated_by = %admin.subject, "Rotated platform admin API key");
    audit_rotation(&state, None, &admin.subject, &rotated).await;
    Ok(Json(rotated))
}

pub async fn exchange_token(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ExchangeTokenRequest>,
) -> PlatformResult<Json<ExchangeTokenResponse>> {
    let slug = req
        .tenant_slug
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| PlatformError::Validation("tenant_slug is required".into()))?;

    let tenant = crate::repo::tenants::get_by_slug(&state.platform_db, slug)
        .await?
        .ok_or_else(|| PlatformError::BadRequest(format!("Tenant '{}' not found.", slug)))?;

    let tenant_kc_url = tenant.keycloak_url.as_deref().ok_or_else(|| {
        PlatformError::BadRequest(format!(
            "Tenant '{}' is not configured for login. Ask a platform admin to set keycloak_url.",
            slug
        ))
    })?;
    let tenant_kc_realm = tenant.keycloak_realm.as_deref().ok_or_else(|| {
        PlatformError::BadRequest(format!(
            "Tenant '{}' is not configured for login. Ask a platform admin to set keycloak_realm.",
            slug
        ))
    })?;

    let token_info = state
        .tenant_keycloak_auths
        .validate(
            tenant_kc_url,
            tenant_kc_realm,
            crate::auth::issuer::issuer_base(&state.config, &tenant),
            &req.access_token,
        )
        .await
        .map_err(|e| PlatformError::Unauthorized(format!("Invalid token: {}", e)))?;

    if !token_info.realm_roles.contains(&tenant.deploy_role) {
        return Err(PlatformError::Forbidden(format!(
            "You need the '{}' role in the '{}' realm to access this tenant. Contact your admin.",
            tenant.deploy_role, tenant_kc_realm
        )));
    }

    let username = token_info
        .preferred_username
        .clone()
        .unwrap_or_else(|| token_info.sub.clone());
    let minted = api_keys::create_tenant_key(
        &state.platform_db,
        tenant.id,
        &format!("cli-{}", username),
        Some(&token_info.sub),
        state.config.api_key_ttl,
    )
    .await?;

    info!(
        "Created API key for user {} in tenant {}",
        username, tenant.slug
    );

    Ok(Json(ExchangeTokenResponse {
        api_key: minted.raw,
        tenant_slug: tenant.slug,
        username,
        expires_at: minted.expires_at,
    }))
}

pub async fn auth_config(State(state): State<Arc<AppState>>) -> Json<AuthConfigResponse> {
    let keycloak_url = state
        .config
        .keycloak_issuer
        .as_deref()
        .unwrap_or(&state.config.keycloak_url);

    Json(AuthConfigResponse {
        keycloak_url: keycloak_url.to_string(),
        realm: state.config.keycloak_realm.clone(),
        cli_client_id: "cufflink-cli".to_string(),
        web_client_id: "cufflink-web".to_string(),
    })
}

pub async fn me(
    State(state): State<Arc<AppState>>,
    auth: crate::auth::OptionalAuth,
    headers: axum::http::HeaderMap,
) -> PlatformResult<Json<MeResponse>> {
    if let Some(user) = auth.0 {
        return Ok(Json(MeResponse {
            subject: user.subject,
            name: user.name,
            email: user.email,
            tenant_id: Some(user.tenant_id.to_string()),
            tenant_slug: Some(user.tenant_slug),
            auth_method: user.auth_method.name().to_string(),
            is_platform_admin: user.is_platform_admin,
        }));
    }

    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .ok_or_else(|| PlatformError::Unauthorized("Missing Authorization header".into()))?;

    let jwt_realm = crate::auth::peek_jwt_realm(token)
        .ok_or_else(|| PlatformError::Unauthorized("JWT is not from a Keycloak realm".into()))?;
    if jwt_realm != state.config.keycloak_realm {
        return Err(PlatformError::Unauthorized(format!(
            "No tenant configured for realm '{}'",
            jwt_realm
        )));
    }

    let auth = state
        .keycloak_auth
        .as_ref()
        .ok_or_else(|| PlatformError::Internal("Keycloak auth not configured".into()))?;

    let token_info = auth
        .validate_token(token)
        .await
        .map_err(|e| PlatformError::Unauthorized(format!("Invalid token: {}", e)))?;

    crate::auth::extractor::require_platform_admin_role(
        &token_info,
        &state.config.platform_admin_role,
    )?;

    Ok(Json(MeResponse {
        subject: token_info.sub,
        name: token_info.name,
        email: token_info.email,
        tenant_id: None,
        tenant_slug: None,
        auth_method: "Jwt".to_string(),
        is_platform_admin: true,
    }))
}

#[derive(Deserialize)]
pub struct ExchangeTokenRequest {
    pub access_token: String,
    pub tenant_slug: Option<String>,
}

#[derive(Serialize)]
pub struct ExchangeTokenResponse {
    pub api_key: String,
    pub tenant_slug: String,
    pub username: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct AuthConfigResponse {
    pub keycloak_url: String,
    pub realm: String,
    pub cli_client_id: String,
    pub web_client_id: String,
}

#[derive(Serialize)]
pub struct MeResponse {
    pub subject: String,
    pub name: Option<String>,
    pub email: Option<String>,
    pub tenant_id: Option<String>,
    pub tenant_slug: Option<String>,
    pub auth_method: String,
    pub is_platform_admin: bool,
}

pub async fn logout(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
) -> PlatformResult<axum::http::StatusCode> {
    let revoked =
        api_keys::revoke_for_subject(&state.platform_db, user.tenant_id, &user.subject).await?;
    if revoked > 0 {
        metrics::counter!("cufflink_api_keys_revoked_total", "reason" => "logout")
            .increment(revoked);
    }

    let access = crate::auth::keycloak_admin::OpsAccess::of(&state, user.tenant_id).await?;
    let admin_token = access.admin_token(&state.config.keycloak_url).await?;
    let realm = access.realm;

    crate::auth::keycloak_admin::logout_user(
        &state.config.keycloak_url,
        &realm,
        &admin_token,
        &user.subject,
    )
    .await?;

    info!(
        tenant_id = %user.tenant_id,
        user_id = %user.subject,
        revoked_api_keys = revoked,
        "Terminated Keycloak SSO session"
    );

    Ok(axum::http::StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grace_defaults_to_a_day_and_is_bounded_by_a_week() {
        let grace = |hours| RotateQuery { grace_hours: hours }.grace();
        assert_eq!(grace(None).expect("default"), chrono::Duration::hours(24));
        assert_eq!(grace(Some(0)).expect("none"), chrono::Duration::zero());
        assert_eq!(
            grace(Some(168)).expect("week"),
            chrono::Duration::hours(168)
        );
        for hours in [-1, 169] {
            assert!(matches!(
                grace(Some(hours)),
                Err(PlatformError::Validation(_))
            ));
        }
    }
}
