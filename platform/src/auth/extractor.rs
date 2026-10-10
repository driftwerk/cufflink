use super::{audience, issuer, service_accounts};
use crate::errors::PlatformError;
use crate::repo::api_keys;
use crate::AppState;
use axum::extract::{ConnectInfo, MatchedPath};
use axum::{extract::FromRequestParts, http::request::Parts, http::HeaderMap};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

pub(crate) fn tenant_slug_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-tenant-slug")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub subject: String,
    pub name: Option<String>,
    pub email: Option<String>,
    pub tenant_id: Uuid,
    pub tenant_slug: String,
    pub auth_method: AuthMethod,
    pub token_info: Option<super::keycloak::TokenInfo>,
    pub is_service_account: bool,
    pub is_platform_admin: bool,
    pub has_deploy_authority: bool,
    pub realm: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    Jwt,
    ApiKey { id: Uuid, user_sub: Option<String> },
}

impl AuthMethod {
    pub fn name(&self) -> &'static str {
        match self {
            AuthMethod::Jwt => "Jwt",
            AuthMethod::ApiKey { .. } => "ApiKey",
        }
    }
}

impl AuthUser {
    pub fn to_auth_json(&self) -> serde_json::Value {
        match &self.token_info {
            Some(info) => json!({
                "sub": info.sub,
                "preferred_username": info.preferred_username,
                "name": info.name,
                "email": info.email,
                "realm_roles": info.realm_roles,
                "claims": info.claims,
                "is_service_account": self.is_service_account,
            }),
            None => {
                json!({
                    "sub": self.subject,
                    "name": self.name,
                    "email": self.email,
                    "realm_roles": [],
                    "claims": {},
                    "is_service_account": false,
                })
            }
        }
    }
}

pub(crate) enum AuthOutcome {
    Absent,
    Present(Result<Box<AuthUser>, PlatformError>),
}

pub(crate) async fn authenticate(headers: &HeaderMap, state: &Arc<AppState>) -> AuthOutcome {
    let Some(auth_header) = headers.get("authorization").and_then(|v| v.to_str().ok()) else {
        return AuthOutcome::Absent;
    };
    AuthOutcome::Present(
        authenticate_header(auth_header, headers, state)
            .await
            .map(Box::new),
    )
}

async fn authenticate_header(
    auth_header: &str,
    headers: &HeaderMap,
    state: &Arc<AppState>,
) -> Result<AuthUser, PlatformError> {
    if let Some(token) = auth_header.strip_prefix("Bearer ") {
        jwt_principal(token, headers, state).await
    } else if let Some(key) = auth_header.strip_prefix("ApiKey ") {
        api_key_principal(key, headers, state).await
    } else {
        Err(PlatformError::Unauthorized(
            "Authorization header must start with 'Bearer ' or 'ApiKey '".into(),
        ))
    }
}

async fn jwt_principal(
    token: &str,
    headers: &HeaderMap,
    state: &Arc<AppState>,
) -> Result<AuthUser, PlatformError> {
    let jwt_realm = crate::auth::peek_jwt_realm(token)
        .ok_or_else(|| PlatformError::Unauthorized("JWT issuer is not a Keycloak realm".into()))?;

    let slug_hint = tenant_slug_header(headers);
    let resolved =
        crate::repo::tenants::resolve_for_jwt(&state.platform_db, &jwt_realm, slug_hint).await?;

    if let Some(tenant) = resolved {
        let realm = tenant.keycloak_realm.clone().ok_or_else(|| {
            PlatformError::Internal(format!("tenant '{}' resolved without a realm", tenant.slug))
        })?;
        let token_info = state
            .tenant_keycloak_auths
            .validate(
                &state.config.keycloak_url,
                &realm,
                issuer::issuer_base(&state.config, &tenant),
                token,
            )
            .await
            .map_err(|e| {
                debug!("JWT validation failed: {}", e);
                PlatformError::Unauthorized(format!("Invalid token: {}", e))
            })?;

        let entry = state
            .tenants
            .get_or_load(tenant.id)
            .await?
            .ok_or_else(|| PlatformError::Unauthorized("Tenant no longer exists".into()))?;
        audience::check(&entry, &token_info)?;
        let is_service_account =
            service_accounts::resolve(&token_info, &entry, &state.config.service_account_clients);

        let has_deploy_authority = token_info.realm_roles.contains(&tenant.deploy_role);

        Ok(AuthUser {
            subject: token_info.sub.clone(),
            name: token_info.name.clone(),
            email: token_info.email.clone(),
            tenant_id: tenant.id,
            tenant_slug: tenant.slug,
            auth_method: AuthMethod::Jwt,
            token_info: Some(token_info),
            is_service_account,
            is_platform_admin: false,
            has_deploy_authority,
            realm: Some(realm),
        })
    } else if jwt_realm == state.config.keycloak_realm {
        let auth = state
            .keycloak_auth
            .as_ref()
            .ok_or_else(|| PlatformError::Internal("Keycloak auth not configured".into()))?;

        let token_info = auth.validate_token(token).await.map_err(|e| {
            debug!("Platform admin JWT validation failed: {}", e);
            PlatformError::Unauthorized(format!("Invalid platform token: {}", e))
        })?;

        require_platform_admin_role(&token_info, &state.config.platform_admin_role)?;

        let tenant = admin_tenant(
            state,
            headers,
            "Platform admins must specify X-Tenant-Slug header for tenant-scoped endpoints",
        )
        .await?;

        Ok(AuthUser {
            subject: token_info.sub.clone(),
            name: token_info.name.clone(),
            email: token_info.email.clone(),
            tenant_id: tenant.id,
            tenant_slug: tenant.slug,
            auth_method: AuthMethod::Jwt,
            token_info: Some(token_info),
            is_service_account: false,
            is_platform_admin: true,
            has_deploy_authority: true,
            realm: Some(state.config.keycloak_realm.clone()),
        })
    } else {
        debug!("No tenant found for realm '{}'", jwt_realm);
        Err(PlatformError::Unauthorized(format!(
            "No tenant configured for realm '{}'",
            jwt_realm
        )))
    }
}

async fn admin_tenant(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    missing: &str,
) -> Result<crate::repo::tenants::Tenant, PlatformError> {
    let tenant_slug = tenant_slug_header(headers)
        .ok_or_else(|| PlatformError::BadRequest(missing.to_string()))?;
    crate::repo::tenants::require_by_slug(&state.platform_db, tenant_slug).await
}

async fn api_key_principal(
    key: &str,
    headers: &HeaderMap,
    state: &Arc<AppState>,
) -> Result<AuthUser, PlatformError> {
    let key = api_keys::find(&state.platform_db, &api_keys::hash(key))
        .await?
        .active()?;

    if key.is_platform_admin {
        let tenant = admin_tenant(
            state,
            headers,
            "Platform admin API keys must specify X-Tenant-Slug header",
        )
        .await?;
        return Ok(AuthUser {
            subject: format!("apikey:{}", key.name),
            name: Some(key.name),
            email: None,
            tenant_id: tenant.id,
            tenant_slug: tenant.slug,
            auth_method: AuthMethod::ApiKey {
                id: key.id,
                user_sub: key.user_sub,
            },
            token_info: None,
            is_service_account: false,
            is_platform_admin: true,
            has_deploy_authority: true,
            realm: None,
        });
    }

    let tenant_id = key
        .tenant_id
        .ok_or_else(|| PlatformError::Unauthorized("API key has no tenant".into()))?;
    let tenant = state
        .tenants
        .get_or_load(tenant_id)
        .await?
        .ok_or_else(|| PlatformError::Unauthorized("API key tenant not found".into()))?;

    Ok(AuthUser {
        subject: tenant.tenant.keycloak_user_id.clone(),
        name: Some(key.name),
        email: None,
        tenant_id,
        tenant_slug: tenant.tenant.slug.clone(),
        auth_method: AuthMethod::ApiKey {
            id: key.id,
            user_sub: key.user_sub,
        },
        token_info: None,
        is_service_account: false,
        is_platform_admin: false,
        has_deploy_authority: true,
        realm: None,
    })
}

impl FromRequestParts<Arc<AppState>> for AuthUser {
    type Rejection = PlatformError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        match authenticate(&parts.headers, state).await {
            AuthOutcome::Absent => Err(PlatformError::Unauthorized(
                "Missing Authorization header".into(),
            )),
            AuthOutcome::Present(result) => result.map(|u| *u),
        }
    }
}

pub struct OptionalAuth(pub Option<AuthUser>);

impl FromRequestParts<Arc<AppState>> for OptionalAuth {
    type Rejection = PlatformError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        match authenticate(&parts.headers, state).await {
            AuthOutcome::Absent => Ok(OptionalAuth(None)),
            AuthOutcome::Present(Ok(user)) => Ok(OptionalAuth(Some(*user))),
            AuthOutcome::Present(Err(e)) => Err(e),
        }
    }
}

pub struct DeployAuth(pub AuthUser);

fn has_control_plane_authority(user: &AuthUser) -> bool {
    user.is_platform_admin || user.is_service_account || user.has_deploy_authority
}

pub(crate) async fn require_permission_or_deploy(
    state: &Arc<AppState>,
    user: &AuthUser,
    area: &str,
    operation: &str,
) -> Result<(), PlatformError> {
    if has_control_plane_authority(user) {
        return Ok(());
    }
    let resolved =
        super::permissions::resolve_permissions_cached(state, user.tenant_id, &user.subject)
            .await?;
    crate::router::authz::permission_decision(area, operation, &resolved)
}

fn rbac_admin_authorized(has_deploy_authority: bool, permissions: &[String]) -> bool {
    has_deploy_authority || super::grants::has_permission(permissions, "roles", "manage")
}

impl FromRequestParts<Arc<AppState>> for DeployAuth {
    type Rejection = PlatformError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        AuthUser::from_request_parts(parts, state)
            .await
            .and_then(require_deploy_authority)
            .map(DeployAuth)
    }
}

pub(crate) fn require_deploy_authority(user: AuthUser) -> Result<AuthUser, PlatformError> {
    match has_control_plane_authority(&user) {
        true => Ok(user),
        false => Err(PlatformError::Forbidden(
            "This operation requires deploy authority (the tenant's deploy role).".into(),
        )),
    }
}

pub struct RbacAdmin(pub AuthUser);

impl FromRequestParts<Arc<AppState>> for RbacAdmin {
    type Rejection = PlatformError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let user = AuthUser::from_request_parts(parts, state).await?;
        let permissions = if user.has_deploy_authority {
            Vec::new()
        } else {
            super::permissions::resolve_permissions(
                &state.platform_db,
                user.tenant_id,
                &user.subject,
            )
            .await?
            .permissions
        };
        if rbac_admin_authorized(user.has_deploy_authority, &permissions) {
            Ok(RbacAdmin(user))
        } else {
            Err(PlatformError::Forbidden(
                "Managing roles requires the 'roles:manage' permission.".into(),
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlatformAdmin {
    pub subject: String,
    pub name: Option<String>,
    pub email: Option<String>,
    pub key_id: Option<Uuid>,
}

impl PlatformAdmin {
    pub fn credential(&self) -> crate::repo::audit::Credential {
        match (self.key_id, &self.name) {
            (Some(id), Some(name)) => crate::repo::audit::Credential::Key {
                id,
                name: name.clone(),
            },
            _ => crate::repo::audit::Credential::Token,
        }
    }
}

impl AuthUser {
    pub fn credential(&self) -> crate::repo::audit::Credential {
        match &self.auth_method {
            AuthMethod::ApiKey { id, .. } => crate::repo::audit::Credential::Key {
                id: *id,
                name: self.name.clone().unwrap_or_default(),
            },
            AuthMethod::Jwt => crate::repo::audit::Credential::Token,
        }
    }
}

pub(crate) fn has_platform_admin_role(token: &super::keycloak::TokenInfo, role: &str) -> bool {
    token.realm_roles.iter().any(|r| r == role)
}

pub(crate) fn require_platform_admin_role(
    token: &super::keycloak::TokenInfo,
    role: &str,
) -> Result<(), PlatformError> {
    if has_platform_admin_role(token, role) {
        Ok(())
    } else {
        Err(PlatformError::Forbidden(format!(
            "You need the '{role}' role in the platform realm."
        )))
    }
}

async fn platform_admin(
    parts: &Parts,
    state: &Arc<AppState>,
) -> Result<PlatformAdmin, PlatformError> {
    let auth_header = parts
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| PlatformError::Unauthorized("Missing Authorization header".into()))?;

    if let Some(key) = auth_header.strip_prefix("ApiKey ") {
        let key = api_keys::find(&state.platform_db, &api_keys::hash(key))
            .await?
            .active()?;
        if !key.is_platform_admin {
            return Err(PlatformError::Unauthorized(
                "Invalid or non-admin API key".into(),
            ));
        }
        return Ok(PlatformAdmin {
            subject: format!("apikey:{}", key.name),
            name: Some(key.name),
            email: None,
            key_id: Some(key.id),
        });
    }

    let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
        PlatformError::Unauthorized("Platform admin requires JWT or admin API key".into())
    })?;

    let auth = state
        .keycloak_auth
        .as_ref()
        .ok_or_else(|| PlatformError::Internal("Keycloak auth not configured".into()))?;

    let token_info = auth.validate_token(token).await.map_err(|e| {
        debug!("Platform admin JWT validation failed: {}", e);
        PlatformError::Unauthorized(format!("Invalid platform token: {}", e))
    })?;

    require_platform_admin_role(&token_info, &state.config.platform_admin_role)?;

    Ok(PlatformAdmin {
        subject: token_info.sub,
        name: token_info.name,
        email: token_info.email,
        key_id: None,
    })
}

async fn audit_platform_admin(
    state: &Arc<AppState>,
    parts: &Parts,
    admin: &PlatformAdmin,
) -> Result<(), PlatformError> {
    let route = parts
        .extensions
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_string();
    let action = format!("{} {}", parts.method, parts.uri.path());
    let peer = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip());
    let ip = crate::middleware::rate_limit::client_ip(
        &parts.headers,
        peer,
        state.config.client_ip_source,
    )
    .map(|ip| ip.to_string());
    let client = state.platform_db.get().await?;
    crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
        client: &client,
        tenant_id: None,
        actor: &admin.subject,
        action: &action,
        resource_type: "platform_admin",
        resource_id: None,
        details: None,
        ip_address: ip.as_deref(),
    })
    .await?;
    metrics::counter!("cufflink_platform_admin_calls_total", "route" => route).increment(1);
    tracing::info!(target: "cufflink::audit", actor = %admin.subject, action = %action, "Platform admin call");
    Ok(())
}

impl FromRequestParts<Arc<AppState>> for PlatformAdmin {
    type Rejection = PlatformError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let admin = platform_admin(parts, state).await?;
        audit_platform_admin(state, parts, &admin).await?;
        Ok(admin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::keycloak::TokenInfo;

    fn auth_user(is_platform_admin: bool, is_service_account: bool, has_deploy: bool) -> AuthUser {
        AuthUser {
            subject: "u".into(),
            name: None,
            email: None,
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "default".into(),
            auth_method: AuthMethod::Jwt,
            token_info: None,
            is_service_account,
            is_platform_admin,
            has_deploy_authority: has_deploy,
            realm: None,
        }
    }

    #[test]
    fn control_plane_allows_deploy_authority_admin_and_service_account() {
        assert!(has_control_plane_authority(&auth_user(false, false, true)));
        assert!(has_control_plane_authority(&auth_user(true, false, false)));
        assert!(has_control_plane_authority(&auth_user(false, true, false)));
    }

    #[test]
    fn control_plane_denies_plain_authenticated_user() {
        assert!(!has_control_plane_authority(&auth_user(
            false, false, false
        )));
    }

    #[test]
    fn rbac_admin_via_roles_manage_permission_not_deploy() {
        assert!(rbac_admin_authorized(false, &["roles:manage".into()]));
        assert!(rbac_admin_authorized(false, &["roles:*".into()]));
        assert!(rbac_admin_authorized(false, &["*".into()]));
    }

    #[test]
    fn rbac_admin_allows_deploy_bootstrap() {
        assert!(rbac_admin_authorized(true, &[]));
    }

    #[test]
    fn rbac_admin_denies_non_admin() {
        assert!(!rbac_admin_authorized(
            false,
            &["items:view".into(), "customers:view".into()]
        ));
        assert!(!rbac_admin_authorized(false, &[]));
    }

    #[test]
    fn platform_admin_requires_role() {
        let token = |roles: &[&str]| TokenInfo {
            sub: "ops".into(),
            preferred_username: None,
            name: None,
            email: None,
            realm_roles: roles.iter().map(|r| r.to_string()).collect(),
            claims: Default::default(),
        };
        assert!(has_platform_admin_role(
            &token(&["cufflink-deploy", "cufflink-platform-admin"]),
            "cufflink-platform-admin"
        ));
        assert!(!has_platform_admin_role(
            &token(&["cufflink-deploy"]),
            "cufflink-platform-admin"
        ));
        assert!(!has_platform_admin_role(
            &token(&[]),
            "cufflink-platform-admin"
        ));
    }

    #[test]
    fn tenant_slug_header_treats_blank_as_absent() {
        let mut headers = HeaderMap::new();
        assert_eq!(tenant_slug_header(&headers), None);
        headers.insert("x-tenant-slug", " ".parse().expect("header"));
        assert_eq!(tenant_slug_header(&headers), None);
        headers.insert("x-tenant-slug", " preview-pr-7 ".parse().expect("header"));
        assert_eq!(tenant_slug_header(&headers), Some("preview-pr-7"));
    }
}
