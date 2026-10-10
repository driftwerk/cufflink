use crate::auth::PlatformAdmin;
use crate::deployer::validator::validate_tenant_scope;
use crate::errors::{PlatformError, PlatformResult};
use crate::nats_manager::subjects::{Claim, ClaimKind, Namespace};
use crate::registry::RegisteredService;
use crate::repo::nats_claims::ClaimRow;
use crate::repo::tenants::{NewTenant, Tenant, TenantUpdate};
use crate::repo::{api_keys, nats_claims};
use crate::secrets::{SecretAad, TENANT_OPS_SECRET};
use crate::tenant_db::DropOutcome;
use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use cufflink_types::ServiceMode;
use deadpool_postgres::GenericClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct CreateTenantRequest {
    pub name: String,
    pub slug: String,
    pub keycloak_url: Option<String>,
    pub keycloak_realm: Option<String>,
    pub deploy_role: Option<String>,
    pub keycloak_ops_client_id: Option<String>,
    pub keycloak_ops_client_secret: Option<String>,
    pub clone_rbac_from: Option<String>,
    pub service_account_clients: Option<Vec<String>>,
    pub keycloak_issuer: Option<String>,
    pub database_name: Option<String>,
}

#[derive(Serialize)]
pub struct TenantResponse {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub keycloak_url: Option<String>,
    pub keycloak_realm: Option<String>,
    pub deploy_role: String,
    pub service_account_clients: Vec<String>,
    pub keycloak_issuer: Option<String>,
    pub source_tenant_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_name: Option<String>,
    pub created_at: String,
}

impl From<Tenant> for TenantResponse {
    fn from(t: Tenant) -> Self {
        Self {
            id: t.id.to_string(),
            name: t.name,
            slug: t.slug,
            keycloak_url: t.keycloak_url,
            keycloak_realm: t.keycloak_realm,
            deploy_role: t.deploy_role,
            service_account_clients: t.service_account_clients,
            keycloak_issuer: t.keycloak_issuer,
            source_tenant_id: t.source_tenant_id.map(|id| id.to_string()),
            database_name: t.database_name,
            created_at: t.created_at.to_rfc3339(),
        }
    }
}

pub async fn create_tenant(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Json(req): Json<CreateTenantRequest>,
) -> PlatformResult<Json<TenantResponse>> {
    if req.name.is_empty() {
        return Err(PlatformError::Validation("name is required".into()));
    }
    if !cufflink_types::slug::is_valid_tenant_slug(&req.slug) {
        return Err(PlatformError::Validation(format!(
            "slug must match ^[a-z0-9-]{{1,{}}}$",
            cufflink_types::slug::TENANT_SLUG_MAX
        )));
    }

    validate_ops_credentials_pair(
        req.keycloak_ops_client_id.as_deref(),
        req.keycloak_ops_client_secret.as_deref(),
    )?;
    if let Some(clients) = &req.service_account_clients {
        validate_service_account_clients(clients)?;
    }
    if let Some(issuer) = &req.keycloak_issuer {
        validate_issuer(issuer)?;
    }
    if let Some(database) = &req.database_name {
        state
            .tenant_sessions
            .validate_adoption(database)
            .map_err(PlatformError::Validation)?;
        state
            .tenant_sessions
            .verify_adoptable(database)
            .await
            .map_err(|e| PlatformError::Validation(format!("database_name: {e}")))?;
    }

    let source = match &req.clone_rbac_from {
        Some(source_slug) => Some(
            crate::repo::tenants::get_by_slug(&state.platform_db, source_slug)
                .await?
                .ok_or_else(|| {
                    PlatformError::NotFound(format!(
                        "clone_rbac_from tenant '{}' not found",
                        source_slug
                    ))
                })?,
        ),
        None => None,
    };
    let service_account_clients = req
        .service_account_clients
        .as_deref()
        .or(source
            .as_ref()
            .map(|s| s.service_account_clients.as_slice()))
        .unwrap_or_default();
    let keycloak_issuer = req
        .keycloak_issuer
        .as_deref()
        .or(source.as_ref().and_then(|s| s.keycloak_issuer.as_deref()));

    let mut client = state.platform_db.get().await?;
    let txn = client.transaction().await?;
    let tenant = crate::repo::tenants::create_tenant(
        &txn,
        &NewTenant {
            name: &req.name,
            slug: &req.slug,
            keycloak_url: req.keycloak_url.as_deref(),
            keycloak_realm: req.keycloak_realm.as_deref(),
            deploy_role: req.deploy_role.as_deref(),
            keycloak_ops_client_id: req.keycloak_ops_client_id.as_deref(),
            keycloak_ops_client_secret: None,
            service_account_clients,
            keycloak_issuer,
            source_tenant_id: source.as_ref().map(Tenant::group_id),
            database_name: req.database_name.as_deref(),
        },
    )
    .await?;
    require_own_realm(&txn, &tenant).await?;
    if let Some(secret) = &req.keycloak_ops_client_secret {
        let stored = state
            .secrets
            .store_within(&txn, tenant.id, &ops_secret_aad(tenant.id), secret)
            .await?;
        TENANT_OPS_SECRET.write(&txn, tenant.id, &stored).await?;
    }
    if let Some(source) = &source {
        let counts = crate::repo::rbac::clone_tenant_rbac(&txn, source.id, tenant.id).await?;
        let policies_cloned =
            crate::repo::tenant_policies::clone_from(&txn, source.id, tenant.id).await?;
        info!(
            tenant = %tenant.slug,
            source = %source.slug,
            roles_created = counts.roles_created,
            roles_synced = counts.roles_synced,
            assignments_created = counts.assignments_created,
            policies_cloned,
            "Cloning RBAC and policies into new tenant"
        );
    }
    txn.commit().await?;

    if req.keycloak_ops_client_secret.is_some() && state.secrets.mode().stores_plain() {
        warn!(
            tenant = %tenant.slug,
            "Per-tenant Keycloak ops secret supplied — stored in plaintext. Prefer the platform default (CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET) unless this tenant truly needs a different service-account client."
        );
    }

    if tenant.database_name.is_none() {
        if let Err(e) = state
            .tenant_sessions
            .create_database(&state.platform_db, &tenant)
            .await
        {
            tracing::warn!(
                "Failed to create tenant database (may already exist): {}",
                e
            );
        }
    }

    state.tenants.refresh().await;
    info!("Created tenant '{}' (slug={})", tenant.name, tenant.slug);

    Ok(Json(tenant.into()))
}

async fn require_own_realm(txn: &impl GenericClient, tenant: &Tenant) -> PlatformResult<()> {
    let (None, Some(realm)) = (tenant.source_tenant_id, tenant.keycloak_realm.as_deref()) else {
        return Ok(());
    };
    match crate::repo::tenants::sharing_realm(txn, realm, &tenant.deploy_role, tenant.id).await? {
        None => Ok(()),
        Some(other) => Err(PlatformError::Conflict(format!(
            "tenant '{other}' already uses realm '{realm}' with deploy role '{}': a deployer of \
             either would deploy to both; give tenant '{}' a deploy role of its own",
            tenant.deploy_role, tenant.slug
        ))),
    }
}

pub async fn list_tenants(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
) -> PlatformResult<Json<Vec<TenantResponse>>> {
    let tenants = crate::repo::tenants::list_all(&state.platform_db).await?;
    Ok(Json(tenants.into_iter().map(Into::into).collect()))
}

#[derive(Deserialize)]
pub struct UpdateTenantRequest {
    pub name: Option<String>,
    pub keycloak_url: Option<String>,
    pub keycloak_realm: Option<String>,
    pub deploy_role: Option<String>,
    pub keycloak_ops_client_id: Option<String>,
    pub keycloak_ops_client_secret: Option<String>,
    pub service_account_clients: Option<Vec<String>>,
    pub keycloak_issuer: Option<String>,
    pub database_name: Option<String>,
}

pub async fn update_tenant(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(slug): Path<String>,
    Json(req): Json<UpdateTenantRequest>,
) -> PlatformResult<Json<TenantResponse>> {
    if let Some(clients) = &req.service_account_clients {
        validate_service_account_clients(clients)?;
    }
    if let Some(issuer) = req.keycloak_issuer.as_deref().filter(|i| !i.is_empty()) {
        validate_issuer(issuer)?;
    }
    if let Some(requested) = req.database_name.as_deref() {
        let stored = crate::repo::tenants::require_by_slug(&state.platform_db, &slug).await?;
        require_same_database(stored.database_name.as_deref(), requested)?;
    }
    let mut client = state.platform_db.get().await?;
    let txn = client.transaction().await?;
    let tenant = crate::repo::tenants::update_by_slug(
        &txn,
        &slug,
        &TenantUpdate {
            name: req.name.as_deref(),
            keycloak_url: req.keycloak_url.as_deref(),
            keycloak_realm: req.keycloak_realm.as_deref(),
            deploy_role: req.deploy_role.as_deref(),
            keycloak_ops_client_id: req.keycloak_ops_client_id.as_deref(),
            keycloak_ops_client_secret: None,
            service_account_clients: req.service_account_clients.as_deref(),
            keycloak_issuer: req.keycloak_issuer.as_deref(),
        },
    )
    .await?
    .ok_or_else(|| PlatformError::NotFound(format!("Tenant '{}' not found", slug)))?;
    require_own_realm(&txn, &tenant).await?;
    if let Some(secret) = &req.keycloak_ops_client_secret {
        let stored = state
            .secrets
            .store_within(&txn, tenant.id, &ops_secret_aad(tenant.id), secret)
            .await?;
        TENANT_OPS_SECRET.write(&txn, tenant.id, &stored).await?;
    }
    txn.commit().await?;
    drop(client);

    state.tenants.refresh().await;
    info!("Updated tenant '{}'", slug);

    Ok(Json(tenant.into()))
}

pub async fn delete_tenant(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(slug): Path<String>,
) -> PlatformResult<Json<serde_json::Value>> {
    let tenant = crate::repo::tenants::require_by_slug(&state.platform_db, &slug).await?;

    let web_artifacts = crate::repo::services::list_artifacts_for_tenant(
        &state.platform_db,
        tenant.id,
        &ServiceMode::Web,
    )
    .await?;

    let mut leaving = Vec::new();
    let mut native_services = Vec::new();
    let mut service_ids = Vec::new();
    for service in crate::repo::services::list_by_tenant(&state.platform_db, tenant.id).await? {
        leaving.push(crate::deployer::eventing::leave(&state, tenant.id, &slug, service.id).await?);
        service_ids.push(service.id);
        if let Some(ServiceMode::Native) =
            crate::repo::deployments::current_mode(&state.platform_db, service.id).await?
        {
            native_services.push(service);
        }
    }

    let deleted = crate::repo::tenants::delete_by_slug(&state.platform_db, &slug).await?;
    if !deleted {
        return Err(PlatformError::NotFound(format!(
            "Tenant '{}' not found",
            slug
        )));
    }
    for service in state.registry.get_by_tenant(tenant.id).await {
        state.registry.remove(tenant.id, &service.name).await;
    }
    state.tenants.refresh().await;
    for service in leaving {
        service.complete(&state).await;
    }
    state.registry.notify_change().await;

    let undeployed_services: std::collections::HashSet<&str> = web_artifacts
        .iter()
        .map(|w| w.service_name.as_str())
        .collect();
    for service_name in &undeployed_services {
        let subject = format!("undeploy.web.{}.{}", slug, service_name);
        let payload = serde_json::json!({
            "service_name": service_name,
            "tenant_slug": slug,
        });
        if let Err(e) = crate::nats_conn::publish_bounded(
            &state.nats,
            subject.clone(),
            None,
            payload.to_string().into(),
        )
        .await
        {
            warn!(
                "Failed to publish undeploy event on {} for tenant teardown: {}",
                subject, e
            );
        }
    }

    for service in &native_services {
        crate::deployer::native::announce_removal(&state, &slug, service.id, &service.name).await;
    }
    for service_id in service_ids {
        crate::deployer::native::delete_objects(&state, service_id).await;
    }

    if let Some(ref storage) = state.storage {
        for artifact in &web_artifacts {
            if let Err(e) = storage.delete(&artifact.artifact_s3_key).await {
                warn!(
                    "Failed to delete S3 artifact {} for tenant teardown: {}",
                    artifact.artifact_s3_key, e
                );
            }
        }
    }

    let database = state
        .tenant_sessions
        .drop_database(&state.platform_db, &tenant)
        .await
        .inspect_err(|e| warn!(tenant = %slug, "Failed to drop tenant database: {}", e))
        .ok();
    let database_dropped = matches!(database, Some(DropOutcome::Dropped | DropOutcome::Absent));

    let stream_name = crate::nats_manager::subjects::tenant_stream_name(&slug);
    if let Err(e) = state.jetstream.delete_stream(&stream_name).await {
        warn!(tenant = %slug, stream = %stream_name, "Failed to delete tenant stream: {}", e);
    }

    info!(
        tenant = %slug,
        undeployed_services = undeployed_services.len(),
        removed_artifacts = web_artifacts.len(),
        native_services = native_services.len(),
        database_dropped,
        "Deleted tenant"
    );

    Ok(Json(serde_json::json!({
        "message": format!("Tenant '{}' deleted", slug),
        "web_services_undeployed": undeployed_services.len(),
        "web_artifacts_removed": web_artifacts.len(),
        "native_services_undeployed": native_services.len(),
        "database_dropped": database_dropped,
        "database": database.map_or("drop failed", DropOutcome::describe),
    })))
}

fn require_same_database(stored: Option<&str>, requested: &str) -> PlatformResult<()> {
    if stored == Some(requested) {
        Ok(())
    } else {
        Err(PlatformError::Validation(format!(
            "database_name is immutable: the tenant has {}",
            stored.map_or_else(
                || "the database the platform created for it".to_string(),
                |name| format!("'{name}'")
            )
        )))
    }
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct NatsClaims {
    pub subjects: Vec<String>,
    pub streams: Vec<String>,
}

impl NatsClaims {
    fn claims(&self) -> Vec<Claim> {
        let subjects = self.subjects.iter().map(Claim::subject);
        let streams = self.streams.iter().map(Claim::stream);
        subjects.chain(streams).collect()
    }

    fn of(claims: &[Claim]) -> Self {
        let named = |kind| {
            nats_claims::patterns(claims, kind)
                .into_iter()
                .map(str::to_string)
                .collect()
        };
        Self {
            subjects: named(ClaimKind::Subject),
            streams: named(ClaimKind::Stream),
        }
    }
}

fn claims_body(slug: &str, claims: &[Claim]) -> serde_json::Value {
    let NatsClaims { subjects, streams } = NatsClaims::of(claims);
    serde_json::json!({ "tenant": slug, "subjects": subjects, "streams": streams })
}

pub async fn get_nats_claims(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(slug): Path<String>,
) -> PlatformResult<Json<serde_json::Value>> {
    let tenant = crate::repo::tenants::require_by_slug(&state.platform_db, &slug).await?;
    let claims = nats_claims::list_for_tenant(&state.platform_db.get().await?, tenant.id).await?;
    Ok(Json(claims_body(&slug, &claims)))
}

fn stranded_by(
    services: &[Arc<RegisteredService>],
    every_claim: &[ClaimRow],
    tenant: &Tenant,
    claims: &[Claim],
) -> Option<String> {
    let after: Vec<(uuid::Uuid, &str, &Claim)> = every_claim
        .iter()
        .filter(|row| row.tenant_id != tenant.id)
        .map(|row| (row.tenant_id, row.tenant_slug.as_str(), &row.claim))
        .chain(
            claims
                .iter()
                .map(|claim| (tenant.id, tenant.slug.as_str(), claim)),
        )
        .collect();
    services.iter().find_map(|service| {
        let own = after
            .iter()
            .filter(|(owner, _, _)| *owner == service.tenant_id)
            .map(|(_, _, claim)| (*claim).clone());
        let foreign: Vec<(String, String)> = after
            .iter()
            .filter(|(owner, _, claim)| {
                *owner != service.tenant_id && claim.kind == ClaimKind::Subject
            })
            .map(|(_, slug, claim)| (slug.to_string(), claim.pattern.clone()))
            .collect();
        validate_tenant_scope(
            &service.manifest,
            &Namespace::new(&service.tenant_slug, own),
            &foreign,
        )
        .err()
        .map(|refusal| match refusal {
            PlatformError::Validation(reason) => reason,
            other => other.to_string(),
        })
        .map(|reason| {
            format!(
                "service '{}' of tenant '{}' is deployed with what these claims take from it: \
                 {reason}",
                service.name, service.tenant_slug
            )
        })
    })
}

pub async fn put_nats_claims(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path(slug): Path<String>,
    Json(request): Json<NatsClaims>,
) -> PlatformResult<Json<serde_json::Value>> {
    let tenant = crate::repo::tenants::require_by_slug(&state.platform_db, &slug).await?;
    let claims = request.claims();
    let every_claim = nats_claims::list_all(&state.platform_db.get().await?).await?;
    if let Some(stranded) = stranded_by(
        &state.registry.all_services().await,
        &every_claim,
        &tenant,
        &claims,
    ) {
        return Err(PlatformError::Conflict(stranded));
    }
    let stored =
        nats_claims::replace(&state.platform_db, tenant.id, &claims, &admin.subject).await?;
    info!(
        tenant = %slug,
        subjects = claims.iter().filter(|c| c.kind == ClaimKind::Subject).count(),
        streams = claims.iter().filter(|c| c.kind == ClaimKind::Stream).count(),
        actor = %admin.subject,
        "Tenant NATS claims set"
    );
    crate::repo::audit::record(
        &state.platform_db,
        crate::repo::audit::Change {
            tenant_id: Some(tenant.id),
            actor: &admin.subject,
            credential: admin.credential(),
            action: "nats_claims.set",
            resource_type: "tenant",
            resource_id: Some(&slug),
            details: Some(claims_body(&slug, &stored)),
        },
    )
    .await;
    state.tenants.refresh().await;
    Ok(Json(claims_body(&slug, &stored)))
}

#[derive(Deserialize)]
pub struct SyncRbacRequest {
    pub source_tenant: String,
}

pub async fn sync_tenant_rbac(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(slug): Path<String>,
    Json(req): Json<SyncRbacRequest>,
) -> PlatformResult<Json<serde_json::Value>> {
    let target = crate::repo::tenants::require_by_slug(&state.platform_db, &slug).await?;
    let source = crate::repo::tenants::get_by_slug(&state.platform_db, &req.source_tenant)
        .await?
        .ok_or_else(|| {
            PlatformError::NotFound(format!("Source tenant '{}' not found", req.source_tenant))
        })?;

    let mut client = state.platform_db.get().await?;
    let txn = client.transaction().await?;
    let counts = crate::repo::rbac::clone_tenant_rbac(&txn, source.id, target.id).await?;
    txn.commit().await?;
    drop(client);
    crate::auth::permissions::invalidate_tenant(&state, target.id).await;

    info!(
        tenant = %slug,
        source = %req.source_tenant,
        roles_created = counts.roles_created,
        roles_synced = counts.roles_synced,
        assignments_created = counts.assignments_created,
        "Synced RBAC from source tenant"
    );

    Ok(Json(serde_json::json!({
        "roles_created": counts.roles_created,
        "roles_synced": counts.roles_synced,
        "assignments_created": counts.assignments_created,
    })))
}

#[derive(Deserialize)]
pub struct CreatePlatformApiKeyRequest {
    pub name: String,
}

pub async fn create_platform_api_key(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Json(req): Json<CreatePlatformApiKeyRequest>,
) -> PlatformResult<Json<serde_json::Value>> {
    let minted = api_keys::create_platform_key(
        &state.platform_db,
        &req.name,
        state.config.admin_api_key_ttl,
    )
    .await?;
    info!(
        "Created platform admin API key '{}' ({})",
        req.name, minted.id
    );

    Ok(Json(serde_json::json!({
        "id": minted.id.to_string(),
        "name": req.name,
        "key": minted.raw,
        "expires_at": minted.expires_at,
    })))
}

pub async fn list_platform_api_keys(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
) -> PlatformResult<Json<serde_json::Value>> {
    let keys = api_keys::list_platform(&state.platform_db).await?;
    Ok(Json(serde_json::json!({ "keys": keys })))
}

pub async fn revoke_platform_api_key(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(key_id): Path<Uuid>,
) -> PlatformResult<Json<serde_json::Value>> {
    if !api_keys::revoke_platform(&state.platform_db, key_id).await? {
        return Err(PlatformError::NotFound("Platform API key not found".into()));
    }
    info!("Revoked platform admin API key {}", key_id);
    Ok(Json(serde_json::json!({ "revoked": true })))
}

fn ops_secret_aad(tenant_id: Uuid) -> SecretAad {
    TENANT_OPS_SECRET.aad([tenant_id.to_string()])
}

pub(crate) fn validate_ops_credentials_pair(
    client_id: Option<&str>,
    client_secret: Option<&str>,
) -> PlatformResult<()> {
    if client_id.is_some() == client_secret.is_some() {
        return Ok(());
    }
    Err(PlatformError::Validation(
        "keycloak_ops_client_id and keycloak_ops_client_secret must be set together".into(),
    ))
}

fn validate_service_account_clients(clients: &[String]) -> PlatformResult<()> {
    match clients
        .iter()
        .find(|c| c.is_empty() || c.chars().any(|ch| ch.is_whitespace() || ch == ','))
    {
        Some(bad) => Err(PlatformError::Validation(format!(
            "service_account_clients entry '{bad}' must be a non-empty client id without whitespace or commas"
        ))),
        None => Ok(()),
    }
}

fn validate_issuer(issuer: &str) -> PlatformResult<()> {
    let is_base_url = (issuer.starts_with("https://") || issuer.starts_with("http://"))
        && !issuer.contains("/realms/");
    if is_base_url {
        Ok(())
    } else {
        Err(PlatformError::Validation(
            "keycloak_issuer must be an http(s) base URL without a /realms/ path".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_are_read_and_answered_by_kind() {
        let request: NatsClaims = serde_json::from_value(serde_json::json!({
            "subjects": ["books.>", "audit.logs"],
            "streams": ["BOOKS"],
        }))
        .expect("request");
        let claims = request.claims();
        assert_eq!(
            claims,
            [
                Claim::subject("books.>"),
                Claim::subject("audit.logs"),
                Claim::stream("BOOKS")
            ]
        );
        assert_eq!(NatsClaims::of(&claims), request);
        assert_eq!(
            claims_body("harbor", &claims),
            serde_json::json!({
                "tenant": "harbor",
                "subjects": ["books.>", "audit.logs"],
                "streams": ["BOOKS"],
            })
        );
        let none: NatsClaims = serde_json::from_value(serde_json::json!({})).expect("empty");
        assert!(none.claims().is_empty());
        assert!(
            serde_json::from_value::<NatsClaims>(serde_json::json!({"subject": ["books.>"]}))
                .is_err(),
            "a misspelt field is refused, not ignored"
        );
    }

    #[test]
    fn a_tenants_database_is_the_one_it_was_created_with() {
        assert!(require_same_database(Some("harbor"), "harbor").is_ok());
        for (stored, message) in [
            (
                Some("harbor"),
                "database_name is immutable: the tenant has 'harbor'",
            ),
            (
                None,
                "database_name is immutable: the tenant has the database the platform created \
                 for it",
            ),
        ] {
            let refused = require_same_database(stored, "elsewhere").unwrap_err();
            assert!(
                matches!(&refused, PlatformError::Validation(m) if m == message),
                "{refused}"
            );
        }
    }

    #[test]
    fn service_account_clients_are_plain_ids() {
        assert!(validate_service_account_clients(&["orchard-ops".into()]).is_ok());
        assert!(validate_service_account_clients(&[]).is_ok());
        for bad in ["", "a b", "a,b"] {
            assert!(
                validate_service_account_clients(&[bad.into()]).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn issuer_is_a_base_url() {
        assert!(validate_issuer("https://auth.example.com").is_ok());
        assert!(validate_issuer("http://auth.localhost").is_ok());
        assert!(validate_issuer("auth.example.com").is_err());
        assert!(validate_issuer("https://auth.example.com/realms/orchard").is_err());
    }

    #[test]
    fn ops_credentials_pair_accepts_both_set() {
        assert!(validate_ops_credentials_pair(Some("id"), Some("secret")).is_ok());
    }

    #[test]
    fn ops_credentials_pair_accepts_neither_set() {
        assert!(validate_ops_credentials_pair(None, None).is_ok());
    }

    #[test]
    fn ops_credentials_pair_rejects_id_without_secret() {
        let err = validate_ops_credentials_pair(Some("id"), None).unwrap_err();
        assert!(
            matches!(err, PlatformError::Validation(_)),
            "expected Validation error, got {err:?}"
        );
    }

    #[test]
    fn ops_credentials_pair_rejects_secret_without_id() {
        let err = validate_ops_credentials_pair(None, Some("secret")).unwrap_err();
        assert!(
            matches!(err, PlatformError::Validation(_)),
            "expected Validation error, got {err:?}"
        );
    }
}
