use crate::auth::DeployAuth;
use crate::deployer::web;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::services::Status;
use crate::wasm_runtime::sha256_hex;
use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use cufflink_types::ServiceMode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveRequest {
    pub name: String,
}

fn service_id(id: &str) -> PlatformResult<Uuid> {
    id.parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))
}

pub async fn reserve_web_app(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Json(req): Json<ReserveRequest>,
) -> PlatformResult<Json<Value>> {
    let manifest = crate::deployer::ServiceManifest {
        name: req.name,
        mode: ServiceMode::Web,
        ..Default::default()
    };
    crate::deployer::validator::validate_manifest(&manifest)?;
    let pool = &state.platform_db;
    let existing = crate::repo::services::find(pool, user.tenant_id, &manifest.name).await?;
    if let Some(service) = &existing {
        let running = crate::repo::deployments::current_mode(pool, service.id).await?;
        web::require_web(&manifest.name, running.as_ref())?;
    }
    let service = crate::repo::services::find_or_create(
        pool,
        user.tenant_id,
        &manifest.name,
        &manifest.schema(),
        Status::Active,
    )
    .await?;
    Ok(Json(json!({
        "service_id": service.id.to_string(),
        "name": service.name,
        "current_version": service.current_version,
        "created": existing.is_none(),
    })))
}

pub async fn upload_web_bundle(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((id, sha256)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> PlatformResult<Json<Value>> {
    let service_id = service_id(&id)?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
    let stored = async {
        web::require_sha256(&sha256)?;
        let running =
            crate::repo::deployments::current_mode(&state.platform_db, service_id).await?;
        web::require_web(&service.name, running.as_ref())?;
        web::require_content(&sha256, &sha256_hex(&body))?;
        web::store(&state, service_id, &sha256, body, &user.subject).await
    }
    .await;
    let result = stored
        .as_ref()
        .map_or("refused", |(_, stored)| stored.as_str());
    metrics::counter!("cufflink_web_uploads_total", "result" => result).increment(1);
    let (upload, stored) = stored?;

    crate::repo::audit::record(
        &state.platform_db,
        crate::repo::audit::Change {
            tenant_id: Some(user.tenant_id),
            actor: &user.subject,
            credential: user.credential(),
            action: "web.upload",
            resource_type: "service",
            resource_id: Some(&service_id.to_string()),
            details: Some(json!({ "sha256": upload.hash, "size": upload.size })),
        },
    )
    .await;
    info!(
        service = %service.name,
        sha256 = %upload.hash,
        size = upload.size,
        stored = stored.as_str(),
        "Web bundle uploaded"
    );
    Ok(Json(json!({
        "sha256": upload.hash,
        "size": upload.size,
        "s3_key": upload.s3_key,
        "stored": stored.as_str(),
    })))
}

pub async fn upload_web_artifact(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> PlatformResult<Json<Value>> {
    let service_id = service_id(&id)?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
    require_web_version(&state.platform_db, &service).await?;

    let sha256 = sha256_hex(&body);
    let (bundle, _) = web::store(&state, service_id, &sha256, body, &user.subject).await?;

    let lock =
        super::deployments::acquire_deploy_lock(&state, service_id, super::deployments::OPERATION)
            .await?;
    let released = async {
        let current = require_web_version(&state.platform_db, &service).await?;
        let released = web::release_legacy(&state, &current, &bundle).await?;
        web::sweep(&state, service_id).await;
        Ok::<_, PlatformError>(released)
    }
    .await;
    lock.release().await;
    let (deployment, released) = released?;

    if released {
        crate::deployer::module::announce(&state, service_id, None).await;
    }
    let runners_told =
        web::announce(&state, &service.identity(&user.tenant_slug), &deployment).await;

    info!(
        service = %service.name,
        version = deployment.version,
        sha256 = %bundle.hash,
        size = bundle.size,
        released,
        "Web bundle uploaded through the legacy route"
    );

    Ok(Json(json!({
        "service_id": service_id.to_string(),
        "version": deployment.version,
        "artifact_hash": bundle.hash,
        "artifact_s3_key": bundle.s3_key,
        "size_bytes": bundle.size,
        "released": released,
        "runners_told": runners_told,
    })))
}

async fn require_web_version(
    pool: &deadpool_postgres::Pool,
    service: &crate::repo::services::Service,
) -> PlatformResult<crate::repo::deployments::Deployment> {
    let current = crate::repo::deployments::current(pool, service.id).await?;
    match current {
        Some(current) if matches!(current.mode()?, ServiceMode::Web) => Ok(current),
        Some(_) | None => Err(PlatformError::Conflict(format!(
            "service '{}' is not a web app; only a web app's version takes a web artifact",
            service.name
        ))),
    }
}

pub async fn web_status(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
) -> PlatformResult<Json<Value>> {
    let service_id = service_id(&id)?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let row = client
        .query_opt(
            "SELECT artifact_s3_key, artifact_hash, artifact_size
             FROM deployments
             WHERE service_id = $1 AND version = $2 AND manifest->>'mode' = 'web'",
            &[&service_id, &service.current_version],
        )
        .await?;

    let (s3_key, hash, size): (Option<String>, Option<String>, Option<i64>) = match row {
        Some(r) => (r.get(0), r.get(1), r.get(2)),
        None => (None, None, None),
    };

    Ok(Json(json!({
        "service_id": service_id.to_string(),
        "version": service.current_version,
        "artifact_uploaded": s3_key.is_some(),
        "artifact_s3_key": s3_key,
        "artifact_hash": hash,
        "artifact_size": size,
    })))
}
