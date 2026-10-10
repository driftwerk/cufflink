use crate::auth::DeployAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use cufflink_types::ServiceMode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

pub(crate) fn artifact_key(service_id: Uuid, version: i32) -> String {
    format!("web/{service_id}/v{version}.tar.gz")
}

pub async fn upload_web_artifact(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let storage = state
        .storage
        .as_ref()
        .ok_or_else(|| PlatformError::Internal("S3 storage not configured".into()))?;

    let service = crate::repo::services::get_by_id(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;
    require_web_app(&state.platform_db, &service).await?;

    let mut hasher = Sha256::new();
    hasher.update(&body);
    let artifact_hash = hex::encode(hasher.finalize());

    let artifact_size = body.len() as i64;
    let s3_key = artifact_key(service_id, service.current_version);

    storage
        .upload(&s3_key, body.to_vec(), "application/gzip")
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to upload artifact: {}", e)))?;

    let client = state.platform_db.get().await?;
    client
        .execute(
            "UPDATE deployments SET artifact_s3_key = $1, artifact_hash = $2, artifact_size = $3
             WHERE service_id = $4 AND version = $5 AND manifest->>'mode' = 'web'",
            &[
                &s3_key,
                &artifact_hash,
                &artifact_size,
                &service_id,
                &service.current_version,
            ],
        )
        .await?;

    let event_payload = json!({
        "service_id": service_id.to_string(),
        "service_name": service.name,
        "version": service.current_version,
        "s3_key": s3_key,
        "artifact_hash": artifact_hash,
        "tenant_slug": user.tenant_slug,
    });
    let subject = format!("deploy.web.{}.{}", user.tenant_slug, service.name);
    if let Err(e) = crate::nats_conn::publish_bounded(
        &state.nats,
        subject,
        None,
        event_payload.to_string().into(),
    )
    .await
    {
        tracing::warn!("Failed to publish web deploy event: {}", e);
    }

    info!(
        "Web artifact uploaded for {} v{} (hash: {}, size: {} bytes)",
        service.name,
        service.current_version,
        &artifact_hash[..16],
        artifact_size
    );

    Ok(Json(json!({
        "service_id": service_id.to_string(),
        "version": service.current_version,
        "artifact_hash": artifact_hash,
        "artifact_s3_key": s3_key,
        "size_bytes": artifact_size,
    })))
}

async fn require_web_app(
    pool: &deadpool_postgres::Pool,
    service: &crate::repo::services::Service,
) -> PlatformResult<()> {
    match crate::repo::deployments::current_mode(pool, service.id).await? {
        Some(ServiceMode::Web) => Ok(()),
        Some(
            ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Native,
        )
        | None => Err(PlatformError::Conflict(format!(
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
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let service = crate::repo::services::get_by_id(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;

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
