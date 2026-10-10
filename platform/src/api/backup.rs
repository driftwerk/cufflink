use crate::auth::DeployAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub fn export_key(service_id: Uuid, job_id: Uuid) -> String {
    format!("backups/{service_id}/{job_id}/export.jsonl")
}

pub fn restore_key(service_id: Uuid, upload_id: Uuid) -> String {
    format!("backups/{service_id}/restore-{upload_id}.jsonl")
}

pub fn backup_source_service(s3_key: &str) -> Option<Uuid> {
    let rest = s3_key.strip_prefix("backups/")?;
    let (service, tail) = rest.split_once('/')?;
    let traverses = tail
        .split('/')
        .any(|segment| segment == ".." || segment.is_empty());
    (!traverses).then(|| service.parse().ok()).flatten()
}

async fn authorize_restore_source(
    state: &AppState,
    user: &crate::auth::AuthUser,
    target: Uuid,
    s3_key: &str,
) -> PlatformResult<()> {
    let source = backup_source_service(s3_key).ok_or_else(|| {
        PlatformError::BadRequest(format!("s3_key must be under backups/{target}/"))
    })?;
    if source == target || user.is_platform_admin {
        return Ok(());
    }
    super::get_service_for_tenant(&state.platform_db, source, user.tenant_id)
        .await
        .map(drop)
}

async fn validate_table_filter(
    state: &AppState,
    tenant_slug: &str,
    service_name: &str,
    tables: Option<&[String]>,
) -> PlatformResult<()> {
    let Some(tables) = tables else {
        return Ok(());
    };
    let service = state
        .registry
        .get_by_slug(tenant_slug, service_name)
        .await
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;
    let unknown: Vec<&str> = tables
        .iter()
        .filter(|name| service.manifest.get_table(name).is_none())
        .map(String::as_str)
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(PlatformError::Validation(format!(
            "unknown tables: {}",
            unknown.join(", ")
        )))
    }
}

#[derive(Deserialize)]
pub struct ExportRequest {
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct RestoreRequest {
    pub s3_key: String,
    #[serde(default)]
    pub tables: Option<Vec<String>>,
    #[serde(default)]
    pub clear_existing: bool,
}

pub async fn start_export(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
    Json(req): Json<ExportRequest>,
) -> PlatformResult<Json<Value>> {
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    if state.storage.is_none() {
        return Err(PlatformError::Internal("S3 storage not configured".into()));
    }
    validate_table_filter(
        &state,
        &user.tenant_slug,
        &service.name,
        req.tables.as_deref(),
    )
    .await?;

    let job = crate::repo::backup_jobs::create(
        &state.platform_db,
        service.id,
        user.tenant_id,
        "export",
        req.tables.as_deref(),
        false,
        None,
    )
    .await?;

    let msg = json!({
        "job_id": job.id.to_string(),
        "service_id": service.id.to_string(),
        "tenant_id": user.tenant_id.to_string(),
        "tenant_slug": user.tenant_slug,
        "service_name": service.name,
        "job_type": "export",
    });

    let subject = format!("cufflink.backup.{}", service.id);
    crate::nats_conn::jetstream_publish_bounded(
        &state.jetstream,
        subject,
        None,
        serde_json::to_vec(&msg)?.into(),
    )
    .await
    .map_err(|e| PlatformError::Internal(format!("Failed to publish backup job: {}", e)))?;

    Ok(Json(json!({
        "job_id": job.id.to_string(),
        "status": "queued",
    })))
}

pub async fn start_restore(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
    Json(req): Json<RestoreRequest>,
) -> PlatformResult<Json<Value>> {
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
    authorize_restore_source(&state, &user, service.id, &req.s3_key).await?;
    validate_table_filter(
        &state,
        &user.tenant_slug,
        &service.name,
        req.tables.as_deref(),
    )
    .await?;

    let job = crate::repo::backup_jobs::create(
        &state.platform_db,
        service.id,
        user.tenant_id,
        "restore",
        req.tables.as_deref(),
        req.clear_existing,
        Some(&req.s3_key),
    )
    .await?;

    let msg = json!({
        "job_id": job.id.to_string(),
        "service_id": service.id.to_string(),
        "tenant_id": user.tenant_id.to_string(),
        "tenant_slug": user.tenant_slug,
        "service_name": service.name,
        "job_type": "restore",
    });

    let subject = format!("cufflink.backup.{}", service.id);
    crate::nats_conn::jetstream_publish_bounded(
        &state.jetstream,
        subject,
        None,
        serde_json::to_vec(&msg)?.into(),
    )
    .await
    .map_err(|e| PlatformError::Internal(format!("Failed to publish restore job: {}", e)))?;

    Ok(Json(json!({
        "job_id": job.id.to_string(),
        "status": "queued",
    })))
}

pub async fn list_jobs(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let jobs = crate::repo::backup_jobs::list_by_service(&state.platform_db, service_id).await?;
    Ok(Json(json!({ "jobs": jobs })))
}

pub async fn get_job(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((service_id, job_id)): Path<(Uuid, Uuid)>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let job = crate::repo::backup_jobs::get_by_id(&state.platform_db, job_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Job not found".into()))?;

    if job.service_id != service_id {
        return Err(PlatformError::NotFound("Job not found".into()));
    }

    Ok(Json(serde_json::to_value(&job).unwrap_or_default()))
}

pub async fn cancel_job(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((service_id, job_id)): Path<(Uuid, Uuid)>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let job = crate::repo::backup_jobs::get_by_id(&state.platform_db, job_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Job not found".into()))?;

    if job.service_id != service_id {
        return Err(PlatformError::NotFound("Job not found".into()));
    }

    let cancelled = crate::repo::backup_jobs::cancel(&state.platform_db, job_id).await?;
    if !cancelled {
        return Err(PlatformError::BadRequest(
            "Job cannot be cancelled (not queued or running)".into(),
        ));
    }

    Ok(Json(json!({ "cancelled": true })))
}

pub async fn download_export(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((service_id, job_id)): Path<(Uuid, Uuid)>,
) -> PlatformResult<axum::response::Response> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let job = crate::repo::backup_jobs::get_by_id(&state.platform_db, job_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Job not found".into()))?;

    if job.service_id != service_id {
        return Err(PlatformError::NotFound("Job not found".into()));
    }
    if job.status != "completed" {
        return Err(PlatformError::BadRequest("Export not yet completed".into()));
    }
    let s3_key = job
        .s3_key
        .as_deref()
        .ok_or_else(|| PlatformError::Internal("No S3 key for completed export".into()))?;

    let storage = state
        .storage
        .as_ref()
        .ok_or_else(|| PlatformError::Internal("S3 storage not configured".into()))?;

    let data = storage
        .download(s3_key)
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to download from S3: {}", e)))?;

    axum::response::Response::builder()
        .header("content-type", "application/x-ndjson")
        .header(
            "content-disposition",
            format!("attachment; filename=\"backup-{}.jsonl\"", job_id),
        )
        .body(axum::body::Body::from(data))
        .map_err(|e| PlatformError::Internal(format!("Failed to build the download response: {e}")))
}

pub async fn get_upload_url(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let storage = state
        .storage
        .as_ref()
        .ok_or_else(|| PlatformError::Internal("S3 storage not configured".into()))?;

    let s3_key = restore_key(service_id, Uuid::new_v4());

    let url = storage
        .presign_upload(&s3_key, "application/x-ndjson", 3600)
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to generate upload URL: {}", e)))?;

    Ok(Json(json!({
        "upload_url": url,
        "s3_key": s3_key,
    })))
}

pub async fn get_schema(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let manifest = crate::repo::deployments::current_manifest(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("No deployment found".into()))?;
    Ok(Json(schema_answer(&manifest)))
}

fn schema_answer(manifest: &Value) -> Value {
    let private_tables = match &manifest["private_tables"] {
        Value::Null => json!([]),
        declared => declared.clone(),
    };
    json!({
        "tables": manifest["tables"],
        "private_tables": private_tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schema_answers_private_tables_beside_the_served_ones() {
        let served = json!({"name": "svc", "tables": [{"name": "items", "columns": []}]});
        assert_eq!(
            schema_answer(&served),
            json!({"tables": [{"name": "items", "columns": []}], "private_tables": []})
        );
        let private = json!({
            "name": "svc",
            "tables": [],
            "private_tables": [{"name": "notes", "columns": [], "managed": false}]
        });
        assert_eq!(
            schema_answer(&private),
            json!({
                "tables": [],
                "private_tables": [{"name": "notes", "columns": [], "managed": false}]
            })
        );
    }

    #[test]
    fn restore_prefix_check() {
        let id = Uuid::new_v4();
        assert_eq!(
            backup_source_service(&format!("backups/{id}/job/export.jsonl")),
            Some(id)
        );
        assert_eq!(
            backup_source_service(&format!("backups/{id}/restore-x.jsonl")),
            Some(id)
        );
        for key in [
            format!("backups/{id}/../{}/export.jsonl", Uuid::new_v4()),
            format!("backups/{id}//x"),
            format!("backups/{id}"),
            "backups/not-a-uuid/export.jsonl".to_string(),
            format!("wasm/{id}/v1.wasm"),
            format!("/backups/{id}/x"),
        ] {
            assert_eq!(backup_source_service(&key), None, "{key}");
        }
    }
}
