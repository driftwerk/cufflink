use crate::auth::DeployAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::AppState;
use axum::{
    body::Body,
    extract::{Multipart, Path, State},
    http::header,
    response::Response,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) fn storage_key(service_id: Uuid, file_id: Uuid, extension: &str) -> String {
    format!("files/{service_id}/{file_id}.{extension}")
}

pub async fn upload_file(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
    mut multipart: Multipart,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let mut uploaded = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| PlatformError::BadRequest(format!("Invalid multipart: {}", e)))?
    {
        let filename = field
            .file_name()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unnamed".to_string());
        let content_type = field
            .content_type()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string());

        let data = field
            .bytes()
            .await
            .map_err(|e| PlatformError::BadRequest(format!("Failed to read file: {}", e)))?;

        let file_id = Uuid::new_v4();
        let ext = std::path::Path::new(&filename)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("bin");
        let storage_key = storage_key(service_id, file_id, ext);
        let size_bytes = data.len() as i64;

        if let Some(ref storage) = state.storage {
            storage
                .upload(&storage_key, data.to_vec(), &content_type)
                .await
                .map_err(|e| PlatformError::Internal(format!("Failed to upload file: {}", e)))?;
        } else {
            let storage_dir =
                std::env::var("CUFFLINK_STORAGE_DIR").unwrap_or_else(|_| "./data/files".into());
            let service_dir = format!("{}/{}", storage_dir, service_id);
            tokio::fs::create_dir_all(&service_dir).await.map_err(|e| {
                PlatformError::Internal(format!("Failed to create storage dir: {}", e))
            })?;
            let local_path = format!("{}/{}.{}", service_dir, file_id, ext);
            tokio::fs::write(&local_path, &data)
                .await
                .map_err(|e| PlatformError::Internal(format!("Failed to write file: {}", e)))?;
        }

        let client = state.platform_db.get().await?;
        client
            .execute(
                "INSERT INTO file_uploads (id, service_id, filename, content_type, size_bytes, storage_path)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[&file_id, &service_id, &filename, &content_type, &size_bytes, &storage_key],
            )
            .await
            .map_err(|e| PlatformError::Internal(format!("Failed to save file record: {}", e)))?;

        uploaded.push(json!({
            "id": file_id,
            "filename": filename,
            "content_type": content_type,
            "size_bytes": size_bytes,
        }));
    }

    Ok(Json(json!({ "files": uploaded })))
}

pub async fn list_files(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let rows = client
        .query(
            "SELECT id, filename, content_type, size_bytes, created_at
             FROM file_uploads WHERE service_id = $1 ORDER BY created_at DESC",
            &[&service_id],
        )
        .await?;

    let files: Vec<Value> = rows
        .iter()
        .map(|r| {
            let id: Uuid = r.get(0);
            let filename: String = r.get(1);
            let content_type: String = r.get(2);
            let size_bytes: i64 = r.get(3);
            let created_at: chrono::DateTime<chrono::Utc> = r.get(4);
            json!({
                "id": id,
                "filename": filename,
                "content_type": content_type,
                "size_bytes": size_bytes,
                "created_at": created_at.to_rfc3339(),
            })
        })
        .collect();

    Ok(Json(json!({ "files": files })))
}

pub async fn download_file(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((service_id, file_id)): Path<(Uuid, Uuid)>,
) -> Result<Response, PlatformError> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let row = client
        .query_opt(
            "SELECT filename, content_type, storage_path FROM file_uploads
             WHERE id = $1 AND service_id = $2",
            &[&file_id, &service_id],
        )
        .await?
        .ok_or_else(|| PlatformError::NotFound("File not found".into()))?;

    let filename: String = row.get(0);
    let content_type: String = row.get(1);
    let storage_key: String = row.get(2);

    let data = if let Some(ref storage) = state.storage {
        storage
            .download(&storage_key)
            .await
            .map_err(|e| PlatformError::Internal(format!("Failed to download file: {}", e)))?
    } else {
        tokio::fs::read(&storage_key)
            .await
            .map_err(|e| PlatformError::Internal(format!("Failed to read file: {}", e)))?
    };

    let response = Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(Body::from(data))
        .map_err(|e| PlatformError::Internal(format!("Failed to build response: {}", e)))?;

    Ok(response)
}

pub async fn delete_file(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((service_id, file_id)): Path<(Uuid, Uuid)>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let row = client
        .query_opt(
            "SELECT storage_path FROM file_uploads WHERE id = $1 AND service_id = $2",
            &[&file_id, &service_id],
        )
        .await?
        .ok_or_else(|| PlatformError::NotFound("File not found".into()))?;

    let storage_key: String = row.get(0);

    if let Some(ref storage) = state.storage {
        let _ = storage.delete(&storage_key).await;
    } else {
        let _ = tokio::fs::remove_file(&storage_key).await;
    }

    client
        .execute("DELETE FROM file_uploads WHERE id = $1", &[&file_id])
        .await?;

    Ok(Json(json!({ "deleted": true })))
}
