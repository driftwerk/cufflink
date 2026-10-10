use super::deployments::{
    acquire_deploy_lock, decode_wasm_payload, require_platform_features, OPERATION,
};
use crate::auth::DeployAuth;
use crate::deployer::manifest::ServiceManifest;
use crate::deployer::module::{
    activate_module, announce, cleanup_orphans, load_wasm_module, ModuleActivation, UploadTracker,
};
use crate::deployer::pipeline::{admit_calls, deploy_hash};
use crate::errors::{PlatformError, PlatformResult};
use crate::nats_manager::deploy_fanout::DeployReady;
use crate::repo::deployments::Deployment;
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
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct UploadWasmRequest {
    pub wasm_base64: String,
    #[serde(default)]
    pub commit_hash: Option<String>,
}

enum Upload {
    Unchanged {
        version: i32,
        wasm_hash: String,
        restored: bool,
    },
    Activated {
        service_name: String,
        deployment: Box<Deployment>,
        previous_version: i32,
    },
}

pub async fn upload_wasm(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    Json(req): Json<UploadWasmRequest>,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
    if state.wasm_engine.is_none() {
        return Err(PlatformError::Internal("WASM runtime not available".into()));
    }
    let wasm_bytes = decode_wasm_payload(Some(&req.wasm_base64))?.unwrap_or_default();

    let lock = acquire_deploy_lock(&state, service_id, OPERATION).await?;
    let uploads = UploadTracker::default();
    let outcome = upload_locked(
        &state,
        service_id,
        &wasm_bytes,
        req.commit_hash.as_deref(),
        &uploads,
    )
    .await;
    if outcome.is_err() {
        cleanup_orphans(&state, uploads.take(), "upload").await;
    }
    lock.release().await;

    match outcome? {
        Upload::Unchanged {
            version,
            wasm_hash,
            restored,
        } => Ok(Json(json!({
            "service_id": service_id.to_string(),
            "version": version,
            "wasm_hash": wasm_hash,
            "size_bytes": wasm_bytes.len(),
            "unchanged": true,
            "restored": restored,
        }))),
        Upload::Activated {
            service_name,
            deployment,
            previous_version,
        } => {
            let (version, wasm_hash) = (deployment.version, deployment.wasm_hash.clone());
            info!(
                service = %service_name,
                version,
                previous_version,
                "WASM module uploaded as a new version"
            );
            announce(
                &state,
                service_id,
                DeployReady::of(&deployment, &service_name),
            )
            .await;
            let details = json!({
                "version": version,
                "previous_version": previous_version,
                "wasm_hash": wasm_hash,
                "commit_hash": req.commit_hash,
            });
            let audit_client = state.platform_db.get().await?;
            let _ = crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
                client: &audit_client,
                tenant_id: Some(user.tenant_id),
                actor: &user.subject,
                action: "upload_wasm",
                resource_type: "service",
                resource_id: Some(&service_id.to_string()),
                details: Some(details),
                ip_address: None,
            })
            .await;
            Ok(Json(json!({
                "service_id": service_id.to_string(),
                "version": version,
                "previous_version": previous_version,
                "wasm_hash": wasm_hash,
                "commit_hash": req.commit_hash,
                "size_bytes": wasm_bytes.len(),
            })))
        }
    }
}

async fn upload_locked(
    state: &Arc<AppState>,
    service_id: Uuid,
    wasm_bytes: &[u8],
    commit_hash: Option<&str>,
    uploads: &UploadTracker,
) -> PlatformResult<Upload> {
    let engine = state
        .wasm_engine
        .as_ref()
        .ok_or_else(|| PlatformError::Internal("WASM runtime not available".into()))?;
    let service = crate::repo::services::get_by_id(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;
    let deployments =
        crate::repo::deployments::list_by_service(&state.platform_db, service_id).await?;
    let running = crate::repo::deployments::running(&deployments, service.current_version)
        .ok_or_else(|| {
            PlatformError::NotFound(format!(
                "Service '{}' has no running deployment to upload a module for",
                service.name
            ))
        })?;
    let manifest: ServiceManifest = serde_json::from_value(running.manifest.clone())
        .map_err(|e| PlatformError::Internal(format!("Failed to parse manifest: {e}")))?;
    match manifest.mode {
        ServiceMode::Native => {
            return Err(PlatformError::Conflict(format!(
                "service '{}' is a native service: its code is a binary deployed with \
                 `cufflink deploy`, not a WASM module",
                service.name
            )))
        }
        ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Web => {}
    }
    let wasm_hash = sha256_hex(wasm_bytes);
    if running.wasm_hash.as_deref() == Some(wasm_hash.as_str()) {
        let version = service.current_version;
        let persisted = engine
            .is_persisted(service_id, version, wasm_bytes.len() as u64)
            .await;
        if !persisted {
            load_wasm_module(
                state,
                service_id,
                version,
                wasm_bytes,
                &wasm_hash,
                &manifest.declared_wasm_handlers(),
            )
            .await?;
            warn!(
                service = %service.name,
                version,
                "the running version's module was missing from storage; stored it again"
            );
        }
        return Ok(Upload::Unchanged {
            version,
            wasm_hash,
            restored: !persisted,
        });
    }
    require_platform_features(state, &manifest, Some(wasm_bytes))?;
    admit_calls(state, &manifest, service.tenant_id).await?;

    let version = service.current_version + 1;
    uploads.mark(service_id, version);
    load_wasm_module(
        state,
        service_id,
        version,
        wasm_bytes,
        &wasm_hash,
        &manifest.declared_wasm_handlers(),
    )
    .await?;
    let manifest_hash = deploy_hash(&manifest)?;
    let deployment = activate_module(
        state,
        ModuleActivation {
            service_id,
            version,
            manifest: &manifest,
            manifest_hash: &manifest_hash,
            wasm_hash: Some(&wasm_hash),
            commit_hash,
            adoption: crate::repo::deployments::Adoption::Kept,
        },
    )
    .await?;
    Ok(Upload::Activated {
        service_name: manifest.name,
        deployment: Box::new(deployment),
        previous_version: service.current_version,
    })
}

pub async fn wasm_status(
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
            "SELECT wasm_hash FROM deployments
             WHERE service_id = $1 AND version = $2",
            &[&service_id, &service.current_version],
        )
        .await?;
    let wasm_hash: Option<String> = row.and_then(|r| r.get("wasm_hash"));
    let db_state = wasm_hash.is_some();

    let local_cache = match &state.wasm_engine {
        Some(engine) => engine
            .get_module(service_id, service.current_version, wasm_hash.as_deref())
            .await
            .is_some(),
        None => false,
    };

    let ready = db_state && local_cache;

    Ok(Json(json!({
        "service_id": service_id.to_string(),
        "version": service.current_version,
        "db_state": db_state,
        "local_cache": local_cache,
        "ready": ready,
        "wasm_hash": wasm_hash,
        "wasm_loaded": local_cache,
        "runtime_available": state.wasm_engine.is_some(),
    })))
}
