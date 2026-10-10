use crate::deployer::manifest::ServiceManifest;
use crate::errors::{PlatformError, PlatformResult};
use crate::nats_manager::deploy_fanout::{publish_deploy_ready, DeployReady};
use crate::repo::deployments::{activate_version, ActivateVersion, Adoption, Deployment};
use crate::wasm_runtime::{sha256_hex, LoadError};
use crate::AppState;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Stored {
    Module { service_id: Uuid, version: i32 },
    Object(String),
}

#[derive(Default)]
pub(crate) struct UploadTracker(std::sync::Mutex<Option<Stored>>);

impl UploadTracker {
    fn set(&self, stored: Stored) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stored);
    }

    pub(crate) fn mark(&self, service_id: Uuid, version: i32) {
        self.set(Stored::Module {
            service_id,
            version,
        });
    }

    pub(crate) fn mark_object(&self, key: String) {
        self.set(Stored::Object(key));
    }

    pub(crate) fn take(&self) -> Option<Stored> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

pub(crate) struct ModuleActivation<'a> {
    pub service_id: Uuid,
    pub version: i32,
    pub manifest: &'a ServiceManifest,
    pub manifest_hash: &'a str,
    pub wasm_hash: Option<&'a str>,
    pub commit_hash: Option<&'a str>,
    pub adoption: Adoption<'a>,
}

pub(crate) async fn activate_module(
    state: &Arc<AppState>,
    activation: ModuleActivation<'_>,
) -> PlatformResult<Deployment> {
    let ModuleActivation {
        service_id,
        version,
        manifest,
        manifest_hash,
        wasm_hash,
        commit_hash,
        adoption,
    } = activation;
    if let (Some(hash), Some(engine)) = (wasm_hash, state.wasm_engine.as_ref()) {
        if engine
            .get_module(service_id, version, Some(hash))
            .await
            .is_none()
        {
            return Err(PlatformError::Internal(format!(
                "WASM load for {} v{} reported success but the module is not \
                 retrievable from the engine — refusing to activate this version",
                manifest.name, version
            )));
        }
    }
    let deployment = activate_version(
        &state.platform_db,
        ActivateVersion {
            service_id,
            version,
            manifest_hash,
            manifest,
            wasm_hash,
            commit_hash,
            schema_json: Some(
                &adoption
                    .snapshot(
                        &state.platform_db,
                        &manifest.name,
                        service_id,
                        &super::schema::declared(manifest),
                    )
                    .await?,
            ),
            schema_hash: &manifest.schema_hash()?,
            native: None,
        },
    )
    .await?;
    if let Some(hash) = wasm_hash {
        info!(
            service = %manifest.name,
            version,
            hash = &hash[..hash.len().min(16)],
            "WASM module activated"
        );
    }
    Ok(deployment)
}

pub(crate) fn fail_after_schema() -> PlatformResult<()> {
    match std::env::var_os("CUFFLINK_TEST_FAIL_AFTER_SCHEMA") {
        Some(flag) if flag == "1" => Err(PlatformError::Internal(
            "deploy failed by CUFFLINK_TEST_FAIL_AFTER_SCHEMA".into(),
        )),
        _ => Ok(()),
    }
}

pub(crate) async fn cleanup_orphans(state: &Arc<AppState>, stored: Option<Stored>, context: &str) {
    match stored {
        None => {}
        Some(Stored::Object(key)) => super::native::remove_orphan(state, &key, context).await,
        Some(Stored::Module {
            service_id,
            version,
        }) => {
            if let Some(ref engine) = state.wasm_engine {
                tracing::warn!(
                    service_id = %service_id,
                    version,
                    context,
                    "Cleaning up orphaned WASM binary after failed operation"
                );
                let cleared = match state.platform_db.get().await {
                    Ok(client) => {
                        engine
                            .delete_precompiled(&client, service_id, version)
                            .await
                    }
                    Err(e) => Err(e.into()),
                };
                if let Err(e) = cleared {
                    tracing::warn!(
                        service_id = %service_id,
                        version,
                        error = %e,
                        "Could not remove the orphaned version's precompiled modules"
                    );
                }
                engine.delete_persisted(service_id, version).await;
            }
        }
    }
}

pub(crate) async fn load_wasm_module(
    state: &Arc<AppState>,
    service_id: Uuid,
    version: i32,
    bytes: &[u8],
    sha256: &str,
    declared_handlers: &[&str],
) -> PlatformResult<()> {
    let wasm_engine = state
        .wasm_engine
        .as_ref()
        .ok_or_else(|| PlatformError::Internal("WASM runtime not available".into()))?;
    wasm_engine
        .load_module(service_id, version, bytes)
        .await
        .map_err(|e| map_load_error(state, e))?;

    let missing = wasm_engine
        .missing_handler_exports(service_id, version, sha256, declared_handlers)
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to inspect WASM exports: {}", e)))?;
    if !missing.is_empty() {
        return Err(PlatformError::Validation(format!(
            "WASM module is missing handler exports declared in the manifest: {}. \
             Add a `handler!({}, ...)` definition for each in your `src/lib.rs`.",
            missing.join(", "),
            missing[0]
        )));
    }
    Ok(())
}

pub(crate) fn map_load_error(state: &AppState, error: LoadError) -> PlatformError {
    match error {
        LoadError::UnknownImports(imports) => {
            metrics::counter!("cufflink_deploy_import_rejected_total").increment(1);
            PlatformError::Validation(format!(
                "WASM module imports functions this platform does not provide: {}. \
                 Compare the cufflink-fn features your service enables with \
                 GET /api/version.supports ({}); either disable the feature or upgrade the platform.",
                imports.join(", "),
                crate::api::version::supports(state).join(", ")
            ))
        }
        other => PlatformError::Internal(format!("Failed to load WASM module: {other}")),
    }
}

pub(crate) async fn restore_wasm_binary(
    state: &Arc<AppState>,
    service_id: Uuid,
    source_version: i32,
    dest_version: i32,
    expected_sha256: &str,
) -> PlatformResult<()> {
    let storage = state.storage.as_ref().ok_or_else(|| {
        PlatformError::Internal(
            "Cannot restore WASM binary for rollback: S3 storage is not configured".into(),
        )
    })?;

    let source_key = format!("wasm/{}/v{}.wasm", service_id, source_version);

    let wasm_bytes = storage.download(&source_key).await.map_err(|e| {
        PlatformError::Internal(format!(
            "Failed to download WASM binary for rollback (key={}): {}",
            source_key, e
        ))
    })?;
    let actual = sha256_hex(&wasm_bytes);
    if actual != expected_sha256 {
        metrics::counter!(
            "cufflink_wasm_hash_mismatch_total",
            "service_id" => service_id.to_string(),
        )
        .increment(1);
        return Err(PlatformError::Internal(format!(
            "WASM binary for rollback (key={source_key}) does not match v{source_version}'s \
             recorded hash {expected_sha256} (got {actual}); refusing to restore it"
        )));
    }

    let wasm_engine = state.wasm_engine.as_ref().ok_or_else(|| {
        PlatformError::Internal(
            "Cannot restore WASM binary for rollback: WASM engine is not available".into(),
        )
    })?;

    wasm_engine
        .load_module(service_id, dest_version, &wasm_bytes)
        .await
        .map_err(|e| map_load_error(state, e))?;

    info!(
        service_id = %service_id,
        source_version = source_version,
        dest_version = dest_version,
        "Restored WASM binary for rollback"
    );

    Ok(())
}

pub(crate) async fn announce(state: &Arc<AppState>, service_id: Uuid, ready: Option<DeployReady>) {
    if let Err(error) = state.registry.refresh(service_id).await {
        warn!(
            %service_id,
            %error,
            "The registry was not refreshed after an activation; the next reload brings it up to date"
        );
    }
    state.registry.notify_change().await;
    if let Some(ready) = ready {
        publish_deploy_ready(&state.nats, &ready).await;
    }
}
