use super::module::UploadTracker;
use crate::deployer::manifest::ServiceManifest;
use crate::errors::{PlatformError, PlatformResult};
use crate::extensions::{self, Declaration, ExtensionsConfig, Registration};
use crate::registry::ServiceIdentity;
use crate::repo::deployments::{Artifact, Deployment};
use crate::storage::S3Storage;
use crate::wasm_runtime::sha256_hex;
use crate::AppState;
use cufflink_types::ext::{deploy_subject, undeploy_subject};
use cufflink_types::features::NATIVE_SERVICES;
use cufflink_types::ServiceMode;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tracing::{info, warn};
use uuid::Uuid;

pub const FEATURES: &[&str] = &[NATIVE_SERVICES];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeBinary {
    pub bytes: Vec<u8>,
    pub hash: String,
}

fn object_prefix(service_id: Uuid) -> String {
    format!("native/{service_id}")
}

pub(crate) fn object_key(service_id: Uuid, hash: &str) -> String {
    format!("{}/{hash}", object_prefix(service_id))
}

pub(crate) fn allowed(config: &ExtensionsConfig, mode: &ServiceMode) -> PlatformResult<()> {
    match (mode, config.enabled) {
        (ServiceMode::Native, false) => Err(PlatformError::Forbidden(
            "native services cannot be deployed or rolled back on this platform \
             (CUFFLINK_EXTENSIONS=false)"
                .into(),
        )),
        (ServiceMode::Native, true)
        | (ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Web, _) => {
            Ok(())
        }
    }
}

pub(crate) fn binary(
    mode: &ServiceMode,
    native_base64: Option<&str>,
    carries_wasm: bool,
    max_bytes: usize,
) -> PlatformResult<Option<NativeBinary>> {
    match (mode, native_base64) {
        (ServiceMode::Native, _) if carries_wasm => Err(PlatformError::BadRequest(
            "a native service carries no WASM module: send its binary as native_base64 only".into(),
        )),
        (ServiceMode::Native, None) => Err(missing()),
        (ServiceMode::Native, Some(encoded)) => decode(encoded, max_bytes).map(Some),
        (
            ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Web,
            Some(_),
        ) => Err(PlatformError::BadRequest(
            "native_base64 is accepted only with mode: native".into(),
        )),
        (
            ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Web,
            None,
        ) => Ok(None),
    }
}

pub(crate) fn missing() -> PlatformError {
    PlatformError::BadRequest("mode: native requires the service's binary in native_base64".into())
}

fn decode(encoded: &str, max_bytes: usize) -> PlatformResult<NativeBinary> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| PlatformError::BadRequest(format!("Invalid native_base64: {e}")))?;
    match bytes.len() {
        0 => Err(PlatformError::BadRequest("native_base64 is empty".into())),
        size if size > max_bytes => Err(PlatformError::BadRequest(format!(
            "the native binary is {size} bytes; this platform takes at most {max_bytes} \
             (NATIVE_ARTIFACT_MAX_BYTES)"
        ))),
        _ => Ok(NativeBinary {
            hash: sha256_hex(&bytes),
            bytes,
        }),
    }
}

pub(crate) fn require_same_kind(
    name: &str,
    running: Option<&ServiceMode>,
    mode: &ServiceMode,
) -> PlatformResult<()> {
    let native = |mode: &ServiceMode| matches!(mode, ServiceMode::Native);
    match (running.map(native), native(mode)) {
        (Some(true), false) => Err(PlatformError::Conflict(format!(
            "service '{name}' is a native service; delete it before deploying it in another mode"
        ))),
        (Some(false), true) => Err(PlatformError::Conflict(format!(
            "service '{name}' is not a native service; delete it before deploying it as one"
        ))),
        (None, _) | (Some(true), true) | (Some(false), false) => Ok(()),
    }
}

pub(crate) async fn admit(
    state: &AppState,
    manifest: &ServiceManifest,
    tenant_id: Uuid,
    registrations: &BTreeMap<String, Registration>,
) -> PlatformResult<()> {
    let pool = &state.platform_db;
    let service_id = crate::repo::services::find(pool, tenant_id, &manifest.name)
        .await?
        .map(|service| service.id);
    let running = match service_id {
        Some(service_id) => crate::repo::deployments::current_mode(pool, service_id).await?,
        None => None,
    };
    require_same_kind(&manifest.name, running.as_ref(), &manifest.mode)?;
    if !matches!(manifest.mode, ServiceMode::Native) {
        return Ok(());
    }
    let refusal = match (
        extensions::unprovidable(&manifest.provides, service_id, registrations),
        service_id,
    ) {
        (Some(reason), _) => Some(reason),
        (None, Some(service_id)) => crate::repo::extensions::stranding(
            &pool.get().await?,
            tenant_id,
            service_id,
            &manifest.provides,
            registrations,
        )
        .await?
        .as_ref()
        .map(Declaration::provides_refusal),
        (None, None) => None,
    };
    refusal.map_or(Ok(()), |reason| Err(PlatformError::Conflict(reason)))
}

pub(crate) fn storage(state: &AppState) -> PlatformResult<&S3Storage> {
    state.storage.as_ref().ok_or_else(|| {
        PlatformError::Internal(
            "native services need object storage, which this platform does not have".into(),
        )
    })
}

async fn stored(storage: &S3Storage, key: &str) -> PlatformResult<bool> {
    storage
        .head_in_bucket(storage.primary_bucket(), key)
        .await
        .map(|meta| meta.is_some())
        .map_err(|e| PlatformError::Internal(format!("Failed to look up {key}: {e}")))
}

pub(crate) async fn store(
    state: &AppState,
    service_id: Uuid,
    binary: &NativeBinary,
    uploads: &UploadTracker,
) -> PlatformResult<Artifact> {
    let storage = storage(state)?;
    let key = object_key(service_id, &binary.hash);
    let size = i64::try_from(binary.bytes.len())
        .map_err(|_| PlatformError::BadRequest("the native binary is too large".into()))?;
    if !stored(storage, &key).await? {
        storage
            .upload(&key, binary.bytes.clone(), "application/octet-stream")
            .await
            .map_err(|e| PlatformError::Internal(format!("Failed to store {key}: {e}")))?;
        uploads.mark_object(key.clone());
        info!(%service_id, key = %key, size, "Native binary stored");
    }
    Ok(Artifact {
        s3_key: key,
        hash: binary.hash.clone(),
        size,
    })
}

pub(crate) async fn restore(state: &AppState, target: &Deployment) -> PlatformResult<Artifact> {
    let artifact = target.artifact.clone().ok_or_else(|| {
        PlatformError::Internal(format!(
            "native deployment v{} records no binary",
            target.version
        ))
    })?;
    if stored(storage(state)?, &artifact.s3_key).await? {
        Ok(artifact)
    } else {
        Err(PlatformError::Internal(format!(
            "the binary of v{} ({}) is missing from object storage; deploy the service again",
            target.version, artifact.s3_key
        )))
    }
}

pub(crate) async fn remove_orphan(state: &AppState, key: &str, context: &str) {
    let Some(storage) = state.storage.as_ref() else {
        return;
    };
    let recorded = match state.platform_db.get().await {
        Ok(client) => crate::repo::deployments::records_artifact(&client, key).await,
        Err(e) => Err(e.into()),
    };
    match recorded {
        Ok(true) => {}
        Ok(false) => {
            warn!(
                key,
                context, "Removing a native binary no deployment records"
            );
            if let Err(e) = storage.delete(key).await {
                warn!(key, context, error = %e, "Could not remove the native binary");
            }
        }
        Err(e) => warn!(
            key,
            context,
            error = %e,
            "Could not tell whether a deployment records the native binary; leaving it"
        ),
    }
}

fn deployed(
    identity: &ServiceIdentity,
    deployment: &Deployment,
    manifest: &ServiceManifest,
) -> Value {
    json!({
        "service_id": identity.service_id,
        "service_name": identity.service_name,
        "tenant_slug": identity.tenant_slug,
        "version": deployment.version,
        "s3_key": deployment.artifact.as_ref().map(|artifact| &artifact.s3_key),
        "artifact_hash": deployment.artifact.as_ref().map(|artifact| &artifact.hash),
        "extensions": manifest.provides.iter().map(|provided| &provided.name).collect::<Vec<_>>(),
    })
}

async fn publish(state: &AppState, subject: String, payload: Value) {
    if let Err(e) = crate::nats_conn::publish_bounded(
        &state.nats,
        subject.clone(),
        None,
        payload.to_string().into(),
    )
    .await
    {
        warn!(subject = %subject, error = %e, "Failed to publish a native service event");
    }
}

pub(crate) async fn announce(
    state: &AppState,
    identity: &ServiceIdentity,
    deployment: &Deployment,
    manifest: &ServiceManifest,
) {
    state.tenants.refresh().await;
    publish(
        state,
        deploy_subject(&identity.tenant_slug, &identity.service_name),
        deployed(identity, deployment, manifest),
    )
    .await;
}

pub(crate) async fn announce_removal(
    state: &AppState,
    tenant_slug: &str,
    service_id: Uuid,
    service_name: &str,
) {
    state.tenants.refresh().await;
    publish(
        state,
        undeploy_subject(tenant_slug, service_name),
        json!({
            "service_id": service_id,
            "service_name": service_name,
            "tenant_slug": tenant_slug,
        }),
    )
    .await;
}

pub(crate) async fn delete_objects(state: &AppState, service_id: Uuid) {
    if let Some(storage) = state.storage.as_ref() {
        let prefix = object_prefix(service_id);
        if let Err(e) = storage.delete_prefix(&prefix).await {
            warn!(prefix = %prefix, error = %e, "Failed to delete a native service's binaries");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    const MODES: [ServiceMode; 5] = [
        ServiceMode::Crud,
        ServiceMode::Wasm,
        ServiceMode::Container,
        ServiceMode::Web,
        ServiceMode::Native,
    ];

    fn encoded(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn enabled(enabled: bool) -> ExtensionsConfig {
        ExtensionsConfig {
            enabled,
            ..ExtensionsConfig::default()
        }
    }

    #[test]
    fn a_native_service_is_deployed_only_where_extensions_are_allowed() {
        for mode in &MODES[..4] {
            for on in [false, true] {
                assert!(allowed(&enabled(on), mode).is_ok(), "{mode:?}");
            }
        }
        assert!(allowed(&enabled(true), &ServiceMode::Native).is_ok());
        let refused = allowed(&enabled(false), &ServiceMode::Native).unwrap_err();
        assert!(
            matches!(&refused, PlatformError::Forbidden(m) if m.contains("CUFFLINK_EXTENSIONS=false")),
            "{refused}"
        );
    }

    #[test]
    fn a_native_deploy_carries_its_binary_and_nothing_else_does() {
        let binary = encoded(b"\x7fELF binary");
        let decoded = super::binary(&ServiceMode::Native, Some(&binary), false, 1024)
            .expect("a binary")
            .expect("carried");
        assert_eq!(decoded.bytes, b"\x7fELF binary");
        assert_eq!(decoded.hash, sha256_hex(b"\x7fELF binary"));
        for mode in &MODES[..4] {
            assert_eq!(super::binary(mode, None, false, 1024).expect("none"), None);
            assert_eq!(super::binary(mode, None, true, 1024).expect("none"), None);
            let refused = super::binary(mode, Some(&binary), false, 1024).unwrap_err();
            assert!(
                matches!(&refused, PlatformError::BadRequest(m) if m.contains("only with mode: native")),
                "{refused}"
            );
        }
        let bad_request = |refused: PlatformError, says: &str| {
            assert!(
                matches!(&refused, PlatformError::BadRequest(m) if m.contains(says)),
                "{refused}"
            );
        };
        let native = ServiceMode::Native;
        bad_request(
            super::binary(&native, None, false, 1024).unwrap_err(),
            "requires the service's binary",
        );
        bad_request(
            super::binary(&native, Some(&binary), true, 1024).unwrap_err(),
            "carries no WASM module",
        );
        bad_request(
            super::binary(&native, Some("not base64!"), false, 1024).unwrap_err(),
            "Invalid native_base64",
        );
        bad_request(
            super::binary(&native, Some(""), false, 1024).unwrap_err(),
            "empty",
        );
        bad_request(
            super::binary(&native, Some(&encoded(&[0; 1025])), false, 1024).unwrap_err(),
            "NATIVE_ARTIFACT_MAX_BYTES",
        );
        assert!(super::binary(&native, Some(&encoded(&[0; 1024])), false, 1024).is_ok());
    }

    #[test]
    fn a_service_keeps_its_kind() {
        let native = ServiceMode::Native;
        for (running, mode) in [
            (None, &native),
            (None, &ServiceMode::Wasm),
            (Some(&native), &native),
            (Some(&ServiceMode::Web), &ServiceMode::Wasm),
        ] {
            assert!(
                require_same_kind("svc", running, mode).is_ok(),
                "{running:?} {mode:?}"
            );
        }
        for (running, mode) in [
            (Some(&native), &ServiceMode::Wasm),
            (Some(&ServiceMode::Crud), &native),
        ] {
            assert!(
                matches!(
                    require_same_kind("svc", running, mode),
                    Err(PlatformError::Conflict(m)) if m.starts_with("service 'svc' is")
                ),
                "{running:?} {mode:?}"
            );
        }
    }

    #[test]
    fn a_binary_is_stored_under_its_service_and_content() {
        let id = Uuid::from_u128(1);
        assert_eq!(
            object_key(id, "ab12"),
            "native/00000000-0000-0000-0000-000000000001/ab12"
        );
        assert!(object_key(id, "ab12").starts_with(&format!("{}/", object_prefix(id))));
    }

    #[test]
    fn the_runner_is_told_what_a_native_version_runs_and_provides() {
        let identity = ServiceIdentity {
            tenant_id: Uuid::from_u128(1),
            tenant_slug: "acme".into(),
            service_id: Uuid::from_u128(2),
            service_name: "ext-google".into(),
            schema: "ext_google".into(),
        };
        let deployment = Deployment {
            id: Uuid::from_u128(3),
            service_id: identity.service_id,
            version: 4,
            manifest_hash: "m".into(),
            manifest: json!({}),
            wasm_hash: None,
            status: "active".into(),
            deployed_at: chrono::Utc::now(),
            commit_hash: None,
            schema_hash: None,
            artifact: Some(Artifact {
                s3_key: "native/2/ab".into(),
                hash: "ab".into(),
                size: 9,
            }),
        };
        let manifest = ServiceManifest {
            name: "ext-google".into(),
            mode: ServiceMode::Native,
            provides: vec![cufflink_types::ProvidedExtension {
                name: "google.token".into(),
                callers: vec!["*".into()],
            }],
            ..Default::default()
        };
        assert_eq!(
            deployed(&identity, &deployment, &manifest),
            json!({
                "service_id": "00000000-0000-0000-0000-000000000002",
                "service_name": "ext-google",
                "tenant_slug": "acme",
                "version": 4,
                "s3_key": "native/2/ab",
                "artifact_hash": "ab",
                "extensions": ["google.token"],
            })
        );
    }
}
