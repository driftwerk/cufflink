use crate::errors::{PlatformError, PlatformResult};
use crate::registry::ServiceIdentity;
use crate::repo::deployments::{Artifact, Deployment};
use crate::storage::S3Storage;
use crate::AppState;
use cufflink_types::features::WEB_RELEASES;
use cufflink_types::ServiceMode;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};
use uuid::Uuid;

pub const FEATURES: &[&str] = &[WEB_RELEASES];

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebArtifactRef {
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stored {
    Uploaded,
    Present,
}

impl Stored {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Stored::Uploaded => "uploaded",
            Stored::Present => "present",
        }
    }
}

pub(crate) fn upload_result(stored: &PlatformResult<(Artifact, Stored)>) -> &'static str {
    match stored {
        Ok((_, stored)) => stored.as_str(),
        Err(
            PlatformError::NotFound(_)
            | PlatformError::BadRequest(_)
            | PlatformError::Unauthorized(_)
            | PlatformError::Forbidden(_)
            | PlatformError::Conflict(_)
            | PlatformError::Validation(_)
            | PlatformError::PayloadTooLarge(_)
            | PlatformError::DestructiveChanges(_)
            | PlatformError::SerdeJson(_),
        ) => "refused",
        Err(
            PlatformError::TooManyRequests(_)
            | PlatformError::Unavailable(_)
            | PlatformError::RetryLater { .. }
            | PlatformError::GatewayTimeout(_)
            | PlatformError::SchemaMigration(_)
            | PlatformError::Internal(_)
            | PlatformError::Database(_)
            | PlatformError::DatabaseOp { .. }
            | PlatformError::Pool(_)
            | PlatformError::Eyre(_),
        ) => "error",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Legacy {
    Fill,
    Confirm,
    Replace,
}

pub(crate) fn object_prefix(service_id: Uuid) -> String {
    format!("web/{service_id}/")
}

pub(crate) fn object_key(service_id: Uuid, sha256: &str) -> String {
    format!("{}{sha256}.tar.gz", object_prefix(service_id))
}

pub(crate) fn storage(state: &AppState) -> PlatformResult<&S3Storage> {
    state.storage.as_ref().ok_or_else(|| {
        PlatformError::Internal(
            "web apps need object storage, which this platform does not have".into(),
        )
    })
}

fn is_sha256(candidate: &str) -> bool {
    candidate.len() == 64
        && candidate
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) fn require_sha256(candidate: &str) -> PlatformResult<()> {
    if is_sha256(candidate) {
        Ok(())
    } else {
        Err(PlatformError::BadRequest(format!(
            "'{candidate}' is not a bundle's SHA-256 (64 lowercase hex digits)"
        )))
    }
}

pub(crate) fn require_web(name: &str, running: Option<&ServiceMode>) -> PlatformResult<()> {
    match running {
        None | Some(ServiceMode::Web) => Ok(()),
        Some(
            ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Native,
        ) => Err(PlatformError::Conflict(format!(
            "service '{name}' is not a web app; delete it before deploying it as one"
        ))),
    }
}

pub(crate) fn require_content(sha256: &str, actual: &str) -> PlatformResult<()> {
    if sha256 == actual {
        Ok(())
    } else {
        Err(PlatformError::Validation(format!(
            "the bundle's SHA-256 is {actual}, not {sha256}"
        )))
    }
}

async fn stored_size(storage: &S3Storage, key: &str) -> PlatformResult<Option<u64>> {
    storage
        .head_in_bucket(storage.primary_bucket(), key)
        .await
        .map(|meta| meta.map(|meta| meta.size))
        .map_err(|e| PlatformError::Internal(format!("Failed to look up {key}: {e}")))
}

pub(crate) async fn store(
    state: &AppState,
    service_id: Uuid,
    sha256: &str,
    bundle: bytes::Bytes,
    uploaded_by: &str,
) -> PlatformResult<(Artifact, Stored)> {
    if bundle.is_empty() {
        return Err(PlatformError::BadRequest("the bundle is empty".into()));
    }
    let storage = storage(state)?;
    let upload = Artifact {
        s3_key: object_key(service_id, sha256),
        hash: sha256.to_string(),
        size: i64::try_from(bundle.len())
            .map_err(|_| PlatformError::BadRequest("the bundle is too large".into()))?,
    };
    let present = stored_size(storage, &upload.s3_key).await? == Some(bundle.len() as u64);
    if !present {
        storage
            .upload(&upload.s3_key, Vec::from(bundle), "application/gzip")
            .await
            .map_err(|e| {
                PlatformError::Internal(format!("Failed to store {}: {e}", upload.s3_key))
            })?;
        info!(%service_id, key = %upload.s3_key, size = upload.size, "Web bundle stored");
    }
    crate::repo::web_uploads::record(
        &state.platform_db.get().await?,
        service_id,
        &upload,
        uploaded_by,
    )
    .await?;
    let stored = if present {
        Stored::Present
    } else {
        Stored::Uploaded
    };
    Ok((upload, stored))
}

fn claimed(
    name: &str,
    wanted: &WebArtifactRef,
    upload: Option<Artifact>,
    stored: Option<u64>,
) -> PlatformResult<Artifact> {
    upload
        .filter(|upload| {
            u64::try_from(upload.size).ok() == Some(wanted.size) && stored == Some(wanted.size)
        })
        .ok_or_else(|| {
            PlatformError::Conflict(format!(
                "bundle {} of {name} was not uploaded, or is no longer stored; upload it again",
                wanted.sha256
            ))
        })
}

pub(crate) async fn claim(
    state: &AppState,
    identity: &ServiceIdentity,
    wanted: &WebArtifactRef,
) -> PlatformResult<Artifact> {
    let upload = crate::repo::web_uploads::claim(
        &state.platform_db.get().await?,
        identity.service_id,
        &wanted.sha256,
    )
    .await?;
    let stored = match &upload {
        Some(upload) => stored_size(storage(state)?, &upload.s3_key).await?,
        None => None,
    };
    claimed(&identity.service_name, wanted, upload, stored)
}

pub(crate) async fn sweep(state: &AppState, service_id: Uuid) {
    let expired = async {
        crate::repo::web_uploads::expired(
            &state.platform_db.get().await?,
            service_id,
            state.config.web_upload_retention,
        )
        .await
    }
    .await;
    match expired {
        Ok(keys) => {
            for key in keys {
                super::artifacts::remove_orphan(state, &key, "unreleased upload").await;
            }
        }
        Err(e) => warn!(
            %service_id,
            error = %e,
            "Could not remove the unreleased web bundles past their retention; the next release or prune tries again"
        ),
    }
}

fn legacy(current: &Deployment, bundle: &Artifact) -> Legacy {
    match &current.artifact {
        None => Legacy::Fill,
        Some(recorded) if recorded.hash == bundle.hash => Legacy::Confirm,
        Some(_) => Legacy::Replace,
    }
}

pub(crate) async fn release_legacy(
    state: &AppState,
    current: &Deployment,
    bundle: &Artifact,
) -> PlatformResult<(Deployment, bool)> {
    match legacy(current, bundle) {
        Legacy::Confirm => Ok((current.clone(), false)),
        Legacy::Fill => {
            let filled = crate::repo::deployments::record_bundle(
                &state.platform_db.get().await?,
                current.service_id,
                current.version,
                bundle,
            )
            .await?;
            if !filled {
                return Err(PlatformError::Conflict(format!(
                    "v{} recorded a bundle while this one was uploaded; upload it again",
                    current.version
                )));
            }
            Ok((
                Deployment {
                    artifact: Some(bundle.clone()),
                    ..current.clone()
                },
                true,
            ))
        }
        Legacy::Replace => {
            let manifest: crate::deployer::ServiceManifest =
                serde_json::from_value(current.manifest.clone()).map_err(|e| {
                    PlatformError::Internal(format!(
                        "the manifest of v{} cannot be read: {e}",
                        current.version
                    ))
                })?;
            let deployment = crate::repo::deployments::activate_version(
                &state.platform_db,
                crate::repo::deployments::ActivateVersion {
                    service_id: current.service_id,
                    version: current.version + 1,
                    manifest_hash: &current.manifest_hash,
                    manifest: &manifest,
                    wasm_hash: None,
                    commit_hash: current.commit_hash.as_deref(),
                    schema_json: None,
                    schema_hash: &manifest.schema_hash()?,
                    artifact: Some(crate::repo::deployments::VersionArtifact::Web(bundle)),
                },
            )
            .await?;
            Ok((deployment, true))
        }
    }
}

fn recorded<'a>(
    name: &str,
    retained: &[Deployment],
    target: &'a Deployment,
) -> PlatformResult<&'a Artifact> {
    target.artifact.as_ref().ok_or_else(|| {
        let newest = retained
            .iter()
            .filter(|d| d.version < target.version && d.artifact.is_some())
            .map(|d| d.version)
            .max();
        let instead = newest.map_or_else(
            || "no earlier version has one; deploy the app again".to_string(),
            |version| {
                format!(
                    "the newest earlier version with one is v{version}: \
                     cufflink rollback --version {version}"
                )
            },
        );
        PlatformError::Conflict(format!(
            "v{} of {name} has no bundle (its build or upload never completed); {instead}",
            target.version
        ))
    })
}

fn intact(version: i32, artifact: &Artifact, stored: Option<u64>) -> PlatformResult<Artifact> {
    let recorded = u64::try_from(artifact.size).ok();
    match stored {
        Some(size) if Some(size) == recorded => Ok(artifact.clone()),
        Some(size) => Err(PlatformError::Conflict(format!(
            "the bundle of v{version} ({}) is {size} bytes in object storage, not the {} \
             its version records; deploy the app again",
            artifact.s3_key, artifact.size
        ))),
        None => Err(PlatformError::Conflict(format!(
            "the bundle of v{version} ({}) is missing from object storage; deploy the app again",
            artifact.s3_key
        ))),
    }
}

pub(crate) async fn restore(
    state: &AppState,
    name: &str,
    retained: &[Deployment],
    target: &Deployment,
) -> PlatformResult<Artifact> {
    let artifact = recorded(name, retained, target)?;
    let stored = stored_size(storage(state)?, &artifact.s3_key).await?;
    intact(target.version, artifact, stored)
}

fn deployed(identity: &ServiceIdentity, deployment: &Deployment) -> Value {
    json!({
        "service_id": identity.service_id.to_string(),
        "service_name": identity.service_name,
        "version": deployment.version,
        "s3_key": deployment.artifact.as_ref().map(|artifact| &artifact.s3_key),
        "artifact_hash": deployment.artifact.as_ref().map(|artifact| &artifact.hash),
        "tenant_slug": identity.tenant_slug,
    })
}

pub(crate) async fn announce(
    state: &AppState,
    identity: &ServiceIdentity,
    deployment: &Deployment,
) -> Option<bool> {
    deployment.artifact.as_ref()?;
    let subject = format!(
        "deploy.web.{}.{}",
        identity.tenant_slug, identity.service_name
    );
    let published = crate::nats_conn::publish_bounded(
        &state.nats,
        subject.clone(),
        None,
        deployed(identity, deployment).to_string().into(),
    )
    .await;
    Some(match published {
        Ok(()) => true,
        Err(e) => {
            metrics::counter!("cufflink_web_announce_failures_total").increment(1);
            warn!(
                subject = %subject,
                version = deployment.version,
                error = %e,
                "Failed to tell the web runners of a version; they switch when they next read it"
            );
            false
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn bundle(version: i32) -> Artifact {
        Artifact {
            s3_key: format!("web/00000000-0000-0000-0000-000000000002/{version:064x}.tar.gz"),
            hash: format!("{version:064x}"),
            size: 41,
        }
    }

    fn version(version: i32, artifact: Option<Artifact>) -> Deployment {
        Deployment {
            id: Uuid::from_u128(version as u128),
            service_id: Uuid::from_u128(2),
            version,
            manifest_hash: "m".into(),
            manifest: json!({"name": "site", "mode": "web"}),
            wasm_hash: None,
            status: "active".into(),
            deployed_at: chrono::Utc::now(),
            commit_hash: None,
            schema_hash: None,
            artifact,
        }
    }

    fn conflict(refused: PlatformError) -> String {
        match refused {
            PlatformError::Conflict(message) => message,
            other => panic!("a conflict, not {other}"),
        }
    }

    #[test]
    fn a_bundle_is_keyed_by_its_service_and_content() {
        let (site, shop) = (Uuid::from_u128(2), Uuid::from_u128(3));
        let sha256 = "3f".repeat(32);
        assert_eq!(
            object_key(site, &sha256),
            format!("web/00000000-0000-0000-0000-000000000002/{sha256}.tar.gz")
        );
        assert_ne!(object_key(site, &sha256), object_key(shop, &sha256));
        assert_ne!(
            object_key(site, &sha256),
            object_key(site, &"40".repeat(32))
        );
        assert!(object_key(site, &sha256).starts_with(&object_prefix(site)));
        assert!(!object_key(shop, &sha256).starts_with(&object_prefix(site)));
    }

    #[test]
    fn a_bundle_hash_is_sixty_four_lowercase_hex_digits() {
        assert!(require_sha256(&"0123456789abcdef".repeat(4)).is_ok());
        for refused in [
            "",
            "ab",
            &"AB".repeat(32),
            &"zz".repeat(32),
            &"ab".repeat(33),
        ] {
            assert!(
                matches!(require_sha256(refused), Err(PlatformError::BadRequest(_))),
                "{refused}"
            );
        }
        assert!(matches!(
            require_content(&"ab".repeat(32), &"cd".repeat(32)),
            Err(PlatformError::Validation(message))
                if message == format!("the bundle's SHA-256 is {}, not {}", "cd".repeat(32), "ab".repeat(32))
        ));
    }

    #[test]
    fn a_release_claims_only_an_upload_stored_at_the_size_it_names() {
        let upload = bundle(5);
        let wanted = WebArtifactRef {
            sha256: upload.hash.clone(),
            size: 41,
        };
        assert_eq!(
            claimed("site", &wanted, Some(upload.clone()), Some(41)).ok(),
            Some(upload.clone())
        );
        let refusal = format!(
            "bundle {} of site was not uploaded, or is no longer stored; upload it again",
            upload.hash
        );
        for (upload, stored) in [
            (None, None),
            (Some(upload.clone()), None),
            (Some(upload.clone()), Some(40)),
            (
                Some(Artifact {
                    size: 40,
                    ..upload.clone()
                }),
                Some(41),
            ),
        ] {
            assert_eq!(
                conflict(claimed("site", &wanted, upload.clone(), stored).unwrap_err()),
                refusal,
                "{upload:?} stored at {stored:?}"
            );
        }
    }

    #[test]
    fn an_upload_the_platform_failed_to_store_is_counted_apart_from_a_refused_one() {
        let stored = |stored: Stored| Ok((bundle(5), stored));
        assert_eq!(upload_result(&stored(Stored::Uploaded)), "uploaded");
        assert_eq!(upload_result(&stored(Stored::Present)), "present");
        for refused in [
            PlatformError::BadRequest("the bundle is empty".into()),
            PlatformError::Conflict("service 'site' is not a web app".into()),
            PlatformError::Validation("the bundle's SHA-256 is …".into()),
        ] {
            assert_eq!(upload_result(&Err(refused)), "refused");
        }
        for failed in [
            PlatformError::Internal("Failed to store web/…".into()),
            PlatformError::Unavailable("object storage".into()),
            PlatformError::Pool(deadpool_postgres::PoolError::Closed),
        ] {
            assert_eq!(upload_result(&Err(failed)), "error");
        }
    }

    #[test]
    fn the_legacy_upload_never_rewrites_a_recorded_bundle() {
        let uploaded = bundle(9);
        assert_eq!(legacy(&version(7, None), &uploaded), Legacy::Fill);
        assert_eq!(
            legacy(&version(7, Some(uploaded.clone())), &uploaded),
            Legacy::Confirm
        );
        assert_eq!(
            legacy(&version(7, Some(bundle(8))), &uploaded),
            Legacy::Replace
        );
    }

    #[test]
    fn restore_refuses_a_target_without_a_bundle_and_names_the_newest_with_one() {
        let retained = [
            version(8, Some(bundle(8))),
            version(7, None),
            version(6, None),
            version(5, Some(bundle(5))),
            version(4, Some(bundle(4))),
        ];
        assert_eq!(
            conflict(recorded("site", &retained, &retained[1]).unwrap_err()),
            "v7 of site has no bundle (its build or upload never completed); \
             the newest earlier version with one is v5: cufflink rollback --version 5"
        );
        assert_eq!(
            recorded("site", &retained, &retained[3]).ok(),
            Some(&bundle(5))
        );
        let none_earlier = [version(3, Some(bundle(3))), version(2, None)];
        assert_eq!(
            conflict(recorded("site", &none_earlier, &none_earlier[1]).unwrap_err()),
            "v2 of site has no bundle (its build or upload never completed); \
             no earlier version has one; deploy the app again",
            "a later version with a bundle is not offered"
        );
    }

    #[test]
    fn restore_refuses_a_bundle_missing_from_storage_or_of_another_size() {
        let artifact = bundle(5);
        assert_eq!(intact(5, &artifact, Some(41)).ok(), Some(artifact.clone()));
        assert_eq!(
            conflict(intact(5, &artifact, None).unwrap_err()),
            format!(
                "the bundle of v5 ({}) is missing from object storage; deploy the app again",
                artifact.s3_key
            )
        );
        assert_eq!(
            conflict(intact(5, &artifact, Some(40)).unwrap_err()),
            format!(
                "the bundle of v5 ({}) is 40 bytes in object storage, not the 41 its version \
                 records; deploy the app again",
                artifact.s3_key
            )
        );
    }

    #[test]
    fn restore_keeps_a_legacy_key_as_recorded() {
        let legacy = Artifact {
            s3_key: "web/00000000-0000-0000-0000-000000000002/v5.tar.gz".into(),
            hash: "ab".repeat(32),
            size: 41,
        };
        let target = version(5, Some(legacy.clone()));
        let kept = recorded("site", std::slice::from_ref(&target), &target)
            .and_then(|artifact| intact(5, artifact, Some(41)))
            .expect("restored");
        assert_eq!(kept, legacy);
    }

    #[test]
    fn the_runners_are_told_what_a_web_version_runs() {
        let identity = ServiceIdentity {
            tenant_id: Uuid::from_u128(1),
            tenant_slug: "acme".into(),
            service_id: Uuid::from_u128(2),
            service_name: "site".into(),
            schema: "site".into(),
        };
        let artifact = bundle(8);
        assert_eq!(
            deployed(&identity, &version(8, Some(artifact.clone()))),
            json!({
                "service_id": "00000000-0000-0000-0000-000000000002",
                "service_name": "site",
                "version": 8,
                "s3_key": artifact.s3_key,
                "artifact_hash": artifact.hash,
                "tenant_slug": "acme",
            })
        );
    }
}
