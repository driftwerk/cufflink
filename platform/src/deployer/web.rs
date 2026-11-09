use crate::errors::{PlatformError, PlatformResult};
use crate::registry::ServiceIdentity;
use crate::repo::deployments::{Artifact, Deployment};
use crate::storage::S3Storage;
use crate::AppState;
use serde_json::{json, Value};
use tracing::warn;

pub(crate) fn storage(state: &AppState) -> PlatformResult<&S3Storage> {
    state.storage.as_ref().ok_or_else(|| {
        PlatformError::Internal(
            "web apps need object storage, which this platform does not have".into(),
        )
    })
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
    let storage = storage(state)?;
    let stored = storage
        .head_in_bucket(storage.primary_bucket(), &artifact.s3_key)
        .await
        .map_err(|e| {
            PlatformError::Internal(format!("Failed to look up {}: {e}", artifact.s3_key))
        })?;
    intact(target.version, artifact, stored.map(|meta| meta.size))
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
) -> bool {
    let subject = format!(
        "deploy.web.{}.{}",
        identity.tenant_slug, identity.service_name
    );
    match crate::nats_conn::publish_bounded(
        &state.nats,
        subject.clone(),
        None,
        deployed(identity, deployment).to_string().into(),
    )
    .await
    {
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
    }
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
