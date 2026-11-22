use crate::errors::{PlatformError, PlatformResult};
use crate::registry::ServiceIdentity;
use crate::repo::deployments::{Artifact, Deployment};
use crate::storage::S3Storage;
use crate::AppState;
use cufflink_types::features::{WEB_BUNDLE_PARTS, WEB_RELEASES};
use cufflink_types::ServiceMode;
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

pub const FEATURES: &[&str] = &[WEB_RELEASES, WEB_BUNDLE_PARTS];

pub(crate) const PART_MAX_BYTES: usize = 16 * 1024 * 1024;

pub(crate) const ASSEMBLY_MAX_BYTES: usize = 128 * 1024;

const ASSEMBLY_MAX_PARTS: usize = 1024;

const PARTS_REMOVED_AT_ONCE: usize = 4;

const ASSEMBLIES_AT_ONCE: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assembly {
    pub size: u64,
    pub parts: Vec<String>,
}

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

pub(crate) fn part_key(service_id: Uuid, sha256: &str) -> String {
    format!("{}parts/{sha256}", object_prefix(service_id))
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

async fn stored_size(storage: &S3Storage, key: &str) -> PlatformResult<Option<u64>> {
    storage
        .head_in_bucket(storage.primary_bucket(), key)
        .await
        .map(|meta| meta.map(|meta| meta.size))
        .map_err(|e| PlatformError::Internal(format!("Failed to look up {key}: {e}")))
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Object {
    Bundle,
    Part,
}

impl Object {
    fn name(self) -> &'static str {
        match self {
            Object::Bundle => "bundle",
            Object::Part => "part",
        }
    }

    pub(crate) fn require_sha256(self, candidate: &str) -> PlatformResult<()> {
        if is_sha256(candidate) {
            Ok(())
        } else {
            Err(PlatformError::BadRequest(format!(
                "'{candidate}' is not a {}'s SHA-256 (64 lowercase hex digits)",
                self.name()
            )))
        }
    }

    pub(crate) fn require_content(self, sha256: &str, actual: &str) -> PlatformResult<()> {
        if sha256 == actual {
            Ok(())
        } else {
            Err(PlatformError::Validation(format!(
                "the {}'s SHA-256 is {actual}, not {sha256}",
                self.name()
            )))
        }
    }

    fn key(self, service_id: Uuid, sha256: &str) -> String {
        match self {
            Object::Bundle => object_key(service_id, sha256),
            Object::Part => part_key(service_id, sha256),
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Object::Bundle => "application/gzip",
            Object::Part => "application/octet-stream",
        }
    }
}

async fn put(
    state: &AppState,
    object: Object,
    service_id: Uuid,
    sha256: &str,
    bytes: bytes::Bytes,
) -> PlatformResult<(Artifact, Stored)> {
    if bytes.is_empty() {
        return Err(PlatformError::BadRequest(format!(
            "the {} is empty",
            object.name()
        )));
    }
    let storage = storage(state)?;
    let stored = Artifact {
        s3_key: object.key(service_id, sha256),
        hash: sha256.to_string(),
        size: i64::try_from(bytes.len()).map_err(|_| {
            PlatformError::BadRequest(format!("the {} is too large", object.name()))
        })?,
    };
    if stored_size(storage, &stored.s3_key).await? == Some(bytes.len() as u64) {
        return Ok((stored, Stored::Present));
    }
    storage
        .upload(&stored.s3_key, Vec::from(bytes), object.content_type())
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to store {}: {e}", stored.s3_key)))?;
    info!(%service_id, key = %stored.s3_key, size = stored.size, "Web {} stored", object.name());
    Ok((stored, Stored::Uploaded))
}

async fn record_upload(
    state: &AppState,
    service_id: Uuid,
    (upload, stored): (Artifact, Stored),
    uploaded_by: &str,
) -> PlatformResult<(Artifact, Stored)> {
    crate::repo::web_uploads::record(
        &state.platform_db.get().await?,
        service_id,
        &upload,
        uploaded_by,
    )
    .await?;
    Ok((upload, stored))
}

pub(crate) async fn store(
    state: &AppState,
    service_id: Uuid,
    sha256: &str,
    bundle: bytes::Bytes,
    uploaded_by: &str,
) -> PlatformResult<(Artifact, Stored)> {
    let stored = put(state, Object::Bundle, service_id, sha256, bundle).await?;
    record_upload(state, service_id, stored, uploaded_by).await
}

pub(crate) async fn store_part(
    state: &AppState,
    service_id: Uuid,
    sha256: &str,
    part: bytes::Bytes,
) -> PlatformResult<(Artifact, Stored)> {
    let (part, stored) = put(state, Object::Part, service_id, sha256, part).await?;
    crate::repo::web_upload_parts::record(&state.platform_db.get().await?, service_id, &part)
        .await?;
    Ok((part, stored))
}

fn missing_part(part: &str, bundle: &str) -> PlatformError {
    PlatformError::Conflict(format!(
        "part {part} of bundle {bundle} was not uploaded, or is no longer stored; upload it again"
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Missing {
    bundle: String,
    parts: Vec<String>,
}

impl From<Missing> for PlatformError {
    fn from(Missing { bundle, parts }: Missing) -> Self {
        match parts.as_slice() {
            [part] => missing_part(part, &bundle),
            parts => PlatformError::Conflict(format!(
                "parts {} of bundle {bundle} were not uploaded, or are no longer stored; upload \
                 them again",
                parts.join(", ")
            )),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Assembled {
    Stored((Artifact, Stored)),
    Incomplete(Missing),
}

fn require_assembly(assembly: &Assembly) -> PlatformResult<i64> {
    if !(1..=ASSEMBLY_MAX_PARTS).contains(&assembly.parts.len()) {
        return Err(PlatformError::BadRequest(format!(
            "a bundle is assembled from 1 to {ASSEMBLY_MAX_PARTS} parts, not {}",
            assembly.parts.len()
        )));
    }
    assembly
        .parts
        .iter()
        .try_for_each(|part| Object::Part.require_sha256(part))?;
    Some(assembly.size)
        .filter(|size| *size <= crate::app::layers::ARTIFACT_BODY_LIMIT as u64)
        .and_then(|size| i64::try_from(size).ok())
        .ok_or_else(|| {
            PlatformError::PayloadTooLarge(format!(
                "a bundle of {} bytes is larger than the {} a bundle may hold",
                assembly.size,
                crate::app::layers::ARTIFACT_BODY_LIMIT
            ))
        })
}

fn ordered(
    assembly: &Assembly,
    bundle: &str,
    recorded: &[Artifact],
) -> Result<Vec<Artifact>, Missing> {
    let recorded = recorded
        .iter()
        .map(|part| (part.hash.as_str(), part))
        .collect::<HashMap<_, _>>();
    let missing = assembly
        .parts
        .iter()
        .filter(|part| !recorded.contains_key(part.as_str()))
        .fold(Vec::<String>::new(), |mut missing, part| {
            if !missing.contains(part) {
                missing.push(part.clone());
            }
            missing
        });
    if missing.is_empty() {
        Ok(assembly
            .parts
            .iter()
            .filter_map(|part| recorded.get(part.as_str()).map(|part| (*part).clone()))
            .collect())
    } else {
        Err(Missing {
            bundle: bundle.to_string(),
            parts: missing,
        })
    }
}

fn require_held(bundle: &Artifact, parts: &[Artifact]) -> PlatformResult<()> {
    let held = parts.iter().map(|part| part.size).sum::<i64>();
    if held == bundle.size {
        Ok(())
    } else {
        Err(PlatformError::Validation(format!(
            "the parts of bundle {} hold {held} bytes, not {}",
            bundle.hash, bundle.size
        )))
    }
}

async fn opened(
    storage: S3Storage,
    part: Artifact,
    bundle: String,
) -> eyre::Result<BoxStream<'static, eyre::Result<bytes::Bytes>>> {
    match storage
        .open_read(storage.primary_bucket(), &part.s3_key)
        .await?
    {
        Some(reader) if reader.size == u64::try_from(part.size).ok() => Ok(reader.stream),
        _ => Err(eyre::Report::new(missing_part(&part.hash, &bundle))),
    }
}

fn verified(
    storage: &S3Storage,
    bundle: &Artifact,
    parts: Vec<Artifact>,
) -> BoxStream<'static, eyre::Result<bytes::Bytes>> {
    let (storage, sha256) = (storage.clone(), bundle.hash.clone());
    let read = stream::iter(parts)
        .then(move |part| opened(storage.clone(), part, sha256.clone()))
        .try_flatten()
        .boxed();
    stream::try_unfold(
        (read, Sha256::new(), bundle.hash.clone()),
        |(mut read, mut digest, sha256)| async move {
            match read.try_next().await? {
                Some(chunk) => {
                    digest.update(&chunk);
                    Ok(Some((chunk, (read, digest, sha256))))
                }
                None => Object::Bundle
                    .require_content(&sha256, &hex::encode(digest.finalize()))
                    .map(|()| None)
                    .map_err(eyre::Report::new),
            }
        },
    )
    .boxed()
}

async fn streamed(
    storage: &S3Storage,
    bundle: &Artifact,
    parts: Vec<Artifact>,
) -> PlatformResult<Stored> {
    let streamed = storage
        .upload_stream(
            &bundle.s3_key,
            verified(storage, bundle, parts),
            Object::Bundle.content_type(),
        )
        .await;
    match streamed {
        Ok(_) => {
            info!(key = %bundle.s3_key, size = bundle.size, "Web bundle assembled");
            Ok(Stored::Uploaded)
        }
        Err(e) => match stored_size(storage, &bundle.s3_key).await {
            Ok(Some(stored)) if i64::try_from(stored).ok() == Some(bundle.size) => {
                Ok(Stored::Present)
            }
            _ => Err(e.downcast::<PlatformError>().unwrap_or_else(|e| {
                PlatformError::Internal(format!("Failed to assemble {}: {e}", bundle.s3_key))
            })),
        },
    }
}

async fn remove_parts(state: &AppState, service_id: Uuid, parts: Vec<Artifact>) {
    let Ok(storage) = storage(state) else {
        return;
    };
    let removed = stream::iter(parts)
        .map(|part| async move {
            match storage
                .delete_from_bucket(storage.primary_bucket(), &part.s3_key)
                .await
            {
                Ok(()) => Some(part.hash),
                Err(e) => {
                    warn!(
                        %service_id,
                        key = %part.s3_key,
                        error = %e,
                        "Could not remove a web bundle's part; the sweep tries again once it is past its retention"
                    );
                    None
                }
            }
        })
        .buffer_unordered(PARTS_REMOVED_AT_ONCE)
        .filter_map(futures_util::future::ready)
        .collect::<Vec<_>>()
        .await;
    if removed.is_empty() {
        return;
    }
    let forgotten = async {
        crate::repo::web_upload_parts::forget(&state.platform_db.get().await?, service_id, &removed)
            .await
    }
    .await;
    if let Err(e) = forgotten {
        warn!(%service_id, error = %e, "Could not forget the removed parts of a web bundle");
    }
}

#[derive(Clone)]
pub(crate) struct Assemblies {
    slots: Arc<tokio::sync::Semaphore>,
    running: Arc<std::sync::Mutex<HashMap<(Uuid, String), Arc<tokio::sync::Mutex<()>>>>>,
}

impl Default for Assemblies {
    fn default() -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(ASSEMBLIES_AT_ONCE)),
            running: Arc::default(),
        }
    }
}

struct Turn {
    running: Arc<std::sync::Mutex<HashMap<(Uuid, String), Arc<tokio::sync::Mutex<()>>>>>,
    key: (Uuid, String),
    held: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for Turn {
    fn drop(&mut self) {
        drop(self.held.take());
        let mut running = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if running
            .get(&self.key)
            .is_some_and(|gate| Arc::strong_count(gate) == 1)
        {
            running.remove(&self.key);
        }
    }
}

impl Assemblies {
    async fn turn(&self, key: (Uuid, String)) -> Turn {
        let gate = Arc::clone(
            self.running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(key.clone())
                .or_default(),
        );
        Turn {
            running: Arc::clone(&self.running),
            key,
            held: Some(gate.lock_owned().await),
        }
    }
}

pub(crate) async fn assemble(
    state: &AppState,
    service_id: Uuid,
    sha256: &str,
    assembly: &Assembly,
    uploaded_by: &str,
) -> PlatformResult<Assembled> {
    let bundle = Artifact {
        s3_key: object_key(service_id, sha256),
        hash: sha256.to_string(),
        size: require_assembly(assembly)?,
    };
    let _turn = state
        .web_assemblies
        .turn((service_id, sha256.to_string()))
        .await;
    let storage = storage(state)?;
    let recorded = crate::repo::web_upload_parts::find(
        &state.platform_db.get().await?,
        service_id,
        &assembly.parts,
    )
    .await?;
    let stored = if stored_size(storage, &bundle.s3_key).await? == Some(assembly.size) {
        Stored::Present
    } else {
        let parts = match ordered(assembly, sha256, &recorded) {
            Ok(parts) => parts,
            Err(missing) => return Ok(Assembled::Incomplete(missing)),
        };
        require_held(&bundle, &parts)?;
        let _slot = state
            .web_assemblies
            .slots
            .acquire()
            .await
            .map_err(|e| PlatformError::Internal(format!("No assembly can start: {e}")))?;
        streamed(storage, &bundle, parts).await?
    };
    let stored = record_upload(state, service_id, (bundle, stored), uploaded_by).await?;
    remove_parts(state, service_id, recorded).await;
    Ok(Assembled::Stored(stored))
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
    let retention = state.config.web_upload_retention;
    let expired = async {
        let client = state.platform_db.get().await?;
        let bundles = crate::repo::web_uploads::expired(&client, service_id, retention).await?;
        let parts = crate::repo::web_upload_parts::expired(&client, service_id, retention).await?;
        Ok::<_, PlatformError>((bundles, parts))
    }
    .await;
    match expired {
        Ok((bundles, parts)) => {
            for key in bundles {
                super::artifacts::remove_orphan(state, &key, "unreleased upload").await;
            }
            remove_parts(state, service_id, parts).await;
        }
        Err(e) => warn!(
            %service_id,
            error = %e,
            "Could not remove the unreleased web bundles and parts past their retention; the next release or prune tries again"
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
    fn an_assembly_takes_its_parts_in_order_or_names_every_missing_one_once() {
        let part = |byte: u8, size: i64| Artifact {
            s3_key: format!("web/parts/{byte:02x}"),
            hash: format!("{byte:02x}").repeat(32),
            size,
        };
        let (first, second, third) = (part(0x1a, 4), part(0x2b, 3), part(0x3c, 2));
        let assembly = |parts: &[&Artifact]| Assembly {
            size: 0,
            parts: parts.iter().map(|part| part.hash.clone()).collect(),
        };
        let recorded = [third.clone(), first.clone()];

        assert_eq!(
            ordered(&assembly(&[&first, &third, &first]), "b", &recorded),
            Ok(vec![first.clone(), third.clone(), first.clone()])
        );
        let missing = ordered(
            &assembly(&[&second, &first, &part(0x4d, 1), &second]),
            "b",
            &recorded,
        )
        .expect_err("missing");
        assert_eq!(
            missing,
            Missing {
                bundle: "b".into(),
                parts: vec![second.hash.clone(), "4d".repeat(32)],
            }
        );
        assert_eq!(
            conflict(missing.into()),
            format!(
                "parts {}, {} of bundle b were not uploaded, or are no longer stored; upload them \
                 again",
                second.hash,
                "4d".repeat(32)
            )
        );
        assert_eq!(
            conflict(
                Missing {
                    bundle: "b".into(),
                    parts: vec![second.hash.clone()],
                }
                .into()
            ),
            format!(
                "part {} of bundle b was not uploaded, or is no longer stored; upload it again",
                second.hash
            )
        );

        let bundle = part(0x5e, 9);
        assert!(require_held(&bundle, &[first.clone(), second.clone(), third.clone()]).is_ok());
        assert!(matches!(
            require_held(&bundle, &[first, third]),
            Err(PlatformError::Validation(_))
        ));
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
        assert!(Object::Bundle
            .require_sha256(&"0123456789abcdef".repeat(4))
            .is_ok());
        for refused in [
            "",
            "ab",
            &"AB".repeat(32),
            &"zz".repeat(32),
            &"ab".repeat(33),
        ] {
            assert!(
                matches!(
                    Object::Bundle.require_sha256(refused),
                    Err(PlatformError::BadRequest(_))
                ),
                "{refused}"
            );
        }
        assert!(matches!(
            Object::Bundle.require_content(&"ab".repeat(32), &"cd".repeat(32)),
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
