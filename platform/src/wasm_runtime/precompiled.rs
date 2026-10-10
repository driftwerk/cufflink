use super::{sha256_hex, wt, WasmEngine};
use crate::errors::PlatformResult;
use crate::repo::module_artifacts::{self, ModuleArtifact};
use deadpool_postgres::GenericClient;
use sha2::{Digest, Sha256};
use std::hash::{Hash, Hasher};
use std::time::Instant;
use uuid::Uuid;
use wasmtime::{Engine, Module};

struct Sha256Hasher(Sha256);

impl Hasher for Sha256Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finish(&self) -> u64 {
        let digest = self.0.clone().finalize();
        u64::from_be_bytes([
            digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
        ])
    }
}

pub(super) fn fingerprint(engine: &Engine) -> String {
    let mut hasher = Sha256Hasher(Sha256::new());
    engine.precompile_compatibility_hash().hash(&mut hasher);
    hex::encode(hasher.0.finalize())
}

pub(super) enum Precompiled {
    Loaded(Module),
    Missing { stale: Option<ModuleArtifact> },
    Unusable,
}

impl WasmEngine {
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn set_artifact_index(&self, platform_db: deadpool_postgres::Pool) {
        if self.artifact_index.set(platform_db).is_err() {
            tracing::warn!("the precompiled-module index was already set");
        }
    }

    pub(super) async fn load_precompiled(
        &self,
        service_id: Uuid,
        version: i32,
        wasm_sha256: &str,
    ) -> Precompiled {
        let Some(pool) = self.artifact_index.get() else {
            return Precompiled::Unusable;
        };
        let row = match pool.get().await {
            Ok(client) => {
                module_artifacts::find(&client, service_id, version, &self.fingerprint).await
            }
            Err(e) => Err(e.into()),
        };
        let artifact = match row {
            Ok(Some(artifact)) if artifact.wasm_sha256 == wasm_sha256 => artifact,
            Ok(Some(stale)) => {
                tracing::info!(
                    %service_id,
                    version,
                    "the precompiled module was compiled from another .wasm; replacing it"
                );
                return Precompiled::Missing { stale: Some(stale) };
            }
            Ok(None) => return Precompiled::Missing { stale: None },
            Err(e) => {
                tracing::warn!(%service_id, version, error = %e, "precompiled-module lookup failed");
                return Precompiled::Unusable;
            }
        };
        let bytes = {
            let storage = self.storage.read().await;
            let Some(s3) = storage.as_ref() else {
                return Precompiled::Unusable;
            };
            match s3.download(&artifact.object_key()).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(%service_id, version, error = %e, "precompiled module not in storage; replacing it");
                    return Precompiled::Missing {
                        stale: Some(artifact),
                    };
                }
            }
        };
        if sha256_hex(&bytes) != artifact.sha256 {
            metrics::counter!("cufflink_wasm_artifact_mismatch_total").increment(1);
            tracing::error!(
                %service_id,
                version,
                key = %artifact.object_key(),
                "precompiled module does not match its recorded hash; compiling from the .wasm instead"
            );
            return Precompiled::Unusable;
        }
        let engine = self.engine.clone();
        // SAFETY: these bytes were produced by `Module::serialize` on an
        let deserialized =
            tokio::task::spawn_blocking(move || unsafe { Module::deserialize(&engine, bytes) })
                .await
                .map_err(|e| e.to_string())
                .and_then(|module| module.map_err(|e| wt(e).to_string()));
        match deserialized {
            Ok(module) => Precompiled::Loaded(module),
            Err(e) => {
                tracing::warn!(%service_id, version, error = %e, "precompiled module rejected");
                Precompiled::Unusable
            }
        }
    }

    pub(super) async fn store_precompiled(
        &self,
        service_id: Uuid,
        version: i32,
        wasm_sha256: &str,
        module: &Module,
        stale: Option<&ModuleArtifact>,
    ) {
        if let Err(e) = self
            .try_store_precompiled(service_id, version, wasm_sha256, module, stale)
            .await
        {
            tracing::warn!(%service_id, version, error = %e, "could not store the precompiled module");
        }
    }

    async fn try_store_precompiled(
        &self,
        service_id: Uuid,
        version: i32,
        wasm_sha256: &str,
        module: &Module,
        stale: Option<&ModuleArtifact>,
    ) -> eyre::Result<()> {
        let Some(pool) = self.artifact_index.get() else {
            return Ok(());
        };
        let bytes = module.serialize().map_err(wt)?;
        let artifact = ModuleArtifact {
            service_id,
            version,
            engine_fingerprint: self.fingerprint.to_string(),
            wasm_sha256: wasm_sha256.to_string(),
            sha256: sha256_hex(&bytes),
            size_bytes: i64::try_from(bytes.len())?,
        };
        let key = artifact.object_key();
        let storage = self.storage.read().await;
        let Some(s3) = storage.as_ref() else {
            return Ok(());
        };
        s3.upload(&key, bytes, "application/octet-stream").await?;
        let settled = async {
            let client = pool.get().await?;
            let recorded = match stale {
                Some(stale) => module_artifacts::replace(&client, stale, &artifact).await?,
                None => module_artifacts::insert(&client, &artifact).await?,
            };
            eyre::Ok(if recorded {
                Settled::Recorded {
                    replaced: stale.map(ModuleArtifact::object_key),
                }
            } else {
                Settled::Lost {
                    winner: module_artifacts::find(&client, service_id, version, &self.fingerprint)
                        .await?
                        .map(|winner| winner.object_key()),
                }
            })
        }
        .await;
        let orphan = match settled {
            Ok(settled) => settled.orphan(&key),
            Err(e) => {
                s3.delete(&key).await?;
                return Err(e);
            }
        };
        match orphan {
            Some(orphan) => s3.delete(&orphan).await,
            None => Ok(()),
        }
    }

    pub(super) async fn replace_precompiled(
        &self,
        service_id: Uuid,
        version: i32,
        wasm_sha256: &str,
        module: &Module,
    ) {
        let Some(pool) = self.artifact_index.get() else {
            return;
        };
        let cleared = match pool.get().await {
            Ok(client) => self.delete_precompiled(&client, service_id, version).await,
            Err(e) => Err(e.into()),
        };
        match cleared {
            Ok(()) => {
                self.store_precompiled(service_id, version, wasm_sha256, module, None)
                    .await
            }
            Err(e) => tracing::warn!(
                %service_id,
                version,
                error = %e,
                "could not clear the version's precompiled modules; not storing a new one"
            ),
        }
    }

    pub async fn delete_precompiled(
        &self,
        client: &impl GenericClient,
        service_id: Uuid,
        version: i32,
    ) -> PlatformResult<()> {
        let artifacts = module_artifacts::delete_version(client, service_id, version).await?;
        let storage = self.storage.read().await;
        let Some(s3) = storage.as_ref() else {
            return Ok(());
        };
        for artifact in &artifacts {
            if let Err(e) = s3.delete(&artifact.object_key()).await {
                tracing::warn!(key = %artifact.object_key(), error = %e, "could not delete a precompiled module");
            }
        }
        Ok(())
    }
}

enum Settled {
    Recorded { replaced: Option<String> },
    Lost { winner: Option<String> },
}

impl Settled {
    fn orphan(self, ours: &str) -> Option<String> {
        match self {
            Settled::Recorded { replaced } => replaced.filter(|key| key != ours),
            Settled::Lost { winner } => (winner.as_deref() != Some(ours)).then(|| ours.to_string()),
        }
    }
}

pub(super) fn record_load(source: &'static str, started: Instant) {
    metrics::histogram!("cufflink_wasm_module_load_seconds", "source" => source)
        .record(started.elapsed().as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fingerprint_is_stable_per_engine_configuration() {
        let engine = Engine::default();
        let first = fingerprint(&engine);
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(fingerprint(&Engine::default()), first);
        let mut config = wasmtime::Config::new();
        config.epoch_interruption(true);
        let other = Engine::new(&config).expect("engine");
        assert_ne!(fingerprint(&other), first, "codegen settings change it");
    }

    #[test]
    fn a_settled_store_orphans_only_objects_no_row_names() {
        let ours = "wasm/s/v1/f/new.cwasm";
        let other = "wasm/s/v1/f/old.cwasm";
        let recorded = |replaced: Option<&str>| Settled::Recorded {
            replaced: replaced.map(str::to_string),
        };
        let lost = |winner: Option<&str>| Settled::Lost {
            winner: winner.map(str::to_string),
        };
        assert_eq!(recorded(None).orphan(ours), None);
        assert_eq!(recorded(Some(other)).orphan(ours).as_deref(), Some(other));
        assert_eq!(recorded(Some(ours)).orphan(ours), None, "healed in place");
        assert_eq!(lost(Some(ours)).orphan(ours), None, "the winner's object");
        assert_eq!(lost(Some(other)).orphan(ours).as_deref(), Some(ours));
        assert_eq!(lost(None).orphan(ours).as_deref(), Some(ours));
    }

    #[test]
    fn the_hasher_digests_everything_written() {
        let digest = |parts: &[&[u8]]| {
            let mut hasher = Sha256Hasher(Sha256::new());
            parts.iter().for_each(|part| hasher.write(part));
            hasher.finish()
        };
        assert_eq!(digest(&[b"ab", b"c"]), digest(&[b"abc"]));
        assert_ne!(digest(&[b"abc"]), digest(&[b"abd"]));
    }
}
