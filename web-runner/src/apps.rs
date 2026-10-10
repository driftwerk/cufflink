use crate::artifact::ArtifactStore;
use crate::process_manager::{DeployOutcome, ProcessManager, Release};
use crate::source::{Source, Standing, WebDeployment};
use cufflink_supervisor::locked;
use cufflink_supervisor::runner_api::key_refused;
use cufflink_types::runner::is_sha256;
use eyre::WrapErr;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

const OLD_VERSION_KEPT: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
struct Holds(Mutex<HashMap<PathBuf, usize>>);

struct Held {
    holds: Arc<Holds>,
    dir: PathBuf,
}

impl Holds {
    fn hold(self: &Arc<Self>, dir: PathBuf) -> Held {
        *locked(&self.0).entry(dir.clone()).or_default() += 1;
        Held {
            holds: self.clone(),
            dir,
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut held = locked(&self.holds.0);
        if let Some(count) = held.get_mut(&self.dir) {
            *count -= 1;
            if *count == 0 {
                held.remove(&self.dir);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ran {
    AlreadyRunning,
    Started { port: u16 },
    Unhealthy { port: u16, error: String },
}

pub struct Apps {
    pub source: Arc<Source>,
    artifacts: ArtifactStore,
    pub process_manager: Arc<ProcessManager>,
    apps_dir: PathBuf,
    holds: Arc<Holds>,
}

impl Apps {
    pub fn new(
        source: Arc<Source>,
        artifacts: ArtifactStore,
        process_manager: Arc<ProcessManager>,
        apps_dir: PathBuf,
    ) -> Self {
        Self {
            source,
            artifacts,
            process_manager,
            apps_dir,
            holds: Arc::default(),
        }
    }

    fn service_dir(&self, tenant: &str, service: &str) -> PathBuf {
        self.apps_dir.join(tenant).join(service)
    }

    fn content_dir(&self, deployment: &WebDeployment, content: &str) -> PathBuf {
        self.service_dir(&deployment.tenant, &deployment.service)
            .join(format!("{}-{content}", deployment.service_id))
    }

    fn label(deployment: &WebDeployment) -> String {
        format!(
            "{}/{} v{}",
            deployment.tenant, deployment.service, deployment.version
        )
    }

    fn recorded_hash(deployment: &WebDeployment) -> eyre::Result<Option<&str>> {
        match deployment.sha256.as_deref() {
            Some(sha256) if is_sha256(sha256) => Ok(Some(sha256)),
            Some(_) => Err(eyre::eyre!(
                "{} records a bundle hash that is not a SHA-256",
                Self::label(deployment)
            )),
            None => Ok(None),
        }
    }

    fn runs_from(&self, deployment: &WebDeployment, dir: &Path) -> bool {
        self.process_manager
            .release(&deployment.tenant, &deployment.service)
            .is_some_and(|release| release.dir == dir)
    }

    async fn installed(&self, deployment: &WebDeployment) -> eyre::Result<Held> {
        let (content, data) = match Self::recorded_hash(deployment)? {
            Some(sha256) => (sha256.to_string(), None),
            None => {
                warn!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, "Web artifact loaded without a recorded hash; downloading it to learn its content");
                let data = self.source.bundle(deployment).await?;
                (hex::encode(Sha256::digest(&data)), Some(data))
            }
        };
        let held = self.holds.hold(self.content_dir(deployment, &content));
        if !held.dir.is_dir() {
            info!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, "Downloading and unpacking the bundle");
            let data = match data {
                Some(data) => data,
                None => self.source.bundle(deployment).await?,
            };
            self.artifacts
                .install(&Self::label(deployment), data, &held.dir, Some(&content))
                .await?;
        }
        Ok(held)
    }

    pub async fn run(self: &Arc<Self>, deployment: &WebDeployment) -> eyre::Result<Ran> {
        let (tenant, service) = (deployment.tenant.as_str(), deployment.service.as_str());
        let recorded =
            Self::recorded_hash(deployment)?.map(|sha256| self.content_dir(deployment, sha256));
        if recorded.is_some_and(|dir| self.runs_from(deployment, &dir)) {
            return Ok(Ran::AlreadyRunning);
        }
        let environment = self
            .source
            .environment(deployment)
            .await
            .wrap_err("configs")?;
        let installed = self
            .installed(deployment)
            .await
            .wrap_err("download/extract")?;
        if self.runs_from(deployment, &installed.dir) {
            return Ok(Ran::AlreadyRunning);
        }
        let replaced = self.process_manager.release(tenant, service);
        let release = Release {
            version: deployment.version,
            dir: installed.dir.clone(),
        };
        let outcome = self
            .process_manager
            .deploy(tenant, service, release, environment)
            .await;
        drop(installed);
        if let (Ok(DeployOutcome::Success { .. }), Some(replaced)) = (&outcome, replaced) {
            self.drain(deployment, replaced.dir);
        }
        self.prune(tenant, service).await;
        Ok(match outcome.wrap_err("spawn")? {
            DeployOutcome::Success { port } => Ran::Started { port },
            DeployOutcome::HealthCheckFailed { port, error } => Ran::Unhealthy { port, error },
        })
    }

    pub async fn deploy(self: &Arc<Self>, deployment: &WebDeployment) {
        let (tenant, service, version) = (
            deployment.tenant.as_str(),
            deployment.service.as_str(),
            deployment.version,
        );
        info!(tenant, service, version, "Received deploy event");
        self.process_manager.forget_exits(tenant, service);
        match self.run(deployment).await {
            Ok(Ran::AlreadyRunning) => {
                info!(tenant, service, version, "The app already runs this bundle")
            }
            Ok(Ran::Started { port }) => info!(tenant, service, version, port, "Deploy complete"),
            Ok(Ran::Unhealthy { port, error }) => {
                error!(tenant, service, version, port, error = %error, "New process failed health check; the running one is unchanged")
            }
            Err(e) => {
                error!(tenant, service, version, error = %format!("{e:#}"), "Deploy failed; the running one is unchanged")
            }
        }
    }

    fn drain(self: &Arc<Self>, deployment: &WebDeployment, dir: PathBuf) {
        let (apps, held) = (self.clone(), self.holds.hold(dir));
        let (tenant, service) = (deployment.tenant.clone(), deployment.service.clone());
        tokio::spawn(async move {
            tokio::time::sleep(OLD_VERSION_KEPT).await;
            drop(held);
            apps.prune(&tenant, &service).await;
        });
    }

    async fn prune(&self, tenant: &str, service: &str) {
        let service_dir = self.service_dir(tenant, service);
        let doomed = self.doomed(tenant, service, &service_dir);
        if let Err(e) =
            tokio::task::spawn_blocking(move || doomed.iter().for_each(|path| remove(path))).await
        {
            warn!(error = %e, "Pruning APPS_DIR failed");
        }
        let held = locked(&self.holds.0);
        if !held.keys().any(|dir| dir.parent() == Some(&service_dir)) {
            let _ = std::fs::remove_dir(&service_dir);
            let _ = std::fs::remove_dir(self.apps_dir.join(tenant));
        }
    }

    fn doomed(&self, tenant: &str, service: &str, service_dir: &Path) -> Vec<PathBuf> {
        let held = locked(&self.holds.0);
        let running = self
            .process_manager
            .release(tenant, service)
            .map(|release| release.dir);
        let busy = held.keys().any(|dir| dir.parent() == Some(service_dir));
        entries(service_dir)
            .into_iter()
            .filter(|(name, path)| match name.starts_with('.') {
                true => !busy,
                false => running.as_ref() != Some(path) && !held.contains_key(path),
            })
            .filter_map(|(name, path)| match name.starts_with('.') {
                true => Some(path),
                false => set_aside(&path),
            })
            .collect()
    }

    pub async fn retire(&self, tenant: &str, service: &str) {
        self.process_manager.stop(tenant, service).await;
        remove_dir(&self.service_dir(tenant, service)).await;
    }

    pub async fn retire_tenant(&self, tenant: &str) {
        let running: Vec<String> = self
            .process_manager
            .list()
            .into_iter()
            .filter(|app| app.tenant == tenant)
            .map(|app| app.service)
            .collect();
        for service in running {
            self.process_manager.stop(tenant, &service).await;
        }
        remove_dir(&self.apps_dir.join(tenant)).await;
    }
}

fn entries(dir: &Path) -> Vec<(String, PathBuf)> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|entry| {
                    (
                        entry.file_name().to_string_lossy().into_owned(),
                        entry.path(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn remove(path: &Path) {
    let removed = match path.is_dir() {
        true => std::fs::remove_dir_all(path),
        false => std::fs::remove_file(path),
    };
    match removed {
        Ok(()) => info!(path = %path.display(), "Removed a directory no app runs from"),
        Err(e) => {
            warn!(path = %path.display(), error = %e, "Cannot remove a directory no app runs from")
        }
    }
}

fn set_aside(path: &Path) -> Option<PathBuf> {
    let aside = path.parent()?.join(format!(".pruned-{}", Uuid::new_v4()));
    match std::fs::rename(path, &aside) {
        Ok(()) => Some(aside),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "Cannot move aside a directory no app runs from");
            None
        }
    }
}

async fn remove_dir(dir: &Path) {
    let Some(aside) = set_aside(dir) else {
        return;
    };
    match tokio::fs::remove_dir_all(&aside).await {
        Ok(()) => info!("Cleaned up app directory: {}", dir.display()),
        Err(e) => warn!("Failed to clean up app directory {}: {}", dir.display(), e),
    }
}

pub async fn watch_key(apps: Arc<Apps>, stop: CancellationToken) {
    let Source::TenantScoped(scoped) = apps.source.as_ref() else {
        return;
    };
    let mut ticks = tokio::time::interval(scoped.poll_every);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticks.tick() => {}
        }
        match scoped.api.web().await {
            Ok(_) => {
                if scoped.stand(Standing::Accepted) {
                    info!(tenant = %scoped.tenant(), "The platform accepts the runner key");
                }
            }
            Err(e) if key_refused(&e) => {
                if scoped.stand(Standing::Refused) {
                    error!(tenant = %scoped.tenant(), error = %e, "The runner key is not this runner's; stopping every app of the tenant until a key the platform accepts is in place");
                }
                apps.retire_tenant(scoped.tenant()).await;
            }
            Err(e) => {
                warn!(tenant = %scoped.tenant(), error = %format!("{e:#}"), "Cannot reach the runner API; the apps keep running as they are");
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::artifact::tests::tarball;
    use crate::artifact::ArchiveCaps;
    use crate::host::HostnameFormat;
    use crate::process_manager::tests::node_available;
    use crate::source::tests::shared;
    use crate::source::Bundle;
    use cufflink_shutdown::Shutdown;
    use cufflink_storage::ObjectStorage;
    use tempfile::TempDir;

    const SERVICE_ID: Uuid = Uuid::from_u128(7);

    fn bundle(marker: &str) -> Vec<u8> {
        let next = format!(
            "const [, , , , port, , host] = process.argv;\n\
             require(\"http\").createServer((req, res) => res.end(\"{marker}\")).listen(Number(port), host);\n"
        );
        tarball(&[
            (".next/BUILD_ID", marker),
            ("node_modules/next/package.json", "{}"),
            ("node_modules/next/dist/bin/next", &next),
        ])
    }

    struct World {
        apps: Arc<Apps>,
        storage: Arc<dyn ObjectStorage>,
        root: TempDir,
    }

    fn world(ports: std::ops::RangeInclusive<u16>) -> World {
        let root = TempDir::new().expect("apps dir");
        let storage = cufflink_storage::in_memory("platform");
        let caps = ArchiveCaps {
            max_entries: 100,
            max_bytes: 1 << 20,
        };
        let apps = Arc::new(Apps::new(
            Arc::new(shared(storage.clone(), caps.max_bytes)),
            ArtifactStore::new(caps),
            Arc::new(ProcessManager::new(
                ports,
                "apps.test".into(),
                "http".into(),
                HostnameFormat::Flat,
                crate::child_env::env_mode(None).expect("the built-in allowlist"),
                cufflink_supervisor::ChildSettings::default(),
                Shutdown::new(Duration::from_secs(5)),
            )),
            root.path().to_path_buf(),
        ));
        World {
            apps,
            storage,
            root,
        }
    }

    impl World {
        async fn uploaded(&self, version: i32, bundle: Vec<u8>, hashed: bool) -> WebDeployment {
            let key = format!("web/{SERVICE_ID}/v{version}.tar.gz");
            let sha256 = hex::encode(Sha256::digest(&bundle));
            self.storage
                .upload(&key, bundle, "application/gzip")
                .await
                .expect("upload");
            WebDeployment {
                tenant: "acme".into(),
                service: "site".into(),
                service_id: SERVICE_ID,
                version,
                sha256: hashed.then_some(sha256),
                bundle: Bundle::Object(key),
            }
        }

        async fn forget(&self, deployment: &WebDeployment) {
            let Bundle::Object(key) = &deployment.bundle else {
                unreachable!("a shared runner's bundle");
            };
            self.storage.delete(key).await.expect("delete");
        }

        fn service_dir(&self) -> PathBuf {
            self.root.path().join("acme").join("site")
        }

        fn kept(&self) -> Vec<String> {
            let mut kept: Vec<String> = entries(&self.service_dir())
                .into_iter()
                .map(|(name, _)| name)
                .collect();
            kept.sort();
            kept
        }
    }

    #[tokio::test]
    async fn a_bundle_is_unpacked_once_per_content_and_one_without_a_hash_each_time() {
        let w = world(21100..=21100);
        let hashed = w.uploaded(1, bundle("one"), true).await;
        let dir = w
            .apps
            .installed(&hashed)
            .await
            .expect("installed")
            .dir
            .clone();
        let content = hashed.sha256.clone().expect("a hash");
        assert_eq!(dir, w.service_dir().join(format!("{SERVICE_ID}-{content}")));
        assert!(dir.join(".next/BUILD_ID").is_file());
        w.forget(&hashed).await;
        assert_eq!(
            w.apps.installed(&hashed).await.expect("in place").dir,
            dir,
            "a directory of that content is not downloaded again"
        );

        let unhashed = w.uploaded(2, bundle("two"), false).await;
        let named = w
            .apps
            .installed(&unhashed)
            .await
            .expect("installed")
            .dir
            .clone();
        assert_eq!(
            named,
            w.service_dir().join(format!(
                "{SERVICE_ID}-{}",
                hex::encode(Sha256::digest(bundle("two")))
            )),
            "named by the hash of what arrived"
        );
        w.forget(&unhashed).await;
        assert!(
            w.apps.installed(&unhashed).await.is_err(),
            "a bundle recording no hash is downloaded each time"
        );

        let malformed = WebDeployment {
            sha256: Some("../../elsewhere".into()),
            ..hashed
        };
        assert!(w
            .apps
            .installed(&malformed)
            .await
            .err()
            .expect("no path of it")
            .to_string()
            .contains("not a SHA-256"));
    }

    #[tokio::test]
    async fn what_nothing_runs_from_or_holds_is_pruned_and_staging_is_kept_while_in_use() {
        let w = world(21101..=21101);
        let (held, idle) = (
            w.service_dir()
                .join(format!("{SERVICE_ID}-{}", "a".repeat(64))),
            w.service_dir()
                .join(format!("{SERVICE_ID}-{}", "b".repeat(64))),
        );
        for dir in [&held, &idle]
            .into_iter()
            .cloned()
            .chain(["v1", "v1.old", ".tmpX1"].map(|name| w.service_dir().join(name)))
        {
            std::fs::create_dir_all(dir.join(".next")).expect("a directory");
        }
        std::fs::create_dir_all(w.root.path().join("globex/site/v1")).expect("another tenant's");
        let hold = w.apps.holds.hold(held.clone());
        w.apps.prune("acme", "site").await;
        assert_eq!(
            w.kept(),
            [
                ".tmpX1".to_string(),
                held.file_name()
                    .expect("a name")
                    .to_string_lossy()
                    .into_owned()
            ],
            "what is held, and staging while anything of the service is held"
        );
        drop(hold);
        w.apps.prune("acme", "site").await;
        assert!(
            !w.root.path().join("acme").exists(),
            "an emptied service and tenant go too"
        );
        assert!(
            w.root.path().join("globex/site/v1").is_dir(),
            "another tenant's is left alone"
        );

        std::fs::create_dir_all(held.join(".next")).expect("a directory");
        w.apps.retire("acme", "site").await;
        assert_eq!(
            entries(&w.root.path().join("acme")),
            [],
            "a retired service is set aside, then removed, and nothing of it is left"
        );
        w.apps.retire_tenant("globex").await;
        assert_eq!(
            entries(w.root.path())
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            ["acme"]
        );
    }

    #[tokio::test]
    async fn a_deployment_whose_bundle_runs_changes_nothing_and_a_replaced_one_drains() {
        if !node_available().await {
            return;
        }
        let w = world(21102..=21104);
        let first = w.uploaded(1, bundle("one"), true).await;
        assert!(matches!(
            w.apps.run(&first).await.expect("started"),
            Ran::Started { .. }
        ));
        let running = w
            .apps
            .process_manager
            .release("acme", "site")
            .expect("runs");
        w.forget(&first).await;
        let reuploaded = WebDeployment {
            version: 2,
            ..first.clone()
        };
        assert_eq!(
            w.apps.run(&reuploaded).await.expect("nothing to do"),
            Ran::AlreadyRunning
        );
        assert_eq!(
            w.apps.process_manager.release("acme", "site"),
            Some(running.clone()),
            "the same process, of the same version"
        );

        let second = w.uploaded(3, bundle("three"), true).await;
        assert!(matches!(
            w.apps.run(&second).await.expect("started"),
            Ran::Started { .. }
        ));
        let replacing = w
            .apps
            .process_manager
            .release("acme", "site")
            .expect("runs");
        assert_ne!(replacing.dir, running.dir);
        assert!(running.dir.is_dir(), "kept while requests to it drain");
        tokio::time::timeout(OLD_VERSION_KEPT * 3, async {
            while running.dir.exists() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("pruned once drained");
        assert!(replacing.dir.is_dir());
        w.apps
            .process_manager
            .shutdown_all(Duration::from_secs(2))
            .await;
    }
}
