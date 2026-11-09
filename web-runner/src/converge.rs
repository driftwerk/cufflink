use crate::apps::{Apps, Ran};
use crate::source::{AppKey, Source, Standing, Unread, WebDeployment};
use crate::warmer::LazyWarmer;
use cufflink_supervisor::locked;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Cause {
    Reconcile,
    Event,
}

#[derive(Debug, Default)]
struct Pending {
    queue: Mutex<VecDeque<(AppKey, Cause)>>,
    ready: Notify,
}

impl Pending {
    fn push(&self, key: AppKey, cause: Cause) {
        {
            let mut queue = locked(&self.queue);
            match queue.iter_mut().find(|(queued, _)| *queued == key) {
                Some((_, queued)) => *queued = (*queued).max(cause),
                None => queue.push_back((key, cause)),
            }
        }
        self.ready.notify_one();
    }

    fn pop(&self) -> Option<(AppKey, Cause)> {
        locked(&self.queue).pop_front()
    }

    async fn next(&self) -> (AppKey, Cause) {
        loop {
            if let Some(next) = self.pop() {
                return next;
            }
            self.ready.notified().await;
        }
    }
}

pub struct Converger {
    apps: Arc<Apps>,
    warmer: Arc<LazyWarmer>,
    pending: Pending,
    failed: Mutex<HashMap<AppKey, String>>,
}

impl Converger {
    pub fn new(apps: Arc<Apps>, warmer: Arc<LazyWarmer>) -> Self {
        Self {
            apps,
            warmer,
            pending: Pending::default(),
            failed: Mutex::default(),
        }
    }

    pub fn announce(&self, payload: &Value) -> Result<AppKey, String> {
        let key = self.apps.source.announced(payload)?;
        locked(&self.failed).remove(&key);
        self.push(key.clone(), Cause::Event);
        Ok(key)
    }

    fn push(&self, key: AppKey, cause: Cause) {
        self.warmer.forget(&key);
        self.pending.push(key, cause);
    }

    fn switches(&self, deployment: &WebDeployment) -> bool {
        self.apps.outdated(deployment).is_some_and(|sha256| {
            locked(&self.failed)
                .get(&deployment.key())
                .map(String::as_str)
                != Some(sha256)
        })
    }

    pub async fn converging(self: Arc<Self>, stop: CancellationToken) {
        loop {
            let (key, cause) = tokio::select! {
                () = stop.cancelled() => return,
                next = self.pending.next() => next,
            };
            self.converge(key, cause).await;
        }
    }

    async fn converge(&self, key: AppKey, cause: Cause) {
        let (tenant, service) = (key.0.as_str(), key.1.as_str());
        let deployment = match self.apps.source.current(tenant, service).await {
            Ok(Some(deployment)) => deployment,
            Ok(None) => {
                if cause == Cause::Event {
                    warn!(
                        tenant,
                        service, "Refusing a deploy event: the app has no web deployment to run"
                    );
                }
                return;
            }
            Err(Unread::KeyRefused(e)) => {
                warn!(tenant, service, error = %e, "Cannot read the app's deployment: the platform refuses the runner key");
                return;
            }
            Err(Unread::Failed(e)) => {
                warn!(tenant, service, error = %format!("{e:#}"), "Cannot read the app's deployment; it keeps running as it is");
                return;
            }
        };
        if cause == Cause::Reconcile && !self.switches(&deployment) {
            return;
        }
        match (self.apps.deploy(&deployment).await, deployment.sha256) {
            (Some(Ran::Unhealthy { .. }), Some(sha256)) => {
                locked(&self.failed).insert(key, sha256);
            }
            (Some(Ran::AlreadyRunning | Ran::Started { .. }), _) => {
                locked(&self.failed).remove(&key);
            }
            (Some(Ran::Unhealthy { .. }) | None, _) => {}
        }
    }

    pub async fn reconciling(self: Arc<Self>, every: Duration, stop: CancellationToken) {
        let mut ticks = tokio::time::interval(every);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            self.reconcile().await;
        }
    }

    async fn reconcile(&self) {
        let running: Vec<AppKey> = self
            .apps
            .process_manager
            .list()
            .into_iter()
            .map(|app| (app.tenant, app.service))
            .collect();
        match (
            self.apps.source.currents(&running).await,
            self.apps.source.as_ref(),
        ) {
            (Ok(deployments), source) => {
                if let Source::TenantScoped(scoped) = source {
                    if scoped.stand(Standing::Accepted) {
                        info!(tenant = %scoped.tenant(), "The platform accepts the runner key");
                    }
                }
                deployments
                    .into_iter()
                    .filter(|deployment| self.switches(deployment))
                    .for_each(|deployment| {
                        info!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, "The app runs another bundle than its current version records; switching");
                        self.push(deployment.key(), Cause::Reconcile);
                    });
            }
            (Err(Unread::KeyRefused(e)), Source::TenantScoped(scoped)) => {
                if scoped.stand(Standing::Refused) {
                    error!(tenant = %scoped.tenant(), error = %e, "The runner key is not this runner's; stopping every app of the tenant until a key the platform accepts is in place");
                }
                self.apps.retire_tenant(scoped.tenant()).await;
            }
            (Err(Unread::KeyRefused(e) | Unread::Failed(e)), _) => {
                warn!(error = %format!("{e:#}"), "Cannot read the apps' current deployments; they keep running as they are");
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::apps::tests::next_bundle;
    use crate::artifact::{ArchiveCaps, ArtifactStore};
    use crate::host::HostnameFormat;
    use crate::process_manager::tests::node_available;
    use crate::process_manager::ProcessManager;
    use crate::source::tests::shared_at;
    use crate::warmer::WarmError;
    use cufflink_shutdown::Shutdown;
    use cufflink_storage::ObjectStorage;
    use cufflink_testkit::TestDb;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;
    use uuid::Uuid;

    fn key(tenant: &str, service: &str) -> AppKey {
        (tenant.into(), service.into())
    }

    #[tokio::test]
    async fn a_burst_for_one_app_is_queued_once_and_an_event_outranks_a_reconcile() {
        let pending = Pending::default();
        pending.push(key("acme", "site"), Cause::Reconcile);
        pending.push(key("acme", "blog"), Cause::Event);
        pending.push(key("acme", "site"), Cause::Event);
        pending.push(key("acme", "site"), Cause::Reconcile);
        assert_eq!(pending.next().await, (key("acme", "site"), Cause::Event));
        assert_eq!(pending.next().await, (key("acme", "blog"), Cause::Event));
        assert_eq!(pending.pop(), None);
        pending.push(key("acme", "site"), Cause::Reconcile);
        assert_eq!(
            pending.next().await,
            (key("acme", "site"), Cause::Reconcile),
            "a push once its app was taken queues it again"
        );
    }

    fn marking(marker: &str, log: &Path, then: &str) -> Vec<u8> {
        next_bundle(
            marker,
            &format!(
                "require(\"fs\").appendFileSync({log:?}, \"{marker}\\n\");\n\
                 const [, , , , port, , host] = process.argv;\n{then}\n"
            ),
        )
    }

    fn serving(marker: &str, log: &Path) -> Vec<u8> {
        marking(
            marker,
            log,
            &format!("require(\"http\").createServer((req, res) => res.end(\"{marker}\")).listen(Number(port), host);"),
        )
    }

    fn slow(marker: &str, log: &Path) -> Vec<u8> {
        marking(
            marker,
            log,
            &format!("setTimeout(() => require(\"http\").createServer((req, res) => res.end(\"{marker}\")).listen(Number(port), host), 1500);"),
        )
    }

    fn failing(marker: &str, log: &Path) -> Vec<u8> {
        marking(marker, log, "process.exit(1);")
    }

    struct World {
        converger: Arc<Converger>,
        apps: Arc<Apps>,
        warmer: Arc<LazyWarmer>,
        storage: Arc<dyn ObjectStorage>,
        db: TestDb,
        tenant: String,
        service: Uuid,
        log: PathBuf,
        _root: TempDir,
    }

    async fn world(ports: std::ops::RangeInclusive<u16>) -> Option<World> {
        if !node_available().await {
            return None;
        }
        let infra = cufflink_testkit::infra().await?;
        let mut db = infra.create_migrated_database().await;
        let tenant = db.mint_tenant().await;
        let service: Uuid = db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "INSERT INTO services (tenant_id, name, current_version)
                 VALUES ($1, 'site', 0) RETURNING id",
                &[&tenant.id],
            )
            .await
            .expect("a service")
            .get(0);
        let root = TempDir::new().expect("apps dir");
        let storage = cufflink_storage::in_memory("platform");
        let caps = ArchiveCaps {
            max_entries: 100,
            max_bytes: 1 << 20,
        };
        let apps = Arc::new(Apps::new(
            Arc::new(shared_at(&db.url, storage.clone(), caps.max_bytes)),
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
            root.path().join("apps"),
        ));
        let warmer = Arc::new(LazyWarmer::new(apps.clone()));
        Some(World {
            converger: Arc::new(Converger::new(apps.clone(), warmer.clone())),
            apps,
            warmer,
            storage,
            db,
            tenant: tenant.slug,
            service,
            log: root.path().join("starts.log"),
            _root: root,
        })
    }

    impl World {
        fn key(&self) -> AppKey {
            key(&self.tenant, "site")
        }

        async fn execute(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) {
            self.db
                .pool()
                .get()
                .await
                .expect("checkout")
                .execute(sql, params)
                .await
                .expect(sql);
        }

        async fn release(&self, version: i32, bundle: Option<Vec<u8>>) -> Option<String> {
            let stored = match bundle {
                Some(bundle) => {
                    let (key, sha256) = (
                        format!("web/{}/v{version}.tar.gz", self.service),
                        hex::encode(Sha256::digest(&bundle)),
                    );
                    self.storage
                        .upload(&key, bundle, "application/gzip")
                        .await
                        .expect("upload");
                    Some((key, sha256))
                }
                None => None,
            };
            let (key, sha256) = stored.clone().unzip();
            self.execute(
                "INSERT INTO deployments
                   (service_id, version, manifest_hash, manifest, artifact_s3_key, artifact_hash)
                 VALUES ($1, $2, 'h', '{\"mode\": \"web\"}', $3, $4)",
                &[&self.service, &version, &key, &sha256],
            )
            .await;
            self.execute(
                "UPDATE services SET current_version = $2 WHERE id = $1",
                &[&self.service, &version],
            )
            .await;
            sha256
        }

        async fn current(&self) -> WebDeployment {
            self.apps
                .source
                .current(&self.tenant, "site")
                .await
                .map_err(|e| format!("{e:?}"))
                .expect("read")
                .expect("a deployment")
        }

        async fn start(&self, version: i32, bundle: Vec<u8>) {
            self.release(version, Some(bundle)).await;
            assert!(matches!(
                self.apps.run(&self.current().await).await.expect("run"),
                Ran::Started { .. }
            ));
        }

        fn running(&self) -> Option<i32> {
            self.apps
                .process_manager
                .release(&self.tenant, "site")
                .map(|release| release.version)
        }

        fn starts(&self) -> Vec<String> {
            std::fs::read_to_string(&self.log)
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect()
        }

        fn queued(&self) -> Vec<(AppKey, Cause)> {
            locked(&self.converger.pending.queue)
                .iter()
                .cloned()
                .collect()
        }

        fn converging(&self) -> CancellationToken {
            let stop = CancellationToken::new();
            tokio::spawn(self.converger.clone().converging(stop.clone()));
            stop
        }

        async fn until_running(&self, version: i32) {
            tokio::time::timeout(Duration::from_secs(20), async {
                while self.running() != Some(version) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("v{version} never ran; v{:?} does", self.running()));
        }

        async fn until_starts(&self, starts: usize) {
            tokio::time::timeout(Duration::from_secs(20), async {
                while self.starts().len() < starts {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{starts} starts expected: {:?}", self.starts()));
        }

        async fn drain(&self) {
            while let Some((key, cause)) = self.converger.pending.pop() {
                self.converger.converge(key, cause).await;
            }
        }

        fn event(&self) -> Value {
            json!({"tenant_slug": self.tenant, "service_name": "site"})
        }

        async fn stop(&self) {
            self.apps
                .process_manager
                .shutdown_all(Duration::from_secs(2))
                .await;
        }
    }

    #[tokio::test]
    async fn a_missed_event_is_caught_by_the_next_reconcile() {
        let Some(w) = world(21200..=21202).await else {
            return;
        };
        w.start(1, serving("one", &w.log)).await;
        w.release(2, Some(serving("two", &w.log))).await;
        w.converger.reconcile().await;
        assert_eq!(w.queued(), [(w.key(), Cause::Reconcile)]);
        w.drain().await;
        assert_eq!(w.running(), Some(2));
        w.converger.reconcile().await;
        assert_eq!(
            w.queued(),
            [],
            "an app that runs its current bundle is left alone"
        );
        w.stop().await;
    }

    #[tokio::test]
    async fn reconcile_never_stops_an_app_whose_current_version_has_no_bundle() {
        let Some(w) = world(21203..=21204).await else {
            return;
        };
        w.start(1, serving("one", &w.log)).await;
        w.release(2, None).await;
        w.converger.reconcile().await;
        assert_eq!(w.queued(), []);
        w.execute(
            "UPDATE services SET current_version = 9 WHERE id = $1",
            &[&w.service],
        )
        .await;
        w.converger.reconcile().await;
        assert_eq!(w.queued(), [], "nor one whose current version is gone");
        assert_eq!(w.running(), Some(1));
        w.stop().await;
    }

    #[tokio::test]
    async fn reconcile_does_not_retry_a_bundle_that_failed_its_health_check_until_an_event() {
        let Some(w) = world(21205..=21207).await else {
            return;
        };
        w.start(1, serving("one", &w.log)).await;
        w.release(2, Some(failing("two", &w.log))).await;
        w.converger.reconcile().await;
        w.drain().await;
        assert_eq!(w.starts(), ["one", "two"]);
        assert_eq!(w.running(), Some(1), "the running process is kept");
        w.converger.reconcile().await;
        assert_eq!(w.queued(), [], "the failed bundle is not tried again");

        assert_eq!(w.converger.announce(&w.event()), Ok(w.key()));
        w.drain().await;
        assert_eq!(w.starts(), ["one", "two", "two"], "an event tries it again");
        w.converger.reconcile().await;
        assert_eq!(w.queued(), []);

        w.release(3, Some(serving("three", &w.log))).await;
        w.converger.reconcile().await;
        w.drain().await;
        assert_eq!(w.running(), Some(3), "and so does another bundle");
        w.stop().await;
    }

    #[tokio::test]
    async fn reconcile_starts_no_app_that_is_not_running() {
        let Some(w) = world(21208..=21209).await else {
            return;
        };
        w.release(1, Some(serving("one", &w.log))).await;
        w.converger.reconcile().await;
        assert_eq!(w.queued(), []);
        w.converger.push(w.key(), Cause::Reconcile);
        w.drain().await;
        assert_eq!(
            (w.running(), w.starts()),
            (None, Vec::new()),
            "a switch queued for an app that has stopped since starts nothing"
        );
        w.converger.announce(&w.event()).expect("an event");
        w.drain().await;
        assert_eq!(w.running(), Some(1), "an event starts it");
        w.stop().await;
    }

    #[tokio::test]
    async fn an_event_and_a_reconcile_for_one_app_start_it_once() {
        let Some(w) = world(21210..=21213).await else {
            return;
        };
        w.start(1, serving("one", &w.log)).await;
        w.release(2, Some(slow("two", &w.log))).await;
        let stop = w.converging();
        w.converger.announce(&w.event()).expect("an event");
        w.until_starts(2).await;
        w.release(3, Some(serving("three", &w.log))).await;
        w.converger.reconcile().await;
        w.converger.announce(&w.event()).expect("an event");
        assert_eq!(
            w.queued(),
            [(w.key(), Cause::Event)],
            "both wait, as one, for the start in progress"
        );
        w.until_running(3).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(w.starts(), ["one", "two", "three"]);
        stop.cancel();
        w.stop().await;
    }

    #[tokio::test]
    async fn a_source_it_cannot_read_leaves_the_running_app_as_it_is() {
        let Some(w) = world(21216..=21217).await else {
            return;
        };
        w.start(1, serving("one", &w.log)).await;
        w.release(2, Some(serving("two", &w.log))).await;
        w.execute("ALTER TABLE deployments RENAME TO deployments_away", &[])
            .await;
        w.converger.reconcile().await;
        assert_eq!(w.queued(), []);
        w.converger.announce(&w.event()).expect("an event");
        w.drain().await;
        assert_eq!((w.running(), w.starts()), (Some(1), vec!["one".into()]));
        w.stop().await;
    }

    #[tokio::test]
    async fn a_deploy_event_clears_the_warmers_not_found_entry() {
        let Some(w) = world(21214..=21215).await else {
            return;
        };
        let warm = || w.warmer.ensure_warm(&w.tenant, "site");
        assert!(matches!(warm().await, Err(WarmError::NotFound { .. })));
        w.release(1, Some(serving("one", &w.log))).await;
        assert!(
            matches!(warm().await, Err(WarmError::NotFound { .. })),
            "remembered for a while"
        );
        w.converger.announce(&w.event()).expect("an event");
        warm().await.expect("started on the next request");
        assert_eq!(w.running(), Some(1));
        w.stop().await;
    }
}
