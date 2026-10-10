use crate::apps::{self, Kept};
use crate::child::{identity, NativeChildren, Starts};
use crate::config::Config;
use crate::deployment::{self, Deployment, Desired};
use crate::plan::{forgotten, plan, Action};
use crate::ready::{self, Mismatch};
use crate::relay::{Destination, Relay, Target};
use crate::restart::{Failure, Hold, Wait};
use crate::socket::{self, Endpoint};
use crate::status::{self, View};
use async_nats::{Message, Subscriber};
use cufflink_shutdown::Shutdown;
use cufflink_supervisor::runner_api::{key_refused, RunnerApi};
use cufflink_supervisor::{
    child_env, locked, runner_env, Exit, ExitReason, Launch, StartError, Stop, Supervised,
    Supervisor, Unready,
};
use cufflink_telemetry::Component;
use futures_util::future::{join_all, OptionFuture};
use futures_util::{FutureExt, Stream, StreamExt};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const HEALTH_FAILURES: u32 = 3;
const UNREADY: &str = "unready";

struct Background {
    stop: DropGuard,
    task: JoinHandle<()>,
}

impl Background {
    fn spawn<F>(
        shutdown: &Shutdown,
        name: &'static str,
        work: impl FnOnce(CancellationToken) -> F,
    ) -> Self
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let stop = CancellationToken::new();
        let task = shutdown.spawn(name, work(stop.clone()));
        Self {
            stop: stop.drop_guard(),
            task,
        }
    }

    async fn finish(self) {
        let Self { stop, task } = self;
        drop(stop);
        let _ = task.await;
    }
}

struct Route {
    destination: Destination,
    permits: Arc<Semaphore>,
    subscriptions: BTreeMap<String, Background>,
    health: Background,
}

impl Route {
    async fn finish(self) {
        let Self {
            destination,
            subscriptions,
            health,
            ..
        } = self;
        join_all(subscriptions.into_values().map(Background::finish)).await;
        drop(health);
        destination.current().settled().await;
    }
}

#[derive(Debug, thiserror::Error)]
#[error("its calls cannot be routed: {0}")]
struct Unrouted(String);

#[derive(Default)]
pub struct Shared {
    routes: Mutex<HashMap<Uuid, Route>>,
    board: Mutex<Board>,
    draining: Mutex<BTreeMap<PathBuf, Arc<Deployment>>>,
    changed: Notify,
}

struct Draining {
    shared: Arc<Shared>,
    socket: PathBuf,
}

impl Drop for Draining {
    fn drop(&mut self) {
        locked(&self.shared.draining).remove(&self.socket);
    }
}

#[derive(Default)]
struct Board {
    holds: HashMap<Uuid, Hold>,
    starting: BTreeMap<Uuid, Arc<Deployment>>,
}

impl Shared {
    fn draining(self: &Arc<Self>, child: &Supervised<NativeChildren>) -> Draining {
        locked(&self.draining).insert(child.endpoint.clone(), child.info.clone());
        Draining {
            shared: self.clone(),
            socket: child.endpoint.clone(),
        }
    }

    fn unroute(&self, service_id: Uuid, socket: &Path) -> Option<Route> {
        let mut routes = locked(&self.routes);
        match routes.get(&service_id) {
            Some(route) if route.destination.current().socket() == socket => {
                routes.remove(&service_id)
            }
            Some(_) | None => None,
        }
    }

    fn routed(&self, service_id: Uuid) -> BTreeSet<String> {
        locked(&self.routes)
            .get(&service_id)
            .map(|route| route.subscriptions.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn hold(&self, deployment: Arc<Deployment>, failure: Failure, reason: &'static str) {
        let id = deployment.service_id;
        let hold = {
            let mut board = locked(&self.board);
            let hold = Hold::after(board.holds.get(&id), deployment, failure, Instant::now());
            board.holds.insert(id, hold.clone());
            hold
        };
        let deployment = &hold.deployment;
        let reason = match hold.wait {
            Wait::Until(_) => Some(reason),
            Wait::CrashLooping => {
                error!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, "Native service keeps failing right after it starts; not starting it again until its next deploy");
                Some(ExitReason::CrashLoop.as_str())
            }
            Wait::Refused => None,
        };
        if let Some(reason) = reason {
            metrics::counter!("cufflink_ext_runner_restarts_total", "reason" => reason)
                .increment(1);
        }
        self.changed.notify_one();
    }

    pub fn exited(&self, exit: Exit<NativeChildren>) {
        let deployment = exit.info.clone();
        if !exit.current {
            debug!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, "A replaced child exited before its stop");
            return;
        }
        drop(self.unroute(exit.key, &exit.endpoint));
        let reason = exit.reason().as_str();
        warn!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, status = ?exit.status, reason, uptime = ?exit.uptime, "Native service exited");
        self.hold(
            deployment,
            Failure::Exited {
                uptime: exit.uptime,
            },
            reason,
        );
    }

    fn next_retry(&self, now: Instant) -> Option<Instant> {
        locked(&self.board)
            .holds
            .values()
            .filter_map(|hold| hold.ends_after(now))
            .min()
    }

    fn forget(&self, desired: &Desired) {
        let mut board = locked(&self.board);
        forgotten(&board.holds, desired).iter().for_each(|id| {
            board.holds.remove(id);
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reconciled {
    Converged,
    Unread,
    KeyRefused,
}

pub struct Sources {
    pub platform: RunnerApi,
    pub relay: Relay,
}

pub struct Fleet {
    shared: Arc<Shared>,
    children: Arc<Supervisor<NativeChildren>>,
    sources: Sources,
    config: Arc<Config>,
    shutdown: Shutdown,
    key_refused: AtomicBool,
}

impl Fleet {
    pub fn new(config: Arc<Config>, sources: Sources, shutdown: Shutdown) -> Self {
        let shared = Arc::new(Shared::default());
        let children = Arc::new(Supervisor::new(
            Arc::new(NativeChildren::new(
                shared.clone(),
                config.children.output_level,
            )),
            Starts::default(),
            shutdown.clone(),
        ));
        Self {
            shared,
            children,
            sources,
            config,
            shutdown,
            key_refused: AtomicBool::new(false),
        }
    }

    pub async fn run(
        &self,
        mut events: impl Stream<Item = Message> + Unpin,
        reconciled: &AtomicBool,
        stop: CancellationToken,
    ) {
        let mut ticks = tokio::time::interval(self.config.reconcile_every);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let retry = self.shared.next_retry(Instant::now());
            tokio::select! {
                biased;
                () = stop.cancelled() => break,
                () = self.shared.changed.notified() => {
                    self.record();
                    continue;
                }
                Some(event) = events.next() => {
                    let coalesced = std::iter::from_fn(|| events.next().now_or_never().flatten()).count();
                    debug!(subject = %event.subject, coalesced, "A deploy event brings the reconcile forward");
                }
                _ = ticks.tick() => {}
                () = tokio::time::sleep_until(retry.unwrap_or_else(Instant::now)), if retry.is_some() => {}
            }
            tokio::select! {
                () = stop.cancelled() => break,
                done = self.reconcile() => match done {
                    Reconciled::Converged => reconciled.store(true, Ordering::SeqCst),
                    Reconciled::KeyRefused => reconciled.store(false, Ordering::SeqCst),
                    Reconciled::Unread => {}
                },
            }
        }
    }

    async fn reconcile(&self) -> Reconciled {
        let (desired, reconciled) = match deployment::desired(&self.sources.platform).await {
            Ok(desired) => {
                if self.key_refused.swap(false, Ordering::SeqCst) {
                    info!("The platform accepts the runner key");
                }
                (desired, Reconciled::Converged)
            }
            Err(e) if key_refused(&e) => {
                if !self.key_refused.swap(true, Ordering::SeqCst) {
                    error!(error = %e, "The runner key is not this runner's; stopping every native service and leaving its call subjects until a key the platform accepts is in place");
                }
                (Desired::default(), Reconciled::KeyRefused)
            }
            Err(e) => {
                warn!(error = %e, "Cannot read which native services should run; the children keep running as they are");
                return Reconciled::Unread;
            }
        };
        self.shared.forget(&desired);
        let running = self.running();
        let actions = plan(
            &desired,
            &running,
            &locked(&self.shared.board).holds,
            Instant::now(),
        );
        join_all(actions.into_iter().map(|action| self.execute(action))).await;
        let running = self.running();
        let draining: Vec<Arc<Deployment>> =
            locked(&self.shared.draining).values().cloned().collect();
        let (apps_dir, kept, tenant) = (
            self.config.apps_dir.clone(),
            Kept::of(&desired, running.values().chain(&draining)),
            self.config.tenant.slug.clone(),
        );
        if let Err(e) =
            tokio::task::spawn_blocking(move || apps::prune(&apps_dir, &kept, &tenant)).await
        {
            warn!(error = %e, "Pruning APPS_DIR failed");
        }
        self.record();
        reconciled
    }

    fn running(&self) -> BTreeMap<Uuid, Arc<Deployment>> {
        self.children
            .list(|id, child| (*id, child.info.clone()))
            .into_iter()
            .collect()
    }

    async fn execute(&self, action: Action) {
        match action {
            Action::Start(deployment) | Action::Replace(deployment) => {
                self.launch(deployment).await
            }
            Action::Stop(service_id) => self.stop(service_id),
        }
    }

    async fn launch(&self, deployment: Arc<Deployment>) {
        let id = deployment.service_id;
        locked(&self.shared.board)
            .starting
            .insert(id, deployment.clone());
        let failure = self.start(&deployment).await;
        locked(&self.shared.board).starting.remove(&id);
        if let Some(failure) = failure {
            self.shared.hold(deployment, failure, UNREADY);
        }
    }

    async fn start(&self, deployment: &Arc<Deployment>) -> Option<Failure> {
        match self.prepare(deployment).await {
            Ok((binary, configs)) => self.start_from(deployment, binary, &configs).await,
            Err(e) => {
                error!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, error = %format!("{e:#}"), "Cannot prepare the native service's child; trying again at the next reconcile");
                None
            }
        }
    }

    async fn start_from(
        &self,
        deployment: &Arc<Deployment>,
        binary: PathBuf,
        configs: &HashMap<String, String>,
    ) -> Option<Failure> {
        let start = self.children.endpoints(Starts::next);
        let socket = self.config.socket_dir.join(socket::name(
            deployment.service_id,
            deployment.version,
            start,
        ));
        let endpoint = match Endpoint::generate(&socket) {
            Ok(endpoint) => endpoint,
            Err(e) => {
                error!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, error = %format!("{e:#}"), "Cannot make the native service's child a token; trying again at the next reconcile");
                return None;
            }
        };
        let launch = Launch {
            key: deployment.service_id,
            endpoint: socket.clone(),
            info: deployment.clone(),
            cwd: binary
                .parent()
                .map_or_else(|| self.config.apps_dir.clone(), Path::to_path_buf),
            program: binary,
            args: Vec::new(),
            env: child_env(
                runner_env(),
                &self.config.env_mode,
                configs,
                self.config
                    .children
                    .telemetry
                    .naming(&deployment.scope(), Component::ExtRunner),
                identity(&endpoint, deployment),
            ),
        };
        info!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, socket = %socket.display(), "Starting a native service");
        let deadline = Instant::now() + self.config.ready_timeout;
        let ready = async {
            ready::until_ready(&endpoint, &deployment.extensions, self.config.ready_timeout)
                .await?;
            let routed = self.shared.routed(deployment.service_id);
            self.subscribe(
                deployment,
                deployment.extensions.difference(&routed),
                deadline,
            )
            .await
        };
        match self.children.start(launch, ready).await {
            Ok((replaced, added)) => {
                self.switch(deployment, &endpoint, added, replaced).await;
                None
            }
            Err(StartError::Unready(Unready::Probe(report))) => {
                match (
                    report.downcast_ref::<Mismatch>(),
                    report.downcast_ref::<Unrouted>(),
                ) {
                    (Some(mismatch), _) => {
                        error!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, %mismatch, "Native service refused: it does not serve what its deployment provides; the running child keeps serving until the next deploy");
                        Some(Failure::Refused)
                    }
                    (None, Some(unrouted)) => {
                        error!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, error = %unrouted, "Native service is ready but its calls cannot be routed; trying again at the next reconcile");
                        None
                    }
                    (None, None) => {
                        warn!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, error = %report, "Native service did not answer ready");
                        Some(Failure::Unready)
                    }
                }
            }
            Err(StartError::Unready(unready)) => {
                warn!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, ?unready, "Native service ended before it answered ready");
                Some(Failure::Unready)
            }
            Err(StartError::Spawn(e)) => {
                error!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, error = %e, "Native service's binary cannot be started");
                Some(Failure::Unready)
            }
            Err(StartError::Closed) => None,
        }
    }

    async fn prepare(
        &self,
        deployment: &Deployment,
    ) -> eyre::Result<(PathBuf, HashMap<String, String>)> {
        let binary =
            apps::installed(&self.sources.platform, &self.config.apps_dir, deployment).await?;
        let configs = self
            .sources
            .platform
            .native_environment(deployment.service_id)
            .await?;
        Ok((binary, configs))
    }

    async fn subscribe<'a>(
        &self,
        deployment: &Deployment,
        names: impl IntoIterator<Item = &'a String>,
        deadline: Instant,
    ) -> eyre::Result<Vec<(String, Subscriber)>> {
        self.sources
            .relay
            .subscribe(&deployment.tenant, names.into_iter().cloned(), deadline)
            .await
            .map_err(|e| Unrouted(format!("{e:#}")).into())
    }

    fn attach(
        &self,
        route: &mut Route,
        deployment: &Deployment,
        calls: Vec<(String, Subscriber)>,
    ) -> Vec<Background> {
        let tenant: Arc<str> = Arc::from(deployment.tenant.as_str());
        calls
            .into_iter()
            .filter_map(|(name, calls)| {
                let (relay, destination, permits, tenant) = (
                    self.sources.relay.clone(),
                    route.destination.clone(),
                    route.permits.clone(),
                    tenant.clone(),
                );
                let subscription = Background::spawn(&self.shutdown, "ext_relay", |stop| {
                    relay.run(calls, tenant, destination, permits, stop)
                });
                route.subscriptions.insert(name, subscription)
            })
            .collect()
    }

    fn health(&self, deployment: &Arc<Deployment>, endpoint: &Endpoint) -> Background {
        let (children, deployment, endpoint, interval) = (
            self.children.clone(),
            deployment.clone(),
            endpoint.clone(),
            self.config.health_interval,
        );
        Background::spawn(&self.shutdown, "ext_health", |stop| {
            watch(children, deployment, endpoint, interval, stop)
        })
    }

    async fn switch(
        &self,
        deployment: &Arc<Deployment>,
        endpoint: &Endpoint,
        added: Vec<(String, Subscriber)>,
        replaced: Option<Supervised<NativeChildren>>,
    ) {
        let (id, socket, target, health) = (
            deployment.service_id,
            endpoint.socket(),
            Target::new(endpoint.clone()),
            self.health(deployment, endpoint),
        );
        let (retired, dropped, missing) = {
            let mut routes = locked(&self.shared.routes);
            let (retired, mut route) = match routes.remove(&id) {
                Some(route) => (
                    Some(route.destination.retarget(target.clone())),
                    Route { health, ..route },
                ),
                None => (
                    None,
                    Route {
                        destination: Destination::new(target.clone()),
                        permits: Arc::new(Semaphore::new(self.config.max_concurrent_calls)),
                        subscriptions: BTreeMap::new(),
                        health,
                    },
                ),
            };
            let mut dropped = self.attach(&mut route, deployment, added);
            let stale: Vec<String> = route
                .subscriptions
                .keys()
                .filter(|name| !deployment.extensions.contains(*name))
                .cloned()
                .collect();
            dropped.extend(
                stale
                    .iter()
                    .filter_map(|name| route.subscriptions.remove(name)),
            );
            let missing: Vec<String> = deployment
                .extensions
                .iter()
                .filter(|name| !route.subscriptions.contains_key(*name))
                .cloned()
                .collect();
            routes.insert(id, route);
            (retired, dropped, missing)
        };
        if let Some(old) = &replaced {
            info!(tenant = %deployment.tenant, service = %deployment.service, old_version = old.info.version, version = deployment.version, "Replaced the native service's child; stopping the old one once its calls are answered");
        }
        self.retire(
            async move {
                join_all(dropped.into_iter().map(Background::finish)).await;
                OptionFuture::from(retired.map(|retired| async move { retired.settled().await }))
                    .await;
            },
            replaced,
        );
        let serving = match self.children.endpoint(&id).as_deref() == Some(socket) {
            false => {
                drop(self.shared.unroute(id, socket));
                false
            }
            true if missing.is_empty() => true,
            true => self.complete(deployment, socket, &missing).await,
        };
        if serving {
            info!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, extensions = ?deployment.extensions, "Native service serving");
        }
    }

    async fn complete(
        &self,
        deployment: &Arc<Deployment>,
        socket: &Path,
        missing: &[String],
    ) -> bool {
        let id = deployment.service_id;
        let deadline = Instant::now() + self.config.ready_timeout;
        match self.subscribe(deployment, missing, deadline).await {
            Ok(calls) => {
                let mut routes = locked(&self.shared.routes);
                match routes.get_mut(&id) {
                    Some(route) if route.destination.current().socket() == socket => {
                        drop(self.attach(route, deployment, calls));
                        true
                    }
                    Some(_) | None => false,
                }
            }
            Err(e) => {
                error!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, error = %e, "Native service's calls cannot all be routed; stopping it, to start it again at the next reconcile");
                let route = self.shared.unroute(id, socket);
                let child = self.children.take(&id, &socket.to_path_buf());
                self.retire(
                    OptionFuture::from(route.map(Route::finish)).map(drop),
                    child,
                );
                false
            }
        }
    }

    fn stop(&self, service_id: Uuid) {
        let route = locked(&self.shared.routes).remove(&service_id);
        let child = self.children.remove(&service_id);
        if let Some(child) = &child {
            let deployment = &child.info;
            info!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, "Stopping a native service that should no longer run");
        }
        self.retire(
            OptionFuture::from(route.map(Route::finish)).map(drop),
            child,
        );
    }

    fn retire(
        &self,
        settled: impl std::future::Future<Output = ()> + Send + 'static,
        child: Option<Supervised<NativeChildren>>,
    ) {
        let (draining, timeout) = (
            child.as_ref().map(|child| self.shared.draining(child)),
            self.config.child_stop_timeout,
        );
        self.shutdown.spawn("ext_retire", async move {
            settled.await;
            if let Some(child) = child {
                child.stop(Stop::Terminate(timeout)).await;
            }
            drop(draining);
        });
    }

    pub async fn unroute_all(&self) {
        let routes: Vec<Route> = locked(&self.shared.routes)
            .drain()
            .map(|(_, route)| route)
            .collect();
        join_all(routes.into_iter().map(Route::finish)).await;
    }

    pub async fn stop_all(&self) {
        let timeout = self.config.child_stop_timeout;
        join_all(
            self.children
                .close()
                .into_iter()
                .map(|(_, child)| child.stop(Stop::Terminate(timeout))),
        )
        .await;
    }

    fn views(&self) -> Vec<View> {
        let running = self
            .children
            .list(|_, child| (child.info.clone(), child.pid));
        let board = locked(&self.shared.board);
        status::views(running, &board.starting, &board.holds)
    }

    pub fn status(&self) -> Value {
        json!({ "children": self.views().iter().map(View::json).collect::<Vec<_>>() })
    }

    fn record(&self) {
        for (state, count) in status::counts(&self.views()) {
            metrics::gauge!("cufflink_ext_runner_children", "state" => state.as_str())
                .set(count as f64);
        }
    }
}

async fn watch(
    children: Arc<Supervisor<NativeChildren>>,
    deployment: Arc<Deployment>,
    endpoint: Endpoint,
    interval: Duration,
    stop: CancellationToken,
) {
    let mut ticks = tokio::time::interval_at(Instant::now() + interval, interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut failures = 0;
    while failures < HEALTH_FAILURES {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticks.tick() => {}
        }
        let probed =
            tokio::time::timeout(interval, ready::probe(&endpoint, &deployment.extensions))
                .await
                .map_err(|_| format!("no answer within {interval:?}"))
                .and_then(|probed| probed.map_err(|e| e.to_string()));
        failures = match probed {
            Ok(()) => 0,
            Err(e) => {
                warn!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, error = %e, failures = failures + 1, "Native service failed a health probe");
                failures + 1
            }
        };
    }
    error!(tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, "Native service stopped answering; killing it");
    children.kill(&deployment.service_id, &endpoint.socket().to_path_buf());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::child::EXT_ENV;
    use cufflink_supervisor::runner_api::{RunnerKey, RunnerTenant};
    use cufflink_supervisor::{ArtifactCap, EnvMode};
    use tempfile::TempDir;

    const NO_NATS: &str = "nats://127.0.0.1:1";

    fn echo_extensions() -> BTreeSet<String> {
        ["echo.fail", "echo.reply", "echo.sleep"]
            .map(String::from)
            .into()
    }

    fn deployment() -> Arc<Deployment> {
        Arc::new(Deployment {
            service_id: Uuid::new_v4(),
            tenant: "acme".into(),
            service: "ext-echo".into(),
            version: 1,
            sha256: "0".repeat(64),
            size: 1,
            extensions: echo_extensions(),
        })
    }

    struct Harness {
        fleet: Fleet,
        dir: TempDir,
    }

    async fn harness(nats_url: &str, ready_timeout: Duration) -> Harness {
        harness_of("http://127.0.0.1:1", nats_url, ready_timeout).await
    }

    async fn harness_of(platform_url: &str, nats_url: &str, ready_timeout: Duration) -> Harness {
        let dir = tempfile::Builder::new()
            .prefix("cxf")
            .tempdir_in("/tmp")
            .expect("a short directory");
        let (runner_key, cap) = (
            RunnerKey::Value("cfr_AAAAAAAAAAAA_s3cret".into()),
            ArtifactCap {
                max_bytes: 1 << 20,
                setting: "EXT_ARTIFACT_MAX_BYTES",
            },
        );
        let client =
            cufflink_nats::connect(nats_url, "fleet-test", &cufflink_nats::NatsAuth::Anonymous)
                .await
                .expect("a client");
        let tenant = RunnerTenant::parse("EXT_TENANT", "acme".into()).expect("a slug");
        let config = Config {
            tenant: tenant.clone(),
            platform_url: platform_url.into(),
            runner_key: runner_key.clone(),
            nats_url: nats_url.into(),
            nats_auth: cufflink_nats::NatsAuth::Anonymous,
            runner_port: 0,
            apps_dir: dir.path().join("apps"),
            socket_dir: dir.path().to_path_buf(),
            env_mode: EnvMode::parse(EXT_ENV, None).expect("the allowlist"),
            children: Default::default(),
            artifact_max_bytes: 1 << 20,
            reconcile_every: Duration::from_secs(30),
            ready_timeout,
            health_interval: Duration::from_secs(30),
            child_stop_timeout: Duration::from_secs(5),
            max_concurrent_calls: 4,
            shutdown_grace: Duration::from_secs(30),
            status_token: None,
        };
        let fleet = Fleet::new(
            Arc::new(config),
            Sources {
                platform: RunnerApi::new(platform_url, runner_key, tenant, cap).expect("a client"),
                relay: Relay::new(client),
            },
            Shutdown::new(Duration::from_secs(30)),
        );
        Harness { fleet, dir }
    }

    impl Harness {
        async fn sh(
            &self,
            deployment: &Arc<Deployment>,
            script: &str,
        ) -> (Endpoint, Option<Supervised<NativeChildren>>) {
            let start = self.fleet.children.endpoints(Starts::next);
            let socket = self.dir.path().join(format!("{start}.sock"));
            let launch = Launch {
                key: deployment.service_id,
                endpoint: socket.clone(),
                info: deployment.clone(),
                program: "sh".into(),
                args: vec!["-c".into(), script.into()],
                cwd: self.dir.path().to_path_buf(),
                env: [("PATH".to_string(), "/usr/bin:/bin".to_string())].into(),
            };
            let (replaced, ()) = self
                .fleet
                .children
                .start(launch, async { Ok(()) })
                .await
                .expect("started");
            (Endpoint::new(&socket, "t0ken"), replaced)
        }

        fn routes(&self) -> usize {
            locked(&self.fleet.shared.routes).len()
        }

        fn holds(&self) -> usize {
            locked(&self.fleet.shared.board).holds.len()
        }

        fn draining(&self) -> usize {
            locked(&self.fleet.shared.draining).len()
        }
    }

    fn is_alive(pid: Option<u32>) -> bool {
        let pid = libc::pid_t::try_from(pid.expect("a pid")).expect("pid");
        // SAFETY: a signal-less probe of a pid.
        unsafe { libc::kill(pid, 0) == 0 }
    }

    async fn eventually<T>(what: &str, probe: impl Fn() -> Option<T>) -> T {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match probe() {
                    Some(found) => return found,
                    None => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what} in time"))
    }

    async fn answering<A>(answer: A) -> String
    where
        A: axum::response::IntoResponse + Clone + Send + Sync + 'static,
    {
        let app = axum::Router::new().route(
            cufflink_types::runner::RUNNER_NATIVE_PATH,
            axum::routing::get(move || std::future::ready(answer.clone())),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let url = format!("http://{}", listener.local_addr().expect("an address"));
        tokio::spawn(async move { axum::serve(listener, app).await });
        url
    }

    #[tokio::test]
    async fn a_platform_that_cannot_answer_leaves_the_children_running() {
        for platform in [
            "http://127.0.0.1:1".to_string(),
            answering(axum::http::StatusCode::SERVICE_UNAVAILABLE).await,
            answering(axum::http::StatusCode::FORBIDDEN).await,
        ] {
            let h = harness_of(&platform, NO_NATS, Duration::from_secs(1)).await;
            let deployment = deployment();
            h.sh(&deployment, "exec sleep 30").await;
            let pid = h.fleet.children.list(|_, child| child.pid).pop().flatten();
            assert_eq!(h.fleet.reconcile().await, Reconciled::Unread, "{platform}");
            assert!(h.fleet.children.endpoint(&deployment.service_id).is_some());
            assert!(is_alive(pid), "{platform}");
            h.fleet.stop_all().await;
        }
    }

    #[tokio::test]
    async fn a_refused_key_or_one_of_another_tenant_stops_every_child() {
        for platform in [
            answering(axum::http::StatusCode::UNAUTHORIZED).await,
            answering(axum::Json(cufflink_types::runner::NativeSet {
                tenant: "globex".into(),
                services: Vec::new(),
            }))
            .await,
        ] {
            let h = harness_of(&platform, NO_NATS, Duration::from_secs(1)).await;
            let deployment = deployment();
            h.sh(&deployment, "exec sleep 30").await;
            let pid = h.fleet.children.list(|_, child| child.pid).pop().flatten();
            for _ in 0..2 {
                assert_eq!(h.fleet.reconcile().await, Reconciled::KeyRefused);
                assert!(h.fleet.key_refused.load(Ordering::SeqCst));
            }
            assert_eq!(h.fleet.children.endpoint(&deployment.service_id), None);
            eventually("the child stopped", || (!is_alive(pid)).then_some(())).await;
            eventually("it drained", || (h.draining() == 0).then_some(())).await;
            assert_eq!(h.holds(), 0, "a stop the runner chose is not an exit");
        }
    }

    #[tokio::test]
    async fn a_ready_child_whose_calls_cannot_be_routed_is_stopped_and_not_held_against_it() {
        let Some(echo) = cufflink_testkit::require_binary("ext-echo") else {
            return;
        };
        let h = harness(NO_NATS, Duration::from_secs(3)).await;
        let deployment = deployment();
        let failure = h.fleet.start_from(&deployment, echo, &HashMap::new()).await;
        assert_eq!(failure, None, "the runner's failure, not the child's");
        assert_eq!(h.fleet.children.endpoint(&deployment.service_id), None);
        assert_eq!((h.routes(), h.holds()), (0, 0));
        let sockets = std::fs::read_dir(h.dir.path())
            .expect("the socket directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sock"))
            .count();
        assert_eq!(sockets, 0, "the child and its socket are gone");
    }

    #[tokio::test]
    async fn a_child_whose_missing_calls_cannot_be_routed_is_stopped_and_not_held_against_it() {
        let h = harness(NO_NATS, Duration::from_millis(500)).await;
        let deployment = deployment();
        let (endpoint, _) = h.sh(&deployment, "exec sleep 30").await;
        let pid = h.fleet.children.list(|_, child| child.pid).pop().flatten();
        h.fleet
            .switch(&deployment, &endpoint, Vec::new(), None)
            .await;
        assert_eq!(h.fleet.children.endpoint(&deployment.service_id), None);
        assert_eq!(h.routes(), 0, "nothing relays to it");
        eventually("the child stopped", || (!is_alive(pid)).then_some(())).await;
        eventually("it drained", || (h.draining() == 0).then_some(())).await;
        assert_eq!(h.holds(), 0, "a stop the runner chose is not an exit");
    }

    #[tokio::test]
    async fn a_new_child_that_died_before_its_calls_were_routed_is_left_unrouted() {
        let h = harness(NO_NATS, Duration::from_secs(1)).await;
        let deployment = deployment();
        h.sh(&deployment, "exec sleep 30").await;
        let (endpoint, replaced) = h.sh(&deployment, "exit 3").await;
        let old = replaced.expect("the first child is replaced");
        let old_pid = old.pid;
        eventually("the new child's exit", || {
            h.fleet
                .children
                .endpoint(&deployment.service_id)
                .is_none()
                .then_some(())
        })
        .await;
        h.fleet
            .switch(&deployment, &endpoint, Vec::new(), Some(old))
            .await;
        assert_eq!(h.routes(), 0, "no route to a dead child");
        eventually("the replaced child stopped", || {
            (!is_alive(old_pid)).then_some(())
        })
        .await;
        eventually("it drained", || (h.draining() == 0).then_some(())).await;
        assert_eq!(h.holds(), 1, "the new child's own exit is its failure");
    }

    #[tokio::test]
    async fn a_retired_child_is_stopped_in_the_background_once_settled_and_kept_until_then() {
        let h = harness(NO_NATS, Duration::from_secs(1)).await;
        let deployment = deployment();
        h.sh(&deployment, "exec sleep 30").await;
        let child = h
            .fleet
            .children
            .remove(&deployment.service_id)
            .expect("running");
        let pid = child.pid;
        let (settle, settled) = tokio::sync::oneshot::channel::<()>();
        h.fleet.retire(
            async {
                let _ = settled.await;
            },
            Some(child),
        );
        assert_eq!(
            locked(&h.fleet.shared.draining)
                .values()
                .map(|draining| draining.service_id)
                .collect::<Vec<_>>(),
            [deployment.service_id],
            "its binary is kept while it drains"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(is_alive(pid), "it answers its calls until they settle");
        settle.send(()).expect("still settling");
        eventually("the child stopped", || (!is_alive(pid)).then_some(())).await;
        eventually("it drained", || (h.draining() == 0).then_some(())).await;
    }
}
