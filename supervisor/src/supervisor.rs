use crate::child::{stop_child, ChildHandle, ChildStop, Stop, Supervised};
use crate::output::{forward_output, Stream};
use crate::runner::{Exit, Runner};
use crate::table::{Recorded, Table};
use cufflink_shutdown::Shutdown;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;
use tokio::io::AsyncRead;
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, Notify};

pub struct Launch<R: Runner> {
    pub key: R::Key,
    pub endpoint: R::Endpoint,
    pub info: R::Info,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug)]
pub enum Unready {
    Exited(ExitStatus),
    Unwaitable(std::io::Error),
    Probe(eyre::Report),
}

#[derive(Debug)]
pub enum StartError {
    Spawn(std::io::Error),
    Unready(Unready),
    Closed,
}

pub struct Supervisor<R: Runner> {
    runner: Arc<R>,
    table: Arc<Mutex<Table<R>>>,
    shutdown: Shutdown,
}

pub fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Claim<'a, R: Runner> {
    table: &'a Mutex<Table<R>>,
    endpoint: Option<R::Endpoint>,
}

impl<R: Runner> Claim<'_, R> {
    fn hand_over(mut self) {
        self.endpoint = None;
    }
}

impl<R: Runner> Drop for Claim<'_, R> {
    fn drop(&mut self) {
        if let Some(endpoint) = self.endpoint.take() {
            R::release(&mut locked(self.table).endpoints, &endpoint);
        }
    }
}

impl<R: Runner> Supervisor<R> {
    pub fn new(runner: Arc<R>, endpoints: R::Endpoints, shutdown: Shutdown) -> Self {
        Self {
            runner,
            table: Arc::new(Mutex::new(Table::new(endpoints))),
            shutdown,
        }
    }

    pub fn endpoints<T>(&self, work: impl FnOnce(&mut R::Endpoints) -> T) -> T {
        work(&mut locked(&self.table).endpoints)
    }

    pub async fn start<T>(
        &self,
        launch: Launch<R>,
        ready: impl Future<Output = eyre::Result<T>>,
    ) -> Result<(Option<Supervised<R>>, T), StartError> {
        let Launch {
            key,
            endpoint,
            info,
            program,
            args,
            cwd,
            env,
        } = launch;
        let claim = Claim {
            table: &self.table,
            endpoint: Some(endpoint.clone()),
        };
        let spawned = Command::new(program)
            .args(args)
            .current_dir(cwd)
            .env_clear()
            .envs(env)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn();
        let mut child = spawned.map_err(StartError::Spawn)?;
        let pid = child.id();
        if let Some(stdout) = child.stdout.take() {
            self.forward(stdout, Stream::Stdout, &key, &info);
        }
        if let Some(stderr) = child.stderr.take() {
            self.forward(stderr, Stream::Stderr, &key, &info);
        }

        let ready = tokio::select! {
            ready = ready => ready.map_err(Unready::Probe),
            exited = child.wait() => Err(match exited {
                Ok(status) => Unready::Exited(status),
                Err(e) => Unready::Unwaitable(e),
            }),
        };
        let found = match ready {
            Ok(found) => found,
            Err(unready) => {
                let _ = child.kill().await;
                return Err(StartError::Unready(unready));
            }
        };

        let (stop, stopped) = oneshot::channel();
        let kill = Arc::new(Notify::new());
        let admitted = {
            let mut table = locked(&self.table);
            claim.hand_over();
            let supervisor = self.shutdown.spawn(
                R::SUPERVISOR_TASK,
                supervise(
                    child,
                    Requests {
                        stop: stopped,
                        kill: kill.clone(),
                    },
                    self.table.clone(),
                    self.runner.clone(),
                    key.clone(),
                    endpoint.clone(),
                    info.clone(),
                ),
            );
            table.admit(
                key,
                Supervised {
                    endpoint,
                    info,
                    pid,
                    handle: ChildHandle {
                        stop,
                        kill,
                        supervisor,
                    },
                },
            )
        };
        match admitted {
            Ok(replaced) => Ok((replaced, found)),
            Err(refused) => {
                let _ = refused.stop(Stop::Kill).await;
                Err(StartError::Closed)
            }
        }
    }

    pub fn endpoint(&self, key: &R::Key) -> Option<R::Endpoint> {
        locked(&self.table)
            .running
            .get(key)
            .map(|child| child.endpoint.clone())
    }

    pub fn info(&self, key: &R::Key) -> Option<R::Info> {
        locked(&self.table)
            .running
            .get(key)
            .map(|child| child.info.clone())
    }

    pub fn list<T>(&self, view: impl Fn(&R::Key, &Supervised<R>) -> T) -> Vec<T> {
        locked(&self.table)
            .running
            .iter()
            .map(|(key, child)| view(key, child))
            .collect()
    }

    pub fn is_crash_looping(&self, key: &R::Key) -> bool {
        locked(&self.table).is_crash_looping(key)
    }

    pub fn forget_exits(&self, key: &R::Key) {
        locked(&self.table).forget_exits(key);
    }

    pub fn remove(&self, key: &R::Key) -> Option<Supervised<R>> {
        locked(&self.table).running.remove(key)
    }

    pub fn take(&self, key: &R::Key, endpoint: &R::Endpoint) -> Option<Supervised<R>> {
        let mut table = locked(&self.table);
        match table.running.get(key) {
            Some(child) if child.endpoint == *endpoint => table.running.remove(key),
            Some(_) | None => None,
        }
    }

    pub fn kill(&self, key: &R::Key, endpoint: &R::Endpoint) -> bool {
        locked(&self.table)
            .running
            .get(key)
            .filter(|child| child.endpoint == *endpoint)
            .map(|child| child.handle.kill.notify_one())
            .is_some()
    }

    pub fn close(&self) -> Vec<(R::Key, Supervised<R>)> {
        locked(&self.table).close()
    }

    fn forward(
        &self,
        output: impl AsyncRead + Unpin + Send + 'static,
        stream: Stream,
        key: &R::Key,
        info: &R::Info,
    ) {
        self.shutdown.spawn(
            R::OUTPUT_TASK,
            forward_output(
                output,
                stream,
                self.runner.clone(),
                key.clone(),
                info.clone(),
            ),
        );
    }
}

struct Requests {
    stop: oneshot::Receiver<Stop>,
    kill: Arc<Notify>,
}

async fn exit(child: &mut Child, kill: &Notify) -> (std::io::Result<ExitStatus>, bool) {
    tokio::select! {
        status = child.wait() => (status, false),
        () = kill.notified() => {
            let _ = child.start_kill();
            (child.wait().await, true)
        }
    }
}

async fn supervise<R: Runner>(
    mut child: Child,
    requests: Requests,
    table: Arc<Mutex<Table<R>>>,
    runner: Arc<R>,
    key: R::Key,
    endpoint: R::Endpoint,
    info: R::Info,
) -> Option<ChildStop> {
    let started = Instant::now();
    let Requests { stop, kill } = requests;
    let ended = tokio::select! {
        exited = exit(&mut child, &kill) => Err(exited),
        how = stop => Ok(match how {
            Ok(Stop::Terminate(timeout)) => stop_child(&mut child, timeout).await,
            Ok(Stop::Kill) | Err(_) => {
                let _ = child.kill().await;
                ChildStop::Killed
            }
        }),
    };
    let mut table = locked(&table);
    R::release(&mut table.endpoints, &endpoint);
    let (status, killed) = match ended {
        Ok(stopped) => return Some(stopped),
        Err(exited) => exited,
    };
    let uptime = started.elapsed();
    let Recorded {
        current,
        crash_loop,
    } = table.exited(&key, &endpoint, uptime);
    drop(table);
    runner.exited(Exit {
        key,
        endpoint,
        info,
        status,
        uptime,
        killed,
        current,
        crash_loop,
    });
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::runner::ExitReason;
    use crate::testing::{sleeping, Probe};
    use std::collections::BTreeSet;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn supervisor(probe: &Arc<Probe>) -> Supervisor<Probe> {
        Supervisor::new(
            probe.clone(),
            BTreeSet::new(),
            Shutdown::new(Duration::from_secs(5)),
        )
    }

    fn claim(supervisor: &Supervisor<Probe>) -> u16 {
        supervisor.endpoints(|taken| {
            let endpoint = (3200..).find(|e| !taken.contains(e)).expect("free");
            taken.insert(endpoint);
            endpoint
        })
    }

    fn in_use(supervisor: &Supervisor<Probe>) -> Vec<u16> {
        supervisor.endpoints(|taken| taken.iter().copied().collect())
    }

    fn shell(key: &'static str, endpoint: u16, version: i32, script: &str) -> Launch<Probe> {
        Launch {
            key,
            endpoint,
            info: version,
            program: "sh".into(),
            args: vec!["-c".into(), script.into()],
            cwd: std::env::temp_dir(),
            env: BTreeMap::from([("PATH".to_string(), "/usr/bin:/bin".to_string())]),
        }
    }

    async fn started(
        supervisor: &Supervisor<Probe>,
        key: &'static str,
        version: i32,
        script: &str,
    ) -> Result<Option<Supervised<Probe>>, StartError> {
        let endpoint = claim(supervisor);
        supervisor
            .start(shell(key, endpoint, version, script), async { Ok(()) })
            .await
            .map(|(replaced, ())| replaced)
    }

    async fn next_exit(exits: &mut mpsc::UnboundedReceiver<Exit<Probe>>) -> Exit<Probe> {
        tokio::time::timeout(Duration::from_secs(10), exits.recv())
            .await
            .expect("an exit in time")
            .expect("an exit")
    }

    async fn poll<T>(probe: impl Fn() -> Option<T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match probe() {
                    Some(found) => return found,
                    None => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .expect("found in time")
    }

    async fn no_exit(exits: &mut mpsc::UnboundedReceiver<Exit<Probe>>) {
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(exits.try_recv().is_err(), "no exit is reported");
    }

    #[tokio::test]
    async fn a_ready_child_is_registered_and_replaces_the_last_one_still_running() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        let first = started(&supervisor, "web", 1, "exec sleep 30").await;
        assert!(matches!(first, Ok(None)));
        assert_eq!(supervisor.endpoint(&"web"), Some(3200));

        let old = started(&supervisor, "web", 2, "exec sleep 30")
            .await
            .expect("started")
            .expect("the last child is handed back");
        assert_eq!((old.endpoint, old.info), (3200, 1));
        assert!(old.pid.is_some());
        assert_eq!(supervisor.endpoint(&"web"), Some(3201));
        assert_eq!(in_use(&supervisor), [3200, 3201], "both still run");
        assert_eq!(old.stop(Stop::Kill).await, Some(ChildStop::Killed));
        assert_eq!(in_use(&supervisor), [3201]);
        assert_eq!(
            supervisor.list(|key, child| (*key, child.info)),
            [("web", 2)]
        );
        no_exit(&mut exits).await;
    }

    #[tokio::test]
    async fn a_child_that_exits_before_it_is_ready_fails_at_once_and_frees_its_endpoint() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        let endpoint = claim(&supervisor);
        let started_at = Instant::now();
        let outcome = supervisor
            .start(
                shell("web", endpoint, 1, "exit 3"),
                std::future::pending::<eyre::Result<()>>(),
            )
            .await;
        assert!(
            matches!(&outcome, Err(StartError::Unready(Unready::Exited(status))) if status.code() == Some(3)),
            "{outcome:?}"
        );
        assert!(started_at.elapsed() < Duration::from_secs(5));
        assert!(in_use(&supervisor).is_empty());
        assert_eq!(supervisor.endpoint(&"web"), None);
        no_exit(&mut exits).await;
    }

    #[tokio::test]
    async fn a_failed_probe_kills_the_child_and_frees_its_endpoint() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        let endpoint = claim(&supervisor);
        let outcome = supervisor
            .start(shell("web", endpoint, 1, "exec sleep 30"), async {
                Err::<(), _>(eyre::eyre!("never answered"))
            })
            .await;
        assert!(
            matches!(&outcome, Err(StartError::Unready(Unready::Probe(e))) if e.to_string() == "never answered"),
            "{outcome:?}"
        );
        assert!(in_use(&supervisor).is_empty());
        no_exit(&mut exits).await;
    }

    #[tokio::test]
    async fn a_start_dropped_while_it_waits_for_ready_kills_its_child_and_frees_its_endpoint() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        let endpoint = claim(&supervisor);
        let dir = tempfile::TempDir::new().expect("temp dir");
        let pid_file = dir.path().join("pid");
        let script = format!(
            "echo $$ > {}.tmp && mv {0}.tmp {0}; exec sleep 30",
            pid_file.display()
        );
        let pid = tokio::select! {
            outcome = supervisor.start(shell("web", endpoint, 1, &script), std::future::pending::<eyre::Result<()>>()) => {
                panic!("the start never ends by itself: {outcome:?}")
            }
            pid = poll(|| std::fs::read_to_string(&pid_file).ok()?.trim().parse::<libc::pid_t>().ok()) => pid,
        };
        assert!(in_use(&supervisor).is_empty(), "its endpoint is free");
        assert_eq!(supervisor.endpoint(&"web"), None);
        // SAFETY: probing, with no signal, a pid this test's child had.
        poll(|| (unsafe { libc::kill(pid, 0) } != 0).then_some(())).await;
        no_exit(&mut exits).await;
    }

    #[tokio::test]
    async fn a_start_hands_back_what_its_ready_probe_found() {
        let (probe, _exits) = Probe::new();
        let supervisor = supervisor(&probe);
        let endpoint = claim(&supervisor);
        let (replaced, found) = supervisor
            .start(shell("web", endpoint, 1, "exec sleep 30"), async {
                Ok("routed")
            })
            .await
            .expect("started");
        assert!(replaced.is_none());
        assert_eq!(found, "routed");
        for (_, child) in supervisor.close() {
            assert_eq!(child.stop(Stop::Kill).await, Some(ChildStop::Killed));
        }
    }

    #[tokio::test]
    async fn a_program_that_cannot_spawn_frees_its_endpoint() {
        let (probe, _exits) = Probe::new();
        let supervisor = supervisor(&probe);
        let endpoint = claim(&supervisor);
        let launch = Launch {
            program: "/nonexistent/cufflink-child".into(),
            ..shell("web", endpoint, 1, "")
        };
        let outcome = supervisor.start(launch, async { Ok(()) }).await;
        assert!(matches!(outcome, Err(StartError::Spawn(_))), "{outcome:?}");
        assert!(in_use(&supervisor).is_empty());
    }

    #[tokio::test]
    async fn a_closed_supervisor_refuses_a_ready_child_and_frees_its_endpoint() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        started(&supervisor, "web", 1, "exec sleep 30")
            .await
            .expect("started");
        let open = supervisor.close();
        assert_eq!(open.len(), 1);
        let outcome = started(&supervisor, "api", 1, "exec sleep 30").await;
        assert!(matches!(outcome, Err(StartError::Closed)), "{outcome:?}");
        assert_eq!(in_use(&supervisor), [3200], "only the open child's");
        for (_, child) in open {
            assert_eq!(
                child.stop(Stop::Terminate(Duration::from_secs(5))).await,
                Some(ChildStop::Terminated)
            );
        }
        assert!(in_use(&supervisor).is_empty());
        no_exit(&mut exits).await;
    }

    #[tokio::test]
    async fn a_child_that_exits_on_its_own_is_reported_after_it_leaves_the_table() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        started(&supervisor, "web", 7, "sleep 0.2; exit 4")
            .await
            .expect("started");
        let exit = next_exit(&mut exits).await;
        assert_eq!((exit.key, exit.endpoint, exit.info), ("web", 3200, 7));
        assert_eq!(exit.status.as_ref().ok().and_then(|s| s.code()), Some(4));
        assert!(exit.current && !exit.killed && !exit.crash_loop);
        assert_eq!(exit.reason(), ExitReason::Exit);
        assert!(exit.uptime < Duration::from_secs(10));
        assert_eq!(supervisor.endpoint(&"web"), None, "out of the table");
        assert!(in_use(&supervisor).is_empty(), "and its endpoint free");
    }

    #[tokio::test]
    async fn a_replaced_or_removed_child_that_exits_before_its_stop_is_not_current() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        started(&supervisor, "web", 1, "sleep 0.5; exit 1")
            .await
            .expect("started");
        let old = started(&supervisor, "web", 2, "exec sleep 30")
            .await
            .expect("started")
            .expect("replaced");
        let exit = next_exit(&mut exits).await;
        assert_eq!((exit.endpoint, exit.info, exit.current), (3200, 1, false));
        assert_eq!(supervisor.endpoint(&"web"), Some(3201), "still served");
        assert_eq!(old.stop(Stop::Kill).await, None, "it had exited");

        started(&supervisor, "api", 1, "sleep 0.5; exit 1")
            .await
            .expect("started");
        let removed = supervisor.remove(&"api").expect("running");
        let exit = next_exit(&mut exits).await;
        assert_eq!((exit.key, exit.current), ("api", false));
        assert_eq!(removed.stop(Stop::Kill).await, None);
        assert_eq!(in_use(&supervisor), [3201]);
    }

    #[tokio::test]
    async fn a_signalled_child_is_reported_with_its_reason() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        started(&supervisor, "web", 1, "sleep 0.2; kill -9 $$")
            .await
            .expect("started");
        let exit = next_exit(&mut exits).await;
        assert!(!exit.killed);
        assert_eq!(exit.reason(), ExitReason::Signal);
        assert!(in_use(&supervisor).is_empty());
    }

    #[tokio::test]
    async fn a_kill_reaches_only_the_child_on_its_endpoint() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        started(&supervisor, "web", 1, "exec sleep 30")
            .await
            .expect("started");
        assert!(!supervisor.kill(&"web", &3999), "a replaced child's");
        assert!(!supervisor.kill(&"api", &3200), "another app's");
        no_exit(&mut exits).await;
        assert!(supervisor.kill(&"web", &3200));
        let exit = next_exit(&mut exits).await;
        assert_eq!(exit.reason(), ExitReason::Killed);
        assert_eq!(supervisor.endpoint(&"web"), None);
    }

    #[tokio::test]
    async fn a_take_reaches_only_the_child_on_its_endpoint() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        started(&supervisor, "web", 1, "exec sleep 30")
            .await
            .expect("started");
        assert!(
            supervisor.take(&"web", &3999).is_none(),
            "a replaced child's"
        );
        assert!(supervisor.take(&"api", &3200).is_none(), "another app's");
        assert_eq!(supervisor.endpoint(&"web"), Some(3200));
        let taken = supervisor.take(&"web", &3200).expect("the child on 3200");
        assert_eq!(supervisor.endpoint(&"web"), None);
        assert_eq!(taken.stop(Stop::Kill).await, Some(ChildStop::Killed));
        assert!(in_use(&supervisor).is_empty());
        no_exit(&mut exits).await;
    }

    #[tokio::test]
    async fn three_quick_exits_are_reported_as_a_crash_loop() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        let mut reasons = Vec::new();
        for _ in 0..3 {
            let endpoint = claim(&supervisor);
            supervisor
                .start(shell("web", endpoint, 1, "sleep 0.2; exit 1"), async {
                    Ok(())
                })
                .await
                .expect("started");
            reasons.push(next_exit(&mut exits).await.reason());
        }
        assert_eq!(
            reasons,
            [ExitReason::Exit, ExitReason::Exit, ExitReason::CrashLoop]
        );
        assert!(supervisor.is_crash_looping(&"web"));
        supervisor.forget_exits(&"web");
        assert!(!supervisor.is_crash_looping(&"web"));
    }

    #[tokio::test]
    async fn a_stop_request_terminates_gracefully_within_the_timeout() {
        let (probe, mut exits) = Probe::new();
        let supervisor = supervisor(&probe);
        let supervised = |ignore_term: bool| {
            let supervisor = &supervisor;
            async move {
                let endpoint = claim(supervisor);
                let (stop, stopped) = oneshot::channel();
                let kill = Arc::new(Notify::new());
                let task = tokio::spawn(supervise(
                    sleeping(ignore_term).await,
                    Requests {
                        stop: stopped,
                        kill: kill.clone(),
                    },
                    supervisor.table.clone(),
                    supervisor.runner.clone(),
                    "web",
                    endpoint,
                    1,
                ));
                Supervised::<Probe> {
                    endpoint,
                    info: 1,
                    pid: None,
                    handle: ChildHandle {
                        stop,
                        kill,
                        supervisor: task,
                    },
                }
            }
        };

        let graceful = supervised(false).await;
        let started_at = Instant::now();
        assert_eq!(
            graceful.stop(Stop::Terminate(Duration::from_secs(5))).await,
            Some(ChildStop::Terminated)
        );
        assert!(started_at.elapsed() < Duration::from_secs(5));
        assert!(in_use(&supervisor).is_empty(), "endpoint freed");

        let stubborn = supervised(true).await;
        assert_eq!(
            stubborn
                .stop(Stop::Terminate(Duration::from_millis(300)))
                .await,
            Some(ChildStop::Killed)
        );
        let killed = supervised(false).await;
        assert_eq!(killed.stop(Stop::Kill).await, Some(ChildStop::Killed));
        assert!(in_use(&supervisor).is_empty());
        no_exit(&mut exits).await;
    }
}
