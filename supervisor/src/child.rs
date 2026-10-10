use crate::runner::Runner;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Child;
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    Kill,
    Terminate(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildStop {
    Terminated,
    Killed,
}

pub(crate) struct ChildHandle {
    pub(crate) stop: oneshot::Sender<Stop>,
    pub(crate) kill: Arc<Notify>,
    pub(crate) supervisor: JoinHandle<Option<ChildStop>>,
}

pub struct Supervised<R: Runner> {
    pub endpoint: R::Endpoint,
    pub info: R::Info,
    pub pid: Option<u32>,
    pub(crate) handle: ChildHandle,
}

impl<R: Runner> fmt::Debug for Supervised<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Supervised")
            .field("endpoint", &self.endpoint)
            .field("info", &self.info)
            .field("pid", &self.pid)
            .finish_non_exhaustive()
    }
}

impl<R: Runner> Supervised<R> {
    pub async fn stop(self, how: Stop) -> Option<ChildStop> {
        let _ = self.handle.stop.send(how);
        self.handle.supervisor.await.ok().flatten()
    }
}

pub(crate) async fn stop_child(child: &mut Child, stop_timeout: Duration) -> ChildStop {
    if let Err(e) = terminate(child) {
        warn!(error = %e, "SIGTERM to a child process failed; killing it");
    }
    match tokio::time::timeout(stop_timeout, child.wait()).await {
        Ok(_) => ChildStop::Terminated,
        Err(_) => {
            let _ = child.kill().await;
            ChildStop::Killed
        }
    }
}

#[cfg(unix)]
fn terminate(child: &Child) -> std::io::Result<()> {
    let Some(pid) = child.id() else {
        return Ok(());
    };
    let pid = libc::pid_t::try_from(pid).map_err(std::io::Error::other)?;
    // SAFETY: a plain signal to a pid this process spawned and still owns.
    match unsafe { libc::kill(pid, libc::SIGTERM) } {
        0 => Ok(()),
        _ => Err(std::io::Error::last_os_error()),
    }
}

#[cfg(not(unix))]
fn terminate(child: &Child) -> std::io::Result<()> {
    child.start_kill()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::testing::sleeping;
    use std::time::Instant;

    #[tokio::test]
    async fn a_child_that_honours_sigterm_is_terminated() {
        let mut child = sleeping(false).await;
        let started = Instant::now();
        assert_eq!(
            stop_child(&mut child, Duration::from_secs(5)).await,
            ChildStop::Terminated
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_child_that_ignores_sigterm_is_killed_after_the_stop_timeout() {
        let mut child = sleeping(true).await;
        let started = Instant::now();
        assert_eq!(
            stop_child(&mut child, Duration::from_millis(300)).await,
            ChildStop::Killed
        );
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(child.try_wait().expect("reaped").is_some());
    }
}
