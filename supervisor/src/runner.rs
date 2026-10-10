use crate::output::Stream;
use cufflink_telemetry::Scope;
use std::fmt::Debug;
use std::hash::Hash;
use std::process::ExitStatus;
use std::time::Duration;

pub trait Runner: Send + Sync + Sized + 'static {
    type Key: Clone + Eq + Hash + Debug + Send + Sync + 'static;
    type Endpoint: Clone + Eq + Debug + Send + Sync + 'static;
    type Endpoints: Send + Sync + 'static;
    type Info: Clone + Debug + Send + Sync + 'static;

    const OUTPUT_TASK: &'static str;
    const SUPERVISOR_TASK: &'static str;
    const TRACKS_CRASH_LOOPS: bool;

    fn release(endpoints: &mut Self::Endpoints, endpoint: &Self::Endpoint);

    fn scope(&self, key: &Self::Key, info: &Self::Info) -> Scope;

    fn output_line(&self, key: &Self::Key, info: &Self::Info, stream: Stream, line: &[u8]);

    fn output_dropped(&self, key: &Self::Key, info: &Self::Info, stream: Stream, lines: u64);

    fn exited(&self, exit: Exit<Self>);
}

pub struct Exit<R: Runner> {
    pub key: R::Key,
    pub endpoint: R::Endpoint,
    pub info: R::Info,
    pub status: std::io::Result<ExitStatus>,
    pub uptime: Duration,
    pub killed: bool,
    pub current: bool,
    pub crash_loop: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    CrashLoop,
    Killed,
    Signal,
    Exit,
}

impl ExitReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CrashLoop => "crash_loop",
            Self::Killed => "killed",
            Self::Signal => "signal",
            Self::Exit => "exit",
        }
    }
}

impl<R: Runner> Exit<R> {
    pub fn reason(&self) -> ExitReason {
        #[cfg(unix)]
        let signalled =
            |status: &ExitStatus| std::os::unix::process::ExitStatusExt::signal(status).is_some();
        #[cfg(not(unix))]
        let signalled = |_: &ExitStatus| false;
        match &self.status {
            _ if self.crash_loop => ExitReason::CrashLoop,
            _ if self.killed => ExitReason::Killed,
            Ok(status) if signalled(status) => ExitReason::Signal,
            _ => ExitReason::Exit,
        }
    }
}
