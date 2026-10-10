use crate::output::Stream;
use crate::runner::{Exit, Runner};
use cufflink_telemetry::Scope;
use std::collections::BTreeSet;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

#[path = "../../telemetry/tests/support/mod.rs"]
pub(crate) mod support;

pub(crate) struct Probe {
    exits: mpsc::UnboundedSender<Exit<Probe>>,
    pub(crate) lines: AtomicU64,
    pub(crate) dropped: AtomicU64,
    pub(crate) longest: AtomicUsize,
}

impl Probe {
    pub(crate) fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<Exit<Probe>>) {
        let (exits, received) = mpsc::unbounded_channel();
        let probe = Self {
            exits,
            lines: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            longest: AtomicUsize::new(0),
        };
        (Arc::new(probe), received)
    }
}

impl Runner for Probe {
    type Key = &'static str;
    type Endpoint = u16;
    type Endpoints = BTreeSet<u16>;
    type Info = i32;

    const OUTPUT_TASK: &'static str = "test_child_output";
    const SUPERVISOR_TASK: &'static str = "test_child_supervisor";
    const TRACKS_CRASH_LOOPS: bool = true;

    fn release(endpoints: &mut BTreeSet<u16>, endpoint: &u16) {
        endpoints.remove(endpoint);
    }

    fn scope(&self, service: &&'static str, version: &i32) -> Scope {
        Scope::service("acme", *service, *version)
    }

    fn output_line(&self, _: &&'static str, _: &i32, _: Stream, line: &[u8]) {
        tracing::info!(target: "test_child", "{}", String::from_utf8_lossy(line));
        self.lines.fetch_add(1, Ordering::SeqCst);
        self.longest.fetch_max(line.len(), Ordering::SeqCst);
    }

    fn output_dropped(&self, _: &&'static str, _: &i32, _: Stream, lines: u64) {
        tracing::info!(target: "test_child", lines, "not logged");
        self.dropped.fetch_add(lines, Ordering::SeqCst);
    }

    fn exited(&self, exit: Exit<Self>) {
        let _ = self.exits.send(exit);
    }
}

pub(crate) async fn sleeping(ignore_term: bool) -> Child {
    let script = if ignore_term {
        "trap '' TERM; echo ready; exec sleep 30"
    } else {
        "echo ready; exec sleep 30"
    };
    let mut child = Command::new("sh")
        .args(["-c", script])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn sh");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::io::AsyncBufReadExt::read_line(&mut tokio::io::BufReader::new(stdout), &mut line),
    )
    .await
    .expect("the child announces itself in time")
    .expect("read readiness line");
    assert_eq!(line.trim(), "ready");
    child
}
