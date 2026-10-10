use crate::child::Supervised;
use crate::runner::Runner;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const QUICK_EXIT: Duration = Duration::from_secs(10);
const CRASH_LOOP_EXITS: u32 = 3;

fn quick_exits_after(previous: u32, uptime: Duration) -> u32 {
    if uptime < QUICK_EXIT {
        previous + 1
    } else {
        0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Recorded {
    pub(crate) current: bool,
    pub(crate) crash_loop: bool,
}

pub(crate) struct Table<R: Runner> {
    pub(crate) running: HashMap<R::Key, Supervised<R>>,
    closed: bool,
    pub(crate) endpoints: R::Endpoints,
    quick_exits: HashMap<R::Key, u32>,
    crash_looping: HashSet<R::Key>,
}

impl<R: Runner> Table<R> {
    pub(crate) fn new(endpoints: R::Endpoints) -> Self {
        Self {
            running: HashMap::new(),
            closed: false,
            endpoints,
            quick_exits: HashMap::new(),
            crash_looping: HashSet::new(),
        }
    }

    pub(crate) fn admit(
        &mut self,
        key: R::Key,
        child: Supervised<R>,
    ) -> Result<Option<Supervised<R>>, Box<Supervised<R>>> {
        if self.closed {
            return Err(Box::new(child));
        }
        Ok(self.running.insert(key, child))
    }

    pub(crate) fn close(&mut self) -> Vec<(R::Key, Supervised<R>)> {
        self.closed = true;
        self.running.drain().collect()
    }

    pub(crate) fn exited(
        &mut self,
        key: &R::Key,
        endpoint: &R::Endpoint,
        uptime: Duration,
    ) -> Recorded {
        let current = self
            .running
            .get(key)
            .is_some_and(|child| child.endpoint == *endpoint);
        if current {
            self.running.remove(key);
        }
        let crash_loop = R::TRACKS_CRASH_LOOPS && self.crash_loops(key, uptime);
        Recorded {
            current,
            crash_loop,
        }
    }

    fn crash_loops(&mut self, key: &R::Key, uptime: Duration) -> bool {
        let exits = quick_exits_after(self.quick_exits.get(key).copied().unwrap_or(0), uptime);
        self.quick_exits.insert(key.clone(), exits);
        let crash_loop = exits >= CRASH_LOOP_EXITS;
        if crash_loop {
            self.crash_looping.insert(key.clone());
        }
        crash_loop
    }

    pub(crate) fn is_crash_looping(&self, key: &R::Key) -> bool {
        self.crash_looping.contains(key)
    }

    pub(crate) fn forget_exits(&mut self, key: &R::Key) {
        self.quick_exits.remove(key);
        self.crash_looping.remove(key);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::child::ChildHandle;
    use crate::testing::Probe;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use tokio::sync::{oneshot, Notify};

    fn child(version: i32) -> Supervised<Probe> {
        let (stop, _) = oneshot::channel();
        Supervised {
            endpoint: 3200,
            info: version,
            pid: None,
            handle: ChildHandle {
                stop,
                kill: Arc::new(Notify::new()),
                supervisor: tokio::spawn(async { None }),
            },
        }
    }

    #[tokio::test]
    async fn a_closed_table_refuses_new_children() {
        let mut table = Table::<Probe>::new(BTreeSet::new());
        assert!(matches!(table.admit("web", child(1)), Ok(None)));
        let replaced = table.admit("web", child(2)).expect("open table admits");
        assert_eq!(replaced.map(|old| old.info), Some(1));
        let stopped = table.close();
        assert_eq!(stopped.len(), 1);
        let refused = table
            .admit("web", child(3))
            .expect_err("closed table refuses");
        assert_eq!(refused.info, 3);
        assert!(table.running.is_empty());
    }

    #[test]
    fn three_quick_exits_in_a_row_are_a_crash_loop() {
        let mut table = Table::<Probe>::new(BTreeSet::new());
        let quick = Duration::from_secs(2);
        assert!(!table.exited(&"web", &3200, quick).crash_loop);
        assert!(!table.exited(&"web", &3201, quick).crash_loop);
        assert!(
            !table.exited(&"web", &3202, QUICK_EXIT).crash_loop,
            "a long run resets the count"
        );
        assert!(!table.exited(&"web", &3203, quick).crash_loop);
        assert!(!table.exited(&"web", &3204, quick).crash_loop);
        assert!(table.exited(&"web", &3205, quick).crash_loop);
        assert!(table.is_crash_looping(&"web"));
        assert!(!table.is_crash_looping(&"other"));
        table.forget_exits(&"web");
        assert!(!table.is_crash_looping(&"web"));
        assert!(
            !table.exited(&"web", &3206, quick).crash_loop,
            "a new deploy counts afresh"
        );
    }

    struct Unwatched;

    impl Runner for Unwatched {
        type Key = &'static str;
        type Endpoint = u16;
        type Endpoints = BTreeSet<u16>;
        type Info = i32;

        const OUTPUT_TASK: &'static str = "unwatched_output";
        const SUPERVISOR_TASK: &'static str = "unwatched_supervisor";
        const TRACKS_CRASH_LOOPS: bool = false;

        fn release(_: &mut BTreeSet<u16>, _: &u16) {}
        fn scope(&self, service: &&'static str, version: &i32) -> cufflink_telemetry::Scope {
            cufflink_telemetry::Scope::service("acme", *service, *version)
        }
        fn output_line(&self, _: &&'static str, _: &i32, _: crate::Stream, _: &[u8]) {}
        fn output_dropped(&self, _: &&'static str, _: &i32, _: crate::Stream, _: u64) {}
        fn exited(&self, _: crate::Exit<Self>) {}
    }

    #[test]
    fn a_runner_that_decides_its_own_restarts_is_never_told_of_a_crash_loop() {
        let mut table = Table::<Unwatched>::new(BTreeSet::new());
        for endpoint in 3200..3210 {
            assert!(!table.exited(&"svc", &endpoint, Duration::ZERO).crash_loop);
        }
        assert!(!table.is_crash_looping(&"svc"));
        assert!(table.quick_exits.is_empty(), "nothing is counted");
    }

    #[tokio::test]
    async fn only_the_exited_child_leaves_the_table() {
        let mut table = Table::<Probe>::new(BTreeSet::new());
        table.admit("web", child(1)).expect("admitted");
        let replaced = table.exited(&"web", &3999, Duration::from_secs(60));
        assert!(!replaced.current);
        assert!(table.running.contains_key("web"), "a replaced child's exit");
        let current = table.exited(&"web", &3200, Duration::from_secs(60));
        assert!(current.current);
        assert!(!table.running.contains_key("web"));
    }
}
