use axum::http::StatusCode;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct Readiness {
    migrations_done: AtomicBool,
    registry_loaded: AtomicBool,
    shutting_down: AtomicBool,
    modules: Mutex<ModuleWarmup>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModuleWarmup {
    pub warmed: usize,
    pub broken: usize,
    pub pending: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    MigrationsDone,
    RegistryLoaded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Starting,
    Migrated,
    Ready,
    ShuttingDown,
}

impl State {
    pub const ALL: [State; 4] = [
        State::Starting,
        State::Migrated,
        State::Ready,
        State::ShuttingDown,
    ];

    pub fn label(self) -> &'static str {
        match self {
            State::Starting => "starting",
            State::Migrated => "migrated",
            State::Ready => "ready",
            State::ShuttingDown => "shutting_down",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmupOutcome {
    Warmed,
    Broken,
}

#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub state: State,
    pub modules: ModuleWarmup,
}

#[derive(Debug, Clone, Copy)]
pub struct Probe {
    pub db_ok: bool,
    pub nats_connected: bool,
    pub redis: &'static str,
    pub registry_version: u64,
}

impl Readiness {
    pub fn mark(&self, gate: Gate) {
        match gate {
            Gate::MigrationsDone => self.migrations_done.store(true, Ordering::SeqCst),
            Gate::RegistryLoaded => self.registry_loaded.store(true, Ordering::SeqCst),
        }
    }

    pub fn mark_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    pub fn set_pending_modules(&self, n: usize) {
        self.modules().pending = n;
    }

    pub fn record_module(&self, outcome: WarmupOutcome) {
        let mut modules = self.modules();
        modules.pending = modules.pending.saturating_sub(1);
        match outcome {
            WarmupOutcome::Warmed => modules.warmed += 1,
            WarmupOutcome::Broken => modules.broken += 1,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            state: self.state(),
            modules: *self.modules(),
        }
    }

    fn state(&self) -> State {
        let migrated = self.migrations_done.load(Ordering::SeqCst);
        let loaded = self.registry_loaded.load(Ordering::SeqCst);
        if self.shutting_down.load(Ordering::SeqCst) {
            State::ShuttingDown
        } else if migrated && loaded {
            State::Ready
        } else if migrated {
            State::Migrated
        } else {
            State::Starting
        }
    }

    fn modules(&self) -> std::sync::MutexGuard<'_, ModuleWarmup> {
        self.modules
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub fn evaluate(snapshot: Snapshot, probe: Probe) -> (StatusCode, Value) {
    let status = match (snapshot.state, probe.db_ok) {
        (State::Ready, true) => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    let body = json!({
        "state": snapshot.state.label(),
        "db": if probe.db_ok { "ok" } else { "error" },
        "nats": if probe.nats_connected { "connected" } else { "disconnected" },
        "redis": probe.redis,
        "modules": {
            "warmed": snapshot.modules.warmed,
            "broken": snapshot.modules.broken,
            "pending": snapshot.modules.pending,
        },
        "registry_version": probe.registry_version,
    });
    (status, body)
}

pub fn record_state(state: State) {
    for candidate in State::ALL {
        metrics::gauge!("cufflink_ready", "state" => candidate.label())
            .set(if candidate == state { 1.0 } else { 0.0 });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(db_ok: bool) -> Probe {
        Probe {
            db_ok,
            nats_connected: false,
            redis: "disabled",
            registry_version: 3,
        }
    }

    fn snapshot(state: State) -> Snapshot {
        Snapshot {
            state,
            modules: ModuleWarmup::default(),
        }
    }

    #[test]
    fn only_ready_with_a_reachable_database_is_200() {
        for state in State::ALL {
            for db_ok in [true, false] {
                let (status, body) = evaluate(snapshot(state), probe(db_ok));
                let expected = if state == State::Ready && db_ok {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                assert_eq!(status, expected, "state={state:?} db_ok={db_ok}");
                assert_eq!(body["state"], state.label());
                assert_eq!(body["db"], if db_ok { "ok" } else { "error" });
            }
        }
    }

    #[test]
    fn body_reports_dependencies_modules_and_registry_version() {
        let snapshot = Snapshot {
            state: State::Ready,
            modules: ModuleWarmup {
                warmed: 4,
                broken: 1,
                pending: 2,
            },
        };
        let (_, body) = evaluate(
            snapshot,
            Probe {
                db_ok: true,
                nats_connected: true,
                redis: "unavailable",
                registry_version: 9,
            },
        );
        assert_eq!(body["nats"], "connected");
        assert_eq!(body["redis"], "unavailable");
        assert_eq!(body["modules"]["warmed"], 4);
        assert_eq!(body["modules"]["broken"], 1);
        assert_eq!(body["modules"]["pending"], 2);
        assert_eq!(body["registry_version"], 9);
    }

    #[test]
    fn gates_advance_in_order_and_are_idempotent() {
        let readiness = Readiness::default();
        assert_eq!(readiness.snapshot().state, State::Starting);
        readiness.mark(Gate::RegistryLoaded);
        assert_eq!(
            readiness.snapshot().state,
            State::Starting,
            "the registry gate alone does not count without migrations"
        );
        readiness.mark(Gate::MigrationsDone);
        assert_eq!(readiness.snapshot().state, State::Ready);
        readiness.mark(Gate::MigrationsDone);
        readiness.mark(Gate::RegistryLoaded);
        assert_eq!(readiness.snapshot().state, State::Ready);

        let migrated_only = Readiness::default();
        migrated_only.mark(Gate::MigrationsDone);
        assert_eq!(migrated_only.snapshot().state, State::Migrated);
    }

    #[test]
    fn shutting_down_is_terminal() {
        let readiness = Readiness::default();
        readiness.mark(Gate::MigrationsDone);
        readiness.mark(Gate::RegistryLoaded);
        readiness.mark_shutting_down();
        assert_eq!(readiness.snapshot().state, State::ShuttingDown);
        readiness.mark(Gate::RegistryLoaded);
        assert_eq!(readiness.snapshot().state, State::ShuttingDown);
    }

    #[test]
    fn module_accounting_moves_pending_into_warmed_or_broken() {
        let readiness = Readiness::default();
        readiness.set_pending_modules(3);
        readiness.record_module(WarmupOutcome::Warmed);
        readiness.record_module(WarmupOutcome::Broken);
        assert_eq!(
            readiness.snapshot().modules,
            ModuleWarmup {
                warmed: 1,
                broken: 1,
                pending: 1,
            }
        );
        readiness.record_module(WarmupOutcome::Warmed);
        readiness.record_module(WarmupOutcome::Warmed);
        assert_eq!(
            readiness.snapshot().modules.pending,
            0,
            "pending never underflows"
        );
    }
}
