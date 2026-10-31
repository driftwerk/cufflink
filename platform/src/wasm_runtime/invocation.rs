use super::CompiledModule;
use crate::registry::ServiceIdentity;
use cufflink_telemetry::{Kind, Scope};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InvocationKind {
    Route,
    Hook,
    Subscription,
    OnMigrate,
    Job,
    WorkerRoute,
    SseReducer,
}

impl InvocationKind {
    pub fn label(self) -> &'static str {
        match self {
            InvocationKind::Route => "route",
            InvocationKind::Hook => "hook",
            InvocationKind::Subscription => "subscription",
            InvocationKind::OnMigrate => "on_migrate",
            InvocationKind::Job => "job",
            InvocationKind::WorkerRoute => "worker_route",
            InvocationKind::SseReducer => "sse_reducer",
        }
    }

    pub fn budget(self, default: Duration) -> Duration {
        static OVERRIDE: OnceLock<Option<Duration>> = OnceLock::new();
        match self {
            InvocationKind::Route => *OVERRIDE.get_or_init(|| {
                std::env::var("WASM_TEST_ROUTE_BUDGET_SECS")
                    .ok()
                    .and_then(|s| s.trim().parse().ok())
                    .filter(|secs| *secs > 0)
                    .map(Duration::from_secs)
            }),
            _ => None,
        }
        .unwrap_or(default)
    }
}

impl From<InvocationKind> for Kind {
    fn from(kind: InvocationKind) -> Self {
        match kind {
            InvocationKind::Route => Kind::Route,
            InvocationKind::Hook => Kind::Hook,
            InvocationKind::Subscription => Kind::Subscription,
            InvocationKind::OnMigrate => Kind::OnMigrate,
            InvocationKind::Job => Kind::Job,
            InvocationKind::WorkerRoute => Kind::WorkerRoute,
            InvocationKind::SseReducer => Kind::SseReducer,
        }
    }
}

pub struct Invocation {
    pub compiled: Arc<CompiledModule>,
    pub handler: String,
    pub request_json: String,
    pub identity: ServiceIdentity,
    pub publishes: Option<Vec<String>>,
    pub extensions: Vec<String>,
    pub kind: InvocationKind,
    pub scope_kind: Option<Kind>,
    pub budget: Duration,
    pub cancel: Option<CancellationToken>,
    pub class: super::Class,
    pub admission: Option<super::Admission>,
}

impl Invocation {
    pub fn scope(&self) -> Scope {
        self.identity.scope(self.compiled.version).running(
            self.scope_kind.unwrap_or_else(|| self.kind.into()),
            self.handler.as_str(),
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("WASM execution timed out after {}s", budget.as_secs())]
    DeadlineExceeded { budget: Duration },
    #[error("WASM execution aborted: platform is shutting down")]
    ShuttingDown,
    #[error("WASM execution not started: platform is shutting down")]
    NotStarted,
    #[error("WASM execution cancelled")]
    Cancelled,
    #[error(
        "WASM guest exceeded its memory limit (requested {requested} bytes, limit {limit} bytes)"
    )]
    MemoryLimit { requested: usize, limit: usize },
    #[error("WASM guest returned an invalid response: {0}")]
    Response(String),
    #[error("Handler '{0}' not found in WASM module")]
    HandlerNotFound(String),
    #[error("WASM trap: {0}")]
    Trap(String),
    #[error("WASM runtime error: {0}")]
    Runtime(String),
    #[error(
        "WASM guest needs more memory than its service may hold (requested {requested} bytes, \
         service share {share} bytes)"
    )]
    TooLarge { requested: usize, share: usize },
    #[error("WASM guest capacity exhausted ({reason})")]
    Overloaded {
        reason: super::budget::Reason,
        retry_after: Duration,
        ran: bool,
    },
}

impl ExecutionError {
    pub fn interrupted_by_shutdown(&self, at_hard_deadline: bool) -> bool {
        match self {
            ExecutionError::ShuttingDown | ExecutionError::NotStarted => true,
            ExecutionError::DeadlineExceeded { .. } => at_hard_deadline,
            _ => false,
        }
    }

    pub(super) fn label(&self) -> &'static str {
        match self {
            ExecutionError::DeadlineExceeded { .. } => "timeout",
            ExecutionError::ShuttingDown | ExecutionError::NotStarted => "shutdown",
            ExecutionError::Cancelled => "cancelled",
            ExecutionError::MemoryLimit { .. } => "memory_limit",
            ExecutionError::Trap(_) => "trap",
            ExecutionError::Overloaded { .. } => "overloaded",
            ExecutionError::TooLarge { .. } => "too_large",
            ExecutionError::Response(_)
            | ExecutionError::HandlerNotFound(_)
            | ExecutionError::Runtime(_) => "error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invocation_kind_is_the_telemetry_kind_of_its_label() {
        for kind in [
            InvocationKind::Route,
            InvocationKind::Hook,
            InvocationKind::Subscription,
            InvocationKind::OnMigrate,
            InvocationKind::Job,
            InvocationKind::WorkerRoute,
            InvocationKind::SseReducer,
        ] {
            assert_eq!(Kind::from(kind).as_str(), kind.label());
        }
    }

    #[test]
    fn only_shutdown_stops_count_as_interruptions() {
        let deadline = ExecutionError::DeadlineExceeded {
            budget: Duration::from_secs(30),
        };
        assert!(ExecutionError::ShuttingDown.interrupted_by_shutdown(false));
        assert!(ExecutionError::NotStarted.interrupted_by_shutdown(false));
        assert!(deadline.interrupted_by_shutdown(true));
        assert!(!deadline.interrupted_by_shutdown(false));
        assert!(!ExecutionError::Runtime("join".into()).interrupted_by_shutdown(true));
        assert!(!ExecutionError::Cancelled.interrupted_by_shutdown(true));
    }

    #[test]
    fn sse_reducers_have_their_own_label_and_no_budget_override() {
        assert_eq!(InvocationKind::SseReducer.label(), "sse_reducer");
        assert_eq!(
            InvocationKind::SseReducer.budget(Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }
}
