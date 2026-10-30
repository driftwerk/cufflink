mod slots;

pub(crate) use slots::RouteSlots;

use crate::policy::record::{LogThrottle, LOG_THROTTLE};
use std::sync::LazyLock;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    RouteConcurrency,
}

impl Reason {
    pub fn label(self) -> &'static str {
        match self {
            Reason::RouteConcurrency => "route_concurrency",
        }
    }
}

impl std::fmt::Display for Reason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

static REFUSALS: LazyLock<LogThrottle<(String, String, Reason)>> =
    LazyLock::new(|| LogThrottle::new(LOG_THROTTLE));

pub(crate) fn refused(tenant: &str, service: &str, handler: &str, reason: Reason) {
    metrics::counter!(
        "cufflink_wasm_admission_refused_total",
        "tenant" => tenant.to_string(),
        "service" => service.to_string(),
        "class" => "interactive",
        "reason" => reason.label(),
        "mode" => "enforce",
    )
    .increment(1);
    if REFUSALS.should_log(
        (tenant.to_string(), service.to_string(), reason),
        Instant::now(),
    ) {
        tracing::warn!(
            target: "cufflink::wasm::budget",
            tenant,
            service,
            handler,
            class = "interactive",
            reason = reason.label(),
            "Guest invocation refused for capacity"
        );
    }
}
