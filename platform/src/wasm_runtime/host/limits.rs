use crate::policy::{PolicyKey, PolicyMode, PolicySet, Violation};
use crate::wasm_runtime::WasmLimits;
use std::sync::Arc;
use wasmtime::ResourceLimiter;

pub(crate) struct LimitContext {
    pub tenant: String,
    pub service: String,
    pub handler: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Denial {
    pub requested: usize,
    pub limit: usize,
}

pub(crate) struct GuestLimits {
    limits: Arc<WasmLimits>,
    policies: Arc<PolicySet>,
    memory_cap: usize,
    context: LimitContext,
    peak_memory: usize,
    denied: Option<Denial>,
    memory_verdict: Option<bool>,
    table_warned: bool,
}

impl GuestLimits {
    pub(crate) fn new(
        limits: Arc<WasmLimits>,
        policies: Arc<PolicySet>,
        context: LimitContext,
    ) -> Self {
        let memory_cap = limits.memory_cap(&policies);
        Self {
            limits,
            policies,
            memory_cap,
            context,
            peak_memory: 0,
            denied: None,
            memory_verdict: None,
            table_warned: false,
        }
    }

    pub(crate) fn peak_memory(&self) -> usize {
        self.peak_memory
    }

    pub(crate) fn denied(&self) -> Option<Denial> {
        self.denied
    }

    fn memory_allowed(&mut self, requested: usize) -> bool {
        let limit = self.memory_cap;
        let mode = self.policies.mode(PolicyKey::WasmLimits);
        if mode != PolicyMode::Off {
            metrics::counter!(
                "cufflink_wasm_limit_events_total",
                "limit" => "memory",
                "mode" => mode.as_str(),
            )
            .increment(1);
        }
        let violation = Violation::new(
            "memory",
            format!(
                "service {} handler {} requested {requested} bytes over the {limit} byte cap",
                self.context.service, self.context.handler
            ),
        );
        self.policies
            .apply(PolicyKey::WasmLimits, &self.context.tenant, Some(violation))
            .is_ok()
    }
}

impl ResourceLimiter for GuestLimits {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.peak_memory = self.peak_memory.max(desired);
        if desired <= self.memory_cap {
            return Ok(true);
        }
        let allowed = match self.memory_verdict {
            Some(allowed) => allowed,
            None => {
                let allowed = self.memory_allowed(desired);
                self.memory_verdict = Some(allowed);
                allowed
            }
        };
        if !allowed {
            self.denied = Some(Denial {
                requested: desired,
                limit: self.memory_cap,
            });
        }
        Ok(allowed)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let allowed = desired <= self.limits.max_table_elements;
        if !allowed && !self.table_warned {
            self.table_warned = true;
            metrics::counter!(
                "cufflink_wasm_limit_events_total",
                "limit" => "table",
                "mode" => "enforce",
            )
            .increment(1);
        }
        Ok(allowed)
    }

    fn instances(&self) -> usize {
        self.limits.max_instances as usize
    }

    fn tables(&self) -> usize {
        self.limits.max_tables as usize
    }

    fn memories(&self) -> usize {
        1
    }
}
