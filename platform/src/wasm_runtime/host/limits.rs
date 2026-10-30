use crate::policy::{PolicyKey, PolicyMode, PolicySet, Violation};
use crate::wasm_runtime::budget::{Account, Charge, Denied, Hold, Reason, Refused, Verdict};
use crate::wasm_runtime::{InvocationKind, WasmLimits};
use std::sync::Arc;
use wasmtime::ResourceLimiter;

pub(crate) struct LimitContext {
    pub tenant: String,
    pub service: String,
    pub handler: String,
    pub kind: InvocationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BudgetDenial {
    pub reason: Reason,
    pub requested: usize,
    pub holding: usize,
    pub host: bool,
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
    charge: Option<Charge>,
    account: Option<Account>,
    budget_denial: Option<BudgetDenial>,
}

impl GuestLimits {
    pub(crate) fn new(
        limits: Arc<WasmLimits>,
        policies: Arc<PolicySet>,
        context: LimitContext,
        charge: Option<Charge>,
    ) -> Self {
        let memory_cap = limits.memory_cap(&policies);
        let account = charge
            .as_ref()
            .map(|charge| charge.account(&context.handler, context.kind));
        Self {
            limits,
            policies,
            memory_cap,
            context,
            peak_memory: 0,
            denied: None,
            memory_verdict: None,
            table_warned: false,
            charge,
            account,
            budget_denial: None,
        }
    }

    pub(crate) fn hold(&self) -> Hold {
        Hold::on(self.account.clone())
    }

    pub(crate) fn host_refused(&mut self, refused: Refused) {
        if let Refused::Budget(capacity) = refused {
            let grown = self.charge.as_ref().map_or(0, |charge| {
                charge.holding_with(charge.linear(), charge.tables())
            });
            self.budget_denial = Some(BudgetDenial {
                reason: capacity.reason,
                requested: capacity.requested,
                holding: grown.saturating_add(capacity.holding),
                host: true,
            });
        }
    }

    pub(crate) fn charge_mut(&mut self) -> Option<&mut Charge> {
        self.charge.as_mut()
    }

    pub(crate) fn budget_denial(&self) -> Option<BudgetDenial> {
        self.budget_denial
    }

    pub(crate) fn sample(&self) -> Option<usize> {
        self.charge.as_ref().map(|charge| {
            charge.peak()
                + self
                    .budget_denial
                    .filter(|denial| !denial.host)
                    .map_or(0, |denial| denial.requested)
        })
    }

    fn judge(
        &mut self,
        verdict: Verdict,
        what: &'static str,
        requested: usize,
        holding: usize,
    ) -> bool {
        let (reason, mode, allowed) = match verdict {
            Verdict::Allowed => return true,
            Verdict::Warned(reason) => (reason, PolicyMode::Warn, true),
            Verdict::Denied(reason) => (reason, PolicyMode::Enforce, false),
        };
        Denied {
            tenant: &self.context.tenant,
            service: &self.context.service,
            handler: &self.context.handler,
            kind: self.context.kind,
            reason,
            what,
            mode,
            requested,
        }
        .record();
        if !allowed {
            self.budget_denial = Some(BudgetDenial {
                reason,
                requested,
                holding,
                host: false,
            });
        }
        allowed
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
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.peak_memory = self.peak_memory.max(desired);
        if desired > self.memory_cap {
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
                return Ok(false);
            }
        }
        let Some((verdict, holding)) = self.charge.as_mut().map(|charge| {
            (
                charge.grow_linear(current, desired),
                charge.holding_with(desired, charge.tables()),
            )
        }) else {
            return Ok(true);
        };
        Ok(self.judge(verdict, "grow", desired.saturating_sub(current), holding))
    }

    fn memory_grow_failed(&mut self, _error: wasmtime::Error) -> wasmtime::Result<()> {
        if let Some(charge) = self.charge.as_mut() {
            charge.linear_grow_failed();
        }
        Ok(())
    }

    fn table_growing(
        &mut self,
        current: usize,
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
        if !allowed {
            return Ok(false);
        }
        let bytes = |elements: usize| {
            elements.saturating_mul(crate::wasm_runtime::budget::TABLE_ELEMENT_BYTES)
        };
        let Some((verdict, holding)) = self.charge.as_mut().map(|charge| {
            let tables = charge.tables().saturating_sub(bytes(current)) + bytes(desired);
            (
                charge.grow_tables(current, desired),
                charge.holding_with(charge.linear(), tables),
            )
        }) else {
            return Ok(true);
        };
        Ok(self.judge(
            verdict,
            "table",
            bytes(desired.saturating_sub(current)),
            holding,
        ))
    }

    fn table_grow_failed(&mut self, _error: wasmtime::Error) -> wasmtime::Result<()> {
        if let Some(charge) = self.charge.as_mut() {
            charge.table_grow_failed();
        }
        Ok(())
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
