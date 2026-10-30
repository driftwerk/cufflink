mod cgroup;
mod config;
mod estimates;
mod ledger;
mod pressure;
mod slots;
mod stack;

pub use cgroup::{detect as detect_cgroup, CgroupLimit, LimitSource};
pub use config::{thread_stack_bytes, BudgetConfig};
pub use ledger::Limits;
pub(crate) use ledger::{Charge, Verdict, TABLE_ELEMENT_BYTES};
pub(crate) use stack::enter as enter_thread;

use super::{ExecutionError, InvocationKind};
use crate::auth::service_identity::MAX_CALL_DEPTH;
use crate::policy::record::{LogThrottle, LOG_THROTTLE};
use crate::policy::PolicyMode;
use crate::registry::ServiceIdentity;
use estimates::Estimates;
use ledger::Ledger;
use slots::{RouteSlots, ServiceSlots};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    Interactive,
    Nested,
    Committed,
    Background,
}

impl Class {
    pub(crate) const ALL: [Class; 4] = [
        Class::Interactive,
        Class::Nested,
        Class::Committed,
        Class::Background,
    ];

    pub(crate) fn index(self) -> usize {
        match self {
            Class::Interactive => 0,
            Class::Nested => 1,
            Class::Committed => 2,
            Class::Background => 3,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Class::Interactive => "interactive",
            Class::Nested => "nested",
            Class::Committed => "committed",
            Class::Background => "background",
        }
    }

    pub(crate) fn gated_by_pressure(self) -> bool {
        matches!(self, Class::Interactive | Class::Background)
    }

    pub fn of_call(depth: Option<u8>) -> Self {
        match depth {
            Some(depth) if depth > 0 => Class::Nested,
            _ => Class::Interactive,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    Memory,
    ServiceShare,
    ServiceConcurrency,
    RouteConcurrency,
    Pressure,
}

impl Reason {
    pub fn label(self) -> &'static str {
        match self {
            Reason::Memory => "memory",
            Reason::ServiceShare => "service_share",
            Reason::ServiceConcurrency => "service_concurrency",
            Reason::RouteConcurrency => "route_concurrency",
            Reason::Pressure => "pressure",
        }
    }
}

impl std::fmt::Display for Reason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ServiceKey {
    pub tenant: Arc<str>,
    pub service: Arc<str>,
}

impl ServiceKey {
    fn none() -> Self {
        Self {
            tenant: "".into(),
            service: "".into(),
        }
    }

    fn of(identity: &ServiceIdentity) -> Self {
        Self {
            tenant: identity.tenant_slug.as_str().into(),
            service: identity.service_name.as_str().into(),
        }
    }
}

static LOGGED: LazyLock<LogThrottle<(String, String, Reason)>> =
    LazyLock::new(|| LogThrottle::new(LOG_THROTTLE));

fn throttled(tenant: &str, service: &str, reason: Reason) -> bool {
    LOGGED.should_log(
        (tenant.to_string(), service.to_string(), reason),
        std::time::Instant::now(),
    )
}

pub(crate) struct Refusal<'a> {
    pub tenant: &'a str,
    pub service: &'a str,
    pub handler: &'a str,
    pub class: Class,
    pub reason: Reason,
    pub mode: PolicyMode,
}

impl Refusal<'_> {
    fn record(&self, in_use: Option<usize>) {
        metrics::counter!(
            "cufflink_wasm_admission_refused_total",
            "tenant" => self.tenant.to_string(),
            "service" => self.service.to_string(),
            "class" => self.class.label(),
            "reason" => self.reason.label(),
            "mode" => self.mode.as_str(),
        )
        .increment(1);
        if throttled(self.tenant, self.service, self.reason) {
            tracing::warn!(
                target: "cufflink::wasm::budget",
                tenant = self.tenant,
                service = self.service,
                handler = self.handler,
                class = self.class.label(),
                reason = self.reason.label(),
                mode = self.mode.as_str(),
                in_use,
                "Guest invocation refused for capacity"
            );
        }
    }
}

pub(crate) struct Denied<'a> {
    pub tenant: &'a str,
    pub service: &'a str,
    pub handler: &'a str,
    pub kind: InvocationKind,
    pub reason: Reason,
    pub what: &'static str,
    pub mode: PolicyMode,
    pub requested: usize,
}

impl Denied<'_> {
    pub(crate) fn record(&self) {
        metrics::counter!(
            "cufflink_wasm_memory_denied_total",
            "tenant" => self.tenant.to_string(),
            "service" => self.service.to_string(),
            "kind" => self.kind.label(),
            "reason" => self.reason.label(),
            "what" => self.what,
            "mode" => self.mode.as_str(),
        )
        .increment(1);
        metrics::counter!(
            "cufflink_wasm_limit_events_total",
            "limit" => "budget",
            "mode" => self.mode.as_str(),
        )
        .increment(1);
        if throttled(self.tenant, self.service, self.reason) {
            tracing::warn!(
                target: "cufflink::wasm::budget",
                tenant = self.tenant,
                service = self.service,
                handler = self.handler,
                kind = self.kind.label(),
                reason = self.reason.label(),
                what = self.what,
                mode = self.mode.as_str(),
                requested = self.requested,
                "Guest growth past the memory budget"
            );
        }
    }
}

pub(crate) struct Ticket<'a> {
    pub class: Class,
    pub kind: InvocationKind,
    pub identity: &'a ServiceIdentity,
    pub handler: &'a str,
    pub route_limit: Option<u32>,
    pub initial_memory: usize,
    pub budget: Duration,
}

pub struct Admission {
    slots: Vec<OwnedSemaphorePermit>,
    charge: Option<Charge>,
    budget_from: Option<Instant>,
}

impl Admission {
    pub(crate) fn counting_from(self, started: Instant) -> Self {
        Self {
            budget_from: Some(started),
            ..self
        }
    }

    pub(crate) fn budget_from(&self) -> Option<Instant> {
        self.budget_from
    }

    pub(crate) fn into_parts(self) -> (Vec<OwnedSemaphorePermit>, Option<Charge>) {
        (self.slots, self.charge)
    }
}

impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admission")
            .field("slots", &self.slots.len())
            .field("charged", &self.charge.is_some())
            .finish()
    }
}

pub(crate) struct Budget {
    config: BudgetConfig,
    ledger: Option<Arc<Ledger>>,
    estimates: Estimates,
    routes: RouteSlots,
    services: ServiceSlots,
    nested: Arc<Semaphore>,
    background: Arc<Semaphore>,
    max_memory: usize,
    wait: Duration,
    retry_after: Duration,
}

impl Budget {
    pub(crate) fn new(
        config: BudgetConfig,
        max_concurrent: usize,
        max_memory: usize,
        wait: Duration,
        retry_after: Duration,
    ) -> Self {
        metrics::gauge!("cufflink_wasm_memory_budget_bytes")
            .set(config.limits.map_or(0, |limits| limits.budget) as f64);
        Self {
            ledger: config.limits.map(|limits| {
                Arc::new(Ledger::new(ledger::Settings {
                    limits,
                    enforce: config.enforces(),
                    stack: config.thread_stack,
                    aging: config.background_aging,
                }))
            }),
            estimates: Estimates::new(
                config.estimate_map_max,
                config.estimate_max_age,
                config.cold_estimate,
            ),
            routes: RouteSlots::default(),
            services: ServiceSlots::new(config.per_service),
            nested: Arc::new(Semaphore::new(
                max_concurrent.saturating_mul(usize::from(MAX_CALL_DEPTH)),
            )),
            background: Arc::new(Semaphore::new((max_concurrent / 4 * 3).max(1))),
            config,
            max_memory,
            wait,
            retry_after,
        }
    }

    pub(crate) fn start_pressure_gate(&self) -> eyre::Result<()> {
        match (&self.config.limit, &self.ledger) {
            (Some(limit), Some(ledger)) => pressure::start(
                limit,
                ledger,
                self.config.reserve,
                self.config.pressure_gate,
            ),
            _ => Ok(()),
        }
    }

    pub(crate) fn share(&self) -> Option<usize> {
        self.ledger.as_ref().map(|ledger| ledger.limits().service)
    }

    pub(crate) fn enforces(&self) -> bool {
        self.config.enforces()
    }

    pub(crate) fn capacity_retries(&self) -> u32 {
        match self.config.enforces() {
            true => self.config.capacity_retries,
            false => 0,
        }
    }

    pub(crate) fn capacity_retries_job(&self) -> u32 {
        self.config.capacity_retries_job
    }

    #[cfg(test)]
    pub(crate) fn ledger(&self) -> Option<&Arc<Ledger>> {
        self.ledger.as_ref()
    }

    pub(crate) fn overloaded(&self, reason: Reason, ran: bool) -> ExecutionError {
        ExecutionError::Overloaded {
            reason,
            retry_after: self.retry_after,
            ran,
        }
    }

    pub(crate) fn record_sample(&self, service_id: Uuid, handler: &str, peak: usize) {
        if self.ledger.is_some() {
            self.estimates.record(service_id, handler, peak);
        }
    }

    pub(crate) fn waits_outside_budget(&self, class: Class, kind: InvocationKind) -> bool {
        self.config.enforces()
            && match class {
                Class::Committed => kind != InvocationKind::OnMigrate,
                Class::Background => true,
                Class::Interactive | Class::Nested => false,
            }
    }

    pub(crate) async fn room(
        &self,
        identity: Option<&ServiceIdentity>,
        handler: Option<&str>,
    ) -> usize {
        let Some(ledger) = self.ledger.as_ref().filter(|_| self.config.enforces()) else {
            return usize::MAX;
        };
        let bytes = match (identity, handler) {
            (Some(identity), Some(handler)) => {
                self.estimate(ledger, Class::Background, identity, handler, 0)
            }
            _ => self.config.job_floor.min(ledger.largest(Class::Background)),
        };
        let service = identity.map_or_else(ServiceKey::none, ServiceKey::of);
        ledger.room(Class::Background, &service, bytes).await
    }

    fn estimate(
        &self,
        ledger: &Ledger,
        class: Class,
        identity: &ServiceIdentity,
        handler: &str,
        initial_memory: usize,
    ) -> usize {
        self.estimates
            .estimate(
                identity.service_id,
                handler,
                initial_memory,
                self.max_memory,
            )
            .min(ledger.largest(class))
    }

    pub(crate) fn admission_deadline(&self, ticket: &Ticket<'_>) -> Option<Instant> {
        let now = Instant::now();
        match ticket.class {
            Class::Interactive => Some(now + self.wait.min(ticket.budget)),
            Class::Committed if self.waits_outside_budget(ticket.class, ticket.kind) => {
                Some(now + self.config.committed_wait)
            }
            Class::Committed | Class::Nested | Class::Background => None,
        }
    }

    pub(crate) async fn admit(
        &self,
        ticket: Ticket<'_>,
        global: &Arc<Semaphore>,
    ) -> Result<Admission, ExecutionError> {
        let started = Instant::now();
        let until = self.admission_deadline(&ticket);
        let admitted = self.take(&ticket, global, until).await;
        let result = match &admitted {
            Ok(_) => "admitted",
            Err(_) => "refused",
        };
        metrics::histogram!(
            "cufflink_wasm_admission_wait_seconds",
            "class" => ticket.class.label(),
            "result" => result,
        )
        .record(started.elapsed().as_secs_f64());
        metrics::histogram!("cufflink_wasm_queue_wait_seconds", "kind" => ticket.kind.label())
            .record(started.elapsed().as_secs_f64());
        admitted
    }

    fn refuse(&self, ticket: &Ticket<'_>, reason: Reason, mode: PolicyMode) {
        Refusal {
            tenant: &ticket.identity.tenant_slug,
            service: &ticket.identity.service_name,
            handler: ticket.handler,
            class: ticket.class,
            reason,
            mode,
        }
        .record(self.ledger.as_ref().map(|ledger| ledger.in_use()));
    }

    async fn slot(
        &self,
        ticket: &Ticket<'_>,
        semaphore: Arc<Semaphore>,
        reason: Reason,
        until: Option<Instant>,
        enforced: bool,
    ) -> Result<Option<OwnedSemaphorePermit>, ExecutionError> {
        let waits = enforced && ticket.class != Class::Nested;
        let taken = match (waits, until) {
            (false, _) => semaphore.try_acquire_owned().ok(),
            (true, Some(until)) => tokio::time::timeout_at(until, semaphore.acquire_owned())
                .await
                .ok()
                .and_then(Result::ok),
            (true, None) => semaphore.acquire_owned().await.ok(),
        };
        match (taken, enforced) {
            (Some(permit), _) => Ok(Some(permit)),
            (None, true) => {
                self.refuse(ticket, reason, PolicyMode::Enforce);
                Err(self.overloaded(reason, false))
            }
            (None, false) => {
                self.refuse(ticket, reason, PolicyMode::Warn);
                Ok(None)
            }
        }
    }

    pub(crate) async fn route_slot(
        &self,
        ticket: &Ticket<'_>,
        until: Option<Instant>,
    ) -> Result<Option<OwnedSemaphorePermit>, ExecutionError> {
        match ticket.route_limit {
            Some(max) => {
                let semaphore =
                    self.routes
                        .semaphore(ticket.identity.service_id, ticket.handler, max);
                self.slot(ticket, semaphore, Reason::RouteConcurrency, until, true)
                    .await
            }
            None => Ok(None),
        }
    }

    async fn global_slot(
        &self,
        ticket: &Ticket<'_>,
        global: &Arc<Semaphore>,
        until: Option<Instant>,
    ) -> Result<OwnedSemaphorePermit, ExecutionError> {
        let reason = Reason::ServiceConcurrency;
        match (self.config.enforces(), ticket.class) {
            (true, Class::Nested) => Arc::clone(&self.nested).try_acquire_owned().map_err(|_| {
                self.refuse(ticket, reason, PolicyMode::Enforce);
                self.overloaded(reason, false)
            }),
            (true, _) => self
                .slot(ticket, Arc::clone(global), reason, until, true)
                .await?
                .ok_or_else(|| self.overloaded(reason, false)),
            (false, _) => Arc::clone(global)
                .acquire_owned()
                .await
                .map_err(|e| ExecutionError::Runtime(format!("executor closed: {e}"))),
        }
    }

    async fn take(
        &self,
        ticket: &Ticket<'_>,
        global: &Arc<Semaphore>,
        until: Option<Instant>,
    ) -> Result<Admission, ExecutionError> {
        let mode = self.config.mode;
        let enforce = self.config.enforces();
        let governed = mode != PolicyMode::Off;
        let reason = Reason::ServiceConcurrency;
        let memory_first = ticket.class == Class::Background;
        let mut charge = match (&self.ledger, memory_first) {
            (Some(ledger), true) => Some(self.reserve(ledger, ticket, until).await?),
            _ => None,
        };
        let mut slots = Vec::with_capacity(6);
        slots.extend(self.route_slot(ticket, until).await?);
        if governed {
            let service = self.services.of(&ServiceKey::of(ticket.identity));
            let capped = match (ticket.class, ticket.kind) {
                (Class::Committed, InvocationKind::OnMigrate) => None,
                (Class::Committed, _) => Some(service.committed),
                (Class::Background, _) => Some(service.background),
                (Class::Interactive | Class::Nested, _) => None,
            };
            if let Some(capped) = capped {
                slots.extend(self.slot(ticket, capped, reason, until, enforce).await?);
            }
            slots.extend(
                self.slot(ticket, service.all, reason, until, enforce)
                    .await?,
            );
        }
        if enforce && ticket.class == Class::Background {
            slots.extend(
                self.slot(ticket, Arc::clone(&self.background), reason, until, true)
                    .await?,
            );
        }
        slots.push(self.global_slot(ticket, global, until).await?);
        if let (Some(ledger), false) = (&self.ledger, memory_first) {
            charge = Some(self.reserve(ledger, ticket, until).await?);
        }
        Ok(Admission {
            slots,
            charge,
            budget_from: None,
        })
    }

    async fn reserve(
        &self,
        ledger: &Arc<Ledger>,
        ticket: &Ticket<'_>,
        until: Option<Instant>,
    ) -> Result<Charge, ExecutionError> {
        let service = ServiceKey::of(ticket.identity);
        let estimate = self.estimate(
            ledger,
            ticket.class,
            ticket.identity,
            ticket.handler,
            ticket.initial_memory,
        );
        metrics::histogram!("cufflink_wasm_estimate_bytes", "kind" => ticket.kind.label())
            .record(estimate as f64);
        let refused = |reason| {
            self.refuse(ticket, reason, PolicyMode::Enforce);
            self.overloaded(reason, false)
        };
        match (self.config.enforces(), ticket.class) {
            (true, Class::Nested) => ledger
                .try_reserve(ticket.class, &service, estimate)
                .map_err(refused),
            (true, _) => ledger
                .reserve(ticket.class, &service, estimate, until)
                .await
                .map_err(refused),
            (false, _) => {
                if let Some(reason) = ledger.refusal(ticket.class, &service, estimate) {
                    self.refuse(ticket, reason, PolicyMode::Warn);
                }
                ledger
                    .force_reserve(ticket.class, &service, estimate)
                    .ok_or_else(|| self.overloaded(Reason::Memory, false))
            }
        }
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn return_large_blocks_to_the_kernel() {
    const ONE_MIB: libc::c_int = 1024 * 1024;
    // SAFETY: mallopt only sets an allocator parameter, and runs before the runtime starts.
    if unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, ONE_MIB) } == 0 {
        tracing::warn!(target: "cufflink::wasm::budget", "mallopt(M_MMAP_THRESHOLD) was refused");
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn return_large_blocks_to_the_kernel() {}

pub fn runtime() -> eyre::Result<tokio::runtime::Runtime> {
    return_large_blocks_to_the_kernel();
    let max_concurrent: usize = cufflink_db::env_var("WASM_MAX_CONCURRENT", 256)?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(thread_stack_bytes(cufflink_settings::process_var)?)
        .thread_keep_alive(Duration::from_secs(2))
        .max_blocking_threads(
            max_concurrent
                .saturating_mul(1 + usize::from(MAX_CALL_DEPTH))
                .saturating_add(64),
        )
        .build()
        .map_err(|e| eyre::eyre!("build the runtime: {e}"))
}
