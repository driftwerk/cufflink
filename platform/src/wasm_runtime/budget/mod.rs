mod caps;
mod cgroup;
mod config;
mod estimates;
mod hold;
mod json;
mod ledger;
mod pressure;
mod seat;
mod slots;
mod stack;

pub(crate) use caps::{base64_len, IMAGE_MAX_SIDE};
#[doc(hidden)]
pub use caps::{s3_host_bytes, IMAGE_BYTES_PER_PIXEL};
pub use caps::{CapExceeded, HostCaps};
pub use cgroup::{detect as detect_cgroup, pids_max as detect_pids_max, CgroupLimit, LimitSource};
pub use config::{thread_stack_bytes, BudgetConfig, WaitRelease};
pub(crate) use hold::{Account, Capped, Drawing, STEP};
pub use hold::{Capacity, Hold, Meter, Refused};
pub(crate) use json::stepped;
pub use json::{ChargedJson, ChargedJsonError, Parsed};
pub use ledger::Limits;
pub(crate) use ledger::{Charge, Ledger, Verdict, TABLE_ELEMENT_BYTES};
pub(crate) use seat::{Gate, Held, Seat, Taken};
pub(crate) use stack::enter as enter_thread;

use super::{ExecutionError, InvocationKind};
use crate::auth::service_identity::MAX_CALL_DEPTH;
use crate::policy::record::{LogThrottle, LOG_THROTTLE};
use crate::policy::PolicyMode;
use crate::registry::ServiceIdentity;
use estimates::Estimates;
use seat::Parking;
pub(crate) use slots::RouteKey;
use slots::{RouteSlots, ServiceSlot, ServiceSlots};
use std::sync::atomic::{AtomicUsize, Ordering};
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
    DeliveryLimit,
}

impl Reason {
    pub fn label(self) -> &'static str {
        match self {
            Reason::Memory => "memory",
            Reason::ServiceShare => "service_share",
            Reason::ServiceConcurrency => "service_concurrency",
            Reason::RouteConcurrency => "route_concurrency",
            Reason::Pressure => "pressure",
            Reason::DeliveryLimit => "delivery_limit",
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
    pub route: Option<(RouteKey, u32)>,
    pub initial_memory: usize,
    pub budget: Duration,
}

pub struct Admission {
    seat: Seat,
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

    pub(crate) fn into_parts(self) -> (Seat, Option<Charge>) {
        (self.seat, self.charge)
    }
}

impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admission")
            .field("slots", &self.seat.len())
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
    nested: Arc<Gate>,
    background: Arc<Gate>,
    parked: Option<Arc<Semaphore>>,
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
            services: ServiceSlots::new(
                config.per_service,
                config
                    .wait_release
                    .map_or(0, |release| release.max_parked_per_service),
            ),
            nested: Gate::new(Pools::of(max_concurrent, config.wait_release).nested),
            background: Gate::new((max_concurrent / 4 * 3).max(1)),
            parked: config
                .wait_release
                .map(|release| Arc::new(Semaphore::new(release.max_parked))),
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

    pub(crate) fn account(
        &self,
        identity: &ServiceIdentity,
        class: Class,
        handler: &str,
        kind: InvocationKind,
    ) -> Option<Account> {
        self.ledger.as_ref().map(|ledger| {
            Account::new(
                Arc::clone(ledger),
                class,
                ServiceKey::of(identity),
                handler,
                kind,
            )
        })
    }

    pub(crate) fn kept(
        &self,
        identity: &ServiceIdentity,
        handler: &str,
        initial_memory: usize,
    ) -> Hold {
        let room = self.ledger.as_ref().map_or(0, |ledger| {
            self.estimate(ledger, Class::Committed, identity, handler, initial_memory)
        });
        Hold::kept(
            self.account(identity, Class::Committed, handler, InvocationKind::Hook),
            room,
        )
    }

    pub(crate) fn publish(&self) {
        if let Some(ledger) = &self.ledger {
            ledger.publish();
        }
        metrics::gauge!("cufflink_wasm_blocking_threads").set(blocking_threads() as f64);
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

    pub(crate) fn refusal(&self, reason: Reason, holding: usize, ran: bool) -> ExecutionError {
        let limit = self.ledger.as_ref().map(|ledger| match reason {
            Reason::DeliveryLimit => ledger.limits().delivery(),
            _ => ledger.limits().service,
        });
        match limit {
            Some(share) if holding > share => ExecutionError::TooLarge {
                requested: holding,
                share,
            },
            _ => self.overloaded(reason, ran),
        }
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
        global: &Arc<Gate>,
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
        gate: &Gate,
        reason: Reason,
        until: Option<Instant>,
        enforced: bool,
    ) -> Result<Option<OwnedSemaphorePermit>, ExecutionError> {
        let waits = enforced && ticket.class != Class::Nested;
        let taken = match (waits, until) {
            (false, _) => gate.try_take(),
            (true, Some(until)) => tokio::time::timeout_at(until, gate.admit())
                .await
                .ok()
                .and_then(Result::ok),
            (true, None) => gate.admit().await.ok(),
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
        match ticket.route {
            Some((key, max)) => {
                let gate = self.routes.gate(key, max);
                self.slot(ticket, &gate, Reason::RouteConcurrency, until, true)
                    .await
            }
            None => Ok(None),
        }
    }

    async fn releasable(
        &self,
        ticket: &Ticket<'_>,
        gate: &Arc<Gate>,
        until: Option<Instant>,
        enforced: bool,
    ) -> Result<Option<Taken>, ExecutionError> {
        Ok(self
            .slot(ticket, gate, Reason::ServiceConcurrency, until, enforced)
            .await?
            .map(|permit| Taken::new(Arc::clone(gate), permit)))
    }

    async fn global_slot(
        &self,
        ticket: &Ticket<'_>,
        global: &Arc<Gate>,
        until: Option<Instant>,
    ) -> Result<Taken, ExecutionError> {
        let reason = Reason::ServiceConcurrency;
        match (self.config.enforces(), ticket.class) {
            (true, Class::Nested) => self
                .nested
                .try_take()
                .map(|permit| Taken::new(Arc::clone(&self.nested), permit))
                .ok_or_else(|| {
                    self.refuse(ticket, reason, PolicyMode::Enforce);
                    self.overloaded(reason, false)
                }),
            (true, _) => self
                .releasable(ticket, global, until, true)
                .await?
                .ok_or_else(|| self.overloaded(reason, false)),
            (false, _) => global
                .admit()
                .await
                .map(|permit| Taken::new(Arc::clone(global), permit))
                .map_err(|e| ExecutionError::Runtime(format!("executor closed: {e}"))),
        }
    }

    fn parking(&self, ticket: &Ticket<'_>, service: &ServiceSlot) -> Result<Parking, Held> {
        match (ticket.class, ticket.kind, &self.parked) {
            (Class::Nested | Class::Committed, _, _)
            | (_, InvocationKind::SseReducer | InvocationKind::OnMigrate, _) => {
                Err(Held::NotParkable)
            }
            (_, _, None) => Err(Held::Disabled),
            (Class::Interactive | Class::Background, _, Some(global)) => Ok(Parking::new(
                Arc::clone(global),
                Arc::clone(&service.parked),
            )),
        }
    }

    async fn take(
        &self,
        ticket: &Ticket<'_>,
        global: &Arc<Gate>,
        until: Option<Instant>,
    ) -> Result<Admission, ExecutionError> {
        let enforce = self.config.enforces();
        let governed = self.config.mode != PolicyMode::Off;
        let memory_first = ticket.class == Class::Background;
        let mut charge = match (&self.ledger, memory_first) {
            (Some(ledger), true) => Some(self.reserve(ledger, ticket, until).await?),
            _ => None,
        };
        let route = self.route_slot(ticket, until).await?;
        let service = self.services.of(&ServiceKey::of(ticket.identity));
        let mut releasable = Vec::with_capacity(5);
        if governed {
            let capped = match (ticket.class, ticket.kind) {
                (Class::Committed, InvocationKind::OnMigrate) => None,
                (Class::Committed, _) => Some(&service.committed),
                (Class::Background, _) => Some(&service.background),
                (Class::Interactive | Class::Nested, _) => None,
            };
            if let Some(capped) = capped {
                releasable.extend(self.releasable(ticket, capped, until, enforce).await?);
            }
            releasable.extend(
                self.releasable(ticket, &service.all, until, enforce)
                    .await?,
            );
        }
        if enforce && ticket.class == Class::Background {
            releasable.extend(
                self.releasable(ticket, &self.background, until, true)
                    .await?,
            );
        }
        releasable.push(self.global_slot(ticket, global, until).await?);
        if let (Some(ledger), false) = (&self.ledger, memory_first) {
            charge = Some(self.reserve(ledger, ticket, until).await?);
        }
        Ok(Admission {
            seat: Seat::new(
                route,
                releasable,
                self.parking(ticket, &service),
                ticket.kind,
            ),
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

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
fn sets_mmap_threshold(var: impl Fn(&str) -> Option<String>) -> bool {
    let budgeted = var("WASM_MEMORY_BUDGET_MODE")
        .and_then(|mode| mode.parse::<PolicyMode>().ok())
        .unwrap_or(PolicyMode::Warn)
        != PolicyMode::Off;
    let operator_set = var("MALLOC_MMAP_THRESHOLD_").is_some()
        || var("GLIBC_TUNABLES")
            .is_some_and(|tunables| tunables.contains("glibc.malloc.mmap_threshold"));
    budgeted && !operator_set
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn return_large_blocks_to_the_kernel() {
    const ONE_MIB: libc::c_int = 1024 * 1024;
    if !sets_mmap_threshold(cufflink_settings::process_var) {
        return;
    }
    // SAFETY: mallopt only sets an allocator parameter, and runs before the runtime starts.
    if unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, ONE_MIB) } == 0 {
        tracing::warn!(target: "cufflink::wasm::budget", "mallopt(M_MMAP_THRESHOLD) was refused");
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn return_large_blocks_to_the_kernel() {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pools {
    pub blocking: usize,
    pub nested: usize,
}

impl Pools {
    const SPARE_BLOCKING: usize = 64;

    pub fn of(max_concurrent: usize, wait_release: Option<WaitRelease>) -> Self {
        let in_flight = max_concurrent.saturating_add(WaitRelease::parked(wait_release));
        let depth = usize::from(MAX_CALL_DEPTH);
        Self {
            blocking: in_flight
                .saturating_mul(1 + depth)
                .saturating_add(Self::SPARE_BLOCKING),
            nested: in_flight.saturating_mul(depth),
        }
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let vars = cufflink_db::Vars(&var);
        let max_concurrent = vars.positive("WASM_MAX_CONCURRENT", 256usize)?;
        let per_service = vars
            .positive(
                "WASM_MAX_CONCURRENT_PER_SERVICE",
                BudgetConfig::default().per_service,
            )?
            .min(max_concurrent);
        Ok(Self::of(
            max_concurrent,
            WaitRelease::from_vars(&var, max_concurrent, per_service)?,
        ))
    }

    pub fn log_startup(&self, pids_max: Option<u64>, workers: usize) {
        if let Some(pids_max) = cgroup::ceiling_reaches(pids_max, self.blocking, workers) {
            tracing::warn!(
                target: "cufflink::wasm::budget",
                pids_max,
                blocking = self.blocking,
                workers,
                "The blocking pool's ceiling reaches the cgroup's pids.max; lower WASM_MAX_PARKED \
                 or WASM_MAX_CONCURRENT, or raise the limit"
            );
        }
    }
}

static RUNTIME_THREADS: AtomicUsize = AtomicUsize::new(0);

fn blocking_threads() -> usize {
    let workers =
        tokio::runtime::Handle::try_current().map_or(0, |runtime| runtime.metrics().num_workers());
    RUNTIME_THREADS
        .load(Ordering::Relaxed)
        .saturating_sub(workers)
}

pub fn runtime() -> eyre::Result<tokio::runtime::Runtime> {
    return_large_blocks_to_the_kernel();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(thread_stack_bytes(cufflink_settings::process_var)?)
        .thread_keep_alive(Duration::from_secs(2))
        .max_blocking_threads(Pools::from_vars(cufflink_settings::process_var)?.blocking)
        .on_thread_start(|| {
            RUNTIME_THREADS.fetch_add(1, Ordering::Relaxed);
        })
        .on_thread_stop(|| {
            RUNTIME_THREADS.fetch_sub(1, Ordering::Relaxed);
        })
        .build()
        .map_err(|e| eyre::eyre!("build the runtime: {e}"))
}

#[cfg(test)]
mod tests {
    use super::{sets_mmap_threshold, Class, Pools};

    #[test]
    fn the_mmap_threshold_is_set_only_under_a_budget_and_never_over_the_operators() {
        let with = |pairs: &'static [(&'static str, &'static str)]| {
            sets_mmap_threshold(cufflink_settings::fixed(pairs))
        };
        assert!(with(&[]));
        assert!(with(&[("WASM_MEMORY_BUDGET_MODE", "enforce")]));
        assert!(!with(&[("WASM_MEMORY_BUDGET_MODE", "off")]));
        assert!(!with(&[("MALLOC_MMAP_THRESHOLD_", "4194304")]));
        assert!(!with(&[(
            "GLIBC_TUNABLES",
            "glibc.malloc.arena_max=2:glibc.malloc.mmap_threshold=4194304"
        )]));
        assert!(with(&[("GLIBC_TUNABLES", "glibc.malloc.arena_max=2")]));
    }

    #[test]
    fn the_blocking_and_nested_pools_cover_parked_chains() {
        let pools = |pairs: &'static [(&'static str, &'static str)]| {
            Pools::from_vars(cufflink_settings::fixed(pairs)).expect("pools")
        };
        assert_eq!(
            pools(&[]),
            Pools {
                blocking: (256 + 256) * 5 + 64,
                nested: (256 + 256) * 4,
            }
        );
        assert_eq!(
            pools(&[("WASM_MAX_CONCURRENT", "16"), ("WASM_MAX_PARKED", "8")]),
            Pools {
                blocking: (16 + 8) * 5 + 64,
                nested: (16 + 8) * 4,
            }
        );
        assert_eq!(
            pools(&[("WASM_WAIT_RELEASE", "off")]),
            Pools {
                blocking: 256 * 5 + 64,
                nested: 256 * 4,
            },
            "with releasing off, the sizes are today's"
        );
        for (name, value) in [
            ("WASM_WAIT_RELEASE", "maybe"),
            ("WASM_MAX_PARKED", "0"),
            ("WASM_WAIT_RELEASE_AFTER_MS", "soon"),
        ] {
            let err = Pools::from_vars(cufflink_settings::fixed(&[(name, value)])).unwrap_err();
            assert!(err.to_string().contains(name), "{name}: {err}");
        }
    }

    #[test]
    fn a_call_with_a_caller_is_nested_and_any_other_is_interactive() {
        assert_eq!(Class::of_call(None), Class::Interactive);
        assert_eq!(Class::of_call(Some(0)), Class::Interactive);
        assert_eq!(Class::of_call(Some(1)), Class::Nested);
        assert_eq!(Class::of_call(Some(u8::MAX)), Class::Nested);
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::ledger::{Ledger, Limits, Settings};
    use super::{Account, Class, Hold, ServiceKey};
    use crate::wasm_runtime::InvocationKind;
    use std::sync::Arc;
    use std::time::Duration;

    pub(crate) const MIB: usize = 1024 * 1024;

    pub(crate) fn hold(budget: usize) -> (Arc<Ledger>, Hold) {
        held(budget, true)
    }

    pub(crate) fn warned(budget: usize) -> (Arc<Ledger>, Hold) {
        held(budget, false)
    }

    fn held(budget: usize, enforce: bool) -> (Arc<Ledger>, Hold) {
        let ledger = Arc::new(Ledger::new(Settings {
            limits: Limits {
                budget,
                admit: budget,
                service: budget,
                background: budget,
                overrun: budget,
            },
            enforce,
            stack: MIB,
            aging: Duration::from_millis(100),
        }));
        let account = Account::new(
            Arc::clone(&ledger),
            Class::Interactive,
            ServiceKey {
                tenant: "acme".into(),
                service: "catalog-service".into(),
            },
            "list",
            InvocationKind::Route,
        );
        (ledger, Hold::on(Some(account)))
    }
}
