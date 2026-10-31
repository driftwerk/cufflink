use super::{Class, Reason, ServiceKey};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};
use tokio::time::Instant;

pub(crate) const TABLE_ELEMENT_BYTES: usize = 8;
const GRANT_TIERS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub budget: usize,
    pub admit: usize,
    pub service: usize,
    pub background: usize,
    pub overrun: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Settings {
    pub limits: Limits,
    pub enforce: bool,
    pub stack: usize,
    pub aging: Duration,
}

struct Waiter {
    id: u64,
    class: Class,
    service: ServiceKey,
    bytes: usize,
    since: Instant,
    granted: oneshot::Sender<Stack>,
}

struct DeliveryWaiter {
    id: u64,
    bytes: usize,
    granted: oneshot::Sender<()>,
}

#[derive(Default)]
struct State {
    in_use: usize,
    by_class: [usize; Class::ALL.len()],
    by_service: HashMap<ServiceKey, usize>,
    kept: HashMap<ServiceKey, usize>,
    kept_total: usize,
    delivering: usize,
    deliveries: VecDeque<DeliveryWaiter>,
    residue: usize,
    threads: usize,
    busy: usize,
    claimed: usize,
    queue: VecDeque<Waiter>,
    next_id: u64,
}

impl State {
    fn service(&self, service: &ServiceKey) -> usize {
        self.by_service.get(service).copied().unwrap_or_default()
    }

    fn against_share(&self, class: Class, service: &ServiceKey) -> usize {
        let held = self.service(service);
        match class {
            Class::Committed => {
                held.saturating_sub(self.kept.get(service).copied().unwrap_or_default())
            }
            Class::Interactive | Class::Nested | Class::Background => held,
        }
    }

    fn keep(&mut self, service: &ServiceKey, bytes: usize) {
        *self.kept.entry(service.clone()).or_default() += bytes;
        self.kept_total += bytes;
    }

    fn unkeep(&mut self, service: &ServiceKey, bytes: usize) {
        self.kept_total = self.kept_total.saturating_sub(bytes);
        let kept = self.kept.get_mut(service).map_or(0, |kept| {
            *kept = kept.saturating_sub(bytes);
            *kept
        });
        if kept == 0 {
            self.kept.remove(service);
        }
    }

    fn open_delivery(&mut self, bytes: usize) {
        self.in_use += bytes;
        self.delivering += bytes;
    }

    fn close_delivery(&mut self, bytes: usize) {
        self.in_use = self.in_use.saturating_sub(bytes);
        self.delivering = self.delivering.saturating_sub(bytes);
    }

    fn idle_threads(&self) -> usize {
        self.threads.saturating_sub(self.busy + self.claimed)
    }

    fn snapshot(&self, now: Instant) -> Snapshot {
        let mut waiters = [0; Class::ALL.len()];
        let mut oldest = [Duration::ZERO; Class::ALL.len()];
        for waiter in &self.queue {
            let at = waiter.class.index();
            waiters[at] += 1;
            oldest[at] = oldest[at].max(now.saturating_duration_since(waiter.since));
        }
        Snapshot {
            by_class: self.by_class,
            residue: self.residue,
            delivering: self.delivering,
            waiters,
            delivery_waiters: self.deliveries.len(),
            oldest,
        }
    }
}

struct Snapshot {
    by_class: [usize; Class::ALL.len()],
    residue: usize,
    delivering: usize,
    waiters: [usize; Class::ALL.len()],
    delivery_waiters: usize,
    oldest: [Duration; Class::ALL.len()],
}

impl Limits {
    pub(crate) fn delivery(&self) -> usize {
        self.service.min(self.overrun)
    }
}

impl Snapshot {
    fn publish(&self) {
        for class in Class::ALL {
            let at = class.index();
            metrics::gauge!("cufflink_wasm_memory_in_use_bytes", "class" => class.label())
                .set(self.by_class[at] as f64);
            metrics::gauge!("cufflink_wasm_admission_waiters", "class" => class.label())
                .set(self.waiters[at] as f64);
            metrics::gauge!("cufflink_wasm_admission_oldest_wait_seconds", "class" => class.label())
                .set(self.oldest[at].as_secs_f64());
        }
        metrics::gauge!("cufflink_wasm_memory_in_use_bytes", "class" => "stack_residue")
            .set(self.residue as f64);
        metrics::gauge!("cufflink_wasm_memory_in_use_bytes", "class" => "delivery")
            .set(self.delivering as f64);
        metrics::gauge!("cufflink_wasm_admission_waiters", "class" => "delivery")
            .set(self.delivery_waiters as f64);
    }
}

fn publish_service(service: &ServiceKey, held: usize) {
    metrics::gauge!(
        "cufflink_wasm_memory_service_bytes",
        "tenant" => service.tenant.to_string(),
        "service" => service.service.to_string(),
    )
    .set(held as f64);
}

impl Snapshot {
    fn publish_services(
        services: &HashMap<ServiceKey, usize>,
        published: &mut HashSet<ServiceKey>,
    ) {
        published.retain(|service| {
            let held = services.contains_key(service);
            if !held {
                publish_service(service, 0);
            }
            held
        });
        for (service, held) in services {
            publish_service(service, *held);
            published.insert(service.clone());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stack {
    Reserved,
    Claimed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThreadStart {
    Charged,
    ReleaseReserved,
    MoveReserved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Allowed,
    Warned(Reason),
    Denied(Reason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Limit {
    Pressure,
    Ceiling,
    Service,
    Background,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Breach {
    pressure: bool,
    ceiling: bool,
    service: bool,
    background: bool,
}

impl Breach {
    fn reason(self) -> Option<Reason> {
        if self.pressure {
            Some(Reason::Pressure)
        } else if self.ceiling {
            Some(Reason::Memory)
        } else if self.service {
            Some(Reason::ServiceShare)
        } else if self.background {
            Some(Reason::Memory)
        } else {
            None
        }
    }

    fn narrowest(self) -> Option<Limit> {
        if self.service {
            Some(Limit::Service)
        } else if self.background {
            Some(Limit::Background)
        } else if self.ceiling {
            Some(Limit::Ceiling)
        } else if self.pressure {
            Some(Limit::Pressure)
        } else {
            None
        }
    }
}

#[derive(Default)]
struct HeldBack {
    all: bool,
    background: bool,
    services: HashSet<ServiceKey>,
}

impl HeldBack {
    fn hold(&mut self, limit: Limit, service: &ServiceKey) {
        match limit {
            Limit::Pressure => {}
            Limit::Ceiling => self.all = true,
            Limit::Background => self.background = true,
            Limit::Service => {
                self.services.insert(service.clone());
            }
        }
    }

    fn holds(&self, waiter: &Waiter) -> bool {
        self.all
            || (self.background && waiter.class == Class::Background)
            || self.services.contains(&waiter.service)
    }

    fn join(&mut self, other: HeldBack) {
        self.all |= other.all;
        self.background |= other.background;
        self.services.extend(other.services);
    }
}

pub(crate) struct Ledger {
    settings: Settings,
    pressure: AtomicBool,
    credited: Notify,
    state: Mutex<State>,
    published: Mutex<HashSet<ServiceKey>>,
}

impl Ledger {
    pub(crate) fn new(settings: Settings) -> Self {
        Self {
            settings,
            pressure: AtomicBool::new(false),
            credited: Notify::new(),
            state: Mutex::default(),
            published: Mutex::default(),
        }
    }

    pub(crate) fn limits(&self) -> Limits {
        self.settings.limits
    }

    pub(crate) fn in_use(&self) -> usize {
        self.lock().in_use
    }

    #[cfg(test)]
    pub(crate) fn kept_total(&self) -> usize {
        self.lock().kept_total
    }

    #[cfg(test)]
    pub(crate) fn delivering(&self) -> usize {
        self.lock().delivering
    }

    #[cfg(test)]
    pub(crate) fn delivery_waiters(&self) -> usize {
        self.lock().deliveries.len()
    }

    #[cfg(test)]
    pub(crate) fn residue(&self) -> usize {
        self.lock().residue
    }

    #[cfg(test)]
    pub(crate) fn in_use_by(&self, class: Class) -> usize {
        self.lock().by_class[class.index()]
    }

    pub(crate) fn set_pressure(&self, on: bool) {
        if self.pressure.swap(on, Ordering::SeqCst) && !on {
            self.settled(|state, _| self.grant(state));
            self.credited.notify_waiters();
        }
    }

    pub(crate) fn largest(&self, class: Class) -> usize {
        let limits = self.settings.limits;
        let ceiling = match class {
            Class::Nested => limits.budget,
            Class::Background => limits.admit.min(limits.background),
            Class::Interactive | Class::Committed => limits.admit,
        };
        ceiling
            .min(limits.service)
            .saturating_sub(self.settings.stack)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn settled<T>(&self, change: impl FnOnce(&mut State, Instant) -> T) -> T {
        let (answer, credited) = {
            let mut state = self.lock();
            let before = state.in_use;
            let answer = change(&mut state, Instant::now());
            (answer, state.in_use < before)
        };
        if credited {
            self.credited.notify_waiters();
        }
        answer
    }

    pub(crate) fn publish(&self) {
        let (snapshot, services) = {
            let state = self.lock();
            (state.snapshot(Instant::now()), state.by_service.clone())
        };
        snapshot.publish();
        let mut published = self
            .published
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Snapshot::publish_services(&services, &mut published);
    }

    fn stack_for(&self, state: &State) -> (usize, Stack) {
        match state.idle_threads() {
            0 => (self.settings.stack, Stack::Reserved),
            _ => (0, Stack::Claimed),
        }
    }

    fn breach(&self, state: &State, class: Class, service: &ServiceKey, bytes: usize) -> Breach {
        let limits = self.settings.limits;
        let ceiling = match class {
            Class::Nested => limits.budget,
            _ => limits.admit,
        };
        Breach {
            pressure: class.gated_by_pressure() && self.pressure.load(Ordering::SeqCst),
            ceiling: state.in_use + bytes > ceiling,
            service: state.against_share(class, service) + bytes > limits.service,
            background: class == Class::Background
                && state.by_class[Class::Background.index()] + bytes > limits.background,
        }
    }

    fn admits(
        &self,
        state: &State,
        class: Class,
        service: &ServiceKey,
        bytes: usize,
    ) -> Result<(), Reason> {
        self.breach(state, class, service, bytes)
            .reason()
            .map_or(Ok(()), Err)
    }

    fn fits(&self, state: &State, held: usize, bytes: usize) -> Result<(), Reason> {
        let limits = self.settings.limits;
        if state.in_use + bytes > limits.budget {
            Err(Reason::Memory)
        } else if held + bytes > limits.service {
            Err(Reason::ServiceShare)
        } else {
            Ok(())
        }
    }

    fn allows_keeping(
        &self,
        state: &State,
        service: &ServiceKey,
        bytes: usize,
        room: usize,
    ) -> Result<(), Reason> {
        self.fits(state, state.service(service), bytes)?;
        let crowded = state.kept_total + bytes + room + self.settings.stack;
        match crowded > self.settings.limits.admit {
            true => Err(Reason::Memory),
            false => Ok(()),
        }
    }

    fn debit_unless(
        &self,
        allowed: impl FnOnce(&State) -> Result<(), Reason>,
        debit: impl FnOnce(&mut State),
    ) -> Verdict {
        self.settled(|state, _| match allowed(state) {
            Err(reason) if self.settings.enforce => Verdict::Denied(reason),
            allowed => {
                debit(state);
                allowed.err().map_or(Verdict::Allowed, Verdict::Warned)
            }
        })
    }

    fn take(&self, state: &mut State, class: Class, service: &ServiceKey, bytes: usize) {
        state.in_use += bytes;
        state.by_class[class.index()] += bytes;
        *state.by_service.entry(service.clone()).or_default() += bytes;
    }

    fn give(&self, state: &mut State, class: Class, service: &ServiceKey, bytes: usize) {
        state.in_use = state.in_use.saturating_sub(bytes);
        let by_class = &mut state.by_class[class.index()];
        *by_class = by_class.saturating_sub(bytes);
        let held = state.by_service.get_mut(service).map_or(0, |held| {
            *held = held.saturating_sub(bytes);
            *held
        });
        if held == 0 {
            state.by_service.remove(service);
        }
    }

    fn tier(&self, waiter: &Waiter, now: Instant) -> usize {
        match waiter.class {
            Class::Committed => 0,
            Class::Background
                if now.saturating_duration_since(waiter.since) >= self.settings.aging =>
            {
                1
            }
            Class::Interactive | Class::Nested => 2,
            Class::Background => 3,
        }
    }

    fn arrival_tier(class: Class) -> usize {
        match class {
            Class::Committed => 0,
            Class::Interactive | Class::Nested => 2,
            Class::Background => 3,
        }
    }

    fn queued_ahead(&self, state: &State, class: Class, now: Instant) -> bool {
        let tier = Self::arrival_tier(class);
        state
            .queue
            .iter()
            .any(|waiter| !waiter.granted.is_closed() && self.tier(waiter, now) <= tier)
    }

    fn delivery_fits(&self, state: &State, bytes: usize) -> bool {
        let limits = self.settings.limits;
        state.in_use + bytes <= limits.budget + limits.overrun
    }

    fn grant_deliveries(&self, state: &mut State) {
        while let Some(waiter) = state.deliveries.front() {
            let gone = waiter.granted.is_closed();
            if !gone && !self.delivery_fits(state, waiter.bytes) {
                break;
            }
            let Some(waiter) = state.deliveries.pop_front() else {
                break;
            };
            if !gone {
                state.open_delivery(waiter.bytes);
                if waiter.granted.send(()).is_err() {
                    state.close_delivery(waiter.bytes);
                }
            }
        }
    }

    fn grant(&self, state: &mut State) {
        let now = Instant::now();
        self.grant_deliveries(state);
        let mut held = HeldBack::default();
        for tier in 0..GRANT_TIERS {
            let mut blocked = HeldBack::default();
            let mut at = 0;
            while let Some(waiter) = state.queue.get(at) {
                let closed = waiter.granted.is_closed();
                if !closed && (self.tier(waiter, now) != tier || held.holds(waiter)) {
                    at += 1;
                    continue;
                }
                let (stack, claim) = self.stack_for(state);
                let breach = match closed {
                    true => Breach::default(),
                    false => {
                        self.breach(state, waiter.class, &waiter.service, waiter.bytes + stack)
                    }
                };
                if let Some(limit) = breach.narrowest() {
                    if tier <= 1 {
                        blocked.hold(limit, &waiter.service);
                    }
                    at += 1;
                    continue;
                }
                let Some(waiter) = state.queue.remove(at) else {
                    break;
                };
                if closed {
                    continue;
                }
                self.take(state, waiter.class, &waiter.service, waiter.bytes + stack);
                if claim == Stack::Claimed {
                    state.claimed += 1;
                }
                if waiter.granted.send(claim).is_err() {
                    self.give(state, waiter.class, &waiter.service, waiter.bytes + stack);
                    if claim == Stack::Claimed {
                        state.claimed -= 1;
                    }
                }
            }
            held.join(blocked);
        }
    }

    pub(super) fn grow(&self, class: Class, service: &ServiceKey, extra: usize) -> Verdict {
        self.debit_unless(
            |state| self.fits(state, state.against_share(class, service), extra),
            |state| self.take(state, class, service, extra),
        )
    }

    pub(super) fn keep(
        &self,
        class: Class,
        service: &ServiceKey,
        extra: usize,
        room: usize,
    ) -> Verdict {
        self.debit_unless(
            |state| self.allows_keeping(state, service, extra, room),
            |state| {
                self.take(state, class, service, extra);
                state.keep(service, extra);
            },
        )
    }

    pub(super) fn deliver(
        &self,
        class: Class,
        service: &ServiceKey,
        extra: usize,
        holding: usize,
    ) -> Verdict {
        self.debit_unless(
            |_| match holding > self.settings.limits.delivery() {
                true => Err(Reason::DeliveryLimit),
                false => Ok(()),
            },
            |state| self.take(state, class, service, extra),
        )
    }

    pub(super) fn force(&self, class: Class, service: &ServiceKey, bytes: usize) {
        self.settled(|state, _| self.take(state, class, service, bytes));
    }

    pub(super) fn force_kept(&self, class: Class, service: &ServiceKey, bytes: usize) {
        self.settled(|state, _| {
            self.take(state, class, service, bytes);
            state.keep(service, bytes);
        });
    }

    pub(super) fn past_the_ceiling(&self) -> bool {
        let limits = self.settings.limits;
        self.settings.enforce && self.lock().in_use > limits.budget + limits.overrun
    }

    pub(super) async fn delivery(
        self: &Arc<Self>,
        wanted: usize,
        holding: usize,
    ) -> Option<Delivery> {
        if !self.settings.enforce {
            return None;
        }
        let bytes = wanted
            .min(self.settings.limits.delivery())
            .saturating_sub(holding);
        let waiting = self.settled(|state, _| {
            if state.deliveries.is_empty() && self.delivery_fits(state, bytes) {
                state.open_delivery(bytes);
                return None;
            }
            let (granted, receiver) = oneshot::channel();
            let id = state.next_id;
            state.next_id += 1;
            state
                .deliveries
                .push_back(DeliveryWaiter { id, bytes, granted });
            Some(WaitingDelivery {
                ledger: Arc::clone(self),
                id,
                bytes,
                receiver,
                settled: false,
            })
        });
        if let Some(mut waiting) = waiting {
            (&mut waiting.receiver).await.ok()?;
            waiting.settled = true;
        }
        Some(Delivery {
            ledger: Arc::clone(self),
            bytes,
        })
    }

    pub(super) fn release(&self, class: Class, service: &ServiceKey, bytes: usize) {
        self.settled(|state, _| {
            self.give(state, class, service, bytes);
            self.grant(state);
        });
    }

    pub(super) fn release_kept(&self, class: Class, service: &ServiceKey, bytes: usize) {
        self.settled(|state, _| {
            state.unkeep(service, bytes);
            self.give(state, class, service, bytes);
            self.grant(state);
        });
    }

    fn charge(
        self: &Arc<Self>,
        class: Class,
        service: &ServiceKey,
        reserved: usize,
        stack: Stack,
    ) -> Charge {
        let stack_part = match stack {
            Stack::Reserved => self.settings.stack,
            Stack::Claimed => 0,
        };
        Charge {
            ledger: Arc::clone(self),
            class,
            service: service.clone(),
            reserved: reserved + stack_part,
            held: reserved + stack_part,
            linear: 0,
            tables: 0,
            grown_from: None,
            tables_from: None,
            peak: 0,
            stack: Some(stack),
        }
    }

    fn debit(
        &self,
        state: &mut State,
        class: Class,
        service: &ServiceKey,
        bytes: usize,
        checked: bool,
    ) -> Result<Stack, Reason> {
        let (stack, claim) = self.stack_for(state);
        if checked {
            self.admits(state, class, service, bytes + stack)?;
        }
        self.take(state, class, service, bytes + stack);
        if claim == Stack::Claimed {
            state.claimed += 1;
        }
        Ok(claim)
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        class: Class,
        service: &ServiceKey,
        bytes: usize,
    ) -> Result<Charge, Reason> {
        let claim = self.settled(|state, _| self.debit(state, class, service, bytes, true))?;
        Ok(self.charge(class, service, bytes, claim))
    }

    pub(crate) fn force_reserve(
        self: &Arc<Self>,
        class: Class,
        service: &ServiceKey,
        bytes: usize,
    ) -> Option<Charge> {
        let claim = self
            .settled(|state, _| self.debit(state, class, service, bytes, false))
            .ok()?;
        Some(self.charge(class, service, bytes, claim))
    }

    pub(crate) fn refusal(
        &self,
        class: Class,
        service: &ServiceKey,
        bytes: usize,
    ) -> Option<Reason> {
        let state = self.lock();
        let (stack, _) = self.stack_for(&state);
        self.admits(&state, class, service, bytes + stack).err()
    }

    pub(crate) async fn reserve(
        self: &Arc<Self>,
        class: Class,
        service: &ServiceKey,
        bytes: usize,
        until: Option<Instant>,
    ) -> Result<Charge, Reason> {
        let queued = {
            let waiting = service.clone();
            self.settled(|state, now| {
                let fast = match self.queued_ahead(state, class, now) {
                    true => None,
                    false => self.debit(state, class, service, bytes, true).ok(),
                };
                match fast {
                    Some(claim) => Ok(claim),
                    None => {
                        let (granted, receiver) = oneshot::channel();
                        let id = state.next_id;
                        state.next_id += 1;
                        state.queue.push_back(Waiter {
                            id,
                            class,
                            service: waiting,
                            bytes,
                            since: now,
                            granted,
                        });
                        self.grant(state);
                        Err((id, receiver))
                    }
                }
            })
        };
        let mut queued = match queued {
            Ok(claim) => return Ok(self.charge(class, service, bytes, claim)),
            Err((id, receiver)) => Queued {
                ledger: Arc::clone(self),
                id,
                class,
                service: service.clone(),
                bytes,
                receiver,
                settled: false,
            },
        };
        let answered = match until {
            Some(until) => tokio::time::timeout_at(until, &mut queued.receiver)
                .await
                .ok(),
            None => Some((&mut queued.receiver).await),
        };
        match answered {
            Some(Ok(stack)) => {
                queued.settled = true;
                Ok(self.charge(class, service, bytes, stack))
            }
            Some(Err(_)) | None => Err(self
                .refusal(class, service, bytes)
                .unwrap_or(Reason::Memory)),
        }
    }

    fn fitting(&self, class: Class, service: &ServiceKey, bytes: usize) -> usize {
        let each = bytes.saturating_add(self.settings.stack).max(1);
        let limits = self.settings.limits;
        let state = self.lock();
        if self.admits(&state, class, service, each).is_err() {
            return 0;
        }
        let room = |limit: usize, used: usize| limit.saturating_sub(used) / each;
        let background = match class {
            Class::Background => room(limits.background, state.by_class[Class::Background.index()]),
            _ => usize::MAX,
        };
        room(limits.admit, state.in_use)
            .min(room(limits.service, state.service(service)))
            .min(background)
    }

    pub(crate) async fn room(&self, class: Class, service: &ServiceKey, bytes: usize) -> usize {
        loop {
            let credited = self.credited.notified();
            tokio::pin!(credited);
            credited.as_mut().enable();
            match self.fitting(class, service, bytes) {
                0 => credited.await,
                fitting => return fitting,
            }
        }
    }

    pub(crate) fn thread_started(&self, stack: Option<Stack>, charged: bool) -> ThreadStart {
        self.settled(|state, _| {
            state.busy += 1;
            if stack == Some(Stack::Claimed) {
                state.claimed = state.claimed.saturating_sub(1);
            }
            match (charged, stack) {
                (true, Some(Stack::Reserved)) => ThreadStart::ReleaseReserved,
                (true, _) => ThreadStart::Charged,
                (false, Some(Stack::Reserved)) => {
                    state.threads += 1;
                    ThreadStart::MoveReserved
                }
                (false, _) => {
                    state.threads += 1;
                    state.in_use += self.settings.stack;
                    state.residue += self.settings.stack;
                    ThreadStart::Charged
                }
            }
        })
    }

    pub(crate) fn move_to_residue(&self, class: Class, service: &ServiceKey) {
        self.settled(|state, _| {
            self.give(state, class, service, self.settings.stack);
            state.in_use += self.settings.stack;
            state.residue += self.settings.stack;
            self.grant(state);
        });
    }

    pub(crate) fn thread_idle(&self) {
        self.settled(|state, _| {
            state.busy = state.busy.saturating_sub(1);
            self.grant(state);
        });
    }

    pub(crate) fn thread_retired(&self) {
        self.settled(|state, _| {
            state.threads = state.threads.saturating_sub(1);
            state.in_use = state.in_use.saturating_sub(self.settings.stack);
            state.residue = state.residue.saturating_sub(self.settings.stack);
            self.grant(state);
        });
    }
}

struct WaitingDelivery {
    ledger: Arc<Ledger>,
    id: u64,
    bytes: usize,
    receiver: oneshot::Receiver<()>,
    settled: bool,
}

impl Drop for WaitingDelivery {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let ledger = Arc::clone(&self.ledger);
        ledger.settled(|state, _| {
            match state
                .deliveries
                .iter()
                .position(|waiter| waiter.id == self.id)
            {
                Some(at) => {
                    state.deliveries.remove(at);
                }
                None => {
                    if self.receiver.try_recv().is_ok() {
                        state.close_delivery(self.bytes);
                    }
                }
            }
            ledger.grant(state);
        });
    }
}

pub(crate) struct Delivery {
    ledger: Arc<Ledger>,
    bytes: usize,
}

impl Drop for Delivery {
    fn drop(&mut self) {
        self.ledger.settled(|state, _| {
            state.close_delivery(self.bytes);
            self.ledger.grant(state);
        });
    }
}

struct Queued {
    ledger: Arc<Ledger>,
    id: u64,
    class: Class,
    service: ServiceKey,
    bytes: usize,
    receiver: oneshot::Receiver<Stack>,
    settled: bool,
}

impl Drop for Queued {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let ledger = Arc::clone(&self.ledger);
        ledger.settled(|state, _| {
            match state.queue.iter().position(|waiter| waiter.id == self.id) {
                Some(at) => {
                    state.queue.remove(at);
                }
                None => {
                    if let Ok(stack) = self.receiver.try_recv() {
                        let stack_part = match stack {
                            Stack::Reserved => ledger.settings.stack,
                            Stack::Claimed => {
                                state.claimed = state.claimed.saturating_sub(1);
                                0
                            }
                        };
                        ledger.give(state, self.class, &self.service, self.bytes + stack_part);
                        ledger.grant(state);
                    }
                }
            }
        });
    }
}

pub(crate) struct Charge {
    ledger: Arc<Ledger>,
    class: Class,
    service: ServiceKey,
    reserved: usize,
    held: usize,
    linear: usize,
    tables: usize,
    grown_from: Option<usize>,
    tables_from: Option<usize>,
    peak: usize,
    stack: Option<Stack>,
}

impl Charge {
    pub(crate) fn ledger(&self) -> &Arc<Ledger> {
        &self.ledger
    }

    pub(crate) fn class(&self) -> Class {
        self.class
    }

    pub(crate) fn service(&self) -> &ServiceKey {
        &self.service
    }

    pub(crate) fn peak(&self) -> usize {
        self.peak
    }

    pub(crate) fn account(
        &self,
        handler: &str,
        kind: crate::wasm_runtime::InvocationKind,
    ) -> super::hold::Account {
        super::hold::Account::new(
            Arc::clone(&self.ledger),
            self.class,
            self.service.clone(),
            handler,
            kind,
        )
    }

    pub(crate) fn linear(&self) -> usize {
        self.linear
    }

    pub(crate) fn tables(&self) -> usize {
        self.tables
    }

    pub(crate) fn holding_with(&self, linear: usize, tables: usize) -> usize {
        linear.saturating_add(tables)
    }

    pub(crate) fn take_stack(&mut self) -> Option<Stack> {
        self.stack.take()
    }

    pub(crate) fn stack_moved(&mut self) {
        let stack = self.ledger.settings.stack;
        self.reserved = self.reserved.saturating_sub(stack);
        self.held = self.held.saturating_sub(stack);
    }

    pub(crate) fn release_stack(&mut self) {
        self.stack_moved();
        self.ledger
            .release(self.class, &self.service, self.ledger.settings.stack);
    }

    pub(crate) fn grow_linear(&mut self, current: usize, desired: usize) -> Verdict {
        let verdict = self.resize(desired, self.tables);
        if !matches!(verdict, Verdict::Denied(_)) {
            self.grown_from = Some(current);
        }
        verdict
    }

    pub(crate) fn linear_grow_failed(&mut self) {
        if let Some(current) = self.grown_from.take() {
            self.resize(current, self.tables);
        }
    }

    pub(crate) fn grow_tables(&mut self, current: usize, desired: usize) -> Verdict {
        let bytes = |elements: usize| elements.saturating_mul(TABLE_ELEMENT_BYTES);
        let before = self.tables;
        let tables = before.saturating_sub(bytes(current)) + bytes(desired);
        let verdict = self.resize(self.linear, tables);
        if !matches!(verdict, Verdict::Denied(_)) {
            self.tables_from = Some(before);
        }
        verdict
    }

    pub(crate) fn table_grow_failed(&mut self) {
        if let Some(tables) = self.tables_from.take() {
            self.resize(self.linear, tables);
        }
    }

    fn resize(&mut self, linear: usize, tables: usize) -> Verdict {
        let used = linear.saturating_add(tables);
        let wanted = self.reserved.max(used);
        let verdict = match wanted.checked_sub(self.held) {
            Some(0) => Verdict::Allowed,
            None => {
                self.ledger
                    .release(self.class, &self.service, self.held - wanted);
                Verdict::Allowed
            }
            Some(extra) => match self.ledger.grow(self.class, &self.service, extra) {
                Verdict::Denied(reason) => return Verdict::Denied(reason),
                verdict => verdict,
            },
        };
        self.held = wanted;
        self.linear = linear;
        self.tables = tables;
        self.peak = self.peak.max(used);
        verdict
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.ledger.settled(|state, _| {
            if self.stack == Some(Stack::Claimed) {
                state.claimed = state.claimed.saturating_sub(1);
            }
            self.ledger
                .give(state, self.class, &self.service, self.held);
            self.ledger.grant(state);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::time::Duration;

    const MIB: usize = 1024 * 1024;
    const STACK: usize = MIB;

    fn ledger(enforce: bool) -> Arc<Ledger> {
        Arc::new(Ledger::new(Settings {
            limits: Limits {
                budget: 100 * MIB,
                admit: 90 * MIB,
                service: 50 * MIB,
                background: 75 * MIB,
                overrun: 50 * MIB,
            },
            enforce,
            stack: STACK,
            aging: Duration::from_millis(100),
        }))
    }

    fn service(name: &str) -> ServiceKey {
        ServiceKey {
            tenant: "acme".into(),
            service: name.into(),
        }
    }

    #[test]
    fn reservations_growth_and_tables_are_credited_when_the_charge_drops() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        let mut charge = ledger
            .try_reserve(Class::Interactive, &catalog, 4 * MIB)
            .expect("fits");
        assert_eq!(ledger.in_use(), 4 * MIB + STACK);
        assert_eq!(charge.grow_linear(0, 2 * MIB), Verdict::Allowed);
        assert_eq!(ledger.in_use(), 4 * MIB + STACK, "within the reservation");
        assert_eq!(charge.grow_linear(2 * MIB, 10 * MIB), Verdict::Allowed);
        assert_eq!(ledger.in_use(), 10 * MIB);
        charge.linear_grow_failed();
        assert_eq!(
            ledger.in_use(),
            4 * MIB + STACK,
            "a failed grow credits what it was approved"
        );
        assert_eq!(charge.grow_tables(0, 1_000_000), Verdict::Allowed);
        assert_eq!(ledger.in_use(), 2 * MIB + 8_000_000);
        charge.table_grow_failed();
        assert_eq!(ledger.in_use(), 4 * MIB + STACK);
        assert_eq!(charge.grow_linear(0, 12 * MIB), Verdict::Allowed);
        assert_eq!(charge.peak(), 12 * MIB);
        drop(charge);
        assert_eq!(ledger.in_use(), 0);
        assert_eq!(ledger.in_use_by(Class::Interactive), 0);
    }

    #[test]
    fn a_service_at_its_share_is_denied_while_the_budget_has_room() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        let mut first = ledger
            .try_reserve(Class::Interactive, &catalog, 20 * MIB)
            .expect("fits");
        assert_eq!(
            first.grow_linear(0, 51 * MIB),
            Verdict::Denied(Reason::ServiceShare)
        );
        assert_eq!(
            ledger.in_use(),
            20 * MIB + STACK,
            "a denial charges nothing"
        );
        assert!(matches!(
            ledger.try_reserve(Class::Interactive, &catalog, 30 * MIB),
            Err(Reason::ServiceShare)
        ));
        assert!(ledger
            .try_reserve(Class::Interactive, &service("loan-service"), 30 * MIB)
            .is_ok());
        assert_eq!(first.grow_linear(0, 25 * MIB), Verdict::Allowed);
    }

    #[test]
    fn the_admit_threshold_leaves_headroom_that_nested_calls_and_growth_may_use() {
        let ledger = ledger(true);
        let held: Vec<Charge> = ["a", "b"]
            .into_iter()
            .map(|name| {
                ledger
                    .try_reserve(Class::Interactive, &service(name), 43 * MIB)
                    .expect("fits")
            })
            .collect();
        assert_eq!(ledger.in_use(), 86 * MIB + 2 * STACK);
        assert!(matches!(
            ledger.try_reserve(Class::Interactive, &service("c"), 4 * MIB),
            Err(Reason::Memory)
        ));
        let nested = ledger
            .try_reserve(Class::Nested, &service("c"), 4 * MIB)
            .expect("a nested call may use the headroom");
        assert!(matches!(
            ledger.try_reserve(Class::Nested, &service("d"), 8 * MIB),
            Err(Reason::Memory)
        ));
        drop((held, nested));
        assert_eq!(ledger.in_use(), 0);
    }

    #[tokio::test]
    async fn kept_text_leaves_a_committed_run_the_whole_of_its_share() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        assert_eq!(
            ledger.keep(Class::Committed, &catalog, 48 * MIB, 4 * MIB),
            Verdict::Allowed
        );
        let until = Instant::now() + Duration::from_millis(10);
        let run = ledger
            .reserve(Class::Committed, &catalog, 4 * MIB, Some(until))
            .await
            .expect("the kept text does not count against the run");
        assert_eq!(ledger.in_use(), 52 * MIB + STACK);
        drop(run);
        ledger.release_kept(Class::Committed, &catalog, 48 * MIB);
        assert_eq!(ledger.in_use(), 0);
    }

    #[tokio::test]
    async fn queued_texts_past_what_a_run_leaves_of_the_share_all_drain() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        for _ in 0..6 {
            ledger.keep(Class::Committed, &catalog, 8 * MIB, 4 * MIB);
        }
        for _ in 0..6 {
            let until = Instant::now() + Duration::from_millis(10);
            ledger
                .reserve(Class::Committed, &catalog, 4 * MIB, Some(until))
                .await
                .expect("each hook is admitted past the other hooks' texts");
            ledger.release_kept(Class::Committed, &catalog, 8 * MIB);
        }
        assert_eq!(ledger.in_use(), 0);
    }

    #[test]
    fn kept_text_counts_against_the_share_for_everything_but_committed_work() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        ledger.keep(Class::Committed, &catalog, 48 * MIB, 4 * MIB);
        for class in [Class::Interactive, Class::Background, Class::Nested] {
            assert!(matches!(
                ledger.try_reserve(class, &catalog, 4 * MIB),
                Err(Reason::ServiceShare)
            ));
            assert_eq!(
                ledger.grow(class, &catalog, 3 * MIB),
                Verdict::Denied(Reason::ServiceShare)
            );
        }
        assert_eq!(
            ledger.grow(Class::Committed, &catalog, 3 * MIB),
            Verdict::Allowed,
            "a hook's growth is not held to the texts either"
        );
    }

    #[test]
    fn kept_text_counts_toward_the_ceiling_for_committed_work_too() {
        let ledger = ledger(true);
        ledger.keep(Class::Committed, &service("a"), 45 * MIB, 4 * MIB);
        ledger.keep(Class::Committed, &service("b"), 40 * MIB, 4 * MIB);
        assert!(matches!(
            ledger.try_reserve(Class::Committed, &service("a"), 8 * MIB),
            Err(Reason::Memory)
        ));
    }

    #[test]
    fn text_is_kept_within_the_share_and_leaves_one_run_room_among_the_texts() {
        let ledger = ledger(true);
        let (a, b) = (service("a"), service("b"));
        assert_eq!(
            ledger.keep(Class::Committed, &a, 51 * MIB, 0),
            Verdict::Denied(Reason::ServiceShare)
        );
        assert_eq!(ledger.in_use(), 0, "a refusal keeps nothing");
        assert_eq!(
            ledger.keep(Class::Committed, &a, 45 * MIB, 8 * MIB),
            Verdict::Allowed
        );
        assert_eq!(
            ledger.keep(Class::Committed, &b, 40 * MIB, 8 * MIB),
            Verdict::Denied(Reason::Memory),
            "85 MiB of text leaves no 8 MiB run under the 90 MiB ceiling"
        );
        assert_eq!(
            ledger.keep(Class::Committed, &b, 40 * MIB, 4 * MIB),
            Verdict::Allowed
        );

        let busy = super::tests::ledger(true);
        let _running = busy
            .try_reserve(Class::Interactive, &b, 49 * MIB)
            .expect("fits");
        let _other = busy
            .try_reserve(Class::Interactive, &service("c"), 30 * MIB)
            .expect("fits");
        assert_eq!(
            busy.keep(Class::Committed, &a, 4 * MIB, 16 * MIB),
            Verdict::Allowed,
            "what runs now frees itself, so only text counts toward that room"
        );

        let warned = super::tests::ledger(false);
        assert_eq!(
            warned.keep(Class::Committed, &a, 51 * MIB, 0),
            Verdict::Warned(Reason::ServiceShare)
        );
        assert_eq!(warned.in_use(), 51 * MIB);
    }

    #[test]
    fn a_delivered_buffer_is_refused_only_for_its_own_size() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        let _full = ledger
            .try_reserve(Class::Interactive, &catalog, 49 * MIB)
            .expect("fits");
        assert_eq!(
            ledger.deliver(Class::Interactive, &catalog, 10 * MIB, 10 * MIB),
            Verdict::Allowed,
            "past the share and held anyway"
        );
        assert_eq!(ledger.in_use(), 60 * MIB);
        assert_eq!(
            ledger.deliver(Class::Interactive, &catalog, MIB, 51 * MIB),
            Verdict::Denied(Reason::DeliveryLimit),
            "alone past the share"
        );
        assert_eq!(ledger.in_use(), 60 * MIB);
        assert_eq!(
            super::tests::ledger(false).deliver(Class::Interactive, &catalog, MIB, 51 * MIB),
            Verdict::Warned(Reason::DeliveryLimit)
        );
    }

    #[tokio::test]
    async fn a_hook_whose_estimate_rose_after_it_was_kept_can_wait_out_its_deadline() {
        let ledger = ledger(true);
        let (a, b) = (service("a"), service("b"));
        ledger.keep(Class::Committed, &a, 45 * MIB, 4 * MIB);
        ledger.keep(Class::Committed, &b, 40 * MIB, 4 * MIB);
        let until = Instant::now() + Duration::from_millis(10);
        let refused = ledger
            .reserve(Class::Committed, &a, 8 * MIB, Some(until))
            .await;
        assert!(
            matches!(refused, Err(Reason::Memory)),
            "the room a text was kept with is the estimate when it was kept"
        );
    }

    #[tokio::test]
    async fn a_delivery_counts_what_it_already_holds_towards_its_room() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        ledger.force(Class::Interactive, &catalog, 50 * MIB);
        let delivery = ledger.delivery(200 * MIB, 50 * MIB).await;
        assert!(delivery.is_some());
        assert_eq!(ledger.delivering(), 0, "the whole limit is held already");
        drop(delivery);
        let delivery = ledger.delivery(30 * MIB, 10 * MIB).await;
        assert_eq!(ledger.delivering(), 20 * MIB);
        drop(delivery);
    }

    #[tokio::test]
    async fn a_delivery_takes_the_band_above_a_full_budget_and_waits_while_it_is_full() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        ledger.force(Class::Interactive, &catalog, 100 * MIB);
        let first = ledger.delivery(35 * MIB, 0).await;
        assert_eq!(ledger.in_use(), 135 * MIB);
        let mut second = Box::pin(ledger.delivery(35 * MIB, 0));
        assert!(
            second.as_mut().now_or_never().is_none(),
            "135 + 35 passes the 150 MiB ceiling"
        );
        assert_eq!(ledger.delivery_waiters(), 1);
        drop(second);
        assert_eq!(
            ledger.delivery_waiters(),
            0,
            "a dropped wait leaves the queue"
        );
        drop(first);
        let third = ledger.delivery(35 * MIB, 0).await;
        assert_eq!(
            ledger.delivering(),
            35 * MIB,
            "the first gave its room back"
        );
        drop(third);
        assert_eq!(ledger.in_use(), 100 * MIB);
    }

    #[tokio::test]
    async fn a_large_delivery_is_not_overtaken_by_small_ones() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        ledger.force(Class::Interactive, &catalog, 100 * MIB);
        let first = ledger.delivery(40 * MIB, 0).await;
        let mut large = Box::pin(ledger.delivery(45 * MIB, 0));
        assert!(large.as_mut().now_or_never().is_none());
        let mut small = Box::pin(ledger.delivery(MIB, 0));
        assert!(
            small.as_mut().now_or_never().is_none(),
            "it would fit, and waits its turn"
        );
        drop(first);
        assert!(large.await.is_some());
        assert!(small.await.is_some());
    }

    #[tokio::test]
    async fn a_delivery_granted_to_a_caller_that_has_gone_is_given_back() {
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        ledger.force(Class::Interactive, &catalog, 130 * MIB);
        let mut delivery = Box::pin(ledger.delivery(25 * MIB, 0));
        assert!(delivery.as_mut().now_or_never().is_none());
        ledger.release(Class::Interactive, &catalog, 10 * MIB);
        assert_eq!(ledger.delivering(), 25 * MIB, "granted, not yet received");
        drop(delivery);
        assert_eq!(ledger.delivering(), 0);
        assert_eq!(ledger.in_use(), 120 * MIB);
    }

    #[tokio::test]
    async fn only_enforce_takes_a_room_and_a_room_never_passes_the_limit() {
        let warned = ledger(false);
        assert!(warned.delivery(200 * MIB, 0).await.is_none());
        assert_eq!(warned.in_use(), 0);

        let ledger = ledger(true);
        ledger.force(Class::Interactive, &service("catalog-service"), 100 * MIB);
        let delivery = ledger.delivery(200 * MIB, 0).now_or_never().flatten();
        assert!(delivery.is_some(), "a room never waits on a full budget");
        assert_eq!(ledger.delivering(), 50 * MIB, "a tree past it is refused");
    }

    #[test]
    fn a_tree_is_refused_alone_past_the_smaller_of_the_share_and_the_overrun() {
        let limits = |service, overrun| Limits {
            budget: 100 * MIB,
            admit: 90 * MIB,
            service,
            background: 75 * MIB,
            overrun,
        };
        assert_eq!(limits(50 * MIB, 20 * MIB).delivery(), 20 * MIB);
        assert_eq!(limits(50 * MIB, 200 * MIB).delivery(), 50 * MIB);
        let ledger = ledger(true);
        let catalog = service("catalog-service");
        assert_eq!(
            ledger.deliver(Class::Interactive, &catalog, MIB, 50 * MIB),
            Verdict::Allowed
        );
        assert_eq!(
            ledger.deliver(Class::Interactive, &catalog, MIB, 50 * MIB + 1),
            Verdict::Denied(Reason::DeliveryLimit)
        );
    }

    #[test]
    fn only_enforce_is_past_the_ceiling_and_only_above_it() {
        let catalog = service("catalog-service");
        let ledger = ledger(true);
        ledger.force(Class::Interactive, &catalog, 150 * MIB);
        assert!(!ledger.past_the_ceiling());
        ledger.force(Class::Interactive, &catalog, 1);
        assert!(ledger.past_the_ceiling());

        let warned = super::tests::ledger(false);
        warned.force(Class::Interactive, &catalog, 151 * MIB);
        assert!(!warned.past_the_ceiling());
    }

    #[test]
    fn warn_charges_what_enforce_would_deny() {
        let ledger = ledger(false);
        let mut charge = ledger
            .force_reserve(Class::Interactive, &service("a"), 95 * MIB)
            .expect("a charge");
        assert_eq!(
            charge.grow_linear(0, 99 * MIB),
            Verdict::Warned(Reason::ServiceShare)
        );
        assert_eq!(ledger.in_use(), 99 * MIB);
        drop(charge);
        assert_eq!(ledger.in_use(), 0);
    }

    #[tokio::test]
    async fn committed_waiters_are_granted_first_and_one_that_does_not_fit_blocks_nobody() {
        let ledger = ledger(true);
        let full = ledger
            .try_reserve(Class::Interactive, &service("a"), 45 * MIB)
            .expect("fits");
        let other = ledger
            .try_reserve(Class::Interactive, &service("b"), 42 * MIB)
            .expect("fits");
        let until = Some(Instant::now() + Duration::from_secs(5));
        let big = tokio::spawn({
            let ledger = Arc::clone(&ledger);
            async move {
                ledger
                    .reserve(Class::Interactive, &service("c"), 48 * MIB, until)
                    .await
                    .map(drop)
            }
        });
        let small = tokio::spawn({
            let ledger = Arc::clone(&ledger);
            async move {
                ledger
                    .reserve(Class::Interactive, &service("d"), 2 * MIB, until)
                    .await
            }
        });
        let committed = tokio::spawn({
            let ledger = Arc::clone(&ledger);
            async move {
                ledger
                    .reserve(Class::Committed, &service("e"), 30 * MIB, None)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(other);
        let committed = committed.await.expect("join").expect("granted first");
        let small = small
            .await
            .expect("join")
            .expect("not blocked by the big one");
        assert!(!big.is_finished(), "48 MiB does not fit yet");
        drop((full, committed, small));
        big.await.expect("join").expect("granted once room frees");
        assert_eq!(ledger.in_use(), 0);
    }

    #[tokio::test]
    async fn a_waiter_past_its_deadline_is_refused_and_leaves_nothing() {
        let ledger = ledger(true);
        let full = ledger
            .try_reserve(Class::Interactive, &service("a"), 48 * MIB)
            .expect("fits");
        let refused = ledger
            .reserve(
                Class::Interactive,
                &service("a"),
                MIB,
                Some(Instant::now() + Duration::from_millis(30)),
            )
            .await;
        assert!(matches!(refused, Err(Reason::ServiceShare)));
        drop(full);
        assert_eq!(ledger.in_use(), 0);
        assert!(ledger.lock().queue.is_empty());
    }

    #[tokio::test]
    async fn aged_background_is_granted_before_interactive_and_fresh_background_after_it() {
        let ledger = ledger(true);
        let held = ledger
            .try_reserve(Class::Interactive, &service("a"), 45 * MIB)
            .expect("fits");
        let filler = ledger
            .try_reserve(Class::Interactive, &service("b"), 42 * MIB)
            .expect("fits");
        let reserve = |class, name: &'static str, bytes| {
            let ledger = Arc::clone(&ledger);
            tokio::spawn(async move {
                ledger
                    .reserve(class, &service(name), bytes, None)
                    .await
                    .expect("granted")
            })
        };
        let aged = reserve(Class::Background, "c", 40 * MIB);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let interactive = reserve(Class::Interactive, "d", 40 * MIB);
        let fresh = reserve(Class::Background, "e", 40 * MIB);
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(filler);
        let aged = aged.await.expect("join");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!interactive.is_finished() && !fresh.is_finished());
        drop(held);
        let interactive = interactive.await.expect("join");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!fresh.is_finished(), "fresh background comes last");
        drop((aged, interactive));
        drop(fresh.await.expect("join"));
        assert_eq!(ledger.in_use(), 0);
    }

    fn waiting(
        ledger: &Arc<Ledger>,
        class: Class,
        name: &'static str,
        bytes: usize,
    ) -> tokio::task::JoinHandle<Result<Charge, Reason>> {
        let ledger = Arc::clone(ledger);
        tokio::spawn(async move { ledger.reserve(class, &service(name), bytes, None).await })
    }

    async fn granted(waiter: tokio::task::JoinHandle<Result<Charge, Reason>>) -> Charge {
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("granted in time")
            .expect("join")
            .expect("granted")
    }

    #[tokio::test]
    async fn a_committed_waiter_past_its_service_share_holds_back_only_that_service() {
        for committed in [12 * MIB, 49 * MIB] {
            let ledger = ledger(true);
            let held = ledger
                .try_reserve(Class::Interactive, &service("a"), 40 * MIB)
                .expect("fits");
            let first = waiting(&ledger, Class::Committed, "a", committed);
            tokio::time::sleep(Duration::from_millis(20)).await;
            let same = waiting(&ledger, Class::Interactive, "a", 2 * MIB);
            let other = granted(waiting(&ledger, Class::Interactive, "b", 4 * MIB)).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(
                !first.is_finished() && !same.is_finished(),
                "the service's own interactive work waits behind its committed work"
            );
            drop(held);
            drop(granted(first).await);
            drop((granted(same).await, other));
            assert_eq!(ledger.in_use(), 0);
        }
    }

    #[tokio::test]
    async fn an_aged_waiter_past_the_background_share_holds_back_only_background() {
        for aged in [10 * MIB, 20 * MIB] {
            let ledger = ledger(true);
            let backlog: Vec<Charge> = ["a", "b"]
                .into_iter()
                .map(|name| {
                    ledger
                        .try_reserve(Class::Background, &service(name), 34 * MIB)
                        .expect("fits")
                })
                .collect();
            let first = waiting(&ledger, Class::Background, "c", aged);
            tokio::time::sleep(Duration::from_millis(150)).await;
            let fresh = waiting(&ledger, Class::Background, "d", 2 * MIB);
            let interactive = granted(waiting(&ledger, Class::Interactive, "e", 4 * MIB)).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(
                !first.is_finished() && !fresh.is_finished(),
                "fresh background waits behind aged background"
            );
            drop(backlog);
            let first = granted(first).await;
            let fresh = granted(fresh).await;
            drop((first, fresh, interactive));
            assert_eq!(ledger.in_use(), 0);
        }
    }

    #[tokio::test]
    async fn a_large_aged_waiter_is_granted_against_a_stream_of_small_arrivals() {
        let ledger = ledger(true);
        let mut small: VecDeque<Charge> = (0..20)
            .map(|i| {
                ledger
                    .try_reserve(Class::Interactive, &service(&format!("s{i}")), 2 * MIB)
                    .expect("fits")
            })
            .collect();
        let filler = ledger
            .try_reserve(Class::Interactive, &service("filler"), 25 * MIB)
            .expect("fits");
        let aged = tokio::spawn({
            let ledger = Arc::clone(&ledger);
            async move {
                ledger
                    .reserve(Class::Background, &service("big"), 40 * MIB, None)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut arrivals = Vec::new();
        for i in 0..40 {
            if aged.is_finished() {
                break;
            }
            drop(small.pop_front());
            let ledger = Arc::clone(&ledger);
            let until = Some(Instant::now() + Duration::from_millis(200));
            arrivals.push(tokio::spawn(async move {
                ledger
                    .reserve(
                        Class::Interactive,
                        &service(&format!("n{i}")),
                        2 * MIB,
                        until,
                    )
                    .await
                    .ok()
            }));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let aged = aged
            .await
            .expect("join")
            .expect("the aged waiter is granted");
        drop((aged, filler, small));
        for arrival in arrivals {
            drop(arrival.await.expect("join"));
        }
        assert_eq!(ledger.in_use(), 0);
    }

    #[tokio::test]
    async fn background_stops_at_its_share_while_interactive_goes_on() {
        let ledger = ledger(true);
        let backlog: Vec<Charge> = ["a", "b"]
            .into_iter()
            .map(|name| {
                ledger
                    .try_reserve(Class::Background, &service(name), 36 * MIB)
                    .expect("fits")
            })
            .collect();
        assert!(matches!(
            ledger.try_reserve(Class::Background, &service("c"), 4 * MIB),
            Err(Reason::Memory)
        ));
        assert!(ledger
            .try_reserve(Class::Interactive, &service("c"), 4 * MIB)
            .is_ok());
        drop(backlog);
    }

    #[tokio::test]
    async fn pressure_gates_new_interactive_and_background_work_only() {
        let ledger = ledger(true);
        ledger.set_pressure(true);
        for class in [Class::Interactive, Class::Background] {
            assert!(matches!(
                ledger.try_reserve(class, &service("a"), MIB),
                Err(Reason::Pressure)
            ));
        }
        assert!(ledger
            .try_reserve(Class::Nested, &service("a"), MIB)
            .is_ok());
        assert!(ledger
            .try_reserve(Class::Committed, &service("a"), MIB)
            .is_ok());
        let waiting = tokio::spawn({
            let ledger = Arc::clone(&ledger);
            async move {
                ledger
                    .reserve(Class::Interactive, &service("b"), MIB, None)
                    .await
                    .map(drop)
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        ledger.set_pressure(false);
        waiting
            .await
            .expect("join")
            .expect("granted when pressure ends");
    }

    #[tokio::test]
    async fn the_headroom_probe_reserves_nothing_and_counts_what_fits() {
        let ledger = ledger(true);
        let idle = service("catalog-service");
        assert_eq!(ledger.room(Class::Background, &idle, 9 * MIB).await, 5);
        assert_eq!(ledger.in_use(), 0, "an idle durable holds nothing");
        let full = ledger
            .try_reserve(Class::Background, &service("a"), 48 * MIB)
            .expect("fits");
        let more = ledger
            .try_reserve(Class::Background, &service("b"), 24 * MIB)
            .expect("fits");
        let probe = tokio::spawn({
            let ledger = Arc::clone(&ledger);
            let idle = idle.clone();
            async move { ledger.room(Class::Background, &idle, 9 * MIB).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!probe.is_finished(), "no room, so the pump waits");
        drop(more);
        assert_eq!(probe.await.expect("join"), 2);
        drop(full);
    }

    #[test]
    fn a_poisoned_ledger_is_recovered() {
        let ledger = ledger(true);
        let poisoner = Arc::clone(&ledger);
        let _ = std::thread::spawn(move || {
            let _held = poisoner.lock();
            panic!("poison the ledger");
        })
        .join();
        assert!(ledger
            .try_reserve(Class::Interactive, &service("a"), MIB)
            .is_ok());
    }

    #[test]
    fn a_thread_keeps_its_stack_charge_outside_every_service_until_it_retires() {
        let ledger = ledger(true);
        let mut charge = ledger
            .try_reserve(Class::Interactive, &service("a"), MIB)
            .expect("fits");
        assert_eq!(charge.take_stack(), Some(Stack::Reserved));
        assert_eq!(
            ledger.thread_started(Some(Stack::Reserved), false),
            ThreadStart::MoveReserved
        );
        ledger.move_to_residue(Class::Interactive, &service("a"));
        charge.stack_moved();
        assert_eq!(ledger.residue(), STACK);
        assert_eq!(ledger.in_use(), MIB + STACK);
        ledger.thread_idle();
        drop(charge);
        assert_eq!(
            ledger.in_use(),
            STACK,
            "the idle thread keeps its stack charged"
        );

        let mut claimed = ledger
            .try_reserve(Class::Interactive, &service("b"), MIB)
            .expect("fits");
        assert_eq!(
            ledger.in_use(),
            MIB + STACK,
            "an admission takes over the idle thread's stack"
        );
        assert_eq!(claimed.take_stack(), Some(Stack::Claimed));
        assert_eq!(
            ledger.thread_started(Some(Stack::Claimed), true),
            ThreadStart::Charged
        );
        ledger.thread_idle();
        drop(claimed);
        ledger.thread_retired();
        assert_eq!(ledger.in_use(), 0);
        assert_eq!(ledger.residue(), 0);
    }

    #[test]
    fn a_scrape_publishes_each_service_and_zeroes_one_that_let_go() {
        let recorder = cufflink_telemetry::metrics_builder()
            .expect("metrics builder")
            .build_recorder();
        let handle = recorder.handle();
        let ledger = ledger(true);
        let (catalog, loans) = (service("catalog-service"), service("loan-service"));
        let line = |rendered: &str, name: &str| {
            rendered
                .lines()
                .find(|line| {
                    line.starts_with("cufflink_wasm_memory_service_bytes")
                        && line.contains(&format!("service=\"{name}\""))
                })
                .map(str::to_owned)
        };
        metrics::with_local_recorder(&recorder, || {
            ledger.force(Class::Interactive, &catalog, 3 * MIB);
            ledger.force(Class::Background, &loans, MIB);
            ledger.publish();
            let rendered = handle.render();
            assert!(line(&rendered, "catalog-service").is_some_and(|l| l.ends_with(" 3145728")));
            assert!(line(&rendered, "loan-service").is_some_and(|l| l.ends_with(" 1048576")));

            ledger.release(Class::Background, &loans, MIB);
            ledger.publish();
            let rendered = handle.render();
            assert!(
                line(&rendered, "loan-service").is_some_and(|l| l.ends_with(" 0")),
                "{rendered}"
            );
        });
    }

    #[test]
    fn a_charge_held_by_a_thread_that_panics_is_credited_as_it_unwinds() {
        let ledger = ledger(true);
        let holder = std::thread::spawn({
            let ledger = Arc::clone(&ledger);
            move || {
                let _charge = ledger
                    .try_reserve(Class::Interactive, &service("catalog-service"), 8 * MIB)
                    .expect("fits");
                panic!("a host call panicked");
            }
        });
        assert!(holder.join().is_err());
        assert_eq!(ledger.in_use_by(Class::Interactive), 0);
    }
}
