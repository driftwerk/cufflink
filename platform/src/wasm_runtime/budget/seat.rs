use super::{Class, Reason, Refusal, ServiceKey};
use crate::policy::PolicyMode;
use crate::shutdown::Shutdown;
use crate::wasm_runtime::{Interrupt, InvocationKind};
use cufflink_telemetry::GaugeGuard;
use cufflink_types::host::wait::Wait;
use futures_util::{StreamExt, TryStreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{AcquireError, Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub(crate) struct Gate {
    slots: Arc<Semaphore>,
    resuming: AtomicUsize,
    changed: Notify,
    arrivals: Mutex<()>,
}

struct Resuming<'a>(&'a Gate);

impl<'a> Resuming<'a> {
    fn on(gate: &'a Gate) -> Self {
        gate.resuming.fetch_add(1, Ordering::AcqRel);
        gate.changed.notify_waiters();
        Self(gate)
    }
}

impl Drop for Resuming<'_> {
    fn drop(&mut self) {
        if self.0.resuming.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.changed.notify_waiters();
        }
    }
}

impl Gate {
    pub(crate) fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(permits)),
            resuming: AtomicUsize::new(0),
            changed: Notify::new(),
            arrivals: Mutex::new(()),
        })
    }

    pub(crate) fn available(&self) -> usize {
        self.slots.available_permits()
    }

    pub(crate) fn try_take(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.slots).try_acquire_owned().ok()
    }

    pub(crate) async fn admit(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        let _turn = self.arrivals.lock().await;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.resuming.load(Ordering::Acquire) > 0 {
                changed.await;
                continue;
            }
            tokio::select! {
                biased;
                () = &mut changed => {}
                permit = Arc::clone(&self.slots).acquire_owned() => return permit,
            }
        }
    }

    async fn resume(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        let _resuming = Resuming::on(self);
        Arc::clone(&self.slots).acquire_owned().await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Taking {
    Waited,
    Tried,
}

impl Taking {
    pub(crate) fn of(enforced: bool, class: Class) -> Self {
        match enforced && class != Class::Nested {
            true => Taking::Waited,
            false => Taking::Tried,
        }
    }
}

pub(crate) struct Taken {
    gate: Arc<Gate>,
    taking: Taking,
    _permit: OwnedSemaphorePermit,
}

impl Taken {
    pub(crate) fn new(gate: Arc<Gate>, taking: Taking, permit: OwnedSemaphorePermit) -> Self {
        Self {
            gate,
            taking,
            _permit: permit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Held {
    NotRequested,
    NotParkable,
    PoolFull,
    ServicePoolFull,
    Transaction,
    Disabled,
}

impl Held {
    fn result(self) -> Option<&'static str> {
        match self {
            Held::NotRequested => None,
            Held::NotParkable => Some("held_not_parkable"),
            Held::PoolFull => Some("held_pool_full"),
            Held::ServicePoolFull => Some("held_service_pool_full"),
            Held::Transaction => Some("held_transaction"),
            Held::Disabled => Some("held_disabled"),
        }
    }

    fn count(self, kind: InvocationKind) -> Self {
        if let Some(result) = self.result() {
            released(kind, result);
        }
        self
    }
}

fn released(kind: InvocationKind, result: &'static str) {
    metrics::counter!(
        "cufflink_wasm_wait_release_total",
        "kind" => kind.label(),
        "result" => result,
    )
    .increment(1);
}

pub(crate) struct Whose {
    service: ServiceKey,
    handler: Arc<str>,
    class: Class,
}

impl Whose {
    pub(crate) fn new(service: ServiceKey, handler: &str, class: Class) -> Arc<Self> {
        Arc::new(Self {
            service,
            handler: handler.into(),
            class,
        })
    }

    fn missed(&self) {
        Refusal {
            tenant: &self.service.tenant,
            service: &self.service.service,
            handler: &self.handler,
            class: self.class,
            reason: Reason::ServiceConcurrency,
            mode: PolicyMode::Warn,
        }
        .record(None);
    }
}

#[derive(Clone)]
pub(crate) struct Parking {
    global: Arc<Semaphore>,
    service: Arc<Semaphore>,
    warned: Option<Arc<Whose>>,
}

impl Parking {
    pub(crate) fn new(
        global: Arc<Semaphore>,
        service: Arc<Semaphore>,
        warned: Option<Arc<Whose>>,
    ) -> Self {
        Self {
            global,
            service,
            warned,
        }
    }
}

pub(crate) struct Seat {
    route: Option<OwnedSemaphorePermit>,
    releasable: Vec<Taken>,
    parking: Result<Parking, Held>,
    kind: InvocationKind,
    after: Duration,
}

impl Seat {
    pub(crate) fn new(
        route: Option<OwnedSemaphorePermit>,
        releasable: Vec<Taken>,
        parking: Result<Parking, Held>,
        kind: InvocationKind,
        after: Duration,
    ) -> Self {
        Self {
            route,
            releasable,
            parking,
            kind,
            after,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.releasable.len() + usize::from(self.route.is_some())
    }

    pub(crate) fn lend(&mut self, wait: Wait, transaction_open: bool) -> Lent {
        let parking = match (wait, transaction_open) {
            (Wait::Hold, _) => Err(Held::NotRequested),
            (Wait::Release, true) => Err(Held::Transaction),
            (Wait::Release, false) => self.parking.clone(),
        };
        Lent {
            releasable: std::mem::take(&mut self.releasable),
            parking,
            kind: self.kind,
            after: self.after,
        }
    }

    pub(crate) fn restore(&mut self, retaken: Vec<Taken>) {
        self.releasable = retaken;
    }
}

pub(crate) struct Lent {
    releasable: Vec<Taken>,
    parking: Result<Parking, Held>,
    kind: InvocationKind,
    after: Duration,
}

impl Lent {
    pub(crate) fn requested(&self) -> bool {
        self.parking.as_ref().err() != Some(&Held::NotRequested)
    }

    pub(crate) fn after(&self) -> Duration {
        self.after
    }

    pub(crate) fn answered_first(self) -> Vec<Taken> {
        if self.parking.is_ok() {
            released(self.kind, "answered_first");
        }
        self.releasable
    }

    pub(crate) fn into_releasable(self) -> Vec<Taken> {
        self.releasable
    }

    pub(crate) fn park(&mut self) -> Result<Parked, Held> {
        let (global, service, warned) = self.reserve().map_err(|held| held.count(self.kind))?;
        released(self.kind, "parked");
        tracing::debug!(
            target: "cufflink::wasm::budget",
            kind = self.kind.label(),
            slots = self.releasable.len(),
            "Guest parked while its call waits"
        );
        Ok(Parked {
            gates: std::mem::take(&mut self.releasable)
                .into_iter()
                .map(|taken| (taken.gate, taken.taking))
                .collect(),
            warned,
            kind: self.kind,
            since: Instant::now(),
            _parked: GaugeGuard::enter(
                metrics::gauge!("cufflink_wasm_parked", "kind" => self.kind.label()),
            ),
            _global: global,
            _service: service,
        })
    }

    fn reserve(
        &self,
    ) -> Result<
        (
            OwnedSemaphorePermit,
            OwnedSemaphorePermit,
            Option<Arc<Whose>>,
        ),
        Held,
    > {
        let parking = self.parking.as_ref().map_err(|held| *held)?;
        let global = Arc::clone(&parking.global)
            .try_acquire_owned()
            .map_err(|_| Held::PoolFull)?;
        let service = Arc::clone(&parking.service)
            .try_acquire_owned()
            .map_err(|_| Held::ServicePoolFull)?;
        Ok((global, service, parking.warned.clone()))
    }
}

pub(crate) struct ResumeBy<'a> {
    pub until: Instant,
    pub cancel: &'a CancellationToken,
    pub shutdown: &'a Shutdown,
}

pub(crate) struct Parked {
    gates: Vec<(Arc<Gate>, Taking)>,
    warned: Option<Arc<Whose>>,
    kind: InvocationKind,
    since: Instant,
    _parked: GaugeGuard,
    _global: OwnedSemaphorePermit,
    _service: OwnedSemaphorePermit,
}

async fn retake(
    gate: Arc<Gate>,
    taking: Taking,
    warned: Option<&Whose>,
) -> Result<Option<Taken>, AcquireError> {
    let permit = match taking {
        Taking::Waited => Some(gate.resume().await?),
        Taking::Tried => {
            let permit = gate.try_take();
            if let (None, Some(warned)) = (&permit, warned) {
                warned.missed();
            }
            permit
        }
    };
    Ok(permit.map(|permit| Taken::new(gate, taking, permit)))
}

impl Parked {
    pub(crate) async fn resume(self, by: ResumeBy<'_>) -> Result<Vec<Taken>, Interrupt> {
        let answered = Instant::now();
        let kind = self.kind.label();
        metrics::histogram!("cufflink_wasm_parked_seconds", "kind" => kind)
            .record(answered.duration_since(self.since).as_secs_f64());
        let warned = self.warned.as_deref();
        let retaking = futures_util::stream::iter(self.gates)
            .then(|(gate, taking)| retake(gate, taking, warned))
            .try_collect::<Vec<_>>();
        let resumed = tokio::select! {
            biased;
            () = by.cancel.cancelled() => Err(Interrupt::Cancelled),
            () = by.shutdown.hard_deadline() => Err(Interrupt::Shutdown),
            () = tokio::time::sleep_until(by.until.into()) => Err(Interrupt::Deadline),
            retaken = retaking => retaken
                .map(|retaken| retaken.into_iter().flatten().collect())
                .map_err(|_closed| Interrupt::Shutdown),
        };
        let result = match &resumed {
            Ok(_) => "resumed",
            Err(Interrupt::Deadline) => "deadline",
            Err(Interrupt::Cancelled) => "cancelled",
            Err(Interrupt::Shutdown) => "shutdown",
        };
        metrics::histogram!(
            "cufflink_wasm_resume_wait_seconds",
            "kind" => kind,
            "result" => result,
        )
        .record(answered.elapsed().as_secs_f64());
        if resumed.as_ref().err() == Some(&Interrupt::Deadline) {
            tracing::warn!(
                target: "cufflink::wasm::budget",
                kind,
                parked_ms = answered.duration_since(self.since).as_millis() as u64,
                "A guest could not take its slots back before its deadline"
            );
        }
        resumed
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    pub(crate) fn lone(gate: &Arc<Gate>, pools: Option<(usize, usize)>, after: Duration) -> Seat {
        let parking = pools
            .map(|(global, service)| {
                Parking::new(
                    Arc::new(Semaphore::new(global)),
                    Arc::new(Semaphore::new(service)),
                    None,
                )
            })
            .ok_or(Held::Disabled);
        let taken = Taken::new(
            Arc::clone(gate),
            Taking::Waited,
            gate.try_take().expect("a free slot"),
        );
        Seat::new(None, vec![taken], parking, InvocationKind::Route, after)
    }

    pub(crate) fn pools(seat: &Seat) -> (usize, usize) {
        seat.parking.as_ref().map_or((0, 0), |parking| {
            (
                parking.global.available_permits(),
                parking.service.available_permits(),
            )
        })
    }

    pub(crate) fn quiet(gate: &Gate) -> bool {
        gate.resuming.load(Ordering::Acquire) == 0
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        Admission, Budget, BudgetConfig, Class, RouteKey, ServiceSlot, Ticket, WaitRelease,
    };
    use super::testing::quiet;
    use super::*;
    use crate::policy::PolicyMode;
    use crate::registry::ServiceIdentity;
    use crate::wasm_runtime::ExecutionError;
    use futures_util::poll;
    use std::pin::pin;
    use std::task::Poll;
    use uuid::Uuid;

    const WAIT: Duration = Duration::from_millis(100);

    fn identity(service: &str) -> ServiceIdentity {
        ServiceIdentity {
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            service_id: Uuid::new_v4(),
            service_name: service.into(),
            schema: "acme".into(),
        }
    }

    fn budget_in(mode: PolicyMode, max_parked: usize, max_parked_per_service: usize) -> Budget {
        Budget::new(
            BudgetConfig {
                mode,
                per_service: 2,
                wait_release: Some(WaitRelease {
                    after: Duration::from_millis(50),
                    max_parked,
                    max_parked_per_service,
                }),
                ..BudgetConfig::default()
            },
            4,
            usize::MAX,
            WAIT,
            Duration::from_secs(1),
        )
    }

    fn budget(max_parked: usize, max_parked_per_service: usize) -> Budget {
        budget_in(PolicyMode::Enforce, max_parked, max_parked_per_service)
    }

    fn ticket<'a>(identity: &'a ServiceIdentity, class: Class, kind: InvocationKind) -> Ticket<'a> {
        Ticket {
            class,
            kind,
            identity,
            handler: "assess",
            route: None,
            initial_memory: 0,
            budget: Duration::from_secs(5),
        }
    }

    fn route(identity: &ServiceIdentity) -> Ticket<'_> {
        ticket(identity, Class::Interactive, InvocationKind::Route)
    }

    async fn seat(budget: &Budget, global: &Arc<Gate>, ticket: Ticket<'_>) -> Seat {
        let admission: Admission = budget.admit(ticket, global).await.expect("admitted");
        admission.into_parts().0
    }

    fn slots(budget: &Budget, identity: &ServiceIdentity) -> ServiceSlot {
        budget.services.of(&ServiceKey::of(identity))
    }

    fn released(seat: &mut Seat) -> Lent {
        seat.lend(Wait::Release, false)
    }

    fn by<'a>(
        until: Duration,
        cancel: &'a CancellationToken,
        shutdown: &'a Shutdown,
    ) -> ResumeBy<'a> {
        ResumeBy {
            until: Instant::now() + until,
            cancel,
            shutdown,
        }
    }

    fn running() -> Shutdown {
        Shutdown::new(Duration::from_secs(30))
    }

    fn pool_permit() -> OwnedSemaphorePermit {
        Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("a wait pool permit")
    }

    fn parked(gates: &[&Arc<Gate>], kind: InvocationKind) -> Parked {
        Parked {
            gates: gates
                .iter()
                .map(|gate| (Arc::clone(gate), Taking::Waited))
                .collect(),
            warned: None,
            kind,
            since: Instant::now(),
            _parked: GaugeGuard::enter(
                metrics::gauge!("cufflink_wasm_parked", "kind" => kind.label()),
            ),
            _global: pool_permit(),
            _service: pool_permit(),
        }
    }

    #[tokio::test]
    async fn parking_releases_the_service_and_global_slots_and_keeps_the_route_slot() {
        let budget = budget(4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let key = RouteKey {
            service_id: loans.service_id,
            version: 1,
            index: 0,
        };
        let mut seat = seat(
            &budget,
            &global,
            Ticket {
                route: Some((key, 1)),
                ..route(&loans)
            },
        )
        .await;
        let service = slots(&budget, &loans);
        let route_gate = budget.routes.gate(key, 1);
        let held = || {
            (
                service.all.available(),
                global.available(),
                route_gate.available(),
            )
        };
        assert_eq!(held(), (1, 3, 0));

        let mut lent = released(&mut seat);
        let parked = lent.park().expect("parked");
        assert_eq!(
            held(),
            (2, 4, 0),
            "the service and global slots go back, the route slot stays held"
        );

        let (cancel, shutdown) = (CancellationToken::new(), running());
        let retaken = parked
            .resume(by(Duration::from_secs(1), &cancel, &shutdown))
            .await
            .expect("resumed");
        seat.restore(retaken);
        assert_eq!(held(), (1, 3, 0));
        drop(seat);
        assert_eq!(held(), (2, 4, 1));
    }

    #[tokio::test]
    async fn a_seat_parks_only_with_a_permit_from_both_wait_pools() {
        let global = Gate::new(1);
        let lent = |pool: usize, service: usize| Lent {
            releasable: vec![Taken::new(
                Arc::clone(&global),
                Taking::Waited,
                global.try_take().expect("slot"),
            )],
            parking: Ok(Parking::new(
                Arc::new(Semaphore::new(pool)),
                Arc::new(Semaphore::new(service)),
                None,
            )),
            kind: InvocationKind::Route,
            after: WAIT,
        };

        let mut no_pool = lent(0, 1);
        assert_eq!(no_pool.park().err(), Some(Held::PoolFull));
        assert_eq!(global.available(), 0, "a held call keeps its slots");
        drop(no_pool);

        let mut no_service = lent(1, 0);
        let pool = Arc::clone(&no_service.parking.as_ref().expect("parking").global);
        assert_eq!(no_service.park().err(), Some(Held::ServicePoolFull));
        assert_eq!(global.available(), 0);
        assert_eq!(
            pool.available_permits(),
            1,
            "the global wait permit goes back"
        );
        drop(no_service);

        let mut both = lent(1, 1);
        let parked = both.park().expect("parked");
        assert_eq!(global.available(), 1);
        assert!(both.into_releasable().is_empty());
        drop(parked);
    }

    #[tokio::test]
    async fn a_call_answered_first_hands_its_slots_back_untouched() {
        let budget = budget(4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let mut seat = seat(&budget, &global, route(&loans)).await;
        let held = seat.lend(Wait::Hold, false);
        assert!(!held.requested());
        seat.restore(held.answered_first());
        let released = released(&mut seat);
        assert!(released.requested());
        let retaken = released.answered_first();
        assert_eq!(retaken.len(), 2, "the service and global slots");
        assert_eq!(global.available(), 3);
        seat.restore(retaken);
        drop(seat);
        assert_eq!(global.available(), 4);
    }

    #[tokio::test]
    async fn a_full_service_wait_pool_holds_one_service_and_parks_another() {
        let budget = budget(4, 1);
        let global = Gate::new(4);
        let catalog = identity("catalog-service");
        let loans = identity("loan-service");

        let mut first = seat(&budget, &global, route(&catalog)).await;
        let mut second = seat(&budget, &global, route(&catalog)).await;
        let mut other = seat(&budget, &global, route(&loans)).await;

        let parked = released(&mut first).park().expect("the first parks");
        assert_eq!(
            released(&mut second).park().err(),
            Some(Held::ServicePoolFull)
        );
        let elsewhere = released(&mut other).park().expect("another service parks");
        drop((parked, elsewhere));
    }

    #[tokio::test]
    async fn a_full_global_wait_pool_holds_every_service() {
        let budget = budget(1, 4);
        let global = Gate::new(4);
        let catalog = identity("catalog-service");
        let loans = identity("loan-service");
        let mut first = seat(&budget, &global, route(&catalog)).await;
        let mut other = seat(&budget, &global, route(&loans)).await;

        let parked = released(&mut first).park().expect("the first parks");
        assert_eq!(released(&mut other).park().err(), Some(Held::PoolFull));
        drop(parked);
        assert!(
            released(&mut other).park().is_ok(),
            "a resumed guest's wait permit goes back to the pool"
        );
    }

    #[tokio::test]
    async fn nested_committed_and_reducer_seats_never_park() {
        let budget = budget(4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        for (class, kind) in [
            (Class::Nested, InvocationKind::Route),
            (Class::Committed, InvocationKind::Hook),
            (Class::Committed, InvocationKind::OnMigrate),
            (Class::Interactive, InvocationKind::SseReducer),
        ] {
            let mut seat = seat(&budget, &global, ticket(&loans, class, kind)).await;
            assert_eq!(
                released(&mut seat).park().err(),
                Some(Held::NotParkable),
                "{class:?} {kind:?}"
            );
        }

        let mut route = seat(&budget, &global, route(&loans)).await;
        assert_eq!(
            route.lend(Wait::Hold, false).park().err(),
            Some(Held::NotRequested)
        );
        assert_eq!(
            route.lend(Wait::Release, true).park().err(),
            Some(Held::Transaction),
            "a guest-owned transaction pins a connection, so its slots stay held"
        );

        let off = Budget::new(
            BudgetConfig {
                mode: PolicyMode::Enforce,
                wait_release: None,
                ..BudgetConfig::default()
            },
            4,
            usize::MAX,
            WAIT,
            Duration::from_secs(1),
        );
        let mut disabled = seat(
            &off,
            &global,
            ticket(&loans, Class::Background, InvocationKind::Job),
        )
        .await;
        assert_eq!(released(&mut disabled).park().err(), Some(Held::Disabled));
    }

    #[tokio::test]
    async fn resume_retakes_slots_in_admission_order() {
        let budget = budget(4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let mut seat = seat(
            &budget,
            &global,
            ticket(&loans, Class::Background, InvocationKind::Job),
        )
        .await;
        let service = slots(&budget, &loans);
        let parked = released(&mut seat).park().expect("parked");
        let class_cap = (0..service.background.available())
            .filter_map(|_| service.background.try_take())
            .collect::<Vec<_>>();

        let (cancel, shutdown) = (CancellationToken::new(), running());
        let mut resuming = pin!(parked.resume(by(Duration::from_secs(5), &cancel, &shutdown)));
        assert!(poll!(resuming.as_mut()).is_pending());
        assert_eq!(
            (
                service.all.available(),
                budget.background.available(),
                global.available()
            ),
            (2, 3, 4),
            "nothing after the class cap is taken while the class cap is held"
        );
        assert!(!quiet(&service.background));

        drop(class_cap);
        let Poll::Ready(Ok(retaken)) = poll!(resuming.as_mut()) else {
            panic!("the resume takes every slot once the class cap is free");
        };
        assert_eq!(
            retaken.len(),
            4,
            "class cap, service, background cap and global"
        );
        assert!(quiet(&service.background));
    }

    #[tokio::test]
    async fn a_resume_is_granted_before_arrivals_already_queued() {
        let gate = Gate::new(1);
        let running_guest = gate.try_take().expect("the running guest's slot");
        let mut first = pin!(gate.admit());
        let mut second = pin!(gate.admit());
        assert!(poll!(first.as_mut()).is_pending(), "queued for the slot");
        assert!(poll!(second.as_mut()).is_pending(), "queued behind it");

        let (cancel, shutdown) = (CancellationToken::new(), running());
        let mut resume = pin!(parked(&[&gate], InvocationKind::Route).resume(by(
            Duration::from_secs(5),
            &cancel,
            &shutdown
        )));
        assert!(poll!(resume.as_mut()).is_pending());

        drop(running_guest);
        assert!(
            poll!(first.as_mut()).is_pending(),
            "the arrival at the head of the queue steps back for the resume"
        );
        assert!(poll!(second.as_mut()).is_pending());
        let Poll::Ready(Ok(retaken)) = poll!(resume.as_mut()) else {
            panic!("the freed slot goes to the resume");
        };
        assert!(quiet(&gate));
        assert!(poll!(first.as_mut()).is_pending());

        drop(retaken);
        let Poll::Ready(Ok(admitted)) = poll!(first.as_mut()) else {
            panic!("the first arrival is admitted once the resumed guest finishes");
        };
        assert!(
            poll!(second.as_mut()).is_pending(),
            "arrivals keep their order"
        );
        drop(admitted);
        assert!(matches!(poll!(second.as_mut()), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn arrivals_held_behind_a_resume_keep_their_order_whatever_order_they_wake_in() {
        let gate = Gate::new(1);
        let running_guest = gate.try_take().expect("the running guest's slot");
        let mut first = pin!(gate.admit());
        let mut second = pin!(gate.admit());
        assert!(poll!(first.as_mut()).is_pending());
        assert!(poll!(second.as_mut()).is_pending());

        let (cancel, shutdown) = (CancellationToken::new(), running());
        let mut resume = pin!(parked(&[&gate], InvocationKind::Route).resume(by(
            Duration::from_secs(5),
            &cancel,
            &shutdown
        )));
        assert!(poll!(resume.as_mut()).is_pending());
        drop(running_guest);
        assert!(poll!(first.as_mut()).is_pending(), "the head steps back");
        let Poll::Ready(Ok(retaken)) = poll!(resume.as_mut()) else {
            panic!("the slot the head gave back goes to the resume, not the arrival behind it");
        };

        drop(retaken);
        assert!(
            poll!(second.as_mut()).is_pending(),
            "the later arrival does not overtake the earlier one by waking first"
        );
        assert!(matches!(poll!(first.as_mut()), Poll::Ready(Ok(_))));
    }

    #[tokio::test]
    async fn resumes_take_freed_slots_in_their_own_order() {
        let gate = Gate::new(1);
        let running_guest = gate.try_take().expect("the running guest's slot");
        let (cancel, shutdown) = (CancellationToken::new(), running());
        let mut earlier = pin!(parked(&[&gate], InvocationKind::Route).resume(by(
            Duration::from_secs(5),
            &cancel,
            &shutdown
        )));
        let mut later = pin!(parked(&[&gate], InvocationKind::Job).resume(by(
            Duration::from_secs(5),
            &cancel,
            &shutdown
        )));
        assert!(poll!(earlier.as_mut()).is_pending());
        assert!(poll!(later.as_mut()).is_pending());
        let mut arrival = pin!(gate.admit());
        assert!(poll!(arrival.as_mut()).is_pending());

        drop(running_guest);
        let Poll::Ready(Ok(first)) = poll!(earlier.as_mut()) else {
            panic!("the earlier resume is granted first");
        };
        assert!(poll!(later.as_mut()).is_pending());
        drop(first);
        assert!(poll!(arrival.as_mut()).is_pending(), "a resume still waits");
        let Poll::Ready(Ok(second)) = poll!(later.as_mut()) else {
            panic!("then the later one");
        };
        drop(second);
        assert!(matches!(poll!(arrival.as_mut()), Poll::Ready(Ok(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn new_work_held_behind_resumes_is_refused_at_its_admission_deadline() {
        let budget = budget(4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let mut parking = seat(&budget, &global, route(&loans)).await;
        let parked = released(&mut parking).park().expect("parked");
        let running_guests = (
            seat(&budget, &global, route(&loans)).await,
            seat(&budget, &global, route(&loans)).await,
        );

        let (cancel, shutdown) = (CancellationToken::new(), running());
        let mut resume = pin!(parked.resume(by(Duration::from_secs(5), &cancel, &shutdown)));
        assert!(poll!(resume.as_mut()).is_pending());

        let started = tokio::time::Instant::now();
        let mut arrival = pin!(budget.admit(route(&loans), &global));
        assert!(poll!(arrival.as_mut()).is_pending());
        drop(running_guests.0);
        let Poll::Ready(Ok(_resumed)) = poll!(resume.as_mut()) else {
            panic!("the freed slot goes to the resume");
        };

        let refused = arrival.await;
        assert!(
            matches!(refused, Err(ExecutionError::Overloaded { .. })),
            "{refused:?}"
        );
        assert_eq!(started.elapsed(), WAIT, "refused at its admission deadline");
    }

    #[tokio::test]
    async fn a_resume_that_reaches_its_deadline_holds_nothing() {
        let budget = budget(4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let mut parking = seat(&budget, &global, route(&loans)).await;
        let parked = released(&mut parking).park().expect("parked");
        let service = slots(&budget, &loans);
        let running_guests = (
            seat(&budget, &global, route(&loans)).await,
            seat(&budget, &global, route(&loans)).await,
        );

        let (cancel, shutdown) = (CancellationToken::new(), running());
        let ended = parked
            .resume(by(Duration::from_millis(50), &cancel, &shutdown))
            .await
            .map(|retaken| retaken.len());
        assert_eq!(ended, Err(Interrupt::Deadline));
        assert!(quiet(&service.all) && quiet(&global));
        assert_eq!(global.available(), 2, "the prefix it retook is given back");

        drop(running_guests);
        let arrival = tokio::time::timeout(WAIT, budget.admit(route(&loans), &global)).await;
        assert!(
            matches!(arrival, Ok(Ok(_))),
            "a new arrival is admitted at once"
        );
    }

    #[tokio::test]
    async fn a_cancelled_resume_releases_the_slots_it_retook() {
        let first = Gate::new(1);
        let second = Gate::new(1);
        let held = second.try_take().expect("held by a running guest");

        let (cancel, shutdown) = (CancellationToken::new(), running());
        let mut resume = pin!(parked(&[&first, &second], InvocationKind::Job).resume(by(
            Duration::from_secs(5),
            &cancel,
            &shutdown
        )));
        assert!(poll!(resume.as_mut()).is_pending());
        assert_eq!(first.available(), 0, "the first slot is retaken");
        cancel.cancel();
        assert_eq!(
            poll!(resume.as_mut()).map(|ended| ended.map(|retaken| retaken.len())),
            Poll::Ready(Err(Interrupt::Cancelled))
        );
        assert_eq!(first.available(), 1);
        assert!(quiet(&first) && quiet(&second));
        drop(held);
        assert!(second.admit().await.is_ok());
    }

    #[tokio::test]
    async fn under_warn_a_resume_never_waits_for_a_slot_admission_only_tried() {
        let budget = budget_in(PolicyMode::Warn, 4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let mut parking = seat(&budget, &global, route(&loans)).await;
        let parked = released(&mut parking).park().expect("parked");
        let service = slots(&budget, &loans);
        let running_guests = (
            seat(&budget, &global, route(&loans)).await,
            seat(&budget, &global, route(&loans)).await,
        );
        assert_eq!(service.all.available(), 0);

        let recorder = cufflink_telemetry::metrics_builder()
            .expect("builder")
            .build_recorder();
        let handle = recorder.handle();
        let recording = metrics::set_default_local_recorder(&recorder);
        let (cancel, shutdown) = (CancellationToken::new(), running());
        let retaken = tokio::time::timeout(
            WAIT,
            parked.resume(by(Duration::from_secs(5), &cancel, &shutdown)),
        )
        .await
        .expect("a warn-mode resume does not wait for the service slot")
        .expect("resumed");
        drop(recording);
        assert_eq!(retaken.len(), 1, "the global slot, waited for at admission");
        assert_eq!(global.available(), 1);
        assert!(quiet(&service.all));
        let rendered = handle.render();
        assert!(
            rendered.lines().any(
                |line| line.starts_with("cufflink_wasm_admission_refused_total{")
                    && line.contains(r#"mode="warn""#)
                    && line.contains(r#"reason="service_concurrency""#)
                    && line.ends_with(" 1")
            ),
            "the slot it went on without is counted as a warn-mode arrival's is: {rendered}"
        );
        drop((running_guests, retaken));
    }

    #[tokio::test]
    async fn a_released_call_is_counted_once_by_what_it_did() {
        let budget = budget(4, 1);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let mut first = seat(&budget, &global, route(&loans)).await;
        let mut second = seat(&budget, &global, route(&loans)).await;
        let catalog = identity("catalog-service");
        let mut reducer = seat(
            &budget,
            &global,
            ticket(&catalog, Class::Interactive, InvocationKind::SseReducer),
        )
        .await;
        let answer = |seat: &mut Seat, wait: Wait, transaction_open: bool| {
            let lent = seat.lend(wait, transaction_open);
            seat.restore(lent.answered_first());
        };
        let recorder = cufflink_telemetry::metrics_builder()
            .expect("builder")
            .build_recorder();
        let handle = recorder.handle();
        let parked = metrics::with_local_recorder(&recorder, || {
            answer(&mut first, Wait::Hold, false);
            answer(&mut first, Wait::Release, true);
            answer(&mut reducer, Wait::Release, false);
            answer(&mut first, Wait::Release, false);
            let parked = released(&mut first).park().expect("parked");
            assert_eq!(
                released(&mut second).park().err(),
                Some(Held::ServicePoolFull)
            );
            assert_eq!(
                second.lend(Wait::Release, true).park().err(),
                Some(Held::Transaction)
            );
            assert_eq!(released(&mut reducer).park().err(), Some(Held::NotParkable));
            assert_eq!(
                second.lend(Wait::Hold, false).park().err(),
                Some(Held::NotRequested)
            );
            parked
        });
        let rendered = handle.render();
        let count = |kind: &str, result: &str| {
            let series =
                format!(r#"cufflink_wasm_wait_release_total{{kind="{kind}",result="{result}"}} "#);
            rendered
                .lines()
                .find_map(|line| line.strip_prefix(series.as_str()))
                .map_or(0, |value| value.parse::<u64>().expect("a count"))
        };
        assert_eq!(
            [
                count("route", "answered_first"),
                count("route", "parked"),
                count("route", "held_service_pool_full"),
                count("route", "held_transaction"),
                count("sse_reducer", "held_not_parkable"),
                count("sse_reducer", "answered_first"),
            ],
            [1, 1, 1, 1, 1, 0],
            "{rendered}"
        );
        assert!(
            rendered.contains(r#"cufflink_wasm_parked{kind="route"} 1"#),
            "{rendered}"
        );
        drop(parked);
    }

    #[tokio::test]
    async fn a_resume_at_the_shutdown_hard_deadline_holds_nothing() {
        let first = Gate::new(1);
        let second = Gate::new(1);
        let held = second.try_take().expect("held by a running guest");
        let shutdown = Shutdown::new(crate::shutdown::KILL_MARGIN);
        shutdown.trigger();
        let cancel = CancellationToken::new();
        let ended = tokio::time::timeout(
            Duration::from_secs(1),
            parked(&[&first, &second], InvocationKind::Subscription).resume(by(
                Duration::from_secs(30),
                &cancel,
                &shutdown,
            )),
        )
        .await
        .expect("the hard deadline ends the resume")
        .map(|retaken| retaken.len());
        assert_eq!(ended, Err(Interrupt::Shutdown));
        assert_eq!(first.available(), 1, "the retaken prefix is given back");
        assert!(quiet(&first) && quiet(&second));
        drop(held);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_resumes_and_arrivals_all_finish_and_give_every_slot_back() {
        const SLOTS: usize = 3;
        const ROUNDS: usize = 200;
        let gate = Gate::new(SLOTS);
        let tasks = (0..ROUNDS)
            .flat_map(|round| {
                let resuming = Arc::clone(&gate);
                let arriving = Arc::clone(&gate);
                [
                    tokio::spawn(async move {
                        let permit = resuming.resume().await.expect("open");
                        if round % 3 == 0 {
                            tokio::task::yield_now().await;
                        }
                        drop(permit);
                    }),
                    tokio::spawn(async move {
                        let permit = arriving.admit().await.expect("open");
                        if round % 2 == 0 {
                            tokio::task::yield_now().await;
                        }
                        drop(permit);
                    }),
                ]
            })
            .collect::<Vec<_>>();
        tokio::time::timeout(
            Duration::from_secs(10),
            futures_util::future::try_join_all(tasks),
        )
        .await
        .expect("every resume and arrival finishes")
        .expect("joined");
        assert!(quiet(&gate));
        assert_eq!(gate.available(), SLOTS);
    }
}
