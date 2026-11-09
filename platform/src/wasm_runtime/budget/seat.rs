#![cfg_attr(
    not(test),
    expect(dead_code, reason = "the host's released waits park and resume seats")
)]

use crate::shutdown::Shutdown;
use crate::wasm_runtime::{Interrupt, InvocationKind};
use cufflink_telemetry::GaugeGuard;
use cufflink_types::host::wait::Wait;
use futures_util::{StreamExt, TryStreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{AcquireError, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub(crate) struct Gate {
    slots: Arc<Semaphore>,
    resuming: AtomicUsize,
    resumed: Notify,
}

struct Resuming<'a>(&'a Gate);

impl<'a> Resuming<'a> {
    fn on(gate: &'a Gate) -> Self {
        gate.resuming.fetch_add(1, Ordering::AcqRel);
        Self(gate)
    }
}

impl Drop for Resuming<'_> {
    fn drop(&mut self) {
        if self.0.resuming.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.resumed.notify_waiters();
        }
    }
}

impl Gate {
    pub(crate) fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(permits)),
            resuming: AtomicUsize::new(0),
            resumed: Notify::new(),
        })
    }

    pub(crate) fn available(&self) -> usize {
        self.slots.available_permits()
    }

    pub(crate) fn try_take(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.slots).try_acquire_owned().ok()
    }

    pub(crate) async fn admit(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        loop {
            let resumed = self.resumed.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            if self.resuming.load(Ordering::Acquire) == 0 {
                break;
            }
            resumed.await;
        }
        Arc::clone(&self.slots).acquire_owned().await
    }

    async fn resume(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        let _resuming = Resuming::on(self);
        Arc::clone(&self.slots).acquire_owned().await
    }
}

pub(crate) struct Taken {
    gate: Arc<Gate>,
    _permit: OwnedSemaphorePermit,
}

impl Taken {
    pub(crate) fn new(gate: Arc<Gate>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            gate,
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

#[derive(Clone)]
pub(crate) struct Parking {
    global: Arc<Semaphore>,
    service: Arc<Semaphore>,
}

impl Parking {
    pub(crate) fn new(global: Arc<Semaphore>, service: Arc<Semaphore>) -> Self {
        Self { global, service }
    }
}

pub(crate) struct Seat {
    route: Option<OwnedSemaphorePermit>,
    releasable: Vec<Taken>,
    parking: Result<Parking, Held>,
    kind: InvocationKind,
}

impl Seat {
    pub(crate) fn new(
        route: Option<OwnedSemaphorePermit>,
        releasable: Vec<Taken>,
        parking: Result<Parking, Held>,
        kind: InvocationKind,
    ) -> Self {
        Self {
            route,
            releasable,
            parking,
            kind,
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
}

impl Lent {
    pub(crate) fn requested(&self) -> bool {
        self.parking.as_ref().err() != Some(&Held::NotRequested)
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
        let (global, service) = self.reserve().map_err(|held| held.count(self.kind))?;
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
                .map(|taken| taken.gate)
                .collect(),
            kind: self.kind,
            since: Instant::now(),
            _parked: GaugeGuard::enter(
                metrics::gauge!("cufflink_wasm_parked", "kind" => self.kind.label()),
            ),
            _global: global,
            _service: service,
        })
    }

    fn reserve(&self) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), Held> {
        let parking = self.parking.as_ref().map_err(|held| *held)?;
        let global = Arc::clone(&parking.global)
            .try_acquire_owned()
            .map_err(|_| Held::PoolFull)?;
        let service = Arc::clone(&parking.service)
            .try_acquire_owned()
            .map_err(|_| Held::ServicePoolFull)?;
        Ok((global, service))
    }
}

pub(crate) struct ResumeBy<'a> {
    pub until: Instant,
    pub cancel: &'a CancellationToken,
    pub shutdown: &'a Shutdown,
}

pub(crate) struct Parked {
    gates: Vec<Arc<Gate>>,
    kind: InvocationKind,
    since: Instant,
    _parked: GaugeGuard,
    _global: OwnedSemaphorePermit,
    _service: OwnedSemaphorePermit,
}

impl Parked {
    pub(crate) async fn resume(self, by: ResumeBy<'_>) -> Result<Vec<Taken>, Interrupt> {
        let answered = Instant::now();
        let kind = self.kind.label();
        metrics::histogram!("cufflink_wasm_parked_seconds", "kind" => kind)
            .record(answered.duration_since(self.since).as_secs_f64());
        let retake = futures_util::stream::iter(self.gates)
            .then(|gate| async move { gate.resume().await.map(|permit| Taken::new(gate, permit)) })
            .try_collect::<Vec<_>>();
        let resumed = tokio::select! {
            biased;
            () = by.cancel.cancelled() => Err(Interrupt::Cancelled),
            () = by.shutdown.hard_deadline() => Err(Interrupt::Shutdown),
            () = tokio::time::sleep_until(by.until.into()) => Err(Interrupt::Deadline),
            retaken = retake => retaken.map_err(|_closed| Interrupt::Shutdown),
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
        resumed
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Admission, Budget, BudgetConfig, Class, RouteKey, Ticket, WaitRelease};
    use super::*;
    use crate::policy::PolicyMode;
    use crate::registry::ServiceIdentity;
    use crate::wasm_runtime::ExecutionError;
    use std::time::Duration;
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

    fn budget(max_parked: usize, max_parked_per_service: usize) -> Budget {
        Budget::new(
            BudgetConfig {
                mode: PolicyMode::Enforce,
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

    async fn seat(budget: &Budget, global: &Arc<Gate>, ticket: Ticket<'_>) -> Seat {
        let admission: Admission = budget.admit(ticket, global).await.expect("admitted");
        admission.into_parts().0
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

    fn quiet(gate: &Gate) -> bool {
        gate.resuming.load(Ordering::Acquire) == 0
    }

    #[tokio::test]
    async fn parking_releases_the_service_and_global_slots_and_keeps_the_route_slot() {
        let budget = budget(4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let route = RouteKey {
            service_id: loans.service_id,
            version: 1,
            index: 0,
        };
        let mut seat = seat(
            &budget,
            &global,
            Ticket {
                route: Some((route, 1)),
                ..ticket(&loans, Class::Interactive, InvocationKind::Route)
            },
        )
        .await;
        let service = budget.services.of(&super::super::ServiceKey::of(&loans));
        let route_gate = budget.routes.gate(route, 1);
        assert_eq!(
            (
                service.all.available(),
                global.available(),
                route_gate.available()
            ),
            (1, 3, 0)
        );

        let mut lent = released(&mut seat);
        let parked = lent.park().expect("parked");
        assert_eq!(
            (
                service.all.available(),
                global.available(),
                route_gate.available()
            ),
            (2, 4, 0),
            "the service and global slots go back, the route slot stays held"
        );

        let shutdown = Shutdown::new(Duration::from_secs(30));
        let cancel = CancellationToken::new();
        let retaken = parked
            .resume(by(Duration::from_secs(1), &cancel, &shutdown))
            .await
            .expect("resumed");
        seat.restore(retaken);
        assert_eq!(
            (
                service.all.available(),
                global.available(),
                route_gate.available()
            ),
            (1, 3, 0)
        );
        drop(seat);
        assert_eq!(
            (
                service.all.available(),
                global.available(),
                route_gate.available()
            ),
            (2, 4, 1)
        );
    }

    #[tokio::test]
    async fn a_seat_parks_only_with_a_permit_from_both_wait_pools() {
        let global = Gate::new(1);
        let lent = |pool: usize, service: usize| Lent {
            releasable: vec![Taken::new(
                Arc::clone(&global),
                global.try_take().expect("slot"),
            )],
            parking: Ok(Parking::new(
                Arc::new(Semaphore::new(pool)),
                Arc::new(Semaphore::new(service)),
            )),
            kind: InvocationKind::Route,
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
        let mut seat = seat(
            &budget,
            &global,
            ticket(&loans, Class::Interactive, InvocationKind::Route),
        )
        .await;
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
        let route = |identity| ticket(identity, Class::Interactive, InvocationKind::Route);

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

        let mut route = seat(
            &budget,
            &global,
            ticket(&loans, Class::Interactive, InvocationKind::Route),
        )
        .await;
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
        let service = budget.services.of(&super::super::ServiceKey::of(&loans));
        let parked = released(&mut seat).park().expect("parked");
        let class_cap = (0..service.background.available())
            .filter_map(|_| service.background.try_take())
            .collect::<Vec<_>>();

        let shutdown = Shutdown::new(Duration::from_secs(30));
        let cancel = CancellationToken::new();
        let resuming = tokio::spawn(async move {
            parked
                .resume(by(Duration::from_secs(5), &cancel, &shutdown))
                .await
                .map(|retaken| retaken.len())
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
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
        let retaken = resuming.await.expect("joined").expect("resumed");
        assert_eq!(retaken, 4, "class cap, service, background cap and global");
        assert!(quiet(&service.background));
    }

    #[tokio::test]
    async fn a_resume_is_granted_before_new_interactive_work() {
        let gate = Gate::new(1);
        let parked = Parked {
            gates: vec![Arc::clone(&gate)],
            kind: InvocationKind::Route,
            since: Instant::now(),
            _parked: GaugeGuard::enter(metrics::gauge!("cufflink_wasm_parked", "kind" => "route")),
            _global: Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("pool"),
            _service: Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("pool"),
        };
        let running = gate.try_take().expect("the running guest's slot");

        let shutdown = Shutdown::new(Duration::from_secs(30));
        let cancel = CancellationToken::new();
        let resume = tokio::spawn(async move {
            parked
                .resume(by(Duration::from_secs(5), &cancel, &shutdown))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let arrival = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { gate.admit().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(running);
        let retaken = resume
            .await
            .expect("joined")
            .expect("the resume is granted");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !arrival.is_finished(),
            "the arrival waits behind the resume"
        );

        drop(retaken);
        let admitted = tokio::time::timeout(Duration::from_secs(1), arrival)
            .await
            .expect("admitted once the resumed guest finishes")
            .expect("joined");
        assert!(admitted.is_ok());
    }

    #[tokio::test]
    async fn new_work_held_behind_resumes_is_refused_at_its_admission_deadline() {
        let budget = Arc::new(budget(4, 4));
        let global = Gate::new(4);
        let loans = Arc::new(identity("loan-service"));
        let route = || ticket(&loans, Class::Interactive, InvocationKind::Route);
        let mut parking = seat(&budget, &global, route()).await;
        let parked = released(&mut parking).park().expect("parked");
        let (running, _other) = (
            seat(&budget, &global, route()).await,
            seat(&budget, &global, route()).await,
        );

        let shutdown = Shutdown::new(Duration::from_secs(30));
        let cancel = CancellationToken::new();
        let resume = tokio::spawn(async move {
            parked
                .resume(by(Duration::from_secs(5), &cancel, &shutdown))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let started = std::time::Instant::now();
        let arrival = tokio::spawn({
            let (budget, global, loans) =
                (Arc::clone(&budget), Arc::clone(&global), Arc::clone(&loans));
            async move {
                budget
                    .admit(
                        ticket(&loans, Class::Interactive, InvocationKind::Route),
                        &global,
                    )
                    .await
                    .map(drop)
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(running);
        let resumed = resume.await.expect("joined");
        assert!(resumed.is_ok(), "the freed slot goes to the resume");

        let refused = arrival.await.expect("joined");
        assert!(
            matches!(refused, Err(ExecutionError::Overloaded { .. })),
            "{refused:?}"
        );
        let waited = started.elapsed();
        assert!(waited >= WAIT && waited < WAIT * 5, "{waited:?}");
    }

    #[tokio::test]
    async fn a_resume_that_reaches_its_deadline_holds_nothing() {
        let budget = budget(4, 4);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let route = || ticket(&loans, Class::Interactive, InvocationKind::Route);
        let mut parking = seat(&budget, &global, route()).await;
        let parked = released(&mut parking).park().expect("parked");
        let service = budget.services.of(&super::super::ServiceKey::of(&loans));
        let running = (
            seat(&budget, &global, route()).await,
            seat(&budget, &global, route()).await,
        );

        let shutdown = Shutdown::new(Duration::from_secs(30));
        let cancel = CancellationToken::new();
        let ended = parked
            .resume(by(Duration::from_millis(50), &cancel, &shutdown))
            .await
            .map(|retaken| retaken.len());
        assert_eq!(ended, Err(Interrupt::Deadline));
        assert!(quiet(&service.all) && quiet(&global));
        assert_eq!(global.available(), 2, "the prefix it retook is given back");

        drop(running);
        let arrival = tokio::time::timeout(WAIT, budget.admit(route(), &global)).await;
        assert!(
            matches!(arrival, Ok(Ok(_))),
            "a new arrival is admitted at once"
        );
    }

    #[tokio::test]
    async fn a_cancelled_resume_releases_the_slots_it_retook() {
        let first = Gate::new(1);
        let second = Gate::new(1);
        let parked = Parked {
            gates: vec![Arc::clone(&first), Arc::clone(&second)],
            kind: InvocationKind::Job,
            since: Instant::now(),
            _parked: GaugeGuard::enter(metrics::gauge!("cufflink_wasm_parked", "kind" => "job")),
            _global: Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("pool"),
            _service: Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("pool"),
        };
        let held = second.try_take().expect("held by a running guest");

        let shutdown = Shutdown::new(Duration::from_secs(30));
        let cancel = CancellationToken::new();
        let resume = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                parked
                    .resume(by(Duration::from_secs(5), &cancel, &shutdown))
                    .await
                    .map(|retaken| retaken.len())
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(first.available(), 0, "the first slot is retaken");
        cancel.cancel();
        assert_eq!(resume.await.expect("joined"), Err(Interrupt::Cancelled));
        assert_eq!(first.available(), 1);
        assert!(quiet(&first) && quiet(&second));
        drop(held);
        assert!(second.admit().await.is_ok());
    }
    #[tokio::test]
    async fn a_released_call_is_counted_once_by_what_it_did() {
        let budget = budget(4, 1);
        let global = Gate::new(4);
        let loans = identity("loan-service");
        let route = || ticket(&loans, Class::Interactive, InvocationKind::Route);
        let mut first = seat(&budget, &global, route()).await;
        let mut second = seat(&budget, &global, route()).await;
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
        let parked = Parked {
            gates: vec![Arc::clone(&first), Arc::clone(&second)],
            kind: InvocationKind::Subscription,
            since: Instant::now(),
            _parked: GaugeGuard::enter(
                metrics::gauge!("cufflink_wasm_parked", "kind" => "subscription"),
            ),
            _global: Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("pool"),
            _service: Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("pool"),
        };
        let held = second.try_take().expect("held by a running guest");
        let shutdown = Shutdown::new(crate::shutdown::KILL_MARGIN);
        shutdown.trigger();
        let cancel = CancellationToken::new();
        let ended = tokio::time::timeout(
            Duration::from_secs(1),
            parked.resume(by(Duration::from_secs(30), &cancel, &shutdown)),
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
