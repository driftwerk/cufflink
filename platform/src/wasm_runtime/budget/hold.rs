use super::caps::{CapExceeded, HostCaps};
use super::ledger::{Delivery, Ledger, Verdict};
use super::{Class, Denied, Reason, ServiceKey};
use crate::policy::PolicyMode;
use crate::wasm_runtime::InvocationKind;
use std::sync::Arc;

pub(crate) const STEP: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity {
    pub reason: Reason,
    pub requested: usize,
    pub holding: usize,
}

struct Owner {
    ledger: Arc<Ledger>,
    class: Class,
    service: ServiceKey,
    handler: Arc<str>,
    kind: InvocationKind,
}

#[derive(Clone)]
pub(crate) struct Account(Arc<Owner>);

impl Account {
    pub(super) fn new(
        ledger: Arc<Ledger>,
        class: Class,
        service: ServiceKey,
        handler: &str,
        kind: InvocationKind,
    ) -> Self {
        Self(Arc::new(Owner {
            ledger,
            class,
            service,
            handler: handler.into(),
            kind,
        }))
    }

    pub(super) fn ledger(&self) -> &Arc<Ledger> {
        &self.0.ledger
    }

    pub(super) fn class(&self) -> Class {
        self.0.class
    }

    pub(super) fn service(&self) -> &ServiceKey {
        &self.0.service
    }

    fn record(&self, reason: Reason, mode: PolicyMode, requested: usize) {
        Denied {
            tenant: &self.0.service.tenant,
            service: &self.0.service.service,
            handler: &self.0.handler,
            kind: self.0.kind,
            reason,
            what: "host",
            mode,
            requested,
        }
        .record();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terms {
    Refusable,
    Unrefused,
    Kept { room: usize },
}

pub struct Hold {
    account: Option<Account>,
    bytes: usize,
    terms: Terms,
    delivery: Option<Delivery>,
}

impl std::fmt::Debug for Hold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hold")
            .field("charged", &self.account.is_some())
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Hold {
    pub const fn none() -> Self {
        Self {
            account: None,
            bytes: 0,
            terms: Terms::Refusable,
            delivery: None,
        }
    }

    pub(crate) fn on(account: Option<Account>) -> Self {
        Self {
            account,
            bytes: 0,
            terms: Terms::Refusable,
            delivery: None,
        }
    }

    pub(crate) fn unrefused(account: Option<Account>) -> Self {
        Self {
            account,
            bytes: 0,
            terms: Terms::Unrefused,
            delivery: None,
        }
    }

    pub(crate) fn kept(account: Option<Account>, room: usize) -> Self {
        Self {
            account,
            bytes: 0,
            terms: Terms::Kept { room },
            delivery: None,
        }
    }

    pub(crate) fn sibling(&self) -> Self {
        Self {
            account: self.account.clone(),
            bytes: 0,
            terms: self.terms,
            delivery: None,
        }
    }

    pub(crate) fn delivering(&self) -> Self {
        Self::unrefused(self.account.clone())
    }

    pub(crate) async fn make_room(&mut self, bytes: usize) {
        if let Some(account) = &self.account {
            self.delivery = account.ledger().delivery(bytes, self.bytes).await;
        }
    }

    pub(crate) fn delivered(&mut self) {
        self.delivery = None;
    }

    pub(crate) fn past_the_ceiling(&self) -> bool {
        self.account
            .as_ref()
            .is_some_and(|account| account.ledger().past_the_ceiling())
    }

    pub(crate) fn share(&self) -> Option<usize> {
        self.account
            .as_ref()
            .map(|account| account.ledger().limits().service)
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn grow(&mut self, extra: usize) -> Result<(), Capacity> {
        let Some(account) = self.account.as_ref().filter(|_| extra > 0) else {
            return Ok(());
        };
        let holding = self.bytes.saturating_add(extra);
        let (ledger, class, service) = (account.ledger(), account.class(), account.service());
        let verdict = match self.terms {
            Terms::Refusable => ledger.grow(class, service, extra),
            Terms::Unrefused => ledger.deliver(class, service, extra, holding),
            Terms::Kept { room } => ledger.keep(class, service, extra, room),
        };
        match verdict {
            Verdict::Allowed => {}
            Verdict::Warned(reason) => account.record(reason, PolicyMode::Warn, extra),
            Verdict::Denied(reason) => {
                account.record(reason, PolicyMode::Enforce, extra);
                return Err(Capacity {
                    reason,
                    requested: extra,
                    holding,
                });
            }
        }
        self.bytes += extra;
        Ok(())
    }

    pub(crate) fn force(&mut self, extra: usize) {
        if let Some(account) = self.account.as_ref().filter(|_| extra > 0) {
            let (ledger, class, service) = (account.ledger(), account.class(), account.service());
            match self.terms {
                Terms::Kept { .. } => ledger.force_kept(class, service, extra),
                Terms::Refusable | Terms::Unrefused => ledger.force(class, service, extra),
            }
            self.bytes += extra;
        }
    }

    pub(crate) fn shrink_to(&mut self, bytes: usize) {
        if let Some(extra) = self.bytes.checked_sub(bytes).filter(|extra| *extra > 0) {
            self.give_back(extra);
        }
    }

    fn give_back(&mut self, bytes: usize) {
        self.bytes -= bytes;
        if let Some(account) = &self.account {
            let (ledger, class, service) = (account.ledger(), account.class(), account.service());
            match self.terms {
                Terms::Kept { .. } => ledger.release_kept(class, service, bytes),
                Terms::Refusable | Terms::Unrefused => ledger.release(class, service, bytes),
            }
        }
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.give_back(self.bytes);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    Budget(Capacity),
    Cap(CapExceeded),
}

impl Refused {
    pub(crate) fn needing(self, bytes: usize) -> Self {
        match self {
            Self::Budget(capacity) => Self::Budget(Capacity {
                holding: capacity.holding.max(bytes),
                ..capacity
            }),
            Self::Cap(cap) => Self::Cap(cap),
        }
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Budget(capacity) => write!(
                f,
                "{} more bytes would pass the guest memory budget ({})",
                capacity.requested, capacity.reason
            ),
            Self::Cap(cap) => cap.fmt(f),
        }
    }
}

pub trait Meter {
    fn charge(&mut self, bytes: usize) -> Result<(), Refused>;

    fn credit(&mut self, bytes: usize);
}

impl Meter for Hold {
    fn charge(&mut self, bytes: usize) -> Result<(), Refused> {
        self.grow(bytes).map_err(Refused::Budget)
    }

    fn credit(&mut self, bytes: usize) {
        self.shrink_to(self.bytes.saturating_sub(bytes));
    }
}

pub(crate) struct Capped<'a, M: ?Sized> {
    meter: &'a mut M,
    caps: &'a HostCaps,
    path: &'static str,
    used: usize,
    counted: bool,
}

impl<'a, M: Meter + ?Sized> Capped<'a, M> {
    pub(crate) fn new(meter: &'a mut M, caps: &'a HostCaps, path: &'static str) -> Self {
        Self {
            meter,
            caps,
            path,
            used: 0,
            counted: false,
        }
    }
}

impl<M: Meter + ?Sized> Meter for Capped<'_, M> {
    fn charge(&mut self, bytes: usize) -> Result<(), Refused> {
        let used = self.used.saturating_add(bytes);
        if used > self.caps.response && !self.counted {
            self.counted = true;
            self.caps.response(self.path, used).map_err(Refused::Cap)?;
        }
        self.meter.charge(bytes)?;
        self.used = used;
        Ok(())
    }

    fn credit(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes);
        self.meter.credit(bytes);
    }
}

pub(crate) struct Drawing<'a> {
    hold: &'a mut Hold,
    left: usize,
}

impl<'a> Drawing<'a> {
    pub(crate) fn from(hold: &'a mut Hold, left: usize) -> Self {
        let left = left.min(hold.bytes());
        Self { hold, left }
    }
}

impl Meter for Drawing<'_> {
    fn charge(&mut self, bytes: usize) -> Result<(), Refused> {
        let drawn = bytes.min(self.left);
        self.left -= drawn;
        self.hold.force(bytes - drawn);
        Ok(())
    }

    fn credit(&mut self, bytes: usize) {
        self.left = self.left.saturating_add(bytes).min(self.hold.bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::super::ledger::{Limits, Settings};
    use super::*;
    use std::time::Duration;

    const MIB: usize = 1024 * 1024;

    fn owned(enforce: bool) -> (Arc<Ledger>, Account) {
        let ledger = Arc::new(Ledger::new(Settings {
            limits: Limits {
                budget: 100 * MIB,
                admit: 90 * MIB,
                service: 50 * MIB,
                background: 75 * MIB,
                overrun: 50 * MIB,
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
        (ledger, account)
    }

    #[test]
    fn a_hold_debits_what_it_grows_and_credits_on_drop() {
        let (ledger, account) = owned(true);
        let mut hold = Hold::on(Some(account));
        hold.grow(3 * MIB).expect("fits");
        hold.grow(2 * MIB).expect("fits");
        assert_eq!((hold.bytes(), ledger.in_use()), (5 * MIB, 5 * MIB));
        hold.shrink_to(2 * MIB);
        assert_eq!(ledger.in_use(), 2 * MIB);
        drop(hold);
        assert_eq!(ledger.in_use(), 0);
    }

    #[test]
    fn a_delivering_hold_and_its_siblings_charge_past_the_share() {
        let (ledger, account) = owned(true);
        let mut full = Hold::on(Some(account));
        full.grow(50 * MIB).expect("at the share");
        let mut hold = full.delivering();
        hold.grow(MIB).expect("past the share");
        let mut sibling = hold.sibling();
        sibling.grow(MIB).expect("past the share");
        hold.force(MIB);
        assert_eq!(ledger.in_use(), 53 * MIB);
        drop((full, hold, sibling));
        assert_eq!(ledger.in_use(), 0);
    }

    #[test]
    fn a_delivering_hold_is_refused_when_it_alone_passes_the_share() {
        let (ledger, account) = owned(true);
        let mut hold = Hold::unrefused(Some(account));
        hold.grow(50 * MIB).expect("at the share");
        let refused = hold.grow(1).expect_err("alone past the share");
        assert_eq!(refused.reason, Reason::DeliveryLimit);
        assert_eq!(refused.holding, 50 * MIB + 1);
        assert_eq!(ledger.in_use(), 50 * MIB);
    }

    #[test]
    fn a_kept_hold_is_refused_at_the_share_and_gives_back_what_it_kept() {
        let (ledger, account) = owned(true);
        let mut kept = Hold::kept(Some(account), 4 * MIB);
        kept.grow(48 * MIB).expect("fits");
        let mut tree = kept.sibling();
        let refused = tree.grow(3 * MIB).expect_err("past the share");
        assert_eq!(refused.reason, Reason::ServiceShare);
        kept.shrink_to(MIB);
        tree.grow(3 * MIB).expect("fits now");
        drop(tree);
        assert_eq!(ledger.in_use(), MIB);
        drop(kept);
        assert_eq!(ledger.in_use(), 0);
    }

    #[tokio::test]
    async fn a_hold_ends_its_delivery_with_its_tree_or_when_its_text_is_rendered() {
        use super::super::testing;

        let (ledger, base) = testing::hold(64 * MIB);
        let mut hold = base.delivering();
        hold.make_room(8 * MIB).await;
        assert_eq!(ledger.delivering(), 8 * MIB);
        drop(hold);
        assert_eq!(ledger.delivering(), 0);

        let mut hold = base.delivering();
        hold.make_room(8 * MIB).await;
        hold.delivered();
        assert_eq!(ledger.delivering(), 0);
        assert_eq!(ledger.in_use(), 0);
    }

    #[test]
    fn a_refusal_needing_more_than_it_held_names_what_is_needed() {
        let refused = |holding| {
            Refused::Budget(Capacity {
                reason: Reason::ServiceShare,
                requested: MIB,
                holding,
            })
        };
        let needed = |refused: Refused| match refused {
            Refused::Budget(capacity) => capacity.holding,
            Refused::Cap(_) => panic!("a budget refusal"),
        };
        assert_eq!(needed(refused(2 * MIB).needing(3 * MIB)), 3 * MIB);
        assert_eq!(needed(refused(4 * MIB).needing(3 * MIB)), 4 * MIB);
    }

    #[test]
    fn a_forced_charge_on_a_kept_hold_is_credited_as_kept() {
        let (ledger, account) = owned(true);
        let mut kept = Hold::kept(Some(account), 4 * MIB);
        kept.grow(MIB).expect("fits");
        kept.force(MIB);
        assert_eq!(ledger.kept_total(), 2 * MIB);
        drop(kept);
        assert_eq!(ledger.in_use(), 0);
        assert_eq!(ledger.kept_total(), 0, "nothing is left marked as kept");
    }

    #[test]
    fn enforce_refuses_past_the_share_and_warn_counts_it() {
        let (ledger, account) = owned(true);
        let mut hold = Hold::on(Some(account));
        let refused = hold.grow(51 * MIB).expect_err("past the share");
        assert_eq!(refused.reason, Reason::ServiceShare);
        assert_eq!((hold.bytes(), ledger.in_use()), (0, 0));

        let (ledger, account) = owned(false);
        let mut hold = Hold::on(Some(account));
        hold.grow(51 * MIB).expect("warn allows");
        assert_eq!(ledger.in_use(), 51 * MIB);
    }

    #[test]
    fn a_drawing_spends_the_reservation_then_debits_past_it_unrefused() {
        let (ledger, account) = owned(true);
        let mut hold = Hold::on(Some(account));
        hold.grow(2 * MIB).expect("fits");
        let mut drawing = Drawing::from(&mut hold, 2 * MIB);
        drawing.charge(MIB).expect("drawn");
        drawing.charge(MIB).expect("drawn");
        assert_eq!(ledger.in_use(), 2 * MIB);
        drawing.charge(60 * MIB).expect("never refused");
        assert_eq!(ledger.in_use(), 62 * MIB);
        drop(hold);
        assert_eq!(ledger.in_use(), 0);
    }

    #[test]
    fn without_a_ledger_nothing_is_evaluated() {
        let mut hold = Hold::none();
        hold.grow(usize::MAX).expect("off");
        hold.force(usize::MAX);
        assert_eq!(hold.bytes(), 0);
    }
}
