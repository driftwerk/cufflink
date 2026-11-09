use super::seat::Gate;
use super::ServiceKey;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::Semaphore;
use uuid::Uuid;

const PRUNE_ABOVE: usize = 4_096;

struct RouteSlot {
    max: u32,
    gate: Arc<Gate>,
}

impl RouteSlot {
    fn idle(&self) -> bool {
        self.gate.available() == self.max as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RouteKey {
    pub service_id: Uuid,
    pub version: i32,
    pub index: usize,
}

#[derive(Default)]
pub(crate) struct RouteSlots {
    slots: Mutex<HashMap<RouteKey, RouteSlot>>,
}

impl RouteSlots {
    pub(crate) fn gate(&self, key: RouteKey, max: u32) -> Arc<Gate> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = slots.get(&key).filter(|slot| slot.max == max) {
            return Arc::clone(&slot.gate);
        }
        if slots.len() >= PRUNE_ABOVE {
            slots.retain(|_, slot| !slot.idle());
        }
        let gate = Gate::new(max as usize);
        slots.insert(
            key,
            RouteSlot {
                max,
                gate: Arc::clone(&gate),
            },
        );
        gate
    }
}

#[derive(Clone)]
pub(crate) struct ServiceSlot {
    pub all: Arc<Gate>,
    pub committed: Arc<Gate>,
    pub background: Arc<Gate>,
    pub parked: Arc<Semaphore>,
    per_service: usize,
    parked_per_service: usize,
}

impl ServiceSlot {
    fn idle(&self) -> bool {
        self.all.available() == self.per_service
            && self.parked.available_permits() == self.parked_per_service
    }
}

pub(crate) struct ServiceSlots {
    slots: Mutex<HashMap<ServiceKey, ServiceSlot>>,
    per_service: usize,
    parked_per_service: usize,
}

impl ServiceSlots {
    pub(crate) fn new(per_service: usize, parked_per_service: usize) -> Self {
        Self {
            slots: Mutex::default(),
            per_service,
            parked_per_service,
        }
    }

    pub(crate) fn of(&self, service: &ServiceKey) -> ServiceSlot {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = slots.get(service) {
            return slot.clone();
        }
        if slots.len() >= PRUNE_ABOVE {
            slots.retain(|_, slot| !slot.idle());
        }
        let slot = ServiceSlot {
            all: Gate::new(self.per_service),
            committed: Gate::new((self.per_service / 2).max(1)),
            background: Gate::new((self.per_service / 2).max(1)),
            parked: Arc::new(Semaphore::new(self.parked_per_service)),
            per_service: self.per_service,
            parked_per_service: self.parked_per_service,
        };
        slots.insert(service.clone(), slot.clone());
        slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(service_id: Uuid, version: i32, index: usize) -> RouteKey {
        RouteKey {
            service_id,
            version,
            index,
        }
    }

    #[test]
    fn a_route_keeps_its_slots_and_routes_sharing_a_handler_keep_their_own() {
        let slots = RouteSlots::default();
        let id = Uuid::new_v4();
        let first = slots.gate(key(id, 1, 0), 2);
        assert!(Arc::ptr_eq(&first, &slots.gate(key(id, 1, 0), 2)));
        let sibling = slots.gate(key(id, 1, 1), 5);
        assert!(!Arc::ptr_eq(&first, &sibling));
        assert!(
            Arc::ptr_eq(&first, &slots.gate(key(id, 1, 0), 2)),
            "a sibling route on the same handler leaves this one's slots alone"
        );
        assert_eq!(sibling.available(), 5);
        assert!(!Arc::ptr_eq(
            &first,
            &slots.gate(key(Uuid::new_v4(), 1, 0), 2)
        ));
        let redeployed = slots.gate(key(id, 2, 0), 3);
        assert!(!Arc::ptr_eq(&first, &redeployed));
        assert_eq!(redeployed.available(), 3);
    }

    #[test]
    fn committed_and_background_work_hold_at_most_half_of_a_services_slots() {
        let slots = ServiceSlots::new(32, 64);
        let key = ServiceKey {
            tenant: "acme".into(),
            service: "catalog-service".into(),
        };
        let slot = slots.of(&key);
        assert_eq!(slot.all.available(), 32);
        assert_eq!(slot.committed.available(), 16);
        assert_eq!(slot.background.available(), 16);
        assert_eq!(slot.parked.available_permits(), 64);
        assert!(Arc::ptr_eq(&slot.all, &slots.of(&key).all));
        assert_eq!(ServiceSlots::new(1, 2).of(&key).committed.available(), 1);
    }

    #[test]
    fn idle_routes_are_pruned_and_busy_ones_kept() {
        let slots = RouteSlots::default();
        let busy = slots.gate(key(Uuid::nil(), 1, 0), 1);
        let held = busy.try_take().expect("the only slot");
        (0..PRUNE_ABOVE).for_each(|n| {
            slots.gate(key(Uuid::new_v4(), 1, n), 1);
        });
        let len = slots.slots.lock().expect("slots").len();
        assert!(len < PRUNE_ABOVE, "{len}");
        assert!(Arc::ptr_eq(&busy, &slots.gate(key(Uuid::nil(), 1, 0), 1)));
        drop(held);
    }

    #[test]
    fn a_service_with_a_parked_guest_is_never_pruned() {
        let slots = ServiceSlots::new(1, 1);
        let service = |name: String| ServiceKey {
            tenant: "acme".into(),
            service: name.into(),
        };
        let parked = slots.of(&service("loan-service".into()));
        let waiting = Arc::clone(&parked.parked)
            .try_acquire_owned()
            .expect("the wait pool's permit");
        (0..PRUNE_ABOVE).for_each(|n| {
            slots.of(&service(format!("catalog-service-{n}")));
        });
        let len = slots.slots.lock().expect("slots").len();
        assert!(len < PRUNE_ABOVE, "{len}");
        assert!(
            Arc::ptr_eq(&parked.all, &slots.of(&service("loan-service".into())).all),
            "a parked guest holds no slot, yet its service keeps its slots"
        );
        drop(waiting);
    }
}
