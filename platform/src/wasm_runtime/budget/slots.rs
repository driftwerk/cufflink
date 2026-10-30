use super::ServiceKey;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::Semaphore;
use uuid::Uuid;

const PRUNE_ABOVE: usize = 4_096;

struct RouteSlot {
    max: u32,
    semaphore: Arc<Semaphore>,
}

impl RouteSlot {
    fn idle(&self) -> bool {
        self.semaphore.available_permits() == self.max as usize
    }
}

#[derive(Default)]
pub(crate) struct RouteSlots {
    slots: Mutex<HashMap<(Uuid, String), RouteSlot>>,
}

impl RouteSlots {
    pub(crate) fn semaphore(&self, service_id: Uuid, handler: &str, max: u32) -> Arc<Semaphore> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let key = (service_id, handler.to_string());
        if let Some(slot) = slots.get(&key).filter(|slot| slot.max == max) {
            return Arc::clone(&slot.semaphore);
        }
        if slots.len() >= PRUNE_ABOVE {
            slots.retain(|_, slot| !slot.idle());
        }
        let semaphore = Arc::new(Semaphore::new(max as usize));
        slots.insert(
            key,
            RouteSlot {
                max,
                semaphore: Arc::clone(&semaphore),
            },
        );
        semaphore
    }
}

#[derive(Clone)]
pub(crate) struct ServiceSlot {
    pub all: Arc<Semaphore>,
    pub committed: Arc<Semaphore>,
    pub background: Arc<Semaphore>,
    per_service: usize,
}

impl ServiceSlot {
    fn idle(&self) -> bool {
        self.all.available_permits() == self.per_service
    }
}

pub(crate) struct ServiceSlots {
    slots: Mutex<HashMap<ServiceKey, ServiceSlot>>,
    per_service: usize,
}

impl ServiceSlots {
    pub(crate) fn new(per_service: usize) -> Self {
        Self {
            slots: Mutex::default(),
            per_service,
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
            all: Arc::new(Semaphore::new(self.per_service)),
            committed: Arc::new(Semaphore::new((self.per_service / 2).max(1))),
            background: Arc::new(Semaphore::new((self.per_service / 2).max(1))),
            per_service: self.per_service,
        };
        slots.insert(service.clone(), slot.clone());
        slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_route_keeps_its_slots_until_its_limit_changes() {
        let slots = RouteSlots::default();
        let id = Uuid::new_v4();
        let first = slots.semaphore(id, "export", 2);
        assert!(Arc::ptr_eq(&first, &slots.semaphore(id, "export", 2)));
        assert!(!Arc::ptr_eq(&first, &slots.semaphore(id, "import", 2)));
        assert!(!Arc::ptr_eq(
            &first,
            &slots.semaphore(Uuid::new_v4(), "export", 2)
        ));
        let redeployed = slots.semaphore(id, "export", 3);
        assert!(!Arc::ptr_eq(&first, &redeployed));
        assert_eq!(redeployed.available_permits(), 3);
    }

    #[test]
    fn committed_and_background_work_hold_at_most_half_of_a_services_slots() {
        let slots = ServiceSlots::new(32);
        let key = ServiceKey {
            tenant: "acme".into(),
            service: "catalog-service".into(),
        };
        let slot = slots.of(&key);
        assert_eq!(slot.all.available_permits(), 32);
        assert_eq!(slot.committed.available_permits(), 16);
        assert_eq!(slot.background.available_permits(), 16);
        assert!(Arc::ptr_eq(&slot.all, &slots.of(&key).all));
        assert_eq!(
            ServiceSlots::new(1).of(&key).committed.available_permits(),
            1
        );
    }

    #[test]
    fn idle_routes_are_pruned_and_busy_ones_kept() {
        let slots = RouteSlots::default();
        let busy = slots.semaphore(Uuid::nil(), "busy", 1);
        let held = Arc::clone(&busy)
            .try_acquire_owned()
            .expect("the only slot");
        (0..PRUNE_ABOVE).for_each(|n| {
            slots.semaphore(Uuid::new_v4(), &n.to_string(), 1);
        });
        let len = slots.slots.lock().expect("slots").len();
        assert!(len < PRUNE_ABOVE, "{len}");
        assert!(Arc::ptr_eq(&busy, &slots.semaphore(Uuid::nil(), "busy", 1)));
        drop(held);
    }
}
