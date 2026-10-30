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
