use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::OwnedMutexGuard;

pub struct Turns<K> {
    queues: Mutex<HashMap<K, Arc<tokio::sync::Mutex<()>>>>,
}

impl<K> Default for Turns<K> {
    fn default() -> Self {
        Self {
            queues: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Eq + Hash + Clone> Turns<K> {
    pub async fn take(&self, key: &K) -> OwnedMutexGuard<()> {
        let queue = {
            let mut queues = self.queues.lock().unwrap_or_else(PoisonError::into_inner);
            queues.retain(|_, queue| Arc::strong_count(queue) > 1);
            queues.entry(key.clone()).or_default().clone()
        };
        queue.lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn a_key_waits_for_its_own_turn_alone() {
        let turns = Turns::default();
        let held = turns.take(&"a").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), turns.take(&"b"))
                .await
                .is_ok(),
            "another key is not held up"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), turns.take(&"a"))
                .await
                .is_err(),
            "the same key waits"
        );
        drop(held);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), turns.take(&"a"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_key_no_one_holds_is_let_go() {
        let turns = Turns::default();
        drop(turns.take(&1).await);
        drop(turns.take(&2).await);
        let _held = turns.take(&3).await;
        assert_eq!(
            turns
                .queues
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len(),
            1
        );
    }
}
