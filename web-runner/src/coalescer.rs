use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

pub struct Coalescer<K, V> {
    inflight: Arc<Mutex<HashMap<K, watch::Receiver<Option<V>>>>>,
}

impl<K, V> Clone for Coalescer<K, V> {
    fn clone(&self) -> Self {
        Self {
            inflight: self.inflight.clone(),
        }
    }
}

impl<K, V> Default for Coalescer<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self {
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K, V> Coalescer<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run<F, Fut>(&self, key: K, work: F) -> Option<V>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = V> + Send,
    {
        let mut rx = self.acquire_or_spawn(key, work).await;
        rx.changed().await.ok()?;
        let value = rx.borrow_and_update().clone();
        value
    }

    async fn acquire_or_spawn<F, Fut>(&self, key: K, work: F) -> watch::Receiver<Option<V>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = V> + Send,
    {
        let mut inflight = self.inflight.lock().await;
        if let Some(rx) = inflight.get(&key) {
            return rx.clone();
        }

        let (tx, rx) = watch::channel(None);
        inflight.insert(key.clone(), rx.clone());
        drop(inflight);

        let inflight_map = self.inflight.clone();
        tokio::spawn(async move {
            let value = work().await;
            let _ = tx.send(Some(value));
            inflight_map.lock().await.remove(&key);
        });

        rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn concurrent_calls_for_same_key_run_work_once() {
        let coalescer: Coalescer<&'static str, u32> = Coalescer::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..16)
            .map(|_| {
                let c = coalescer.clone();
                let cnt = counter.clone();
                tokio::spawn(async move {
                    c.run("k", move || async move {
                        cnt.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(50)).await;
                        42
                    })
                    .await
                })
            })
            .collect();

        let results: Vec<_> = futures_util::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(counter.load(Ordering::SeqCst), 1, "work ran more than once");
        assert!(
            results.iter().all(|r| *r == Some(42)),
            "all callers should observe the same value"
        );
    }

    #[tokio::test]
    async fn calls_for_different_keys_run_in_parallel() {
        let coalescer: Coalescer<u32, u32> = Coalescer::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let spawn_one = |key: u32, value: u32| {
            let c = coalescer.clone();
            let cnt = counter.clone();
            tokio::spawn(async move {
                c.run(key, move || async move {
                    cnt.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(50)).await;
                    value
                })
                .await
            })
        };

        let (ra, rb) = tokio::join!(spawn_one(1, 1), spawn_one(2, 2));
        assert_eq!(ra.unwrap(), Some(1));
        assert_eq!(rb.unwrap(), Some(2));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn slot_released_after_completion_so_retries_run_fresh_work() {
        let coalescer: Coalescer<&'static str, u32> = Coalescer::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let run = |cnt: Arc<AtomicUsize>| {
            coalescer.run("k", move || async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                7
            })
        };

        let r1 = run(counter.clone()).await;
        sleep(Duration::from_millis(20)).await;
        let r2 = run(counter.clone()).await;

        assert_eq!(r1, Some(7));
        assert_eq!(r2, Some(7));
        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "second call after completion must run fresh work"
        );
    }
}
