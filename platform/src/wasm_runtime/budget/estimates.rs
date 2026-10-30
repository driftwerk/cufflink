use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use uuid::Uuid;

const REMEMBERED: usize = 16;
const COLD_HEADROOM: usize = 1024 * 1024;

struct Seen {
    samples: VecDeque<(Instant, usize)>,
    last_used: Instant,
}

pub(crate) struct Estimates {
    seen: Mutex<HashMap<(Uuid, String), Seen>>,
    max_entries: usize,
    max_age: Duration,
    cold: usize,
}

impl Estimates {
    pub(crate) fn new(max_entries: usize, max_age: Duration, cold: usize) -> Self {
        Self {
            seen: Mutex::default(),
            max_entries: max_entries.max(1),
            max_age,
            cold,
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<(Uuid, String), Seen>> {
        self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn estimate(
        &self,
        service_id: Uuid,
        handler: &str,
        initial: usize,
        cap: usize,
    ) -> usize {
        let now = Instant::now();
        let mut seen = self.lock();
        seen.get_mut(&(service_id, handler.to_string()))
            .and_then(|seen| {
                seen.last_used = now;
                seen.samples
                    .iter()
                    .filter(|(at, _)| now.saturating_duration_since(*at) < self.max_age)
                    .map(|(_, peak)| *peak)
                    .max()
            })
            .unwrap_or_else(|| initial.saturating_add(COLD_HEADROOM).max(self.cold))
            .min(cap)
    }

    pub(crate) fn record(&self, service_id: Uuid, handler: &str, peak: usize) {
        let now = Instant::now();
        let mut seen = self.lock();
        let key = (service_id, handler.to_string());
        if !seen.contains_key(&key) && seen.len() >= self.max_entries {
            if let Some(victim) = seen
                .iter()
                .min_by_key(|(_, seen)| seen.last_used)
                .map(|(key, _)| key.clone())
            {
                seen.remove(&victim);
            }
        }
        let entry = seen.entry(key).or_insert_with(|| Seen {
            samples: VecDeque::with_capacity(REMEMBERED),
            last_used: now,
        });
        if entry.samples.len() == REMEMBERED {
            entry.samples.pop_front();
        }
        entry.samples.push_back((now, peak));
        entry.last_used = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;

    fn estimates(max_entries: usize, max_age: Duration) -> Estimates {
        Estimates::new(max_entries, max_age, 32 * MIB)
    }

    #[test]
    fn a_cold_handler_reserves_the_seed_or_its_initial_memory_and_a_mebibyte() {
        let cold = estimates(8, Duration::from_secs(600));
        assert_eq!(
            cold.estimate(Uuid::nil(), "h", 2 * 65_536, 512 * MIB),
            32 * MIB
        );
        assert_eq!(
            cold.estimate(Uuid::nil(), "h", 40 * MIB, 512 * MIB),
            41 * MIB
        );
        assert_eq!(
            cold.estimate(Uuid::nil(), "h", 0, 16 * MIB),
            16 * MIB,
            "capped"
        );
    }

    #[test]
    fn a_measured_handler_reserves_the_largest_of_its_last_sixteen_samples() {
        let estimates = estimates(8, Duration::from_secs(600));
        let id = Uuid::new_v4();
        estimates.record(id, "h", 2 * MIB);
        assert_eq!(
            estimates.estimate(id, "h", 0, 512 * MIB),
            2 * MIB,
            "no ratchet to the cold seed"
        );
        estimates.record(id, "h", 40 * MIB);
        (0..15).for_each(|_| estimates.record(id, "h", 2 * MIB));
        assert_eq!(estimates.estimate(id, "h", 0, 512 * MIB), 40 * MIB);
        assert_eq!(estimates.estimate(id, "h", 0, 24 * MIB), 24 * MIB, "capped");
        estimates.record(id, "h", 3 * MIB);
        assert_eq!(estimates.estimate(id, "h", 0, 512 * MIB), 3 * MIB);
        assert_eq!(estimates.estimate(id, "other", 0, 512 * MIB), 32 * MIB);
    }

    #[test]
    fn samples_decay_past_the_maximum_age() {
        let estimates = estimates(8, Duration::from_millis(50));
        let id = Uuid::new_v4();
        estimates.record(id, "h", 80 * MIB);
        assert_eq!(estimates.estimate(id, "h", 0, usize::MAX), 80 * MIB);
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(estimates.estimate(id, "h", 0, usize::MAX), 32 * MIB);
    }

    #[test]
    fn the_map_is_bounded_and_outlives_the_version() {
        let estimates = estimates(2, Duration::from_secs(600));
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        estimates.record(a, "h", 10 * MIB);
        estimates.record(b, "h", 20 * MIB);
        assert_eq!(estimates.estimate(a, "h", 0, usize::MAX), 10 * MIB);
        estimates.record(c, "h", 30 * MIB);
        assert_eq!(estimates.estimate(a, "h", 0, usize::MAX), 10 * MIB);
        assert_eq!(
            estimates.estimate(b, "h", 0, usize::MAX),
            32 * MIB,
            "b was evicted"
        );
    }
}
