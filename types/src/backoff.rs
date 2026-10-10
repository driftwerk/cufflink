//! Retry delays for subscriptions, jobs, the outbox and webhook deliveries.

use std::time::Duration;

const JITTER_PERCENT: u64 = 20;
const MIN_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backoff {
    steps: Vec<Duration>,
    base: Duration,
    max: Duration,
}

impl Backoff {
    /// The n-th retry waits `steps[min(n, len) - 1]`; with no steps every
    /// retry waits the minimum delay.
    pub fn explicit(steps: Vec<Duration>) -> Self {
        Self {
            steps,
            base: MIN_DELAY,
            max: MIN_DELAY,
        }
    }

    /// The n-th retry waits `base * 2^(n - 1)`, capped at `max`.
    pub fn exponential(base: Duration, max: Duration) -> Self {
        Self {
            steps: Vec::new(),
            base,
            max: max.max(base),
        }
    }

    /// The delay before retry `attempt` (1 for the first), spread ±20% by a
    /// jitter that depends only on the seed and the attempt, and never
    /// below one second.
    pub fn delay(&self, attempt: u32, jitter_seed: u64) -> Duration {
        let attempt = attempt.max(1);
        jittered(self.nominal(attempt), attempt, jitter_seed).max(MIN_DELAY)
    }

    /// The longest a retry waits, before its jitter.
    pub fn longest(&self) -> Duration {
        self.steps.iter().copied().max().unwrap_or(self.max)
    }

    fn nominal(&self, attempt: u32) -> Duration {
        let index = usize::try_from(attempt - 1).unwrap_or(usize::MAX);
        match self.steps.last() {
            Some(last) => self.steps.get(index).unwrap_or(last).to_owned(),
            None => 1u32
                .checked_shl(attempt - 1)
                .and_then(|factor| self.base.checked_mul(factor))
                .map_or(self.max, |delay| delay.min(self.max)),
        }
    }
}

fn jittered(nominal: Duration, attempt: u32, seed: u64) -> Duration {
    let span = 2 * JITTER_PERCENT + 1;
    let percent = 100 - JITTER_PERCENT + mix(seed, attempt) % span;
    let millis = u128::from(percent) * nominal.as_millis() / 100;
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
}

/// SplitMix64 over the seed and the attempt.
fn mix(seed: u64, attempt: u32) -> u64 {
    let z = seed
        .wrapping_add(u64::from(attempt).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn within_jitter(delay: Duration, nominal: Duration) -> bool {
        let (low, high) = (nominal.mul_f64(0.8), nominal.mul_f64(1.2));
        (low.max(MIN_DELAY)..=high.max(MIN_DELAY)).contains(&delay)
    }

    #[test]
    fn explicit_steps_clamp_to_the_last() {
        let backoff = Backoff::explicit(vec![secs(10), secs(30)]);
        for (attempt, nominal) in [(1, 10), (2, 30), (3, 30), (50, 30), (u32::MAX, 30)] {
            let delay = backoff.delay(attempt, 7);
            assert!(within_jitter(delay, secs(nominal)), "{attempt}: {delay:?}");
        }
    }

    #[test]
    fn attempt_zero_is_the_first_retry() {
        let backoff = Backoff::explicit(vec![secs(10), secs(30)]);
        assert_eq!(backoff.delay(0, 7), backoff.delay(1, 7));
    }

    #[test]
    fn exponential_doubles_and_caps() {
        let backoff = Backoff::exponential(secs(5), secs(300));
        for (attempt, nominal) in [
            (1, 5),
            (2, 10),
            (3, 20),
            (6, 160),
            (7, 300),
            (8, 300),
            (33, 300),
            (64, 300),
            (u32::MAX, 300),
        ] {
            let delay = backoff.delay(attempt, 99);
            assert!(within_jitter(delay, secs(nominal)), "{attempt}: {delay:?}");
        }
    }

    #[test]
    fn a_cap_below_the_base_is_the_base() {
        let backoff = Backoff::exponential(secs(30), secs(10));
        assert!(within_jitter(backoff.delay(1, 1), secs(30)));
        assert!(within_jitter(backoff.delay(9, 1), secs(30)));
    }

    #[test]
    fn the_longest_wait_is_the_cap_or_the_longest_step() {
        for (backoff, longest) in [
            (Backoff::exponential(secs(5), secs(300)), secs(300)),
            (Backoff::exponential(secs(30), secs(10)), secs(30)),
            (
                Backoff::explicit(vec![secs(10), secs(30), secs(20)]),
                secs(30),
            ),
            (Backoff::explicit(vec![]), MIN_DELAY),
        ] {
            assert_eq!(backoff.longest(), longest, "{backoff:?}");
            let jittered = longest.mul_f64(1.2).max(MIN_DELAY);
            for attempt in (1..=40).chain([u32::MAX]) {
                assert!(backoff.delay(attempt, 3) <= jittered, "{backoff:?}");
            }
        }
    }

    #[test]
    fn jitter_is_deterministic_bounded_and_spread() {
        let backoff = Backoff::exponential(secs(100), secs(100));
        let delays: Vec<Duration> = (0..500).map(|seed| backoff.delay(1, seed)).collect();
        assert!(delays.iter().all(|d| within_jitter(*d, secs(100))));
        assert_eq!(delays[3], backoff.delay(1, 3));
        let distinct: std::collections::BTreeSet<_> = delays.iter().collect();
        assert!(distinct.len() > 20, "{}", distinct.len());
        assert!(delays.iter().any(|d| *d < secs(100)));
        assert!(delays.iter().any(|d| *d > secs(100)));
    }

    #[test]
    fn a_delay_is_never_below_one_second() {
        for backoff in [
            Backoff::explicit(vec![]),
            Backoff::explicit(vec![Duration::ZERO]),
            Backoff::explicit(vec![Duration::from_millis(900)]),
            Backoff::exponential(Duration::ZERO, Duration::ZERO),
            Backoff::exponential(Duration::from_millis(10), Duration::from_millis(20)),
        ] {
            for seed in 0..50 {
                assert!(backoff.delay(1, seed) >= MIN_DELAY, "{backoff:?}");
            }
        }
    }

    #[test]
    fn huge_durations_do_not_overflow() {
        let backoff = Backoff::exponential(Duration::MAX, Duration::MAX);
        assert!(backoff.delay(40, 5) >= MIN_DELAY);
        let backoff = Backoff::explicit(vec![Duration::MAX]);
        assert!(backoff.delay(1, 5) >= MIN_DELAY);
    }
}
