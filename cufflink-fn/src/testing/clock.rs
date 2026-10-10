//! A clock a test sets.

use crate::host::clock::ClockHost;
use std::cell::Cell;
use std::rc::Rc;

/// Time that moves only when told to, or slept through: a sleep returns
/// at once and the clock is later by as much. Its identifiers count up
/// from one. Clones share one clock.
#[derive(Clone, Default)]
pub struct FakeClock {
    millis: Rc<Cell<i64>>,
    slept: Rc<Cell<u64>>,
    identifiers: Rc<Cell<u64>>,
}

impl FakeClock {
    /// A clock at `millis` since the Unix epoch.
    pub fn at(millis: i64) -> Self {
        let clock = Self::default();
        clock.set(millis);
        clock
    }

    pub fn set(&self, millis: i64) {
        self.millis.set(millis);
    }

    pub fn advance(&self, millis: i64) {
        self.millis.set(self.millis.get().saturating_add(millis));
    }

    /// The milliseconds slept so far.
    pub fn slept_ms(&self) -> u64 {
        self.slept.get()
    }
}

impl ClockHost for FakeClock {
    fn now_rfc3339(&self) -> String {
        crate::util::rfc3339(self.millis.get())
    }

    fn now_millis(&self) -> i64 {
        self.millis.get()
    }

    fn sleep_ms(&self, ms: u32) -> u32 {
        self.advance(i64::from(ms));
        self.slept.set(self.slept.get() + u64::from(ms));
        ms
    }

    fn uuid(&self) -> String {
        let next = self.identifiers.get() + 1;
        self.identifiers.set(next);
        format!("00000000-0000-4000-8000-{next:012x}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_moves_when_told_to_or_slept_through() {
        let clock = FakeClock::at(1_790_589_600_000);
        assert_eq!(clock.now_rfc3339(), "2026-09-28T10:00:00.000+00:00");
        clock.advance(1_500);
        assert_eq!(clock.sleep_ms(30_000), 30_000);
        assert_eq!(clock.now_millis(), 1_790_589_631_500);
        assert_eq!(clock.now_rfc3339(), "2026-09-28T10:00:31.500+00:00");
        assert_eq!(clock.slept_ms(), 30_000);
        let shared = clock.clone();
        shared.set(0);
        assert_eq!(clock.now_rfc3339(), "1970-01-01T00:00:00.000+00:00");
    }

    #[test]
    fn identifiers_are_version_4_and_count_up() {
        let clock = FakeClock::default();
        assert_eq!(clock.uuid(), "00000000-0000-4000-8000-000000000001");
        assert_eq!(clock.uuid(), "00000000-0000-4000-8000-000000000002");
    }
}
