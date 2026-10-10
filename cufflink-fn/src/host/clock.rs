//! Time, sleep and identifiers.

/// What answers `util::*`.
pub trait ClockHost {
    fn now_rfc3339(&self) -> String;
    #[cfg_attr(not(feature = "util-v2"), allow(dead_code))]
    fn now_millis(&self) -> i64;
    /// The milliseconds slept.
    #[cfg_attr(not(feature = "util-v2"), allow(dead_code))]
    fn sleep_ms(&self, ms: u32) -> u32;
    fn uuid(&self) -> String;
}
