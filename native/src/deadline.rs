use cufflink_types::ext::CallContext;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A wait no call outlives, so a deadline far off stays an [`Instant`].
const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365 * 30);

/// The deadline of a call as an [`Instant`].
pub trait CallDeadline {
    /// When the caller stops waiting for the reply; no later than now once
    /// it has passed.
    fn deadline(&self) -> Instant;
}

impl CallDeadline for CallContext {
    fn deadline(&self) -> Instant {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Instant::now()
            + Duration::from_millis(self.deadline_ms)
                .saturating_sub(now)
                .min(FAR_FUTURE)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use cufflink_types::ext::CallerService;

    pub(crate) fn context_until(deadline_ms: u64) -> CallContext {
        CallContext {
            tenant: "harbor".into(),
            service: CallerService {
                name: "reports-service".into(),
                version: 3,
            },
            auth: None,
            deadline_ms,
        }
    }

    pub(crate) fn in_ms(ms: u64) -> u64 {
        u64::try_from(
            (SystemTime::now() + Duration::from_millis(ms))
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("millis")
    }

    #[test]
    fn the_deadline_is_when_the_caller_stops_waiting() {
        let before = Instant::now();
        let deadline = context_until(in_ms(1_000)).deadline();
        assert!(
            deadline > before + Duration::from_millis(900)
                && deadline <= Instant::now() + Duration::from_millis(1_000),
            "{:?}",
            deadline - before
        );
    }

    #[test]
    fn a_passed_deadline_is_now_and_a_far_one_is_bounded() {
        assert!(context_until(0).deadline() <= Instant::now());
        assert!(context_until(in_ms(0) - 5_000).deadline() <= Instant::now());
        assert!(context_until(u64::MAX).deadline() <= Instant::now() + FAR_FUTURE);
    }
}
