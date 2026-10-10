use super::{Outcome, PolicyKey, PolicyMode, Violation};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

pub(crate) const LOG_THROTTLE: Duration = Duration::from_secs(60);
const LOG_THROTTLE_KEYS: usize = 4_096;

pub(crate) struct LogThrottle<K> {
    window: Duration,
    last: Mutex<HashMap<K, Instant>>,
}

impl<K: Hash + Eq> LogThrottle<K> {
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            window,
            last: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn should_log(&self, key: K, now: Instant) -> bool {
        let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        match last.get(&key) {
            Some(at) if now.saturating_duration_since(*at) < self.window => false,
            _ => {
                if last.len() >= LOG_THROTTLE_KEYS {
                    last.retain(|_, at| now.saturating_duration_since(*at) < self.window);
                }
                if last.len() >= LOG_THROTTLE_KEYS {
                    last.clear();
                }
                last.insert(key, now);
                true
            }
        }
    }
}

static VIOLATIONS: LazyLock<LogThrottle<(PolicyKey, String, &'static str)>> =
    LazyLock::new(|| LogThrottle::new(LOG_THROTTLE));

pub(crate) fn record(
    key: PolicyKey,
    mode: PolicyMode,
    tenant_slug: &str,
    outcome: Outcome,
    violation: &Violation,
) {
    metrics::counter!(
        "cufflink_policy_violation_total",
        "key" => key.as_str(),
        "tenant" => tenant_slug.to_string(),
        "outcome" => outcome.as_str(),
        "reason" => violation.code,
    )
    .increment(1);
    if VIOLATIONS.should_log(
        (key, tenant_slug.to_string(), violation.code),
        Instant::now(),
    ) {
        tracing::warn!(
            target: "cufflink::policy",
            policy_key = key.as_str(),
            policy_mode = mode.as_str(),
            policy_outcome = outcome.as_str(),
            tenant = %tenant_slug,
            reason = violation.code,
            detail = %violation.detail,
            "Policy violation"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_throttle_once_per_window_per_key_tenant_reason() {
        let throttle = LogThrottle::new(Duration::from_secs(60));
        let t0 = Instant::now();
        let key = |tenant: &str, code: &'static str| (PolicyKey::Egress, tenant.to_string(), code);
        assert!(throttle.should_log(key("a", "private"), t0));
        assert!(!throttle.should_log(key("a", "private"), t0 + Duration::from_secs(59)));
        assert!(throttle.should_log(key("b", "private"), t0));
        assert!(throttle.should_log(key("a", "other"), t0));
        assert!(throttle.should_log(key("a", "private"), t0 + Duration::from_secs(60)));
    }

    #[test]
    fn log_throttle_forgets_keys_past_their_window() {
        let throttle = LogThrottle::new(Duration::from_secs(60));
        let t0 = Instant::now();
        (0..LOG_THROTTLE_KEYS).for_each(|n| assert!(throttle.should_log(n, t0)));
        let later = t0 + Duration::from_secs(61);
        assert!(throttle.should_log(LOG_THROTTLE_KEYS, later));
        let held = throttle
            .last
            .lock()
            .map(|last| last.len())
            .unwrap_or_default();
        assert_eq!(held, 1, "the expired keys were dropped");
        assert!(!throttle.should_log(LOG_THROTTLE_KEYS, later));
    }
}
