use crate::schedules::fire::CatchUpLimits;
use cufflink_db::{env_secs, env_var};
use cufflink_types::ConsumerSpec;
use eyre::{eyre, Result};
use std::time::Duration;

const DEFAULT_ACK_WAIT_SECS: u32 = 30;
const DEFAULT_RECONCILE_INTERVAL_SECS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventingConfig {
    pub consumer_default_ack_wait: Duration,
    pub reconcile_interval: Duration,
    pub catch_up: CatchUpLimits,
}

impl Default for EventingConfig {
    fn default() -> Self {
        Self {
            consumer_default_ack_wait: Duration::from_secs(DEFAULT_ACK_WAIT_SECS.into()),
            reconcile_interval: Duration::from_secs(DEFAULT_RECONCILE_INTERVAL_SECS),
            catch_up: CatchUpLimits::default(),
        }
    }
}

fn ack_wait(name: &str, secs: u32) -> Result<Duration> {
    if ConsumerSpec::ACK_WAIT_SECS.contains(&secs) {
        Ok(Duration::from_secs(secs.into()))
    } else {
        Err(eyre!(
            "{name} must be between {} and {} seconds, got {secs}",
            ConsumerSpec::ACK_WAIT_SECS.start(),
            ConsumerSpec::ACK_WAIT_SECS.end()
        ))
    }
}

fn interval(name: &str, secs: u64) -> Result<Duration> {
    if secs > 0 {
        Ok(Duration::from_secs(secs))
    } else {
        Err(eyre!("{name} must be at least 1 second"))
    }
}

fn catch_up_cap(name: &str, cap: usize) -> Result<usize> {
    if cap > 0 {
        Ok(cap)
    } else {
        Err(eyre!(
            "{name} must be at least 1; a schedule that catches nothing up declares catch_up: none"
        ))
    }
}

impl EventingConfig {
    pub fn from_env() -> Result<Self> {
        const ACK_WAIT: &str = "CONSUMER_DEFAULT_ACK_WAIT_SECS";
        const INTERVAL: &str = "CONSUMER_RECONCILE_INTERVAL_SECS";
        const GRACE: &str = "SCHEDULE_ON_TIME_GRACE_SECS";
        const CAP: &str = "SCHEDULE_CATCH_UP_CAP";
        let catch_up = CatchUpLimits::default();
        Ok(Self {
            catch_up: CatchUpLimits {
                grace: env_secs(GRACE, catch_up.grace.as_secs())?,
                cap: catch_up_cap(CAP, env_var(CAP, catch_up.cap)?)?,
            },
            consumer_default_ack_wait: ack_wait(
                ACK_WAIT,
                env_var(ACK_WAIT, DEFAULT_ACK_WAIT_SECS)?,
            )?,
            reconcile_interval: interval(
                INTERVAL,
                env_var(INTERVAL, DEFAULT_RECONCILE_INTERVAL_SECS)?,
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_durable_waits_thirty_seconds_unless_told() {
        assert_eq!(
            EventingConfig::default().consumer_default_ack_wait,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn replicas_reconcile_every_minute_unless_told() {
        assert_eq!(
            EventingConfig::default().reconcile_interval,
            Duration::from_secs(60)
        );
        assert_eq!(interval("X", 5).ok(), Some(Duration::from_secs(5)));
        let error = interval("CONSUMER_RECONCILE_INTERVAL_SECS", 0)
            .expect_err("refused")
            .to_string();
        assert!(
            error.contains("CONSUMER_RECONCILE_INTERVAL_SECS"),
            "{error}"
        );
    }

    #[test]
    fn a_fire_is_on_time_for_a_minute_and_ten_missed_are_caught_up_unless_told() {
        assert_eq!(
            EventingConfig::default().catch_up,
            CatchUpLimits {
                grace: Duration::from_secs(60),
                cap: 10,
            }
        );
        assert_eq!(catch_up_cap("X", 3).ok(), Some(3));
        let error = catch_up_cap("SCHEDULE_CATCH_UP_CAP", 0)
            .expect_err("refused")
            .to_string();
        assert!(error.contains("SCHEDULE_CATCH_UP_CAP"), "{error}");
    }

    #[test]
    fn an_ack_wait_no_manifest_could_declare_is_refused_by_name() {
        assert_eq!(ack_wait("X", 5).ok(), Some(Duration::from_secs(5)));
        assert_eq!(ack_wait("X", 3600).ok(), Some(Duration::from_secs(3600)));
        for refused in [0, 4, 3601] {
            let error = ack_wait("CONSUMER_DEFAULT_ACK_WAIT_SECS", refused)
                .expect_err("refused")
                .to_string();
            assert!(error.contains("CONSUMER_DEFAULT_ACK_WAIT_SECS"), "{error}");
        }
    }
}
