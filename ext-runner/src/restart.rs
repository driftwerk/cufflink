use crate::deployment::Deployment;
use cufflink_types::backoff::Backoff;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const QUICK_EXIT: Duration = Duration::from_secs(10);
const CRASH_LOOP_FAILURES: u32 = 3;
const STABLE_UPTIME: Duration = Duration::from_secs(60);

fn backoff() -> Backoff {
    Backoff::exponential(Duration::from_secs(1), Duration::from_secs(30))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    Exited { uptime: Duration },
    Unready,
    Refused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wait {
    Until(Instant),
    CrashLooping,
    Refused,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    pub deployment: Arc<Deployment>,
    quick: u32,
    failures: u32,
    pub wait: Wait,
}

impl Hold {
    pub fn after(
        previous: Option<&Hold>,
        deployment: Arc<Deployment>,
        failure: Failure,
        now: Instant,
    ) -> Self {
        let (quick, failures) = previous
            .filter(|hold| hold.deployment == deployment)
            .map_or((0, 0), |hold| (hold.quick, hold.failures));
        let (quick, failures) = match failure {
            Failure::Exited { uptime } => (
                if uptime < QUICK_EXIT { quick + 1 } else { 0 },
                if uptime < STABLE_UPTIME {
                    failures + 1
                } else {
                    1
                },
            ),
            Failure::Unready | Failure::Refused => (quick + 1, failures + 1),
        };
        let wait = match (failure, quick >= CRASH_LOOP_FAILURES) {
            (Failure::Refused, _) => Wait::Refused,
            (Failure::Exited { .. } | Failure::Unready, true) => Wait::CrashLooping,
            (Failure::Exited { .. } | Failure::Unready, false) => {
                Wait::Until(now + backoff().delay(failures, deployment.service_id.as_u64_pair().1))
            }
        };
        Self {
            deployment,
            quick,
            failures,
            wait,
        }
    }

    pub fn holds(&self, deployment: &Deployment, now: Instant) -> bool {
        *self.deployment == *deployment
            && match self.wait {
                Wait::Until(at) => now < at,
                Wait::CrashLooping | Wait::Refused => true,
            }
    }

    pub fn ends_after(&self, now: Instant) -> Option<Instant> {
        match self.wait {
            Wait::Until(at) if at > now => Some(at),
            Wait::Until(_) | Wait::CrashLooping | Wait::Refused => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use uuid::Uuid;

    pub(crate) fn deployment(version: i32) -> Arc<Deployment> {
        Arc::new(Deployment {
            service_id: Uuid::from_u128(7),
            tenant: "acme".into(),
            service: "ext-pdf".into(),
            version,
            sha256: format!("{version:064x}"),
            size: 1,
            extensions: ["pdf.render".to_string()].into(),
        })
    }

    fn waited(hold: &Hold, now: Instant) -> Option<Duration> {
        match hold.wait {
            Wait::Until(at) => Some(at - now),
            Wait::CrashLooping | Wait::Refused => None,
        }
    }

    fn within(delay: Option<Duration>, nominal: Duration) -> bool {
        delay.is_some_and(|delay| {
            delay >= (nominal * 4 / 5).max(Duration::from_secs(1)) && delay <= nominal * 6 / 5
        })
    }

    #[test]
    fn each_failure_in_a_row_doubles_the_wait_up_to_thirty_seconds() {
        let now = Instant::now();
        let long = Failure::Exited {
            uptime: Duration::from_secs(20),
        };
        let holds = std::iter::successors(
            Some(Hold::after(None, deployment(1), long, now)),
            |previous| Some(Hold::after(Some(previous), deployment(1), long, now)),
        )
        .take(7)
        .collect::<Vec<_>>();
        for (hold, nominal) in holds.iter().zip([1, 2, 4, 8, 16, 30, 30]) {
            assert!(
                within(waited(hold, now), Duration::from_secs(nominal)),
                "{:?} for {nominal}s",
                hold.wait
            );
        }
        let stable = Hold::after(
            holds.last(),
            deployment(1),
            Failure::Exited {
                uptime: STABLE_UPTIME,
            },
            now,
        );
        assert!(
            within(waited(&stable, now), Duration::from_secs(1)),
            "a child that ran stably starts the backoff afresh"
        );
    }

    #[test]
    fn three_quick_failures_in_a_row_are_a_crash_loop_until_the_next_deploy() {
        let now = Instant::now();
        let quick = Failure::Exited {
            uptime: Duration::from_secs(2),
        };
        let first = Hold::after(None, deployment(1), quick, now);
        let second = Hold::after(Some(&first), deployment(1), Failure::Unready, now);
        assert!(matches!(second.wait, Wait::Until(_)));
        let third = Hold::after(Some(&second), deployment(1), quick, now);
        assert_eq!(third.wait, Wait::CrashLooping);
        assert!(third.holds(&deployment(1), now + Duration::from_secs(3600)));
        assert!(
            !third.holds(&deployment(2), now),
            "a new deploy is not held"
        );
        assert!(matches!(
            Hold::after(Some(&third), deployment(2), quick, now).wait,
            Wait::Until(_)
        ));

        let reset = Hold::after(
            Some(&second),
            deployment(1),
            Failure::Exited { uptime: QUICK_EXIT },
            now,
        );
        let after_reset = Hold::after(Some(&reset), deployment(1), quick, now);
        let again = Hold::after(Some(&after_reset), deployment(1), quick, now);
        assert!(
            matches!(again.wait, Wait::Until(_)),
            "a child that ran past the quick window breaks the run"
        );
    }

    #[test]
    fn a_refused_readiness_holds_until_the_next_deploy() {
        let now = Instant::now();
        let refused = Hold::after(None, deployment(1), Failure::Refused, now);
        assert_eq!(refused.wait, Wait::Refused);
        assert!(refused.holds(&deployment(1), now + Duration::from_secs(3600)));
        assert!(!refused.holds(&deployment(2), now));
        assert_eq!(refused.ends_after(now), None);
    }

    #[test]
    fn a_backoff_holds_until_it_ends() {
        let now = Instant::now();
        let hold = Hold::after(None, deployment(1), Failure::Unready, now);
        let end = hold.ends_after(now).expect("ahead");
        assert!(hold.holds(&deployment(1), end - Duration::from_millis(1)));
        assert!(!hold.holds(&deployment(1), end));
        assert_eq!(hold.ends_after(end), None, "an ended hold wakes nobody");
    }
}
