use super::JobConfig;
use crate::features;
use serde::{Deserialize, Serialize};

/// A handler the platform runs on a timetable, once per fire across every
/// replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleDefinition {
    pub name: String,
    pub handler: String,
    pub spec: ScheduleSpec,
    /// The IANA time zone a cron expression is read in; UTC when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Overlap::is_skip")]
    pub overlap: Overlap,
    #[serde(default, skip_serializing_if = "CatchUp::is_latest")]
    pub catch_up: CatchUp,
    /// Run each fire as a job on a worker instead of inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<JobConfig>,
    /// The inline handler's execution budget; 60 s when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u32>,
}

impl ScheduleDefinition {
    pub const NAME_MAX: usize = 64;
    pub const DEFAULT_TIMEOUT_SECS: u32 = 60;
    /// The range `timeout_secs` must fall in.
    pub const TIMEOUT_SECS: std::ops::RangeInclusive<u32> = 1..=300;
    /// The shortest interval between two fires.
    pub const MIN_INTERVAL_SECS: u32 = 10;

    /// A schedule with the platform's defaults: a handler run inline, in
    /// UTC, that skips a fire while the last one runs.
    pub fn new(name: impl Into<String>, handler: impl Into<String>, spec: ScheduleSpec) -> Self {
        Self {
            name: name.into(),
            handler: handler.into(),
            spec,
            timezone: None,
            overlap: Overlap::default(),
            catch_up: CatchUp::default(),
            job: None,
            timeout_secs: None,
        }
    }

    /// `^[a-z0-9_]{1,64}$`
    pub fn is_valid_name(name: &str) -> bool {
        (1..=Self::NAME_MAX).contains(&name.len())
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    }

    /// Platform features this schedule's declaration requires beyond
    /// `schedules` itself.
    pub fn derived_features(&self) -> impl Iterator<Item = &'static str> {
        self.job
            .as_ref()
            .is_some_and(JobConfig::uses_v2)
            .then_some(features::JOBS_V2)
            .into_iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleSpec {
    /// A cron expression of five fields, or six with seconds first.
    Cron { expr: String },
    /// Every `secs` seconds, at the Unix-epoch multiples of `secs`.
    Every { secs: u32 },
}

/// What a fire does while the previous run is still going.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overlap {
    #[default]
    Skip,
    Allow,
}

impl Overlap {
    pub fn is_skip(&self) -> bool {
        *self == Overlap::Skip
    }
}

/// Which of the fires missed during an outage still run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatchUp {
    /// The newest one.
    #[default]
    Latest,
    None,
    /// The oldest ones, up to the platform's cap.
    All,
}

impl CatchUp {
    pub fn is_latest(&self) -> bool {
        *self == CatchUp::Latest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BackoffSpec, RetryOn};
    use serde_json::json;

    fn every(secs: u32) -> ScheduleDefinition {
        ScheduleDefinition {
            name: "order_retry".into(),
            handler: "retry_orders".into(),
            spec: ScheduleSpec::Every { secs },
            timezone: None,
            overlap: Overlap::Skip,
            catch_up: CatchUp::Latest,
            job: None,
            timeout_secs: None,
        }
    }

    #[test]
    fn a_minimal_schedule_carries_only_what_it_declares() {
        let wire = json!({
            "name": "order_retry",
            "handler": "retry_orders",
            "spec": {"kind": "every", "secs": 30}
        });
        assert_eq!(serde_json::to_value(every(30)).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<ScheduleDefinition>(wire).unwrap(),
            every(30)
        );
        assert_eq!(every(30).derived_features().count(), 0);
    }

    #[test]
    fn every_field_round_trips() {
        let schedule = ScheduleDefinition {
            name: "payment_reminders".into(),
            handler: "run_payment_reminders".into(),
            spec: ScheduleSpec::Cron {
                expr: "0 * * * *".into(),
            },
            timezone: Some("Europe/London".into()),
            overlap: Overlap::Allow,
            catch_up: CatchUp::None,
            job: Some(JobConfig {
                timeout_secs: 900,
                ..Default::default()
            }),
            timeout_secs: Some(120),
        };
        let wire = json!({
            "name": "payment_reminders",
            "handler": "run_payment_reminders",
            "spec": {"kind": "cron", "expr": "0 * * * *"},
            "timezone": "Europe/London",
            "overlap": "allow",
            "catch_up": "none",
            "job": {"timeout_secs": 900, "max_attempts": 3},
            "timeout_secs": 120
        });
        assert_eq!(serde_json::to_value(&schedule).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<ScheduleDefinition>(wire).unwrap(),
            schedule
        );
        assert_eq!(serde_json::to_value(CatchUp::All).unwrap(), json!("all"));
    }

    #[test]
    fn a_retrying_job_demands_jobs_v2() {
        let plain = ScheduleDefinition {
            job: Some(JobConfig::default()),
            ..every(60)
        };
        assert_eq!(plain.derived_features().count(), 0);
        for job in [
            JobConfig {
                retry_on: vec![RetryOn::Error],
                ..Default::default()
            },
            JobConfig {
                backoff: Some(BackoffSpec::default()),
                ..Default::default()
            },
        ] {
            let schedule = ScheduleDefinition {
                job: Some(job),
                ..every(60)
            };
            assert_eq!(
                schedule.derived_features().collect::<Vec<_>>(),
                [features::JOBS_V2]
            );
        }
    }

    #[test]
    fn names_are_lowercase_identifiers() {
        for valid in ["order_retry", "a", "sync_2", &"s".repeat(64)] {
            assert!(ScheduleDefinition::is_valid_name(valid), "{valid}");
        }
        for invalid in [
            "",
            "Order",
            "order-retry",
            "order.retry",
            "a b",
            &"s".repeat(65),
        ] {
            assert!(!ScheduleDefinition::is_valid_name(invalid), "{invalid}");
        }
    }
}
