//! When a schedule fires. Every replica, the macros and the CLI compute the
//! same instants from the same declaration.

use crate::manifest::validate::validate_schedule;
use crate::{ScheduleDefinition, ScheduleSpec};
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;

/// The fires a cron expression is checked over for their spacing.
const SPACING_SAMPLE: usize = 20;

/// A schedule's timetable, ready to evaluate.
#[derive(Debug, Clone)]
pub enum CompiledSpec {
    Cron { cron: Box<Cron>, timezone: Tz },
    Every { secs: u32 },
}

/// The timetable of a valid schedule; the error names the field it
/// refuses.
pub fn compile(path: &str, schedule: &ScheduleDefinition) -> Result<CompiledSpec, String> {
    validate_schedule(path, schedule)?;
    match &schedule.spec {
        ScheduleSpec::Every { secs } => Ok(CompiledSpec::Every { secs: *secs }),
        ScheduleSpec::Cron { expr } => {
            let timezone = schedule.timezone.as_deref().map_or(Ok(Tz::UTC), |zone| {
                zone.parse::<Tz>()
                    .map_err(|_| format!("{path}.timezone: '{zone}' is not an IANA time zone name"))
            })?;
            let cron = Cron::new(expr)
                .with_seconds_optional()
                .parse()
                .map_err(|e| format!("{path}.spec.expr: '{expr}' is not a cron expression: {e}"))?;
            let compiled = CompiledSpec::Cron {
                cron: Box::new(cron),
                timezone,
            };
            match compiled.closest_fires(spacing_reference()) {
                Some(gap) if gap < i64::from(ScheduleDefinition::MIN_INTERVAL_SECS) => {
                    Err(format!(
                        "{path}.spec.expr: '{expr}' fires {gap} s apart, closer than the {} second minimum",
                        ScheduleDefinition::MIN_INTERVAL_SECS
                    ))
                }
                Some(_) => Ok(compiled),
                None => Err(format!("{path}.spec.expr: '{expr}' never fires")),
            }
        }
    }
}

/// A fixed instant, so a schedule is accepted or refused identically
/// wherever and whenever it is compiled.
fn spacing_reference() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH + chrono::Duration::days(20_000)
}

impl CompiledSpec {
    /// The first fire strictly after `after`; `None` when the timetable has
    /// none left.
    pub fn next_fire(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            CompiledSpec::Every { secs } => {
                let secs = i64::from(*secs).max(1);
                after
                    .timestamp()
                    .div_euclid(secs)
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(secs))
                    .and_then(|at| Utc.timestamp_opt(at, 0).single())
            }
            CompiledSpec::Cron { cron, timezone } => {
                next_cron_fire(cron, after.with_timezone(timezone), after)
            }
        }
    }

    /// The fires after `after`, in order.
    pub fn fires_after(&self, after: DateTime<Utc>) -> impl Iterator<Item = DateTime<Utc>> + '_ {
        std::iter::successors(self.next_fire(after), |fire| self.next_fire(*fire))
    }

    /// The smallest gap, in seconds, between consecutive fires among the
    /// first few after `from`; `None` when there is no fire at all.
    fn closest_fires(&self, from: DateTime<Utc>) -> Option<i64> {
        let fires: Vec<DateTime<Utc>> = self.fires_after(from).take(SPACING_SAMPLE).collect();
        fires.first()?;
        Some(
            fires
                .windows(2)
                .map(|pair| (pair[1] - pair[0]).num_seconds())
                .min()
                .unwrap_or(i64::MAX),
        )
    }
}

/// When local time is set back, the wall-clock search can land on the
/// first of an hour's two passes, which is not after `after`: the search
/// then continues from there, so a repeated hour fires once.
fn next_cron_fire(cron: &Cron, from: DateTime<Tz>, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let found = cron.find_next_occurrence(&from, false).ok()?;
    if found.with_timezone(&Utc) > after {
        Some(found.with_timezone(&Utc))
    } else if found.naive_local() > from.naive_local() {
        next_cron_fire(cron, found, after)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CatchUp, Overlap};

    fn schedule(spec: ScheduleSpec, timezone: Option<&str>) -> ScheduleDefinition {
        ScheduleDefinition {
            name: "s".into(),
            handler: "h".into(),
            spec,
            timezone: timezone.map(str::to_string),
            overlap: Overlap::Skip,
            catch_up: CatchUp::Latest,
            job: None,
            timeout_secs: None,
        }
    }

    fn cron(expr: &str, timezone: Option<&str>) -> Result<CompiledSpec, String> {
        compile(
            "schedule",
            &schedule(ScheduleSpec::Cron { expr: expr.into() }, timezone),
        )
    }

    fn every(secs: u32) -> CompiledSpec {
        compile("schedule", &schedule(ScheduleSpec::Every { secs }, None)).expect("compiles")
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("timestamp")
            .with_timezone(&Utc)
    }

    fn fires(spec: &CompiledSpec, after: &str, count: usize) -> Vec<String> {
        spec.fires_after(at(after))
            .take(count)
            .map(|fire| fire.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .collect()
    }

    #[test]
    fn every_fires_at_epoch_multiples_whoever_computes_it() {
        let spec = every(30);
        assert_eq!(
            fires(&spec, "2026-09-28T10:00:07Z", 3),
            [
                "2026-09-28T10:00:30Z",
                "2026-09-28T10:01:00Z",
                "2026-09-28T10:01:30Z"
            ]
        );
        for replica_clock in ["2026-09-28T10:00:00Z", "2026-09-28T10:00:29Z"] {
            assert_eq!(
                spec.next_fire(at(replica_clock)),
                Some(at("2026-09-28T10:00:30Z")),
                "{replica_clock}"
            );
        }
        assert_eq!(
            every(300).next_fire(at("2026-09-28T10:00:00Z")),
            Some(at("2026-09-28T10:05:00Z"))
        );
        assert_eq!(
            every(3600).next_fire(at("1969-12-31T23:30:00Z")),
            Some(at("1970-01-01T00:00:00Z"))
        );
    }

    #[test]
    fn a_fire_is_strictly_after_the_instant_asked() {
        let hourly = cron("0 * * * *", None).expect("compiles");
        assert_eq!(
            hourly.next_fire(at("2026-09-28T10:00:00Z")),
            Some(at("2026-09-28T11:00:00Z"))
        );
        assert_eq!(
            hourly.next_fire(at("2026-09-28T09:59:59Z")),
            Some(at("2026-09-28T10:00:00Z"))
        );
    }

    #[test]
    fn cron_takes_five_fields_or_six_with_seconds() {
        let minutes = cron("*/15 * * * *", None).expect("compiles");
        assert_eq!(
            fires(&minutes, "2026-09-28T10:01:00Z", 2),
            ["2026-09-28T10:15:00Z", "2026-09-28T10:30:00Z"]
        );
        let seconds = cron("*/20 * * * * *", None).expect("compiles");
        assert_eq!(
            fires(&seconds, "2026-09-28T10:00:00Z", 2),
            ["2026-09-28T10:00:20Z", "2026-09-28T10:00:40Z"]
        );
    }

    #[test]
    fn cron_is_read_in_its_time_zone() {
        let nine = cron("0 9 * * *", Some("Europe/London")).expect("compiles");
        assert_eq!(
            fires(&nine, "2026-07-01T00:00:00Z", 1),
            ["2026-07-01T08:00:00Z"]
        );
        assert_eq!(
            fires(&nine, "2026-12-01T00:00:00Z", 1),
            ["2026-12-01T09:00:00Z"]
        );
    }

    #[test]
    fn a_time_the_clocks_skip_fires_when_they_resume() {
        let daily = cron("30 1 * * *", Some("Europe/London")).expect("compiles");
        assert_eq!(
            fires(&daily, "2026-03-28T12:00:00Z", 3),
            [
                "2026-03-29T01:00:00Z",
                "2026-03-30T00:30:00Z",
                "2026-03-31T00:30:00Z"
            ]
        );
    }

    #[test]
    fn a_time_the_clocks_repeat_fires_once() {
        let daily = cron("30 1 * * *", Some("Europe/London")).expect("compiles");
        assert_eq!(
            fires(&daily, "2026-10-24T12:00:00Z", 3),
            [
                "2026-10-25T00:30:00Z",
                "2026-10-26T01:30:00Z",
                "2026-10-27T01:30:00Z"
            ]
        );
        assert_eq!(
            daily.next_fire(at("2026-10-25T01:15:00Z")),
            Some(at("2026-10-26T01:30:00Z")),
            "asked during the hour's second pass"
        );
        let hourly = cron("30 * * * *", Some("Europe/London")).expect("compiles");
        assert_eq!(
            fires(&hourly, "2026-10-24T23:00:00Z", 3),
            [
                "2026-10-24T23:30:00Z",
                "2026-10-25T00:30:00Z",
                "2026-10-25T02:30:00Z"
            ]
        );
    }

    #[test]
    fn a_schedule_is_refused_with_the_field_it_breaks() {
        for (result, path, reason) in [
            (
                cron("* * * * * *", None),
                "schedule.spec.expr",
                "fires 1 s apart",
            ),
            (
                cron("*/5 * * * * *", None),
                "schedule.spec.expr",
                "fires 5 s apart",
            ),
            (
                cron("0,9 * * * * *", None),
                "schedule.spec.expr",
                "fires 9 s apart",
            ),
            (
                cron("61 * * * *", None),
                "schedule.spec.expr",
                "is not a cron expression",
            ),
            (
                cron("not a cron at all", None),
                "schedule.spec.expr",
                "is not a cron expression",
            ),
            (cron("0 * *", None), "schedule.spec.expr", "five fields"),
            (
                cron("0 0 31 2 *", None),
                "schedule.spec.expr",
                "never fires",
            ),
            (
                cron("0 * * * *", Some("Mars/Olympus")),
                "schedule.timezone",
                "is not an IANA time zone name",
            ),
            (
                compile("schedule", &schedule(ScheduleSpec::Every { secs: 5 }, None)),
                "schedule.spec.secs",
                "below the 10 second minimum",
            ),
        ] {
            let error = result.expect_err(reason);
            assert!(error.starts_with(path), "'{error}' does not name {path}");
            assert!(error.contains(reason), "'{error}' lacks '{reason}'");
        }
    }

    #[test]
    fn ten_seconds_apart_is_allowed() {
        assert!(cron("*/10 * * * * *", None).is_ok());
        assert!(cron("* * * * *", None).is_ok());
        assert!(matches!(every(10), CompiledSpec::Every { secs: 10 }));
    }
}
