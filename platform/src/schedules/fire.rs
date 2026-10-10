use chrono::{DateTime, Utc};
use cufflink_types::schedule::CompiledSpec;
use cufflink_types::CatchUp;
use std::time::Duration;

pub const HORIZON: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fire {
    pub at: DateTime<Utc>,
    pub catch_up: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatchUpLimits {
    pub grace: Duration,
    pub cap: usize,
}

impl Default for CatchUpLimits {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(60),
            cap: 10,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Due {
    pub fires: Vec<Fire>,
    pub through: Option<DateTime<Utc>>,
}

pub fn due_fires(
    spec: &CompiledSpec,
    last_fired: DateTime<Utc>,
    now: DateTime<Utc>,
    catch_up: CatchUp,
    limits: CatchUpLimits,
) -> Due {
    let from = chrono::Duration::from_std(HORIZON)
        .ok()
        .and_then(|horizon| now.checked_sub_signed(horizon))
        .map_or(last_fired, |horizon| last_fired.max(horizon));
    let missed = |at: &DateTime<Utc>| (now - *at).to_std().is_ok_and(|late| late > limits.grace);
    let fire = |at: DateTime<Utc>| Fire {
        catch_up: missed(&at),
        at,
    };
    let due = || spec.fires_after(from).take_while(|at| *at <= now);
    let newest = due().last();
    let fires = match catch_up {
        CatchUp::Latest => newest.map(fire).into_iter().collect(),
        CatchUp::None => due().filter(|at| !missed(at)).map(fire).collect(),
        CatchUp::All => due()
            .filter(missed)
            .take(limits.cap)
            .chain(due().filter(|at| !missed(at)))
            .map(fire)
            .collect(),
    };
    Due {
        fires,
        through: newest.or((from > last_fired).then_some(from)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedules::tests::at;
    use cufflink_types::schedule::compile;
    use cufflink_types::{ScheduleDefinition, ScheduleSpec};

    fn every(secs: u32) -> CompiledSpec {
        compile(
            "schedule",
            &ScheduleDefinition::new("s", "h", ScheduleSpec::Every { secs }),
        )
        .expect("compiles")
    }

    fn hourly() -> CompiledSpec {
        compile(
            "schedule",
            &ScheduleDefinition::new(
                "s",
                "h",
                ScheduleSpec::Cron {
                    expr: "0 * * * *".into(),
                },
            ),
        )
        .expect("compiles")
    }

    fn limits(grace_secs: u64, cap: usize) -> CatchUpLimits {
        CatchUpLimits {
            grace: Duration::from_secs(grace_secs),
            cap,
        }
    }

    fn times(due: &Due) -> Vec<(String, bool)> {
        due.fires
            .iter()
            .map(|fire| (fire.at.format("%H:%M:%S").to_string(), fire.catch_up))
            .collect()
    }

    fn fired(times: &[(&str, bool)]) -> Vec<(String, bool)> {
        times
            .iter()
            .map(|(time, catch_up)| (time.to_string(), *catch_up))
            .collect()
    }

    #[test]
    fn a_fire_on_time_is_published_whatever_is_caught_up() {
        for catch_up in [CatchUp::Latest, CatchUp::None, CatchUp::All] {
            let due = due_fires(
                &every(30),
                at("2026-09-28T10:00:00Z"),
                at("2026-09-28T10:00:30Z"),
                catch_up,
                CatchUpLimits::default(),
            );
            assert_eq!(times(&due), fired(&[("10:00:30", false)]), "{catch_up:?}");
            assert_eq!(due.through, Some(at("2026-09-28T10:00:30Z")));
        }
    }

    #[test]
    fn nothing_is_due_before_the_next_fire() {
        for catch_up in [CatchUp::Latest, CatchUp::None, CatchUp::All] {
            assert_eq!(
                due_fires(
                    &every(30),
                    at("2026-09-28T10:00:30Z"),
                    at("2026-09-28T10:00:59Z"),
                    catch_up,
                    CatchUpLimits::default(),
                ),
                Due::default(),
                "{catch_up:?}"
            );
        }
        assert_eq!(
            due_fires(
                &every(30),
                at("2026-09-28T11:00:00Z"),
                at("2026-09-28T10:00:00Z"),
                CatchUp::All,
                CatchUpLimits::default(),
            ),
            Due::default(),
            "a timetable read to an instant that is yet to come"
        );
    }

    #[test]
    fn after_an_outage_latest_publishes_the_newest_alone() {
        let due = due_fires(
            &every(600),
            at("2026-09-28T10:00:00Z"),
            at("2026-09-28T10:35:00Z"),
            CatchUp::Latest,
            limits(60, 10),
        );
        assert_eq!(times(&due), fired(&[("10:30:00", true)]));
        assert_eq!(due.through, Some(at("2026-09-28T10:30:00Z")));

        let on_time = due_fires(
            &every(600),
            at("2026-09-28T10:00:00Z"),
            at("2026-09-28T10:30:20Z"),
            CatchUp::Latest,
            limits(60, 10),
        );
        assert_eq!(times(&on_time), fired(&[("10:30:00", false)]));
    }

    #[test]
    fn after_an_outage_none_publishes_what_is_on_time_alone() {
        let missed = due_fires(
            &every(600),
            at("2026-09-28T10:00:00Z"),
            at("2026-09-28T10:35:00Z"),
            CatchUp::None,
            limits(60, 10),
        );
        assert_eq!(missed.fires, []);
        assert_eq!(
            missed.through,
            Some(at("2026-09-28T10:30:00Z")),
            "what was passed over is not due again"
        );

        let due = due_fires(
            &every(10),
            at("2026-09-28T10:00:00Z"),
            at("2026-09-28T10:00:45Z"),
            CatchUp::None,
            limits(25, 10),
        );
        assert_eq!(
            times(&due),
            fired(&[
                ("10:00:20", false),
                ("10:00:30", false),
                ("10:00:40", false)
            ])
        );
    }

    #[test]
    fn after_an_outage_all_publishes_the_oldest_missed_up_to_the_cap() {
        let three = due_fires(
            &every(600),
            at("2026-09-28T10:00:00Z"),
            at("2026-09-28T10:35:00Z"),
            CatchUp::All,
            limits(60, 10),
        );
        assert_eq!(
            times(&three),
            fired(&[("10:10:00", true), ("10:20:00", true), ("10:30:00", true)])
        );

        let capped = due_fires(
            &every(600),
            at("2026-09-28T10:00:00Z"),
            at("2026-09-28T11:00:30Z"),
            CatchUp::All,
            limits(60, 2),
        );
        assert_eq!(
            times(&capped),
            fired(&[("10:10:00", true), ("10:20:00", true), ("11:00:00", false)]),
            "the cap is of what is caught up, not of what is on time"
        );
        assert_eq!(capped.through, Some(at("2026-09-28T11:00:00Z")));
    }

    #[test]
    fn a_fire_is_late_once_the_grace_has_passed() {
        let published_at = |now: &str| {
            due_fires(
                &hourly(),
                at("2026-09-28T09:00:00Z"),
                at(now),
                CatchUp::Latest,
                limits(60, 10),
            )
        };
        assert_eq!(
            times(&published_at("2026-09-28T10:01:00Z")),
            fired(&[("10:00:00", false)])
        );
        assert_eq!(
            times(&published_at("2026-09-28T10:01:01Z")),
            fired(&[("10:00:00", true)])
        );
    }

    #[test]
    fn a_timetable_is_read_no_further_back_than_its_stream_keeps() {
        let due = due_fires(
            &hourly(),
            at("2026-09-01T00:00:00Z"),
            at("2026-09-28T10:30:00Z"),
            CatchUp::All,
            limits(60, 100),
        );
        assert_eq!(due.fires.len(), 24);
        assert_eq!(
            due.fires.first().map(|fire| fire.at),
            Some(at("2026-09-27T11:00:00Z"))
        );
        assert_eq!(due.through, Some(at("2026-09-28T10:00:00Z")));
    }

    #[test]
    fn what_was_due_longer_ago_than_a_timetable_is_read_is_not_due_again() {
        let weekly = compile(
            "schedule",
            &ScheduleDefinition::new(
                "s",
                "h",
                ScheduleSpec::Cron {
                    expr: "0 9 * * 1".into(),
                },
            ),
        )
        .expect("compiles");
        let (last_fired, now) = (at("2026-09-27T12:00:00Z"), at("2026-09-30T12:00:00Z"));
        assert_eq!(
            weekly.next_fire(last_fired),
            Some(at("2026-09-28T09:00:00Z")),
            "due, two days ago"
        );
        let due = due_fires(&weekly, last_fired, now, CatchUp::All, limits(60, 10));
        assert_eq!(due.fires, []);
        assert_eq!(due.through, Some(at("2026-09-29T12:00:00Z")));
        assert!(
            due.through
                .and_then(|read_to| weekly.next_fire(read_to))
                .is_some_and(|next| next > now),
            "and read from there, nothing is due"
        );
    }

    #[test]
    fn replicas_whose_clocks_differ_name_the_same_instants() {
        let (spec, last) = (every(30), at("2026-09-28T10:00:00Z"));
        let fires = |now: &str| due_fires(&spec, last, at(now), CatchUp::All, limits(60, 10)).fires;
        assert_eq!(fires("2026-09-28T10:01:00Z"), fires("2026-09-28T10:01:07Z"));
    }
}
