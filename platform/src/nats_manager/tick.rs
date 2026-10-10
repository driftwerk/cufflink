use crate::shutdown::Shutdown;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream;
use async_nats::Client;
use async_nats::HeaderMap;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

const STREAM_NAME: &str = "CUFFLINK_TICKS";
pub(crate) const SUBJECT_PATTERN: &str = "cufflink.tick.>";
const MAX_AGE: Duration = Duration::from_secs(60);
const DEDUPE_WINDOW: Duration = Duration::from_secs(30);
const POLL_PERIOD: Duration = Duration::from_secs(60);

pub const SUBJECT_1MIN: &str = "cufflink.tick.1min";
pub const SUBJECT_5MIN: &str = "cufflink.tick.5min";
pub const SUBJECT_1HOUR: &str = "cufflink.tick.1hour";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TickPeriod {
    OneMin,
    FiveMin,
    OneHour,
}

impl TickPeriod {
    const ALL: [TickPeriod; 3] = [Self::OneMin, Self::FiveMin, Self::OneHour];

    const fn as_secs(self) -> u64 {
        match self {
            Self::OneMin => 60,
            Self::FiveMin => 5 * 60,
            Self::OneHour => 60 * 60,
        }
    }

    const fn subject(self) -> &'static str {
        match self {
            Self::OneMin => SUBJECT_1MIN,
            Self::FiveMin => SUBJECT_5MIN,
            Self::OneHour => SUBJECT_1HOUR,
        }
    }

    const fn tag(self) -> &'static str {
        match self {
            Self::OneMin => "1min",
            Self::FiveMin => "5min",
            Self::OneHour => "1hour",
        }
    }
}

pub async fn setup_stream(js: &jetstream::Context) -> eyre::Result<()> {
    super::replicas::create_or_update_stream(
        js,
        jetstream::stream::Config {
            name: STREAM_NAME.to_string(),
            subjects: vec![SUBJECT_PATTERN.to_string()],
            retention: jetstream::stream::RetentionPolicy::Interest,
            max_age: MAX_AGE,
            duplicate_window: DEDUPE_WINDOW,
            ..Default::default()
        },
    )
    .await?;
    info!(
        "NATS stream '{}' ready (max_age={:?}, dedupe_window={:?})",
        STREAM_NAME, MAX_AGE, DEDUPE_WINDOW
    );
    Ok(())
}

pub fn start_publisher(nats: Client, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn("tick_publisher", async move {
        let start = next_boundary_after(SystemTime::now(), TickPeriod::OneMin.as_secs());
        let mut interval = tokio::time::interval_at(instant_at(start), POLL_PERIOD);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = interval.tick() => {}
            }
            let now_secs = unix_secs(SystemTime::now());
            for period in TickPeriod::ALL {
                if now_secs.is_multiple_of(period.as_secs()) {
                    publish(&nats, period, now_secs).await;
                }
            }
        }
    });
    info!("Tick publisher started (1min/5min/1hour, wall-clock aligned)");
}

async fn publish(nats: &Client, period: TickPeriod, now_secs: u64) {
    let mut headers = HeaderMap::new();
    headers.insert(NATS_MESSAGE_ID, msg_id(period, now_secs).as_str());
    let payload = json!({ "timestamp": chrono::Utc::now().to_rfc3339() })
        .to_string()
        .into_bytes();
    if let Err(e) = crate::nats_conn::publish_bounded(
        nats,
        period.subject().to_string(),
        Some(headers),
        payload.into(),
    )
    .await
    {
        warn!(error = %e, subject = period.subject(), "Failed to publish tick");
    }
}

fn msg_id(period: TickPeriod, now_secs: u64) -> String {
    let secs = period.as_secs();
    format!(
        "cufflink.tick.{}.{}",
        period.tag(),
        (now_secs / secs) * secs
    )
}

fn next_boundary_after(now: SystemTime, period_secs: u64) -> SystemTime {
    let secs = unix_secs(now);
    let next = secs - (secs % period_secs) + period_secs;
    UNIX_EPOCH + Duration::from_secs(next)
}

fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn instant_at(t: SystemTime) -> tokio::time::Instant {
    let now_sys = SystemTime::now();
    let delta = t.duration_since(now_sys).unwrap_or(Duration::ZERO);
    tokio::time::Instant::now() + delta
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_match_stream_pattern() {
        let prefix = SUBJECT_PATTERN.trim_end_matches('>');
        for period in TickPeriod::ALL {
            assert!(
                period.subject().starts_with(prefix),
                "subject {} should be covered by {SUBJECT_PATTERN}",
                period.subject()
            );
        }
    }

    #[test]
    fn period_secs_are_canonical() {
        assert_eq!(TickPeriod::OneMin.as_secs(), 60);
        assert_eq!(TickPeriod::FiveMin.as_secs(), 300);
        assert_eq!(TickPeriod::OneHour.as_secs(), 3600);
    }

    #[test]
    fn msg_id_floors_to_period_boundary() {
        assert_eq!(msg_id(TickPeriod::OneMin, 90), "cufflink.tick.1min.60");
        assert_eq!(msg_id(TickPeriod::OneMin, 59), "cufflink.tick.1min.0");
        assert_eq!(msg_id(TickPeriod::FiveMin, 901), "cufflink.tick.5min.900");
        assert_eq!(
            msg_id(TickPeriod::OneHour, 7199),
            "cufflink.tick.1hour.3600"
        );
    }

    #[test]
    fn msg_id_identical_across_calls_within_same_period() {
        let a = msg_id(TickPeriod::FiveMin, 905);
        let b = msg_id(TickPeriod::FiveMin, 1199);
        assert_eq!(a, b, "every second within a 5min period must dedupe");
    }

    #[test]
    fn msg_id_changes_across_period_boundary() {
        let a = msg_id(TickPeriod::OneMin, 59);
        let b = msg_id(TickPeriod::OneMin, 60);
        assert_ne!(a, b, "crossing a minute boundary must produce a fresh id");
    }

    #[test]
    fn next_boundary_after_strictly_greater_than_now() {
        let now = UNIX_EPOCH + Duration::from_secs(123);
        let boundary = next_boundary_after(now, 60);
        assert!(boundary > now);
    }

    #[test]
    fn next_boundary_after_aligns_to_period() {
        let now = UNIX_EPOCH + Duration::from_secs(123);
        let boundary = next_boundary_after(now, 60);
        assert_eq!(unix_secs(boundary), 180);
    }

    #[test]
    fn next_boundary_after_on_an_exact_boundary_returns_next_one() {
        let now = UNIX_EPOCH + Duration::from_secs(120);
        let boundary = next_boundary_after(now, 60);
        assert_eq!(unix_secs(boundary), 180);
    }

    #[test]
    fn cadence_alignment_at_known_boundaries() {
        let publishes = |now_secs: u64| -> Vec<&'static str> {
            TickPeriod::ALL
                .iter()
                .filter(|p| now_secs.is_multiple_of(p.as_secs()))
                .map(|p| p.subject())
                .collect()
        };
        assert_eq!(publishes(60), vec![SUBJECT_1MIN]);
        assert_eq!(publishes(300), vec![SUBJECT_1MIN, SUBJECT_5MIN]);
        assert_eq!(
            publishes(3600),
            vec![SUBJECT_1MIN, SUBJECT_5MIN, SUBJECT_1HOUR]
        );
        assert_eq!(publishes(420), vec![SUBJECT_1MIN]);
    }
}
