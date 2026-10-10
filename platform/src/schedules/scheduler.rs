use super::fire::{due_fires, CatchUpLimits, Fire};
use crate::nats_conn::{jetstream_publish_bounded, Backoff};
use crate::registry::{RegisteredService, ServiceIdentity};
use crate::repo::schedule_state::{self, LastFired};
use crate::AppState;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::HeaderMap;
use chrono::{DateTime, SecondsFormat, Utc};
use cufflink_types::host::request::ScheduleFire;
use cufflink_types::schedule::{compile, CompiledSpec};
use cufflink_types::CatchUp;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

const LONGEST_SLEEP: Duration = Duration::from_secs(30);
const PUBLISH_CONCURRENCY: usize = 16;

#[derive(Debug, Clone)]
struct Timetable {
    identity: ServiceIdentity,
    name: String,
    spec: CompiledSpec,
    catch_up: CatchUp,
    last_fired: DateTime<Utc>,
}

impl Timetable {
    fn key(&self) -> (Uuid, String) {
        (self.identity.service_id, self.name.clone())
    }

    fn next_fire(&self) -> Option<DateTime<Utc>> {
        self.spec.next_fire(self.last_fired)
    }
}

fn read_to(timetables: &[Timetable]) -> LastFired {
    timetables
        .iter()
        .map(|timetable| (timetable.key(), timetable.last_fired))
        .collect()
}

fn timetables(
    services: &[Arc<RegisteredService>],
    known: &LastFired,
    recorded: &LastFired,
    now: DateTime<Utc>,
) -> Vec<Timetable> {
    services
        .iter()
        .filter(|service| service.manifest.mode.runs_on_platform())
        .flat_map(|service| {
            let identity = service.identity();
            service
                .manifest
                .schedules
                .iter()
                .enumerate()
                .filter_map(move |(i, schedule)| {
                    compile(&format!("schedules[{i}]"), schedule)
                        .inspect_err(|e| {
                            warn!(
                                tenant = %identity.tenant_slug,
                                service = %identity.service_name,
                                "A schedule that was deployed cannot be read, and does not fire: {e}"
                            )
                        })
                        .ok()
                        .map(|spec| (identity.clone(), schedule, spec))
                })
        })
        .map(|(identity, schedule, spec)| {
            let key = (identity.service_id, schedule.name.clone());
            let last_fired = known
                .get(&key)
                .into_iter()
                .chain(recorded.get(&key))
                .max()
                .copied()
                .unwrap_or(now);
            Timetable {
                identity,
                name: schedule.name.clone(),
                spec,
                catch_up: schedule.catch_up,
                last_fired,
            }
        })
        .collect()
}

fn until_next(timetables: &[Timetable], now: DateTime<Utc>) -> Duration {
    timetables
        .iter()
        .filter_map(Timetable::next_fire)
        .min()
        .map_or(LONGEST_SLEEP, |next| {
            (next - now)
                .to_std()
                .unwrap_or(Duration::ZERO)
                .min(LONGEST_SLEEP)
        })
}

fn is_due(timetable: &Timetable, now: DateTime<Utc>) -> bool {
    timetable.next_fire().is_some_and(|next| next <= now)
}

fn message(timetable: &Timetable, fire: Fire) -> serde_json::Result<(HeaderMap, bytes::Bytes)> {
    let mut headers = HeaderMap::new();
    headers.insert(
        NATS_MESSAGE_ID,
        super::fire_id(&timetable.identity, &timetable.name, fire.at).as_str(),
    );
    let payload = serde_json::to_vec(&ScheduleFire {
        schedule: timetable.name.clone(),
        fire_at: fire.at.to_rfc3339_opts(SecondsFormat::Secs, true),
        catch_up: fire.catch_up,
    })?;
    Ok((headers, payload.into()))
}

async fn publish(state: &AppState, timetable: &Timetable, fire: Fire) -> eyre::Result<()> {
    let (headers, payload) = message(timetable, fire)?;
    jetstream_publish_bounded(
        &state.jetstream,
        super::subject(&timetable.identity, &timetable.name),
        Some(headers),
        payload,
    )
    .await?;
    metrics::counter!(
        "cufflink_schedule_fires_published_total",
        "tenant" => timetable.identity.tenant_slug.clone(),
        "service" => timetable.identity.service_name.clone(),
        "schedule" => timetable.name.clone(),
        "catch_up" => if fire.catch_up { "true" } else { "false" },
    )
    .increment(1);
    if fire.catch_up {
        info!(
            event = "schedule_catch_up",
            tenant = %timetable.identity.tenant_slug,
            service = %timetable.identity.service_name,
            schedule = %timetable.name,
            fire_at = %fire.at,
            "A fire that was missed is caught up"
        );
    }
    Ok(())
}

struct Read {
    key: (Uuid, String),
    to: Option<DateTime<Utc>>,
    failed: bool,
}

async fn read(
    state: &AppState,
    timetable: &Timetable,
    now: DateTime<Utc>,
    limits: CatchUpLimits,
) -> Read {
    let due = due_fires(
        &timetable.spec,
        timetable.last_fired,
        now,
        timetable.catch_up,
        limits,
    );
    let mut published = None;
    for fire in &due.fires {
        if let Err(e) = publish(state, timetable, *fire).await {
            warn!(
                tenant = %timetable.identity.tenant_slug,
                service = %timetable.identity.service_name,
                schedule = %timetable.name,
                fire_at = %fire.at,
                error = %e,
                "A fire could not be published; it is published when the stream takes it"
            );
            return Read {
                key: timetable.key(),
                to: published,
                failed: true,
            };
        }
        published = Some(fire.at);
    }
    Read {
        key: timetable.key(),
        to: due.through,
        failed: false,
    }
}

async fn record(state: &AppState, read_to: &LastFired) {
    let recorded = async {
        let client = state.platform_db.get().await?;
        for ((service_id, name), to) in read_to {
            schedule_state::fired(&client, *service_id, name, *to).await?;
        }
        crate::errors::PlatformResult::Ok(())
    }
    .await;
    if let Err(e) = recorded {
        warn!(
            error = %e,
            "Where the schedules' timetables were read to could not be recorded"
        );
    }
}

fn caught_up_with(timetables: Vec<Timetable>, recorded: &LastFired) -> Vec<Timetable> {
    timetables
        .into_iter()
        .map(|timetable| Timetable {
            last_fired: recorded
                .get(&timetable.key())
                .map_or(timetable.last_fired, |at| timetable.last_fired.max(*at)),
            ..timetable
        })
        .collect()
}

async fn publish_due(
    state: &Arc<AppState>,
    timetables: Vec<Timetable>,
    limits: CatchUpLimits,
) -> (Vec<Timetable>, bool) {
    let now = Utc::now();
    if !timetables.iter().any(|timetable| is_due(timetable, now)) {
        return (timetables, true);
    }
    let timetables = match recorded(state).await {
        Ok(recorded) => caught_up_with(timetables, &recorded),
        Err(e) => {
            warn!(
                error = %e,
                "Where the schedules' timetables were read to cannot be read; \
                 going by what this replica published"
            );
            timetables
        }
    };
    let due: Vec<Timetable> = timetables
        .iter()
        .filter(|timetable| is_due(timetable, now))
        .cloned()
        .collect();
    let reads: Vec<Read> = futures_util::stream::iter(due)
        .map(|timetable| {
            let state = Arc::clone(state);
            async move { read(&state, &timetable, now, limits).await }
        })
        .buffer_unordered(PUBLISH_CONCURRENCY)
        .collect()
        .await;
    let published = reads.iter().all(|read| !read.failed);
    let read_to: LastFired = reads
        .into_iter()
        .filter_map(|read| read.to.map(|to| (read.key, to)))
        .collect();
    record(state, &read_to).await;
    (caught_up_with(timetables, &read_to), published)
}

async fn recorded(state: &AppState) -> eyre::Result<LastFired> {
    Ok(schedule_state::all(&state.platform_db.get().await?).await?)
}

async fn load(state: &AppState, known: &LastFired) -> eyre::Result<Vec<Timetable>> {
    let services = state.registry.all_services().await;
    if services
        .iter()
        .all(|service| service.manifest.schedules.is_empty())
    {
        return Ok(Vec::new());
    }
    let recorded = recorded(state).await?;
    let loaded = timetables(&services, known, &recorded, Utc::now());
    let client = state.platform_db.get().await?;
    for timetable in loaded
        .iter()
        .filter(|timetable| !recorded.contains_key(&timetable.key()))
    {
        schedule_state::introduce(
            &client,
            timetable.identity.service_id,
            std::slice::from_ref(&timetable.name),
            timetable.last_fired,
        )
        .await?;
    }
    Ok(loaded)
}

pub fn start(state: Arc<AppState>) {
    state.shutdown.clone().spawn("scheduler", run(state));
}

async fn run(state: Arc<AppState>) {
    let shutdown = state.shutdown.clone();
    let mut registry = state.registry.subscribe_version();
    let limits = state.config.eventing.catch_up;
    let mut timetables: Vec<Timetable> = Vec::new();
    let mut retry = Backoff::new();
    let mut stale = true;
    loop {
        if stale {
            registry.borrow_and_update();
            match load(&state, &read_to(&timetables)).await {
                Ok(loaded) => {
                    timetables = loaded;
                    stale = false;
                }
                Err(e) => {
                    warn!(error = %format!("{e:#}"), "The schedules could not be read; retrying")
                }
            }
        }
        let (read, published) = publish_due(&state, timetables, limits).await;
        timetables = read;
        let sleep = if stale || !published {
            retry.next_delay()
        } else {
            retry.reset();
            until_next(&timetables, Utc::now())
        };
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            changed = registry.changed() => match changed {
                Ok(()) => stale = true,
                Err(_) => return,
            },
            _ = tokio::time::sleep(sleep) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedules::tests::{at, identity};
    use cufflink_types::{ScheduleDefinition, ScheduleSpec};

    fn service(name: &str, schedules: Vec<ScheduleDefinition>) -> Arc<RegisteredService> {
        let identity = identity("default", name);
        Arc::new(RegisteredService {
            service_id: identity.service_id,
            tenant_id: identity.tenant_id,
            tenant_slug: identity.tenant_slug,
            name: name.into(),
            manifest: crate::deployer::ServiceManifest {
                name: name.into(),
                schedules,
                ..Default::default()
            },
            version: 1,
            wasm_hash: None,
            commit_hash: None,
        })
    }

    fn every(name: &str, secs: u32) -> ScheduleDefinition {
        ScheduleDefinition::new(name, "run", ScheduleSpec::Every { secs })
    }

    fn timetable(name: &str, secs: u32, last_fired: &str) -> Timetable {
        Timetable {
            identity: identity("default", "order-service"),
            name: name.into(),
            spec: compile("schedule", &every(name, secs)).expect("compiles"),
            catch_up: CatchUp::Latest,
            last_fired: at(last_fired),
        }
    }

    #[test]
    fn a_schedule_nobody_knows_starts_now() {
        let now = at("2026-09-28T10:00:07Z");
        let loaded = timetables(
            &[service("order-service", vec![every("order_retry", 30)])],
            &LastFired::new(),
            &LastFired::new(),
            now,
        );
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].last_fired, now);
        assert_eq!(loaded[0].next_fire(), Some(at("2026-09-28T10:00:30Z")));
    }

    #[test]
    fn a_timetable_is_read_from_the_latest_anyone_read_it_to() {
        let services = [service("order-service", vec![every("order_retry", 30)])];
        let key = (
            identity("default", "order-service").service_id,
            "order_retry".to_string(),
        );
        let known = [timetable("order_retry", 30, "2026-09-28T10:00:30Z")];
        for (recorded, read_from) in [
            ("2026-09-28T10:01:00Z", "2026-09-28T10:01:00Z"),
            ("2026-09-28T09:00:00Z", "2026-09-28T10:00:30Z"),
        ] {
            let recorded = LastFired::from([(key.clone(), at(recorded))]);
            let loaded = timetables(
                &services,
                &read_to(&known),
                &recorded,
                at("2026-09-28T10:05:00Z"),
            );
            assert_eq!(loaded[0].last_fired, at(read_from));
            assert_eq!(
                caught_up_with(known.to_vec(), &recorded)[0].last_fired,
                at(read_from)
            );
        }
        let after_an_outage = timetables(
            &services,
            &LastFired::new(),
            &LastFired::from([(key, at("2026-09-28T09:00:00Z"))]),
            at("2026-09-28T10:05:00Z"),
        );
        assert_eq!(after_an_outage[0].last_fired, at("2026-09-28T09:00:00Z"));
    }

    #[test]
    fn a_schedule_that_was_removed_has_no_timetable() {
        let known = [timetable("order_retry", 30, "2026-09-28T10:00:30Z")];
        let loaded = timetables(
            &[service("order-service", vec![every("document_retry", 60)])],
            &read_to(&known),
            &LastFired::new(),
            at("2026-09-28T10:05:00Z"),
        );
        assert_eq!(
            loaded.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            ["document_retry"]
        );
    }

    #[test]
    fn the_scheduler_sleeps_until_the_first_fire_and_no_longer_than_it_may() {
        let timetables = [
            timetable("hourly", 3600, "2026-09-28T10:00:00Z"),
            timetable("order_retry", 30, "2026-09-28T10:00:30Z"),
        ];
        assert_eq!(
            until_next(&timetables, at("2026-09-28T10:00:48Z")),
            Duration::from_secs(12)
        );
        assert_eq!(
            until_next(&timetables, at("2026-09-28T10:01:20Z")),
            Duration::ZERO,
            "a fire that is due is published at once"
        );
        assert_eq!(
            until_next(&timetables[..1], at("2026-09-28T10:00:48Z")),
            LONGEST_SLEEP
        );
        assert_eq!(until_next(&[], at("2026-09-28T10:00:48Z")), LONGEST_SLEEP);
        assert!(is_due(&timetables[1], at("2026-09-28T10:01:00Z")));
        assert!(!is_due(&timetables[1], at("2026-09-28T10:00:59Z")));
    }

    #[test]
    fn a_fire_is_published_under_the_id_of_its_instant() {
        let (headers, payload) = message(
            &timetable("order_retry", 30, "2026-09-28T10:00:00Z"),
            Fire {
                at: at("2026-09-28T10:00:30Z"),
                catch_up: true,
            },
        )
        .expect("message");
        assert_eq!(
            headers.get(NATS_MESSAGE_ID).map(|id| id.as_str()),
            Some("sched:default:order-service:order_retry:1790589630")
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&payload).expect("json"),
            serde_json::json!({
                "schedule": "order_retry",
                "fire_at": "2026-09-28T10:00:30Z",
                "catch_up": true,
            })
        );
    }
}
