use crate::nats_manager::delivery::{decide, Decision, Outcome};
use crate::nats_manager::dlq::{dlq_subject, DeadLetter, DlqKind};
use crate::nats_manager::pump::{MessageMeta, PumpConfig, Work, WorkLabel};
use crate::nats_manager::subscriptions::{answered, delivery_meta, run_handler, Run};
use crate::registry::ServiceIdentity;
use crate::repo::wasm_jobs;
use crate::router::jobs::{enqueue_job, JobIdentity, JobRequest};
use crate::AppState;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use cufflink_telemetry::{scoped_span, Kind};
use cufflink_types::backoff::Backoff;
use cufflink_types::host::request::{GuestRequest, ScheduleFire, METHOD_SCHEDULE};
use cufflink_types::{AckMode, JobConfig, Overlap, ScheduleDefinition};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, Instrument};

pub const MAX_DELIVER: i64 = 3;
pub const OVERLAPPING: usize = 4;
const RETRY_AFTER: [Duration; 2] = [Duration::from_secs(10), Duration::from_secs(30)];

pub fn in_flight(overlap: Overlap) -> i64 {
    match overlap {
        Overlap::Skip => 1,
        Overlap::Allow => OVERLAPPING as i64,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScheduleRun {
    name: String,
    handler: String,
    overlap: Overlap,
    job: Option<JobConfig>,
    timeout: Duration,
}

impl ScheduleRun {
    pub fn of(schedule: &ScheduleDefinition) -> Self {
        Self {
            name: schedule.name.clone(),
            handler: schedule.handler.clone(),
            overlap: schedule.overlap,
            job: schedule.job.clone(),
            timeout: Duration::from_secs(
                schedule
                    .timeout_secs
                    .unwrap_or(ScheduleDefinition::DEFAULT_TIMEOUT_SECS)
                    .into(),
            ),
        }
    }

    pub(crate) fn pump(&self, ack_wait: Duration) -> PumpConfig {
        match self.overlap {
            Overlap::Skip => PumpConfig::sequential(ack_wait),
            Overlap::Allow => PumpConfig::sequential(ack_wait).concurrent(OVERLAPPING),
        }
    }
}

struct ScheduleContext {
    state: Arc<AppState>,
    identity: ServiceIdentity,
    run: ScheduleRun,
    max_deliver: i64,
    backoff: Backoff,
}

pub(crate) struct ScheduleWork {
    ctx: Arc<ScheduleContext>,
}

impl ScheduleWork {
    pub(crate) fn new(
        state: &Arc<AppState>,
        identity: &ServiceIdentity,
        run: &ScheduleRun,
        max_deliver: i64,
    ) -> Self {
        Self {
            ctx: Arc::new(ScheduleContext {
                state: Arc::clone(state),
                identity: identity.clone(),
                run: run.clone(),
                max_deliver,
                backoff: Backoff::explicit(RETRY_AFTER.to_vec()),
            }),
        }
    }
}

impl Work for ScheduleWork {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "schedule",
            subscription: None,
        }
    }

    fn handle(
        &self,
        msg: Arc<jetstream::Message>,
        meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let ctx = Arc::clone(&self.ctx);
        async move { handle_fire(&ctx, &msg, &meta).await }.boxed()
    }

    fn decide(&self, outcome: &Outcome, meta: &MessageMeta) -> Decision {
        decide(
            AckMode::OnSuccess,
            outcome,
            meta.delivered,
            self.ctx.max_deliver,
            &self.ctx.backoff,
            true,
            meta.stream_sequence,
        )
    }

    fn dead_letter<'a>(
        &'a self,
        msg: &'a jetstream::Message,
        meta: &'a MessageMeta,
        reason: &'a str,
    ) -> BoxFuture<'a, Option<DeadLetter>> {
        std::future::ready(Some(DeadLetter::now(
            dlq_subject(
                &self.ctx.identity.tenant_slug,
                &self.ctx.identity.service_name,
                DlqKind::Schedule,
                &self.ctx.run.handler,
            ),
            msg.payload.clone(),
            meta,
            reason,
        )))
        .boxed()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ran {
    Ok,
    Error,
    Coalesced,
    Enqueued,
}

impl Ran {
    fn as_str(self) -> &'static str {
        match self {
            Ran::Ok => "ok",
            Ran::Error => "error",
            Ran::Coalesced => "coalesced",
            Ran::Enqueued => "enqueued",
        }
    }

    fn of(outcome: &Outcome) -> Option<Self> {
        match outcome {
            Outcome::Ok => Some(Ran::Ok),
            Outcome::Interrupted | Outcome::Deferred { .. } => None,
            Outcome::Retry { .. } | Outcome::Terminal { .. } | Outcome::Invalid { .. } => {
                Some(Ran::Error)
            }
        }
    }
}

fn count(ctx: &ScheduleContext, ran: Ran) {
    metrics::counter!(
        "cufflink_schedule_runs_total",
        "tenant" => ctx.identity.tenant_slug.clone(),
        "service" => ctx.identity.service_name.clone(),
        "schedule" => ctx.run.name.clone(),
        "result" => ran.as_str(),
    )
    .increment(1);
}

fn fire_of(payload: &[u8]) -> Result<(ScheduleFire, DateTime<Utc>), String> {
    let fire: ScheduleFire =
        serde_json::from_slice(payload).map_err(|e| format!("the payload is not a fire: {e}"))?;
    let at = DateTime::parse_from_rfc3339(&fire.fire_at)
        .map_err(|e| format!("'{}' is not when a fire was due: {e}", fire.fire_at))?
        .with_timezone(&Utc);
    Ok((fire, at))
}

fn superseded(overlap: Overlap, fire: &ScheduleFire, meta: &MessageMeta) -> bool {
    overlap == Overlap::Skip && !fire.catch_up && meta.pending > 0
}

fn schedule_request(
    identity: &ServiceIdentity,
    handler: &str,
    fire: &ScheduleFire,
    meta: &MessageMeta,
) -> GuestRequest {
    GuestRequest {
        method: METHOD_SCHEDULE.to_string(),
        handler: handler.to_string(),
        headers: Some(serde_json::Map::new()),
        body: json!({
            "schedule": fire.schedule,
            "fire_at": fire.fire_at,
            "catch_up": fire.catch_up,
        }),
        tenant: identity.tenant_slug.clone(),
        service: identity.service_name.clone(),
        auth: Some(Value::Null),
        subscription: Some(delivery_meta(meta)),
        ..GuestRequest::default()
    }
}

fn retry(reason: String) -> Outcome {
    Outcome::Retry {
        after: None,
        reason,
    }
}

async fn run_inline(ctx: &ScheduleContext, request: &GuestRequest) -> Outcome {
    let ran = run_handler(
        &ctx.state,
        Run {
            identity: &ctx.identity,
            handler: &ctx.run.handler,
            request,
            budget: ctx.run.timeout,
            kind: Kind::Schedule,
        },
    )
    .await;
    match ran {
        Ok(response) => answered(&response, ctx.state.wasm_engine.as_ref()),
        Err(failure) => failure.outcome(),
    }
}

async fn enqueue(
    ctx: &ScheduleContext,
    job: &JobConfig,
    request: &GuestRequest,
    at: DateTime<Utc>,
) -> (Outcome, Option<Ran>) {
    let failed = |reason: String| (retry(reason), Some(Ran::Error));
    if ctx.run.overlap == Overlap::Skip {
        let unfinished = wasm_jobs::schedule_has_unfinished(
            &ctx.state.platform_db,
            ctx.identity.service_id,
            &ctx.run.name,
        )
        .await;
        match unfinished {
            Ok(true) => return (Outcome::Ok, Some(Ran::Coalesced)),
            Ok(false) => {}
            Err(e) => return failed(format!("the schedule's jobs cannot be read: {e}")),
        }
    }
    let Some(service) = ctx
        .state
        .registry
        .get_by_slug(&ctx.identity.tenant_slug, &ctx.identity.service_name)
        .await
    else {
        return failed(format!(
            "Service '{}/{}' not found in registry",
            ctx.identity.tenant_slug, ctx.identity.service_name
        ));
    };
    let request_json = match request.to_value() {
        Ok(request_json) => request_json,
        Err(e) => return failed(format!("the fire's request cannot be written: {e}")),
    };
    let key = super::job_key(&ctx.run.name, at);
    let enqueued = enqueue_job(
        &ctx.state,
        &JobIdentity {
            tenant_id: service.tenant_id,
            tenant_slug: &service.tenant_slug,
            service_id: service.service_id,
            service_slug: &service.name,
            service_version: service.version,
        },
        JobRequest {
            idempotency_key: Some(&key),
            schedule_name: Some(&ctx.run.name),
            ..JobRequest::new(&ctx.run.handler, &request_json, None, job)
        },
    )
    .await;
    match enqueued {
        Ok(job_id) => {
            info!(
                tenant = %ctx.identity.tenant_slug,
                service = %ctx.identity.service_name,
                schedule = %ctx.run.name,
                %job_id,
                "A fire's job is enqueued"
            );
            (Outcome::Ok, Some(Ran::Enqueued))
        }
        Err(e) => failed(format!("the fire's job was not enqueued: {e}")),
    }
}

fn fire_span(identity: &ServiceIdentity, run: &ScheduleRun) -> tracing::Span {
    scoped_span!(
        parent: None,
        "schedule",
        identity
            .scope(None)
            .running(Kind::Schedule, run.handler.as_str()),
        otel.name = %format!(
            "{}/{}/{}",
            identity.tenant_slug, identity.service_name, run.handler
        ),
        otel.kind = "consumer",
        otel.status_code = tracing::field::Empty,
        schedule = %run.name,
    )
}

async fn handle_fire(
    ctx: &ScheduleContext,
    msg: &jetstream::Message,
    meta: &MessageMeta,
) -> Outcome {
    let (fire, at) = match fire_of(&msg.payload) {
        Ok(fire) => fire,
        Err(reason) => {
            count(ctx, Ran::Error);
            return Outcome::Invalid { reason };
        }
    };
    if superseded(ctx.run.overlap, &fire, meta) {
        count(ctx, Ran::Coalesced);
        return Outcome::Ok;
    }
    metrics::histogram!("cufflink_schedule_lag_seconds").record(
        (Utc::now() - at)
            .to_std()
            .unwrap_or(Duration::ZERO)
            .as_secs_f64(),
    );
    let span = fire_span(&ctx.identity, &ctx.run);
    let request = schedule_request(&ctx.identity, &ctx.run.handler, &fire, meta);
    let (outcome, ran) = async {
        match &ctx.run.job {
            Some(job) => enqueue(ctx, job, &request, at).await,
            None => {
                let outcome = run_inline(ctx, &request).await;
                let ran = Ran::of(&outcome);
                (outcome, ran)
            }
        }
    }
    .instrument(span.clone())
    .await;
    if let Outcome::Retry { reason, .. } | Outcome::Terminal { reason } = &outcome {
        span.record("otel.status_code", "ERROR");
        span.in_scope(|| error!("Schedule '{}' failed: {}", ctx.run.name, reason));
    }
    if let Some(ran) = ran {
        count(ctx, ran);
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedules::tests::identity;
    use cufflink_types::ScheduleSpec;

    fn schedule() -> ScheduleDefinition {
        ScheduleDefinition::new(
            "order_retry",
            "retry_orders",
            ScheduleSpec::Every { secs: 30 },
        )
    }

    #[test]
    fn a_fire_starts_its_own_trace_under_its_pump() {
        assert!(crate::telemetry::is_a_root_under_a_pump(|| {
            fire_span(
                &identity("default", "order-service"),
                &ScheduleRun::of(&schedule()),
            )
        }));
    }

    fn delivered(pending: u64) -> MessageMeta {
        MessageMeta {
            subject: "cufflink.schedule.default.order-service.order_retry".into(),
            stream: "CUFFLINK_SCHEDULES".into(),
            consumer: "sched_default_order-service_order_retry_0a1b2c3d".into(),
            stream_sequence: 7,
            consumer_sequence: 3,
            delivered: 1,
            pending,
            published: crate::schedules::tests::at("2026-09-28T10:00:30.004Z"),
            headers: vec![("Nats-Msg-Id".into(), vec!["sched:x".into()])],
        }
    }

    #[test]
    fn a_schedule_runs_inline_for_a_minute_one_fire_at_a_time_unless_it_says() {
        let run = ScheduleRun::of(&schedule());
        assert_eq!(run.timeout, Duration::from_secs(60));
        assert_eq!(run.job, None);
        let pump = run.pump(Duration::from_secs(30));
        assert_eq!((pump.concurrency, pump.batch), (1, 1));
        assert_eq!(in_flight(Overlap::Skip), 1);

        let overlapping = ScheduleRun::of(&ScheduleDefinition {
            overlap: Overlap::Allow,
            timeout_secs: Some(120),
            ..schedule()
        });
        assert_eq!(overlapping.timeout, Duration::from_secs(120));
        let pump = overlapping.pump(Duration::from_secs(30));
        assert_eq!((pump.concurrency, pump.batch), (4, 4));
        assert_eq!(in_flight(Overlap::Allow), 4);
    }

    #[test]
    fn a_loop_is_the_same_until_what_it_does_changes() {
        let run = ScheduleRun::of(&schedule());
        assert_eq!(
            run,
            ScheduleRun::of(&ScheduleDefinition {
                spec: ScheduleSpec::Every { secs: 60 },
                catch_up: cufflink_types::CatchUp::All,
                ..schedule()
            }),
            "when it fires is the scheduler's"
        );
        for changed in [
            ScheduleDefinition {
                handler: "retry_all".into(),
                ..schedule()
            },
            ScheduleDefinition {
                overlap: Overlap::Allow,
                ..schedule()
            },
            ScheduleDefinition {
                timeout_secs: Some(30),
                ..schedule()
            },
            ScheduleDefinition {
                job: Some(JobConfig::default()),
                ..schedule()
            },
        ] {
            assert_ne!(run, ScheduleRun::of(&changed));
        }
    }

    #[test]
    fn a_fire_is_read_from_its_message() {
        let (fire, at) = fire_of(
            br#"{"schedule":"order_retry","fire_at":"2026-09-28T10:00:30Z","catch_up":true}"#,
        )
        .expect("fire");
        assert_eq!(fire.schedule, "order_retry");
        assert!(fire.catch_up);
        assert_eq!(at, crate::schedules::tests::at("2026-09-28T10:00:30Z"));
        for (payload, reason) in [
            (&b"not json"[..], "is not a fire"),
            (br#"{"schedule":"s"}"#, "is not a fire"),
            (
                br#"{"schedule":"s","fire_at":"soon","catch_up":false}"#,
                "is not when a fire was due",
            ),
        ] {
            let error = fire_of(payload).expect_err("refused");
            assert!(error.contains(reason), "{error}");
        }
    }

    #[test]
    fn a_fire_a_newer_one_waits_behind_is_passed_over_where_fires_do_not_overlap() {
        let fire = |catch_up| ScheduleFire {
            schedule: "order_retry".into(),
            fire_at: "2026-09-28T10:00:30Z".into(),
            catch_up,
        };
        assert!(superseded(Overlap::Skip, &fire(false), &delivered(1)));
        assert!(!superseded(Overlap::Skip, &fire(false), &delivered(0)));
        assert!(!superseded(Overlap::Allow, &fire(false), &delivered(3)));
        assert!(
            !superseded(Overlap::Skip, &fire(true), &delivered(2)),
            "what is caught up is run, each fire of it"
        );
    }

    #[test]
    fn a_handler_is_told_of_the_fire_and_of_its_delivery() {
        let fire = ScheduleFire {
            schedule: "order_retry".into(),
            fire_at: "2026-09-28T10:00:30Z".into(),
            catch_up: false,
        };
        let request = schedule_request(
            &identity("default", "order-service"),
            "retry_orders",
            &fire,
            &delivered(0),
        );
        assert_eq!(
            request.to_value().expect("json"),
            json!({
                "method": "SCHEDULE",
                "handler": "retry_orders",
                "headers": {},
                "body": {
                    "schedule": "order_retry",
                    "fire_at": "2026-09-28T10:00:30Z",
                    "catch_up": false,
                },
                "tenant": "default",
                "service": "order-service",
                "auth": null,
                "subscription": {
                    "subject": "cufflink.schedule.default.order-service.order_retry",
                    "stream": "CUFFLINK_SCHEDULES",
                    "consumer": "sched_default_order-service_order_retry_0a1b2c3d",
                    "stream_sequence": 7,
                    "consumer_sequence": 3,
                    "num_delivered": 1,
                    "num_pending": 0,
                    "published_at": "2026-09-28T10:00:30.004Z",
                    "headers": {"Nats-Msg-Id": ["sched:x"]},
                },
            })
        );
    }

    #[test]
    fn a_run_is_counted_by_what_became_of_it() {
        for (outcome, ran) in [
            (Outcome::Ok, Some(Ran::Ok)),
            (retry("handler answered 503".into()), Some(Ran::Error)),
            (
                Outcome::Terminal {
                    reason: "no such order".into(),
                },
                Some(Ran::Error),
            ),
            (Outcome::Interrupted, None),
        ] {
            assert_eq!(Ran::of(&outcome), ran, "{outcome:?}");
        }
        assert_eq!(
            [Ran::Ok, Ran::Error, Ran::Coalesced, Ran::Enqueued].map(Ran::as_str),
            ["ok", "error", "coalesced", "enqueued"]
        );
    }

    #[test]
    fn a_fire_that_keeps_failing_is_given_up_after_three_deliveries() {
        let backoff = Backoff::explicit(RETRY_AFTER.to_vec());
        let failed = retry("handler answered 503".into());
        let decided = |delivered: u64| {
            decide(
                AckMode::OnSuccess,
                &failed,
                delivered,
                MAX_DELIVER,
                &backoff,
                true,
                7,
            )
        };
        for (delivered, around) in [(1, 10.0), (2, 30.0)] {
            let Decision::Nak(after) = decided(delivered) else {
                panic!("delivery {delivered} is not handed back");
            };
            assert!(
                (after.as_secs_f64() - around).abs() <= around / 5.0,
                "delivery {delivered} comes back after {after:?}"
            );
        }
        assert_eq!(
            decided(3),
            Decision::DeadLetterThenTerm {
                reason: "handler answered 503".into()
            }
        );
    }
}
