use crate::app::Permanence;
use crate::egress::{EgressError, EgressGuard, EgressPolicy, EgressTarget, OutboundRequest};
use crate::nats_conn::Backoff as ErrorBackoff;
use crate::nats_manager::delivery::{decide, Decision, Outcome};
use crate::nats_manager::dlq::{dlq_subject, DeadLetter, DlqKind};
use crate::nats_manager::events::webhooks_stream;
use crate::nats_manager::pump::{self, MessageMeta, PumpConfig, PumpExit, Work, WorkLabel};
use crate::outbox::route::{WEBHOOKS_STREAM, WEBHOOK_SUBJECT_PREFIX};
use crate::repo::webhook_deliveries::{self, Attempt, DeliveryStatus, Recorded};
use crate::repo::webhooks::{self, WebhookRecord};
use crate::secrets::SecretError;
use crate::tenant_catalog::TenantEntry;
use crate::webhook_dispatcher::{refusal, signed_request, RESPONSE_READ_CAP};
use crate::webhooks::DeliveryConfig;
use crate::AppState;
use async_nats::jetstream::{self, consumer::pull, consumer::AckPolicy, consumer::PullConsumer};
use chrono::{DateTime, Utc};
use cufflink_types::backoff::Backoff;
use cufflink_types::AckMode;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use reqwest::header::RETRY_AFTER;
use reqwest::StatusCode;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

pub const CONSUMER: &str = "cufflink-webhook-delivery";
const ACK_WAIT: Duration = Duration::from_secs(30);
pub const MAX_ATTEMPTS: u64 = 8;
const UNLIMITED: i64 = -1;
const RETRY_BASE: Duration = Duration::from_secs(10);
const RETRY_MAX: Duration = Duration::from_secs(60 * 60);
const MSG_ID_HEADER: &str = "Nats-Msg-Id";

fn backoff() -> Backoff {
    Backoff::exponential(RETRY_BASE, RETRY_MAX)
}

pub fn parse_subject(subject: &str) -> Option<(&str, Uuid)> {
    let (tenant, webhook) = subject
        .strip_prefix(WEBHOOK_SUBJECT_PREFIX)?
        .split_once('.')?;
    let webhook = webhook.parse().ok()?;
    (!tenant.is_empty()).then_some((tenant, webhook))
}

fn delivery_id(meta: &MessageMeta) -> Option<Uuid> {
    meta.headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(MSG_ID_HEADER))
        .and_then(|(_, values)| values.first()?.parse().ok())
}

fn asked_wait(retry_after: &str, now: DateTime<Utc>) -> Option<Duration> {
    let asked = retry_after.trim();
    asked
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            DateTime::parse_from_rfc2822(asked)
                .ok()
                .map(|at| (at.with_timezone(&Utc) - now).to_std().unwrap_or_default())
        })
        .filter(|wait| *wait <= RETRY_MAX)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Delivered,
    Again(Option<Duration>),
    Refused,
}

pub fn answer(status: StatusCode, retry_after: Option<&str>, now: DateTime<Utc>) -> Answer {
    match status.as_u16() {
        200..=299 => Answer::Delivered,
        408 | 429 | 500..=599 => {
            Answer::Again(retry_after.and_then(|asked| asked_wait(asked, now)))
        }
        _ => Answer::Refused,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Attempted {
    outcome: Outcome,
    status: DeliveryStatus,
    answered: Option<StatusCode>,
    error: Option<String>,
}

impl Attempted {
    fn delivered(answered: StatusCode) -> Self {
        Self {
            outcome: Outcome::Ok,
            status: DeliveryStatus::Delivered,
            answered: Some(answered),
            error: None,
        }
    }

    fn dropped(why: &str) -> Self {
        Self {
            outcome: Outcome::Ok,
            status: DeliveryStatus::Dropped,
            answered: None,
            error: Some(why.to_string()),
        }
    }

    fn again(
        after: Option<Duration>,
        answered: Option<StatusCode>,
        reason: String,
        attempt: &Numbered,
    ) -> Self {
        if attempt.number >= MAX_ATTEMPTS {
            return Self::given_up(answered, reason);
        }
        Self {
            outcome: Outcome::Retry {
                after: Some(after.unwrap_or_else(|| attempt.backoff())),
                reason: reason.clone(),
            },
            status: DeliveryStatus::Pending,
            answered,
            error: Some(reason),
        }
    }

    fn given_up(answered: Option<StatusCode>, reason: String) -> Self {
        Self {
            outcome: Outcome::Terminal {
                reason: reason.clone(),
            },
            status: DeliveryStatus::Failed,
            answered,
            error: Some(reason),
        }
    }

    fn result(&self) -> &'static str {
        match self.status {
            DeliveryStatus::Delivered => "delivered",
            DeliveryStatus::Dropped => "dropped",
            DeliveryStatus::Pending => "retry",
            DeliveryStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Numbered {
    number: u64,
    seed: u64,
}

impl Numbered {
    fn after(recorded: i32, seed: u64) -> Self {
        Self {
            number: u64::try_from(recorded)
                .unwrap_or_default()
                .saturating_add(1),
            seed,
        }
    }

    fn backoff(&self) -> Duration {
        backoff().delay(u32::try_from(self.number).unwrap_or(u32::MAX), self.seed)
    }
}

fn judged(
    sent: Result<(StatusCode, Option<String>), EgressError>,
    attempt: &Numbered,
    now: DateTime<Utc>,
) -> Attempted {
    match sent {
        Ok((status, retry_after)) => match answer(status, retry_after.as_deref(), now) {
            Answer::Delivered => Attempted::delivered(status),
            Answer::Again(after) => Attempted::again(
                after,
                Some(status),
                format!("the receiver answered {status}"),
                attempt,
            ),
            Answer::Refused => {
                Attempted::given_up(Some(status), format!("the receiver answered {status}"))
            }
        },
        Err(e) => match refusal(&e) {
            Some(reason) => Attempted::given_up(None, format!("{reason}: {e}")),
            None => Attempted::again(None, None, e.to_string(), attempt),
        },
    }
}

fn count(result: &'static str) {
    metrics::counter!("cufflink_webhook_deliveries_total", "result" => result).increment(1);
}

fn deferral(config: &DeliveryConfig, meta: &MessageMeta) -> Duration {
    Backoff::exponential(config.timeout, config.timeout).delay(1, meta.stream_sequence)
}

async fn load(state: &AppState, id: Uuid) -> Result<Option<WebhookRecord>, SecretError> {
    let client = state.platform_db.get().await?;
    webhooks::find(&state.secrets, &client, id).await
}

async fn send(
    state: &AppState,
    tenant: &TenantEntry,
    webhook: &WebhookRecord,
    request: OutboundRequest,
) -> Result<(StatusCode, Option<String>), EgressError> {
    let request = OutboundRequest {
        timeout: state.config.webhook_delivery.timeout,
        ..request
    };
    let policy = EgressPolicy::new(&tenant.policies, &tenant.tenant.slug);
    let check = |target: &EgressTarget| policy.check(target);
    let response = state.egress.send(request, &check, false).await?;
    let status = response.status();
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|asked| asked.to_str().ok())
        .map(str::to_string);
    if let Err(e) = EgressGuard::read_body(response, RESPONSE_READ_CAP).await {
        warn!(webhook = %webhook.id, error = %e, "Webhook response not read");
    }
    Ok((status, retry_after))
}

async fn recorded(state: &AppState, id: Uuid) -> Result<Recorded, String> {
    let client = state.platform_db.get().await.map_err(|e| e.to_string())?;
    webhook_deliveries::recorded(&client, id)
        .await
        .map_err(|e| crate::db_error::from_pg(&e).message.clone())
}

async fn record(
    state: &AppState,
    webhook: &WebhookRecord,
    id: Uuid,
    attempt: &Numbered,
    attempted: &Attempted,
) {
    let attempt = Attempt {
        id,
        webhook_id: webhook.id,
        tenant_id: webhook.tenant_id,
        event: &webhook.event,
        status: attempted.status,
        attempts: i32::try_from(attempt.number).unwrap_or(i32::MAX),
        last_status: attempted.answered.map(|status| i32::from(status.as_u16())),
        last_error: attempted.error.as_deref(),
    };
    let recorded = match state.platform_db.get().await {
        Ok(client) => webhook_deliveries::record(&client, &attempt)
            .await
            .map_err(|e| crate::db_error::from_pg(&e).message.clone()),
        Err(e) => Err(e.to_string()),
    };
    if let Err(e) = recorded {
        warn!(webhook = %webhook.id, delivery = %id, error = %e, "Webhook delivery not recorded");
    }
}

async fn deliver(state: &AppState, payload: &[u8], meta: &MessageMeta) -> Outcome {
    let Some((slug, webhook_id)) = parse_subject(&meta.subject) else {
        return Outcome::Invalid {
            reason: format!("'{}' names no webhook", meta.subject),
        };
    };
    let Some(id) = delivery_id(meta) else {
        return Outcome::Invalid {
            reason: format!("a delivery without a {MSG_ID_HEADER}"),
        };
    };
    let again = |reason: String| Outcome::Retry {
        after: None,
        reason,
    };
    let Some(_making) = state.webhooks.permits.take(slug, webhook_id) else {
        count("deferred");
        return Outcome::Retry {
            after: Some(deferral(&state.config.webhook_delivery, meta)),
            reason: format!(
                "the deliveries of the webhook {webhook_id} or of the tenant '{slug}' are all being made"
            ),
        };
    };
    let tenant = match state.tenants.get_or_load_by_slug(slug).await {
        Ok(Some(tenant)) => tenant,
        Ok(None) => {
            return Outcome::Terminal {
                reason: format!("no tenant '{slug}'"),
            }
        }
        Err(e) => return again(e.to_string()),
    };
    let webhook = match load(state, webhook_id).await {
        Ok(Some(webhook)) => webhook,
        Ok(None) => {
            info!(webhook = %webhook_id, delivery = %id, "Webhook is no more; its delivery is dropped");
            count("dropped");
            return Outcome::Ok;
        }
        Err(e) if e.is_permanent() => {
            return Outcome::Terminal {
                reason: e.to_string(),
            }
        }
        Err(e) => return again(e.to_string()),
    };
    if webhook.tenant_id != tenant.tenant.id {
        return Outcome::Terminal {
            reason: format!("the webhook {webhook_id} is not of the tenant '{slug}'"),
        };
    }
    let attempt = match recorded(state, id).await {
        Ok(Recorded::Delivered) => {
            info!(webhook = %webhook.id, delivery = %id, "Webhook delivery was taken already");
            return Outcome::Ok;
        }
        Ok(Recorded::Attempted(attempts)) => Numbered::after(attempts, meta.stream_sequence),
        Err(e) => return again(e),
    };
    let signed = webhook
        .active
        .then(|| signed_request(&webhook, &webhook.event, payload, id));
    let attempted = match signed {
        None => Attempted::dropped("the webhook is switched off"),
        Some(_) if webhook.secret.expose().is_empty() => Attempted::given_up(
            None,
            "the webhook has no secret to sign its deliveries with".into(),
        ),
        Some(Ok(request)) => judged(
            send(state, &tenant, &webhook, request).await,
            &attempt,
            Utc::now(),
        ),
        Some(Err(e)) => Attempted::given_up(None, format!("the delivery cannot be signed: {e}")),
    };
    match &attempted.error {
        None => {
            info!(webhook = %webhook.id, delivery = %id, event = %webhook.event, "Webhook delivered")
        }
        Some(error) => {
            warn!(webhook = %webhook.id, delivery = %id, attempt = attempt.number, error = %error, "Webhook not delivered")
        }
    }
    record(state, &webhook, id, &attempt, &attempted).await;
    count(attempted.result());
    attempted.outcome
}

pub struct WebhookDeliveryWork {
    state: Arc<AppState>,
}

impl Work for WebhookDeliveryWork {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "webhook",
            subscription: None,
        }
    }

    fn handle(
        &self,
        msg: Arc<jetstream::Message>,
        meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let state = Arc::clone(&self.state);
        async move { deliver(&state, &msg.payload, &meta).await }.boxed()
    }

    fn decide(&self, outcome: &Outcome, meta: &MessageMeta) -> Decision {
        decide(
            AckMode::OnSuccess,
            outcome,
            meta.delivered,
            UNLIMITED,
            &backoff(),
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
        async move {
            let (slug, webhook_id) = parse_subject(&meta.subject)?;
            let webhook = load(&self.state, webhook_id).await.ok().flatten()?;
            Some(DeadLetter::now(
                dlq_subject(
                    slug,
                    &webhook.service_name,
                    DlqKind::Webhook,
                    &webhook_id.to_string(),
                ),
                msg.payload.clone(),
                meta,
                reason,
            ))
        }
        .boxed()
    }
}

async fn consumer(js: &jetstream::Context) -> eyre::Result<PullConsumer> {
    let config = pull::Config {
        durable_name: Some(CONSUMER.to_string()),
        ack_policy: AckPolicy::Explicit,
        ack_wait: ACK_WAIT,
        max_deliver: UNLIMITED,
        ..Default::default()
    };
    Ok(webhooks_stream(js).await?.create_consumer(config).await?)
}

async fn has_stream(js: &jetstream::Context) -> bool {
    js.get_stream(WEBHOOKS_STREAM).await.is_ok()
}

async fn run(state: Arc<AppState>) {
    let shutdown = state.shutdown.clone();
    if !has_stream(&state.jetstream).await {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = state.webhooks.delivering() => {}
        }
    }
    let work: Arc<dyn Work> = Arc::new(WebhookDeliveryWork {
        state: Arc::clone(&state),
    });
    let config =
        PumpConfig::sequential(ACK_WAIT).concurrent(state.config.webhook_delivery.concurrency);
    let mut errors = ErrorBackoff::new();
    loop {
        match consumer(&state.jetstream).await {
            Ok(consumer) => {
                errors.reset();
                info!(consumer = CONSUMER, "Webhook deliveries started");
                let exit = pump::run(
                    consumer,
                    config,
                    Arc::clone(&work),
                    Arc::new(state.jetstream.clone()),
                    CancellationToken::new(),
                    shutdown.clone(),
                )
                .await;
                if exit != PumpExit::ConsumerGone {
                    return;
                }
                warn!(
                    consumer = CONSUMER,
                    "Webhook deliveries lost their consumer; making it again"
                );
            }
            Err(e) => warn!(error = %e, "Webhook deliveries not started; retrying"),
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(errors.next_delay()) => {}
        }
    }
}

pub fn start(state: Arc<AppState>) {
    let shutdown = state.shutdown.clone();
    shutdown.spawn("webhook_delivery", run(state));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::egress::EgressDenied;

    fn meta(subject: &str, headers: &[(&str, &str)], delivered: u64) -> MessageMeta {
        MessageMeta {
            subject: subject.into(),
            stream: WEBHOOKS_STREAM.into(),
            consumer: CONSUMER.into(),
            stream_sequence: 7,
            consumer_sequence: 7,
            delivered,
            pending: 0,
            published: DateTime::UNIX_EPOCH,
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), vec![value.to_string()]))
                .collect(),
        }
    }

    #[test]
    fn a_subject_names_the_tenant_and_the_webhook() {
        let id = Uuid::from_u128(5);
        assert_eq!(
            parse_subject(&format!("cufflink.webhook.harbor.{id}")),
            Some(("harbor", id))
        );
        for subject in [
            "cufflink.webhook.harbor",
            "cufflink.webhook.harbor.not-an-id",
            &format!("cufflink.webhook..{id}"),
            &format!("cufflink.webhook.harbor.{id}.more"),
            &format!("cufflink.audit.harbor.{id}"),
            &format!("dw.harbor.{id}"),
        ] {
            assert_eq!(parse_subject(subject), None, "{subject}");
        }
    }

    #[test]
    fn a_delivery_is_known_by_the_id_of_its_row() {
        let id = Uuid::from_u128(9);
        let subject = format!("cufflink.webhook.harbor.{id}");
        assert_eq!(
            delivery_id(&meta(
                &subject,
                &[
                    ("traceparent", "00-1-2-01"),
                    ("nats-msg-id", &id.to_string())
                ],
                1
            )),
            Some(id)
        );
        assert_eq!(delivery_id(&meta(&subject, &[], 1)), None);
        assert_eq!(
            delivery_id(&meta(&subject, &[("Nats-Msg-Id", "seven")], 1)),
            None
        );
    }

    #[test]
    fn an_answer_is_a_delivery_another_attempt_or_a_refusal() {
        let now: DateTime<Utc> = "2026-09-28T12:00:00Z".parse().expect("a time");
        let of = |status: u16, retry_after: Option<&str>| {
            answer(
                StatusCode::from_u16(status).expect("a status"),
                retry_after,
                now,
            )
        };
        for status in [200, 201, 204, 299] {
            assert_eq!(of(status, None), Answer::Delivered, "{status}");
        }
        for status in [408, 429, 500, 502, 503, 504, 599] {
            assert_eq!(of(status, None), Answer::Again(None), "{status}");
        }
        for status in [301, 302, 400, 401, 403, 404, 410, 422] {
            assert_eq!(of(status, Some("5")), Answer::Refused, "{status}");
        }
        let seconds = |seconds| Answer::Again(Some(Duration::from_secs(seconds)));
        assert_eq!(of(503, Some("120")), seconds(120));
        assert_eq!(of(429, Some(" 3600 ")), seconds(3600));
        assert_eq!(of(503, Some("0")), seconds(0));
        assert_eq!(of(503, Some("Mon, 28 Sep 2026 12:05:00 GMT")), seconds(300));
        assert_eq!(
            of(503, Some("Mon, 28 Sep 2026 11:00:00 GMT")),
            seconds(0),
            "a date that has passed is no wait"
        );
        for unheard in ["3601", "Mon, 28 Sep 2026 14:00:00 GMT", "soon", "-5", ""] {
            assert_eq!(of(503, Some(unheard)), Answer::Again(None), "{unheard}");
        }
    }

    fn numbered(number: u64) -> Numbered {
        Numbered { number, seed: 7 }
    }

    #[test]
    fn an_attempt_is_recorded_as_what_became_of_its_delivery() {
        let now = Utc::now();
        let status = |code: u16| StatusCode::from_u16(code).expect("a status");
        let answered = |code: u16, retry_after: Option<&str>, attempt| {
            judged(
                Ok((status(code), retry_after.map(str::to_string))),
                &numbered(attempt),
                now,
            )
        };

        let delivered = answered(200, None, 3);
        assert_eq!(delivered, Attempted::delivered(status(200)));
        assert_eq!(
            (delivered.outcome.clone(), delivered.result()),
            (Outcome::Ok, "delivered")
        );

        let unavailable = answered(503, Some("2"), 1);
        assert_eq!(
            unavailable.outcome,
            Outcome::Retry {
                after: Some(Duration::from_secs(2)),
                reason: "the receiver answered 503 Service Unavailable".into(),
            }
        );
        assert_eq!(
            (
                unavailable.status,
                unavailable.answered,
                unavailable.result()
            ),
            (DeliveryStatus::Pending, Some(status(503)), "retry")
        );

        let last = answered(503, None, MAX_ATTEMPTS);
        assert_eq!(
            (last.status, last.result()),
            (DeliveryStatus::Failed, "failed"),
            "no attempt follows the eighth"
        );
        assert_eq!(
            last.outcome,
            Outcome::Terminal {
                reason: "the receiver answered 503 Service Unavailable".into()
            }
        );

        let gone = answered(410, None, 1);
        assert_eq!(
            gone.outcome,
            Outcome::Terminal {
                reason: "the receiver answered 410 Gone".into()
            }
        );
        assert_eq!(
            (gone.status, gone.result()),
            (DeliveryStatus::Failed, "failed")
        );

        let unreachable = judged(
            Err(EgressError::TimedOut(Duration::from_secs(10))),
            &numbered(2),
            now,
        );
        assert_eq!(
            unreachable.outcome,
            Outcome::Retry {
                after: Some(numbered(2).backoff()),
                reason: "request timed out after 10s".into(),
            }
        );
        assert_eq!(
            (unreachable.status, unreachable.answered),
            (DeliveryStatus::Pending, None)
        );

        let refused = judged(
            Err(EgressError::Denied(EgressDenied::MetadataEndpoint(
                "169.254.169.254".into(),
            ))),
            &numbered(1),
            now,
        );
        assert!(
            matches!(&refused.outcome, Outcome::Terminal { reason } if reason.starts_with("metadata: ")),
            "{refused:?}"
        );
        assert_eq!(refused.status, DeliveryStatus::Failed);

        let dropped = Attempted::dropped("the webhook is switched off");
        assert_eq!(
            (dropped.outcome.clone(), dropped.status, dropped.result()),
            (Outcome::Ok, DeliveryStatus::Dropped, "dropped")
        );
    }

    #[test]
    fn an_attempt_waits_longer_each_time_by_the_attempts_that_were_made() {
        assert_eq!(Numbered::after(0, 7), numbered(1));
        assert_eq!(Numbered::after(7, 7), numbered(8));
        assert_eq!(Numbered::after(-1, 7), numbered(1));
        let nominal = [10, 20, 40, 80, 160, 320, 640].map(Duration::from_secs);
        for (number, nominal) in (1..).zip(nominal) {
            let wait = numbered(number).backoff();
            assert!(
                wait >= nominal.mul_f64(0.8) && wait <= nominal.mul_f64(1.2),
                "{wait:?} is not about {nominal:?}"
            );
        }
    }

    #[test]
    fn the_server_gives_up_no_delivery_the_attempts_do() {
        let work_decides = |outcome: &Outcome, delivered| {
            decide(
                AckMode::OnSuccess,
                outcome,
                delivered,
                UNLIMITED,
                &backoff(),
                true,
                7,
            )
        };
        let waited = Outcome::Retry {
            after: Some(Duration::from_secs(9)),
            reason: "no room".into(),
        };
        assert_eq!(
            work_decides(&waited, 1000),
            Decision::Nak(Duration::from_secs(9)),
            "a delivery that waited for room however often is not given up"
        );
        assert_eq!(
            work_decides(
                &Outcome::Terminal {
                    reason: "the receiver answered 503".into()
                },
                3
            ),
            Decision::DeadLetterThenTerm {
                reason: "the receiver answered 503".into()
            }
        );
        assert_eq!(work_decides(&Outcome::Ok, 8), Decision::Ack);
    }

    #[test]
    fn a_delivery_that_finds_no_room_waits_about_as_long_as_an_attempt() {
        let config = DeliveryConfig::default();
        for sequence in 1..50 {
            let wait = deferral(
                &config,
                &MessageMeta {
                    stream_sequence: sequence,
                    ..meta("cufflink.webhook.harbor.1", &[], 1)
                },
            );
            assert!(
                wait >= config.timeout.mul_f64(0.8) && wait <= config.timeout.mul_f64(1.2),
                "{wait:?}"
            );
        }
    }
}
