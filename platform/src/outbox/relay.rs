use super::bootstrap;
use super::publish::{max_payload, publish_in_order, wire_size, NatsTransport};
use super::route::{
    classify, dispose, retry_delay, Disposition, GivenUp, Outcome, PendingRow, Route, Routing,
};
use super::seal::NO_KEY;
use crate::nats_conn::Backoff as ErrorBackoff;
use crate::nats_manager::dlq::{cut, dlq_subject, publish_dead_letter, DeadLetter, DlqKind};
use crate::policy::{PolicyKey, PolicyMode};
use crate::repo::outbox_leases as leases;
use crate::tenant_catalog::TenantEntry;
use crate::AppState;
use cufflink_db::Listener;
use cufflink_telemetry::{scoped_span, Scope};
use cufflink_types::backoff::Backoff;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn, Instrument};
use uuid::Uuid;

const LEASE_TTL: Duration = Duration::from_secs(15);
const RENEW_EVERY: Duration = Duration::from_secs(5);
const SUPERVISE_EVERY: Duration = Duration::from_secs(5);
const LEASE_SCAN_EVERY: Duration = Duration::from_secs(60);
const REPORT_EVERY: Duration = Duration::from_secs(30);
const POOLED_POLL: Duration = Duration::from_millis(250);
const RETIRE_AFTER: Duration = Duration::from_secs(30);
const CHECKOUT_WAIT: Duration = Duration::from_secs(5);
const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(5 * 60);
const LAST_ERROR_MAX: usize = 500;

pub const NOTIFY_CHANNEL: &str = "_cufflink_outbox";
pub const OUTBOX_TABLE: &str = "_cufflink.outbox";

const HEAD: &str = "SELECT id, msg_id, kind, service, source_role::text AS source_role, subject, \
            payload, headers, attempts, \
            GREATEST(EXTRACT(EPOCH FROM (available_at - now())), 0)::float8 AS due_in \
     FROM _cufflink.outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED";
const DELETE: &str = "DELETE FROM _cufflink.outbox WHERE id = ANY($1)";
const RETRY: &str = "UPDATE _cufflink.outbox o \
     SET attempts = o.attempts + 1, available_at = now() + make_interval(secs => f.delay), \
         last_error = f.error \
     FROM unnest($1::bigint[], $2::float8[], $3::text[]) AS f(id, delay, error) \
     WHERE o.id = f.id";
const DEPTH: &str = "SELECT count(*), \
            COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)::float8 \
     FROM _cufflink.outbox";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayConfig {
    pub batch: usize,
    pub poll: Duration,
    pub publish_concurrency: usize,
    pub max_attempts: u32,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            batch: 256,
            poll: Duration::from_millis(1000),
            publish_concurrency: 64,
            max_attempts: 20,
        }
    }
}

impl RelayConfig {
    pub fn from_env() -> eyre::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let (defaults, vars) = (Self::default(), cufflink_db::Vars(var));
        Ok(Self {
            batch: vars.positive("OUTBOX_RELAY_BATCH", defaults.batch)?,
            poll: Duration::from_millis(vars.positive(
                "OUTBOX_RELAY_POLL_MS",
                u64::try_from(defaults.poll.as_millis())?,
            )?),
            publish_concurrency: vars
                .positive("OUTBOX_PUBLISH_CONCURRENCY", defaults.publish_concurrency)?,
            max_attempts: vars.positive("OUTBOX_MAX_ATTEMPTS", defaults.max_attempts)?,
        })
    }

    fn poll_through(&self, pooler: cufflink_db::Pooler) -> Duration {
        if pooler.is_transaction_pooler() {
            self.poll.min(POOLED_POLL)
        } else {
            self.poll
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Supervision {
    pub start: Vec<Uuid>,
    pub stop: Vec<Uuid>,
    pub keep: Vec<(Uuid, bool)>,
}

pub fn supervision(
    tenants: &HashMap<Uuid, bool>,
    leased: &HashSet<Uuid>,
    running: &HashSet<Uuid>,
) -> Supervision {
    let sorted = |mut ids: Vec<Uuid>| {
        ids.sort();
        ids
    };
    let mut keep: Vec<(Uuid, bool)> = running
        .iter()
        .filter_map(|id| tenants.get(id).map(|enforced| (*id, *enforced)))
        .collect();
    keep.sort();
    Supervision {
        start: sorted(
            tenants
                .iter()
                .filter(|(id, enforced)| {
                    (**enforced || leased.contains(*id)) && !running.contains(*id)
                })
                .map(|(id, _)| *id)
                .collect(),
        ),
        stop: sorted(
            running
                .iter()
                .filter(|id| !tenants.contains_key(*id))
                .copied()
                .collect(),
        ),
        keep,
    }
}

struct Running {
    stop: CancellationToken,
    written: watch::Sender<bool>,
    task: JoinHandle<()>,
}

fn holder() -> Arc<str> {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "platform".into());
    format!("{host}:{}:{}", std::process::id(), Uuid::new_v4()).into()
}

pub fn start(state: Arc<AppState>) {
    let shutdown = state.shutdown.clone();
    shutdown.spawn("outbox_relay_supervisor", supervise(state));
}

async fn supervise(state: Arc<AppState>) {
    let holder = holder();
    let token = state.shutdown.token();
    let mut running: HashMap<Uuid, Running> = HashMap::new();
    let mut leased: HashSet<Uuid> = HashSet::new();
    let mut scanned: Option<Instant> = None;
    let mut listed: Option<Instant> = None;
    loop {
        running.retain(|id, relay| {
            let ended = relay.task.is_finished();
            if ended {
                leased.remove(id);
            }
            !ended
        });
        if scanned.is_none_or(|at| at.elapsed() >= LEASE_SCAN_EVERY) {
            match leases::tenants(&state.platform_db).await {
                Ok(found) => {
                    leased = found.into_iter().collect();
                    scanned = Some(Instant::now());
                }
                Err(e) => warn!(error = %e, "Could not read the outbox relay leases; retrying"),
            }
        }
        let tenants: HashMap<Uuid, bool> = state
            .tenants
            .entries()
            .iter()
            .map(|entry| {
                (
                    entry.tenant.id,
                    entry.mode(PolicyKey::Outbox) == PolicyMode::Enforce,
                )
            })
            .collect();
        let plan = supervision(&tenants, &leased, &running.keys().copied().collect());
        let relaying = !running.is_empty() || !plan.start.is_empty();
        if relaying && listed.is_none_or(|at| at.elapsed() >= REPORT_EVERY) {
            listed = Some(Instant::now());
            crate::nats_manager::bootstrap::spawn_ensure_outbox_streams(&state);
            state.webhooks.deliver();
            if let Err(e) = state.stream_index.refresh(&state.jetstream).await {
                warn!(error = %e, "Could not list the streams; routing by the last listing");
            }
        }
        for id in &plan.stop {
            if let Some(relay) = running.get(id) {
                relay.stop.cancel();
            }
        }
        for (id, written) in &plan.keep {
            if let Some(relay) = running.get(id) {
                relay.written.send_replace(*written);
            }
        }
        for id in plan.start {
            let written = tenants.get(&id).copied().unwrap_or_default();
            running.insert(id, spawn_relay(&state, id, holder.clone(), written));
        }
        tokio::select! {
            _ = token.cancelled() => return,
            _ = tokio::time::sleep(SUPERVISE_EVERY) => {}
        }
    }
}

fn spawn_relay(state: &Arc<AppState>, tenant_id: Uuid, holder: Arc<str>, written: bool) -> Running {
    let stop = state.shutdown.token().child_token();
    let (written, is_written) = watch::channel(written);
    let relay = Relay {
        state: state.clone(),
        tenant_id,
        holder,
    };
    Running {
        task: state
            .shutdown
            .spawn("outbox_relay", relay.run(stop.clone(), is_written)),
        stop,
        written,
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Lease {
    until: Option<Instant>,
    asked: Option<Instant>,
}

impl Lease {
    fn held(&self) -> bool {
        self.until.is_some_and(|until| Instant::now() < until)
    }

    fn renewal_due(&self) -> bool {
        self.asked.is_none_or(|at| at.elapsed() >= RENEW_EVERY)
    }

    fn until_renewal(&self) -> Duration {
        self.asked.map_or(Duration::ZERO, |at| {
            RENEW_EVERY.saturating_sub(at.elapsed())
        })
    }
}

#[derive(Default)]
struct Leading {
    prepared: bool,
    reported: Option<Instant>,
    listener: Option<Listener>,
}

#[derive(Debug, PartialEq)]
enum Step {
    Drained,
    Blocked(Duration),
    Empty,
    Gone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Exit {
    Stopped,
    Retired,
}

#[derive(Debug, Default, PartialEq)]
struct Settled {
    delete: Vec<i64>,
    retry: Vec<(i64, Duration, String)>,
}

impl Settled {
    fn with(mut self, row: &PendingRow, disposition: Disposition) -> Self {
        match disposition {
            Disposition::Delete | Disposition::DeadLetter { .. } => self.delete.push(row.id),
            Disposition::Retry { delay, error } => {
                self.retry
                    .push((row.id, delay, error.chars().take(LAST_ERROR_MAX).collect()))
            }
        }
        self
    }
}

const SERVICE_KEPT_BYTES: usize = 64;
const TEXT_KEPT_BYTES: usize = 1024;

fn dead_letter(tenant_slug: &str, row: &PendingRow, why: GivenUp, reason: &str) -> DeadLetter {
    DeadLetter {
        subject: dlq_subject(
            tenant_slug,
            cut(&row.service, SERVICE_KEPT_BYTES),
            DlqKind::Outbox,
            why.handler(row.kind),
        ),
        payload: row.payload.clone().into(),
        headers: row
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), vec![value.clone()]))
            .collect(),
        reason: reason.to_string(),
        stream: OUTBOX_TABLE.to_string(),
        original_subject: row.subject.clone(),
        sequence: row.id.unsigned_abs(),
        deliveries: u64::try_from(row.attempts).unwrap_or_default(),
        consumer: format!("outbox-relay-{tenant_slug}"),
        at: chrono::Utc::now(),
    }
}

struct Relay {
    state: Arc<AppState>,
    tenant_id: Uuid,
    holder: Arc<str>,
}

impl Relay {
    async fn run(self, stop: CancellationToken, written: watch::Receiver<bool>) {
        let mut lease = Lease::default();
        let mut leading: Option<Leading> = None;
        let mut errors = ErrorBackoff::new();
        let mut unwritten_since: Option<Instant> = None;
        let exit = loop {
            if stop.is_cancelled() {
                break Exit::Stopped;
            }
            unwritten_since = match *written.borrow() {
                true => None,
                false => unwritten_since.or_else(|| Some(Instant::now())),
            };
            if lease.renewal_due() {
                lease = self.renew(lease).await;
            }
            if !lease.held() {
                leading = None;
            }
            self.report_leader(lease.held());
            let step = match lease.held() {
                true => {
                    let leading = leading.get_or_insert_with(Leading::default);
                    self.lead(leading).await.map(Some).inspect_err(|_| {
                        leading.prepared = false;
                    })
                }
                false => Ok(None),
            };
            if step.is_ok() {
                errors.reset();
            }
            let (pause, listening) = match step {
                Ok(Some(Step::Gone)) => break Exit::Stopped,
                Ok(Some(Step::Empty))
                    if unwritten_since.is_some_and(|at| at.elapsed() >= RETIRE_AFTER) =>
                {
                    break Exit::Retired
                }
                Ok(Some(Step::Drained)) => (Duration::ZERO, false),
                Ok(Some(Step::Blocked(wait))) => (wait, false),
                Ok(Some(Step::Empty)) => (self.poll(), true),
                Ok(None) => (RENEW_EVERY, false),
                Err(e) => {
                    warn!(tenant_id = %self.tenant_id, error = %e, "Outbox relay failed; retrying");
                    (errors.next_delay(), false)
                }
            };
            let listener = leading
                .as_mut()
                .and_then(|leading| leading.listener.as_mut())
                .filter(|_| listening);
            let connected = tokio::select! {
                _ = stop.cancelled() => true,
                _ = tokio::time::sleep(pause.min(lease.until_renewal())) => true,
                notified = notified(listener) => notified,
            };
            if !connected {
                if let Some(leading) = leading.as_mut() {
                    leading.listener = None;
                }
            }
        };
        self.report_leader(false);
        let given_up = match exit {
            Exit::Stopped => {
                leases::release(&self.state.platform_db, self.tenant_id, &self.holder).await
            }
            Exit::Retired => {
                info!(tenant_id = %self.tenant_id, "Outbox drained and written no more; its relay ends");
                leases::retire(&self.state.platform_db, self.tenant_id, &self.holder).await
            }
        };
        if let Err(e) = given_up {
            warn!(tenant_id = %self.tenant_id, error = %e, "Outbox relay lease not given up; it expires");
        }
    }

    fn poll(&self) -> Duration {
        self.state
            .config
            .outbox
            .poll_through(self.state.tenant_sessions.pooler())
    }

    fn slug(&self) -> String {
        self.state
            .tenants
            .get(self.tenant_id)
            .map(|entry| entry.tenant.slug.clone())
            .unwrap_or_else(|| self.tenant_id.to_string())
    }

    fn report_leader(&self, books: bool) {
        metrics::gauge!("cufflink_outbox_relay_leader", "tenant" => self.slug()).set(if books {
            1.0
        } else {
            0.0
        });
    }

    async fn renew(&self, lease: Lease) -> Lease {
        let asked = Instant::now();
        let acquired = leases::acquire(
            &self.state.platform_db,
            self.tenant_id,
            &self.holder,
            LEASE_TTL,
        )
        .await;
        match acquired {
            Ok(true) => Lease {
                until: Some(asked + LEASE_TTL),
                asked: Some(asked),
            },
            Ok(false) => Lease {
                until: None,
                asked: Some(asked),
            },
            Err(e) => {
                warn!(tenant_id = %self.tenant_id, error = %e, "Outbox relay lease not renewed");
                Lease {
                    asked: Some(asked),
                    ..lease
                }
            }
        }
    }

    async fn lead(&self, leading: &mut Leading) -> eyre::Result<Step> {
        let Some(tenant) = self.state.tenants.get(self.tenant_id) else {
            return Ok(Step::Gone);
        };
        let tenant = &tenant;
        if self.state.outbox_seal.is_none() {
            eyre::bail!("the outbox is held, and nothing is published of it: {NO_KEY}");
        }
        if !leading.prepared {
            bootstrap::ensure(&self.state.tenant_sessions, self.tenant_id).await?;
            crate::nats_manager::bootstrap::ensure_outbox_streams(&self.state).await?;
            leading.prepared = true;
        }
        if leading
            .reported
            .is_none_or(|at| at.elapsed() >= REPORT_EVERY)
        {
            leading.reported = Some(Instant::now());
            self.report(tenant, leading).await;
        }
        self.drain(tenant).await
    }

    async fn report(&self, tenant: &TenantEntry, leading: &mut Leading) {
        if let Err(e) = self.report_depth(tenant).await {
            warn!(tenant = %tenant.tenant.slug, error = %e, "Could not measure the outbox");
        }
        if leading.listener.is_none()
            && !self.state.tenant_sessions.pooler().is_transaction_pooler()
        {
            leading.listener = self
                .state
                .tenant_sessions
                .listen(self.tenant_id, NOTIFY_CHANNEL)
                .await
                .inspect_err(|e| warn!(tenant = %tenant.tenant.slug, error = %e, "Outbox relay cannot listen; polling"))
                .ok();
        }
    }

    async fn report_depth(&self, tenant: &TenantEntry) -> eyre::Result<()> {
        let client = self
            .state
            .tenant_sessions
            .platform(self.tenant_id, Some(CHECKOUT_WAIT))
            .await?;
        let row = client.query_one(DEPTH, &[]).await?;
        let (depth, oldest): (i64, f64) = (row.get(0), row.get(1));
        metrics::gauge!("cufflink_outbox_depth", "tenant" => tenant.tenant.slug.clone())
            .set(depth as f64);
        metrics::gauge!("cufflink_outbox_oldest_age_seconds", "tenant" => tenant.tenant.slug.clone())
            .set(oldest);
        Ok(())
    }

    async fn drain(&self, tenant: &TenantEntry) -> eyre::Result<Step> {
        let config = &self.state.config.outbox;
        let mut client = self
            .state
            .tenant_sessions
            .platform(self.tenant_id, Some(CHECKOUT_WAIT))
            .await?;
        let tx = client.transaction().await?;
        let head = tx.query(HEAD, &[&i64::try_from(config.batch)?]).await?;
        let due_in = |row: &tokio_postgres::Row| {
            Duration::try_from_secs_f64(row.get("due_in")).unwrap_or(Duration::MAX)
        };
        let due: Vec<PendingRow> = head
            .iter()
            .take_while(|row| due_in(row).is_zero())
            .map(PendingRow::of)
            .collect::<Result<_, _>>()
            .map_err(|e| eyre::eyre!(e))?;
        if due.is_empty() {
            return Ok(head
                .first()
                .map_or(Step::Empty, |row| Step::Blocked(due_in(row))));
        }
        self.relay(tenant, tx, due)
            .instrument(scoped_span!(
                parent: None,
                "outbox_relay",
                Scope::tenant(tenant.tenant.slug.as_str())
            ))
            .await
    }

    async fn relay(
        &self,
        tenant: &TenantEntry,
        tx: deadpool_postgres::Transaction<'_>,
        due: Vec<PendingRow>,
    ) -> eyre::Result<Step> {
        let config = &self.state.config.outbox;
        let streams = self.state.stream_index.snapshot();
        let routing = Routing {
            tenant_id: self.tenant_id,
            seal: self.state.outbox_seal.as_ref(),
            namespace: &tenant.namespace,
            streams: &streams,
            max_payload: max_payload(&self.state.nats),
        };
        let routed: Vec<(PendingRow, Route)> = due
            .into_iter()
            .map(|row| {
                let route = classify(&row, &routing);
                (row, route)
            })
            .collect();
        let transport = NatsTransport {
            nats: &self.state.nats,
            jetstream: &self.state.jetstream,
        };
        let outcomes = publish_in_order(&transport, &routed, config.publish_concurrency).await;
        let settled = self.settle(tenant, &routed, outcomes).await;
        if !settled.delete.is_empty() {
            tx.execute(DELETE, &[&settled.delete]).await?;
        }
        if !settled.retry.is_empty() {
            let (ids, delays, errors): (Vec<i64>, Vec<f64>, Vec<String>) = settled
                .retry
                .into_iter()
                .map(|(id, delay, error)| (id, delay.as_secs_f64(), error))
                .fold(
                    Default::default(),
                    |(mut ids, mut delays, mut errors), (id, delay, error)| {
                        ids.push(id);
                        delays.push(delay);
                        errors.push(error);
                        (ids, delays, errors)
                    },
                );
            tx.execute(RETRY, &[&ids, &delays, &errors]).await?;
        }
        tx.commit().await?;
        Ok(Step::Drained)
    }

    async fn settle(
        &self,
        tenant: &TenantEntry,
        routed: &[(PendingRow, Route)],
        outcomes: Vec<Outcome>,
    ) -> Settled {
        let config = &self.state.config.outbox;
        let slug = tenant.tenant.slug.as_str();
        let backoff = Backoff::exponential(RETRY_BASE, RETRY_MAX);
        let mut settled = Settled::default();
        for ((row, route), outcome) in routed.iter().zip(outcomes) {
            count_published(slug, route, &outcome);
            let disposition = match dispose(row, outcome, config.max_attempts, &backoff) {
                Disposition::DeadLetter { why, reason } => {
                    self.give_up(slug, row, why, reason, &backoff).await
                }
                other => other,
            };
            settled = settled.with(row, disposition);
        }
        settled
    }

    async fn give_up(
        &self,
        slug: &str,
        row: &PendingRow,
        why: GivenUp,
        reason: String,
        backoff: &Backoff,
    ) -> Disposition {
        match why {
            GivenUp::Rejected => error!(
                event = "outbox_row_rejected",
                tenant = slug,
                service = %row.service,
                source_role = %row.source_role,
                id = row.id,
                reason = %reason,
                "Outbox row rejected"
            ),
            GivenUp::Unauthenticated => error!(
                event = "outbox_row_unauthenticated",
                tenant = slug,
                kind = row.kind.as_str(),
                service = %row.service,
                source_role = %row.source_role,
                id = row.id,
                reason = %reason,
                "Outbox row not written by the platform"
            ),
            GivenUp::Exhausted => {}
        }
        let (letter, whole) = storable(
            dead_letter(slug, row, why, &reason),
            row,
            max_payload(&self.state.nats),
        );
        match publish_dead_letter(&self.state.jetstream, &letter).await {
            Ok(()) => {
                if !whole {
                    warn!(
                        tenant = slug,
                        id = row.id,
                        bytes = row.payload.len(),
                        "Outbox row dead-lettered without its payload, which the server cannot carry"
                    );
                    metrics::counter!(
                        "cufflink_outbox_dead_letter_truncated_total",
                        "tenant" => slug.to_string(),
                    )
                    .increment(1);
                }
                metrics::counter!(
                    "cufflink_outbox_dead_lettered_total",
                    "tenant" => slug.to_string(),
                    "reason" => why.as_str(),
                )
                .increment(1);
                if why == GivenUp::Unauthenticated {
                    metrics::counter!(
                        "cufflink_outbox_rows_unauthenticated_total",
                        "tenant" => slug.to_string(),
                        "kind" => row.kind.as_str(),
                    )
                    .increment(1);
                }
                Disposition::DeadLetter { why, reason }
            }
            Err(e) => Disposition::Retry {
                delay: retry_delay(backoff, row),
                error: format!("{reason}; dead letter not stored: {e}"),
            },
        }
    }
}

fn storable(whole: DeadLetter, row: &PendingRow, max_payload: Option<usize>) -> (DeadLetter, bool) {
    let size = wire_size(whole.payload.len(), &whole.publish_headers());
    if max_payload.is_none_or(|max_payload| size <= max_payload) {
        (whole, true)
    } else {
        (truncated(whole, row), false)
    }
}

fn truncated(letter: DeadLetter, row: &PendingRow) -> DeadLetter {
    use sha2::Digest;
    let subject = cut(&row.subject, TEXT_KEPT_BYTES);
    let reference = serde_json::json!({
        "truncated": true,
        "outbox_id": row.id,
        "msg_id": row.msg_id,
        "subject": subject,
        "payload_bytes": row.payload.len(),
        "payload_sha256": hex::encode(sha2::Sha256::digest(&row.payload)),
    });
    let headers_bytes: usize = row
        .headers
        .iter()
        .map(|(name, value)| name.len() + value.len())
        .sum();
    let (headers, dropped) = if headers_bytes <= TEXT_KEPT_BYTES {
        (letter.headers, "")
    } else {
        (Vec::new(), " nor its headers")
    };
    DeadLetter {
        payload: reference.to_string().into(),
        headers,
        original_subject: subject.to_string(),
        reason: format!(
            "{}; the payload of {} bytes is not stored{dropped}",
            cut(&letter.reason, TEXT_KEPT_BYTES),
            row.payload.len()
        ),
        ..letter
    }
}

async fn notified(listener: Option<&mut Listener>) -> bool {
    match listener {
        Some(listener) => listener.notified().await,
        None => std::future::pending().await,
    }
}

fn count_published(tenant_slug: &str, route: &Route, outcome: &Outcome) {
    let result = match outcome {
        Outcome::Published => "ok",
        Outcome::Failed(_) => "error",
        Outcome::Rejected(_) => "rejected",
        Outcome::Unauthenticated(_) => "unauthenticated",
    };
    metrics::counter!(
        "cufflink_outbox_published_total",
        "tenant" => tenant_slug.to_string(),
        "route" => route.label(),
        "result" => result,
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::super::route::tests::row;
    use super::super::writer::OutboxKind;
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn the_defaults_are_the_documented_ones_and_nothing_may_be_zero() {
        assert_eq!(
            RelayConfig::from_vars(|_| None).expect("defaults"),
            RelayConfig {
                batch: 256,
                poll: Duration::from_secs(1),
                publish_concurrency: 64,
                max_attempts: 20,
            }
        );
        let set = |name: &'static str, value: &'static str| {
            RelayConfig::from_vars(move |asked| (asked == name).then(|| value.to_string()))
        };
        assert_eq!(set("OUTBOX_RELAY_BATCH", "10").expect("set").batch, 10);
        assert_eq!(
            set("OUTBOX_RELAY_POLL_MS", "50").expect("set").poll,
            Duration::from_millis(50)
        );
        assert_eq!(
            set("OUTBOX_PUBLISH_CONCURRENCY", "1")
                .expect("set")
                .publish_concurrency,
            1
        );
        assert_eq!(
            set("OUTBOX_MAX_ATTEMPTS", "3").expect("set").max_attempts,
            3
        );
        for name in [
            "OUTBOX_RELAY_BATCH",
            "OUTBOX_RELAY_POLL_MS",
            "OUTBOX_PUBLISH_CONCURRENCY",
            "OUTBOX_MAX_ATTEMPTS",
        ] {
            let refused = set(name, "0").expect_err("zero").to_string();
            assert_eq!(refused, format!("{name} must be greater than 0"));
        }
    }

    #[test]
    fn a_transaction_pooler_is_polled_faster_than_a_second() {
        let config = RelayConfig::default();
        assert_eq!(
            config.poll_through(cufflink_db::Pooler::Direct),
            Duration::from_secs(1)
        );
        assert_eq!(
            config.poll_through(cufflink_db::Pooler::PgBouncerTransaction),
            Duration::from_millis(250)
        );
        let eager = RelayConfig {
            poll: Duration::from_millis(100),
            ..config
        };
        assert_eq!(
            eager.poll_through(cufflink_db::Pooler::PgBouncerTransaction),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn a_relay_runs_for_every_outbox_that_is_written_or_may_hold_rows() {
        let tenants: HashMap<Uuid, bool> =
            [(id(1), true), (id(2), false), (id(3), false), (id(4), true)].into();
        let leased: HashSet<Uuid> = [id(3), id(9)].into();
        assert_eq!(
            supervision(&tenants, &leased, &HashSet::new()),
            Supervision {
                start: vec![id(1), id(3), id(4)],
                stop: vec![],
                keep: vec![],
            }
        );
    }

    #[test]
    fn a_relay_is_told_when_its_outbox_is_written_no_more_and_stopped_with_its_tenant() {
        let tenants: HashMap<Uuid, bool> = [(id(1), true), (id(2), false)].into();
        let running: HashSet<Uuid> = [id(1), id(2), id(5)].into();
        assert_eq!(
            supervision(&tenants, &HashSet::new(), &running),
            Supervision {
                start: vec![],
                stop: vec![id(5)],
                keep: vec![(id(1), true), (id(2), false)],
            }
        );
    }

    #[test]
    fn with_every_policy_at_its_default_no_relay_runs() {
        let tenants: HashMap<Uuid, bool> = [(id(1), false), (id(2), false)].into();
        assert_eq!(
            supervision(&tenants, &HashSet::new(), &HashSet::new()),
            Supervision::default()
        );
    }

    #[test]
    fn a_lease_is_held_until_it_runs_out_and_renewed_well_before() {
        let never = Lease::default();
        assert!(!never.held());
        assert!(never.renewal_due());
        assert_eq!(never.until_renewal(), Duration::ZERO);
        let now = Instant::now();
        let fresh = Lease {
            until: Some(now + LEASE_TTL),
            asked: Some(now),
        };
        assert!(fresh.held());
        assert!(!fresh.renewal_due());
        assert!(fresh.until_renewal() <= RENEW_EVERY);
        assert!(RENEW_EVERY * 2 < LEASE_TTL);
        let lapsed = Lease {
            until: Some(now),
            asked: Some(now),
        };
        assert!(!lapsed.held());
    }

    #[test]
    fn what_is_published_or_given_up_is_deleted_and_the_rest_waits() {
        let rows = [
            row(1, OutboxKind::Event, "books.created"),
            row(2, OutboxKind::Event, "receipts.created"),
            row(3, OutboxKind::Event, "books.updated"),
        ];
        let settled = Settled::default()
            .with(&rows[0], Disposition::Delete)
            .with(
                &rows[1],
                Disposition::DeadLetter {
                    why: GivenUp::Rejected,
                    reason: "not the tenant's".into(),
                },
            )
            .with(
                &rows[2],
                Disposition::Retry {
                    delay: Duration::from_secs(2),
                    error: "e".repeat(LAST_ERROR_MAX + 40),
                },
            );
        assert_eq!(
            settled,
            Settled {
                delete: vec![1, 2],
                retry: vec![(3, Duration::from_secs(2), "e".repeat(LAST_ERROR_MAX))],
            }
        );
    }

    #[test]
    fn a_dead_letter_is_filed_under_the_relays_tenant_and_names_its_row() {
        let failed = PendingRow {
            attempts: 19,
            ..row(44, OutboxKind::Audit, "dw.harbor.logs")
        };
        let letter = dead_letter("harbor", &failed, GivenUp::Exhausted, "timed out");
        assert_eq!(letter.subject, "cufflink.dlq.harbor.loans.outbox.audit");
        assert_eq!(letter.original_subject, "dw.harbor.logs");
        assert_eq!(
            letter.msg_id(),
            "dlq:_cufflink.outbox:outbox-relay-harbor:44"
        );
        assert_eq!(letter.deliveries, 19);
        assert_eq!(letter.payload, failed.payload);
        let rejected = dead_letter(
            "harbor",
            &row(45, OutboxKind::Event, "receipts.created"),
            GivenUp::Rejected,
            "not the tenant's",
        );
        assert_eq!(
            rejected.subject,
            "cufflink.dlq.harbor.loans.outbox.rejected"
        );
    }

    #[test]
    fn a_dead_letter_without_its_payload_says_what_the_payload_was() {
        let failed = PendingRow {
            payload: vec![b'x'; 3000],
            ..row(44, OutboxKind::Event, "books.created")
        };
        let whole = dead_letter("harbor", &failed, GivenUp::Rejected, "too large");
        let letter = truncated(whole.clone(), &failed);
        let reference: serde_json::Value =
            serde_json::from_slice(&letter.payload).expect("a reference");
        assert_eq!(
            reference,
            serde_json::json!({
                "truncated": true,
                "outbox_id": 44,
                "msg_id": failed.msg_id,
                "subject": "books.created",
                "payload_bytes": 3000,
                "payload_sha256": hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&failed.payload)),
            })
        );
        assert_eq!(
            letter.reason,
            "too large; the payload of 3000 bytes is not stored"
        );
        assert_eq!(letter.msg_id(), whole.msg_id());
        assert_eq!(
            (letter.subject, letter.original_subject, letter.headers),
            (whole.subject, whole.original_subject, whole.headers)
        );
    }

    #[test]
    fn a_dead_letter_is_stored_whole_when_the_server_carries_it() {
        let failed = PendingRow {
            payload: vec![b'x'; 3000],
            ..row(44, OutboxKind::Event, "books.created")
        };
        let whole = dead_letter("harbor", &failed, GivenUp::Exhausted, "no room");
        let size = wire_size(whole.payload.len(), &whole.publish_headers());
        assert!(size > failed.wire_size(), "the queue records what it was");
        for carried in [None, Some(size)] {
            assert_eq!(
                storable(whole.clone(), &failed, carried),
                (whole.clone(), true),
                "{carried:?}"
            );
        }
        let (letter, is_whole) = storable(whole.clone(), &failed, Some(size - 1));
        assert!(!is_whole);
        assert_eq!(letter, truncated(whole, &failed));
    }

    #[test]
    fn a_dead_letter_without_its_payload_is_small_whatever_its_row_holds() {
        let long = "é".repeat(1_000_000);
        let forged = PendingRow {
            service: long.clone(),
            subject: long.clone(),
            headers: vec![("X-Long".into(), long.clone())],
            payload: long.clone().into_bytes(),
            ..row(45, OutboxKind::Event, "books.created")
        };
        let whole = dead_letter("harbor", &forged, GivenUp::Rejected, &long);
        assert_eq!(
            whole.subject,
            format!("cufflink.dlq.harbor.{}.outbox.rejected", "_".repeat(32))
        );
        let (letter, is_whole) = storable(whole, &forged, Some(1024 * 1024));
        assert!(!is_whole);
        assert!(letter.headers.is_empty());
        assert_eq!(letter.original_subject, "é".repeat(512));
        assert_eq!(
            letter.reason,
            format!(
                "{}; the payload of 2000000 bytes is not stored nor its headers",
                "é".repeat(512)
            )
        );
        let size = wire_size(letter.payload.len(), &letter.publish_headers());
        assert!(size < 8 * 1024, "{size}");
    }
}
