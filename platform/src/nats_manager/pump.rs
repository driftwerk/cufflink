use super::delivery::{result_label, Decision, Outcome};
use super::dlq::DeadLetter;
use crate::nats_conn::{
    nak_pending, next_or_hand_back, start_fetch, BatchStep, FetchError, HandBack, NAK_DRAIN_BUDGET,
};
use crate::shutdown::Shutdown;
use async_nats::jetstream::{self, consumer::PullConsumer, AckKind};
use chrono::{DateTime, Utc};
use cufflink_telemetry::GaugeGuard;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

const DEAD_LETTER_RETRY: Duration = Duration::from_secs(30);
const MIN_HEARTBEAT: Duration = Duration::from_secs(1);
const GONE_AFTER: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMeta {
    pub subject: String,
    pub stream: String,
    pub consumer: String,
    pub stream_sequence: u64,
    pub consumer_sequence: u64,
    pub delivered: u64,
    pub pending: u64,
    pub published: DateTime<Utc>,
    pub headers: Vec<(String, Vec<String>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionLabel {
    pub tenant: String,
    pub service: String,
    pub handler: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkLabel {
    pub kind: &'static str,
    pub subscription: Option<SubscriptionLabel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settle {
    Ack,
    Nak(Duration),
    Progress,
    Term,
}

pub trait Delivery: HandBack + Sync + 'static {
    fn meta(&self) -> Result<MessageMeta, String>;
    fn settle(&self, settle: Settle) -> BoxFuture<'_, Result<(), String>>;
}

pub trait Work<M: Delivery = jetstream::Message>: Send + Sync + 'static {
    fn label(&self) -> WorkLabel;

    fn handle(&self, msg: Arc<M>, meta: MessageMeta) -> BoxFuture<'static, Outcome>;

    fn decide(&self, outcome: &Outcome, meta: &MessageMeta) -> Decision;

    fn dead_letter<'a>(
        &'a self,
        msg: &'a M,
        meta: &'a MessageMeta,
        reason: &'a str,
    ) -> BoxFuture<'a, Option<DeadLetter>>;
}

pub type Batch<M> = BoxStream<'static, Result<M, String>>;

pub trait Source: Send + Sync + 'static {
    type Message: Delivery;

    fn fetch(
        &self,
        max: usize,
        expires: Duration,
    ) -> BoxFuture<'_, Result<Batch<Self::Message>, FetchError>>;
}

pub trait DeadLetters: Send + Sync + 'static {
    fn publish<'a>(&'a self, letter: &'a DeadLetter) -> BoxFuture<'a, Result<(), String>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PumpConfig {
    pub concurrency: usize,
    pub batch: usize,
    pub heartbeat: Option<Duration>,
    pub fetch_expires: Duration,
}

impl PumpConfig {
    pub fn sequential(ack_wait: Duration) -> Self {
        Self {
            concurrency: 1,
            batch: 1,
            heartbeat: Some(heartbeat_for(ack_wait)),
            fetch_expires: crate::nats_conn::FETCH_EXPIRES,
        }
    }

    pub fn concurrent(self, concurrency: usize) -> Self {
        Self {
            concurrency,
            batch: concurrency,
            ..self
        }
    }
}

pub fn heartbeat_for(ack_wait: Duration) -> Duration {
    (ack_wait / 3).max(MIN_HEARTBEAT)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpExit {
    Stopped,
    ShuttingDown,
    ConsumerGone,
}

#[derive(Debug, Clone, Default)]
pub struct Stop {
    stopped: CancellationToken,
    replaced: Arc<AtomicBool>,
}

impl Stop {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stop(&self) {
        self.stopped.cancel();
    }

    pub fn replace(&self) {
        self.replaced.store(true, Ordering::SeqCst);
        self.stopped.cancel();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.is_cancelled()
    }

    pub fn is_replaced(&self) -> bool {
        self.replaced.load(Ordering::SeqCst)
    }
}

impl From<CancellationToken> for Stop {
    fn from(stopped: CancellationToken) -> Self {
        Self {
            stopped,
            replaced: Arc::default(),
        }
    }
}

fn fetch_size(batch: usize, free_beside_held: usize) -> usize {
    batch.clamp(1, 1 + free_beside_held)
}

pub(crate) fn fetch_failure_backoff(consecutive_failures: u32) -> Duration {
    Duration::from_secs(u64::from(consecutive_failures.clamp(1, 5)))
}

fn is_gone(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    [
        "no responders",
        "503",
        "consumer deleted",
        "consumer not found",
        "stream not found",
    ]
    .iter()
    .any(|sign| error.contains(sign))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Failures {
    consecutive: u32,
    gone: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AfterFailure {
    RetryIn(Duration),
    Gone,
}

impl Failures {
    fn after(self, error: &str) -> (Self, AfterFailure) {
        let failures = Self {
            consecutive: self.consecutive.saturating_add(1),
            gone: if is_gone(error) {
                self.gone.saturating_add(1)
            } else {
                0
            },
        };
        let next = if failures.gone >= GONE_AFTER {
            AfterFailure::Gone
        } else {
            AfterFailure::RetryIn(fetch_failure_backoff(failures.consecutive))
        };
        (failures, next)
    }
}

async fn halted(stop: &Stop, shutdown: &Shutdown) -> PumpExit {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => PumpExit::ShuttingDown,
        _ = stop.stopped.cancelled() => PumpExit::Stopped,
    }
}

struct Pump<M: Delivery> {
    config: PumpConfig,
    work: Arc<dyn Work<M>>,
    dead_letters: Arc<dyn DeadLetters>,
    label: WorkLabel,
    shutdown: Shutdown,
}

impl<M: Delivery> Clone for Pump<M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config,
            work: Arc::clone(&self.work),
            dead_letters: Arc::clone(&self.dead_letters),
            label: self.label.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<M: Delivery> Pump<M> {
    fn count(&self, result: &'static str, messages: u64) {
        if let Some(labels) = &self.label.subscription {
            metrics::counter!(
                "cufflink_subscription_messages_total",
                "tenant" => labels.tenant.clone(),
                "service" => labels.service.clone(),
                "handler" => labels.handler.clone(),
                "result" => result,
            )
            .increment(messages);
        }
    }

    async fn handle(&self, msg: &Arc<M>, meta: &MessageMeta) -> Outcome {
        let mut handling = self.work.handle(Arc::clone(msg), meta.clone());
        let Some(every) = self.config.heartbeat else {
            return handling.await;
        };
        let mut beat = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                outcome = &mut handling => return outcome,
                _ = beat.tick() => {
                    metrics::counter!("cufflink_pump_heartbeats_total", "kind" => self.label.kind)
                        .increment(1);
                    if let Err(e) = msg.settle(Settle::Progress).await {
                        warn!(kind = self.label.kind, error = %e, "Could not extend a message's ack wait");
                    }
                }
            }
        }
    }

    async fn settle(&self, msg: &M, meta: &MessageMeta, decision: &Decision) {
        let settled = match decision {
            Decision::Ack => msg.settle(Settle::Ack).await,
            Decision::Nak(delay) => msg.settle(Settle::Nak(*delay)).await,
            Decision::Term { .. } => msg.settle(Settle::Term).await,
            Decision::DeadLetterThenTerm { reason } => {
                match self.work.dead_letter(msg, meta, reason).await {
                    None => msg.settle(Settle::Term).await,
                    Some(letter) => match self.dead_letters.publish(&letter).await {
                        Ok(()) => msg.settle(Settle::Term).await,
                        Err(_) => msg.settle(Settle::Nak(DEAD_LETTER_RETRY)).await,
                    },
                }
            }
        };
        if let Err(e) = settled {
            warn!(
                kind = self.label.kind,
                stream = %meta.stream,
                sequence = meta.stream_sequence,
                error = %e,
                "Could not settle a message; the server redelivers it"
            );
        }
    }

    async fn deliver(self, msg: M, _slot: OwnedSemaphorePermit) {
        let _in_flight =
            GaugeGuard::enter(metrics::gauge!("cufflink_pump_inflight", "kind" => self.label.kind));
        let meta = match msg.meta() {
            Ok(meta) => meta,
            Err(e) => {
                error!(kind = self.label.kind, error = %e, "A delivery without JetStream metadata; ending it");
                let _ = msg.settle(Settle::Term).await;
                return;
            }
        };
        let msg = Arc::new(msg);
        let started = Instant::now();
        let outcome = self.handle(&msg, &meta).await;
        metrics::histogram!(
            "cufflink_subscription_handler_duration_seconds",
            "kind" => self.label.kind
        )
        .record(started.elapsed().as_secs_f64());
        let decision = self.work.decide(&outcome, &meta);
        self.settle(&msg, &meta, &decision).await;
        self.count(result_label(&outcome, &decision), 1);
    }
}

enum BatchEnd {
    Drained,
    Failed(String),
    Exit(PumpExit),
}

pub async fn run<S: Source>(
    source: S,
    config: PumpConfig,
    work: Arc<dyn Work<S::Message>>,
    dead_letters: Arc<dyn DeadLetters>,
    stop: impl Into<Stop>,
    shutdown: Shutdown,
) -> PumpExit {
    let stop = stop.into();
    let pump = Pump {
        config,
        label: work.label(),
        work,
        dead_letters,
        shutdown: shutdown.clone(),
    };
    let slots = Arc::new(Semaphore::new(config.concurrency.max(1)));
    let mut failures = Failures::default();
    loop {
        let held = tokio::select! {
            biased;
            exit = halted(&stop, &shutdown) => return exit,
            slot = Arc::clone(&slots).acquire_owned() => match slot {
                Ok(slot) => slot,
                Err(_) => return PumpExit::Stopped,
            },
        };
        let wanted = fetch_size(config.batch, slots.available_permits());
        let fetched = tokio::select! {
            biased;
            exit = halted(&stop, &shutdown) => return exit,
            fetched = source.fetch(wanted, config.fetch_expires) => fetched,
        };
        let end = match fetched {
            Ok(batch) => drain(&pump, batch, held, &slots, &stop).await,
            Err(e) => BatchEnd::Failed(e.to_string()),
        };
        match end {
            BatchEnd::Drained => failures = Failures::default(),
            BatchEnd::Exit(exit) => return exit,
            BatchEnd::Failed(e) => {
                let (next, after) = failures.after(&e);
                failures = next;
                warn!(kind = pump.label.kind, error = %e, "A fetch failed");
                match after {
                    AfterFailure::Gone => return PumpExit::ConsumerGone,
                    AfterFailure::RetryIn(delay) => {
                        tokio::select! {
                            biased;
                            exit = halted(&stop, &shutdown) => return exit,
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                }
            }
        }
    }
}

async fn drain<M: Delivery>(
    pump: &Pump<M>,
    mut batch: Batch<M>,
    held: OwnedSemaphorePermit,
    slots: &Arc<Semaphore>,
    stop: &Stop,
) -> BatchEnd {
    let mut held = Some(held);
    let mut replaced = None;
    loop {
        let step = match replaced {
            None => tokio::select! {
                biased;
                step = next_or_hand_back(&mut batch, &pump.shutdown) => step,
                _ = stop.stopped.cancelled() => {
                    if !stop.is_replaced() {
                        pump.count("nak", nak_pending(&mut batch).await as u64);
                        return BatchEnd::Exit(PumpExit::Stopped);
                    }
                    replaced = Some(tokio::time::Instant::now() + NAK_DRAIN_BUDGET);
                    continue;
                }
            },
            Some(until) => {
                match tokio::time::timeout_at(until, next_or_hand_back(&mut batch, &pump.shutdown))
                    .await
                {
                    Ok(step) => step,
                    Err(_) => return BatchEnd::Exit(PumpExit::Stopped),
                }
            }
        };
        match step {
            BatchStep::Message(msg) => {
                let slot = match held.take() {
                    Some(slot) => slot,
                    None => match Arc::clone(slots).acquire_owned().await {
                        Ok(slot) => slot,
                        Err(_) => return BatchEnd::Exit(PumpExit::Stopped),
                    },
                };
                pump.shutdown
                    .spawn("pump_delivery", pump.clone().deliver(*msg, slot));
            }
            BatchStep::HandedBack(handed_back) => {
                pump.count("nak", handed_back as u64);
                return BatchEnd::Exit(PumpExit::ShuttingDown);
            }
            BatchStep::End | BatchStep::Failed(_) if replaced.is_some() => {
                return BatchEnd::Exit(PumpExit::Stopped)
            }
            BatchStep::End => return BatchEnd::Drained,
            BatchStep::Failed(e) => return BatchEnd::Failed(e),
        }
    }
}

impl Delivery for jetstream::Message {
    fn meta(&self) -> Result<MessageMeta, String> {
        let info = self.info().map_err(|e| e.to_string())?;
        Ok(MessageMeta {
            subject: self.subject.to_string(),
            stream: info.stream.to_string(),
            consumer: info.consumer.to_string(),
            stream_sequence: info.stream_sequence,
            consumer_sequence: info.consumer_sequence,
            delivered: u64::try_from(info.delivered).unwrap_or(1),
            pending: info.pending,
            published: DateTime::from_timestamp(
                info.published.unix_timestamp(),
                info.published.nanosecond(),
            )
            .unwrap_or_default(),
            headers: self.headers.as_ref().map(header_values).unwrap_or_default(),
        })
    }

    fn settle(&self, settle: Settle) -> BoxFuture<'_, Result<(), String>> {
        let kind = match settle {
            Settle::Ack => AckKind::Ack,
            Settle::Nak(delay) if delay.is_zero() => AckKind::Nak(None),
            Settle::Nak(delay) => AckKind::Nak(Some(delay)),
            Settle::Progress => AckKind::Progress,
            Settle::Term => AckKind::Term,
        };
        async move { self.ack_with(kind).await.map_err(|e| e.to_string()) }.boxed()
    }
}

fn header_values(headers: &async_nats::HeaderMap) -> Vec<(String, Vec<String>)> {
    let mut named: Vec<(String, Vec<String>)> = headers
        .iter()
        .map(|(name, values)| {
            (
                name.to_string(),
                values.iter().map(|value| value.to_string()).collect(),
            )
        })
        .collect();
    named.sort();
    named
}

impl Source for PullConsumer {
    type Message = jetstream::Message;

    fn fetch(
        &self,
        max: usize,
        expires: Duration,
    ) -> BoxFuture<'_, Result<Batch<jetstream::Message>, FetchError>> {
        async move {
            start_fetch(self.batch().max_messages(max).expires(expires).messages())
                .await
                .map(|batch| batch.map(|item| item.map_err(|e| e.to_string())).boxed())
        }
        .boxed()
    }
}

impl DeadLetters for jetstream::Context {
    fn publish<'a>(&'a self, letter: &'a DeadLetter) -> BoxFuture<'a, Result<(), String>> {
        async move {
            super::dlq::publish_dead_letter(self, letter)
                .await
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Event {
        Fetched(usize),
        Settled(u64, Settle),
        DeadLettered(u64),
    }

    type Log = Arc<Mutex<Vec<Event>>>;

    fn log_of(log: &Log) -> Vec<Event> {
        log.lock().expect("log").clone()
    }

    fn record(log: &Log, event: Event) {
        log.lock().expect("log").push(event);
    }

    struct FakeMessage {
        sequence: u64,
        delivered: u64,
        log: Log,
    }

    impl HandBack for FakeMessage {
        async fn hand_back(&self) -> bool {
            record(
                &self.log,
                Event::Settled(self.sequence, Settle::Nak(Duration::ZERO)),
            );
            true
        }
    }

    impl Delivery for FakeMessage {
        fn meta(&self) -> Result<MessageMeta, String> {
            Ok(MessageMeta {
                subject: "dw.acme.notes.created".into(),
                stream: "DW_ACME".into(),
                consumer: "notes_on_created".into(),
                stream_sequence: self.sequence,
                consumer_sequence: self.sequence,
                delivered: self.delivered,
                pending: 0,
                published: DateTime::UNIX_EPOCH,
                headers: Vec::new(),
            })
        }

        fn settle(&self, settle: Settle) -> BoxFuture<'_, Result<(), String>> {
            record(&self.log, Event::Settled(self.sequence, settle));
            async { Ok(()) }.boxed()
        }
    }

    enum Fetch {
        Messages(Vec<u64>),
        Trickle(Vec<(Duration, u64)>),
        Fails(&'static str),
    }

    struct FakeSource {
        fetches: Mutex<VecDeque<Fetch>>,
        delivered: u64,
        log: Log,
    }

    impl FakeSource {
        fn new(log: &Log, fetches: Vec<Fetch>) -> Self {
            Self {
                fetches: Mutex::new(fetches.into()),
                delivered: 1,
                log: Arc::clone(log),
            }
        }

        fn message(&self, sequence: u64) -> Result<FakeMessage, String> {
            Ok(FakeMessage {
                sequence,
                delivered: self.delivered,
                log: Arc::clone(&self.log),
            })
        }
    }

    impl Source for FakeSource {
        type Message = FakeMessage;

        fn fetch(
            &self,
            max: usize,
            _expires: Duration,
        ) -> BoxFuture<'_, Result<Batch<FakeMessage>, FetchError>> {
            record(&self.log, Event::Fetched(max));
            let next = self.fetches.lock().expect("fetches").pop_front();
            async move {
                match next {
                    Some(Fetch::Messages(sequences)) => Ok(futures_util::stream::iter(
                        sequences
                            .into_iter()
                            .take(max)
                            .map(|sequence| self.message(sequence))
                            .collect::<Vec<_>>(),
                    )
                    .boxed()),
                    Some(Fetch::Trickle(arrivals)) => Ok(futures_util::stream::iter(
                        arrivals
                            .into_iter()
                            .take(max)
                            .map(|(delay, sequence)| (delay, self.message(sequence)))
                            .collect::<Vec<_>>(),
                    )
                    .then(|(delay, message)| async move {
                        tokio::time::sleep(delay).await;
                        message
                    })
                    .chain(futures_util::stream::pending())
                    .boxed()),
                    Some(Fetch::Fails(error)) => Err(FetchError(error.to_string())),
                    None => Ok(futures_util::stream::pending().boxed()),
                }
            }
            .boxed()
        }
    }

    struct FakeWork {
        takes: Duration,
        panics_on: Option<u64>,
        outcome: Outcome,
        decision: Decision,
        running: Arc<AtomicUsize>,
        most_at_once: Arc<AtomicUsize>,
        handled: Arc<AtomicUsize>,
    }

    impl FakeWork {
        fn new(takes: Duration) -> Self {
            Self {
                takes,
                panics_on: None,
                outcome: Outcome::Ok,
                decision: Decision::Ack,
                running: Arc::default(),
                most_at_once: Arc::default(),
                handled: Arc::default(),
            }
        }
    }

    impl Work<FakeMessage> for FakeWork {
        fn label(&self) -> WorkLabel {
            WorkLabel {
                kind: "subscription",
                subscription: None,
            }
        }

        fn handle(&self, msg: Arc<FakeMessage>, _meta: MessageMeta) -> BoxFuture<'static, Outcome> {
            let panics = self.panics_on == Some(msg.sequence);
            let (running, most, handled) = (
                Arc::clone(&self.running),
                Arc::clone(&self.most_at_once),
                Arc::clone(&self.handled),
            );
            let (takes, outcome) = (self.takes, self.outcome.clone());
            async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(takes).await;
                running.fetch_sub(1, Ordering::SeqCst);
                assert!(!panics, "the handler panics");
                handled.fetch_add(1, Ordering::SeqCst);
                outcome
            }
            .boxed()
        }

        fn decide(&self, _outcome: &Outcome, _meta: &MessageMeta) -> Decision {
            self.decision.clone()
        }

        fn dead_letter<'a>(
            &'a self,
            _msg: &'a FakeMessage,
            meta: &'a MessageMeta,
            reason: &'a str,
        ) -> BoxFuture<'a, Option<DeadLetter>> {
            std::future::ready(Some(DeadLetter::of(
                "cufflink.dlq.acme.notes.sub.on_created".into(),
                bytes::Bytes::new(),
                meta,
                reason,
                DateTime::UNIX_EPOCH,
            )))
            .boxed()
        }
    }

    struct FakeQueue {
        accepts: bool,
        log: Log,
    }

    impl DeadLetters for FakeQueue {
        fn publish<'a>(&'a self, letter: &'a DeadLetter) -> BoxFuture<'a, Result<(), String>> {
            let accepted = self.accepts;
            if accepted {
                record(&self.log, Event::DeadLettered(letter.sequence));
            }
            async move {
                if accepted {
                    Ok(())
                } else {
                    Err("the queue is unavailable".to_string())
                }
            }
            .boxed()
        }
    }

    struct Running {
        log: Log,
        stop: Stop,
        shutdown: Shutdown,
        pump: tokio::task::JoinHandle<PumpExit>,
    }

    fn start(
        config: PumpConfig,
        work: FakeWork,
        fetches: Vec<Fetch>,
        queue_accepts: bool,
    ) -> Running {
        let log = Log::default();
        let stop = Stop::new();
        let shutdown = Shutdown::new(Duration::from_secs(30));
        let pump = tokio::spawn(run(
            FakeSource::new(&log, fetches),
            config,
            Arc::new(work),
            Arc::new(FakeQueue {
                accepts: queue_accepts,
                log: Arc::clone(&log),
            }),
            stop.clone(),
            shutdown.clone(),
        ));
        Running {
            log,
            stop,
            shutdown,
            pump,
        }
    }

    fn config(concurrency: usize, batch: usize, heartbeat: Option<Duration>) -> PumpConfig {
        PumpConfig {
            concurrency,
            batch,
            heartbeat,
            fetch_expires: Duration::from_secs(30),
        }
    }

    fn settled(log: &Log, settle: Settle) -> Vec<u64> {
        let mut sequences: Vec<u64> = log_of(log)
            .into_iter()
            .filter_map(|event| match event {
                Event::Settled(sequence, how) if how == settle => Some(sequence),
                _ => None,
            })
            .collect();
        sequences.sort_unstable();
        sequences
    }

    fn fetches(log: &Log) -> Vec<usize> {
        log_of(log)
            .into_iter()
            .filter_map(|event| match event {
                Event::Fetched(size) => Some(size),
                _ => None,
            })
            .collect()
    }

    async fn stop_and_join(running: Running) -> (PumpExit, Log) {
        end_and_join(running, Stop::stop).await
    }

    async fn end_and_join(running: Running, end: fn(&Stop)) -> (PumpExit, Log) {
        end(&running.stop);
        let exit = running.pump.await.expect("pump");
        running.shutdown.drain_tasks().await;
        (exit, running.log)
    }

    #[test]
    fn a_fetch_asks_for_a_message_per_free_slot() {
        assert_eq!(fetch_size(10, 0), 1);
        assert_eq!(fetch_size(10, 3), 4);
        assert_eq!(fetch_size(2, 7), 2);
        assert_eq!(fetch_size(0, 7), 1, "a batch of nothing still asks for one");
        assert_eq!(
            heartbeat_for(Duration::from_secs(30)),
            Duration::from_secs(10)
        );
        assert_eq!(
            heartbeat_for(Duration::from_secs(2)),
            Duration::from_secs(1)
        );
        assert_eq!(
            PumpConfig::sequential(Duration::from_secs(60)),
            config(1, 1, Some(Duration::from_secs(20)))
        );
        assert_eq!(
            PumpConfig::sequential(Duration::from_secs(60)).concurrent(8),
            config(8, 8, Some(Duration::from_secs(20)))
        );
    }

    #[test]
    fn a_consumer_is_gone_after_three_fetches_in_a_row_find_it_missing() {
        let missing = "error while processing messages from the stream: 503, None";
        let (first, after) = Failures::default().after(missing);
        assert_eq!(after, AfterFailure::RetryIn(Duration::from_secs(1)));
        let (second, after) = first.after("409, Some(\"Consumer Deleted\")");
        assert_eq!(after, AfterFailure::RetryIn(Duration::from_secs(2)));
        let (_, after) = second.after("no responders");
        assert_eq!(after, AfterFailure::Gone);

        let (interrupted, after) = second.after("fetch request timed out after 5s");
        assert_eq!(after, AfterFailure::RetryIn(Duration::from_secs(3)));
        assert_eq!(
            interrupted.gone, 0,
            "another failure starts the count again"
        );
        let transient = (0..20).fold(Failures::default(), |failures, _| {
            let (next, after) = failures.after("connection reset by peer");
            assert!(
                matches!(after, AfterFailure::RetryIn(delay) if delay <= Duration::from_secs(5))
            );
            next
        });
        assert_eq!(transient.consecutive, 20);
        for gone in ["stream not found", "Consumer Not Found", "consumer deleted"] {
            assert!(is_gone(gone), "{gone}");
        }
        assert!(!is_gone("fetch batch stalled"));
    }

    #[tokio::test(start_paused = true)]
    async fn no_more_messages_run_at_once_than_the_pump_has_slots() {
        let work = FakeWork::new(Duration::from_secs(2));
        let (most, handled) = (Arc::clone(&work.most_at_once), Arc::clone(&work.handled));
        let running = start(
            config(3, 8, None),
            work,
            vec![
                Fetch::Messages(vec![1, 2, 3]),
                Fetch::Messages(vec![4]),
                Fetch::Messages(vec![5, 6]),
                Fetch::Messages(vec![7]),
            ],
            true,
        );
        tokio::time::sleep(Duration::from_secs(20)).await;
        let (exit, log) = stop_and_join(running).await;
        assert_eq!(exit, PumpExit::Stopped);
        assert_eq!(handled.load(Ordering::SeqCst), 7);
        assert_eq!(most.load(Ordering::SeqCst), 3);
        assert_eq!(settled(&log, Settle::Ack), [1, 2, 3, 4, 5, 6, 7]);
        let sizes = fetches(&log);
        assert_eq!(sizes[0], 3, "every slot is free at first");
        assert!(
            sizes.iter().all(|size| (1..=3).contains(size)),
            "a fetch never asks for more than the slots allow: {sizes:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_sequential_pump_handles_one_message_at_a_time_in_order() {
        let work = FakeWork::new(Duration::from_secs(1));
        let most = Arc::clone(&work.most_at_once);
        let running = start(
            config(1, 1, None),
            work,
            vec![
                Fetch::Messages(vec![1]),
                Fetch::Messages(vec![2]),
                Fetch::Messages(vec![3]),
            ],
            true,
        );
        tokio::time::sleep(Duration::from_secs(10)).await;
        let (_, log) = stop_and_join(running).await;
        assert_eq!(most.load(Ordering::SeqCst), 1);
        assert_eq!(
            log_of(&log)
                .into_iter()
                .filter(|event| matches!(event, Event::Settled(..)))
                .collect::<Vec<_>>(),
            [
                Event::Settled(1, Settle::Ack),
                Event::Settled(2, Settle::Ack),
                Event::Settled(3, Settle::Ack)
            ]
        );
        assert!(fetches(&log).iter().all(|size| *size == 1));
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_handler_keeps_its_message_alive() {
        let running = start(
            config(1, 1, Some(Duration::from_secs(10))),
            FakeWork::new(Duration::from_secs(35)),
            vec![Fetch::Messages(vec![1])],
            true,
        );
        tokio::time::sleep(Duration::from_secs(40)).await;
        let (_, log) = stop_and_join(running).await;
        assert_eq!(
            log_of(&log)
                .into_iter()
                .filter(|event| matches!(event, Event::Settled(..)))
                .collect::<Vec<_>>(),
            [
                Event::Settled(1, Settle::Progress),
                Event::Settled(1, Settle::Progress),
                Event::Settled(1, Settle::Progress),
                Event::Settled(1, Settle::Ack)
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_quick_handler_sends_no_heartbeat() {
        let running = start(
            config(1, 1, Some(Duration::from_secs(10))),
            FakeWork::new(Duration::from_secs(9)),
            vec![Fetch::Messages(vec![1])],
            true,
        );
        tokio::time::sleep(Duration::from_secs(12)).await;
        let (_, log) = stop_and_join(running).await;
        assert!(settled(&log, Settle::Progress).is_empty());
        assert_eq!(settled(&log, Settle::Ack), [1]);
    }

    fn trickle() -> Vec<Fetch> {
        vec![Fetch::Trickle(vec![
            (Duration::ZERO, 1),
            (Duration::from_millis(1_500), 2),
        ])]
    }

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_hands_back_what_was_not_started_and_finishes_what_was() {
        let work = FakeWork::new(Duration::from_secs(5));
        let handled = Arc::clone(&work.handled);
        let running = start(config(2, 2, None), work, trickle(), true);
        tokio::time::sleep(Duration::from_secs(1)).await;
        running.shutdown.trigger();
        let exit = running.pump.await.expect("pump");
        assert_eq!(exit, PumpExit::ShuttingDown);
        running.shutdown.drain_tasks().await;
        assert_eq!(
            handled.load(Ordering::SeqCst),
            1,
            "the message in hand finishes"
        );
        assert_eq!(settled(&running.log, Settle::Ack), [1]);
        assert_eq!(
            settled(&running.log, Settle::Nak(Duration::ZERO)),
            [2],
            "the one that arrived after is handed back"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_pump_hands_back_the_rest_of_its_batch() {
        let work = FakeWork::new(Duration::from_secs(5));
        let handled = Arc::clone(&work.handled);
        let running = start(config(2, 2, None), work, trickle(), true);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (exit, log) = stop_and_join(running).await;
        assert_eq!(exit, PumpExit::Stopped);
        assert_eq!(handled.load(Ordering::SeqCst), 1);
        assert_eq!(settled(&log, Settle::Ack), [1]);
        assert_eq!(settled(&log, Settle::Nak(Duration::ZERO)), [2]);
        assert_eq!(fetches(&log), [2], "a stopped pump fetches nothing more");
    }

    #[tokio::test(start_paused = true)]
    async fn a_replaced_pump_handles_what_its_fetch_still_brings_and_fetches_nothing_more() {
        let work = FakeWork::new(Duration::from_secs(5));
        let handled = Arc::clone(&work.handled);
        let running = start(
            config(2, 2, None),
            work,
            vec![Fetch::Trickle(vec![
                (Duration::ZERO, 1),
                (Duration::from_millis(1_500), 2),
                (Duration::from_secs(2), 3),
            ])],
            true,
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (exit, log) = end_and_join(running, Stop::replace).await;
        assert_eq!(exit, PumpExit::Stopped);
        assert_eq!(
            handled.load(Ordering::SeqCst),
            2,
            "what arrives within the budget is handled, not handed back"
        );
        assert_eq!(settled(&log, Settle::Ack), [1, 2]);
        assert_eq!(
            settled(&log, Settle::Nak(Duration::ZERO)),
            Vec::<u64>::new(),
            "no delivery is spent handing a message back"
        );
        assert_eq!(fetches(&log), [2], "a replaced pump fetches nothing more");
    }

    #[tokio::test(start_paused = true)]
    async fn a_pump_gives_up_a_consumer_that_is_gone_and_not_one_that_is_unreachable() {
        let gone = start(
            config(1, 1, None),
            FakeWork::new(Duration::ZERO),
            vec![
                Fetch::Fails("connection reset by peer"),
                Fetch::Fails("no responders"),
                Fetch::Fails("no responders"),
                Fetch::Fails("no responders"),
            ],
            true,
        );
        assert_eq!(gone.pump.await.expect("pump"), PumpExit::ConsumerGone);
        assert_eq!(fetches(&gone.log).len(), 4);

        let unreachable = start(
            config(1, 1, None),
            FakeWork::new(Duration::ZERO),
            vec![
                Fetch::Fails("connection reset by peer"),
                Fetch::Fails("fetch request timed out after 5s"),
                Fetch::Fails("connection reset by peer"),
                Fetch::Fails("fetch batch stalled"),
                Fetch::Messages(vec![1]),
            ],
            true,
        );
        tokio::time::sleep(Duration::from_secs(60)).await;
        let (exit, log) = stop_and_join(unreachable).await;
        assert_eq!(exit, PumpExit::Stopped);
        assert_eq!(
            settled(&log, Settle::Ack),
            [1],
            "it fetches again once it can"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_that_panics_frees_its_slot() {
        let work = FakeWork {
            panics_on: Some(1),
            ..FakeWork::new(Duration::from_secs(1))
        };
        let running = start(
            config(1, 1, None),
            work,
            vec![Fetch::Messages(vec![1]), Fetch::Messages(vec![2])],
            true,
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
        let (exit, log) = stop_and_join(running).await;
        assert_eq!(exit, PumpExit::Stopped);
        assert_eq!(
            log_of(&log)
                .into_iter()
                .filter(|event| matches!(event, Event::Settled(..)))
                .collect::<Vec<_>>(),
            [Event::Settled(2, Settle::Ack)],
            "the message after it is handled; its own is left for the server to redeliver"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_decision_is_what_the_server_is_told() {
        for (decision, queue_accepts, expected) in [
            (Decision::Ack, true, vec![Event::Settled(1, Settle::Ack)]),
            (
                Decision::Nak(Duration::from_secs(30)),
                true,
                vec![Event::Settled(1, Settle::Nak(Duration::from_secs(30)))],
            ),
            (
                Decision::Term { reason: "r".into() },
                true,
                vec![Event::Settled(1, Settle::Term)],
            ),
            (
                Decision::DeadLetterThenTerm { reason: "r".into() },
                true,
                vec![Event::DeadLettered(1), Event::Settled(1, Settle::Term)],
            ),
            (
                Decision::DeadLetterThenTerm { reason: "r".into() },
                false,
                vec![Event::Settled(1, Settle::Nak(DEAD_LETTER_RETRY))],
            ),
        ] {
            let work = FakeWork {
                decision: decision.clone(),
                outcome: Outcome::Retry {
                    after: None,
                    reason: "r".into(),
                },
                ..FakeWork::new(Duration::ZERO)
            };
            let running = start(
                config(1, 1, None),
                work,
                vec![Fetch::Messages(vec![1])],
                queue_accepts,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            let (_, log) = stop_and_join(running).await;
            let told: Vec<Event> = log_of(&log)
                .into_iter()
                .filter(|event| !matches!(event, Event::Fetched(_)))
                .collect();
            assert_eq!(told, expected, "{decision:?} accepted={queue_accepts}");
        }
    }
}
