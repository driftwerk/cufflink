use super::subjects::subject_matches;
use crate::shutdown::Shutdown;
use async_nats::jetstream::{self, stream::RetentionPolicy};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};
use tracing::warn;

pub const STREAMS_CHANGED_SUBJECT: &str = "cufflink.streams.changed";

const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const REGISTRY_REFRESH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MISS_REFRESH_EVERY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub name: String,
    pub subjects: Vec<String>,
    pub retention: RetentionPolicy,
}

impl StreamEntry {
    pub fn captures(&self, subject: &str) -> bool {
        self.subjects
            .iter()
            .any(|pattern| subject_matches(pattern, subject))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRef {
    pub name: String,
    pub retention: RetentionPolicy,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamSnapshot(Arc<Vec<StreamEntry>>);

impl StreamSnapshot {
    pub fn new(streams: Vec<StreamEntry>) -> Self {
        Self(Arc::new(streams))
    }

    pub async fn read(js: &jetstream::Context) -> eyre::Result<Self> {
        let mut streams = js.streams();
        let mut found = Vec::new();
        while let Some(info) = streams.next().await {
            let config = info
                .map_err(|e| eyre::eyre!("failed to enumerate JetStream streams: {}", e))?
                .config;
            found.push(StreamEntry {
                name: config.name,
                subjects: config.subjects,
                retention: config.retention,
            });
        }
        Ok(Self::new(found))
    }

    pub fn get(&self, name: &str) -> Option<&StreamEntry> {
        self.0.iter().find(|stream| stream.name == name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &StreamEntry> {
        self.0.iter()
    }

    pub fn capturing(&self, subject: &str) -> Option<StreamRef> {
        self.iter()
            .find(|stream| stream.captures(subject))
            .map(|stream| StreamRef {
                name: stream.name.clone(),
                retention: stream.retention,
            })
    }
}

#[derive(Default)]
struct Shared {
    current: RwLock<StreamSnapshot>,
    syncing: AtomicBool,
    missed: Mutex<Option<Instant>>,
}

#[derive(Clone, Default)]
pub struct StreamIndex {
    shared: Arc<Shared>,
}

impl StreamIndex {
    pub fn snapshot(&self) -> StreamSnapshot {
        self.shared
            .current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set(&self, snapshot: StreamSnapshot) {
        *self
            .shared
            .current
            .write()
            .unwrap_or_else(PoisonError::into_inner) = snapshot;
    }

    pub fn capturing(&self, subject: &str) -> Option<StreamRef> {
        self.snapshot().capturing(subject)
    }

    pub async fn refresh(&self, js: &jetstream::Context) -> eyre::Result<StreamSnapshot> {
        let snapshot = StreamSnapshot::read(js).await?;
        self.set(snapshot.clone());
        Ok(snapshot)
    }

    fn asks_on_a_miss(&self, now: Instant) -> bool {
        let mut missed = self
            .shared
            .missed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let due = missed.is_none_or(|at| now.duration_since(at) >= MISS_REFRESH_EVERY);
        if due {
            *missed = Some(now);
        }
        due
    }

    pub async fn capturing_or_refresh(
        &self,
        js: &jetstream::Context,
        subject: &str,
        within: Duration,
    ) -> Option<StreamRef> {
        if let Some(stream) = self.capturing(subject) {
            return Some(stream);
        }
        if !self.asks_on_a_miss(Instant::now()) {
            return None;
        }
        tokio::time::timeout(within, self.refresh(js))
            .await
            .ok()?
            .ok()?
            .capturing(subject)
    }

    pub fn start_sync(
        &self,
        shutdown: &Shutdown,
        nats: &async_nats::Client,
        js: &jetstream::Context,
    ) -> bool {
        if self.shared.syncing.swap(true, Ordering::AcqRel) {
            return false;
        }
        for (task, subject, interval) in [
            (
                "stream_index_sync",
                STREAMS_CHANGED_SUBJECT,
                REFRESH_INTERVAL,
            ),
            (
                "stream_index_registry_sync",
                crate::registry::REGISTRY_CHANGED_SUBJECT,
                REGISTRY_REFRESH_INTERVAL,
            ),
        ] {
            let (index, js) = (self.clone(), js.clone());
            crate::nats_conn::spawn_reload_loop(
                shutdown,
                task,
                nats.clone(),
                subject,
                interval,
                move || {
                    let (index, js) = (index.clone(), js.clone());
                    async move {
                        if let Err(e) = index.refresh(&js).await {
                            warn!(error = %e, "Could not refresh the stream index; keeping the last one");
                        }
                    }
                },
            );
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(name: &str, subjects: &[&str], retention: RetentionPolicy) -> StreamEntry {
        StreamEntry {
            name: name.into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            retention,
        }
    }

    fn snapshot() -> StreamSnapshot {
        StreamSnapshot::new(vec![
            stream(
                "CUFFLINK_TICKS",
                &["cufflink.tick.*"],
                RetentionPolicy::Interest,
            ),
            stream(
                "DW_ACME",
                &["dw.acme.notes.created", "dw.acme.books.>"],
                RetentionPolicy::Interest,
            ),
            stream("RECEIPTS", &["receipts.>"], RetentionPolicy::WorkQueue),
        ])
    }

    #[test]
    fn a_subject_is_captured_by_the_stream_whose_pattern_matches_it() {
        let streams = snapshot();
        for (subject, expected) in [
            (
                "cufflink.tick.hourly",
                Some(("CUFFLINK_TICKS", RetentionPolicy::Interest)),
            ),
            (
                "dw.acme.notes.created",
                Some(("DW_ACME", RetentionPolicy::Interest)),
            ),
            (
                "dw.acme.books.status.changed",
                Some(("DW_ACME", RetentionPolicy::Interest)),
            ),
            (
                "receipts.pdf.requested",
                Some(("RECEIPTS", RetentionPolicy::WorkQueue)),
            ),
            ("dw.acme.notes.deleted", None),
            ("receipts", None),
            ("cufflink.tick.hourly.extra", None),
        ] {
            assert_eq!(
                streams.capturing(subject),
                expected.map(|(name, retention)| StreamRef {
                    name: name.into(),
                    retention
                }),
                "{subject}"
            );
        }
        assert!(streams
            .get("RECEIPTS")
            .is_some_and(|s| s.captures("receipts.a")));
        assert!(streams.get("BOOKS").is_none());
        assert_eq!(streams.iter().count(), 3);
    }

    #[test]
    fn an_index_answers_from_its_latest_snapshot() {
        let index = StreamIndex::default();
        assert_eq!(index.capturing("receipts.a"), None);
        index.set(snapshot());
        assert_eq!(
            index.capturing("receipts.a").map(|s| s.name),
            Some("RECEIPTS".to_string())
        );
        let shared = index.clone();
        index.set(StreamSnapshot::default());
        assert_eq!(
            shared.capturing("receipts.a"),
            None,
            "clones share the index"
        );
    }

    #[test]
    fn the_server_is_asked_about_a_miss_once_a_second() {
        let index = StreamIndex::default();
        let first = Instant::now();
        assert!(index.asks_on_a_miss(first));
        assert!(!index
            .clone()
            .asks_on_a_miss(first + Duration::from_millis(999)));
        assert!(index.asks_on_a_miss(first + MISS_REFRESH_EVERY));
        assert!(
            StreamIndex::default().asks_on_a_miss(first),
            "each index keeps its own time"
        );
    }

    #[tokio::test]
    async fn a_listing_that_captures_the_subject_asks_nothing_of_the_server() {
        let index = StreamIndex::default();
        index.set(snapshot());
        let nowhere = jetstream::new(
            crate::nats_conn::connect("nats://127.0.0.1:1", "cufflink-test", &Default::default())
                .await
                .expect("client"),
        );
        assert_eq!(
            index
                .capturing_or_refresh(&nowhere, "receipts.a", Duration::ZERO)
                .await
                .map(|stream| stream.name),
            Some("RECEIPTS".to_string())
        );
        let asked = Instant::now();
        assert_eq!(
            index
                .capturing_or_refresh(&nowhere, "orders.a", Duration::from_millis(50))
                .await,
            None,
            "a server that does not answer is waited for no longer than allowed"
        );
        assert!(
            asked.elapsed() < Duration::from_secs(1),
            "{:?}",
            asked.elapsed()
        );
        assert!(!index.asks_on_a_miss(Instant::now()), "and was asked");
    }

    #[tokio::test]
    async fn the_sync_is_started_once() {
        let index = StreamIndex::default();
        let shutdown = Shutdown::new(Duration::from_secs(1));
        let nats =
            crate::nats_conn::connect("nats://127.0.0.1:1", "cufflink-test", &Default::default())
                .await
                .expect("client");
        let js = jetstream::new(nats.clone());
        assert!(index.start_sync(&shutdown, &nats, &js));
        assert!(!index.start_sync(&shutdown, &nats, &js));
        assert!(
            !index.clone().start_sync(&shutdown, &nats, &js),
            "a clone is the same index"
        );
        shutdown.trigger();
        assert_eq!(
            shutdown.drain_tasks().await,
            crate::shutdown::DrainOutcome::Completed
        );
    }
}
