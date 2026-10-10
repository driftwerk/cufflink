//! Jobs recorded as they are enqueued.

use crate::host::jobs::JobsHost;
use crate::jobs::JobOptions;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

/// A job as it was enqueued.
#[derive(Debug, Clone, PartialEq)]
pub struct EnqueuedJob {
    pub id: String,
    pub handler: String,
    pub payload: Value,
    pub options: JobOptions,
}

#[derive(Default)]
struct Recorded {
    enqueued: Vec<EnqueuedJob>,
    refused: BTreeSet<String>,
}

/// Records every job enqueued and answers each with an id of its own,
/// the first `00000000-0000-0000-0000-000000000001`. Clones share one
/// record.
#[derive(Clone, Default)]
pub struct JobRecorder {
    recorded: Rc<RefCell<Recorded>>,
}

impl JobRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Refuse every job of `handler`, as the platform refuses one when it
    /// cannot record it or the tenant's backlog is full.
    pub fn refuse(self, handler: &str) -> Self {
        self.recorded
            .borrow_mut()
            .refused
            .insert(handler.to_string());
        self
    }

    /// The jobs enqueued, in order.
    pub fn enqueued(&self) -> Vec<EnqueuedJob> {
        self.recorded.borrow().enqueued.clone()
    }
}

impl JobsHost for JobRecorder {
    fn enqueue(&self, handler: &str, payload: &Value, options: JobOptions) -> Option<String> {
        let mut recorded = self.recorded.borrow_mut();
        if handler.is_empty() || recorded.refused.contains(handler) {
            return None;
        }
        let id = format!(
            "00000000-0000-0000-0000-{:012}",
            recorded.enqueued.len() + 1
        );
        recorded.enqueued.push(EnqueuedJob {
            id: id.clone(),
            handler: handler.to_string(),
            payload: payload.clone(),
            options,
        });
        Some(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::{self, HostSet};
    use crate::jobs;
    use serde_json::json;

    fn hosts(recorder: &JobRecorder) -> HostSet {
        HostSet {
            jobs: Some(Rc::new(recorder.clone())),
            ..Default::default()
        }
    }

    #[test]
    fn a_job_enqueued_is_recorded_and_answered_with_its_id() {
        let recorder = JobRecorder::new();
        let ids = host::with(hosts(&recorder), || {
            [
                jobs::enqueue("handle_backfill_chunk", &json!({"chunk_id": "c-1"})),
                jobs::enqueue_with(
                    "handle_backfill_chunk",
                    &json!({"chunk_id": "c-2"}),
                    jobs::JobOptions {
                        timeout_secs: 600,
                        max_attempts: 2,
                    },
                ),
            ]
        });
        assert_eq!(
            ids,
            [
                Some("00000000-0000-0000-0000-000000000001".to_string()),
                Some("00000000-0000-0000-0000-000000000002".to_string()),
            ]
        );
        assert_eq!(
            recorder.enqueued(),
            [
                EnqueuedJob {
                    id: "00000000-0000-0000-0000-000000000001".into(),
                    handler: "handle_backfill_chunk".into(),
                    payload: json!({"chunk_id": "c-1"}),
                    options: jobs::JobOptions::default(),
                },
                EnqueuedJob {
                    id: "00000000-0000-0000-0000-000000000002".into(),
                    handler: "handle_backfill_chunk".into(),
                    payload: json!({"chunk_id": "c-2"}),
                    options: jobs::JobOptions {
                        timeout_secs: 600,
                        max_attempts: 2,
                    },
                },
            ]
        );
    }

    #[test]
    fn a_refused_job_is_none_as_on_the_platform_and_is_not_recorded() {
        let recorder = JobRecorder::new().refuse("handle_run_report");
        let (refused, unnamed, accepted) = host::with(hosts(&recorder), || {
            (
                jobs::enqueue("handle_run_report", &json!({})),
                jobs::enqueue("", &json!({})),
                jobs::enqueue("handle_backfill_chunk", &json!({})),
            )
        });
        assert_eq!((refused, unnamed), (None, None));
        assert_eq!(
            accepted.as_deref(),
            Some("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(recorder.enqueued().len(), 1);
    }

    #[test]
    fn without_a_jobs_host_nothing_is_enqueued() {
        assert_eq!(jobs::enqueue("handle_chunk", &json!({})), None);
        host::with(HostSet::default(), || {
            assert_eq!(jobs::enqueue("handle_chunk", &json!({})), None)
        });
    }
}
