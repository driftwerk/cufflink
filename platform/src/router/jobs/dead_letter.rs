use super::subject_for;
use crate::nats_manager::dlq::{dlq_subject, publish_dead_letter, DeadLetter, DlqKind};
use crate::nats_manager::events::LONG_JOBS_STREAM;
use crate::repo::wasm_jobs::{self, WasmJob};
use crate::AppState;
use chrono::{DateTime, Utc};
use serde_json::json;
use tracing::warn;

pub fn letter_of(job: &WasmJob, at: DateTime<Utc>) -> DeadLetter {
    let attempts = u64::try_from(job.attempts).unwrap_or_default();
    DeadLetter {
        subject: dlq_subject(
            &job.tenant_slug,
            &job.service_slug,
            DlqKind::Job,
            &job.handler,
        ),
        payload: json!({
            "job_id": job.id,
            "handler": job.handler,
            "attempts": job.attempts,
            "error": job.error,
            "error_class": job.error_class,
        })
        .to_string()
        .into(),
        headers: Vec::new(),
        reason: job.error.clone().unwrap_or_default(),
        stream: LONG_JOBS_STREAM.to_string(),
        original_subject: subject_for(&job.tenant_slug, &job.service_slug, &job.handler),
        sequence: attempts,
        deliveries: attempts,
        consumer: job.id.to_string(),
        at,
    }
}

pub async fn file(state: &AppState, job: &WasmJob) {
    if let Err(e) = publish_dead_letter(&state.jetstream, &letter_of(job, Utc::now())).await {
        warn!(job_id = %job.id, error = %e, "Could not file a job's dead letter; the reclaimer files it");
        return;
    }
    if let Err(e) = wasm_jobs::set_dead_lettered(&state.platform_db, job.id).await {
        warn!(job_id = %job.id, error = %e, "Could not record a job's dead letter as filed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::wasm_jobs::JobStatus;
    use crate::router::jobs::state::tests::job;
    use serde_json::Value;

    fn failed(attempts: i32) -> WasmJob {
        WasmJob {
            error: Some("Handler exceeded timeout (900s)".into()),
            error_class: Some("timeout".into()),
            ..job(JobStatus::Failed, attempts, 3)
        }
    }

    #[test]
    fn a_jobs_letter_says_what_failed_and_why() {
        let at = DateTime::<Utc>::UNIX_EPOCH;
        let letter = letter_of(&failed(2), at);
        assert_eq!(
            letter.subject,
            "cufflink.dlq.acme.stock-service.job.handle_run_report"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&letter.payload).expect("json"),
            json!({
                "job_id": "00000000-0000-0000-0000-000000000000",
                "handler": "handle_run_report",
                "attempts": 2,
                "error": "Handler exceeded timeout (900s)",
                "error_class": "timeout",
            })
        );
        assert_eq!(letter.reason, "Handler exceeded timeout (900s)");
        assert_eq!(letter.stream, "CUFFLINK_LONG_JOBS");
        assert_eq!(
            letter.original_subject,
            "cufflink.jobs.acme.stock-service.handle_run_report"
        );
        assert_eq!((letter.deliveries, letter.at), (2, at));
    }

    #[test]
    fn a_job_has_one_letter_for_each_time_it_fails() {
        let at = DateTime::<Utc>::UNIX_EPOCH;
        assert_eq!(
            letter_of(&failed(2), at).msg_id(),
            "dlq:CUFFLINK_LONG_JOBS:00000000-0000-0000-0000-000000000000:2"
        );
        assert_eq!(
            letter_of(&failed(2), at).msg_id(),
            letter_of(&failed(2), Utc::now()).msg_id(),
            "filed again, it is stored once"
        );
        assert_ne!(
            letter_of(&failed(2), at).msg_id(),
            letter_of(&failed(3), at).msg_id(),
            "failed again after a retry, it has another"
        );
    }
}
