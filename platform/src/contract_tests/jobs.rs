use super::*;
use crate::repo::wasm_jobs::{JobStatus, WasmJob};
use crate::router::jobs::state::tests::job;
use crate::router::jobs::state::{classified, decide_job, Attempt, JobDecision, JobErrorClass};
use crate::wasm_runtime::ExecutionError;
use chrono::{DateTime, Utc};
use cufflink_types::{features, JobConfig};
use std::time::Duration;
use uuid::Uuid;

fn sample_jobs() -> [JobConfig; 2] {
    [(900, 2), (600, 2)].map(|(timeout_secs, max_attempts)| {
        serde_json::from_value(json!({
            "timeout_secs": timeout_secs,
            "max_attempts": max_attempts,
        }))
        .expect("job config")
    })
}

fn enqueued(config: &JobConfig, attempts: i32) -> WasmJob {
    WasmJob {
        timeout_secs: i32::try_from(config.timeout_secs).expect("timeout"),
        retry_on: config.retry_on.clone(),
        ..job(
            JobStatus::Running,
            attempts,
            i32::try_from(config.max_attempts).expect("attempts"),
        )
    }
}

fn after(job: &WasmJob, class: &JobErrorClass) -> JobDecision {
    decide_job(
        class,
        u32::try_from(job.attempts).expect("attempts"),
        u32::try_from(job.max_attempts).expect("attempts"),
        &job.retry_on,
        &job.backoff(),
        DateTime::<Utc>::UNIX_EPOCH,
        job.jitter_seed(),
    )
}

#[test]
fn a_job_route_demands_nothing_this_release_added() {
    let manifest = sample_manifest();
    let route = manifest.http_route("reprice").expect("job route");
    assert_eq!(
        route.job,
        Some(JobConfig {
            timeout_secs: 900,
            max_attempts: 1,
            retry_on: Vec::new(),
            backoff: None,
        })
    );
    assert!(route
        .derived_features()
        .all(|feature| feature != features::JOBS_V2));
    assert!(sample_jobs().iter().all(|config| !config.uses_v2()));
}

#[test]
fn a_handlers_error_or_timeout_is_never_attempted_again() {
    for config in sample_jobs() {
        let first = enqueued(&config, 1);
        for run in [
            Err(ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(900),
            }),
            Err(ExecutionError::Trap("unreachable".into())),
            Err(ExecutionError::MemoryLimit {
                requested: 2,
                limit: 1,
            }),
            Err(ExecutionError::Response("not a response".into())),
            Err(ExecutionError::HandlerNotFound(
                "handle_run_report_job".into(),
            )),
        ] {
            let Attempt::Failed { class, error } = classified(run, &first, false, 10) else {
                panic!("a failed attempt");
            };
            assert_eq!(after(&first, &class), JobDecision::Fail, "{error}");
        }
    }
}

#[test]
fn a_crash_is_attempted_again_while_attempts_remain() {
    for config in sample_jobs() {
        let crash = || Err(ExecutionError::Runtime("the worker's task ended".into()));
        let Attempt::Failed { class, .. } = classified(crash(), &enqueued(&config, 1), false, 10)
        else {
            panic!("a failed attempt");
        };
        assert_eq!(class, JobErrorClass::Crash);
        assert!(matches!(
            after(&enqueued(&config, 1), &class),
            JobDecision::Retry { .. }
        ));
        assert_eq!(after(&enqueued(&config, 2), &class), JobDecision::Fail);
    }
}

#[test]
fn what_the_handler_answered_is_the_jobs_result_whatever_it_says() {
    let [long, _] = sample_jobs();
    for answer in [
        json!({"item_id": "i-1", "status": "completed"}),
        json!({"__status": 400, "__body": {"error": "missing item id"}}),
        json!({"__status": 502, "__body": {"error": "failed to enqueue AI run — try again"}}),
        json!({"error": "budget exhausted"}),
    ] {
        assert_eq!(
            classified(Ok(answer.to_string()), &enqueued(&long, 1), false, 10),
            Attempt::Completed(answer)
        );
    }
}

#[test]
fn a_job_route_answers_202_with_the_jobs_id() {
    let id = Uuid::nil();
    assert_eq!(
        crate::router::jobs::accepted(id).to_string(),
        r#"{"job_id":"00000000-0000-0000-0000-000000000000","status":"pending"}"#
    );
}

#[test]
fn a_polled_job_is_answered_with_the_keys_it_always_was() {
    let started = DateTime::<Utc>::UNIX_EPOCH + chrono::Duration::seconds(1_800_000_000);
    let [long, _] = sample_jobs();
    let running = WasmJob {
        lease_owner: Some("worker-1".into()),
        lease_until: Some(started + chrono::Duration::seconds(60)),
        started_at: Some(started),
        last_error: Some("WASM runtime error: join".into()),
        error_class: Some("crash".into()),
        ..enqueued(&long, 2)
    };
    assert_eq!(
        crate::api::jobs::polled(&running),
        json!({
            "job_id": "00000000-0000-0000-0000-000000000000",
            "status": "running",
            "handler": "handle_run_report",
            "service": "stock-service",
            "attempts": 2,
            "max_attempts": 2,
            "result": null,
            "error": null,
            "created_at": "2027-01-15T08:00:00Z",
            "started_at": "2027-01-15T08:00:00Z",
            "finished_at": null,
        })
    );
    let completed = WasmJob {
        status: JobStatus::Completed,
        result_json: Some(json!({"ok": true})),
        finished_at: Some(started + chrono::Duration::seconds(30)),
        ..running.clone()
    };
    let polled = crate::api::jobs::polled(&completed);
    assert_eq!(polled["status"], "completed");
    assert_eq!(polled["result"], json!({"ok": true}));
    assert_eq!(polled["finished_at"], "2027-01-15T08:00:30Z");
    let failed = WasmJob {
        status: JobStatus::Failed,
        error: Some("Handler exceeded timeout (900s)".into()),
        ..running
    };
    let polled = crate::api::jobs::polled(&failed);
    assert_eq!(polled["status"], "failed");
    assert_eq!(polled["error"], "Handler exceeded timeout (900s)");
    assert_eq!(
        polled.as_object().map(|keys| keys.len()),
        Some(11),
        "{polled}"
    );
}
