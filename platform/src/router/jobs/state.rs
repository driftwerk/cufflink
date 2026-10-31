use crate::repo::wasm_jobs::{JobStatus, WasmJob};
use crate::router::guest_response::{answer, parse_output};
use crate::wasm_runtime::budget::{Hold, Refused};
use crate::wasm_runtime::ExecutionError;
use crate::wasm_runtime::GuestOutput;
use chrono::{DateTime, Utc};
use cufflink_types::backoff::Backoff;
use cufflink_types::delivery::DeliveryControl;
use cufflink_types::RetryOn;
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobErrorClass {
    Crash,
    Capacity,
    Timeout,
    HandlerError,
    Explicit { retry_after: Duration },
    Terminal { reason: String },
}

impl JobErrorClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobErrorClass::Crash => "crash",
            JobErrorClass::Capacity => "capacity",
            JobErrorClass::Timeout => "timeout",
            JobErrorClass::HandlerError => "handler_error",
            JobErrorClass::Explicit { .. } => "explicit",
            JobErrorClass::Terminal { .. } => "terminal",
        }
    }

    fn is_retried(&self, retry_on: &[RetryOn]) -> bool {
        match self {
            JobErrorClass::Crash | JobErrorClass::Capacity | JobErrorClass::Explicit { .. } => true,
            JobErrorClass::Timeout => retry_on.contains(&RetryOn::Timeout),
            JobErrorClass::HandlerError => retry_on.contains(&RetryOn::Error),
            JobErrorClass::Terminal { .. } => false,
        }
    }
}

pub const EXHAUSTED_CLASS: &str = "exhausted";

pub fn exhausted_error(job: &WasmJob) -> String {
    format!("worker lost after {} attempts", job.attempts)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobDecision {
    Retry { at: DateTime<Utc> },
    Fail,
}

pub fn decide_job(
    class: &JobErrorClass,
    attempts: u32,
    max_attempts: u32,
    retry_on: &[RetryOn],
    backoff: &Backoff,
    now: DateTime<Utc>,
    seed: u64,
) -> JobDecision {
    if !class.is_retried(retry_on) || attempts >= max_attempts {
        return JobDecision::Fail;
    }
    let delay = match class {
        JobErrorClass::Explicit { retry_after } => (*retry_after).min(backoff.longest()),
        _ => backoff.delay(attempts, seed),
    };
    JobDecision::Retry {
        at: chrono::Duration::from_std(delay)
            .ok()
            .and_then(|delay| now.checked_add_signed(delay))
            .unwrap_or(DateTime::<Utc>::MAX_UTC),
    }
}

const CONTENDED_RETRY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Inspection<'a> {
    Settled(&'static str),
    Wait(Duration),
    Exhausted(&'a WasmJob),
}

pub fn inspect(job: Option<&WasmJob>, now: DateTime<Utc>) -> Inspection<'_> {
    let Some(job) = job else {
        return Inspection::Settled("its row is gone");
    };
    match job.status {
        JobStatus::Completed | JobStatus::Failed => Inspection::Settled("the job has ended"),
        JobStatus::Running if job.is_held(now) => {
            Inspection::Settled("another worker holds the job")
        }
        JobStatus::Pending | JobStatus::Running if job.out_of_attempts() => {
            Inspection::Exhausted(job)
        }
        JobStatus::Running => Inspection::Wait(CONTENDED_RETRY),
        JobStatus::Pending => Inspection::Wait(
            (job.next_attempt_at - now)
                .to_std()
                .map_or(CONTENDED_RETRY, |wait| wait.max(CONTENDED_RETRY)),
        ),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Attempt {
    Completed(Value),
    Failed { class: JobErrorClass, error: String },
    Interrupted(String),
    Deferred(Duration),
    LeaseLost,
}

impl Attempt {
    pub fn failed(class: JobErrorClass, error: impl Into<String>) -> Self {
        Attempt::Failed {
            class,
            error: error.into(),
        }
    }
}

fn answered(response: GuestOutput, job: &WasmJob) -> Result<(Attempt, Hold), Refused> {
    let answer = answer(&response)
        .ok()
        .map(|answer| (answer.status.as_u16(), answer.delivery));
    let failed = match answer {
        Some((status, Some(DeliveryControl::RetryAfter { retry_after_ms }))) => Attempt::failed(
            JobErrorClass::Explicit {
                retry_after: Duration::from_millis(retry_after_ms),
            },
            format!("Handler asked for another attempt ({status})"),
        ),
        Some((_, Some(DeliveryControl::Terminal { terminal }))) => Attempt::failed(
            JobErrorClass::Terminal {
                reason: terminal.clone(),
            },
            terminal,
        ),
        Some((status, None)) if status >= 500 && job.retry_on.contains(&RetryOn::Error) => {
            Attempt::failed(
                JobErrorClass::HandlerError,
                format!("Handler answered {status}"),
            )
        }
        _ => {
            let (value, hold) = parse_output(response)?;
            return Ok((Attempt::Completed(value), hold));
        }
    };
    Ok((failed, Hold::none()))
}

pub fn classify(
    run: Result<GuestOutput, ExecutionError>,
    job: &WasmJob,
    at_hard_deadline: bool,
    deferrals_allowed: u32,
    refusal: impl FnOnce(Refused) -> ExecutionError,
) -> (Attempt, Hold) {
    let error = match run.map(|response| answered(response, job)) {
        Ok(Ok(settled)) => return settled,
        Ok(Err(refused)) => refusal(refused),
        Err(error) => error,
    };
    (
        unanswered(error, job, at_hard_deadline, deferrals_allowed),
        Hold::none(),
    )
}

#[cfg(test)]
pub(crate) fn classified(
    run: Result<String, ExecutionError>,
    job: &WasmJob,
    at_hard_deadline: bool,
    deferrals_allowed: u32,
) -> Attempt {
    let run = run.map(|text| GuestOutput {
        text,
        hold: Hold::none(),
    });
    classify(run, job, at_hard_deadline, deferrals_allowed, |refused| {
        ExecutionError::Response(refused.to_string())
    })
    .0
}

fn unanswered(
    error: ExecutionError,
    job: &WasmJob,
    at_hard_deadline: bool,
    deferrals_allowed: u32,
) -> Attempt {
    match error {
        ExecutionError::Cancelled => Attempt::LeaseLost,
        e if e.interrupted_by_shutdown(at_hard_deadline) => Attempt::Interrupted(e.to_string()),
        ExecutionError::Overloaded { retry_after, .. }
            if u32::try_from(job.capacity_deferrals).unwrap_or(u32::MAX) < deferrals_allowed =>
        {
            metrics::counter!("cufflink_wasm_job_deferrals_total", "result" => "deferred")
                .increment(1);
            Attempt::Deferred(retry_after)
        }
        e @ ExecutionError::Overloaded { .. } => {
            metrics::counter!("cufflink_wasm_job_deferrals_total", "result" => "counted")
                .increment(1);
            Attempt::failed(JobErrorClass::Capacity, e.to_string())
        }
        ExecutionError::DeadlineExceeded { .. } => Attempt::failed(
            JobErrorClass::Timeout,
            format!("Handler exceeded timeout ({}s)", job.timeout_secs),
        ),
        e @ ExecutionError::Runtime(_) => Attempt::failed(JobErrorClass::Crash, e.to_string()),
        e @ ExecutionError::HandlerNotFound(_) => {
            let error = format!("Handler error: {e}");
            Attempt::failed(
                JobErrorClass::Terminal {
                    reason: error.clone(),
                },
                error,
            )
        }
        e => Attempt::failed(JobErrorClass::HandlerError, format!("Handler error: {e}")),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use uuid::Uuid;

    pub(crate) fn job(status: JobStatus, attempts: i32, max_attempts: i32) -> WasmJob {
        let now = at(0);
        WasmJob {
            id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            service_id: Uuid::nil(),
            service_slug: "stock-service".into(),
            service_version: 1,
            handler: "handle_run_report".into(),
            status,
            request_json: Value::Null,
            auth_subject: None,
            auth_claims: None,
            result_json: None,
            error: None,
            attempts,
            max_attempts,
            timeout_secs: 900,
            created_at: now,
            started_at: None,
            finished_at: None,
            expires_at: now + chrono::Duration::hours(24),
            lease_owner: None,
            lease_until: None,
            next_attempt_at: now,
            published_at: None,
            last_error: None,
            error_class: None,
            retry_on: Vec::new(),
            backoff_base_secs: None,
            backoff_max_secs: None,
            trace_context: None,
            dead_lettered_at: None,
            idempotency_key: None,
            capacity_deferrals: 0,
            schedule_name: None,
        }
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::UNIX_EPOCH + chrono::Duration::seconds(1_800_000_000 + secs)
    }

    fn classes() -> [JobErrorClass; 6] {
        [
            JobErrorClass::Crash,
            JobErrorClass::Timeout,
            JobErrorClass::HandlerError,
            JobErrorClass::Explicit {
                retry_after: Duration::from_secs(7),
            },
            JobErrorClass::Explicit {
                retry_after: Duration::from_millis(u64::MAX),
            },
            JobErrorClass::Terminal {
                reason: "unknown book".into(),
            },
        ]
    }

    fn retry_sets() -> [Vec<RetryOn>; 4] {
        [
            vec![],
            vec![RetryOn::Error],
            vec![RetryOn::Timeout],
            vec![RetryOn::Error, RetryOn::Timeout],
        ]
    }

    fn decide(class: &JobErrorClass, attempts: u32, retry_on: &[RetryOn]) -> JobDecision {
        let backoff = Backoff::exponential(Duration::from_secs(30), Duration::from_secs(3600));
        decide_job(class, attempts, 3, retry_on, &backoff, at(0), 42)
    }

    #[test]
    fn a_capacity_denial_defers_until_the_bound_then_counts_as_a_failure_that_is_retried() {
        let overloaded = || {
            Err(ExecutionError::Overloaded {
                reason: crate::wasm_runtime::budget::Reason::Memory,
                retry_after: Duration::from_secs(2),
                ran: true,
            })
        };
        let mut waiting = job(JobStatus::Running, 1, 3);
        waiting.capacity_deferrals = 9;
        assert_eq!(
            classified(overloaded(), &waiting, false, 10),
            Attempt::Deferred(Duration::from_secs(2))
        );
        waiting.capacity_deferrals = 10;
        let Attempt::Failed { class, error } = classified(overloaded(), &waiting, false, 10) else {
            panic!("the eleventh deferral counts");
        };
        assert_eq!(class, JobErrorClass::Capacity);
        assert!(error.contains("capacity"), "{error}");
        assert_eq!(class.as_str(), "capacity");
        assert!(
            class.is_retried(&[]),
            "as a crash is, whatever retry_on says"
        );
        assert_eq!(
            classified(
                Err(ExecutionError::TooLarge {
                    requested: 2,
                    share: 1
                }),
                &waiting,
                false,
                10
            ),
            Attempt::failed(
                JobErrorClass::HandlerError,
                "Handler error: WASM guest needs more memory than its service may hold \
                 (requested 2 bytes, service share 1 bytes)"
            ),
            "no retry cures what can never fit"
        );
    }

    #[test]
    fn every_class_is_retried_by_who_declares_it_while_attempts_remain() {
        for class in classes() {
            for retry_on in retry_sets() {
                let retried = match class {
                    JobErrorClass::Crash
                    | JobErrorClass::Capacity
                    | JobErrorClass::Explicit { .. } => true,
                    JobErrorClass::Timeout => retry_on.contains(&RetryOn::Timeout),
                    JobErrorClass::HandlerError => retry_on.contains(&RetryOn::Error),
                    JobErrorClass::Terminal { .. } => false,
                };
                for attempts in 1..=4 {
                    let decision = decide(&class, attempts, &retry_on);
                    assert_eq!(
                        matches!(decision, JobDecision::Retry { .. }),
                        retried && attempts < 3,
                        "{class:?} with {retry_on:?} after attempt {attempts}: {decision:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_job_that_declares_nothing_retries_a_crash_alone() {
        assert_eq!(
            decide(&JobErrorClass::HandlerError, 1, &[]),
            JobDecision::Fail
        );
        assert_eq!(decide(&JobErrorClass::Timeout, 1, &[]), JobDecision::Fail);
        assert!(matches!(
            decide(&JobErrorClass::Crash, 1, &[]),
            JobDecision::Retry { .. }
        ));
    }

    #[test]
    fn one_attempt_is_never_followed_by_another() {
        let backoff = Backoff::exponential(Duration::from_secs(30), Duration::from_secs(3600));
        for class in classes() {
            assert_eq!(
                decide_job(
                    &class,
                    1,
                    1,
                    &[RetryOn::Error, RetryOn::Timeout],
                    &backoff,
                    at(0),
                    42
                ),
                JobDecision::Fail,
                "{class:?}"
            );
        }
    }

    #[test]
    fn a_retry_waits_its_backoff_or_what_the_handler_asked() {
        let delay = |decision| match decision {
            JobDecision::Retry { at: due } => (due - at(0)).num_milliseconds(),
            JobDecision::Fail => -1,
        };
        let first = delay(decide(&JobErrorClass::Crash, 1, &[]));
        let second = delay(decide(&JobErrorClass::Crash, 2, &[]));
        assert!((24_000..=36_000).contains(&first), "{first}");
        assert!((48_000..=72_000).contains(&second), "{second}");
        assert_eq!(
            decide(&JobErrorClass::Crash, 1, &[]),
            decide(&JobErrorClass::Crash, 1, &[]),
            "the same job waits the same"
        );
        assert_eq!(
            delay(decide(
                &JobErrorClass::Explicit {
                    retry_after: Duration::from_secs(7)
                },
                1,
                &[]
            )),
            7_000
        );
    }

    #[test]
    fn a_handler_is_given_no_longer_than_the_jobs_backoff_ever_waits() {
        let asked = |retry_after| decide(&JobErrorClass::Explicit { retry_after }, 1, &[]);
        let hour = JobDecision::Retry {
            at: at(0) + chrono::Duration::hours(1),
        };
        for beyond in [
            Duration::from_secs(3601),
            Duration::from_millis(u64::MAX),
            Duration::MAX,
        ] {
            assert_eq!(asked(beyond), hour, "{beyond:?}");
        }
        assert_eq!(asked(Duration::from_secs(3600)), hour);
        assert_eq!(
            asked(Duration::ZERO),
            JobDecision::Retry { at: at(0) },
            "a handler may ask for its next attempt at once"
        );

        let declared = Backoff::exponential(Duration::from_secs(1), Duration::from_secs(5));
        assert_eq!(
            decide_job(
                &JobErrorClass::Explicit {
                    retry_after: Duration::from_millis(u64::MAX)
                },
                1,
                3,
                &[],
                &declared,
                at(0),
                42
            ),
            JobDecision::Retry {
                at: at(0) + chrono::Duration::seconds(5)
            },
            "the cap is the job's own"
        );
    }

    #[test]
    fn a_message_without_a_job_to_run_is_settled() {
        assert!(matches!(inspect(None, at(0)), Inspection::Settled(_)));
        for ended in [JobStatus::Completed, JobStatus::Failed] {
            for (attempts, max_attempts) in [(1, 3), (3, 3)] {
                assert!(matches!(
                    inspect(Some(&job(ended, attempts, max_attempts)), at(0)),
                    Inspection::Settled(_)
                ));
            }
        }
    }

    #[test]
    fn a_job_not_yet_due_waits_until_it_is() {
        let pending = WasmJob {
            next_attempt_at: at(45),
            ..job(JobStatus::Pending, 1, 3)
        };
        assert_eq!(
            inspect(Some(&pending), at(0)),
            Inspection::Wait(Duration::from_secs(45))
        );
        assert_eq!(
            inspect(Some(&pending), at(60)),
            Inspection::Wait(CONTENDED_RETRY),
            "due, and claimed by nobody: the claim raced another"
        );
    }

    #[test]
    fn a_held_job_is_its_holders_even_on_its_last_attempt() {
        for (attempts, max_attempts) in [(1, 3), (3, 3)] {
            let held = WasmJob {
                lease_owner: Some("worker-1".into()),
                lease_until: Some(at(60)),
                started_at: Some(at(0)),
                ..job(JobStatus::Running, attempts, max_attempts)
            };
            assert!(held.is_held(at(59)));
            assert!(matches!(
                inspect(Some(&held), at(59)),
                Inspection::Settled(_)
            ));
        }
    }

    #[test]
    fn a_job_nobody_holds_is_failed_once_out_of_attempts() {
        let lost = |attempts| WasmJob {
            lease_owner: Some("worker-1".into()),
            lease_until: Some(at(60)),
            started_at: Some(at(0)),
            ..job(JobStatus::Running, attempts, 3)
        };
        assert_eq!(
            inspect(Some(&lost(3)), at(61)),
            Inspection::Exhausted(&lost(3))
        );
        assert_eq!(
            inspect(Some(&lost(2)), at(61)),
            Inspection::Wait(CONTENDED_RETRY)
        );
        let pending = job(JobStatus::Pending, 3, 3);
        assert_eq!(
            inspect(Some(&pending), at(0)),
            Inspection::Exhausted(&pending)
        );
    }

    #[test]
    fn a_job_an_earlier_release_runs_is_held_for_twice_its_timeout() {
        let running = WasmJob {
            started_at: Some(at(0)),
            ..job(JobStatus::Running, 1, 3)
        };
        assert!(running.is_held(at(1800)));
        assert!(!running.is_held(at(1801)));
        let never_started = job(JobStatus::Running, 1, 3);
        assert!(!never_started.is_held(at(0)));
    }

    fn retrying(retry_on: &[RetryOn]) -> WasmJob {
        WasmJob {
            retry_on: retry_on.to_vec(),
            ..job(JobStatus::Running, 1, 3)
        }
    }

    #[test]
    fn a_response_is_the_jobs_result_whatever_its_status() {
        for response in [
            r#"{"ok":true}"#,
            r#"{"__status":500,"__body":{"error":"upstream"}}"#,
            r#"{"__v":2,"__status":503,"__body":{"error":"upstream"}}"#,
            r#"{"__status":404,"__body":{"error":"no such item"}}"#,
        ] {
            for retry_on in [vec![], vec![RetryOn::Timeout]] {
                assert_eq!(
                    classified(Ok(response.into()), &retrying(&retry_on), false, 10),
                    Attempt::Completed(serde_json::from_str(response).expect("json")),
                    "{response}"
                );
            }
        }
        assert_eq!(
            classified(Ok("plain text".into()), &retrying(&[]), false, 10),
            Attempt::Completed(Value::String("plain text".into()))
        );
    }

    #[test]
    fn a_job_that_retries_errors_fails_on_a_5xx_alone() {
        let job = retrying(&[RetryOn::Error]);
        for (response, status) in [
            (r#"{"__status":500,"__body":{"error":"upstream"}}"#, 500),
            (
                r#"{"__v":2,"__status":503,"__body":{"error":"upstream"}}"#,
                503,
            ),
        ] {
            assert_eq!(
                classified(Ok(response.into()), &job, false, 10),
                Attempt::failed(
                    JobErrorClass::HandlerError,
                    format!("Handler answered {status}")
                )
            );
        }
        for response in [
            r#"{"ok":true}"#,
            r#"{"error":"a body that only says so"}"#,
            r#"{"__status":404,"__body":{"error":"no such item"}}"#,
            r#"{"__v":2,"__status":99}"#,
            "plain text",
        ] {
            assert!(
                matches!(
                    classified(Ok(response.into()), &job, false, 10),
                    Attempt::Completed(_)
                ),
                "{response}"
            );
        }
    }

    #[test]
    fn a_handler_that_asks_is_given_what_it_asked_whatever_the_job_declares() {
        for retry_on in retry_sets() {
            let job = retrying(&retry_on);
            assert_eq!(
                classified(
                    Ok(r#"{"__v":2,"__status":503,"__body":{"error":"busy"},"__delivery":{"retry_after_ms":1500}}"#.into()),
                    &job,
                    false,
                    10
                ),
                Attempt::failed(
                    JobErrorClass::Explicit {
                        retry_after: Duration::from_millis(1500)
                    },
                    "Handler asked for another attempt (503)"
                )
            );
            assert_eq!(
                classified(
                    Ok(r#"{"__v":2,"__status":422,"__body":{"error":"unknown book"},"__delivery":{"terminal":"unknown book"}}"#.into()),
                    &job,
                    false,
                    10
                ),
                Attempt::failed(
                    JobErrorClass::Terminal {
                        reason: "unknown book".into()
                    },
                    "unknown book"
                )
            );
        }
    }

    #[test]
    fn a_block_only_a_v2_response_carries_asks_nothing_elsewhere() {
        let job = retrying(&[]);
        for response in [
            r#"{"__status":503,"__body":{},"__delivery":{"retry_after_ms":1500}}"#,
            r#"{"__delivery":{"terminal":"unknown book"}}"#,
            r#"{"__v":2,"__status":200,"__body":{},"__delivery":null}"#,
            r#"{"__v":2,"__status":503,"__body":{},"__delivery":{"retry_after_ms":"soon"}}"#,
        ] {
            assert_eq!(
                classified(Ok(response.into()), &job, false, 10),
                Attempt::Completed(serde_json::from_str(response).expect("json")),
                "{response}"
            );
        }
    }

    #[test]
    fn a_failure_is_classed_by_who_gave_up() {
        let job = retrying(&[]);
        let deadline = || {
            Err(ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(900),
            })
        };
        assert_eq!(
            classified(deadline(), &job, false, 10),
            Attempt::failed(JobErrorClass::Timeout, "Handler exceeded timeout (900s)")
        );
        assert!(matches!(
            classified(deadline(), &job, true, 10),
            Attempt::Interrupted(_)
        ));
        assert!(matches!(
            classified(Err(ExecutionError::ShuttingDown), &job, false, 10),
            Attempt::Interrupted(_)
        ));
        assert!(
            matches!(
                classified(Err(ExecutionError::NotStarted), &job, false, 0),
                Attempt::Interrupted(_)
            ),
            "released, whatever deferrals are left"
        );
        assert_eq!(
            classified(Err(ExecutionError::Cancelled), &job, false, 10),
            Attempt::LeaseLost
        );
        assert_eq!(
            classified(Err(ExecutionError::Runtime("join".into())), &job, false, 10),
            Attempt::failed(JobErrorClass::Crash, "WASM runtime error: join")
        );
        assert_eq!(
            classified(
                Err(ExecutionError::Trap("unreachable".into())),
                &job,
                false,
                10
            ),
            Attempt::failed(
                JobErrorClass::HandlerError,
                "Handler error: WASM trap: unreachable"
            )
        );
        assert!(matches!(
            classified(
                Err(ExecutionError::MemoryLimit {
                    requested: 2,
                    limit: 1
                }),
                &job,
                false,
                10
            ),
            Attempt::Failed {
                class: JobErrorClass::HandlerError,
                ..
            }
        ));
        assert!(matches!(
            classified(Err(ExecutionError::HandlerNotFound("h".into())), &job, false, 10),
            Attempt::Failed {
                class: JobErrorClass::Terminal { .. },
                error,
            } if error == "Handler error: Handler 'h' not found in WASM module"
        ));
    }
}
