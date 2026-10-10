use cufflink_testkit as common;

use chrono::Utc;
use common::jobs::{Jobs, LEASE, NO_LEASE, RETENTION};
use cufflink_platform::errors::PlatformError;
use cufflink_platform::repo::wasm_jobs::{
    self, Claim, Enqueued, Failure, Fenced, JobStatus, NewJob, Requeue, WasmJob,
};
use cufflink_platform::router::jobs;
use cufflink_types::{BackoffSpec, JobConfig, RetryOn};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

const UPSTREAM: Failure<'static> = Failure {
    error: "Handler answered 503",
    class: "handler_error",
};

fn within(actual: chrono::DateTime<Utc>, expected: Duration) -> bool {
    let expected = Utc::now() + chrono::Duration::from_std(expected).expect("duration");
    (actual - expected).num_seconds().abs() <= 5
}

#[tokio::test]
async fn a_job_is_created_pending_and_due_with_what_it_retries() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let plain: WasmJob = jobs.enqueue(3).await;
    assert_eq!(plain.status, JobStatus::Pending);
    assert_eq!((plain.attempts, plain.max_attempts), (0, 3));
    assert!(plain.next_attempt_at <= Utc::now() + chrono::Duration::seconds(1));
    assert!(within(plain.expires_at, RETENTION));
    assert_eq!(plain.retry_on, vec![]);
    assert_eq!(
        (plain.backoff_base_secs, plain.backoff_max_secs),
        (None, None)
    );
    assert_eq!(
        (&plain.lease_owner, plain.lease_until, plain.published_at),
        (&None, None, None)
    );

    let retrying = jobs
        .enqueue_as(&JobConfig {
            max_attempts: 5,
            retry_on: vec![RetryOn::Timeout, RetryOn::Error],
            backoff: Some(BackoffSpec {
                base_secs: 10,
                max_secs: 600,
            }),
            ..jobs.config(5)
        })
        .await;
    assert_eq!(retrying.retry_on, vec![RetryOn::Error, RetryOn::Timeout]);
    assert_eq!(
        (retrying.backoff_base_secs, retrying.backoff_max_secs),
        (Some(10), Some(600))
    );
    assert_eq!(jobs.row(&retrying).await, retrying);
}

#[tokio::test]
async fn a_due_job_is_claimed_by_one_worker() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    let Claim { job: taken, fence } = jobs.claimed(&job, "worker-1", LEASE).await;
    assert_eq!(taken.status, JobStatus::Running);
    assert_eq!(taken.attempts, 1);
    assert_eq!(taken.lease_owner.as_deref(), Some("worker-1"));
    assert!(within(taken.lease_until.expect("lease"), LEASE));
    assert!(taken.started_at.is_some());
    assert_eq!(
        (fence.job_id, fence.owner.as_str(), fence.attempt),
        (job.id, "worker-1", 1)
    );

    assert_eq!(jobs.claim(&job, "worker-2", LEASE).await, None);
    assert_eq!(jobs.claim(&job, "worker-1", LEASE).await, None);
    assert_eq!(jobs.row(&job).await, taken);
    assert_eq!(
        wasm_jobs::claim(&jobs.pool, Uuid::new_v4(), "worker-1", LEASE)
            .await
            .expect("claim"),
        None
    );
}

#[tokio::test]
async fn a_completed_job_keeps_its_result_for_its_retention_from_its_end() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    let Claim { fence, .. } = jobs.claimed(&job, "worker-1", LEASE).await;
    let kept = Duration::from_secs(3600);
    assert_eq!(
        wasm_jobs::complete(&jobs.pool, &fence, &json!({"ok": true}), kept)
            .await
            .expect("complete"),
        Fenced::Held
    );
    let row = jobs.row(&job).await;
    assert_eq!(row.status, JobStatus::Completed);
    assert_eq!(row.result_json, Some(json!({"ok": true})));
    assert_eq!((row.error, row.attempts), (None, 1));
    assert!(row.finished_at.is_some());
    assert!(within(row.expires_at, kept));
    assert_eq!((row.lease_owner, row.lease_until), (None, None));

    assert_eq!(jobs.claim(&job, "worker-2", LEASE).await, None);
    assert_eq!(
        wasm_jobs::complete(&jobs.pool, &fence, &json!({"ok": false}), kept)
            .await
            .expect("complete again"),
        Fenced::Lost,
        "an ended job is nobody's"
    );
}

#[tokio::test]
async fn a_lease_that_ran_out_is_taken_and_fences_its_first_holder() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    let first = jobs.claimed(&job, "worker-1", NO_LEASE).await.fence;
    let Claim {
        job: retaken,
        fence: second,
    } = jobs.claimed(&job, "worker-2", LEASE).await;
    assert_eq!(retaken.attempts, 2);
    assert_eq!(retaken.lease_owner.as_deref(), Some("worker-2"));

    let pool = &jobs.pool;
    assert_eq!(
        wasm_jobs::heartbeat(pool, &first, LEASE)
            .await
            .expect("heartbeat"),
        Fenced::Lost
    );
    assert_eq!(
        wasm_jobs::complete(pool, &first, &json!({"late": true}), RETENTION)
            .await
            .expect("complete"),
        Fenced::Lost
    );
    assert_eq!(
        wasm_jobs::retry(pool, &first, UPSTREAM, Utc::now(), Requeue::Redelivered)
            .await
            .expect("retry"),
        Fenced::Lost
    );
    assert_eq!(
        wasm_jobs::fail(pool, &first, UPSTREAM, RETENTION)
            .await
            .expect("fail"),
        None
    );
    assert_eq!(
        wasm_jobs::release(pool, &first).await.expect("release"),
        Fenced::Lost
    );
    assert_eq!(
        jobs.row(&job).await,
        retaken,
        "nothing its first holder did changed the job"
    );

    assert_eq!(
        wasm_jobs::complete(pool, &second, &json!({"ok": true}), RETENTION)
            .await
            .expect("complete"),
        Fenced::Held
    );
}

#[tokio::test]
async fn the_same_worker_is_fenced_from_its_earlier_attempt() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    let first = jobs.claimed(&job, "worker-1", NO_LEASE).await.fence;
    let second = jobs.claimed(&job, "worker-1", LEASE).await.fence;
    assert_eq!((first.attempt, second.attempt), (1, 2));
    assert_eq!(
        wasm_jobs::complete(&jobs.pool, &first, &json!({"late": true}), RETENTION)
            .await
            .expect("complete"),
        Fenced::Lost
    );
    assert_eq!(jobs.row(&job).await.status, JobStatus::Running);
}

#[tokio::test]
async fn a_heartbeat_extends_the_lease() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    let Claim { fence, .. } = jobs.claimed(&job, "worker-1", Duration::from_secs(5)).await;
    assert_eq!(
        wasm_jobs::heartbeat(&jobs.pool, &fence, Duration::from_secs(300))
            .await
            .expect("heartbeat"),
        Fenced::Held
    );
    let row = jobs.row(&job).await;
    assert!(within(
        row.lease_until.expect("lease"),
        Duration::from_secs(300)
    ));
    assert_eq!((row.status, row.attempts), (JobStatus::Running, 1));
}

#[tokio::test]
async fn a_retry_waits_until_it_is_due_and_says_why() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    let Claim { fence, .. } = jobs.claimed(&job, "worker-1", LEASE).await;
    let due = Utc::now() + chrono::Duration::seconds(2);
    assert_eq!(
        wasm_jobs::retry(&jobs.pool, &fence, UPSTREAM, due, Requeue::Redelivered)
            .await
            .expect("retry"),
        Fenced::Held
    );
    let row = jobs.row(&job).await;
    assert_eq!((row.status, row.attempts), (JobStatus::Pending, 1));
    assert_eq!(row.last_error.as_deref(), Some("Handler answered 503"));
    assert_eq!(row.error_class.as_deref(), Some("handler_error"));
    assert_eq!(
        row.error, None,
        "a job that is attempted again has not failed"
    );
    assert_eq!(
        (row.next_attempt_at - due).num_milliseconds().abs(),
        0,
        "{row:?}"
    );
    assert!(row.published_at.is_some(), "its message is in the stream");
    assert_eq!((&row.lease_owner, row.lease_until), (&None, None));

    assert_eq!(
        jobs.claim(&job, "worker-2", LEASE).await,
        None,
        "not yet due"
    );
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let again = jobs.claimed(&job, "worker-2", LEASE).await;
    assert_eq!(again.job.attempts, 2);
    assert_eq!(
        again.job.last_error.as_deref(),
        Some("Handler answered 503"),
        "the attempt is told what the one before it failed of"
    );

    assert_eq!(
        wasm_jobs::retry(
            &jobs.pool,
            &again.fence,
            UPSTREAM,
            Utc::now(),
            Requeue::Republished
        )
        .await
        .expect("retry"),
        Fenced::Held
    );
    assert_eq!(
        jobs.row(&job).await.published_at,
        None,
        "its message is gone: another is to be published"
    );
}

#[tokio::test]
async fn a_failed_job_says_why_and_is_kept_from_its_end() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    let Claim { fence, .. } = jobs.claimed(&job, "worker-1", LEASE).await;
    let kept = Duration::from_secs(7200);
    let failed = wasm_jobs::fail(&jobs.pool, &fence, UPSTREAM, kept)
        .await
        .expect("fail")
        .expect("the job it failed");
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.error.as_deref(), Some("Handler answered 503"));
    assert_eq!(failed.last_error, failed.error);
    assert_eq!(failed.error_class.as_deref(), Some("handler_error"));
    assert!(failed.finished_at.is_some());
    assert!(within(failed.expires_at, kept));
    assert_eq!((&failed.lease_owner, failed.lease_until), (&None, None));
    assert_eq!(failed.dead_lettered_at, None);
    assert_eq!(jobs.row(&job).await, failed);
    assert_eq!(jobs.claim(&job, "worker-2", LEASE).await, None);
}

#[tokio::test]
async fn an_attempt_given_up_is_not_counted() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(1).await;
    let Claim { fence, .. } = jobs.claimed(&job, "worker-1", LEASE).await;
    assert_eq!(
        wasm_jobs::release(&jobs.pool, &fence)
            .await
            .expect("release"),
        Fenced::Held
    );
    let row = jobs.row(&job).await;
    assert_eq!((row.status, row.attempts), (JobStatus::Pending, 0));
    assert_eq!((&row.lease_owner, row.lease_until), (&None, None));
    assert_eq!((&row.last_error, &row.error), (&None, &None));

    let again = jobs.claimed(&job, "worker-2", LEASE).await;
    assert_eq!(
        again.job.attempts, 1,
        "its one attempt is still the job's to make"
    );
}

#[tokio::test]
async fn a_job_out_of_attempts_is_failed_once_nobody_holds_it() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let lost = Failure {
        error: "worker lost after 1 attempts",
        class: "exhausted",
    };
    let pool = &jobs.pool;

    let held = jobs.enqueue(1).await;
    jobs.claimed(&held, "worker-1", LEASE).await;
    assert_eq!(jobs.claim(&held, "worker-2", LEASE).await, None);
    assert_eq!(
        wasm_jobs::fail_exhausted(pool, held.id, lost, RETENTION)
            .await
            .expect("fail"),
        None,
        "its last attempt is running"
    );
    assert_eq!(jobs.row(&held).await.status, JobStatus::Running);

    let unheld = jobs.enqueue(1).await;
    jobs.claimed(&unheld, "worker-1", NO_LEASE).await;
    assert_eq!(
        jobs.claim(&unheld, "worker-2", LEASE).await,
        None,
        "it has no attempt left"
    );
    let failed = wasm_jobs::fail_exhausted(pool, unheld.id, lost, RETENTION)
        .await
        .expect("fail")
        .expect("failed");
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(
        failed.error.as_deref(),
        Some("worker lost after 1 attempts")
    );
    assert_eq!(failed.error_class.as_deref(), Some("exhausted"));
    assert_eq!(failed.attempts, 1);
    assert_eq!(
        wasm_jobs::fail_exhausted(pool, unheld.id, lost, RETENTION)
            .await
            .expect("fail again"),
        None,
        "it is failed once"
    );

    let with_attempts_left = jobs.enqueue(2).await;
    jobs.claimed(&with_attempts_left, "worker-1", NO_LEASE)
        .await;
    assert_eq!(
        wasm_jobs::fail_exhausted(pool, with_attempts_left.id, lost, RETENTION)
            .await
            .expect("fail"),
        None
    );
}

#[tokio::test]
async fn a_job_an_earlier_release_runs_is_taken_after_twice_its_timeout() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    jobs.execute(
        "UPDATE wasm_jobs SET status = 'running', attempts = 1,
                started_at = now() - interval '1799 seconds'
          WHERE id = $1",
        &job,
    )
    .await;
    assert!(jobs.row(&job).await.is_held(Utc::now()));
    assert_eq!(jobs.claim(&job, "worker-1", LEASE).await, None);

    jobs.execute(
        "UPDATE wasm_jobs SET started_at = now() - interval '1801 seconds' WHERE id = $1",
        &job,
    )
    .await;
    assert!(!jobs.row(&job).await.is_held(Utc::now()));
    let taken = jobs.claimed(&job, "worker-1", LEASE).await;
    assert_eq!(taken.job.attempts, 2);
}

#[tokio::test]
async fn a_status_this_release_does_not_know_is_an_error_not_a_panic() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    jobs.pool
        .get()
        .await
        .expect("checkout")
        .batch_execute("ALTER TABLE wasm_jobs DROP CONSTRAINT wasm_jobs_status_check")
        .await
        .expect("a later release's statuses");
    jobs.execute("UPDATE wasm_jobs SET status = 'dead' WHERE id = $1", &job)
        .await;
    let error = wasm_jobs::get(&jobs.pool, job.id)
        .await
        .expect_err("unknown status");
    assert!(
        error.to_string().contains("unknown status 'dead'"),
        "{error}"
    );
}

#[tokio::test]
async fn a_job_enqueued_again_under_its_key_is_the_job_enqueued_before() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let config = jobs.config(3);
    let keyed = |key| NewJob {
        idempotency_key: Some(key),
        schedule_name: Some("payment_reminders"),
        ..jobs.new_job(&config)
    };
    let first = jobs
        .create(keyed("sched:payment_reminders:1800000000"))
        .await;
    let Enqueued::Created(job) = &first else {
        panic!("created: {first:?}");
    };
    assert_eq!(
        job.idempotency_key.as_deref(),
        Some("sched:payment_reminders:1800000000")
    );
    assert_eq!(job.schedule_name.as_deref(), Some("payment_reminders"));

    jobs.claimed(job, "worker-1", LEASE).await;
    let again = jobs
        .create(keyed("sched:payment_reminders:1800000000"))
        .await;
    assert_eq!(
        again,
        Enqueued::Existing(jobs.row(job).await),
        "as it is now, not as it was enqueued"
    );

    let next = jobs
        .create(keyed("sched:payment_reminders:1800003600"))
        .await;
    assert!(matches!(&next, Enqueued::Created(next) if next.id != job.id));

    let other_service = cufflink_testkit::active_service(
        &jobs.pool,
        jobs.tenant.id,
        "receipt-service",
        "receipt_service",
    )
    .await
    .expect("service");
    let elsewhere = jobs
        .create(NewJob {
            service_id: other_service.id,
            ..keyed("sched:payment_reminders:1800000000")
        })
        .await;
    assert!(
        matches!(&elsewhere, Enqueued::Created(other) if other.id != job.id),
        "a key is its service's"
    );

    let unkeyed = [jobs.enqueue(3).await, jobs.enqueue(3).await];
    assert_ne!(unkeyed[0].id, unkeyed[1].id);
    assert_eq!(unkeyed[0].idempotency_key, None);
}

#[tokio::test]
async fn a_job_enqueued_before_is_answered_however_full_the_backlog() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let config = jobs.config(3);
    let keyed = |key| NewJob {
        idempotency_key: Some(key),
        ..jobs.new_job(&config)
    };
    let pool = &jobs.pool;
    let record = |job| jobs::record(pool, job);
    let first = match record(keyed("sched:payment_reminders:1800000000")).await {
        Ok(Enqueued::Created(job)) => job,
        other => panic!("created: {other:?}"),
    };
    jobs.execute(
        "INSERT INTO wasm_jobs (tenant_id, tenant_slug, service_id, service_slug,
                                service_version, handler, status, request_json,
                                timeout_secs, expires_at)
         SELECT tenant_id, tenant_slug, service_id, service_slug, service_version, handler,
                'pending', '{}'::jsonb, 60, now() + interval '1 day'
           FROM wasm_jobs, generate_series(1, 999)
          WHERE id = $1",
        &first,
    )
    .await;

    assert_eq!(
        record(keyed("sched:payment_reminders:1800000000"))
            .await
            .expect("the job there is"),
        Enqueued::Existing(first),
        "it adds nothing to the backlog"
    );
    for refused in [
        keyed("sched:payment_reminders:1800003600"),
        jobs.new_job(&config),
    ] {
        assert!(matches!(
            record(refused).await,
            Err(PlatformError::TooManyRequests(_))
        ));
    }
}

#[tokio::test]
async fn a_schedule_has_a_job_until_the_job_has_ended() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let config = jobs.config(3);
    let unfinished = |schedule: &'static str| {
        let (pool, service_id) = (jobs.pool.clone(), jobs.service_id);
        async move {
            wasm_jobs::schedule_has_unfinished(&pool, service_id, schedule)
                .await
                .expect("lookup")
        }
    };
    assert!(!unfinished("payment_reminders").await);
    let Enqueued::Created(job) = jobs
        .create(NewJob {
            schedule_name: Some("payment_reminders"),
            ..jobs.new_job(&config)
        })
        .await
    else {
        panic!("created");
    };
    assert!(unfinished("payment_reminders").await, "pending");
    assert!(!unfinished("order_retry").await);
    let Claim { fence, .. } = jobs.claimed(&job, "worker-1", LEASE).await;
    assert!(unfinished("payment_reminders").await, "running");
    wasm_jobs::complete(&jobs.pool, &fence, &json!({"ok": true}), RETENTION)
        .await
        .expect("complete");
    assert!(!unfinished("payment_reminders").await, "ended");
}
