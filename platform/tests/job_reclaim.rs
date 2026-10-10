use cufflink_testkit as common;

use common::jobs::{Jobs, LEASE, NO_LEASE, RETENTION};
use cufflink_platform::repo::wasm_jobs::{self, Failure, JobStatus, Requeue, WasmJob};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

const REPUBLISH_AFTER: Duration = Duration::from_secs(300);
const UPSTREAM: Failure<'static> = Failure {
    error: "Handler answered 503",
    class: "handler_error",
};

async fn reclaim(jobs: &Jobs, republish_after: Duration, limit: i64) -> Vec<Uuid> {
    let mut taken: Vec<Uuid> = wasm_jobs::reclaim(&jobs.pool, republish_after, limit)
        .await
        .expect("reclaim")
        .iter()
        .map(|job| job.id)
        .collect();
    taken.sort();
    taken
}

const NONE: [Uuid; 0] = [];

fn ids(jobs: &[&WasmJob]) -> Vec<Uuid> {
    let mut ids: Vec<Uuid> = jobs.iter().map(|job| job.id).collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn a_job_never_published_is_taken_by_one_reclaimer() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    assert_eq!(job.published_at, None);
    let (first, second) = tokio::join!(
        reclaim(&jobs, REPUBLISH_AFTER, 200),
        reclaim(&jobs, REPUBLISH_AFTER, 200)
    );
    assert_eq!(
        [first, second].concat(),
        [job.id],
        "taken once, by whichever was first"
    );
    let row = jobs.row(&job).await;
    assert!(row.published_at.is_some());
    assert_eq!((row.status, row.attempts), (JobStatus::Pending, 0));
    assert_eq!(reclaim(&jobs, REPUBLISH_AFTER, 200).await, NONE);
}

#[tokio::test]
async fn a_published_job_is_taken_again_once_it_has_waited_long_enough() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    wasm_jobs::set_published(&jobs.pool, job.id, true)
        .await
        .expect("published");
    assert_eq!(reclaim(&jobs, REPUBLISH_AFTER, 200).await, NONE);
    jobs.execute(
        "UPDATE wasm_jobs SET published_at = now() - interval '301 seconds' WHERE id = $1",
        &job,
    )
    .await;
    assert_eq!(reclaim(&jobs, REPUBLISH_AFTER, 200).await, [job.id]);
    assert_eq!(reclaim(&jobs, REPUBLISH_AFTER, 200).await, NONE);

    wasm_jobs::set_published(&jobs.pool, job.id, false)
        .await
        .expect("unpublished");
    assert_eq!(
        reclaim(&jobs, REPUBLISH_AFTER, 200).await,
        [job.id],
        "a job the stream could not take is the next pass's"
    );
}

#[tokio::test]
async fn a_job_is_not_taken_before_it_is_due() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let job = jobs.enqueue(3).await;
    let fence = jobs.claimed(&job, "worker-1", LEASE).await.fence;
    let due = chrono::Utc::now() + chrono::Duration::seconds(2);
    wasm_jobs::retry(&jobs.pool, &fence, UPSTREAM, due, Requeue::Republished)
        .await
        .expect("retry");
    assert_eq!(reclaim(&jobs, REPUBLISH_AFTER, 200).await, NONE);
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(reclaim(&jobs, REPUBLISH_AFTER, 200).await, [job.id]);
}

#[tokio::test]
async fn a_job_whose_lease_ran_out_is_taken_and_a_held_one_is_not() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let (held, lost, waiting) = (
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
    );
    for job in [&held, &lost, &waiting] {
        wasm_jobs::set_published(&jobs.pool, job.id, true)
            .await
            .expect("published");
    }
    jobs.claimed(&held, "worker-1", LEASE).await;
    jobs.claimed(&lost, "worker-1", NO_LEASE).await;
    assert_eq!(
        reclaim(&jobs, REPUBLISH_AFTER, 200).await,
        [lost.id],
        "its message was the worker's that let the lease run out"
    );
    let row = jobs.row(&lost).await;
    assert_eq!(
        (row.status, row.attempts),
        (JobStatus::Running, 1),
        "it is its next worker's to claim"
    );
    assert_eq!(
        reclaim(&jobs, REPUBLISH_AFTER, 200).await,
        NONE,
        "published since, it waits for a worker"
    );
    jobs.execute(
        "UPDATE wasm_jobs SET published_at = now() - interval '301 seconds'
          WHERE id <> $1",
        &waiting,
    )
    .await;
    assert_eq!(
        reclaim(&jobs, REPUBLISH_AFTER, 200).await,
        [lost.id],
        "and is published again when none came"
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
                started_at = now() - interval '1799 seconds',
                published_at = now() - interval '1799 seconds'
          WHERE id = $1",
        &job,
    )
    .await;
    assert_eq!(reclaim(&jobs, Duration::from_secs(3600), 200).await, NONE);
    jobs.execute(
        "UPDATE wasm_jobs SET started_at = now() - interval '1801 seconds' WHERE id = $1",
        &job,
    )
    .await;
    assert_eq!(
        reclaim(&jobs, Duration::from_secs(3600), 200).await,
        [job.id]
    );
    assert_eq!(reclaim(&jobs, Duration::from_secs(3600), 200).await, NONE);
}

#[tokio::test]
async fn a_job_out_of_attempts_that_nobody_holds_is_taken_at_once() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let (lost, held) = (jobs.enqueue(1).await, jobs.enqueue(1).await);
    for job in [&lost, &held] {
        wasm_jobs::set_published(&jobs.pool, job.id, true)
            .await
            .expect("published");
    }
    jobs.claimed(&lost, "worker-1", NO_LEASE).await;
    jobs.claimed(&held, "worker-1", LEASE).await;
    let taken = wasm_jobs::reclaim(&jobs.pool, REPUBLISH_AFTER, 200)
        .await
        .expect("reclaim");
    assert_eq!(ids(&taken.iter().collect::<Vec<_>>()), [lost.id]);
    assert!(taken[0].out_of_attempts());
}

#[tokio::test]
async fn an_ended_job_is_never_taken() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let (completed, failed) = (jobs.enqueue(3).await, jobs.enqueue(3).await);
    let fence = jobs.claimed(&completed, "worker-1", LEASE).await.fence;
    wasm_jobs::complete(&jobs.pool, &fence, &json!({"ok": true}), RETENTION)
        .await
        .expect("complete");
    let fence = jobs.claimed(&failed, "worker-1", LEASE).await.fence;
    wasm_jobs::fail(&jobs.pool, &fence, UPSTREAM, RETENTION)
        .await
        .expect("fail");
    assert_eq!(reclaim(&jobs, Duration::ZERO, 200).await, NONE);
}

#[tokio::test]
async fn a_pass_takes_no_more_than_its_batch() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let all = [
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
    ];
    let first = reclaim(&jobs, REPUBLISH_AFTER, 2).await;
    let second = reclaim(&jobs, REPUBLISH_AFTER, 2).await;
    assert_eq!((first.len(), second.len()), (2, 1));
    let mut taken = [first, second].concat();
    taken.sort();
    assert_eq!(taken, ids(&all.iter().collect::<Vec<_>>()));
}

#[tokio::test]
async fn a_failed_job_has_a_dead_letter_to_file_until_it_is_filed() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let undelivered = || async {
        wasm_jobs::undelivered_dead_letters(&jobs.pool, 200)
            .await
            .expect("dead letters")
    };
    let (failed, by_an_earlier_release, completed) = (
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
    );
    let fence = jobs.claimed(&failed, "worker-1", LEASE).await.fence;
    wasm_jobs::fail(&jobs.pool, &fence, UPSTREAM, RETENTION)
        .await
        .expect("fail");
    jobs.execute(
        "UPDATE wasm_jobs SET status = 'failed', error = 'Handler error', finished_at = now()
          WHERE id = $1",
        &by_an_earlier_release,
    )
    .await;
    let fence = jobs.claimed(&completed, "worker-1", LEASE).await.fence;
    wasm_jobs::complete(&jobs.pool, &fence, &json!({"ok": true}), RETENTION)
        .await
        .expect("complete");

    let letters = undelivered().await;
    assert_eq!(ids(&letters.iter().collect::<Vec<_>>()), [failed.id]);
    assert_eq!(letters[0].error.as_deref(), Some("Handler answered 503"));

    wasm_jobs::set_dead_lettered(&jobs.pool, failed.id)
        .await
        .expect("filed");
    assert!(jobs.row(&failed).await.dead_lettered_at.is_some());
    assert_eq!(undelivered().await, vec![]);

    wasm_jobs::set_dead_lettered(&jobs.pool, completed.id)
        .await
        .expect("nothing to file");
    assert_eq!(jobs.row(&completed).await.dead_lettered_at, None);
}

#[tokio::test]
async fn an_ended_job_is_deleted_once_it_was_kept_long_enough() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let (kept, expired, expired_later, unfinished) = (
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
    );
    for (job, retain_for) in [
        (&kept, RETENTION),
        (&expired, Duration::ZERO),
        (&expired_later, Duration::ZERO),
    ] {
        let fence = jobs.claimed(job, "worker-1", LEASE).await.fence;
        wasm_jobs::complete(&jobs.pool, &fence, &json!({"ok": true}), retain_for)
            .await
            .expect("complete");
    }
    jobs.execute(
        "UPDATE wasm_jobs SET expires_at = now() - interval '1 hour' WHERE id = $1",
        &unfinished,
    )
    .await;
    let left = |job: &WasmJob| {
        let (pool, id) = (jobs.pool.clone(), job.id);
        async move { wasm_jobs::get(&pool, id).await.expect("lookup").is_some() }
    };
    let delete = |limit| {
        let pool = jobs.pool.clone();
        async move {
            wasm_jobs::delete_expired(&pool, limit)
                .await
                .expect("delete")
        }
    };

    let held = jobs.pool.get().await.expect("checkout");
    held.batch_execute("BEGIN").await.expect("begin");
    held.query_one(
        "SELECT id FROM wasm_jobs WHERE id = $1 FOR UPDATE",
        &[&expired_later.id],
    )
    .await
    .expect("another replica deletes it");
    assert_eq!(delete(1).await, 1, "a batch is no more than its limit");
    assert!(!left(&expired).await, "the job that expired first");
    assert_eq!(delete(10).await, 0, "what another replica deletes is left");
    held.batch_execute("ROLLBACK").await.expect("rollback");
    drop(held);
    assert_eq!(delete(10).await, 1);
    assert!(!left(&expired_later).await);
    assert_eq!(delete(10).await, 0);

    assert!(left(&kept).await);
    assert!(
        left(&unfinished).await,
        "a job that has not ended is never deleted"
    );
}

#[tokio::test]
async fn the_backlog_is_the_jobs_that_have_not_ended() {
    let Some(jobs) = Jobs::new().await else {
        return;
    };
    let unfinished = || async {
        (
            wasm_jobs::unfinished(&jobs.pool, jobs.tenant.id)
                .await
                .expect("count"),
            wasm_jobs::unfinished_by_tenant(&jobs.pool)
                .await
                .expect("counts"),
        )
    };
    assert_eq!(unfinished().await, (0, vec![]));
    let (pending, running, completed) = (
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
        jobs.enqueue(3).await,
    );
    jobs.claimed(&running, "worker-1", LEASE).await;
    let fence = jobs.claimed(&completed, "worker-1", LEASE).await.fence;
    wasm_jobs::complete(&jobs.pool, &fence, &json!({"ok": true}), RETENTION)
        .await
        .expect("complete");
    assert_eq!(jobs.row(&pending).await.status, JobStatus::Pending);
    assert_eq!(unfinished().await, (2, vec![(jobs.tenant.slug.clone(), 2)]));
    assert_eq!(
        wasm_jobs::unfinished(&jobs.pool, Uuid::new_v4())
            .await
            .expect("count"),
        0
    );
}
