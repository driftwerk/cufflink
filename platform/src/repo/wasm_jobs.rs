use crate::errors::{PlatformError, PlatformResult};
use chrono::{DateTime, Utc};
use cufflink_types::backoff::Backoff;
use cufflink_types::{BackoffSpec, RetryOn};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl JobStatus {
    const ALL: [JobStatus; 4] = [
        JobStatus::Pending,
        JobStatus::Running,
        JobStatus::Completed,
        JobStatus::Failed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
        }
    }

    pub fn parse(status: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|known| known.as_str() == status)
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, JobStatus::Completed | JobStatus::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WasmJob {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub tenant_slug: String,
    pub service_id: Uuid,
    pub service_slug: String,
    pub service_version: i32,
    pub handler: String,
    pub status: JobStatus,
    pub request_json: Value,
    pub auth_subject: Option<String>,
    pub auth_claims: Option<Value>,
    pub result_json: Option<Value>,
    pub error: Option<String>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub timeout_secs: i32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub next_attempt_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub error_class: Option<String>,
    pub retry_on: Vec<RetryOn>,
    pub backoff_base_secs: Option<i32>,
    pub backoff_max_secs: Option<i32>,
    pub trace_context: Option<Value>,
    pub dead_lettered_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    pub capacity_deferrals: i32,
    pub schedule_name: Option<String>,
}

impl WasmJob {
    fn unrecorded_lease(&self) -> Option<DateTime<Utc>> {
        self.started_at
            .map(|started| started + chrono::Duration::seconds(i64::from(self.timeout_secs) * 2))
    }

    pub fn is_held(&self, now: DateTime<Utc>) -> bool {
        self.status == JobStatus::Running
            && self
                .lease_until
                .or_else(|| self.unrecorded_lease())
                .is_some_and(|until| until >= now)
    }

    pub fn out_of_attempts(&self) -> bool {
        self.attempts >= self.max_attempts
    }

    pub fn backoff(&self) -> Backoff {
        let secs = |declared: Option<i32>, default: u32| {
            Duration::from_secs(u64::from(
                declared
                    .and_then(|secs| u32::try_from(secs).ok())
                    .filter(|secs| *secs > 0)
                    .unwrap_or(default),
            ))
        };
        Backoff::exponential(
            secs(self.backoff_base_secs, BackoffSpec::DEFAULT_BASE_SECS),
            secs(self.backoff_max_secs, BackoffSpec::DEFAULT_MAX_SECS),
        )
    }

    pub fn jitter_seed(&self) -> u64 {
        let (high, low) = self.id.as_u64_pair();
        high ^ low
    }
}

fn retry_on_of(names: &[String]) -> Vec<RetryOn> {
    [RetryOn::Error, RetryOn::Timeout]
        .into_iter()
        .filter(|failure| names.iter().any(|name| name == failure.as_str()))
        .collect()
}

fn job_of(row: &Row) -> PlatformResult<WasmJob> {
    let id: Uuid = row.try_get("id")?;
    let status: String = row.try_get("status")?;
    let retry_on: Vec<String> = row.try_get("retry_on")?;
    Ok(WasmJob {
        id,
        tenant_id: row.try_get("tenant_id")?,
        tenant_slug: row.try_get("tenant_slug")?,
        service_id: row.try_get("service_id")?,
        service_slug: row.try_get("service_slug")?,
        service_version: row.try_get("service_version")?,
        handler: row.try_get("handler")?,
        status: JobStatus::parse(&status).ok_or_else(|| {
            PlatformError::Internal(format!("job {id} has the unknown status '{status}'"))
        })?,
        request_json: row.try_get("request_json")?,
        auth_subject: row.try_get("auth_subject")?,
        auth_claims: row.try_get("auth_claims")?,
        result_json: row.try_get("result_json")?,
        error: row.try_get("error")?,
        attempts: row.try_get("attempts")?,
        max_attempts: row.try_get("max_attempts")?,
        timeout_secs: row.try_get("timeout_secs")?,
        created_at: row.try_get("created_at")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        expires_at: row.try_get("expires_at")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_until: row.try_get("lease_until")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        published_at: row.try_get("published_at")?,
        last_error: row.try_get("last_error")?,
        error_class: row.try_get("error_class")?,
        retry_on: retry_on_of(&retry_on),
        backoff_base_secs: row.try_get("backoff_base_secs")?,
        backoff_max_secs: row.try_get("backoff_max_secs")?,
        trace_context: row.try_get("trace_context")?,
        dead_lettered_at: row.try_get("dead_lettered_at")?,
        idempotency_key: row.try_get("idempotency_key")?,
        capacity_deferrals: row.try_get("capacity_deferrals")?,
        schedule_name: row.try_get("schedule_name")?,
    })
}

fn only_job(row: Option<Row>) -> PlatformResult<Option<WasmJob>> {
    row.as_ref().map(job_of).transpose()
}

fn jobs_of(rows: Vec<Row>) -> PlatformResult<Vec<WasmJob>> {
    rows.iter().map(job_of).collect()
}

#[derive(Clone)]
pub struct NewJob<'a> {
    pub tenant_id: Uuid,
    pub tenant_slug: &'a str,
    pub service_id: Uuid,
    pub service_slug: &'a str,
    pub service_version: i32,
    pub handler: &'a str,
    pub request_json: &'a Value,
    pub auth_subject: Option<&'a str>,
    pub auth_claims: Option<&'a Value>,
    pub max_attempts: i32,
    pub timeout_secs: i32,
    pub retry_on: &'a [RetryOn],
    pub backoff: Option<BackoffSpec>,
    pub trace_context: Option<&'a Value>,
    pub idempotency_key: Option<&'a str>,
    pub schedule_name: Option<&'a str>,
    pub retain_for: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Enqueued {
    Created(WasmJob),
    Existing(WasmJob),
}

impl Enqueued {
    pub fn job(&self) -> &WasmJob {
        match self {
            Enqueued::Created(job) | Enqueued::Existing(job) => job,
        }
    }
}

fn seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

fn whole_secs(secs: u32) -> i32 {
    i32::try_from(secs).unwrap_or(i32::MAX)
}

pub async fn create(pool: &deadpool_postgres::Pool, job: NewJob<'_>) -> PlatformResult<Enqueued> {
    let client = pool.get().await?;
    let retry_on: Vec<&str> = job
        .retry_on
        .iter()
        .map(|failure| failure.as_str())
        .collect();
    let created = only_job(
        client
            .query_opt(
                "INSERT INTO wasm_jobs
                    (tenant_id, tenant_slug, service_id, service_slug, service_version,
                     handler, status, request_json, auth_subject, auth_claims,
                     max_attempts, timeout_secs, retry_on, backoff_base_secs, backoff_max_secs,
                     trace_context, idempotency_key, schedule_name, expires_at)
                 VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $9, $10, $11, $12, $13, $14,
                         $15, $16, $17, now() + make_interval(secs => $18))
                 ON CONFLICT (service_id, idempotency_key) WHERE idempotency_key IS NOT NULL
                 DO NOTHING
                 RETURNING *",
                &[
                    &job.tenant_id,
                    &job.tenant_slug,
                    &job.service_id,
                    &job.service_slug,
                    &job.service_version,
                    &job.handler,
                    &job.request_json,
                    &job.auth_subject,
                    &job.auth_claims,
                    &job.max_attempts,
                    &job.timeout_secs,
                    &retry_on,
                    &job.backoff.map(|backoff| whole_secs(backoff.base_secs)),
                    &job.backoff.map(|backoff| whole_secs(backoff.max_secs)),
                    &job.trace_context,
                    &job.idempotency_key,
                    &job.schedule_name,
                    &seconds(job.retain_for),
                ],
            )
            .await?,
    )?;
    match created {
        Some(job) => Ok(Enqueued::Created(job)),
        None => keyed(&client, job.service_id, job.idempotency_key)
            .await?
            .map(Enqueued::Existing)
            .ok_or_else(|| {
                PlatformError::Internal(
                    "the job enqueued under this idempotency key before is gone".into(),
                )
            }),
    }
}

async fn keyed(
    client: &deadpool_postgres::Object,
    service_id: Uuid,
    idempotency_key: Option<&str>,
) -> PlatformResult<Option<WasmJob>> {
    only_job(
        client
            .query_opt(
                "SELECT * FROM wasm_jobs WHERE service_id = $1 AND idempotency_key = $2",
                &[&service_id, &idempotency_key],
            )
            .await?,
    )
}

pub async fn enqueued_under(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
    idempotency_key: &str,
) -> PlatformResult<Option<WasmJob>> {
    keyed(&pool.get().await?, service_id, Some(idempotency_key)).await
}

pub async fn unfinished(pool: &deadpool_postgres::Pool, tenant_id: Uuid) -> PlatformResult<i64> {
    let client = pool.get().await?;
    Ok(client
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM wasm_jobs
              WHERE tenant_id = $1 AND status IN ('pending','running')",
            &[&tenant_id],
        )
        .await?
        .try_get(0)?)
}

pub async fn unfinished_by_tenant(
    pool: &deadpool_postgres::Pool,
) -> PlatformResult<Vec<(String, i64)>> {
    let client = pool.get().await?;
    client
        .query(
            "SELECT tenant_slug, COUNT(*)::BIGINT FROM wasm_jobs
              WHERE status IN ('pending','running')
              GROUP BY tenant_slug",
            &[],
        )
        .await?
        .iter()
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .collect()
}

pub async fn schedule_has_unfinished(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
    schedule_name: &str,
) -> PlatformResult<bool> {
    let client = pool.get().await?;
    Ok(client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM wasm_jobs
                             WHERE service_id = $1 AND schedule_name = $2
                               AND status IN ('pending', 'running'))",
            &[&service_id, &schedule_name],
        )
        .await?
        .try_get(0)?)
}

pub async fn set_published(
    pool: &deadpool_postgres::Pool,
    job_id: Uuid,
    published: bool,
) -> PlatformResult<()> {
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE wasm_jobs SET published_at = CASE WHEN $2 THEN now() END WHERE id = $1",
            &[&job_id, &published],
        )
        .await?;
    Ok(())
}

pub async fn get(pool: &deadpool_postgres::Pool, job_id: Uuid) -> PlatformResult<Option<WasmJob>> {
    let client = pool.get().await?;
    only_job(
        client
            .query_opt("SELECT * FROM wasm_jobs WHERE id = $1", &[&job_id])
            .await?,
    )
}

const LEASE_EXPIRED: &str = "(lease_until < now()
      OR (lease_until IS NULL AND started_at < now() - make_interval(secs => timeout_secs * 2)))";

const UNHELD: &str = "lease_owner = NULL, lease_until = NULL";
const CONCLUDED: &str = "lease_owner = NULL, lease_until = NULL, capacity_deferrals = 0";

#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    pub job: WasmJob,
    pub fence: Fence,
}

pub async fn claim(
    pool: &deadpool_postgres::Pool,
    job_id: Uuid,
    owner: &str,
    lease: Duration,
) -> PlatformResult<Option<Claim>> {
    let client = pool.get().await?;
    let claimed = only_job(
        client
            .query_opt(
                &format!(
                    "UPDATE wasm_jobs
                        SET status = 'running',
                            attempts = attempts + 1,
                            lease_owner = $2,
                            lease_until = now() + make_interval(secs => $3),
                            started_at = now()
                      WHERE id = $1
                        AND attempts < max_attempts
                        AND ((status = 'pending' AND next_attempt_at <= now())
                          OR (status = 'running' AND {LEASE_EXPIRED}))
                      RETURNING *"
                ),
                &[&job_id, &owner, &seconds(lease)],
            )
            .await?,
    )?;
    Ok(claimed.map(|job| Claim {
        fence: Fence {
            job_id: job.id,
            owner: owner.to_string(),
            attempt: job.attempts,
        },
        job,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fence {
    pub job_id: Uuid,
    pub owner: String,
    pub attempt: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fenced {
    Held,
    Lost,
}

fn held(written: Option<WasmJob>) -> Fenced {
    written.map_or(Fenced::Lost, |_| Fenced::Held)
}

const FENCE: &str = "id = $1 AND lease_owner = $2 AND attempts = $3 AND status = 'running'";

async fn fenced(
    pool: &deadpool_postgres::Pool,
    fence: &Fence,
    set: &str,
    params: &[&(dyn ToSql + Sync)],
) -> PlatformResult<Option<WasmJob>> {
    let client = pool.get().await?;
    let of_fence: [&(dyn ToSql + Sync); 3] = [&fence.job_id, &fence.owner, &fence.attempt];
    let params: Vec<&(dyn ToSql + Sync)> =
        of_fence.into_iter().chain(params.iter().copied()).collect();
    only_job(
        client
            .query_opt(
                &format!("UPDATE wasm_jobs SET {set} WHERE {FENCE} RETURNING *"),
                &params,
            )
            .await?,
    )
}

pub async fn heartbeat(
    pool: &deadpool_postgres::Pool,
    fence: &Fence,
    lease: Duration,
) -> PlatformResult<Fenced> {
    fenced(
        pool,
        fence,
        "lease_until = now() + make_interval(secs => $4)",
        &[&seconds(lease)],
    )
    .await
    .map(held)
}

pub async fn complete(
    pool: &deadpool_postgres::Pool,
    fence: &Fence,
    result: &Value,
    retain_for: Duration,
) -> PlatformResult<Fenced> {
    fenced(
        pool,
        fence,
        &format!(
            "status = 'completed', result_json = $4, finished_at = now(),
             expires_at = now() + make_interval(secs => $5), {CONCLUDED}"
        ),
        &[&result, &seconds(retain_for)],
    )
    .await
    .map(held)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure<'a> {
    pub error: &'a str,
    pub class: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requeue {
    Redelivered,
    Republished,
}

pub async fn retry(
    pool: &deadpool_postgres::Pool,
    fence: &Fence,
    failure: Failure<'_>,
    next_attempt_at: DateTime<Utc>,
    requeue: Requeue,
) -> PlatformResult<Fenced> {
    fenced(
        pool,
        fence,
        &format!(
            "status = 'pending', next_attempt_at = $4, last_error = $5, error_class = $6,
             published_at = CASE WHEN $7 THEN now() END, {CONCLUDED}"
        ),
        &[
            &next_attempt_at,
            &failure.error,
            &failure.class,
            &(requeue == Requeue::Redelivered),
        ],
    )
    .await
    .map(held)
}

fn failed(first: usize) -> String {
    format!(
        "status = 'failed', error = ${first}, last_error = ${first}, error_class = ${},
         finished_at = now(), expires_at = now() + make_interval(secs => ${}), {CONCLUDED}",
        first + 1,
        first + 2
    )
}

pub async fn fail(
    pool: &deadpool_postgres::Pool,
    fence: &Fence,
    failure: Failure<'_>,
    retain_for: Duration,
) -> PlatformResult<Option<WasmJob>> {
    fenced(
        pool,
        fence,
        &failed(4),
        &[&failure.error, &failure.class, &seconds(retain_for)],
    )
    .await
}

pub async fn release(pool: &deadpool_postgres::Pool, fence: &Fence) -> PlatformResult<Fenced> {
    fenced(
        pool,
        fence,
        &format!(
            "status = 'pending', attempts = attempts - 1, next_attempt_at = now(),
             published_at = now(), {CONCLUDED}"
        ),
        &[],
    )
    .await
    .map(held)
}

pub async fn defer(
    pool: &deadpool_postgres::Pool,
    fence: &Fence,
    after: Duration,
) -> PlatformResult<Fenced> {
    fenced(
        pool,
        fence,
        &format!(
            "status = 'pending', attempts = attempts - 1,
             capacity_deferrals = capacity_deferrals + 1,
             next_attempt_at = now() + make_interval(secs => $4),
             published_at = now(), {UNHELD}"
        ),
        &[&seconds(after)],
    )
    .await
    .map(held)
}

pub async fn fail_exhausted(
    pool: &deadpool_postgres::Pool,
    job_id: Uuid,
    failure: Failure<'_>,
    retain_for: Duration,
) -> PlatformResult<Option<WasmJob>> {
    let client = pool.get().await?;
    only_job(
        client
            .query_opt(
                &format!(
                    "UPDATE wasm_jobs SET {}
                      WHERE id = $1
                        AND attempts >= max_attempts
                        AND (status = 'pending' OR (status = 'running' AND {LEASE_EXPIRED}))
                      RETURNING *",
                    failed(2)
                ),
                &[
                    &job_id,
                    &failure.error,
                    &failure.class,
                    &seconds(retain_for),
                ],
            )
            .await?,
    )
}

pub async fn retry_failed(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    job_id: Uuid,
    retain_for: Duration,
) -> PlatformResult<Option<WasmJob>> {
    let client = pool.get().await?;
    only_job(
        client
            .query_opt(
                "UPDATE wasm_jobs
                    SET status = 'pending', max_attempts = attempts + 1, next_attempt_at = now(),
                        published_at = NULL, dead_lettered_at = NULL, error = NULL,
                        finished_at = NULL,
                        expires_at = now() + make_interval(secs => $3)
                  WHERE id = $1 AND tenant_id = $2 AND status = 'failed'
                  RETURNING *",
                &[&job_id, &tenant_id, &seconds(retain_for)],
            )
            .await?,
    )
}

pub async fn reclaim(
    pool: &deadpool_postgres::Pool,
    republish_after: Duration,
    limit: i64,
) -> PlatformResult<Vec<WasmJob>> {
    let client = pool.get().await?;
    jobs_of(
        client
            .query(
                &format!(
                    "WITH due AS (
                       SELECT id FROM wasm_jobs
                        WHERE ((status = 'pending' AND next_attempt_at <= now())
                            OR (status = 'running' AND {LEASE_EXPIRED}))
                          AND (published_at IS NULL
                            OR published_at < now() - make_interval(secs => $1)
                            OR attempts >= max_attempts
                            OR (status = 'running' AND published_at < COALESCE(
                                  lease_until,
                                  started_at + make_interval(secs => timeout_secs * 2))))
                        ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED)
                     UPDATE wasm_jobs j SET published_at = now() FROM due WHERE j.id = due.id
                     RETURNING j.*"
                ),
                &[&seconds(republish_after), &limit],
            )
            .await?,
    )
}

pub async fn undelivered_dead_letters(
    pool: &deadpool_postgres::Pool,
    limit: i64,
) -> PlatformResult<Vec<WasmJob>> {
    let client = pool.get().await?;
    jobs_of(
        client
            .query(
                "SELECT * FROM wasm_jobs
                  WHERE status = 'failed' AND dead_lettered_at IS NULL
                    AND error_class IS NOT NULL
                  ORDER BY finished_at LIMIT $1",
                &[&limit],
            )
            .await?,
    )
}

pub async fn set_dead_lettered(pool: &deadpool_postgres::Pool, job_id: Uuid) -> PlatformResult<()> {
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE wasm_jobs SET dead_lettered_at = now() WHERE id = $1 AND status = 'failed'",
            &[&job_id],
        )
        .await?;
    Ok(())
}

pub async fn delete_expired(pool: &deadpool_postgres::Pool, limit: i64) -> PlatformResult<u64> {
    let client = pool.get().await?;
    Ok(client
        .execute(
            "DELETE FROM wasm_jobs
              WHERE id IN (
                SELECT id FROM wasm_jobs
                 WHERE status IN ('completed', 'failed')
                   AND expires_at < now()
                 ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED)",
            &[&limit],
        )
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_is_the_text_the_table_holds() {
        for status in JobStatus::ALL {
            assert_eq!(JobStatus::parse(status.as_str()), Some(status));
            assert_eq!(
                serde_json::to_value(status).expect("serialize"),
                serde_json::json!(status.as_str())
            );
        }
        assert_eq!(JobStatus::parse("dead"), None);
        assert!(JobStatus::Completed.is_terminal() && JobStatus::Failed.is_terminal());
        assert!(!JobStatus::Pending.is_terminal() && !JobStatus::Running.is_terminal());
    }

    #[test]
    fn a_failure_this_release_does_not_know_is_not_retried() {
        let names = |names: &[&str]| {
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(retry_on_of(&names(&[])), vec![]);
        assert_eq!(
            retry_on_of(&names(&["timeout", "error"])),
            vec![RetryOn::Error, RetryOn::Timeout]
        );
        assert_eq!(
            retry_on_of(&names(&["rate_limited", "timeout"])),
            vec![RetryOn::Timeout]
        );
    }
}
