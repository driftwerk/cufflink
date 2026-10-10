use crate::errors::PlatformResult;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl JobStatus {
    pub const ALL: [JobStatus; 4] = [
        JobStatus::Queued,
        JobStatus::Running,
        JobStatus::Succeeded,
        JobStatus::Failed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Succeeded => "succeeded",
            JobStatus::Failed => "failed",
        }
    }

    fn of(text: &str) -> Self {
        match text {
            "queued" => JobStatus::Queued,
            "running" => JobStatus::Running,
            "succeeded" => JobStatus::Succeeded,
            _ => JobStatus::Failed,
        }
    }

    pub fn is_final(self) -> bool {
        matches!(self, JobStatus::Succeeded | JobStatus::Failed)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DeployJob {
    pub id: Uuid,
    pub service_id: Uuid,
    pub status: JobStatus,
    pub replica: String,
    pub heartbeat_at: DateTime<Utc>,
    pub request: Value,
    pub progress: Value,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    Succeeded(Value),
    Failed(String),
}

const COLUMNS: &str = "id, service_id, status, replica, heartbeat_at, request, progress, result, \
                       error, created_by, created_at, finished_at";

fn row_to_job(row: &tokio_postgres::Row) -> DeployJob {
    DeployJob {
        id: row.get("id"),
        service_id: row.get("service_id"),
        status: JobStatus::of(row.get("status")),
        replica: row.get("replica"),
        heartbeat_at: row.get("heartbeat_at"),
        request: row.get("request"),
        progress: row.get("progress"),
        result: row.get("result"),
        error: row.get("error"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        finished_at: row.get("finished_at"),
    }
}

pub async fn create(
    pool: &Pool,
    service_id: Uuid,
    replica: &str,
    request: &Value,
    created_by: &str,
) -> PlatformResult<DeployJob> {
    let row = pool
        .get()
        .await?
        .query_one(
            &format!(
                "INSERT INTO deploy_jobs (service_id, status, replica, request, created_by)
                 VALUES ($1, 'queued', $2, $3, $4)
                 RETURNING {COLUMNS}"
            ),
            &[&service_id, &replica, request, &created_by],
        )
        .await?;
    Ok(row_to_job(&row))
}

pub const LOST: &str = "replica lost";

pub type Heartbeat = DateTime<Utc>;

pub async fn start(pool: &Pool, id: Uuid) -> PlatformResult<Heartbeat> {
    let row = pool
        .get()
        .await?
        .query_one(
            "UPDATE deploy_jobs SET status = 'running', heartbeat_at = now() WHERE id = $1
             RETURNING heartbeat_at",
            &[&id],
        )
        .await?;
    Ok(row.get(0))
}

pub async fn heartbeat(pool: &Pool, id: Uuid) -> PlatformResult<Option<Heartbeat>> {
    let row = pool
        .get()
        .await?
        .query_opt(
            "UPDATE deploy_jobs SET heartbeat_at = now() WHERE id = $1 AND status = 'running'
             RETURNING heartbeat_at",
            &[&id],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}

pub async fn progress(pool: &Pool, id: Uuid, progress: &Value) -> PlatformResult<()> {
    pool.get()
        .await?
        .execute(
            "UPDATE deploy_jobs SET progress = $2
             WHERE id = $1 AND (status IN ('queued', 'running') OR error = $3)",
            &[&id, progress, &LOST],
        )
        .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finished {
    Recorded,
    Corrected,
    Kept,
}

pub async fn finish(
    pool: &Pool,
    id: Uuid,
    ending: &Ending,
    last: Option<Heartbeat>,
) -> PlatformResult<Finished> {
    let (status, result, error) = match ending {
        Ending::Succeeded(result) => ("succeeded", Some(result), None),
        Ending::Failed(error) => ("failed", None, Some(error.as_str())),
    };
    let row = pool
        .get()
        .await?
        .query_opt(
            "WITH ended AS (
                 SELECT id, status = 'failed' AS lost FROM deploy_jobs
                 WHERE id = $1
                   AND (status IN ('queued', 'running')
                        OR (status = 'failed' AND error = $5 AND heartbeat_at = $6))
                 FOR UPDATE)
             UPDATE deploy_jobs j
             SET status = $2, result = $3, error = $4, finished_at = now(), heartbeat_at = now()
             FROM ended WHERE j.id = ended.id
             RETURNING ended.lost",
            &[&id, &status, &result, &error, &LOST, &last],
        )
        .await?;
    Ok(match row.map(|row| row.get::<_, bool>(0)) {
        Some(false) => Finished::Recorded,
        Some(true) => Finished::Corrected,
        None => Finished::Kept,
    })
}

pub async fn get_for_tenant(
    pool: &Pool,
    id: Uuid,
    tenant_id: Uuid,
) -> PlatformResult<Option<DeployJob>> {
    let row = pool
        .get()
        .await?
        .query_opt(
            &format!(
                "SELECT {} FROM deploy_jobs j
                 JOIN services s ON s.id = j.service_id
                 WHERE j.id = $1 AND s.tenant_id = $2",
                COLUMNS
                    .split(", ")
                    .map(|column| format!("j.{column}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            &[&id, &tenant_id],
        )
        .await?;
    Ok(row.as_ref().map(row_to_job))
}

pub async fn sweep_lost(pool: &Pool, stale: Duration) -> PlatformResult<Vec<Uuid>> {
    let rows = pool
        .get()
        .await?
        .query(
            "UPDATE deploy_jobs
             SET status = 'failed', error = $2, finished_at = now()
             WHERE status IN ('queued', 'running')
               AND heartbeat_at < now() - make_interval(secs => $1)
             RETURNING id",
            &[&stale.as_secs_f64(), &LOST],
        )
        .await?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

pub async fn prune(pool: &Pool, retention: Duration) -> PlatformResult<u64> {
    Ok(pool
        .get()
        .await?
        .execute(
            "DELETE FROM deploy_jobs
             WHERE status IN ('succeeded', 'failed')
               AND finished_at < now() - make_interval(secs => $1)",
            &[&retention.as_secs_f64()],
        )
        .await?)
}

pub async fn counts(pool: &Pool) -> PlatformResult<Vec<(JobStatus, i64)>> {
    let rows = pool
        .get()
        .await?
        .query(
            "SELECT status, count(*) FROM deploy_jobs GROUP BY status",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| (JobStatus::of(row.get(0)), row.get(1)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_is_read_back_as_it_was_written() {
        for status in JobStatus::ALL {
            assert_eq!(JobStatus::of(status.as_str()), status);
        }
        assert_eq!(JobStatus::of("lost"), JobStatus::Failed);
        assert!(JobStatus::Succeeded.is_final() && JobStatus::Failed.is_final());
        assert!(!JobStatus::Running.is_final() && !JobStatus::Queued.is_final());
        assert_eq!(
            serde_json::to_string(&JobStatus::Running).expect("json"),
            "\"running\""
        );
    }
}
