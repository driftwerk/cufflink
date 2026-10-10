use crate::errors::PlatformResult;
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct BackupJob {
    pub id: Uuid,
    pub service_id: Uuid,
    pub tenant_id: Uuid,
    pub job_type: String,
    pub status: String,
    pub tables_filter: Option<Vec<String>>,
    pub clear_existing: bool,
    pub s3_key: Option<String>,
    pub total_rows: i64,
    pub processed_rows: i64,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

fn row_to_job(row: &tokio_postgres::Row) -> BackupJob {
    BackupJob {
        id: row.get("id"),
        service_id: row.get("service_id"),
        tenant_id: row.get("tenant_id"),
        job_type: row.get("job_type"),
        status: row.get("status"),
        tables_filter: row.get("tables_filter"),
        clear_existing: row.get("clear_existing"),
        s3_key: row.get("s3_key"),
        total_rows: row.get("total_rows"),
        processed_rows: row.get("processed_rows"),
        error_message: row.get("error_message"),
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
    }
}

pub async fn create(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
    tenant_id: Uuid,
    job_type: &str,
    tables_filter: Option<&[String]>,
    clear_existing: bool,
    s3_key: Option<&str>,
) -> PlatformResult<BackupJob> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "INSERT INTO backup_jobs (service_id, tenant_id, job_type, tables_filter, clear_existing, s3_key)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING *",
            &[&service_id, &tenant_id, &job_type, &tables_filter, &clear_existing, &s3_key],
        )
        .await?;
    Ok(row_to_job(&row))
}

pub async fn get_by_id(
    pool: &deadpool_postgres::Pool,
    job_id: Uuid,
) -> PlatformResult<Option<BackupJob>> {
    let client = pool.get().await?;
    let row = client
        .query_opt("SELECT * FROM backup_jobs WHERE id = $1", &[&job_id])
        .await?;
    Ok(row.as_ref().map(row_to_job))
}

pub async fn list_by_service(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
) -> PlatformResult<Vec<BackupJob>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT * FROM backup_jobs WHERE service_id = $1 ORDER BY created_at DESC LIMIT 50",
            &[&service_id],
        )
        .await?;
    Ok(rows.iter().map(row_to_job).collect())
}

pub async fn update_status(
    pool: &deadpool_postgres::Pool,
    job_id: Uuid,
    status: &str,
    error_message: Option<&str>,
) -> PlatformResult<()> {
    let client = pool.get().await?;
    let now: Option<DateTime<Utc>> = match status {
        "running" => Some(Utc::now()),
        _ => None,
    };
    let completed: Option<DateTime<Utc>> = match status {
        "completed" | "failed" | "cancelled" => Some(Utc::now()),
        _ => None,
    };
    client
        .execute(
            "UPDATE backup_jobs SET status = $2, error_message = $3,
             started_at = COALESCE($4, started_at),
             completed_at = COALESCE($5, completed_at)
             WHERE id = $1",
            &[&job_id, &status, &error_message, &now, &completed],
        )
        .await?;
    Ok(())
}

pub async fn claim(
    pool: &deadpool_postgres::Pool,
    job: &BackupJob,
    redelivered: bool,
) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let claimed = client
        .execute(
            "UPDATE backup_jobs SET status = 'running', started_at = now()
             WHERE id = $1 AND service_id = $2 AND tenant_id = $3
               AND (status = 'queued' OR (status = 'running' AND $4))",
            &[&job.id, &job.service_id, &job.tenant_id, &redelivered],
        )
        .await?;
    Ok(claimed == 1)
}

pub async fn update_progress(
    pool: &deadpool_postgres::Pool,
    job_id: Uuid,
    processed_rows: i64,
    total_rows: i64,
) -> PlatformResult<()> {
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE backup_jobs SET processed_rows = $2, total_rows = $3 WHERE id = $1",
            &[&job_id, &processed_rows, &total_rows],
        )
        .await?;
    Ok(())
}

pub async fn update_s3_key(
    pool: &deadpool_postgres::Pool,
    job_id: Uuid,
    s3_key: &str,
) -> PlatformResult<()> {
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE backup_jobs SET s3_key = $2 WHERE id = $1",
            &[&job_id, &s3_key],
        )
        .await?;
    Ok(())
}

pub async fn cancel(pool: &deadpool_postgres::Pool, job_id: Uuid) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let rows = client
        .execute(
            "UPDATE backup_jobs SET status = 'cancelled', completed_at = now()
             WHERE id = $1 AND status IN ('queued', 'running')",
            &[&job_id],
        )
        .await?;
    Ok(rows > 0)
}

pub async fn is_cancelled(pool: &deadpool_postgres::Pool, job_id: Uuid) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT 1 FROM backup_jobs WHERE id = $1 AND status = 'cancelled'",
            &[&job_id],
        )
        .await?;
    Ok(row.is_some())
}

pub async fn mark_failed(
    pool: &deadpool_postgres::Pool,
    job_id: Uuid,
    error: &str,
) -> PlatformResult<()> {
    update_status(pool, job_id, "failed", Some(error)).await
}
