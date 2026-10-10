use crate::http::WorkerReadiness;
use crate::jobs;
use cufflink_platform::api::backup::{backup_source_service, export_key};
use cufflink_platform::nats_manager::delivery::{decide, Decision, Outcome};
use cufflink_platform::nats_manager::dlq::DeadLetter;
use cufflink_platform::nats_manager::pump::{self, MessageMeta, PumpConfig, Work, WorkLabel};
use cufflink_platform::repo::backup_jobs::{self, BackupJob};
use cufflink_platform::repo::services;
use cufflink_platform::router::row_convert::{manifest_ident, row_to_json};
use cufflink_platform::tenant_sessions::{Purpose, Scope, ServiceSessions, Statements};
use cufflink_platform::AppState;
use cufflink_telemetry::{scopable_span, Kind};
use cufflink_types::backoff::Backoff;
use cufflink_types::AckMode;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;
use uuid::Uuid;

const BACKUP_ACK_WAIT: Duration = Duration::from_secs(300);
const BACKUP_MAX_DELIVER: i64 = 3;
const BACKUP_RETRY: Duration = Duration::from_secs(30);
const CURSOR_BATCH_SIZE: i64 = 1000;
const INSERT_BATCH_SIZE: usize = 1000;

#[derive(Deserialize)]
struct BackupMessage {
    job_id: Uuid,
    service_id: Uuid,
    tenant_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
enum Refused {
    #[error("backup job {0} does not exist")]
    Unknown(Uuid),
    #[error("backup job {0} is not a job of the service and tenant its message names")]
    Foreign(Uuid),
}

fn admitted(message: &BackupMessage, job: Option<BackupJob>) -> Result<BackupJob, Refused> {
    let job = job.ok_or(Refused::Unknown(message.job_id))?;
    match (job.service_id, job.tenant_id) == (message.service_id, message.tenant_id) {
        true => Ok(job),
        false => Err(Refused::Foreign(message.job_id)),
    }
}

const NO_STATEMENT_TIMEOUT: Duration = Duration::ZERO;

pub async fn run_backup_consumer(
    app_state: Arc<AppState>,
    readiness: Arc<WorkerReadiness>,
) -> eyre::Result<()> {
    use async_nats::jetstream::consumer;

    let stream =
        cufflink_platform::nats_manager::events::backup_stream(&app_state.jetstream).await?;

    let consumer = stream
        .get_or_create_consumer(
            "backup-worker",
            consumer::pull::Config {
                durable_name: Some("backup-worker".to_string()),
                ack_wait: BACKUP_ACK_WAIT,
                max_deliver: BACKUP_MAX_DELIVER,
                ..Default::default()
            },
        )
        .await?;

    tracing::info!("Backup consumer ready");
    let _up = readiness.consumer_up();
    let exit = pump::run(
        consumer,
        PumpConfig::sequential(BACKUP_ACK_WAIT),
        Arc::new(BackupWork {
            app_state: app_state.clone(),
        }),
        Arc::new(app_state.jetstream.clone()),
        pump::Stop::new(),
        app_state.shutdown.clone(),
    )
    .await;
    jobs::ended("backup", exit)
}

struct BackupWork {
    app_state: Arc<AppState>,
}

impl Work for BackupWork {
    fn label(&self) -> WorkLabel {
        WorkLabel {
            kind: "backup",
            subscription: None,
        }
    }

    fn handle(
        &self,
        msg: Arc<async_nats::jetstream::Message>,
        meta: MessageMeta,
    ) -> BoxFuture<'static, Outcome> {
        let app_state = self.app_state.clone();
        async move { run_backup_job(&app_state, &msg.payload, meta.delivered > 1).await }.boxed()
    }

    fn decide(&self, outcome: &Outcome, meta: &MessageMeta) -> Decision {
        settle(outcome, meta)
    }

    fn dead_letter<'a>(
        &'a self,
        _msg: &'a async_nats::jetstream::Message,
        _meta: &'a MessageMeta,
        _reason: &'a str,
    ) -> BoxFuture<'a, Option<DeadLetter>> {
        std::future::ready(None).boxed()
    }
}

fn settle(outcome: &Outcome, meta: &MessageMeta) -> Decision {
    decide(
        AckMode::OnSuccess,
        outcome,
        meta.delivered,
        BACKUP_MAX_DELIVER,
        &Backoff::explicit(vec![BACKUP_RETRY]),
        false,
        meta.stream_sequence,
    )
}

fn retry(job_id: Uuid, doing: &str, error: impl std::fmt::Display) -> Outcome {
    tracing::error!(job_id = %job_id, error = %error, "Cannot {doing} the backup job; trying again");
    Outcome::Retry {
        after: None,
        reason: error.to_string(),
    }
}

async fn claimed(
    pool: &deadpool_postgres::Pool,
    message: &BackupMessage,
    redelivered: bool,
) -> Result<BackupJob, Outcome> {
    let job = backup_jobs::get_by_id(pool, message.job_id)
        .await
        .map_err(|e| retry(message.job_id, "read", e))?;
    let job = admitted(message, job).map_err(|refused| {
        tracing::warn!(job_id = %message.job_id, %refused, "Refusing a backup message");
        Outcome::Invalid {
            reason: refused.to_string(),
        }
    })?;
    match backup_jobs::claim(pool, &job, redelivered).await {
        Ok(true) => Ok(job),
        Ok(false) => {
            tracing::info!(job_id = %job.id, "Backup job already taken; not running it again");
            Err(Outcome::Ok)
        }
        Err(e) => Err(retry(job.id, "claim", e)),
    }
}

async fn run_backup_job(app_state: &Arc<AppState>, payload: &[u8], redelivered: bool) -> Outcome {
    let message: BackupMessage = match serde_json::from_slice(payload) {
        Ok(message) => message,
        Err(e) => {
            tracing::error!("Invalid backup message: {}", e);
            return Outcome::Invalid {
                reason: e.to_string(),
            };
        }
    };
    match claimed(&app_state.platform_db, &message, redelivered).await {
        Ok(job) => {
            run_claimed(app_state, &job)
                .instrument(scopable_span!(parent: None, "backup_job", job_id = %job.id))
                .await
        }
        Err(settled) => settled,
    }
}

async fn run_claimed(app_state: &Arc<AppState>, job: &BackupJob) -> Outcome {
    let result = match job.job_type.as_str() {
        "export" => process_export(app_state, job).await,
        "restore" => process_restore(app_state, job).await,
        other => Err(eyre::eyre!("Unknown backup job type: {other}")),
    };
    match result {
        Ok(()) => Outcome::Ok,
        Err(e) => {
            tracing::error!(job_id = %job.id, "Backup job failed: {}", e);
            let _ = backup_jobs::mark_failed(&app_state.platform_db, job.id, &e.to_string()).await;
            Outcome::Terminal {
                reason: e.to_string(),
            }
        }
    }
}

async fn process_export(state: &Arc<AppState>, job: &BackupJob) -> eyre::Result<()> {
    let manifest = load_manifest(state, job.service_id).await?;
    let tables = filter_tables(&manifest, job.tables_filter.as_deref());

    let tenant_pool = service_sessions(state, job).await?;

    let mut output = Vec::new();
    let mut total_rows: i64 = 0;

    for table in &tables {
        if backup_jobs::is_cancelled(&state.platform_db, job.id).await? {
            tracing::info!(job_id = %job.id, "Export cancelled");
            return Ok(());
        }

        let table_name = table["name"].as_str().unwrap_or("");
        let columns: Vec<cufflink_platform::deployer::manifest::ColumnDefinition> =
            serde_json::from_value(table["columns"].clone()).unwrap_or_default();

        let mut client = tenant_pool.get().await?;
        let prelude = client.prelude(Scope::Transaction, NO_STATEMENT_TIMEOUT);
        let txn = client.transaction().await?;
        txn.batch_execute(&prelude).await?;

        let cursor_name = manifest_ident(&format!("backup_cursor_{table_name}"));
        txn.execute(
            &format!(
                "DECLARE {} NO SCROLL CURSOR FOR SELECT * FROM {}",
                cursor_name,
                manifest_ident(table_name)
            ),
            &[],
        )
        .await?;

        loop {
            let rows = txn
                .query(
                    &format!("FETCH {} FROM {}", CURSOR_BATCH_SIZE, cursor_name),
                    &[],
                )
                .await?;

            if rows.is_empty() {
                break;
            }

            for row in &rows {
                let mut record = row_to_json(row, &columns, None);
                if let Some(obj) = record.as_object_mut() {
                    obj.insert("_table".to_string(), serde_json::json!(table_name));
                }
                output.extend_from_slice(serde_json::to_string(&record)?.as_bytes());
                output.push(b'\n');
                total_rows += 1;
            }

            backup_jobs::update_progress(&state.platform_db, job.id, total_rows, total_rows)
                .await?;
        }

        txn.commit().await?;

        tracing::info!(
            job_id = %job.id,
            table = table_name,
            rows = total_rows,
            "Exported table"
        );
    }

    let s3_key = export_key(job.service_id, job.id);
    let storage = state
        .storage
        .as_ref()
        .ok_or_else(|| eyre::eyre!("S3 not configured"))?;

    storage
        .upload(&s3_key, output, "application/x-ndjson")
        .await?;

    backup_jobs::update_s3_key(&state.platform_db, job.id, &s3_key).await?;
    backup_jobs::update_progress(&state.platform_db, job.id, total_rows, total_rows).await?;
    backup_jobs::update_status(&state.platform_db, job.id, "completed", None).await?;

    tracing::info!(
        job_id = %job.id,
        total_rows = total_rows,
        "Export completed"
    );

    Ok(())
}

async fn process_restore(state: &Arc<AppState>, job: &BackupJob) -> eyre::Result<()> {
    let s3_key = job
        .s3_key
        .as_deref()
        .ok_or_else(|| eyre::eyre!("No S3 key for restore job"))?;
    if backup_source_service(s3_key).is_none() {
        eyre::bail!("s3_key outside the backup prefix");
    }

    let manifest = load_manifest(state, job.service_id).await?;

    let storage = state
        .storage
        .as_ref()
        .ok_or_else(|| eyre::eyre!("S3 not configured"))?;

    let data = storage.download(s3_key).await?;
    let content = String::from_utf8(data)?;

    let tenant_pool = service_sessions(state, job).await?;

    let mut db_client = tenant_pool.get().await?;
    let prelude = db_client.prelude(Scope::Transaction, NO_STATEMENT_TIMEOUT);
    let txn = db_client.transaction().await?;
    txn.batch_execute(&prelude).await?;

    if job.clear_existing {
        for table_name in table_names(&filter_tables(&manifest, job.tables_filter.as_deref())) {
            txn.execute(&format!("DELETE FROM {}", manifest_ident(table_name)), &[])
                .await?;
        }
    }

    let all_columns = build_column_map(&manifest);
    let mut total_rows: i64 = 0;
    let mut batch_count = 0;

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }

        let record: serde_json::Value = serde_json::from_str(line)?;
        let table_name = record["_table"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("Record missing _table field"))?;

        if let Some(ref filter) = job.tables_filter {
            if !filter.iter().any(|t| t == table_name) {
                continue;
            }
        }

        let columns = all_columns
            .get(table_name)
            .ok_or_else(|| eyre::eyre!("Unknown table in backup: {}", table_name))?;

        let obj = record
            .as_object()
            .ok_or_else(|| eyre::eyre!("Record is not an object"))?;

        let mut col_names = Vec::new();
        let mut placeholders = Vec::new();
        let mut values: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

        for col in columns {
            let val = match obj.get(&col.name) {
                Some(v) => v,
                None => continue,
            };
            if val.is_null() && col.nullable {
                continue;
            }
            col_names.push(manifest_ident(&col.name));
            values.push(cufflink_platform::router::row_convert::json_param(
                col, val,
            )?);
            placeholders.push(format!("${}", values.len()));
        }

        if !col_names.is_empty() {
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING",
                manifest_ident(table_name),
                col_names.join(", "),
                placeholders.join(", "),
            );
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = values
                .iter()
                .map(|v| v.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            txn.execute(&sql, &param_refs).await?;
            total_rows += 1;
        }

        batch_count += 1;
        if batch_count % INSERT_BATCH_SIZE == 0 {
            backup_jobs::update_progress(&state.platform_db, job.id, total_rows, total_rows)
                .await?;

            if backup_jobs::is_cancelled(&state.platform_db, job.id).await? {
                tracing::info!(job_id = %job.id, "Restore cancelled, rolling back");
                txn.rollback().await?;
                return Ok(());
            }
        }
    }

    txn.commit().await?;

    backup_jobs::update_progress(&state.platform_db, job.id, total_rows, total_rows).await?;
    backup_jobs::update_status(&state.platform_db, job.id, "completed", None).await?;

    tracing::info!(
        job_id = %job.id,
        total_rows = total_rows,
        "Restore completed"
    );

    Ok(())
}

async fn load_manifest(state: &Arc<AppState>, service_id: Uuid) -> eyre::Result<serde_json::Value> {
    cufflink_platform::repo::deployments::current_manifest(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| eyre::eyre!("No deployment found for service"))
}

async fn service_sessions(state: &AppState, job: &BackupJob) -> eyre::Result<ServiceSessions> {
    let service = services::get_by_id(&state.platform_db, job.service_id)
        .await?
        .filter(|service| service.tenant_id == job.tenant_id)
        .ok_or_else(|| eyre::eyre!("Service not found"))?;
    let tenant = state
        .tenants
        .get_or_load(service.tenant_id)
        .await?
        .ok_or_else(|| eyre::eyre!("Tenant of the service not found"))?;
    let identity = service.identity(&tenant.tenant.slug);
    identity
        .scope(None)
        .of_kind(Kind::Job)
        .record_on(&tracing::Span::current());
    Ok(state
        .tenant_sessions
        .service(&identity, Purpose::Platform)
        .await?)
}

fn filter_tables<'a>(
    manifest: &'a serde_json::Value,
    tables_filter: Option<&[String]>,
) -> Vec<&'a serde_json::Value> {
    manifest["tables"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter(|t| {
                    tables_filter
                        .map(|filter| {
                            t["name"]
                                .as_str()
                                .map(|n| filter.iter().any(|f| f == n))
                                .unwrap_or(false)
                        })
                        .unwrap_or(true)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn table_names<'a>(tables: &[&'a serde_json::Value]) -> Vec<&'a str> {
    tables.iter().filter_map(|t| t["name"].as_str()).collect()
}

fn build_column_map(
    manifest: &serde_json::Value,
) -> std::collections::HashMap<String, Vec<cufflink_platform::deployer::manifest::ColumnDefinition>>
{
    let mut map = std::collections::HashMap::new();
    if let Some(tables) = manifest["tables"].as_array() {
        for table in tables {
            let name = table["name"].as_str().unwrap_or("").to_string();
            let cols: Vec<cufflink_platform::deployer::manifest::ColumnDefinition> =
                serde_json::from_value(table["columns"].clone()).unwrap_or_default();
            map.insert(name, cols);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn job(service_id: Uuid, tenant_id: Uuid) -> BackupJob {
        BackupJob {
            id: Uuid::from_u128(1),
            service_id,
            tenant_id,
            job_type: "export".into(),
            status: "queued".into(),
            tables_filter: None,
            clear_existing: false,
            s3_key: None,
            total_rows: 0,
            processed_rows: 0,
            error_message: None,
            created_at: chrono::Utc::now(),
            started_at: None,
            completed_at: None,
        }
    }

    #[test]
    fn a_message_runs_only_the_job_its_row_says_it_names() {
        let (service, tenant, other) = (Uuid::from_u128(2), Uuid::from_u128(3), Uuid::from_u128(4));
        let message = |service_id, tenant_id| BackupMessage {
            job_id: Uuid::from_u128(1),
            service_id,
            tenant_id,
        };
        assert_eq!(
            admitted(&message(service, tenant), Some(job(service, tenant)))
                .map(|job| (job.service_id, job.tenant_id)),
            Ok((service, tenant))
        );
        for (service_id, tenant_id) in [(other, tenant), (service, other), (other, other)] {
            assert_eq!(
                admitted(&message(service_id, tenant_id), Some(job(service, tenant))).map(drop),
                Err(Refused::Foreign(Uuid::from_u128(1)))
            );
        }
        assert_eq!(
            admitted(&message(service, tenant), None).map(drop),
            Err(Refused::Unknown(Uuid::from_u128(1)))
        );
    }

    fn meta(delivered: u64) -> MessageMeta {
        MessageMeta {
            subject: "cufflink.backup.x".into(),
            stream: "BACKUP".into(),
            consumer: "backup-worker".into(),
            stream_sequence: 1,
            consumer_sequence: 1,
            delivered,
            pending: 0,
            published: chrono::Utc::now(),
            headers: Vec::new(),
        }
    }

    #[tokio::test]
    async fn a_job_that_cannot_be_read_is_handed_back_not_acknowledged() {
        let pool =
            cufflink_db::build_pool("postgres://nobody@127.0.0.1:1/nowhere", 1).expect("a pool");
        let message = BackupMessage {
            job_id: Uuid::from_u128(1),
            service_id: Uuid::from_u128(2),
            tenant_id: Uuid::from_u128(3),
        };
        for redelivered in [false, true] {
            let settled = claimed(&pool, &message, redelivered)
                .await
                .map(drop)
                .expect_err("no database");
            assert!(matches!(settled, Outcome::Retry { .. }), "{settled:?}");
            assert!(matches!(settle(&settled, &meta(1)), Decision::Nak(_)));
        }
    }

    #[test]
    fn a_job_that_ran_is_acknowledged_and_an_interrupted_one_handed_back_at_once() {
        let terminal = Outcome::Terminal {
            reason: "export failed".into(),
        };
        let invalid = Outcome::Invalid {
            reason: "not a backup message".into(),
        };
        let retry = Outcome::Retry {
            after: None,
            reason: "the database is down".into(),
        };
        assert_eq!(settle(&Outcome::Ok, &meta(1)), Decision::Ack);
        assert_eq!(
            settle(&Outcome::Interrupted, &meta(1)),
            Decision::Nak(Duration::ZERO),
            "its redelivery claims the running job again"
        );
        for given_up in [&terminal, &invalid] {
            assert!(matches!(settle(given_up, &meta(1)), Decision::Term { .. }));
        }
        assert!(matches!(
            settle(&retry, &meta(1)),
            Decision::Nak(after) if after >= BACKUP_RETRY * 4 / 5
        ));
        assert!(matches!(
            settle(&retry, &meta(BACKUP_MAX_DELIVER as u64)),
            Decision::Term { .. }
        ));
    }

    #[test]
    fn clear_uses_manifest_intersection() {
        let manifest = json!({"tables": [{"name": "items"}, {"name": "batches"}]});
        let filter = vec![
            "items".to_string(),
            "users; DROP TABLE items".to_string(),
            "other_service_table".to_string(),
        ];
        assert_eq!(
            table_names(&filter_tables(&manifest, Some(&filter))),
            vec!["items"]
        );
        assert_eq!(
            table_names(&filter_tables(&manifest, None)),
            vec!["items", "batches"]
        );
    }
}
