use crate::auth::DeployAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::{PolicyKey, PolicyMode};
use crate::row_json::column_to_json;
use crate::tenant_sessions::{Purpose, Scope, ServiceSessions, Statements};
use crate::AppState;
use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    response::Response,
    Json,
};
use cufflink_pgcodec::error::server_message;
use cufflink_pgcodec::session as pg_session;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_postgres::types::Type;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

type FrameItem = Result<Bytes, std::io::Error>;

fn purpose(db_roles: PolicyMode) -> Purpose {
    match db_roles {
        PolicyMode::Enforce => Purpose::Guest,
        PolicyMode::Off | PolicyMode::Warn => Purpose::Platform,
    }
}

pub async fn exec_query(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
    Json(req): Json<QueryRequest>,
) -> PlatformResult<Response> {
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let sql = req.sql.trim();
    if sql.is_empty() {
        return Err(PlatformError::BadRequest(
            "Empty SQL statement is not allowed".into(),
        ));
    }

    let db_roles = state
        .tenants
        .get_or_load(service.tenant_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?
        .mode(PolicyKey::DbRoles);
    let sessions = state
        .tenant_sessions
        .service(&service.identity(&user.tenant_slug), purpose(db_roles))
        .await?;

    let audit = json!({
        "action": "sql.query",
        "actor": user.subject,
        "tenant_slug": user.tenant_slug,
        "service_name": service.name,
        "service_id": service.id.to_string(),
        "sql": sql,
    });
    let audit_subject = format!("cufflink.audit.sql.{}", user.tenant_slug);
    if let Ok(payload) = serde_json::to_vec(&audit) {
        if let Err(e) = crate::nats_conn::jetstream_publish_bounded(
            &state.jetstream,
            audit_subject,
            None,
            payload.into(),
        )
        .await
        {
            warn!("Failed to publish SQL audit event: {}", e);
        }
    }

    info!(
        tenant = %user.tenant_slug,
        service = %service.name,
        actor = %user.subject,
        "sql.query"
    );

    let max_rows = state.config.sql_max_rows;
    let timeout = Duration::from_secs(state.config.sql_timeout_secs);
    let sql_owned = sql.to_string();

    let (tx, rx) = mpsc::channel::<FrameItem>(64);

    tokio::spawn(async move {
        let started = Instant::now();
        let result =
            tokio::time::timeout(timeout, run_query(sessions, &sql_owned, max_rows, &tx)).await;
        let elapsed_ms = started.elapsed().as_millis() as u64;

        match result {
            Ok(Ok((rows, truncated))) => {
                let frame = json!({
                    "type": "done",
                    "rows": rows,
                    "truncated": truncated,
                    "elapsed_ms": elapsed_ms,
                });
                let _ = send_frame(&tx, &frame).await;
            }
            Ok(Err(msg)) => {
                let frame = json!({ "type": "error", "message": msg });
                let _ = send_frame(&tx, &frame).await;
            }
            Err(_) => {
                let frame = json!({
                    "type": "error",
                    "message": format!("Query exceeded {}s timeout", timeout.as_secs()),
                });
                let _ = send_frame(&tx, &frame).await;
            }
        }
    });

    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);
    Ok(Response::builder()
        .status(200)
        .header("content-type", "application/x-ndjson")
        .body(body)
        .unwrap())
}

async fn run_query(
    sessions: ServiceSessions,
    sql: &str,
    max_rows: u64,
    tx: &mpsc::Sender<FrameItem>,
) -> Result<(u64, bool), String> {
    let schema = sessions.schema_ident();
    let mut client = sessions
        .get()
        .await
        .map_err(|e| format!("Checkout tenant connection: {}", e.reason()))?;

    let role = client.bound_read_only_role().map(str::to_string);
    let tx_db = client
        .read_only_transaction()
        .await
        .map_err(|e| format!("BEGIN READ ONLY: {}", server_message(&e)))?;

    let setup: String = role
        .map(|role| pg_session::set_role(Scope::Transaction, &role))
        .into_iter()
        .chain([format!("SET LOCAL search_path TO {schema}, public")])
        .collect::<Vec<_>>()
        .join("; ");
    tx_db
        .batch_execute(&setup)
        .await
        .map_err(|e| format!("Session setup: {}", server_message(&e)))?;

    let stmt = tx_db
        .prepare(sql)
        .await
        .map_err(|e| format!("Prepare: {}", server_message(&e)))?;

    let cols_json: Vec<Value> = stmt
        .columns()
        .iter()
        .map(|c| json!({ "name": c.name(), "type_name": c.type_().name() }))
        .collect();
    send_frame(tx, &json!({ "type": "columns", "columns": cols_json })).await?;

    let col_types: Vec<Type> = stmt.columns().iter().map(|c| c.type_().clone()).collect();

    let (row_count, truncated) = {
        let rows = tx_db
            .query_raw(&stmt)
            .await
            .map_err(|e| format!("query_raw: {}", server_message(&e)))?;
        tokio::pin!(rows);

        let mut row_count: u64 = 0;
        let mut truncated = false;

        while let Some(row_result) = rows.next().await {
            let row = row_result.map_err(|e| format!("Row fetch: {}", server_message(&e)))?;
            if max_rows > 0 && row_count >= max_rows {
                truncated = true;
                break;
            }
            let values: Vec<Value> = (0..col_types.len())
                .map(|i| column_to_json(&row, i, &col_types[i]))
                .collect();
            send_frame(tx, &json!({ "type": "row", "values": values })).await?;
            row_count += 1;
        }
        (row_count, truncated)
    };

    tx_db
        .commit()
        .await
        .map_err(|e| format!("COMMIT: {}", server_message(&e)))?;

    Ok((row_count, truncated))
}

async fn send_frame(tx: &mpsc::Sender<FrameItem>, value: &Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(value).map_err(|e| format!("Serialize frame: {e}"))?;
    bytes.push(b'\n');
    tx.send(Ok(Bytes::from(bytes)))
        .await
        .map_err(|_| "Client disconnected".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_run_as_the_services_role_once_db_roles_is_enforced() {
        assert_eq!(purpose(PolicyMode::Enforce), Purpose::Guest);
        assert_eq!(purpose(PolicyMode::Warn), Purpose::Platform);
        assert_eq!(purpose(PolicyMode::Off), Purpose::Platform);
    }
}
