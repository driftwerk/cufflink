use crate::auth::binding::bind_principal;
use crate::auth::{require_deploy_authority, AuthUser};
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::wasm_jobs::{self, WasmJob};
use crate::router::jobs::{count_transition, publish, JOB_RETENTION};
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use cufflink_telemetry::Scope;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

fn not_found() -> PlatformError {
    PlatformError::NotFound("Job not found".into())
}

async fn bound(
    state: &AppState,
    tenant_slug: &str,
    headers: &HeaderMap,
    auth: AuthUser,
) -> PlatformResult<AuthUser> {
    let tenant = state
        .tenants
        .get_or_load_by_slug(tenant_slug)
        .await?
        .ok_or_else(not_found)?;
    let auth = bind_principal(state, Some(auth), &tenant, headers)
        .ok()
        .flatten()
        .filter(|auth| auth.tenant_id == tenant.tenant.id)
        .ok_or_else(not_found)?;
    crate::telemetry::resolve(&Scope::tenant(tenant.tenant.slug.as_str()), "_jobs");
    Ok(auth)
}

pub async fn get_job(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, job_id)): Path<(String, Uuid)>,
    headers: HeaderMap,
    auth: AuthUser,
) -> PlatformResult<Json<Value>> {
    let auth = bound(&state, &tenant_slug, &headers, auth).await?;

    let job = wasm_jobs::get(&state.platform_db, job_id)
        .await?
        .filter(|job| job.tenant_id == auth.tenant_id)
        .ok_or_else(not_found)?;

    let is_admin = auth.is_platform_admin || auth.is_service_account;
    let is_submitter = job
        .auth_subject
        .as_deref()
        .map(|s| s == auth.subject)
        .unwrap_or(false);
    if !is_admin && !is_submitter {
        return Err(not_found());
    }

    Ok(Json(polled(&job)))
}

pub async fn retry_job(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, job_id)): Path<(String, Uuid)>,
    headers: HeaderMap,
    auth: AuthUser,
) -> PlatformResult<Json<Value>> {
    let user = require_deploy_authority(bound(&state, &tenant_slug, &headers, auth).await?)?;
    let retried =
        wasm_jobs::retry_failed(&state.platform_db, user.tenant_id, job_id, JOB_RETENTION).await?;
    let job = match retried {
        Some(job) => job,
        None => {
            return Err(wasm_jobs::get(&state.platform_db, job_id)
                .await?
                .filter(|job| job.tenant_id == user.tenant_id)
                .map_or_else(not_found, |job| not_failed(&job)))
        }
    };
    count_transition("retry");
    let client = state.platform_db.get().await?;
    if let Err(e) = crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
        client: &client,
        tenant_id: Some(user.tenant_id),
        actor: &user.subject,
        action: "job.retry",
        resource_type: "job",
        resource_id: Some(&job.id.to_string()),
        details: Some(json!({
            "handler": job.handler,
            "service": job.service_slug,
            "attempts": job.attempts,
            "last_error": job.last_error,
        })),
        ip_address: None,
    })
    .await
    {
        tracing::warn!(job_id = %job.id, error = %e, "Could not audit a job's retry");
    }
    if let Err(e) = publish(&state, &job).await {
        tracing::warn!(
            job_id = %job.id,
            error = %e,
            "Job to retry not published; the reclaimer publishes it"
        );
    }
    Ok(Json(polled(&job)))
}

fn not_failed(job: &WasmJob) -> PlatformError {
    PlatformError::Conflict(format!(
        "only a failed job is retried; this one is {}",
        job.status.as_str()
    ))
}

pub(crate) fn polled(job: &WasmJob) -> Value {
    json!({
        "job_id": job.id,
        "status": job.status,
        "handler": job.handler,
        "service": job.service_slug,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "result": job.result_json,
        "error": job.error,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
    })
}
