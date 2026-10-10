use crate::auth::PlatformAdmin;
use crate::errors::PlatformResult;
use crate::repo::{audit, tenants};
use crate::secrets::reseal::{self, Operation, Report};
use crate::AppState;
use axum::extract::State;
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResealRequest {
    pub operation: Operation,
    pub tenant: Option<String>,
}

pub async fn reseal(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Json(req): Json<ResealRequest>,
) -> PlatformResult<Json<Report>> {
    let tenant = match &req.tenant {
        Some(slug) => Some(tenants::require_by_slug(&state.platform_db, slug).await?),
        None => None,
    };
    let report = reseal::run(
        &state.secrets,
        req.operation,
        tenant.as_ref().map(|tenant| tenant.id),
    )
    .await?;
    let asked_for = req.tenant.as_deref().unwrap_or("*");
    match &report.verified {
        Some(verified) => {
            let rows = |by_table: &BTreeMap<&'static str, u64>| by_table.values().sum::<u64>();
            let event = if verified.is_clean() {
                "secret_verify_clean"
            } else {
                "secret_verify_failed"
            };
            info!(
                event,
                actor = %admin.subject,
                operation = req.operation.as_str(),
                tenant = asked_for,
                rows_checked = report.rows_checked,
                data_keys_checked = verified.data_keys_checked,
                data_keys_unopenable = verified.data_keys_unopenable,
                data_keys_under_previous_kek = verified.data_keys_under_previous_kek,
                unopenable = rows(&verified.unopenable),
                not_sealed = rows(&verified.not_sealed),
                plain_differs = rows(&verified.plain_differs),
                rows_under_retired_data_key = verified.rows_under_retired_data_key,
                "Verified secrets"
            )
        }
        None => info!(
            actor = %admin.subject,
            operation = req.operation.as_str(),
            tenant = asked_for,
            rows_rewritten = report.rows_rewritten,
            keys_rewrapped = report.keys_rewrapped,
            tenants_rotated = report.tenants_rotated,
            "Resealed secrets"
        ),
    }
    let client = state.platform_db.get().await?;
    audit::log_action(audit::AuditEntry {
        client: &client,
        tenant_id: tenant.as_ref().map(|tenant| tenant.id),
        actor: &admin.subject,
        action: "secrets.reseal",
        resource_type: "secrets",
        resource_id: req.tenant.as_deref(),
        details: Some(json!(report)),
        ip_address: None,
    })
    .await?;
    Ok(Json(report))
}
