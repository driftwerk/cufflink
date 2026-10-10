use crate::auth::AuthUser;
use crate::errors::{PlatformError, PlatformResult};
use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use cufflink_types::openapi::service_spec;
use cufflink_types::ServiceManifest;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

pub async fn openapi_spec(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path(id): Path<String>,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let manifest = crate::repo::deployments::current_manifest(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("No deployments found".into()))?;
    let manifest: ServiceManifest = serde_json::from_value(manifest)
        .map_err(|e| PlatformError::Internal(format!("Failed to parse manifest: {}", e)))?;

    let client = state.platform_db.get().await?;
    let tenant_row = client
        .query_one(
            "SELECT slug FROM tenants WHERE id = $1",
            &[&service.tenant_id],
        )
        .await?;
    let tenant_slug: String = tenant_row.get("slug");

    service_spec(&manifest, &tenant_slug)
        .map(Json)
        .map_err(PlatformError::from)
}
