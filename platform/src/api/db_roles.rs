use crate::auth::PlatformAdmin;
use crate::db_roles::verify::VerifyReport;
use crate::db_roles::{self, BindRequest};
use crate::errors::PlatformResult;
use crate::repo::service_db_roles::{self, Binding};
use crate::repo::tenants;
use crate::AppState;
use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetDbRoleRequest {
    pub role: Option<String>,
    pub password: Option<String>,
    pub read_only_role: Option<String>,
    #[serde(default)]
    pub register: bool,
    pub schema: Option<String>,
}

fn binding_body(binding: &Binding) -> Value {
    json!({
        "role": binding.role_name,
        "managed": binding.managed,
        "login": binding.login,
        "read_only_role": binding.read_only_role,
        "verified_at": binding.verified_at.map(|at| at.to_rfc3339()),
        "verify_report": binding.verify_report,
        "updated_by": binding.updated_by,
        "updated_at": binding.updated_at.to_rfc3339(),
    })
}

fn checked(slug: &str, service: &str, binding: &Binding, report: &VerifyReport) -> Json<Value> {
    Json(json!({
        "tenant": slug,
        "service": service,
        "binding": binding_body(binding),
        "report": report,
    }))
}

pub async fn get_db_role(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path((slug, service_name)): Path<(String, String)>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let service = db_roles::require_service(&state, &tenant, &service_name).await?;
    let client = state.platform_db.get().await?;
    let binding = service_db_roles::get(&client, service.id).await?;
    Ok(Json(json!({
        "tenant": slug,
        "service": service.name,
        "binding": binding.as_ref().map(binding_body),
    })))
}

pub async fn set_db_role(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, service_name)): Path<(String, String)>,
    Json(req): Json<SetDbRoleRequest>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let bound = db_roles::set(
        &state,
        &tenant,
        &service_name,
        &BindRequest {
            role: req.role.as_deref(),
            password: req.password.as_deref(),
            read_only_role: req.read_only_role.as_deref(),
            actor: &admin.subject,
            register: req.register,
            schema: req.schema.as_deref(),
        },
    )
    .await?;
    let Json(mut body) = checked(&slug, &bound.service.name, &bound.binding, &bound.report);
    body["registered"] = json!(bound.registered);
    body["schema"] = json!(bound.service.schema());
    Ok(Json(body))
}

pub async fn verify_db_role(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, service_name)): Path<(String, String)>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let service = db_roles::require_service(&state, &tenant, &service_name).await?;
    let (binding, report) = db_roles::verify(&state, &tenant, &service, &admin.subject).await?;
    Ok(checked(&slug, &service.name, &binding, &report))
}

pub async fn rotate_db_role(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, service_name)): Path<(String, String)>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let service = db_roles::require_service(&state, &tenant, &service_name).await?;
    let (binding, report) = db_roles::rotate(&state, &tenant, &service, &admin.subject).await?;
    Ok(checked(&slug, &service.name, &binding, &report))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_set_reads_a_registration_only_when_it_is_asked_for() {
        let read = |body: Value| serde_json::from_value::<SetDbRoleRequest>(body);
        let plain = read(json!({"role": "r", "password": null, "read_only_role": null}))
            .expect("a set as before");
        assert!(!plain.register && plain.schema.is_none());
        let registering = read(json!({"role": "r", "register": true, "schema": "catalog"}))
            .expect("a registration");
        assert!(registering.register);
        assert_eq!(registering.schema.as_deref(), Some("catalog"));
        assert!(read(json!({"role": "r", "registered": true})).is_err());
    }
}
