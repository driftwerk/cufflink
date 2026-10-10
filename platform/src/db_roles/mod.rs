pub mod bindings;
pub mod enqueue;
pub mod fallback;
pub mod login_pools;
pub mod names;
pub mod provision;
pub mod turns;
pub mod verify;

use crate::errors::{PlatformError, PlatformResult};
use crate::policy::config::DbRolesConfig;
use crate::policy::{PolicyKey, PolicyMode, PolicySet};
use crate::repo::service_db_roles::{self, Binding};
use crate::repo::services::Service;
use crate::repo::tenant_policies::PolicyRow;
use crate::repo::tenants;
use crate::AppState;
use provision::{Change, Provisioning};
use serde_json::json;
use verify::VerifyReport;

pub const NEEDS_CLEAN_RECYCLE: &str = "db_roles needs DB_POOL_RECYCLE=clean where tenant databases are reached directly: a guest statement cut short would otherwise leave its service's role set on a connection the tenant's pool hands out again";

pub fn asks_for_roles(defaults: &PolicySet, rows: &[PolicyRow]) -> bool {
    defaults.mode(PolicyKey::DbRoles) != PolicyMode::Off
        || rows
            .iter()
            .any(|row| row.key == PolicyKey::DbRoles.as_str() && row.mode != PolicyMode::Off)
}

pub struct BindRequest<'a> {
    pub role: Option<&'a str>,
    pub password: Option<&'a str>,
    pub read_only_role: Option<&'a str>,
    pub actor: &'a str,
}

pub async fn require_service(
    state: &AppState,
    tenant: &tenants::Tenant,
    service_name: &str,
) -> PlatformResult<Service> {
    crate::repo::services::find(&state.platform_db, tenant.id, service_name)
        .await?
        .ok_or_else(|| {
            PlatformError::NotFound(format!(
                "Service '{service_name}' not found in tenant '{}'",
                tenant.slug
            ))
        })
}

pub async fn bind(
    state: &AppState,
    tenant: &tenants::Tenant,
    service: &Service,
    request: &BindRequest<'_>,
) -> PlatformResult<(Binding, VerifyReport)> {
    for (what, name) in [
        ("role", request.role),
        ("read_only_role", request.read_only_role),
    ] {
        name.map_or(Ok(()), |name| names::validate_role_name(what, name))
            .map_err(PlatformError::Validation)?;
    }
    let bound = apply(
        state,
        tenant,
        service,
        request.actor,
        Change {
            role: request.role,
            password: request.password,
            read_only_role: request.read_only_role,
            rotate: false,
        },
    )
    .await?;
    audit(
        state,
        tenant,
        service,
        request.actor,
        "db_role.set",
        &bound.0,
    )
    .await?;
    Ok(bound)
}

async fn audit(
    state: &AppState,
    tenant: &tenants::Tenant,
    service: &Service,
    actor: &str,
    action: &str,
    binding: &Binding,
) -> PlatformResult<()> {
    crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
        client: &state.platform_db.get().await?,
        tenant_id: Some(tenant.id),
        actor,
        action,
        resource_type: "service",
        resource_id: Some(&service.name),
        details: Some(json!({
            "role": binding.role_name,
            "managed": binding.managed,
            "login": binding.login,
            "read_only_role": binding.read_only_role,
        })),
        ip_address: None,
    })
    .await?;
    Ok(())
}

async fn apply(
    state: &AppState,
    tenant: &tenants::Tenant,
    service: &Service,
    actor: &str,
    change: Change<'_>,
) -> PlatformResult<(Binding, VerifyReport)> {
    let policies = state.tenants.policies_or_load(tenant.id).await?;
    provision::apply(
        state,
        &Provisioning {
            tenant,
            service,
            policy: policies.mode(PolicyKey::DbRoles),
            config: policies.config::<DbRolesConfig>(),
            actor,
        },
        change,
    )
    .await
}

async fn require_binding(state: &AppState, service: &Service) -> PlatformResult<Binding> {
    service_db_roles::get(&state.platform_db.get().await?, service.id)
        .await?
        .ok_or_else(|| {
            PlatformError::NotFound(format!(
                "Service '{}' has no database role; set one first",
                service.name
            ))
        })
}

pub async fn verify(
    state: &AppState,
    tenant: &tenants::Tenant,
    service: &Service,
    actor: &str,
) -> PlatformResult<(Binding, VerifyReport)> {
    require_binding(state, service).await?;
    apply(state, tenant, service, actor, Change::default()).await
}

pub async fn rotate(
    state: &AppState,
    tenant: &tenants::Tenant,
    service: &Service,
    actor: &str,
) -> PlatformResult<(Binding, VerifyReport)> {
    require_binding(state, service).await?;
    let rotated = apply(
        state,
        tenant,
        service,
        actor,
        Change {
            rotate: true,
            ..Change::default()
        },
    )
    .await?;
    audit(state, tenant, service, actor, "db_role.rotate", &rotated.0).await?;
    Ok(rotated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn row(key: PolicyKey, mode: PolicyMode) -> PolicyRow {
        PolicyRow {
            tenant_id: Uuid::nil(),
            key: key.as_str().to_string(),
            mode,
            config: json!({}),
            updated_at: Utc::now(),
            updated_by: None,
        }
    }

    #[test]
    fn roles_are_asked_for_by_the_default_or_by_a_tenant() {
        let off = PolicySet::builtin_defaults();
        assert_eq!(off.mode(PolicyKey::DbRoles), PolicyMode::Off);
        assert!(!asks_for_roles(&off, &[]));
        assert!(!asks_for_roles(
            &off,
            &[
                row(PolicyKey::DbRoles, PolicyMode::Off),
                row(PolicyKey::Outbox, PolicyMode::Enforce)
            ]
        ));
        for mode in [PolicyMode::Warn, PolicyMode::Enforce] {
            assert!(asks_for_roles(&off, &[row(PolicyKey::DbRoles, mode)]));
        }
        let warn = off.with_tenant_rows(&[row(PolicyKey::DbRoles, PolicyMode::Warn)]);
        assert!(asks_for_roles(&warn, &[]));
    }
}
