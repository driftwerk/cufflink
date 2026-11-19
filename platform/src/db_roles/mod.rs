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
use cufflink_types::manifest::validate::validate_schema_name;
use provision::{Change, Provisioning};
use serde_json::{json, Value};
use tracing::warn;
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
    /// Record the service when it has not been deployed, with the schema
    /// given or the one its name gives.
    pub register: bool,
    pub schema: Option<&'a str>,
}

pub struct Bound {
    pub service: Service,
    pub binding: Binding,
    pub report: VerifyReport,
    pub registered: bool,
}

/// The schema a registration records, or `None` for a set of a service
/// that must exist.
fn registration(service_name: &str, request: &BindRequest<'_>) -> Result<Option<String>, String> {
    match (request.register, request.schema, request.role) {
        (false, None, _) => Ok(None),
        (false, Some(_), _) => Err("schema is given only with register".to_string()),
        (true, _, None) => Err(
            "a service registered before its first deploy is bound to a role that exists: name it with role"
                .to_string(),
        ),
        (true, schema, Some(_)) => {
            if !cufflink_types::slug::is_valid_service_name(service_name) {
                return Err(format!("'{service_name}' is not a service name"));
            }
            let schema = schema.map_or_else(
                || cufflink_types::service_schema(service_name, None),
                str::to_string,
            );
            validate_schema_name("schema", &schema).map(|()| Some(schema))
        }
    }
}

/// Binds the service, first recording it when the request registers one
/// that has not been deployed. The service's deploy lock is held from the
/// registration to the end of the binding, and a service this request
/// recorded is removed again when its binding is refused.
pub async fn set(
    state: &AppState,
    tenant: &tenants::Tenant,
    service_name: &str,
    request: &BindRequest<'_>,
) -> PlatformResult<Bound> {
    let Some(schema) = registration(service_name, request).map_err(PlatformError::Validation)?
    else {
        let service = require_service(state, tenant, service_name).await?;
        let (binding, report) = bind(state, tenant, &service, request, false).await?;
        return Ok(Bound {
            service,
            binding,
            report,
            registered: false,
        });
    };
    let (service, registered) =
        crate::repo::services::register(&state.platform_db, tenant.id, service_name, &schema)
            .await?;
    let lock =
        crate::api::deployments::acquire_deploy_lock(state, service.id, "deployment").await?;
    let bound = bind(state, tenant, &service, request, registered).await;
    if bound.is_err() && registered {
        if let Err(e) = crate::repo::services::unregister(&state.platform_db, service.id).await {
            warn!(tenant = %tenant.slug, service = %service.name, error = %e, "The service registered for a binding that was refused could not be removed; `cufflink services delete` removes it");
        }
    }
    lock.release().await;
    let (binding, report) = bound?;
    Ok(Bound {
        service,
        binding,
        report,
        registered,
    })
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

async fn bind(
    state: &AppState,
    tenant: &tenants::Tenant,
    service: &Service,
    request: &BindRequest<'_>,
    registered: bool,
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
    let mut details = binding_details(&bound.0);
    details["registered"] = json!(registered);
    details["schema"] = json!(service.schema());
    audit(
        state,
        tenant,
        service,
        request.actor,
        "db_role.set",
        details,
    )
    .await?;
    Ok(bound)
}

fn binding_details(binding: &Binding) -> Value {
    json!({
        "role": binding.role_name,
        "managed": binding.managed,
        "login": binding.login,
        "read_only_role": binding.read_only_role,
    })
}

async fn audit(
    state: &AppState,
    tenant: &tenants::Tenant,
    service: &Service,
    actor: &str,
    action: &str,
    details: Value,
) -> PlatformResult<()> {
    crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
        client: &state.platform_db.get().await?,
        tenant_id: Some(tenant.id),
        actor,
        action,
        resource_type: "service",
        resource_id: Some(&service.name),
        details: Some(details),
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
            deploying: None,
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
    audit(
        state,
        tenant,
        service,
        actor,
        "db_role.rotate",
        binding_details(&rotated.0),
    )
    .await?;
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

    fn request<'a>(
        role: Option<&'a str>,
        register: bool,
        schema: Option<&'a str>,
    ) -> BindRequest<'a> {
        BindRequest {
            role,
            password: None,
            read_only_role: None,
            actor: "admin",
            register,
            schema,
        }
    }

    #[test]
    fn a_registration_names_its_role_and_a_schema_a_deploy_could_declare() {
        let role = Some("catalog_service_user");
        assert_eq!(
            registration("catalog-service", &request(role, false, None)),
            Ok(None)
        );
        assert_eq!(
            registration("catalog-service", &request(None, false, None)),
            Ok(None),
            "a set that registers nothing is checked as before"
        );
        assert_eq!(
            registration("catalog-service", &request(role, true, None)),
            Ok(Some("catalog_service".to_string())),
            "the schema a manifest without schema_name has"
        );
        assert_eq!(
            registration("catalog-service", &request(role, true, Some("catalog"))),
            Ok(Some("catalog".to_string()))
        );
        for (refused, why) in [
            (
                request(role, false, Some("catalog")),
                "schema is given only with register",
            ),
            (request(None, true, None), "is bound to a role that exists"),
            (request(role, true, Some("Catalog")), "is not a schema name"),
            (request(role, true, Some("pg_catalog")), "is reserved"),
        ] {
            let error = registration("catalog-service", &refused).expect_err(why);
            assert!(error.contains(why), "{error}");
        }
        assert!(registration("catalog service", &request(role, true, None))
            .expect_err("a name no deploy could have")
            .contains("is not a service name"));
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
