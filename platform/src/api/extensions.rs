use crate::auth::PlatformAdmin;
use crate::errors::{PlatformError, PlatformResult};
use crate::extensions::{self, Backend, ExtensionsConfig};
use crate::repo::extensions::ExtensionRow;
use crate::repo::{audit, tenants};
use crate::AppState;
use axum::extract::{Path, Query, State};
use axum::Json;
use cufflink_types::ext::name_violation;
use deadpool_postgres::GenericClient;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterRequest {
    pub callers: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Overrides {
    pub force: bool,
}

fn extension_body(row: &ExtensionRow) -> Value {
    json!({
        "name": row.name,
        "backend": row.registration.backend.as_str(),
        "service_id": row.registration.backend.service_id(),
        "callers": row.registration.callers,
        "created_by": row.created_by,
        "created_at": row.created_at.to_rfc3339(),
    })
}

fn require_enabled(config: &ExtensionsConfig) -> PlatformResult<()> {
    if config.enabled {
        Ok(())
    } else {
        Err(PlatformError::Forbidden(
            "extensions cannot be registered on this platform (CUFFLINK_EXTENSIONS=false)".into(),
        ))
    }
}

fn require_name(name: &str) -> PlatformResult<()> {
    name_violation(name).map_or(Ok(()), |violation| {
        Err(PlatformError::Validation(violation))
    })
}

fn require_nats(row: Option<&ExtensionRow>) -> PlatformResult<()> {
    match row.map(|row| (row, row.registration.backend)) {
        Some((row, Backend::Native { .. })) => Err(native(&row.name)),
        Some((_, Backend::Nats)) | None => Ok(()),
    }
}

fn native(name: &str) -> PlatformError {
    PlatformError::Conflict(format!(
        "extension '{name}' is provided by a native service; it changes with that service's \
         deploys"
    ))
}

async fn require_unstranded(
    client: &impl GenericClient,
    tenant: &tenants::Tenant,
    name: &str,
    after: Option<&[String]>,
    overrides: &Overrides,
) -> PlatformResult<()> {
    if overrides.force {
        return Ok(());
    }
    let declared =
        crate::repo::extensions::declarations(client, tenant.id, &[name.to_string()]).await?;
    extensions::stranded_by(&declared, name, after).map_or(Ok(()), |stranded| {
        Err(PlatformError::Conflict(format!(
            "service '{}' is deployed calling extension '{name}'; redeploy it without the \
             extension first, or pass force=true",
            stranded.service
        )))
    })
}

async fn record(
    state: &AppState,
    admin: &PlatformAdmin,
    tenant: &tenants::Tenant,
    action: &str,
    name: &str,
    details: Value,
) {
    audit::record(
        &state.platform_db,
        audit::Change {
            tenant_id: Some(tenant.id),
            actor: &admin.subject,
            credential: admin.credential(),
            action,
            resource_type: "extension",
            resource_id: Some(name),
            details: Some(details),
        },
    )
    .await;
}

pub async fn list_extensions(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(slug): Path<String>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let rows = crate::repo::extensions::list_for_tenant(&state.platform_db.get().await?, tenant.id)
        .await?;
    Ok(Json(json!({
        "tenant": slug,
        "extensions": rows.iter().map(extension_body).collect::<Vec<_>>(),
    })))
}

pub async fn register_extension(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, name)): Path<(String, String)>,
    Query(overrides): Query<Overrides>,
    Json(request): Json<RegisterRequest>,
) -> PlatformResult<Json<Value>> {
    require_enabled(&state.config.extensions)?;
    require_name(&name)?;
    let callers =
        cufflink_types::ext::callers(request.callers).map_err(PlatformError::Validation)?;
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let mut client = state.platform_db.get().await?;
    let tx = client.transaction().await?;
    let current = crate::repo::extensions::locked(&tx, tenant.id, &name).await?;
    require_nats(current.as_ref())?;
    if current.is_some() {
        require_unstranded(&tx, &tenant, &name, Some(&callers), &overrides).await?;
    }
    let stored =
        crate::repo::extensions::register_nats(&tx, tenant.id, &name, &callers, &admin.subject)
            .await?
            .ok_or_else(|| native(&name))?;
    tx.commit().await?;
    drop(client);
    info!(
        tenant = %slug,
        extension = %name,
        callers = ?stored.registration.callers,
        force = overrides.force,
        actor = %admin.subject,
        "Extension registered"
    );
    record(
        &state,
        &admin,
        &tenant,
        "extension.register",
        &name,
        json!({
            "name": name,
            "callers": stored.registration.callers,
            "previous": current.map(|row| row.registration.callers),
            "force": overrides.force,
        }),
    )
    .await;
    state.tenants.refresh().await;
    Ok(Json(
        json!({"tenant": slug, "extension": extension_body(&stored)}),
    ))
}

pub async fn unregister_extension(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, name)): Path<(String, String)>,
    Query(overrides): Query<Overrides>,
) -> PlatformResult<Json<Value>> {
    require_name(&name)?;
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let mut client = state.platform_db.get().await?;
    let tx = client.transaction().await?;
    let not_registered =
        || PlatformError::NotFound(format!("extension '{name}' is not registered"));
    let current = crate::repo::extensions::locked(&tx, tenant.id, &name)
        .await?
        .ok_or_else(not_registered)?;
    require_nats(Some(&current))?;
    require_unstranded(&tx, &tenant, &name, None, &overrides).await?;
    if !crate::repo::extensions::unregister_nats(&tx, tenant.id, &name).await? {
        return Err(not_registered());
    }
    tx.commit().await?;
    drop(client);
    info!(
        tenant = %slug,
        extension = %name,
        force = overrides.force,
        actor = %admin.subject,
        "Extension unregistered"
    );
    record(
        &state,
        &admin,
        &tenant,
        "extension.unregister",
        &name,
        json!({"name": name, "callers": current.registration.callers, "force": overrides.force}),
    )
    .await;
    state.tenants.refresh().await;
    Ok(Json(json!({"tenant": slug, "name": name, "removed": true})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::Registration;

    fn row(backend: Backend) -> ExtensionRow {
        ExtensionRow {
            tenant_id: uuid::Uuid::nil(),
            name: "google.token".into(),
            registration: Registration {
                backend,
                callers: vec!["reports".into()],
            },
            created_by: "admin".into(),
            created_at: chrono::DateTime::from_timestamp(1_790_000_000, 0).expect("time"),
        }
    }

    #[test]
    fn registering_is_refused_by_name_while_the_platform_disables_it() {
        let refused = require_enabled(&ExtensionsConfig::default()).unwrap_err();
        assert!(
            matches!(&refused, PlatformError::Forbidden(m) if m.contains("CUFFLINK_EXTENSIONS")),
            "{refused}"
        );
        assert!(require_enabled(&ExtensionsConfig {
            enabled: true,
            ..Default::default()
        })
        .is_ok());
    }

    #[test]
    fn names_are_extension_names() {
        assert!(require_name("google.token").is_ok());
        for invalid in ["Google", "a..b", "*", "a/b"] {
            assert!(
                matches!(require_name(invalid), Err(PlatformError::Validation(_))),
                "{invalid}"
            );
        }
    }

    #[test]
    fn a_native_services_extension_is_not_the_operators_to_change() {
        assert!(require_nats(None).is_ok());
        assert!(require_nats(Some(&row(Backend::Nats))).is_ok());
        let refused = require_nats(Some(&row(Backend::Native {
            service_id: uuid::Uuid::nil(),
        })))
        .unwrap_err();
        assert!(matches!(refused, PlatformError::Conflict(_)), "{refused}");
    }

    #[test]
    fn a_registration_is_answered_with_its_backend_and_callers() {
        assert_eq!(
            extension_body(&row(Backend::Nats)),
            json!({
                "name": "google.token",
                "backend": "nats",
                "service_id": null,
                "callers": ["reports"],
                "created_by": "admin",
                "created_at": "2026-09-21T14:13:20+00:00",
            })
        );
        assert!(serde_json::from_value::<RegisterRequest>(json!({"caller": ["a"]})).is_err());
        assert!(serde_json::from_value::<Overrides>(json!({"force": "yes"})).is_err());
    }
}
