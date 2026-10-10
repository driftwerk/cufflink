use crate::errors::{PlatformError, PlatformResult};
use crate::extensions::{self, Backend, Declaration, Registration};
use chrono::{DateTime, Utc};
use cufflink_types::ProvidedExtension;
use deadpool_postgres::GenericClient;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionRow {
    pub tenant_id: Uuid,
    pub name: String,
    pub registration: Registration,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

const COLUMNS: &str = "tenant_id, name, backend, service_id, callers, created_by, created_at";

fn backend(kind: &str, service_id: Option<Uuid>) -> Result<Backend, String> {
    match (kind, service_id) {
        ("nats", None) => Ok(Backend::Nats),
        ("native", Some(service_id)) => Ok(Backend::Native { service_id }),
        ("nats", Some(_)) => Err("a NATS provider has no service".into()),
        ("native", None) => Err("a native extension has a service".into()),
        (other, _) => Err(format!("'{other}' is not one of nats, native")),
    }
}

fn row_to_extension(row: &tokio_postgres::Row) -> PlatformResult<ExtensionRow> {
    let name: String = row.get("name");
    Ok(ExtensionRow {
        registration: Registration {
            backend: backend(row.get("backend"), row.get("service_id"))
                .map_err(|e| PlatformError::Internal(format!("extension '{name}': {e}")))?,
            callers: row.get("callers"),
        },
        tenant_id: row.get("tenant_id"),
        name,
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
    })
}

fn rows_to_extensions(rows: &[tokio_postgres::Row]) -> PlatformResult<Vec<ExtensionRow>> {
    rows.iter().map(row_to_extension).collect()
}

pub async fn list_all(client: &impl GenericClient) -> PlatformResult<Vec<ExtensionRow>> {
    rows_to_extensions(
        &client
            .query(
                &format!("SELECT {COLUMNS} FROM tenant_extensions ORDER BY tenant_id, name"),
                &[],
            )
            .await?,
    )
}

pub async fn list_for_tenant(
    client: &impl GenericClient,
    tenant_id: Uuid,
) -> PlatformResult<Vec<ExtensionRow>> {
    rows_to_extensions(
        &client
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM tenant_extensions WHERE tenant_id = $1 ORDER BY name"
                ),
                &[&tenant_id],
            )
            .await?,
    )
}

fn by_name(rows: Vec<ExtensionRow>) -> BTreeMap<String, Registration> {
    rows.into_iter()
        .map(|row| (row.name, row.registration))
        .collect()
}

pub async fn registrations(
    client: &impl GenericClient,
    tenant_id: Uuid,
) -> PlatformResult<BTreeMap<String, Registration>> {
    list_for_tenant(client, tenant_id).await.map(by_name)
}

pub async fn shared(
    client: &impl GenericClient,
    tenant_id: Uuid,
    names: &[String],
) -> PlatformResult<BTreeMap<String, Registration>> {
    rows_to_extensions(
        &client
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM tenant_extensions
                     WHERE tenant_id = $1 AND name = ANY($2)
                     ORDER BY name FOR SHARE"
                ),
                &[&tenant_id, &names],
            )
            .await?,
    )
    .map(by_name)
}

pub async fn locked(
    client: &impl GenericClient,
    tenant_id: Uuid,
    name: &str,
) -> PlatformResult<Option<ExtensionRow>> {
    client
        .query_opt(
            &format!(
                "SELECT {COLUMNS} FROM tenant_extensions WHERE tenant_id = $1 AND name = $2
                 FOR UPDATE"
            ),
            &[&tenant_id, &name],
        )
        .await?
        .as_ref()
        .map(row_to_extension)
        .transpose()
}

pub async fn declarations(
    client: &impl GenericClient,
    tenant_id: Uuid,
    names: &[String],
) -> PlatformResult<Vec<Declaration>> {
    Ok(client
        .query(
            "SELECT s.name, declared.name
             FROM services s
             JOIN deployments d ON d.service_id = s.id AND d.version = s.current_version
             CROSS JOIN LATERAL jsonb_array_elements_text(d.manifest->'extensions') AS declared(name)
             WHERE s.tenant_id = $1 AND s.status = 'active'
               AND d.manifest->'extensions' ?| $2 AND declared.name = ANY($2)
             ORDER BY 1, 2",
            &[&tenant_id, &names],
        )
        .await?
        .iter()
        .map(|row| Declaration {
            service: row.get(0),
            extension: row.get(1),
        })
        .collect())
}

pub async fn register_nats(
    client: &impl GenericClient,
    tenant_id: Uuid,
    name: &str,
    callers: &[String],
    actor: &str,
) -> PlatformResult<Option<ExtensionRow>> {
    client
        .query_opt(
            &format!(
                "INSERT INTO tenant_extensions (tenant_id, name, backend, callers, created_by)
                 VALUES ($1, $2, 'nats', $3, $4)
                 ON CONFLICT (tenant_id, name) DO UPDATE SET callers = EXCLUDED.callers
                 WHERE tenant_extensions.backend = 'nats'
                 RETURNING {COLUMNS}"
            ),
            &[&tenant_id, &name, &callers, &actor],
        )
        .await?
        .as_ref()
        .map(row_to_extension)
        .transpose()
}

pub async fn unregister_nats(
    client: &impl GenericClient,
    tenant_id: Uuid,
    name: &str,
) -> PlatformResult<bool> {
    Ok(client
        .execute(
            "DELETE FROM tenant_extensions
             WHERE tenant_id = $1 AND name = $2 AND backend = 'nats'",
            &[&tenant_id, &name],
        )
        .await?
        > 0)
}

pub async fn stranding(
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
    provides: &[ProvidedExtension],
    registrations: &BTreeMap<String, Registration>,
) -> PlatformResult<Option<Declaration>> {
    let names: Vec<String> = extensions::provided_by(registrations, service_id)
        .map(str::to_string)
        .collect();
    let declared = declarations(client, tenant_id, &names).await?;
    Ok(extensions::stranded_by_provides(&declared, service_id, provides, registrations).cloned())
}

pub async fn lock_native(
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
    provides: &[ProvidedExtension],
) -> PlatformResult<Option<Declaration>> {
    let own = by_name(rows_to_extensions(
        &client
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM tenant_extensions WHERE service_id = $1
                     ORDER BY name FOR UPDATE"
                ),
                &[&service_id],
            )
            .await?,
    )?);
    stranding(client, tenant_id, service_id, provides, &own).await
}

pub async fn replace_native(
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
    provides: &[ProvidedExtension],
    actor: &str,
) -> PlatformResult<()> {
    let provides = stored(provides)?;
    if let Some(stranded) = lock_native(client, tenant_id, service_id, &provides).await? {
        return Err(PlatformError::Conflict(stranded.provides_refusal()));
    }
    client
        .execute(
            "DELETE FROM tenant_extensions WHERE service_id = $1",
            &[&service_id],
        )
        .await?;
    let registered: Vec<String> = client
        .query(
            "INSERT INTO tenant_extensions (tenant_id, name, backend, service_id, callers, created_by)
             SELECT $1, provided->>'name', 'native', $2,
                    ARRAY(SELECT jsonb_array_elements_text(provided->'callers')), $4
             FROM jsonb_array_elements($3::jsonb) AS provided
             ON CONFLICT (tenant_id, name) DO NOTHING
             RETURNING name",
            &[&tenant_id, &service_id, &serde_json::to_value(&provides)?, &actor],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    provides
        .iter()
        .find(|provided| !registered.contains(&provided.name))
        .map_or(Ok(()), |taken| {
            Err(PlatformError::Conflict(format!(
                "provides: '{}' was registered for this tenant while the deploy ran",
                taken.name
            )))
        })
}

fn stored(provides: &[ProvidedExtension]) -> PlatformResult<Vec<ProvidedExtension>> {
    provides
        .iter()
        .map(|provided| {
            cufflink_types::ext::callers(provided.callers.clone())
                .map(|callers| ProvidedExtension {
                    name: provided.name.clone(),
                    callers,
                })
                .map_err(|e| {
                    PlatformError::Validation(format!("provides: '{}' {e}", provided.name))
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_native_services_callers_are_stored_as_an_operators_are() {
        let provided = |name: &str, callers: &[&str]| ProvidedExtension {
            name: name.into(),
            callers: callers.iter().map(|c| c.to_string()).collect(),
        };
        assert_eq!(
            stored(&[
                provided("google.token", &["reports", "billing", "reports"]),
                provided("pdf.render", &["*", "*"]),
            ])
            .expect("stored"),
            [
                provided("google.token", &["billing", "reports"]),
                provided("pdf.render", &["*"]),
            ]
        );
        assert!(matches!(
            stored(&[provided("google.token", &[])]),
            Err(PlatformError::Validation(m)) if m.contains("'google.token' callers:")
        ));
    }

    #[test]
    fn a_stored_backend_has_a_service_exactly_when_it_is_native() {
        let id = Uuid::from_u128(7);
        assert_eq!(backend("nats", None), Ok(Backend::Nats));
        assert_eq!(
            backend("native", Some(id)),
            Ok(Backend::Native { service_id: id })
        );
        assert!(backend("nats", Some(id)).is_err());
        assert!(backend("native", None).is_err());
        assert_eq!(
            backend("grpc", None),
            Err("'grpc' is not one of nats, native".into())
        );
        for stored in [Backend::Nats, Backend::Native { service_id: id }] {
            assert_eq!(backend(stored.as_str(), stored.service_id()), Ok(stored));
        }
    }
}
