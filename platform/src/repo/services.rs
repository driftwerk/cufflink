use crate::errors::{PlatformError, PlatformResult};
use crate::registry::ServiceIdentity;
use chrono::{DateTime, Utc};
use cufflink_types::manifest::validate::{folded, is_reserved_schema};
use cufflink_types::ServiceMode;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

const COLUMNS: &str =
    "id, tenant_id, name, schema_name, current_version, status, created_at, updated_at";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Active,
    Inactive,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Active => "active",
            Status::Inactive => "inactive",
        }
    }

    fn stored(status: &str) -> Self {
        status.parse().unwrap_or(Status::Inactive)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Status must be 'active' or 'inactive'")]
pub struct UnknownStatus;

impl FromStr for Status {
    type Err = UnknownStatus;

    fn from_str(status: &str) -> Result<Self, Self::Err> {
        [Status::Active, Status::Inactive]
            .into_iter()
            .find(|known| known.as_str() == status)
            .ok_or(UnknownStatus)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Service {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    pub current_version: i32,
    pub status: Status,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const SCHEMA_SQL: &str = "lower(COALESCE(schema_name, replace(name, '-', '_')))";

pub const SCHEMA_INDEX: &str = "services_schema_idx";

impl Service {
    pub fn schema(&self) -> String {
        cufflink_types::service_schema(&self.name, self.schema_name.as_deref())
    }

    pub fn identity(&self, tenant_slug: &str) -> ServiceIdentity {
        ServiceIdentity {
            tenant_id: self.tenant_id,
            tenant_slug: tenant_slug.to_string(),
            service_id: self.id,
            service_name: self.name.clone(),
            schema: self.schema(),
        }
    }
}

pub fn require_recorded_schema(service: &Service, declared: &str) -> PlatformResult<()> {
    let recorded = service.schema();
    if recorded == declared {
        Ok(())
    } else {
        Err(PlatformError::Validation(format!(
            "schema_name is immutable once deployed: {recorded} \u{2192} {declared}"
        )))
    }
}

fn schema_taken(schema: &str, owner: &str) -> PlatformError {
    PlatformError::Validation(format!(
        "schema_name: '{schema}' is the schema of service '{owner}' of this tenant"
    ))
}

fn require_unreserved_schema(name: &str, schema: &str) -> PlatformResult<()> {
    if is_reserved_schema(schema) {
        Err(PlatformError::Validation(format!(
            "schema_name: schema '{schema}' of service '{name}' is reserved; \
             declare a schema_name of its own"
        )))
    } else {
        Ok(())
    }
}

async fn require_free_schema(
    client: &deadpool_postgres::Client,
    tenant_id: Uuid,
    name: &str,
    schema: &str,
) -> PlatformResult<()> {
    client
        .query_opt(
            &format!(
                "SELECT name FROM services
                 WHERE tenant_id = $1 AND name <> $2 AND {SCHEMA_SQL} = $3 LIMIT 1"
            ),
            &[&tenant_id, &name, &folded(schema)],
        )
        .await?
        .map_or(Ok(()), |row| {
            Err(schema_taken(schema, &row.get::<_, String>(0)))
        })
}

fn recording_error(schema: &str, error: tokio_postgres::Error) -> PlatformError {
    match error.as_db_error().and_then(|e| e.constraint()) {
        Some(SCHEMA_INDEX) => PlatformError::Validation(format!(
            "schema_name: '{schema}' was taken by a service of this tenant deployed at the same time"
        )),
        _ => PlatformError::Internal(format!("Failed to record the service: {error}")),
    }
}

pub async fn find_or_create(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    name: &str,
    schema: &str,
    created: Status,
) -> PlatformResult<Service> {
    let client = pool.get().await?;
    let existing = client
        .query_opt(
            &format!("SELECT {COLUMNS} FROM services WHERE tenant_id = $1 AND name = $2"),
            &[&tenant_id, &name],
        )
        .await?
        .as_ref()
        .map(row_to_service);
    if let Some(service) = existing {
        return require_recorded_schema(&service, schema).map(|()| service);
    }
    require_unreserved_schema(name, schema)?;
    require_free_schema(&client, tenant_id, name, schema).await?;
    client
        .query_one(
            &format!(
                "INSERT INTO services (tenant_id, name, schema_name, status)
                 VALUES ($1, $2, $3, $4) RETURNING {COLUMNS}"
            ),
            &[&tenant_id, &name, &schema, &created.as_str()],
        )
        .await
        .map(|row| row_to_service(&row))
        .map_err(|e| recording_error(schema, e))
}

pub async fn sharing_a_schema(
    pool: &deadpool_postgres::Pool,
) -> PlatformResult<Vec<(Uuid, String, Vec<String>)>> {
    Ok(pool
        .get()
        .await?
        .query(
            &format!(
                "SELECT tenant_id, {SCHEMA_SQL} AS schema, array_agg(name ORDER BY name) AS names
                 FROM services GROUP BY 1, 2 HAVING count(*) > 1 ORDER BY 1, 2"
            ),
            &[],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect())
}

pub async fn without_verified_role(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
) -> PlatformResult<Vec<String>> {
    Ok(pool
        .get()
        .await?
        .query(
            "SELECT s.name FROM services s
             LEFT JOIN service_db_roles r ON r.service_id = s.id
             LEFT JOIN deployments d ON d.service_id = s.id AND d.version = s.current_version
             WHERE s.tenant_id = $1 AND r.verified_at IS NULL
               AND COALESCE(d.manifest->>'mode', '') NOT IN ('web', 'native')
             ORDER BY s.name",
            &[&tenant_id],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

#[allow(dead_code)]
pub async fn list_by_tenant(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
) -> PlatformResult<Vec<Service>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!("SELECT {COLUMNS} FROM services WHERE tenant_id = $1 ORDER BY name"),
            &[&tenant_id],
        )
        .await?;

    Ok(rows.iter().map(row_to_service).collect())
}

pub async fn find(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    name: &str,
) -> PlatformResult<Option<Service>> {
    let row = pool
        .get()
        .await?
        .query_opt(
            &format!("SELECT {COLUMNS} FROM services WHERE tenant_id = $1 AND name = $2"),
            &[&tenant_id, &name],
        )
        .await?;
    Ok(row.as_ref().map(row_to_service))
}

pub async fn get_by_id(
    pool: &deadpool_postgres::Pool,
    id: Uuid,
) -> PlatformResult<Option<Service>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!("SELECT {COLUMNS} FROM services WHERE id = $1"),
            &[&id],
        )
        .await?;

    Ok(row.as_ref().map(row_to_service))
}

pub async fn hold(
    tx: &deadpool_postgres::Transaction<'_>,
    id: Uuid,
    tenant_id: Uuid,
) -> Result<bool, tokio_postgres::Error> {
    tx.query_opt(
        "SELECT 1 FROM services WHERE id = $1 AND tenant_id = $2 FOR KEY SHARE",
        &[&id, &tenant_id],
    )
    .await
    .map(|held| held.is_some())
}

pub async fn update_status(
    pool: &deadpool_postgres::Pool,
    id: Uuid,
    status: Status,
) -> PlatformResult<()> {
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE services SET status = $1, updated_at = now() WHERE id = $2",
            &[&status.as_str(), &id],
        )
        .await?;
    Ok(())
}

pub async fn list_artifacts_for_tenant(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    mode: &ServiceMode,
) -> PlatformResult<Vec<ServiceArtifact>> {
    Ok(pool
        .get()
        .await?
        .query(
            "SELECT DISTINCT s.id, s.name, d.artifact_s3_key
             FROM services s
             JOIN deployments d ON d.service_id = s.id
             WHERE s.tenant_id = $1
               AND d.manifest->>'mode' = $2
               AND d.artifact_s3_key IS NOT NULL
             ORDER BY 2, 3",
            &[&tenant_id, &mode.as_str()],
        )
        .await?
        .iter()
        .map(|row| ServiceArtifact {
            service_id: row.get(0),
            service_name: row.get(1),
            artifact_s3_key: row.get(2),
        })
        .collect())
}

#[derive(Debug, Clone)]
pub struct ServiceArtifact {
    pub service_id: Uuid,
    pub service_name: String,
    pub artifact_s3_key: String,
}

pub async fn delete_service(
    client: &impl deadpool_postgres::GenericClient,
    id: Uuid,
) -> PlatformResult<()> {
    client
        .execute("DELETE FROM services WHERE id = $1", &[&id])
        .await?;
    Ok(())
}

fn row_to_service(row: &tokio_postgres::Row) -> Service {
    Service {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        schema_name: row.get("schema_name"),
        current_version: row.get("current_version"),
        status: Status::stored(row.get("status")),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(name: &str, schema_name: Option<&str>, current_version: i32) -> Service {
        Service {
            id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
            name: name.into(),
            schema_name: schema_name.map(str::to_string),
            current_version,
            status: Status::Active,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn a_status_round_trips_its_wire_strings() {
        for (status, wire) in [(Status::Active, "active"), (Status::Inactive, "inactive")] {
            assert_eq!(status.as_str(), wire);
            assert_eq!(wire.parse(), Ok(status));
            assert_eq!(Status::stored(wire), status);
            let json = serde_json::to_value(status).expect("wire");
            assert_eq!(json, serde_json::json!(wire));
            assert_eq!(
                serde_json::from_value::<Status>(json).expect("status"),
                status
            );
        }
        for unknown in ["", "Active", "ACTIVE", "disabled", " active"] {
            assert_eq!(unknown.parse::<Status>(), Err(UnknownStatus), "{unknown}");
            assert!(serde_json::from_value::<Status>(serde_json::json!(unknown)).is_err());
            assert_eq!(
                Status::stored(unknown),
                Status::Inactive,
                "only 'active' runs: {unknown}"
            );
        }
        assert_eq!(
            UnknownStatus.to_string(),
            "Status must be 'active' or 'inactive'"
        );
        assert_eq!(
            serde_json::to_value(service("notes", None, 1)).expect("service")["status"],
            "active"
        );
    }

    #[test]
    fn a_service_recorded_without_a_schema_has_the_one_its_name_gives() {
        assert_eq!(service("stock-service", None, 4).schema(), "stock_service");
        assert_eq!(
            service("notes", Some("loan_service"), 4).schema(),
            "loan_service"
        );
    }

    #[test]
    fn a_service_keeps_the_schema_recorded_or_the_one_its_name_gave() {
        let legacy = service("stock-service", None, 4);
        assert!(require_recorded_schema(&legacy, "stock_service").is_ok());
        let refused = require_recorded_schema(&legacy, "stocks").expect_err("immutable");
        assert!(
            matches!(
                &refused,
                PlatformError::Validation(m)
                    if m == "schema_name is immutable once deployed: stock_service \u{2192} stocks"
            ),
            "{refused}"
        );
        let named = service("notes", Some("loan_service"), 1);
        assert!(require_recorded_schema(&named, "notes").is_err());
        assert!(require_recorded_schema(&named, "loan_service").is_ok());
    }

    #[test]
    fn a_new_service_may_not_take_a_reserved_schema_by_its_name_either() {
        for (name, schema) in [
            ("public", "public"),
            ("Public", "Public"),
            ("_cufflink", "_cufflink"),
            ("_cufflink-audit", "_cufflink_audit"),
            ("pg-catalog", "pg_catalog"),
            ("information-schema", "information_schema"),
            ("notes", "pg_temp"),
        ] {
            let refused = require_unreserved_schema(name, schema).expect_err(schema);
            assert!(
                matches!(&refused, PlatformError::Validation(m)
                    if m.starts_with(&format!("schema_name: schema '{schema}' of service '{name}' is reserved"))),
                "{refused}"
            );
        }
        for (name, schema) in [("order", "order"), ("user", "user"), ("notes", "notes")] {
            assert!(require_unreserved_schema(name, schema).is_ok(), "{schema}");
        }
    }
}
