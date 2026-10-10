use crate::errors::{PlatformError, PlatformResult};
use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct Tenant {
    pub id: Uuid,
    pub keycloak_user_id: String,
    pub name: String,
    pub slug: String,
    pub keycloak_url: Option<String>,
    pub keycloak_realm: Option<String>,
    pub deploy_role: String,
    pub keycloak_ops_client_id: Option<String>,
    pub keycloak_ops_client_secret: Option<String>,
    pub service_account_clients: Vec<String>,
    pub keycloak_issuer: Option<String>,
    pub source_tenant_id: Option<Uuid>,
    pub database_name: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl Tenant {
    pub fn group_id(&self) -> Uuid {
        self.source_tenant_id.unwrap_or(self.id)
    }
}

pub const DEFAULT_DEPLOY_ROLE: &str = "cufflink-deploy";

const DATABASE_NAME_INDEX: &str = "tenants_database_name_idx";

#[derive(Debug, Clone, Default)]
pub struct NewTenant<'a> {
    pub name: &'a str,
    pub slug: &'a str,
    pub keycloak_url: Option<&'a str>,
    pub keycloak_realm: Option<&'a str>,
    pub deploy_role: Option<&'a str>,
    pub keycloak_ops_client_id: Option<&'a str>,
    pub keycloak_ops_client_secret: Option<&'a str>,
    pub service_account_clients: &'a [String],
    pub keycloak_issuer: Option<&'a str>,
    pub source_tenant_id: Option<Uuid>,
    pub database_name: Option<&'a str>,
}

#[derive(Debug, Clone, Default)]
pub struct TenantUpdate<'a> {
    pub name: Option<&'a str>,
    pub keycloak_url: Option<&'a str>,
    pub keycloak_realm: Option<&'a str>,
    pub deploy_role: Option<&'a str>,
    pub keycloak_ops_client_id: Option<&'a str>,
    pub keycloak_ops_client_secret: Option<&'a str>,
    pub service_account_clients: Option<&'a [String]>,
    pub keycloak_issuer: Option<&'a str>,
}

const TENANT_COLUMNS: &str =
    "id, keycloak_user_id, name, slug, keycloak_url, keycloak_realm, deploy_role, keycloak_ops_client_id, keycloak_ops_client_secret, service_account_clients, keycloak_issuer, source_tenant_id, database_name, created_at";

fn row_to_tenant(row: &tokio_postgres::Row) -> Tenant {
    Tenant {
        id: row.get("id"),
        keycloak_user_id: row.get("keycloak_user_id"),
        name: row.get("name"),
        slug: row.get("slug"),
        keycloak_url: row.get("keycloak_url"),
        keycloak_realm: row.get("keycloak_realm"),
        deploy_role: row.get("deploy_role"),
        keycloak_ops_client_id: row.get("keycloak_ops_client_id"),
        keycloak_ops_client_secret: row.get("keycloak_ops_client_secret"),
        service_account_clients: row.get("service_account_clients"),
        keycloak_issuer: row.get("keycloak_issuer"),
        source_tenant_id: row.get("source_tenant_id"),
        database_name: row.get("database_name"),
        created_at: row.get("created_at"),
    }
}

pub async fn resolve_for_jwt(
    pool: &deadpool_postgres::Pool,
    jwt_realm: &str,
    slug: Option<&str>,
) -> PlatformResult<Option<Tenant>> {
    let client = pool.get().await?;
    let row = match slug {
        Some(s) => {
            client
                .query_opt(
                    &format!(
                        "SELECT {} FROM tenants
                         WHERE slug = $1 AND keycloak_realm = $2 AND keycloak_url IS NOT NULL",
                        TENANT_COLUMNS
                    ),
                    &[&s, &jwt_realm],
                )
                .await?
        }
        None => {
            client
                .query_opt(
                    &format!(
                        "SELECT {} FROM tenants
                         WHERE keycloak_realm = $1 AND keycloak_url IS NOT NULL
                         ORDER BY created_at ASC
                         LIMIT 1",
                        TENANT_COLUMNS
                    ),
                    &[&jwt_realm],
                )
                .await?
        }
    };
    Ok(row.map(|r| row_to_tenant(&r)))
}

pub async fn find_or_create(
    pool: &deadpool_postgres::Pool,
    keycloak_user_id: &str,
    name: &str,
    slug: &str,
) -> PlatformResult<Tenant> {
    let client = pool.get().await?;

    let row = client
        .query_opt(
            &format!(
                "SELECT {} FROM tenants WHERE keycloak_user_id = $1",
                TENANT_COLUMNS
            ),
            &[&keycloak_user_id],
        )
        .await?;

    if let Some(row) = row {
        return Ok(row_to_tenant(&row));
    }

    let row = client
        .query_one(
            &format!(
                "INSERT INTO tenants (keycloak_user_id, name, slug) VALUES ($1, $2, $3) RETURNING {}",
                TENANT_COLUMNS
            ),
            &[&keycloak_user_id, &name, &slug],
        )
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to create tenant: {}", e)))?;

    Ok(row_to_tenant(&row))
}

pub async fn create_tenant(
    client: &impl GenericClient,
    new: &NewTenant<'_>,
) -> PlatformResult<Tenant> {
    let row = client
        .query_one(
            &format!(
                "INSERT INTO tenants (keycloak_user_id, name, slug, keycloak_url, keycloak_realm, deploy_role, keycloak_ops_client_id, keycloak_ops_client_secret, service_account_clients, keycloak_issuer, source_tenant_id, database_name)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                 RETURNING {}",
                TENANT_COLUMNS
            ),
            &[
                &format!("platform-{}", new.slug),
                &new.name,
                &new.slug,
                &new.keycloak_url,
                &new.keycloak_realm,
                &new.deploy_role.unwrap_or(DEFAULT_DEPLOY_ROLE),
                &new.keycloak_ops_client_id,
                &new.keycloak_ops_client_secret,
                &new.service_account_clients,
                &new.keycloak_issuer,
                &new.source_tenant_id,
                &new.database_name,
            ],
        )
        .await
        .map_err(|e| {
            let violated = e
                .as_db_error()
                .filter(|db| db.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
                .map(|db| db.constraint());
            match (violated, new.database_name) {
                (Some(Some(DATABASE_NAME_INDEX)), Some(database)) => PlatformError::Conflict(
                    format!("Database '{database}' is already a tenant's"),
                ),
                (Some(_), _) => {
                    PlatformError::Conflict(format!("Tenant '{}' already exists", new.slug))
                }
                (None, _) => PlatformError::Internal(format!("Failed to create tenant: {}", e)),
            }
        })?;

    Ok(row_to_tenant(&row))
}

pub async fn get_by_id(pool: &deadpool_postgres::Pool, id: Uuid) -> PlatformResult<Option<Tenant>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!("SELECT {} FROM tenants WHERE id = $1", TENANT_COLUMNS),
            &[&id],
        )
        .await?;
    Ok(row.map(|r| row_to_tenant(&r)))
}

pub async fn get_by_slug(
    pool: &deadpool_postgres::Pool,
    slug: &str,
) -> PlatformResult<Option<Tenant>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!("SELECT {} FROM tenants WHERE slug = $1", TENANT_COLUMNS),
            &[&slug],
        )
        .await?;

    Ok(row.map(|r| row_to_tenant(&r)))
}

pub async fn require_by_slug(pool: &deadpool_postgres::Pool, slug: &str) -> PlatformResult<Tenant> {
    get_by_slug(pool, slug)
        .await?
        .ok_or_else(|| PlatformError::NotFound(format!("Tenant '{slug}' not found")))
}

pub async fn list_all(pool: &deadpool_postgres::Pool) -> PlatformResult<Vec<Tenant>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!("SELECT {} FROM tenants ORDER BY created_at", TENANT_COLUMNS),
            &[],
        )
        .await?;

    Ok(rows.iter().map(row_to_tenant).collect())
}

pub async fn update_by_slug(
    client: &impl GenericClient,
    slug: &str,
    update: &TenantUpdate<'_>,
) -> PlatformResult<Option<Tenant>> {
    let sql = format!(
        "UPDATE tenants SET
            name = COALESCE($1, name),
            keycloak_url = COALESCE($2, keycloak_url),
            keycloak_realm = COALESCE($3, keycloak_realm),
            deploy_role = COALESCE($4, deploy_role),
            keycloak_ops_client_id = COALESCE($5, keycloak_ops_client_id),
            keycloak_ops_client_secret = COALESCE($6, keycloak_ops_client_secret),
            service_account_clients = COALESCE($7, service_account_clients),
            keycloak_issuer = CASE WHEN $8::text IS NULL THEN keycloak_issuer ELSE NULLIF($8, '') END
         WHERE slug = $9
         RETURNING {}",
        TENANT_COLUMNS
    );
    let row = client
        .query_opt(
            &sql,
            &[
                &update.name,
                &update.keycloak_url,
                &update.keycloak_realm,
                &update.deploy_role,
                &update.keycloak_ops_client_id,
                &update.keycloak_ops_client_secret,
                &update.service_account_clients,
                &update.keycloak_issuer,
                &slug,
            ],
        )
        .await?;
    Ok(row.map(|r| row_to_tenant(&r)))
}

pub async fn sharing_realm(
    client: &impl GenericClient,
    realm: &str,
    deploy_role: &str,
    excluding: Uuid,
) -> PlatformResult<Option<String>> {
    client
        .execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('cufflink.tenant_realm:' || $1, 0))",
            &[&realm],
        )
        .await?;
    let row = client
        .query_opt(
            "SELECT slug FROM tenants
             WHERE keycloak_realm = $1 AND deploy_role = $2
               AND source_tenant_id IS NULL AND id <> $3
             ORDER BY created_at
             LIMIT 1",
            &[&realm, &deploy_role, &excluding],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}

pub async fn delete_by_slug(pool: &deadpool_postgres::Pool, slug: &str) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let count = client
        .execute("DELETE FROM tenants WHERE slug = $1", &[&slug])
        .await?;
    Ok(count > 0)
}
