use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct Binding {
    pub service_id: Uuid,
    pub role_name: String,
    pub managed: bool,
    pub login: bool,
    #[serde(skip)]
    pub password_enc: Option<Vec<u8>>,
    pub read_only_role: Option<String>,
    pub verified_at: Option<DateTime<Utc>>,
    pub verify_report: Option<Value>,
    pub updated_by: String,
    pub updated_at: DateTime<Utc>,
}

const COLUMNS: &str = "service_id, role_name, managed, login, password_enc, read_only_role,
                       verified_at, verify_report, updated_by, updated_at";

fn qualified(alias: &str) -> String {
    COLUMNS
        .split(',')
        .map(|column| format!("{alias}.{}", column.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn row_to_binding(row: &tokio_postgres::Row) -> Binding {
    Binding {
        service_id: row.get("service_id"),
        role_name: row.get("role_name"),
        managed: row.get("managed"),
        login: row.get("login"),
        password_enc: row.get("password_enc"),
        read_only_role: row.get("read_only_role"),
        verified_at: row.get("verified_at"),
        verify_report: row.get("verify_report"),
        updated_by: row.get("updated_by"),
        updated_at: row.get("updated_at"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    Verified,
    Unverified,
    AsBefore,
}

impl Verification {
    fn as_str(self) -> &'static str {
        match self {
            Verification::Verified => "verified",
            Verification::Unverified => "unverified",
            Verification::AsBefore => "as_before",
        }
    }
}

pub struct NewBinding<'a> {
    pub service_id: Uuid,
    pub role_name: &'a str,
    pub managed: bool,
    pub password_enc: Option<&'a [u8]>,
    pub read_only_role: Option<&'a str>,
    pub updated_by: &'a str,
    pub verification: Verification,
    pub verify_report: &'a Value,
}

pub async fn get(
    client: &impl GenericClient,
    service_id: Uuid,
) -> Result<Option<Binding>, tokio_postgres::Error> {
    client
        .query_opt(
            &format!("SELECT {COLUMNS} FROM service_db_roles WHERE service_id = $1"),
            &[&service_id],
        )
        .await
        .map(|row| row.as_ref().map(row_to_binding))
}

pub async fn write(
    client: &impl GenericClient,
    new: &NewBinding<'_>,
) -> Result<Binding, tokio_postgres::Error> {
    client
        .query_one(
            &format!(
                "INSERT INTO service_db_roles
                     (service_id, role_name, managed, login, password_enc, read_only_role,
                      updated_by, verify_report, verified_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                         CASE WHEN $9 = 'verified' THEN now() END)
                 ON CONFLICT (service_id) DO UPDATE SET
                     role_name = EXCLUDED.role_name, managed = EXCLUDED.managed,
                     login = EXCLUDED.login, password_enc = EXCLUDED.password_enc,
                     read_only_role = EXCLUDED.read_only_role, updated_by = EXCLUDED.updated_by,
                     verify_report = EXCLUDED.verify_report,
                     verified_at = CASE $9 WHEN 'verified' THEN now()
                                           WHEN 'as_before' THEN service_db_roles.verified_at END,
                     updated_at = now()
                 RETURNING {COLUMNS}"
            ),
            &[
                &new.service_id,
                &new.role_name,
                &new.managed,
                &new.password_enc.is_some(),
                &new.password_enc,
                &new.read_only_role,
                &new.updated_by,
                &new.verify_report,
                &new.verification.as_str(),
            ],
        )
        .await
        .map(|row| row_to_binding(&row))
}

pub async fn bound_elsewhere(
    client: &impl GenericClient,
    role_name: &str,
    service_id: Uuid,
) -> Result<Option<(String, String)>, tokio_postgres::Error> {
    client
        .query_opt(
            "SELECT t.slug, s.name FROM service_db_roles r
             JOIN services s ON s.id = r.service_id JOIN tenants t ON t.id = s.tenant_id
             WHERE r.role_name = $1 AND r.service_id <> $2
             ORDER BY t.slug, s.name LIMIT 1",
            &[&role_name, &service_id],
        )
        .await
        .map(|row| row.map(|row| (row.get(0), row.get(1))))
}

pub async fn list_by_tenant(
    client: &impl GenericClient,
    tenant_id: Uuid,
) -> Result<Vec<(String, Binding)>, tokio_postgres::Error> {
    client
        .query(
            &format!(
                "SELECT s.name, {} FROM service_db_roles r
                 JOIN services s ON s.id = r.service_id
                 WHERE s.tenant_id = $1 ORDER BY s.name",
                qualified("r")
            ),
            &[&tenant_id],
        )
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| (row.get("name"), row_to_binding(row)))
                .collect()
        })
}
