pub mod admin_secrets;
pub mod audit;
pub mod auth;
pub mod backup;
pub mod config;
pub mod consumers;
pub mod db_roles;
pub mod deployments;
pub mod dlq;
pub mod extensions;
pub mod files;
pub mod health;
pub mod jobs;
pub mod logs;
pub mod metrics;
pub mod openapi;
pub mod partner_keys;
pub mod policies;
pub mod query;
pub mod rbac;
pub mod runner;
pub mod runner_keys;
pub mod seed;
pub mod services;
pub mod tenants;
pub mod version;
pub mod wasm;
pub mod web;
pub mod webhooks;

use crate::errors::{PlatformError, PlatformResult};
use crate::repo;
use uuid::Uuid;

pub async fn get_service_for_tenant(
    db: &deadpool_postgres::Pool,
    service_id: Uuid,
    tenant_id: Uuid,
) -> PlatformResult<repo::services::Service> {
    repo::services::get_by_id(db, service_id)
        .await?
        .filter(|service| service.tenant_id == tenant_id)
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))
}
