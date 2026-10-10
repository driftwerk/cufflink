use crate::{api, AppState};
use axum::routing::{delete, get, patch, post, put};
use axum::Router;
use std::sync::Arc;

pub(in crate::app) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/metrics", get(api::metrics::metrics_handler))
        .route("/api/version", get(api::version::version))
        .route("/api/auth/config", get(api::auth::auth_config))
        .route(
            "/api/auth/api-keys",
            post(api::auth::create_api_key).get(api::auth::list_api_keys),
        )
        .route("/api/auth/api-keys/{id}", delete(api::auth::revoke_api_key))
        .route(
            "/api/auth/api-keys/{id}/rotate",
            post(api::auth::rotate_api_key),
        )
        .route("/api/auth/device/token", post(api::auth::exchange_token))
        .route("/api/auth/me", get(api::auth::me))
        .route("/api/auth/logout", post(api::auth::logout))
        .route("/api/tenants", post(api::tenants::create_tenant))
        .route("/api/tenants", get(api::tenants::list_tenants))
        .route("/api/tenants/{slug}", put(api::tenants::update_tenant))
        .route("/api/tenants/{slug}", delete(api::tenants::delete_tenant))
        .route(
            "/api/tenants/{slug}/rbac/sync",
            post(api::tenants::sync_tenant_rbac),
        )
        .route(
            "/api/tenants/{slug}/nats-claims",
            get(api::tenants::get_nats_claims).put(api::tenants::put_nats_claims),
        )
        .route(
            "/api/tenants/{slug}/extensions",
            get(api::extensions::list_extensions),
        )
        .route(
            "/api/tenants/{slug}/extensions/{name}",
            put(api::extensions::register_extension).delete(api::extensions::unregister_extension),
        )
        .route(
            "/api/tenants/{slug}/runner-keys",
            post(api::runner_keys::create_runner_key).get(api::runner_keys::list_runner_keys),
        )
        .route(
            "/api/tenants/{slug}/runner-keys/{id}",
            delete(api::runner_keys::revoke_runner_key),
        )
        .route(
            "/api/tenants/{slug}/policies",
            get(api::policies::get_policies).put(api::policies::put_policies),
        )
        .route(
            "/api/tenants/{slug}/policies/{key}",
            delete(api::policies::delete_policy),
        )
        .route(
            "/api/tenants/{slug}/services/{service}/db-role",
            get(api::db_roles::get_db_role).put(api::db_roles::set_db_role),
        )
        .route(
            "/api/tenants/{slug}/services/{service}/db-role/verify",
            post(api::db_roles::verify_db_role),
        )
        .route(
            "/api/tenants/{slug}/services/{service}/db-role/rotate",
            post(api::db_roles::rotate_db_role),
        )
        .route(
            "/api/tenants/{slug}/partner-keys",
            post(api::partner_keys::create_partner_key).get(api::partner_keys::list_partner_keys),
        )
        .route(
            "/api/tenants/{slug}/partner-keys/{id}",
            delete(api::partner_keys::revoke_partner_key),
        )
        .route(
            "/api/platform/api-keys",
            post(api::tenants::create_platform_api_key),
        )
        .route(
            "/api/platform/api-keys",
            get(api::tenants::list_platform_api_keys),
        )
        .route(
            "/api/platform/api-keys/{id}",
            delete(api::tenants::revoke_platform_api_key),
        )
        .route(
            "/api/platform/api-keys/{id}/rotate",
            post(api::auth::rotate_platform_api_key),
        )
        .route("/api/audit", get(api::audit::list_audit_log))
        .route(
            "/api/admin/secrets/reseal",
            post(api::admin_secrets::reseal),
        )
        .route("/api/services", get(api::services::list_services))
        .route("/api/services/{id}", get(api::services::get_service))
        .route("/api/services/{id}", delete(api::services::delete_service))
        .route(
            "/api/services/{id}/status",
            patch(api::services::set_status),
        )
        .route(
            "/api/services/{id}/permission-areas",
            get(api::services::permission_areas),
        )
        .route(
            "/api/services/{id}/openapi.json",
            get(api::openapi::openapi_spec),
        )
        .route(
            "/api/services/deploy",
            post(api::deployments::deploy_service),
        )
        .route(
            "/api/services/{id}/deployments",
            get(api::deployments::list_deployments),
        )
        .route("/api/services/{id}/consumers", get(api::consumers::list))
        .route("/api/tenants/{slug}/dlq", get(api::dlq::list))
        .route(
            "/api/tenants/{slug}/dlq/{sequence}/replay",
            post(api::dlq::replay),
        )
        .route("/api/tenants/{slug}/dlq/{sequence}", delete(api::dlq::drop))
        .route(
            "/api/services/{id}/deployments/prune",
            post(api::deployments::prune_deployments),
        )
        .route(
            "/api/services/{id}/deployments/{version}",
            delete(api::deployments::delete_deployment),
        )
        .route(
            "/api/services/{id}/rollback",
            post(api::deployments::rollback_service),
        )
        .route("/api/deploys/{id}", get(api::deployments::deploy_status))
        .route("/api/services/{id}/seed", post(api::seed::seed_handler))
        .route(
            "/api/services/{id}/backup/export",
            post(api::backup::start_export),
        )
        .route(
            "/api/services/{id}/backup/restore",
            post(api::backup::start_restore),
        )
        .route(
            "/api/services/{id}/backup/jobs",
            get(api::backup::list_jobs),
        )
        .route(
            "/api/services/{id}/backup/jobs/{job_id}",
            get(api::backup::get_job),
        )
        .route(
            "/api/services/{id}/backup/jobs/{job_id}/cancel",
            post(api::backup::cancel_job),
        )
        .route(
            "/api/services/{id}/backup/jobs/{job_id}/download",
            get(api::backup::download_export),
        )
        .route(
            "/api/services/{id}/backup/upload",
            post(api::backup::get_upload_url),
        )
        .route("/api/services/{id}/schema", get(api::backup::get_schema))
        .route("/api/services/{id}/query", post(api::query::exec_query))
        .route("/api/services/{id}/logs", get(api::logs::query_logs))
        .route("/api/services/{id}/logs/tail", get(api::logs::tail_logs))
        .route("/api/services/{id}/config", get(api::config::list_config))
        .route("/api/services/{id}/config", put(api::config::set_config))
        .route(
            "/api/services/{id}/config/apply",
            post(api::config::apply_config).delete(api::config::delete_config_named_apply),
        )
        .route(
            "/api/services/{id}/config/{key}",
            delete(api::config::delete_config),
        )
        .route(
            "/api/services/{id}/webhooks",
            post(api::webhooks::register_webhook),
        )
        .route(
            "/api/services/{id}/webhooks",
            get(api::webhooks::list_webhooks),
        )
        .route(
            "/api/services/{id}/webhooks/{webhook_id}",
            delete(api::webhooks::delete_webhook),
        )
        .route(
            "/api/services/{id}/webhooks/{webhook_id}",
            put(api::webhooks::update_webhook),
        )
        .route(
            "/api/services/{id}/webhooks/{webhook_id}/deliveries",
            get(api::webhooks::list_deliveries),
        )
        .route("/api/users/lookup", get(api::rbac::lookup_users))
        .route(
            "/api/tenants/{tenant_id}/permission-areas",
            get(api::services::tenant_permission_areas),
        )
        .route(
            "/api/tenants/{tenant_id}/roles",
            post(api::rbac::create_role),
        )
        .route("/api/tenants/{tenant_id}/roles", get(api::rbac::list_roles))
        .route(
            "/api/tenants/{tenant_id}/roles/{role_id}",
            put(api::rbac::update_role),
        )
        .route(
            "/api/tenants/{tenant_id}/roles/{role_id}",
            delete(api::rbac::delete_role),
        )
        .route(
            "/api/tenants/{tenant_id}/roles/assign",
            post(api::rbac::assign_role),
        )
        .route(
            "/api/tenants/{tenant_id}/roles/unassign",
            delete(api::rbac::unassign_role),
        )
        .route(
            "/api/tenants/{tenant_id}/users/search",
            get(api::rbac::search_tenant_users),
        )
        .route(
            "/api/tenants/{tenant_id}/users/{user_id}/permissions",
            get(api::rbac::get_user_permissions),
        )
        .route("/api/services/{id}/files", post(api::files::upload_file))
        .route("/api/services/{id}/files", get(api::files::list_files))
        .route(
            "/api/services/{id}/files/{file_id}",
            get(api::files::download_file),
        )
        .route(
            "/api/services/{id}/files/{file_id}",
            delete(api::files::delete_file),
        )
        .route(
            "/api/services/{id}/web",
            post(api::web::upload_web_artifact),
        )
        .route("/api/services/{id}/web/status", get(api::web::web_status))
        .route("/api/services/{id}/wasm", post(api::wasm::upload_wasm))
        .route(
            "/api/services/{id}/wasm/status",
            get(api::wasm::wasm_status),
        )
        .merge(api::runner::routes())
}
