use crate::auth::grants;
use crate::auth::permissions::{invalidate_tenant, invalidate_user};
use crate::auth::{AuthUser, PlatformAdmin, RbacAdmin};
use crate::errors::{PlatformError, PlatformResult};
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct LookupUsersQuery {
    pub q: String,
}

pub async fn lookup_users(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Query(query): Query<LookupUsersQuery>,
) -> PlatformResult<Json<Value>> {
    if query.q.trim().is_empty() {
        return Err(PlatformError::BadRequest(
            "Query parameter 'q' is required".into(),
        ));
    }

    let client = state.platform_db.get().await?;
    let search = query.q.trim().to_lowercase();

    let rows = client
        .query(
            "SELECT keycloak_user_id, name, slug
             FROM tenants
             WHERE LOWER(slug) LIKE '%' || $1 || '%'
                OR LOWER(name) LIKE '%' || $1 || '%'
             ORDER BY name
             LIMIT 20",
            &[&search],
        )
        .await?;

    let users: Vec<Value> = rows
        .iter()
        .filter(|r| {
            let uid: String = r.get(0);
            !uid.starts_with("bootstrap-")
        })
        .map(|r| {
            json!({
                "user_id": r.get::<_, String>(0),
                "name": r.get::<_, String>(1),
                "slug": r.get::<_, String>(2),
            })
        })
        .collect();

    Ok(Json(json!({ "users": users })))
}

#[derive(Debug, Deserialize)]
pub struct CreateRoleRequest {
    pub name: String,
    pub description: Option<String>,
    pub permissions: Vec<String>,
}

impl CreateRoleRequest {
    fn permissions_json(&self) -> PlatformResult<Value> {
        grants::validate_scoped(&self.permissions)
            .map_err(|invalid| PlatformError::Validation(grants::describe(&invalid)))?;
        serde_json::to_value(&self.permissions)
            .map_err(|e| PlatformError::Internal(format!("Failed to serialize permissions: {}", e)))
    }
}

#[derive(Debug, Deserialize)]
pub struct AssignRoleRequest {
    pub user_id: String,
    pub role_id: Uuid,
}

pub async fn create_role(
    State(state): State<Arc<AppState>>,
    RbacAdmin(user): RbacAdmin,
    Path(tenant_id): Path<Uuid>,
    Json(req): Json<CreateRoleRequest>,
) -> PlatformResult<Json<Value>> {
    if tenant_id != user.tenant_id {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }

    let permissions_json = req.permissions_json()?;
    let client = state.platform_db.get().await?;

    let row = client
        .query_one(
            "INSERT INTO roles (tenant_id, name, description, permissions)
             VALUES ($1, $2, $3, $4)
             RETURNING id, name, description, permissions, created_at",
            &[&tenant_id, &req.name, &req.description, &permissions_json],
        )
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to create role: {}", e)))?;

    let id: Uuid = row.get(0);
    let name: String = row.get(1);
    let description: Option<String> = row.get(2);
    let permissions: Value = row.get(3);
    let created_at: chrono::DateTime<chrono::Utc> = row.get(4);

    Ok(Json(json!({
        "id": id,
        "name": name,
        "description": description,
        "permissions": permissions,
        "created_at": created_at.to_rfc3339(),
    })))
}

pub async fn list_roles(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path(tenant_id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    if tenant_id != user.tenant_id {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }

    let client = state.platform_db.get().await?;
    let rows = client
        .query(
            "SELECT id, name, description, permissions, created_at
             FROM roles WHERE tenant_id = $1 ORDER BY name",
            &[&tenant_id],
        )
        .await?;

    let roles: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "id": r.get::<_, Uuid>(0),
                "name": r.get::<_, String>(1),
                "description": r.get::<_, Option<String>>(2),
                "permissions": r.get::<_, Value>(3),
                "created_at": r.get::<_, chrono::DateTime<chrono::Utc>>(4).to_rfc3339(),
            })
        })
        .collect();

    Ok(Json(json!({ "roles": roles })))
}

pub async fn update_role(
    State(state): State<Arc<AppState>>,
    RbacAdmin(user): RbacAdmin,
    Path((tenant_id, role_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateRoleRequest>,
) -> PlatformResult<Json<Value>> {
    if tenant_id != user.tenant_id {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }

    let permissions_json = req.permissions_json()?;
    let client = state.platform_db.get().await?;

    let result = client
        .query_opt(
            "UPDATE roles SET name = $1, description = $2, permissions = $3
             WHERE id = $4 AND tenant_id = $5
             RETURNING id, name, description, permissions, created_at",
            &[
                &req.name,
                &req.description,
                &permissions_json,
                &role_id,
                &tenant_id,
            ],
        )
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to update role: {}", e)))?;

    let row = result.ok_or_else(|| PlatformError::NotFound("Role not found".into()))?;
    drop(client);
    invalidate_tenant(&state, tenant_id).await;

    let id: Uuid = row.get(0);
    let name: String = row.get(1);
    let description: Option<String> = row.get(2);
    let permissions: Value = row.get(3);
    let created_at: chrono::DateTime<chrono::Utc> = row.get(4);

    Ok(Json(json!({
        "id": id,
        "name": name,
        "description": description,
        "permissions": permissions,
        "created_at": created_at.to_rfc3339(),
    })))
}

pub async fn delete_role(
    State(state): State<Arc<AppState>>,
    RbacAdmin(user): RbacAdmin,
    Path((tenant_id, role_id)): Path<(Uuid, Uuid)>,
) -> PlatformResult<Json<Value>> {
    if tenant_id != user.tenant_id {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }

    let client = state.platform_db.get().await?;
    let count = client
        .execute(
            "DELETE FROM roles WHERE id = $1 AND tenant_id = $2",
            &[&role_id, &user.tenant_id],
        )
        .await?;

    if count == 0 {
        return Err(PlatformError::NotFound("Role not found".into()));
    }
    drop(client);
    invalidate_tenant(&state, tenant_id).await;

    Ok(Json(json!({ "deleted": true })))
}

pub async fn assign_role(
    State(state): State<Arc<AppState>>,
    RbacAdmin(user): RbacAdmin,
    Path(tenant_id): Path<Uuid>,
    Json(req): Json<AssignRoleRequest>,
) -> PlatformResult<Json<Value>> {
    if tenant_id != user.tenant_id {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }

    if !crate::repo::rbac::assign_role(&state.platform_db, tenant_id, &req.user_id, req.role_id)
        .await?
    {
        return Err(PlatformError::NotFound("Role not found".into()));
    }
    invalidate_user(&state, tenant_id, &req.user_id).await;

    Ok(Json(json!({
        "user_id": req.user_id,
        "role_id": req.role_id,
        "assigned": true,
    })))
}

pub async fn unassign_role(
    State(state): State<Arc<AppState>>,
    RbacAdmin(user): RbacAdmin,
    Path(tenant_id): Path<Uuid>,
    Json(req): Json<AssignRoleRequest>,
) -> PlatformResult<Json<Value>> {
    if tenant_id != user.tenant_id {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }

    let client = state.platform_db.get().await?;
    let count = client
        .execute(
            "DELETE FROM user_roles WHERE tenant_id = $1 AND user_id = $2 AND role_id = $3",
            &[&tenant_id, &req.user_id, &req.role_id],
        )
        .await?;
    drop(client);
    invalidate_user(&state, tenant_id, &req.user_id).await;

    Ok(Json(json!({
        "user_id": req.user_id,
        "role_id": req.role_id,
        "removed": count > 0,
    })))
}

pub async fn search_tenant_users(
    State(state): State<Arc<AppState>>,
    RbacAdmin(user): RbacAdmin,
    Path(tenant_id): Path<Uuid>,
    Query(query): Query<LookupUsersQuery>,
) -> PlatformResult<Json<Value>> {
    if tenant_id != user.tenant_id {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }

    if query.q.trim().is_empty() {
        return Err(PlatformError::BadRequest(
            "Query parameter 'q' is required".into(),
        ));
    }

    let access = crate::auth::keycloak_admin::OpsAccess::of(&state, tenant_id).await?;
    let admin_token = access.admin_token(&state.config.keycloak_url).await?;
    let realm = access.realm;

    let kc_users = crate::auth::keycloak_admin::search_users(
        &state.config.keycloak_url,
        &realm,
        &admin_token,
        query.q.trim(),
    )
    .await?;

    let users: Vec<Value> = kc_users
        .into_iter()
        .filter(|u| u.enabled.unwrap_or(true))
        .map(|u| {
            let name = match (u.first_name.as_deref(), u.last_name.as_deref()) {
                (Some(f), Some(l)) => format!("{f} {l}"),
                (Some(f), None) => f.to_string(),
                (None, Some(l)) => l.to_string(),
                (None, None) => u.username.clone().unwrap_or_default(),
            };
            json!({
                "user_id": u.id,
                "name": name,
                "email": u.email,
            })
        })
        .collect();

    Ok(Json(json!({ "users": users })))
}

fn may_read_permissions_of(caller: &AuthUser, user_id: &str) -> bool {
    caller.subject == user_id || caller.has_deploy_authority || caller.is_service_account
}

pub async fn get_user_permissions(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((tenant_id, user_id)): Path<(Uuid, String)>,
) -> PlatformResult<Json<Value>> {
    if tenant_id != user.tenant_id || !may_read_permissions_of(&user, &user_id) {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }

    let client = state.platform_db.get().await?;
    let rows = client
        .query(
            "SELECT r.name, r.permissions
             FROM user_roles ur
             JOIN roles r ON ur.role_id = r.id
             WHERE ur.tenant_id = $1 AND ur.user_id = $2",
            &[&tenant_id, &user_id],
        )
        .await?;

    let mut all_permissions = std::collections::HashSet::new();
    let mut role_names = Vec::new();

    for row in &rows {
        let role_name: String = row.get(0);
        let permissions: Value = row.get(1);

        role_names.push(role_name);
        if let Some(perms) = permissions.as_array() {
            for p in perms {
                if let Some(s) = p.as_str() {
                    all_permissions.insert(s.to_string());
                }
            }
        }
    }

    let mut perms: Vec<String> = all_permissions.into_iter().collect();
    perms.sort();

    Ok(Json(json!({
        "user_id": user_id,
        "roles": role_names,
        "permissions": perms,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(permissions: &[&str]) -> CreateRoleRequest {
        CreateRoleRequest {
            name: "staff".into(),
            description: None,
            permissions: permissions.iter().map(|p| p.to_string()).collect(),
        }
    }

    #[test]
    fn role_writes_store_unscoped_and_valid_scoped_permissions() {
        assert_eq!(
            request(&["items:view", "items:edit@all", "any old string"])
                .permissions_json()
                .expect("valid"),
            json!(["items:view", "items:edit@all", "any old string"])
        );
    }

    fn caller(subject: &str, deploy: bool, service_account: bool) -> AuthUser {
        AuthUser {
            subject: subject.into(),
            name: None,
            email: None,
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            auth_method: crate::auth::extractor::AuthMethod::Jwt,
            token_info: None,
            is_service_account: service_account,
            is_platform_admin: false,
            has_deploy_authority: deploy,
            realm: None,
        }
    }

    #[test]
    fn a_user_reads_only_their_own_permissions_unless_they_hold_authority() {
        assert!(may_read_permissions_of(&caller("u1", false, false), "u1"));
        assert!(!may_read_permissions_of(&caller("u1", false, false), "u2"));
        assert!(may_read_permissions_of(&caller("ops", true, false), "u2"));
        assert!(may_read_permissions_of(&caller("svc", false, true), "u2"));
    }

    #[test]
    fn a_malformed_scope_is_a_validation_error_naming_it() {
        let err = request(&["items:view@team"])
            .permissions_json()
            .expect_err("malformed");
        assert!(
            matches!(&err, PlatformError::Validation(m) if m.contains("items:view@team")),
            "{err:?}"
        );
    }
}
