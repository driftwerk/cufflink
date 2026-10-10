use crate::errors::PlatformResult;
use deadpool_postgres::GenericClient;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct RbacCloneCounts {
    pub roles_created: u64,
    pub roles_synced: u64,
    pub assignments_created: u64,
}

pub async fn clone_tenant_rbac(
    txn: &impl GenericClient,
    source_tenant_id: Uuid,
    target_tenant_id: Uuid,
) -> PlatformResult<RbacCloneCounts> {
    let roles_created = txn
        .execute(
            "INSERT INTO roles (tenant_id, name, description, permissions)
             SELECT $2, r.name, r.description, r.permissions
             FROM roles r
             WHERE r.tenant_id = $1
               AND NOT EXISTS (
                   SELECT 1 FROM roles t WHERE t.tenant_id = $2 AND t.name = r.name
               )",
            &[&source_tenant_id, &target_tenant_id],
        )
        .await?;

    let roles_synced = txn
        .execute(
            "UPDATE roles t SET permissions = s.permissions, description = s.description
             FROM roles s
             WHERE s.tenant_id = $1 AND t.tenant_id = $2 AND t.name = s.name
               AND (t.permissions IS DISTINCT FROM s.permissions
                    OR t.description IS DISTINCT FROM s.description)",
            &[&source_tenant_id, &target_tenant_id],
        )
        .await?;

    let assignments_created = txn
        .execute(
            "INSERT INTO user_roles (tenant_id, user_id, role_id)
             SELECT $2, ur.user_id, t.id
             FROM user_roles ur
             JOIN roles s ON s.id = ur.role_id AND s.tenant_id = $1
             JOIN roles t ON t.tenant_id = $2 AND t.name = s.name
             WHERE ur.tenant_id = $1
               AND NOT EXISTS (
                   SELECT 1 FROM user_roles e
                   WHERE e.tenant_id = $2 AND e.user_id = ur.user_id AND e.role_id = t.id
               )",
            &[&source_tenant_id, &target_tenant_id],
        )
        .await?;

    Ok(RbacCloneCounts {
        roles_created,
        roles_synced,
        assignments_created,
    })
}

pub async fn assign_role(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    user_id: &str,
    role_id: Uuid,
) -> PlatformResult<bool> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "WITH role AS (SELECT id FROM roles WHERE id = $3 AND tenant_id = $1),
                  assigned AS (
                      INSERT INTO user_roles (tenant_id, user_id, role_id)
                      SELECT $1, $2, id FROM role
                      ON CONFLICT (tenant_id, user_id, role_id) DO NOTHING
                  )
             SELECT EXISTS (SELECT 1 FROM role)",
            &[&tenant_id, &user_id, &role_id],
        )
        .await?;
    Ok(row.get(0))
}
