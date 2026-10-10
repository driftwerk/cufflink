use crate::errors::PlatformResult;
use deadpool_postgres::GenericClient;
use uuid::Uuid;

pub async fn declare(
    client: &impl GenericClient,
    tenant_id: Uuid,
    name: &str,
    service_id: Uuid,
    config_hash: &str,
) -> PlatformResult<bool> {
    Ok(client
        .query_opt(
            "INSERT INTO tenant_streams (tenant_id, name, declared_by, config_hash)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (tenant_id, name) DO UPDATE
             SET declared_by = COALESCE(tenant_streams.declared_by, EXCLUDED.declared_by),
                 config_hash = EXCLUDED.config_hash,
                 updated_at = now()
             WHERE tenant_streams.config_hash = EXCLUDED.config_hash
                OR tenant_streams.declared_by IS NULL
                OR tenant_streams.declared_by = EXCLUDED.declared_by
             RETURNING name",
            &[&tenant_id, &name, &service_id, &config_hash],
        )
        .await?
        .is_some())
}

pub async fn release(
    client: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
    declared: &[String],
    heirs: &[(String, Uuid)],
) -> PlatformResult<u64> {
    let (names, services): (Vec<&str>, Vec<Uuid>) = heirs
        .iter()
        .map(|(name, service)| (name.as_str(), *service))
        .unzip();
    let inherited = client
        .execute(
            "UPDATE tenant_streams held
             SET declared_by = heir.service_id, updated_at = now()
             FROM unnest($4::text[], $5::uuid[]) AS heir(name, service_id)
             WHERE held.tenant_id = $1 AND held.declared_by = $2
               AND held.name <> ALL($3) AND held.name = heir.name",
            &[&tenant_id, &service_id, &declared, &names, &services],
        )
        .await?;
    let released = client
        .execute(
            "DELETE FROM tenant_streams
             WHERE tenant_id = $1 AND declared_by = $2 AND name <> ALL($3)",
            &[&tenant_id, &service_id, &declared],
        )
        .await?;
    Ok(inherited + released)
}
