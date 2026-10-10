use crate::errors::PlatformResult;
use deadpool_postgres::Pool;
use std::time::Duration;
use uuid::Uuid;

pub async fn acquire(
    pool: &Pool,
    tenant_id: Uuid,
    holder: &str,
    ttl: Duration,
) -> PlatformResult<bool> {
    Ok(pool
        .get()
        .await?
        .query_opt(
            "INSERT INTO outbox_relay_leases (tenant_id, holder, expires_at)
             VALUES ($1, $2, now() + make_interval(secs => $3))
             ON CONFLICT (tenant_id) DO UPDATE
             SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
             WHERE outbox_relay_leases.expires_at < now()
                OR outbox_relay_leases.holder = EXCLUDED.holder
             RETURNING holder",
            &[&tenant_id, &holder, &ttl.as_secs_f64()],
        )
        .await?
        .is_some())
}

pub async fn release(pool: &Pool, tenant_id: Uuid, holder: &str) -> PlatformResult<()> {
    pool.get()
        .await?
        .execute(
            "UPDATE outbox_relay_leases SET expires_at = now()
             WHERE tenant_id = $1 AND holder = $2",
            &[&tenant_id, &holder],
        )
        .await?;
    Ok(())
}

pub async fn retire(pool: &Pool, tenant_id: Uuid, holder: &str) -> PlatformResult<()> {
    pool.get()
        .await?
        .execute(
            "DELETE FROM outbox_relay_leases WHERE tenant_id = $1 AND holder = $2",
            &[&tenant_id, &holder],
        )
        .await?;
    Ok(())
}

pub async fn tenants(pool: &Pool) -> PlatformResult<Vec<Uuid>> {
    Ok(pool
        .get()
        .await?
        .query("SELECT tenant_id FROM outbox_relay_leases", &[])
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect())
}
