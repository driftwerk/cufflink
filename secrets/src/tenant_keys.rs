use crate::cipher::Key;
use crate::envelope::DataKey;
use crate::{KeyRing, SecretError};
use deadpool_postgres::GenericClient;
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

const ACTIVE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Durability {
    Committed,
    Pending,
}

#[derive(Default)]
pub(crate) struct TenantKeys {
    keys: RwLock<HashMap<Uuid, Arc<DataKey>>>,
    active: RwLock<HashMap<Uuid, (Uuid, Instant)>>,
}

pub(crate) fn unwrapped(
    ring: &KeyRing,
    tenant_id: Uuid,
    row: &tokio_postgres::Row,
) -> Result<DataKey, SecretError> {
    let id: Uuid = row.get("key_id");
    ring.unwrap(row.get("kek_id"), tenant_id, id, row.get("wrapped_dek"))
        .map(|key| DataKey { id, tenant_id, key })
}

async fn load_active(
    client: &impl GenericClient,
    ring: &KeyRing,
    tenant_id: Uuid,
) -> Result<Option<DataKey>, SecretError> {
    client
        .query_opt(
            "SELECT key_id, kek_id, wrapped_dek FROM tenant_data_keys
             WHERE tenant_id = $1 AND active",
            &[&tenant_id],
        )
        .await?
        .map(|row| unwrapped(ring, tenant_id, &row))
        .transpose()
}

async fn insert_active(
    client: &impl GenericClient,
    ring: &KeyRing,
    tenant_id: Uuid,
) -> Result<(), SecretError> {
    let id = Uuid::new_v4();
    let (kek_id, wrapped) = ring.wrap(tenant_id, id, &Key::generate()?)?;
    client
        .execute(
            "INSERT INTO tenant_data_keys (key_id, tenant_id, kek_id, wrapped_dek, active)
             VALUES ($1, $2, $3, $4, true)
             ON CONFLICT (tenant_id) WHERE active DO NOTHING",
            &[&id, &tenant_id, &kek_id, &wrapped],
        )
        .await?;
    Ok(())
}

impl TenantKeys {
    fn remembered(&self, tenant_id: Uuid, key_id: Uuid) -> Option<Arc<DataKey>> {
        self.keys
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key_id)
            .filter(|key| key.tenant_id == tenant_id)
            .cloned()
    }

    fn remembered_active(&self, tenant_id: Uuid) -> Option<Arc<DataKey>> {
        let (key_id, seen) = self
            .active
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant_id)
            .copied()?;
        (seen.elapsed() < ACTIVE_TTL)
            .then(|| self.remembered(tenant_id, key_id))
            .flatten()
    }

    fn remember(&self, key: DataKey, durability: Durability) -> Arc<DataKey> {
        let key = Arc::new(key);
        if durability == Durability::Committed {
            self.keys
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key.id, key.clone());
        }
        key
    }

    fn remember_active(&self, key: DataKey, durability: Durability) -> Arc<DataKey> {
        let key = self.remember(key, durability);
        if durability == Durability::Committed {
            self.active
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key.tenant_id, (key.id, Instant::now()));
        }
        key
    }

    pub(crate) fn forget_active(&self, tenant_id: Uuid) {
        self.active
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&tenant_id);
    }

    pub(crate) async fn active(
        &self,
        client: &impl GenericClient,
        durability: Durability,
        ring: &KeyRing,
        tenant_id: Uuid,
    ) -> Result<Arc<DataKey>, SecretError> {
        if let Some(key) = self.remembered_active(tenant_id) {
            return Ok(key);
        }
        if !ring.has_key() {
            return Err(SecretError::NoKey);
        }
        let key = match load_active(client, ring, tenant_id).await? {
            Some(key) => key,
            None => {
                insert_active(client, ring, tenant_id).await?;
                load_active(client, ring, tenant_id)
                    .await?
                    .ok_or(SecretError::Malformed(
                        "the tenant's data key vanished as it was made",
                    ))?
            }
        };
        Ok(self.remember_active(key, durability))
    }

    pub(crate) async fn active_id(
        client: &impl GenericClient,
        tenant_id: Uuid,
    ) -> Result<Option<Uuid>, SecretError> {
        Ok(client
            .query_opt(
                "SELECT key_id FROM tenant_data_keys WHERE tenant_id = $1 AND active",
                &[&tenant_id],
            )
            .await?
            .map(|row| row.get(0)))
    }

    pub(crate) async fn get(
        &self,
        client: &impl GenericClient,
        durability: Durability,
        ring: &KeyRing,
        tenant_id: Uuid,
        key_id: Uuid,
    ) -> Result<Arc<DataKey>, SecretError> {
        if let Some(key) = self.remembered(tenant_id, key_id) {
            return Ok(key);
        }
        let row = client
            .query_opt(
                "SELECT key_id, kek_id, wrapped_dek FROM tenant_data_keys
                 WHERE key_id = $1 AND tenant_id = $2",
                &[&key_id, &tenant_id],
            )
            .await?
            .ok_or(SecretError::UnknownDataKey { tenant_id, key_id })?;
        unwrapped(ring, tenant_id, &row).map(|key| self.remember(key, durability))
    }

    pub(crate) async fn rotate(
        tx: &deadpool_postgres::Transaction<'_>,
        ring: &KeyRing,
        tenant_id: Uuid,
    ) -> Result<(), SecretError> {
        tx.execute(
            "UPDATE tenant_data_keys SET active = false WHERE tenant_id = $1 AND active",
            &[&tenant_id],
        )
        .await?;
        insert_active(tx, ring, tenant_id).await
    }

    pub(crate) async fn rewrap(
        tx: &deadpool_postgres::Transaction<'_>,
        ring: &KeyRing,
        tenant_id: Option<Uuid>,
        limit: i64,
    ) -> Result<u64, SecretError> {
        let current = ring.current_id().ok_or(SecretError::NoKey)?;
        let rows = tx
            .query(
                "SELECT key_id, tenant_id, kek_id, wrapped_dek FROM tenant_data_keys
                 WHERE kek_id <> $1 AND ($2::uuid IS NULL OR tenant_id = $2)
                 ORDER BY key_id LIMIT $3 FOR UPDATE",
                &[&current, &tenant_id, &limit],
            )
            .await?;
        for row in &rows {
            let tenant_id: Uuid = row.get("tenant_id");
            let key = unwrapped(ring, tenant_id, row)?;
            let (kek_id, wrapped) = ring.wrap(tenant_id, key.id, &key.key)?;
            tx.execute(
                "UPDATE tenant_data_keys SET kek_id = $1, wrapped_dek = $2 WHERE key_id = $3",
                &[&kek_id, &wrapped, &key.id],
            )
            .await?;
        }
        Ok(rows.len() as u64)
    }
}
