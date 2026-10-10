use crate::errors::PlatformResult;
use crate::shutdown::Shutdown;
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;

pub const WEBHOOKS_CHANGED_SUBJECT: &str = "cufflink.webhooks.changed";

const RELOAD_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    pub id: Uuid,
    pub service_id: Uuid,
    pub table: String,
    pub event: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Snapshot(HashMap<Uuid, Vec<Registered>>);

impl Snapshot {
    pub fn of(registered: impl IntoIterator<Item = Registered>) -> Self {
        Self(
            registered
                .into_iter()
                .fold(HashMap::new(), |mut by_service, webhook| {
                    by_service
                        .entry(webhook.service_id)
                        .or_insert_with(Vec::new)
                        .push(webhook);
                    by_service
                }),
        )
    }

    pub fn matching(&self, service_id: Uuid, table: &str, event: &str) -> Vec<Uuid> {
        self.0
            .get(&service_id)
            .into_iter()
            .flatten()
            .filter(|webhook| webhook.table == table && webhook.event == event)
            .map(|webhook| webhook.id)
            .collect()
    }
}

#[derive(Clone, Default)]
pub struct WebhookIndex {
    current: Arc<RwLock<Option<Arc<Snapshot>>>>,
}

impl WebhookIndex {
    fn snapshot(&self) -> Option<Arc<Snapshot>> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn set(&self, snapshot: Snapshot) -> Arc<Snapshot> {
        let snapshot = Arc::new(snapshot);
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&snapshot));
        snapshot
    }

    pub async fn reload(&self, db: &deadpool_postgres::Pool) -> PlatformResult<Arc<Snapshot>> {
        let client = db.get().await?;
        let registered = crate::repo::webhooks::list_active_targets(&client).await?;
        Ok(self.set(Snapshot::of(registered)))
    }

    pub async fn matching(
        &self,
        db: &deadpool_postgres::Pool,
        service_id: Uuid,
        table: &str,
        event: &str,
    ) -> PlatformResult<Vec<Uuid>> {
        let snapshot = match self.snapshot() {
            Some(snapshot) => snapshot,
            None => self.reload(db).await?,
        };
        Ok(snapshot.matching(service_id, table, event))
    }

    async fn refresh(&self, db: &deadpool_postgres::Pool) {
        if self.snapshot().is_none() {
            return;
        }
        if let Err(e) = self.reload(db).await {
            warn!(error = %e, "Could not read the webhooks; writes use the last reading");
        }
    }

    pub async fn changed(&self, db: &deadpool_postgres::Pool, nats: &async_nats::Client) {
        self.refresh(db).await;
        crate::nats_conn::publish_change(nats, WEBHOOKS_CHANGED_SUBJECT).await;
    }

    pub fn start_sync(
        &self,
        shutdown: &Shutdown,
        nats: async_nats::Client,
        db: deadpool_postgres::Pool,
    ) {
        let index = self.clone();
        crate::nats_conn::spawn_reload_loop(
            shutdown,
            "webhook_index_sync",
            nats,
            WEBHOOKS_CHANGED_SUBJECT,
            RELOAD_INTERVAL,
            move || {
                let (index, db) = (index.clone(), db.clone());
                async move { index.refresh(&db).await }
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registered(service: u128, table: &str, event: &str, id: u128) -> Registered {
        Registered {
            id: Uuid::from_u128(id),
            service_id: Uuid::from_u128(service),
            table: table.into(),
            event: event.into(),
        }
    }

    #[test]
    fn an_event_has_the_webhooks_registered_for_it_and_no_other() {
        let snapshot = Snapshot::of([
            registered(1, "orders", "create", 10),
            registered(1, "orders", "update", 11),
            registered(1, "notes", "create", 12),
            registered(2, "orders", "create", 13),
            registered(1, "orders", "create", 14),
        ]);
        let ids = |service: u128, table: &str, event: &str| -> Vec<u128> {
            snapshot
                .matching(Uuid::from_u128(service), table, event)
                .iter()
                .map(Uuid::as_u128)
                .collect()
        };
        assert_eq!(ids(1, "orders", "create"), [10, 14]);
        assert_eq!(ids(1, "orders", "update"), [11]);
        assert_eq!(ids(2, "orders", "create"), [13]);
        assert!(ids(1, "orders", "delete").is_empty());
        assert!(ids(3, "orders", "create").is_empty());
        assert_eq!(ids(1, "notes", "create"), [12]);
    }

    #[test]
    fn nothing_is_read_before_a_write_asks() {
        let index = WebhookIndex::default();
        assert!(index.snapshot().is_none());
        index.set(Snapshot::of([registered(1, "orders", "create", 10)]));
        assert_eq!(
            index.snapshot().map(|snapshot| snapshot
                .matching(Uuid::from_u128(1), "orders", "create")
                .len()),
            Some(1)
        );
    }
}
