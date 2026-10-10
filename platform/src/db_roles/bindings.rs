use crate::repo::service_db_roles::{self, Binding};
use deadpool_postgres::{Pool, PoolError};
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};
use tracing::warn;
use uuid::Uuid;

const FRESH_FOR: Duration = Duration::from_secs(30);
const HELD_MAX: usize = 4_096;

struct Held {
    binding: Option<Arc<Binding>>,
    read_at: Instant,
}

fn fresh(held: &Held, now: Instant) -> bool {
    now.saturating_duration_since(held.read_at) < FRESH_FOR
}

pub struct Bindings {
    db: Pool,
    held: RwLock<HashMap<Uuid, Held>>,
}

impl Bindings {
    pub fn new(db: Pool) -> Self {
        Self {
            db,
            held: RwLock::default(),
        }
    }

    fn remembered(&self, service_id: Uuid) -> Option<(Option<Arc<Binding>>, bool)> {
        self.held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&service_id)
            .map(|held| (held.binding.clone(), fresh(held, Instant::now())))
    }

    fn remember(&self, service_id: Uuid, binding: Option<Arc<Binding>>) {
        let now = Instant::now();
        let mut held = self.held.write().unwrap_or_else(PoisonError::into_inner);
        if held.len() >= HELD_MAX {
            held.retain(|_, held| fresh(held, now));
        }
        held.insert(
            service_id,
            Held {
                binding,
                read_at: now,
            },
        );
    }

    async fn read(&self, service_id: Uuid) -> Result<Option<Binding>, PoolError> {
        service_db_roles::get(&self.db.get().await?, service_id)
            .await
            .map_err(PoolError::Backend)
    }

    pub async fn get(&self, service_id: Uuid) -> Result<Option<Arc<Binding>>, PoolError> {
        let remembered = self.remembered(service_id);
        if let Some((binding, true)) = remembered {
            return Ok(binding);
        }
        match (self.read(service_id).await, remembered) {
            (Ok(binding), _) => {
                let binding = binding.map(Arc::new);
                self.remember(service_id, binding.clone());
                Ok(binding)
            }
            (Err(e), Some((binding, _))) => {
                warn!(service_id = %service_id, error = %e, "The service's database role could not be read again; the one last read stays in use");
                Ok(binding)
            }
            (Err(e), None) => Err(e),
        }
    }

    pub fn forget(&self, service_id: Uuid) {
        self.held
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&service_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_was_read_is_fresh_for_half_a_minute() {
        let read_at = Instant::now();
        let held = Held {
            binding: None,
            read_at,
        };
        assert!(fresh(&held, read_at));
        assert!(fresh(&held, read_at + Duration::from_secs(29)));
        assert!(!fresh(&held, read_at + FRESH_FOR));
    }

    #[tokio::test]
    async fn a_binding_forgotten_is_asked_for_again() {
        let pool = cufflink_db::build_pool_with(
            cufflink_db::parse_config("postgres://cufflink:pw@127.0.0.1:1/cufflink").expect("url"),
            cufflink_db::PoolSettings::from_vars(1, |_| None).expect("settings"),
        )
        .expect("pool");
        let bindings = Bindings::new(pool);
        let service = Uuid::new_v4();
        bindings.remember(service, None);
        assert!(matches!(bindings.get(service).await, Ok(None)));
        bindings.forget(service);
        assert!(
            bindings.get(service).await.is_err(),
            "nothing is remembered and the database is not there"
        );
    }
}
