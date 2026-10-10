use crate::db_roles::turns::Turns;
use crate::db_roles::verify::Probe;
use crate::tenant_db::lru_victim;
use cufflink_db::PoolSettings;
use cufflink_secrets::SecretString;
use deadpool_postgres::{Pool, PoolError, Status};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

const INVALID_AUTHORIZATION: &str = "28";

const POOLS_PER_TENANT: usize = 32;

const GONE: Status = Status {
    max_size: 0,
    size: 0,
    available: 0,
    waiting: 0,
};

pub fn login_config(
    base: &tokio_postgres::Config,
    role: &str,
    password: &SecretString,
) -> tokio_postgres::Config {
    let mut config = base.clone();
    config.user(role).password(password.expose());
    config
}

fn refusal(error: &tokio_postgres::Error) -> bool {
    error
        .code()
        .is_some_and(|code| code.code().starts_with(INVALID_AUTHORIZATION))
}

pub async fn probe(config: tokio_postgres::Config, settings: &PoolSettings) -> Probe {
    let attempt = async {
        cufflink_db::connect_single(config, settings)
            .await?
            .query_one("SELECT current_user", &[])
            .await
            .map_err(eyre::Report::from)
    };
    match attempt.await {
        Ok(_) => Probe::Accepted,
        Err(e) => match e.downcast_ref::<tokio_postgres::Error>() {
            Some(error) if refusal(error) => {
                Probe::Refused(crate::db_error::from_pg(error).message.clone())
            }
            _ => Probe::Unanswered(format!("{e:#}")),
        },
    }
}

pub fn login_refused(error: &PoolError) -> bool {
    match error {
        PoolError::Backend(e) => refusal(e),
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct Login {
    pub tenant_id: Uuid,
    pub service_id: Uuid,
    pub tenant_slug: String,
    pub service_name: String,
    pub role: String,
    pub base: tokio_postgres::Config,
    pub settings: PoolSettings,
}

impl Login {
    pub fn label(&self) -> String {
        format!("login:{}:{}", self.tenant_slug, self.service_name)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoginError {
    #[error("the login pool could not be built: {0}")]
    Build(String),
    #[error(transparent)]
    Checkout(#[from] PoolError),
}

struct Entry {
    pool: Pool,
    label: String,
    sealed: Vec<u8>,
    last_used: AtomicU64,
}

pub struct LoginPools {
    pools: RwLock<HashMap<Uuid, Arc<Entry>>>,
    creating: Turns<Uuid>,
    max_pools: usize,
    epoch: Instant,
}

impl LoginPools {
    pub fn new(max_tenant_pools: usize) -> Self {
        Self {
            pools: RwLock::default(),
            creating: Turns::default(),
            max_pools: max_tenant_pools.saturating_mul(POOLS_PER_TENANT).max(1),
            epoch: Instant::now(),
        }
    }

    fn now_millis(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    pub fn cached(&self, login: &Login, sealed: &[u8]) -> Option<Pool> {
        let entry = self
            .pools
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&login.service_id)
            .filter(|entry| entry.sealed == sealed)
            .cloned()?;
        entry.last_used.store(self.now_millis(), Ordering::Relaxed);
        if entry.pool.status().max_size != login.settings.max_size {
            entry.pool.resize(login.settings.max_size);
        }
        Some(entry.pool.clone())
    }

    pub async fn open(
        &self,
        login: &Login,
        sealed: &[u8],
        password: &SecretString,
    ) -> Result<Pool, LoginError> {
        let _creating = self.creating.take(&login.service_id).await;
        if let Some(pool) = self.cached(login, sealed) {
            return Ok(pool);
        }
        let pool = cufflink_db::build_pool_with(
            login_config(&login.base, &login.role, password),
            login.settings.clone(),
        )
        .map_err(|e| LoginError::Build(e.to_string()))?;
        drop(pool.get().await?);
        let (kept, evicted) = {
            let mut pools = self.pools.write().unwrap_or_else(PoisonError::into_inner);
            let evicted =
                match !pools.contains_key(&login.service_id) && pools.len() >= self.max_pools {
                    true => lru_victim(pools.iter().map(|(id, e)| (*id, &e.last_used)))
                        .and_then(|victim| pools.remove(&victim)),
                    false => None,
                };
            pools.insert(
                login.service_id,
                Arc::new(Entry {
                    pool: pool.clone(),
                    label: login.label(),
                    sealed: sealed.to_vec(),
                    last_used: AtomicU64::new(self.now_millis()),
                }),
            );
            (pools.len(), evicted)
        };
        if let Some(evicted) = evicted {
            cufflink_db::record_status_gauges(&evicted.label, &GONE);
        }
        info!(
            tenant = %login.tenant_slug,
            service = %login.service_name,
            role = %login.role,
            max_size = login.settings.max_size,
            kept,
            "Created login pool"
        );
        Ok(pool)
    }

    pub fn statuses(&self) -> Vec<(String, Status)> {
        self.pools
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|entry| (entry.label.clone(), entry.pool.status()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "postgres://cufflink:pw@db:5432/cufflink_acme";

    fn login(service_id: Uuid, max_size: usize) -> Login {
        Login {
            tenant_id: Uuid::nil(),
            service_id,
            tenant_slug: "acme".into(),
            service_name: "loan-service".into(),
            role: "svc_acme_loan_service".into(),
            base: cufflink_db::parse_config(URL).expect("url"),
            settings: PoolSettings::from_vars(max_size, |_| None).expect("settings"),
        }
    }

    fn keep(pools: &LoginPools, login: &Login, sealed: &[u8], last_used: u64) {
        let pool = cufflink_db::build_pool_with(
            login_config(&login.base, &login.role, &SecretString::new("pw")),
            login.settings.clone(),
        )
        .expect("pool");
        pools.pools.write().expect("pools").insert(
            login.service_id,
            Arc::new(Entry {
                pool,
                label: login.label(),
                sealed: sealed.to_vec(),
                last_used: AtomicU64::new(last_used),
            }),
        );
    }

    #[test]
    fn a_login_config_changes_only_who_connects() {
        let base = cufflink_db::parse_config(URL).expect("url");
        let config = login_config(&base, "svc_acme_notes", &SecretString::new("s3cret"));
        assert_eq!(config.get_user(), Some("svc_acme_notes"));
        assert_eq!(config.get_password(), Some(&b"s3cret"[..]));
        assert_eq!(config.get_dbname(), Some("cufflink_acme"));
        assert_eq!(config.get_hosts().len(), 1);
    }

    #[tokio::test]
    async fn a_pool_is_the_services_while_its_password_is_the_one_on_file() {
        let pools = LoginPools::new(1);
        let service = login(Uuid::new_v4(), 4);
        assert!(pools.cached(&service, b"sealed-1").is_none());
        keep(&pools, &service, b"sealed-1", 0);
        assert!(pools.cached(&service, b"sealed-1").is_some());
        assert!(
            pools.cached(&service, b"sealed-2").is_none(),
            "a pool made before a rotation is not the one to log in through"
        );
        let labels: Vec<String> = pools
            .statuses()
            .into_iter()
            .map(|(label, _)| label)
            .collect();
        assert_eq!(labels, ["login:acme:loan-service"]);
        let resized = pools
            .cached(&login(service.service_id, 2), b"sealed-1")
            .expect("pool");
        assert_eq!(
            resized.status().max_size,
            2,
            "the pool follows login_pool_max"
        );
    }

    #[test]
    fn only_a_refused_login_asks_for_the_password_again() {
        assert!(!login_refused(&PoolError::Closed));
        assert!(!login_refused(&PoolError::NoRuntimeSpecified));
    }

    #[test]
    fn there_is_room_for_the_services_of_every_tenant_pool() {
        assert_eq!(LoginPools::new(64).max_pools, 64 * POOLS_PER_TENANT);
        assert_eq!(LoginPools::new(0).max_pools, 1);
    }
}
