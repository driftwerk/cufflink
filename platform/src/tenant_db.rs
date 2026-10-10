use crate::repo::tenants::Tenant;
use cufflink_db::{quote_ident, PoolSettings, TlsMode};
use deadpool_postgres::Pool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

pub struct TenantPoolManager {
    base: tokio_postgres::Config,
    direct: tokio_postgres::Config,
    settings: PoolSettings,
    tls: Arc<TlsMode>,
    pools: RwLock<HashMap<Uuid, Arc<TenantPoolEntry>>>,
    create_lock: tokio::sync::Mutex<()>,
    max_pools: usize,
    epoch: Instant,
}

struct TenantPoolEntry {
    pool: Pool,
    tenant_slug: String,
    last_used: AtomicU64,
}

impl TenantPoolManager {
    pub fn new(
        platform_db_url: &str,
        max_pools: usize,
        max_conns_per_tenant: usize,
    ) -> eyre::Result<Self> {
        let settings = PoolSettings::from_env(max_conns_per_tenant)?;
        Ok(Self {
            base: cufflink_db::parse_config(platform_db_url)?,
            direct: cufflink_db::parse_config(platform_db_url)?,
            tls: Arc::new(settings.tls.clone()),
            settings,
            pools: RwLock::new(HashMap::new()),
            create_lock: tokio::sync::Mutex::new(()),
            max_pools,
            epoch: Instant::now(),
        })
    }

    pub fn with_pooler(mut self, pooler: cufflink_db::Pooler) -> Self {
        self.settings = self.settings.pooler(pooler);
        self
    }

    pub fn with_direct_url(mut self, database_url: &str) -> eyre::Result<Self> {
        self.direct = cufflink_db::parse_config(database_url)?;
        Ok(self)
    }

    pub fn pooler(&self) -> cufflink_db::Pooler {
        self.settings.pooler
    }

    pub fn tls(&self) -> Arc<TlsMode> {
        Arc::clone(&self.tls)
    }

    pub fn resets_session_state(&self) -> bool {
        self.settings.pooler.is_transaction_pooler()
            || self.settings.recycling == cufflink_db::Recycling::Clean
    }

    pub fn statement_timeout(&self) -> Option<std::time::Duration> {
        self.settings.statement_timeout
    }

    pub fn managed_db_name(tenant_slug: &str) -> String {
        format!("{MANAGED_PREFIX}{}", tenant_slug.replace('-', "_"))
    }

    pub fn db_name(tenant: &Tenant) -> String {
        tenant
            .database_name
            .clone()
            .unwrap_or_else(|| Self::managed_db_name(&tenant.slug))
    }

    fn tenant_config(&self, tenant: &Tenant) -> tokio_postgres::Config {
        cufflink_db::with_dbname(&self.base, &Self::db_name(tenant))
    }

    pub fn login_config(&self, tenant: &Tenant) -> (tokio_postgres::Config, PoolSettings) {
        let config = match self.settings.pooler {
            cufflink_db::Pooler::Direct => self.tenant_config(tenant),
            cufflink_db::Pooler::PgBouncerTransaction => {
                cufflink_db::with_dbname(&self.direct, &Self::db_name(tenant))
            }
        };
        (
            config,
            self.settings.clone().pooler(cufflink_db::Pooler::Direct),
        )
    }

    pub async fn connect_direct(&self, tenant: &Tenant) -> eyre::Result<tokio_postgres::Client> {
        let (config, settings) = self.direct_config(tenant);
        cufflink_db::connect_single(config, &settings).await
    }

    pub fn direct_config(&self, tenant: &Tenant) -> (tokio_postgres::Config, PoolSettings) {
        self.direct_to(&Self::db_name(tenant))
    }

    fn direct_to(&self, db_name: &str) -> (tokio_postgres::Config, PoolSettings) {
        (
            cufflink_db::with_dbname(&self.direct, db_name),
            self.settings
                .clone()
                .pooler(cufflink_db::Pooler::Direct)
                .statement_timeout(None),
        )
    }

    pub fn validate_adoption(&self, database_name: &str) -> Result<(), String> {
        let own: Vec<&str> = [&self.base, &self.direct]
            .into_iter()
            .filter_map(tokio_postgres::Config::get_dbname)
            .collect();
        validate_adopted_name(database_name, &own)
    }

    fn now_millis(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    pub fn cached(&self, tenant_id: Uuid) -> Option<Pool> {
        let entry = self.read().get(&tenant_id).cloned()?;
        entry.last_used.store(self.now_millis(), Ordering::Relaxed);
        Some(entry.pool.clone())
    }

    pub async fn get_pool(&self, tenant: &Tenant) -> eyre::Result<Pool> {
        let (tenant_id, tenant_slug) = (tenant.id, tenant.slug.as_str());
        if let Some(pool) = self.cached(tenant_id) {
            return Ok(pool);
        }

        let _creating = self.create_lock.lock().await;
        if let Some(pool) = self.cached(tenant_id) {
            return Ok(pool);
        }

        let pool = cufflink_db::build_pool_with(self.tenant_config(tenant), self.settings.clone())?;
        drop(pool.get().await?);

        let cached = {
            let mut pools = self.write();
            if pools.len() >= self.max_pools {
                if let Some(evicted) = lru_victim(pools.iter().map(|(id, e)| (*id, &e.last_used))) {
                    info!("Evicting tenant pool for LRU: {}", evicted);
                    pools.remove(&evicted);
                }
            }
            pools.insert(
                tenant_id,
                Arc::new(TenantPoolEntry {
                    pool: pool.clone(),
                    tenant_slug: tenant_slug.to_string(),
                    last_used: AtomicU64::new(self.now_millis()),
                }),
            );
            pools.len()
        };

        info!(
            "Created tenant pool for '{}' (db: {}, cached: {})",
            tenant_slug,
            Self::db_name(tenant),
            cached
        );
        Ok(pool)
    }

    pub fn pool_statuses(&self) -> Vec<(String, deadpool_postgres::Status)> {
        self.read()
            .values()
            .map(|entry| (entry.tenant_slug.clone(), entry.pool.status()))
            .collect()
    }

    pub async fn create_tenant_db(
        &self,
        platform_pool: &Pool,
        tenant: &Tenant,
    ) -> eyre::Result<TenantDbProvision> {
        if let Some(adopted) = &tenant.database_name {
            return self
                .verify_adoptable(adopted)
                .await
                .map(|()| TenantDbProvision::Adopted);
        }
        let db_name = Self::db_name(tenant);
        let client = platform_pool.get().await?;

        if database_exists(&client, &db_name).await? {
            info!("Tenant database '{}' already exists", db_name);
            return Ok(TenantDbProvision::AlreadyExisted);
        }

        client.execute("SET statement_timeout = 0", &[]).await?;
        client
            .execute(&format!("CREATE DATABASE {}", quote_ident(&db_name)), &[])
            .await?;

        info!("Created tenant database '{}'", db_name);

        let tenant_pool = cufflink_db::build_pool_with(
            self.tenant_config(tenant),
            self.settings.clone().max_size(1),
        )?;
        tenant_pool
            .get()
            .await?
            .execute("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\"", &[])
            .await?;

        info!("Enabled pgcrypto extension in '{}'", db_name);

        Ok(TenantDbProvision::Created)
    }

    pub async fn verify_adoptable(&self, db_name: &str) -> eyre::Result<()> {
        let (config, settings) = self.direct_to(db_name);
        let client = cufflink_db::connect_single(config, &settings)
            .await
            .map_err(|e| {
                eyre::eyre!(
                "cannot connect to database '{db_name}': {e:#}; it must exist, and the platform's \
                 role needs: GRANT CONNECT, CREATE ON DATABASE {} TO <platform role>",
                quote_ident(db_name)
            )
            })?;
        let row = client
            .query_one(
                "SELECT current_user::text, current_setting('server_version_num')::int, \
                        has_database_privilege(current_user, current_database(), 'CREATE')",
                &[],
            )
            .await?;
        let (role, version, may_create): (String, i32, bool) = (row.get(0), row.get(1), row.get(2));
        adoptable(db_name, &role, version, may_create).map_err(|reason| eyre::eyre!(reason))
    }

    pub async fn drop_tenant_db(
        &self,
        platform_pool: &Pool,
        tenant: &Tenant,
    ) -> eyre::Result<DropOutcome> {
        self.write().remove(&tenant.id);
        if tenant.database_name.is_some() {
            info!("Kept adopted tenant database '{}'", Self::db_name(tenant));
            return Ok(DropOutcome::KeptAdopted);
        }

        let db_name = Self::db_name(tenant);
        let client = platform_pool.get().await?;
        if !database_exists(&client, &db_name).await? {
            return Ok(DropOutcome::Absent);
        }
        client
            .execute(
                &format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    quote_ident(&db_name)
                ),
                &[],
            )
            .await?;

        info!("Dropped tenant database '{}'", db_name);
        Ok(DropOutcome::Dropped)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<Uuid, Arc<TenantPoolEntry>>> {
        self.pools
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<Uuid, Arc<TenantPoolEntry>>> {
        self.pools
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantDbProvision {
    Created,
    AlreadyExisted,
    Adopted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropOutcome {
    Dropped,
    KeptAdopted,
    Absent,
}

impl DropOutcome {
    pub fn describe(self) -> &'static str {
        match self {
            DropOutcome::Dropped => "dropped",
            DropOutcome::KeptAdopted => "kept (adopted)",
            DropOutcome::Absent => "absent",
        }
    }
}

const MANAGED_PREFIX: &str = "cufflink_";

const MIN_ADOPTED_SERVER_VERSION: i32 = 130_000;

const SERVER_DATABASES: [&str; 3] = ["postgres", "template0", "template1"];

pub fn validate_adopted_name(database_name: &str, platform: &[&str]) -> Result<(), String> {
    let refuse = |reason: &str| Err(format!("database_name: '{database_name}' {reason}"));
    let is_one_of = |names: &[&str]| {
        names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(database_name))
    };
    if !cufflink_types::manifest::validate::is_identifier(database_name) {
        refuse("is not an identifier (ASCII letters, digits and '_', not starting with a digit)")
    } else if is_one_of(&SERVER_DATABASES) {
        refuse("is a database of the server itself")
    } else if is_one_of(platform) {
        refuse("is the platform's own database")
    } else if database_name
        .to_ascii_lowercase()
        .starts_with(MANAGED_PREFIX)
    {
        refuse("is named like a database the platform creates and drops")
    } else {
        Ok(())
    }
}

fn adoptable(
    db_name: &str,
    role: &str,
    server_version: i32,
    may_create: bool,
) -> Result<(), String> {
    if server_version < MIN_ADOPTED_SERVER_VERSION {
        Err(format!(
            "database '{db_name}' is on Postgres {}; an adopted database needs 13 or newer",
            server_version / 10_000
        ))
    } else if !may_create {
        Err(format!(
            "role '{role}' may not create a schema in database '{db_name}'; run: \
             GRANT CONNECT, CREATE ON DATABASE {} TO {};",
            quote_ident(db_name),
            quote_ident(role)
        ))
    } else {
        Ok(())
    }
}

async fn database_exists(client: &deadpool_postgres::Client, db_name: &str) -> eyre::Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&db_name],
        )
        .await?;
    Ok(row.get(0))
}

pub(crate) fn lru_victim<'a>(entries: impl Iterator<Item = (Uuid, &'a AtomicU64)>) -> Option<Uuid> {
    entries
        .min_by_key(|(_, last_used)| last_used.load(Ordering::Relaxed))
        .map(|(id, _)| id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const URL: &str = "postgres://app:pw@db.internal:6432/platform?sslmode=require";

    fn manager(max_pools: usize) -> TenantPoolManager {
        TenantPoolManager::new(URL, max_pools, 2).expect("valid url")
    }

    fn tenant(slug: &str, database_name: Option<&str>) -> Tenant {
        Tenant {
            database_name: database_name.map(str::to_string),
            ..crate::tenant_catalog::tests::tenant(slug, None)
        }
    }

    fn insert_entry(manager: &TenantPoolManager, tenant_id: Uuid, slug: &str, last_used: u64) {
        let pool = cufflink_db::build_pool_with(
            manager.tenant_config(&tenant(slug, None)),
            manager.settings.clone(),
        )
        .expect("pool builds without connecting");
        manager.write().insert(
            tenant_id,
            Arc::new(TenantPoolEntry {
                pool,
                tenant_slug: slug.to_string(),
                last_used: AtomicU64::new(last_used),
            }),
        );
    }

    #[test]
    fn new_rejects_a_malformed_url() {
        assert!(TenantPoolManager::new("not a url", 8, 2).is_err());
    }

    #[test]
    fn tenant_config_swaps_only_the_database_name() {
        let manager = manager(8);
        let config = manager.tenant_config(&tenant("acme-corp", None));
        assert_eq!(config.get_dbname(), Some("cufflink_acme_corp"));
        assert_eq!(config.get_user(), Some("app"));
        assert_eq!(config.get_ports(), &[6432]);
        assert!(matches!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        ));
    }

    #[test]
    fn a_tenant_has_the_database_it_adopted_or_the_one_named_for_it() {
        assert_eq!(
            TenantPoolManager::managed_db_name("acme-corp"),
            "cufflink_acme_corp"
        );
        assert_eq!(
            TenantPoolManager::db_name(&tenant("acme-corp", None)),
            "cufflink_acme_corp"
        );
        assert_eq!(
            TenantPoolManager::db_name(&tenant("harbor", Some("harbor"))),
            "harbor"
        );
    }

    #[test]
    fn a_direct_connection_bypasses_the_pooler() {
        let manager = manager(8)
            .with_pooler(cufflink_db::Pooler::PgBouncerTransaction)
            .with_direct_url("postgres://app:pw@db.internal:5432/platform")
            .expect("valid url");
        let (config, settings) = manager.direct_config(&tenant("harbor", Some("harbor")));
        assert_eq!(config.get_dbname(), Some("harbor"));
        assert_eq!(config.get_ports(), &[5432]);
        assert_eq!(settings.pooler, cufflink_db::Pooler::Direct);
        assert_eq!(
            manager
                .tenant_config(&tenant("harbor", Some("harbor")))
                .get_ports(),
            &[6432]
        );
    }

    #[test]
    fn a_login_as_a_service_role_reaches_the_server_with_the_pools_timeout() {
        let pooled = manager(8)
            .with_pooler(cufflink_db::Pooler::PgBouncerTransaction)
            .with_direct_url("postgres://app:pw@db.internal:5432/platform")
            .expect("valid url");
        let (config, settings) = pooled.login_config(&tenant("acme", None));
        assert_eq!(config.get_dbname(), Some("cufflink_acme"));
        assert_eq!(config.get_ports(), &[5432], "never the pooler");
        assert_eq!(settings.pooler, cufflink_db::Pooler::Direct);
        assert_eq!(
            settings.statement_timeout, pooled.settings.statement_timeout,
            "bounded as the tenant's own pools are"
        );
        assert!(settings.statement_timeout.is_some());
        let direct = manager(8)
            .with_direct_url("postgres://app:pw@db.internal:5432/platform")
            .expect("valid url");
        assert_eq!(
            direct.login_config(&tenant("acme", None)).0.get_ports(),
            &[6432],
            "without a pooler it logs in where the tenant's pools connect"
        );
    }

    #[test]
    fn what_a_session_sets_is_reset_by_a_clean_recycle_or_never_set_behind_a_pooler() {
        let with = |recycling, pooler| {
            let mut manager = manager(8).with_pooler(pooler);
            manager.settings.recycling = recycling;
            manager.resets_session_state()
        };
        assert!(with(
            cufflink_db::Recycling::Clean,
            cufflink_db::Pooler::Direct
        ));
        for recycling in [
            cufflink_db::Recycling::Fast,
            cufflink_db::Recycling::Verified,
        ] {
            assert!(
                !with(recycling, cufflink_db::Pooler::Direct),
                "{recycling:?}"
            );
            assert!(with(recycling, cufflink_db::Pooler::PgBouncerTransaction));
        }
    }

    #[test]
    fn only_a_database_the_platform_would_never_drop_is_adopted() {
        let platform = ["cufflink", "platform"];
        assert_eq!(validate_adopted_name("harbor", &platform), Ok(()));
        assert_eq!(validate_adopted_name("Harbor_2", &platform), Ok(()));
        for (name, reason) in [
            ("cufflink", "is the platform's own database"),
            ("Cufflink", "is the platform's own database"),
            ("platform", "is the platform's own database"),
            ("Postgres", "is a database of the server itself"),
            ("template0", "is a database of the server itself"),
            (
                "Cufflink_acme",
                "is named like a database the platform creates and drops",
            ),
            (
                "cufflink_acme",
                "is named like a database the platform creates and drops",
            ),
            (
                "cufflink_",
                "is named like a database the platform creates and drops",
            ),
            ("postgres", "is a database of the server itself"),
            ("template1", "is a database of the server itself"),
            ("my-db", "is not an identifier"),
            ("", "is not an identifier"),
            ("a\"; DROP DATABASE x; --", "is not an identifier"),
        ] {
            let refused = validate_adopted_name(name, &platform).expect_err(name);
            assert!(
                refused.starts_with(&format!("database_name: '{name}' {reason}")),
                "{refused}"
            );
        }
    }

    #[test]
    fn the_platforms_own_database_is_the_one_it_connects_to() {
        let manager = manager(8)
            .with_direct_url("postgres://app:pw@db.internal:5432/control?sslmode=require")
            .expect("direct url");
        for own in ["platform", "control"] {
            assert_eq!(
                manager.validate_adoption(own),
                Err(format!(
                    "database_name: '{own}' is the platform's own database"
                ))
            );
        }
        assert_eq!(manager.validate_adoption("harbor"), Ok(()));
    }

    #[test]
    fn an_adopted_database_must_let_the_platform_create_its_schema() {
        assert_eq!(adoptable("harbor", "cufflink", 160_004, true), Ok(()));
        assert_eq!(
            adoptable("harbor", "cufflink", 160_004, false),
            Err(
                "role 'cufflink' may not create a schema in database 'harbor'; run: \
                 GRANT CONNECT, CREATE ON DATABASE \"harbor\" TO \"cufflink\";"
                    .to_string()
            )
        );
        assert_eq!(
            adoptable("harbor", "cufflink", 120_011, true),
            Err(
                "database 'harbor' is on Postgres 12; an adopted database needs 13 or newer"
                    .to_string()
            )
        );
    }

    #[test]
    fn a_drop_outcome_reads_as_the_response_says_it() {
        assert_eq!(DropOutcome::Dropped.describe(), "dropped");
        assert_eq!(DropOutcome::KeptAdopted.describe(), "kept (adopted)");
        assert_eq!(DropOutcome::Absent.describe(), "absent");
    }

    #[test]
    fn lru_victim_is_the_smallest_last_used() {
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let (ta, tb, tc) = (AtomicU64::new(50), AtomicU64::new(5), AtomicU64::new(500));
        assert_eq!(
            lru_victim([(a, &ta), (b, &tb), (c, &tc)].into_iter()),
            Some(b)
        );
        assert_eq!(lru_victim(std::iter::empty()), None);
    }

    #[tokio::test]
    async fn hit_path_takes_only_the_read_lock_and_touches_last_used() {
        let manager = Arc::new(manager(8));
        let tenant_id = Uuid::new_v4();
        insert_entry(&manager, tenant_id, "acme", u64::MAX);

        let hold = Duration::from_millis(500);
        let reader = std::thread::spawn({
            let manager = Arc::clone(&manager);
            move || {
                let _guard = manager.read();
                std::thread::sleep(hold);
            }
        });
        std::thread::sleep(Duration::from_millis(50));
        let started = Instant::now();
        let pool = manager
            .get_pool(&Tenant {
                id: tenant_id,
                ..tenant("acme", None)
            })
            .await
            .expect("cached pool");
        assert!(
            started.elapsed() < hold,
            "a cache hit waited for the reader to release the lock"
        );
        assert_eq!(pool.status().max_size, 2);
        reader.join().expect("reader thread");

        let touched = manager.read()[&tenant_id].last_used.load(Ordering::Relaxed);
        assert_ne!(touched, u64::MAX, "a hit refreshes last_used");
        assert_eq!(manager.pool_statuses().len(), 1);
        assert_eq!(manager.pool_statuses()[0].0, "acme");
    }
}
