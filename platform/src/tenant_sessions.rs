use crate::db_roles::bindings::Bindings;
use crate::db_roles::fallback::Fallback;
use crate::db_roles::login_pools::{login_config, login_refused, Login, LoginError, LoginPools};
use crate::db_roles::verify;
use crate::errors::PlatformError;
use crate::policy::config::DbRolesConfig;
use crate::policy::record::{LogThrottle, LOG_THROTTLE};
use crate::policy::{PolicyKey, PolicyMode, Violation};
use crate::registry::ServiceIdentity;
use crate::repo::service_db_roles::Binding;
use crate::repo::tenants::Tenant;
use crate::secrets::{SecretBox, SecretString};
use crate::tenant_catalog::{TenantCatalog, TenantEntry};
use crate::tenant_db::{DropOutcome, TenantDbProvision, TenantPoolManager};
use async_trait::async_trait;
use cufflink_db::TlsMode;
use cufflink_pgcodec::session;
use cufflink_types::manifest::validate::folded_ident;
use deadpool_postgres::{GenericClient, Object, Pool, PoolError, Timeouts};
use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;
use tokio_postgres::types::ToSql;
use tokio_postgres::{CancelToken, Error, Row, RowStream, Statement, ToStatement};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const FEATURES: &[&str] = &["secret_encryption", "db_roles"];

pub const UNBOUND: &str = "no_verified_binding";

static UNBOUND_RECORDED: LazyLock<LogThrottle<Uuid>> =
    LazyLock::new(|| LogThrottle::new(LOG_THROTTLE));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Purpose {
    Guest,
    Crud,
    Ddl,
    Platform,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleMode {
    Platform,
    SetLocal { role: String, fallback: bool },
    Login { role: String },
}

impl RoleMode {
    pub fn role(&self) -> Option<&str> {
        match self {
            RoleMode::Platform => None,
            RoleMode::SetLocal { role, .. } | RoleMode::Login { role } => Some(role),
        }
    }

    fn imposed(&self) -> Option<&str> {
        match self {
            RoleMode::SetLocal { role, .. } => Some(role),
            RoleMode::Platform | RoleMode::Login { .. } => None,
        }
    }
}

pub use cufflink_pgcodec::session::Scope;

#[derive(Debug, thiserror::Error)]
pub enum AcquireError {
    #[error("tenant {0} does not exist")]
    UnknownTenant(Uuid),
    #[error("{0}")]
    Unavailable(String),
    #[error("connection checkout failed: {0}")]
    Checkout(#[source] PoolError),
    #[error(
        "service {0} has no database role; `cufflink services db-role verify` says what it lacks"
    )]
    NoRole(String),
}

impl AcquireError {
    pub fn reason(&self) -> String {
        match self {
            AcquireError::Checkout(e) => e.to_string(),
            other => other.to_string(),
        }
    }
}

impl From<AcquireError> for PlatformError {
    fn from(error: AcquireError) -> Self {
        match error {
            AcquireError::Checkout(e) => PlatformError::Pool(e),
            AcquireError::NoRole(_) => PlatformError::Unavailable(error.to_string()),
            other => PlatformError::Internal(format!("Tenant pool error: {other}")),
        }
    }
}

enum Source {
    Tenants {
        pools: Box<TenantPoolManager>,
        catalog: TenantCatalog,
    },
    Fixed {
        pool: Pool,
        pooler: cufflink_db::Pooler,
    },
}

struct Roles {
    platform_db: Pool,
    bindings: Bindings,
    logins: LoginPools,
    secrets: SecretBox,
}

impl Roles {
    async fn binding(&self, service_id: Uuid) -> Result<Option<Arc<Binding>>, AcquireError> {
        self.bindings.get(service_id).await.map_err(|e| {
            AcquireError::Unavailable(format!(
                "the service's database role could not be read: {e}"
            ))
        })
    }

    async fn sealed(&self, login: &Login) -> Result<(Arc<Binding>, Vec<u8>), AcquireError> {
        let unbound = || AcquireError::NoRole(login.service_name.clone());
        let binding = self
            .binding(login.service_id)
            .await?
            .filter(|binding| binding.verified_at.is_some())
            .ok_or_else(unbound)?;
        let sealed = binding.password_enc.clone().ok_or_else(unbound)?;
        Ok((binding, sealed))
    }

    async fn password(
        &self,
        login: &Login,
        binding: &Binding,
    ) -> Result<SecretString, AcquireError> {
        let platform = self.platform_db.get().await.map_err(|e| {
            AcquireError::Unavailable(format!("the platform's database is not reachable: {e}"))
        })?;
        verify::password(&self.secrets, &platform, login.tenant_id, binding)
            .await
            .map_err(|e| {
                AcquireError::Unavailable(format!(
                    "the password of the service's database role could not be opened: {e}"
                ))
            })?
            .ok_or_else(|| AcquireError::NoRole(login.service_name.clone()))
    }

    async fn pool(&self, login: &Login) -> Result<Pool, AcquireError> {
        let (binding, sealed) = self.sealed(login).await?;
        if let Some(pool) = self.logins.cached(login, &sealed) {
            return Ok(pool);
        }
        let password = self.password(login, &binding).await?;
        self.logins
            .open(login, &sealed, &password)
            .await
            .map_err(|e| match e {
                LoginError::Checkout(e) => AcquireError::Checkout(e),
                build => AcquireError::Unavailable(build.to_string()),
            })
    }

    async fn connect(&self, login: &Login) -> Result<tokio_postgres::Client, AcquireError> {
        self.bindings.forget(login.service_id);
        let (binding, _) = self.sealed(login).await?;
        let password = self.password(login, &binding).await?;
        cufflink_db::connect_single(
            login_config(&login.base, &login.role, &password),
            &login.settings,
        )
        .await
        .map_err(|e| AcquireError::Unavailable(format!("{e:#}")))
    }

    async fn pool_again(&self, login: &Login) -> Result<Pool, AcquireError> {
        self.bindings.forget(login.service_id);
        self.pool(login).await
    }

    async fn login_pool(&self, login: &Login) -> Result<Pool, AcquireError> {
        match self.pool(login).await {
            Err(AcquireError::Checkout(e)) if login_refused(&e) => self.pool_again(login).await,
            outcome => outcome,
        }
    }
}

pub struct TenantSessions {
    source: Source,
    roles: Option<Arc<Roles>>,
}

impl TenantSessions {
    pub fn new(pools: TenantPoolManager, catalog: TenantCatalog) -> Self {
        Self {
            source: Source::Tenants {
                pools: Box::new(pools),
                catalog,
            },
            roles: None,
        }
    }

    pub fn over_pool(pool: Pool, pooler: cufflink_db::Pooler) -> Self {
        Self {
            source: Source::Fixed { pool, pooler },
            roles: None,
        }
    }

    pub fn with_roles(
        mut self,
        platform_db: Pool,
        secrets: SecretBox,
        max_tenant_pools: usize,
    ) -> Self {
        self.roles = Some(Arc::new(Roles {
            bindings: Bindings::new(platform_db.clone()),
            platform_db,
            logins: LoginPools::new(max_tenant_pools),
            secrets,
        }));
        self
    }

    fn databases(&self) -> Option<&TenantPoolManager> {
        match &self.source {
            Source::Tenants { pools, .. } => Some(pools),
            Source::Fixed { .. } => None,
        }
    }

    fn require_databases(&self) -> eyre::Result<&TenantPoolManager> {
        self.databases()
            .ok_or_else(|| eyre::eyre!("a fixed pool has no tenant databases"))
    }

    pub async fn create_database(
        &self,
        platform: &Pool,
        tenant: &Tenant,
    ) -> eyre::Result<TenantDbProvision> {
        self.require_databases()?
            .create_tenant_db(platform, tenant)
            .await
    }

    pub fn validate_adoption(&self, database_name: &str) -> Result<(), String> {
        self.require_databases()
            .map_err(|e| e.to_string())?
            .validate_adoption(database_name)
    }

    pub async fn verify_adoptable(&self, database_name: &str) -> eyre::Result<()> {
        self.require_databases()?
            .verify_adoptable(database_name)
            .await
    }

    pub async fn drop_database(
        &self,
        platform: &Pool,
        tenant: &Tenant,
    ) -> eyre::Result<DropOutcome> {
        self.require_databases()?
            .drop_tenant_db(platform, tenant)
            .await
    }

    pub fn resets_session_state(&self) -> bool {
        self.databases()
            .is_none_or(TenantPoolManager::resets_session_state)
    }

    pub fn statement_timeout(&self) -> Option<Duration> {
        self.databases()
            .and_then(TenantPoolManager::statement_timeout)
    }

    pub fn pooler(&self) -> cufflink_db::Pooler {
        match &self.source {
            Source::Tenants { pools, .. } => pools.pooler(),
            Source::Fixed { pooler, .. } => *pooler,
        }
    }

    fn tls(&self) -> Arc<TlsMode> {
        match &self.source {
            Source::Tenants { pools, .. } => pools.tls(),
            Source::Fixed { .. } => Arc::new(TlsMode::Off),
        }
    }

    pub fn pool_statuses(&self) -> Vec<(String, deadpool_postgres::Status)> {
        self.databases()
            .map(|pools| pools.pool_statuses())
            .unwrap_or_default()
    }

    pub fn login_pool_statuses(&self) -> Vec<(String, deadpool_postgres::Status)> {
        self.roles
            .as_ref()
            .map(|roles| roles.logins.statuses())
            .unwrap_or_default()
    }

    pub fn forget_role(&self, service_id: Uuid) {
        if let Some(roles) = &self.roles {
            roles.bindings.forget(service_id);
        }
    }

    async fn entry(
        &self,
        catalog: &TenantCatalog,
        tenant_id: Uuid,
    ) -> Result<Arc<TenantEntry>, AcquireError> {
        catalog
            .get_or_load(tenant_id)
            .await
            .map_err(|e| AcquireError::Unavailable(e.to_string()))?
            .ok_or(AcquireError::UnknownTenant(tenant_id))
    }

    async fn tenant(
        &self,
        catalog: &TenantCatalog,
        tenant_id: Uuid,
    ) -> Result<Tenant, AcquireError> {
        self.entry(catalog, tenant_id)
            .await
            .map(|entry| entry.tenant.clone())
    }

    async fn standing(
        &self,
        id: &ServiceIdentity,
        purpose: Purpose,
    ) -> Result<(Standing, Option<Login>), AcquireError> {
        let platform = || Standing::platform(&id.schema);
        let (Source::Tenants { pools, catalog }, Some(roles)) = (&self.source, &self.roles) else {
            return Ok((platform(), None));
        };
        if purpose == Purpose::Platform {
            return Ok((platform(), None));
        }
        let entry = self.entry(catalog, id.tenant_id).await?;
        let policy = entry.mode(PolicyKey::DbRoles);
        if policy == PolicyMode::Off {
            return Ok((platform(), None));
        }
        let binding = roles.binding(id.service_id).await?;
        let binding = binding.as_deref();
        let refused = unbound(binding, purpose, &id.service_name)
            .filter(|_| {
                policy == PolicyMode::Enforce
                    || UNBOUND_RECORDED.should_log(id.service_id, Instant::now())
            })
            .is_some_and(|violation| entry.apply(PolicyKey::DbRoles, Some(violation)).is_err());
        if refused {
            return Err(AcquireError::NoRole(id.service_name.clone()));
        }
        let mode = role_mode(policy, binding, purpose);
        let login = match &mode {
            RoleMode::Login { role } => {
                let (base, settings) = pools.login_config(&entry.tenant);
                Some(Login {
                    tenant_id: id.tenant_id,
                    service_id: id.service_id,
                    tenant_slug: id.tenant_slug.clone(),
                    service_name: id.service_name.clone(),
                    role: role.clone(),
                    base,
                    settings: settings
                        .max_size(entry.config::<DbRolesConfig>().login_pool_max() as usize),
                })
            }
            RoleMode::Platform | RoleMode::SetLocal { .. } => None,
        };
        let standing = Standing {
            mode,
            schema: id.schema.clone(),
            owner: Some(Owner {
                tenant: id.tenant_slug.clone(),
                service: id.service_name.clone(),
                purpose,
                read_only_role: verified(binding).and_then(|b| b.read_only_role.clone()),
            }),
        };
        Ok((standing, login))
    }

    pub async fn role_of(
        &self,
        id: &ServiceIdentity,
        purpose: Purpose,
    ) -> Result<RoleMode, AcquireError> {
        self.standing(id, purpose)
            .await
            .map(|(standing, _)| standing.mode)
    }

    async fn pool(&self, tenant_id: Uuid) -> Result<Pool, AcquireError> {
        match &self.source {
            Source::Fixed { pool, .. } => Ok(pool.clone()),
            Source::Tenants { pools, catalog } => match pools.cached(tenant_id) {
                Some(pool) => Ok(pool),
                None => pools
                    .get_pool(&self.tenant(catalog, tenant_id).await?)
                    .await
                    .map_err(|e| AcquireError::Unavailable(e.to_string())),
            },
        }
    }

    pub async fn service(
        &self,
        id: &ServiceIdentity,
        purpose: Purpose,
    ) -> Result<ServiceSessions, AcquireError> {
        let (standing, login) = self.standing(id, purpose).await?;
        let (pool, relogin) = match (login, &self.roles) {
            (Some(login), Some(roles)) => (
                roles.login_pool(&login).await?,
                Some(Relogin {
                    roles: roles.clone(),
                    login,
                }),
            ),
            _ => (self.pool(id.tenant_id).await?, None),
        };
        Ok(ServiceSessions {
            pool,
            standing: Arc::new(standing),
            relogin,
            tls: self.tls(),
        })
    }

    pub async fn acquire(
        &self,
        id: &ServiceIdentity,
        purpose: Purpose,
        wait: Option<Duration>,
    ) -> Result<Session, AcquireError> {
        self.service(id, purpose).await?.checkout(wait).await
    }

    pub async fn platform(
        &self,
        tenant_id: Uuid,
        wait: Option<Duration>,
    ) -> Result<Object, AcquireError> {
        checkout(&self.pool(tenant_id).await?, wait).await
    }

    pub async fn login_config(
        &self,
        tenant_id: Uuid,
    ) -> Result<(tokio_postgres::Config, cufflink_db::PoolSettings), AcquireError> {
        match &self.source {
            Source::Tenants { pools, catalog } => {
                Ok(pools.login_config(&self.tenant(catalog, tenant_id).await?))
            }
            Source::Fixed { .. } => Err(AcquireError::Unavailable(
                "a fixed pool has no direct connections".to_string(),
            )),
        }
    }

    pub async fn direct_for(
        &self,
        id: &ServiceIdentity,
        purpose: Purpose,
    ) -> Result<tokio_postgres::Client, AcquireError> {
        match (self.standing(id, purpose).await?, &self.roles) {
            ((_, Some(login)), Some(roles)) => roles.connect(&login).await,
            _ => self.direct(id.tenant_id).await,
        }
    }

    pub async fn direct(&self, tenant_id: Uuid) -> Result<tokio_postgres::Client, AcquireError> {
        match &self.source {
            Source::Tenants { pools, catalog } => pools
                .connect_direct(&self.tenant(catalog, tenant_id).await?)
                .await
                .map_err(|e| AcquireError::Unavailable(e.to_string())),
            Source::Fixed { .. } => Err(AcquireError::Unavailable(
                "a fixed pool has no direct connections".to_string(),
            )),
        }
    }

    pub async fn listen(
        &self,
        tenant_id: Uuid,
        channel: &str,
    ) -> Result<cufflink_db::Listener, AcquireError> {
        match &self.source {
            Source::Tenants { pools, catalog } => {
                let (config, settings) =
                    pools.direct_config(&self.tenant(catalog, tenant_id).await?);
                cufflink_db::listen(config, &settings, channel)
                    .await
                    .map_err(|e| AcquireError::Unavailable(e.to_string()))
            }
            Source::Fixed { .. } => Err(AcquireError::Unavailable(
                "a fixed pool has no direct connections".to_string(),
            )),
        }
    }
}

pub struct ServiceSessions {
    pool: Pool,
    standing: Arc<Standing>,
    relogin: Option<Relogin>,
    tls: Arc<TlsMode>,
}

struct Relogin {
    roles: Arc<Roles>,
    login: Login,
}

#[derive(Debug)]
struct Standing {
    mode: RoleMode,
    schema: String,
    owner: Option<Owner>,
}

impl Standing {
    fn platform(schema: &str) -> Self {
        Self {
            mode: RoleMode::Platform,
            schema: schema.to_string(),
            owner: None,
        }
    }

    fn fallback(&self) -> Option<Fallback<'_>> {
        match (&self.mode, &self.owner) {
            (
                RoleMode::SetLocal {
                    role,
                    fallback: true,
                },
                Some(owner),
            ) => Some(Fallback {
                role,
                tenant: &owner.tenant,
                service: &owner.service,
            }),
            _ => None,
        }
    }

    fn fallback_outside(&self) -> Option<Fallback<'_>> {
        self.fallback().filter(|_| {
            self.owner
                .as_ref()
                .is_some_and(|owner| owner.purpose != Purpose::Ddl)
        })
    }
}

#[derive(Debug)]
struct Owner {
    tenant: String,
    service: String,
    purpose: Purpose,
    read_only_role: Option<String>,
}

impl ServiceSessions {
    pub fn over(pool: Pool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            standing: Arc::new(Standing::platform(&schema.into())),
            relogin: None,
            tls: Arc::new(TlsMode::Off),
        }
    }

    pub fn schema(&self) -> &str {
        &self.standing.schema
    }

    pub fn schema_ident(&self) -> String {
        folded_ident(&self.standing.schema)
    }

    pub async fn get(&self) -> Result<Session, AcquireError> {
        self.checkout(None).await
    }

    async fn client(&self, wait: Option<Duration>) -> Result<Object, AcquireError> {
        match (checkout(&self.pool, wait).await, &self.relogin) {
            (Err(AcquireError::Checkout(e)), Some(relogin)) if login_refused(&e) => {
                checkout(&relogin.roles.pool_again(&relogin.login).await?, wait).await
            }
            (outcome, _) => outcome,
        }
    }

    async fn checkout(&self, wait: Option<Duration>) -> Result<Session, AcquireError> {
        Ok(Session::of(
            self.client(wait).await?,
            self.standing.clone(),
            Arc::clone(&self.tls),
        ))
    }
}

fn verified(binding: Option<&Binding>) -> Option<&Binding> {
    binding.filter(|binding| binding.verified_at.is_some())
}

pub fn role_mode(policy: PolicyMode, binding: Option<&Binding>, purpose: Purpose) -> RoleMode {
    match (policy, purpose, verified(binding)) {
        (PolicyMode::Off, _, _) | (_, Purpose::Platform, _) | (_, _, None) => RoleMode::Platform,
        (PolicyMode::Warn, _, Some(binding)) => RoleMode::SetLocal {
            role: binding.role_name.clone(),
            fallback: true,
        },
        (PolicyMode::Enforce, Purpose::Ddl, Some(binding)) => RoleMode::SetLocal {
            role: binding.role_name.clone(),
            fallback: false,
        },
        (PolicyMode::Enforce, Purpose::Guest | Purpose::Crud, Some(binding)) => RoleMode::Login {
            role: binding.role_name.clone(),
        },
    }
}

pub fn unbound(binding: Option<&Binding>, purpose: Purpose, service: &str) -> Option<Violation> {
    match (purpose, verified(binding)) {
        (Purpose::Guest | Purpose::Crud, None) => Some(Violation::new(
            UNBOUND,
            format!("service {service} has no verified database role binding"),
        )),
        _ => None,
    }
}

async fn checkout(pool: &Pool, wait: Option<Duration>) -> Result<Object, AcquireError> {
    match wait {
        None => pool.get().await,
        Some(wait) => {
            let configured = pool.timeouts();
            let within = |t: Option<Duration>| Some(t.map_or(wait, |t| t.min(wait)));
            pool.timeout_get(&Timeouts {
                wait: within(configured.wait),
                create: within(configured.create),
                recycle: within(configured.recycle),
            })
            .await
        }
    }
    .map_err(AcquireError::Checkout)
}

#[async_trait]
pub trait Statements: Sync {
    async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    async fn query<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    async fn query_raw<T>(&self, statement: &T) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    async fn prepare(&self, sql: &str) -> Result<Statement, Error>;

    async fn batch_execute(&self, sql: &str) -> Result<(), Error>;

    fn role_mode(&self) -> &RoleMode;
}

macro_rules! statements_through {
    ($type:ty, $field:ident) => {
        #[async_trait]
        impl Statements for $type {
            async fn execute<T>(
                &self,
                statement: &T,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<u64, Error>
            where
                T: ?Sized + ToStatement + Sync + Send,
            {
                self.as_role(|| GenericClient::execute(&self.$field, statement, params))
                    .await
            }

            async fn query<T>(
                &self,
                statement: &T,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Vec<Row>, Error>
            where
                T: ?Sized + ToStatement + Sync + Send,
            {
                self.as_role(|| GenericClient::query(&self.$field, statement, params))
                    .await
            }

            async fn query_one<T>(
                &self,
                statement: &T,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Row, Error>
            where
                T: ?Sized + ToStatement + Sync + Send,
            {
                self.as_role(|| GenericClient::query_one(&self.$field, statement, params))
                    .await
            }

            async fn query_opt<T>(
                &self,
                statement: &T,
                params: &[&(dyn ToSql + Sync)],
            ) -> Result<Option<Row>, Error>
            where
                T: ?Sized + ToStatement + Sync + Send,
            {
                self.as_role(|| GenericClient::query_opt(&self.$field, statement, params))
                    .await
            }

            async fn query_raw<T>(&self, statement: &T) -> Result<RowStream, Error>
            where
                T: ?Sized + ToStatement + Sync + Send,
            {
                self.in_turn(GenericClient::query_raw(
                    &self.$field,
                    statement,
                    std::iter::empty::<&(dyn ToSql + Sync)>(),
                ))
                .await
            }

            async fn prepare(&self, sql: &str) -> Result<Statement, Error> {
                self.as_role(|| GenericClient::prepare(&self.$field, sql))
                    .await
            }

            async fn batch_execute(&self, sql: &str) -> Result<(), Error> {
                self.in_turn(GenericClient::batch_execute(&self.$field, sql))
                    .await
            }

            fn role_mode(&self) -> &RoleMode {
                self.mode()
            }
        }
    };
}

pub const STATEMENT_STOP_EVERY: Duration = Duration::from_secs(1);

pub struct StatementStop {
    token: CancelToken,
    tls: Arc<TlsMode>,
}

impl StatementStop {
    pub async fn stopping<T>(
        self,
        cancelled: &CancellationToken,
        work: impl Future<Output = T>,
    ) -> T {
        let stop = async {
            cancelled.cancelled().await;
            self.keep_asking().await
        };
        tokio::select! {
            biased;
            ended = work => ended,
            never = stop => match never {},
        }
    }

    async fn keep_asking(&self) -> Infallible {
        let mut asks = tokio::time::interval(STATEMENT_STOP_EVERY);
        asks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            asks.tick().await;
            let asked = cufflink_db::cancel_statement(&self.token, &self.tls);
            match tokio::time::timeout(STATEMENT_STOP_EVERY, asked).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Could not ask the server to stop a statement")
                }
                Err(_) => tracing::warn!("Asking the server to stop a statement timed out"),
            }
        }
    }
}

pub struct Session {
    client: Object,
    standing: Arc<Standing>,
    tls: Arc<TlsMode>,
    session_scoped: AtomicBool,
    read_only_query: AtomicBool,
    by_hand: AtomicBool,
    turn: tokio::sync::Mutex<()>,
}

statements_through!(Session, client);

impl Session {
    pub fn over(client: Object, schema: impl Into<String>) -> Self {
        Self::of(
            client,
            Arc::new(Standing::platform(&schema.into())),
            Arc::new(TlsMode::Off),
        )
    }

    fn of(client: Object, standing: Arc<Standing>, tls: Arc<TlsMode>) -> Self {
        Self {
            client,
            standing,
            tls,
            session_scoped: AtomicBool::new(false),
            read_only_query: AtomicBool::new(false),
            by_hand: AtomicBool::new(false),
            turn: tokio::sync::Mutex::new(()),
        }
    }

    pub fn stop(&self) -> StatementStop {
        StatementStop {
            token: self.client.cancel_token(),
            tls: Arc::clone(&self.tls),
        }
    }

    pub fn release(self, cancelled: &CancellationToken) {
        if cancelled.is_cancelled() {
            self.discard();
        }
    }

    pub fn schema(&self) -> &str {
        &self.standing.schema
    }

    pub fn schema_ident(&self) -> String {
        folded_ident(&self.standing.schema)
    }

    pub fn mode(&self) -> &RoleMode {
        &self.standing.mode
    }

    pub fn prelude(&self, scope: Scope, statement_timeout: Duration) -> String {
        if scope == Scope::Session {
            self.session_scoped.store(true, Ordering::Relaxed);
        }
        prelude(
            &self.standing.mode,
            &self.standing.schema,
            scope,
            statement_timeout,
        )
    }

    pub fn reset(&self) -> &'static str {
        self.session_scoped.store(false, Ordering::Relaxed);
        reset(&self.standing.mode)
    }

    pub fn read_only_role<'a>(&'a self, configured: &'a str) -> &'a str {
        self.read_only_query.store(true, Ordering::Relaxed);
        read_only_role(&self.standing, configured)
    }

    pub fn bound_read_only_role(&self) -> Option<&str> {
        bound_read_only_role(&self.standing)
    }

    async fn as_role<T, F, Fut>(&self, run: F) -> Result<T, Error>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        match self.standing.fallback_outside() {
            None => run().await,
            Some(fallback) => {
                let _turn = self.turn.lock().await;
                match (
                    self.session_scoped.load(Ordering::Relaxed),
                    self.by_hand.load(Ordering::Relaxed),
                ) {
                    (true, _) => fallback.in_session(&self.client, &run).await,
                    (false, true) => fallback.in_transaction(&self.client, &run).await,
                    (false, false) => fallback.alone(&self.client, &run).await,
                }
            }
        }
    }

    async fn in_turn<T>(
        &self,
        statement: impl Future<Output = Result<T, Error>> + Send,
    ) -> Result<T, Error> {
        let _turn = match self.standing.fallback_outside() {
            Some(_) => Some(self.turn.lock().await),
            None => None,
        };
        statement.await
    }

    pub async fn begin_by_hand(&self, setup: &str) -> Result<(), Error> {
        self.in_turn(self.client.batch_execute(setup)).await?;
        self.by_hand.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub async fn end_by_hand(&self, end: &str) -> Result<(), Error> {
        let ended = self.in_turn(self.client.batch_execute(end)).await;
        self.by_hand.store(false, Ordering::Relaxed);
        ended
    }

    pub async fn transaction(&mut self) -> Result<SessionTx<'_>, Error> {
        let tx = self.client.transaction().await?;
        match self.read_only_query.load(Ordering::Relaxed) {
            true => Ok(SessionTx {
                tx,
                mode: &self.standing.mode,
                fallback: None,
                turn: &self.turn,
            }),
            false => SessionTx::begun(tx, &self.standing, &self.turn).await,
        }
    }

    pub async fn read_only_transaction(&mut self) -> Result<SessionTx<'_>, Error> {
        let tx = self
            .client
            .build_transaction()
            .read_only(true)
            .start()
            .await?;
        SessionTx::begun(tx, &self.standing, &self.turn).await
    }

    pub fn discard(self) {
        drop(Object::take(self.client));
    }
}

fn prelude(mode: &RoleMode, schema: &str, scope: Scope, statement_timeout: Duration) -> String {
    session::prelude(schema, mode.imposed(), scope, statement_timeout)
}

fn reset(mode: &RoleMode) -> &'static str {
    session::reset(mode.imposed().is_some())
}

fn read_only_role<'a>(standing: &'a Standing, configured: &'a str) -> &'a str {
    bound_read_only_role(standing).unwrap_or(configured)
}

fn bound_read_only_role(standing: &Standing) -> Option<&str> {
    match &standing.mode {
        RoleMode::Platform | RoleMode::SetLocal { .. } => None,
        RoleMode::Login { role } => Some(
            standing
                .owner
                .as_ref()
                .and_then(|owner| owner.read_only_role.as_deref())
                .unwrap_or(role),
        ),
    }
}

pub struct SessionTx<'a> {
    tx: deadpool_postgres::Transaction<'a>,
    mode: &'a RoleMode,
    fallback: Option<Fallback<'a>>,
    turn: &'a tokio::sync::Mutex<()>,
}

statements_through!(SessionTx<'_>, tx);

impl<'a> SessionTx<'a> {
    async fn begun(
        tx: deadpool_postgres::Transaction<'a>,
        standing: &'a Standing,
        turn: &'a tokio::sync::Mutex<()>,
    ) -> Result<Self, Error> {
        if let Some(role) = standing.mode.imposed() {
            tx.batch_execute(&session::set_role(Scope::Transaction, role))
                .await?;
        }
        Ok(Self {
            tx,
            mode: &standing.mode,
            fallback: standing.fallback(),
            turn,
        })
    }

    pub fn mode(&self) -> &RoleMode {
        self.mode
    }

    async fn as_role<T, F, Fut>(&self, run: F) -> Result<T, Error>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        match self.fallback {
            None => run().await,
            Some(fallback) => {
                let _turn = self.turn.lock().await;
                fallback.in_transaction(&self.tx, &run).await
            }
        }
    }

    async fn in_turn<T>(
        &self,
        statement: impl Future<Output = Result<T, Error>> + Send,
    ) -> Result<T, Error> {
        let _turn = match self.fallback {
            Some(_) => Some(self.turn.lock().await),
            None => None,
        };
        statement.await
    }

    pub async fn commit(self) -> Result<(), Error> {
        self.tx.commit().await
    }

    pub async fn rollback(self) -> Result<(), Error> {
        self.tx.rollback().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    const PURPOSES: [Purpose; 4] = [
        Purpose::Guest,
        Purpose::Crud,
        Purpose::Ddl,
        Purpose::Platform,
    ];

    fn binding(verified: bool) -> Binding {
        Binding {
            service_id: Uuid::nil(),
            role_name: "svc_acme_loan_service".into(),
            managed: true,
            login: true,
            password_enc: Some(vec![1]),
            read_only_role: Some("reports_ro".into()),
            verified_at: verified.then(Utc::now),
            verify_report: None,
            updated_by: "platform".into(),
            updated_at: Utc::now(),
        }
    }

    fn set_local(fallback: bool) -> RoleMode {
        RoleMode::SetLocal {
            role: "svc_acme_loan_service".into(),
            fallback,
        }
    }

    fn login() -> RoleMode {
        RoleMode::Login {
            role: "svc_acme_loan_service".into(),
        }
    }

    fn standing(mode: RoleMode, purpose: Purpose, read_only_role: Option<&str>) -> Standing {
        Standing {
            mode,
            schema: "loan_service".into(),
            owner: Some(Owner {
                tenant: "acme".into(),
                service: "loan-service".into(),
                purpose,
                read_only_role: read_only_role.map(str::to_string),
            }),
        }
    }

    #[test]
    fn with_db_roles_off_every_purpose_runs_as_the_platform() {
        for purpose in PURPOSES {
            for bound in [None, Some(binding(false)), Some(binding(true))] {
                assert_eq!(
                    role_mode(PolicyMode::Off, bound.as_ref(), purpose),
                    RoleMode::Platform,
                    "{purpose:?}"
                );
                assert_eq!(
                    unbound(bound.as_ref(), Purpose::Platform, "loan-service"),
                    None
                );
            }
        }
    }

    #[test]
    fn a_verified_role_is_set_under_warn_and_logged_in_as_under_enforce() {
        let verified = binding(true);
        for (policy, purpose, mode) in [
            (PolicyMode::Warn, Purpose::Guest, set_local(true)),
            (PolicyMode::Warn, Purpose::Crud, set_local(true)),
            (PolicyMode::Warn, Purpose::Ddl, set_local(true)),
            (PolicyMode::Warn, Purpose::Platform, RoleMode::Platform),
            (PolicyMode::Enforce, Purpose::Guest, login()),
            (PolicyMode::Enforce, Purpose::Crud, login()),
            (PolicyMode::Enforce, Purpose::Ddl, set_local(false)),
            (PolicyMode::Enforce, Purpose::Platform, RoleMode::Platform),
        ] {
            assert_eq!(
                role_mode(policy, Some(&verified), purpose),
                mode,
                "{policy:?} {purpose:?}"
            );
            assert_eq!(unbound(Some(&verified), purpose, "loan-service"), None);
        }
    }

    #[test]
    fn without_a_verified_role_the_platforms_is_all_there_is_and_only_a_deploy_may_use_it() {
        for policy in [PolicyMode::Warn, PolicyMode::Enforce] {
            for bound in [None, Some(binding(false))] {
                for purpose in PURPOSES {
                    assert_eq!(
                        role_mode(policy, bound.as_ref(), purpose),
                        RoleMode::Platform,
                        "{policy:?} {purpose:?}"
                    );
                }
                for purpose in [Purpose::Guest, Purpose::Crud] {
                    assert_eq!(
                        unbound(bound.as_ref(), purpose, "loan-service"),
                        Some(Violation::new(
                            UNBOUND,
                            "service loan-service has no verified database role binding"
                        )),
                        "{purpose:?}"
                    );
                }
                for purpose in [Purpose::Ddl, Purpose::Platform] {
                    assert_eq!(
                        unbound(bound.as_ref(), purpose, "loan-service"),
                        None,
                        "{purpose:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn errors_read_as_the_callers_always_did() {
        assert_eq!(
            AcquireError::Unavailable("pool is closed".into()).to_string(),
            "pool is closed"
        );
        let checkout = AcquireError::Checkout(PoolError::Closed);
        assert!(checkout
            .to_string()
            .starts_with("connection checkout failed: "));
        assert!(matches!(
            PlatformError::from(checkout),
            PlatformError::Pool(PoolError::Closed)
        ));
        let missing = PlatformError::from(AcquireError::UnknownTenant(Uuid::nil()));
        assert!(
            matches!(&missing, PlatformError::Internal(m)
                if m == "Tenant pool error: tenant 00000000-0000-0000-0000-000000000000 does not exist"),
            "{missing}"
        );
    }

    #[test]
    fn a_service_without_a_role_is_unavailable_and_says_how_to_find_out_why() {
        let refused = PlatformError::from(AcquireError::NoRole("loan-service".into()));
        assert!(
            matches!(&refused, PlatformError::Unavailable(m)
                if m == "service loan-service has no database role; \
                         `cufflink services db-role verify` says what it lacks"),
            "{refused}"
        );
    }

    #[test]
    fn the_prelude_puts_the_schema_first_and_bounds_the_statement() {
        let timeout = Duration::from_millis(29_500);
        assert_eq!(
            prelude(
                &RoleMode::Platform,
                "loan_service",
                Scope::Transaction,
                timeout
            ),
            "SET LOCAL search_path TO \"loan_service\", public; SET LOCAL statement_timeout = 29500"
        );
        assert_eq!(
            prelude(&RoleMode::Platform, "loan_service", Scope::Session, timeout),
            "SET search_path TO \"loan_service\", public; SET statement_timeout = 29500"
        );
        for (schema, path) in [("Order_Desk_2", "\"order_desk_2\""), ("order", "\"order\"")] {
            assert_eq!(
                prelude(&RoleMode::Platform, schema, Scope::Transaction, timeout),
                format!(
                    "SET LOCAL search_path TO {path}, public; SET LOCAL statement_timeout = 29500"
                ),
                "the path the migration executor sets for the same service"
            );
        }
        assert_eq!(
            reset(&RoleMode::Platform),
            "RESET search_path; RESET statement_timeout"
        );
    }

    #[test]
    fn a_role_set_on_the_platforms_connection_is_set_by_the_prelude_and_reset_with_it() {
        let timeout = Duration::from_millis(29_500);
        for mode in [set_local(true), set_local(false)] {
            assert_eq!(
                prelude(&mode, "loan_service", Scope::Transaction, timeout),
                "SET LOCAL search_path TO \"loan_service\", public; \
                 SET LOCAL statement_timeout = 29500; \
                 SET LOCAL ROLE \"svc_acme_loan_service\""
            );
            assert_eq!(
                prelude(&mode, "loan_service", Scope::Session, timeout),
                "SET search_path TO \"loan_service\", public; SET statement_timeout = 29500; \
                 SET ROLE \"svc_acme_loan_service\""
            );
            assert_eq!(
                reset(&mode),
                "RESET search_path; RESET statement_timeout; RESET ROLE"
            );
            assert_eq!(mode.role(), Some("svc_acme_loan_service"));
        }
        assert_eq!(
            prelude(&login(), "loan_service", Scope::Transaction, timeout),
            prelude(
                &RoleMode::Platform,
                "loan_service",
                Scope::Transaction,
                timeout
            ),
            "a connection that logged in as the role has no role to set"
        );
        assert_eq!(reset(&login()), reset(&RoleMode::Platform));
        assert_eq!(login().role(), Some("svc_acme_loan_service"));
        assert_eq!(RoleMode::Platform.role(), None);
    }

    #[test]
    fn a_read_only_query_runs_as_the_platforms_role_for_them_until_the_service_logs_in() {
        for mode in [RoleMode::Platform, set_local(true), set_local(false)] {
            assert_eq!(
                read_only_role(
                    &standing(mode, Purpose::Guest, Some("report_ro")),
                    "reports_ro"
                ),
                "reports_ro"
            );
        }
        assert_eq!(
            read_only_role(
                &standing(login(), Purpose::Guest, Some("report_ro")),
                "reports_ro"
            ),
            "report_ro"
        );
        assert_eq!(
            read_only_role(&standing(login(), Purpose::Guest, None), "reports_ro"),
            "svc_acme_loan_service",
            "with no read-only role bound the query stays the service's, read-only"
        );
    }

    #[test]
    fn only_a_role_set_under_warn_falls_back_and_a_deploy_only_in_its_transactions() {
        let guest = standing(set_local(true), Purpose::Guest, None);
        let expected = Fallback {
            role: "svc_acme_loan_service",
            tenant: "acme",
            service: "loan-service",
        };
        assert_eq!(guest.fallback(), Some(expected));
        assert_eq!(guest.fallback_outside(), Some(expected));
        assert_eq!(
            standing(set_local(true), Purpose::Crud, None).fallback_outside(),
            Some(expected)
        );
        let deploy = standing(set_local(true), Purpose::Ddl, None);
        assert_eq!(deploy.fallback(), Some(expected));
        assert_eq!(deploy.fallback_outside(), None);
        for mode in [set_local(false), login(), RoleMode::Platform] {
            let standing = standing(mode, Purpose::Guest, None);
            assert_eq!(standing.fallback(), None);
            assert_eq!(standing.fallback_outside(), None);
        }
        assert_eq!(Standing::platform("loan_service").fallback(), None);
    }
}
