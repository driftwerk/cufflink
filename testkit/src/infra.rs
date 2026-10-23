use crate::error::TestkitError;
use postgresql_embedded::VersionReq;
use postgresql_embedded::{PostgreSQL, SettingsBuilder};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, TryLockError};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Once};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio::sync::{OnceCell, Semaphore};
use uuid::Uuid;

const EMBEDDED_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
const DATABASE_PROBE_INTERVAL: Duration = Duration::from_secs(2);
const DATABASE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Infra {
    pub database_url: String,
    nats_url: Option<String>,
    redis_url: Option<String>,
}

#[derive(Default)]
struct Servers {
    postgres: Vec<PostgreSQL>,
    nats: Vec<(Child, PathBuf)>,
    redis: Option<Child>,
}

static INFRA: OnceCell<Result<Infra, String>> = OnceCell::const_new();
static SPARE_NATS: OnceCell<Option<String>> = OnceCell::const_new();
static SERVERS: Mutex<Servers> = Mutex::new(Servers {
    postgres: Vec::new(),
    nats: Vec::new(),
    redis: None,
});
static STOP_AT_EXIT: Once = Once::new();

pub async fn try_infra() -> Result<&'static Infra, TestkitError> {
    INFRA
        .get_or_init(start_infra)
        .await
        .as_ref()
        .map_err(|reason| TestkitError::Unavailable(reason.clone()))
}

pub async fn infra() -> Option<&'static Infra> {
    match INFRA.get_or_init(start_infra).await {
        Ok(infra) => Some(infra),
        Err(reason) => {
            skip(reason);
            None
        }
    }
}

pub(crate) fn skips_are_failures() -> bool {
    std::env::var_os("CUFFLINK_CI").is_some_and(|v| !v.is_empty())
}

pub(crate) fn report_skip(reason: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr().lock(), "skipped: {reason}");
}

pub fn skip(reason: &str) {
    report_skip(reason);
    if skips_are_failures() {
        panic!("test skipped in CI: {reason}");
    }
}

impl Infra {
    pub fn nats_url(&self) -> Option<&str> {
        self.nats_url.as_deref()
    }

    pub fn require_nats(&self) -> Option<&str> {
        match self.nats_url.as_deref() {
            Some(url) => Some(url),
            None => {
                skip("no NATS (CUFFLINK_TEST_NATS_URL unset and no nats-server binary)");
                None
            }
        }
    }

    pub async fn require_spare_nats(&self) -> Option<&'static str> {
        let spare = SPARE_NATS
            .get_or_init(|| async {
                match std::env::var("CUFFLINK_TEST_SPARE_NATS_URL") {
                    Ok(url) => Some(url),
                    Err(_) => spawn_nats_server().await,
                }
            })
            .await
            .as_deref();
        if spare.is_none() {
            skip("no spare NATS (CUFFLINK_TEST_SPARE_NATS_URL unset and no nats-server binary)");
        }
        spare
    }

    pub fn redis_url(&self) -> Option<&str> {
        self.redis_url.as_deref()
    }

    pub fn require_redis(&self) -> Option<&str> {
        match self.redis_url.as_deref() {
            Some(url) => Some(url),
            None => {
                skip("no Redis (CUFFLINK_TEST_REDIS_URL unset and no redis-server binary)");
                None
            }
        }
    }

    pub async fn try_create_database(&self) -> Result<TestDb, TestkitError> {
        TestDb::try_create(&self.database_url).await
    }

    pub async fn create_database(&self) -> TestDb {
        self.try_create_database()
            .await
            .expect("create test database")
    }

    pub async fn create_migrated_database(&self) -> TestDb {
        let db = self.create_database().await;
        cufflink_platform::repo::migrations::run(&cufflink_platform::repo::migrations::Target {
            config: cufflink_db::parse_config(&db.url).expect("url"),
            settings: cufflink_db::PoolSettings::from_vars(1, |_| None).expect("defaults"),
        })
        .await
        .expect("migrations");
        db
    }
}

async fn start_infra() -> Result<Infra, String> {
    let database_url = match std::env::var("CUFFLINK_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => start_embedded_postgres().await?,
    };
    let nats_url = match std::env::var("CUFFLINK_TEST_NATS_URL") {
        Ok(url) => Some(url),
        Err(_) => spawn_nats_server().await,
    };
    let redis_url = match std::env::var("CUFFLINK_TEST_REDIS_URL") {
        Ok(url) => Some(url),
        Err(_) => spawn_redis_server().await,
    };
    Ok(Infra {
        database_url,
        nats_url,
        redis_url,
    })
}

fn profile_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|test| test.parent()?.parent().map(Path::to_path_buf))
        .expect("a test binary runs from target/<profile>/deps")
}

pub fn workspace_binary(name: &str) -> PathBuf {
    profile_dir().join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

pub fn require_binary(name: &str) -> Option<PathBuf> {
    let binary = workspace_binary(name);
    if binary.is_file() {
        Some(binary)
    } else {
        skip(&format!(
            "the {name} binary is not built (cargo test --workspace builds it)"
        ));
        None
    }
}

fn target_tmp_dir() -> PathBuf {
    profile_dir()
        .parent()
        .map(|target| target.join("tmp"))
        .expect("a profile directory is in the target directory")
}

async fn start_embedded_postgres() -> Result<String, String> {
    start_embedded(
        SettingsBuilder::new()
            .installation_dir(target_tmp_dir().join("postgresql"))
            .timeout(Some(EMBEDDED_COMMAND_TIMEOUT))
            .build(),
    )
    .await
}

type Started = Arc<OnceCell<Result<String, String>>>;

static PINNED: LazyLock<Mutex<HashMap<&'static str, Started>>> = LazyLock::new(Mutex::default);

pub async fn postgres_at(version: &'static str) -> Option<String> {
    let major = version.split('.').next().unwrap_or(version);
    if let Ok(url) = std::env::var(format!("CUFFLINK_TEST_POSTGRES_{major}_URL")) {
        return Some(url);
    }
    let cell = PINNED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(version)
        .or_default()
        .clone();
    let started = cell
        .get_or_init(|| async move {
            let requirement = VersionReq::parse(&format!("={version}"))
                .map_err(|e| format!("Postgres version {version}: {e}"))?;
            let installation = target_tmp_dir().join("postgresql");
            link_cached_release(&installation, version)?;
            start_embedded(
                SettingsBuilder::new()
                    .version(requirement)
                    .installation_dir(installation)
                    .timeout(Some(EMBEDDED_COMMAND_TIMEOUT))
                    .build(),
            )
            .await
        })
        .await;
    match started {
        Ok(url) => Some(url.clone()),
        Err(reason) => {
            skip(&format!("Postgres {version}: {reason}"));
            None
        }
    }
}

fn link_cached_release(installation: &Path, version: &str) -> Result<(), String> {
    let installed = installation.join(version);
    let cached = std::env::var_os("HOME")
        .map(|home| Path::new(&home).join(".theseus/postgresql").join(version))
        .filter(|cached| cached.join("bin").is_dir());
    match cached {
        Some(cached) if !installed.exists() => std::fs::create_dir_all(installation)
            .and_then(|()| std::os::unix::fs::symlink(&cached, &installed))
            .map_err(|e| format!("linking the cached Postgres {version}: {e}")),
        _ => Ok(()),
    }
}

async fn start_embedded(settings: postgresql_embedded::Settings) -> Result<String, String> {
    let mut pg = PostgreSQL::new(settings);
    pg.setup().await.map_err(|e| {
        format!("no Postgres (CUFFLINK_TEST_DATABASE_URL unset, embedded setup failed: {e})")
    })?;
    pg.start().await.map_err(|e| {
        format!("no Postgres (CUFFLINK_TEST_DATABASE_URL unset, embedded start failed: {e})")
    })?;
    let url = pg.settings().url("postgres");
    servers().postgres.push(pg);
    stop_servers_at_exit();
    Ok(url)
}

async fn spawn_nats_server() -> Option<String> {
    let bin = std::env::var("NATS_SERVER_BIN").unwrap_or_else(|_| "nats-server".into());
    let dir = std::env::temp_dir().join(format!("cufflink-nats-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).ok()?;
    match start_nats_server(&bin, &dir).await {
        Some((child, url)) => {
            servers().nats.push((child, dir));
            stop_servers_at_exit();
            Some(url)
        }
        None => {
            let _ = std::fs::remove_dir_all(&dir);
            None
        }
    }
}

async fn start_nats_server(bin: &str, dir: &Path) -> Option<(Child, String)> {
    let mut child = Command::new(bin)
        .args(["-js", "-p", "0", "--store_dir"])
        .arg(dir)
        .arg("--ports_file_dir")
        .arg(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let ports_file = dir.join(format!("nats-server_{}.ports", child.id()));
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(url) = std::fs::read_to_string(&ports_file)
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|ports| ports["nats"][0].as_str().map(str::to_string))
        {
            return Some((child, url));
        }
        if let Ok(Some(_)) = child.try_wait() {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    None
}

async fn spawn_redis_server() -> Option<String> {
    let bin = std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".into());
    let port = free_port();
    let mut child = Command::new(bin)
        .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
        .args(["--save", "", "--appendonly", "no"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            servers().redis = Some(child);
            stop_servers_at_exit();
            return Some(format!("redis://127.0.0.1:{port}"));
        }
        if child.try_wait().ok()?.is_some() {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = child.kill();
    None
}

fn servers() -> MutexGuard<'static, Servers> {
    SERVERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn stop_servers_at_exit() {
    STOP_AT_EXIT.call_once(|| {
        // SAFETY: `stop_servers` is a plain `extern "C" fn` with no
        unsafe { libc::atexit(stop_servers) };
    });
}

extern "C" fn stop_servers() {
    let mut servers = servers();
    servers.postgres.clear();
    for (mut nats, dir) in servers.nats.drain(..) {
        let _ = nats.kill();
        let _ = nats.wait();
        let _ = std::fs::remove_dir_all(dir);
    }
    if let Some(mut redis) = servers.redis.take() {
        let _ = redis.kill();
        let _ = redis.wait();
    }
}

pub struct TestDb {
    pub url: String,
    pub name: String,
    id: String,
    admin_url: String,
    tenant_dbs: Vec<String>,
}

impl TestDb {
    pub async fn create(admin_url: &str) -> Self {
        Self::try_create(admin_url)
            .await
            .expect("create test database")
    }

    pub async fn try_create(admin_url: &str) -> Result<Self, TestkitError> {
        let id = short_id();
        let name = format!("cufflink_platform_{id}");
        try_admin_client(admin_url)
            .await?
            .execute(&format!("CREATE DATABASE \"{name}\""), &[])
            .await?;
        Ok(Self {
            url: with_dbname(admin_url, &name),
            name,
            id,
            admin_url: admin_url.to_string(),
            tenant_dbs: Vec::new(),
        })
    }

    pub async fn create_unprivileged(admin_url: &str) -> Self {
        let id = short_id();
        let (role, password, name) = (
            format!("cufflink_{id}"),
            Uuid::new_v4().simple().to_string(),
            format!("cufflink_platform_{id}"),
        );
        let admin = admin_client(admin_url).await;
        for statement in [
            format!(
                "CREATE ROLE \"{role}\" LOGIN CREATEROLE CREATEDB NOSUPERUSER PASSWORD '{password}'"
            ),
            format!("CREATE DATABASE \"{name}\" OWNER \"{role}\""),
        ] {
            admin
                .batch_execute(&statement)
                .await
                .expect("create the platform's role and database");
        }
        Self {
            url: with_dbname(&with_login(admin_url, &role, &password), &name),
            name,
            id,
            admin_url: admin_url.to_string(),
            tenant_dbs: Vec::new(),
        }
    }

    pub fn role(&self, name: &str) -> String {
        format!("{name}_{}", self.id)
    }

    pub fn pool(&self) -> deadpool_postgres::Pool {
        cufflink_db::build_pool(&self.url, 4).expect("test pool")
    }

    pub async fn mint_tenant(&mut self) -> Tenant {
        self.mint_tenant_with(Default::default()).await
    }

    pub async fn mint_tenant_with(
        &mut self,
        fields: cufflink_platform::repo::tenants::NewTenant<'_>,
    ) -> Tenant {
        let slug = format!("it-{}", short_id());
        let pool = self.pool();
        let tenant = cufflink_platform::repo::tenants::create_tenant(
            &pool.get().await.expect("checkout"),
            &cufflink_platform::repo::tenants::NewTenant {
                name: &slug,
                slug: &slug,
                ..fields
            },
        )
        .await
        .expect("create tenant row");
        cufflink_platform::tenant_db::TenantPoolManager::new(&self.url, 4, 1)
            .expect("tenant pool manager")
            .create_tenant_db(&pool, &tenant)
            .await
            .expect("create tenant database");
        self.tenant_dbs
            .push(cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(&slug));

        let api_key = format!("cfk_{}", Uuid::new_v4().simple());
        let key_hash = hex::encode(Sha256::digest(api_key.as_bytes()));
        pool.get()
            .await
            .expect("checkout")
            .execute(
                "INSERT INTO api_keys (tenant_id, key_hash, name) VALUES ($1, $2, $3)",
                &[&tenant.id, &key_hash, &"integration"],
            )
            .await
            .expect("insert api key");
        Tenant {
            id: tenant.id,
            slug,
            api_key,
        }
    }

    pub async fn managed_role(&self, tenant: &Tenant, service_name: &str) -> String {
        let service =
            cufflink_platform::repo::services::find(&self.pool(), tenant.id, service_name)
                .await
                .expect("services")
                .unwrap_or_else(|| panic!("service {service_name} is deployed"));
        cufflink_platform::db_roles::names::managed_role(
            &tenant.slug,
            &service.schema(),
            tenant.id,
            service.id,
        )
    }

    pub async fn mint_admin_key(&self) -> String {
        let api_key = format!("ck_platform_{}", Uuid::new_v4().simple());
        let key_hash = hex::encode(Sha256::digest(api_key.as_bytes()));
        self.pool()
            .get()
            .await
            .expect("checkout")
            .execute(
                "INSERT INTO api_keys (key_hash, name, is_platform_admin) VALUES ($1, $2, true)",
                &[&key_hash, &"integration-admin"],
            )
            .await
            .expect("insert admin api key");
        api_key
    }

    pub async fn create_external_database(&mut self, name: &str) -> String {
        let url = create_database_beside(&self.admin_url, name)
            .await
            .expect("create external database");
        self.adopt_database(name);
        url
    }

    pub fn admin_url(&self) -> &str {
        &self.admin_url
    }

    pub(crate) fn adopt_database(&mut self, name: &str) {
        self.tenant_dbs.push(name.to_string());
    }

    pub async fn database_exists(&self, name: &str) -> bool {
        admin_client(&self.admin_url)
            .await
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
                &[&name],
            )
            .await
            .expect("pg_database")
            .get(0)
    }

    pub fn tenant_url(&self, slug: &str) -> String {
        with_dbname(
            &self.admin_url,
            &cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(slug),
        )
    }

    pub fn adopt_tenant_db(&mut self, slug: &str) {
        self.tenant_dbs
            .push(cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(slug));
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let admin_url = self.admin_url.clone();
        let names: Vec<String> = self
            .tenant_dbs
            .iter()
            .chain(std::iter::once(&self.name))
            .cloned()
            .collect();
        let cleanup = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            runtime.block_on(async {
                let admin = admin_client(&admin_url).await;
                for db in names {
                    let _ = admin
                        .execute(
                            &format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"),
                            &[],
                        )
                        .await;
                }
            });
        });
        let _ = cleanup.join();
    }
}

pub struct Tenant {
    pub id: Uuid,
    pub slug: String,
    pub api_key: String,
}

pub async fn active_service(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    name: &str,
    schema: &str,
) -> Result<cufflink_platform::repo::services::Service, TestkitError> {
    cufflink_platform::repo::services::find_or_create(
        pool,
        tenant_id,
        name,
        schema,
        cufflink_platform::repo::services::Status::Active,
    )
    .await
    .map_err(|e| TestkitError::Platform(format!("service {name}: {e}")))
}

pub(crate) async fn create_database_beside(
    admin_url: &str,
    name: &str,
) -> Result<String, TestkitError> {
    try_admin_client(admin_url)
        .await?
        .execute(&format!("CREATE DATABASE \"{name}\""), &[])
        .await?;
    Ok(with_dbname(admin_url, name))
}

async fn try_admin_client(url: &str) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    tokio::spawn(connection);
    Ok(client)
}

async fn admin_client(url: &str) -> tokio_postgres::Client {
    try_admin_client(url)
        .await
        .expect("connect to the admin database")
}

fn with_login(url: &str, user: &str, password: &str) -> String {
    let (scheme, rest) = url.split_once("://").unwrap_or(("postgres", url));
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let host = rest[..authority_end]
        .rsplit_once('@')
        .map_or(&rest[..authority_end], |(_, host)| host);
    format!(
        "{scheme}://{user}:{password}@{host}{}",
        &rest[authority_end..]
    )
}

pub fn with_dbname(url: &str, dbname: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let cut = base.rfind('/').map_or(base.len(), |i| i);
    let mut out = format!("{}/{dbname}", &base[..cut]);
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    out
}

fn short_id() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

static ISSUED_PORTS: Mutex<BTreeMap<u16, Option<File>>> = Mutex::new(BTreeMap::new());

const SPAWN_ATTEMPTS: usize = 3;
const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const OUTPUT_TAIL_BYTES: usize = 64 * 1024;

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(default)
}

pub(crate) fn health_deadline() -> Duration {
    Duration::from_secs(env_number("CUFFLINK_TEST_HEALTH_TIMEOUT_SECS", 90))
}

static STARTING: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(env_number("CUFFLINK_TEST_MAX_STARTING", 4).max(1) as usize));

fn port_leases() -> PathBuf {
    std::env::var_os("CUFFLINK_TEST_PORT_LEASES")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("cufflink-test-ports"))
}

#[derive(Debug, PartialEq, Eq)]
struct LeasedElsewhere;

fn lease_port(leases: &Path, port: u16) -> Result<Option<File>, LeasedElsewhere> {
    let lease = std::fs::create_dir_all(leases).and_then(|()| {
        File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(leases.join(port.to_string()))
    });
    match lease.map(|lease| lease.try_lock().map(|()| lease)) {
        Ok(Ok(lease)) => Ok(Some(lease)),
        Ok(Err(TryLockError::WouldBlock)) => Err(LeasedElsewhere),
        Ok(Err(TryLockError::Error(_))) | Err(_) => Ok(None),
    }
}

pub fn free_port() -> u16 {
    let leases = port_leases();
    let mut issued = ISSUED_PORTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::iter::repeat_with(|| {
        std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map(|a| a.port())
            .expect("bind a loopback port")
    })
    .filter(|port| !issued.contains_key(port))
    .find_map(|port| lease_port(&leases, port).ok().map(|lease| (port, lease)))
    .map(|(port, lease)| {
        issued.insert(port, lease);
        port
    })
    .expect("an unissued port")
}

pub struct ChildProcess {
    child: Mutex<Child>,
    database_url: Option<String>,
    output: Option<NamedTempFile>,
    pub base_url: String,
    pub port: u16,
    pub http: reqwest::Client,
}

impl ChildProcess {
    pub async fn spawn(binary: &Path, port_var: &str, env: &[(&str, &str)]) -> Self {
        let _starting = STARTING
            .acquire()
            .await
            .expect("the start gate is never closed");
        let mut exits = Vec::with_capacity(SPAWN_ATTEMPTS);
        for _ in 0..SPAWN_ATTEMPTS {
            let process = Self::launch(binary, free_port(), port_var, env);
            match process.wait_for_health(binary).await {
                Ok(()) => return process,
                Err(status) => exits.push((status, process.output_tail())),
            }
        }
        panic!(
            "{} exited before answering /health on {SPAWN_ATTEMPTS} ports: {exits:#?}",
            binary.display()
        );
    }

    pub fn launch_unchecked(binary: &Path, port_var: &str, env: &[(&str, &str)]) -> Self {
        Self::launch(binary, free_port(), port_var, env)
    }

    fn launch(binary: &Path, port: u16, port_var: &str, env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(binary);
        command
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .envs(env.iter().copied())
            .env(port_var, port.to_string());
        let output = std::env::var("CUFFLINK_TEST_LOG")
            .is_err()
            .then(|| NamedTempFile::new().expect("a file for the child's output"));
        if let Some(file) = &output {
            let shared = || file.as_file().try_clone().expect("share the output file");
            command.stdout(shared()).stderr(shared());
        }
        let child = command
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", binary.display()));
        Self {
            child: Mutex::new(child),
            database_url: env
                .iter()
                .find(|(name, _)| *name == "DATABASE_URL")
                .map(|(_, url)| url.to_string()),
            output,
            base_url: format!("http://127.0.0.1:{port}"),
            port,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("http client"),
        }
    }

    async fn wait_for_health(&self, binary: &Path) -> Result<(), std::process::ExitStatus> {
        let timeout = health_deadline();
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            self.running()?;
            let probe = tokio::time::timeout(HEALTH_PROBE_TIMEOUT, self.status("/health")).await;
            if probe == Ok(Some(200)) {
                return self.running();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "{} did not answer /health within {timeout:?} \
             (CUFFLINK_TEST_HEALTH_TIMEOUT_SECS); its output:\n{}",
            binary.display(),
            self.output_tail()
        );
    }

    pub fn output_tail(&self) -> String {
        let Some(file) = &self.output else {
            return "(passed through: CUFFLINK_TEST_LOG is set)".to_string();
        };
        let bytes = std::fs::read(file.path()).unwrap_or_default();
        let tail = &bytes[bytes.len().saturating_sub(OUTPUT_TAIL_BYTES)..];
        String::from_utf8_lossy(tail).into_owned()
    }

    fn child(&self) -> MutexGuard<'_, Child> {
        self.child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn running(&self) -> Result<(), std::process::ExitStatus> {
        match self.child().try_wait() {
            Ok(Some(status)) => Err(status),
            _ => Ok(()),
        }
    }

    pub async fn status(&self, path: &str) -> Option<u16> {
        self.http
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .ok()
            .map(|r| r.status().as_u16())
    }

    pub async fn status_line(&self, sent: &[u8]) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let address = self.base_url.trim_start_matches("http://");
        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        stream.write_all(sent).await.expect("write");
        let mut received = Vec::new();
        let mut buffer = [0u8; 1024];
        while !received.windows(4).any(|w| w == b"\r\n\r\n") {
            let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buffer))
                .await
                .expect("the child answers without waiting for the rest of the body")
                .expect("read");
            if read == 0 {
                break;
            }
            received.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8_lossy(&received)
            .lines()
            .next()
            .unwrap_or_default()
            .to_string()
    }

    pub async fn get_json(&self, path: &str) -> Option<(u16, Value)> {
        let response = self
            .http
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .ok()?;
        let status = response.status().as_u16();
        let body = response.json().await.unwrap_or(Value::Null);
        Some((status, body))
    }

    pub async fn get_text(&self, path: &str) -> Option<String> {
        self.http
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()
    }

    pub async fn wait_for_state(&self, state: &str, timeout: Duration) -> Value {
        let deadline = Instant::now() + timeout;
        let mut next_database_probe = Instant::now();
        let mut last = Value::Null;
        while Instant::now() < deadline {
            if let Err(status) = self.running() {
                panic!(
                    "the child exited ({status}) before /ready reported {state:?}; its output:\n{}",
                    self.output_tail()
                );
            }
            if let Some((_, body)) = self.get_json("/ready").await {
                if body["state"] == state {
                    return body;
                }
                if body["db"] == "error" && Instant::now() >= next_database_probe {
                    self.fail_if_the_database_is_gone().await;
                    next_database_probe = Instant::now() + DATABASE_PROBE_INTERVAL;
                }
                last = body;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("/ready never reported state {state:?}; last body: {last}");
    }

    async fn fail_if_the_database_is_gone(&self) {
        let Some(url) = &self.database_url else {
            return;
        };
        let reachable = tokio::time::timeout(
            DATABASE_PROBE_TIMEOUT,
            tokio_postgres::connect(url, tokio_postgres::NoTls),
        )
        .await
        .is_ok_and(|connected| connected.is_ok());
        if !reachable {
            panic!(
                "the test Postgres at {url} went away: the child reports its database down and \
                 the harness cannot connect either (a killed or stalled server, not a slow start)"
            );
        }
    }

    pub async fn wait_ready(&self) -> Value {
        let body = self.wait_for_state("ready", health_deadline()).await;
        assert_eq!(
            body["db"], "ok",
            "ready state must come with a live database"
        );
        body
    }

    pub fn pid(&self) -> u32 {
        self.child().id()
    }

    #[cfg(unix)]
    fn signal(&self, signal: libc::c_int, name: &str) {
        let rc = unsafe { libc::kill(self.pid() as libc::pid_t, signal) };
        assert_eq!(rc, 0, "{name} delivery failed");
    }

    #[cfg(unix)]
    pub fn sigterm(&self) {
        self.signal(libc::SIGTERM, "SIGTERM");
    }

    #[cfg(unix)]
    pub fn sigstop(&self) {
        self.signal(libc::SIGSTOP, "SIGSTOP");
    }

    #[cfg(unix)]
    pub fn sigcont(&self) {
        self.signal(libc::SIGCONT, "SIGCONT");
    }

    pub async fn wait_exit(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Err(status) = self.running() {
                return Some(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }

    pub fn kill(&mut self) {
        let mut child = self.child();
        let _ = child.kill();
        let _ = child.wait();
    }

    pub async fn metric(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let text = self.get_text("/metrics").await?;
        metric_value(&text, name, labels)
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

pub fn metric_value(text: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    text.lines()
        .filter(|line| line.starts_with(name))
        .filter(|line| {
            labels
                .iter()
                .all(|(k, v)| line.contains(&format!("{k}=\"{v}\"")))
        })
        .filter_map(|line| line.rsplit(' ').next()?.parse().ok())
        .next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::io::{BufRead, BufReader};

    const HOLD: &str = "CUFFLINK_TEST_HOLD_PORT_LEASE";
    const HELD: &str = "lease held";

    fn taken_once_let_go(leases: &Path, port: u16) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        std::iter::repeat_with(|| {
            let taken = lease_port(leases, port).is_ok_and(|lease| lease.is_some());
            if !taken {
                std::thread::sleep(Duration::from_millis(20));
            }
            taken
        })
        .find(|taken| *taken || Instant::now() >= deadline)
        .unwrap_or_default()
    }

    #[test]
    fn a_port_is_its_takers_until_the_lease_is_let_go() {
        let leases = tempfile::tempdir().expect("a directory");
        let leases = leases.path().join("ports");
        let taken = lease_port(&leases, 50_001).expect("taken");
        assert!(taken.is_some(), "a lease is kept");
        assert_eq!(
            lease_port(&leases, 50_001).err(),
            Some(LeasedElsewhere),
            "it is its taker's"
        );
        assert!(lease_port(&leases, 50_002).is_ok_and(|lease| lease.is_some()));

        drop(taken);
        assert!(taken_once_let_go(&leases, 50_001), "its taker let it go");
    }

    #[test]
    fn a_port_is_handed_out_where_no_lease_can_be_kept() {
        let not_a_directory = NamedTempFile::new().expect("a file");
        for _ in 0..2 {
            assert!(lease_port(not_a_directory.path(), 50_003).is_ok_and(|lease| lease.is_none()));
        }
    }

    #[test]
    fn no_port_is_handed_out_twice() {
        let ports: BTreeSet<u16> = std::iter::repeat_with(free_port).take(20).collect();
        assert_eq!(ports.len(), 20);
        let leases = port_leases();
        assert!(ports.iter().all(|port| lease_port(&leases, *port).is_err()));
    }

    #[test]
    fn a_child_is_seen_to_have_exited_and_keeps_its_status() {
        let child = ChildProcess::launch_unchecked(Path::new("false"), "PORT", &[]);
        let deadline = Instant::now() + Duration::from_secs(10);
        let exited = std::iter::repeat_with(|| {
            std::thread::sleep(Duration::from_millis(20));
            child.running()
        })
        .find(|running| running.is_err() || Instant::now() >= deadline);
        let code = |running: Option<Result<(), std::process::ExitStatus>>| {
            running
                .and_then(Result::err)
                .and_then(|status| status.code())
        };
        assert_eq!(code(exited), Some(1), "the child that exited is seen to");
        assert_eq!(code(Some(child.running())), Some(1), "asked again");
    }

    #[test]
    fn holds_a_lease_while_asked_to() {
        let Some((leases, port)) = std::env::var(HOLD).ok().and_then(|hold| {
            let (leases, port) = hold.rsplit_once(':')?;
            Some((PathBuf::from(leases), port.parse().ok()?))
        }) else {
            return;
        };
        let _lease = lease_port(&leases, port).expect("the lease");
        println!("{HELD}");
        std::thread::sleep(Duration::from_secs(60));
    }

    #[test]
    fn a_port_another_process_took_is_not_handed_out_while_it_runs() {
        let leases = tempfile::tempdir().expect("a directory");
        let mut holder = Command::new(std::env::current_exe().expect("this test binary"))
            .args([
                "--exact",
                "infra::tests::holds_a_lease_while_asked_to",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HOLD, format!("{}:50004", leases.path().display()))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("another test process");
        let said = BufReader::new(holder.stdout.take().expect("its output"))
            .lines()
            .map_while(Result::ok)
            .any(|line| line.contains(HELD));
        assert!(said, "the other process took the lease");
        assert_eq!(
            lease_port(leases.path(), 50_004).err(),
            Some(LeasedElsewhere)
        );

        holder.kill().expect("kill");
        holder.wait().expect("reaped");
        assert!(
            taken_once_let_go(leases.path(), 50_004),
            "a lease does not outlive the process that took it, however it ended"
        );
    }

    #[test]
    fn a_login_replaces_whoever_the_url_logged_in_as() {
        assert_eq!(
            with_login(
                "postgres://cufflink:cufflink@127.0.0.1:5432/postgres",
                "cufflink_ab12",
                "pw"
            ),
            "postgres://cufflink_ab12:pw@127.0.0.1:5432/postgres"
        );
        assert_eq!(
            with_login("postgresql://localhost:6000/db?sslmode=disable", "r", "p"),
            "postgresql://r:p@localhost:6000/db?sslmode=disable"
        );
    }
}
