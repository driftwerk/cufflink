pub mod budget;
pub mod host;
pub mod image;
mod invocation;
mod precompiled;
pub mod redis_scope;
pub mod storage_scope;

use crate::policy::PolicySet;
use crate::shutdown::Shutdown;
use crate::storage::S3Storage;
use crate::AppState;
use cufflink_db::env_var;
use cufflink_telemetry::{scoped_span, GaugeGuard};
use eyre::Result;
use host::{GuestLimits, HostState, HostStateInit, NatsPublishPolicy};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::info;
use uuid::Uuid;
use wasmtime::*;

pub(crate) use budget::RouteKey;
pub use budget::{Admission, Class};
pub use invocation::{ExecutionError, Invocation, InvocationKind};

pub(crate) const FEATURES: &[&str] = &["precompiled_modules"];

pub(super) fn wt(err: wasmtime::Error) -> eyre::Report {
    eyre::eyre!("{}", err)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

const MIB: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmLimits {
    pub max_memory_bytes: usize,
    pub max_tables: u32,
    pub max_table_elements: usize,
    pub max_instances: u32,
    pub max_guest_len: usize,
    pub max_response_bytes: usize,
    pub max_concurrent: usize,
    pub epoch_tick: Duration,
    pub module_cache_max: usize,
    pub admission_wait: Duration,
    pub retry_after: Duration,
    pub budget: budget::BudgetConfig,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 512 * MIB,
            max_tables: 8,
            max_table_elements: 1_000_000,
            max_instances: 1,
            max_guest_len: 64 * MIB,
            max_response_bytes: 64 * MIB,
            max_concurrent: 256,
            epoch_tick: Duration::from_millis(100),
            module_cache_max: 128,
            admission_wait: Duration::from_millis(1000),
            retry_after: Duration::from_secs(2),
            budget: budget::BudgetConfig::default(),
        }
    }
}

const RETIRED_LIMITS_VAR: &str = "CUFFLINK_WASM_LIMITS";

impl WasmLimits {
    pub fn memory_cap(&self, policies: &PolicySet) -> usize {
        policies
            .config::<crate::policy::config::WasmLimitsConfig>()
            .memory_bytes
            .map_or(usize::MAX, |bytes| {
                usize::try_from(bytes).unwrap_or(usize::MAX)
            })
            .min(self.max_memory_bytes)
    }

    pub fn from_env() -> Result<Self> {
        if std::env::var_os(RETIRED_LIMITS_VAR).is_some() {
            tracing::warn!(
                "{RETIRED_LIMITS_VAR} is no longer read; the wasm_limits policy decides \
                 (CUFFLINK_POLICY_WASM_LIMITS, or per tenant with `cufflink tenants policy set`)"
            );
        }
        let defaults = Self::default();
        let max_concurrent = env_var("WASM_MAX_CONCURRENT", defaults.max_concurrent)?;
        let budget = budget::BudgetConfig::from_vars(
            cufflink_settings::process_var,
            budget::detect_cgroup(std::path::Path::new("/")),
            max_concurrent,
        )?;
        let max_memory_bytes =
            env_var("WASM_MAX_MEMORY_MB", defaults.max_memory_bytes / MIB)? * MIB;
        budget.log_startup(max_memory_bytes);
        let limits = Self {
            max_memory_bytes: budget.clamped_memory_cap(max_memory_bytes),
            max_tables: env_var("WASM_MAX_TABLES", defaults.max_tables)?,
            max_table_elements: env_var("WASM_MAX_TABLE_ELEMENTS", defaults.max_table_elements)?,
            max_instances: env_var("WASM_MAX_INSTANCES", defaults.max_instances)?,
            max_guest_len: env_var("WASM_MAX_GUEST_LEN_MB", defaults.max_guest_len / MIB)? * MIB,
            max_response_bytes: env_var("WASM_MAX_RESPONSE_MB", defaults.max_response_bytes / MIB)?
                * MIB,
            max_concurrent,
            epoch_tick: Duration::from_millis(env_var(
                "WASM_EPOCH_TICK_MS",
                defaults.epoch_tick.as_millis() as u64,
            )?),
            module_cache_max: env_var("WASM_MODULE_CACHE_MAX", defaults.module_cache_max)?,
            admission_wait: Duration::from_millis(env_var(
                "WASM_ADMISSION_WAIT_MS",
                defaults.admission_wait.as_millis() as u64,
            )?),
            retry_after: Duration::from_secs(env_var(
                "WASM_ADMISSION_RETRY_AFTER_SECS",
                defaults.retry_after.as_secs(),
            )?),
            budget,
        };
        limits.validate()?;
        Ok(limits)
    }

    fn validate(&self) -> Result<()> {
        [
            ("WASM_MAX_MEMORY_MB", self.max_memory_bytes as u64),
            ("WASM_MAX_TABLES", u64::from(self.max_tables)),
            ("WASM_MAX_TABLE_ELEMENTS", self.max_table_elements as u64),
            ("WASM_MAX_INSTANCES", u64::from(self.max_instances)),
            ("WASM_MAX_GUEST_LEN_MB", self.max_guest_len as u64),
            ("WASM_MAX_RESPONSE_MB", self.max_response_bytes as u64),
            ("WASM_MAX_CONCURRENT", self.max_concurrent as u64),
            ("WASM_MODULE_CACHE_MAX", self.module_cache_max as u64),
            ("WASM_EPOCH_TICK_MS", self.epoch_tick.as_millis() as u64),
            (
                "WASM_ADMISSION_WAIT_MS",
                self.admission_wait.as_millis() as u64,
            ),
            (
                "WASM_ADMISSION_RETRY_AFTER_SECS",
                self.retry_after.as_secs(),
            ),
        ]
        .iter()
        .find(|(_, value)| *value == 0)
        .map_or(Ok(()), |(name, _)| {
            Err(eyre::eyre!("{name} must be greater than 0"))
        })
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
enum Interrupt {
    #[error("deadline exceeded")]
    Deadline,
    #[error("platform shutting down")]
    Shutdown,
    #[error("cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompileSource {
    Deploy,
    Storage,
}

impl CompileSource {
    pub fn label(self) -> &'static str {
        match self {
            CompileSource::Deploy => "deploy",
            CompileSource::Storage => "s3",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error(
        "WASM persistence is not configured — \
         set S3_ENDPOINT / S3_REGION / S3_ACCESS_KEY / S3_SECRET_KEY / S3_BUCKET"
    )]
    NoStorage,
    #[error("{0}")]
    Compile(String),
    #[error("WASM module imports functions this platform does not provide: {}", .0.join(", "))]
    UnknownImports(Vec<String>),
    #[error("{0}")]
    Storage(String),
}

pub struct CompiledModule {
    pub module: Module,
    pub instance_pre: InstancePre<HostState>,
    pub service_id: Uuid,
    pub version: i32,
    pub initial_memory: usize,
}

pub(crate) struct RouteAdmission<'a> {
    pub handler: &'a str,
    pub route: budget::RouteKey,
    pub class: Class,
    pub max_concurrent: Option<u32>,
    pub budget: Duration,
}

impl RouteAdmission<'_> {
    fn limit(&self) -> Option<(budget::RouteKey, u32)> {
        self.max_concurrent.map(|max| (self.route, max))
    }
}

const WASM_PAGE: u64 = 64 * 1024;

fn initial_memory(module: &Module) -> usize {
    let required = module.resources_required();
    let memory = required
        .max_initial_memory_size
        .unwrap_or_default()
        .saturating_mul(WASM_PAGE)
        .saturating_mul(u64::from(required.num_memories));
    let tables = required
        .max_initial_table_size
        .unwrap_or_default()
        .saturating_mul(budget::TABLE_ELEMENT_BYTES as u64)
        .saturating_mul(u64::from(required.num_tables));
    usize::try_from(memory.saturating_add(tables)).unwrap_or(usize::MAX)
}

struct CacheEntry {
    module: Arc<CompiledModule>,
    last_used: AtomicU64,
}

type LoadingGuards = Mutex<HashMap<(Uuid, i32), Arc<tokio::sync::Mutex<()>>>>;

struct ModuleCache {
    entries: Mutex<HashMap<(Uuid, i32), CacheEntry>>,
    loading: LoadingGuards,
    max_entries: usize,
    clock: AtomicU64,
    #[cfg(test)]
    compiles: std::sync::atomic::AtomicUsize,
}

impl ModuleCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            loading: Mutex::new(HashMap::new()),
            max_entries,
            clock: AtomicU64::new(0),
            #[cfg(test)]
            compiles: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<(Uuid, i32), CacheEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn get(&self, key: (Uuid, i32)) -> Option<Arc<CompiledModule>> {
        let entries = self.entries();
        let entry = entries.get(&key)?;
        entry.last_used.store(
            self.clock.fetch_add(1, Ordering::Relaxed),
            Ordering::Relaxed,
        );
        Some(Arc::clone(&entry.module))
    }

    fn insert(&self, key: (Uuid, i32), module: Arc<CompiledModule>) {
        let mut entries = self.entries();
        if !entries.contains_key(&key) && entries.len() >= self.max_entries {
            if let Some(victim) = entries
                .iter()
                .min_by_key(|(_, e)| e.last_used.load(Ordering::Relaxed))
                .map(|(k, _)| *k)
            {
                entries.remove(&victim);
            }
        }
        entries.insert(
            key,
            CacheEntry {
                module,
                last_used: AtomicU64::new(self.clock.fetch_add(1, Ordering::Relaxed)),
            },
        );
    }

    fn remove(&self, key: (Uuid, i32)) {
        self.entries().remove(&key);
    }

    fn retain(&self, keep: impl Fn(&(Uuid, i32)) -> bool) {
        self.entries().retain(|key, _| keep(key));
    }

    fn len(&self) -> usize {
        self.entries().len()
    }

    fn loading_guard(&self, key: (Uuid, i32)) -> Arc<tokio::sync::Mutex<()>> {
        self.loading
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(key)
            .or_default()
            .clone()
    }

    fn loading_done(&self, key: (Uuid, i32)) {
        self.loading
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&key);
    }
}

#[derive(Clone)]
pub struct WasmEngine {
    engine: Engine,
    linker: Arc<Linker<HostState>>,
    host_fn_names: Arc<HashSet<&'static str>>,
    limits: Arc<WasmLimits>,
    permits: Arc<Semaphore>,
    budget: Arc<budget::Budget>,
    modules: Arc<ModuleCache>,
    storage: Arc<RwLock<Option<S3Storage>>>,
    artifact_index: Arc<std::sync::OnceLock<deadpool_postgres::Pool>>,
    fingerprint: Arc<str>,
    shutdown: Shutdown,
}

impl WasmEngine {
    pub fn new(limits: WasmLimits, shutdown: Shutdown) -> Result<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(wt)?;

        let mut linker = Linker::new(&engine);
        let host_fn_names = host::register_host_functions(&mut linker)?;
        start_epoch_ticker(&engine, limits.epoch_tick)?;
        let memory_budget = budget::Budget::new(
            limits.budget.clone(),
            limits.max_concurrent,
            limits.max_memory_bytes,
            limits.admission_wait,
            limits.retry_after,
        );
        memory_budget.start_pressure_gate()?;

        Ok(Self {
            fingerprint: precompiled::fingerprint(&engine).into(),
            engine,
            linker: Arc::new(linker),
            host_fn_names: Arc::new(host_fn_names),
            permits: Arc::new(Semaphore::new(limits.max_concurrent)),
            budget: Arc::new(memory_budget),
            modules: Arc::new(ModuleCache::new(limits.module_cache_max)),
            limits: Arc::new(limits),
            storage: Arc::new(RwLock::new(None)),
            artifact_index: Arc::default(),
            shutdown,
        })
    }

    pub async fn set_storage(&self, storage: S3Storage) {
        let mut s = self.storage.write().await;
        *s = Some(storage);
    }

    pub(crate) fn s3_key(service_id: Uuid, version: i32) -> String {
        format!("wasm/{}/v{}.wasm", service_id, version)
    }

    pub async fn is_persisted(&self, service_id: Uuid, version: i32, size: u64) -> bool {
        let storage = self.storage.read().await;
        match storage.as_ref() {
            Some(s3) => s3
                .head(&Self::s3_key(service_id, version))
                .await
                .is_ok_and(|actual| actual == size),
            None => false,
        }
    }

    pub async fn has_storage(&self) -> bool {
        self.storage.read().await.is_some()
    }

    pub fn limits(&self) -> &WasmLimits {
        &self.limits
    }

    pub fn host_fn_names(&self) -> &HashSet<&'static str> {
        &self.host_fn_names
    }

    pub fn unknown_imports(&self, module: &Module) -> Vec<String> {
        module
            .imports()
            .filter(|import| {
                import.module() != "env" || !self.host_fn_names.contains(import.name())
            })
            .map(|import| format!("{}::{}", import.module(), import.name()))
            .collect()
    }

    pub async fn compile(
        &self,
        wasm_bytes: Vec<u8>,
        source: CompileSource,
    ) -> Result<Module, ExecutionError> {
        let engine = self.engine.clone();
        let started = Instant::now();
        let module = tokio::task::spawn_blocking(move || Module::from_binary(&engine, &wasm_bytes))
            .await
            .map_err(|e| ExecutionError::Runtime(format!("compile task failed: {e}")))?
            .map_err(|e| ExecutionError::Runtime(e.to_string()))?;
        #[cfg(test)]
        self.modules.compiles.fetch_add(1, Ordering::Relaxed);
        metrics::histogram!(
            "cufflink_wasm_module_compile_seconds",
            "source" => source.label(),
        )
        .record(started.elapsed().as_secs_f64());
        Ok(module)
    }

    fn prepare(
        &self,
        module: Module,
        service_id: Uuid,
        version: i32,
    ) -> Result<Arc<CompiledModule>, LoadError> {
        let unknown = self.unknown_imports(&module);
        if !unknown.is_empty() {
            return Err(LoadError::UnknownImports(unknown));
        }
        let instance_pre = self
            .linker
            .instantiate_pre(&module)
            .map_err(|e| LoadError::Compile(e.to_string()))?;
        Ok(Arc::new(CompiledModule {
            initial_memory: initial_memory(&module),
            module,
            instance_pre,
            service_id,
            version,
        }))
    }

    pub async fn load_module(
        &self,
        service_id: Uuid,
        version: i32,
        wasm_bytes: &[u8],
    ) -> Result<(), LoadError> {
        if !self.has_storage().await {
            return Err(LoadError::NoStorage);
        }
        let module = self
            .compile(wasm_bytes.to_vec(), CompileSource::Deploy)
            .await
            .map_err(|e| LoadError::Compile(e.to_string()))?;
        let compiled = self.prepare(module, service_id, version)?;

        let key = Self::s3_key(service_id, version);
        let expected_size = wasm_bytes.len() as u64;
        {
            let storage = self.storage.read().await;
            let s3 = storage.as_ref().ok_or(LoadError::NoStorage)?;
            s3.upload(&key, wasm_bytes.to_vec(), "application/wasm")
                .await
                .map_err(|e| {
                    LoadError::Storage(format!("Failed to persist WASM to S3 (key={key}): {e}"))
                })?;
            let actual_size = s3.head(&key).await.map_err(|e| {
                LoadError::Storage(format!("Failed to verify WASM upload (key={key}): {e}"))
            })?;
            if actual_size != expected_size {
                return Err(LoadError::Storage(format!(
                    "WASM upload size mismatch (key={key}): uploaded {expected_size} bytes, S3 reports {actual_size}"
                )));
            }
        }

        self.replace_precompiled(
            service_id,
            version,
            &sha256_hex(wasm_bytes),
            &compiled.module,
        )
        .await;
        self.modules.insert((service_id, version), compiled);
        info!(
            service_id = %service_id,
            version = version,
            size_bytes = expected_size,
            "WASM module loaded and persisted"
        );
        Ok(())
    }

    pub async fn missing_handler_exports(
        &self,
        service_id: Uuid,
        version: i32,
        wasm_sha256: &str,
        expected: &[&str],
    ) -> Result<Vec<String>> {
        if expected.is_empty() {
            return Ok(vec![]);
        }
        let compiled = self
            .get_module(service_id, version, Some(wasm_sha256))
            .await
            .ok_or_else(|| {
                eyre::eyre!(
                    "missing_handler_exports: module {} v{} is neither loaded nor in storage",
                    service_id,
                    version
                )
            })?;
        let exported_fns: HashSet<String> = compiled
            .module
            .exports()
            .filter_map(|e| match e.ty() {
                ExternType::Func(_) => Some(e.name().to_string()),
                _ => None,
            })
            .collect();
        Ok(expected
            .iter()
            .filter(|h| !exported_fns.contains(**h))
            .map(|h| (*h).to_string())
            .collect())
    }

    pub async fn delete_persisted(&self, service_id: Uuid, version: i32) {
        let storage = self.storage.read().await;
        let Some(ref s3) = *storage else {
            return;
        };
        let key = Self::s3_key(service_id, version);
        if let Err(e) = s3.delete(&key).await {
            tracing::warn!(
                service_id = %service_id,
                version = version,
                key = %key,
                error = %e,
                "Failed to delete orphaned WASM binary from S3"
            );
        } else {
            info!(
                service_id = %service_id,
                version = version,
                key = %key,
                "Deleted orphaned WASM binary from S3"
            );
        }

        drop(storage);
        self.modules.remove((service_id, version));
    }

    pub async fn get_module(
        &self,
        service_id: Uuid,
        version: i32,
        expected_sha256: Option<&str>,
    ) -> Option<Arc<CompiledModule>> {
        let key = (service_id, version);
        if let Some(m) = self.modules.get(key) {
            return Some(m);
        }
        let guard = self.modules.loading_guard(key);
        let _loading = guard.lock().await;
        let loaded = match self.modules.get(key) {
            Some(m) => Some(m),
            None => {
                self.load_from_storage(service_id, version, expected_sha256)
                    .await
            }
        };
        self.modules.loading_done(key);
        loaded
    }

    async fn load_from_storage(
        &self,
        service_id: Uuid,
        version: i32,
        expected_sha256: Option<&str>,
    ) -> Option<Arc<CompiledModule>> {
        let started = Instant::now();
        let precompiled = match expected_sha256 {
            Some(wasm_sha256) => {
                self.load_precompiled(service_id, version, wasm_sha256)
                    .await
            }
            None => precompiled::Precompiled::Unusable,
        };
        let missing = match precompiled {
            precompiled::Precompiled::Loaded(module) => {
                match self.prepare(module, service_id, version) {
                    Ok(compiled) => {
                        self.modules
                            .insert((service_id, version), Arc::clone(&compiled));
                        precompiled::record_load("cwasm", started);
                        return Some(compiled);
                    }
                    Err(e) => {
                        tracing::warn!(
                            service_id = %service_id,
                            version,
                            error = %e,
                            "precompiled module could not be prepared; compiling from the .wasm"
                        );
                        None
                    }
                }
            }
            precompiled::Precompiled::Missing { stale } => Some(stale),
            precompiled::Precompiled::Unusable => None,
        };
        let compiled = self
            .compile_from_storage(service_id, version, expected_sha256)
            .await?;
        precompiled::record_load("compile", started);
        if let (Some(stale), Some(wasm_sha256)) = (missing, expected_sha256) {
            let engine = self.clone();
            let module = compiled.module.clone();
            let wasm_sha256 = wasm_sha256.to_string();
            self.shutdown.spawn("store_precompiled", async move {
                engine
                    .store_precompiled(service_id, version, &wasm_sha256, &module, stale.as_ref())
                    .await
            });
        }
        Some(compiled)
    }

    async fn compile_from_storage(
        &self,
        service_id: Uuid,
        version: i32,
        expected_sha256: Option<&str>,
    ) -> Option<Arc<CompiledModule>> {
        let storage = self.storage.read().await;
        let s3 = storage.as_ref()?;
        let key = Self::s3_key(service_id, version);
        let wasm_bytes = match s3.download(&key).await {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::debug!(
                    service_id = %service_id,
                    version = version,
                    key = %key,
                    error = %e,
                    "WASM module not available from S3"
                );
                return None;
            }
        };
        drop(storage);
        let actual = sha256_hex(&wasm_bytes);
        match expected_sha256 {
            Some(expected) if expected != actual => {
                metrics::counter!(
                    "cufflink_wasm_hash_mismatch_total",
                    "service_id" => service_id.to_string(),
                )
                .increment(1);
                tracing::error!(
                    service_id = %service_id,
                    version,
                    expected,
                    actual = %actual,
                    "WASM binary in storage does not match its deployment's hash; refusing to load it"
                );
                return None;
            }
            Some(_) => {}
            None => tracing::warn!(
                service_id = %service_id,
                version,
                "WASM module loaded without a recorded hash"
            ),
        }
        info!(
            service_id = %service_id,
            version = version,
            key = %key,
            "Loading WASM from S3"
        );
        let prepared = match self.compile(wasm_bytes, CompileSource::Storage).await {
            Ok(module) => self.prepare(module, service_id, version),
            Err(e) => Err(LoadError::Compile(e.to_string())),
        };
        match prepared {
            Ok(compiled) => {
                self.modules
                    .insert((service_id, version), Arc::clone(&compiled));
                Some(compiled)
            }
            Err(e) => {
                tracing::error!(
                    service_id = %service_id,
                    version = version,
                    key = %key,
                    error = %e,
                    "WASM binary found in S3 but failed to compile — possible corruption"
                );
                None
            }
        }
    }

    pub async fn remove_module(&self, service_id: Uuid, version: i32) {
        self.modules.remove((service_id, version));
    }

    pub fn retain_versions(&self, current: &HashMap<Uuid, i32>) {
        self.modules.retain(|(service_id, version)| {
            current
                .get(service_id)
                .is_some_and(|current| version >= current)
        });
    }

    pub fn cache_len(&self) -> usize {
        self.modules.len()
    }

    pub(crate) fn admits_before_running(&self) -> bool {
        self.budget.enforces()
    }

    pub(crate) async fn admit_route(
        &self,
        compiled: &CompiledModule,
        identity: &crate::registry::ServiceIdentity,
        route: RouteAdmission<'_>,
    ) -> Result<Admission, ExecutionError> {
        let started = tokio::time::Instant::now();
        self.budget
            .admit(
                budget::Ticket {
                    class: route.class,
                    kind: InvocationKind::Route,
                    identity,
                    handler: route.handler,
                    route: route.limit(),
                    initial_memory: compiled.initial_memory,
                    budget: route.budget,
                },
                &self.permits,
            )
            .await
            .map(|admission| admission.counting_from(started))
    }

    pub(crate) async fn route_slot(
        &self,
        identity: &crate::registry::ServiceIdentity,
        route: RouteAdmission<'_>,
    ) -> Result<Option<OwnedSemaphorePermit>, ExecutionError> {
        let ticket = budget::Ticket {
            class: route.class,
            kind: InvocationKind::Route,
            identity,
            handler: route.handler,
            route: route.limit(),
            initial_memory: 0,
            budget: route.budget,
        };
        let until = self.budget.admission_deadline(&ticket);
        self.budget.route_slot(&ticket, until).await
    }

    pub async fn headroom(
        &self,
        identity: &crate::registry::ServiceIdentity,
        handler: Option<&str>,
    ) -> usize {
        self.budget.room(Some(identity), handler).await
    }

    pub async fn headroom_for_any_handler(&self) -> usize {
        self.budget.room(None, None).await
    }

    pub fn publish_gauges(&self) {
        self.budget.publish();
    }

    pub(crate) fn hold(
        &self,
        identity: &crate::registry::ServiceIdentity,
        class: Class,
        handler: &str,
        kind: InvocationKind,
    ) -> budget::Hold {
        budget::Hold::on(self.budget.account(identity, class, handler, kind))
    }

    pub fn refusal(&self, refused: budget::Refused, ran: bool) -> ExecutionError {
        match refused {
            budget::Refused::Budget(capacity) => {
                self.budget.refusal(capacity.reason, capacity.holding, ran)
            }
            budget::Refused::Cap(cap) => ExecutionError::Response(cap.to_string()),
        }
    }

    pub fn capacity_deferrals_allowed(&self) -> u32 {
        self.budget.capacity_retries_job()
    }

    async fn admit(
        &self,
        inv: &Invocation,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<Admission, ExecutionError> {
        let admitted = self.budget.admit(
            budget::Ticket {
                class: inv.class,
                kind: inv.kind,
                identity: &inv.identity,
                handler: &inv.handler,
                route: None,
                initial_memory: inv.compiled.initial_memory,
                budget: inv.budget,
            },
            &self.permits,
        );
        let hard_deadline = async {
            self.shutdown.cancelled().await;
            tokio::time::sleep(
                self.shutdown
                    .remaining()
                    .saturating_sub(crate::shutdown::KILL_MARGIN),
            )
            .await;
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(ExecutionError::Cancelled),
            _ = hard_deadline, if self.budget.waits_outside_budget(inv.class, inv.kind) => {
                Err(self.budget.overloaded(budget::Reason::Memory, false))
            }
            admitted = admitted => admitted,
        }
    }

    #[cfg(test)]
    pub(crate) async fn run_text(
        &self,
        state: Arc<AppState>,
        inv: Invocation,
    ) -> Result<String, ExecutionError> {
        self.run(state, inv).await.map(|output| output.text)
    }

    pub async fn run(
        &self,
        state: Arc<AppState>,
        inv: Invocation,
    ) -> Result<GuestOutput, ExecutionError> {
        let kind = inv.kind.label();
        let started = Instant::now();
        let result = self.run_admitted(state, inv).await;
        let label = result.as_ref().map_or_else(ExecutionError::label, |_| "ok");
        metrics::histogram!(
            "cufflink_wasm_handler_duration_seconds",
            "kind" => kind,
            "result" => label,
        )
        .record(started.elapsed().as_secs_f64());
        result
    }

    pub async fn run_retrying_capacity(
        &self,
        state: Arc<AppState>,
        kind: InvocationKind,
        request: String,
        invocation: impl Fn(String) -> Invocation,
    ) -> Result<GuestOutput, ExecutionError> {
        for _ in 0..self.budget.capacity_retries() {
            let run = self
                .run(Arc::clone(&state), invocation(request.clone()))
                .await;
            match run {
                Err(ExecutionError::Overloaded { .. }) => {
                    metrics::counter!(
                        "cufflink_wasm_capacity_retries_total",
                        "kind" => kind.label(),
                    )
                    .increment(1);
                }
                ended => return ended,
            }
        }
        self.run(state, invocation(request)).await
    }

    async fn run_admitted(
        &self,
        state: Arc<AppState>,
        mut inv: Invocation,
    ) -> Result<GuestOutput, ExecutionError> {
        let cancel = inv.cancel.clone().unwrap_or_default();
        let early = match inv.admission.take() {
            Some(admission) => Some(admission),
            None if self.budget.waits_outside_budget(inv.class, inv.kind) => {
                Some(self.admit(&inv, &cancel).await?)
            }
            None => None,
        };
        let budget = inv.budget;
        let from = early
            .as_ref()
            .and_then(Admission::budget_from)
            .unwrap_or_else(tokio::time::Instant::now);
        let deadline = from.into_std() + budget;
        tokio::time::timeout_at(from + budget, async {
            let policies = state
                .tenants
                .policies_or_load(inv.identity.tenant_id)
                .await
                .map_err(|e| {
                    ExecutionError::Runtime(format!("tenant policies unavailable: {e}"))
                })?;
            let admission = match early {
                Some(admission) => admission,
                None => self.admit(&inv, &cancel).await?,
            };
            let in_flight = GaugeGuard::enter(
                metrics::gauge!("cufflink_wasm_inflight", "kind" => inv.kind.label()),
            );
            let span = tracing::Span::current();
            let engine = self.clone();
            tokio::task::spawn_blocking(move || {
                let _in_flight = in_flight;
                let _guard = span.enter();
                engine.execute(state, &inv, admission, policies, deadline, cancel)
            })
            .await
            .map_err(|e| ExecutionError::Runtime(format!("WASM task join error: {e}")))?
        })
        .await
        .unwrap_or(Err(ExecutionError::DeadlineExceeded { budget }))
    }

    fn execute(
        &self,
        state: Arc<AppState>,
        inv: &Invocation,
        admission: Admission,
        policies: Arc<PolicySet>,
        deadline: Instant,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<GuestOutput, ExecutionError> {
        let (_slots, charge) = admission.into_parts();
        let _span = scoped_span!(
            "wasm_handler",
            inv.scope(),
            tenant = %inv.identity.tenant_slug,
            service = %inv.identity.service_name,
        )
        .entered();
        let kind = inv.kind.label();

        let RequestContext { auth, caller } =
            serde_json::from_str(&inv.request_json).unwrap_or_default();
        let call_depth = host::caller_depth(&caller);
        let auth = crate::router::jobs::AuthSnapshot::from_auth(auth);
        let guest_limits = GuestLimits::new(
            Arc::clone(&self.limits),
            Arc::clone(&policies),
            host::LimitContext {
                tenant: inv.identity.tenant_slug.clone(),
                service: inv.identity.service_name.clone(),
                handler: inv.handler.clone(),
                kind: inv.kind,
            },
            charge,
        );
        let nats_publish_scope = state
            .tenants
            .publish_scope(inv.identity.tenant_id, &inv.identity.tenant_slug);
        let mut store = Store::new(
            &self.engine,
            HostState::new(HostStateInit {
                app_state: state,
                identity: inv.identity.clone(),
                service_version: inv.compiled.version,
                auth,
                call_depth,
                nats_publish_policy: NatsPublishPolicy::from_manifest_publishes(
                    inv.publishes.clone(),
                ),
                nats_publish_scope,
                extensions: inv.extensions.as_slice().into(),
                deadline,
                cancel,
                limits: Arc::clone(&self.limits),
                guest_limits,
                kind: inv.kind,
                policies,
            }),
        );
        store.limiter(|host| &mut host.guest_limits);
        store.set_epoch_deadline(1);
        let shutdown = self.shutdown.clone();
        store.epoch_deadline_callback(move |ctx| {
            if Instant::now() >= ctx.data().deadline {
                Err(Interrupt::Deadline.into())
            } else if ctx.data().cancel.is_cancelled() {
                Err(Interrupt::Cancelled.into())
            } else if shutdown.hard_deadline_reached() {
                Err(Interrupt::Shutdown.into())
            } else {
                Ok(UpdateDeadline::Continue(1))
            }
        });

        let on_thread = store
            .data_mut()
            .guest_limits
            .charge_mut()
            .map(budget::enter_thread);
        let result = self.call_handler(&mut store, inv);
        store
            .data_mut()
            .db
            .abandon_all(host::AbandonReason::of(&result));
        if let Some(sample) = store.data().guest_limits.sample() {
            self.budget
                .record_sample(inv.identity.service_id, &inv.handler, sample);
        }
        drop(on_thread);

        metrics::histogram!("cufflink_wasm_memory_peak_bytes", "kind" => kind)
            .record(store.data().guest_limits.peak_memory() as f64);
        if let Ok(response) = &result {
            metrics::histogram!("cufflink_wasm_response_bytes", "kind" => kind)
                .record(response.text.len() as f64);
        }
        result
    }

    fn call_handler(
        &self,
        store: &mut Store<HostState>,
        inv: &Invocation,
    ) -> Result<GuestOutput, ExecutionError> {
        let instance =
            inv.compiled
                .instance_pre
                .instantiate(&mut *store)
                .map_err(|e| match store.data().guest_limits.budget_denial() {
                    Some(denial) => unrun(denial, &self.budget),
                    None => classify(store, e, inv.budget, &self.budget),
                })?;

        let memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| ExecutionError::Runtime("WASM module has no exported memory".into()))?;

        let alloc = instance
            .get_typed_func::<i32, i32>(&mut *store, "alloc")
            .or_else(|_| instance.get_typed_func::<i32, i32>(&mut *store, "__alloc"))
            .map_err(|e| ExecutionError::Runtime(e.to_string()))?;

        let request_bytes = inv.request_json.as_bytes();
        let request_len = i32::try_from(request_bytes.len()).map_err(|_| {
            ExecutionError::Runtime("request exceeds the guest's addressable memory".into())
        })?;
        let request_ptr = alloc
            .call(&mut *store, request_len)
            .map_err(|e| classify(store, e, inv.budget, &self.budget))?;
        let request_ptr = usize::try_from(request_ptr).map_err(|_| {
            ExecutionError::Runtime("guest allocator returned a negative pointer".into())
        })?;
        memory
            .write(&mut *store, request_ptr, request_bytes)
            .map_err(|e| {
                ExecutionError::Runtime(format!("write request into guest memory: {e}"))
            })?;

        let handle_fn = instance
            .get_typed_func::<(i32, i32), i32>(&mut *store, &inv.handler)
            .map_err(|_| ExecutionError::HandlerNotFound(inv.handler.clone()))?;

        let response_ptr = handle_fn
            .call(&mut *store, (request_ptr as i32, request_len))
            .map_err(|e| classify(store, e, inv.budget, &self.budget))?;

        let mut hold = store.data().hold();
        let text = read_response(
            memory.data(&*store),
            response_ptr,
            self.limits.max_response_bytes,
            |len| hold.grow(len),
        )
        .map_err(|read| match read {
            ResponseRead::Invalid(error) => error,
            ResponseRead::Refused(capacity) => {
                let limits = &mut store.data_mut().guest_limits;
                limits.host_refused(budget::Refused::Budget(capacity));
                limits.budget_denial().map_or_else(
                    || self.budget.overloaded(capacity.reason, true),
                    |denial| denied(denial, &self.budget, true),
                )
            }
        })?;
        Ok(GuestOutput { text, hold })
    }
}

#[derive(Default, serde::Deserialize)]
struct RequestContext {
    #[serde(default)]
    auth: serde_json::Value,
    #[serde(default)]
    caller: serde_json::Value,
}

pub struct GuestOutput {
    pub text: String,
    pub hold: budget::Hold,
}

enum ResponseRead {
    Invalid(ExecutionError),
    Refused(budget::Capacity),
}

impl From<ExecutionError> for ResponseRead {
    fn from(error: ExecutionError) -> Self {
        Self::Invalid(error)
    }
}

fn classify(
    store: &Store<HostState>,
    err: wasmtime::Error,
    budget: Duration,
    memory: &budget::Budget,
) -> ExecutionError {
    let limits = &store.data().guest_limits;
    let trapped = err.downcast_ref::<Trap>().is_some();
    match err.downcast_ref::<Interrupt>() {
        Some(Interrupt::Deadline) => ExecutionError::DeadlineExceeded { budget },
        Some(Interrupt::Shutdown) => ExecutionError::ShuttingDown,
        Some(Interrupt::Cancelled) => ExecutionError::Cancelled,
        None => match (limits.denied(), limits.budget_denial()) {
            (Some(denied), _) => ExecutionError::MemoryLimit {
                requested: denied.requested,
                limit: denied.limit,
            },
            (None, Some(denial)) if trapped => denied(denial, memory, true),
            _ if trapped => ExecutionError::Trap(err.to_string()),
            _ => ExecutionError::Runtime(err.to_string()),
        },
    }
}

fn denied(denial: host::BudgetDenial, memory: &budget::Budget, ran: bool) -> ExecutionError {
    memory.refusal(denial.reason, denial.holding, ran)
}

fn unrun(denial: host::BudgetDenial, memory: &budget::Budget) -> ExecutionError {
    denied(denial, memory, false)
}

fn read_response(
    memory: &[u8],
    ptr: i32,
    max_bytes: usize,
    charge: impl FnOnce(usize) -> Result<(), budget::Capacity>,
) -> Result<String, ResponseRead> {
    let start = usize::try_from(ptr)
        .map_err(|_| ExecutionError::Response("negative response pointer".into()))?;
    let len_bytes: [u8; 4] = memory
        .get(start..start + 4)
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| ExecutionError::Response("response header out of bounds".into()))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > max_bytes {
        metrics::counter!(
            "cufflink_wasm_limit_events_total",
            "limit" => "response",
            "mode" => "enforce",
        )
        .increment(1);
        return Err(ExecutionError::Response(format!(
            "response of {len} bytes exceeds the {max_bytes} byte cap"
        ))
        .into());
    }
    let body = memory
        .get(start + 4..start + 4 + len)
        .ok_or_else(|| ExecutionError::Response("response body out of bounds".into()))?;
    charge(len).map_err(ResponseRead::Refused)?;
    String::from_utf8(body.to_vec())
        .map_err(|e| ExecutionError::Response(format!("response is not UTF-8: {e}")).into())
}

fn start_epoch_ticker(engine: &Engine, tick: Duration) -> Result<()> {
    let weak = engine.weak();
    std::thread::Builder::new()
        .name("wasmtime-epoch".into())
        .spawn(move || {
            while let Some(engine) = weak.upgrade() {
                engine.increment_epoch();
                drop(engine);
                std::thread::sleep(tick);
            }
        })
        .map(drop)
        .map_err(|e| eyre::eyre!("spawn wasmtime epoch ticker: {e}"))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::OnceLock;

    pub(crate) fn engine_with(limits: WasmLimits) -> WasmEngine {
        WasmEngine::new(limits, Shutdown::new(Duration::from_secs(45))).expect("engine construct")
    }

    pub(crate) fn engine() -> WasmEngine {
        engine_with(WasmLimits::default())
    }

    fn global_metrics() -> metrics_exporter_prometheus::PrometheusHandle {
        static HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> = OnceLock::new();
        HANDLE
            .get_or_init(|| {
                let recorder = cufflink_telemetry::metrics_builder()
                    .expect("metrics builder")
                    .build_recorder();
                let handle = recorder.handle();
                metrics::set_global_recorder(recorder).expect("first global recorder");
                handle
            })
            .clone()
    }

    fn guest(handler: &str, body: &str) -> String {
        format!(
            r#"
            (module
              (import "env" "db_query" (func $db_query (param i32 i32) (result i32)))
              (memory (export "memory") 2)
              (global $heap (mut i32) (i32.const 1024))
              (data (i32.const 0) "{{\"ok\":true}}")
              (func (export "alloc") (param $n i32) (result i32)
                (local $p i32)
                (local.set $p (global.get $heap))
                (global.set $heap (i32.add (local.get $p) (local.get $n)))
                (local.get $p))
              (func $respond_ok (result i32)
                (i32.store (i32.const 512) (i32.const 11))
                (memory.copy (i32.const 516) (i32.const 0) (i32.const 11))
                (i32.const 512))
              (func (export "{handler}") (param i32 i32) (result i32)
                {body})
            )
            "#
        )
    }

    pub(crate) fn install(engine: &WasmEngine, wat: &str) -> (Uuid, Arc<CompiledModule>) {
        let module = Module::new(&engine.engine, wat).expect("compile WAT");
        let id = Uuid::new_v4();
        let compiled = engine.prepare(module, id, 1).expect("prepare");
        engine.modules.insert((id, 1), Arc::clone(&compiled));
        (id, compiled)
    }

    pub(crate) fn invocation(
        compiled: Arc<CompiledModule>,
        handler: &str,
        kind: InvocationKind,
        budget: Duration,
    ) -> Invocation {
        Invocation {
            compiled,
            handler: handler.to_string(),
            request_json: r#"{"method":"POST","handler":"h","body":null}"#.to_string(),
            identity: crate::registry::ServiceIdentity {
                tenant_id: Uuid::nil(),
                tenant_slug: "acme".to_string(),
                service_id: uuid::Uuid::nil(),
                schema: "test_service".to_string(),
                service_name: "test-service".to_string(),
            },
            publishes: Some(vec![]),
            extensions: Vec::new(),
            kind,
            scope_kind: None,
            budget,
            cancel: None,
            class: match kind {
                InvocationKind::Job | InvocationKind::Subscription => Class::Background,
                InvocationKind::OnMigrate => Class::Committed,
                _ => Class::Interactive,
            },
            admission: None,
        }
    }

    const SPIN: &str = "(loop $l (br $l)) (unreachable)";

    fn enforce_wasm_limits() -> Vec<crate::repo::tenant_policies::PolicyRow> {
        vec![crate::tenant_catalog::tests::policy_row(
            Uuid::nil(),
            crate::policy::PolicyKey::WasmLimits,
            crate::policy::PolicyMode::Enforce,
        )]
    }

    #[test]
    fn limits_default_and_reject_zero() {
        let defaults = WasmLimits::default();
        assert_eq!(defaults.max_memory_bytes, 512 * MIB);
        assert_eq!(defaults.max_concurrent, 256);
        assert_eq!(defaults.epoch_tick, Duration::from_millis(100));
        assert!(defaults.validate().is_ok());
        let zero = WasmLimits {
            max_concurrent: 0,
            ..WasmLimits::default()
        };
        let err = zero.validate().unwrap_err().to_string();
        assert!(err.contains("WASM_MAX_CONCURRENT"), "{err}");
        assert_eq!(defaults.admission_wait, Duration::from_millis(1000));
        assert_eq!(defaults.retry_after, Duration::from_secs(2));
        let no_wait = WasmLimits {
            admission_wait: Duration::ZERO,
            ..WasmLimits::default()
        };
        let err = no_wait.validate().unwrap_err().to_string();
        assert!(err.contains("WASM_ADMISSION_WAIT_MS"), "{err}");
    }

    #[test]
    fn budget_override_applies_to_routes_only() {
        assert_eq!(
            InvocationKind::Hook.budget(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        assert_eq!(InvocationKind::Job.label(), "job");
    }

    #[test]
    fn execution_error_strings_are_pinned() {
        assert_eq!(
            ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(30)
            }
            .to_string(),
            "WASM execution timed out after 30s"
        );
        assert_eq!(
            ExecutionError::ShuttingDown.to_string(),
            "WASM execution aborted: platform is shutting down"
        );
        assert_eq!(
            ExecutionError::HandlerNotFound("x".into()).to_string(),
            "Handler 'x' not found in WASM module"
        );
        assert!(ExecutionError::MemoryLimit {
            requested: 2,
            limit: 1
        }
        .to_string()
        .contains("exceeded its memory limit"));
    }

    #[test]
    fn unknown_imports_lists_foreign_modules_and_unregistered_names() {
        let engine = engine();
        let wat = r#"
            (module
              (import "env" "db_query" (func (param i32 i32) (result i32)))
              (import "env" "no_such_host_fn" (func (param i32 i32) (result i32)))
              (import "wasi_snapshot_preview1" "fd_write" (func (param i32 i32 i32 i32) (result i32)))
            )
        "#;
        let module = Module::new(&engine.engine, wat).unwrap();
        assert_eq!(
            engine.unknown_imports(&module),
            vec![
                "env::no_such_host_fn".to_string(),
                "wasi_snapshot_preview1::fd_write".to_string()
            ]
        );
        let err = engine
            .prepare(module, Uuid::new_v4(), 1)
            .err()
            .expect("unknown imports are rejected");
        assert!(
            matches!(&err, LoadError::UnknownImports(list) if list.len() == 2),
            "{err}"
        );
        assert!(err.to_string().contains("env::no_such_host_fn"), "{err}");
    }

    #[test]
    fn every_sdk_guest_imports_only_registered_host_fns() {
        use cufflink_test_guests::Profile;
        if let Some(reason) = cufflink_test_guests::UNAVAILABLE {
            cufflink_testkit::infra::skip(reason);
            return;
        }
        let engine = engine();
        let refused: Vec<String> = cufflink_test_guests::names()
            .flat_map(|guest| Profile::ALL.map(|profile| (guest, profile)))
            .filter_map(|(guest, profile)| {
                let bytes = cufflink_test_guests::wasm(guest, profile).expect("built guest");
                let module = Module::from_binary(&engine.engine, &bytes).expect("compile guest");
                let unknown = engine.unknown_imports(&module);
                (!unknown.is_empty())
                    .then(|| format!("{guest} ({}): {}", profile.name(), unknown.join(", ")))
            })
            .collect();
        assert!(
            refused.is_empty(),
            "guests import what the platform does not provide:\n{}",
            refused.join("\n")
        );
    }

    #[tokio::test]
    async fn load_module_rejects_unknown_imports_before_uploading() {
        let engine = engine();
        let storage = cufflink_storage::in_memory("test");
        engine
            .set_storage(S3Storage::from_inner(storage.clone()))
            .await;
        let id = Uuid::new_v4();
        let bytes = wat::parse_str(
            r#"(module (import "env" "no_such_host_fn" (func (param i32 i32) (result i32))))"#,
        )
        .unwrap();
        let err = engine.load_module(id, 1, &bytes).await.unwrap_err();
        assert!(matches!(err, LoadError::UnknownImports(_)), "{err}");
        assert!(
            S3Storage::from_inner(storage)
                .head(&WasmEngine::s3_key(id, 1))
                .await
                .is_err(),
            "a rejected module must not be persisted"
        );
        assert_eq!(engine.cache_len(), 0);
    }

    #[test]
    fn module_cache_evicts_least_recently_used_and_retains_current_versions() {
        let cache = ModuleCache::new(2);
        let engine = engine();
        let module = Module::new(&engine.engine, "(module)").unwrap();
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        for (id, version) in [(a, 1), (b, 1)] {
            cache.insert(
                (id, version),
                engine.prepare(module.clone(), id, version).unwrap(),
            );
        }
        assert!(cache.get((a, 1)).is_some(), "touching a makes b the LRU");
        cache.insert((c, 1), engine.prepare(module.clone(), c, 1).unwrap());
        assert_eq!(cache.len(), 2);
        assert!(cache.get((b, 1)).is_none(), "b was evicted");
        assert!(cache.get((a, 1)).is_some() && cache.get((c, 1)).is_some());

        let engine = engine_with(WasmLimits {
            module_cache_max: 8,
            ..WasmLimits::default()
        });
        let module = Module::new(&engine.engine, "(module)").unwrap();
        for (id, version) in [(a, 1), (a, 2), (a, 3), (b, 1), (c, 1)] {
            engine.modules.insert(
                (id, version),
                engine.prepare(module.clone(), id, version).unwrap(),
            );
        }
        engine.retain_versions(&HashMap::from([(a, 2), (c, 0)]));
        assert_eq!(engine.cache_len(), 3);
        assert!(engine.modules.get((a, 2)).is_some());
        assert!(
            engine.modules.get((a, 3)).is_some(),
            "a deploy in flight keeps its module"
        );
        assert!(
            engine.modules.get((c, 1)).is_some(),
            "a first deploy in flight keeps its module"
        );
    }

    #[tokio::test]
    async fn concurrent_misses_compile_once() {
        let engine = engine();
        engine
            .set_storage(S3Storage::from_inner(cufflink_storage::in_memory("test")))
            .await;
        let id = Uuid::new_v4();
        let bytes = wat::parse_str(guest("h", "(call $respond_ok)")).unwrap();
        engine.load_module(id, 1, &bytes).await.expect("load");
        engine.remove_module(id, 1).await;
        assert_eq!(engine.modules.compiles.load(Ordering::Relaxed), 1);

        let loads = (0..8).map(|_| {
            let engine = engine.clone();
            async move { engine.get_module(id, 1, None).await.is_some() }
        });
        let outcomes = futures_util::future::join_all(loads).await;
        assert!(outcomes.iter().all(|ok| *ok));
        assert_eq!(
            engine.modules.compiles.load(Ordering::Relaxed),
            2,
            "eight concurrent misses must share one compile"
        );
        assert!(engine.get_module(Uuid::new_v4(), 1, None).await.is_none());
    }

    #[tokio::test]
    async fn load_module_refuses_the_text_format() {
        let engine = engine();
        engine
            .set_storage(S3Storage::from_inner(cufflink_storage::in_memory("test")))
            .await;
        let err = engine
            .load_module(Uuid::new_v4(), 1, b"(module)")
            .await
            .unwrap_err();
        assert!(matches!(err, LoadError::Compile(_)), "{err}");
    }

    #[tokio::test]
    async fn load_module_without_storage_is_a_hard_error() {
        let engine = engine();
        let err = engine
            .load_module(Uuid::new_v4(), 1, &[0u8; 16])
            .await
            .unwrap_err();
        assert!(matches!(err, LoadError::NoStorage), "{err}");
        assert!(!engine.has_storage().await);
    }

    #[test]
    fn s3_key_is_stable() {
        assert_eq!(
            WasmEngine::s3_key(Uuid::nil(), 42),
            "wasm/00000000-0000-0000-0000-000000000000/v42.wasm"
        );
    }

    #[tokio::test]
    async fn missing_handler_exports_finds_undeclared_and_non_function_exports() {
        let engine = engine();
        let wat = r#"
            (module
                (memory (export "audit") 1)
                (func (export "validate") (param i32 i32) (result i32) i32.const 0)
            )
        "#;
        let (id, _) = install(&engine, wat);
        assert_eq!(
            engine
                .missing_handler_exports(id, 1, "unread", &["validate", "audit", "nope"])
                .await
                .unwrap(),
            vec!["audit".to_string(), "nope".to_string()]
        );
        assert!(engine
            .missing_handler_exports(Uuid::new_v4(), 1, "unread", &[])
            .await
            .unwrap()
            .is_empty());
        assert!(engine
            .missing_handler_exports(Uuid::new_v4(), 1, "unread", &["validate"])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn exports_are_inspected_after_the_module_was_evicted() {
        let engine = engine();
        engine
            .set_storage(S3Storage::from_inner(cufflink_storage::in_memory("test")))
            .await;
        let id = Uuid::new_v4();
        let bytes = wat::parse_str(guest("validate", "(call $respond_ok)")).unwrap();
        let hash = sha256_hex(&bytes);
        engine.load_module(id, 1, &bytes).await.expect("load");

        engine.retain_versions(&HashMap::new());
        assert_eq!(
            engine.cache_len(),
            0,
            "a reload drops the module of a service that is not running"
        );
        assert_eq!(
            engine
                .missing_handler_exports(id, 1, &hash, &["validate", "nope"])
                .await
                .expect("inspected from storage"),
            vec!["nope".to_string()]
        );
        assert_eq!(
            engine.cache_len(),
            1,
            "and the deploy goes on with it cached"
        );

        engine.retain_versions(&HashMap::new());
        let refused = engine
            .missing_handler_exports(id, 1, &sha256_hex(b"another binary"), &["validate"])
            .await
            .expect_err("storage is not trusted to hold what was deployed");
        assert!(
            refused
                .to_string()
                .contains("neither loaded nor in storage"),
            "{refused}"
        );
    }

    #[test]
    fn read_response_bounds_length_and_memory() {
        let mut memory = vec![0u8; 64];
        memory[8..12].copy_from_slice(&5u32.to_le_bytes());
        memory[12..17].copy_from_slice(b"hello");
        let read = |memory: &[u8], ptr, max| read_response(memory, ptr, max, |_| Ok(()));
        assert!(matches!(read(&memory, 8, 64), Ok(text) if text == "hello"));
        let refused = budget::Capacity {
            reason: budget::Reason::Memory,
            requested: 5,
            holding: 5,
        };
        assert!(matches!(
            read_response(&memory, 8, 64, |len| {
                assert_eq!(len, 5, "charged before the copy");
                Err(refused)
            }),
            Err(ResponseRead::Refused(capacity)) if capacity == refused
        ));
        assert!(matches!(
            read(&memory, 8, 4),
            Err(ResponseRead::Invalid(ExecutionError::Response(msg))) if msg.contains("cap")
        ));
        memory[8..12].copy_from_slice(&1000u32.to_le_bytes());
        assert!(matches!(
            read(&memory, 8, 4096),
            Err(ResponseRead::Invalid(ExecutionError::Response(msg))) if msg.contains("out of bounds")
        ));
        assert!(read(&memory, -1, 64).is_err());
        assert!(read(&memory, 62, 64).is_err());
    }

    const PROBES: &str = r#"
        (module
          (import "env" "db_query" (func $db_query (param i32 i32) (result i32)))
          (import "env" "set_response" (func $set_response (param i32 i32)))
          (import "env" "get_host_response_len" (func $response_len (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "{\"ok\":true}")
          (data (i32.const 32) "\ff\fe")
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func $ok (result i32)
            (i32.store (i32.const 512) (i32.const 11))
            (memory.copy (i32.const 516) (i32.const 0) (i32.const 11))
            (i32.const 512))
          (func $expect_len (param $want i32)
            (if (i32.ne (call $response_len) (local.get $want)) (then (unreachable))))
          (func (export "oob") (param i32 i32) (result i32)
            (call $set_response (i32.const 0x7fff0000) (i32.const 4))
            (call $expect_len (i32.const 0))
            (call $ok))
          (func (export "not_utf8") (param i32 i32) (result i32)
            (call $set_response (i32.const 32) (i32.const 2))
            (call $expect_len (i32.const 0))
            (call $ok))
          (func (export "valid") (param i32 i32) (result i32)
            (call $set_response (i32.const 0) (i32.const 11))
            (call $expect_len (i32.const 11))
            (call $ok))
          (func (export "neg_len") (param i32 i32) (result i32)
            (if (i32.ne (call $db_query (i32.const 0) (i32.const -1)) (i32.const -1))
              (then (unreachable)))
            (call $ok))
        )
    "#;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_reads_are_bounded_and_negative_lengths_return_minus_one() {
        let handle = global_metrics();
        let engine = engine();
        let (_, compiled) = install(&engine, PROBES);
        let state = AppState::minimal(engine.clone()).await;
        for handler in ["oob", "not_utf8", "valid", "neg_len"] {
            let response = engine
                .run_text(
                    Arc::clone(&state),
                    invocation(
                        Arc::clone(&compiled),
                        handler,
                        InvocationKind::Route,
                        Duration::from_secs(5),
                    ),
                )
                .await
                .unwrap_or_else(|e| panic!("{handler}: {e}"));
            assert_eq!(response, r#"{"ok":true}"#, "{handler}");
        }
        let counted = handle.render().lines().any(|line| {
            line.starts_with("cufflink_wasm_limit_events_total")
                && line.contains("limit=\"length\"")
        });
        assert!(counted, "a negative length is a limit event");
    }

    const READBACK: &str = r#"
        (module
          (import "env" "set_response" (func $set_response (param i32 i32)))
          (import "env" "get_host_response" (func $get_host_response (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "{\"ok\":true}")
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "readback") (param i32 i32) (result i32)
            (call $set_response (i32.const 0) (i32.const 11))
            (if (i32.ne (call $get_host_response (i32.const 2048) (i32.const 64)) (i32.const 11))
              (then (unreachable)))
            (if (i32.ne (call $get_host_response (i32.const 2048) (i32.const -1)) (i32.const -1))
              (then (unreachable)))
            (i32.store (i32.const 512) (i32.const 11))
            (memory.copy (i32.const 516) (i32.const 2048) (i32.const 11))
            (i32.const 512))
        )
    "#;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_responses_are_not_capped_by_the_guest_length_limit() {
        let engine = engine_with(WasmLimits {
            max_guest_len: 16,
            ..WasmLimits::default()
        });
        let (_, compiled) = install(&engine, READBACK);
        let state = AppState::minimal(engine.clone()).await;
        let response = engine
            .run_text(
                state,
                invocation(
                    compiled,
                    "readback",
                    InvocationKind::Route,
                    Duration::from_secs(5),
                ),
            )
            .await
            .expect("readback");
        assert_eq!(response, r#"{"ok":true}"#);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_guest_that_survives_a_denied_grow_and_overruns_reports_the_deadline() {
        let engine = engine();
        let (_, compiled) = install(
            &engine,
            &guest(
                "grow_then_spin",
                "(drop (memory.grow (i32.const 16384))) (loop $l (br $l)) (unreachable)",
            ),
        );
        let state = AppState::minimal_with_policies(engine.clone(), &enforce_wasm_limits()).await;
        let err = engine
            .run_text(
                state,
                invocation(
                    compiled,
                    "grow_then_spin",
                    InvocationKind::Route,
                    Duration::from_millis(200),
                ),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, ExecutionError::DeadlineExceeded { .. }),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runaway_guest_is_killed_at_the_deadline() {
        let engine = engine();
        let (_, compiled) = install(&engine, &guest("spin", SPIN));
        let state = AppState::minimal(engine.clone()).await;
        let started = Instant::now();
        let err = engine
            .run_text(
                state,
                invocation(
                    compiled,
                    "spin",
                    InvocationKind::Route,
                    Duration::from_millis(200),
                ),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, ExecutionError::DeadlineExceeded { budget } if budget == Duration::from_millis(200)),
            "{err}"
        );
        assert!(started.elapsed() < Duration::from_millis(400));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            engine.permits.available_permits(),
            engine.limits.max_concurrent
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_guest_is_trapped_long_before_its_deadline() {
        let engine = engine();
        let (_, compiled) = install(&engine, &guest("spin", SPIN));
        let state = AppState::minimal(engine.clone()).await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let running = tokio::spawn({
            let (engine, cancel) = (engine.clone(), cancel.clone());
            async move {
                engine
                    .run_text(
                        state,
                        Invocation {
                            cancel: Some(cancel),
                            ..invocation(
                                compiled,
                                "spin",
                                InvocationKind::Job,
                                Duration::from_secs(30),
                            )
                        },
                    )
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!running.is_finished());
        let cancelled = Instant::now();
        cancel.cancel();
        let err = running.await.expect("join").unwrap_err();
        assert!(matches!(err, ExecutionError::Cancelled), "{err}");
        assert_eq!(err.label(), "cancelled");
        assert!(cancelled.elapsed() < Duration::from_secs(2));
        assert_eq!(
            engine.permits.available_permits(),
            engine.limits.max_concurrent
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn off_keeps_the_wait_for_a_slot_inside_the_budget() {
        let engine = engine_with(WasmLimits {
            max_concurrent: 1,
            budget: budget::BudgetConfig {
                mode: crate::policy::PolicyMode::Off,
                ..budget::BudgetConfig::default()
            },
            ..WasmLimits::default()
        });
        let (_, compiled) = install(&engine, &guest("h", "(call $respond_ok)"));
        let state = AppState::minimal(engine.clone()).await;
        let _held = Arc::clone(&engine.permits)
            .acquire_owned()
            .await
            .expect("the only permit");
        let started = Instant::now();
        let err = engine
            .run_text(
                state,
                invocation(
                    compiled,
                    "h",
                    InvocationKind::Route,
                    Duration::from_millis(150),
                ),
            )
            .await
            .expect_err("no slot frees");
        assert!(
            matches!(err, ExecutionError::DeadlineExceeded { .. }),
            "{err:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_invocation_stops_waiting_for_its_turn() {
        let engine = engine_with(WasmLimits {
            max_concurrent: 1,
            ..WasmLimits::default()
        });
        let (_, compiled) = install(&engine, &guest("h", "(call $respond_ok)"));
        let state = AppState::minimal(engine.clone()).await;
        let held = Arc::clone(&engine.permits)
            .acquire_owned()
            .await
            .expect("the only permit");
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let queued = |cancel| Invocation {
            cancel,
            ..invocation(
                Arc::clone(&compiled),
                "h",
                InvocationKind::Job,
                Duration::from_secs(30),
            )
        };
        let err = engine
            .run_text(Arc::clone(&state), queued(Some(cancel)))
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutionError::Cancelled), "{err}");
        drop(held);
        let response = engine
            .run_text(state, queued(None))
            .await
            .expect("response");
        assert!(!response.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_route_at_its_limit_waits_for_a_slot_then_is_refused_unrun() {
        let handle = global_metrics();
        let engine = engine_with(WasmLimits {
            admission_wait: Duration::from_millis(150),
            ..WasmLimits::default()
        });
        let identity = crate::registry::ServiceIdentity {
            tenant_id: Uuid::new_v4(),
            tenant_slug: "globex".to_string(),
            service_id: Uuid::new_v4(),
            schema: "loan_service".to_string(),
            service_name: "loan-service".to_string(),
        };
        let refusals = || {
            counter(
                &handle.render(),
                "cufflink_wasm_admission_refused_total",
                &["tenant=\"globex\"", "reason=\"route_concurrency\""],
            )
        };
        let before = refusals();
        let limited = |handler| RouteAdmission {
            handler,
            route: budget::RouteKey {
                service_id: identity.service_id,
                version: 1,
                index: usize::from(handler == "import"),
            },
            class: Class::Interactive,
            max_concurrent: Some(2),
            budget: Duration::from_secs(30),
        };
        let slot = || engine.route_slot(&identity, limited("export"));
        let first = slot().await.expect("first slot");
        let _second = slot().await.expect("second slot");

        let started = Instant::now();
        let err = slot().await.expect_err("a third waits, then is refused");
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert!(
            matches!(
                err,
                ExecutionError::Overloaded {
                    reason: budget::Reason::RouteConcurrency,
                    retry_after,
                    ran: false,
                } if retry_after == Duration::from_secs(2)
            ),
            "{err}"
        );
        assert_eq!(err.label(), "overloaded");
        assert_eq!(refusals(), before + 1.0);
        assert!(
            engine
                .route_slot(&identity, limited("import"))
                .await
                .is_ok(),
            "another route of the service has its own slots"
        );

        let waiting = tokio::spawn({
            let (engine, identity) = (engine.clone(), identity.clone());
            async move {
                engine
                    .route_slot(
                        &identity,
                        RouteAdmission {
                            handler: "export",
                            route: budget::RouteKey {
                                service_id: identity.service_id,
                                version: 1,
                                index: 0,
                            },
                            class: Class::Interactive,
                            max_concurrent: Some(2),
                            budget: Duration::from_secs(30),
                        },
                    )
                    .await
                    .map(drop)
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(first);
        waiting
            .await
            .expect("join")
            .expect("a waiter takes the slot freed within the wait");
    }

    fn budgeted(mode: &str, mib: &str) -> WasmEngine {
        engine_with(budgeted_limits(mode, mib))
    }

    fn budgeted_limits(mode: &str, mib: &str) -> WasmLimits {
        WasmLimits {
            budget: budget::BudgetConfig::from_vars(
                cufflink_settings::fixed(&[
                    ("WASM_MEMORY_BUDGET_MODE", mode),
                    ("WASM_MEMORY_BUDGET_MB", mib),
                    ("WASM_MEMORY_SERVICE_SHARE_PERCENT", "100"),
                    ("WASM_MEMORY_ADMIT_PERCENT", "100"),
                    ("WASM_THREAD_STACK_KB", "1024"),
                ]),
                None,
                256,
            )
            .expect("budget"),
            ..WasmLimits::default()
        }
    }

    fn holding_guest(pages: u32, ms: u32) -> String {
        format!(
            r#"(module
              (import "env" "util_sleep_ms" (func $sleep (param i32) (result i32)))
              (memory (export "memory") 2)
              (data (i32.const 0) "{{\"ok\":true}}")
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "hold") (param i32 i32) (result i32)
                (if (i32.eq (memory.grow (i32.const {pages})) (i32.const -1)) (then (unreachable)))
                (drop (call $sleep (i32.const {ms})))
                (i32.store (i32.const 512) (i32.const 11))
                (memory.copy (i32.const 516) (i32.const 0) (i32.const 11))
                (i32.const 512))
              (func (export "probe") (param i32 i32) (result i32)
                (drop (memory.grow (i32.const {pages})))
                (i32.store (i32.const 512) (i32.const 11))
                (memory.copy (i32.const 516) (i32.const 0) (i32.const 11))
                (i32.const 512)))"#
        )
    }

    const PAGES_24_MIB: u32 = 384;

    async fn burst(engine: &WasmEngine) -> (Vec<Result<String, ExecutionError>>, usize) {
        let (_, compiled) = install(engine, &holding_guest(PAGES_24_MIB, 300));
        let state = AppState::minimal(engine.clone()).await;
        let run = |compiled: Arc<CompiledModule>| {
            let (engine, state) = (engine.clone(), Arc::clone(&state));
            async move {
                engine
                    .run_text(
                        state,
                        invocation(
                            compiled,
                            "hold",
                            InvocationKind::Route,
                            Duration::from_secs(10),
                        ),
                    )
                    .await
            }
        };
        run(Arc::clone(&compiled))
            .await
            .expect("a first run measures the handler");
        let ledger = Arc::clone(engine.budget.ledger().expect("a ledger"));
        let sampling = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let sampler = tokio::spawn({
            let (ledger, sampling) = (Arc::clone(&ledger), Arc::clone(&sampling));
            async move {
                let mut peak = 0;
                while sampling.load(Ordering::Relaxed) {
                    peak = peak.max(ledger.in_use() - ledger.residue());
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                peak
            }
        });
        let results =
            futures_util::future::join_all((0..8).map(|_| run(Arc::clone(&compiled)))).await;
        sampling.store(false, Ordering::Relaxed);
        let peak = sampler.await.expect("sampler");
        assert_eq!(
            ledger.in_use(),
            ledger.residue(),
            "only idle threads' stacks remain"
        );
        (results, peak)
    }

    #[test]
    fn execute_reads_only_the_auth_and_the_caller_from_the_request() {
        let request = r#"{"body":{"rows":[[1],[2]]},"auth":{"sub":"u-1","roles":["a"]},"caller":{"depth":2},"method":"POST"}"#;
        let RequestContext { auth, caller } = serde_json::from_str(request).expect("context");
        let full: serde_json::Value = serde_json::from_str(request).expect("request");
        assert_eq!(host::caller_depth(&caller), 2);
        let snapshot = crate::router::jobs::AuthSnapshot::from_auth(auth).expect("auth");
        let expected = crate::router::jobs::AuthSnapshot::from_request_auth(&full).expect("auth");
        assert_eq!(
            (snapshot.subject, snapshot.claims),
            (expected.subject, expected.claims)
        );
        for other in [r#""text""#, "null", r#"{"auth":null}"#, "{"] {
            let RequestContext { auth, caller } = serde_json::from_str(other).unwrap_or_default();
            assert!(
                crate::router::jobs::AuthSnapshot::from_auth(auth).is_none(),
                "{other}"
            );
            assert_eq!(host::caller_depth(&caller), 0, "{other}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_waiting_for_memory_at_shutdown_is_answered_unrun_at_the_hard_deadline() {
        let limits = budgeted_limits("enforce", "64");
        let shutdown = Shutdown::new(crate::shutdown::KILL_MARGIN + Duration::from_millis(300));
        let engine = WasmEngine::new(limits, shutdown.clone()).expect("engine");
        let (_, compiled) = install(&engine, &holding_guest(1, 0));
        let state = AppState::minimal(engine.clone()).await;
        let ledger = Arc::clone(engine.budget.ledger().expect("a ledger"));
        let full = ledger
            .try_reserve(
                Class::Interactive,
                &budget::ServiceKey {
                    tenant: "globex".into(),
                    service: "loan-service".into(),
                },
                60 * MIB,
            )
            .expect("fills the budget");
        let waiting = tokio::spawn({
            let engine = engine.clone();
            async move {
                engine
                    .run_text(
                        state,
                        invocation(
                            compiled,
                            "hold",
                            InvocationKind::Subscription,
                            Duration::from_secs(30),
                        ),
                    )
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.trigger();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!waiting.is_finished(), "the drain does not end the wait");
        let answered = waiting.await.expect("join");
        assert!(
            matches!(answered, Err(ExecutionError::Overloaded { ran: false, .. })),
            "a not-run answer, which a subscription defers and naks: {answered:?}"
        );
        drop(full);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn under_enforce_a_burst_waits_for_the_budget_and_never_passes_it() {
        let engine = budgeted("enforce", "64");
        let (results, peak) = burst(&engine).await;
        assert!(
            peak <= 64 * MIB,
            "at most two 24 MiB guests hold memory at once, within the budget: {peak}"
        );
        let ran = results.iter().filter(|r| r.is_ok()).count();
        assert!(ran >= 2, "{results:?}");
        assert!(
            results.iter().all(|result| matches!(
                result,
                Ok(_) | Err(ExecutionError::Overloaded { ran: false, .. })
            )),
            "the rest wait and run, or are refused unrun: {results:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn under_warn_a_burst_runs_and_counts_what_would_have_waited() {
        let handle = global_metrics();
        let refused = || {
            counter(
                &handle.render(),
                "cufflink_wasm_admission_refused_total",
                &["mode=\"warn\"", "reason=\"memory\""],
            )
        };
        let before = refused();
        let engine = budgeted("warn", "64");
        let (results, _) = burst(&engine).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert!(refused() > before, "would-be refusals are counted");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_grow_past_the_budget_is_denied_at_once_and_attributed_only_to_a_trap() {
        let engine = budgeted("enforce", "64");
        let (_, compiled) = install(&engine, &holding_guest(1100, 0));
        let state = AppState::minimal(engine.clone()).await;
        let run = |handler: &str| {
            engine.run_text(
                Arc::clone(&state),
                invocation(
                    Arc::clone(&compiled),
                    handler,
                    InvocationKind::Route,
                    Duration::from_secs(5),
                ),
            )
        };
        let started = Instant::now();
        assert_eq!(
            run("probe").await.expect("the guest handled -1"),
            r#"{"ok":true}"#
        );
        assert!(started.elapsed() < Duration::from_secs(1), "never blocks");
        assert!(
            matches!(
                run("hold").await,
                Err(ExecutionError::TooLarge { requested, share }) if requested > share
            ),
            "a guest growing past the whole service share can never fit"
        );

        let engine = budgeted("enforce", "64");
        let (_, small) = install(&engine, &holding_guest(PAGES_24_MIB, 0));
        let state = AppState::minimal(engine.clone()).await;
        engine.budget.record_sample(Uuid::nil(), "hold", MIB);
        let ledger = Arc::clone(engine.budget.ledger().expect("a ledger"));
        let other = super::budget::ServiceKey {
            tenant: "globex".into(),
            service: "loan-service".into(),
        };
        let held = ledger
            .try_reserve(Class::Interactive, &other, 40 * MIB)
            .expect("fits");
        let err = engine
            .run_text(
                state,
                invocation(small, "hold", InvocationKind::Route, Duration::from_secs(5)),
            )
            .await
            .expect_err("denied mid-run");
        assert!(
            matches!(err, ExecutionError::Overloaded { ran: true, .. }),
            "{err:?}"
        );
        drop(held);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_guest_creeping_past_its_share_in_small_steps_is_too_large_not_overloaded() {
        let engine = budgeted("enforce", "64");
        let creep = r#"(module
          (memory (export "memory") 2)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "creep") (param i32 i32) (result i32)
            (if (i32.eq (memory.grow (i32.const 640)) (i32.const -1)) (then (unreachable)))
            (if (i32.eq (memory.grow (i32.const 640)) (i32.const -1)) (then (unreachable)))
            (i32.const 0)))"#;
        let (_, compiled) = install(&engine, creep);
        let state = AppState::minimal(engine.clone()).await;
        let err = engine
            .run_text(
                state,
                invocation(
                    compiled,
                    "creep",
                    InvocationKind::Subscription,
                    Duration::from_secs(5),
                ),
            )
            .await
            .expect_err("80 MiB can never fit a 64 MiB share");
        assert!(
            matches!(
                err,
                ExecutionError::TooLarge { requested, share }
                    if requested > share
            ),
            "a denied 40 MiB step of an 80 MiB guest is its own size, not capacity: {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_cap_is_enforced_or_warned_by_mode() {
        let hog = guest(
            "hog",
            "(if (i32.eq (memory.grow (i32.const 16384)) (i32.const -1)) (then (unreachable))) (call $respond_ok)",
        );
        let enforce = engine();
        let (_, compiled) = install(&enforce, &hog);
        let state = AppState::minimal_with_policies(enforce.clone(), &enforce_wasm_limits()).await;
        let err = enforce
            .run_text(
                state,
                invocation(compiled, "hog", InvocationKind::Job, Duration::from_secs(5)),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, ExecutionError::MemoryLimit { requested, limit } if requested > limit),
            "{err}"
        );

        let handle = global_metrics();
        let warn = engine();
        let (_, compiled) = install(&warn, &hog);
        let state = AppState::minimal(warn.clone()).await;
        let response = warn
            .run_text(
                state,
                invocation(compiled, "hog", InvocationKind::Job, Duration::from_secs(5)),
            )
            .await
            .expect("warn mode lets the guest finish");
        assert_eq!(response, r#"{"ok":true}"#);
        let rendered = handle.render();
        let warned = rendered.lines().any(|line| {
            line.starts_with("cufflink_wasm_limit_events_total")
                && line.contains("limit=\"memory\"")
                && line.contains("mode=\"warn\"")
        });
        assert!(warned, "{rendered}");
        assert!(
            rendered.contains("cufflink_wasm_memory_peak_bytes"),
            "peak memory is recorded on completion"
        );
    }

    #[test]
    fn a_tenant_cap_lowers_the_ceiling_but_never_raises_it() {
        let limits = WasmLimits {
            max_memory_bytes: 512 * MIB,
            ..WasmLimits::default()
        };
        let with = |memory_bytes: Option<usize>| {
            let rows = memory_bytes
                .map(|bytes| crate::repo::tenant_policies::PolicyRow {
                    config: serde_json::json!({ "memory_bytes": bytes }),
                    ..enforce_wasm_limits().remove(0)
                })
                .into_iter()
                .collect::<Vec<_>>();
            limits.memory_cap(&PolicySet::builtin_defaults().with_tenant_rows(&rows))
        };
        assert_eq!(with(None), 512 * MIB);
        assert_eq!(with(Some(64 * MIB)), 64 * MIB);
        assert_eq!(with(Some(512 * MIB)), 512 * MIB);
        assert_eq!(with(Some(4096 * MIB)), 512 * MIB);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tenant_memory_cap_applies_and_off_mode_allows() {
        let grow_65_mib = guest(
            "grow",
            "(if (i32.eq (memory.grow (i32.const 1040)) (i32.const -1)) (then (unreachable))) (call $respond_ok)",
        );
        let engine = engine();
        let (_, compiled) = install(&engine, &grow_65_mib);
        let run = |state: Arc<AppState>| {
            engine.run_text(
                state,
                invocation(
                    Arc::clone(&compiled),
                    "grow",
                    InvocationKind::Job,
                    Duration::from_secs(5),
                ),
            )
        };

        let capped = crate::repo::tenant_policies::PolicyRow {
            config: serde_json::json!({ "memory_bytes": 64 * MIB }),
            ..enforce_wasm_limits().remove(0)
        };
        let state = AppState::minimal_with_policies(engine.clone(), &[capped]).await;
        let err = run(state).await.unwrap_err();
        assert!(
            matches!(err, ExecutionError::MemoryLimit { limit, .. } if limit == 64 * MIB),
            "{err}"
        );

        let state = AppState::minimal(engine.clone()).await;
        assert!(run(state).await.is_ok(), "the platform cap is 512 MiB");

        let above_ceiling = crate::repo::tenant_policies::PolicyRow {
            config: serde_json::json!({ "memory_bytes": 4096 * MIB }),
            ..enforce_wasm_limits().remove(0)
        };
        let hog = guest(
            "hog",
            "(if (i32.eq (memory.grow (i32.const 16384)) (i32.const -1)) (then (unreachable))) (call $respond_ok)",
        );
        let (_, hog) = install(&engine, &hog);
        let state = AppState::minimal_with_policies(engine.clone(), &[above_ceiling]).await;
        let err = engine
            .run_text(
                state,
                invocation(hog, "hog", InvocationKind::Job, Duration::from_secs(5)),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, ExecutionError::MemoryLimit { limit, .. } if limit == 512 * MIB),
            "a tenant cap above WASM_MAX_MEMORY_MB is held to it: {err}"
        );

        let off = crate::repo::tenant_policies::PolicyRow {
            mode: crate::policy::PolicyMode::Off,
            config: serde_json::json!({ "memory_bytes": 64 * MIB }),
            ..enforce_wasm_limits().remove(0)
        };
        let state = AppState::minimal_with_policies(engine.clone(), &[off]).await;
        assert!(run(state).await.is_ok(), "off never denies");
    }

    async fn stored_module(engine: &WasmEngine) -> (Uuid, Vec<u8>) {
        let bytes = wat::parse_str(guest("h", "(call $respond_ok)")).expect("wat");
        let storage = S3Storage::from_inner(cufflink_storage::in_memory("cufflink"));
        let id = Uuid::new_v4();
        storage
            .upload(
                &WasmEngine::s3_key(id, 1),
                bytes.clone(),
                "application/wasm",
            )
            .await
            .expect("upload");
        engine.set_storage(storage).await;
        (id, bytes)
    }

    #[tokio::test]
    async fn get_module_accepts_matching_hash() {
        let engine = engine();
        let (id, bytes) = stored_module(&engine).await;
        let expected = sha256_hex(&bytes);
        assert!(engine.get_module(id, 1, Some(&expected)).await.is_some());
        assert_eq!(engine.cache_len(), 1);
    }

    #[tokio::test]
    async fn get_module_rejects_hash_mismatch_and_caches_nothing() {
        let handle = global_metrics();
        let engine = engine();
        let (id, bytes) = stored_module(&engine).await;
        let wrong = sha256_hex(b"what was deployed");
        assert!(engine.get_module(id, 1, Some(&wrong)).await.is_none());
        assert_eq!(engine.cache_len(), 0);
        assert!(handle.render().contains(&format!(
            "cufflink_wasm_hash_mismatch_total{{service_id=\"{id}\"}} 1"
        )));
        assert!(
            engine
                .get_module(id, 1, Some(&sha256_hex(&bytes)))
                .await
                .is_some(),
            "a refused load leaves no cache entry behind"
        );
    }

    #[tokio::test]
    async fn get_module_without_recorded_hash_loads() {
        let engine = engine();
        let (id, _) = stored_module(&engine).await;
        assert!(engine.get_module(id, 1, None).await.is_some());
    }

    #[test]
    fn sha256_hex_is_lowercase_hex_of_the_bytes() {
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    fn presigning_guest() -> String {
        r#"(module
          (import "env" "s3_presign_upload" (func $presign (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 2)
          (global $heap (mut i32) (i32.const 1024))
          (data (i32.const 0) "{\"ok\":true}")
          (data (i32.const 64) "cufflink")
          (data (i32.const 96) "wasm/x/v1.wasm")
          (data (i32.const 128) "photos/a.jpg")
          (data (i32.const 160) "image/jpeg")
          (func (export "alloc") (param $n i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $heap))
            (global.set $heap (i32.add (local.get $p) (local.get $n)))
            (local.get $p))
          (func $respond_ok (result i32)
            (i32.store (i32.const 512) (i32.const 11))
            (memory.copy (i32.const 516) (i32.const 0) (i32.const 11))
            (i32.const 512))
          (func $expect_ok (param $rc i32) (result i32)
            (if (i32.ne (local.get $rc) (i32.const 0)) (then unreachable))
            (call $respond_ok))
          (func (export "platform_key") (param i32 i32) (result i32)
            (call $expect_ok (call $presign (i32.const 64) (i32.const 8) (i32.const 96) (i32.const 14) (i32.const 160) (i32.const 10) (i32.const 60))))
          (func (export "app_key") (param i32 i32) (result i32)
            (call $expect_ok (call $presign (i32.const 64) (i32.const 8) (i32.const 128) (i32.const 12) (i32.const 160) (i32.const 10) (i32.const 60)))))"#
            .to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn storage_host_functions_refuse_platform_keys_in_every_mode() {
        let engine = engine();
        let (_, compiled) = install(&engine, &presigning_guest());
        let call = |state: Arc<AppState>, handler: &str| {
            engine.run_text(
                state,
                invocation(
                    Arc::clone(&compiled),
                    handler,
                    InvocationKind::Route,
                    Duration::from_secs(5),
                ),
            )
        };
        let state = AppState::minimal(engine.clone()).await;
        assert!(call(Arc::clone(&state), "app_key").await.is_ok());
        let err = call(state, "platform_key").await.unwrap_err();
        assert!(matches!(err, ExecutionError::Trap(_)), "{err}");

        for mode in [
            crate::policy::PolicyMode::Off,
            crate::policy::PolicyMode::Warn,
        ] {
            let row = crate::tenant_catalog::tests::policy_row(
                Uuid::nil(),
                crate::policy::PolicyKey::StorageScope,
                mode,
            );
            let state = AppState::minimal_with_policies(engine.clone(), &[row]).await;
            assert!(call(Arc::clone(&state), "app_key").await.is_ok(), "{mode}");
            let err = call(state, "platform_key").await.unwrap_err();
            assert!(
                matches!(err, ExecutionError::Trap(_)),
                "platform keys are refused under {mode}: {err}"
            );
        }
    }

    fn reserved_keys_guest() -> String {
        r#"(module
          (import "env" "s3_download_many" (func $download_many (param i32 i32) (result i32)))
          (import "env" "image_transform_multi" (func $transform_multi (param i32 i32 i32 i32 i32 i32) (result i32)))
          (import "env" "redis_mget" (func $mget (param i32 i32) (result i32)))
          (import "env" "get_host_response" (func $response (param i32 i32) (result i32)))
          (memory (export "memory") 2)
          (global $heap (mut i32) (i32.const 8192))
          (data (i32.const 0) "[{\"bucket\":\"cufflink\",\"key\":\"wasm/x/v1.wasm\"},{\"bucket\":\"cufflink\",\"key\":\"photos/a.png\"}]")
          (data (i32.const 128) "cufflink")
          (data (i32.const 160) "photos/a.png")
          (data (i32.const 192) "[{\"out_key\":\"wasm/x/v1.wasm\",\"max_dim\":1,\"quality\":80}]")
          (data (i32.const 256) "[{\"out_key\":\"photos/b.jpg\",\"max_dim\":1,\"quality\":80}]")
          (data (i32.const 320) "[\"flag\",\"auth:session\"]")
          (func (export "alloc") (param $n i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $heap))
            (global.set $heap (i32.add (local.get $p) (local.get $n)))
            (local.get $p))
          (func $reply (param $rc i32) (result i32)
            (i32.store8 (i32.const 1028)
              (select (i32.const 69) (i32.const 75) (i32.lt_s (local.get $rc) (i32.const 0))))
            (i32.store (i32.const 1024)
              (i32.add (call $response (i32.const 1029) (i32.const 4096)) (i32.const 1)))
            (i32.const 1024))
          (func (export "download_many") (param i32 i32) (result i32)
            (call $reply (call $download_many (i32.const 0) (i32.const 89))))
          (func (export "transform_platform") (param i32 i32) (result i32)
            (call $reply (call $transform_multi (i32.const 128) (i32.const 8) (i32.const 160) (i32.const 12) (i32.const 192) (i32.const 55))))
          (func (export "transform_app") (param i32 i32) (result i32)
            (call $reply (call $transform_multi (i32.const 128) (i32.const 8) (i32.const 160) (i32.const 12) (i32.const 256) (i32.const 53))))
          (func (export "mget_reserved") (param i32 i32) (result i32)
            (call $reply (call $mget (i32.const 320) (i32.const 23)))))"#
            .to_string()
    }

    fn png() -> Vec<u8> {
        let mut bytes = std::io::Cursor::new(Vec::new());
        ::image::RgbImage::new(2, 2)
            .write_to(&mut bytes, ::image::ImageFormat::Png)
            .expect("encode png");
        bytes.into_inner()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_storage_and_redis_calls_refuse_reserved_keys() {
        let engine = engine();
        let (_, compiled) = install(&engine, &reserved_keys_guest());
        let state = AppState::minimal(engine.clone()).await;
        state
            .storage
            .as_ref()
            .expect("storage")
            .upload_to_bucket("cufflink", "photos/a.png", png(), "image/png")
            .await
            .expect("seed image");
        let call = |handler: &str| {
            engine.run_text(
                Arc::clone(&state),
                invocation(
                    Arc::clone(&compiled),
                    handler,
                    InvocationKind::Route,
                    Duration::from_secs(5),
                ),
            )
        };

        let downloads = call("download_many").await.expect("download_many");
        let items: serde_json::Value =
            serde_json::from_str(downloads.strip_prefix('K').expect("ok")).expect("json");
        assert_eq!(items[0]["ok"], false, "{items}");
        assert_eq!(items[0]["error"], "storage access denied", "{items}");
        assert_eq!(
            items[1]["ok"], true,
            "the permitted item is served: {items}"
        );

        assert_eq!(
            call("transform_platform")
                .await
                .expect("transform")
                .chars()
                .next(),
            Some('E'),
            "an output key under wasm/ refuses the whole call"
        );
        assert!(call("transform_app")
            .await
            .expect("transform")
            .starts_with('K'));

        assert_eq!(
            call("mget_reserved").await.expect("mget"),
            format!("E{}", host::RESERVED_REDIS_KEY)
        );
    }

    fn counter(rendered: &str, name: &str, labels: &[&str]) -> f64 {
        rendered
            .lines()
            .filter(|line| line.starts_with(name) && labels.iter().all(|l| line.contains(l)))
            .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
            .sum()
    }

    fn fetching_guest() -> String {
        r#"(module
          (import "env" "http_fetch" (func $fetch (param i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 2)
          (global $heap (mut i32) (i32.const 1024))
          (data (i32.const 0) "{\"ok\":true}")
          (data (i32.const 64) "GET")
          (data (i32.const 96) "http://127.0.0.1:1/internal")
          (data (i32.const 160) "{}")
          (data (i32.const 192) "http://169.254.169.254/latest/")
          (func (export "alloc") (param $n i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $heap))
            (global.set $heap (i32.add (local.get $p) (local.get $n)))
            (local.get $p))
          (func $respond_ok (result i32)
            (i32.store (i32.const 512) (i32.const 11))
            (memory.copy (i32.const 516) (i32.const 0) (i32.const 11))
            (i32.const 512))
          (func (export "internal") (param i32 i32) (result i32)
            (drop (call $fetch (i32.const 64) (i32.const 3) (i32.const 96) (i32.const 27) (i32.const 160) (i32.const 2) (i32.const 0) (i32.const 0)))
            (call $respond_ok))
          (func (export "metadata") (param i32 i32) (result i32)
            (drop (call $fetch (i32.const 64) (i32.const 3) (i32.const 192) (i32.const 30) (i32.const 160) (i32.const 2) (i32.const 0) (i32.const 0)))
            (call $respond_ok)))"#
            .to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guest_fetches_go_through_the_egress_guard() {
        let handle = global_metrics();
        let engine = engine();
        let (_, compiled) = install(&engine, &fetching_guest());
        let call = |state: Arc<AppState>, handler: &str| {
            engine.run_text(
                state,
                invocation(
                    Arc::clone(&compiled),
                    handler,
                    InvocationKind::Route,
                    Duration::from_secs(5),
                ),
            )
        };
        let egress = |outcome: &str| {
            counter(
                &handle.render(),
                "cufflink_policy_violation_total",
                &["key=\"egress\"", &format!("outcome=\"{outcome}\"")],
            )
        };
        let metadata = || {
            counter(
                &handle.render(),
                "cufflink_egress_denied_total",
                &["reason=\"metadata\""],
            )
        };

        let (warned, denied, refused) = (egress("warned"), egress("denied"), metadata());
        let state = AppState::minimal(engine.clone()).await;
        call(Arc::clone(&state), "internal").await.expect("handler");
        call(state, "metadata").await.expect("handler");
        assert_eq!(
            egress("warned"),
            warned + 1.0,
            "warn records the internal target"
        );
        assert_eq!(
            metadata(),
            refused + 1.0,
            "metadata is refused in every mode"
        );

        let enforce = crate::tenant_catalog::tests::policy_row(
            Uuid::nil(),
            crate::policy::PolicyKey::Egress,
            crate::policy::PolicyMode::Enforce,
        );
        let state = AppState::minimal_with_policies(engine.clone(), &[enforce]).await;
        call(state, "internal").await.expect("handler");
        assert_eq!(egress("denied"), denied + 1.0, "enforce refuses it");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guests_cannot_publish_to_reserved_subjects() {
        let handle = global_metrics();
        let wat = r#"(module
          (import "env" "nats_publish" (func $publish (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 2)
          (global $heap (mut i32) (i32.const 1024))
          (data (i32.const 0) "{\"ok\":true}")
          (data (i32.const 64) "cufflink.wasm.victim.h")
          (data (i32.const 128) "{}")
          (func (export "alloc") (param $n i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $heap))
            (global.set $heap (i32.add (local.get $p) (local.get $n)))
            (local.get $p))
          (func (export "forge") (param i32 i32) (result i32)
            (if (i32.ne (call $publish (i32.const 64) (i32.const 22) (i32.const 128) (i32.const 2)) (i32.const -1))
              (then unreachable))
            (i32.store (i32.const 512) (i32.const 11))
            (memory.copy (i32.const 516) (i32.const 0) (i32.const 11))
            (i32.const 512)))"#;
        let engine = engine();
        let (_, compiled) = install(&engine, wat);
        let reserved = || {
            counter(
                &handle.render(),
                "cufflink_nats_publish_denied_total",
                &["reason=\"reserved\""],
            )
        };
        let before = reserved();
        let state = AppState::minimal(engine.clone()).await;
        engine
            .run_text(
                state,
                Invocation {
                    publishes: None,
                    ..invocation(
                        compiled,
                        "forge",
                        InvocationKind::Route,
                        Duration::from_secs(5),
                    )
                },
            )
            .await
            .expect("the publish returns -1 even under the legacy policy");
        assert_eq!(reserved(), before + 1.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oversized_response_handler_not_found_and_shutdown_are_reported() {
        let engine = engine_with(WasmLimits {
            max_response_bytes: 4,
            ..WasmLimits::default()
        });
        let (_, compiled) = install(&engine, &guest("h", "(call $respond_ok)"));
        let state = AppState::minimal(engine.clone()).await;
        let err = engine
            .run_text(
                Arc::clone(&state),
                invocation(
                    Arc::clone(&compiled),
                    "h",
                    InvocationKind::Hook,
                    Duration::from_secs(5),
                ),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutionError::Response(_)), "{err}");

        let err = engine
            .run_text(
                Arc::clone(&state),
                invocation(
                    Arc::clone(&compiled),
                    "missing",
                    InvocationKind::Hook,
                    Duration::from_secs(5),
                ),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutionError::HandlerNotFound(ref h) if h == "missing"));

        let engine = WasmEngine::new(
            WasmLimits {
                epoch_tick: Duration::from_millis(10),
                ..WasmLimits::default()
            },
            Shutdown::new(crate::shutdown::KILL_MARGIN),
        )
        .expect("engine construct");
        let (_, compiled) = install(&engine, &guest("spin", SPIN));
        let state = AppState::minimal(engine.clone()).await;
        engine.shutdown.trigger();
        let err = engine
            .run_text(
                state,
                invocation(
                    compiled,
                    "spin",
                    InvocationKind::Route,
                    Duration::from_secs(5),
                ),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutionError::ShuttingDown), "{err}");
    }
}
