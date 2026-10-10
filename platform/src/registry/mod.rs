mod identity;

pub use identity::ServiceIdentity;

use crate::deployer::ServiceManifest;
use crate::router::strict::{RouteTable, RouteTableError};
use crate::shutdown::Shutdown;
use crate::wasm_runtime::WasmEngine;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

pub const REGISTRY_CHANGED_SUBJECT: &str = "cufflink.registry.changed";

const RELOAD_ATTEMPTS: usize = 3;

const SELECT_ACTIVE: &str =
    "SELECT s.id, s.tenant_id, t.slug, s.name, s.current_version, d.manifest, d.wasm_hash, \
            d.commit_hash
     FROM services s
     JOIN tenants t ON t.id = s.tenant_id
     LEFT JOIN deployments d ON d.service_id = s.id AND d.version = s.current_version
     WHERE s.status = 'active'";

#[derive(Clone)]
pub struct ServiceRegistry {
    db: deadpool_postgres::Pool,
    nats: async_nats::Client,
    services: Arc<RwLock<RegistryIndex>>,
    version: Arc<AtomicU64>,
    mutations: Arc<AtomicU64>,
    changes: Arc<watch::Sender<u64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Membership {
    Routed,
    Absent,
}

#[derive(Debug, Clone)]
pub struct RegisteredService {
    pub service_id: Uuid,
    pub tenant_id: Uuid,
    pub tenant_slug: String,
    pub name: String,
    pub manifest: ServiceManifest,
    pub version: i32,
    pub wasm_hash: Option<String>,
    pub commit_hash: Option<String>,
}

#[derive(Default)]
struct RegistryIndex {
    by_key: HashMap<(Uuid, String), Arc<RegisteredService>>,
    by_slug: HashMap<(String, String), Arc<RegisteredService>>,
    route_tables: HashMap<Uuid, Arc<RouteTable>>,
    current_versions: HashMap<Uuid, i32>,
}

struct Routable {
    service: RegisteredService,
    routes: Option<RouteTable>,
}

impl Routable {
    fn new(service: RegisteredService) -> Result<Self, RouteTableError> {
        let routes = match service.manifest.routing {
            cufflink_types::RoutingMode::Legacy => None,
            cufflink_types::RoutingMode::Strict => {
                Some(RouteTable::build(&service.manifest.custom_routes)?)
            }
        };
        Ok(Self {
            service: RegisteredService {
                manifest: service.manifest.without_migration_bodies(),
                ..service
            },
            routes,
        })
    }
}

impl RegistryIndex {
    fn insert(&mut self, Routable { service, routes }: Routable) {
        self.current_versions
            .insert(service.service_id, service.version);
        match routes {
            Some(routes) => self
                .route_tables
                .insert(service.service_id, Arc::new(routes)),
            None => self.route_tables.remove(&service.service_id),
        };
        let service = Arc::new(service);
        self.by_slug.insert(
            (service.tenant_slug.clone(), service.name.clone()),
            Arc::clone(&service),
        );
        self.by_key
            .insert((service.tenant_id, service.name.clone()), service);
    }

    fn remove_by_id(&mut self, service_id: Uuid) {
        self.current_versions.remove(&service_id);
        self.route_tables.remove(&service_id);
        self.by_key
            .retain(|_, service| service.service_id != service_id);
        self.by_slug
            .retain(|_, service| service.service_id != service_id);
    }

    fn remove(&mut self, tenant_id: Uuid, service_name: &str) {
        let routed = self
            .by_key
            .get(&(tenant_id, service_name.to_string()))
            .map(|service| service.service_id);
        if let Some(service_id) = routed {
            self.remove_by_id(service_id);
        }
    }

    fn place(&mut self, row: LoadedRow) -> Placed {
        match row.deployed {
            None => {
                self.current_versions.insert(row.service_id, row.version);
                Placed::Unrouted
            }
            Some(Ok(service)) => {
                self.insert(service);
                Placed::Routed
            }
            Some(Err(skipped)) => {
                metrics::counter!("cufflink_registry_load_errors_total").increment(1);
                error!(
                    service = %skipped.service,
                    tenant = %skipped.tenant,
                    error = %skipped.error,
                    "Skipping service whose stored manifest cannot be decoded or routed"
                );
                Placed::Skipped
            }
        }
    }

    fn replace(&mut self, service_id: Uuid, row: Option<LoadedRow>) -> Membership {
        self.remove_by_id(service_id);
        match row.map(|row| self.place(row)) {
            Some(Placed::Routed) => Membership::Routed,
            Some(Placed::Unrouted | Placed::Skipped) | None => Membership::Absent,
        }
    }
}

enum Placed {
    Routed,
    Unrouted,
    Skipped,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct LoadReport {
    pub loaded: usize,
    pub skipped: usize,
}

impl ServiceRegistry {
    pub fn new(db: deadpool_postgres::Pool, nats: async_nats::Client) -> Self {
        Self {
            db,
            nats,
            services: Arc::new(RwLock::new(RegistryIndex::default())),
            version: Arc::new(AtomicU64::new(0)),
            mutations: Arc::new(AtomicU64::new(0)),
            changes: Arc::new(watch::Sender::new(0)),
        }
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    pub fn subscribe_version(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    fn changed(&self) {
        self.changes.send_modify(|changes| *changes += 1);
    }

    pub async fn load_all(&self) -> eyre::Result<LoadReport> {
        for _ in 0..RELOAD_ATTEMPTS {
            let epoch = self.mutations.load(Ordering::Acquire);
            let (index, report) = build_index(self.read_active().await?);
            if self.swap_unless_mutated(epoch, index).await {
                self.version.fetch_add(1, Ordering::Relaxed);
                metrics::gauge!("cufflink_registry_version").set(self.version() as f64);
                return Ok(report);
            }
        }
        Err(eyre::eyre!(
            "the registry changed during {RELOAD_ATTEMPTS} consecutive reloads"
        ))
    }

    async fn read_active(&self) -> eyre::Result<Vec<LoadedRow>> {
        Ok(self
            .db
            .get()
            .await?
            .query(SELECT_ACTIVE, &[])
            .await?
            .iter()
            .map(decode_row)
            .collect())
    }

    async fn read_one(&self, service_id: Uuid) -> eyre::Result<Option<LoadedRow>> {
        Ok(self
            .db
            .get()
            .await?
            .query_opt(&format!("{SELECT_ACTIVE} AND s.id = $1"), &[&service_id])
            .await?
            .as_ref()
            .map(decode_row))
    }

    pub async fn refresh(&self, service_id: Uuid) -> eyre::Result<Membership> {
        let refreshed = self
            .refresh_from(service_id, || self.read_one(service_id))
            .await;
        count_reload(refreshed.is_ok());
        refreshed
    }

    async fn refresh_from<Read, Reading>(
        &self,
        service_id: Uuid,
        read: Read,
    ) -> eyre::Result<Membership>
    where
        Read: Fn() -> Reading,
        Reading: Future<Output = eyre::Result<Option<LoadedRow>>>,
    {
        for _ in 0..RELOAD_ATTEMPTS {
            let epoch = *self.changes.borrow();
            let row = read().await?;
            let replaced = self
                .mutate_unless_changed(epoch, |index| index.replace(service_id, row))
                .await;
            if let Some(membership) = replaced {
                return Ok(membership);
            }
        }
        Err(eyre::eyre!(
            "the registry changed during {RELOAD_ATTEMPTS} consecutive reads of service {service_id}"
        ))
    }

    async fn swap_unless_mutated(&self, epoch: u64, index: RegistryIndex) -> bool {
        let mut services = self.services.write().await;
        let unchanged = self.mutations.load(Ordering::Acquire) == epoch;
        if unchanged {
            *services = index;
            self.changed();
        }
        unchanged
    }

    async fn mutate<T>(&self, change: impl FnOnce(&mut RegistryIndex) -> T) -> T {
        let mut services = self.services.write().await;
        self.mutated(&mut services, change)
    }

    async fn mutate_unless_changed<T>(
        &self,
        epoch: u64,
        change: impl FnOnce(&mut RegistryIndex) -> T,
    ) -> Option<T> {
        let mut services = self.services.write().await;
        let unchanged = *self.changes.borrow() == epoch;
        unchanged.then(|| self.mutated(&mut services, change))
    }

    fn mutated<T>(
        &self,
        index: &mut RegistryIndex,
        change: impl FnOnce(&mut RegistryIndex) -> T,
    ) -> T {
        let changed = change(index);
        self.mutations.fetch_add(1, Ordering::Release);
        self.changed();
        changed
    }

    pub async fn notify_change(&self) {
        crate::nats_conn::publish_change(&self.nats, REGISTRY_CHANGED_SUBJECT).await;
    }

    pub fn start_sync(&self, shutdown: &Shutdown, engine: Option<WasmEngine>, interval: Duration) {
        let registry = self.clone();
        crate::nats_conn::spawn_reload_loop(
            shutdown,
            "registry_sync",
            self.nats.clone(),
            REGISTRY_CHANGED_SUBJECT,
            interval,
            move || {
                let registry = registry.clone();
                let engine = engine.clone();
                async move { registry.reload(engine.as_ref()).await }
            },
        );
    }

    async fn reload(&self, engine: Option<&WasmEngine>) {
        let loaded = self.load_all().await;
        count_reload(loaded.is_ok());
        match loaded {
            Ok(report) => {
                if let Some(engine) = engine {
                    engine.retain_versions(&self.current_versions().await);
                }
                info!(
                    loaded = report.loaded,
                    skipped = report.skipped,
                    "Registry reloaded"
                );
            }
            Err(e) => {
                warn!(error = %e, "Registry reload failed; the previous index stays in place");
            }
        }
    }

    pub async fn get_by_slug(
        &self,
        tenant_slug: &str,
        service_name: &str,
    ) -> Option<Arc<RegisteredService>> {
        self.services
            .read()
            .await
            .by_slug
            .get(&(tenant_slug.to_string(), service_name.to_string()))
            .cloned()
    }

    pub(crate) async fn strict_routes(
        &self,
        tenant_slug: &str,
        service_name: &str,
    ) -> Option<(Arc<RegisteredService>, Arc<RouteTable>)> {
        let index = self.services.read().await;
        let service = index
            .by_slug
            .get(&(tenant_slug.to_string(), service_name.to_string()))?;
        index
            .route_tables
            .get(&service.service_id)
            .map(|routes| (Arc::clone(service), Arc::clone(routes)))
    }

    pub async fn get_by_tenant(&self, tenant_id: Uuid) -> Vec<Arc<RegisteredService>> {
        self.services
            .read()
            .await
            .by_key
            .values()
            .filter(|s| s.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    pub async fn remove(&self, tenant_id: Uuid, service_name: &str) {
        self.mutate(|index| index.remove(tenant_id, service_name))
            .await;
    }

    pub async fn all_services(&self) -> Vec<Arc<RegisteredService>> {
        self.services
            .read()
            .await
            .by_key
            .values()
            .cloned()
            .collect()
    }

    pub async fn current_versions(&self) -> HashMap<Uuid, i32> {
        self.services.read().await.current_versions.clone()
    }

    pub async fn count(&self) -> usize {
        self.services.read().await.by_key.len()
    }
}

fn count_reload(succeeded: bool) {
    let result = if succeeded { "ok" } else { "error" };
    metrics::counter!("cufflink_registry_reload_total", "result" => result).increment(1);
}

struct SkippedRow {
    service: String,
    tenant: String,
    error: String,
}

struct LoadedRow {
    service_id: Uuid,
    version: i32,
    deployed: Option<Result<Routable, SkippedRow>>,
}

fn build_index(rows: impl IntoIterator<Item = LoadedRow>) -> (RegistryIndex, LoadReport) {
    rows.into_iter().fold(
        (RegistryIndex::default(), LoadReport::default()),
        |(mut index, mut report), row| {
            match index.place(row) {
                Placed::Routed => report.loaded += 1,
                Placed::Skipped => report.skipped += 1,
                Placed::Unrouted => {}
            }
            (index, report)
        },
    )
}

fn decode_row(row: &tokio_postgres::Row) -> LoadedRow {
    let service_id = row.get("id");
    let version = row.get("current_version");
    let deployed = row
        .get::<_, Option<serde_json::Value>>("manifest")
        .map(|manifest| {
            let name: String = row.get("name");
            let tenant_slug: String = row.get("slug");
            serde_json::from_value(manifest)
                .map_err(|e| e.to_string())
                .and_then(|manifest| {
                    Routable::new(RegisteredService {
                        service_id,
                        tenant_id: row.get("tenant_id"),
                        tenant_slug: tenant_slug.clone(),
                        name: name.clone(),
                        manifest,
                        version,
                        wasm_hash: row.get("wasm_hash"),
                        commit_hash: row.get("commit_hash"),
                    })
                    .map_err(|e| e.to_string())
                })
                .map_err(|error| SkippedRow {
                    service: name,
                    tenant: tenant_slug,
                    error,
                })
        });
    LoadedRow {
        service_id,
        version,
        deployed,
    }
}

#[cfg(test)]
pub(crate) mod replica;

#[cfg(test)]
mod tests {
    use super::*;

    fn service(tenant_id: Uuid, slug: &str, name: &str, version: i32) -> RegisteredService {
        RegisteredService {
            service_id: Uuid::new_v4(),
            tenant_id,
            tenant_slug: slug.to_string(),
            name: name.to_string(),
            manifest: serde_json::from_value(serde_json::json!({
                "name": name,
                "version": "1",
                "tables": [],
                "events": [],
                "subscriptions": [],
                "custom_routes": [],
                "mode": "crud"
            }))
            .expect("manifest"),
            version,
            wasm_hash: None,
            commit_hash: None,
        }
    }

    fn routable(service: RegisteredService) -> Routable {
        Routable::new(service).expect("routable")
    }

    #[test]
    fn both_indexes_agree_through_insert_and_remove() {
        let tenant = Uuid::new_v4();
        let mut index = RegistryIndex::default();
        index.insert(routable(service(tenant, "acme", "orders", 1)));
        index.insert(routable(service(tenant, "acme", "orders", 2)));
        index.insert(routable(service(Uuid::new_v4(), "other", "orders", 1)));
        assert_eq!(index.by_key.len(), 2);
        assert_eq!(index.by_slug.len(), 2);
        assert_eq!(
            index.by_slug[&("acme".to_string(), "orders".to_string())].version,
            2,
            "a re-registration replaces the entry under both keys"
        );
        index.remove(tenant, "orders");
        assert!(!index.by_key.contains_key(&(tenant, "orders".to_string())));
        assert!(!index
            .by_slug
            .contains_key(&("acme".to_string(), "orders".to_string())));
        assert_eq!(index.by_key.len(), 1);
    }

    #[test]
    fn a_registered_manifest_holds_no_migration_text() {
        let mut deployed = service(Uuid::new_v4(), "acme", "notes", 1);
        deployed.manifest.migrations = vec![cufflink_types::SqlMigration {
            version: 1,
            name: "initial_tables".into(),
            sql: "CREATE TABLE notes ();".into(),
            no_transaction: false,
        }];
        let registered = routable(deployed).service.manifest.migrations;
        assert_eq!(
            registered
                .iter()
                .map(|m| (m.version, m.name.as_str(), m.sql.as_str()))
                .collect::<Vec<_>>(),
            [(1, "initial_tables", "")]
        );
    }

    fn deployed(service: RegisteredService) -> LoadedRow {
        LoadedRow {
            service_id: service.service_id,
            version: service.version,
            deployed: Some(Ok(routable(service))),
        }
    }

    #[test]
    fn build_index_skips_undecodable_rows_and_counts_them() {
        let bad = serde_json::from_value::<ServiceManifest>(serde_json::json!({ "bogus": 1 }))
            .expect_err("undecodable")
            .to_string();
        let (index, report) = build_index([
            deployed(service(Uuid::new_v4(), "acme", "orders", 1)),
            LoadedRow {
                service_id: Uuid::new_v4(),
                version: 2,
                deployed: Some(Err(SkippedRow {
                    service: "broken".into(),
                    tenant: "acme".into(),
                    error: bad,
                })),
            },
            deployed(service(Uuid::new_v4(), "acme", "billing", 4)),
        ]);
        assert_eq!(
            report,
            LoadReport {
                loaded: 2,
                skipped: 1
            }
        );
        assert_eq!(index.by_key.len(), 2);
        assert_eq!(
            index.current_versions.len(),
            2,
            "an undecodable service is not kept running"
        );
        assert!(index
            .by_slug
            .contains_key(&("acme".to_string(), "billing".to_string())));
    }

    #[test]
    fn a_service_awaiting_its_first_deploy_has_a_current_version_but_no_route() {
        let pending = Uuid::new_v4();
        let orders = service(Uuid::new_v4(), "acme", "orders", 3);
        let orders_id = orders.service_id;
        let (index, report) = build_index([
            LoadedRow {
                service_id: pending,
                version: 0,
                deployed: None,
            },
            deployed(orders),
        ]);
        assert_eq!(
            report,
            LoadReport {
                loaded: 1,
                skipped: 0
            }
        );
        assert_eq!(index.by_key.len(), 1);
        assert_eq!(
            index.current_versions,
            HashMap::from([(pending, 0), (orders_id, 3)])
        );
    }

    #[test]
    fn strict_services_carry_a_route_table_that_follows_the_service() {
        let tenant = Uuid::new_v4();
        let mut strict = service(tenant, "acme", "books", 1);
        strict.manifest.routing = cufflink_types::RoutingMode::Strict;
        strict.manifest.custom_routes = serde_json::from_value(serde_json::json!([
            {"method": "GET", "path": "/books/{id}/close", "handler": "close"}
        ]))
        .expect("routes");
        let id = strict.service_id;
        let mut index = RegistryIndex::default();
        index.insert(routable(strict.clone()));
        assert!(index.route_tables.contains_key(&id));
        index.insert(routable(RegisteredService {
            manifest: service(tenant, "acme", "books", 2).manifest,
            ..strict.clone()
        }));
        assert!(
            !index.route_tables.contains_key(&id),
            "a legacy redeploy drops the table"
        );
        index.insert(routable(strict));
        index.remove(tenant, "books");
        assert!(index.route_tables.is_empty());

        let conflicting = routed_strictly(service(tenant, "acme", "bad", 1), conflicting_routes());
        assert!(Routable::new(conflicting).is_err());
    }

    async fn offline_nats() -> async_nats::Client {
        crate::nats_conn::connect(
            "nats://127.0.0.1:1",
            "test",
            &crate::nats_conn::NatsAuth::Anonymous,
        )
        .await
        .expect("client")
    }

    async fn offline_registry() -> ServiceRegistry {
        ServiceRegistry::new(
            cufflink_db::build_pool("postgres://u:p@127.0.0.1:1/db", 1).expect("pool"),
            offline_nats().await,
        )
    }

    async fn refreshed(
        registry: &ServiceRegistry,
        service_id: Uuid,
        row: impl Fn() -> Option<LoadedRow>,
    ) -> Membership {
        registry
            .refresh_from(service_id, || async { Ok(row()) })
            .await
            .expect("refreshed")
    }

    async fn route(registry: &ServiceRegistry, service: &RegisteredService) {
        let routed = refreshed(registry, service.service_id, || {
            Some(deployed(service.clone()))
        })
        .await;
        assert_eq!(routed, Membership::Routed);
    }

    fn routed_strictly(service: RegisteredService, routes: serde_json::Value) -> RegisteredService {
        let mut service = service;
        service.manifest.routing = cufflink_types::RoutingMode::Strict;
        service.manifest.custom_routes = serde_json::from_value(routes).expect("routes");
        service
    }

    fn conflicting_routes() -> serde_json::Value {
        serde_json::json!([
            {"method": "GET", "path": "/a/{x}", "handler": "one"},
            {"method": "GET", "path": "/a/{y}", "handler": "two"}
        ])
    }

    #[test]
    fn remove_by_id_drops_every_index_of_the_service() {
        let tenant = Uuid::new_v4();
        let books = routed_strictly(
            service(tenant, "acme", "books", 3),
            serde_json::json!([{"method": "GET", "path": "/books/{id}", "handler": "book"}]),
        );
        let orders = service(tenant, "acme", "orders", 1);
        let awaiting = Uuid::new_v4();
        let mut index = RegistryIndex::default();
        index.insert(routable(books.clone()));
        index.insert(routable(orders.clone()));
        index.current_versions.insert(awaiting, 0);

        index.remove_by_id(books.service_id);
        assert_eq!(
            index.by_key.keys().collect::<Vec<_>>(),
            [&(tenant, "orders".to_string())]
        );
        assert_eq!(
            index.by_slug.keys().collect::<Vec<_>>(),
            [&("acme".to_string(), "orders".to_string())]
        );
        assert!(index.route_tables.is_empty());
        assert_eq!(
            index.current_versions,
            HashMap::from([(orders.service_id, 1), (awaiting, 0)])
        );

        index.remove_by_id(awaiting);
        index.remove_by_id(Uuid::new_v4());
        assert_eq!(
            index.current_versions,
            HashMap::from([(orders.service_id, 1)]),
            "a service awaiting its first deploy has a current version alone"
        );
        assert_eq!(index.by_key.len(), 1);

        index.remove(tenant, "books");
        assert_eq!(
            index.by_key.len(),
            1,
            "a name that is not routed removes nothing"
        );
        index.remove(tenant, "orders");
        assert!(index.by_key.is_empty() && index.by_slug.is_empty());
        assert!(index.current_versions.is_empty());
    }

    #[tokio::test]
    async fn a_version_whose_route_table_does_not_build_leaves_the_service_unrouted() {
        let registry = offline_registry().await;
        let tenant = Uuid::new_v4();
        let orders = service(tenant, "acme", "orders", 1);
        route(&registry, &orders).await;
        let broken = routed_strictly(
            RegisteredService {
                version: 2,
                ..orders.clone()
            },
            conflicting_routes(),
        );
        let unrouted = refreshed(&registry, orders.service_id, || {
            Some(LoadedRow {
                service_id: broken.service_id,
                version: broken.version,
                deployed: Some(Routable::new(broken.clone()).map_err(|error| SkippedRow {
                    service: broken.name.clone(),
                    tenant: broken.tenant_slug.clone(),
                    error: error.to_string(),
                })),
            })
        })
        .await;
        assert_eq!(unrouted, Membership::Absent);
        assert!(registry.get_by_slug("acme", "orders").await.is_none());
        assert_eq!(registry.count().await, 0);
        assert!(
            registry.current_versions().await.is_empty(),
            "as a reload leaves it: its modules are not kept"
        );
    }

    #[tokio::test]
    async fn a_reader_holding_a_clone_is_unaffected_by_the_swap() {
        let registry = offline_registry().await;
        let tenant = Uuid::new_v4();
        let orders = service(tenant, "acme", "orders", 1);
        route(&registry, &orders).await;
        let before = registry
            .get_by_slug("acme", "orders")
            .await
            .expect("registered");
        let (index, _) = build_index([deployed(RegisteredService {
            version: 2,
            ..orders
        })]);
        *registry.services.write().await = index;
        assert_eq!(before.version, 1);
        assert_eq!(
            registry
                .get_by_slug("acme", "orders")
                .await
                .map(|s| s.version),
            Some(2)
        );
        assert_eq!(registry.current_versions().await[&before.service_id], 2);
        registry.remove(tenant, "orders").await;
        assert_eq!(registry.count().await, 0);
        assert!(registry.current_versions().await.is_empty());
    }

    #[tokio::test]
    async fn a_snapshot_older_than_a_refresh_or_remove_is_not_swapped_in() {
        let registry = offline_registry().await;
        let tenant = Uuid::new_v4();
        let orders = service(tenant, "acme", "orders", 1);
        route(&registry, &orders).await;
        let version_of = || async {
            registry
                .get_by_slug("acme", "orders")
                .await
                .map(|s| s.version)
        };

        let epoch = registry.mutations.load(Ordering::Acquire);
        let (stale, _) = build_index([deployed(orders.clone())]);
        route(
            &registry,
            &RegisteredService {
                version: 2,
                ..orders.clone()
            },
        )
        .await;
        assert!(!registry.swap_unless_mutated(epoch, stale).await);
        assert_eq!(
            version_of().await,
            Some(2),
            "the deploy survives the reload"
        );

        let epoch = registry.mutations.load(Ordering::Acquire);
        let (stale, _) = build_index([deployed(orders.clone())]);
        registry.remove(tenant, "orders").await;
        assert!(!registry.swap_unless_mutated(epoch, stale).await);
        assert_eq!(version_of().await, None, "the removal survives the reload");

        let epoch = registry.mutations.load(Ordering::Acquire);
        let (fresh, _) = build_index([deployed(RegisteredService {
            version: 3,
            ..orders
        })]);
        assert!(registry.swap_unless_mutated(epoch, fresh).await);
        assert_eq!(
            version_of().await,
            Some(3),
            "an up-to-date snapshot swaps in"
        );
    }

    #[tokio::test]
    async fn refresh_is_seen_by_a_concurrent_load_all() {
        let registry = offline_registry().await;
        let orders = service(Uuid::new_v4(), "acme", "orders", 1);
        route(&registry, &orders).await;

        let epoch = registry.mutations.load(Ordering::Acquire);
        let (stale, _) = build_index([deployed(orders.clone())]);
        let disabled = refreshed(&registry, orders.service_id, || None).await;
        assert_eq!(disabled, Membership::Absent);
        assert!(
            !registry.swap_unless_mutated(epoch, stale).await,
            "a reload that read the service active does not bring it back"
        );
        assert_eq!(registry.count().await, 0);

        let epoch = registry.mutations.load(Ordering::Acquire);
        let (stale, _) = build_index([]);
        route(&registry, &orders).await;
        assert!(
            !registry.swap_unless_mutated(epoch, stale).await,
            "nor does one that read it inactive drop it"
        );
        assert_eq!(registry.count().await, 1);
    }

    #[tokio::test]
    async fn refresh_repeats_a_read_the_registry_changed_under() {
        let registry = offline_registry().await;
        let tenant = Uuid::new_v4();
        let orders = service(tenant, "acme", "orders", 1);
        let redeployed = RegisteredService {
            version: 2,
            ..orders.clone()
        };
        let reads = std::sync::atomic::AtomicUsize::new(0);
        let read = || reads.fetch_add(1, Ordering::SeqCst);

        route(&registry, &orders).await;
        let after_a_disable_here = registry
            .refresh_from(orders.service_id, || async {
                Ok(match read() {
                    0 => {
                        registry.remove(tenant, "orders").await;
                        Some(deployed(redeployed.clone()))
                    }
                    _ => None,
                })
            })
            .await
            .expect("refreshed");
        assert_eq!(after_a_disable_here, Membership::Absent);
        assert_eq!(
            reads.swap(0, Ordering::SeqCst),
            2,
            "the stale row was read again"
        );
        assert_eq!(registry.count().await, 0);

        route(&registry, &orders).await;
        assert_eq!(reads.swap(0, Ordering::SeqCst), 0);
        let after_a_disable_elsewhere = registry
            .refresh_from(orders.service_id, || async {
                Ok(match read() {
                    0 => {
                        let epoch = registry.mutations.load(Ordering::Acquire);
                        assert!(registry.swap_unless_mutated(epoch, build_index([]).0).await);
                        Some(deployed(redeployed.clone()))
                    }
                    _ => None,
                })
            })
            .await
            .expect("refreshed");
        assert_eq!(after_a_disable_elsewhere, Membership::Absent);
        assert_eq!(
            reads.swap(0, Ordering::SeqCst),
            2,
            "a reload swapped in counts too"
        );
        assert_eq!(registry.count().await, 0);

        let never_settled = registry
            .refresh_from(orders.service_id, || async {
                read();
                registry.remove(tenant, "orders").await;
                Ok(Some(deployed(redeployed.clone())))
            })
            .await
            .expect_err("the registry changed under every read");
        assert_eq!(reads.swap(0, Ordering::SeqCst), RELOAD_ATTEMPTS);
        assert!(
            never_settled
                .to_string()
                .starts_with("the registry changed during 3 consecutive reads of service"),
            "{never_settled}"
        );
        assert_eq!(registry.count().await, 0, "no stale row was applied");

        let unread = registry
            .refresh_from(orders.service_id, || async {
                read();
                Err(eyre::eyre!("the database is away"))
            })
            .await
            .expect_err("a read that fails is the caller's");
        assert_eq!(unread.to_string(), "the database is away");
        assert_eq!(reads.load(Ordering::SeqCst), 1, "and is not repeated");
    }

    #[tokio::test]
    async fn every_change_of_the_index_reaches_whoever_follows_it() {
        let registry = offline_registry().await;
        let mut followed = registry.subscribe_version();
        let tenant = Uuid::new_v4();
        let orders = service(tenant, "acme", "orders", 1);
        assert!(!followed.has_changed().expect("open"));

        route(&registry, &orders).await;
        assert_eq!(*followed.borrow_and_update(), 1);
        registry.remove(tenant, "orders").await;
        assert_eq!(*followed.borrow_and_update(), 2);

        let epoch = registry.mutations.load(Ordering::Acquire);
        let (index, _) = build_index([deployed(orders.clone())]);
        assert!(registry.swap_unless_mutated(epoch, index).await);
        assert_eq!(*followed.borrow_and_update(), 3);

        let (stale, _) = build_index([deployed(orders.clone())]);
        route(&registry, &orders).await;
        assert!(!registry.swap_unless_mutated(epoch, stale).await);
        assert_eq!(
            *followed.borrow_and_update(),
            4,
            "a reload that is discarded changes nothing"
        );
        assert_eq!(*registry.clone().subscribe_version().borrow(), 4);
    }

    struct Stored {
        registry: ServiceRegistry,
        pool: deadpool_postgres::Pool,
        tenant: cufflink_testkit::Tenant,
        _db: cufflink_testkit::TestDb,
    }

    async fn stored() -> Option<Stored> {
        let infra = cufflink_testkit::infra().await?;
        let mut db = infra.create_migrated_database().await;
        let tenant = db.mint_tenant().await;
        let pool = cufflink_db::build_pool(&db.url, 2).expect("pool");
        Some(Stored {
            registry: ServiceRegistry::new(pool.clone(), offline_nats().await),
            pool,
            tenant,
            _db: db,
        })
    }

    impl Stored {
        async fn service(&self, name: &str) -> crate::repo::services::Service {
            crate::repo::services::find_or_create(
                &self.pool,
                self.tenant.id,
                name,
                name,
                crate::repo::services::Status::Active,
            )
            .await
            .expect("service")
        }

        async fn activate(
            &self,
            created: &crate::repo::services::Service,
            manifest: &ServiceManifest,
            wasm_hash: Option<&str>,
        ) -> i32 {
            let current = crate::repo::services::get_by_id(&self.pool, created.id)
                .await
                .expect("lookup")
                .expect("service")
                .current_version;
            crate::repo::deployments::activate_version(
                &self.pool,
                crate::repo::deployments::ActivateVersion {
                    service_id: created.id,
                    version: current + 1,
                    manifest_hash: "m",
                    manifest,
                    wasm_hash,
                    commit_hash: None,
                    schema_json: None,
                    schema_hash: "",
                    native: None,
                },
            )
            .await
            .expect("activated")
            .version
        }

        async fn set_status(&self, service_id: Uuid, status: crate::repo::services::Status) {
            crate::repo::services::update_status(&self.pool, service_id, status)
                .await
                .expect("status");
        }

        fn manifest(&self, name: &str) -> ServiceManifest {
            service(self.tenant.id, &self.tenant.slug, name, 0).manifest
        }
    }

    async fn held(registry: &ServiceRegistry) -> serde_json::Value {
        let index = registry.services.read().await;
        let entry = |service: &Arc<RegisteredService>| {
            let mut entry = service.projected();
            entry["strict"] = index.route_tables.contains_key(&service.service_id).into();
            entry
        };
        let sorted = |entries: Vec<(String, serde_json::Value)>| {
            entries
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        serde_json::json!({
            "by_key": sorted(index.by_key.iter().map(|((tenant, name), service)| {
                (format!("{tenant}/{name}"), entry(service))
            }).collect()),
            "by_slug": sorted(index.by_slug.iter().map(|((slug, name), service)| {
                (format!("{slug}/{name}"), entry(service))
            }).collect()),
            "route_tables": index.route_tables.keys().collect::<std::collections::BTreeSet<_>>(),
            "current_versions": index.current_versions.iter().collect::<std::collections::BTreeMap<_, _>>(),
        })
    }

    #[tokio::test]
    async fn refresh_routes_an_active_service_at_its_current_version() {
        let Some(stored) = stored().await else {
            return;
        };
        let registry = &stored.registry;
        let orders = stored.service("orders").await;
        assert_eq!(
            registry.refresh(orders.id).await.expect("refreshed"),
            Membership::Absent,
            "nothing of it is deployed yet"
        );
        assert_eq!(
            registry.current_versions().await,
            HashMap::from([(orders.id, orders.current_version)]),
            "and its first deploy, in flight, keeps its module"
        );

        let manifest = stored.manifest("orders");
        let first = stored.activate(&orders, &manifest, Some("w1")).await;
        assert_eq!(
            registry.refresh(orders.id).await.expect("refreshed"),
            Membership::Routed
        );
        let routed = registry
            .get_by_slug(&stored.tenant.slug, "orders")
            .await
            .expect("routed");
        assert_eq!(
            (
                routed.service_id,
                routed.tenant_id,
                routed.version,
                routed.wasm_hash.as_deref()
            ),
            (orders.id, stored.tenant.id, first, Some("w1"))
        );
        assert_eq!(
            serde_json::to_value(&routed.manifest).expect("routed"),
            serde_json::to_value(&manifest).expect("deployed")
        );

        let books = routed_strictly(
            service(stored.tenant.id, &stored.tenant.slug, "orders", 0),
            serde_json::json!([{"method": "GET", "path": "/orders/{id}", "handler": "order"}]),
        )
        .manifest;
        let second = stored.activate(&orders, &books, None).await;
        assert_eq!(
            registry.refresh(orders.id).await.expect("refreshed"),
            Membership::Routed
        );
        let (routed, _) = registry
            .strict_routes(&stored.tenant.slug, "orders")
            .await
            .expect("its route table came with it");
        assert_eq!((routed.version, &routed.wasm_hash), (second, &None));
        assert_eq!(second, first + 1);
        assert_eq!(registry.count().await, 1);
        assert_eq!(
            registry.current_versions().await,
            HashMap::from([(orders.id, second)])
        );
        assert_eq!(
            registry.get_by_tenant(stored.tenant.id).await.len(),
            1,
            "one entry, under both keys"
        );
    }

    #[tokio::test]
    async fn refresh_removes_an_inactive_service() {
        use crate::repo::services::Status;
        let Some(stored) = stored().await else {
            return;
        };
        let registry = &stored.registry;
        let orders = stored.service("orders").await;
        let billing = stored.service("billing").await;
        for created in [&orders, &billing] {
            let manifest = stored.manifest(&created.name);
            stored.activate(created, &manifest, None).await;
            registry.refresh(created.id).await.expect("refreshed");
        }
        assert_eq!(registry.count().await, 2);

        stored.set_status(orders.id, Status::Inactive).await;
        assert_eq!(
            registry.refresh(orders.id).await.expect("refreshed"),
            Membership::Absent
        );
        assert!(registry
            .get_by_slug(&stored.tenant.slug, "orders")
            .await
            .is_none());
        assert_eq!(
            registry.current_versions().await.keys().collect::<Vec<_>>(),
            [&billing.id],
            "its modules are a reload's to drop"
        );

        let redeployed = stored
            .activate(&orders, &stored.manifest("orders"), None)
            .await;
        assert_eq!(
            registry.refresh(orders.id).await.expect("refreshed"),
            Membership::Absent,
            "a version activated while it is inactive is not routed"
        );
        stored.set_status(orders.id, Status::Active).await;
        assert_eq!(
            registry.refresh(orders.id).await.expect("refreshed"),
            Membership::Routed
        );
        assert_eq!(
            registry
                .get_by_slug(&stored.tenant.slug, "orders")
                .await
                .map(|service| service.version),
            Some(redeployed),
            "enabled, it runs the version it was last given"
        );

        crate::repo::services::delete_service(
            &stored.pool.get().await.expect("checkout"),
            orders.id,
        )
        .await
        .expect("deleted");
        for gone in [orders.id, Uuid::new_v4()] {
            assert_eq!(
                registry.refresh(gone).await.expect("refreshed"),
                Membership::Absent
            );
        }
        assert_eq!(registry.count().await, 1);
    }

    #[tokio::test]
    async fn refresh_and_load_all_decode_a_row_alike() {
        let Some(stored) = stored().await else {
            return;
        };
        let mut ids = Vec::new();
        let legacy = stored.service("legacy").await;
        stored
            .activate(&legacy, &stored.manifest("legacy"), Some("w"))
            .await;
        let routed = stored.service("strict").await;
        let strict_manifest = routed_strictly(
            service(stored.tenant.id, &stored.tenant.slug, "strict", 0),
            serde_json::json!([{"method": "GET", "path": "/a/{x}", "handler": "one"}]),
        )
        .manifest;
        stored.activate(&routed, &strict_manifest, None).await;
        let unroutable = stored.service("unroutable").await;
        let conflicting = routed_strictly(
            service(stored.tenant.id, &stored.tenant.slug, "unroutable", 0),
            conflicting_routes(),
        )
        .manifest;
        stored.activate(&unroutable, &conflicting, None).await;
        let undecodable = stored.service("undecodable").await;
        stored
            .activate(&undecodable, &stored.manifest("undecodable"), None)
            .await;
        stored
            .pool
            .get()
            .await
            .expect("checkout")
            .execute(
                "UPDATE deployments SET manifest = '{\"bogus\": 1}' WHERE service_id = $1",
                &[&undecodable.id],
            )
            .await
            .expect("an undecodable manifest");
        let awaiting = stored.service("awaiting").await;
        let inactive = stored.service("inactive").await;
        stored
            .activate(&inactive, &stored.manifest("inactive"), None)
            .await;
        stored
            .set_status(inactive.id, crate::repo::services::Status::Inactive)
            .await;
        ids.extend([
            legacy.id,
            routed.id,
            unroutable.id,
            undecodable.id,
            awaiting.id,
            inactive.id,
        ]);

        let reloaded = ServiceRegistry::new(stored.pool.clone(), offline_nats().await);
        assert_eq!(
            reloaded.load_all().await.expect("loaded"),
            LoadReport {
                loaded: 2,
                skipped: 2
            }
        );
        let mut memberships = Vec::new();
        for id in &ids {
            memberships.push(stored.registry.refresh(*id).await.expect("refreshed"));
        }
        assert_eq!(
            memberships,
            [
                Membership::Routed,
                Membership::Routed,
                Membership::Absent,
                Membership::Absent,
                Membership::Absent,
                Membership::Absent,
            ]
        );
        let (one_by_one, at_once) = (held(&stored.registry).await, held(&reloaded).await);
        assert_eq!(one_by_one, at_once);
        assert_eq!(
            at_once["current_versions"]
                .as_object()
                .expect("versions")
                .len(),
            3,
            "the two routed, and the one awaiting its first deploy"
        );
        assert_eq!(at_once["route_tables"], serde_json::json!([routed.id]));
    }
}
