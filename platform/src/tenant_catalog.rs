use crate::errors::PlatformResult;
use crate::extensions::Registration;
use crate::nats_manager::subjects::{Claim, ClaimKind, Namespace, PublishScope};
use crate::policy::config::{KeyConfig, StorageScopeConfig};
use crate::policy::{Correction, Denied, PolicyKey, PolicyMode, PolicySet, Violation};
use crate::repo::extensions::{self, ExtensionRow};
use crate::repo::nats_claims::{self, ClaimRow};
use crate::repo::tenant_policies::{self, PolicyRow};
use crate::repo::tenants::{self, Tenant};
use crate::shutdown::Shutdown;
use crate::wasm_runtime::storage_scope::BucketClaims;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;

pub const TENANTS_CHANGED_SUBJECT: &str = "cufflink.tenants.changed";
const RELOAD_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct TenantEntry {
    pub tenant: Tenant,
    pub policies: Arc<PolicySet>,
    pub namespace: Namespace,
    pub extensions: BTreeMap<String, Registration>,
}

impl TenantEntry {
    pub fn new(
        tenant: Tenant,
        policies: Arc<PolicySet>,
        claims: Vec<Claim>,
        extensions: BTreeMap<String, Registration>,
    ) -> Self {
        Self {
            namespace: Namespace::new(&tenant.slug, claims),
            tenant,
            policies,
            extensions,
        }
    }

    pub fn mode(&self, key: PolicyKey) -> PolicyMode {
        self.policies.mode(key)
    }

    pub fn config<T: KeyConfig>(&self) -> &T {
        self.policies.config()
    }

    pub fn apply(&self, key: PolicyKey, violation: Option<Violation>) -> Result<(), Denied> {
        self.policies.apply(key, &self.tenant.slug, violation)
    }

    pub fn correct(&self, key: PolicyKey, violation: Option<Violation>) -> Correction {
        self.policies.correct(key, &self.tenant.slug, violation)
    }
}

#[derive(Default, Clone)]
struct Snapshot {
    by_id: HashMap<Uuid, Arc<TenantEntry>>,
    by_slug: HashMap<String, Uuid>,
    claimed_buckets: HashMap<String, HashSet<Uuid>>,
    subject_claims: Vec<(Uuid, String, String)>,
}

impl Snapshot {
    fn build(
        tenants: Vec<Tenant>,
        rows: &[PolicyRow],
        claims: Vec<ClaimRow>,
        extensions: Vec<ExtensionRow>,
        defaults: &PolicySet,
    ) -> Self {
        let mut rows_by_tenant: HashMap<Uuid, Vec<PolicyRow>> = HashMap::new();
        for row in rows {
            rows_by_tenant
                .entry(row.tenant_id)
                .or_default()
                .push(row.clone());
        }
        let mut claims_by_tenant: HashMap<Uuid, Vec<Claim>> = HashMap::new();
        for row in claims {
            claims_by_tenant
                .entry(row.tenant_id)
                .or_default()
                .push(row.claim);
        }
        let mut extensions_by_tenant = by_tenant(extensions);
        tenants
            .into_iter()
            .map(|tenant| {
                let rows = rows_by_tenant.remove(&tenant.id).unwrap_or_default();
                let claims = claims_by_tenant.remove(&tenant.id).unwrap_or_default();
                let extensions = extensions_by_tenant.remove(&tenant.id).unwrap_or_default();
                entry(tenant, &rows, claims, extensions, defaults)
            })
            .fold(Snapshot::default(), Snapshot::with_entry)
    }

    fn foreign_subject_claims(&self, tenant_id: Uuid) -> Vec<(String, String)> {
        self.subject_claims
            .iter()
            .filter(|(owner, _, _)| *owner != tenant_id)
            .map(|(_, slug, pattern)| (slug.clone(), pattern.clone()))
            .collect()
    }

    fn with_entry(mut self, entry: Arc<TenantEntry>) -> Self {
        self.subject_claims.extend(
            entry
                .namespace
                .claims()
                .filter(|claim| claim.kind == ClaimKind::Subject)
                .map(|claim| (entry.tenant.id, entry.tenant.slug.clone(), claim.pattern)),
        );
        self.subject_claims
            .sort_by(|(_, a_slug, a), (_, b_slug, b)| (a_slug, a).cmp(&(b_slug, b)));
        for bucket in &entry.config::<StorageScopeConfig>().allowed_buckets {
            self.claimed_buckets
                .entry(bucket.clone())
                .or_default()
                .insert(entry.tenant.group_id());
        }
        self.by_slug
            .insert(entry.tenant.slug.clone(), entry.tenant.id);
        self.by_id.insert(entry.tenant.id, entry);
        self
    }

    fn bucket_claims(&self, tenant_id: Uuid, bucket: &str) -> BucketClaims {
        let group = self
            .get(tenant_id)
            .map_or(tenant_id, |entry| entry.tenant.group_id());
        self.claimed_buckets
            .get(bucket)
            .map_or_else(BucketClaims::default, |owners| BucketClaims {
                by_group: owners.contains(&group),
                by_other: owners.iter().any(|owner| *owner != group),
            })
    }

    fn get(&self, tenant_id: Uuid) -> Option<Arc<TenantEntry>> {
        self.by_id.get(&tenant_id).cloned()
    }

    fn get_by_slug(&self, slug: &str) -> Option<Arc<TenantEntry>> {
        self.by_slug.get(slug).and_then(|id| self.get(*id))
    }
}

fn by_tenant(rows: Vec<ExtensionRow>) -> HashMap<Uuid, BTreeMap<String, Registration>> {
    rows.into_iter()
        .fold(HashMap::<_, BTreeMap<_, _>>::new(), |mut by_tenant, row| {
            by_tenant
                .entry(row.tenant_id)
                .or_default()
                .insert(row.name, row.registration);
            by_tenant
        })
}

fn entry(
    tenant: Tenant,
    rows: &[PolicyRow],
    claims: Vec<Claim>,
    extensions: BTreeMap<String, Registration>,
    defaults: &PolicySet,
) -> Arc<TenantEntry> {
    Arc::new(TenantEntry::new(
        tenant,
        Arc::new(defaults.with_tenant_rows(rows)),
        claims,
        extensions,
    ))
}

#[derive(Clone)]
pub struct TenantCatalog {
    db: deadpool_postgres::Pool,
    nats: async_nats::Client,
    defaults: Arc<PolicySet>,
    snapshot: Arc<RwLock<Arc<Snapshot>>>,
    reload_lock: Arc<tokio::sync::Mutex<()>>,
}

impl TenantCatalog {
    pub fn new(db: deadpool_postgres::Pool, nats: async_nats::Client, defaults: PolicySet) -> Self {
        Self {
            db,
            nats,
            defaults: Arc::new(defaults),
            snapshot: Arc::default(),
            reload_lock: Arc::default(),
        }
    }

    fn current(&self) -> Arc<Snapshot> {
        Arc::clone(&self.snapshot.read().unwrap_or_else(PoisonError::into_inner))
    }

    pub async fn reload(&self) -> eyre::Result<()> {
        let _serialised = self.reload_lock.lock().await;
        let loaded = async {
            let tenants = tenants::list_all(&self.db).await?;
            let rows = tenant_policies::list_all(&self.db).await?;
            let client = self.db.get().await?;
            let claims = nats_claims::list_all(&client).await?;
            let extensions = extensions::list_all(&client).await?;
            PlatformResult::Ok(Snapshot::build(
                tenants,
                &rows,
                claims,
                extensions,
                &self.defaults,
            ))
        }
        .await;
        let result = if loaded.is_ok() { "ok" } else { "error" };
        metrics::counter!("cufflink_policy_reload_total", "result" => result).increment(1);
        let snapshot = loaded?;
        *self
            .snapshot
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(snapshot);
        Ok(())
    }

    pub fn get(&self, tenant_id: Uuid) -> Option<Arc<TenantEntry>> {
        self.current().get(tenant_id)
    }

    pub fn get_by_slug(&self, slug: &str) -> Option<Arc<TenantEntry>> {
        self.current().get_by_slug(slug)
    }

    pub fn entries(&self) -> Vec<Arc<TenantEntry>> {
        self.current().by_id.values().cloned().collect()
    }

    pub fn bucket_claims(&self, tenant_id: Uuid, bucket: &str) -> BucketClaims {
        self.current().bucket_claims(tenant_id, bucket)
    }

    pub fn foreign_subject_claims(&self, tenant_id: Uuid) -> Vec<(String, String)> {
        self.current().foreign_subject_claims(tenant_id)
    }

    pub fn publish_scope(&self, tenant_id: Uuid, tenant_slug: &str) -> PublishScope {
        let current = self.current();
        PublishScope {
            namespace: current.get(tenant_id).map_or_else(
                || Namespace::own(tenant_slug),
                |entry| entry.namespace.clone(),
            ),
            foreign: current
                .foreign_subject_claims(tenant_id)
                .into_iter()
                .map(|(_, pattern)| pattern)
                .collect(),
        }
    }

    pub fn defaults(&self) -> &PolicySet {
        &self.defaults
    }

    pub async fn get_or_load(&self, tenant_id: Uuid) -> PlatformResult<Option<Arc<TenantEntry>>> {
        match self.get(tenant_id) {
            Some(found) => Ok(Some(found)),
            None => {
                self.load(tenants::get_by_id(&self.db, tenant_id).await?)
                    .await
            }
        }
    }

    pub async fn get_or_load_by_slug(
        &self,
        slug: &str,
    ) -> PlatformResult<Option<Arc<TenantEntry>>> {
        match self.get_by_slug(slug) {
            Some(found) => Ok(Some(found)),
            None => self.load(tenants::get_by_slug(&self.db, slug).await?).await,
        }
    }

    async fn load(&self, tenant: Option<Tenant>) -> PlatformResult<Option<Arc<TenantEntry>>> {
        let Some(tenant) = tenant else {
            return Ok(None);
        };
        let client = self.db.get().await?;
        let rows = tenant_policies::list_for_tenant(&client, tenant.id).await?;
        let claims = nats_claims::list_for_tenant(&client, tenant.id).await?;
        let extensions = extensions::registrations(&client, tenant.id).await?;
        Ok(Some(self.insert_if_absent(entry(
            tenant,
            &rows,
            claims,
            extensions,
            &self.defaults,
        ))))
    }

    fn insert_if_absent(&self, loaded: Arc<TenantEntry>) -> Arc<TenantEntry> {
        let mut current = self
            .snapshot
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(present) = current.get(loaded.tenant.id) {
            return present;
        }
        *current = Arc::new(Snapshot::clone(&current).with_entry(Arc::clone(&loaded)));
        loaded
    }

    pub async fn policies_or_load(&self, tenant_id: Uuid) -> PlatformResult<Arc<PolicySet>> {
        Ok(self
            .get_or_load(tenant_id)
            .await?
            .map_or_else(|| Arc::clone(&self.defaults), |e| Arc::clone(&e.policies)))
    }

    pub async fn notify_change(&self) {
        crate::nats_conn::publish_change(&self.nats, TENANTS_CHANGED_SUBJECT).await;
    }

    pub async fn refresh(&self) {
        if let Err(e) = self.reload().await {
            warn!(error = %e, "Tenant catalog reload after a write failed; the periodic reload will retry");
        }
        self.notify_change().await;
    }

    pub fn start_sync_listener(&self, shutdown: &Shutdown) {
        let catalog = self.clone();
        crate::nats_conn::spawn_reload_loop(
            shutdown,
            "tenant_catalog_sync",
            self.nats.clone(),
            TENANTS_CHANGED_SUBJECT,
            RELOAD_INTERVAL,
            move || {
                let catalog = catalog.clone();
                async move {
                    if let Err(e) = catalog.reload().await {
                        warn!(error = %e, "Tenant catalog reload failed; the previous snapshot stays in place");
                    }
                }
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn seed(&self, tenant: Tenant, rows: &[PolicyRow]) {
        self.insert_if_absent(entry(
            tenant,
            rows,
            Vec::new(),
            BTreeMap::new(),
            &self.defaults,
        ));
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::policy::{PolicyKey, PolicySource};
    use serde_json::json;

    pub(crate) fn tenant(slug: &str, realm: Option<&str>) -> Tenant {
        Tenant {
            id: Uuid::new_v4(),
            keycloak_user_id: format!("platform-{slug}"),
            name: slug.into(),
            slug: slug.into(),
            keycloak_url: realm.map(|_| "http://keycloak:8080".into()),
            keycloak_realm: realm.map(Into::into),
            deploy_role: tenants::DEFAULT_DEPLOY_ROLE.into(),
            keycloak_ops_client_id: None,
            keycloak_ops_client_secret: None,
            service_account_clients: vec![],
            keycloak_issuer: None,
            source_tenant_id: None,
            database_name: None,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn an_entry_holds_its_tenants_namespace_and_the_catalog_every_other_claim() {
        let (harbor, acme, plain) = (
            tenant("harbor", None),
            tenant("acme", None),
            tenant("plain", None),
        );
        let claim = |tenant: &Tenant, claim: Claim| ClaimRow {
            tenant_id: tenant.id,
            tenant_slug: tenant.slug.clone(),
            claim,
        };
        let snapshot = Snapshot::build(
            vec![harbor.clone(), acme.clone(), plain.clone()],
            &[],
            vec![
                claim(&harbor, Claim::subject("books.>")),
                claim(&harbor, Claim::stream("BOOKS")),
                claim(&acme, Claim::subject("orders.>")),
            ],
            Vec::new(),
            &PolicySet::builtin_defaults(),
        );
        let namespace = |tenant: &Tenant| snapshot.get(tenant.id).expect("entry").namespace.clone();
        assert!(namespace(&harbor).contains("books.created"));
        assert!(namespace(&harbor).has_stream("BOOKS"));
        assert!(!namespace(&acme).contains("books.created"));
        assert_eq!(namespace(&plain), Namespace::own("plain"));
        assert_eq!(
            snapshot.foreign_subject_claims(plain.id),
            [
                ("acme".to_string(), "orders.>".to_string()),
                ("harbor".to_string(), "books.>".to_string())
            ]
        );
        assert_eq!(
            snapshot.foreign_subject_claims(harbor.id),
            [("acme".to_string(), "orders.>".to_string())]
        );
    }

    #[test]
    fn an_entry_holds_the_extensions_its_tenant_registers() {
        let (harbor, acme) = (tenant("harbor", None), tenant("acme", None));
        let registered = |tenant: &Tenant, name: &str, callers: &[&str]| ExtensionRow {
            tenant_id: tenant.id,
            name: name.into(),
            registration: Registration {
                backend: crate::extensions::Backend::Nats,
                callers: callers.iter().map(|c| c.to_string()).collect(),
            },
            created_by: "admin".into(),
            created_at: chrono::Utc::now(),
        };
        let snapshot = Snapshot::build(
            vec![harbor.clone(), acme.clone()],
            &[],
            Vec::new(),
            vec![
                registered(&harbor, "google.token", &["reports"]),
                registered(&harbor, "pdf.render", &["*"]),
                registered(&acme, "google.token", &["billing"]),
            ],
            &PolicySet::builtin_defaults(),
        );
        let extensions = |tenant: &Tenant| {
            snapshot
                .get(tenant.id)
                .expect("entry")
                .extensions
                .iter()
                .map(|(name, registration)| (name.clone(), registration.callers.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            extensions(&harbor),
            [
                ("google.token".to_string(), vec!["reports".to_string()]),
                ("pdf.render".to_string(), vec!["*".to_string()]),
            ]
        );
        assert_eq!(
            extensions(&acme),
            [("google.token".to_string(), vec!["billing".to_string()])]
        );
    }

    pub(crate) fn policy_row(tenant_id: Uuid, key: PolicyKey, mode: PolicyMode) -> PolicyRow {
        PolicyRow {
            tenant_id,
            key: key.as_str().into(),
            mode,
            config: json!({}),
            updated_at: chrono::Utc::now(),
            updated_by: Some("test".into()),
        }
    }

    #[test]
    fn snapshot_build_layers_policies_over_defaults() {
        let orchard = tenant("default", Some("orchard"));
        let harbor = tenant("harbor", Some("harbor"));
        let rows = [policy_row(
            harbor.id,
            PolicyKey::TenantBinding,
            PolicyMode::Enforce,
        )];
        let (s_id, m_id) = (orchard.id, harbor.id);
        let snapshot = Snapshot::build(
            vec![orchard, harbor],
            &rows,
            Vec::new(),
            Vec::new(),
            &PolicySet::builtin_defaults(),
        );
        let m = snapshot.get(m_id).expect("harbor");
        assert_eq!(m.mode(PolicyKey::TenantBinding), PolicyMode::Enforce);
        assert_eq!(
            m.policies.get(PolicyKey::TenantBinding).source,
            PolicySource::Tenant
        );
        let s = snapshot.get(s_id).expect("orchard");
        assert_eq!(*s.policies, PolicySet::builtin_defaults());
    }

    fn storage_row(tenant_id: Uuid, buckets: &[&str]) -> PolicyRow {
        PolicyRow {
            config: json!({"scope": "legacy", "allowed_buckets": buckets}),
            ..policy_row(tenant_id, PolicyKey::StorageScope, PolicyMode::Enforce)
        }
    }

    fn claims(by_group: bool, by_other: bool) -> BucketClaims {
        BucketClaims { by_group, by_other }
    }

    #[test]
    fn claimed_buckets_are_foreign_to_every_other_tenant() {
        let harbor = tenant("harbor", Some("harbor"));
        let orchard = tenant("default", Some("orchard"));
        let (m_id, s_id) = (harbor.id, orchard.id);
        let snapshot = Snapshot::build(
            vec![harbor, orchard],
            &[storage_row(m_id, &["harbor-data"])],
            Vec::new(),
            Vec::new(),
            &PolicySet::builtin_defaults(),
        );
        assert_eq!(
            snapshot.bucket_claims(m_id, "harbor-data"),
            claims(true, false)
        );
        assert_eq!(
            snapshot.bucket_claims(s_id, "harbor-data"),
            claims(false, true)
        );
        assert_eq!(
            snapshot.bucket_claims(Uuid::new_v4(), "harbor-data"),
            claims(false, true)
        );
        assert_eq!(
            snapshot.bucket_claims(s_id, "example-app-data"),
            claims(false, false)
        );
    }

    #[test]
    fn previews_share_their_source_groups_buckets() {
        let source = tenant("default", Some("orchard"));
        let linked = |slug: &str| Tenant {
            source_tenant_id: Some(source.id),
            ..tenant(slug, Some("orchard"))
        };
        let (old_preview, cloned_preview) = (linked("preview-pr-1"), linked("preview-pr-2"));
        let unlinked = tenant("preview-pr-0", Some("orchard"));
        let (src, old, cloned, legacy) =
            (source.id, old_preview.id, cloned_preview.id, unlinked.id);
        let snapshot = Snapshot::build(
            vec![source, old_preview, cloned_preview, unlinked],
            &[
                storage_row(src, &["app", "added-later"]),
                storage_row(cloned, &["app", "preview-scratch"]),
            ],
            Vec::new(),
            Vec::new(),
            &PolicySet::builtin_defaults(),
        );
        for (tenant, bucket) in [
            (old, "app"),
            (cloned, "added-later"),
            (src, "preview-scratch"),
        ] {
            assert_eq!(
                snapshot.bucket_claims(tenant, bucket),
                claims(true, false),
                "{bucket}"
            );
        }
        assert_eq!(
            snapshot.bucket_claims(legacy, "app"),
            claims(false, true),
            "a preview that predates the link is its own group"
        );
    }

    #[test]
    fn snapshot_by_slug_index() {
        let t = tenant("preview-pr-7", Some("orchard"));
        let id = t.id;
        let snapshot = Snapshot::build(
            vec![t],
            &[],
            Vec::new(),
            Vec::new(),
            &PolicySet::builtin_defaults(),
        );
        assert_eq!(
            snapshot.get_by_slug("preview-pr-7").map(|e| e.tenant.id),
            Some(id)
        );
        assert!(snapshot.get_by_slug("default").is_none());
        assert!(snapshot.get(Uuid::new_v4()).is_none());
    }
}
