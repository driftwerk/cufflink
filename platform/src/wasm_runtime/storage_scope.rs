use crate::policy::config::{StorageScopeConfig, StorageScopeKind};
use crate::policy::{self, Denied, PolicyKey, PolicySet, Violation};
use crate::storage::S3Storage;
use crate::tenant_buckets::{Access, State, TenantBucket};
use crate::AppState;
use cufflink_storage::CannedAcl;
use uuid::Uuid;

pub const PLATFORM_KEY_PREFIXES: [&str; 5] = ["wasm/", "backups/", "files/", "web/", "native/"];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketClaims {
    pub by_group: bool,
    pub by_other: bool,
}

pub struct StorageContext<'a> {
    pub tenant_slug: &'a str,
    pub service_name: &'a str,
    pub platform_bucket: Option<&'a str>,
    pub claims: BucketClaims,
    pub config: &'a StorageScopeConfig,
}

impl StorageContext<'_> {
    fn violation(&self, code: &'static str, bucket: &str, keys: &[&str]) -> Violation {
        Violation::new(
            code,
            format!(
                "service {} of tenant {} on bucket {bucket} keys {keys:?}",
                self.service_name, self.tenant_slug
            ),
        )
    }

    fn denied(&self, code: &'static str, bucket: &str, keys: &[&str]) -> Option<Violation> {
        Some(self.violation(code, bucket, keys))
    }
}

pub fn strict_prefix(tenant_slug: &str, service_name: &str) -> String {
    format!("tenants/{tenant_slug}/{service_name}/")
}

fn has_traversal(key: &str) -> bool {
    key.split('/').any(|segment| segment == "..")
}

fn in_platform_prefix(key: &str) -> bool {
    let key = key.trim_start_matches('/');
    PLATFORM_KEY_PREFIXES.iter().any(|p| key.starts_with(p))
}

pub fn reserved_violation(ctx: &StorageContext, bucket: &str, keys: &[&str]) -> Option<Violation> {
    if !cufflink_storage::is_bucket_name(bucket) {
        return ctx.denied("invalid_bucket", bucket, keys);
    }
    if ctx.platform_bucket == Some(bucket) && keys.iter().any(|key| in_platform_prefix(key)) {
        return ctx.denied("platform_prefix", bucket, keys);
    }
    if keys.iter().any(|key| has_traversal(key)) {
        return ctx.denied("path_traversal", bucket, keys);
    }
    None
}

pub fn authorize(
    ctx: &StorageContext,
    policies: &PolicySet,
    bucket: &str,
    keys: &[&str],
) -> Result<(), Denied> {
    match reserved_violation(ctx, bucket, keys) {
        Some(reserved) => Err(policy::deny(
            PolicyKey::StorageScope,
            ctx.tenant_slug,
            reserved,
        )),
        None => policies.apply(
            PolicyKey::StorageScope,
            ctx.tenant_slug,
            violation(ctx, bucket, keys),
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route<'a> {
    Scope,
    TenantBucket(&'a Access),
    Closed(&'static str),
}

/// How a guest call on `bucket` is served: the reserved checks first, then
/// a tenant bucket's own rules in every mode, then `storage_scope`. Only a
/// tenant bucket admits a canned ACL.
pub fn route<'a>(
    ctx: &StorageContext,
    policies: &PolicySet,
    tenant_id: Uuid,
    owned: Option<&'a TenantBucket>,
    bucket: &str,
    keys: &[&str],
    acl: Option<CannedAcl>,
) -> Result<Route<'a>, Denied> {
    let deny = |code| {
        policy::deny(
            PolicyKey::StorageScope,
            ctx.tenant_slug,
            ctx.violation(code, bucket, keys),
        )
    };
    if let Some(reserved) = reserved_violation(ctx, bucket, keys) {
        return Err(policy::deny(
            PolicyKey::StorageScope,
            ctx.tenant_slug,
            reserved,
        ));
    }
    match owned {
        Some(owned) if owned.owner != Some(tenant_id) => Err(deny("foreign_tenant_bucket")),
        Some(owned) => match &owned.state {
            State::Retired => Err(deny("bucket_retired")),
            State::Unreadable => Ok(Route::Closed("the bucket's registration cannot be read")),
            State::Active { rules, access } => rules
                .admit(keys, acl)
                .map(|()| Route::TenantBucket(access))
                .map_err(|refusal| deny(refusal.code())),
        },
        None if acl.is_some() => Err(deny("acl_not_admitted")),
        None => policies
            .apply(
                PolicyKey::StorageScope,
                ctx.tenant_slug,
                violation(ctx, bucket, keys),
            )
            .map(|()| Route::Scope),
    }
}

pub struct Guest<'a> {
    pub tenant_id: Uuid,
    pub tenant_slug: &'a str,
    pub service_name: &'a str,
    pub policies: &'a PolicySet,
}

#[derive(Debug)]
pub enum Unserved {
    Denied(Denied),
    NotConfigured(&'static str),
    Backend(eyre::Report),
}

impl std::fmt::Display for Unserved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unserved::Denied(denied) => write!(f, "storage access denied: {}", denied.code),
            Unserved::NotConfigured(why) => f.write_str(why),
            Unserved::Backend(e) => write!(f, "{e}"),
        }
    }
}

/// The store a guest call on `bucket` must use, or why it may not run.
/// Every guest storage path takes its store from here, so none can be
/// authorized against one bucket's rules and served by another's store.
pub fn resolve(
    app_state: &AppState,
    guest: &Guest,
    bucket: &str,
    keys: &[&str],
    acl: Option<CannedAcl>,
) -> Result<S3Storage, Unserved> {
    let platform = app_state.storage.as_ref();
    let context = StorageContext {
        tenant_slug: guest.tenant_slug,
        service_name: guest.service_name,
        platform_bucket: platform.map(|s| s.primary_bucket()),
        claims: app_state.tenants.bucket_claims(guest.tenant_id, bucket),
        config: guest.policies.config::<StorageScopeConfig>(),
    };
    let owned = app_state.tenants.tenant_bucket(bucket);
    let routed = route(
        &context,
        guest.policies,
        guest.tenant_id,
        owned.as_deref(),
        bucket,
        keys,
        acl,
    )
    .map_err(Unserved::Denied)?;
    store(platform, bucket, routed)
}

fn store(platform: Option<&S3Storage>, bucket: &str, routed: Route) -> Result<S3Storage, Unserved> {
    let platform = platform.ok_or(Unserved::NotConfigured("object storage is not configured"))?;
    match routed {
        Route::Scope => Ok(platform.clone()),
        Route::TenantBucket(Access::Platform) => platform
            .tenant_bucket(bucket, None)
            .map_err(Unserved::Backend),
        Route::TenantBucket(Access::Own(credential)) => platform
            .tenant_bucket(bucket, Some(credential))
            .map_err(Unserved::Backend),
        Route::TenantBucket(Access::Unopenable) => Err(Unserved::NotConfigured(
            "the bucket's credential cannot be opened",
        )),
        Route::Closed(why) => Err(Unserved::NotConfigured(why)),
    }
}

pub fn violation(ctx: &StorageContext, bucket: &str, keys: &[&str]) -> Option<Violation> {
    let allowlist = &ctx.config.allowed_buckets;
    let own = ctx.claims.by_group || allowlist.iter().any(|b| b == bucket);
    if !allowlist.is_empty() && !own {
        return ctx.denied("bucket_not_allowed", bucket, keys);
    }
    if allowlist.is_empty() && ctx.claims.by_other && !own {
        return ctx.denied("foreign_bucket", bucket, keys);
    }
    match ctx.config.scope {
        StorageScopeKind::Legacy => None,
        StorageScopeKind::Strict if allowlist.is_empty() => {
            ctx.denied("strict_requires_allowlist", bucket, keys)
        }
        StorageScopeKind::Strict => {
            let prefix = strict_prefix(ctx.tenant_slug, ctx.service_name);
            if keys.iter().all(|key| key.starts_with(&prefix)) {
                None
            } else {
                ctx.denied("key_outside_prefix", bucket, keys)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::tenant_policies::PolicyRow;
    use crate::tenant_buckets::Rules;
    use cufflink_storage::Credential;
    use cufflink_types::policy::PolicyMode;

    fn config(scope: StorageScopeKind, buckets: &[&str]) -> StorageScopeConfig {
        StorageScopeConfig {
            scope,
            allowed_buckets: buckets.iter().map(|b| b.to_string()).collect(),
        }
    }

    fn code(ctx: &StorageContext, bucket: &str, keys: &[&str]) -> Option<&'static str> {
        reserved_violation(ctx, bucket, keys)
            .or_else(|| violation(ctx, bucket, keys))
            .map(|v| v.code)
    }

    fn ctx(config: &StorageScopeConfig, claimed: bool) -> StorageContext<'_> {
        claims_ctx(
            config,
            BucketClaims {
                by_group: false,
                by_other: claimed,
            },
        )
    }

    fn claims_ctx(config: &StorageScopeConfig, claims: BucketClaims) -> StorageContext<'_> {
        StorageContext {
            tenant_slug: "harbor",
            service_name: "books",
            platform_bucket: Some("cufflink-platform"),
            claims,
            config,
        }
    }

    const DISGUISED: [&str; 14] = [
        "",
        ".",
        "..",
        "a\\b",
        "cufflink-platform\\wasm",
        "a?b",
        "a#b",
        "a\tb",
        "a\nb",
        "Example-Documents",
        "%2e",
        "example-documents.",
        "example..documents",
        "example documents",
    ];

    #[test]
    fn platform_prefix_denied_in_platform_bucket_only() {
        let legacy = config(StorageScopeKind::Legacy, &[]);
        let c = ctx(&legacy, false);
        for key in [
            "wasm/x/v1.wasm",
            "/backups/s/export.json",
            "files/a",
            "web/svc/v3.tar.gz",
            "native/svc/9f86d081",
        ] {
            assert_eq!(
                code(&c, "cufflink-platform", &[key]),
                Some("platform_prefix"),
                "{key}"
            );
        }
        assert_eq!(code(&c, "app-data", &["wasm/x/v1.wasm"]), None);
        assert_eq!(code(&c, "cufflink-platform", &["photos/a.jpg"]), None);
        assert_eq!(
            code(&c, "cufflink-platform", &["photos/a.jpg", "wasm/x"]),
            Some("platform_prefix"),
            "any key of a multi-key call"
        );
    }

    #[test]
    fn bucket_names_that_could_alias_a_path_are_refused() {
        let legacy = config(StorageScopeKind::Legacy, &[]);
        let c = ctx(&legacy, false);
        for bucket in DISGUISED
            .iter()
            .copied()
            .chain(["cufflink-platform/wasm", "../x", "/"])
        {
            assert_eq!(
                code(&c, bucket, &["a"]),
                Some("invalid_bucket"),
                "{bucket:?}"
            );
        }
        assert_eq!(code(&c, "app.data-1", &["a"]), None);
    }

    #[test]
    fn reserved_keys_are_not_policy_violations() {
        let legacy = config(StorageScopeKind::Legacy, &[]);
        let c = ctx(&legacy, false);
        for (bucket, key) in [
            ("cufflink-platform", "web/svc/v1.tar.gz"),
            ("app-data", "a/../b"),
        ] {
            assert!(reserved_violation(&c, bucket, &[key]).is_some(), "{key}");
            assert_eq!(violation(&c, bucket, &[key]), None, "{key}");
        }
    }

    #[test]
    fn allowlist_denies_unlisted_bucket() {
        let listed = config(StorageScopeKind::Legacy, &["app-data"]);
        let c = ctx(&listed, true);
        assert_eq!(code(&c, "app-data", &["a"]), None);
        assert_eq!(code(&c, "other", &["a"]), Some("bucket_not_allowed"));
    }

    #[test]
    fn empty_allowlist_denies_foreign_declared_bucket() {
        let legacy = config(StorageScopeKind::Legacy, &[]);
        assert_eq!(
            code(&ctx(&legacy, true), "harbor-data", &["a"]),
            Some("foreign_bucket")
        );
        assert_eq!(code(&ctx(&legacy, false), "app-data", &["a"]), None);
    }

    #[test]
    fn buckets_the_tenant_group_declares_are_the_tenants_own() {
        let group_and_other = BucketClaims {
            by_group: true,
            by_other: true,
        };
        let unlisted = config(StorageScopeKind::Legacy, &[]);
        assert_eq!(
            code(&claims_ctx(&unlisted, group_and_other), "shared", &["a"]),
            None,
            "a preview without a row uses its source's buckets"
        );
        let cloned = config(StorageScopeKind::Legacy, &["app-data"]);
        assert_eq!(
            code(&claims_ctx(&cloned, group_and_other), "added-later", &["a"]),
            None,
            "a bucket the source added after the clone"
        );
        assert_eq!(
            code(
                &claims_ctx(&cloned, BucketClaims::default()),
                "other",
                &["a"]
            ),
            Some("bucket_not_allowed")
        );
        let strict = config(StorageScopeKind::Strict, &["app-data"]);
        assert_eq!(
            code(
                &claims_ctx(&strict, group_and_other),
                "added-later",
                &["photo.jpg"]
            ),
            Some("key_outside_prefix"),
            "the group widens buckets, not keys"
        );
    }

    #[test]
    fn strict_requires_allowlist() {
        let strict = config(StorageScopeKind::Strict, &[]);
        assert_eq!(
            code(
                &ctx(&strict, false),
                "app-data",
                &["tenants/harbor/books/a"]
            ),
            Some("strict_requires_allowlist")
        );
    }

    #[test]
    fn strict_enforces_tenant_service_prefix() {
        let strict = config(StorageScopeKind::Strict, &["harbor-data"]);
        let c = ctx(&strict, false);
        assert_eq!(
            code(&c, "harbor-data", &["tenants/harbor/books/photo.jpg"]),
            None
        );
        assert_eq!(
            code(&c, "harbor-data", &["tenants/harbor/other/photo.jpg"]),
            Some("key_outside_prefix")
        );
        assert_eq!(
            code(&c, "harbor-data", &["tenants/harbor/books/a", "photo.jpg"]),
            Some("key_outside_prefix")
        );
        assert_eq!(strict_prefix("harbor", "books"), "tenants/harbor/books/");
    }

    #[test]
    fn path_traversal_denied() {
        let strict = config(StorageScopeKind::Strict, &["harbor-data"]);
        assert_eq!(
            code(
                &ctx(&strict, false),
                "harbor-data",
                &["tenants/harbor/books/../../x"]
            ),
            Some("path_traversal")
        );
        let legacy = config(StorageScopeKind::Legacy, &[]);
        assert_eq!(
            code(&ctx(&legacy, false), "app-data", &["a/../b"]),
            Some("path_traversal")
        );
        assert_eq!(code(&ctx(&legacy, false), "app-data", &["a/..b/c"]), None);
    }

    #[test]
    fn legacy_empty_allowlist_allows_app_bucket() {
        let legacy = StorageScopeConfig::default();
        let c = StorageContext {
            tenant_slug: "default",
            service_name: "stock-service",
            platform_bucket: Some("example-platform"),
            claims: BucketClaims::default(),
            config: &legacy,
        };
        for key in [
            "items/42/photo.jpg",
            "wasm-exports/report.csv",
            "qr/batch-7.zip",
        ] {
            assert_eq!(code(&c, "example-app-data", &[key]), None, "{key}");
        }
    }

    #[test]
    fn every_prefix_the_platform_writes_is_reserved() {
        use crate::repo::module_artifacts::ModuleArtifact;
        let (service, other) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let written = [
            crate::wasm_runtime::WasmEngine::s3_key(service, 3),
            ModuleArtifact {
                service_id: service,
                version: 3,
                engine_fingerprint: "f".into(),
                wasm_sha256: "w".into(),
                sha256: "s".into(),
                size_bytes: 1,
            }
            .object_key(),
            crate::api::web::artifact_key(service, 3),
            crate::api::files::storage_key(service, other, "png"),
            crate::api::backup::export_key(service, other),
            crate::api::backup::restore_key(service, other),
            crate::deployer::native::object_key(service, "ab12"),
        ];
        for key in &written {
            assert!(in_platform_prefix(key), "{key} is not reserved");
        }
        for prefix in PLATFORM_KEY_PREFIXES {
            assert!(
                written.iter().any(|key| key.starts_with(prefix)),
                "{prefix} is reserved for what the platform writes"
            );
        }
    }

    fn policies(mode: PolicyMode, config: &StorageScopeConfig) -> PolicySet {
        PolicySet::builtin_defaults().with_tenant_rows(&[PolicyRow {
            tenant_id: Uuid::nil(),
            key: PolicyKey::StorageScope.as_str().to_string(),
            mode,
            config: serde_json::to_value(config).expect("a config"),
            updated_at: chrono::Utc::now(),
            updated_by: None,
        }])
    }

    #[test]
    fn a_guest_never_touches_a_native_binary_in_any_mode() {
        let binary = "native/00000000-0000-0000-0000-000000000001/9f86d081";
        for scope in [StorageScopeKind::Legacy, StorageScopeKind::Strict] {
            for mode in PolicyMode::ALL {
                let config = config(scope, &["cufflink-platform"]);
                let policies = policies(mode, &config);
                let context = ctx(policies.config::<StorageScopeConfig>(), false);
                for key in [
                    binary.to_string(),
                    format!("/{binary}"),
                    "native/".to_string(),
                ] {
                    assert!(
                        authorize(&context, &policies, "cufflink-platform", &[&key]).is_err(),
                        "{scope:?} {mode:?}: {key}"
                    );
                }
            }
        }
    }

    const HARBOR: Uuid = Uuid::from_u128(1);
    const GLOBEX: Uuid = Uuid::from_u128(2);

    fn registered(
        tenant_id: Uuid,
        bucket: &str,
        prefixes: &[&str],
        acls: &[CannedAcl],
    ) -> TenantBucket {
        TenantBucket {
            owner: Some(tenant_id),
            bucket: bucket.to_string(),
            state: State::Active {
                rules: Rules {
                    key_prefixes: Some(prefixes.iter().map(|p| p.to_string()).collect()),
                    acls: acls.iter().copied().collect(),
                },
                access: Access::Platform,
            },
        }
    }

    fn routed_code(result: Result<Route<'_>, Denied>) -> Result<Route<'_>, &'static str> {
        result.map_err(|denied| denied.code)
    }

    #[test]
    fn a_tenant_bucket_keeps_its_own_rules_in_every_mode() {
        let documents = registered(
            HARBOR,
            "example-documents",
            &["leads/"],
            &[CannedAcl::PublicRead],
        );
        let theirs = registered(
            GLOBEX,
            "example-ledgers",
            &["ledgers/"],
            &[CannedAcl::PublicRead],
        );
        for scope in [StorageScopeKind::Legacy, StorageScopeKind::Strict] {
            for mode in PolicyMode::ALL {
                let config = config(scope, &["example-ledgers", "example-media"]);
                let policies = policies(mode, &config);
                let context = ctx(policies.config::<StorageScopeConfig>(), false);
                let call = |owned, bucket, key, acl| {
                    routed_code(route(
                        &context,
                        &policies,
                        HARBOR,
                        owned,
                        bucket,
                        &[key],
                        acl,
                    ))
                };
                let case = format!("{scope:?} {mode:?}");
                assert_eq!(
                    call(
                        Some(&documents),
                        "example-documents",
                        "leads/7/Café façade #2.pdf",
                        None
                    ),
                    Ok(Route::TenantBucket(&Access::Platform)),
                    "{case}"
                );
                assert_eq!(
                    call(
                        Some(&documents),
                        "example-documents",
                        "leads/7/a.png",
                        Some(CannedAcl::PublicRead)
                    ),
                    Ok(Route::TenantBucket(&Access::Platform)),
                    "{case}"
                );
                for (key, acl, code) in [
                    ("other/a.pdf", None, "key_outside_prefix"),
                    ("leads//a.pdf", None, "key_not_exact"),
                    ("leads/a.pdf", Some(CannedAcl::Private), "acl_not_admitted"),
                    ("leads/../ledgers/a.pdf", None, "path_traversal"),
                ] {
                    assert_eq!(
                        call(Some(&documents), "example-documents", key, acl),
                        Err(code),
                        "{case}: {key}"
                    );
                }
                for acl in [None, Some(CannedAcl::PublicRead)] {
                    assert_eq!(
                        call(Some(&theirs), "example-ledgers", "ledgers/a.pdf", acl),
                        Err("foreign_tenant_bucket"),
                        "{case}: another tenant's bucket, though storage_scope lists it"
                    );
                }
                assert_eq!(
                    call(
                        None,
                        "example-media",
                        "tenants/harbor/books/a.png",
                        Some(CannedAcl::PublicRead)
                    ),
                    Err("acl_not_admitted"),
                    "{case}: only a tenant bucket admits an ACL"
                );
            }
        }
    }

    #[test]
    fn a_bucket_no_tenant_registers_is_left_to_storage_scope() {
        let config = config(StorageScopeKind::Legacy, &["example-media"]);
        for (mode, expected) in [
            (PolicyMode::Off, Ok(Route::Scope)),
            (PolicyMode::Warn, Ok(Route::Scope)),
            (PolicyMode::Enforce, Err("bucket_not_allowed")),
        ] {
            let policies = policies(mode, &config);
            let context = ctx(policies.config::<StorageScopeConfig>(), false);
            assert_eq!(
                routed_code(route(
                    &context,
                    &policies,
                    HARBOR,
                    None,
                    "example-other",
                    &["a b/Café.pdf"],
                    None
                )),
                expected,
                "{mode:?}"
            );
            assert_eq!(
                routed_code(route(
                    &context,
                    &policies,
                    HARBOR,
                    None,
                    "example-media",
                    &["a.pdf"],
                    None
                )),
                Ok(Route::Scope),
                "{mode:?}"
            );
        }
    }

    #[test]
    fn a_route_is_served_by_the_store_its_access_names() {
        let refused = |result: Result<S3Storage, Unserved>| match result {
            Ok(_) => "served".to_string(),
            Err(Unserved::Denied(denied)) => denied.code.to_string(),
            Err(Unserved::NotConfigured(why)) => why.to_string(),
            Err(Unserved::Backend(_)) => "backend".to_string(),
        };
        let credential = Access::Own(Credential::new("AKEXAMPLE", "example-secret"));
        let memory = S3Storage::from_inner(cufflink_storage::in_memory("cufflink-platform"));
        let s3 = S3Storage::from_inner(
            cufflink_storage::from_vars(|name| {
                match name {
                    "STORAGE_BACKEND" => Some("s3"),
                    "S3_ENDPOINT" => Some("http://127.0.0.1:9"),
                    "S3_REGION" => Some("us-east-1"),
                    "S3_ACCESS_KEY" => Some("AKPLATFORM"),
                    "S3_SECRET_KEY" => Some("platform-secret"),
                    "S3_BUCKET" => Some("cufflink-platform"),
                    _ => None,
                }
                .map(str::to_string)
            })
            .expect("an s3 backend"),
        );
        assert_eq!(
            refused(store(None, "example-documents", Route::Scope)),
            "object storage is not configured"
        );
        for platform in [&memory, &s3] {
            assert_eq!(
                refused(store(Some(platform), "example-documents", Route::Scope)),
                "served"
            );
            assert_eq!(
                refused(store(
                    Some(platform),
                    "example-documents",
                    Route::TenantBucket(&Access::Platform)
                )),
                "served"
            );
            assert_eq!(
                refused(store(
                    Some(platform),
                    "example-documents",
                    Route::TenantBucket(&Access::Unopenable)
                )),
                "the bucket's credential cannot be opened"
            );
        }
        assert_eq!(
            refused(store(
                Some(&s3),
                "example-documents",
                Route::TenantBucket(&credential)
            )),
            "served"
        );
        assert_eq!(
            refused(store(
                Some(&memory),
                "example-documents",
                Route::TenantBucket(&credential)
            )),
            "backend",
            "only s3 signs with a bucket's own credential"
        );
    }

    #[test]
    fn a_bucket_name_that_is_not_one_is_refused_in_every_mode_before_any_rule() {
        let documents = registered(HARBOR, "example-documents", &["leads/"], &[]);
        for scope in [StorageScopeKind::Legacy, StorageScopeKind::Strict] {
            for mode in PolicyMode::ALL {
                let config = config(scope, &["example-documents"]);
                let policies = policies(mode, &config);
                let context = ctx(policies.config::<StorageScopeConfig>(), false);
                for bucket in DISGUISED {
                    for owned in [None, Some(&documents)] {
                        for acl in [None, Some(CannedAcl::PublicRead)] {
                            assert_eq!(
                                routed_code(route(
                                    &context,
                                    &policies,
                                    GLOBEX,
                                    owned,
                                    bucket,
                                    &["leads/a.pdf"],
                                    acl
                                )),
                                Err("invalid_bucket"),
                                "{scope:?} {mode:?} {bucket:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_retired_or_unreadable_registration_refuses_every_tenant() {
        let config = config(StorageScopeKind::Legacy, &[]);
        for mode in PolicyMode::ALL {
            let policies = policies(mode, &config);
            let context = ctx(policies.config::<StorageScopeConfig>(), false);
            let call = |owned: &TenantBucket, tenant| {
                routed_code(route(
                    &context,
                    &policies,
                    tenant,
                    Some(owned),
                    "example-documents",
                    &["a.pdf"],
                    None,
                ))
                .map(|routed| match routed {
                    Route::Closed(why) => why,
                    Route::Scope | Route::TenantBucket(_) => "served",
                })
            };
            let retired = TenantBucket {
                state: State::Retired,
                ..registered(HARBOR, "example-documents", &["leads/"], &[])
            };
            let orphan = TenantBucket {
                owner: None,
                ..retired.clone()
            };
            let unreadable = TenantBucket {
                state: State::Unreadable,
                ..retired.clone()
            };
            assert_eq!(call(&retired, HARBOR), Err("bucket_retired"), "{mode:?}");
            assert_eq!(
                call(&retired, GLOBEX),
                Err("foreign_tenant_bucket"),
                "{mode:?}"
            );
            assert_eq!(
                call(&orphan, HARBOR),
                Err("foreign_tenant_bucket"),
                "{mode:?}"
            );
            assert_eq!(
                call(&orphan, GLOBEX),
                Err("foreign_tenant_bucket"),
                "{mode:?}"
            );
            assert_eq!(
                call(&unreadable, HARBOR),
                Ok("the bucket's registration cannot be read"),
                "{mode:?}"
            );
            assert_eq!(
                call(&unreadable, GLOBEX),
                Err("foreign_tenant_bucket"),
                "{mode:?}"
            );
        }
        assert!(matches!(
            store(
                Some(&S3Storage::from_inner(cufflink_storage::in_memory(
                    "cufflink-platform"
                ))),
                "example-documents",
                Route::Closed("the bucket's registration cannot be read")
            ),
            Err(Unserved::NotConfigured(
                "the bucket's registration cannot be read"
            ))
        ));
    }
}
