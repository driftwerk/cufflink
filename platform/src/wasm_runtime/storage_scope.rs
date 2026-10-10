use crate::policy::config::{StorageScopeConfig, StorageScopeKind};
use crate::policy::{self, Denied, PolicyKey, PolicySet, Violation};
use crate::AppState;
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
    fn denied(&self, code: &'static str, bucket: &str, keys: &[&str]) -> Option<Violation> {
        Some(Violation::new(
            code,
            format!(
                "service {} of tenant {} on bucket {bucket} keys {keys:?}",
                self.service_name, self.tenant_slug
            ),
        ))
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
    if bucket.is_empty() || bucket.contains('/') {
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

pub struct Guest<'a> {
    pub tenant_id: Uuid,
    pub tenant_slug: &'a str,
    pub service_name: &'a str,
    pub policies: &'a PolicySet,
}

pub fn authorize_guest(
    app_state: &AppState,
    guest: &Guest,
    bucket: &str,
    keys: &[&str],
) -> Result<(), Denied> {
    let context = StorageContext {
        tenant_slug: guest.tenant_slug,
        service_name: guest.service_name,
        platform_bucket: app_state.storage.as_ref().map(|s| s.primary_bucket()),
        claims: app_state.tenants.bucket_claims(guest.tenant_id, bucket),
        config: guest.policies.config::<StorageScopeConfig>(),
    };
    authorize(&context, guest.policies, bucket, keys)
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
        for bucket in ["", "cufflink-platform/wasm", "../x", "/"] {
            assert_eq!(code(&c, bucket, &["a"]), Some("invalid_bucket"), "{bucket}");
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

    #[test]
    fn a_guest_never_touches_a_native_binary_in_any_mode() {
        use crate::repo::tenant_policies::PolicyRow;
        use cufflink_types::policy::PolicyMode;
        let binary = "native/00000000-0000-0000-0000-000000000001/9f86d081";
        for scope in [StorageScopeKind::Legacy, StorageScopeKind::Strict] {
            for mode in PolicyMode::ALL {
                let config = config(scope, &["cufflink-platform"]);
                let policies = PolicySet::builtin_defaults().with_tenant_rows(&[PolicyRow {
                    tenant_id: Uuid::nil(),
                    key: PolicyKey::StorageScope.as_str().to_string(),
                    mode,
                    config: serde_json::to_value(&config).expect("a config"),
                    updated_at: chrono::Utc::now(),
                    updated_by: None,
                }]);
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
}
