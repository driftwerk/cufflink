use crate::auth::binding::bind;
use crate::auth::extractor::{AuthMethod, AuthUser};
use crate::auth::keycloak::TokenInfo;
use crate::auth::permissions::ResolvedPermissions;
use crate::auth::service_accounts;
use crate::deployer::manifest::{ServiceManifest, TableDefinition};
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::{PolicyKey, PolicyMode, PolicySet};
use crate::repo::tenant_policies::PolicyRow;
use crate::repo::tenants::Tenant;
use crate::router::authz::{authorize_table, grant_required, permission_decision};
use crate::router::postures::{Admission, PostureGate};
use crate::tenant_catalog::tests::{policy_row, tenant};
use crate::tenant_catalog::TenantEntry;
use cufflink_types::RoutePosture;
use futures_util::FutureExt;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

mod crud;
mod crud_events;
mod custom_routes;
mod eventing;
mod guest_db;
mod jobs;
mod schema;
mod status;
mod tables;

const ORCHARD_OPS: &str = "orchard-ops";

pub(crate) fn sample_manifest() -> ServiceManifest {
    serde_json::from_value(json!({
        "name": "stock-service",
        "mode": "wasm",
        "tables": [
            {
                "name": "items",
                "columns": [
                    {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true}
                ],
                "auth_required": true,
                "permission_area": "items"
            },
            {
                "name": "customer_notifications",
                "columns": [
                    {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                    {"name": "customer_id", "column_type": "text", "primary_key": false, "nullable": false, "auto_generate": false}
                ],
                "auth_required": true,
                "permission_area": "notifications",
                "owner_field": "customer_id",
                "owner_self_service": true
            }
        ],
        "custom_routes": [
            {"method": "POST", "path": "/stripe", "handler": "stripe_webhook", "posture": {"kind": "public", "reason": "signed webhook"}},
            {"method": "GET", "path": "/me", "handler": "me", "posture": {"kind": "authenticated"}},
            {"method": "GET", "path": "/items", "handler": "list_items", "posture": {"kind": "permission", "area": "items", "operation": "view"}},
            {"method": "GET", "path": "/orders", "handler": "my_orders", "posture": {"kind": "owner", "field": "customer_id"}},
            {"method": "POST", "path": "/reprice", "handler": "reprice", "posture": {"kind": "permission", "area": "items", "operation": "edit"}, "job": {"timeout_secs": 900, "max_attempts": 1}}
        ],
        "subscriptions": [
            {"subject": "dw.{tenant}.stock-service.items.updated", "handler": "on_item", "handler_type": "wasm"},
            {"subject": "cufflink.tick.5min", "handler": "sweep", "handler_type": "wasm", "stream": "CUFFLINK_TICKS"}
        ],
        "publishes": []
    }))
    .expect("sample manifest")
}

fn table<'a>(manifest: &'a ServiceManifest, name: &str) -> &'a TableDefinition {
    manifest.get_table(name).expect("fixture table")
}

fn posture(manifest: &ServiceManifest, handler: &str) -> Option<RoutePosture> {
    manifest.http_route(handler).and_then(|r| r.posture.clone())
}

pub(crate) struct Tenants {
    pub default: Tenant,
    pub preview: Tenant,
    pub harbor: Tenant,
}

pub(crate) fn tenants() -> Tenants {
    let mut default = tenant("default", Some("orchard"));
    default.service_account_clients = vec!["default-bot".into()];
    let mut preview = tenant("preview-pr-7", Some("orchard"));
    preview.service_account_clients = vec!["preview-bot".into()];
    let mut harbor = tenant("harbor", Some("harbor"));
    harbor.service_account_clients = vec!["harbor-bot".into()];
    Tenants {
        default,
        preview,
        harbor,
    }
}

fn entry(t: &Tenant, rows: &[PolicyRow]) -> TenantEntry {
    TenantEntry::new(
        t.clone(),
        Arc::new(PolicySet::builtin_defaults().with_tenant_rows(rows)),
        Vec::new(),
        Default::default(),
    )
}

fn modes(t: &Tenant, binding: PolicyMode, service_accounts: PolicyMode) -> Vec<PolicyRow> {
    vec![
        policy_row(t.id, PolicyKey::TenantBinding, binding),
        policy_row(t.id, PolicyKey::ServiceAccounts, service_accounts),
    ]
}

fn env_allowlist() -> HashSet<String> {
    [ORCHARD_OPS.to_string()].into_iter().collect()
}

fn jwt(resolved: &TenantEntry, realm: &str, sub: &str, azp: &str) -> AuthUser {
    let token = TokenInfo {
        sub: sub.into(),
        preferred_username: None,
        name: None,
        email: None,
        realm_roles: vec![],
        claims: [("azp".to_string(), json!(azp))].into_iter().collect(),
    };
    AuthUser {
        subject: sub.into(),
        name: None,
        email: None,
        tenant_id: resolved.tenant.id,
        tenant_slug: resolved.tenant.slug.clone(),
        auth_method: AuthMethod::Jwt,
        is_service_account: service_accounts::resolve(&token, resolved, &env_allowlist()),
        token_info: Some(token),
        is_platform_admin: false,
        has_deploy_authority: false,
        realm: Some(realm.into()),
    }
}

fn api_key(resolved: &Tenant, platform_admin: bool) -> AuthUser {
    AuthUser {
        subject: if platform_admin {
            "apikey:preview-bot".into()
        } else {
            resolved.keycloak_user_id.clone()
        },
        name: Some("ci".into()),
        email: None,
        tenant_id: resolved.id,
        tenant_slug: resolved.slug.clone(),
        auth_method: AuthMethod::ApiKey {
            id: uuid::Uuid::nil(),
            user_sub: None,
        },
        token_info: None,
        is_service_account: false,
        is_platform_admin: platform_admin,
        has_deploy_authority: true,
        realm: None,
    }
}

struct Grants(HashMap<(uuid::Uuid, String), Vec<String>>);

impl Grants {
    fn resolved(&self, tenant: &Tenant, subject: &str) -> ResolvedPermissions {
        ResolvedPermissions {
            permissions: self
                .0
                .get(&(tenant.id, subject.to_string()))
                .cloned()
                .unwrap_or_default(),
            role_names: vec![],
        }
    }
}

fn staff_grants(t: &Tenants) -> Grants {
    Grants(
        [&t.default, &t.preview]
            .into_iter()
            .map(|tenant| {
                (
                    (tenant.id, "staff".to_string()),
                    vec!["items:*".to_string()],
                )
            })
            .collect(),
    )
}

fn data_route(
    table: &TableDefinition,
    operation: &str,
    principal: Option<AuthUser>,
    header: Option<&str>,
    path: &TenantEntry,
    grants: &Grants,
) -> PlatformResult<Option<AuthUser>> {
    let bound = bind(principal, path, header, &env_allowlist())?;
    authorize_table(table, &[operation], &bound, |user| async {
        Ok(grants.resolved(&path.tenant, &user.subject))
    })
    .now_or_never()
    .unwrap_or_else(|| {
        Err(PlatformError::Internal(
            "grants resolve synchronously".into(),
        ))
    })?;
    Ok(bound)
}

fn fn_route(
    posture: Option<RoutePosture>,
    principal: Option<AuthUser>,
    header: Option<&str>,
    path: &TenantEntry,
    grants: &Grants,
) -> PlatformResult<Option<AuthUser>> {
    let gate = PostureGate::of(posture.as_ref());
    let principal = gate.admit(bind(principal, path, header, &env_allowlist()))?;
    match gate.admission(principal)? {
        Admission::Admit(principal) => Ok(principal),
        Admission::NeedsGrant {
            user,
            area,
            operation,
        } => permission_decision(
            &area,
            &operation,
            &grants.resolved(&path.tenant, &user.subject),
        )
        .map(|()| Some(user)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Ok,
    Unauthorized,
    Forbidden,
}

fn class<T>(result: PlatformResult<T>) -> Class {
    match result {
        Ok(_) => Class::Ok,
        Err(PlatformError::Unauthorized(_)) => Class::Unauthorized,
        Err(PlatformError::Forbidden(_)) => Class::Forbidden,
        Err(other) => panic!("unexpected error class: {other:?}"),
    }
}

mod cross_cutting {
    use super::*;

    #[test]
    fn preview_resolution_without_header_binds_to_path_tenant_and_keeps_permissions() {
        let t = tenants();
        let m = sample_manifest();
        let grants = staff_grants(&t);
        for binding in [PolicyMode::Warn, PolicyMode::Enforce] {
            let preview = entry(&t.preview, &modes(&t.preview, binding, PolicyMode::Warn));
            let staff = jwt(&entry(&t.default, &[]), "orchard", "staff", "backoffice");
            let bound = data_route(
                table(&m, "items"),
                "view",
                Some(staff),
                None,
                &preview,
                &grants,
            )
            .expect("allowed")
            .expect("principal");
            assert_eq!(bound.tenant_id, t.preview.id, "{binding}");
            assert_eq!(bound.tenant_slug, "preview-pr-7");
        }
    }

    #[test]
    fn preview_with_header_still_works() {
        let t = tenants();
        let m = sample_manifest();
        let preview = entry(
            &t.preview,
            &modes(&t.preview, PolicyMode::Enforce, PolicyMode::Warn),
        );
        let staff = jwt(&preview, "orchard", "staff", "backoffice");
        let bound = data_route(
            table(&m, "items"),
            "edit",
            Some(staff),
            Some("preview-pr-7"),
            &preview,
            &staff_grants(&t),
        )
        .expect("allowed")
        .expect("principal");
        assert_eq!(bound.tenant_id, t.preview.id);
    }

    #[test]
    fn the_ops_client_is_service_account_via_env_fallback_in_warn() {
        let t = tenants();
        let recorder = cufflink_telemetry::metrics_builder()
            .expect("builder")
            .build_recorder();
        let handle = recorder.handle();
        let ops = metrics::with_local_recorder(&recorder, || {
            jwt(&entry(&t.default, &[]), "orchard", "ops", ORCHARD_OPS)
        });
        assert!(ops.is_service_account);
        assert!(handle.render().contains(
            r#"cufflink_policy_violation_total{key="service_accounts",tenant="default",outcome="warned",reason="env_fallback"} 1"#
        ));
    }

    #[test]
    fn the_ops_client_is_not_service_account_under_enforce_unless_listed() {
        let t = tenants();
        let rows = modes(&t.default, PolicyMode::Warn, PolicyMode::Enforce);
        let ops = jwt(&entry(&t.default, &rows), "orchard", "ops", ORCHARD_OPS);
        assert!(!ops.is_service_account);
        let mut listed = t.default.clone();
        listed.service_account_clients.push(ORCHARD_OPS.into());
        let ops = jwt(&entry(&listed, &rows), "orchard", "ops", ORCHARD_OPS);
        assert!(ops.is_service_account);
    }

    #[test]
    fn authz_test_client_gets_zero_permissions_and_no_bypass() {
        let t = tenants();
        let m = sample_manifest();
        let default = entry(&t.default, &[]);
        let probe = jwt(&default, "orchard", "authz-test", "authz-test");
        assert!(!probe.is_service_account);
        let grants = staff_grants(&t);
        assert_eq!(
            class(data_route(
                table(&m, "items"),
                "view",
                Some(probe.clone()),
                None,
                &default,
                &grants
            )),
            Class::Forbidden
        );
        assert_eq!(
            class(fn_route(
                posture(&m, "list_items"),
                Some(probe),
                None,
                &default,
                &grants
            )),
            Class::Forbidden
        );
    }

    fn storage_allowed(bucket: &str, key: &str) -> bool {
        use crate::policy::config::StorageScopeConfig;
        use crate::wasm_runtime::storage_scope::{authorize, StorageContext};
        let t = tenants();
        let default = entry(&t.default, &[]);
        let context = StorageContext {
            tenant_slug: "default",
            service_name: "stock-service",
            platform_bucket: Some("example-platform"),
            claims: Default::default(),
            config: default.config::<StorageScopeConfig>(),
        };
        authorize(&context, &default.policies, bucket, &[key]).is_ok()
    }

    #[test]
    fn storage_legacy_scope_allows_app_bucket_keys() {
        for key in [
            "items/1/front.jpg",
            "exports/2026-09.csv",
            "qr/batch.zip",
            "wasm/not-platform",
        ] {
            assert!(storage_allowed("example-app-data", key), "{key}");
        }
    }

    #[test]
    fn denies_platform_wasm_prefix_in_platform_bucket() {
        assert!(!storage_allowed("example-platform", "wasm/victim/v3.wasm"));
        assert!(!storage_allowed(
            "example-platform",
            "backups/svc/export.json"
        ));
        assert!(!storage_allowed("example-platform", "web/svc/v2.tar.gz"));
        assert!(storage_allowed("example-platform", "media/logo.png"));
    }

    #[test]
    fn egress_warn_allows_internal_keycloak_and_platform_hosts() {
        use crate::egress::{EgressGuard, EgressTarget};
        let t = tenants();
        let default = entry(&t.default, &[]);
        for (host, addr) in [
            ("keycloak", "10.12.0.7:8080"),
            ("cufflink-platform", "10.12.3.9:8080"),
            ("dashboards", "10.12.4.2:3000"),
        ] {
            let target = EgressTarget {
                url: reqwest::Url::parse(&format!("http://{host}/")).expect("url"),
                host: host.into(),
                addrs: vec![addr.parse().expect("addr")],
            };
            let violation = EgressGuard::private_violation(&target, &[]);
            assert!(violation.is_some(), "{host} is internal");
            assert!(
                default.apply(PolicyKey::Egress, violation).is_ok(),
                "{host}"
            );
        }
    }

    #[test]
    fn subscriptions_within_own_tenant_and_ticks_validate() {
        let m = sample_manifest();
        for tenant in ["default", "preview-pr-7"] {
            assert!(
                crate::deployer::validator::validate_tenant_scope(
                    &m,
                    &crate::nats_manager::subjects::Namespace::own(tenant),
                    &[]
                )
                .is_ok(),
                "{tenant}"
            );
        }
    }
}
