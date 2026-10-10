use super::extractor::{tenant_slug_header, AuthMethod, AuthUser};
use super::{audience, service_accounts};
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::{PolicyKey, PolicyMode, Violation};
use crate::repo::tenants::Tenant;
use crate::tenant_catalog::TenantEntry;
use crate::AppState;
use axum::http::HeaderMap;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MismatchReason {
    HeaderMismatch,
    AdminHeaderMismatch,
    ApiKeyForeignTenant,
    RealmMismatch,
    PathTenantHasNoRealm,
}

impl MismatchReason {
    pub fn code(self) -> &'static str {
        match self {
            MismatchReason::HeaderMismatch => "header_mismatch",
            MismatchReason::AdminHeaderMismatch => "admin_header_mismatch",
            MismatchReason::ApiKeyForeignTenant => "api_key_foreign_tenant",
            MismatchReason::RealmMismatch => "realm_mismatch",
            MismatchReason::PathTenantHasNoRealm => "path_tenant_has_no_realm",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingDecision {
    Bound { rebound: bool },
    Mismatch(MismatchReason),
}

pub fn classify(user: &AuthUser, path: &Tenant, header_slug: Option<&str>) -> BindingDecision {
    if user.tenant_id == path.id {
        return BindingDecision::Bound { rebound: false };
    }
    let reason = match (&user.auth_method, user.is_platform_admin) {
        (_, true) => MismatchReason::AdminHeaderMismatch,
        (AuthMethod::ApiKey { .. }, false) => MismatchReason::ApiKeyForeignTenant,
        (AuthMethod::Jwt, false) if header_slug.is_some() => MismatchReason::HeaderMismatch,
        (AuthMethod::Jwt, false) => match path.keycloak_realm.as_deref() {
            None => MismatchReason::PathTenantHasNoRealm,
            Some(realm) if user.realm.as_deref() == Some(realm) => {
                return BindingDecision::Bound { rebound: true };
            }
            Some(_) => MismatchReason::RealmMismatch,
        },
    };
    BindingDecision::Mismatch(reason)
}

pub(crate) fn rebind(
    mut user: AuthUser,
    path: &TenantEntry,
    env_allowlist: &HashSet<String>,
) -> AuthUser {
    user.tenant_id = path.tenant.id;
    user.tenant_slug = path.tenant.slug.clone();
    if let Some(token) = &user.token_info {
        user.has_deploy_authority = token.realm_roles.contains(&path.tenant.deploy_role);
        user.is_service_account = service_accounts::resolve(token, path, env_allowlist);
    }
    user
}

pub(crate) fn bind(
    principal: Option<AuthUser>,
    path: &TenantEntry,
    header_slug: Option<&str>,
    env_allowlist: &HashSet<String>,
) -> PlatformResult<Option<AuthUser>> {
    let Some(user) = principal else {
        return Ok(None);
    };
    if path.mode(PolicyKey::TenantBinding) == PolicyMode::Off {
        return Ok(Some(user));
    }
    match classify(&user, &path.tenant, header_slug) {
        BindingDecision::Bound { rebound: false } => Ok(Some(user)),
        BindingDecision::Bound { rebound: true } => {
            let user = rebind(user, path, env_allowlist);
            if let Some(token) = &user.token_info {
                audience::check(path, token)?;
            }
            Ok(Some(user))
        }
        BindingDecision::Mismatch(reason) => {
            let violation = Violation::new(
                reason.code(),
                format!(
                    "subject {} resolved to tenant {} on a route of tenant {}",
                    user.subject, user.tenant_slug, path.tenant.slug
                ),
            );
            path.apply(PolicyKey::TenantBinding, Some(violation))
                .map(|()| None)
                .map_err(PlatformError::from)
        }
    }
}

pub(crate) fn bind_principal(
    state: &AppState,
    principal: Option<AuthUser>,
    path: &TenantEntry,
    headers: &HeaderMap,
) -> PlatformResult<Option<AuthUser>> {
    bind(
        principal,
        path,
        tenant_slug_header(headers),
        &state.config.service_account_clients,
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::auth::keycloak::TokenInfo;
    use crate::policy::{PolicyMode, PolicySet};
    use crate::repo::tenant_policies::PolicyRow;
    use crate::tenant_catalog::tests::{policy_row, tenant};
    use serde_json::json;
    use std::sync::Arc;

    pub(crate) fn entry(t: Tenant, rows: &[PolicyRow]) -> TenantEntry {
        TenantEntry::new(
            t,
            Arc::new(PolicySet::builtin_defaults().with_tenant_rows(rows)),
            Vec::new(),
            Default::default(),
        )
    }

    pub(crate) fn binding(t: &Tenant, mode: PolicyMode) -> Vec<PolicyRow> {
        vec![policy_row(t.id, PolicyKey::TenantBinding, mode)]
    }

    pub(crate) fn jwt_user(on: &Tenant, realm: &str, azp: &str, roles: &[&str]) -> AuthUser {
        AuthUser {
            subject: "user-1".into(),
            name: None,
            email: None,
            tenant_id: on.id,
            tenant_slug: on.slug.clone(),
            auth_method: AuthMethod::Jwt,
            token_info: Some(TokenInfo {
                sub: "user-1".into(),
                preferred_username: None,
                name: None,
                email: None,
                realm_roles: roles.iter().map(|r| r.to_string()).collect(),
                claims: [("azp".to_string(), json!(azp))].into_iter().collect(),
            }),
            is_service_account: false,
            is_platform_admin: false,
            has_deploy_authority: false,
            realm: Some(realm.into()),
        }
    }

    pub(crate) fn api_key_user(on: &Tenant, platform_admin: bool) -> AuthUser {
        AuthUser {
            subject: if platform_admin {
                "apikey:admin".into()
            } else {
                on.keycloak_user_id.clone()
            },
            name: Some("key".into()),
            email: None,
            tenant_id: on.id,
            tenant_slug: on.slug.clone(),
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

    pub(crate) fn platform_jwt_user(on: &Tenant) -> AuthUser {
        AuthUser {
            is_platform_admin: true,
            has_deploy_authority: true,
            ..jwt_user(on, "cufflink", "cufflink-cli", &["cufflink-deploy"])
        }
    }

    struct Fixture {
        default: Tenant,
        preview: Tenant,
        harbor: Tenant,
        realmless: Tenant,
    }

    fn fixture() -> Fixture {
        Fixture {
            default: tenant("default", Some("orchard")),
            preview: tenant("preview-pr-7", Some("orchard")),
            harbor: tenant("harbor", Some("harbor")),
            realmless: tenant("legacy", None),
        }
    }

    use BindingDecision::{Bound, Mismatch};
    use MismatchReason::*;

    #[test]
    fn row_2_same_tenant_is_bound_without_rebinding() {
        let f = fixture();
        let user = jwt_user(&f.default, "orchard", "portal", &[]);
        assert_eq!(classify(&user, &f.default, None), Bound { rebound: false });
    }

    #[test]
    fn row_3_preview_sharing_the_realm_is_rebound() {
        let f = fixture();
        let user = jwt_user(&f.default, "orchard", "portal", &[]);
        assert_eq!(classify(&user, &f.preview, None), Bound { rebound: true });
    }

    #[test]
    fn row_4_header_naming_the_path_tenant_is_bound() {
        let f = fixture();
        let user = jwt_user(&f.preview, "orchard", "portal", &[]);
        assert_eq!(
            classify(&user, &f.preview, Some("preview-pr-7")),
            Bound { rebound: false }
        );
    }

    #[test]
    fn row_5_header_naming_another_tenant_is_a_mismatch() {
        let f = fixture();
        let user = jwt_user(&f.default, "orchard", "portal", &[]);
        assert_eq!(
            classify(&user, &f.preview, Some("default")),
            Mismatch(HeaderMismatch)
        );
    }

    #[test]
    fn row_6_other_realm_is_a_mismatch() {
        let f = fixture();
        let user = jwt_user(&f.default, "orchard", "portal", &[]);
        assert_eq!(classify(&user, &f.harbor, None), Mismatch(RealmMismatch));
    }

    #[test]
    fn row_7_path_tenant_without_realm_is_a_mismatch() {
        let f = fixture();
        let user = jwt_user(&f.default, "orchard", "portal", &[]);
        assert_eq!(
            classify(&user, &f.realmless, None),
            Mismatch(PathTenantHasNoRealm)
        );
    }

    #[test]
    fn rows_11_12_platform_admin_header_decides() {
        let f = fixture();
        let admin = platform_jwt_user(&f.preview);
        assert_eq!(
            classify(&admin, &f.preview, Some("preview-pr-7")),
            Bound { rebound: false }
        );
        assert_eq!(
            classify(&admin, &f.default, Some("preview-pr-7")),
            Mismatch(AdminHeaderMismatch)
        );
    }

    #[test]
    fn rows_14_15_tenant_api_keys_stay_on_their_tenant() {
        let f = fixture();
        let key = api_key_user(&f.default, false);
        assert_eq!(classify(&key, &f.default, None), Bound { rebound: false });
        assert_eq!(
            classify(&key, &f.preview, None),
            Mismatch(ApiKeyForeignTenant)
        );
    }

    #[test]
    fn row_16_admin_api_key_header_decides() {
        let f = fixture();
        let key = api_key_user(&f.harbor, true);
        assert_eq!(
            classify(&key, &f.harbor, Some("harbor")),
            Bound { rebound: false }
        );
        assert_eq!(
            classify(&key, &f.default, Some("harbor")),
            Mismatch(AdminHeaderMismatch)
        );
    }

    #[test]
    fn row_17_service_account_rebound_is_reclassified_on_the_path_tenant() {
        let f = fixture();
        let mut listed = f.default.clone();
        listed.service_account_clients = vec!["orchard-ops".into()];
        let mut user = jwt_user(&listed, "orchard", "orchard-ops", &[]);
        user.is_service_account = true;
        let rows = [policy_row(
            f.preview.id,
            PolicyKey::ServiceAccounts,
            PolicyMode::Enforce,
        )];
        let bound = bind(
            Some(user.clone()),
            &entry(f.preview.clone(), &rows),
            None,
            &HashSet::new(),
        )
        .expect("bound")
        .expect("principal");
        assert_eq!(bound.tenant_id, f.preview.id);
        assert!(
            !bound.is_service_account,
            "the preview does not list the client"
        );

        let off = bind(
            Some(user),
            &entry(f.preview.clone(), &[]),
            None,
            &["orchard-ops".to_string()].into_iter().collect(),
        )
        .expect("bound")
        .expect("principal");
        assert!(off.is_service_account, "warn keeps the platform list");
    }

    #[test]
    fn row_18_cross_realm_service_account_is_anonymous_under_warn_and_refused_under_enforce() {
        let f = fixture();
        let mut user = jwt_user(&f.default, "orchard", "orchard-ops", &["cufflink-deploy"]);
        user.is_service_account = true;
        user.has_deploy_authority = true;
        let warned = bind(
            Some(user.clone()),
            &entry(f.harbor.clone(), &[]),
            None,
            &HashSet::new(),
        )
        .expect("warn allows");
        assert!(warned.is_none());
        let err = bind(
            Some(user),
            &entry(f.harbor.clone(), &binding(&f.harbor, PolicyMode::Enforce)),
            None,
            &HashSet::new(),
        )
        .unwrap_err();
        assert!(
            matches!(&err, PlatformError::Forbidden(m) if m == "tenant_binding: realm_mismatch"),
            "{err:?}"
        );
    }

    #[test]
    fn row_1_anonymous_stays_anonymous() {
        let f = fixture();
        for mode in PolicyMode::ALL {
            let path = entry(f.default.clone(), &binding(&f.default, mode));
            assert!(bind(None, &path, None, &HashSet::new())
                .expect("ok")
                .is_none());
        }
    }

    #[test]
    fn row_22_blank_header_counts_as_absent() {
        let f = fixture();
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant-slug", "  ".parse().expect("header"));
        let user = jwt_user(&f.default, "orchard", "portal", &[]);
        assert_eq!(
            classify(&user, &f.preview, tenant_slug_header(&headers)),
            Bound { rebound: true }
        );
    }

    #[test]
    fn off_leaves_every_principal_as_authenticated() {
        let f = fixture();
        let mut listed = f.default.clone();
        listed.service_account_clients = vec!["orchard-ops".into()];
        let mut service_account = jwt_user(&listed, "orchard", "orchard-ops", &[]);
        service_account.is_service_account = true;
        for user in [
            api_key_user(&f.default, false),
            api_key_user(&f.default, true),
            service_account,
        ] {
            for path in [&f.preview, &f.harbor] {
                let off = entry(path.clone(), &binding(path, PolicyMode::Off));
                let kept = bind(Some(user.clone()), &off, None, &HashSet::new())
                    .expect("allowed")
                    .expect("principal");
                assert_eq!(kept.tenant_id, user.tenant_id);
                assert_eq!(kept.is_service_account, user.is_service_account);
                assert_eq!(kept.is_platform_admin, user.is_platform_admin);
                assert_eq!(kept.has_deploy_authority, user.has_deploy_authority);
            }
        }
    }

    #[test]
    fn a_mismatch_under_warn_is_recorded_and_continues_anonymously() {
        let f = fixture();
        for user in [
            api_key_user(&f.default, false),
            api_key_user(&f.default, true),
            jwt_user(&f.default, "orchard", "portal", &[]),
        ] {
            let path = entry(f.harbor.clone(), &binding(&f.harbor, PolicyMode::Warn));
            let header = user.is_platform_admin.then_some(f.default.slug.as_str());
            let recorder = cufflink_telemetry::metrics_builder()
                .expect("builder")
                .build_recorder();
            let handle = recorder.handle();
            let bound = metrics::with_local_recorder(&recorder, || {
                bind(Some(user.clone()), &path, header, &HashSet::new())
            })
            .expect("allowed");
            assert!(bound.is_none(), "{}", user.subject);
            assert!(handle
                .render()
                .contains(r#"key="tenant_binding",tenant="harbor",outcome="warned""#));
            let enforced = entry(f.harbor.clone(), &binding(&f.harbor, PolicyMode::Enforce));
            assert!(matches!(
                bind(Some(user), &enforced, header, &HashSet::new()),
                Err(PlatformError::Forbidden(_))
            ));
        }
    }

    #[test]
    fn rebind_recomputes_deploy_authority_and_sa_flag() {
        let f = fixture();
        let mut preview = f.preview.clone();
        preview.deploy_role = "preview-deploy".into();
        let mut user = jwt_user(&f.default, "orchard", "portal", &["preview-deploy"]);
        user.has_deploy_authority = false;
        user.is_service_account = true;
        let rebound = rebind(user, &entry(preview.clone(), &[]), &HashSet::new());
        assert_eq!(rebound.tenant_slug, "preview-pr-7");
        assert!(rebound.has_deploy_authority);
        assert!(!rebound.is_service_account);
    }

    #[test]
    fn rebind_keeps_subject_and_token() {
        let f = fixture();
        let user = jwt_user(&f.default, "orchard", "portal", &[]);
        let rebound = rebind(
            user.clone(),
            &entry(f.preview.clone(), &[]),
            &HashSet::new(),
        );
        assert_eq!(rebound.subject, user.subject);
        assert_eq!(rebound.realm, user.realm);
        assert_eq!(
            rebound.token_info.map(|t| t.sub),
            user.token_info.map(|t| t.sub)
        );
    }

    #[test]
    fn row_21_rebinding_rechecks_the_path_tenants_audience() {
        let f = fixture();
        let rows = [PolicyRow {
            config: json!({"allowed": ["backoffice"]}),
            ..policy_row(f.preview.id, PolicyKey::Audience, PolicyMode::Enforce)
        }];
        let user = jwt_user(&f.default, "orchard", "portal", &[]);
        let err = bind(
            Some(user),
            &entry(f.preview.clone(), &rows),
            None,
            &HashSet::new(),
        )
        .unwrap_err();
        assert!(matches!(err, PlatformError::Unauthorized(_)), "{err:?}");
    }
}
