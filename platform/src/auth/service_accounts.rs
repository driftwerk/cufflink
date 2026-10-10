use crate::auth::keycloak::TokenInfo;
use crate::policy::{PolicyKey, PolicyMode, Violation};
use crate::repo::tenants::Tenant;
use crate::tenant_catalog::TenantEntry;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaVerdict {
    NotServiceAccount,
    TenantListed,
    EnvFallback,
}

impl SaVerdict {
    pub fn is_service_account(self) -> bool {
        self != SaVerdict::NotServiceAccount
    }
}

fn azp(token: &TokenInfo) -> Option<&str> {
    token.claims.get("azp").and_then(|v| v.as_str())
}

pub fn classify(
    token: &TokenInfo,
    tenant: &Tenant,
    env_allowlist: &HashSet<String>,
    mode: PolicyMode,
) -> SaVerdict {
    match azp(token) {
        Some(client) if tenant.service_account_clients.iter().any(|c| c == client) => {
            SaVerdict::TenantListed
        }
        Some(client) if mode != PolicyMode::Enforce && env_allowlist.contains(client) => {
            SaVerdict::EnvFallback
        }
        _ => SaVerdict::NotServiceAccount,
    }
}

pub(crate) fn resolve(
    token: &TokenInfo,
    tenant: &TenantEntry,
    env_allowlist: &HashSet<String>,
) -> bool {
    let verdict = classify(
        token,
        &tenant.tenant,
        env_allowlist,
        tenant.mode(PolicyKey::ServiceAccounts),
    );
    let fallback = (verdict == SaVerdict::EnvFallback).then(|| {
        Violation::new(
            "env_fallback",
            format!(
                "client '{}' is a service account only through CUFFLINK_SERVICE_ACCOUNT_CLIENTS",
                azp(token).unwrap_or_default()
            ),
        )
    });
    verdict.is_service_account() && tenant.apply(PolicyKey::ServiceAccounts, fallback).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant_catalog::tests::tenant;
    use serde_json::json;

    fn token(preferred_username: &str, azp: Option<&str>) -> TokenInfo {
        let claims = azp
            .map(|azp| [("azp".to_string(), json!(azp))].into_iter().collect())
            .unwrap_or_default();
        TokenInfo {
            sub: "user-123".to_string(),
            preferred_username: Some(preferred_username.to_string()),
            name: None,
            email: None,
            realm_roles: vec![],
            claims,
        }
    }

    fn allowlist(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    fn classify_env(t: &TokenInfo, env: &HashSet<String>) -> bool {
        classify(t, &tenant("default", Some("r")), env, PolicyMode::Off).is_service_account()
    }

    #[test]
    fn machine_client_on_allowlist_is_service_account() {
        let sa = allowlist(&["acme-ops"]);
        assert!(classify_env(
            &token("service-account-acme-ops", Some("acme-ops")),
            &sa
        ));
    }

    #[test]
    fn human_client_not_on_allowlist_is_not_service_account() {
        let sa = allowlist(&["acme-ops"]);
        assert!(!classify_env(&token("alice", Some("ops-mcp")), &sa));
    }

    #[test]
    fn spoofed_username_cannot_forge_service_account() {
        let sa = allowlist(&["acme-ops"]);
        assert!(!classify_env(
            &token("service-account-acme-ops", Some("portal")),
            &sa
        ));
    }

    #[test]
    fn missing_azp_is_not_service_account() {
        let sa = allowlist(&["acme-ops"]);
        assert!(!classify_env(&token("alice", None), &sa));
    }

    #[test]
    fn empty_allowlist_denies_all() {
        assert!(!classify_env(
            &token("service-account-acme-ops", Some("acme-ops")),
            &HashSet::new()
        ));
    }

    #[test]
    fn off_accepts_env_or_tenant_list() {
        let mut t = tenant("default", Some("r"));
        t.service_account_clients = vec!["tenant-bot".into()];
        let env = allowlist(&["orchard-ops"]);
        let verdict = |azp| classify(&token("x", Some(azp)), &t, &env, PolicyMode::Off);
        assert_eq!(verdict("orchard-ops"), SaVerdict::EnvFallback);
        assert_eq!(verdict("tenant-bot"), SaVerdict::TenantListed);
        assert_eq!(verdict("portal"), SaVerdict::NotServiceAccount);
    }

    #[test]
    fn warn_env_fallback_is_flagged() {
        let t = tenant("default", Some("r"));
        let env = allowlist(&["orchard-ops"]);
        assert_eq!(
            classify(&token("x", Some("orchard-ops")), &t, &env, PolicyMode::Warn),
            SaVerdict::EnvFallback
        );
    }

    #[test]
    fn enforce_ignores_env() {
        let t = tenant("default", Some("r"));
        let env = allowlist(&["orchard-ops"]);
        assert_eq!(
            classify(
                &token("x", Some("orchard-ops")),
                &t,
                &env,
                PolicyMode::Enforce
            ),
            SaVerdict::NotServiceAccount
        );
    }

    #[test]
    fn tenant_list_match_in_all_modes() {
        let mut t = tenant("default", Some("r"));
        t.service_account_clients = vec!["orchard-ops".into()];
        for mode in PolicyMode::ALL {
            assert_eq!(
                classify(&token("x", Some("orchard-ops")), &t, &HashSet::new(), mode),
                SaVerdict::TenantListed
            );
        }
    }
}
