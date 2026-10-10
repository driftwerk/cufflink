use crate::auth::keycloak::TokenInfo;
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::config::AudienceConfig;
use crate::policy::{PolicyKey, Violation};
use crate::tenant_catalog::TenantEntry;
use serde_json::Value;

fn audiences(token: &TokenInfo) -> Vec<&str> {
    let aud = match token.claims.get("aud") {
        Some(Value::String(one)) => vec![one.as_str()],
        Some(Value::Array(many)) => many.iter().filter_map(Value::as_str).collect(),
        _ => vec![],
    };
    aud.into_iter()
        .chain(token.claims.get("azp").and_then(Value::as_str))
        .collect()
}

pub fn violation(token: &TokenInfo, allowed: &[String]) -> Option<Violation> {
    let presented = audiences(token);
    let accepted =
        allowed.is_empty() || presented.iter().any(|aud| allowed.iter().any(|a| a == aud));
    (!accepted).then(|| {
        Violation::new(
            "audience_not_allowed",
            format!("token audiences {presented:?} are not in the allowlist"),
        )
    })
}

pub(crate) fn check(tenant: &TenantEntry, token: &TokenInfo) -> PlatformResult<()> {
    let allowed = &tenant.config::<AudienceConfig>().allowed;
    tenant
        .apply(PolicyKey::Audience, violation(token, allowed))
        .map_err(|denied| PlatformError::Unauthorized(format!("{}: {}", denied.key, denied.code)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn token(claims: Value) -> TokenInfo {
        TokenInfo {
            sub: "u".into(),
            preferred_username: None,
            name: None,
            email: None,
            realm_roles: vec![],
            claims: serde_json::from_value(claims).expect("claims"),
        }
    }

    fn allow(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn empty_allowlist_passes() {
        assert_eq!(violation(&token(json!({})), &[]), None);
    }

    #[test]
    fn aud_string_match() {
        assert_eq!(
            violation(&token(json!({"aud": "portal"})), &allow(&["portal"])),
            None
        );
    }

    #[test]
    fn aud_array_match() {
        let t = token(json!({"aud": ["account", "backoffice"]}));
        assert_eq!(violation(&t, &allow(&["backoffice"])), None);
    }

    #[test]
    fn azp_match() {
        let t = token(json!({"aud": "account", "azp": "cufflink-cli"}));
        assert_eq!(violation(&t, &allow(&["cufflink-cli"])), None);
    }

    #[test]
    fn no_intersection_is_violation() {
        let t = token(json!({"aud": ["account"], "azp": "evil-client"}));
        let v = violation(&t, &allow(&["portal"])).expect("violation");
        assert_eq!(v.code, "audience_not_allowed");
        assert!(v.detail.contains("evil-client"), "{}", v.detail);
        assert!(violation(&token(json!({})), &allow(&["portal"])).is_some());
    }
}
