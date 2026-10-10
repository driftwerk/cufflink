//! Names of the per-tenant platform policies and their modes, shared by the
//! platform (which evaluates them) and the CLI (which sets them).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyKey {
    TenantBinding,
    Graphql,
    StorageScope,
    Egress,
    WasmLimits,
    ServiceAccounts,
    Audience,
    RedisScope,
    StrictFilters,
    Outbox,
    DbRoles,
    SchemaGuard,
}

impl PolicyKey {
    pub const ALL: [PolicyKey; 12] = [
        PolicyKey::TenantBinding,
        PolicyKey::Graphql,
        PolicyKey::StorageScope,
        PolicyKey::Egress,
        PolicyKey::WasmLimits,
        PolicyKey::ServiceAccounts,
        PolicyKey::Audience,
        PolicyKey::RedisScope,
        PolicyKey::StrictFilters,
        PolicyKey::Outbox,
        PolicyKey::DbRoles,
        PolicyKey::SchemaGuard,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            PolicyKey::TenantBinding => "tenant_binding",
            PolicyKey::Graphql => "graphql",
            PolicyKey::StorageScope => "storage_scope",
            PolicyKey::Egress => "egress",
            PolicyKey::WasmLimits => "wasm_limits",
            PolicyKey::ServiceAccounts => "service_accounts",
            PolicyKey::Audience => "audience",
            PolicyKey::RedisScope => "redis_scope",
            PolicyKey::StrictFilters => "strict_filters",
            PolicyKey::Outbox => "outbox",
            PolicyKey::DbRoles => "db_roles",
            PolicyKey::SchemaGuard => "schema_guard",
        }
    }

    /// The suffix of the `CUFFLINK_POLICY_<KEY>` environment variables.
    pub fn env_suffix(self) -> String {
        self.as_str().to_ascii_uppercase()
    }
}

const _: () = {
    let mut i = 0;
    while i < PolicyKey::ALL.len() {
        assert!(PolicyKey::ALL[i] as usize == i);
        i += 1;
    }
};

impl fmt::Display for PolicyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PolicyKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let wanted = s.trim();
        PolicyKey::ALL
            .into_iter()
            .find(|key| key.as_str() == wanted)
            .ok_or_else(|| {
                let valid: Vec<&str> = PolicyKey::ALL.iter().map(|k| k.as_str()).collect();
                format!(
                    "unknown policy '{wanted}' (expected one of {})",
                    valid.join(", ")
                )
            })
    }
}

/// `Off` behaves as the platform did before the policy existed; `Warn`
/// evaluates, records and allows; `Enforce` evaluates, records and denies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyMode {
    #[default]
    Off,
    Warn,
    Enforce,
}

impl PolicyMode {
    pub const ALL: [PolicyMode; 3] = [PolicyMode::Off, PolicyMode::Warn, PolicyMode::Enforce];

    pub fn as_str(self) -> &'static str {
        match self {
            PolicyMode::Off => "off",
            PolicyMode::Warn => "warn",
            PolicyMode::Enforce => "enforce",
        }
    }
}

impl fmt::Display for PolicyMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PolicyMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let wanted = s.trim().to_ascii_lowercase();
        PolicyMode::ALL
            .into_iter()
            .find(|mode| mode.as_str() == wanted)
            .ok_or_else(|| {
                format!(
                    "unknown policy mode '{}' (expected off, warn or enforce)",
                    s.trim()
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_and_key_round_trip_str() {
        for key in PolicyKey::ALL {
            assert_eq!(key.to_string().parse::<PolicyKey>(), Ok(key));
            assert_eq!(
                serde_json::to_value(key).expect("serialize"),
                serde_json::json!(key.as_str())
            );
        }
        for mode in PolicyMode::ALL {
            assert_eq!(mode.to_string().parse::<PolicyMode>(), Ok(mode));
        }
        assert_eq!(" Enforce ".parse::<PolicyMode>(), Ok(PolicyMode::Enforce));
        assert_eq!(PolicyKey::TenantBinding.env_suffix(), "TENANT_BINDING");
    }

    #[test]
    fn unknown_names_list_the_valid_ones() {
        let err = "tenant-binding".parse::<PolicyKey>().unwrap_err();
        assert!(
            err.contains("tenant_binding") && err.contains("strict_filters"),
            "{err}"
        );
        let err = "loud".parse::<PolicyMode>().unwrap_err();
        assert!(err.contains("enforce"), "{err}");
    }
}
