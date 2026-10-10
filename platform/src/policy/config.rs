use super::{PolicyKey, PolicyMode};
use cufflink_types::manifest::validate::is_lowercase_identifier;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::marker::PhantomData;

const MIB: u64 = 1024 * 1024;
pub const MIN_WASM_MEMORY_BYTES: u64 = 64 * MIB;

pub trait PolicyConfig: Serialize + DeserializeOwned + Default {
    fn validate(&self, _mode: PolicyMode) -> Result<(), String> {
        Ok(())
    }
}

pub trait KeyConfig: PolicyConfig + Clone + PartialEq + std::fmt::Debug {
    const KEY: PolicyKey;

    fn slot(configs: &TypedConfigs) -> &Self;
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TypedConfigs {
    pub(super) storage_scope: StorageScopeConfig,
    pub(super) egress: EgressConfig,
    pub(super) wasm_limits: WasmLimitsConfig,
    pub(super) audience: AudienceConfig,
    pub(super) db_roles: DbRolesConfig,
    pub(super) schema_guard: SchemaGuardConfig,
}

macro_rules! key_config {
    ($config:ty, $key:expr, $field:ident) => {
        impl KeyConfig for $config {
            const KEY: PolicyKey = $key;

            fn slot(configs: &TypedConfigs) -> &Self {
                &configs.$field
            }
        }
    };
}

key_config!(StorageScopeConfig, PolicyKey::StorageScope, storage_scope);
key_config!(EgressConfig, PolicyKey::Egress, egress);
key_config!(WasmLimitsConfig, PolicyKey::WasmLimits, wasm_limits);
key_config!(AudienceConfig, PolicyKey::Audience, audience);
key_config!(DbRolesConfig, PolicyKey::DbRoles, db_roles);
key_config!(SchemaGuardConfig, PolicyKey::SchemaGuard, schema_guard);

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyConfig {}

impl PolicyConfig for EmptyConfig {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageScopeKind {
    #[default]
    Legacy,
    Strict,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageScopeConfig {
    pub scope: StorageScopeKind,
    pub allowed_buckets: Vec<String>,
}

impl PolicyConfig for StorageScopeConfig {
    fn validate(&self, _mode: PolicyMode) -> Result<(), String> {
        match self
            .allowed_buckets
            .iter()
            .find(|b| b.is_empty() || b.contains('/'))
        {
            Some(bad) => Err(format!(
                "allowed_buckets entry '{bad}' must be a non-empty bucket name without '/'"
            )),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EgressConfig {
    pub allow_hosts: Vec<String>,
    pub max_response_bytes: Option<u64>,
}

impl PolicyConfig for EgressConfig {
    fn validate(&self, _mode: PolicyMode) -> Result<(), String> {
        if let Some(bad) = self.allow_hosts.iter().find(|h| !is_host_pattern(h)) {
            return Err(format!(
                "allow_hosts entry '{bad}' must be a lowercase host name or '*.suffix' (no scheme, port or path)"
            ));
        }
        match self.max_response_bytes {
            Some(0) => Err("max_response_bytes must be greater than 0".into()),
            _ => Ok(()),
        }
    }
}

pub fn is_host_pattern(pattern: &str) -> bool {
    let host = pattern.strip_prefix("*.").unwrap_or(pattern);
    !host.is_empty()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WasmLimitsConfig {
    pub memory_bytes: Option<u64>,
}

impl PolicyConfig for WasmLimitsConfig {
    fn validate(&self, _mode: PolicyMode) -> Result<(), String> {
        match self.memory_bytes {
            Some(bytes) if bytes < MIN_WASM_MEMORY_BYTES => Err(format!(
                "memory_bytes must be at least {MIN_WASM_MEMORY_BYTES} (64 MiB)"
            )),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AudienceConfig {
    pub allowed: Vec<String>,
}

impl PolicyConfig for AudienceConfig {
    fn validate(&self, mode: PolicyMode) -> Result<(), String> {
        if mode == PolicyMode::Enforce && self.allowed.is_empty() {
            return Err("enforce with an empty audience allowlist would reject every token".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxConfig {}

impl PolicyConfig for OutboxConfig {
    fn validate(&self, mode: PolicyMode) -> Result<(), String> {
        match mode {
            PolicyMode::Warn => {
                Err("outbox has no warn mode; a shadow write would publish twice".into())
            }
            PolicyMode::Off | PolicyMode::Enforce => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DbRolesConfig {
    pub readers: Vec<String>,
    pub login_pool_max: Option<u32>,
}

impl DbRolesConfig {
    pub const DEFAULT_LOGIN_POOL_MAX: u32 = 4;

    pub fn login_pool_max(&self) -> u32 {
        self.login_pool_max.unwrap_or(Self::DEFAULT_LOGIN_POOL_MAX)
    }
}

impl PolicyConfig for DbRolesConfig {
    fn validate(&self, _mode: PolicyMode) -> Result<(), String> {
        if let Some(bad) = self
            .readers
            .iter()
            .find(|role| !is_lowercase_identifier(role))
        {
            return Err(format!(
                "readers entry '{bad}' must be a role name of lowercase letters, digits and '_', \
                 not starting with a digit"
            ));
        }
        match self.login_pool_max {
            Some(0) => Err("login_pool_max must be greater than 0".into()),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SchemaGuardConfig {
    pub accept_extra_columns: bool,
}

impl Default for SchemaGuardConfig {
    fn default() -> Self {
        Self {
            accept_extra_columns: true,
        }
    }
}

impl PolicyConfig for SchemaGuardConfig {}

pub(super) trait Schema: Sync {
    fn default_config(&self) -> Value;
    fn check(&self, mode: PolicyMode, config: &Value) -> Result<(), String>;
}

struct Typed<C>(PhantomData<fn() -> C>);

impl<C: PolicyConfig> Schema for Typed<C> {
    fn default_config(&self) -> Value {
        serde_json::to_value(C::default()).unwrap_or_else(|_| Value::Object(Default::default()))
    }

    fn check(&self, mode: PolicyMode, config: &Value) -> Result<(), String> {
        serde_json::from_value::<C>(config.clone())
            .map_err(|e| format!("invalid config: {e}"))?
            .validate(mode)
    }
}

pub(super) fn schema(key: PolicyKey) -> &'static dyn Schema {
    match key {
        PolicyKey::TenantBinding
        | PolicyKey::Graphql
        | PolicyKey::ServiceAccounts
        | PolicyKey::RedisScope
        | PolicyKey::StrictFilters => &Typed::<EmptyConfig>(PhantomData),
        PolicyKey::StorageScope => &Typed::<StorageScopeConfig>(PhantomData),
        PolicyKey::Egress => &Typed::<EgressConfig>(PhantomData),
        PolicyKey::WasmLimits => &Typed::<WasmLimitsConfig>(PhantomData),
        PolicyKey::Audience => &Typed::<AudienceConfig>(PhantomData),
        PolicyKey::Outbox => &Typed::<OutboxConfig>(PhantomData),
        PolicyKey::DbRoles => &Typed::<DbRolesConfig>(PhantomData),
        PolicyKey::SchemaGuard => &Typed::<SchemaGuardConfig>(PhantomData),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_patterns() {
        for ok in [
            "keycloak",
            "api.stripe.com",
            "*.storefront.example",
            "10.0.0.1",
        ] {
            assert!(is_host_pattern(ok), "{ok}");
        }
        for bad in [
            "",
            "*.",
            "*",
            "https://api.stripe.com",
            "API.stripe.com",
            "host:8080",
            "a..b",
            ".a",
            "a/b",
            "*.*.com",
        ] {
            assert!(!is_host_pattern(bad), "{bad}");
        }
    }

    #[test]
    fn default_configs_serialise_to_the_documented_shapes() {
        let config = |key| schema(key).default_config();
        assert_eq!(config(PolicyKey::TenantBinding), serde_json::json!({}));
        assert_eq!(
            config(PolicyKey::StorageScope),
            serde_json::json!({"scope": "legacy", "allowed_buckets": []})
        );
        assert_eq!(
            config(PolicyKey::Egress),
            serde_json::json!({"allow_hosts": [], "max_response_bytes": null})
        );
        assert_eq!(
            config(PolicyKey::WasmLimits),
            serde_json::json!({"memory_bytes": null})
        );
        assert_eq!(
            config(PolicyKey::Audience),
            serde_json::json!({"allowed": []})
        );
        assert_eq!(config(PolicyKey::StrictFilters), serde_json::json!({}));
        assert_eq!(config(PolicyKey::Outbox), serde_json::json!({}));
        assert_eq!(
            config(PolicyKey::DbRoles),
            serde_json::json!({"readers": [], "login_pool_max": null})
        );
        assert_eq!(
            config(PolicyKey::SchemaGuard),
            serde_json::json!({"accept_extra_columns": true})
        );
    }

    #[test]
    fn typed_configs_of_the_r3_policies_are_read_from_the_set() {
        use crate::policy::PolicySet;
        let set = PolicySet::builtin_defaults();
        assert_eq!(set.config::<DbRolesConfig>().login_pool_max(), 4);
        assert!(set.config::<DbRolesConfig>().readers.is_empty());
        assert!(set.config::<SchemaGuardConfig>().accept_extra_columns);
        assert_eq!(
            serde_json::from_value::<DbRolesConfig>(serde_json::json!({"login_pool_max": 2}))
                .map(|config| config.login_pool_max())
                .ok(),
            Some(2)
        );
    }
}
