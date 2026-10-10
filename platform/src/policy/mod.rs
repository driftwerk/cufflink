pub mod config;
pub(crate) mod record;

use crate::errors::PlatformError;
use crate::repo::tenant_policies::PolicyRow;
pub use config::PolicyConfig;
use config::{KeyConfig, TypedConfigs};
pub use cufflink_types::policy::{PolicyKey, PolicyMode};
use record::LogThrottle;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::LazyLock;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub mode: PolicyMode,
    pub config: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicySource {
    PlatformDefault,
    Tenant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EffectivePolicy {
    pub policy: Policy,
    pub source: PolicySource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub code: &'static str,
    pub detail: String,
}

impl Violation {
    pub fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Warned,
    Denied,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Warned => "warned",
            Outcome::Denied => "denied",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denied {
    pub key: PolicyKey,
    pub code: &'static str,
}

impl From<Denied> for PlatformError {
    fn from(denied: Denied) -> Self {
        PlatformError::Forbidden(format!("{}: {}", denied.key, denied.code))
    }
}

pub fn apply(
    mode: PolicyMode,
    key: PolicyKey,
    tenant_slug: &str,
    violation: Option<Violation>,
) -> Result<(), Denied> {
    let Some(violation) = violation else {
        return Ok(());
    };
    match mode {
        PolicyMode::Off => Ok(()),
        PolicyMode::Warn => {
            record::record(key, mode, tenant_slug, Outcome::Warned, &violation);
            Ok(())
        }
        PolicyMode::Enforce => {
            record::record(key, mode, tenant_slug, Outcome::Denied, &violation);
            Err(Denied {
                key,
                code: violation.code,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Correction {
    Leave,
    Correct,
}

pub fn correct(
    mode: PolicyMode,
    key: PolicyKey,
    tenant_slug: &str,
    violation: Option<Violation>,
) -> Correction {
    match (mode, violation) {
        (PolicyMode::Off, _) | (_, None) => Correction::Leave,
        (PolicyMode::Warn, Some(violation)) => {
            record::record(key, mode, tenant_slug, Outcome::Warned, &violation);
            Correction::Leave
        }
        (PolicyMode::Enforce, Some(_)) => Correction::Correct,
    }
}

pub fn deny(key: PolicyKey, tenant_slug: &str, violation: Violation) -> Denied {
    record::record(
        key,
        PolicyMode::Enforce,
        tenant_slug,
        Outcome::Denied,
        &violation,
    );
    Denied {
        key,
        code: violation.code,
    }
}

pub fn validate(key: PolicyKey, mode: PolicyMode, config: &Value) -> Result<(), String> {
    config::schema(key).check(mode, config)
}

fn builtin_mode(key: PolicyKey) -> PolicyMode {
    match key {
        PolicyKey::Graphql
        | PolicyKey::StrictFilters
        | PolicyKey::Outbox
        | PolicyKey::DbRoles
        | PolicyKey::SchemaGuard => PolicyMode::Off,
        PolicyKey::StorageScope => PolicyMode::Enforce,
        PolicyKey::TenantBinding
        | PolicyKey::Egress
        | PolicyKey::WasmLimits
        | PolicyKey::ServiceAccounts
        | PolicyKey::Audience
        | PolicyKey::RedisScope => PolicyMode::Warn,
    }
}

static UNKNOWN_ROW_KEYS: LazyLock<LogThrottle<String>> =
    LazyLock::new(|| LogThrottle::new(record::LOG_THROTTLE));
static UNREADABLE_CONFIGS: LazyLock<LogThrottle<PolicyKey>> =
    LazyLock::new(|| LogThrottle::new(record::LOG_THROTTLE));

type Entries = [EffectivePolicy; PolicyKey::ALL.len()];

#[derive(Debug, Clone, PartialEq)]
pub struct PolicySet {
    entries: Entries,
    configs: TypedConfigs,
}

fn read<T: KeyConfig>(entries: &Entries) -> T {
    let key = T::KEY;
    serde_json::from_value(entries[key as usize].policy.config.clone()).unwrap_or_else(|e| {
        if UNREADABLE_CONFIGS.should_log(key, Instant::now()) {
            tracing::warn!(target: "cufflink::policy", policy_key = key.as_str(), error = %e, "Unreadable policy config; using the builtin default");
        }
        T::default()
    })
}

impl PolicySet {
    fn from_entries(entries: Entries) -> Self {
        let configs = TypedConfigs {
            storage_scope: read(&entries),
            egress: read(&entries),
            wasm_limits: read(&entries),
            audience: read(&entries),
            db_roles: read(&entries),
            schema_guard: read(&entries),
        };
        Self { entries, configs }
    }

    pub fn builtin_defaults() -> Self {
        Self::from_entries(PolicyKey::ALL.map(|key| EffectivePolicy {
            policy: Policy {
                mode: builtin_mode(key),
                config: config::schema(key).default_config(),
            },
            source: PolicySource::PlatformDefault,
        }))
    }

    pub fn platform_defaults_from_env() -> Result<Self, String> {
        Self::platform_defaults_from(|name| std::env::var(name).ok())
    }

    pub(crate) fn platform_defaults_from(
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let builtin = Self::builtin_defaults();
        let entries: Vec<EffectivePolicy> = PolicyKey::ALL
            .into_iter()
            .map(|key| {
                let mode_var = format!("CUFFLINK_POLICY_{}", key.env_suffix());
                let config_var = format!("{mode_var}_CONFIG");
                let base = &builtin.get(key).policy;
                let mode = lookup(&mode_var)
                    .map(|raw| {
                        raw.parse::<PolicyMode>()
                            .map_err(|e| format!("{mode_var}: {e}"))
                    })
                    .transpose()?
                    .unwrap_or(base.mode);
                let config = lookup(&config_var)
                    .map(|raw| {
                        serde_json::from_str::<Value>(&raw)
                            .map_err(|e| format!("{config_var} is not valid JSON: {e}"))
                    })
                    .transpose()?
                    .unwrap_or_else(|| base.config.clone());
                validate(key, mode, &config).map_err(|e| format!("{mode_var}: {e}"))?;
                Ok(EffectivePolicy {
                    policy: Policy { mode, config },
                    source: PolicySource::PlatformDefault,
                })
            })
            .collect::<Result<_, String>>()?;
        entries
            .try_into()
            .map(Self::from_entries)
            .map_err(|_| "policy defaults are incomplete".to_string())
    }

    pub fn with_tenant_rows(&self, rows: &[PolicyRow]) -> Self {
        let mut layered = self.entries.clone();
        for row in rows {
            match row.key.parse::<PolicyKey>() {
                Ok(key) => {
                    layered[key as usize] = EffectivePolicy {
                        policy: Policy {
                            mode: row.mode,
                            config: row.config.clone(),
                        },
                        source: PolicySource::Tenant,
                    };
                }
                Err(e) => {
                    if UNKNOWN_ROW_KEYS.should_log(row.key.clone(), Instant::now()) {
                        tracing::warn!(target: "cufflink::policy", tenant_id = %row.tenant_id, error = %e, "Ignoring tenant policy row");
                    }
                }
            }
        }
        Self::from_entries(layered)
    }

    pub fn get(&self, key: PolicyKey) -> &EffectivePolicy {
        &self.entries[key as usize]
    }

    pub fn mode(&self, key: PolicyKey) -> PolicyMode {
        self.get(key).policy.mode
    }

    pub fn config<T: KeyConfig>(&self) -> &T {
        T::slot(&self.configs)
    }

    pub fn iter(&self) -> impl Iterator<Item = (PolicyKey, &EffectivePolicy)> {
        PolicyKey::ALL.into_iter().zip(self.entries.iter())
    }

    pub fn apply(
        &self,
        key: PolicyKey,
        tenant_slug: &str,
        violation: Option<Violation>,
    ) -> Result<(), Denied> {
        apply(self.mode(key), key, tenant_slug, violation)
    }

    pub fn correct(
        &self,
        key: PolicyKey,
        tenant_slug: &str,
        violation: Option<Violation>,
    ) -> Correction {
        correct(self.mode(key), key, tenant_slug, violation)
    }
}

#[cfg(test)]
mod tests {
    use super::config::{AudienceConfig, StorageScopeConfig, StorageScopeKind};
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn the_setup_guide_names_the_variable_of_every_policy() {
        let row = include_str!("../../../docs/setup.md")
            .lines()
            .find(|line| line.starts_with("| `CUFFLINK_POLICY_<KEY>`"))
            .expect("the CUFFLINK_POLICY_<KEY> row");
        for key in PolicyKey::ALL {
            let suffix = format!("`{}`", key.env_suffix());
            assert!(row.contains(&suffix), "{suffix} is not in: {row}");
        }
    }

    fn row(key: &str, mode: PolicyMode, config: Value) -> PolicyRow {
        PolicyRow {
            tenant_id: Uuid::nil(),
            key: key.into(),
            mode,
            config,
            updated_at: chrono::Utc::now(),
            updated_by: None,
        }
    }

    fn recorded(f: impl FnOnce()) -> String {
        let recorder = cufflink_telemetry::metrics_builder()
            .expect("builder")
            .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, f);
        handle.render()
    }

    #[test]
    fn builtin_defaults_cover_every_key() {
        let defaults = PolicySet::builtin_defaults();
        let modes: Vec<(PolicyKey, PolicyMode)> =
            defaults.iter().map(|(k, e)| (k, e.policy.mode)).collect();
        assert_eq!(
            modes,
            vec![
                (PolicyKey::TenantBinding, PolicyMode::Warn),
                (PolicyKey::Graphql, PolicyMode::Off),
                (PolicyKey::StorageScope, PolicyMode::Enforce),
                (PolicyKey::Egress, PolicyMode::Warn),
                (PolicyKey::WasmLimits, PolicyMode::Warn),
                (PolicyKey::ServiceAccounts, PolicyMode::Warn),
                (PolicyKey::Audience, PolicyMode::Warn),
                (PolicyKey::RedisScope, PolicyMode::Warn),
                (PolicyKey::StrictFilters, PolicyMode::Off),
                (PolicyKey::Outbox, PolicyMode::Off),
                (PolicyKey::DbRoles, PolicyMode::Off),
                (PolicyKey::SchemaGuard, PolicyMode::Off),
            ]
        );
        assert!(defaults
            .iter()
            .all(|(k, e)| e.source == PolicySource::PlatformDefault
                && validate(k, e.policy.mode, &e.policy.config).is_ok()));
    }

    #[test]
    fn strict_filters_takes_every_mode_and_no_config() {
        for mode in PolicyMode::ALL {
            assert!(validate(PolicyKey::StrictFilters, mode, &serde_json::json!({})).is_ok());
        }
        assert!(validate(
            PolicyKey::StrictFilters,
            PolicyMode::Enforce,
            &serde_json::json!({"max_depth": 8})
        )
        .is_err());
    }

    #[test]
    fn the_retired_wasm_limits_variable_is_ignored() {
        let mode = |vars: &[(&str, &str)]| {
            PolicySet::platform_defaults_from(|name| {
                vars.iter()
                    .find(|(var, _)| *var == name)
                    .map(|(_, value)| value.to_string())
            })
            .map(|set| set.mode(PolicyKey::WasmLimits))
        };
        assert_eq!(mode(&[]), Ok(PolicyMode::Warn));
        assert_eq!(
            mode(&[("CUFFLINK_WASM_LIMITS", "enforce")]),
            Ok(PolicyMode::Warn)
        );
        assert_eq!(
            mode(&[("CUFFLINK_POLICY_WASM_LIMITS", "enforce")]),
            Ok(PolicyMode::Enforce)
        );
    }

    #[test]
    fn env_overrides_mode_and_config() {
        let set = PolicySet::platform_defaults_from(|name| match name {
            "CUFFLINK_POLICY_GRAPHQL" => Some("enforce".into()),
            "CUFFLINK_POLICY_AUDIENCE" => Some("Enforce".into()),
            "CUFFLINK_POLICY_AUDIENCE_CONFIG" => Some(r#"{"allowed":["portal"]}"#.into()),
            _ => None,
        })
        .expect("valid env");
        assert_eq!(set.mode(PolicyKey::Graphql), PolicyMode::Enforce);
        assert_eq!(set.mode(PolicyKey::Audience), PolicyMode::Enforce);
        assert_eq!(
            set.config::<AudienceConfig>().allowed,
            vec!["portal".to_string()]
        );
        assert_eq!(set.mode(PolicyKey::Egress), PolicyMode::Warn);
        assert_eq!(
            set.get(PolicyKey::Graphql).source,
            PolicySource::PlatformDefault
        );
    }

    #[test]
    fn env_malformed_values_are_errors_naming_the_variable() {
        let err = PolicySet::platform_defaults_from(|name| {
            (name == "CUFFLINK_POLICY_EGRESS").then(|| "loud".to_string())
        })
        .unwrap_err();
        assert!(err.contains("CUFFLINK_POLICY_EGRESS"), "{err}");
        let err = PolicySet::platform_defaults_from(|name| {
            (name == "CUFFLINK_POLICY_EGRESS_CONFIG").then(|| "{".to_string())
        })
        .unwrap_err();
        assert!(err.contains("CUFFLINK_POLICY_EGRESS_CONFIG"), "{err}");
        let err = PolicySet::platform_defaults_from(|name| {
            (name == "CUFFLINK_POLICY_AUDIENCE").then(|| "enforce".to_string())
        })
        .unwrap_err();
        assert!(err.contains("empty audience allowlist"), "{err}");
    }

    #[test]
    fn with_tenant_rows_overrides_and_marks_source_tenant() {
        let set = PolicySet::builtin_defaults().with_tenant_rows(&[row(
            "storage_scope",
            PolicyMode::Enforce,
            json!({"scope": "strict", "allowed_buckets": ["harbor"]}),
        )]);
        let storage = set.get(PolicyKey::StorageScope);
        assert_eq!(storage.source, PolicySource::Tenant);
        assert_eq!(
            *set.config::<StorageScopeConfig>(),
            StorageScopeConfig {
                scope: StorageScopeKind::Strict,
                allowed_buckets: vec!["harbor".into()],
            }
        );
        assert_eq!(
            set.get(PolicyKey::Egress).source,
            PolicySource::PlatformDefault
        );
    }

    #[test]
    fn unknown_row_key_is_ignored() {
        let defaults = PolicySet::builtin_defaults();
        let set =
            defaults.with_tenant_rows(&[row("secret_rotation", PolicyMode::Enforce, json!({}))]);
        assert_eq!(set, defaults);
    }

    #[test]
    fn config_falls_back_to_default_on_bad_json() {
        let set = PolicySet::builtin_defaults().with_tenant_rows(&[row(
            "audience",
            PolicyMode::Warn,
            json!({"allowed": "portal"}),
        )]);
        assert_eq!(*set.config::<AudienceConfig>(), AudienceConfig::default());
    }

    #[test]
    fn apply_off_allows_silently() {
        let rendered = recorded(|| {
            let v = Violation::new("realm_mismatch", "detail");
            assert_eq!(
                apply(PolicyMode::Off, PolicyKey::TenantBinding, "t", Some(v)),
                Ok(())
            );
        });
        assert!(
            !rendered.contains("cufflink_policy_violation_total"),
            "{rendered}"
        );
    }

    #[test]
    fn apply_warn_allows_and_records() {
        let rendered = recorded(|| {
            let v = Violation::new("realm_mismatch", "detail");
            assert_eq!(
                apply(PolicyMode::Warn, PolicyKey::TenantBinding, "t", Some(v)),
                Ok(())
            );
        });
        assert!(
            rendered.contains(
                r#"cufflink_policy_violation_total{key="tenant_binding",tenant="t",outcome="warned",reason="realm_mismatch"} 1"#
            ),
            "{rendered}"
        );
    }

    #[test]
    fn apply_enforce_denies_and_records() {
        let rendered = recorded(|| {
            let v = Violation::new("realm_mismatch", "detail");
            let denied =
                apply(PolicyMode::Enforce, PolicyKey::TenantBinding, "t", Some(v)).unwrap_err();
            assert_eq!(
                denied,
                Denied {
                    key: PolicyKey::TenantBinding,
                    code: "realm_mismatch"
                }
            );
            let err = PlatformError::from(denied);
            assert!(
                matches!(&err, PlatformError::Forbidden(m) if m == "tenant_binding: realm_mismatch"),
                "{err}"
            );
        });
        assert!(
            rendered.contains(r#"outcome="denied",reason="realm_mismatch"} 1"#),
            "{rendered}"
        );
    }

    #[test]
    fn deny_records_regardless_of_mode() {
        let rendered = recorded(|| {
            let denied = deny(
                PolicyKey::RedisScope,
                "t",
                Violation::new("reserved_prefix", "auth:perms"),
            );
            assert_eq!(denied.code, "reserved_prefix");
        });
        assert!(
            rendered.contains(r#"cufflink_policy_violation_total{key="redis_scope",tenant="t",outcome="denied",reason="reserved_prefix"} 1"#),
            "{rendered}"
        );
    }

    #[test]
    fn correct_leaves_and_records_only_under_warn() {
        let rendered = recorded(|| {
            let correction = |mode| {
                let v = Violation::new("auth_header_forwarded", "detail");
                correct(mode, PolicyKey::TenantBinding, "t", Some(v))
            };
            assert_eq!(correction(PolicyMode::Off), Correction::Leave);
            assert_eq!(correction(PolicyMode::Warn), Correction::Leave);
            assert_eq!(correction(PolicyMode::Enforce), Correction::Correct);
        });
        assert!(
            rendered.contains(
                r#"cufflink_policy_violation_total{key="tenant_binding",tenant="t",outcome="warned",reason="auth_header_forwarded"} 1"#
            ),
            "{rendered}"
        );
        assert!(!rendered.contains(r#"outcome="denied""#), "{rendered}");
    }

    #[test]
    fn correct_no_violation_leaves_and_never_records() {
        let rendered = recorded(|| {
            for mode in PolicyMode::ALL {
                assert_eq!(
                    correct(mode, PolicyKey::TenantBinding, "t", None),
                    Correction::Leave
                );
            }
        });
        assert!(
            !rendered.contains("cufflink_policy_violation_total"),
            "{rendered}"
        );
    }

    #[test]
    fn apply_no_violation_never_records() {
        let rendered = recorded(|| {
            for mode in PolicyMode::ALL {
                assert_eq!(apply(mode, PolicyKey::Egress, "t", None), Ok(()));
            }
        });
        assert!(
            !rendered.contains("cufflink_policy_violation_total"),
            "{rendered}"
        );
    }

    #[test]
    fn validate_rejects_unknown_config_field() {
        let err =
            validate(PolicyKey::TenantBinding, PolicyMode::Warn, &json!({"x": 1})).unwrap_err();
        assert!(err.contains("unknown field"), "{err}");
        assert!(validate(PolicyKey::Egress, PolicyMode::Warn, &json!({"allow": []})).is_err());
    }

    #[test]
    fn validate_rejects_enforce_audience_with_empty_allowlist() {
        assert!(validate(PolicyKey::Audience, PolicyMode::Enforce, &json!({})).is_err());
        assert!(validate(PolicyKey::Audience, PolicyMode::Warn, &json!({})).is_ok());
        assert!(validate(
            PolicyKey::Audience,
            PolicyMode::Enforce,
            &json!({"allowed": ["portal"]})
        )
        .is_ok());
    }

    #[test]
    fn validate_rejects_bad_host_pattern() {
        let err = validate(
            PolicyKey::Egress,
            PolicyMode::Enforce,
            &json!({"allow_hosts": ["https://keycloak"]}),
        )
        .unwrap_err();
        assert!(err.contains("https://keycloak"), "{err}");
        assert!(validate(
            PolicyKey::Egress,
            PolicyMode::Enforce,
            &json!({"allow_hosts": ["keycloak", "*.storefront.example"], "max_response_bytes": 1024})
        )
        .is_ok());
        assert!(validate(
            PolicyKey::Egress,
            PolicyMode::Warn,
            &json!({"max_response_bytes": 0})
        )
        .is_err());
    }

    #[test]
    fn validate_storage_buckets_and_wasm_memory() {
        assert!(validate(
            PolicyKey::StorageScope,
            PolicyMode::Enforce,
            &json!({"allowed_buckets": ["a/b"]})
        )
        .is_err());
        assert!(validate(
            PolicyKey::StorageScope,
            PolicyMode::Enforce,
            &json!({"allowed_buckets": [""]})
        )
        .is_err());
        assert!(validate(
            PolicyKey::WasmLimits,
            PolicyMode::Enforce,
            &json!({"memory_bytes": 1024})
        )
        .is_err());
        assert!(validate(
            PolicyKey::WasmLimits,
            PolicyMode::Enforce,
            &json!({"memory_bytes": 128 * 1024 * 1024})
        )
        .is_ok());
    }
}
