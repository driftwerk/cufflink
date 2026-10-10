use crate::AppState;
use axum::{extract::State, Json};
use serde_json::{json, Value};
use std::sync::Arc;

const BASE_FEATURES: &[&str] = &[
    "bundled_wasm",
    "on_migrate",
    "wasm_hash_echo",
    "deploy_fanout",
    "tenant_policies",
];

const SUBSYSTEM_FEATURES: [&[&str]; 14] = [
    crate::router::custom::FEATURES,
    crate::auth::scope::FEATURES,
    crate::auth::columns::FEATURES,
    crate::auth::partner_keys::FEATURES,
    crate::wasm_runtime::FEATURES,
    crate::nats_manager::subjects::FEATURES,
    crate::nats_manager::subscriptions::FEATURES,
    crate::webhook_dispatcher::FEATURES,
    crate::router::jobs::FEATURES,
    crate::schema_manager::FEATURES,
    crate::tenant_sessions::FEATURES,
    crate::deployer::native::FEATURES,
    crate::api::deployments::FEATURES,
    crate::api::config::FEATURES,
];

pub fn platform_version() -> &'static str {
    option_env!("CUFFLINK_VERSION").unwrap_or(concat!(env!("CARGO_PKG_VERSION"), "-dev"))
}

pub fn supports(state: &AppState) -> Vec<&'static str> {
    let host = state
        .wasm_engine
        .as_ref()
        .map(|engine| crate::wasm_runtime::host::supported_features(engine.host_fn_names()))
        .unwrap_or_default();
    BASE_FEATURES
        .iter()
        .chain(crate::deployer::validator::PLATFORM_FEATURES)
        .chain(SUBSYSTEM_FEATURES.into_iter().flatten())
        .copied()
        .chain(host)
        .collect()
}

pub async fn version(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({
        "api_version": crate::api::deployments::DEPLOY_API_VERSION,
        "platform_version": platform_version(),
        "supports": supports(&state),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shutdown::Shutdown;
    use crate::wasm_runtime::{WasmEngine, WasmLimits};
    use std::collections::HashSet;
    use std::time::Duration;

    #[tokio::test]
    async fn supports_composes_protocol_manifest_and_host_features_once_each() {
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        let state = AppState::minimal(engine).await;
        let supports = supports(&state);
        for feature in [
            "bundled_wasm",
            "on_migrate",
            "route_posture",
            "nats_publish_authz",
            "host_core",
            "db_execute_all",
            "namespace_claims",
        ] {
            assert!(
                supports.contains(&feature),
                "{feature} missing from {supports:?}"
            );
        }
        let unique: HashSet<_> = supports.iter().collect();
        assert_eq!(unique.len(), supports.len(), "duplicates in {supports:?}");
    }

    #[tokio::test]
    async fn a_manifest_v0_27_0_accepted_demands_nothing_this_platform_lacks() {
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        let state = AppState::minimal(engine).await;
        let supports = supports(&state);
        for manifest in crate::deployer::validator::fixtures::accepted_by_v0_27_0() {
            let demanded = manifest.derived_features().into_iter().chain(
                manifest
                    .required_platform_features
                    .iter()
                    .map(String::as_str),
            );
            crate::deployer::validator::validate_platform_requirements(demanded, &supports)
                .unwrap_or_else(|e| panic!("{}: {e}", manifest.name));
        }
    }

    #[tokio::test]
    async fn a_feature_is_advertised_only_once_it_works() {
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        let state = AppState::minimal(engine).await;
        let supports = supports(&state);
        use cufflink_types::features::*;
        for finished in [
            NAMESPACE_CLAIMS,
            NATS_CONSUMER_CONFIG,
            NATS_MESSAGE_META,
            STREAMS,
            NATS_PUBLISH_V2,
            DELIVERY_CONTROL,
            CONSUMER_RECONCILER,
            HTTP_V2,
            STORAGE_V2,
            UTIL_V2,
            JOBS_V2,
            OUTBOX,
            SCHEMA_ADOPTED,
            SCHEMA_GUARD,
            SQL_MIGRATIONS,
            COLUMN_TYPES_V2,
            KEYSET_LISTS,
            ASYNC_DEPLOY,
            SCHEDULES,
            DB_ROLES,
            SECRET_ENCRYPTION,
            AUDIT_REDACTION,
            EXT_CALL,
            NATIVE_SERVICES,
            PRIVATE_TABLES,
            INACTIVE_DEPLOY,
            CONFIG_APPLY,
            COLUMN_PERMISSIONS,
        ] {
            assert!(supports.contains(&finished), "{finished}");
        }
        let mut advertised = supports.clone();
        advertised.sort_unstable();
        advertised.dedup();
        assert_eq!(
            advertised.len(),
            supports.len(),
            "each feature is advertised once: {supports:?}"
        );
    }

    fn documented(versions: &str) -> Vec<&str> {
        versions
            .split("\n## ")
            .filter(|section| {
                section.starts_with("Features before") || section.starts_with("Features added")
            })
            .flat_map(str::lines)
            .filter_map(|line| line.strip_prefix("| `"))
            .filter_map(|row| row.split_once('`'))
            .filter(|(_, rest)| rest.trim_start().starts_with('|'))
            .map(|(feature, _)| feature)
            .collect()
    }

    #[test]
    fn a_documented_feature_is_a_row_under_a_features_heading() {
        let doc = "# Versions\n\n## Features before R2\n\n| Feature | Provides |\n|---|---|\n\
                   | `a` | x |\n| `b` and `c` | y |\n\n## Map\n\n| `d` | `e` |\n\n\
                   ## Features added in R9\n\n| `f` | z |\n\n## Features removed in R9\n\n\
                   | `g` | gone |\n";
        assert_eq!(documented(doc), ["a", "f"]);
    }

    #[tokio::test]
    async fn the_versions_doc_lists_every_advertised_feature_once() {
        let engine = WasmEngine::new(WasmLimits::default(), Shutdown::new(Duration::from_secs(1)))
            .expect("engine");
        let state = AppState::minimal(engine).await;
        let mut advertised = supports(&state);
        advertised.sort_unstable();
        let mut listed = documented(include_str!("../../../docs/versions.md"));
        listed.sort_unstable();
        assert_eq!(listed, advertised, "docs/versions.md and supports differ");
    }

    #[test]
    fn platform_version_is_the_build_stamp_or_a_dev_marker() {
        let version = platform_version();
        assert!(
            version.starts_with("sha-") || version.ends_with("-dev"),
            "{version}"
        );
    }
}
