use crate::auth::PlatformAdmin;
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::{self, PolicyKey, PolicyMode, PolicySet};
use crate::repo::{audit, tenant_policies, tenants};
use crate::AppState;
use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutPoliciesRequest {
    pub policies: BTreeMap<String, PolicyUpdate>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyUpdate {
    pub mode: String,
    pub config: Option<Value>,
}

fn resolve_updates(
    request: PutPoliciesRequest,
    current: &PolicySet,
) -> PlatformResult<Vec<(PolicyKey, PolicyMode, Value)>> {
    if request.policies.is_empty() {
        return Err(PlatformError::Validation(
            "policies must name at least one key".into(),
        ));
    }
    request
        .policies
        .into_iter()
        .map(|(name, update)| {
            let key: PolicyKey = name.parse().map_err(PlatformError::Validation)?;
            let mode: PolicyMode = update
                .mode
                .parse()
                .map_err(|e| PlatformError::Validation(format!("{key}: {e}")))?;
            let config = update
                .config
                .unwrap_or_else(|| current.get(key).policy.config.clone());
            policy::validate(key, mode, &config)
                .map_err(|e| PlatformError::Validation(format!("{key}: {e}")))?;
            Ok((key, mode, config))
        })
        .collect()
}

fn policies_body(slug: &str, set: &PolicySet) -> Value {
    let policies: Map<String, Value> = set
        .iter()
        .map(|(key, effective)| {
            (
                key.to_string(),
                json!({
                    "mode": effective.policy.mode,
                    "config": effective.policy.config,
                    "source": effective.source,
                }),
            )
        })
        .collect();
    json!({ "tenant": slug, "policies": policies })
}

async fn effective(state: &AppState, tenant: &tenants::Tenant) -> PlatformResult<PolicySet> {
    let client = state.platform_db.get().await?;
    let rows = tenant_policies::list_for_tenant(&client, tenant.id).await?;
    Ok(state.tenants.defaults().with_tenant_rows(&rows))
}

fn require_role_bindings(unbound: &[String]) -> PlatformResult<()> {
    if unbound.is_empty() {
        Ok(())
    } else {
        Err(PlatformError::Validation(format!(
            "db_roles: enforce needs a verified database role for every service of the tenant; \
             without one: {}",
            unbound.join(", ")
        )))
    }
}

async fn require_tenant_rules(
    state: &AppState,
    tenant: &tenants::Tenant,
    current: &PolicySet,
    key: PolicyKey,
    mode: PolicyMode,
) -> PlatformResult<()> {
    match (key, mode) {
        (PolicyKey::DbRoles, PolicyMode::Warn | PolicyMode::Enforce)
            if current.mode(key) == PolicyMode::Off
                && !state.tenant_sessions.resets_session_state() =>
        {
            Err(PlatformError::Validation(
                crate::db_roles::NEEDS_CLEAN_RECYCLE.to_string(),
            ))
        }
        (PolicyKey::DbRoles, PolicyMode::Enforce) if current.mode(key) == mode => Ok(()),
        (PolicyKey::DbRoles, PolicyMode::Enforce) => require_role_bindings(
            &crate::repo::services::without_verified_role(&state.platform_db, tenant.id).await?,
        ),
        (PolicyKey::Outbox, PolicyMode::Enforce) => {
            crate::outbox::seal::require_key(mode, state.outbox_seal.as_ref())
                .map_err(PlatformError::Validation)?;
            crate::outbox::bootstrap::ensure(&state.tenant_sessions, tenant.id)
                .await
                .map(drop)
                .map_err(|e| match PlatformError::from(e) {
                    PlatformError::Validation(reason) => {
                        PlatformError::Validation(format!("outbox: {reason}"))
                    }
                    other => other,
                })
        }
        _ => Ok(()),
    }
}

pub async fn get_policies(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(slug): Path<String>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    Ok(Json(policies_body(
        &slug,
        &effective(&state, &tenant).await?,
    )))
}

pub async fn put_policies(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path(slug): Path<String>,
    Json(request): Json<PutPoliciesRequest>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let current = effective(&state, &tenant).await?;
    let updates = resolve_updates(request, &current)?;
    for (key, mode, _) in &updates {
        require_tenant_rules(&state, &tenant, &current, *key, *mode).await?;
    }
    let mut client = state.platform_db.get().await?;
    let txn = client.transaction().await?;
    for (key, mode, config) in &updates {
        tenant_policies::upsert(&txn, tenant.id, *key, *mode, config, &admin.subject).await?;
    }
    txn.commit().await?;
    drop(client);
    for (key, mode, config) in &updates {
        info!(tenant = %slug, policy = %key, mode = %mode, actor = %admin.subject, "Tenant policy set");
        audit::record(
            &state.platform_db,
            audit::Change {
                tenant_id: Some(tenant.id),
                actor: &admin.subject,
                credential: admin.credential(),
                action: "policy.set",
                resource_type: "policy",
                resource_id: Some(&key.to_string()),
                details: Some(json!({"mode": mode.to_string(), "config": config})),
            },
        )
        .await;
    }
    state.tenants.refresh().await;
    Ok(Json(policies_body(
        &slug,
        &effective(&state, &tenant).await?,
    )))
}

pub async fn delete_policy(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, key)): Path<(String, String)>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let key: PolicyKey = key.parse().map_err(PlatformError::Validation)?;
    require_tenant_rules(
        &state,
        &tenant,
        &effective(&state, &tenant).await?,
        key,
        state.tenants.defaults().mode(key),
    )
    .await?;
    if !tenant_policies::delete(&state.platform_db, tenant.id, key).await? {
        return Err(PlatformError::NotFound(format!(
            "Tenant '{slug}' has no '{key}' policy of its own"
        )));
    }
    info!(tenant = %slug, policy = %key, actor = %admin.subject, "Tenant policy reset to the platform default");
    audit::record(
        &state.platform_db,
        audit::Change {
            tenant_id: Some(tenant.id),
            actor: &admin.subject,
            credential: admin.credential(),
            action: "policy.delete",
            resource_type: "policy",
            resource_id: Some(&key.to_string()),
            details: None,
        },
    )
    .await;
    state.tenants.refresh().await;
    Ok(Json(policies_body(
        &slug,
        &effective(&state, &tenant).await?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::tenant_policies::PolicyRow;

    fn request(body: Value) -> PutPoliciesRequest {
        serde_json::from_value(body).expect("request shape")
    }

    #[test]
    fn misspelled_request_fields_are_refused() {
        for body in [
            json!({"policies": {"egress": {"mode": "enforce", "conifg": {"allow_hosts": []}}}}),
            json!({"policies": {}, "polices": {"egress": {"mode": "enforce"}}}),
        ] {
            let err = serde_json::from_value::<PutPoliciesRequest>(body.clone())
                .expect_err("unknown field");
            assert!(err.to_string().contains("unknown field"), "{body}: {err}");
        }
    }

    #[test]
    fn missing_config_keeps_the_current_effective_config() {
        let current = PolicySet::builtin_defaults().with_tenant_rows(&[PolicyRow {
            tenant_id: uuid::Uuid::nil(),
            key: "egress".into(),
            mode: PolicyMode::Warn,
            config: json!({"allow_hosts": ["keycloak"], "max_response_bytes": null}),
            updated_at: chrono::Utc::now(),
            updated_by: None,
        }]);
        let updates = resolve_updates(
            request(json!({"policies": {"egress": {"mode": "enforce"}, "graphql": {"mode": "enforce"}}})),
            &current,
        )
        .expect("valid");
        assert_eq!(
            updates,
            vec![
                (
                    PolicyKey::Egress,
                    PolicyMode::Enforce,
                    json!({"allow_hosts": ["keycloak"], "max_response_bytes": null})
                ),
                (PolicyKey::Graphql, PolicyMode::Enforce, json!({})),
            ]
        );
    }

    #[test]
    fn unknown_key_mode_or_invalid_config_is_a_validation_error() {
        let defaults = PolicySet::builtin_defaults();
        let cases = [
            (
                json!({"policies": {"secret_rotation": {"mode": "warn"}}}),
                "secret_rotation",
            ),
            (json!({"policies": {"egress": {"mode": "loud"}}}), "egress"),
            (
                json!({"policies": {"audience": {"mode": "enforce", "config": {"allowed": []}}}}),
                "empty audience allowlist",
            ),
            (json!({"policies": {}}), "at least one"),
        ];
        for (body, needle) in cases {
            let err = resolve_updates(request(body), &defaults).unwrap_err();
            assert!(
                matches!(&err, PlatformError::Validation(m) if m.contains(needle)),
                "{err:?} should mention {needle}"
            );
        }
    }

    #[test]
    fn enforcing_db_roles_names_the_services_without_a_role() {
        assert!(require_role_bindings(&[]).is_ok());
        let refused = require_role_bindings(&["books".into(), "notes".into()]).unwrap_err();
        assert!(
            matches!(&refused, PlatformError::Validation(m)
                if m == "db_roles: enforce needs a verified database role for every service of \
                         the tenant; without one: books, notes"),
            "{refused}"
        );
    }

    #[test]
    fn the_r3_policies_validate_their_modes_and_configs() {
        let current = PolicySet::builtin_defaults();
        let set = |body: Value| resolve_updates(request(json!({ "policies": body })), &current);
        for accepted in [
            json!({"outbox": {"mode": "enforce"}}),
            json!({"outbox": {"mode": "off"}}),
            json!({"db_roles": {"mode": "warn", "config": {"readers": ["reporting_owner", "dashboard_ro"], "login_pool_max": 2}}}),
            json!({"schema_guard": {"mode": "enforce", "config": {"accept_extra_columns": false}}}),
            json!({"schema_guard": {"mode": "warn"}}),
        ] {
            assert!(set(accepted.clone()).is_ok(), "{accepted}");
        }
        for (refused, needle) in [
            (
                json!({"outbox": {"mode": "warn"}}),
                "outbox: outbox has no warn mode; a shadow write would publish twice",
            ),
            (
                json!({"outbox": {"mode": "enforce", "config": {"batch": 1}}}),
                "outbox: invalid config",
            ),
            (
                json!({"db_roles": {"mode": "warn", "config": {"readers": ["Reporting-Owner"]}}}),
                "db_roles: readers entry 'Reporting-Owner' must be a role name",
            ),
            (
                json!({"db_roles": {"mode": "warn", "config": {"login_pool_max": 0}}}),
                "db_roles: login_pool_max must be greater than 0",
            ),
            (
                json!({"schema_guard": {"mode": "warn", "config": {"accept_extra": true}}}),
                "schema_guard: invalid config",
            ),
        ] {
            let err = set(refused).unwrap_err();
            assert!(
                matches!(&err, PlatformError::Validation(m) if m.contains(needle)),
                "{err:?} should mention {needle}"
            );
        }
    }

    #[test]
    fn body_lists_every_key_with_mode_config_and_source() {
        let body = policies_body("default", &PolicySet::builtin_defaults());
        assert_eq!(body["tenant"], "default");
        assert_eq!(body["policies"].as_object().map(|p| p.len()), Some(12));
        assert_eq!(
            body["policies"]["outbox"],
            json!({"mode": "off", "config": {}, "source": "platform_default"})
        );
        assert_eq!(
            body["policies"]["db_roles"],
            json!({
                "mode": "off",
                "config": {"readers": [], "login_pool_max": null},
                "source": "platform_default",
            })
        );
        assert_eq!(
            body["policies"]["schema_guard"],
            json!({
                "mode": "off",
                "config": {"accept_extra_columns": true},
                "source": "platform_default",
            })
        );
        assert_eq!(
            body["policies"]["strict_filters"],
            json!({"mode": "off", "config": {}, "source": "platform_default"})
        );
        assert_eq!(
            body["policies"]["storage_scope"],
            json!({
                "mode": "enforce",
                "config": {"scope": "legacy", "allowed_buckets": []},
                "source": "platform_default",
            })
        );
    }
}
