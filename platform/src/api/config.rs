use crate::auth::DeployAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::service_configs::{self, ConfigEntry};
use crate::secrets::{SecretBox, SecretError};
use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use deadpool_postgres::Transaction;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

const MASKED: &str = "***";

pub(crate) const FEATURES: &[&str] = &[cufflink_types::features::CONFIG_APPLY];

#[derive(Deserialize)]
pub struct SetConfigRequest {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub is_secret: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesiredEntry {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub is_secret: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyConfigRequest {
    pub entries: Vec<DesiredEntry>,
    #[serde(default)]
    pub prune: bool,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigDiff {
    pub added: Vec<String>,
    pub changed: Vec<String>,
    pub removed: Vec<String>,
    pub unchanged: Vec<String>,
}

impl ConfigDiff {
    fn writes(&self, key: &str) -> bool {
        [&self.added, &self.changed].iter().any(|keys| {
            keys.binary_search_by(|written| written.as_str().cmp(key))
                .is_ok()
        })
    }
}

#[derive(Serialize)]
pub struct Applied {
    #[serde(flatten)]
    diff: ConfigDiff,
    applied: bool,
}

type Desired<'a> = BTreeMap<&'a str, &'a DesiredEntry>;

fn desired(entries: &[DesiredEntry]) -> PlatformResult<Desired<'_>> {
    entries
        .iter()
        .try_fold(Desired::new(), |mut by_key, entry| {
            match by_key.insert(&entry.key, entry) {
                None => Ok(by_key),
                Some(_) => Err(PlatformError::BadRequest(format!(
                    "Config key '{}' is given more than once",
                    entry.key
                ))),
            }
        })
}

fn holds(current: &ConfigEntry, entry: &DesiredEntry) -> bool {
    current.is_secret == entry.is_secret
        && current
            .value
            .as_ref()
            .is_some_and(|value| value.expose() == entry.value)
}

pub fn plan_apply(current: &[ConfigEntry], desired: &Desired<'_>, prune: bool) -> ConfigDiff {
    let stored: BTreeMap<&str, &ConfigEntry> = current
        .iter()
        .map(|entry| (entry.key.as_str(), entry))
        .collect();
    let removed = stored
        .keys()
        .filter(|key| prune && !desired.contains_key(*key))
        .map(|key| key.to_string())
        .collect();
    desired.values().fold(
        ConfigDiff {
            removed,
            ..ConfigDiff::default()
        },
        |mut diff, entry| {
            let keys = match stored.get(entry.key.as_str()) {
                None => &mut diff.added,
                Some(current) if holds(current, entry) => &mut diff.unchanged,
                Some(_) => &mut diff.changed,
            };
            keys.push(entry.key.clone());
            diff
        },
    )
}

fn waited(error: tokio_postgres::Error) -> PlatformError {
    match error.code() == Some(&SqlState::QUERY_CANCELED) {
        true => PlatformError::Unavailable(
            "Another apply of this service's config is still running; try again".into(),
        ),
        false => error.into(),
    }
}

async fn planned(
    tx: &Transaction<'_>,
    secrets: &SecretBox,
    tenant_id: Uuid,
    service_id: Uuid,
    desired: &Desired<'_>,
    prune: bool,
) -> PlatformResult<ConfigDiff> {
    if !crate::repo::services::hold(tx, service_id, tenant_id).await? {
        return Err(PlatformError::NotFound("Service not found".into()));
    }
    service_configs::lock_apply(tx, service_id)
        .await
        .map_err(waited)?;
    let current = service_configs::opened(secrets, tx, tenant_id, service_id).await?;
    Ok(plan_apply(&current, desired, prune))
}

async fn written<'a>(
    tx: &Transaction<'_>,
    service_id: Uuid,
    desired: &Desired<'a>,
    diff: &ConfigDiff,
    set: impl Fn(&'a DesiredEntry) -> BoxFuture<'a, Result<(), SecretError>>,
) -> PlatformResult<()> {
    for entry in desired.values().filter(|entry| diff.writes(&entry.key)) {
        set(entry).await?;
    }
    for key in &diff.removed {
        service_configs::delete(tx, service_id, key).await?;
    }
    Ok(())
}

pub async fn list_config(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let configs: Vec<Value> =
        service_configs::list(&state.secrets, &client, user.tenant_id, service_id)
            .await?
            .iter()
            .map(|entry| {
                json!({
                    "key": entry.key,
                    "value": entry.value.as_ref().map_or(MASKED, |value| value.expose()),
                    "is_secret": entry.is_secret,
                    "updated_at": entry.updated_at.to_rfc3339(),
                })
            })
            .collect();

    Ok(Json(json!({ "configs": configs })))
}

pub async fn set_config(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
    Json(req): Json<SetConfigRequest>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    service_configs::set(
        &state.secrets,
        &client,
        user.tenant_id,
        service_id,
        &req.key,
        &req.value,
        req.is_secret,
    )
    .await?;
    drop(client);
    audited(
        &state,
        &user,
        "config.set",
        service_id,
        key_details(&req.key, req.is_secret),
    )
    .await;

    Ok(Json(json!({
        "key": req.key,
        "is_secret": req.is_secret,
        "status": "saved",
    })))
}

pub async fn delete_config(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((service_id, key)): Path<(Uuid, String)>,
) -> PlatformResult<Json<Value>> {
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    if !service_configs::delete(&client, service_id, &key).await? {
        return Err(PlatformError::NotFound(format!(
            "Config key '{}' not found",
            key
        )));
    }
    drop(client);
    audited(
        &state,
        &user,
        "config.delete",
        service_id,
        key_details(&key, false),
    )
    .await;

    Ok(Json(json!({ "deleted": true, "key": key })))
}

pub async fn delete_config_named_apply(
    state: State<Arc<AppState>>,
    auth: DeployAuth,
    Path(service_id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    delete_config(state, auth, Path((service_id, "apply".to_string()))).await
}

pub async fn apply_config(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
    Json(req): Json<ApplyConfigRequest>,
) -> PlatformResult<Json<Applied>> {
    let desired = desired(&req.entries)?;

    let mut client = state.platform_db.get().await?;
    let tx = client.transaction().await?;
    let diff = planned(
        &tx,
        &state.secrets,
        user.tenant_id,
        service_id,
        &desired,
        req.prune,
    )
    .await?;
    if !req.dry_run {
        written(&tx, service_id, &desired, &diff, |entry| {
            Box::pin(service_configs::set_within(
                &state.secrets,
                &tx,
                user.tenant_id,
                service_id,
                &entry.key,
                &entry.value,
                entry.is_secret,
            ))
        })
        .await?;
    }
    tx.commit().await?;
    drop(client);
    let action = match req.dry_run {
        true => "config.check",
        false => "config.apply",
    };
    audited(&state, &user, action, service_id, json!(diff)).await;

    Ok(Json(Applied {
        diff,
        applied: !req.dry_run,
    }))
}

fn key_details(key: &str, is_secret: bool) -> Value {
    json!({"key": key, "is_secret": is_secret})
}

async fn audited(
    state: &AppState,
    user: &crate::auth::AuthUser,
    action: &str,
    service_id: Uuid,
    details: Value,
) {
    crate::repo::audit::record(
        &state.platform_db,
        crate::repo::audit::Change {
            tenant_id: Some(user.tenant_id),
            actor: crate::audit::subject(user),
            credential: user.credential(),
            action,
            resource_type: "service_config",
            resource_id: Some(&service_id.to_string()),
            details: Some(details),
        },
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::{KeyRing, SecretString, StorageMode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn stored(key: &str, value: &str, is_secret: bool) -> ConfigEntry {
        ConfigEntry {
            key: key.to_string(),
            value: Some(SecretString::new(value)),
            is_secret,
            updated_at: chrono::Utc::now(),
        }
    }

    fn wanted(key: &str, value: &str, is_secret: bool) -> DesiredEntry {
        DesiredEntry {
            key: key.to_string(),
            value: value.to_string(),
            is_secret,
        }
    }

    fn diff(added: &[&str], changed: &[&str], removed: &[&str], unchanged: &[&str]) -> ConfigDiff {
        let keys = |keys: &[&str]| keys.iter().map(|key| key.to_string()).collect();
        ConfigDiff {
            added: keys(added),
            changed: keys(changed),
            removed: keys(removed),
            unchanged: keys(unchanged),
        }
    }

    #[test]
    fn an_apply_is_planned_key_by_key() {
        let current = [
            stored("FLAG", "on", false),
            stored("GONE", "x", false),
            stored("SAME", "1", false),
            stored("TOKEN", "t", true),
            stored("VALUE", "old", false),
        ];
        let entries = [
            wanted("VALUE", "new", false),
            wanted("TOKEN", "t", true),
            wanted("SAME", "1", false),
            wanted("NEW", "n", true),
            wanted("FLAG", "on", true),
        ];
        let desired = desired(&entries).expect("distinct keys");
        for (prune, removed) in [(false, &[][..]), (true, &["GONE"][..])] {
            assert_eq!(
                plan_apply(&current, &desired, prune),
                diff(&["NEW"], &["FLAG", "VALUE"], removed, &["SAME", "TOKEN"]),
                "prune: {prune}"
            );
        }
    }

    #[test]
    fn an_empty_set_changes_nothing_unless_it_prunes() {
        let current = [stored("A", "1", false), stored("B", "2", true)];
        let nothing = Desired::new();
        assert_eq!(plan_apply(&current, &nothing, false), ConfigDiff::default());
        assert_eq!(
            plan_apply(&current, &nothing, true),
            diff(&[], &[], &["A", "B"], &[])
        );
        assert_eq!(plan_apply(&[], &nothing, true), ConfigDiff::default());
    }

    #[test]
    fn a_stored_value_that_is_absent_or_unopened_is_never_what_is_wanted() {
        let unopened = ConfigEntry {
            value: None,
            ..stored("A", "", false)
        };
        let entries = [wanted("A", "", false)];
        let desired = desired(&entries).expect("distinct keys");
        assert_eq!(
            plan_apply(&[unopened], &desired, false),
            diff(&[], &["A"], &[], &[])
        );
    }

    #[test]
    fn a_key_given_twice_is_refused_whatever_it_is_given() {
        for second in [wanted("A", "1", false), wanted("A", "2", true)] {
            let entries = [wanted("A", "1", false), wanted("B", "1", false), second];
            assert!(matches!(
                desired(&entries),
                Err(PlatformError::BadRequest(message))
                    if message == "Config key 'A' is given more than once"
            ));
        }
    }

    #[test]
    fn a_request_names_only_what_an_apply_reads() {
        let read = |body: Value| serde_json::from_value::<ApplyConfigRequest>(body);
        let request = read(json!({"entries": [{"key": "A", "value": "1"}]})).expect("defaults");
        assert!(!request.prune && !request.dry_run && !request.entries[0].is_secret);
        for refused in [
            json!({"entries": [], "purge": true}),
            json!({"entries": [{"key": "A", "value": "1", "secret": true}]}),
            json!({"prune": true}),
            json!({"entries": [{"key": "A"}]}),
        ] {
            assert!(read(refused.clone()).is_err(), "{refused}");
        }
    }

    #[test]
    fn a_diff_is_answered_as_its_keys_and_whether_it_was_applied() {
        assert_eq!(
            json!(Applied {
                diff: diff(&["A"], &["B"], &["C"], &["D"]),
                applied: false,
            }),
            json!({"added": ["A"], "changed": ["B"], "removed": ["C"], "unchanged": ["D"],
                   "applied": false})
        );
    }

    #[tokio::test]
    async fn a_write_that_fails_ends_the_apply_and_its_transaction_undoes_the_rest() {
        let Some(infra) = cufflink_testkit::infra().await else {
            return;
        };
        let mut db = infra.create_migrated_database().await;
        let tenant = db.mint_tenant().await;
        let pool = cufflink_db::build_pool(&db.url, 2).expect("pool");
        let service = crate::repo::services::find_or_create(
            &pool,
            tenant.id,
            "cfg",
            "cfg",
            crate::repo::services::Status::Active,
        )
        .await
        .expect("service");
        let ring = KeyRing::parse(None, None, None).expect("ring");
        let secrets = SecretBox::new(pool.clone(), ring, StorageMode::Plaintext);
        let mut client = pool.get().await.expect("checkout");
        for (key, value) in [("A", "old"), ("GONE", "x")] {
            service_configs::set(&secrets, &client, tenant.id, service.id, key, value, false)
                .await
                .expect("set");
        }
        let held = |entries: Vec<ConfigEntry>| -> Vec<(String, Option<String>)> {
            entries
                .into_iter()
                .map(|entry| (entry.key, entry.value.map(|v| v.expose().to_string())))
                .collect()
        };
        let before = held(
            service_configs::opened(&secrets, &client, tenant.id, service.id)
                .await
                .expect("configs"),
        );

        let entries = [
            wanted("A", "new", false),
            wanted("B", "1", false),
            wanted("C", "2", false),
        ];
        let desired = desired(&entries).expect("distinct keys");
        let tx = client.transaction().await.expect("begin");
        let diff = planned(&tx, &secrets, tenant.id, service.id, &desired, true)
            .await
            .expect("plan");
        let writes = AtomicUsize::new(0);
        let outcome = written(&tx, service.id, &desired, &diff, |entry| {
            let (secrets, tx, writes) = (&secrets, &tx, &writes);
            Box::pin(async move {
                match writes.fetch_add(1, Ordering::SeqCst) {
                    1 => Err(SecretError::NoKey),
                    _ => {
                        service_configs::set_within(
                            secrets,
                            tx,
                            tenant.id,
                            service.id,
                            &entry.key,
                            &entry.value,
                            entry.is_secret,
                        )
                        .await
                    }
                }
            })
        })
        .await;

        assert!(outcome.is_err(), "the second write failed");
        assert_eq!(
            writes.load(Ordering::SeqCst),
            2,
            "nothing is written after a write that failed"
        );
        drop(tx);
        assert_eq!(
            held(
                service_configs::opened(&secrets, &client, tenant.id, service.id)
                    .await
                    .expect("configs")
            ),
            before,
            "the first write went with the transaction, and nothing was removed"
        );
    }
}
