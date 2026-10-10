use crate::auth::extractor::require_permission_or_deploy;
use crate::auth::partner_keys::{self, CHANGED_SUBJECT};
use crate::auth::secret_keys;
use crate::auth::AuthUser;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo;
use crate::shutdown::Shutdown;
use crate::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

const DEFAULT_EXPIRES_IN_DAYS: i64 = 90;
const MAX_EXPIRES_IN_DAYS: i64 = 365;
const ACTIVE_RECOUNT: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug, Deserialize)]
pub struct CreatePartnerKeyRequest {
    pub name: String,
    pub scopes: Vec<String>,
    pub expires_in_days: Option<i64>,
}

impl CreatePartnerKeyRequest {
    fn validated(&self) -> PlatformResult<chrono::Duration> {
        secret_keys::name(&self.name, "Partner key")?;
        if self.scopes.is_empty() {
            return Err(PlatformError::Validation(
                "A partner key needs at least one scope".into(),
            ));
        }
        if self.scopes.len() > partner_keys::MAX_SCOPES {
            return Err(PlatformError::Validation(format!(
                "A partner key has at most {} scopes",
                partner_keys::MAX_SCOPES
            )));
        }
        if let Some(scope) = self
            .scopes
            .iter()
            .find(|scope| !partner_keys::valid_scope(scope))
        {
            return Err(PlatformError::Validation(format!(
                "Invalid scope '{scope}' (expected resource or resource:action, lowercase)"
            )));
        }
        match self.expires_in_days.unwrap_or(DEFAULT_EXPIRES_IN_DAYS) {
            days @ 1..=MAX_EXPIRES_IN_DAYS => Ok(chrono::Duration::days(days)),
            days => Err(PlatformError::Validation(format!(
                "expires_in_days must be between 1 and {MAX_EXPIRES_IN_DAYS}, not {days}"
            ))),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CreatedPartnerKey {
    pub id: Uuid,
    pub key: String,
    pub prefix: String,
    pub scopes: Vec<String>,
    pub expires_at: DateTime<Utc>,
}

async fn manager(state: &Arc<AppState>, user: AuthUser, slug: &str) -> PlatformResult<AuthUser> {
    if user.tenant_slug != slug {
        return Err(PlatformError::Forbidden("Access denied".into()));
    }
    require_permission_or_deploy(state, &user, "partner_keys", "manage").await?;
    Ok(user)
}

async fn changed(state: &Arc<AppState>) {
    state.partner_keys.clear();
    crate::nats_conn::publish_change(&state.nats, CHANGED_SUBJECT).await;
    record_active(&state.platform_db).await;
}

async fn record_active(pool: &deadpool_postgres::Pool) {
    match repo::partner_keys::count_active(pool).await {
        Ok(active) => metrics::gauge!("cufflink_partner_keys_active").set(active as f64),
        Err(e) => tracing::warn!(error = %e, "Could not count active partner keys"),
    }
}

pub fn spawn_active_gauge(pool: deadpool_postgres::Pool, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn("partner_keys_active", async move {
        loop {
            record_active(&pool).await;
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(ACTIVE_RECOUNT) => {}
            }
        }
    });
}

async fn audit(state: &AppState, user: &AuthUser, action: &str, id: Uuid, details: Value) {
    let recorded = async {
        let client = state.platform_db.get().await?;
        crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
            client: &client,
            tenant_id: Some(user.tenant_id),
            actor: &user.subject,
            action,
            resource_type: "partner_key",
            resource_id: Some(&id.to_string()),
            details: Some(details),
            ip_address: None,
        })
        .await
    }
    .await;
    if let Err(e) = recorded {
        tracing::warn!(key_id = %id, action, error = %e, "Could not audit a partner key change");
    }
}

async fn mint(
    state: &AppState,
    user: &AuthUser,
    req: &CreatePartnerKeyRequest,
    ttl: chrono::Duration,
) -> PlatformResult<(repo::partner_keys::PartnerKey, String)> {
    secret_keys::mint(partner_keys::KIND, |prefix, secret| async move {
        repo::partner_keys::create(
            &state.platform_db,
            &repo::partner_keys::NewPartnerKey {
                tenant_id: user.tenant_id,
                name: req.name.trim(),
                prefix: &prefix,
                secret: &secret,
                scopes: &req.scopes,
                ttl,
                created_by: &user.subject,
            },
        )
        .await
    })
    .await
}

pub async fn create_partner_key(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path(slug): Path<String>,
    Json(req): Json<CreatePartnerKeyRequest>,
) -> PlatformResult<(StatusCode, Json<CreatedPartnerKey>)> {
    let user = manager(&state, user, &slug).await?;
    let ttl = req.validated()?;
    let (key, raw) = mint(&state, &user, &req, ttl).await?;
    changed(&state).await;
    info!(tenant = %slug, key_id = %key.id, created_by = %user.subject, scopes = ?key.scopes, "Created partner key '{}'", key.name);
    audit(
        &state,
        &user,
        "partner_key.create",
        key.id,
        json!({"name": key.name, "scopes": key.scopes}),
    )
    .await;
    Ok((
        StatusCode::CREATED,
        Json(CreatedPartnerKey {
            id: key.id,
            key: raw,
            prefix: key.prefix,
            scopes: key.scopes,
            expires_at: key.expires_at,
        }),
    ))
}

pub async fn list_partner_keys(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path(slug): Path<String>,
) -> PlatformResult<Json<Value>> {
    let user = manager(&state, user, &slug).await?;
    let keys = repo::partner_keys::list(&state.platform_db, user.tenant_id).await?;
    Ok(Json(json!({ "keys": keys })))
}

pub async fn revoke_partner_key(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((slug, id)): Path<(String, Uuid)>,
) -> PlatformResult<Json<Value>> {
    let user = manager(&state, user, &slug).await?;
    if !repo::partner_keys::revoke(&state.platform_db, user.tenant_id, id).await? {
        return Err(PlatformError::NotFound("Partner key not found".into()));
    }
    changed(&state).await;
    info!(tenant = %slug, key_id = %id, revoked_by = %user.subject, "Revoked partner key");
    audit(&state, &user, "partner_key.revoke", id, json!({})).await;
    Ok(Json(json!({ "revoked": true })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(scopes: &[&str], days: Option<i64>) -> CreatePartnerKeyRequest {
        CreatePartnerKeyRequest {
            name: "acme".into(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            expires_in_days: days,
        }
    }

    #[test]
    fn requests_are_validated() {
        assert_eq!(
            request(&["books:ingest"], None).validated().expect("valid"),
            chrono::Duration::days(90)
        );
        assert_eq!(
            request(&["books:*"], Some(365)).validated().expect("valid"),
            chrono::Duration::days(365)
        );
        for (req, reason) in [
            (request(&[], None), "at least one scope"),
            (request(&["Books"], None), "Invalid scope 'Books'"),
            (request(&["books"], Some(0)), "between 1 and 365"),
            (request(&["books"], Some(366)), "between 1 and 365"),
            (request(&["books:in*"], None), "Invalid scope 'books:in*'"),
            (
                CreatePartnerKeyRequest {
                    name: "n".repeat(secret_keys::MAX_NAME_LEN + 1),
                    ..request(&["books"], None)
                },
                "longer than 200 characters",
            ),
            (
                request(&vec!["books"; partner_keys::MAX_SCOPES + 1], None),
                "at most 100 scopes",
            ),
            (
                CreatePartnerKeyRequest {
                    name: " ".into(),
                    ..request(&["books"], None)
                },
                "name cannot be empty",
            ),
        ] {
            let err = req.validated().expect_err(reason);
            assert!(
                matches!(&err, PlatformError::Validation(m) if m.contains(reason)),
                "{err:?}"
            );
        }
    }
}
