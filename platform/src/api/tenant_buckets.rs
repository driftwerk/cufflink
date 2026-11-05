use crate::auth::PlatformAdmin;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::tenant_buckets::{self, BucketRow, NewBucket};
use crate::repo::{audit, tenants};
use crate::tenant_buckets::{validate_bucket_name, Rules};
use crate::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use cufflink_storage::CannedAcl;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

const MAX_ACCESS_KEY_ID_BYTES: usize = 128;
const MAX_SECRET_BYTES: usize = 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutBucketRequest {
    #[serde(default)]
    pub key_prefixes: Option<Vec<String>>,
    #[serde(default)]
    pub acls: BTreeSet<CannedAcl>,
    #[serde(default)]
    pub credential: Option<CredentialRequest>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRequest {
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl fmt::Debug for CredentialRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialRequest")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

fn validate_credential(credential: &CredentialRequest) -> Result<(), String> {
    let printable = |value: &str| !value.is_empty() && !value.chars().any(char::is_control);
    if !printable(&credential.access_key_id)
        || credential.access_key_id.len() > MAX_ACCESS_KEY_ID_BYTES
    {
        return Err(format!(
            "credential.access_key_id must be 1 to {MAX_ACCESS_KEY_ID_BYTES} bytes without control characters"
        ));
    }
    if !printable(&credential.secret_access_key)
        || credential.secret_access_key.len() > MAX_SECRET_BYTES
    {
        return Err(format!(
            "credential.secret_access_key must be 1 to {MAX_SECRET_BYTES} bytes without control characters"
        ));
    }
    Ok(())
}

fn validate(
    state: &AppState,
    bucket: &str,
    rules: &Rules,
    credential: Option<&CredentialRequest>,
) -> PlatformResult<()> {
    let platform = state.storage.as_ref();
    validate_bucket_name(bucket)
        .and_then(|()| rules.validate())
        .and_then(|()| credential.map_or(Ok(()), validate_credential))
        .and_then(
            |()| match platform.map(|storage| storage.primary_bucket()) {
                Some(primary) if primary == bucket => Err(format!(
                    "bucket {bucket} is the platform's own and cannot be registered"
                )),
                _ => Ok(()),
            },
        )
        .and_then(
            |()| match (credential, platform.map(|s| s.backend_kind())) {
                (Some(_), kind) if kind != Some("s3") => Err(format!(
                    "a credential needs the s3 storage backend; this platform's is {}",
                    kind.unwrap_or("not configured")
                )),
                _ => Ok(()),
            },
        )
        .map_err(PlatformError::Validation)
}

fn owner_slug(state: &AppState, tenant_id: Uuid) -> String {
    state
        .tenants
        .get(tenant_id)
        .map_or_else(|| tenant_id.to_string(), |entry| entry.tenant.slug.clone())
}

fn buckets_body(slug: &str, rows: &[BucketRow]) -> Value {
    json!({ "tenant": slug, "buckets": rows })
}

fn audited(rules: &Rules, access_key_id: Option<&str>) -> Value {
    json!({
        "key_prefixes": rules.key_prefixes,
        "acls": rules.acls,
        "access_key_id": access_key_id,
    })
}

pub async fn list_buckets(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(slug): Path<String>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let client = state.platform_db.get().await?;
    let rows = tenant_buckets::list_for_tenant(&client, tenant.id).await?;
    Ok(Json(buckets_body(&slug, &rows)))
}

pub async fn put_bucket(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, bucket)): Path<(String, String)>,
    Json(request): Json<PutBucketRequest>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let rules = Rules {
        key_prefixes: request.key_prefixes,
        acls: request.acls,
    };
    let credential = request.credential;
    validate(&state, &bucket, &rules, credential.as_ref())?;
    if state.tenants.bucket_claims(tenant.id, &bucket).by_other {
        return Err(PlatformError::Conflict(format!(
            "bucket {bucket} is listed in another tenant group's storage_scope allowed_buckets"
        )));
    }
    let mut client = state.platform_db.get().await?;
    let txn = client.transaction().await?;
    let existing = tenant_buckets::find_for_update(&txn, &bucket).await?;
    if let Some(other) = existing.as_ref().filter(|row| row.tenant_id != tenant.id) {
        return Err(PlatformError::Conflict(format!(
            "bucket {bucket} is registered to tenant {}",
            owner_slug(&state, other.tenant_id)
        )));
    }
    let id = existing.map_or_else(Uuid::new_v4, |row| row.id);
    let secret = match &credential {
        Some(credential) => Some(
            state
                .secrets
                .store(
                    &txn,
                    tenant.id,
                    &tenant_buckets::aad(id),
                    &credential.secret_access_key,
                )
                .await?,
        ),
        None => None,
    };
    let access_key_id = credential.as_ref().map(|c| c.access_key_id.as_str());
    let row = tenant_buckets::upsert(
        &txn,
        &NewBucket {
            id,
            tenant_id: tenant.id,
            bucket: &bucket,
            rules: &rules,
            access_key_id,
            secret: secret.as_ref(),
            updated_by: &admin.subject,
        },
    )
    .await?;
    txn.commit().await?;
    drop(client);
    info!(tenant = %slug, bucket = %bucket, actor = %admin.subject, "Tenant bucket set");
    audit::record(
        &state.platform_db,
        audit::Change {
            tenant_id: Some(tenant.id),
            actor: &admin.subject,
            credential: admin.credential(),
            action: "tenant.bucket.set",
            resource_type: "tenant_bucket",
            resource_id: Some(&bucket),
            details: Some(audited(&rules, access_key_id)),
        },
    )
    .await;
    state.tenants.refresh().await;
    Ok(Json(json!(row)))
}

pub async fn delete_bucket(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, bucket)): Path<(String, String)>,
) -> PlatformResult<StatusCode> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let client = state.platform_db.get().await?;
    if tenant_buckets::delete(&client, tenant.id, &bucket)
        .await?
        .is_none()
    {
        return Err(PlatformError::NotFound(format!(
            "Tenant '{slug}' has no bucket '{bucket}'"
        )));
    }
    drop(client);
    info!(tenant = %slug, bucket = %bucket, actor = %admin.subject, "Tenant bucket deleted");
    audit::record(
        &state.platform_db,
        audit::Change {
            tenant_id: Some(tenant.id),
            actor: &admin.subject,
            credential: admin.credential(),
            action: "tenant.bucket.delete",
            resource_type: "tenant_bucket",
            resource_id: Some(&bucket),
            details: None,
        },
    )
    .await;
    state.tenants.refresh().await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(body: Value) -> Result<PutBucketRequest, serde_json::Error> {
        serde_json::from_value(body)
    }

    #[test]
    fn a_request_names_only_what_a_bucket_holds() {
        let full = request(json!({
            "key_prefixes": ["leads/"],
            "acls": ["public-read"],
            "credential": {"access_key_id": "AKEXAMPLE", "secret_access_key": "s3cr3t"}
        }))
        .expect("request");
        assert_eq!(full.key_prefixes, Some(vec!["leads/".to_string()]));
        assert_eq!(full.acls, BTreeSet::from([CannedAcl::PublicRead]));
        let shown = format!("{:?}", full.credential);
        assert!(
            shown.contains("AKEXAMPLE") && !shown.contains("s3cr3t"),
            "{shown}"
        );
        let empty = request(json!({})).expect("defaults");
        assert!(
            empty.key_prefixes.is_none() && empty.acls.is_empty() && empty.credential.is_none()
        );
        for refused in [
            json!({"prefixes": ["leads/"]}),
            json!({"acls": ["public-read-write"]}),
            json!({"credential": {"access_key_id": "a"}}),
            json!({"credential": {"access_key_id": "a", "secret_access_key": "s", "region": "x"}}),
        ] {
            assert!(request(refused.clone()).is_err(), "{refused}");
        }
    }

    #[test]
    fn a_credential_is_printable_and_bounded() {
        let credential = |id: &str, secret: &str| CredentialRequest {
            access_key_id: id.into(),
            secret_access_key: secret.into(),
        };
        assert!(validate_credential(&credential("AKEXAMPLE", "s")).is_ok());
        for bad in [
            credential("", "s"),
            credential("AK", ""),
            credential("AK\n", "s"),
            credential("AK", "s\r\n"),
            credential(&"a".repeat(MAX_ACCESS_KEY_ID_BYTES + 1), "s"),
            credential("AK", &"s".repeat(MAX_SECRET_BYTES + 1)),
        ] {
            assert!(validate_credential(&bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_audit_record_never_holds_the_secret() {
        let details = audited(
            &Rules {
                key_prefixes: None,
                acls: BTreeSet::from([CannedAcl::PublicRead]),
            },
            Some("AKEXAMPLE"),
        );
        assert_eq!(
            details,
            json!({"key_prefixes": null, "acls": ["public-read"], "access_key_id": "AKEXAMPLE"})
        );
    }
}
