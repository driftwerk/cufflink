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
use zeroize::Zeroize;

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

impl Drop for CredentialRequest {
    fn drop(&mut self) {
        self.secret_access_key.zeroize();
    }
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

/// What a registration holds that only the s3 backend can serve.
fn needs_s3(rules: &Rules, credential: Option<&CredentialRequest>) -> Option<&'static str> {
    match (credential.is_some(), rules.acls.is_empty()) {
        (true, _) => Some("a credential"),
        (false, false) => Some("a canned ACL"),
        (false, true) => None,
    }
}

fn validate(
    state: &AppState,
    bucket: &str,
    rules: &Rules,
    credential: Option<&CredentialRequest>,
) -> PlatformResult<()> {
    let platform = state.storage.as_ref();
    let kind = platform.map(|storage| storage.backend_kind());
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
        .and_then(|()| match needs_s3(rules, credential) {
            Some(what) if kind != Some("s3") => Err(format!(
                "{what} needs the s3 storage backend; this platform's is {}",
                kind.unwrap_or("not configured")
            )),
            _ => Ok(()),
        })
        .map_err(PlatformError::Validation)
}

fn owner_slug(state: &AppState, tenant_id: Uuid) -> String {
    state
        .tenants
        .get(tenant_id)
        .map_or_else(|| tenant_id.to_string(), |entry| entry.tenant.slug.clone())
}

fn held(state: &AppState, bucket: &str, row: &BucketRow) -> PlatformError {
    let owner = row
        .tenant_id
        .map(|id| format!("tenant {}", owner_slug(state, id)));
    PlatformError::Conflict(match (row.retired_at, owner) {
        (None, Some(owner)) => format!("bucket {bucket} is registered to {owner}"),
        (_, owner) => format!(
            "bucket {bucket} was retired from {}; release it before registering it to another tenant",
            owner.as_deref().unwrap_or("a deleted tenant")
        ),
    })
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
    if let Some(other) = existing
        .as_ref()
        .filter(|row| row.tenant_id != Some(tenant.id))
    {
        return Err(held(&state, &bucket, other));
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
    if tenant_buckets::retire(&client, tenant.id, &bucket, &admin.subject)
        .await?
        .is_none()
    {
        return Err(PlatformError::NotFound(format!(
            "Tenant '{slug}' has no bucket '{bucket}'"
        )));
    }
    drop(client);
    info!(tenant = %slug, bucket = %bucket, actor = %admin.subject, "Tenant bucket retired");
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

pub async fn release_bucket(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path(bucket): Path<String>,
) -> PlatformResult<StatusCode> {
    let grace = crate::tenant_catalog::RETIREMENT_GRACE;
    let client = state.platform_db.get().await?;
    let Some(released) = tenant_buckets::release(&client, &bucket, grace).await? else {
        return Err(
            match tenant_buckets::find_for_update(&client, &bucket).await? {
                None => PlatformError::NotFound(format!("No tenant holds bucket '{bucket}'")),
                Some(row) if row.retired_at.is_none() => PlatformError::Conflict(format!(
                    "bucket {bucket} is registered to tenant {}; delete it first",
                    row.tenant_id
                        .map_or_else(String::new, |id| owner_slug(&state, id))
                )),
                Some(_) => PlatformError::Conflict(format!(
                    "bucket {bucket} was retired less than {} s ago, when a replica may still \
                     serve it to its tenant; release it later",
                    grace.as_secs()
                )),
            },
        );
    };
    drop(client);
    info!(bucket = %bucket, actor = %admin.subject, "Tenant bucket released");
    audit::record(
        &state.platform_db,
        audit::Change {
            tenant_id: released.tenant_id,
            actor: &admin.subject,
            credential: admin.credential(),
            action: "tenant.bucket.release",
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
    fn a_credential_or_an_acl_needs_the_s3_backend() {
        let rules = |acls: BTreeSet<CannedAcl>| Rules {
            key_prefixes: Some(vec!["leads/".into()]),
            acls,
        };
        let credential = CredentialRequest {
            access_key_id: "AKEXAMPLE".into(),
            secret_access_key: "s".into(),
        };
        assert_eq!(needs_s3(&rules(BTreeSet::new()), None), None);
        assert_eq!(
            needs_s3(&rules(BTreeSet::new()), Some(&credential)),
            Some("a credential")
        );
        assert_eq!(
            needs_s3(&rules(BTreeSet::from([CannedAcl::PublicRead])), None),
            Some("a canned ACL")
        );
        assert_eq!(
            needs_s3(&rules(BTreeSet::from([CannedAcl::Private])), None),
            Some("a canned ACL")
        );
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
