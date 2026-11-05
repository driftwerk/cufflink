use crate::errors::{PlatformError, PlatformResult};
use crate::secrets::{SecretBox, Stored, TENANT_BUCKET_SECRET};
use crate::tenant_buckets::{stored_rules, Access, Rules, State, TenantBucket};
use chrono::{DateTime, Utc};
use cufflink_storage::Credential;
use deadpool_postgres::GenericClient;
use serde::Serialize;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct BucketRow {
    #[serde(skip)]
    pub id: Uuid,
    #[serde(skip)]
    pub tenant_id: Option<Uuid>,
    pub bucket: String,
    #[serde(flatten)]
    pub rules: Option<Rules>,
    pub credential: Option<AccessKeyId>,
    pub retired_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
    #[serde(skip)]
    secret: Stored,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccessKeyId {
    pub access_key_id: String,
}

const COLUMNS: &str = "id, tenant_id, bucket, key_prefixes, acls, access_key_id, \
                       secret_access_key, secret_access_key_enc, retired_at, updated_at, updated_by";

const RETIRED: &str = "retired_at = coalesce(retired_at, now()), access_key_id = NULL, \
                       secret_access_key = NULL, secret_access_key_enc = NULL, updated_at = now()";

fn row(row: &tokio_postgres::Row) -> BucketRow {
    BucketRow {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        bucket: row.get("bucket"),
        rules: stored_rules(row.get("key_prefixes"), &row.get::<_, Vec<String>>("acls")),
        credential: row
            .get::<_, Option<String>>("access_key_id")
            .map(|access_key_id| AccessKeyId { access_key_id }),
        retired_at: row.get("retired_at"),
        updated_at: row.get("updated_at"),
        updated_by: row.get("updated_by"),
        secret: Stored::new(
            row.get("secret_access_key"),
            row.get("secret_access_key_enc"),
        ),
    }
}

pub async fn list_all(client: &impl GenericClient) -> PlatformResult<Vec<BucketRow>> {
    let found = client
        .query(
            &format!("SELECT {COLUMNS} FROM tenant_buckets ORDER BY bucket"),
            &[],
        )
        .await?;
    Ok(found.iter().map(row).collect())
}

pub async fn list_for_tenant(
    client: &impl GenericClient,
    tenant_id: Uuid,
) -> PlatformResult<Vec<BucketRow>> {
    let found = client
        .query(
            &format!("SELECT {COLUMNS} FROM tenant_buckets WHERE tenant_id = $1 ORDER BY bucket"),
            &[&tenant_id],
        )
        .await?;
    Ok(found.iter().map(row).collect())
}

pub async fn find_for_update(
    client: &impl GenericClient,
    bucket: &str,
) -> PlatformResult<Option<BucketRow>> {
    Ok(client
        .query_opt(
            &format!("SELECT {COLUMNS} FROM tenant_buckets WHERE bucket = $1 FOR UPDATE"),
            &[&bucket],
        )
        .await?
        .as_ref()
        .map(row))
}

/// The buckets among `buckets` registered to anyone but `tenant_id`,
/// retired claims included.
pub async fn held_by_others(
    client: &impl GenericClient,
    tenant_id: Uuid,
    buckets: &[String],
) -> PlatformResult<Vec<String>> {
    Ok(client
        .query(
            "SELECT bucket FROM tenant_buckets
             WHERE bucket = ANY($1) AND tenant_id IS DISTINCT FROM $2
             ORDER BY bucket",
            &[&buckets, &tenant_id],
        )
        .await?
        .iter()
        .map(|found| found.get("bucket"))
        .collect())
}

pub struct NewBucket<'a> {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub bucket: &'a str,
    pub rules: &'a Rules,
    pub access_key_id: Option<&'a str>,
    pub secret: Option<&'a Stored>,
    pub updated_by: &'a str,
}

/// Registers the bucket, or sets its tenant's registration again, reviving
/// it when retired. A bucket another tenant holds is a conflict.
pub async fn upsert(client: &impl GenericClient, new: &NewBucket<'_>) -> PlatformResult<BucketRow> {
    let acls: Vec<&str> = new.rules.acls.iter().map(|acl| acl.as_str()).collect();
    client
        .query_opt(
            &format!(
                "INSERT INTO tenant_buckets (id, tenant_id, bucket, key_prefixes, acls, access_key_id,
                                             secret_access_key, secret_access_key_enc, updated_by)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (bucket) DO UPDATE
                 SET key_prefixes = EXCLUDED.key_prefixes, acls = EXCLUDED.acls,
                     access_key_id = EXCLUDED.access_key_id,
                     secret_access_key = EXCLUDED.secret_access_key,
                     secret_access_key_enc = EXCLUDED.secret_access_key_enc,
                     retired_at = NULL,
                     updated_by = EXCLUDED.updated_by, updated_at = now()
                 WHERE tenant_buckets.tenant_id = EXCLUDED.tenant_id
                   AND tenant_buckets.id = EXCLUDED.id
                 RETURNING {COLUMNS}"
            ),
            &[
                &new.id,
                &new.tenant_id,
                &new.bucket,
                &new.rules.key_prefixes,
                &acls,
                &new.access_key_id,
                &new.secret.and_then(Stored::plain),
                &new.secret.and_then(Stored::sealed),
                &new.updated_by,
            ],
        )
        .await?
        .as_ref()
        .map(row)
        .ok_or_else(|| {
            PlatformError::Conflict(format!(
                "bucket {} is registered to another tenant",
                new.bucket
            ))
        })
}

/// Retires the tenant's registration: its credential is dropped and the
/// bucket stays claimed, refused to every guest, until it is released.
pub async fn retire(
    client: &impl GenericClient,
    tenant_id: Uuid,
    bucket: &str,
    actor: &str,
) -> PlatformResult<Option<BucketRow>> {
    Ok(client
        .query_opt(
            &format!(
                "UPDATE tenant_buckets SET {RETIRED}, updated_by = $3
                 WHERE tenant_id = $1 AND bucket = $2
                 RETURNING {COLUMNS}"
            ),
            &[&tenant_id, &bucket, &actor],
        )
        .await?
        .as_ref()
        .map(row))
}

/// Retires every registration of the tenant, as its deletion does.
pub async fn retire_all(
    client: &impl GenericClient,
    tenant_id: Uuid,
    actor: &str,
) -> PlatformResult<Vec<String>> {
    Ok(client
        .query(
            &format!(
                "UPDATE tenant_buckets SET {RETIRED}, updated_by = $2
                 WHERE tenant_id = $1
                 RETURNING bucket"
            ),
            &[&tenant_id, &actor],
        )
        .await?
        .iter()
        .map(|found| found.get("bucket"))
        .collect())
}

/// Deletes a claim retired for longer than `grace`, which frees the bucket.
pub async fn release(
    client: &impl GenericClient,
    bucket: &str,
    grace: Duration,
) -> PlatformResult<Option<BucketRow>> {
    Ok(client
        .query_opt(
            &format!(
                "DELETE FROM tenant_buckets
                 WHERE bucket = $1 AND retired_at < now() - make_interval(secs => $2)
                 RETURNING {COLUMNS}"
            ),
            &[&bucket, &grace.as_secs_f64()],
        )
        .await?
        .as_ref()
        .map(row))
}

pub fn aad(id: Uuid) -> crate::secrets::SecretAad {
    TENANT_BUCKET_SECRET.aad([id.to_string()])
}

impl BucketRow {
    fn state(&self, access: impl FnOnce() -> Access) -> State {
        match (&self.rules, self.retired_at, self.tenant_id) {
            (_, Some(_), _) | (_, _, None) => State::Retired,
            (None, ..) => {
                tracing::error!(bucket = %self.bucket, "A tenant bucket's row cannot be read as rules; its calls are refused");
                State::Unreadable
            }
            (Some(rules), ..) => State::Active {
                rules: rules.clone(),
                access: access(),
            },
        }
    }

    fn loaded(&self, state: State) -> TenantBucket {
        TenantBucket {
            owner: self.tenant_id,
            bucket: self.bucket.clone(),
            state,
        }
    }

    pub fn unopened(self) -> TenantBucket {
        let state = self.state(|| match self.credential {
            None => Access::Platform,
            Some(_) => Access::Unopenable,
        });
        self.loaded(state)
    }

    pub async fn open(self, secrets: &SecretBox, client: &impl GenericClient) -> TenantBucket {
        let access = match (&self.credential, self.tenant_id) {
            (None, _) => Access::Platform,
            (Some(_), None) => Access::Unopenable,
            (Some(AccessKeyId { access_key_id }), Some(tenant_id)) => match secrets
                .read(client, tenant_id, &aad(self.id), &self.secret)
                .await
            {
                Ok(Some(secret)) => {
                    Access::Own(Credential::new(access_key_id.clone(), secret.expose()))
                }
                Ok(None) => {
                    tracing::error!(bucket = %self.bucket, "A tenant bucket's credential has no secret; its calls are refused");
                    Access::Unopenable
                }
                Err(error) => {
                    tracing::error!(bucket = %self.bucket, %error, "A tenant bucket's secret cannot be opened; its calls are refused");
                    Access::Unopenable
                }
            },
        };
        let state = self.state(|| access);
        self.loaded(state)
    }
}
