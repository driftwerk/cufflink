use crate::errors::{PlatformError, PlatformResult};
use crate::secrets::{SecretBox, Stored, TENANT_BUCKET_SECRET};
use crate::tenant_buckets::{Access, Rules, TenantBucket};
use chrono::{DateTime, Utc};
use cufflink_storage::{CannedAcl, Credential};
use deadpool_postgres::GenericClient;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct BucketRow {
    #[serde(skip)]
    pub id: Uuid,
    #[serde(skip)]
    pub tenant_id: Uuid,
    pub bucket: String,
    #[serde(flatten)]
    pub rules: Rules,
    pub credential: Option<AccessKeyId>,
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
                       secret_access_key, secret_access_key_enc, updated_at, updated_by";

fn acls(names: Vec<String>) -> PlatformResult<std::collections::BTreeSet<CannedAcl>> {
    names
        .iter()
        .map(|name| {
            CannedAcl::parse(name)
                .ok_or_else(|| PlatformError::Internal(format!("tenant bucket ACL {name:?}")))
        })
        .collect()
}

fn row(row: &tokio_postgres::Row) -> PlatformResult<BucketRow> {
    Ok(BucketRow {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        bucket: row.get("bucket"),
        rules: Rules {
            key_prefixes: row.get("key_prefixes"),
            acls: acls(row.get("acls"))?,
        },
        credential: row
            .get::<_, Option<String>>("access_key_id")
            .map(|access_key_id| AccessKeyId { access_key_id }),
        updated_at: row.get("updated_at"),
        updated_by: row.get("updated_by"),
        secret: Stored::new(
            row.get("secret_access_key"),
            row.get("secret_access_key_enc"),
        ),
    })
}

fn rows(found: &[tokio_postgres::Row]) -> PlatformResult<Vec<BucketRow>> {
    found.iter().map(row).collect()
}

pub async fn list_all(client: &impl GenericClient) -> PlatformResult<Vec<BucketRow>> {
    let found = client
        .query(
            &format!("SELECT {COLUMNS} FROM tenant_buckets ORDER BY bucket"),
            &[],
        )
        .await?;
    rows(&found)
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
    rows(&found)
}

pub async fn find_for_update(
    client: &impl GenericClient,
    bucket: &str,
) -> PlatformResult<Option<BucketRow>> {
    client
        .query_opt(
            &format!("SELECT {COLUMNS} FROM tenant_buckets WHERE bucket = $1 FOR UPDATE"),
            &[&bucket],
        )
        .await?
        .as_ref()
        .map(row)
        .transpose()
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
        .unwrap_or_else(|| {
            Err(PlatformError::Conflict(format!(
                "bucket {} is registered to another tenant",
                new.bucket
            )))
        })
}

pub async fn delete(
    client: &impl GenericClient,
    tenant_id: Uuid,
    bucket: &str,
) -> PlatformResult<Option<BucketRow>> {
    client
        .query_opt(
            &format!(
                "DELETE FROM tenant_buckets WHERE tenant_id = $1 AND bucket = $2 RETURNING {COLUMNS}"
            ),
            &[&tenant_id, &bucket],
        )
        .await?
        .as_ref()
        .map(row)
        .transpose()
}

pub fn aad(id: Uuid) -> crate::secrets::SecretAad {
    TENANT_BUCKET_SECRET.aad([id.to_string()])
}

impl BucketRow {
    pub fn unopened(self) -> TenantBucket {
        let access = match self.credential {
            None => Access::Platform,
            Some(_) => Access::Unopenable,
        };
        TenantBucket {
            tenant_id: self.tenant_id,
            bucket: self.bucket,
            rules: self.rules,
            access,
        }
    }

    pub async fn open(self, secrets: &SecretBox, client: &impl GenericClient) -> TenantBucket {
        let access = match &self.credential {
            None => Access::Platform,
            Some(AccessKeyId { access_key_id }) => match secrets
                .read(client, self.tenant_id, &aad(self.id), &self.secret)
                .await
            {
                Ok(Some(secret)) => Access::Own(Credential {
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret.expose().to_string(),
                }),
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
        TenantBucket {
            tenant_id: self.tenant_id,
            bucket: self.bucket,
            rules: self.rules,
            access,
        }
    }
}
