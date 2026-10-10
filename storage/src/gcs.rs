use crate::gcs_v4::{sign_url, BlobSigner, IamSignBlobSigner};
use crate::{common, ObjectMeta, ObjectReader, ObjectStorage, UploadOptions, Uploaded};
use async_trait::async_trait;
use chrono::Utc;
use eyre::{Result, WrapErr};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use std::sync::Arc;

const BACKEND: &str = "gcs";

const ENV_BUCKET: &str = "GCS_BUCKET";
const ENV_SA_KEY_PATH: &str = "GOOGLE_APPLICATION_CREDENTIALS";

#[derive(Clone)]
pub(crate) struct GcsBackend {
    primary: Arc<GoogleCloudStorage>,
    bucket: String,
    sa_key_path: Option<String>,
    iam_signer: Option<Arc<IamSignBlobSigner>>,
}

impl GcsBackend {
    pub(crate) fn from_vars(vars: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let bucket = vars(ENV_BUCKET)
            .ok_or_else(|| eyre::eyre!("Missing required environment variable: {}", ENV_BUCKET))?;
        let sa_key_path = vars(ENV_SA_KEY_PATH);
        let primary = build_store(&bucket, sa_key_path.as_deref())?;
        let iam_signer = match sa_key_path.as_deref() {
            Some(_) => None,
            None => Some(Arc::new(IamSignBlobSigner::new()?)),
        };

        tracing::info!(
            backend = BACKEND,
            bucket = %bucket,
            auth = if sa_key_path.is_some() { "service-account-key" } else { "application-default-credentials" },
            presign = if sa_key_path.is_some() { "local-key" } else { "iam-sign-blob" },
            retry_timeout_secs = common::retry_timeout().as_secs(),
            "Object storage configured",
        );

        Ok(Self {
            primary,
            bucket,
            sa_key_path,
            iam_signer,
        })
    }

    fn store_for(&self, bucket: &str) -> Result<Arc<GoogleCloudStorage>> {
        if bucket == self.bucket {
            return Ok(Arc::clone(&self.primary));
        }
        build_store(bucket, self.sa_key_path.as_deref())
    }

    async fn presign(
        &self,
        method: http::Method,
        bucket: &str,
        key: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        match self.iam_signer.as_deref() {
            None => {
                let store = self.store_for(bucket)?;
                common::presign_url(store.as_ref(), BACKEND, method, key, expires_in_secs).await
            }
            Some(signer) => {
                let signer: &dyn BlobSigner = signer;
                sign_url(signer, Utc::now(), &method, bucket, key, expires_in_secs).await
            }
        }
    }
}

fn build_store(bucket: &str, sa_key_path: Option<&str>) -> Result<Arc<GoogleCloudStorage>> {
    let mut builder = GoogleCloudStorageBuilder::new()
        .with_bucket_name(bucket)
        .with_retry(common::retry_config());
    if let Some(path) = sa_key_path {
        builder = builder.with_service_account_path(path);
    }
    let store = builder.build().wrap_err("Failed to build GCS client")?;
    Ok(Arc::new(store))
}

#[async_trait]
impl ObjectStorage for GcsBackend {
    async fn upload(&self, key: &str, data: Vec<u8>, content_type: &str) -> Result<()> {
        self.upload_to_bucket(&self.bucket, key, data, content_type)
            .await
    }

    async fn upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: &str,
    ) -> Result<()> {
        let options = UploadOptions::content_type(content_type);
        self.upload_to_bucket_with(bucket, key, data, &options)
            .await
            .map(drop)
    }

    async fn upload_to_bucket_with(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        options: &UploadOptions,
    ) -> Result<Uploaded> {
        let store = self.store_for(bucket)?;
        common::put(store.as_ref(), BACKEND, key, data, options).await
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>> {
        common::get_bytes(self.primary.as_ref(), BACKEND, None, key).await
    }

    async fn download_from_bucket(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let store = self.store_for(bucket)?;
        common::get_bytes(store.as_ref(), BACKEND, Some(bucket), key).await
    }

    async fn open_read(&self, bucket: &str, key: &str) -> Result<Option<ObjectReader>> {
        let store = self.store_for(bucket)?;
        common::open_read(store.as_ref(), BACKEND, bucket, key).await
    }

    async fn head(&self, key: &str) -> Result<u64> {
        common::head_size(self.primary.as_ref(), BACKEND, key).await
    }

    async fn head_in_bucket(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let store = self.store_for(bucket)?;
        common::head_meta(store.as_ref(), BACKEND, bucket, key).await
    }

    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<()> {
        common::copy_object(self.primary.as_ref(), BACKEND, src_key, dst_key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        common::delete_object(self.primary.as_ref(), BACKEND, key).await
    }

    async fn delete_from_bucket(&self, bucket: &str, key: &str) -> Result<()> {
        let store = self.store_for(bucket)?;
        common::delete_object_idempotent(store.as_ref(), BACKEND, key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        common::delete_prefix(self.primary.as_ref(), BACKEND, prefix).await
    }

    async fn presign_upload(
        &self,
        key: &str,
        _content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        let bucket = self.bucket.clone();
        self.presign(http::Method::PUT, &bucket, key, expires_in_secs)
            .await
    }

    async fn presign_upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        _content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        self.presign(http::Method::PUT, bucket, key, expires_in_secs)
            .await
    }

    async fn presign_download(&self, key: &str, expires_in_secs: u64) -> Result<String> {
        let bucket = self.bucket.clone();
        self.presign(http::Method::GET, &bucket, key, expires_in_secs)
            .await
    }

    async fn presign_download_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        self.presign(http::Method::GET, bucket, key, expires_in_secs)
            .await
    }

    fn primary_bucket(&self) -> &str {
        &self.bucket
    }

    fn backend_kind(&self) -> &'static str {
        BACKEND
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::test_env::with as with_env;
    use cufflink_settings::process_var as var;

    #[test]
    fn from_env_requires_bucket() {
        with_env(&[(ENV_BUCKET, None), (ENV_SA_KEY_PATH, None)], || {
            let err = GcsBackend::from_vars(var)
                .err()
                .expect("missing GCS_BUCKET must error");
            assert!(err.to_string().contains("GCS_BUCKET"));
        });
    }

    #[test]
    fn from_env_builds_with_adc_and_attaches_iam_signer_when_no_key() {
        with_env(
            &[(ENV_BUCKET, Some("my-bucket")), (ENV_SA_KEY_PATH, None)],
            || {
                let backend = GcsBackend::from_vars(var).expect("should build with ADC");
                assert_eq!(backend.bucket, "my-bucket");
                assert!(backend.sa_key_path.is_none());
                assert!(
                    backend.iam_signer.is_some(),
                    "keyless build must attach the IAM signBlob signer"
                );
            },
        );
    }

    #[test]
    fn store_for_returns_primary_when_bucket_matches() {
        with_env(
            &[(ENV_BUCKET, Some("primary")), (ENV_SA_KEY_PATH, None)],
            || {
                let backend = GcsBackend::from_vars(var).expect("should build");
                let same = backend.store_for("primary").expect("primary");
                assert!(Arc::ptr_eq(&same, &backend.primary));
                let other = backend.store_for("other").expect("other");
                assert!(!Arc::ptr_eq(&other, &backend.primary));
            },
        );
    }

    #[test]
    fn backend_kind_and_primary_bucket_accessors() {
        with_env(&[(ENV_BUCKET, Some("b")), (ENV_SA_KEY_PATH, None)], || {
            let backend = GcsBackend::from_vars(var).expect("should build");
            assert_eq!(backend.backend_kind(), "gcs");
            assert_eq!(backend.primary_bucket(), "b");
        });
    }
}
