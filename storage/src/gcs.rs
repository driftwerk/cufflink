use crate::common::{self, Keys};
use crate::gcs_v4::{sign_url, BlobSigner, IamSignBlobSigner};
use crate::parts::Parted;
use crate::{Credential, ObjectMeta, ObjectReader, ObjectStorage, UploadOptions, Uploaded};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use eyre::{Result, WrapErr};
use futures_util::stream::BoxStream;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use std::sync::Arc;

const BACKEND: &str = "gcs";

const ENV_BUCKET: &str = "GCS_BUCKET";
const ENV_SA_KEY_PATH: &str = "GOOGLE_APPLICATION_CREDENTIALS";

#[derive(Clone)]
pub(crate) struct GcsBackend {
    primary: Arc<GoogleCloudStorage>,
    writes: Arc<Parted<GoogleCloudStorage>>,
    bucket: String,
    sa_key_path: Option<String>,
    iam_signer: Option<Arc<IamSignBlobSigner>>,
    keys: Keys,
    stall: common::Stall,
}

impl GcsBackend {
    pub(crate) fn from_vars(vars: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let bucket = vars(ENV_BUCKET)
            .ok_or_else(|| eyre::eyre!("Missing required environment variable: {}", ENV_BUCKET))?;
        let sa_key_path = vars(ENV_SA_KEY_PATH);
        let stall = common::Stall::configured();
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
            request_timeout_secs = common::request_timeout().as_secs(),
            "Object storage configured",
        );
        common::warn_unless_timed_out_requests_retry(BACKEND);

        Ok(Self::serving(
            primary,
            bucket,
            sa_key_path,
            iam_signer,
            Keys::Encoded,
            stall,
        ))
    }

    fn serving(
        primary: Arc<GoogleCloudStorage>,
        bucket: String,
        sa_key_path: Option<String>,
        iam_signer: Option<Arc<IamSignBlobSigner>>,
        keys: Keys,
        stall: common::Stall,
    ) -> Self {
        Self {
            writes: Arc::new(Parted::new(Arc::clone(&primary), BACKEND, stall)),
            primary,
            bucket,
            sa_key_path,
            iam_signer,
            keys,
            stall,
        }
    }

    fn tenant(&self, bucket: &str) -> Result<Self> {
        Ok(Self::serving(
            build_store(bucket, self.sa_key_path.as_deref())?,
            bucket.to_string(),
            self.sa_key_path.clone(),
            self.iam_signer.clone(),
            Keys::Exact,
            self.stall,
        ))
    }

    fn store_for(&self, bucket: &str) -> Result<Arc<GoogleCloudStorage>> {
        common::admit_bucket(&self.bucket, self.keys, bucket)?;
        if bucket == self.bucket {
            return Ok(Arc::clone(&self.primary));
        }
        build_store(bucket, self.sa_key_path.as_deref())
    }

    fn writer_for(&self, bucket: &str) -> Result<Arc<Parted<GoogleCloudStorage>>> {
        common::admit_bucket(&self.bucket, self.keys, bucket)?;
        if bucket == self.bucket {
            return Ok(Arc::clone(&self.writes));
        }
        Ok(Arc::new(Parted::new(
            build_store(bucket, self.sa_key_path.as_deref())?,
            BACKEND,
            self.stall,
        )))
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
                common::presign_url(
                    store.as_ref(),
                    BACKEND,
                    self.keys,
                    method,
                    key,
                    expires_in_secs,
                )
                .await
            }
            Some(signer) => {
                common::admit_bucket(&self.bucket, self.keys, bucket)?;
                self.keys.path(key)?;
                let signer: &dyn BlobSigner = signer;
                sign_url(signer, Utc::now(), &method, bucket, key, expires_in_secs).await
            }
        }
    }
}

fn build_store(bucket: &str, sa_key_path: Option<&str>) -> Result<Arc<GoogleCloudStorage>> {
    let mut builder = GoogleCloudStorageBuilder::new()
        .with_bucket_name(bucket)
        .with_client_options(common::client_options())
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
        let store = self.writer_for(bucket)?;
        common::no_acl(BACKEND, options)?;
        common::put(
            store.as_ref(),
            BACKEND,
            self.keys,
            key,
            data,
            options,
            self.stall,
        )
        .await
    }

    async fn upload_stream(
        &self,
        key: &str,
        data: BoxStream<'static, Result<Bytes>>,
        content_type: &str,
    ) -> Result<Uploaded> {
        let store = self.writer_for(&self.bucket)?;
        let options = UploadOptions::content_type(content_type);
        common::put_stream(
            store.as_ref(),
            BACKEND,
            self.keys,
            key,
            data,
            &options,
            self.stall,
        )
        .await
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>> {
        common::get_bytes(
            self.primary.clone(),
            BACKEND,
            self.keys,
            None,
            key,
            self.stall,
        )
        .await
    }

    async fn download_from_bucket(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let store = self.store_for(bucket)?;
        common::get_bytes(store, BACKEND, self.keys, Some(bucket), key, self.stall).await
    }

    async fn open_read(&self, bucket: &str, key: &str) -> Result<Option<ObjectReader>> {
        let store = self.store_for(bucket)?;
        common::open_read(store, BACKEND, self.keys, bucket, key, self.stall).await
    }

    async fn head(&self, key: &str) -> Result<u64> {
        common::head_size(self.primary.as_ref(), BACKEND, self.keys, key).await
    }

    async fn head_in_bucket(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let store = self.store_for(bucket)?;
        common::head_meta(store.as_ref(), BACKEND, self.keys, bucket, key).await
    }

    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<()> {
        common::copy_object(self.primary.as_ref(), BACKEND, self.keys, src_key, dst_key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        common::delete_object(self.primary.as_ref(), BACKEND, self.keys, key).await
    }

    async fn delete_from_bucket(&self, bucket: &str, key: &str) -> Result<()> {
        let store = self.store_for(bucket)?;
        common::delete_object_idempotent(store.as_ref(), BACKEND, self.keys, key).await
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

    fn tenant_bucket(
        &self,
        bucket: &str,
        credential: Option<&Credential>,
    ) -> Result<Arc<dyn ObjectStorage>> {
        eyre::ensure!(
            credential.is_none(),
            "the {BACKEND} backend takes no bucket credential"
        );
        eyre::ensure!(
            crate::is_bucket_name(bucket),
            "{bucket:?} is not a bucket name"
        );
        Ok(Arc::new(self.tenant(bucket)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::test_env::with as with_env;
    use cufflink_settings::process_var as var;

    const OBJECT: &str = "/example%2Ddocuments/bundles%2Fapp%2Etar%2Egz";

    fn emulated(server: &str) -> GcsBackend {
        emulated_with(server, common::Stall::configured())
    }

    fn emulated_with(server: &str, stall: common::Stall) -> GcsBackend {
        let key = serde_json::json!({
            "gcs_base_url": server,
            "disable_oauth": true,
            "client_email": "",
            "private_key": "",
            "private_key_id": "",
        });
        let store = Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_bucket_name("example-documents")
                .with_service_account_key(key.to_string())
                .with_client_options(common::client_options().with_allow_http(true))
                .with_retry(common::retry_config())
                .build()
                .expect("store"),
        );
        GcsBackend::serving(
            store,
            "example-documents".into(),
            None,
            None,
            Keys::Encoded,
            stall,
        )
    }

    #[tokio::test]
    async fn a_large_upload_goes_up_in_parts_through_the_xml_api() {
        use wiremock::matchers::{header, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(OBJECT))
            .and(query_param("uploads", ""))
            .and(header("content-type", "application/gzip"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(OBJECT))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"part\""))
            .expect(5)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(OBJECT))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CompleteMultipartUploadResult><ETag>\"whole-5\"</ETag></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        let data: Vec<u8> = (0..40 * 1024 * 1024)
            .map(|i: usize| (i % 251) as u8)
            .collect();
        let uploaded = emulated(&server.uri())
            .upload_to_bucket_with(
                "example-documents",
                "bundles/app.tar.gz",
                data.clone(),
                &UploadOptions::content_type("application/gzip"),
            )
            .await
            .expect("uploaded");
        assert_eq!(
            (uploaded.size, uploaded.etag.as_deref()),
            (data.len() as u64, Some("\"whole-5\""))
        );
        assert!(uploaded.completion.is_some());
    }

    #[tokio::test]
    async fn a_part_whose_answer_stalls_is_cut_at_the_stall_timeout_and_sent_again() {
        use std::time::Duration;
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(OBJECT))
            .and(query_param("uploads", ""))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>",
            ))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(OBJECT))
            .and(query_param("partNumber", "1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"stalled\"")
                    .set_delay(Duration::from_secs(10)),
            )
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(OBJECT))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"part\""))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(OBJECT))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CompleteMultipartUploadResult><ETag>\"whole-5\"</ETag></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        let stall = common::Stall::of(
            Duration::from_secs(1),
            3,
            Duration::from_secs(30),
            Duration::from_secs(1),
        );
        let uploaded = emulated_with(&server.uri(), stall)
            .upload_to_bucket_with(
                "example-documents",
                "bundles/app.tar.gz",
                vec![7; 40 * 1024 * 1024],
                &UploadOptions::content_type("application/gzip"),
            )
            .await
            .expect("uploaded");
        assert_eq!(uploaded.etag.as_deref(), Some("\"whole-5\""));
        assert_eq!(common::test_wire::first_part_sent(&server).await, 2);
    }

    #[test]
    fn a_tenant_bucket_writes_through_a_store_of_its_own_bucket() {
        let platform = emulated("http://127.0.0.1:1");
        let tenant = platform.tenant("acme-documents").expect("tenant");
        let writer = tenant
            .writer_for("acme-documents")
            .expect("writer")
            .to_string();
        assert_eq!(writer, format!("Parted({})", tenant.primary));
        assert_ne!(writer, format!("Parted({})", platform.primary));
    }

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
