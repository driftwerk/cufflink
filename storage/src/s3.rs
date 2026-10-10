use crate::{common, ObjectMeta, ObjectReader, ObjectStorage, UploadOptions, Uploaded};
use async_trait::async_trait;
use eyre::{Result, WrapErr};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::sync::Arc;

const BACKEND: &str = "s3";

const ENV_ENDPOINT: &str = "S3_ENDPOINT";
const ENV_REGION: &str = "S3_REGION";
const ENV_ACCESS_KEY: &str = "S3_ACCESS_KEY";
const ENV_SECRET_KEY: &str = "S3_SECRET_KEY";
const ENV_BUCKET: &str = "S3_BUCKET";

#[derive(Clone)]
pub(crate) struct S3Backend {
    primary: Arc<AmazonS3>,
    endpoint: String,
    region: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    allow_http: bool,
}

impl S3Backend {
    pub(crate) fn env_is_complete(vars: impl Fn(&str) -> Option<String>) -> bool {
        [
            ENV_ENDPOINT,
            ENV_REGION,
            ENV_ACCESS_KEY,
            ENV_SECRET_KEY,
            ENV_BUCKET,
        ]
        .iter()
        .all(|k| vars(k).is_some())
    }

    pub(crate) fn from_vars(vars: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let endpoint = env_required(&vars, ENV_ENDPOINT)?;
        let region = vars(ENV_REGION).unwrap_or_else(|| "us-east-1".to_string());
        let access_key = env_required(&vars, ENV_ACCESS_KEY)?;
        let secret_key = env_required(&vars, ENV_SECRET_KEY)?;
        let bucket = vars(ENV_BUCKET).unwrap_or_else(|| "cufflink".to_string());
        let allow_http = endpoint.starts_with("http://");

        let primary = build_store(
            &endpoint,
            &region,
            &access_key,
            &secret_key,
            &bucket,
            allow_http,
        )?;

        tracing::info!(
            backend = BACKEND,
            endpoint = %endpoint,
            bucket = %bucket,
            region = %region,
            retry_timeout_secs = common::retry_timeout().as_secs(),
            "Object storage configured",
        );

        Ok(Self {
            primary,
            endpoint,
            region,
            access_key,
            secret_key,
            bucket,
            allow_http,
        })
    }

    fn store_for(&self, bucket: &str) -> Result<Arc<AmazonS3>> {
        if bucket == self.bucket {
            return Ok(Arc::clone(&self.primary));
        }
        build_store(
            &self.endpoint,
            &self.region,
            &self.access_key,
            &self.secret_key,
            bucket,
            self.allow_http,
        )
    }
}

fn build_store(
    endpoint: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    bucket: &str,
    allow_http: bool,
) -> Result<Arc<AmazonS3>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_bucket_name(bucket)
        .with_allow_http(allow_http)
        .with_retry(common::retry_config())
        .build()
        .wrap_err("Failed to build S3 client")?;
    Ok(Arc::new(store))
}

fn env_required(vars: impl Fn(&str) -> Option<String>, key: &str) -> Result<String> {
    vars(key).ok_or_else(|| eyre::eyre!("Missing required environment variable: {}", key))
}

#[async_trait]
impl ObjectStorage for S3Backend {
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
        common::presign_url(
            self.primary.as_ref(),
            BACKEND,
            http::Method::PUT,
            key,
            expires_in_secs,
        )
        .await
    }

    async fn presign_upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        _content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        let store = self.store_for(bucket)?;
        common::presign_url(
            store.as_ref(),
            BACKEND,
            http::Method::PUT,
            key,
            expires_in_secs,
        )
        .await
    }

    async fn presign_download(&self, key: &str, expires_in_secs: u64) -> Result<String> {
        common::presign_url(
            self.primary.as_ref(),
            BACKEND,
            http::Method::GET,
            key,
            expires_in_secs,
        )
        .await
    }

    async fn presign_download_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        let store = self.store_for(bucket)?;
        common::presign_url(
            store.as_ref(),
            BACKEND,
            http::Method::GET,
            key,
            expires_in_secs,
        )
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
    fn env_is_complete_detects_all_vars_set() {
        with_env(
            &[
                (ENV_ENDPOINT, Some("https://hel1.your-objectstorage.com")),
                (ENV_REGION, Some("us-east-1")),
                (ENV_ACCESS_KEY, Some("ak")),
                (ENV_SECRET_KEY, Some("sk")),
                (ENV_BUCKET, Some("cufflink")),
            ],
            || assert!(S3Backend::env_is_complete(var)),
        );
    }

    #[test]
    fn env_is_complete_rejects_missing_var() {
        with_env(
            &[
                (ENV_ENDPOINT, Some("https://hel1.your-objectstorage.com")),
                (ENV_REGION, Some("us-east-1")),
                (ENV_ACCESS_KEY, Some("ak")),
                (ENV_SECRET_KEY, None),
                (ENV_BUCKET, Some("cufflink")),
            ],
            || assert!(!S3Backend::env_is_complete(var)),
        );
    }

    #[test]
    fn from_env_requires_endpoint() {
        with_env(
            &[
                (ENV_ENDPOINT, None),
                (ENV_ACCESS_KEY, Some("ak")),
                (ENV_SECRET_KEY, Some("sk")),
            ],
            || {
                let err = S3Backend::from_vars(var)
                    .err()
                    .expect("missing S3_ENDPOINT must error");
                assert!(err.to_string().contains("S3_ENDPOINT"));
            },
        );
    }

    #[test]
    fn from_env_defaults_region_and_bucket() {
        with_env(
            &[
                (ENV_ENDPOINT, Some("http://minio:9000")),
                (ENV_REGION, None),
                (ENV_ACCESS_KEY, Some("ak")),
                (ENV_SECRET_KEY, Some("sk")),
                (ENV_BUCKET, None),
            ],
            || {
                let backend = S3Backend::from_vars(var).expect("should build");
                assert_eq!(backend.region, "us-east-1");
                assert_eq!(backend.bucket, "cufflink");
                assert!(backend.allow_http, "http:// endpoint must allow_http");
            },
        );
    }

    #[test]
    fn from_env_disables_allow_http_for_https_endpoint() {
        with_env(
            &[
                (ENV_ENDPOINT, Some("https://hel1.your-objectstorage.com")),
                (ENV_REGION, Some("eu-central-1")),
                (ENV_ACCESS_KEY, Some("ak")),
                (ENV_SECRET_KEY, Some("sk")),
                (ENV_BUCKET, Some("cufflink")),
            ],
            || {
                let backend = S3Backend::from_vars(var).expect("should build");
                assert!(!backend.allow_http);
            },
        );
    }

    #[test]
    fn store_for_returns_primary_when_bucket_matches() {
        with_env(
            &[
                (ENV_ENDPOINT, Some("http://minio:9000")),
                (ENV_REGION, Some("us-east-1")),
                (ENV_ACCESS_KEY, Some("ak")),
                (ENV_SECRET_KEY, Some("sk")),
                (ENV_BUCKET, Some("cufflink")),
            ],
            || {
                let backend = S3Backend::from_vars(var).expect("should build");
                let same = backend.store_for("cufflink").expect("store_for primary");
                assert!(
                    Arc::ptr_eq(&same, &backend.primary),
                    "same bucket must short-circuit to shared Arc"
                );
                let other = backend
                    .store_for("other-bucket")
                    .expect("store_for other bucket");
                assert!(
                    !Arc::ptr_eq(&other, &backend.primary),
                    "different bucket must produce a fresh Arc"
                );
            },
        );
    }

    #[test]
    fn backend_kind_and_primary_bucket_accessors() {
        with_env(
            &[
                (ENV_ENDPOINT, Some("http://minio:9000")),
                (ENV_REGION, Some("us-east-1")),
                (ENV_ACCESS_KEY, Some("ak")),
                (ENV_SECRET_KEY, Some("sk")),
                (ENV_BUCKET, Some("my-bucket")),
            ],
            || {
                let backend = S3Backend::from_vars(var).expect("should build");
                assert_eq!(backend.backend_kind(), "s3");
                assert_eq!(backend.primary_bucket(), "my-bucket");
            },
        );
    }

    fn test_backend() -> S3Backend {
        backend_at("http://minio:9000")
    }

    fn backend_at(endpoint: &str) -> S3Backend {
        let mut backend = None;
        with_env(
            &[
                (ENV_ENDPOINT, Some(endpoint)),
                (ENV_REGION, Some("us-east-1")),
                (ENV_ACCESS_KEY, Some("ak")),
                (ENV_SECRET_KEY, Some("sk")),
                (ENV_BUCKET, Some("cufflink")),
            ],
            || backend = Some(S3Backend::from_vars(var).expect("should build")),
        );
        backend.unwrap()
    }

    #[tokio::test]
    async fn an_upload_sends_how_the_object_is_served_and_answers_its_etag() {
        use wiremock::matchers::{body_bytes, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/receipts/book-1/receipt-2_Acme.pdf"))
            .and(header("content-type", "application/pdf"))
            .and(header(
                "content-disposition",
                "attachment; filename=\"receipt.pdf\"",
            ))
            .and(header("cache-control", "no-store"))
            .and(body_bytes(b"%PDF-1.7".to_vec()))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"9b2cf535\""))
            .expect(1)
            .mount(&server)
            .await;
        let uploaded = backend_at(&server.uri())
            .upload_to_bucket_with(
                "receipts",
                "book-1/receipt-2_Acme.pdf",
                b"%PDF-1.7".to_vec(),
                &UploadOptions {
                    content_type: "application/pdf".into(),
                    content_disposition: Some("attachment; filename=\"receipt.pdf\"".into()),
                    cache_control: Some("no-store".into()),
                },
            )
            .await
            .expect("uploaded");
        assert_eq!(
            uploaded,
            Uploaded {
                size: 8,
                etag: Some("\"9b2cf535\"".into())
            }
        );
    }

    #[tokio::test]
    async fn head_reads_what_is_stored_without_its_bytes() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/receipts/book-1/receipt-2_Acme.pdf"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "8")
                    .insert_header("content-type", "application/pdf")
                    .insert_header("etag", "\"9b2cf535\"")
                    .insert_header("last-modified", "Mon, 28 Sep 2026 10:00:00 GMT"),
            )
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/receipts/missing.pdf"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/receipts/forbidden.pdf"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let backend = backend_at(&server.uri());
        let meta = backend
            .head_in_bucket("receipts", "book-1/receipt-2_Acme.pdf")
            .await
            .expect("head")
            .expect("found");
        assert_eq!(
            meta,
            ObjectMeta {
                size: 8,
                content_type: Some("application/pdf".into()),
                etag: Some("\"9b2cf535\"".into()),
                last_modified: "2026-09-28T10:00:00Z".parse().expect("time"),
            }
        );
        assert_eq!(
            backend
                .head_in_bucket("receipts", "missing.pdf")
                .await
                .expect("head"),
            None
        );
        let refused = backend
            .head_in_bucket("receipts", "forbidden.pdf")
            .await
            .expect_err("refused");
        assert_eq!(
            refused.to_string(),
            "s3 head failed for receipts/forbidden.pdf"
        );
        assert!(server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .all(|request| request.method.as_str() == "HEAD"));
    }

    #[tokio::test]
    async fn presign_download_builds_signed_get_url() {
        let url = test_backend()
            .presign_download("media/professional/photo.jpg", 300)
            .await
            .expect("presign should succeed");
        assert!(url.contains("/cufflink/media/professional/photo.jpg"));
        assert!(url.contains("X-Amz-Expires=300"));
        assert!(url.contains("X-Amz-Signature="));
    }

    #[tokio::test]
    async fn presign_download_to_bucket_honours_bucket_override() {
        let url = test_backend()
            .presign_download_to_bucket("other-bucket", "k.jpg", 60)
            .await
            .expect("presign should succeed");
        assert!(url.contains("/other-bucket/k.jpg"));
        assert!(url.contains("X-Amz-Expires=60"));
    }
}
