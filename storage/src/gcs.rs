use crate::common::{self, Keys};
use crate::compose::{Api, Composer, DEFAULT_BASE_URL};
use crate::copied;
use crate::gcs_v4::{sign_url, BlobSigner, IamSignBlobSigner};
use crate::parts::Parted;
use crate::{
    Credential, ObjectMeta, ObjectReader, ObjectStorage, StoredPart, UploadOptions, Uploaded,
};
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
    composer: Composer,
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
        let api = Api {
            client: copied::client(&common::client_options())
                .wrap_err("Failed to build GCS client")?,
            base_url: base_url(sa_key_path.as_deref())?,
        };
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
            api,
            bucket,
            sa_key_path,
            iam_signer,
            Keys::Encoded,
            stall,
        ))
    }

    fn serving(
        primary: Arc<GoogleCloudStorage>,
        api: Api,
        bucket: String,
        sa_key_path: Option<String>,
        iam_signer: Option<Arc<IamSignBlobSigner>>,
        keys: Keys,
        stall: common::Stall,
    ) -> Self {
        Self {
            writes: Arc::new(Parted::new(Arc::clone(&primary), BACKEND, stall)),
            composer: Composer {
                store: Arc::clone(&primary),
                api,
                bucket: bucket.clone(),
                stall,
            },
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
            self.composer.api.clone(),
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

/// Where the store's JSON API is: the service-account key's
/// `gcs_base_url`, as object_store reads it, or Google's.
fn base_url(sa_key_path: Option<&str>) -> Result<String> {
    #[derive(serde::Deserialize)]
    struct Key {
        gcs_base_url: Option<String>,
    }
    sa_key_path.map_or(Ok(DEFAULT_BASE_URL.to_string()), |path| {
        let key = std::fs::read(path)
            .wrap_err_with(|| format!("Failed to read the GCS service account key {path}"))?;
        let key: Key = serde_json::from_slice(&key)
            .wrap_err_with(|| format!("Failed to parse the GCS service account key {path}"))?;
        Ok(key
            .gcs_base_url
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string()))
    })
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

    async fn assemble(
        &self,
        key: &str,
        parts: Vec<StoredPart>,
        data: BoxStream<'static, Result<Bytes>>,
        content_type: &str,
    ) -> Result<Uploaded> {
        if parts.is_empty() {
            return self.upload_stream(key, data, content_type).await;
        }
        self.composer
            .assemble(self.keys, key, parts, data, content_type)
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
    use crate::common::test_wire::{bundle_bytes, requests_until, stored_parts};
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
            Api {
                client: copied::client(&common::client_options().with_allow_http(true))
                    .expect("client"),
                base_url: server.to_string(),
            },
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

    const COMPOSED: &str = "/storage/v1/b/example-documents/o/bundles%2Fapp.tar.gz/compose";

    fn composed_from(request: &wiremock::Request) -> Vec<String> {
        serde_json::from_slice::<serde_json::Value>(&request.body)
            .ok()
            .and_then(|body| {
                body["sourceObjects"].as_array().map(|sources| {
                    sources
                        .iter()
                        .filter_map(|source| source["name"].as_str().map(str::to_string))
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn an_assembly_of_up_to_32_parts_is_one_compose_with_its_content_type() {
        use wiremock::matchers::{body_partial_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(COMPOSED))
            .and(body_partial_json(serde_json::json!({
                "destination": { "contentType": "application/gzip" }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "etag": "CJ6k", "generation": "7", "size": "256"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let uploaded = emulated(&server.uri())
            .assemble(
                "bundles/app.tar.gz",
                stored_parts(&[8; 32]),
                bundle_bytes(vec![Ok(Bytes::from_static(b"the parts"))]),
                "application/gzip",
            )
            .await
            .expect("assembled");
        assert_eq!(
            (uploaded.size, uploaded.etag.as_deref()),
            (256, Some("CJ6k"))
        );
        assert!(uploaded.completion.is_some());
        assert_eq!(uploaded.parts, crate::PartTimes::default());
        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            composed_from(&requests[0]),
            (0..32)
                .map(|index| format!("parts/{index}"))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn an_assembly_of_more_than_32_parts_is_composed_in_tiers_whose_objects_are_removed() {
        use wiremock::matchers::{method, path_regex};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex("/compose$"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "etag": "CJ6k" })),
            )
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let uploaded = emulated(&server.uri())
            .assemble(
                "bundles/app.tar.gz",
                stored_parts(&[8; 33]),
                bundle_bytes(vec![Ok(Bytes::from_static(b"the parts"))]),
                "application/gzip",
            )
            .await
            .expect("assembled");
        assert_eq!(uploaded.size, 33 * 8);
        assert_eq!(uploaded.parts.in_flight, 1);
        let requests = server.received_requests().await.unwrap_or_default();
        let tier = requests
            .iter()
            .find(|request| request.method.as_str() == "POST" && request.url.path() != COMPOSED)
            .expect("a tier composed");
        assert_eq!(
            composed_from(tier),
            (0..32)
                .map(|index| format!("parts/{index}"))
                .collect::<Vec<_>>()
        );
        let last = requests
            .iter()
            .find(|request| request.url.path() == COMPOSED)
            .map(composed_from)
            .expect("composed into its key");
        let [intermediate, single] = last.as_slice() else {
            panic!("composed from {last:?}");
        };
        assert!(
            intermediate.starts_with("bundles/app.tar.gz.compose/")
                && intermediate.ends_with("/1-0"),
            "{intermediate}"
        );
        assert_eq!(single, "parts/32");
        assert!(
            requests_until(&server, |request| request.method.as_str() == "DELETE").await,
            "the intermediate object is removed"
        );
    }

    #[tokio::test]
    async fn a_missing_part_is_answered_as_the_read_answers_it_not_as_the_compose_does() {
        use futures_util::StreamExt;
        use wiremock::matchers::{method, path_regex};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex("/compose$"))
            .respond_with(ResponseTemplate::new(404).set_body_string("No such object"))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let read = futures_util::stream::iter([
            Ok(Bytes::from_static(b"the parts")),
            Err(eyre::eyre!("part 32 was not uploaded")),
        ])
        .then(|chunk| async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            chunk
        });
        let failed = emulated(&server.uri())
            .assemble(
                "bundles/app.tar.gz",
                stored_parts(&[8; 33]),
                Box::pin(read),
                "application/gzip",
            )
            .await
            .expect_err("failed");
        assert_eq!(failed.to_string(), "part 32 was not uploaded");
    }

    #[tokio::test]
    async fn a_key_that_must_be_encoded_is_encoded_in_the_url_and_named_as_it_is() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/storage/v1/b/example-documents/o/bundles%2Fmy%20app%2B1.tar.gz/compose",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "etag": "CJ6k" })),
            )
            .expect(1)
            .mount(&server)
            .await;
        emulated(&server.uri())
            .assemble(
                "bundles/my app+1.tar.gz",
                vec![StoredPart {
                    key: "parts/a b+0".into(),
                    size: 8,
                }],
                bundle_bytes(vec![Ok(Bytes::from_static(b"the part"))]),
                "application/gzip",
            )
            .await
            .expect("assembled");
        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(composed_from(&requests[0]), ["parts/a b+0"]);
    }

    #[tokio::test]
    async fn an_assembly_whose_bytes_fail_their_check_is_never_composed_into_its_key() {
        use wiremock::matchers::{method, path, path_regex};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(COMPOSED))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path_regex("/compose$"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "etag": "CJ6k" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let failed = emulated(&server.uri())
            .assemble(
                "bundles/app.tar.gz",
                stored_parts(&[8; 33]),
                bundle_bytes(vec![
                    Ok(Bytes::from_static(b"the parts")),
                    Err(eyre::eyre!("the bundle is not that hash")),
                ]),
                "application/gzip",
            )
            .await
            .expect_err("failed");
        assert_eq!(failed.to_string(), "the bundle is not that hash");
    }

    #[test]
    fn a_tenant_bucket_composes_in_its_own_bucket() {
        let platform = emulated("http://127.0.0.1:1");
        let tenant = platform.tenant("acme-documents").expect("tenant");
        assert_eq!(
            tenant.composer.store.to_string(),
            tenant.primary.to_string()
        );
        assert_ne!(
            tenant.composer.store.to_string(),
            platform.primary.to_string()
        );
        assert_eq!(tenant.composer.bucket, "acme-documents");
    }
}
