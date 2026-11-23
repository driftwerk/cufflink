use crate::common::{self, Keys};
use crate::copied;
use crate::parts::{CopyPart, Parted};
use crate::{
    is_bucket_name, CannedAcl, Credential, ObjectMeta, ObjectReader, ObjectStorage, StoredPart,
    UploadOptions, Uploaded,
};
use async_trait::async_trait;
use bytes::Bytes;
use eyre::{Result, WrapErr};
use futures_util::stream::BoxStream;
use object_store::aws::{AmazonS3, AmazonS3Builder, AwsAuthorizer};
use object_store::client::{HttpClient, HttpRequestBody};
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::MultipartId;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use std::sync::Arc;
use zeroize::Zeroizing;

const BACKEND: &str = "s3";

const ENV_ENDPOINT: &str = "S3_ENDPOINT";
const ENV_REGION: &str = "S3_REGION";
const ENV_ACCESS_KEY: &str = "S3_ACCESS_KEY";
const ENV_SECRET_KEY: &str = "S3_SECRET_KEY";
const ENV_BUCKET: &str = "S3_BUCKET";

const ACL_HEADER: http::HeaderName = http::HeaderName::from_static("x-amz-acl");

const COPY_SOURCE_HEADER: http::HeaderName = http::HeaderName::from_static("x-amz-copy-source");

const STRICT: AsciiSet = NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

const STRICT_PATH: AsciiSet = STRICT.remove(b'/');

/// The sizes S3 copies a part of: every part but the last at least 5 MiB,
/// none above 5 GiB.
const COPIED_PART_SIZES: std::ops::RangeInclusive<u64> = 5 * 1024 * 1024..=5 * 1024 * 1024 * 1024;

#[derive(Clone)]
struct Connection {
    endpoint: String,
    region: String,
    access_key: String,
    secret_key: Zeroizing<String>,
    allow_http: bool,
    client: HttpClient,
}

impl Connection {
    fn store(&self, bucket: &str, acl: Option<CannedAcl>) -> Result<Arc<AmazonS3>> {
        let options = common::client_options().with_allow_http(self.allow_http);
        let options = match acl {
            Some(acl) => options.with_default_headers(http::HeaderMap::from_iter([(
                ACL_HEADER,
                http::HeaderValue::from_static(acl.as_str()),
            )])),
            None => options,
        };
        let store = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_region(&self.region)
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(self.secret_key.as_str())
            .with_bucket_name(bucket)
            .with_client_options(options)
            .with_retry(common::retry_config())
            .build()
            .wrap_err("Failed to build S3 client")?;
        Ok(Arc::new(store))
    }

    fn writer(
        &self,
        bucket: &str,
        acl: Option<CannedAcl>,
        stall: common::Stall,
    ) -> Result<Arc<Parted<AmazonS3>>> {
        Ok(Arc::new(Parted::new(
            self.store(bucket, acl)?,
            BACKEND,
            stall,
        )))
    }

    fn copier(&self, store: &Arc<AmazonS3>, bucket: &str) -> Arc<dyn CopyPart> {
        Arc::new(PartCopier {
            store: Arc::clone(store),
            client: self.client.clone(),
            bucket_url: format!("{}/{bucket}", self.endpoint.trim_end_matches('/')),
            bucket: bucket.to_string(),
            region: self.region.clone(),
        })
    }
}

/// Copies an object of the bucket into a part of one of its multipart
/// uploads with UploadPartCopy, signed as the store signs its own requests.
#[derive(Debug)]
struct PartCopier {
    store: Arc<AmazonS3>,
    client: HttpClient,
    bucket_url: String,
    bucket: String,
    region: String,
}

#[async_trait]
impl CopyPart for PartCopier {
    async fn copy_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        from: &Path,
    ) -> object_store::Result<PartId> {
        let credential = self.store.credentials().get_credential().await?;
        let invalid = |source: http::Error| object_store::Error::Generic {
            store: BACKEND,
            source: Box::new(source),
        };
        let mut request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(format!(
                "{}/{}?partNumber={}&uploadId={}",
                self.bucket_url,
                utf8_percent_encode(path.as_ref(), &STRICT_PATH),
                part_idx + 1,
                utf8_percent_encode(id, &STRICT)
            ))
            .header(
                COPY_SOURCE_HEADER,
                format!(
                    "{}/{}",
                    self.bucket,
                    utf8_percent_encode(from.as_ref(), &STRICT_PATH)
                ),
            )
            .body(HttpRequestBody::empty())
            .map_err(invalid)?;
        AwsAuthorizer::new(&credential, "s3", &self.region).try_authorize(&mut request, None)?;
        let answer = copied::sent(&self.client, BACKEND, path.as_ref(), request).await?;
        copied_part(path, &String::from_utf8_lossy(&answer))
    }
}

/// The part an UploadPartCopy answer names, by its ETag as the store
/// quotes it. S3 can answer a copy that failed after it began with `200`
/// and an `<Error>` body.
fn copied_part(path: &Path, answer: &str) -> object_store::Result<PartId> {
    let tagged = |tag: &str| {
        answer
            .split_once(&format!("<{tag}>"))
            .and_then(|(_, rest)| rest.split_once(&format!("</{tag}>")))
            .map(|(value, _)| value.replace("&quot;", "\"").replace("&#34;", "\""))
    };
    match (answer.contains("<Error>"), tagged("ETag")) {
        (false, Some(content_id)) => Ok(PartId { content_id }),
        _ => Err(object_store::Error::Generic {
            store: BACKEND,
            source: format!("the copy of a part of {path} was not stored: {answer}").into(),
        }),
    }
}

/// Whether S3 can copy `parts`, in order, into the parts of one upload.
fn copyable(parts: &[StoredPart]) -> bool {
    parts.split_last().is_some_and(|(last, rest)| {
        parts.len() <= common::MAX_PARTS
            && rest
                .iter()
                .all(|part| COPIED_PART_SIZES.contains(&part.size))
            && (1..=*COPIED_PART_SIZES.end()).contains(&last.size)
    })
}

#[derive(Clone)]
pub(crate) struct S3Backend {
    primary: Arc<AmazonS3>,
    writes: Arc<Parted<AmazonS3>>,
    copier: Arc<dyn CopyPart>,
    connection: Connection,
    bucket: String,
    keys: Keys,
    stall: common::Stall,
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
        let allow_http = endpoint.starts_with("http://");
        let connection = Connection {
            region: vars(ENV_REGION).unwrap_or_else(|| "us-east-1".to_string()),
            access_key: env_required(&vars, ENV_ACCESS_KEY)?,
            secret_key: Zeroizing::new(env_required(&vars, ENV_SECRET_KEY)?),
            allow_http,
            client: copied::client(&common::client_options().with_allow_http(allow_http))
                .wrap_err("Failed to build S3 client")?,
            endpoint,
        };
        let bucket = vars(ENV_BUCKET).unwrap_or_else(|| "cufflink".to_string());
        let backend = Self::new(
            connection,
            bucket,
            Keys::Encoded,
            common::Stall::configured(),
        )?;

        tracing::info!(
            backend = BACKEND,
            endpoint = %backend.connection.endpoint,
            bucket = %backend.bucket,
            region = %backend.connection.region,
            retry_timeout_secs = common::retry_timeout().as_secs(),
            request_timeout_secs = common::request_timeout().as_secs(),
            "Object storage configured",
        );
        common::warn_unless_timed_out_requests_retry(BACKEND);

        Ok(backend)
    }

    fn new(
        connection: Connection,
        bucket: String,
        keys: Keys,
        stall: common::Stall,
    ) -> Result<Self> {
        let primary = connection.store(&bucket, None)?;
        Ok(Self {
            writes: Arc::new(Parted::new(Arc::clone(&primary), BACKEND, stall)),
            copier: connection.copier(&primary, &bucket),
            primary,
            connection,
            bucket,
            keys,
            stall,
        })
    }

    fn store_for(&self, bucket: &str, acl: Option<CannedAcl>) -> Result<Arc<AmazonS3>> {
        common::admit_bucket(&self.bucket, self.keys, bucket)?;
        match acl {
            None if bucket == self.bucket => Ok(Arc::clone(&self.primary)),
            acl => self.connection.store(bucket, acl),
        }
    }

    fn writer_for(&self, bucket: &str, acl: Option<CannedAcl>) -> Result<Arc<Parted<AmazonS3>>> {
        common::admit_bucket(&self.bucket, self.keys, bucket)?;
        match acl {
            None if bucket == self.bucket => Ok(Arc::clone(&self.writes)),
            acl => self.connection.writer(bucket, acl, self.stall),
        }
    }
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
        let store = self.writer_for(bucket, options.acl)?;
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
        let store = self.writer_for(&self.bucket, None)?;
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
        if !copyable(&parts) {
            return self.upload_stream(key, data, content_type).await;
        }
        common::put_copied(
            self.writes.as_ref(),
            &self.copier,
            BACKEND,
            self.keys,
            key,
            parts,
            data,
            &UploadOptions::content_type(content_type),
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
        let store = self.store_for(bucket, None)?;
        common::get_bytes(store, BACKEND, self.keys, Some(bucket), key, self.stall).await
    }

    async fn open_read(&self, bucket: &str, key: &str) -> Result<Option<ObjectReader>> {
        let store = self.store_for(bucket, None)?;
        common::open_read(store, BACKEND, self.keys, bucket, key, self.stall).await
    }

    async fn head(&self, key: &str) -> Result<u64> {
        common::head_size(self.primary.as_ref(), BACKEND, self.keys, key).await
    }

    async fn head_in_bucket(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let store = self.store_for(bucket, None)?;
        common::head_meta(store.as_ref(), BACKEND, self.keys, bucket, key).await
    }

    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<()> {
        common::copy_object(self.primary.as_ref(), BACKEND, self.keys, src_key, dst_key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        common::delete_object(self.primary.as_ref(), BACKEND, self.keys, key).await
    }

    async fn delete_from_bucket(&self, bucket: &str, key: &str) -> Result<()> {
        let store = self.store_for(bucket, None)?;
        common::delete_object_idempotent(store.as_ref(), BACKEND, self.keys, key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        common::delete_prefix(self.primary.as_ref(), BACKEND, prefix).await
    }

    async fn presign_upload(
        &self,
        key: &str,
        content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        self.presign_upload_to_bucket(&self.bucket, key, content_type, expires_in_secs)
            .await
    }

    async fn presign_upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        _content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        let store = self.store_for(bucket, None)?;
        common::presign_url(
            store.as_ref(),
            BACKEND,
            self.keys,
            http::Method::PUT,
            key,
            expires_in_secs,
        )
        .await
    }

    async fn presign_download(&self, key: &str, expires_in_secs: u64) -> Result<String> {
        self.presign_download_to_bucket(&self.bucket, key, expires_in_secs)
            .await
    }

    async fn presign_download_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        let store = self.store_for(bucket, None)?;
        common::presign_url(
            store.as_ref(),
            BACKEND,
            self.keys,
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

    fn tenant_bucket(
        &self,
        bucket: &str,
        credential: Option<&Credential>,
    ) -> Result<Arc<dyn ObjectStorage>> {
        eyre::ensure!(is_bucket_name(bucket), "{bucket:?} is not a bucket name");
        let connection = match credential {
            Some(credential) => Connection {
                access_key: credential.access_key_id.clone(),
                secret_key: credential.secret_access_key.clone(),
                ..self.connection.clone()
            },
            None => self.connection.clone(),
        };
        Ok(Arc::new(Self::new(
            connection,
            bucket.to_string(),
            Keys::Exact,
            self.stall,
        )?))
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
                assert_eq!(backend.connection.region, "us-east-1");
                assert_eq!(backend.bucket, "cufflink");
                assert!(
                    backend.connection.allow_http,
                    "http:// endpoint must allow_http"
                );
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
                assert!(!backend.connection.allow_http);
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
                let same = backend
                    .store_for("cufflink", None)
                    .expect("store_for primary");
                assert!(
                    Arc::ptr_eq(&same, &backend.primary),
                    "same bucket must short-circuit to shared Arc"
                );
                let other = backend
                    .store_for("other-bucket", None)
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
        backend_with(endpoint, &[])
    }

    fn backend_with(endpoint: &str, timeouts: &[(&str, Option<&str>)]) -> S3Backend {
        built_with(endpoint, timeouts, |backend| backend)
    }

    fn built_with<T>(
        endpoint: &str,
        timeouts: &[(&str, Option<&str>)],
        build: impl FnOnce(S3Backend) -> T,
    ) -> T {
        let mut built = None;
        let vars = [
            (ENV_ENDPOINT, Some(endpoint)),
            (ENV_REGION, Some("us-east-1")),
            (ENV_ACCESS_KEY, Some("ak")),
            (ENV_SECRET_KEY, Some("sk")),
            (ENV_BUCKET, Some("cufflink")),
        ];
        with_env(&[&vars[..], timeouts].concat(), || {
            built = Some(build(S3Backend::from_vars(var).expect("should build")))
        });
        built.expect("built")
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
                    acl: None,
                },
            )
            .await
            .expect("uploaded");
        assert_eq!(
            uploaded,
            Uploaded {
                size: 8,
                etag: Some("\"9b2cf535\"".into()),
                completion: None,
                parts: crate::PartTimes::default(),
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

    const EXACT: &str = "leads/7/Café façade #2 50%+off?.pdf";
    const EXACT_ON_THE_WIRE: &str =
        "/example-documents/leads/7/Caf%C3%A9%20fa%C3%A7ade%20%232%2050%25%2Boff%3F.pdf";

    fn signed_by(request: &wiremock::Request) -> String {
        request
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|auth| auth.split("Credential=").nth(1))
            .and_then(|credential| credential.split('/').next())
            .unwrap_or_default()
            .to_string()
    }

    fn tenant_bucket(server: &str, credential: Option<&Credential>) -> Arc<dyn ObjectStorage> {
        backend_at(server)
            .tenant_bucket("example-documents", credential)
            .expect("tenant bucket")
    }

    #[tokio::test]
    async fn a_tenant_bucket_sends_its_keys_exactly_and_signs_with_its_credential() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path(EXACT_ON_THE_WIRE))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"e1\""))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(EXACT_ON_THE_WIRE))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "4")
                    .insert_header("last-modified", "Mon, 28 Sep 2026 10:00:00 GMT")
                    .set_body_bytes(b"%PDF".to_vec()),
            )
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path(EXACT_ON_THE_WIRE))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "4")
                    .insert_header("last-modified", "Mon, 28 Sep 2026 10:00:00 GMT"),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/example-documents"))
            .and(wiremock::matchers::query_param("delete", ""))
            .and(wiremock::matchers::body_string_contains(format!(
                "<Key>{EXACT}</Key>"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "<DeleteResult><Deleted><Key>{EXACT}</Key></Deleted></DeleteResult>"
            )))
            .expect(1)
            .mount(&server)
            .await;
        let credential = Credential::new("AKDOCUMENTS", "documents-secret");
        let bucket = tenant_bucket(&server.uri(), Some(&credential));
        bucket
            .upload_to_bucket_with(
                "example-documents",
                EXACT,
                b"%PDF".to_vec(),
                &UploadOptions::content_type("application/pdf"),
            )
            .await
            .expect("uploaded");
        assert_eq!(
            bucket
                .download_from_bucket("example-documents", EXACT)
                .await
                .expect("downloaded"),
            b"%PDF"
        );
        assert_eq!(
            bucket
                .head_in_bucket("example-documents", EXACT)
                .await
                .expect("head")
                .map(|meta| meta.size),
            Some(4)
        );
        bucket
            .delete_from_bucket("example-documents", EXACT)
            .await
            .expect("deleted");
        let presigned = bucket
            .presign_download_to_bucket("example-documents", EXACT, 60)
            .await
            .expect("presigned");
        assert!(presigned.contains(EXACT_ON_THE_WIRE), "{presigned}");
        assert!(presigned.contains("AKDOCUMENTS"), "{presigned}");
        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(requests.len(), 4);
        assert!(
            requests
                .iter()
                .all(|request| signed_by(request) == "AKDOCUMENTS"),
            "every request is signed with the bucket's credential"
        );

        let platform = tenant_bucket(&server.uri(), None);
        platform
            .head_in_bucket("example-documents", EXACT)
            .await
            .expect("head");
        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(requests.last().map(signed_by).as_deref(), Some("ak"));
    }

    #[tokio::test]
    async fn the_platforms_buckets_keep_their_encoded_keys() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/receipts/Caf%25C3%25A9%20%25232.pdf"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"e\""))
            .expect(1)
            .mount(&server)
            .await;
        backend_at(&server.uri())
            .upload_to_bucket("receipts", "Café #2.pdf", b"x".to_vec(), "text/plain")
            .await
            .expect("uploaded under the encoded name");
    }

    #[tokio::test]
    async fn a_key_that_cannot_be_stored_exactly_is_refused_before_any_request() {
        let server = wiremock::MockServer::start().await;
        let bucket = tenant_bucket(&server.uri(), None);
        for key in [
            "",
            "/leads/a",
            "leads/",
            "leads//a",
            "leads/./a",
            "a/../b",
            "a\nb",
        ] {
            let refused = bucket
                .upload_to_bucket("example-documents", key, b"x".to_vec(), "text/plain")
                .await
                .expect_err("refused");
            assert!(refused.to_string().contains("exactly"), "{key}: {refused}");
            assert!(bucket
                .download_from_bucket("example-documents", key)
                .await
                .is_err());
            assert!(bucket
                .delete_from_bucket("example-documents", key)
                .await
                .is_err());
        }
        assert!(server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty());
    }

    #[tokio::test]
    async fn a_canned_acl_is_a_signed_header_of_the_upload_that_asked_for_it() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/example-documents/logos/a.png"))
            .and(header("x-amz-acl", "public-read"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"a\""))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/example-documents/logos/b.png"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"b\""))
            .expect(1)
            .mount(&server)
            .await;
        let bucket = tenant_bucket(&server.uri(), None);
        let public = UploadOptions {
            acl: Some(CannedAcl::PublicRead),
            ..UploadOptions::content_type("image/png")
        };
        bucket
            .upload_to_bucket_with("example-documents", "logos/a.png", b"png".to_vec(), &public)
            .await
            .expect("public upload");
        bucket
            .upload_to_bucket_with(
                "example-documents",
                "logos/b.png",
                b"png".to_vec(),
                &UploadOptions::content_type("image/png"),
            )
            .await
            .expect("private upload");
        let requests = server.received_requests().await.unwrap_or_default();
        let acl_of = |path: &str| {
            requests
                .iter()
                .find(|request| request.url.path() == path)
                .map(|request| {
                    let signed = request
                        .headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .is_some_and(|auth| auth.contains("x-amz-acl"));
                    (
                        request
                            .headers
                            .get("x-amz-acl")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string),
                        signed,
                    )
                })
        };
        assert_eq!(
            acl_of("/example-documents/logos/a.png"),
            Some((Some("public-read".into()), true))
        );
        assert_eq!(
            acl_of("/example-documents/logos/b.png"),
            Some((None, false))
        );
    }

    const BUNDLE: &str = "/example-documents/bundles/app.tar.gz";

    async fn initiates_upload(server: &wiremock::MockServer) {
        initiates_upload_with(server, Some("public-read")).await;
    }

    async fn initiates_upload_with(server: &wiremock::MockServer, acl: Option<&'static str>) {
        use wiremock::matchers::{header, method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        let initiate = Mock::given(method("POST"))
            .and(path(BUNDLE))
            .and(query_param("uploads", ""))
            .and(header("content-type", "application/gzip"));
        match acl {
            Some(acl) => initiate.and(header("x-amz-acl", acl)),
            None => initiate,
        }
        .respond_with(ResponseTemplate::new(200).set_body_string(
                "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>",
            ))
            .expect(1)
            .mount(server)
            .await;
    }

    fn public_bundle() -> UploadOptions {
        UploadOptions {
            acl: Some(CannedAcl::PublicRead),
            ..UploadOptions::content_type("application/gzip")
        }
    }

    fn large_payload() -> Vec<u8> {
        (0..40 * 1024 * 1024 + 123)
            .map(|i: usize| (i % 251) as u8)
            .collect()
    }

    #[tokio::test]
    async fn a_large_upload_goes_up_in_parts_with_how_the_object_is_served() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_upload(&server).await;
        Mock::given(method("PUT"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"part\""))
            .expect(6)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CompleteMultipartUploadResult><ETag>\"whole-6\"</ETag></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        let data = large_payload();
        let uploaded = tenant_bucket(&server.uri(), None)
            .upload_to_bucket_with(
                "example-documents",
                "bundles/app.tar.gz",
                data.clone(),
                &public_bundle(),
            )
            .await
            .expect("uploaded");
        assert_eq!(
            (uploaded.size, uploaded.etag.as_deref()),
            (data.len() as u64, Some("\"whole-6\""))
        );
        assert!(uploaded.completion.is_some());
        let requests = server.received_requests().await.unwrap_or_default();
        let mut parts: Vec<(usize, &[u8])> = requests
            .iter()
            .filter(|request| request.method.as_str() == "PUT")
            .filter_map(|request| {
                request
                    .url
                    .query_pairs()
                    .find(|(name, _)| name == "partNumber")
                    .and_then(|(_, number)| number.parse().ok())
                    .map(|number| (number, request.body.as_slice()))
            })
            .collect();
        parts.sort_by_key(|(number, _)| *number);
        assert_eq!(
            parts.iter().map(|(number, _)| *number).collect::<Vec<_>>(),
            [1, 2, 3, 4, 5, 6]
        );
        assert!(parts[..5]
            .iter()
            .all(|(_, body)| body.len() == 8 * 1024 * 1024));
        assert_eq!(
            parts
                .iter()
                .flat_map(|(_, body)| body.iter().copied())
                .collect::<Vec<_>>(),
            data
        );
    }

    #[tokio::test]
    async fn a_failed_part_aborts_the_upload_and_names_the_key() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_upload(&server).await;
        Mock::given(method("PUT"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let refused = tenant_bucket(&server.uri(), None)
            .upload_to_bucket_with(
                "example-documents",
                "bundles/app.tar.gz",
                large_payload(),
                &public_bundle(),
            )
            .await
            .expect_err("refused");
        assert_eq!(
            refused.to_string(),
            "s3 upload failed for bundles/app.tar.gz"
        );
        assert!(
            requests_until(&server, |request| request.method.as_str() == "DELETE").await,
            "the failed upload is aborted"
        );
    }

    fn cut_at_a_second(server: &str, timeouts: &[(&str, Option<&str>)]) -> Arc<dyn ObjectStorage> {
        built_with(
            server,
            &[&[(common::ENV_STALL_TIMEOUT_SECS, Some("1"))][..], timeouts].concat(),
            |backend| {
                backend
                    .tenant_bucket("example-documents", None)
                    .expect("tenant bucket")
            },
        )
    }

    async fn parts_answered(server: &wiremock::MockServer, stalled: u64) {
        use std::time::Duration;
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        Mock::given(method("PUT"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .and(query_param("partNumber", "1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"stalled\"")
                    .set_delay(Duration::from_secs(10)),
            )
            .up_to_n_times(stalled)
            .with_priority(1)
            .mount(server)
            .await;
        Mock::given(method("PUT"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"part\""))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn a_part_whose_answer_stalls_is_cut_at_the_stall_timeout_and_sent_again() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_upload(&server).await;
        parts_answered(&server, 1).await;
        Mock::given(method("POST"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CompleteMultipartUploadResult><ETag>\"whole-6\"</ETag></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        let data = large_payload();
        let uploaded = cut_at_a_second(&server.uri(), &[])
            .upload_to_bucket_with(
                "example-documents",
                "bundles/app.tar.gz",
                data.clone(),
                &public_bundle(),
            )
            .await
            .expect("uploaded");
        assert_eq!(
            (uploaded.size, uploaded.etag.as_deref()),
            (data.len() as u64, Some("\"whole-6\""))
        );
        assert_eq!(common::test_wire::first_part_sent(&server).await, 2);
    }

    #[tokio::test]
    async fn a_part_that_stalls_on_every_attempt_fails_its_upload_and_aborts_it() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_upload_with(&server, None).await;
        parts_answered(&server, u64::MAX).await;
        Mock::given(method("DELETE"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let refused = cut_at_a_second(
            &server.uri(),
            &[
                (common::ENV_REQUEST_TIMEOUT_SECS, Some("2")),
                (common::ENV_RETRY_TIMEOUT_SECS, Some("1")),
            ],
        )
        .upload_to_bucket_with(
            "example-documents",
            "bundles/app.tar.gz",
            large_payload(),
            &UploadOptions::content_type("application/gzip"),
        )
        .await
        .expect_err("refused");
        assert_eq!(
            refused.to_string(),
            "s3 upload failed for bundles/app.tar.gz"
        );
        assert!(
            requests_until(&server, |request| request.method.as_str() == "DELETE").await,
            "the failed upload is aborted"
        );
        assert_eq!(
            common::test_wire::first_part_sent(&server).await,
            3,
            "two attempts cut at the stall timeout, then one bounded by the request timeout"
        );
    }

    #[tokio::test]
    async fn a_part_answered_with_a_server_error_is_retried() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_upload(&server).await;
        Mock::given(method("PUT"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"part\""))
            .expect(6)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CompleteMultipartUploadResult><ETag>\"whole-6\"</ETag></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        tenant_bucket(&server.uri(), None)
            .upload_to_bucket_with(
                "example-documents",
                "bundles/app.tar.gz",
                large_payload(),
                &public_bundle(),
            )
            .await
            .expect("uploaded after the retry");
    }

    async fn requests_until(
        server: &wiremock::MockServer,
        seen: impl Fn(&wiremock::Request) -> bool,
    ) -> bool {
        for _ in 0..200 {
            if server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .any(&seen)
            {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
    }

    #[tokio::test]
    async fn a_dropped_upload_is_aborted() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_upload(&server).await;
        Mock::given(method("PUT"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"part\"")
                    .set_delay(std::time::Duration::from_secs(10)),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(BUNDLE))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let bucket = tenant_bucket(&server.uri(), None);
        let data = large_payload();
        let options = public_bundle();
        let upload =
            bucket.upload_to_bucket_with("example-documents", "bundles/app.tar.gz", data, &options);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(300), upload)
                .await
                .is_err(),
            "the upload is still sending its parts"
        );
        assert!(
            requests_until(&server, |request| request.method.as_str() == "DELETE").await,
            "the dropped upload is aborted"
        );
    }

    #[tokio::test]
    async fn a_request_is_bounded_by_the_configured_request_timeout() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/cufflink/slow.txt"))
            .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(10)))
            .mount(&server)
            .await;
        let backend = backend_with(
            &server.uri(),
            &[
                (common::ENV_REQUEST_TIMEOUT_SECS, Some("1")),
                (common::ENV_RETRY_TIMEOUT_SECS, Some("1")),
            ],
        );
        let started = std::time::Instant::now();
        let refused = backend
            .upload("slow.txt", b"x".to_vec(), "text/plain")
            .await
            .expect_err("timed out");
        assert_eq!(refused.to_string(), "s3 upload failed for slow.txt");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_credential_never_shows_its_secret() {
        let credential = Credential::new("AKDOCUMENTS", "documents-secret");
        let shown = format!("{credential:?}");
        assert!(shown.contains("AKDOCUMENTS"), "{shown}");
        assert!(!shown.contains("documents-secret"), "{shown}");
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

    const ASSEMBLED: &str = "/cufflink/bundles/app.tar.gz";

    const MIB: u64 = 1024 * 1024;

    fn stored_parts(sizes: &[u64]) -> Vec<StoredPart> {
        sizes
            .iter()
            .enumerate()
            .map(|(index, size)| StoredPart {
                key: format!("parts/{index}"),
                size: *size,
            })
            .collect()
    }

    fn bundle_bytes(chunks: Vec<Result<Bytes>>) -> BoxStream<'static, Result<Bytes>> {
        Box::pin(futures_util::stream::iter(chunks))
    }

    async fn initiates_assembly(server: &wiremock::MockServer) {
        use wiremock::matchers::{header, method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        Mock::given(method("POST"))
            .and(path(ASSEMBLED))
            .and(query_param("uploads", ""))
            .and(header("content-type", "application/gzip"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>",
            ))
            .expect(1)
            .mount(server)
            .await;
    }

    async fn aborts_assembly(server: &wiremock::MockServer) {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        Mock::given(method("DELETE"))
            .and(path(ASSEMBLED))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(server)
            .await;
    }

    async fn never_completes(server: &wiremock::MockServer) {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, ResponseTemplate};

        Mock::given(method("POST"))
            .and(path(ASSEMBLED))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn an_assembly_copies_its_parts_inside_the_store_and_completes_them_in_order() {
        use wiremock::matchers::{header, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_assembly(&server).await;
        for (number, quote) in [(1, "&quot;"), (2, "&#34;"), (3, "&quot;")] {
            Mock::given(method("PUT"))
                .and(path(ASSEMBLED))
                .and(query_param("uploadId", "upload-1"))
                .and(query_param("partNumber", number.to_string()))
                .and(header(
                    "x-amz-copy-source",
                    format!("cufflink/parts/{}", number - 1),
                ))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    "<CopyPartResult><ETag>{quote}copied-{number}{quote}</ETag></CopyPartResult>"
                )))
                .expect(1)
                .mount(&server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path(ASSEMBLED))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CompleteMultipartUploadResult><ETag>\"whole-3\"</ETag></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        let uploaded = backend_at(&server.uri())
            .assemble(
                "bundles/app.tar.gz",
                stored_parts(&[8 * MIB, 5 * MIB, 3]),
                bundle_bytes(vec![Ok(Bytes::from_static(b"the parts"))]),
                "application/gzip",
            )
            .await
            .expect("assembled");
        assert_eq!(
            (uploaded.size, uploaded.etag.as_deref()),
            (13 * MIB + 3, Some("\"whole-3\""))
        );
        assert!(uploaded.completion.is_some());
        assert!((1..=3).contains(&uploaded.parts.in_flight));
        let requests = server.received_requests().await.unwrap_or_default();
        let copies = requests
            .iter()
            .filter(|request| request.method.as_str() == "PUT")
            .collect::<Vec<_>>();
        assert_eq!(copies.len(), 3);
        assert!(copies
            .iter()
            .all(|copy| copy.body.is_empty() && signed_by(copy) == "ak"));
        let completion = requests
            .iter()
            .find(|request| {
                request.method.as_str() == "POST"
                    && request
                        .url
                        .query()
                        .is_some_and(|query| query.contains("uploadId"))
            })
            .map(|request| String::from_utf8_lossy(&request.body).into_owned())
            .expect("completed");
        let tagged = ["\"copied-1\"", "\"copied-2\"", "\"copied-3\""]
            .map(|etag| completion.find(etag).expect("listed"));
        assert!(tagged.is_sorted(), "{completion}");
    }

    #[tokio::test]
    async fn a_copy_answered_with_an_error_body_aborts_the_assembly() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_assembly(&server).await;
        Mock::given(method("PUT"))
            .and(path(ASSEMBLED))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<Error><Code>InternalError</Code><Message>try again</Message></Error>",
            ))
            .mount(&server)
            .await;
        aborts_assembly(&server).await;
        never_completes(&server).await;
        let failed = backend_at(&server.uri())
            .assemble(
                "bundles/app.tar.gz",
                stored_parts(&[8 * MIB, 3]),
                bundle_bytes(vec![Ok(Bytes::from_static(b"the parts"))]),
                "application/gzip",
            )
            .await
            .expect_err("failed");
        assert_eq!(
            failed.to_string(),
            "s3 upload failed for bundles/app.tar.gz"
        );
        assert!(
            format!("{failed:?}").contains("InternalError"),
            "{failed:?}"
        );
        assert!(
            requests_until(&server, |request| request.method.as_str() == "DELETE").await,
            "the failed assembly is aborted"
        );
    }

    #[tokio::test]
    async fn an_assembly_whose_bytes_fail_their_check_is_aborted_and_never_completed() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        initiates_assembly(&server).await;
        Mock::given(method("PUT"))
            .and(path(ASSEMBLED))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<CopyPartResult><ETag>&quot;copied&quot;</ETag></CopyPartResult>",
            ))
            .mount(&server)
            .await;
        aborts_assembly(&server).await;
        never_completes(&server).await;
        let failed = backend_at(&server.uri())
            .assemble(
                "bundles/app.tar.gz",
                stored_parts(&[8 * MIB, 3]),
                bundle_bytes(vec![
                    Ok(Bytes::from_static(b"the parts")),
                    Err(eyre::eyre!("the bundle is not that hash")),
                ]),
                "application/gzip",
            )
            .await
            .expect_err("failed");
        assert_eq!(failed.to_string(), "the bundle is not that hash");
        assert!(
            requests_until(&server, |request| request.method.as_str() == "DELETE").await,
            "the unchecked assembly is aborted"
        );
    }

    #[tokio::test]
    async fn an_assembly_with_a_part_too_small_to_copy_before_its_last_streams_its_bytes() {
        use wiremock::matchers::{body_bytes, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path(ASSEMBLED))
            .and(body_bytes(b"small parts".to_vec()))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"whole\""))
            .expect(1)
            .mount(&server)
            .await;
        let uploaded = backend_at(&server.uri())
            .assemble(
                "bundles/app.tar.gz",
                stored_parts(&[5 * MIB - 1, 3]),
                bundle_bytes(vec![
                    Ok(Bytes::from_static(b"small ")),
                    Ok(Bytes::from_static(b"parts")),
                ]),
                "application/gzip",
            )
            .await
            .expect("streamed");
        assert_eq!(uploaded.etag.as_deref(), Some("\"whole\""));
        let requests = server.received_requests().await.unwrap_or_default();
        assert!(requests
            .iter()
            .all(|request| !request.headers.contains_key("x-amz-copy-source")));
    }

    #[test]
    fn only_parts_of_at_least_five_mib_but_the_last_are_copied() {
        assert!(copyable(&stored_parts(&[5 * MIB, 8 * MIB, 1])));
        assert!(copyable(&stored_parts(&[1])));
        assert!(!copyable(&stored_parts(&[])));
        assert!(!copyable(&stored_parts(&[5 * MIB - 1, 8 * MIB])));
        assert!(!copyable(&stored_parts(&[8 * MIB, 0])));
        assert!(!copyable(&stored_parts(&[5 * 1024 * MIB + 1])));
        assert!(!copyable(&stored_parts(&vec![
            8 * MIB;
            common::MAX_PARTS + 1
        ])));
    }

    #[test]
    fn a_copied_part_is_named_by_its_quoted_etag_and_an_error_body_fails() {
        let path = Path::from("bundles/app.tar.gz");
        for answer in [
            "<CopyPartResult><ETag>&quot;abc&quot;</ETag></CopyPartResult>",
            "<CopyPartResult><LastModified>x</LastModified><ETag>&#34;abc&#34;</ETag></CopyPartResult>",
        ] {
            assert_eq!(
                copied_part(&path, answer).expect("copied").content_id,
                "\"abc\""
            );
        }
        for answer in [
            "<Error><Code>InternalError</Code></Error>",
            "<CopyPartResult></CopyPartResult>",
            "",
        ] {
            assert!(copied_part(&path, answer).is_err(), "{answer}");
        }
    }
}
