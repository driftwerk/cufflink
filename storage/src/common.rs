use crate::{ObjectMeta, ObjectReader, UploadOptions, Uploaded};
use bytes::Bytes;
use eyre::{Result, WrapErr};
use futures_util::{stream, StreamExt, TryStreamExt};
use object_store::{
    path::Path as OsPath, Attribute, AttributeValue, Attributes, ClientOptions, GetOptions,
    MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use std::time::Duration;

/// How a key becomes the object's name: as `object_store` encodes it, or
/// exactly as given ([`crate::is_exact_key`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Keys {
    #[default]
    Encoded,
    Exact,
}

impl Keys {
    pub(crate) fn path(self, key: &str) -> Result<OsPath> {
        match self {
            Keys::Encoded => Ok(OsPath::from(key)),
            Keys::Exact if crate::is_exact_key(key) => {
                OsPath::parse(key).wrap_err_with(|| format!("the key {key:?} is not exact"))
            }
            Keys::Exact => Err(eyre::eyre!("the key {key:?} cannot be stored exactly")),
        }
    }
}

/// Whether a store whose own bucket is `own` may serve `bucket`: a store
/// built for a tenant's bucket serves that bucket alone, and any other is a
/// valid bucket name ([`crate::is_bucket_name`]).
pub(crate) fn admit_bucket(own: &str, keys: Keys, bucket: &str) -> Result<()> {
    if bucket == own {
        return Ok(());
    }
    eyre::ensure!(
        keys == Keys::Encoded,
        "this store serves bucket {own} alone, not {bucket:?}"
    );
    eyre::ensure!(
        crate::is_bucket_name(bucket),
        "{bucket:?} is not a bucket name"
    );
    Ok(())
}

pub(crate) fn no_acl(backend: &'static str, options: &UploadOptions) -> Result<()> {
    match options.acl {
        Some(acl) => Err(eyre::eyre!(
            "the {backend} backend stores no canned ACL ({})",
            acl.as_str()
        )),
        None => Ok(()),
    }
}

pub(crate) fn attributes(options: &UploadOptions) -> Attributes {
    [
        (Attribute::ContentType, Some(&options.content_type)),
        (
            Attribute::ContentDisposition,
            options.content_disposition.as_ref(),
        ),
        (Attribute::CacheControl, options.cache_control.as_ref()),
    ]
    .into_iter()
    .filter_map(|(attribute, value)| Some((attribute, AttributeValue::from(value?.clone()))))
    .collect()
}

const DEFAULT_RETRY_TIMEOUT_SECS: u64 = 20;

pub(crate) const ENV_RETRY_TIMEOUT_SECS: &str = "OBJECT_STORE_RETRY_TIMEOUT_SECS";

const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

pub(crate) const ENV_REQUEST_TIMEOUT_SECS: &str = "OBJECT_STORE_REQUEST_TIMEOUT_SECS";

fn env_secs(name: &'static str, default: u64) -> Duration {
    let secs = std::env::var(name)
        .ok()
        .map_or(default, |raw| match raw.trim().parse::<u64>() {
            Ok(secs) if secs > 0 => secs,
            _ => {
                tracing::warn!(
                    variable = name,
                    value = %raw,
                    default,
                    "Ignoring a timeout that is not a positive number of seconds"
                );
                default
            }
        });
    Duration::from_secs(secs)
}

pub(crate) fn retry_timeout() -> Duration {
    env_secs(ENV_RETRY_TIMEOUT_SECS, DEFAULT_RETRY_TIMEOUT_SECS)
}

pub(crate) fn request_timeout() -> Duration {
    env_secs(ENV_REQUEST_TIMEOUT_SECS, DEFAULT_REQUEST_TIMEOUT_SECS)
}

pub(crate) fn client_options() -> ClientOptions {
    ClientOptions::new().with_timeout(request_timeout())
}

pub(crate) fn warn_unless_timed_out_requests_retry(backend: &'static str) {
    let (retry, request) = (retry_timeout(), request_timeout());
    if retry < request {
        tracing::warn!(
            backend,
            retry_timeout_secs = retry.as_secs(),
            request_timeout_secs = request.as_secs(),
            "{ENV_RETRY_TIMEOUT_SECS} is below {ENV_REQUEST_TIMEOUT_SECS}, so a request that times out is not retried"
        );
    }
}

pub(crate) fn retry_config() -> object_store::RetryConfig {
    object_store::RetryConfig {
        retry_timeout: retry_timeout(),
        ..Default::default()
    }
}

pub(crate) async fn put(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
    data: Vec<u8>,
    options: &UploadOptions,
) -> Result<Uploaded> {
    let opts = PutOptions {
        attributes: attributes(options),
        ..Default::default()
    };
    put_with(store, backend, keys, key, data, opts).await
}

/// When an upload goes up in parts: above `threshold` bytes, in parts of at
/// least `min_part_size` bytes (and few enough for S3's part limit), at most
/// `concurrency` of them in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Multipart {
    threshold: usize,
    min_part_size: usize,
    concurrency: usize,
}

const MAX_PARTS: usize = 10_000;

enum Put {
    Whole(PutOptions),
    InParts(PutMultipartOptions),
}

impl Multipart {
    const DEFAULT: Self = Self {
        threshold: 32 * 1024 * 1024,
        min_part_size: 8 * 1024 * 1024,
        concurrency: 4,
    };

    fn put(self, size: usize, opts: PutOptions) -> Put {
        match opts {
            PutOptions {
                mode: PutMode::Overwrite,
                tags,
                attributes,
                extensions,
            } if size > self.threshold => Put::InParts(PutMultipartOptions {
                tags,
                attributes,
                extensions,
            }),
            opts => Put::Whole(opts),
        }
    }

    fn part_size(self, size: usize) -> usize {
        self.min_part_size.max(size.div_ceil(MAX_PARTS))
    }

    fn parts(self, data: Bytes) -> impl Iterator<Item = Bytes> {
        let part_size = self.part_size(data.len());
        (0..data.len())
            .step_by(part_size)
            .map(move |start| data.slice(start..data.len().min(start + part_size)))
    }
}

pub(crate) async fn put_with(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
    data: Vec<u8>,
    opts: PutOptions,
) -> Result<Uploaded> {
    put_in(store, backend, keys, key, data, opts, Multipart::DEFAULT).await
}

async fn put_in(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
    data: Vec<u8>,
    opts: PutOptions,
    multipart: Multipart,
) -> Result<Uploaded> {
    let path = keys.path(key)?;
    let size = data.len() as u64;
    let data = Bytes::from(data);
    let stored = match multipart.put(data.len(), opts) {
        Put::Whole(opts) => store.put_opts(&path, PutPayload::from(data), opts).await,
        Put::InParts(opts) => put_parts(store, backend, key, &path, data, opts, multipart).await,
    }
    .wrap_err_with(|| format!("{backend} upload failed for {key}"))?;
    tracing::info!(backend, key = %key, size, "Uploaded");
    Ok(Uploaded {
        size,
        etag: stored.e_tag,
    })
}

async fn put_parts(
    store: &dyn ObjectStore,
    backend: &'static str,
    key: &str,
    path: &OsPath,
    data: Bytes,
    opts: PutMultipartOptions,
    multipart: Multipart,
) -> object_store::Result<PutResult> {
    let upload = store.put_multipart_opts(path, opts).await?;
    Pending::new(upload, backend, key)
        .finish(data, multipart)
        .await
}

/// A multipart upload that is aborted, in the background, unless it
/// completes: when a part or the completion fails, and when the upload is
/// dropped before it finishes.
struct Pending {
    upload: Option<Box<dyn MultipartUpload>>,
    backend: &'static str,
    key: String,
    completing: bool,
}

impl Pending {
    fn new(upload: Box<dyn MultipartUpload>, backend: &'static str, key: &str) -> Self {
        Self {
            upload: Some(upload),
            backend,
            key: key.to_string(),
            completing: false,
        }
    }

    async fn finish(
        mut self,
        data: Bytes,
        multipart: Multipart,
    ) -> object_store::Result<PutResult> {
        let completed = self.send(data, multipart).await;
        if completed.is_ok() {
            self.upload = None;
        }
        completed
    }

    async fn send(&mut self, data: Bytes, multipart: Multipart) -> object_store::Result<PutResult> {
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| object_store::Error::Generic {
                store: self.backend,
                source: format!("the multipart upload of {} has finished", self.key).into(),
            })?;
        stream::iter(
            multipart
                .parts(data)
                .map(|part| upload.put_part(PutPayload::from(part))),
        )
        .buffer_unordered(multipart.concurrency)
        .try_collect::<()>()
        .await?;
        self.completing = true;
        upload.complete().await
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        let Some(mut upload) = self.upload.take() else {
            return;
        };
        let (backend, key, completing) =
            (self.backend, std::mem::take(&mut self.key), self.completing);
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn(async move {
                match upload.abort().await {
                    Err(e) if !already_aborted(&e, completing) => tracing::warn!(
                        backend,
                        key = %key,
                        error = %e,
                        "Failed to abort a multipart upload"
                    ),
                    _ => tracing::info!(backend, key = %key, "Aborted a multipart upload"),
                }
            })),
            Err(_) => tracing::warn!(
                backend,
                key = %key,
                "No runtime to abort a multipart upload on"
            ),
        }
    }
}

/// The local filesystem's upload gives up its staged file as its completion
/// starts, so after a failed completion it has nothing left to abort.
fn already_aborted(error: &object_store::Error, completing: bool) -> bool {
    completing
        && matches!(
            error,
            object_store::Error::Generic {
                store: "LocalFileSystem",
                ..
            }
        )
}

fn content_type(attributes: &Attributes) -> Option<String> {
    attributes
        .get(&Attribute::ContentType)
        .map(|content_type| content_type.as_ref().to_string())
}

pub(crate) async fn head_meta(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    bucket: &str,
    key: &str,
) -> Result<Option<ObjectMeta>> {
    let head = GetOptions {
        head: true,
        ..Default::default()
    };
    match store.get_opts(&keys.path(key)?, head).await {
        Ok(got) => Ok(Some(ObjectMeta {
            size: got.meta.size,
            content_type: content_type(&got.attributes),
            etag: got.meta.e_tag,
            last_modified: got.meta.last_modified,
        })),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).wrap_err_with(|| format!("{backend} head failed for {bucket}/{key}")),
    }
}

pub(crate) async fn get_bytes(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    bucket: Option<&str>,
    key: &str,
) -> Result<Vec<u8>> {
    let path = keys.path(key)?;
    let location = bucket.map_or_else(|| key.to_string(), |b| format!("{b}/{key}"));
    let got = store
        .get(&path)
        .await
        .wrap_err_with(|| format!("{backend} download failed for {location}"))?;
    let bytes = got
        .bytes()
        .await
        .wrap_err_with(|| format!("{backend} read body failed for {location}"))?;
    Ok(bytes.to_vec())
}

pub(crate) async fn open_read(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    bucket: &str,
    key: &str,
) -> Result<Option<ObjectReader>> {
    let location = format!("{bucket}/{key}");
    match store.get(&keys.path(key)?).await {
        Ok(got) => Ok(Some(ObjectReader {
            size: Some(got.meta.size),
            content_type: content_type(&got.attributes),
            stream: got
                .into_stream()
                .map_err(move |e| eyre::eyre!("{backend} read failed for {location}: {e}"))
                .boxed(),
        })),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).wrap_err_with(|| format!("{backend} open failed for {bucket}/{key}")),
    }
}

pub(crate) async fn head_size(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
) -> Result<u64> {
    let path = keys.path(key)?;
    let meta = store
        .head(&path)
        .await
        .wrap_err_with(|| format!("{backend} head failed for {key}"))?;
    Ok(meta.size)
}

pub(crate) async fn copy_object(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    src_key: &str,
    dst_key: &str,
) -> Result<()> {
    let from = keys.path(src_key)?;
    let to = keys.path(dst_key)?;
    store
        .copy(&from, &to)
        .await
        .wrap_err_with(|| format!("{backend} copy failed {src_key} -> {dst_key}"))?;
    tracing::info!(backend, from = %src_key, to = %dst_key, "Copied");
    Ok(())
}

pub(crate) async fn delete_object(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
) -> Result<()> {
    let path = keys.path(key)?;
    store
        .delete(&path)
        .await
        .wrap_err_with(|| format!("{backend} delete failed for {key}"))?;
    tracing::info!(backend, key = %key, "Deleted");
    Ok(())
}

pub(crate) async fn delete_object_idempotent(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
) -> Result<()> {
    let path = keys.path(key)?;
    match store.delete(&path).await {
        Ok(()) => {
            tracing::info!(backend, key = %key, "Deleted");
            Ok(())
        }
        Err(object_store::Error::NotFound { .. }) => {
            tracing::debug!(backend, key = %key, "Delete skipped: already absent");
            Ok(())
        }
        Err(e) => Err(e).wrap_err_with(|| format!("{backend} delete failed for {key}")),
    }
}

pub(crate) async fn delete_prefix(
    store: &dyn ObjectStore,
    backend: &'static str,
    prefix: &str,
) -> Result<usize> {
    let path = OsPath::from(prefix);
    eyre::ensure!(
        path.parts().next().is_some(),
        "{backend} refuses to delete by the empty prefix '{prefix}', which is the whole bucket"
    );
    let listed = store.list(Some(&path)).map_ok(|meta| meta.location).boxed();
    let deleted = store
        .delete_stream(listed)
        .try_fold(0, |deleted, _| async move { Ok(deleted + 1) })
        .await
        .wrap_err_with(|| format!("{backend} delete by prefix failed for {prefix}"))?;
    tracing::info!(backend, prefix = %prefix, deleted, "Deleted by prefix");
    Ok(deleted)
}

pub(crate) async fn presign_url<S: object_store::signer::Signer>(
    signer: &S,
    backend: &'static str,
    keys: Keys,
    method: http::Method,
    key: &str,
    expires_in_secs: u64,
) -> Result<String> {
    let path = keys.path(key)?;
    let url = signer
        .signed_url(method.clone(), &path, Duration::from_secs(expires_in_secs))
        .await
        .wrap_err_with(|| format!("{backend} presign failed for {key}"))?;
    tracing::info!(backend, key = %key, method = %method, expires_in = expires_in_secs, "Generated presigned URL");
    Ok(url.to_string())
}

#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::Mutex;

    static LOCK: Mutex<()> = Mutex::new(());

    pub fn with(vars: &[(&str, Option<&str>)], f: impl FnOnce()) {
        let _guard = LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let saved: Vec<_> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
        f();
        for (k, prev) in saved {
            match prev {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        already_aborted, put_in, request_timeout, retry_timeout, Keys, Multipart, Put,
        ENV_REQUEST_TIMEOUT_SECS, ENV_RETRY_TIMEOUT_SECS, MAX_PARTS,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::stream::BoxStream;
    use futures_util::FutureExt;
    use object_store::memory::InMemory;
    use object_store::path::Path as OsPath;
    use object_store::{
        Attribute, Attributes, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions,
        PutPayload, PutResult, UploadPart,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    const SMALL: Multipart = Multipart {
        threshold: 1024,
        min_part_size: 256,
        concurrency: 2,
    };

    fn payload(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i * 31 % 251) as u8).collect()
    }

    fn typed(content_type: &str) -> PutOptions {
        PutOptions {
            attributes: Attributes::from_iter([(Attribute::ContentType, content_type.to_string())]),
            ..Default::default()
        }
    }

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    enum Fault {
        #[default]
        None,
        Completion,
        StalledParts,
    }

    #[derive(Debug, Default)]
    struct Counts {
        whole: AtomicUsize,
        in_parts: AtomicUsize,
        aborted: AtomicUsize,
    }

    impl Counts {
        fn get(&self) -> [usize; 3] {
            [&self.whole, &self.in_parts, &self.aborted].map(|count| count.load(Ordering::SeqCst))
        }
    }

    #[derive(Debug, Default)]
    struct Probe {
        inner: InMemory,
        counts: Arc<Counts>,
        fault: Fault,
    }

    impl Probe {
        fn failing(fault: Fault) -> Self {
            Self {
                fault,
                ..Self::default()
            }
        }

        async fn aborted(&self) -> usize {
            for _ in 0..200 {
                if self.counts.aborted.load(Ordering::SeqCst) > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            self.counts.aborted.load(Ordering::SeqCst)
        }
    }

    impl std::fmt::Display for Probe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Probe({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for Probe {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.counts.whole.fetch_add(1, Ordering::SeqCst);
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.counts.in_parts.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(ProbeUpload {
                inner: self.inner.put_multipart_opts(location, opts).await?,
                counts: Arc::clone(&self.counts),
                fault: self.fault,
            }))
        }

        async fn get_opts(
            &self,
            location: &OsPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<OsPath>>,
        ) -> BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[derive(Debug)]
    struct ProbeUpload {
        inner: Box<dyn MultipartUpload>,
        counts: Arc<Counts>,
        fault: Fault,
    }

    #[async_trait]
    impl MultipartUpload for ProbeUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            match self.fault {
                Fault::StalledParts => futures_util::future::pending().boxed(),
                _ => self.inner.put_part(data),
            }
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            match self.fault {
                Fault::Completion => Err(object_store::Error::Generic {
                    store: "probe",
                    source: "the store refused the completion".into(),
                }),
                _ => self.inner.complete().await,
            }
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.counts.aborted.fetch_add(1, Ordering::SeqCst);
            self.inner.abort().await
        }
    }

    #[test]
    fn only_overwrites_above_the_threshold_go_up_in_parts() {
        let in_parts = |multipart: Multipart, size, mode| {
            matches!(
                multipart.put(
                    size,
                    PutOptions {
                        mode,
                        ..typed("text/plain")
                    }
                ),
                Put::InParts(_)
            )
        };
        assert!(!in_parts(SMALL, 1024, PutMode::Overwrite));
        assert!(in_parts(SMALL, 1025, PutMode::Overwrite));
        assert!(!in_parts(SMALL, 1025, PutMode::Create));
        assert!(!in_parts(
            Multipart::DEFAULT,
            32 * 1024 * 1024,
            PutMode::Overwrite
        ));
        assert!(in_parts(
            Multipart::DEFAULT,
            198_547_134,
            PutMode::Overwrite
        ));
        assert!(matches!(
            SMALL.put(1025, typed("text/plain")),
            Put::InParts(PutMultipartOptions { attributes, .. })
                if attributes.get(&Attribute::ContentType).map(|v| v.as_ref()) == Some("text/plain")
        ));
    }

    #[test]
    fn parts_cover_the_payload_in_order_within_the_part_limit() {
        let data = Bytes::from(payload(1000));
        let parts: Vec<Bytes> = SMALL.parts(data.clone()).collect();
        assert_eq!(
            parts.iter().map(Bytes::len).collect::<Vec<_>>(),
            [256, 256, 256, 232]
        );
        assert_eq!(parts.concat(), data);
        assert_eq!(SMALL.parts(Bytes::from(payload(768))).count(), 3);

        let default = Multipart::DEFAULT;
        assert_eq!(default.part_size(198_547_134), 8 * 1024 * 1024);
        assert_eq!(
            198_547_134_usize.div_ceil(default.part_size(198_547_134)),
            24
        );
        let huge = 200 * 1024 * 1024 * 1024_usize;
        assert!(huge.div_ceil(default.part_size(huge)) <= MAX_PARTS);
    }

    #[tokio::test]
    async fn a_large_upload_goes_up_in_parts_and_reads_back_byte_identical_with_its_attributes() {
        let store = Probe::default();
        for (key, size) in [
            ("bundle/remainder.tar.gz", 5000),
            ("bundle/exact.tar.gz", 4096),
        ] {
            let data = payload(size);
            let uploaded = put_in(
                &store,
                "memory",
                Keys::Encoded,
                key,
                data.clone(),
                typed("application/gzip"),
                SMALL,
            )
            .await
            .expect("uploaded");
            assert_eq!(uploaded.size, size as u64);
            let got = store.get(&OsPath::from(key)).await.expect("stored");
            assert_eq!(
                got.attributes
                    .get(&Attribute::ContentType)
                    .map(|v| v.as_ref()),
                Some("application/gzip")
            );
            assert_eq!(got.bytes().await.expect("read"), data);
        }
        assert_eq!(store.counts.get(), [0, 2, 0]);
    }

    #[tokio::test]
    async fn a_small_upload_goes_up_whole_and_reads_back_byte_identical_with_its_attributes() {
        let store = Probe::default();
        let data = payload(1024);
        put_in(
            &store,
            "memory",
            Keys::Encoded,
            "small.json",
            data.clone(),
            typed("application/json"),
            SMALL,
        )
        .await
        .expect("uploaded");
        let got = store
            .get(&OsPath::from("small.json"))
            .await
            .expect("stored");
        assert_eq!(
            got.attributes
                .get(&Attribute::ContentType)
                .map(|v| v.as_ref()),
            Some("application/json")
        );
        assert_eq!(got.bytes().await.expect("read"), data);
        assert_eq!(store.counts.get(), [1, 0, 0]);
    }

    #[tokio::test]
    async fn a_conditional_upload_keeps_its_precondition_above_the_threshold() {
        let store = Probe::default();
        let create = || PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        put_in(
            &store,
            "memory",
            Keys::Encoded,
            "k",
            payload(4096),
            create(),
            SMALL,
        )
        .await
        .expect("created");
        let refused = put_in(
            &store,
            "memory",
            Keys::Encoded,
            "k",
            payload(4096),
            create(),
            SMALL,
        )
        .await
        .expect_err("already exists");
        assert_eq!(refused.to_string(), "memory upload failed for k");
        assert_eq!(store.counts.get(), [2, 0, 0]);
    }

    #[tokio::test]
    async fn a_failed_completion_aborts_the_upload() {
        let store = Probe::failing(Fault::Completion);
        let refused = put_in(
            &store,
            "memory",
            Keys::Encoded,
            "k",
            payload(4096),
            PutOptions::default(),
            SMALL,
        )
        .await
        .expect_err("refused");
        assert_eq!(refused.to_string(), "memory upload failed for k");
        assert_eq!(store.aborted().await, 1);
        assert!(store.get(&OsPath::from("k")).await.is_err());
    }

    #[tokio::test]
    async fn a_dropped_upload_is_aborted() {
        let store = Probe::failing(Fault::StalledParts);
        let upload = put_in(
            &store,
            "memory",
            Keys::Encoded,
            "k",
            payload(4096),
            PutOptions::default(),
            SMALL,
        );
        assert!(tokio::time::timeout(Duration::from_millis(20), upload)
            .await
            .is_err());
        assert_eq!(store.aborted().await, 1);
        assert_eq!(store.counts.get(), [0, 1, 1]);
    }

    #[tokio::test]
    async fn a_completed_upload_is_not_aborted() {
        let store = Probe::default();
        put_in(
            &store,
            "memory",
            Keys::Encoded,
            "k",
            payload(4096),
            PutOptions::default(),
            SMALL,
        )
        .await
        .expect("uploaded");
        tokio::task::yield_now().await;
        assert_eq!(store.counts.get(), [0, 1, 0]);
    }

    #[test]
    fn request_timeout_defaults_to_the_clients_default_when_unset() {
        super::test_env::with(&[(ENV_REQUEST_TIMEOUT_SECS, None)], || {
            assert_eq!(request_timeout(), Duration::from_secs(30));
        });
    }

    #[test]
    fn request_timeout_honours_env_override() {
        super::test_env::with(&[(ENV_REQUEST_TIMEOUT_SECS, Some("300"))], || {
            assert_eq!(request_timeout(), Duration::from_secs(300));
        });
    }

    #[test]
    fn only_the_local_stores_failed_completion_counts_as_aborted() {
        let local = || object_store::Error::Generic {
            store: "LocalFileSystem",
            source: "Upload aborted".into(),
        };
        assert!(already_aborted(&local(), true));
        assert!(!already_aborted(&local(), false));
        assert!(!already_aborted(
            &object_store::Error::Generic {
                store: "S3",
                source: "refused".into(),
            },
            true
        ));
    }

    #[test]
    fn retry_timeout_defaults_to_bounded_budget_when_unset() {
        super::test_env::with(&[(ENV_RETRY_TIMEOUT_SECS, None)], || {
            assert_eq!(retry_timeout(), Duration::from_secs(20));
        });
    }

    #[test]
    fn retry_timeout_honours_env_override() {
        super::test_env::with(&[(ENV_RETRY_TIMEOUT_SECS, Some("45"))], || {
            assert_eq!(retry_timeout(), Duration::from_secs(45));
        });
    }

    #[test]
    fn retry_timeout_ignores_zero_and_garbage() {
        super::test_env::with(&[(ENV_RETRY_TIMEOUT_SECS, Some("0"))], || {
            assert_eq!(retry_timeout(), Duration::from_secs(20));
        });
        super::test_env::with(&[(ENV_RETRY_TIMEOUT_SECS, Some("not-a-number"))], || {
            assert_eq!(retry_timeout(), Duration::from_secs(20));
        });
    }

    #[test]
    fn request_timeout_ignores_zero_and_garbage() {
        super::test_env::with(&[(ENV_REQUEST_TIMEOUT_SECS, Some("0"))], || {
            assert_eq!(request_timeout(), Duration::from_secs(30));
        });
        super::test_env::with(&[(ENV_REQUEST_TIMEOUT_SECS, Some("abc"))], || {
            assert_eq!(request_timeout(), Duration::from_secs(30));
        });
        super::test_env::with(&[(ENV_REQUEST_TIMEOUT_SECS, Some(" 30 "))], || {
            assert_eq!(request_timeout(), Duration::from_secs(30));
        });
        super::test_env::with(&[(ENV_REQUEST_TIMEOUT_SECS, Some("60\n"))], || {
            assert_eq!(request_timeout(), Duration::from_secs(60));
        });
    }
}
