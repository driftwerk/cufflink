use crate::{ObjectMeta, ObjectReader, UploadOptions, Uploaded};
use bytes::{Bytes, BytesMut};
use eyre::{Result, WrapErr};
use futures_util::stream::BoxStream;
use futures_util::{future, stream, StreamExt, TryFutureExt, TryStreamExt};
use object_store::{
    path::Path as OsPath, Attribute, AttributeValue, Attributes, ClientOptions, GetOptions,
    GetRange, GetResult, MultipartUpload, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::Arc;
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

const DEFAULT_STALL_TIMEOUT_SECS: u64 = 20;

pub(crate) const ENV_STALL_TIMEOUT_SECS: &str = "OBJECT_STORE_STALL_TIMEOUT_SECS";

const DEFAULT_STALL_ATTEMPTS: u64 = 3;

pub(crate) const ENV_STALL_ATTEMPTS: &str = "OBJECT_STORE_STALL_ATTEMPTS";

pub(crate) const ENV_COMPLETION_TIMEOUT_SECS: &str = "OBJECT_STORE_COMPLETION_TIMEOUT_SECS";

fn env_positive(name: &'static str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map_or(default, |raw| match raw.trim().parse::<u64>() {
            Ok(value) if value > 0 => value,
            _ => {
                tracing::warn!(
                    variable = name,
                    value = %raw,
                    default,
                    "Ignoring a setting that is not a positive whole number"
                );
                default
            }
        })
}

fn env_secs(name: &'static str, default: u64) -> Duration {
    Duration::from_secs(env_positive(name, default))
}

pub(crate) fn retry_timeout() -> Duration {
    env_secs(ENV_RETRY_TIMEOUT_SECS, DEFAULT_RETRY_TIMEOUT_SECS)
}

pub(crate) fn request_timeout() -> Duration {
    env_secs(ENV_REQUEST_TIMEOUT_SECS, DEFAULT_REQUEST_TIMEOUT_SECS)
}

/// How a storage request that makes no progress is cut short and tried
/// again: a write, whole or one part of a multipart upload, that has not
/// finished within `timeout` for each part's worth of its bytes, the
/// completion of a multipart upload not answered within `completion`, an
/// open with no answer within `timeout`, and a read that receives nothing for
/// it, which resumes from its next byte. A request is tried at most `attempts`
/// times in a row, and its last attempt is bounded only by the request
/// timeout, the `ceiling`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stall {
    timeout: Duration,
    attempts: u32,
    ceiling: Duration,
    completion: Duration,
}

impl Stall {
    pub(crate) fn configured() -> Self {
        let timeout = env_secs(ENV_STALL_TIMEOUT_SECS, DEFAULT_STALL_TIMEOUT_SECS);
        Self {
            timeout,
            attempts: u32::try_from(env_positive(ENV_STALL_ATTEMPTS, DEFAULT_STALL_ATTEMPTS))
                .unwrap_or(u32::MAX),
            ceiling: request_timeout(),
            completion: env_secs(ENV_COMPLETION_TIMEOUT_SECS, timeout.as_secs()),
        }
    }

    pub(crate) fn timeout(self) -> Duration {
        self.timeout
    }

    pub(crate) fn completion(self) -> Duration {
        self.completion
    }

    #[cfg(test)]
    pub(crate) const fn of(
        timeout: Duration,
        attempts: u32,
        ceiling: Duration,
        completion: Duration,
    ) -> Self {
        Self {
            timeout,
            attempts,
            ceiling,
            completion,
        }
    }

    fn cuts(self, deadline: Duration) -> bool {
        self.attempts > 1 && deadline < self.ceiling
    }

    fn outlasts_ceiling(self) -> bool {
        self.attempts > 1 && self.timeout >= self.ceiling
    }

    pub(crate) fn for_write(self, size: usize) -> Self {
        self.for_upload(size, Multipart::DEFAULT.min_part_size)
    }

    fn for_upload(self, size: usize, part_size: usize) -> Self {
        let parts = size.div_ceil(part_size).max(1);
        Self {
            timeout: self
                .timeout
                .saturating_mul(u32::try_from(parts).unwrap_or(u32::MAX)),
            ..self
        }
    }
}

/// Runs `attempt` until it answers: each attempt but the last is abandoned
/// once `deadline` passes, and the last is left to the ceiling.
pub(crate) async fn attempted<T, Fut>(
    stall: Stall,
    deadline: Duration,
    backend: &'static str,
    request: &'static str,
    location: &str,
    mut attempt: impl FnMut() -> Fut,
) -> object_store::Result<T>
where
    Fut: std::future::Future<Output = object_store::Result<T>>,
{
    let cut = if stall.cuts(deadline) {
        stall.attempts - 1
    } else {
        0
    };
    for tried in 1..=cut {
        match tokio::time::timeout(deadline, attempt()).await {
            Ok(answered) => return answered,
            Err(_) => tracing::warn!(
                backend,
                request,
                location,
                attempt = tried,
                attempts = stall.attempts,
                timeout_ms = millis(deadline),
                "A storage request made no progress in time; trying it again"
            ),
        }
    }
    attempt().await
}

fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn client_options() -> ClientOptions {
    ClientOptions::new().with_timeout(request_timeout())
}

fn timed_out_requests_cannot_retry(configured: bool, retry: Duration, request: Duration) -> bool {
    configured && retry < request
}

pub(crate) fn warn_unless_timed_out_requests_retry(backend: &'static str) {
    let configured = [ENV_RETRY_TIMEOUT_SECS, ENV_REQUEST_TIMEOUT_SECS]
        .iter()
        .any(|name| std::env::var_os(name).is_some());
    let (retry, request) = (retry_timeout(), request_timeout());
    if timed_out_requests_cannot_retry(configured, retry, request) {
        tracing::warn!(
            backend,
            retry_timeout_secs = retry.as_secs(),
            request_timeout_secs = request.as_secs(),
            "{ENV_RETRY_TIMEOUT_SECS} is below {ENV_REQUEST_TIMEOUT_SECS}, so a request that times out is not retried"
        );
    }
    let stall = Stall::configured();
    if stall.outlasts_ceiling() {
        tracing::warn!(
            backend,
            stall_timeout_secs = stall.timeout.as_secs(),
            request_timeout_secs = stall.ceiling.as_secs(),
            "{ENV_STALL_TIMEOUT_SECS} is not below {ENV_REQUEST_TIMEOUT_SECS}, so a stalled request is never cut short"
        );
    }
    if completions_are_never_cut(
        std::env::var_os(ENV_COMPLETION_TIMEOUT_SECS).is_some(),
        stall,
    ) {
        tracing::warn!(
            backend,
            completion_timeout_secs = stall.completion.as_secs(),
            request_timeout_secs = stall.ceiling.as_secs(),
            "{ENV_COMPLETION_TIMEOUT_SECS} is not below {ENV_REQUEST_TIMEOUT_SECS}, so a stalled completion is never cut short"
        );
    }
}

fn completions_are_never_cut(configured: bool, stall: Stall) -> bool {
    configured && stall.attempts > 1 && !stall.cuts(stall.completion)
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
    stall: Stall,
) -> Result<Uploaded> {
    let opts = PutOptions {
        attributes: attributes(options),
        ..Default::default()
    };
    put_with(store, backend, keys, key, data, opts, stall).await
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
    stall: Stall,
) -> Result<Uploaded> {
    put_in(
        store,
        backend,
        keys,
        key,
        data,
        opts,
        Multipart::DEFAULT,
        stall,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn put_in(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
    data: impl Into<Bytes>,
    opts: PutOptions,
    multipart: Multipart,
    stall: Stall,
) -> Result<Uploaded> {
    let path = keys.path(key)?;
    let data = data.into();
    let size = data.len() as u64;
    let (stored, completion) = match multipart.put(data.len(), opts) {
        Put::Whole(opts) => {
            let stall = stall.for_upload(data.len(), multipart.min_part_size);
            put_whole(store, backend, key, &path, data, opts, stall)
                .await
                .map(|stored| (stored, None))
        }
        Put::InParts(opts) => put_parts(store, backend, key, &path, data, opts, multipart)
            .await
            .map(|(stored, completion)| (stored, Some(completion))),
    }
    .wrap_err_with(|| format!("{backend} upload failed for {key}"))?;
    tracing::info!(backend, key = %key, size, "Uploaded");
    Ok(Uploaded {
        size,
        etag: stored.e_tag,
        completion,
    })
}

async fn put_whole(
    store: &dyn ObjectStore,
    backend: &'static str,
    key: &str,
    path: &OsPath,
    data: Bytes,
    opts: PutOptions,
    stall: Stall,
) -> object_store::Result<PutResult> {
    let payload = PutPayload::from(data);
    match opts.mode {
        PutMode::Overwrite => {
            attempted(stall, stall.timeout, backend, "write", key, || {
                store.put_opts(path, payload.clone(), opts.clone())
            })
            .await
        }
        _ => store.put_opts(path, payload, opts).await,
    }
}

pub(crate) async fn put_stream(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
    data: BoxStream<'static, Result<Bytes>>,
    options: &UploadOptions,
    stall: Stall,
) -> Result<Uploaded> {
    let opts = PutOptions {
        attributes: attributes(options),
        ..Default::default()
    };
    put_stream_with(store, backend, keys, key, data, opts, stall).await
}

pub(crate) async fn put_stream_with(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
    data: BoxStream<'static, Result<Bytes>>,
    opts: PutOptions,
    stall: Stall,
) -> Result<Uploaded> {
    put_stream_in(
        store,
        backend,
        keys,
        key,
        data,
        opts,
        Multipart::DEFAULT,
        stall,
    )
    .await
}

async fn gathered(data: &mut BoxStream<'static, Result<Bytes>>, limit: usize) -> Result<BytesMut> {
    let mut head = BytesMut::new();
    while head.len() < limit {
        match data.try_next().await? {
            Some(chunk) => head.extend_from_slice(&chunk),
            None => break,
        }
    }
    Ok(head)
}

#[allow(clippy::too_many_arguments)]
async fn put_stream_in(
    store: &dyn ObjectStore,
    backend: &'static str,
    keys: Keys,
    key: &str,
    mut data: BoxStream<'static, Result<Bytes>>,
    opts: PutOptions,
    multipart: Multipart,
    stall: Stall,
) -> Result<Uploaded> {
    let head = gathered(&mut data, multipart.threshold + 1).await?;
    match multipart.put(head.len(), opts) {
        Put::Whole(opts) => {
            let whole = data
                .try_fold(head, |mut whole, chunk| {
                    whole.extend_from_slice(&chunk);
                    future::ready(Ok(whole))
                })
                .await?;
            put_in(
                store,
                backend,
                keys,
                key,
                whole.freeze(),
                opts,
                multipart,
                stall,
            )
            .await
        }
        Put::InParts(opts) => {
            let failed = |e: object_store::Error| {
                eyre::Report::new(e).wrap_err(format!("{backend} upload failed for {key}"))
            };
            let upload = store
                .put_multipart_opts(&keys.path(key)?, opts)
                .await
                .map_err(failed)?;
            let (size, stored, completion) = Pending::new(upload, backend, key)
                .finish_stream(head, data, multipart, failed)
                .await?;
            tracing::info!(backend, key = %key, size, "Uploaded");
            Ok(Uploaded {
                size,
                etag: stored.e_tag,
                completion: Some(completion),
            })
        }
    }
}

fn rechunked(
    head: BytesMut,
    data: BoxStream<'static, Result<Bytes>>,
    part_size: usize,
) -> impl futures_util::Stream<Item = Result<Bytes>> {
    stream::try_unfold(
        (data, head, false),
        move |(mut data, mut buffer, mut ended)| async move {
            loop {
                if buffer.len() >= part_size {
                    let part = buffer.split_to(part_size).freeze();
                    return Ok(Some((part, (data, buffer, ended))));
                }
                if ended {
                    return Ok((!buffer.is_empty())
                        .then(|| (buffer.split().freeze(), (data, buffer, ended))));
                }
                match data.try_next().await? {
                    Some(chunk) => buffer.extend_from_slice(&chunk),
                    None => ended = true,
                }
            }
        },
    )
}

async fn put_parts(
    store: &dyn ObjectStore,
    backend: &'static str,
    key: &str,
    path: &OsPath,
    data: Bytes,
    opts: PutMultipartOptions,
    multipart: Multipart,
) -> object_store::Result<(PutResult, Duration)> {
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
    ) -> object_store::Result<(PutResult, Duration)> {
        let completed = self.send(data, multipart).await;
        if completed.is_ok() {
            self.upload = None;
        }
        completed
    }

    async fn finish_stream(
        mut self,
        head: BytesMut,
        data: BoxStream<'static, Result<Bytes>>,
        multipart: Multipart,
        failed: impl Fn(object_store::Error) -> eyre::Report,
    ) -> Result<(u64, PutResult, Duration)> {
        let completed = self.send_stream(head, data, multipart, failed).await;
        if completed.is_ok() {
            self.upload = None;
        }
        completed
    }

    async fn send_stream(
        &mut self,
        head: BytesMut,
        data: BoxStream<'static, Result<Bytes>>,
        multipart: Multipart,
        failed: impl Fn(object_store::Error) -> eyre::Report,
    ) -> Result<(u64, PutResult, Duration)> {
        let upload = unfinished(&mut self.upload, self.backend, &self.key).map_err(&failed)?;
        let key = &self.key;
        let size = rechunked(head, data, multipart.min_part_size)
            .enumerate()
            .map(|(index, part)| {
                part.and_then(|part| {
                    if index < MAX_PARTS {
                        Ok(part)
                    } else {
                        Err(eyre::eyre!(
                            "the upload of {key} needs more than {MAX_PARTS} parts of {} bytes",
                            multipart.min_part_size
                        ))
                    }
                })
            })
            .map_ok(|part| {
                let size = part.len() as u64;
                upload
                    .put_part(PutPayload::from(part))
                    .map_ok(move |()| size)
                    .map_err(&failed)
            })
            .try_buffer_unordered(multipart.concurrency)
            .try_fold(0, |total, size| future::ready(Ok(total + size)))
            .await?;
        self.completing = true;
        let asked = std::time::Instant::now();
        unfinished(&mut self.upload, self.backend, &self.key)
            .map_err(&failed)?
            .complete()
            .await
            .map(|stored| (size, stored, asked.elapsed()))
            .map_err(&failed)
    }

    async fn send(
        &mut self,
        data: Bytes,
        multipart: Multipart,
    ) -> object_store::Result<(PutResult, Duration)> {
        let upload = unfinished(&mut self.upload, self.backend, &self.key)?;
        stream::iter(
            multipart
                .parts(data)
                .map(|part| upload.put_part(PutPayload::from(part))),
        )
        .buffer_unordered(multipart.concurrency)
        .try_collect::<()>()
        .await?;
        self.completing = true;
        let asked = std::time::Instant::now();
        upload
            .complete()
            .await
            .map(|stored| (stored, asked.elapsed()))
    }
}

fn unfinished<'a>(
    upload: &'a mut Option<Box<dyn MultipartUpload>>,
    backend: &'static str,
    key: &str,
) -> object_store::Result<&'a mut Box<dyn MultipartUpload>> {
    upload.as_mut().ok_or_else(|| object_store::Error::Generic {
        store: backend,
        source: format!("the multipart upload of {key} has finished").into(),
    })
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
    store: Arc<dyn ObjectStore>,
    backend: &'static str,
    keys: Keys,
    bucket: Option<&str>,
    key: &str,
    stall: Stall,
) -> Result<Vec<u8>> {
    let path = keys.path(key)?;
    let location = bucket.map_or_else(|| key.to_string(), |b| format!("{b}/{key}"));
    let got = first_read(store.as_ref(), backend, &path, &location, stall)
        .await
        .wrap_err_with(|| format!("{backend} download failed for {location}"))?;
    let capacity = usize::try_from(got.meta.size).unwrap_or_default();
    Read::resumable(store, backend, path, location, got, stall)
        .try_fold(Vec::with_capacity(capacity), |mut bytes, chunk| {
            bytes.extend_from_slice(&chunk);
            future::ready(Ok(bytes))
        })
        .await
}

pub(crate) async fn open_read(
    store: Arc<dyn ObjectStore>,
    backend: &'static str,
    keys: Keys,
    bucket: &str,
    key: &str,
    stall: Stall,
) -> Result<Option<ObjectReader>> {
    let path = keys.path(key)?;
    let location = format!("{bucket}/{key}");
    let first = first_read(store.as_ref(), backend, &path, &location, stall).await;
    match first {
        Ok(got) => Ok(Some(ObjectReader {
            size: Some(got.meta.size),
            content_type: content_type(&got.attributes),
            stream: Read::resumable(store, backend, path, location, got, stall),
        })),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).wrap_err_with(|| format!("{backend} open failed for {location}")),
    }
}

async fn first_read(
    store: &dyn ObjectStore,
    backend: &'static str,
    path: &OsPath,
    location: &str,
    stall: Stall,
) -> object_store::Result<GetResult> {
    attempted(stall, stall.timeout, backend, "open", location, || {
        store.get_opts(path, GetOptions::default())
    })
    .await
}

/// A read of one object that resumes from its next byte when its body stalls
/// or breaks, until it stalls on as many attempts in a row without a byte;
/// its last attempt waits for the ceiling. Each resume asks for the rest of
/// the object by the ETag it was first answered with, so an object that
/// changed fails the read instead of mixing two versions, and one answered
/// without an ETag is never cut short.
struct Read {
    store: Arc<dyn ObjectStore>,
    backend: &'static str,
    path: OsPath,
    location: String,
    etag: Option<String>,
    size: u64,
    next: u64,
    resumes: u32,
    stall: Stall,
    body: BoxStream<'static, object_store::Result<Bytes>>,
}

impl Read {
    fn resumable(
        store: Arc<dyn ObjectStore>,
        backend: &'static str,
        path: OsPath,
        location: String,
        got: GetResult,
        stall: Stall,
    ) -> BoxStream<'static, Result<Bytes>> {
        let read = Self {
            store,
            backend,
            path,
            location,
            etag: got.meta.e_tag.clone(),
            size: got.meta.size,
            next: got.range.start,
            resumes: 0,
            stall,
            body: got.into_stream(),
        };
        stream::try_unfold(read, Self::chunk).boxed()
    }

    fn resumes_left(&self) -> bool {
        self.etag.is_some()
            && self.stall.cuts(self.stall.timeout)
            && self.resumes + 1 < self.stall.attempts
    }

    async fn chunk(mut self) -> Result<Option<(Bytes, Self)>> {
        let mut deadline = tokio::time::Instant::now() + self.stall.timeout;
        loop {
            if self.next >= self.size {
                return Ok(None);
            }
            let waited = if self.resumes_left() {
                tokio::time::timeout_at(deadline, self.body.next()).await
            } else {
                Ok(self.body.next().await)
            };
            let cause = match waited {
                Ok(Some(Ok(chunk))) if chunk.is_empty() => continue,
                Ok(Some(Ok(chunk))) => {
                    self.next += chunk.len() as u64;
                    self.resumes = 0;
                    return Ok(Some((chunk, self)));
                }
                Ok(None) => format!("the body ended at byte {} of {}", self.next, self.size),
                Ok(Some(Err(e))) => e.to_string(),
                Err(_) => format!("no data for {} ms", millis(self.stall.timeout)),
            };
            self = self.resumed(cause).await?;
            deadline = tokio::time::Instant::now() + self.stall.timeout;
        }
    }

    fn failed(&self, cause: impl std::fmt::Display) -> eyre::Report {
        eyre::eyre!(
            "{} read failed for {}: {cause}",
            self.backend,
            self.location
        )
    }

    async fn resumed(mut self, cause: String) -> Result<Self> {
        let Some(etag) = self.etag.clone().filter(|_| self.resumes_left()) else {
            return Err(self.failed(cause));
        };
        self.resumes += 1;
        tracing::warn!(
            backend = self.backend,
            location = %self.location,
            offset = self.next,
            size = self.size,
            resume = self.resumes,
            attempts = self.stall.attempts,
            cause = %cause,
            "A storage read stalled; resuming it from its next byte"
        );
        let rest = GetOptions {
            range: Some(GetRange::Offset(self.next)),
            if_match: Some(etag),
            ..Default::default()
        };
        let got = attempted(
            self.stall,
            self.stall.timeout,
            self.backend,
            "resume",
            &self.location,
            || self.store.get_opts(&self.path, rest.clone()),
        )
        .await
        .map_err(|e| match e {
            object_store::Error::Precondition { .. } => {
                self.failed("the object changed while it was read")
            }
            e => self.failed(e),
        })?;
        if got.range.start != self.next || got.meta.size != self.size {
            return Err(self.failed(format!(
                "the resumed read answered from byte {} of {}, not {} of {}",
                got.range.start, got.meta.size, self.next, self.size
            )));
        }
        self.body = got.into_stream();
        Ok(self)
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
        already_aborted, completions_are_never_cut, get_bytes, open_read, put_in, put_stream_in,
        rechunked, request_timeout, retry_timeout, timed_out_requests_cannot_retry, Keys,
        Multipart, Put, Stall, ENV_COMPLETION_TIMEOUT_SECS, ENV_REQUEST_TIMEOUT_SECS,
        ENV_RETRY_TIMEOUT_SECS, ENV_STALL_ATTEMPTS, ENV_STALL_TIMEOUT_SECS, MAX_PARTS,
    };
    use async_trait::async_trait;
    use bytes::{Bytes, BytesMut};
    use futures_util::stream::BoxStream;
    use futures_util::{FutureExt, StreamExt, TryStreamExt};
    use object_store::memory::InMemory;
    use object_store::path::Path as OsPath;
    use object_store::{
        Attribute, Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload,
        ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    const SMALL: Multipart = Multipart {
        threshold: 1024,
        min_part_size: 256,
        concurrency: 2,
    };

    const UNCUT: Stall = Stall {
        timeout: Duration::from_secs(1),
        attempts: 1,
        ceiling: Duration::from_secs(30),
        completion: Duration::from_secs(2),
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
        StalledWrites(usize),
        SlowWrites(Duration),
        Reads {
            answers: &'static [Answer],
            etag: bool,
        },
        Trickle(usize),
    }

    /// How the probe answers one read, in the order it is asked: `Stalls`
    /// sends that many bytes and then nothing until the request times out,
    /// `Ends` sends them and ends, `Pauses` sends the rest after a pause,
    /// `Empties` sends them and then empty frames, `Hangs` never answers,
    /// `Whole` answers with the whole object whatever range was asked and
    /// `Other` answers from another object, with the range asked but no ETag
    /// pin.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Answer {
        Stalls(usize),
        Ends(usize),
        Pauses(Duration),
        Empties(usize),
        Hangs,
        Whole,
        Other(&'static str),
    }

    #[derive(Debug, Default)]
    struct Counts {
        whole: AtomicUsize,
        in_parts: AtomicUsize,
        aborted: AtomicUsize,
        gets: AtomicUsize,
        offsets: std::sync::Mutex<Vec<Option<u64>>>,
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
            let sent = self.counts.whole.fetch_add(1, Ordering::SeqCst);
            match self.fault {
                Fault::StalledWrites(stalled) if sent < stalled => {
                    return futures_util::future::pending().await
                }
                Fault::SlowWrites(delay) => tokio::time::sleep(delay).await,
                _ => {}
            }
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
            let asked = self.counts.gets.fetch_add(1, Ordering::SeqCst);
            self.counts
                .offsets
                .lock()
                .expect("offsets")
                .push(match options.range {
                    Some(GetRange::Offset(offset)) => Some(offset),
                    _ => None,
                });
            let (answer, etag) = match self.fault {
                Fault::Reads { answers, etag } => (answers.get(asked).copied(), etag),
                Fault::Trickle(sent) => (Some(Answer::Stalls(sent)), true),
                _ => (None, true),
            };
            match answer {
                None => self.inner.get_opts(location, options).await,
                Some(Answer::Hangs) => futures_util::future::pending().await,
                Some(Answer::Other(key)) => {
                    let unpinned = GetOptions {
                        range: options.range,
                        ..Default::default()
                    };
                    answered(
                        self.inner.get_opts(&OsPath::from(key), unpinned).await?,
                        Answer::Whole,
                        etag,
                    )
                    .await
                }
                Some(Answer::Whole) => {
                    let whole = GetOptions {
                        range: None,
                        ..options
                    };
                    answered(
                        self.inner.get_opts(location, whole).await?,
                        Answer::Whole,
                        etag,
                    )
                    .await
                }
                Some(answer) => {
                    answered(self.inner.get_opts(location, options).await?, answer, etag).await
                }
            }
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

    const CEILING: Duration = Duration::from_secs(30);

    async fn answered(
        got: GetResult,
        answer: Answer,
        etag: bool,
    ) -> object_store::Result<GetResult> {
        let (mut meta, range, attributes, extensions) = (
            got.meta.clone(),
            got.range.clone(),
            got.attributes.clone(),
            got.extensions.clone(),
        );
        if !etag {
            meta.e_tag = None;
        }
        let body = got.bytes().await?;
        let head = |sent: usize| {
            let head = body.slice(..sent.min(body.len()));
            futures_util::stream::once(async move { Ok::<_, object_store::Error>(head) })
        };
        let timed_out = futures_util::stream::once(async {
            tokio::time::sleep(CEILING).await;
            Err(object_store::Error::Generic {
                store: "probe",
                source: "the request timed out".into(),
            })
        });
        let stream = match answer {
            Answer::Stalls(sent) => head(sent).chain(timed_out).boxed(),
            Answer::Ends(sent) => head(sent).boxed(),
            Answer::Pauses(pause) => futures_util::stream::once(async move {
                tokio::time::sleep(pause).await;
                Ok(body)
            })
            .boxed(),
            Answer::Empties(sent) => head(sent)
                .chain(
                    futures_util::stream::repeat_with(Bytes::new).then(|empty| async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Ok(empty)
                    }),
                )
                .boxed(),
            Answer::Hangs | Answer::Whole | Answer::Other(_) => head(usize::MAX).boxed(),
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes,
            extensions,
        })
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
                UNCUT,
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
            UNCUT,
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
            UNCUT,
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
            UNCUT,
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
            UNCUT,
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
            UNCUT,
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
            UNCUT,
        )
        .await
        .expect("uploaded");
        tokio::task::yield_now().await;
        assert_eq!(store.counts.get(), [0, 1, 0]);
    }

    fn streamed(
        data: &[u8],
        chunk: usize,
        end: Option<&'static str>,
    ) -> BoxStream<'static, eyre::Result<Bytes>> {
        let chunks = data
            .chunks(chunk)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .chain(end.map(|end| Err(eyre::eyre!(end))))
            .collect::<Vec<_>>();
        futures_util::stream::iter(chunks).boxed()
    }

    #[tokio::test]
    async fn a_stream_is_cut_into_parts_of_the_part_size_in_order() {
        let data = payload(700);
        let parts = rechunked(BytesMut::new(), streamed(&data, 100, None), 256)
            .try_collect::<Vec<_>>()
            .await
            .expect("cut");
        assert_eq!(
            parts.iter().map(Bytes::len).collect::<Vec<_>>(),
            [256, 256, 188]
        );
        assert_eq!(parts.concat(), data);
        let seeded = rechunked(
            BytesMut::from(&data[..300]),
            streamed(&data[300..], 100, None),
            256,
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("cut");
        assert_eq!(seeded, parts, "what is gathered first comes first");
        assert!(rechunked(BytesMut::new(), streamed(&[], 100, None), 256)
            .try_collect::<Vec<_>>()
            .await
            .expect("cut")
            .is_empty());
    }

    #[tokio::test]
    async fn a_large_stream_goes_up_in_parts_and_a_small_one_whole_both_byte_identical() {
        let store = Probe::default();
        for (key, size) in [("large.tar.gz", 5000), ("small.tar.gz", 1024), ("empty", 0)] {
            let data = payload(size);
            let uploaded = put_stream_in(
                &store,
                "memory",
                Keys::Encoded,
                key,
                streamed(&data, 300, None),
                typed("application/gzip"),
                SMALL,
                UNCUT,
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
        assert_eq!(store.counts.get(), [2, 1, 0]);
    }

    #[tokio::test]
    async fn a_stream_that_ends_in_an_error_stores_nothing_and_returns_that_error() {
        let store = Probe::default();
        for (key, size) in [("large", 5000), ("small", 500)] {
            let refused = put_stream_in(
                &store,
                "memory",
                Keys::Encoded,
                key,
                streamed(&payload(size), 300, Some("not the content it claims")),
                PutOptions::default(),
                SMALL,
                UNCUT,
            )
            .await
            .expect_err("refused");
            assert_eq!(refused.to_string(), "not the content it claims");
            assert!(store.get(&OsPath::from(key)).await.is_err(), "{key}");
        }
        assert_eq!(store.aborted().await, 1);
        assert_eq!(store.counts.get(), [0, 1, 1]);
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
    fn only_a_configured_retry_budget_below_the_request_timeout_warns() {
        let secs = Duration::from_secs;
        assert!(!timed_out_requests_cannot_retry(false, secs(20), secs(30)));
        assert!(timed_out_requests_cannot_retry(true, secs(20), secs(30)));
        assert!(!timed_out_requests_cannot_retry(true, secs(30), secs(30)));
        assert!(!timed_out_requests_cannot_retry(true, secs(120), secs(60)));
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

    const STALL: Stall = Stall {
        timeout: Duration::from_secs(1),
        attempts: 3,
        ceiling: CEILING,
        completion: Duration::from_secs(2),
    };

    async fn stored(probe: &Probe, key: &str, data: &[u8]) {
        probe
            .inner
            .put(&OsPath::from(key), PutPayload::from(data.to_vec()))
            .await
            .expect("stored");
    }

    fn offsets(probe: &Probe) -> Vec<Option<u64>> {
        probe.counts.offsets.lock().expect("offsets").clone()
    }

    fn reads(answers: &'static [Answer]) -> Arc<Probe> {
        Arc::new(Probe::failing(Fault::Reads {
            answers,
            etag: true,
        }))
    }

    async fn read_with(probe: &Arc<Probe>, key: &str, stall: Stall) -> eyre::Result<Vec<u8>> {
        let reader = open_read(
            Arc::clone(probe) as Arc<dyn ObjectStore>,
            "memory",
            Keys::Encoded,
            "bucket",
            key,
            stall,
        )
        .await?
        .expect("found");
        reader
            .stream
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
    }

    async fn read(probe: &Arc<Probe>, key: &str) -> eyre::Result<Vec<u8>> {
        read_with(probe, key, STALL).await
    }

    async fn read_back(answers: &'static [Answer]) -> (eyre::Result<Vec<u8>>, Vec<Option<u64>>) {
        let probe = reads(answers);
        stored(&probe, "site/bundle", &payload(1000)).await;
        let read = read(&probe, "site/bundle").await;
        (read, offsets(&probe))
    }

    fn failed_with(read: eyre::Result<Vec<u8>>, cause: &str) {
        let failed = read.expect_err("failed");
        assert!(format!("{failed:#}").contains(cause), "{failed:#}");
    }

    #[test]
    fn stall_settings_default_and_parse() {
        super::test_env::with(
            &[
                (ENV_STALL_TIMEOUT_SECS, None),
                (ENV_STALL_ATTEMPTS, None),
                (ENV_REQUEST_TIMEOUT_SECS, None),
                (ENV_COMPLETION_TIMEOUT_SECS, None),
            ],
            || {
                let stall = Stall::configured();
                assert_eq!(
                    stall,
                    Stall {
                        timeout: Duration::from_secs(20),
                        attempts: 3,
                        ceiling: Duration::from_secs(30),
                        completion: Duration::from_secs(20),
                    }
                );
                assert!(stall.cuts(stall.timeout));
                assert!(stall.cuts(stall.completion));
                assert!(!stall.outlasts_ceiling());
            },
        );
        super::test_env::with(
            &[
                (ENV_STALL_TIMEOUT_SECS, Some("0")),
                (ENV_STALL_ATTEMPTS, Some("many")),
                (ENV_REQUEST_TIMEOUT_SECS, None),
                (ENV_COMPLETION_TIMEOUT_SECS, Some("-1")),
            ],
            || {
                assert_eq!(Stall::configured().timeout, Duration::from_secs(20));
                assert_eq!(Stall::configured().attempts, 3);
                assert_eq!(Stall::configured().completion, Duration::from_secs(20));
            },
        );
        super::test_env::with(
            &[
                (ENV_STALL_TIMEOUT_SECS, Some("7")),
                (ENV_COMPLETION_TIMEOUT_SECS, None),
            ],
            || assert_eq!(Stall::configured().completion, Duration::from_secs(7)),
        );
        super::test_env::with(&[(ENV_COMPLETION_TIMEOUT_SECS, Some("25"))], || {
            assert_eq!(Stall::configured().completion, Duration::from_secs(25));
        });
    }

    #[test]
    fn a_part_is_given_the_stall_timeout_for_each_part_of_its_bytes() {
        let part = 8 * 1024 * 1024;
        assert_eq!(STALL.for_write(1).timeout(), Duration::from_secs(1));
        assert_eq!(STALL.for_write(part).timeout(), Duration::from_secs(1));
        assert_eq!(STALL.for_write(part + 1).timeout(), Duration::from_secs(2));
    }

    #[test]
    fn a_completion_bound_not_below_the_request_timeout_is_warned_of_only_when_set() {
        let long = Stall {
            completion: CEILING,
            ..STALL
        };
        assert!(completions_are_never_cut(true, long));
        assert!(!completions_are_never_cut(false, long));
        assert!(!completions_are_never_cut(true, STALL));
        assert!(!completions_are_never_cut(true, UNCUT));
    }

    #[test]
    fn a_deadline_not_below_the_request_timeout_or_one_attempt_cuts_nothing_short() {
        let at_ceiling = Stall {
            timeout: CEILING,
            ..STALL
        };
        assert!(!at_ceiling.cuts(at_ceiling.timeout));
        assert!(at_ceiling.outlasts_ceiling());
        assert!(!STALL.cuts(CEILING));

        let once = Stall {
            attempts: 1,
            ..STALL
        };
        assert!(!once.cuts(once.timeout));
        assert!(!once.outlasts_ceiling());
    }

    #[test]
    fn an_upload_has_the_stall_timeout_for_each_part_of_its_bytes() {
        let part = Multipart::DEFAULT.min_part_size;
        let deadlines =
            [0, 100, part, part + 1, 4 * part].map(|size| STALL.for_upload(size, part).timeout);
        assert_eq!(deadlines, [1, 1, 1, 2, 4].map(Duration::from_secs));
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_that_makes_no_progress_is_sent_again_and_stored() {
        let store = Probe::failing(Fault::StalledWrites(1));
        let data = payload(100);
        put_in(
            &store,
            "memory",
            Keys::Encoded,
            "site/part",
            data.clone(),
            PutOptions::default(),
            SMALL,
            STALL,
        )
        .await
        .expect("uploaded");
        assert_eq!(store.counts.whole.load(Ordering::SeqCst), 2);
        let got = store.get(&OsPath::from("site/part")).await.expect("stored");
        assert_eq!(got.bytes().await.expect("body").to_vec(), data);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_write_is_cut_short_only_until_its_last_attempt_which_has_the_ceiling() {
        let store = Probe::failing(Fault::SlowWrites(Duration::from_secs(5)));
        put_in(
            &store,
            "memory",
            Keys::Encoded,
            "site/slow",
            payload(100),
            PutOptions::default(),
            SMALL,
            STALL,
        )
        .await
        .expect("uploaded on its last attempt");
        assert_eq!(store.counts.whole.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_create_only_write_is_never_sent_again() {
        let store = Probe::failing(Fault::SlowWrites(Duration::from_secs(5)));
        put_in(
            &store,
            "memory",
            Keys::Encoded,
            "site/once",
            payload(100),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
            SMALL,
            STALL,
        )
        .await
        .expect("uploaded");
        assert_eq!(store.counts.whole.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_read_resumes_from_its_next_byte_by_its_etag() {
        let data = payload(1000);
        let (read, asked) = read_back(&[Answer::Stalls(100)]).await;
        assert_eq!(read.expect("read"), data);
        assert_eq!(asked, [None, Some(100)]);

        let downloaded = reads(&[Answer::Stalls(100)]);
        stored(&downloaded, "site/bundle", &data).await;
        let bytes = get_bytes(
            Arc::clone(&downloaded) as Arc<dyn ObjectStore>,
            "memory",
            Keys::Encoded,
            None,
            "site/bundle",
            STALL,
        )
        .await
        .expect("downloaded");
        assert_eq!(bytes, data);
        assert_eq!(offsets(&downloaded), [None, Some(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_whose_body_ends_early_resumes_from_its_next_byte() {
        let (read, offsets) = read_back(&[Answer::Ends(100)]).await;
        assert_eq!(read.expect("read"), payload(1000));
        assert_eq!(offsets, [None, Some(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_cannot_resume_fails_on_a_body_that_ends_early() {
        let probe = reads(&[Answer::Ends(100)]);
        stored(&probe, "site/short", &payload(1000)).await;
        failed_with(
            read_with(&probe, "site/short", UNCUT).await,
            "the body ended at byte 100 of 1000",
        );
        assert_eq!(offsets(&probe), [None]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_open_and_a_resume_that_never_answer_are_sent_again() {
        let (read, offsets) = read_back(&[Answer::Hangs]).await;
        assert_eq!(read.expect("read"), payload(1000));
        assert_eq!(offsets, [None, None]);

        let (read, offsets) = read_back(&[Answer::Stalls(100), Answer::Hangs]).await;
        assert_eq!(read.expect("read"), payload(1000));
        assert_eq!(offsets, [None, Some(100), Some(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_keeps_moving_resumes_as_often_as_it_stalls() {
        let data = payload(1000);
        let probe = Arc::new(Probe::failing(Fault::Trickle(100)));
        stored(&probe, "site/trickle", &data).await;
        assert_eq!(read(&probe, "site/trickle").await.expect("read"), data);
        assert_eq!(
            offsets(&probe),
            std::iter::once(None)
                .chain((1..10).map(|part| Some(part * 100)))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn empty_frames_are_not_progress() {
        let (read, offsets) = read_back(&[Answer::Empties(100)]).await;
        assert_eq!(read.expect("read"), payload(1000));
        assert_eq!(offsets, [None, Some(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_last_attempt_of_a_read_waits_for_the_ceiling() {
        const PAUSE: Answer = Answer::Pauses(Duration::from_secs(5));
        let (read, offsets) = read_back(&[PAUSE, PAUSE, PAUSE]).await;
        assert_eq!(read.expect("read"), payload(1000));
        assert_eq!(offsets, [None, Some(0), Some(0)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_stalls_on_every_attempt_in_a_row_fails_at_the_cap() {
        let (read, offsets) =
            read_back(&[Answer::Stalls(100), Answer::Stalls(0), Answer::Stalls(0)]).await;
        failed_with(read, "the request timed out");
        assert_eq!(offsets, [None, Some(100), Some(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_answered_without_an_etag_is_never_cut_short() {
        let probe = Arc::new(Probe::failing(Fault::Reads {
            answers: &[Answer::Stalls(100)],
            etag: false,
        }));
        stored(&probe, "site/unpinned", &payload(1000)).await;
        failed_with(read(&probe, "site/unpinned").await, "the request timed out");
        assert_eq!(offsets(&probe), [None]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_resumed_answer_from_another_byte_fails_the_read() {
        let (read, offsets) = read_back(&[Answer::Ends(100), Answer::Whole]).await;
        failed_with(
            read,
            "the resumed read answered from byte 0 of 1000, not 100 of 1000",
        );
        assert_eq!(offsets, [None, Some(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_resumed_answer_from_an_object_of_another_size_fails_the_read() {
        let probe = reads(&[Answer::Ends(100), Answer::Other("site/other")]);
        stored(&probe, "site/bundle", &payload(1000)).await;
        stored(&probe, "site/other", &payload(900)).await;
        failed_with(
            read(&probe, "site/bundle").await,
            "the resumed read answered from byte 100 of 900, not 100 of 1000",
        );
        assert_eq!(offsets(&probe), [None, Some(100)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_of_an_object_that_changed_fails_instead_of_mixing_versions() {
        let probe = reads(&[Answer::Stalls(100)]);
        stored(&probe, "site/changed", &payload(1000)).await;
        let mut reader = open_read(
            Arc::clone(&probe) as Arc<dyn ObjectStore>,
            "memory",
            Keys::Encoded,
            "bucket",
            "site/changed",
            STALL,
        )
        .await
        .expect("opened")
        .expect("found");
        let first = reader.stream.try_next().await.expect("first chunk");
        assert_eq!(first.map(|chunk| chunk.len()), Some(100));
        stored(&probe, "site/changed", &payload(900)).await;
        let failed = reader.stream.try_next().await.expect_err("changed");
        assert!(
            failed
                .to_string()
                .contains("the object changed while it was read"),
            "{failed}"
        );
    }
}
