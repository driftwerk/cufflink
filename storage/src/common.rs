use crate::{ObjectMeta, ObjectReader, UploadOptions, Uploaded};
use bytes::Bytes;
use eyre::{Result, WrapErr};
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    path::Path as OsPath, Attribute, AttributeValue, Attributes, GetOptions, ObjectStore,
    ObjectStoreExt, PutOptions, PutPayload,
};
use std::time::Duration;

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

const ENV_RETRY_TIMEOUT_SECS: &str = "OBJECT_STORE_RETRY_TIMEOUT_SECS";

pub(crate) fn retry_timeout() -> Duration {
    let secs = std::env::var(ENV_RETRY_TIMEOUT_SECS)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_RETRY_TIMEOUT_SECS);
    Duration::from_secs(secs)
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
    key: &str,
    data: Vec<u8>,
    options: &UploadOptions,
) -> Result<Uploaded> {
    let opts = PutOptions {
        attributes: attributes(options),
        ..Default::default()
    };
    put_with(store, backend, key, data, opts).await
}

pub(crate) async fn put_with(
    store: &dyn ObjectStore,
    backend: &'static str,
    key: &str,
    data: Vec<u8>,
    opts: PutOptions,
) -> Result<Uploaded> {
    let path = OsPath::from(key);
    let size = data.len() as u64;
    let payload = PutPayload::from(Bytes::from(data));
    let stored = store
        .put_opts(&path, payload, opts)
        .await
        .wrap_err_with(|| format!("{backend} upload failed for {key}"))?;
    tracing::info!(backend, key = %key, "Uploaded");
    Ok(Uploaded {
        size,
        etag: stored.e_tag,
    })
}

fn content_type(attributes: &Attributes) -> Option<String> {
    attributes
        .get(&Attribute::ContentType)
        .map(|content_type| content_type.as_ref().to_string())
}

pub(crate) async fn head_meta(
    store: &dyn ObjectStore,
    backend: &'static str,
    bucket: &str,
    key: &str,
) -> Result<Option<ObjectMeta>> {
    let head = GetOptions {
        head: true,
        ..Default::default()
    };
    match store.get_opts(&OsPath::from(key), head).await {
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
    bucket: Option<&str>,
    key: &str,
) -> Result<Vec<u8>> {
    let path = OsPath::from(key);
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
    bucket: &str,
    key: &str,
) -> Result<Option<ObjectReader>> {
    let location = format!("{bucket}/{key}");
    match store.get(&OsPath::from(key)).await {
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
    key: &str,
) -> Result<u64> {
    let path = OsPath::from(key);
    let meta = store
        .head(&path)
        .await
        .wrap_err_with(|| format!("{backend} head failed for {key}"))?;
    Ok(meta.size)
}

pub(crate) async fn copy_object(
    store: &dyn ObjectStore,
    backend: &'static str,
    src_key: &str,
    dst_key: &str,
) -> Result<()> {
    let from = OsPath::from(src_key);
    let to = OsPath::from(dst_key);
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
    key: &str,
) -> Result<()> {
    let path = OsPath::from(key);
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
    key: &str,
) -> Result<()> {
    let path = OsPath::from(key);
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
    method: http::Method,
    key: &str,
    expires_in_secs: u64,
) -> Result<String> {
    let path = OsPath::from(key);
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
    use super::{retry_timeout, ENV_RETRY_TIMEOUT_SECS};
    use std::time::Duration;

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
}
