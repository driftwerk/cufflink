//! An object GCS composes from objects it already holds. A compose takes at
//! most 32 sources, so more are composed in tiers through intermediate
//! objects beside the key, removed once the object is stored or has failed.

use crate::common::{self, Completed, Keys, PartClock, Stall};
use crate::copied::{self, Refused, COPIES_AT_ONCE};
use crate::{StoredPart, Uploaded};
use bytes::Bytes;
use eyre::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::{future, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use object_store::client::{HttpClient, HttpRequestBody};
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutResult};
use percent_encoding::utf8_percent_encode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

const BACKEND: &str = "gcs";

pub(crate) const DEFAULT_BASE_URL: &str = "https://storage.googleapis.com";

const MAX_SOURCES: usize = 32;

/// How the store's JSON API is reached: its HTTP client and base URL,
/// shared by every bucket's composer.
#[derive(Debug, Clone)]
pub(crate) struct Api {
    pub(crate) client: HttpClient,
    pub(crate) base_url: String,
}

#[derive(Debug, Clone)]
pub(crate) struct Composer {
    pub(crate) store: Arc<GoogleCloudStorage>,
    pub(crate) api: Api,
    pub(crate) bucket: String,
    pub(crate) stall: Stall,
}

#[derive(serde::Deserialize)]
struct Composed {
    etag: Option<String>,
    generation: Option<String>,
}

impl Composer {
    /// Composes the objects at `parts` into `key` once `data`, their bytes,
    /// has been read to its end; the tiers below the last compose run while
    /// it is read.
    pub(crate) async fn assemble(
        &self,
        keys: Keys,
        key: &str,
        parts: Vec<StoredPart>,
        data: BoxStream<'static, Result<Bytes>>,
        content_type: &str,
    ) -> Result<Uploaded> {
        let failed = |e: object_store::Error| {
            eyre::Report::new(e).wrap_err(format!("{BACKEND} upload failed for {key}"))
        };
        let size = parts.iter().map(|part| part.size).sum();
        let path = keys.path(key)?;
        let sources = parts
            .iter()
            .map(|part| keys.path(&part.key))
            .collect::<Result<Vec<_>>>()?;
        let clock = PartClock::default();
        let made = Intermediates::new(Arc::clone(&self.store));
        let prefix = format!("{path}.compose/{}", nonce());
        let tiers = self
            .tiers(&prefix, sources, content_type, &clock, &made)
            .map_err(&failed);
        let last = copied::alongside(tiers, data).await?;
        let asked = tokio::time::Instant::now();
        let stored = self
            .compose(
                &path,
                &last,
                content_type,
                "complete",
                self.stall.completion(),
            )
            .await
            .map_err(&failed)?;
        Ok(Completed {
            stored,
            completion: Some(asked.elapsed()),
            parts: clock.times(),
        }
        .uploaded(BACKEND, key, size))
    }

    /// Composes `sources` in groups of 32 into intermediate objects under
    /// `prefix` until 32 or fewer remain; a group of one stays as it is.
    async fn tiers(
        &self,
        prefix: &str,
        mut sources: Vec<Path>,
        content_type: &str,
        clock: &PartClock,
        made: &Intermediates,
    ) -> object_store::Result<Vec<Path>> {
        let mut tier = 0;
        while sources.len() > MAX_SOURCES {
            tier += 1;
            let groups: Vec<BoxFuture<'_, object_store::Result<Path>>> = sources
                .chunks(MAX_SOURCES)
                .enumerate()
                .map(|(index, group)| match group {
                    [single] => future::ready(Ok(single.clone())).boxed(),
                    group => {
                        let into = made.add(Path::from(format!("{prefix}/{tier}-{index}")));
                        async move {
                            clock
                                .timed(self.compose(
                                    &into,
                                    group,
                                    content_type,
                                    "compose",
                                    self.stall.timeout(),
                                ))
                                .await
                                .map(|_| into)
                        }
                        .boxed()
                    }
                })
                .collect();
            let composed = stream::iter(groups)
                .buffered(COPIES_AT_ONCE)
                .try_collect::<Vec<_>>()
                .await?;
            sources = composed;
        }
        Ok(sources)
    }

    async fn compose(
        &self,
        into: &Path,
        sources: &[Path],
        content_type: &str,
        request: &'static str,
        deadline: Duration,
    ) -> object_store::Result<PutResult> {
        common::attempted(
            self.stall,
            deadline,
            BACKEND,
            request,
            into.as_ref(),
            || self.composed(into, sources, content_type),
        )
        .await
    }

    async fn composed(
        &self,
        into: &Path,
        sources: &[Path],
        content_type: &str,
    ) -> object_store::Result<PutResult> {
        let url = format!(
            "{}/storage/v1/b/{}/o/{}/compose",
            self.api.base_url,
            utf8_percent_encode(&self.bucket, &copied::STRICT),
            utf8_percent_encode(into.as_ref(), &copied::STRICT)
        );
        let body = serde_json::json!({
            "sourceObjects": sources
                .iter()
                .map(|source| serde_json::json!({ "name": source.as_ref() }))
                .collect::<Vec<_>>(),
            "destination": { "contentType": content_type },
        })
        .to_string();
        let (url, body) = (url.as_str(), body.as_str());
        let request = || async move {
            let credential = self.store.credentials().get_credential().await?;
            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri(url)
                .header(http::header::CONTENT_TYPE, "application/json");
            match credential.bearer.as_str() {
                "" => request,
                bearer => request.header(http::header::AUTHORIZATION, format!("Bearer {bearer}")),
            }
            .body(HttpRequestBody::from(body.to_string()))
            .map_err(|e| copied::generic(BACKEND, e))
        };
        let location = match sources {
            [source] => format!("{source} composed into {into}"),
            _ => format!(
                "{} sources, {} to {}, composed into {into}",
                sources.len(),
                sources.first().map(Path::to_string).unwrap_or_default(),
                sources.last().map(Path::to_string).unwrap_or_default()
            ),
        };
        let composed: Composed =
            copied::sent(&self.api.client, BACKEND, &location, request, |answer| {
                serde_json::from_slice(answer)
                    .map_err(|e| Refused::Final(copied::generic(BACKEND, e)))
            })
            .await?;
        Ok(PutResult {
            e_tag: composed.etag,
            version: composed.generation,
            extensions: object_store::Extensions::default(),
        })
    }
}

/// A name no other assembly's intermediate objects share, so two replicas
/// composing the same bundle at once never remove each other's.
fn nonce() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{now:x}-{:x}-{:x}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// The intermediate objects of one compose, removed in the background once
/// it has finished, whether it stored its object or failed.
struct Intermediates {
    store: Arc<GoogleCloudStorage>,
    made: Mutex<Vec<Path>>,
}

impl Intermediates {
    fn new(store: Arc<GoogleCloudStorage>) -> Self {
        Self {
            store,
            made: Mutex::default(),
        }
    }

    fn add(&self, path: Path) -> Path {
        self.made
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(path.clone());
        path
    }
}

impl Drop for Intermediates {
    fn drop(&mut self) {
        let made = std::mem::take(self.made.get_mut().unwrap_or_else(PoisonError::into_inner));
        if made.is_empty() {
            return;
        }
        let store = Arc::clone(&self.store);
        let removed = async move {
            stream::iter(made)
                .for_each_concurrent(COPIES_AT_ONCE, |path| {
                    let store = Arc::clone(&store);
                    async move {
                        match store.delete(&path).await {
                            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                            Err(e) => tracing::warn!(
                                backend = BACKEND,
                                key = %path,
                                error = %e,
                                "Failed to remove an intermediate object of a compose"
                            ),
                        }
                    }
                })
                .await;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn(removed)),
            Err(_) => tracing::warn!(
                backend = BACKEND,
                "No runtime to remove the intermediate objects of a compose on"
            ),
        }
    }
}
