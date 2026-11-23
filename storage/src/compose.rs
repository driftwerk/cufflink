//! An object GCS composes from objects it already holds. A compose takes at
//! most 32 sources, so more are composed in tiers through intermediate
//! objects beside the key, removed once the object is stored or has failed.

use crate::common::{self, Completed, Keys, PartClock, Stall};
use crate::copied::{self, COPIES_AT_ONCE};
use crate::{StoredPart, Uploaded};
use bytes::Bytes;
use eyre::Result;
use futures_util::stream::{self, BoxStream};
use futures_util::{future, StreamExt, TryFutureExt, TryStreamExt};
use object_store::client::{HttpClient, HttpRequestBody};
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path;
use object_store::{CredentialProvider, ObjectStoreExt, PutResult};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
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
        let tiers = self
            .tiers(&path, sources, content_type, &clock, &made)
            .map_err(&failed);
        let (last, ()) = future::try_join(tiers, copied::drained(data)).await?;
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

    async fn tiers(
        &self,
        path: &Path,
        sources: Vec<Path>,
        content_type: &str,
        clock: &PartClock,
        made: &Intermediates,
    ) -> object_store::Result<Vec<Path>> {
        let mut sources = sources;
        let mut tier = 0;
        while sources.len() > MAX_SOURCES {
            tier += 1;
            let composed = stream::iter(sources.chunks(MAX_SOURCES).enumerate().map(
                |(index, group)| {
                    let into = made.add(Path::from(format!("{path}.compose/{tier}-{index}")));
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
                },
            ))
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
        let generic =
            |source: Box<dyn std::error::Error + Send + Sync>| object_store::Error::Generic {
                store: BACKEND,
                source,
            };
        let credential = self.store.credentials().get_credential().await?;
        let body = serde_json::json!({
            "sourceObjects": sources
                .iter()
                .map(|source| serde_json::json!({ "name": source.as_ref() }))
                .collect::<Vec<_>>(),
            "destination": { "contentType": content_type },
        });
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(format!(
                "{}/storage/v1/b/{}/o/{}/compose",
                self.api.base_url,
                utf8_percent_encode(&self.bucket, NON_ALPHANUMERIC),
                utf8_percent_encode(into.as_ref(), NON_ALPHANUMERIC)
            ))
            .header(http::header::CONTENT_TYPE, "application/json");
        let request = match credential.bearer.as_str() {
            "" => request,
            bearer => request.header(http::header::AUTHORIZATION, format!("Bearer {bearer}")),
        }
        .body(HttpRequestBody::from(body.to_string()))
        .map_err(|e| generic(Box::new(e)))?;
        let answer = copied::sent(&self.api.client, BACKEND, into.as_ref(), request).await?;
        let composed: Composed =
            serde_json::from_slice(&answer).map_err(|e| generic(Box::new(e)))?;
        Ok(PutResult {
            e_tag: composed.etag,
            version: composed.generation,
            extensions: object_store::Extensions::default(),
        })
    }
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
