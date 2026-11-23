//! An object assembled from objects the store already holds, copied inside
//! the store so that none of their bytes are sent to it.

use bytes::Bytes;
use eyre::Result;
use futures_util::stream::BoxStream;
use futures_util::{future, TryStreamExt};
use object_store::client::{HttpClient, HttpConnector, HttpRequest, ReqwestConnector};

/// How many copies run at once: a copy sends no bytes, so it is bounded by
/// the store's work, not by the network.
pub(crate) const COPIES_AT_ONCE: usize = 8;

/// Reads `data` to its end, keeping none of it, and returns the first error
/// it yields as it was yielded.
pub(crate) async fn drained(data: BoxStream<'static, Result<Bytes>>) -> Result<()> {
    data.try_for_each(|_| future::ready(Ok(()))).await
}

pub(crate) fn client(options: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
    ReqwestConnector::default().connect(options)
}

/// Sends `request` and answers its body, or the store's refusal as the
/// storage error it stands for.
pub(crate) async fn sent(
    client: &HttpClient,
    backend: &'static str,
    path: &str,
    request: HttpRequest,
) -> object_store::Result<Bytes> {
    let generic = |source: object_store::client::HttpError| object_store::Error::Generic {
        store: backend,
        source: Box::new(source),
    };
    let answer = client.execute(request).await.map_err(generic)?;
    let status = answer.status();
    let body = answer.into_body().bytes().await.map_err(generic)?;
    if status.is_success() {
        return Ok(body);
    }
    let source = format!(
        "the store answered {status}: {}",
        String::from_utf8_lossy(&body)
    )
    .into();
    let path = path.to_string();
    Err(match status {
        http::StatusCode::NOT_FOUND => object_store::Error::NotFound { path, source },
        http::StatusCode::PRECONDITION_FAILED => object_store::Error::Precondition { path, source },
        http::StatusCode::UNAUTHORIZED => object_store::Error::Unauthenticated { path, source },
        http::StatusCode::FORBIDDEN => object_store::Error::PermissionDenied { path, source },
        _ => object_store::Error::Generic {
            store: backend,
            source,
        },
    })
}
