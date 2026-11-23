//! A store whose multipart uploads send each part first on a client that
//! cuts a stalled answer short, and bound their completion on its own.

use crate::common::{self, Stall};
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use futures_util::FutureExt;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    UploadPart,
};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// Serves every request through `store`, except a multipart upload's parts,
/// which go first through `cut` when there is one: a client of the same
/// store whose read timeout cuts an answer not received in time. A part
/// `cut` could not send goes once more through `store`, bounded by the
/// request timeout. The upload is started, completed and aborted through
/// `store`, its completion cut short after the stall's completion timeout.
#[derive(Debug)]
pub(crate) struct Parted<S> {
    store: Arc<S>,
    cut: Option<Arc<S>>,
    backend: &'static str,
    stall: Stall,
}

impl<S> Parted<S> {
    pub(crate) fn new(
        store: Arc<S>,
        cut: Option<Arc<S>>,
        backend: &'static str,
        stall: Stall,
    ) -> Self {
        Self {
            store,
            cut,
            backend,
            stall,
        }
    }
}

impl<S: std::fmt::Display> std::fmt::Display for Parted<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Parted({})", self.store)
    }
}

#[async_trait]
impl<S: ObjectStore + MultipartStore> ObjectStore for Parted<S> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let id = self.store.create_multipart_opts(location, opts).await?;
        Ok(Box::new(Upload {
            store: Arc::clone(&self.store),
            cut: self.cut.clone(),
            backend: self.backend,
            stall: self.stall,
            path: location.clone(),
            id,
            next: 0,
            parts: Arc::default(),
            sent: Arc::default(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.store.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.store.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.store.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct Upload<S> {
    store: Arc<S>,
    cut: Option<Arc<S>>,
    backend: &'static str,
    stall: Stall,
    path: Path,
    id: MultipartId,
    next: usize,
    parts: Arc<Mutex<Vec<Option<PartId>>>>,
    sent: Arc<AtomicU64>,
}

impl<S> Upload<S> {
    fn sent_parts(&self) -> object_store::Result<Vec<PartId>> {
        let parts = self.parts.lock().unwrap_or_else(PoisonError::into_inner);
        (0..self.next)
            .map(|index| {
                parts
                    .get(index)
                    .cloned()
                    .flatten()
                    .ok_or_else(|| object_store::Error::Generic {
                        store: self.backend,
                        source: format!(
                            "part {index} of the upload of {} was not stored",
                            self.path
                        )
                        .into(),
                    })
            })
            .collect()
    }
}

/// Whether sending a part again can answer what its last attempt did not: a
/// refusal of the request itself (a missing upload, a failed precondition,
/// a denied permission or an unsupported operation) answers the same again.
fn worth_resending(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::Generic { .. })
}

async fn send_part<S: MultipartStore>(
    store: &S,
    cut: Option<&S>,
    backend: &'static str,
    (path, id, index): (&Path, &MultipartId, usize),
    data: PutPayload,
) -> object_store::Result<PartId> {
    match cut {
        None => store.put_part(path, id, index, data).await,
        Some(cut) => match cut.put_part(path, id, index, data.clone()).await {
            Err(error) if worth_resending(&error) => {
                tracing::warn!(
                    backend,
                    location = %path,
                    part = index,
                    error = %error,
                    "A part of a storage upload was not answered in time; sending it once more"
                );
                store.put_part(path, id, index, data).await
            }
            answered => answered,
        },
    }
}

/// What a completion answers when it was sent again: a store that completed
/// the upload before its answer was lost no longer knows it, and the upload
/// stands completed only when the object it stored is the size sent.
async fn completed_before<S: ObjectStore>(
    store: &S,
    backend: &'static str,
    path: &Path,
    sent: u64,
    missing: object_store::Error,
) -> object_store::Result<PutResult> {
    let stored = store.head(path).await.map_err(|_| missing)?;
    if stored.size == sent {
        Ok(PutResult {
            e_tag: stored.e_tag,
            version: stored.version,
            extensions: object_store::Extensions::default(),
        })
    } else {
        Err(object_store::Error::Generic {
            store: backend,
            source: format!(
                "the upload of {path} was no longer open, and the object stored there holds {} bytes, not the {sent} sent",
                stored.size
            )
            .into(),
        })
    }
}

#[async_trait]
impl<S: ObjectStore + MultipartStore> MultipartUpload for Upload<S> {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let index = self.next;
        self.next += 1;
        let (store, cut, backend) = (Arc::clone(&self.store), self.cut.clone(), self.backend);
        let (path, id) = (self.path.clone(), self.id.clone());
        let (parts, sent_bytes) = (Arc::clone(&self.parts), Arc::clone(&self.sent));
        async move {
            let size = u64::try_from(data.content_length()).unwrap_or(u64::MAX);
            let part = send_part(
                store.as_ref(),
                cut.as_deref(),
                backend,
                (&path, &id, index),
                data,
            )
            .await?;
            let mut parts = parts.lock().unwrap_or_else(PoisonError::into_inner);
            if parts.len() <= index {
                parts.resize(index + 1, None);
            }
            if let Some(slot) = parts.get_mut(index) {
                *slot = Some(part);
            }
            sent_bytes.fetch_add(size, Ordering::Relaxed);
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = self.sent_parts()?;
        let sent = self.sent.load(Ordering::Relaxed);
        let tried = AtomicU32::new(0);
        let (store, backend, path, id) = (self.store.as_ref(), self.backend, &self.path, &self.id);
        common::attempted(
            self.stall,
            self.stall.completion(),
            self.backend,
            "complete",
            path.as_ref(),
            || {
                let again = tried.fetch_add(1, Ordering::Relaxed) > 0;
                let parts = parts.clone();
                async move {
                    match store.complete_multipart(path, id, parts).await {
                        Err(missing @ object_store::Error::NotFound { .. }) if again => {
                            completed_before(store, backend, path, sent, missing).await
                        }
                        answered => answered,
                    }
                }
            },
        )
        .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.store.abort_multipart(&self.path, &self.id).await
    }
}

#[cfg(test)]
mod tests {
    use super::{Parted, Stall};
    use async_trait::async_trait;
    use futures_util::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::multipart::{MultipartStore, PartId};
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta,
        ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    const COMPLETION: Duration = Duration::from_secs(2);

    const STALL: Stall = Stall::of(
        Duration::from_secs(1),
        3,
        Duration::from_secs(30),
        COMPLETION,
    );

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    enum Fault {
        #[default]
        None,
        PartsTimeOut,
        PartsRefused,
        CompletionHangs(usize),
        CompletionAnswerLost,
        CompletionHangsThenGone,
        CompletionGone,
    }

    #[derive(Debug, Default)]
    struct Counts {
        parts: AtomicUsize,
        completions: AtomicUsize,
        aborts: AtomicUsize,
    }

    #[derive(Debug)]
    struct Faulty {
        inner: Arc<InMemory>,
        fault: Fault,
        counts: Counts,
    }

    impl Faulty {
        fn over(inner: &Arc<InMemory>, fault: Fault) -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::clone(inner),
                fault,
                counts: Counts::default(),
            })
        }

        fn count(&self, of: impl Fn(&Counts) -> &AtomicUsize) -> usize {
            of(&self.counts).load(Ordering::SeqCst)
        }
    }

    impl std::fmt::Display for Faulty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Faulty({})", self.inner)
        }
    }

    fn generic(message: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "faulty",
            source: message.into(),
        }
    }

    fn gone(path: &Path) -> object_store::Error {
        object_store::Error::NotFound {
            path: path.to_string(),
            source: "the upload is no longer open".into(),
        }
    }

    #[async_trait]
    impl ObjectStore for Faulty {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[async_trait]
    impl MultipartStore for Faulty {
        async fn create_multipart(&self, path: &Path) -> object_store::Result<MultipartId> {
            self.inner.create_multipart(path).await
        }

        async fn create_multipart_opts(
            &self,
            path: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<MultipartId> {
            self.inner.create_multipart_opts(path, opts).await
        }

        async fn put_part(
            &self,
            path: &Path,
            id: &MultipartId,
            part_idx: usize,
            data: PutPayload,
        ) -> object_store::Result<PartId> {
            self.counts.parts.fetch_add(1, Ordering::SeqCst);
            match self.fault {
                Fault::PartsTimeOut => Err(generic("the request timed out")),
                Fault::PartsRefused => Err(object_store::Error::PermissionDenied {
                    path: path.to_string(),
                    source: "the request was refused".into(),
                }),
                _ => self.inner.put_part(path, id, part_idx, data).await,
            }
        }

        async fn complete_multipart(
            &self,
            path: &Path,
            id: &MultipartId,
            parts: Vec<PartId>,
        ) -> object_store::Result<PutResult> {
            let asked = self.counts.completions.fetch_add(1, Ordering::SeqCst);
            match self.fault {
                Fault::CompletionHangs(hung) if asked < hung => {
                    futures_util::future::pending().await
                }
                Fault::CompletionAnswerLost if asked == 0 => {
                    self.inner.complete_multipart(path, id, parts).await?;
                    futures_util::future::pending().await
                }
                Fault::CompletionHangsThenGone if asked == 0 => {
                    futures_util::future::pending().await
                }
                Fault::CompletionAnswerLost
                | Fault::CompletionHangsThenGone
                | Fault::CompletionGone => Err(gone(path)),
                _ => self.inner.complete_multipart(path, id, parts).await,
            }
        }

        async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> object_store::Result<()> {
            self.counts.aborts.fetch_add(1, Ordering::SeqCst);
            self.inner.abort_multipart(path, id).await
        }
    }

    const KEY: &str = "site/bundle";

    fn parts() -> [Vec<u8>; 2] {
        [vec![7; 300], vec![9; 120]]
    }

    async fn uploaded(parted: &Parted<Faulty>) -> object_store::Result<PutResult> {
        let mut upload = parted
            .put_multipart_opts(&Path::from(KEY), PutMultipartOptions::default())
            .await?;
        let sent = parts().map(|part| upload.put_part(PutPayload::from(part)));
        futures_util::future::try_join_all(sent).await?;
        upload.complete().await
    }

    async fn stored(store: &InMemory) -> Option<Vec<u8>> {
        let got = store.get(&Path::from(KEY)).await.ok()?;
        got.bytes().await.ok().map(|bytes| bytes.to_vec())
    }

    #[tokio::test]
    async fn a_part_the_cutting_client_could_not_send_goes_once_more_through_the_store() {
        let memory = Arc::new(InMemory::new());
        let (store, cut) = (
            Faulty::over(&memory, Fault::None),
            Faulty::over(&memory, Fault::PartsTimeOut),
        );
        let parted = Parted::new(Arc::clone(&store), Some(Arc::clone(&cut)), "faulty", STALL);
        uploaded(&parted).await.expect("uploaded");
        assert_eq!(stored(&memory).await, Some(parts().concat()));
        assert_eq!([cut.count(|c| &c.parts), store.count(|c| &c.parts)], [2, 2]);
    }

    #[tokio::test]
    async fn every_part_goes_through_the_cutting_client_when_it_answers() {
        let memory = Arc::new(InMemory::new());
        let (store, cut) = (
            Faulty::over(&memory, Fault::None),
            Faulty::over(&memory, Fault::None),
        );
        let parted = Parted::new(Arc::clone(&store), Some(Arc::clone(&cut)), "faulty", STALL);
        uploaded(&parted).await.expect("uploaded");
        assert_eq!(stored(&memory).await, Some(parts().concat()));
        assert_eq!([cut.count(|c| &c.parts), store.count(|c| &c.parts)], [2, 0]);
        assert_eq!(store.count(|c| &c.completions), 1);
    }

    #[tokio::test]
    async fn a_refused_part_is_not_sent_again() {
        let memory = Arc::new(InMemory::new());
        let (store, cut) = (
            Faulty::over(&memory, Fault::None),
            Faulty::over(&memory, Fault::PartsRefused),
        );
        let parted = Parted::new(Arc::clone(&store), Some(Arc::clone(&cut)), "faulty", STALL);
        let refused = uploaded(&parted).await.expect_err("refused");
        assert!(matches!(
            refused,
            object_store::Error::PermissionDenied { .. }
        ));
        assert_eq!(store.count(|c| &c.parts), 0);
        assert_eq!(stored(&memory).await, None);
    }

    #[tokio::test]
    async fn without_a_cutting_client_every_part_goes_through_the_store() {
        let memory = Arc::new(InMemory::new());
        let store = Faulty::over(&memory, Fault::None);
        let parted = Parted::new(Arc::clone(&store), None, "faulty", STALL);
        uploaded(&parted).await.expect("uploaded");
        assert_eq!(stored(&memory).await, Some(parts().concat()));
        assert_eq!(store.count(|c| &c.parts), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_completion_that_never_answers_is_sent_again_after_its_own_timeout() {
        let memory = Arc::new(InMemory::new());
        let store = Faulty::over(&memory, Fault::CompletionHangs(1));
        let parted = Parted::new(Arc::clone(&store), None, "faulty", STALL);
        let started = tokio::time::Instant::now();
        uploaded(&parted).await.expect("uploaded");
        assert_eq!(started.elapsed(), COMPLETION);
        assert_eq!(store.count(|c| &c.completions), 2);
        assert_eq!(stored(&memory).await, Some(parts().concat()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_completion_that_never_answers_is_tried_at_most_the_stall_attempts() {
        let memory = Arc::new(InMemory::new());
        let store = Faulty::over(&memory, Fault::CompletionHangs(usize::MAX));
        let parted = Parted::new(Arc::clone(&store), None, "faulty", STALL);
        let waited = tokio::time::timeout(Duration::from_secs(600), uploaded(&parted)).await;
        assert!(waited.is_err(), "the last attempt is left to the ceiling");
        assert_eq!(store.count(|c| &c.completions), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_completion_whose_answer_was_lost_stands_when_the_object_is_the_size_sent() {
        let memory = Arc::new(InMemory::new());
        let store = Faulty::over(&memory, Fault::CompletionAnswerLost);
        let parted = Parted::new(Arc::clone(&store), None, "faulty", STALL);
        let completed = uploaded(&parted).await.expect("uploaded");
        let head = memory.head(&Path::from(KEY)).await.expect("stored");
        assert_eq!(completed.e_tag, head.e_tag);
        assert_eq!(store.count(|c| &c.completions), 2);
        assert_eq!(stored(&memory).await, Some(parts().concat()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_completion_resent_onto_an_object_of_another_size_fails() {
        let memory = Arc::new(InMemory::new());
        memory
            .put(&Path::from(KEY), PutPayload::from(vec![1; 5]))
            .await
            .expect("stored");
        let store = Faulty::over(&memory, Fault::CompletionHangsThenGone);
        let parted = Parted::new(Arc::clone(&store), None, "faulty", STALL);
        let failed = uploaded(&parted).await.expect_err("failed");
        assert_eq!(
            failed.to_string(),
            "Generic faulty error: the upload of site/bundle was no longer open, and the object stored there holds 5 bytes, not the 420 sent"
        );
        assert_eq!(store.count(|c| &c.completions), 2);
        assert_eq!(stored(&memory).await, Some(vec![1; 5]));
    }

    #[tokio::test]
    async fn a_first_completion_that_finds_no_upload_fails_even_over_an_object_of_its_size() {
        let memory = Arc::new(InMemory::new());
        memory
            .put(&Path::from(KEY), PutPayload::from(parts().concat()))
            .await
            .expect("stored");
        let store = Faulty::over(&memory, Fault::CompletionGone);
        let parted = Parted::new(Arc::clone(&store), None, "faulty", STALL);
        let failed = uploaded(&parted).await.expect_err("failed");
        assert!(matches!(failed, object_store::Error::NotFound { .. }));
        assert_eq!(store.count(|c| &c.completions), 1);
    }

    #[tokio::test]
    async fn an_aborted_upload_is_aborted_through_the_store() {
        let memory = Arc::new(InMemory::new());
        let (store, cut) = (
            Faulty::over(&memory, Fault::None),
            Faulty::over(&memory, Fault::None),
        );
        let parted = Parted::new(Arc::clone(&store), Some(Arc::clone(&cut)), "faulty", STALL);
        let mut upload = parted
            .put_multipart_opts(&Path::from(KEY), PutMultipartOptions::default())
            .await
            .expect("started");
        upload.abort().await.expect("aborted");
        assert_eq!(
            [store.count(|c| &c.aborts), cut.count(|c| &c.aborts)],
            [1, 0]
        );
    }
}
