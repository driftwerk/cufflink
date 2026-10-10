use crate::{common, ObjectMeta, ObjectReader, ObjectStorage, UploadOptions, Uploaded};
use async_trait::async_trait;
use eyre::{Result, WrapErr};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::prefix::PrefixStore;
use object_store::PutOptions;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct PrefixedBackend {
    store: Arc<dyn object_store::ObjectStore>,
    bucket: String,
    kind: &'static str,
    attributes: bool,
}

impl PrefixedBackend {
    pub(crate) fn memory(bucket: String) -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            bucket,
            kind: "memory",
            attributes: true,
        }
    }

    pub(crate) fn local(dir: &Path, bucket: String) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .wrap_err_with(|| format!("create local storage directory {}", dir.display()))?;
        let store = LocalFileSystem::new_with_prefix(dir)
            .wrap_err_with(|| format!("open local storage directory {}", dir.display()))?;
        Ok(Self {
            store: Arc::new(store),
            bucket,
            kind: "local",
            attributes: false,
        })
    }

    fn put_options(&self, options: &UploadOptions) -> PutOptions {
        if self.attributes {
            PutOptions {
                attributes: common::attributes(options),
                ..Default::default()
            }
        } else {
            PutOptions::default()
        }
    }

    fn store_for(&self, bucket: &str) -> PrefixStore<Arc<dyn object_store::ObjectStore>> {
        PrefixStore::new(Arc::clone(&self.store), bucket)
    }

    fn primary(&self) -> PrefixStore<Arc<dyn object_store::ObjectStore>> {
        self.store_for(&self.bucket)
    }

    fn placeholder_url(&self, bucket: &str, key: &str, expires_in_secs: u64) -> String {
        format!("{}://{bucket}/{key}?expires={expires_in_secs}", self.kind)
    }
}

#[async_trait]
impl ObjectStorage for PrefixedBackend {
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
        common::put_with(
            &self.store_for(bucket),
            self.kind,
            key,
            data,
            self.put_options(options),
        )
        .await
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>> {
        common::get_bytes(&self.primary(), self.kind, None, key).await
    }

    async fn download_from_bucket(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        common::get_bytes(&self.store_for(bucket), self.kind, Some(bucket), key).await
    }

    async fn open_read(&self, bucket: &str, key: &str) -> Result<Option<ObjectReader>> {
        common::open_read(&self.store_for(bucket), self.kind, bucket, key).await
    }

    async fn head(&self, key: &str) -> Result<u64> {
        common::head_size(&self.primary(), self.kind, key).await
    }

    async fn head_in_bucket(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        common::head_meta(&self.store_for(bucket), self.kind, bucket, key).await
    }

    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<()> {
        common::copy_object(&self.primary(), self.kind, src_key, dst_key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        common::delete_object(&self.primary(), self.kind, key).await
    }

    async fn delete_from_bucket(&self, bucket: &str, key: &str) -> Result<()> {
        common::delete_object_idempotent(&self.store_for(bucket), self.kind, key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        common::delete_prefix(&self.primary(), self.kind, prefix).await
    }

    async fn presign_upload(
        &self,
        key: &str,
        _content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        Ok(self.placeholder_url(&self.bucket, key, expires_in_secs))
    }

    async fn presign_upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        _content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        Ok(self.placeholder_url(bucket, key, expires_in_secs))
    }

    async fn presign_download(&self, key: &str, expires_in_secs: u64) -> Result<String> {
        Ok(self.placeholder_url(&self.bucket, key, expires_in_secs))
    }

    async fn presign_download_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        Ok(self.placeholder_url(bucket, key, expires_in_secs))
    }

    fn primary_bucket(&self) -> &str {
        &self.bucket
    }

    fn backend_kind(&self) -> &'static str {
        self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn objects_round_trip_through_the_primary_bucket() {
        let backend = PrefixedBackend::memory("primary".into());
        backend
            .upload("a/b.wasm", b"hello".to_vec(), "application/wasm")
            .await
            .unwrap();
        assert_eq!(backend.head("a/b.wasm").await.unwrap(), 5);
        assert_eq!(backend.download("a/b.wasm").await.unwrap(), b"hello");
        backend.copy("a/b.wasm", "a/c.wasm").await.unwrap();
        assert_eq!(backend.download("a/c.wasm").await.unwrap(), b"hello");
        backend.delete("a/b.wasm").await.unwrap();
        assert!(backend.download("a/b.wasm").await.is_err());
        assert_eq!(backend.backend_kind(), "memory");
        assert_eq!(backend.primary_bucket(), "primary");
    }

    #[tokio::test]
    async fn buckets_are_isolated_and_deletes_are_idempotent() {
        let backend = PrefixedBackend::memory("primary".into());
        backend
            .upload_to_bucket("other", "k", b"x".to_vec(), "text/plain")
            .await
            .unwrap();
        assert!(backend.download("k").await.is_err());
        assert_eq!(
            backend.download_from_bucket("other", "k").await.unwrap(),
            b"x"
        );
        backend.delete_from_bucket("other", "k").await.unwrap();
        backend.delete_from_bucket("other", "k").await.unwrap();
        assert!(backend.download_from_bucket("other", "k").await.is_err());
    }

    async fn read_all(reader: ObjectReader) -> Vec<u8> {
        use futures_util::TryStreamExt;
        reader
            .stream
            .try_fold(Vec::new(), |mut all, chunk| async move {
                all.extend_from_slice(&chunk);
                Ok(all)
            })
            .await
            .expect("stream")
    }

    #[tokio::test]
    async fn objects_stream_with_their_size_and_content_type() {
        let memory = PrefixedBackend::memory("primary".into());
        memory
            .upload_to_bucket("media", "a.pdf", b"%PDF".to_vec(), "application/pdf")
            .await
            .unwrap();
        let reader = memory
            .open_read("media", "a.pdf")
            .await
            .unwrap()
            .expect("found");
        assert_eq!(reader.size, Some(4));
        assert_eq!(reader.content_type.as_deref(), Some("application/pdf"));
        assert_eq!(read_all(reader).await, b"%PDF");
        assert!(memory
            .open_read("media", "missing")
            .await
            .unwrap()
            .is_none());
        assert!(memory
            .open_read("primary", "a.pdf")
            .await
            .unwrap()
            .is_none());

        let dir = std::env::temp_dir().join(format!("cufflink-storage-open-{}", uuid()));
        let local = PrefixedBackend::local(&dir, "primary".into()).expect("local backend");
        local
            .upload("r.csv", b"a,b".to_vec(), "text/csv")
            .await
            .unwrap();
        let reader = local
            .open_read("primary", "r.csv")
            .await
            .unwrap()
            .expect("found");
        assert_eq!((reader.size, reader.content_type.clone()), (Some(3), None));
        assert_eq!(read_all(reader).await, b"a,b");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn an_upload_answers_what_it_stored_and_head_reads_it_back() {
        let memory = PrefixedBackend::memory("primary".into());
        let options = UploadOptions {
            content_type: "application/pdf".into(),
            content_disposition: Some("attachment; filename=\"receipt.pdf\"".into()),
            cache_control: Some("no-store".into()),
        };
        let before = chrono::Utc::now() - chrono::Duration::seconds(1);
        let uploaded = memory
            .upload_to_bucket_with(
                "receipts",
                "book/receipt_org.pdf",
                b"%PDF-1".to_vec(),
                &options,
            )
            .await
            .unwrap();
        assert_eq!(uploaded.size, 6);
        assert!(uploaded.etag.is_some());
        let meta = memory
            .head_in_bucket("receipts", "book/receipt_org.pdf")
            .await
            .unwrap()
            .expect("found");
        assert_eq!(meta.size, 6);
        assert_eq!(meta.content_type.as_deref(), Some("application/pdf"));
        assert_eq!(meta.etag, uploaded.etag);
        assert!(meta.last_modified >= before && meta.last_modified <= chrono::Utc::now());
        assert_eq!(
            memory
                .download_from_bucket("receipts", "book/receipt_org.pdf")
                .await
                .unwrap(),
            b"%PDF-1"
        );
        assert_eq!(
            common::attributes(&options).len(),
            3,
            "disposition and cache control are stored with the content type"
        );
        assert_eq!(
            common::attributes(&UploadOptions::content_type("text/plain")).len(),
            1
        );

        let replaced = memory
            .upload_to_bucket_with(
                "receipts",
                "book/receipt_org.pdf",
                b"%PDF-1.7".to_vec(),
                &UploadOptions::content_type("application/pdf"),
            )
            .await
            .unwrap();
        assert_eq!(replaced.size, 8);
        assert_ne!(replaced.etag, uploaded.etag);

        for (bucket, key) in [("receipts", "missing"), ("primary", "book/receipt_org.pdf")] {
            assert_eq!(memory.head_in_bucket(bucket, key).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn a_directory_keeps_the_bytes_and_no_attributes() {
        let dir = std::env::temp_dir().join(format!("cufflink-storage-head-{}", uuid()));
        let local = PrefixedBackend::local(&dir, "primary".into()).expect("local backend");
        let options = UploadOptions {
            content_type: "application/pdf".into(),
            content_disposition: Some("inline".into()),
            cache_control: None,
        };
        let uploaded = local
            .upload_to_bucket_with("receipts", "a/b.pdf", b"%PDF".to_vec(), &options)
            .await
            .unwrap();
        assert_eq!(uploaded.size, 4);
        let meta = local
            .head_in_bucket("receipts", "a/b.pdf")
            .await
            .unwrap()
            .expect("found");
        assert_eq!((meta.size, meta.content_type), (4, None));
        assert_eq!(
            local.head_in_bucket("receipts", "a/c.pdf").await.unwrap(),
            None
        );
        assert_eq!(
            local
                .download_from_bucket("receipts", "a/b.pdf")
                .await
                .unwrap(),
            b"%PDF"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn presigned_urls_name_bucket_key_and_expiry() {
        let backend = PrefixedBackend::memory("primary".into());
        let url = backend.presign_download("photo.jpg", 60).await.unwrap();
        assert_eq!(url, "memory://primary/photo.jpg?expires=60");
        let url = backend
            .presign_upload_to_bucket("media", "p.jpg", "image/jpeg", 30)
            .await
            .unwrap();
        assert_eq!(url, "memory://media/p.jpg?expires=30");
    }

    #[tokio::test]
    async fn local_backend_persists_objects_on_disk_for_other_processes() {
        let dir = std::env::temp_dir().join(format!("cufflink-storage-{}", uuid()));
        let writer = PrefixedBackend::local(&dir, "primary".into()).expect("local backend");
        writer
            .upload("wasm/x/v1.wasm", b"\0asm".to_vec(), "application/wasm")
            .await
            .unwrap();
        let reader = PrefixedBackend::local(&dir, "primary".into()).expect("local backend");
        assert_eq!(reader.head("wasm/x/v1.wasm").await.unwrap(), 4);
        assert_eq!(reader.download("wasm/x/v1.wasm").await.unwrap(), b"\0asm");
        assert_eq!(reader.backend_kind(), "local");
        assert_eq!(
            reader.presign_download("wasm/x/v1.wasm", 5).await.unwrap(),
            "local://primary/wasm/x/v1.wasm?expires=5"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    async fn keys_after_deleting(backend: &PrefixedBackend, prefix: &str) -> (usize, Vec<bool>) {
        for key in ["native/1/a", "native/1/b/c", "native/10/a", "native/2/a"] {
            backend
                .upload(key, b"x".to_vec(), "application/octet-stream")
                .await
                .unwrap();
        }
        backend
            .upload_to_bucket("other", "native/1/a", b"x".to_vec(), "text/plain")
            .await
            .unwrap();
        let deleted = backend.delete_prefix(prefix).await.unwrap();
        let mut kept = Vec::new();
        for key in ["native/1/a", "native/1/b/c", "native/10/a", "native/2/a"] {
            kept.push(
                backend
                    .head_in_bucket("primary", key)
                    .await
                    .unwrap()
                    .is_some(),
            );
        }
        kept.push(
            backend
                .head_in_bucket("other", "native/1/a")
                .await
                .unwrap()
                .is_some(),
        );
        (deleted, kept)
    }

    #[tokio::test]
    async fn a_prefix_deletes_the_objects_under_its_segments_in_the_primary_bucket() {
        let dir = std::env::temp_dir().join(format!("cufflink-storage-prefix-{}", uuid()));
        for backend in [
            PrefixedBackend::memory("primary".into()),
            PrefixedBackend::local(&dir, "primary".into()).expect("local backend"),
        ] {
            let kind = backend.backend_kind();
            assert_eq!(
                keys_after_deleting(&backend, "native/1/").await,
                (2, vec![false, false, true, true, true]),
                "{kind}"
            );
            assert_eq!(
                backend.delete_prefix("native/1").await.unwrap(),
                0,
                "{kind}"
            );
            assert_eq!(
                backend.delete_prefix("never/stored").await.unwrap(),
                0,
                "{kind}: a prefix nothing is under deletes nothing"
            );
            for everything in ["", "/"] {
                let refused = backend.delete_prefix(everything).await.unwrap_err();
                assert!(
                    refused.to_string().contains("whole bucket"),
                    "{kind}: {refused}"
                );
            }
            assert!(
                backend
                    .head_in_bucket("primary", "native/2/a")
                    .await
                    .unwrap()
                    .is_some(),
                "{kind}"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    fn uuid() -> String {
        format!(
            "{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        )
    }
}
