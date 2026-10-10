use cufflink_storage::{ObjectMeta, ObjectReader, ObjectStorage, UploadOptions, Uploaded};
use eyre::Result;
use std::sync::Arc;

#[derive(Clone)]
pub struct S3Storage {
    inner: Arc<dyn ObjectStorage>,
}

impl S3Storage {
    pub fn from_inner(inner: Arc<dyn ObjectStorage>) -> Self {
        Self { inner }
    }

    pub async fn upload(&self, key: &str, data: Vec<u8>, content_type: &str) -> Result<()> {
        self.inner.upload(key, data, content_type).await
    }

    pub async fn upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: &str,
    ) -> Result<()> {
        self.inner
            .upload_to_bucket(bucket, key, data, content_type)
            .await
    }

    pub async fn upload_to_bucket_with(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        options: &UploadOptions,
    ) -> Result<Uploaded> {
        self.inner
            .upload_to_bucket_with(bucket, key, data, options)
            .await
    }

    pub async fn head(&self, key: &str) -> Result<u64> {
        self.inner.head(key).await
    }

    pub async fn head_in_bucket(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        self.inner.head_in_bucket(bucket, key).await
    }

    pub async fn download(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.download(key).await
    }

    pub async fn download_from_bucket(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        self.inner.download_from_bucket(bucket, key).await
    }

    pub async fn open_read(&self, bucket: &str, key: &str) -> Result<Option<ObjectReader>> {
        self.inner.open_read(bucket, key).await
    }

    pub async fn copy(&self, source_key: &str, dest_key: &str) -> Result<()> {
        self.inner.copy(source_key, dest_key).await
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    pub async fn delete_from_bucket(&self, bucket: &str, key: &str) -> Result<()> {
        self.inner.delete_from_bucket(bucket, key).await
    }

    pub async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        self.inner.delete_prefix(prefix).await
    }

    pub async fn presign_upload(
        &self,
        key: &str,
        content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        self.inner
            .presign_upload(key, content_type, expires_in_secs)
            .await
    }

    pub async fn presign_upload_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        content_type: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        self.inner
            .presign_upload_to_bucket(bucket, key, content_type, expires_in_secs)
            .await
    }

    pub async fn presign_download(&self, key: &str, expires_in_secs: u64) -> Result<String> {
        self.inner.presign_download(key, expires_in_secs).await
    }

    pub async fn presign_download_to_bucket(
        &self,
        bucket: &str,
        key: &str,
        expires_in_secs: u64,
    ) -> Result<String> {
        self.inner
            .presign_download_to_bucket(bucket, key, expires_in_secs)
            .await
    }

    pub fn backend_kind(&self) -> &'static str {
        self.inner.backend_kind()
    }

    pub fn primary_bucket(&self) -> &str {
        self.inner.primary_bucket()
    }
}
