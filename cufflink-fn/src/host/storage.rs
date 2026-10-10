//! Object storage.

use cufflink_types::host::storage::{HeadRequest, HeadResponse, UploadRequest, UploadResponse};

/// A URL to presign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresignOp<'a> {
    Upload {
        bucket: &'a str,
        key: &'a str,
        content_type: &'a str,
        expires_secs: u64,
    },
    Download {
        bucket: &'a str,
        key: &'a str,
        expires_secs: u64,
    },
}

/// What answers `storage::*`. The legacy functions read a failure as
/// `None` or `false`.
pub trait StorageHost {
    fn download(&self, bucket: &str, key: &str) -> Option<Vec<u8>>;
    #[cfg_attr(not(feature = "storage-v2"), allow(dead_code))]
    fn upload(&self, request: &UploadRequest) -> UploadResponse;
    #[cfg_attr(not(feature = "storage-v2"), allow(dead_code))]
    fn head(&self, request: &HeadRequest) -> HeadResponse;
    fn presign(&self, op: PresignOp<'_>) -> Option<String>;
    /// `true` when the object is gone, whether or not it was there.
    fn delete(&self, bucket: &str, key: &str) -> bool;
}
