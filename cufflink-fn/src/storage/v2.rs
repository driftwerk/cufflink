//! Uploads and object metadata, with failures that say what failed. Needs
//! a platform that supports `storage_v2`; the `storage-v2` feature declares
//! that in the module.

pub use cufflink_types::host::storage::StorageErrorKind;
use cufflink_types::host::storage::{
    HeadRequest, HeadResponse, StorageErrorEnvelope, UploadRequest, UploadResponse,
};
use std::fmt;

/// How an object is served beside its content type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UploadOptions {
    content_disposition: Option<String>,
    cache_control: Option<String>,
}

impl UploadOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn content_disposition(mut self, value: &str) -> Self {
        self.content_disposition = Some(value.to_string());
        self
    }

    pub fn cache_control(mut self, value: &str) -> Self {
        self.cache_control = Some(value.to_string());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uploaded {
    pub size: u64,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    /// RFC 3339.
    pub last_modified: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageError {
    pub kind: StorageErrorKind,
    pub message: String,
}

impl StorageError {
    #[cfg(target_arch = "wasm32")]
    fn new(kind: StorageErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// What every call returns where there is no platform to store for it.
    #[cfg(not(target_arch = "wasm32"))]
    fn unavailable() -> Self {
        Self {
            kind: StorageErrorKind::NotConfigured,
            message: "object storage is only available inside the Cufflink platform".into(),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for StorageError {}

impl From<StorageErrorEnvelope> for StorageError {
    fn from(envelope: StorageErrorEnvelope) -> Self {
        Self {
            kind: envelope.kind,
            message: envelope.message,
        }
    }
}

/// Store `body` at `key` in `bucket` with the platform's credentials,
/// replacing what is there.
///
/// ```rust,ignore
/// let key = format!("{book_id}/{receipt_id}_{branch}.pdf");
/// let stored = storage::upload("receipts", &key, "application/pdf", &pdf)?;
/// ```
pub fn upload(
    bucket: &str,
    key: &str,
    content_type: &str,
    body: &[u8],
) -> Result<Uploaded, StorageError> {
    upload_with(bucket, key, content_type, body, &UploadOptions::default())
}

pub fn upload_with(
    bucket: &str,
    key: &str,
    content_type: &str,
    body: &[u8],
    options: &UploadOptions,
) -> Result<Uploaded, StorageError> {
    let request = UploadRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        content_type: content_type.to_string(),
        body: body.to_vec(),
        content_disposition: options.content_disposition.clone(),
        cache_control: options.cache_control.clone(),
    };
    match transport::upload(&request)? {
        UploadResponse::Ok { etag, size } => Ok(Uploaded { size, etag }),
        UploadResponse::Error(envelope) => Err(envelope.into()),
    }
}

/// What is stored at `key` in `bucket`, without its bytes; `None` when
/// there is no such object.
pub fn head(bucket: &str, key: &str) -> Result<Option<ObjectMeta>, StorageError> {
    let request = HeadRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
    };
    match transport::head(&request)? {
        HeadResponse::Found {
            size,
            content_type,
            etag,
            last_modified,
        } => Ok(Some(ObjectMeta {
            size,
            content_type,
            etag,
            last_modified,
        })),
        HeadResponse::NotFound => Ok(None),
        HeadResponse::Error(envelope) => Err(envelope.into()),
    }
}

#[cfg(target_arch = "wasm32")]
mod transport {
    use super::*;
    use crate::envelope::{call, Failure};

    #[link(wasm_import_module = "env")]
    extern "C" {
        fn s3_upload(ptr: i32, len: i32) -> i32;
        fn s3_head(ptr: i32, len: i32) -> i32;
    }

    /// A call that brought no response back, its refusal read as `refused`:
    /// an upload's envelope can be too long, a head's cannot.
    fn failed(failure: Failure, refused: StorageErrorKind) -> StorageError {
        let kind = match failure {
            Failure::Refused => refused,
            Failure::Encode(_) | Failure::Malformed(_) => StorageErrorKind::Backend,
        };
        StorageError::new(kind, failure.to_string())
    }

    pub(super) fn upload(request: &UploadRequest) -> Result<UploadResponse, StorageError> {
        call(s3_upload, request).map_err(|failure| failed(failure, StorageErrorKind::TooLarge))
    }

    pub(super) fn head(request: &HeadRequest) -> Result<HeadResponse, StorageError> {
        call(s3_head, request).map_err(|failure| failed(failure, StorageErrorKind::Backend))
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod transport {
    use super::*;

    pub(super) fn upload(request: &UploadRequest) -> Result<UploadResponse, StorageError> {
        crate::host::storage()
            .map(|host| host.upload(request))
            .ok_or_else(StorageError::unavailable)
    }

    pub(super) fn head(request: &HeadRequest) -> Result<HeadResponse, StorageError> {
        crate::host::storage()
            .map(|host| host.head(request))
            .ok_or_else(StorageError::unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "test-host"))]
    #[test]
    fn outside_the_platform_every_call_is_unavailable() {
        for error in [
            upload("receipts", "k", "application/pdf", b"%PDF").expect_err("no platform"),
            head("receipts", "k").expect_err("no platform"),
        ] {
            assert_eq!(error.kind, StorageErrorKind::NotConfigured);
            assert_eq!(
                error.to_string(),
                "not_configured: object storage is only available inside the Cufflink platform"
            );
        }
    }

    #[cfg(feature = "test-host")]
    mod hosted {
        use super::*;
        use crate::host::{self, HostSet};
        use crate::testing::InMemoryStorage;
        use std::rc::Rc;

        fn hosted<T>(storage: &InMemoryStorage, body: impl FnOnce() -> T) -> T {
            let hosts = HostSet {
                storage: Some(Rc::new(storage.clone())),
                ..Default::default()
            };
            host::with(hosts, body)
        }

        #[test]
        fn a_receipt_pdf_is_stored_under_its_key_and_read_back() {
            let storage = InMemoryStorage::new();
            let key = "0b5c7a9e/7d1f3c2a_Harbor Branch.pdf";
            let pdf = [0x25, 0x50, 0x44, 0x46, 0x00, 0xff];
            let options = UploadOptions::new()
                .content_disposition("attachment; filename=\"receipt.pdf\"")
                .cache_control("no-store");
            let (stored, found, missing) = hosted(&storage, || {
                (
                    upload_with("receipts", key, "application/pdf", &pdf, &options),
                    head("receipts", key),
                    head("receipts", "missing.pdf"),
                )
            });
            let stored = stored.expect("stored");
            assert_eq!(stored.size, 6);
            let found = found.expect("head").expect("found");
            assert_eq!(
                (found.size, found.content_type.as_deref(), &found.etag),
                (6, Some("application/pdf"), &stored.etag)
            );
            assert!(found.last_modified.is_some());
            assert_eq!(missing, Ok(None));
            let object = storage.object("receipts", key).expect("object");
            assert_eq!(object.body, pdf);
            assert_eq!(
                (
                    object.content_disposition.as_deref(),
                    object.cache_control.as_deref()
                ),
                (
                    Some("attachment; filename=\"receipt.pdf\""),
                    Some("no-store")
                )
            );
        }

        #[test]
        fn a_refusal_keeps_its_kind() {
            let storage = InMemoryStorage::new().max_upload_bytes(4).deny("platform");
            let (large, denied, unseen) = hosted(&storage, || {
                (
                    upload("receipts", "k", "text/plain", b"12345"),
                    upload("platform", "wasm/x", "text/plain", b"1"),
                    head("platform", "wasm/x"),
                )
            });
            assert_eq!(large.expect_err("large").kind, StorageErrorKind::TooLarge);
            assert_eq!(denied.expect_err("denied").kind, StorageErrorKind::Denied);
            assert_eq!(unseen.expect_err("denied").kind, StorageErrorKind::Denied);
            assert!(storage.keys("receipts").is_empty());
        }
    }
}
