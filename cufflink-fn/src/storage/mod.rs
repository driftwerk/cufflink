//! Download files from S3-compatible object storage using the platform's credentials.
//!
//! The platform uses its own S3 credentials (configured at deployment time) to
//! perform authenticated downloads. This works with AWS S3, Hetzner Object Storage,
//! MinIO, and any S3-compatible service.

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(feature = "storage-v2")]
mod v2;

#[cfg(not(target_arch = "wasm32"))]
use crate::host::storage::PresignOp;
#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};
#[cfg(target_arch = "wasm32")]
use serde_json::Value;
#[cfg(feature = "storage-v2")]
pub use v2::{
    head, upload, upload_with, ObjectMeta, StorageError, StorageErrorKind, UploadOptions, Uploaded,
};

/// Download a file from S3 and return its contents as a base64-encoded string.
///
/// Returns `None` if the download fails (bucket not found, key not found,
/// S3 not configured, etc.).
///
/// ```rust,ignore
/// if let Some(base64_data) = storage::download("my-bucket", "images/photo.jpg") {
///     log::info(&format!("Downloaded {} bytes of base64", base64_data.len()));
/// }
/// ```
pub fn download(bucket: &str, key: &str) -> Option<String> {
    #[cfg(target_arch = "wasm32")]
    {
        let bucket_bytes = bucket.as_bytes();
        let key_bytes = key.as_bytes();
        let result = unsafe {
            ffi::s3_download(
                bucket_bytes.as_ptr() as i32,
                bucket_bytes.len() as i32,
                key_bytes.as_ptr() as i32,
                key_bytes.len() as i32,
            )
        };
        if result < 0 {
            return None;
        }
        host_buffer::read_host_string()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::download(bucket, key)
    }
}

/// Generate a presigned PUT URL for uploading a file directly to S3.
///
/// The returned URL is valid for `expires_secs` seconds and allows
/// unauthenticated PUT requests. Clients can upload by sending a PUT
/// request to the URL with the file data as the body.
///
/// ```rust,ignore
/// if let Some(url) = storage::presign_upload("my-bucket", "uploads/photo.jpg", "image/jpeg", 300) {
///     // Return this URL to the client for direct upload
///     log::info(&format!("Upload URL: {}", url));
/// }
/// ```
pub fn presign_upload(
    bucket: &str,
    key: &str,
    content_type: &str,
    expires_secs: u64,
) -> Option<String> {
    #[cfg(target_arch = "wasm32")]
    {
        let bucket_bytes = bucket.as_bytes();
        let key_bytes = key.as_bytes();
        let ct_bytes = content_type.as_bytes();
        let result = unsafe {
            ffi::s3_presign_upload(
                bucket_bytes.as_ptr() as i32,
                bucket_bytes.len() as i32,
                key_bytes.as_ptr() as i32,
                key_bytes.len() as i32,
                ct_bytes.as_ptr() as i32,
                ct_bytes.len() as i32,
                expires_secs as i32,
            )
        };
        if result < 0 {
            return None;
        }
        host_buffer::read_host_string()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::presign(PresignOp::Upload {
            bucket,
            key,
            content_type,
            expires_secs,
        })
    }
}

/// Generate a presigned GET URL for downloading a file directly from
/// object storage.
///
/// The returned URL is valid for `expires_secs` seconds and allows
/// unauthenticated GET requests — hand it to external consumers that
/// need to fetch a single object without credentials.
///
/// ```rust,ignore
/// if let Some(url) = storage::presign_download("my-bucket", "media/photo.jpg", 300) {
///     // Return this URL to the client for direct download
///     log::info(&format!("Download URL: {}", url));
/// }
/// ```
pub fn presign_download(bucket: &str, key: &str, expires_secs: u64) -> Option<String> {
    #[cfg(target_arch = "wasm32")]
    {
        let bucket_bytes = bucket.as_bytes();
        let key_bytes = key.as_bytes();
        let result = unsafe {
            ffi::s3_presign_download(
                bucket_bytes.as_ptr() as i32,
                bucket_bytes.len() as i32,
                key_bytes.as_ptr() as i32,
                key_bytes.len() as i32,
                expires_secs as i32,
            )
        };
        if result < 0 {
            return None;
        }
        host_buffer::read_host_string()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::presign(PresignOp::Download {
            bucket,
            key,
            expires_secs,
        })
    }
}

/// Delete an object from object storage.
///
/// Returns `true` when the object is confirmed absent — including when it
/// was already gone, so retrying a partially-failed cleanup is safe.
/// Returns `false` when the host could not delete it (storage not
/// configured, permission denied, transient backend error); assume the
/// object still exists and log the orphan.
///
/// ```rust,ignore
/// if !storage::delete("my-bucket", "media/photo.jpg") {
///     log::warn("orphaned object: my-bucket/media/photo.jpg");
/// }
/// ```
pub fn delete(bucket: &str, key: &str) -> bool {
    #[cfg(target_arch = "wasm32")]
    {
        let bucket_bytes = bucket.as_bytes();
        let key_bytes = key.as_bytes();
        let result = unsafe {
            ffi::s3_delete(
                bucket_bytes.as_ptr() as i32,
                bucket_bytes.len() as i32,
                key_bytes.as_ptr() as i32,
                key_bytes.len() as i32,
            )
        };
        result >= 0
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::delete(bucket, key)
    }
}

/// Download N files from object storage concurrently. Each slot is
/// either the decoded bytes or a per-item error message. Use this
/// instead of looping over [`download`] when N is large — the host
/// fans out in parallel with a bounded semaphore (default 32).
pub fn download_many(items: &[(&str, &str)]) -> Vec<Result<Vec<u8>, String>> {
    #[cfg(target_arch = "wasm32")]
    {
        let payload_items: Vec<Value> = items
            .iter()
            .map(|(b, k)| serde_json::json!({ "bucket": b, "key": k }))
            .collect();
        let payload = serde_json::to_string(&payload_items).unwrap_or_else(|_| "[]".into());
        let bytes = payload.as_bytes();
        let rc = unsafe { ffi::s3_download_many(bytes.as_ptr() as i32, bytes.len() as i32) };
        if rc < 0 {
            return items
                .iter()
                .map(|_| Err("s3_download_many host call failed".to_string()))
                .collect();
        }
        host_buffer::read_host_json::<Vec<Value>>()
            .map(|v| v.into_iter().map(parse_download_slot).collect())
            .unwrap_or_else(|| {
                items
                    .iter()
                    .map(|_| Err("malformed host response".to_string()))
                    .collect()
            })
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        native::download_many(items)
    }
}

#[cfg(target_arch = "wasm32")]
fn parse_download_slot(slot: Value) -> Result<Vec<u8>, String> {
    if !slot["ok"].as_bool().unwrap_or(false) {
        return Err(slot["error"]
            .as_str()
            .unwrap_or("unknown error")
            .to_string());
    }
    let b64 = slot["data_b64"]
        .as_str()
        .ok_or_else(|| "missing data_b64".to_string())?;
    use base64::{engine::general_purpose, Engine};
    general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| format!("base64 decode failed: {}", e))
}

#[cfg(test)]
mod tests {
    use crate::storage;

    #[test]
    fn test_storage_download_noop_on_native() {
        let data = storage::download("my-bucket", "images/photo.jpg");
        assert!(data.is_none());
    }
}
