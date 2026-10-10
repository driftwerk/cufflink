//! Object storage kept in memory.

use crate::host::storage::{PresignOp, StorageHost};
use cufflink_types::host::storage::{
    HeadRequest, HeadResponse, StorageErrorEnvelope, StorageErrorKind, UploadRequest,
    UploadResponse,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

/// The platform's upload cap at its defaults: `HOST_STORAGE_MAX_UPLOAD_BYTES`
/// (50 MiB) held to what fits the envelope a guest may hand over
/// (`WASM_MAX_GUEST_LEN_MB`, 64 MiB, with the body in base64).
const MAX_UPLOAD_BYTES: usize = 50_319_360;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub body: Vec<u8>,
    pub content_type: String,
    pub content_disposition: Option<String>,
    pub cache_control: Option<String>,
    /// RFC 3339.
    pub last_modified: String,
}

impl StoredObject {
    fn etag(&self) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.body.hash(&mut hasher);
        format!("\"{:016x}\"", hasher.finish())
    }
}

struct Store {
    objects: BTreeMap<(String, String), StoredObject>,
    denied: BTreeSet<String>,
    max_upload_bytes: usize,
}

/// Buckets that hold what was put in them. Clones share one store.
#[derive(Clone)]
pub struct InMemoryStorage {
    store: Rc<RefCell<Store>>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self {
            store: Rc::new(RefCell::new(Store {
                objects: BTreeMap::new(),
                denied: BTreeSet::new(),
                max_upload_bytes: MAX_UPLOAD_BYTES,
            })),
        }
    }
}

fn denied(bucket: &str) -> StorageErrorEnvelope {
    StorageErrorEnvelope {
        kind: StorageErrorKind::Denied,
        message: format!("storage access denied: bucket {bucket}"),
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Refuse every call on `bucket`, as the platform refuses a bucket
    /// outside the tenant's `storage_scope`.
    pub fn deny(self, bucket: &str) -> Self {
        self.store.borrow_mut().denied.insert(bucket.to_string());
        self
    }

    /// Refuse an upload longer than `bytes` with `TooLarge`, as the
    /// platform's cap does (about 48 MiB at its defaults).
    pub fn max_upload_bytes(self, bytes: usize) -> Self {
        self.store.borrow_mut().max_upload_bytes = bytes;
        self
    }

    /// Store an object as a test's fixture, whatever is denied or capped.
    pub fn put(&self, bucket: &str, key: &str, content_type: &str, body: &[u8]) {
        self.store.borrow_mut().objects.insert(
            (bucket.to_string(), key.to_string()),
            StoredObject {
                body: body.to_vec(),
                content_type: content_type.to_string(),
                content_disposition: None,
                cache_control: None,
                last_modified: crate::util::rfc3339(crate::util::system_millis()),
            },
        );
    }

    pub fn object(&self, bucket: &str, key: &str) -> Option<StoredObject> {
        self.store
            .borrow()
            .objects
            .get(&(bucket.to_string(), key.to_string()))
            .cloned()
    }

    /// The keys of `bucket`, in order.
    pub fn keys(&self, bucket: &str) -> Vec<String> {
        self.store
            .borrow()
            .objects
            .keys()
            .filter(|(of, _)| of == bucket)
            .map(|(_, key)| key.clone())
            .collect()
    }

    fn allows(&self, bucket: &str) -> bool {
        !self.store.borrow().denied.contains(bucket)
    }
}

impl StorageHost for InMemoryStorage {
    fn download(&self, bucket: &str, key: &str) -> Option<Vec<u8>> {
        self.allows(bucket)
            .then(|| self.object(bucket, key))
            .flatten()
            .map(|object| object.body)
    }

    fn upload(&self, request: &UploadRequest) -> UploadResponse {
        if !self.allows(&request.bucket) {
            return UploadResponse::Error(denied(&request.bucket));
        }
        let cap = self.store.borrow().max_upload_bytes;
        if request.body.len() > cap {
            return UploadResponse::Error(StorageErrorEnvelope {
                kind: StorageErrorKind::TooLarge,
                message: format!(
                    "upload of {} bytes exceeds the cap of {cap} bytes",
                    request.body.len()
                ),
            });
        }
        let object = StoredObject {
            body: request.body.clone(),
            content_type: request.content_type.clone(),
            content_disposition: request.content_disposition.clone(),
            cache_control: request.cache_control.clone(),
            last_modified: crate::util::rfc3339(crate::util::system_millis()),
        };
        let stored = UploadResponse::Ok {
            etag: Some(object.etag()),
            size: object.body.len() as u64,
        };
        self.store
            .borrow_mut()
            .objects
            .insert((request.bucket.clone(), request.key.clone()), object);
        stored
    }

    fn head(&self, request: &HeadRequest) -> HeadResponse {
        if !self.allows(&request.bucket) {
            return HeadResponse::Error(denied(&request.bucket));
        }
        self.object(&request.bucket, &request.key)
            .map_or(HeadResponse::NotFound, |object| HeadResponse::Found {
                size: object.body.len() as u64,
                etag: Some(object.etag()),
                content_type: Some(object.content_type),
                last_modified: Some(object.last_modified),
            })
    }

    fn presign(&self, op: PresignOp<'_>) -> Option<String> {
        let (PresignOp::Upload {
            bucket,
            key,
            expires_secs,
            ..
        }
        | PresignOp::Download {
            bucket,
            key,
            expires_secs,
        }) = op;
        self.allows(bucket)
            .then(|| format!("memory://{bucket}/{key}?expires={expires_secs}"))
    }

    fn delete(&self, bucket: &str, key: &str) -> bool {
        self.allows(bucket) && {
            self.store
                .borrow_mut()
                .objects
                .remove(&(bucket.to_string(), key.to_string()));
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upload(bucket: &str, key: &str, body: &[u8]) -> UploadRequest {
        UploadRequest {
            bucket: bucket.into(),
            key: key.into(),
            content_type: "application/pdf".into(),
            body: body.to_vec(),
            ..Default::default()
        }
    }

    fn head(bucket: &str, key: &str) -> HeadRequest {
        HeadRequest {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    #[test]
    fn buckets_hold_what_was_uploaded() {
        let storage = InMemoryStorage::new();
        let UploadResponse::Ok { etag, size } = storage.upload(&upload("receipts", "b", b"%PDF"))
        else {
            panic!("refused");
        };
        assert_eq!(size, 4);
        storage.upload(&upload("receipts", "a", b"other"));
        storage.upload(&upload("emails", "a", b""));
        assert_eq!(storage.keys("receipts"), ["a", "b"]);
        assert_eq!(
            storage.download("receipts", "b").as_deref(),
            Some(&b"%PDF"[..])
        );
        assert_eq!(storage.download("emails", "b"), None);
        let HeadResponse::Found {
            size,
            content_type,
            etag: found,
            last_modified,
        } = storage.head(&head("receipts", "b"))
        else {
            panic!("not found");
        };
        assert_eq!(
            (size, content_type.as_deref()),
            (4, Some("application/pdf"))
        );
        assert_eq!(found, etag);
        assert!(last_modified.is_some_and(|at| at.ends_with("+00:00")));
        assert_eq!(storage.head(&head("receipts", "c")), HeadResponse::NotFound);

        let UploadResponse::Ok { etag: replaced, .. } =
            storage.upload(&upload("receipts", "b", b"%PDF-1.7"))
        else {
            panic!("refused");
        };
        assert_ne!(replaced, etag, "an etag follows the bytes");
        assert!(storage.delete("receipts", "b") && storage.delete("receipts", "b"));
        assert_eq!(storage.keys("receipts"), ["a"]);
    }
}
