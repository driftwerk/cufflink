//! Object storage kept in memory.

use crate::host::storage::{PresignOp, StorageHost};
use cufflink_types::host::storage::{
    CannedAcl, HeadRequest, HeadResponse, StorageErrorEnvelope, StorageErrorKind, UploadRequest,
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
    pub acl: Option<CannedAcl>,
    /// RFC 3339.
    pub last_modified: String,
}

/// A call a guest made, in the order it was made, whether or not it was
/// refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageCall {
    Download {
        bucket: String,
        key: String,
    },
    Upload {
        bucket: String,
        key: String,
        content_type: String,
        acl: Option<CannedAcl>,
    },
    Head {
        bucket: String,
        key: String,
    },
    Presign {
        bucket: String,
        key: String,
        upload: bool,
    },
    Delete {
        bucket: String,
        key: String,
    },
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
    acls: BTreeSet<(String, CannedAcl)>,
    calls: Vec<StorageCall>,
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
                acls: BTreeSet::new(),
                calls: Vec::new(),
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

    /// Admit uploads to `bucket` that ask for `acl`, as the platform does
    /// for a bucket registered to the tenant that lists it. Every other
    /// upload naming an ACL is refused with `Denied`.
    pub fn admit_acl(self, bucket: &str, acl: CannedAcl) -> Self {
        self.store
            .borrow_mut()
            .acls
            .insert((bucket.to_string(), acl));
        self
    }

    /// Every call the guest made, in order.
    pub fn calls(&self) -> Vec<StorageCall> {
        self.store.borrow().calls.clone()
    }

    fn record(&self, call: StorageCall) {
        self.store.borrow_mut().calls.push(call);
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
                acl: None,
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
        self.record(StorageCall::Download {
            bucket: bucket.to_string(),
            key: key.to_string(),
        });
        self.allows(bucket)
            .then(|| self.object(bucket, key))
            .flatten()
            .map(|object| object.body)
    }

    fn upload(&self, request: &UploadRequest) -> UploadResponse {
        self.record(StorageCall::Upload {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            content_type: request.content_type.clone(),
            acl: request.acl,
        });
        if !self.allows(&request.bucket) {
            return UploadResponse::Error(denied(&request.bucket));
        }
        if let Some(acl) = request.acl {
            if !self
                .store
                .borrow()
                .acls
                .contains(&(request.bucket.clone(), acl))
            {
                return UploadResponse::Error(StorageErrorEnvelope {
                    kind: StorageErrorKind::Denied,
                    message: "storage access denied: acl_not_admitted".into(),
                });
            }
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
            acl: request.acl,
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
        self.record(StorageCall::Head {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
        });
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
        self.record(StorageCall::Presign {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload: matches!(op, PresignOp::Upload { .. }),
        });
        self.allows(bucket)
            .then(|| format!("memory://{bucket}/{key}?expires={expires_secs}"))
    }

    fn delete(&self, bucket: &str, key: &str) -> bool {
        self.record(StorageCall::Delete {
            bucket: bucket.to_string(),
            key: key.to_string(),
        });
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

    #[test]
    fn every_call_is_recorded_in_order_and_keys_are_kept_exactly() {
        let storage = InMemoryStorage::new().deny("platform");
        let key = "leads/7/Café façade #2 50%.pdf";
        storage.upload(&upload("example-documents", key, b"%PDF"));
        assert_eq!(
            storage.download("example-documents", key).as_deref(),
            Some(&b"%PDF"[..])
        );
        storage.upload(&UploadRequest {
            acl: Some(CannedAcl::PublicRead),
            ..upload("example-documents", "b.pdf", b"x")
        });
        storage.head(&head("platform", "wasm/x"));
        storage.delete("example-documents", key);
        assert_eq!(storage.keys("example-documents"), Vec::<String>::new());
        assert_eq!(
            storage.calls(),
            [
                StorageCall::Upload {
                    bucket: "example-documents".into(),
                    key: key.into(),
                    content_type: "application/pdf".into(),
                    acl: None,
                },
                StorageCall::Download {
                    bucket: "example-documents".into(),
                    key: key.into(),
                },
                StorageCall::Upload {
                    bucket: "example-documents".into(),
                    key: "b.pdf".into(),
                    content_type: "application/pdf".into(),
                    acl: Some(CannedAcl::PublicRead),
                },
                StorageCall::Head {
                    bucket: "platform".into(),
                    key: "wasm/x".into(),
                },
                StorageCall::Delete {
                    bucket: "example-documents".into(),
                    key: key.into(),
                },
            ]
        );
    }
}
