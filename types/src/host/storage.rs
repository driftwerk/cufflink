//! `s3_upload` and `s3_head`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadRequest {
    pub bucket: String,
    pub key: String,
    pub content_type: String,
    #[serde(rename = "body_b64", with = "crate::serde_util::base64_bytes")]
    pub body: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_disposition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum UploadResponse {
    Ok { etag: Option<String>, size: u64 },
    Error(StorageErrorEnvelope),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadRequest {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum HeadResponse {
    Found {
        size: u64,
        content_type: Option<String>,
        etag: Option<String>,
        /// RFC 3339.
        last_modified: Option<String>,
    },
    NotFound,
    Error(StorageErrorEnvelope),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageErrorEnvelope {
    pub kind: StorageErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageErrorKind {
    Denied,
    TooLarge,
    NotConfigured,
    Timeout,
    Backend,
}

impl StorageErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            StorageErrorKind::Denied => "denied",
            StorageErrorKind::TooLarge => "too_large",
            StorageErrorKind::NotConfigured => "not_configured",
            StorageErrorKind::Timeout => "timeout",
            StorageErrorKind::Backend => "backend",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::tests::{refused, round_trip};
    use serde_json::json;

    const KINDS: [StorageErrorKind; 5] = [
        StorageErrorKind::Denied,
        StorageErrorKind::TooLarge,
        StorageErrorKind::NotConfigured,
        StorageErrorKind::Timeout,
        StorageErrorKind::Backend,
    ];

    #[test]
    fn upload_round_trips() {
        round_trip(
            &UploadRequest {
                bucket: "receipts".into(),
                key: "book-1/receipt-2.pdf".into(),
                content_type: "application/pdf".into(),
                body: b"%PDF".to_vec(),
                ..Default::default()
            },
            json!({
                "bucket": "receipts",
                "key": "book-1/receipt-2.pdf",
                "content_type": "application/pdf",
                "body_b64": "JVBERg=="
            }),
        );
        round_trip(
            &UploadRequest {
                bucket: "receipts".into(),
                key: "k".into(),
                content_type: "text/plain".into(),
                body: Vec::new(),
                content_disposition: Some("attachment; filename=\"q.pdf\"".into()),
                cache_control: Some("no-store".into()),
            },
            json!({
                "bucket": "receipts",
                "key": "k",
                "content_type": "text/plain",
                "body_b64": "",
                "content_disposition": "attachment; filename=\"q.pdf\"",
                "cache_control": "no-store"
            }),
        );
        round_trip(
            &UploadResponse::Ok {
                etag: Some("\"abc\"".into()),
                size: 4,
            },
            json!({"result": "ok", "etag": "\"abc\"", "size": 4}),
        );
        round_trip(
            &UploadResponse::Ok {
                etag: None,
                size: 0,
            },
            json!({"result": "ok", "etag": null, "size": 0}),
        );
    }

    #[test]
    fn head_round_trips() {
        round_trip(
            &HeadRequest {
                bucket: "receipts".into(),
                key: "k".into(),
            },
            json!({"bucket": "receipts", "key": "k"}),
        );
        round_trip(
            &HeadResponse::Found {
                size: 9,
                content_type: Some("application/pdf".into()),
                etag: None,
                last_modified: Some("2026-09-28T10:00:00Z".into()),
            },
            json!({
                "result": "found",
                "size": 9,
                "content_type": "application/pdf",
                "etag": null,
                "last_modified": "2026-09-28T10:00:00Z"
            }),
        );
        round_trip(&HeadResponse::NotFound, json!({"result": "not_found"}));
    }

    #[test]
    fn errors_name_their_kind() {
        for kind in KINDS {
            let envelope = StorageErrorEnvelope {
                kind,
                message: "why".into(),
            };
            let wire = json!({"result": "error", "kind": kind.as_str(), "message": "why"});
            round_trip(&UploadResponse::Error(envelope.clone()), wire.clone());
            round_trip(&HeadResponse::Error(envelope), wire);
        }
    }

    #[test]
    fn unknown_tags_kinds_and_bodies_are_refused() {
        refused::<UploadResponse>(json!({"result": "stored"}));
        refused::<HeadResponse>(json!({"result": "gone"}));
        refused::<HeadResponse>(json!({"result": "error", "kind": "quota", "message": ""}));
        refused::<UploadRequest>(
            json!({"bucket": "b", "key": "k", "content_type": "t", "body_b64": "*"}),
        );
        refused::<UploadRequest>(json!({"bucket": "b", "key": "k", "content_type": "t"}));
        refused::<HeadRequest>(json!({"bucket": "b", "key": "k", "version": "1"}));
    }
}
