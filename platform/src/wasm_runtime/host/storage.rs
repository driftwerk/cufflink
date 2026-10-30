use super::util::{ended, exchange};
use super::{
    block_on_bounded, fanout_concurrency, read_guest_string, wrap, write_in_order, BatchResult,
    HostIoError, HostState, ResponseText,
};
use crate::policy::Denied;
use crate::storage::S3Storage;
use crate::wasm_runtime::budget::{
    base64_len, s3_host_bytes, ChargedJsonError, Hold, HostCaps, Refused,
};
use crate::wasm_runtime::storage_scope;
use base64::{engine::general_purpose, Engine};
use cufflink_storage::UploadOptions;
use cufflink_types::host::storage::{
    HeadRequest, HeadResponse, StorageErrorEnvelope, StorageErrorKind, UploadRequest,
    UploadResponse,
};
use reqwest::header::HeaderValue;
use std::collections::HashSet;
use std::sync::Arc;
use wasmtime::{Caller, Linker};

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "s3_download",
        |mut caller: Caller<'_, HostState>,
         bucket_ptr: i32,
         bucket_len: i32,
         key_ptr: i32,
         key_len: i32|
         -> i32 {
            let bucket = host_try!(read_guest_string(
                &mut caller,
                "s3_download",
                bucket_ptr,
                bucket_len
            ));

            let key = host_try!(read_guest_string(
                &mut caller,
                "s3_download",
                key_ptr,
                key_len
            ));

            if !caller.data().authorize_storage(&bucket, &[&key]) {
                return -1;
            }

            caller.data_mut().clear_host_response();
            let app_state = caller.data().app_state.clone();
            let limits = Arc::clone(&caller.data().limits);
            let mut hold = caller.data().hold();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state
                    .storage
                    .as_ref()
                    .ok_or_else(|| Fetch::Failed(eyre::eyre!("S3 storage not configured")))?;
                let bytes = fetch_object(
                    storage,
                    &bucket,
                    &key,
                    &mut hold,
                    &limits.budget.caps,
                    "s3_download",
                )
                .await?;
                Ok::<String, Fetch>(general_purpose::STANDARD.encode(bytes))
            });

            match result {
                Ok(b64) => {
                    hold.shrink_to(b64.capacity());
                    caller.data_mut().set_charged_response(b64, hold);
                    0
                }
                Err(HostIoError::Failed(Fetch::Refused(refused))) => {
                    caller.data_mut().refused(refused)
                }
                Err(e) => {
                    tracing::error!("[wasm] s3_download error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "s3_download_many",
        |mut caller: Caller<'_, HostState>, items_ptr: i32, items_len: i32| -> i32 {
            const PATH: &str = "s3_download_many";
            let items_json = host_try!(read_guest_string(&mut caller, PATH, items_ptr, items_len));
            let mut parsed = caller.data().hold();
            if let Err(capacity) = parsed.grow(items_json.len()) {
                return caller.data_mut().refused(Refused::Budget(capacity));
            }
            let items: Vec<S3Item> = match serde_json::from_str(&items_json) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("[wasm] s3_download_many: bad input: {}", e);
                    return -1;
                }
            };
            drop(items_json);
            let limits = Arc::clone(&caller.data().limits);
            let caps = &limits.budget.caps;
            if let Err(cap) = caps.fanout(PATH, items.len()) {
                return caller.data_mut().refused(Refused::Cap(cap));
            }
            let items: Vec<(S3Item, bool, Hold)> = items
                .into_iter()
                .map(|item| {
                    let permitted = caller.data().authorize_storage(&item.bucket, &[&item.key]);
                    (item, permitted, parsed.sibling())
                })
                .collect();
            caller.data_mut().clear_host_response();
            let app_state = caller.data().app_state.clone();
            let mut hold = caller.data().hold();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state.storage.as_ref();
                let fetches = items
                    .into_iter()
                    .map(|(item, permitted, mut held)| async move {
                        if !permitted {
                            return Ok((BatchResult::err("storage access denied"), held));
                        }
                        let Some(storage) = storage else {
                            return Ok((BatchResult::err("S3 storage not configured"), held));
                        };
                        match fetch_object(storage, &item.bucket, &item.key, &mut held, caps, PATH)
                            .await
                        {
                            Ok(bytes) => {
                                let b64 = general_purpose::STANDARD.encode(bytes);
                                Ok((
                                    BatchResult::ok(serde_json::json!({ "data_b64": b64 })),
                                    held,
                                ))
                            }
                            Err(Fetch::Failed(e)) => Ok((BatchResult::err(&e.to_string()), held)),
                            Err(Fetch::Refused(refused)) => Err(refused),
                        }
                    });
                let mut text = ResponseText::new(&mut hold, caps, PATH);
                write_in_order(fetches, fanout_concurrency(), &mut text)
                    .await
                    .map(|()| text.into_text())
            });
            drop(parsed);

            match result {
                Ok(text) => {
                    caller.data_mut().set_charged_response(text, hold);
                    0
                }
                Err(HostIoError::Failed(ChargedJsonError::Capacity(refused))) => {
                    caller.data_mut().refused(refused)
                }
                Err(e) => {
                    tracing::error!("[wasm] s3_download_many error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "s3_presign_upload",
        |mut caller: Caller<'_, HostState>,
         bucket_ptr: i32,
         bucket_len: i32,
         key_ptr: i32,
         key_len: i32,
         content_type_ptr: i32,
         content_type_len: i32,
         expires_secs: i32|
         -> i32 {
            let bucket = host_try!(read_guest_string(
                &mut caller,
                "s3_presign_upload",
                bucket_ptr,
                bucket_len
            ));
            let key = host_try!(read_guest_string(
                &mut caller,
                "s3_presign_upload",
                key_ptr,
                key_len
            ));
            let content_type = host_try!(read_guest_string(
                &mut caller,
                "s3_presign_upload",
                content_type_ptr,
                content_type_len
            ));

            if !caller.data().authorize_storage(&bucket, &[&key]) {
                return -1;
            }

            let app_state = caller.data().app_state.clone();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state
                    .storage
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("S3 storage not configured"))?;

                let url = storage
                    .presign_upload_to_bucket(&bucket, &key, &content_type, expires_secs as u64)
                    .await?;
                Ok::<String, eyre::Report>(url)
            });

            match result {
                Ok(url) => caller.data_mut().respond("s3_presign_upload", url),
                Err(e) => {
                    tracing::error!("[wasm] s3_presign_upload error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "s3_presign_download",
        |mut caller: Caller<'_, HostState>,
         bucket_ptr: i32,
         bucket_len: i32,
         key_ptr: i32,
         key_len: i32,
         expires_secs: i32|
         -> i32 {
            let bucket = host_try!(read_guest_string(
                &mut caller,
                "s3_presign_download",
                bucket_ptr,
                bucket_len
            ));
            let key = host_try!(read_guest_string(
                &mut caller,
                "s3_presign_download",
                key_ptr,
                key_len
            ));

            if !caller.data().authorize_storage(&bucket, &[&key]) {
                return -1;
            }

            let app_state = caller.data().app_state.clone();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state
                    .storage
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("S3 storage not configured"))?;

                let url = storage
                    .presign_download_to_bucket(&bucket, &key, expires_secs as u64)
                    .await?;
                Ok::<String, eyre::Report>(url)
            });

            match result {
                Ok(url) => caller.data_mut().respond("s3_presign_download", url),
                Err(e) => {
                    tracing::error!("[wasm] s3_presign_download error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "s3_delete",
        |mut caller: Caller<'_, HostState>,
         bucket_ptr: i32,
         bucket_len: i32,
         key_ptr: i32,
         key_len: i32|
         -> i32 {
            let bucket = host_try!(read_guest_string(
                &mut caller,
                "s3_delete",
                bucket_ptr,
                bucket_len
            ));
            let key = host_try!(read_guest_string(
                &mut caller,
                "s3_delete",
                key_ptr,
                key_len
            ));

            if !caller.data().authorize_storage(&bucket, &[&key]) {
                return -1;
            }

            let app_state = caller.data().app_state.clone();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state
                    .storage
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("S3 storage not configured"))?;

                storage.delete_from_bucket(&bucket, &key).await?;
                Ok::<(), eyre::Report>(())
            });

            match result {
                Ok(()) => 0,
                Err(e) => {
                    tracing::error!("[wasm] s3_delete error: {}", e);
                    -1
                }
            }
        },
    )?;

    Ok(())
}

pub(crate) enum Fetch {
    Refused(Refused),
    Failed(eyre::Report),
}

impl std::fmt::Display for Fetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refused) => refused.fmt(f),
            Self::Failed(e) => e.fmt(f),
        }
    }
}

pub(super) async fn fetch_object(
    storage: &S3Storage,
    bucket: &str,
    key: &str,
    hold: &mut Hold,
    caps: &HostCaps,
    path: &'static str,
) -> Result<Vec<u8>, Fetch> {
    read_object(storage, bucket, key, |size| {
        caps.response(
            path,
            base64_len(usize::try_from(size).unwrap_or(usize::MAX)),
        )
        .map_err(Refused::Cap)?;
        hold.grow(s3_host_bytes(size)).map_err(Refused::Budget)
    })
    .await
}

pub(crate) async fn read_object(
    storage: &S3Storage,
    bucket: &str,
    key: &str,
    reserve: impl FnOnce(u64) -> Result<(), Refused>,
) -> Result<Vec<u8>, Fetch> {
    let failed = || {
        eyre::eyre!(
            "{} download failed for {bucket}/{key}",
            storage.backend_kind()
        )
    };
    let size = match storage.head_in_bucket(bucket, key).await {
        Ok(Some(meta)) => meta.size,
        Ok(None) => return Err(Fetch::Failed(failed())),
        Err(e) => return Err(Fetch::Failed(e.wrap_err(failed()))),
    };
    reserve(size).map_err(Fetch::Refused)?;
    let reader = match storage.open_read(bucket, key).await {
        Ok(Some(reader)) => reader,
        Ok(None) => return Err(Fetch::Failed(failed())),
        Err(e) => return Err(Fetch::Failed(e.wrap_err(failed()))),
    };
    read_within(reader.stream, size)
        .await
        .map_err(|e| Fetch::Failed(e.wrap_err(failed())))
}

async fn read_within(
    mut stream: futures_util::stream::BoxStream<'static, eyre::Result<bytes::Bytes>>,
    size: u64,
) -> eyre::Result<Vec<u8>> {
    use futures_util::StreamExt;
    let size = usize::try_from(size)?;
    let mut bytes = Vec::with_capacity(size);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if chunk.len() > size - bytes.len() {
            eyre::bail!("the object grew past the {size} bytes it was read for");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[derive(serde::Deserialize)]
struct S3Item {
    bucket: String,
    key: String,
}

impl HostState {
    pub(super) fn authorize_storage(&self, bucket: &str, keys: &[&str]) -> bool {
        self.storage_access(bucket, keys).is_ok()
    }

    fn storage_access(&self, bucket: &str, keys: &[&str]) -> Result<(), Denied> {
        let guest = storage_scope::Guest {
            tenant_id: self.identity.tenant_id,
            tenant_slug: &self.identity.tenant_slug,
            service_name: &self.identity.service_name,
            policies: &self.policies,
        };
        storage_scope::authorize_guest(&self.app_state, &guest, bucket, keys)
    }

    fn guest_storage(&self, bucket: &str, key: &str) -> Result<S3Storage, StorageErrorEnvelope> {
        if key.is_empty() {
            return Err(error(StorageErrorKind::Denied, "the key is empty"));
        }
        self.storage_access(bucket, &[key]).map_err(|denied| {
            error(
                StorageErrorKind::Denied,
                format!("storage access denied: {}", denied.code),
            )
        })?;
        self.app_state.storage.clone().ok_or_else(|| {
            error(
                StorageErrorKind::NotConfigured,
                "object storage is not configured",
            )
        })
    }

    fn stored<T>(
        &self,
        call: impl std::future::Future<Output = eyre::Result<T>>,
    ) -> Result<T, StorageErrorEnvelope> {
        block_on_bounded(&self.bounds(), async {
            call.await
                .map_err(|e| error(StorageErrorKind::Backend, e.to_string()))
        })
        .map_err(|e| ended(e, |waited| error(StorageErrorKind::Timeout, waited)))
    }
}

pub(super) fn register_v2(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "s3_upload",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            exchange(
                &mut caller,
                "s3_upload",
                (ptr, len),
                |reason| uploaded(Err(error(StorageErrorKind::Denied, reason))),
                |state, request| uploaded(upload(state, request)),
            )
        },
    )?;
    wrap(
        linker,
        names,
        "s3_head",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            exchange(
                &mut caller,
                "s3_head",
                (ptr, len),
                |reason| headed(Err(error(StorageErrorKind::Denied, reason))),
                |state, request| headed(head(state, &request)),
            )
        },
    )
}

fn error(kind: StorageErrorKind, message: impl Into<String>) -> StorageErrorEnvelope {
    StorageErrorEnvelope {
        kind,
        message: message.into(),
    }
}

fn count(op: &'static str, result: &'static str) {
    metrics::counter!("cufflink_host_storage_v2_total", "op" => op, "result" => result)
        .increment(1);
}

fn uploaded(outcome: Result<UploadResponse, StorageErrorEnvelope>) -> UploadResponse {
    count(
        "upload",
        outcome.as_ref().map_or_else(|e| e.kind.as_str(), |_| "ok"),
    );
    outcome.unwrap_or_else(UploadResponse::Error)
}

fn headed(outcome: Result<HeadResponse, StorageErrorEnvelope>) -> HeadResponse {
    count(
        "head",
        match &outcome {
            Ok(HeadResponse::NotFound) => "not_found",
            Ok(_) => "ok",
            Err(e) => e.kind.as_str(),
        },
    );
    outcome.unwrap_or_else(HeadResponse::Error)
}

fn upload(
    state: &HostState,
    request: UploadRequest,
) -> Result<UploadResponse, StorageErrorEnvelope> {
    let cap = upload_cap(
        state.app_state.config.storage_max_upload_bytes,
        state.limits.max_guest_len,
    );
    let options = admit(&request, cap)?;
    let storage = state.guest_storage(&request.bucket, &request.key)?;
    let UploadRequest {
        bucket, key, body, ..
    } = request;
    state
        .stored(storage.upload_to_bucket_with(&bucket, &key, body, &options))
        .map(|stored| UploadResponse::Ok {
            etag: stored.etag,
            size: stored.size,
        })
}

fn head(state: &HostState, request: &HeadRequest) -> Result<HeadResponse, StorageErrorEnvelope> {
    let storage = state.guest_storage(&request.bucket, &request.key)?;
    state
        .stored(storage.head_in_bucket(&request.bucket, &request.key))
        .map(|found| {
            found.map_or(HeadResponse::NotFound, |meta| HeadResponse::Found {
                size: meta.size,
                content_type: meta.content_type,
                etag: meta.etag,
                last_modified: Some(
                    meta.last_modified
                        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                ),
            })
        })
}

pub const DEFAULT_MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;

const ENVELOPE_SLACK: usize = 16 * 1024;

pub fn max_upload_bytes_from_env() -> eyre::Result<usize> {
    max_upload_bytes(std::env::var("HOST_STORAGE_MAX_UPLOAD_BYTES").ok())
}

fn max_upload_bytes(raw: Option<String>) -> eyre::Result<usize> {
    cufflink_db::positive_var(
        "HOST_STORAGE_MAX_UPLOAD_BYTES",
        raw,
        DEFAULT_MAX_UPLOAD_BYTES,
    )
}

pub(crate) fn upload_cap(configured: usize, max_guest_len: usize) -> usize {
    configured.min(max_guest_len.saturating_sub(ENVELOPE_SLACK) / 4 * 3)
}

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

fn admit(request: &UploadRequest, cap: usize) -> Result<UploadOptions, StorageErrorEnvelope> {
    if request.body.len() > cap {
        return Err(error(
            StorageErrorKind::TooLarge,
            format!(
                "upload of {} bytes exceeds the cap of {cap} bytes",
                request.body.len()
            ),
        ));
    }
    let header = |name: &str, value: &str| {
        HeaderValue::from_str(value)
            .map(|_| value.to_string())
            .map_err(|_| {
                error(
                    StorageErrorKind::Denied,
                    format!("{name} is not a header value"),
                )
            })
    };
    let optional = |name: &str, value: &Option<String>| {
        value
            .as_deref()
            .map(|value| header(name, value))
            .transpose()
    };
    Ok(UploadOptions {
        content_type: match request.content_type.trim() {
            "" => DEFAULT_CONTENT_TYPE.to_string(),
            _ => header("content_type", &request.content_type)?,
        },
        content_disposition: optional("content_disposition", &request.content_disposition)?,
        cache_control: optional("cache_control", &request.cache_control)?,
    })
}

#[cfg(test)]
mod v2_tests {
    use super::*;

    const MAX: usize = 16;

    fn request(body: &[u8]) -> UploadRequest {
        UploadRequest {
            bucket: "receipts".into(),
            key: "book-1/receipt-2_Acme.pdf".into(),
            content_type: "application/pdf".into(),
            body: body.to_vec(),
            ..Default::default()
        }
    }

    fn refused(request: UploadRequest) -> StorageErrorKind {
        admit(&request, MAX).expect_err("refused").kind
    }

    #[test]
    fn an_upload_is_capped() {
        assert!(admit(&request(&[0; MAX]), MAX).is_ok());
        assert!(admit(&request(&[]), MAX).is_ok());
        let refused = admit(&request(&[0; MAX + 1]), MAX).expect_err("refused");
        assert_eq!(refused.kind, StorageErrorKind::TooLarge);
        assert_eq!(
            refused.message,
            "upload of 17 bytes exceeds the cap of 16 bytes"
        );
    }

    #[test]
    fn the_cap_is_what_fits_the_envelope_when_that_is_lower() {
        const MIB: usize = 1024 * 1024;
        assert_eq!(upload_cap(DEFAULT_MAX_UPLOAD_BYTES, 64 * MIB), 50_319_360);
        assert_eq!(upload_cap(MIB, 64 * MIB), MIB);
        assert_eq!(upload_cap(DEFAULT_MAX_UPLOAD_BYTES, MIB), 774_144);
        assert_eq!(upload_cap(DEFAULT_MAX_UPLOAD_BYTES, ENVELOPE_SLACK), 0);
        assert_eq!(upload_cap(DEFAULT_MAX_UPLOAD_BYTES, 0), 0);
        let fits = |body: usize, max_guest_len: usize| {
            let envelope = serde_json::to_vec(&UploadRequest {
                content_disposition: Some(format!("attachment; filename=\"{}\"", "q".repeat(1024))),
                cache_control: Some("private, max-age=60".into()),
                ..request(&vec![0; body])
            })
            .expect("json");
            envelope.len() <= max_guest_len
        };
        let cap = upload_cap(DEFAULT_MAX_UPLOAD_BYTES, MIB);
        assert!(fits(cap, MIB));
        assert!(fits(cap + 1, MIB), "just over the cap is still an envelope");
        assert!(!fits(MIB, MIB));
    }

    #[test]
    fn the_configured_cap_follows_the_environment() {
        assert_eq!(
            max_upload_bytes(None).expect("default"),
            DEFAULT_MAX_UPLOAD_BYTES
        );
        assert_eq!(max_upload_bytes(Some("1024".into())).expect("set"), 1024);
        for bad in ["0", "-1", "lots"] {
            assert!(max_upload_bytes(Some(bad.into())).is_err(), "{bad}");
        }
    }

    #[test]
    fn an_object_is_stored_with_what_it_is_served_with() {
        let served = UploadRequest {
            content_disposition: Some("attachment; filename=\"receipt.pdf\"".into()),
            cache_control: Some("private, max-age=60".into()),
            ..request(b"%PDF")
        };
        assert_eq!(
            admit(&served, MAX),
            Ok(UploadOptions {
                content_type: "application/pdf".into(),
                content_disposition: Some("attachment; filename=\"receipt.pdf\"".into()),
                cache_control: Some("private, max-age=60".into()),
            })
        );
        for untyped in ["", "  "] {
            let options = admit(
                &UploadRequest {
                    content_type: untyped.into(),
                    ..request(b"%PDF")
                },
                MAX,
            )
            .expect("admitted");
            assert_eq!(
                options,
                UploadOptions::content_type("application/octet-stream")
            );
        }
    }

    #[test]
    fn no_value_can_add_a_header_of_its_own() {
        let injected = "text/html\r\nx-amz-acl: public-read";
        for request in [
            UploadRequest {
                content_type: injected.into(),
                ..request(b"")
            },
            UploadRequest {
                content_type: "text/html\n".into(),
                ..request(b"")
            },
            UploadRequest {
                content_disposition: Some(injected.into()),
                ..request(b"")
            },
            UploadRequest {
                cache_control: Some("no-store\n".into()),
                ..request(b"")
            },
        ] {
            assert_eq!(
                refused(request.clone()),
                StorageErrorKind::Denied,
                "{request:?}"
            );
        }
    }
}

#[cfg(test)]
mod bounded_tests {
    use super::*;
    use crate::policy::PolicyMode;
    use crate::wasm_runtime::budget::testing::{self, MIB};
    use std::time::Duration;

    async fn stored(objects: &[(&str, Vec<u8>)]) -> S3Storage {
        let storage = S3Storage::from_inner(cufflink_storage::in_memory("media"));
        for (key, bytes) in objects {
            storage
                .upload_to_bucket("media", key, bytes.clone(), "application/octet-stream")
                .await
                .expect("stored");
        }
        storage
    }

    fn enforced() -> HostCaps {
        HostCaps {
            mode: PolicyMode::Enforce,
            ..HostCaps::default()
        }
    }

    #[tokio::test]
    async fn an_object_is_read_under_its_reservation_and_a_missing_one_fails_as_before() {
        let storage = stored(&[("a.bin", vec![7; 3 * MIB])]).await;
        let (ledger, mut hold) = testing::hold(64 * MIB);
        let bytes = fetch_object(
            &storage,
            "media",
            "a.bin",
            &mut hold,
            &enforced(),
            "s3_download",
        )
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(bytes, vec![7; 3 * MIB]);
        assert_eq!(ledger.in_use(), 7 * MIB, "the bytes and their base64");
        drop(hold);
        assert_eq!(ledger.in_use(), 0);

        let before = storage
            .download_from_bucket("media", "gone")
            .await
            .expect_err("missing")
            .to_string();
        let (_, mut hold) = testing::hold(64 * MIB);
        match fetch_object(
            &storage,
            "media",
            "gone",
            &mut hold,
            &enforced(),
            "s3_download",
        )
        .await
        {
            Err(Fetch::Failed(e)) => assert_eq!(e.to_string(), before),
            other => panic!(
                "{}",
                other.map_or_else(|e| e.to_string(), |_| "read".into())
            ),
        }
    }

    #[tokio::test]
    async fn an_object_past_the_response_cap_or_the_budget_is_refused_before_it_is_read() {
        let storage = stored(&[("a.bin", vec![1; 4 * MIB])]).await;
        let capped = HostCaps {
            response: 4 * MIB,
            ..enforced()
        };
        let (ledger, mut hold) = testing::hold(64 * MIB);
        assert!(matches!(
            fetch_object(
                &storage,
                "media",
                "a.bin",
                &mut hold,
                &capped,
                "s3_download"
            )
            .await,
            Err(Fetch::Refused(Refused::Cap(_)))
        ));
        assert_eq!(ledger.in_use(), 0);
        let warned = HostCaps {
            mode: PolicyMode::Warn,
            ..capped
        };
        assert!(
            fetch_object(
                &storage,
                "media",
                "a.bin",
                &mut hold,
                &warned,
                "s3_download"
            )
            .await
            .is_ok(),
            "warn counts and reads"
        );
        drop(hold);

        let (ledger, mut hold) = testing::hold(8 * MIB);
        assert!(matches!(
            fetch_object(
                &storage,
                "media",
                "a.bin",
                &mut hold,
                &enforced(),
                "s3_download"
            )
            .await,
            Err(Fetch::Refused(Refused::Budget(_)))
        ));
        assert_eq!(ledger.in_use(), 0);
    }

    #[tokio::test]
    async fn a_read_stops_at_the_size_it_was_reserved_for() {
        let chunks = futures_util::stream::iter([
            Ok(bytes::Bytes::from_static(b"abc")),
            Ok(bytes::Bytes::from_static(b"def")),
        ]);
        assert_eq!(
            read_within(Box::pin(chunks), 6).await.expect("fits"),
            b"abcdef"
        );
        let chunks = futures_util::stream::iter([
            Ok(bytes::Bytes::from_static(b"abc")),
            Ok(bytes::Bytes::from_static(b"def")),
        ]);
        assert!(read_within(Box::pin(chunks), 5).await.is_err());
    }

    #[tokio::test]
    async fn fan_out_results_keep_their_order_and_the_envelope_is_unchanged() {
        let results = || {
            vec![
                BatchResult::ok(serde_json::json!({"data_b64": "AQI="})),
                BatchResult::err("storage access denied"),
                BatchResult::ok(serde_json::json!({"data_b64": ""})),
            ]
        };
        let expected = serde_json::to_string(&results()).expect("json");
        let delays = [30u64, 0, 10];
        let fetches = results()
            .into_iter()
            .zip(delays)
            .map(|(result, delay)| async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                Ok((result, Hold::none()))
            });
        let (ledger, mut hold) = testing::hold(64 * MIB);
        let caps = enforced();
        let mut text = ResponseText::new(&mut hold, &caps, "s3_download_many");
        write_in_order(fetches, 3, &mut text)
            .await
            .expect("written");
        assert_eq!(text.into_text(), expected);
        assert_eq!(hold.bytes(), ledger.in_use());

        let refused = Refused::Budget(crate::wasm_runtime::budget::Capacity {
            reason: crate::wasm_runtime::budget::Reason::Memory,
            requested: 1,
            holding: 1,
        });
        let fetches = [Ok((BatchResult::err("x"), Hold::none())), Err(refused)]
            .into_iter()
            .map(std::future::ready);
        let mut hold = Hold::none();
        let mut text = ResponseText::new(&mut hold, &caps, "s3_download_many");
        assert!(matches!(
            write_in_order(fetches, 2, &mut text).await,
            Err(ChargedJsonError::Capacity(_))
        ));
    }

    #[test]
    fn response_text_is_held_to_a_cap_smaller_than_its_charge_step() {
        let caps = HostCaps {
            response: 64 * 1024,
            ..enforced()
        };
        let (ledger, mut hold) = testing::hold(64 * MIB);
        let mut text = ResponseText::new(&mut hold, &caps, "s3_download_many");
        text.push(&"x".repeat(64 * 1024)).expect("at the cap");
        assert!(matches!(text.push("x"), Err(Refused::Cap(_))));
        drop(text);
        assert_eq!(hold.bytes(), 64 * 1024);
        assert_eq!(ledger.in_use(), 64 * 1024);
    }
}
