use super::util::{ended, exchange};
use super::{
    block_on_bounded, fanout_concurrency, read_guest_string, wrap, BatchResult, HostState,
};
use crate::policy::Denied;
use crate::storage::S3Storage;
use crate::wasm_runtime::storage_scope;
use cufflink_storage::UploadOptions;
use cufflink_types::host::storage::{
    HeadRequest, HeadResponse, StorageErrorEnvelope, StorageErrorKind, UploadRequest,
    UploadResponse,
};
use reqwest::header::HeaderValue;
use std::collections::HashSet;
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

            let app_state = caller.data().app_state.clone();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state
                    .storage
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("S3 storage not configured"))?;

                let data = storage.download_from_bucket(&bucket, &key).await?;

                use base64::Engine;
                let b64 = base64::engine::general_purpose::STANDARD.encode(&data);
                Ok::<String, eyre::Report>(b64)
            });

            match result {
                Ok(b64) => {
                    caller.data_mut().host_response = Some(b64);
                    0
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
            let items_json = host_try!(read_guest_string(
                &mut caller,
                "s3_download_many",
                items_ptr,
                items_len
            ));
            let items: Vec<S3Item> = match serde_json::from_str(&items_json) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("[wasm] s3_download_many: bad input: {}", e);
                    return -1;
                }
            };
            let items: Vec<(S3Item, bool)> = items
                .into_iter()
                .map(|item| {
                    let permitted = caller.data().authorize_storage(&item.bucket, &[&item.key]);
                    (item, permitted)
                })
                .collect();
            let app_state = caller.data().app_state.clone();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state.storage.as_ref().cloned();
                let permits = fanout_concurrency();
                let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(permits));
                let futs = items.into_iter().map(|(item, permitted)| {
                    let sem = sem.clone();
                    let storage = storage.clone();
                    async move {
                        if !permitted {
                            return BatchResult::err("storage access denied");
                        }
                        let _p = sem.acquire_owned().await.ok();
                        let storage = match storage.as_ref() {
                            Some(s) => s,
                            None => {
                                return BatchResult::err("S3 storage not configured");
                            }
                        };
                        match storage.download_from_bucket(&item.bucket, &item.key).await {
                            Ok(bytes) => {
                                use base64::Engine;
                                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                                BatchResult::ok(serde_json::json!({ "data_b64": b64 }))
                            }
                            Err(e) => BatchResult::err(&e.to_string()),
                        }
                    }
                });
                let results: Vec<BatchResult> = futures_util::future::join_all(futs)
                    .await
                    .into_iter()
                    .collect();
                Ok::<String, eyre::Report>(serde_json::to_string(&results)?)
            });

            match result {
                Ok(json) => {
                    caller.data_mut().host_response = Some(json);
                    0
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
                Ok(url) => {
                    caller.data_mut().host_response = Some(url);
                    0
                }
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
                Ok(url) => {
                    caller.data_mut().host_response = Some(url);
                    0
                }
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
