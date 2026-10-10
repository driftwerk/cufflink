use super::{
    block_on_bounded, read_guest_bytes, read_guest_string, wrap, write_guest_bytes, HostIoError,
    HostState,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use wasmtime::{Caller, Linker};

pub(super) fn exchange<Request, Response>(
    caller: &mut Caller<'_, HostState>,
    host_fn: &'static str,
    (ptr, len): (i32, i32),
    malformed: impl FnOnce(String) -> Response,
    handle: impl FnOnce(&HostState, Request) -> Response,
) -> i32
where
    Request: DeserializeOwned,
    Response: Serialize,
{
    caller.data_mut().host_response = None;
    let bytes = host_try!(read_guest_bytes(caller, host_fn, ptr, len));
    let response = match serde_json::from_slice::<Request>(&bytes) {
        Ok(request) => handle(caller.data(), request),
        Err(e) => malformed(format!("malformed {host_fn} request: {e}")),
    };
    match serde_json::to_string(&response) {
        Ok(json) => {
            caller.data_mut().host_response = Some(json);
            0
        }
        Err(e) => {
            tracing::error!("[wasm] {host_fn}: encode failed: {e}");
            -1
        }
    }
}

pub(super) fn ended<E>(ended: HostIoError<E>, waited: impl FnOnce(String) -> E) -> E {
    match ended {
        HostIoError::Failed(e) => e,
        HostIoError::TimedOut(after) => waited(HostIoError::<String>::TimedOut(after).to_string()),
        HostIoError::Cancelled => waited(HostIoError::<String>::Cancelled.to_string()),
    }
}

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "set_response",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| {
            if let Ok(s) = read_guest_string(&mut caller, "set_response", ptr, len) {
                caller.data_mut().host_response = Some(s);
            }
        },
    )?;

    wrap(
        linker,
        names,
        "get_host_response",
        |mut caller: Caller<'_, HostState>, buf_ptr: i32, buf_len: i32| -> i32 {
            let Some(resp) = caller.data_mut().host_response.take() else {
                return 0;
            };
            let written = write_guest_bytes(
                &mut caller,
                "get_host_response",
                buf_ptr,
                buf_len,
                resp.as_bytes(),
            );
            caller.data_mut().host_response = Some(resp);
            match written {
                Ok(n) => n as i32,
                Err(e) => e.code(),
            }
        },
    )?;

    wrap(
        linker,
        names,
        "get_host_response_len",
        |caller: Caller<'_, HostState>| -> i32 {
            match &caller.data().host_response {
                Some(resp) => resp.len() as i32,
                None => 0,
            }
        },
    )?;

    wrap(
        linker,
        names,
        "generate_uuid",
        |mut caller: Caller<'_, HostState>| -> i32 {
            let id = uuid::Uuid::new_v4().to_string();
            caller.data_mut().host_response = Some(id);
            0
        },
    )?;

    wrap(
        linker,
        names,
        "current_time",
        |mut caller: Caller<'_, HostState>| -> i32 {
            let now = chrono::Utc::now().to_rfc3339();
            caller.data_mut().host_response = Some(now);
            0
        },
    )?;

    wrap(
        linker,
        names,
        "context_tenant",
        |mut caller: Caller<'_, HostState>| -> i32 {
            let slug = caller.data().identity.tenant_slug.clone();
            caller.data_mut().host_response = Some(slug);
            0
        },
    )?;

    wrap(
        linker,
        names,
        "get_config",
        |mut caller: Caller<'_, HostState>, key_ptr: i32, key_len: i32| -> i32 {
            let key = host_try!(read_guest_string(
                &mut caller,
                "get_config",
                key_ptr,
                key_len
            ));

            let service_id = caller.data().identity.service_id;
            let app_state = caller.data().app_state.clone();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let client = app_state.platform_db.get().await?;
                let value = crate::repo::service_configs::get(
                    &app_state.secrets,
                    &client,
                    service_id,
                    &key,
                )
                .await?;
                Ok::<_, eyre::Report>(value)
            });

            match result {
                Ok(Some(val)) => {
                    caller.data_mut().host_response = Some(val.expose().to_string());
                    0
                }
                Ok(None) => {
                    caller.data_mut().host_response = None;
                    0
                }
                Err(e) => {
                    tracing::error!("[wasm] get_config error: {}", e);
                    -1
                }
            }
        },
    )?;

    Ok(())
}

pub(super) fn register_v2(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "util_sleep_ms",
        |caller: Caller<'_, HostState>, ms: i32| -> i32 {
            let Some(requested) = requested(ms) else {
                return -1;
            };
            let state = caller.data();
            let shutdown = state.app_state.shutdown.clone();
            let granted = sleep_for(
                requested,
                state.remaining_budget(),
                state.app_state.config.sleep_max,
            );
            let slept = sleep(granted, || {
                state.cancel.is_cancelled() || shutdown.hard_deadline_reached()
            });
            i32::try_from(slept.as_millis()).unwrap_or(i32::MAX)
        },
    )?;
    wrap(linker, names, "util_now_millis", || -> i64 {
        chrono::Utc::now().timestamp_millis()
    })
}

const SLEEP_BUDGET_MARGIN: Duration = Duration::from_secs(1);
const SLEEP_SLICE: Duration = Duration::from_millis(100);

fn requested(ms: i32) -> Option<Duration> {
    u64::try_from(ms).ok().map(Duration::from_millis)
}

fn sleep_for(requested: Duration, remaining: Duration, longest: Duration) -> Duration {
    requested
        .min(remaining.saturating_sub(SLEEP_BUDGET_MARGIN))
        .min(longest)
}

fn sleep(granted: Duration, woken: impl Fn() -> bool) -> Duration {
    let started = Instant::now();
    std::iter::repeat(())
        .map(|()| granted.saturating_sub(started.elapsed()))
        .take_while(|left| !left.is_zero() && !woken())
        .for_each(|left| std::thread::sleep(left.min(SLEEP_SLICE)));
    started.elapsed().min(granted)
}

pub const DEFAULT_SLEEP_MAX: Duration = Duration::from_secs(30);

pub fn sleep_max_from_env() -> eyre::Result<Duration> {
    sleep_max(std::env::var("UTIL_SLEEP_MAX_MS").ok())
}

fn sleep_max(raw: Option<String>) -> eyre::Result<Duration> {
    cufflink_db::positive_var(
        "UTIL_SLEEP_MAX_MS",
        raw,
        DEFAULT_SLEEP_MAX.as_millis() as u64,
    )
    .map(Duration::from_millis)
}

#[cfg(test)]
mod v2_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    const LONGEST: Duration = DEFAULT_SLEEP_MAX;

    #[test]
    fn a_negative_count_is_no_request() {
        assert_eq!(requested(-1), None);
        assert_eq!(requested(i32::MIN), None);
        assert_eq!(requested(0), Some(Duration::ZERO));
        assert_eq!(requested(1_500), Some(Duration::from_millis(1_500)));
        assert_eq!(
            requested(i32::MAX),
            Some(Duration::from_millis(2_147_483_647))
        );
    }

    #[test]
    fn a_sleep_ends_before_the_budget_and_the_installs_longest() {
        let secs = Duration::from_secs;
        assert_eq!(sleep_for(secs(60), secs(5), LONGEST), secs(4));
        assert_eq!(sleep_for(secs(2), secs(5), LONGEST), secs(2));
        assert_eq!(sleep_for(secs(60), secs(900), LONGEST), LONGEST);
        assert_eq!(sleep_for(secs(60), secs(900), secs(120)), secs(60));
        assert_eq!(sleep_for(secs(60), secs(1), LONGEST), Duration::ZERO);
        assert_eq!(
            sleep_for(secs(60), Duration::from_millis(400), LONGEST),
            Duration::ZERO
        );
        assert_eq!(sleep_for(Duration::ZERO, secs(5), LONGEST), Duration::ZERO);
    }

    #[test]
    fn a_sleep_lasts_what_it_was_granted() {
        let granted = Duration::from_millis(250);
        let started = Instant::now();
        assert_eq!(sleep(granted, || false), granted);
        assert!(started.elapsed() >= granted);
        assert!(started.elapsed() < Duration::from_secs(2));
        let looked = AtomicUsize::new(0);
        assert_eq!(
            sleep(Duration::ZERO, || {
                looked.fetch_add(1, Ordering::SeqCst);
                false
            }),
            Duration::ZERO
        );
        assert_eq!(looked.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_cancelled_or_shut_down_invocation_wakes_within_a_slice() {
        assert!(sleep(LONGEST, || true) < SLEEP_SLICE);
        let woken = Arc::new(AtomicBool::new(false));
        let waker = Arc::clone(&woken);
        let cancel = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            waker.store(true, Ordering::SeqCst);
        });
        let slept = sleep(LONGEST, || woken.load(Ordering::SeqCst));
        cancel.join().expect("join");
        assert!(slept >= Duration::from_millis(100), "{slept:?}");
        assert!(slept < Duration::from_secs(2), "{slept:?}");
    }

    #[test]
    fn the_longest_sleep_follows_the_environment() {
        assert_eq!(sleep_max(None).expect("default"), LONGEST);
        assert_eq!(
            sleep_max(Some("2000".into())).expect("set"),
            Duration::from_secs(2)
        );
        for bad in ["0", "-1", "soon"] {
            assert!(sleep_max(Some(bad.into())).is_err(), "{bad}");
        }
    }
}

#[cfg(test)]
mod envelope_tests {
    use crate::wasm_runtime::tests::{engine, install, invocation};
    use crate::wasm_runtime::InvocationKind;
    use crate::AppState;
    use cufflink_types::host::ext::{CallErrorKind, CallResponse};
    use cufflink_types::host::http::{FetchResponseV2, HttpErrorKind};
    use cufflink_types::host::storage::{HeadResponse, StorageErrorKind, UploadResponse};
    use std::sync::Arc;
    use std::time::Duration;

    const MALFORMED: &str = r#"(module
      (import "env" "set_response" (func $set_response (param i32 i32)))
      (import "env" "get_host_response" (func $response (param i32 i32) (result i32)))
      (import "env" "http_fetch_v2" (func $http (param i32 i32) (result i32)))
      (import "env" "s3_upload" (func $upload (param i32 i32) (result i32)))
      (import "env" "s3_head" (func $head (param i32 i32) (result i32)))
      (import "env" "ext_call" (func $ext (param i32 i32) (result i32)))
      (memory (export "memory") 2)
      (global $heap (mut i32) (i32.const 8192))
      (data (i32.const 0) "stale")
      (data (i32.const 16) "{\"nope\":1}")
      (func (export "alloc") (param $n i32) (result i32)
        (local $p i32)
        (local.set $p (global.get $heap))
        (global.set $heap (i32.add (local.get $p) (local.get $n)))
        (local.get $p))
      (func $reply (param $rc i32) (result i32)
        (i32.store8 (i32.const 1028)
          (select (i32.const 69) (i32.const 75) (i32.lt_s (local.get $rc) (i32.const 0))))
        (i32.store (i32.const 1024)
          (i32.add (call $response (i32.const 1029) (i32.const 4096)) (i32.const 1)))
        (i32.const 1024))
      (func $stale (call $set_response (i32.const 0) (i32.const 5)))
      (func (export "http") (param i32 i32) (result i32)
        (call $stale)
        (call $reply (call $http (i32.const 16) (i32.const 10))))
      (func (export "upload") (param i32 i32) (result i32)
        (call $stale)
        (call $reply (call $upload (i32.const 16) (i32.const 10))))
      (func (export "head") (param i32 i32) (result i32)
        (call $stale)
        (call $reply (call $head (i32.const 16) (i32.const 10))))
      (func (export "ext") (param i32 i32) (result i32)
        (call $stale)
        (call $reply (call $ext (i32.const 16) (i32.const 10))))
      (func (export "unreadable") (param i32 i32) (result i32)
        (call $stale)
        (call $reply (call $head (i32.const 16) (i32.const -1)))))"#;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_malformed_envelope_is_an_error_and_never_an_earlier_response() {
        let engine = engine();
        let (_, compiled) = install(&engine, MALFORMED);
        let state = AppState::minimal(engine.clone()).await;
        let answered = |handler: &'static str| {
            let run = engine.run(
                Arc::clone(&state),
                invocation(
                    Arc::clone(&compiled),
                    handler,
                    InvocationKind::Route,
                    Duration::from_secs(5),
                ),
            );
            async move {
                let reply = run.await.unwrap_or_else(|e| panic!("{handler}: {e}"));
                assert!(!reply.contains("stale"), "{handler}: {reply}");
                reply
            }
        };
        fn envelope<T: serde::de::DeserializeOwned>(reply: &str) -> T {
            let (sign, body) = reply.split_at(1);
            assert_eq!(sign, "K", "{reply}");
            serde_json::from_str(body).unwrap_or_else(|e| panic!("{reply}: {e}"))
        }
        let http: FetchResponseV2 = envelope(&answered("http").await);
        assert!(
            matches!(
                &http,
                FetchResponseV2::Error(e)
                    if e.kind == HttpErrorKind::InvalidRequest
                        && e.message.starts_with("malformed http_fetch_v2 request: ")
            ),
            "{http:?}"
        );
        let upload: UploadResponse = envelope(&answered("upload").await);
        assert!(
            matches!(
                &upload,
                UploadResponse::Error(e)
                    if e.kind == StorageErrorKind::Denied
                        && e.message.starts_with("malformed s3_upload request: ")
            ),
            "{upload:?}"
        );
        let head: HeadResponse = envelope(&answered("head").await);
        assert!(
            matches!(
                &head,
                HeadResponse::Error(e)
                    if e.kind == StorageErrorKind::Denied
                        && e.message.starts_with("malformed s3_head request: ")
            ),
            "{head:?}"
        );
        let ext: CallResponse = envelope(&answered("ext").await);
        assert!(
            matches!(
                &ext,
                CallResponse::Error(e)
                    if e.kind == CallErrorKind::Invalid
                        && e.code.is_none()
                        && e.message.starts_with("malformed ext_call request: ")
            ),
            "{ext:?}"
        );
        assert_eq!(answered("unreadable").await, "E");
    }
}
