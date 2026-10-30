use super::budget::{
    stepped, Capped, ChargedJson, ChargedJsonError, Hold, HostCaps, Meter, Refused,
};
use super::{wt, InvocationKind, WasmLimits};
use crate::nats_manager::subjects::PublishScope;
use crate::policy::PolicySet;
use crate::registry::ServiceIdentity;
use crate::router::jobs::AuthSnapshot;
use crate::AppState;
use std::collections::HashSet;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use wasmtime::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GuestReadError {
    NoMemory,
    InvalidLength,
    OutOfBounds,
    NotUtf8,
    Refused,
}

impl GuestReadError {
    pub(crate) const fn code(self) -> i32 {
        -1
    }
}

macro_rules! host_try {
    ($read:expr) => {
        match $read {
            Ok(value) => value,
            Err(err) => return err.code(),
        }
    };
}

mod cache;
mod db;
mod db_v2;
mod ext;
mod http;
mod image;
mod jobs;
mod limits;
mod log;
mod nats;
mod redis;
mod rows;
mod service_call;
mod storage;
mod util;

pub use cache::CacheLimits;
pub use db::{
    run_db_batch, run_guest_statement, BatchFailure, DbCallError, GuestDbOutcome, GuestStatement,
};
pub use db_v2::{call as db_v2_call, AbandonReason, DbSession, DbTarget};
pub use http::{outbound_connect_timeout, FetchLimits};
pub(crate) use limits::{BudgetDenial, GuestLimits, LimitContext};
pub use nats::NatsPublishPolicy;
#[cfg(test)]
pub(crate) use redis::RESERVED_REDIS_KEY;
pub use rows::{RowText, Stopped};
pub(crate) use service_call::caller_depth;
pub use service_call::LocalDispatch;
pub use storage::{max_upload_bytes_from_env, DEFAULT_MAX_UPLOAD_BYTES};
pub(crate) use storage::{read_object, Fetch};
pub use util::{sleep_max_from_env, DEFAULT_SLEEP_MAX};

pub(crate) fn guest_memory(caller: &mut Caller<'_, HostState>) -> Result<Memory, GuestReadError> {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or(GuestReadError::NoMemory)
}

pub(crate) fn checked_len(len: i32, max: usize) -> Result<usize, GuestReadError> {
    usize::try_from(len)
        .ok()
        .filter(|n| *n <= max)
        .ok_or(GuestReadError::InvalidLength)
}

fn guest_range(ptr: i32, len: usize, size: usize) -> Result<Range<usize>, GuestReadError> {
    let start = usize::try_from(ptr).map_err(|_| GuestReadError::OutOfBounds)?;
    let end = start
        .checked_add(len)
        .filter(|end| *end <= size)
        .ok_or(GuestReadError::OutOfBounds)?;
    Ok(start..end)
}

fn checked_guest_len(
    caller: &mut Caller<'_, HostState>,
    host_fn: &'static str,
    len: i32,
    max: usize,
) -> Result<usize, GuestReadError> {
    checked_len(len, max).inspect_err(|_| {
        metrics::counter!(
            "cufflink_wasm_limit_events_total",
            "limit" => "length",
            "mode" => "enforce",
        )
        .increment(1);
        let state = caller.data_mut();
        if !state.length_violation_logged {
            state.length_violation_logged = true;
            tracing::warn!(
                host_fn,
                len,
                max,
                tenant = %state.identity.tenant_slug,
                service = %state.identity.service_name,
                "Guest passed an invalid length to a host function"
            );
        }
    })
}

pub(crate) struct GuestInput<T> {
    value: T,
    hold: Hold,
}

impl<T> std::ops::Deref for GuestInput<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: std::fmt::Display> std::fmt::Display for GuestInput<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl<T> GuestInput<T> {
    pub(crate) fn into_parts(self) -> (T, Hold) {
        (self.value, self.hold)
    }
}

pub(crate) fn read_guest_bytes(
    caller: &mut Caller<'_, HostState>,
    host_fn: &'static str,
    ptr: i32,
    len: i32,
) -> Result<GuestInput<Vec<u8>>, GuestReadError> {
    let memory = guest_memory(caller)?;
    let max = caller.data().limits.max_guest_len;
    let len = checked_guest_len(caller, host_fn, len, max)?;
    let range = guest_range(ptr, len, memory.data_size(&*caller))?;
    let mut hold = caller.data().guest_limits.hold();
    if let Err(capacity) = hold.grow(len) {
        caller
            .data_mut()
            .guest_limits
            .host_refused(Refused::Budget(capacity));
        return Err(GuestReadError::Refused);
    }
    Ok(GuestInput {
        value: memory.data(&*caller)[range].to_vec(),
        hold,
    })
}

pub(crate) fn read_guest_string(
    caller: &mut Caller<'_, HostState>,
    host_fn: &'static str,
    ptr: i32,
    len: i32,
) -> Result<GuestInput<String>, GuestReadError> {
    let (bytes, hold) = read_guest_bytes(caller, host_fn, ptr, len)?.into_parts();
    String::from_utf8(bytes)
        .map(|value| GuestInput { value, hold })
        .map_err(|_| GuestReadError::NotUtf8)
}

pub(crate) fn copy_host_response(
    caller: &mut Caller<'_, HostState>,
    ptr: i32,
    cap: i32,
) -> Result<usize, GuestReadError> {
    let memory = guest_memory(caller)?;
    let cap = checked_guest_len(caller, "get_host_response", cap, usize::MAX)?;
    let (data, state) = memory.data_and_store_mut(&mut *caller);
    match &state.host_response {
        Some(response) => copy_into_guest(data, ptr, cap, response.text.as_bytes()),
        None => Ok(0),
    }
}

fn copy_into_guest(
    data: &mut [u8],
    ptr: i32,
    cap: usize,
    bytes: &[u8],
) -> Result<usize, GuestReadError> {
    let len = bytes.len().min(cap);
    let range = guest_range(ptr, len, data.len())?;
    data[range].copy_from_slice(&bytes[..len]);
    Ok(len)
}

const HOST_IO_DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

fn block_on<T>(fut: impl Future<Output = T>) -> T {
    tokio::runtime::Handle::current().block_on(fut)
}

#[derive(Debug)]
pub(crate) enum HostIoError<E> {
    TimedOut(Duration),
    Cancelled,
    Failed(E),
}

impl<E: std::fmt::Display> std::fmt::Display for HostIoError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut(after) => write!(f, "host call timed out after {after:?}"),
            Self::Cancelled => f.write_str("host call abandoned: the execution was cancelled"),
            Self::Failed(e) => e.fmt(f),
        }
    }
}

pub(crate) struct Bounds {
    pub remaining: Duration,
    pub cancel: CancellationToken,
}

fn block_on_bounded<T, E>(
    bounds: &Bounds,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, HostIoError<E>> {
    block_on_within(
        clamp_to_budget(None, HOST_IO_DEFAULT_TIMEOUT, bounds.remaining),
        &bounds.cancel,
        fut,
    )
}

fn block_on_within<T, E>(
    timeout: Duration,
    cancel: &CancellationToken,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, HostIoError<E>> {
    block_on(async {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(HostIoError::Cancelled),
            ended = tokio::time::timeout(timeout, fut) => ended
                .map_err(|_| HostIoError::TimedOut(timeout))?
                .map_err(HostIoError::Failed),
        }
    })
}

pub(crate) struct HostStateInit {
    pub app_state: Arc<AppState>,
    pub identity: ServiceIdentity,
    pub service_version: i32,
    pub auth: Option<AuthSnapshot>,
    pub call_depth: u8,
    pub nats_publish_policy: NatsPublishPolicy,
    pub nats_publish_scope: PublishScope,
    pub extensions: Arc<[String]>,
    pub deadline: std::time::Instant,
    pub cancel: CancellationToken,
    pub limits: Arc<WasmLimits>,
    pub guest_limits: GuestLimits,
    pub kind: InvocationKind,
    pub policies: Arc<PolicySet>,
}

pub struct HostState {
    pub app_state: Arc<AppState>,
    pub identity: ServiceIdentity,
    pub service_version: i32,
    pub auth: Option<AuthSnapshot>,
    pub call_depth: u8,
    pub nats_publish_policy: NatsPublishPolicy,
    pub nats_publish_scope: PublishScope,
    pub extensions: Arc<[String]>,
    pub deadline: std::time::Instant,
    pub cancel: CancellationToken,
    pub limits: Arc<WasmLimits>,
    pub(crate) guest_limits: GuestLimits,
    pub kind: InvocationKind,
    pub policies: Arc<PolicySet>,
    host_response: Option<HostResponse>,
    pub db: DbSession,
    length_violation_logged: bool,
}

impl HostState {
    pub(crate) fn new(init: HostStateInit) -> Self {
        let db = DbSession::new(init.app_state.config.max_open_tx);
        Self {
            app_state: init.app_state,
            identity: init.identity,
            service_version: init.service_version,
            auth: init.auth,
            call_depth: init.call_depth,
            nats_publish_policy: init.nats_publish_policy,
            nats_publish_scope: init.nats_publish_scope,
            extensions: init.extensions,
            deadline: init.deadline,
            cancel: init.cancel,
            limits: init.limits,
            guest_limits: init.guest_limits,
            kind: init.kind,
            db,
            policies: init.policies,
            host_response: None,
            length_violation_logged: false,
        }
    }

    pub fn remaining_budget(&self) -> std::time::Duration {
        self.deadline
            .saturating_duration_since(std::time::Instant::now())
    }

    pub(crate) fn bounds(&self) -> Bounds {
        Bounds {
            remaining: self.remaining_budget(),
            cancel: self.cancel.clone(),
        }
    }

    pub(crate) fn caps(&self) -> &HostCaps {
        &self.limits.budget.caps
    }

    pub(crate) fn hold(&self) -> Hold {
        self.guest_limits.hold()
    }

    pub(crate) fn host_response_len(&self) -> usize {
        self.host_response
            .as_ref()
            .map_or(0, |response| response.text.len())
    }

    pub(crate) fn clear_host_response(&mut self) {
        self.host_response = None;
    }

    pub(crate) fn set_charged_response(&mut self, text: String, hold: Hold) {
        self.host_response = Some(HostResponse { text, _hold: hold });
    }

    pub(crate) fn set_host_response(
        &mut self,
        path: &'static str,
        text: String,
    ) -> Result<(), Refused> {
        self.host_response = None;
        self.caps()
            .response(path, text.len())
            .map_err(Refused::Cap)?;
        let mut hold = self.hold();
        hold.grow(text.capacity()).map_err(Refused::Budget)?;
        self.set_charged_response(text, hold);
        Ok(())
    }

    pub(crate) fn respond(&mut self, path: &'static str, text: String) -> i32 {
        match self.set_host_response(path, text) {
            Ok(()) => 0,
            Err(refused) => self.refused(refused),
        }
    }

    pub(crate) fn respond_with(&mut self, path: &'static str, text: Option<String>) -> i32 {
        match text {
            Some(text) => self.respond(path, text),
            None => {
                self.clear_host_response();
                0
            }
        }
    }

    pub(crate) fn respond_json<T: serde::Serialize + ?Sized>(
        &mut self,
        path: &'static str,
        value: &T,
    ) -> i32 {
        self.host_response = None;
        let limits = Arc::clone(&self.limits);
        let mut hold = self.hold();
        match ChargedJson::write(
            value,
            &mut Capped::new(&mut hold, &limits.budget.caps, path),
        ) {
            Ok(text) => {
                self.set_charged_response(text, hold);
                0
            }
            Err(ChargedJsonError::Capacity(refused)) => self.refused(refused),
            Err(ChargedJsonError::Syntax(e)) => {
                tracing::error!("[wasm] {path}: encode failed: {e}");
                -1
            }
        }
    }

    pub(crate) fn refused(&mut self, refused: Refused) -> i32 {
        self.guest_limits.host_refused(refused);
        -1
    }
}

struct HostResponse {
    text: String,
    _hold: Hold,
}

fn wrap<Params, Results>(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
    name: &'static str,
    func: impl IntoFunc<HostState, Params, Results>,
) -> eyre::Result<()> {
    linker.func_wrap("env", name, func).map_err(wt)?;
    names.insert(name);
    Ok(())
}

pub const HOST_FEATURES: &[(&str, &[&str])] = &[
    (
        "host_core",
        &[
            "cufflink_log",
            "set_response",
            "get_host_response",
            "get_host_response_len",
            "current_time",
            "generate_uuid",
            "context_tenant",
            "get_config",
        ],
    ),
    ("db", &["db_query", "db_execute"]),
    ("db_v2", &["db_call"]),
    ("db_execute_all", &["db_execute_all"]),
    ("db_query_read_only", &["db_query_read_only"]),
    ("http_fetch", &["http_fetch"]),
    ("http_fetch_timeout", &["http_fetch_timeout"]),
    ("http_fetch_many", &["http_fetch_many"]),
    ("nats", &["nats_publish", "nats_request"]),
    (
        "s3",
        &["s3_download", "s3_presign_upload", "s3_presign_download"],
    ),
    ("s3_delete", &["s3_delete"]),
    ("s3_download_many", &["s3_download_many"]),
    (
        "image_transform",
        &[
            "image_transform_jpeg",
            "image_transform_webp",
            "image_transform_avif",
        ],
    ),
    ("image_transform_multi", &["image_transform_multi"]),
    (
        "redis",
        &[
            "redis_get",
            "redis_set",
            "redis_del",
            "redis_mget",
            "redis_get_status",
        ],
    ),
    ("jobs", &["jobs_enqueue"]),
    ("cache", &["cache_call"]),
    ("service_call", &["service_call"]),
    ("nats_publish_v2", &["nats_publish_v2"]),
    ("http_v2", &["http_fetch_v2"]),
    ("storage_v2", &["s3_upload", "s3_head"]),
    ("util_v2", &["util_sleep_ms", "util_now_millis"]),
    ("ext_call", &["ext_call"]),
];

pub fn supported_features(host_fns: &HashSet<&'static str>) -> Vec<&'static str> {
    HOST_FEATURES
        .iter()
        .filter(|(_, needs)| needs.iter().all(|f| host_fns.contains(f)))
        .map(|(feature, _)| *feature)
        .collect()
}

pub fn register_host_functions(
    linker: &mut Linker<HostState>,
) -> eyre::Result<HashSet<&'static str>> {
    let mut names = HashSet::new();
    [
        log::register,
        util::register,
        db::register,
        db_v2::register,
        nats::register,
        http::register,
        storage::register,
        image::register,
        jobs::register,
        redis::register,
        cache::register,
        service_call::register,
        nats::register_v2,
        http::register_v2,
        storage::register_v2,
        util::register_v2,
        ext::register,
    ]
    .into_iter()
    .try_for_each(|register| register(linker, &mut names))?;
    Ok(names)
}

#[derive(serde::Serialize)]
pub(crate) struct BatchResult {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl BatchResult {
    fn ok(data: serde_json::Value) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
        }
    }
    fn err(msg: &str) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

pub(crate) struct ResponseText<'a> {
    text: String,
    cap: usize,
    meter: Capped<'a, Hold>,
}

impl<'a> ResponseText<'a> {
    pub(crate) fn new(hold: &'a mut Hold, caps: &'a HostCaps, path: &'static str) -> Self {
        Self {
            text: String::new(),
            cap: caps.response,
            meter: Capped::new(hold, caps, path),
        }
    }

    pub(crate) fn push(&mut self, part: &str) -> Result<(), Refused> {
        let needed = self.text.len().saturating_add(part.len());
        if needed > self.text.capacity() {
            let grown = stepped(needed).min(self.cap.max(needed));
            self.meter.charge(grown - self.text.capacity())?;
            self.text.reserve_exact(grown - self.text.len());
        }
        self.text.push_str(part);
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.meter.credit(self.text.capacity());
        self.text = String::new();
    }

    pub(crate) fn into_text(self) -> String {
        self.text
    }
}

pub(crate) async fn write_in_order<Fut>(
    items: impl IntoIterator<Item = Fut>,
    concurrency: usize,
    out: &mut ResponseText<'_>,
) -> Result<(), ChargedJsonError>
where
    Fut: Future<Output = Result<(BatchResult, Hold), Refused>>,
{
    use futures_util::StreamExt;
    out.push("[").map_err(ChargedJsonError::Capacity)?;
    let mut results = futures_util::stream::iter(items).buffered(concurrency.max(1));
    let mut first = true;
    while let Some(item) = results.next().await {
        let (result, mut held) = item.map_err(ChargedJsonError::Capacity)?;
        let text = ChargedJson::write(&result, &mut held)?;
        drop(result);
        if !std::mem::take(&mut first) {
            out.push(",").map_err(ChargedJsonError::Capacity)?;
        }
        out.push(&text).map_err(ChargedJsonError::Capacity)?;
    }
    out.push("]").map_err(ChargedJsonError::Capacity)
}

const FETCH_DEADLINE_MARGIN: std::time::Duration = std::time::Duration::from_secs(1);

const FETCH_TIMEOUT_FLOOR: std::time::Duration = std::time::Duration::from_millis(500);

pub(crate) fn clamp_to_budget(
    requested: Option<std::time::Duration>,
    default: std::time::Duration,
    remaining: std::time::Duration,
) -> std::time::Duration {
    requested.unwrap_or(default).min(budget_cap(remaining))
}

fn budget_cap(remaining: std::time::Duration) -> std::time::Duration {
    remaining
        .saturating_sub(FETCH_DEADLINE_MARGIN)
        .max(FETCH_TIMEOUT_FLOOR)
}

fn duration_from_env(var: &str, default_secs: u64) -> std::time::Duration {
    let secs = std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default_secs);
    std::time::Duration::from_secs(secs)
}

fn fanout_concurrency() -> usize {
    std::env::var("HOST_FANOUT_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(32)
}

#[cfg(test)]
mod bounded_call_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_execution_stops_waiting_on_its_host_call() {
        let cancel = CancellationToken::new();
        let canceller = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            canceller.cancel();
        });
        let started = std::time::Instant::now();
        let ended = tokio::task::spawn_blocking(move || {
            block_on_bounded(
                &Bounds {
                    remaining: Duration::from_secs(30),
                    cancel,
                },
                async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok::<(), String>(())
                },
            )
        })
        .await
        .expect("join");
        assert!(matches!(ended, Err(HostIoError::Cancelled)), "{ended:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_call_ends_as_it_did_when_nothing_cancels_it() {
        let bounded = |wait: Duration, within: Duration| {
            tokio::task::spawn_blocking(move || {
                block_on_within(within, &CancellationToken::new(), async move {
                    tokio::time::sleep(wait).await;
                    Ok::<u8, String>(7)
                })
            })
        };
        assert!(matches!(
            bounded(Duration::ZERO, Duration::from_secs(5))
                .await
                .expect("join"),
            Ok(7)
        ));
        let within = Duration::from_millis(20);
        assert!(matches!(
            bounded(Duration::from_secs(5), within).await.expect("join"),
            Err(HostIoError::TimedOut(after)) if after == within
        ));
    }
}

#[cfg(test)]
mod fetch_timeout_tests {
    use super::{clamp_to_budget, FETCH_DEADLINE_MARGIN, FETCH_TIMEOUT_FLOOR};
    use std::time::Duration;

    const DEFAULT: Duration = Duration::from_secs(28);

    #[test]
    fn default_untouched_when_budget_is_ample() {
        assert_eq!(
            clamp_to_budget(None, DEFAULT, Duration::from_secs(30)),
            DEFAULT
        );
    }

    #[test]
    fn request_honoured_inside_a_long_job() {
        assert_eq!(
            clamp_to_budget(
                Some(Duration::from_secs(120)),
                DEFAULT,
                Duration::from_secs(900)
            ),
            Duration::from_secs(120)
        );
    }

    #[test]
    fn request_clamped_to_remaining_budget() {
        assert_eq!(
            clamp_to_budget(
                Some(Duration::from_secs(60)),
                DEFAULT,
                Duration::from_secs(30)
            ),
            Duration::from_secs(30) - FETCH_DEADLINE_MARGIN
        );
    }

    #[test]
    fn default_clamped_late_in_the_budget() {
        assert_eq!(
            clamp_to_budget(None, DEFAULT, Duration::from_secs(10)),
            Duration::from_secs(9)
        );
    }

    #[test]
    fn exhausted_budget_floors_rather_than_zeroes() {
        assert_eq!(
            clamp_to_budget(None, DEFAULT, Duration::ZERO),
            FETCH_TIMEOUT_FLOOR
        );
        assert_eq!(
            clamp_to_budget(Some(Duration::from_millis(50)), DEFAULT, Duration::ZERO),
            Duration::from_millis(50)
        );
    }
}

#[cfg(test)]
mod batch_tests {
    use super::{duration_from_env, BatchResult};

    #[test]
    fn ok_flattens_data_into_top_level() {
        let v = serde_json::to_value(BatchResult::ok(serde_json::json!({
            "data_b64": "AAAA"
        })))
        .unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["data_b64"], "AAAA");
        assert!(v.get("error").is_none());
    }

    #[test]
    fn err_only_serializes_ok_and_error() {
        let v = serde_json::to_value(BatchResult::err("boom")).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"], "boom");
        let obj = v.as_object().unwrap();
        assert_eq!(obj.len(), 2, "no extra fields on err");
    }

    #[test]
    fn fetch_response_shape_round_trips() {
        let r = BatchResult::ok(serde_json::json!({
            "status": 200,
            "body": "hi",
            "body_encoding": "utf8",
            "headers": { "content-type": "text/plain" },
        }));
        let v = serde_json::to_value(r).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["status"], 200);
        assert_eq!(v["body"], "hi");
        assert_eq!(v["headers"]["content-type"], "text/plain");
    }

    #[test]
    fn outbound_timeouts_default_and_override() {
        assert_eq!(
            duration_from_env("HOST_TEST_TIMEOUT_ABSENT", 28),
            std::time::Duration::from_secs(28)
        );
        unsafe { std::env::set_var("HOST_TEST_TIMEOUT_SET", "45") };
        assert_eq!(
            duration_from_env("HOST_TEST_TIMEOUT_SET", 28),
            std::time::Duration::from_secs(45)
        );

        unsafe { std::env::set_var("HOST_TEST_TIMEOUT_JUNK", "soon") };
        assert_eq!(
            duration_from_env("HOST_TEST_TIMEOUT_JUNK", 28),
            std::time::Duration::from_secs(28)
        );
        unsafe { std::env::set_var("HOST_TEST_TIMEOUT_ZERO", "0") };
        assert_eq!(
            duration_from_env("HOST_TEST_TIMEOUT_ZERO", 28),
            std::time::Duration::from_secs(28)
        );

        unsafe {
            std::env::remove_var("HOST_TEST_TIMEOUT_SET");
            std::env::remove_var("HOST_TEST_TIMEOUT_JUNK");
            std::env::remove_var("HOST_TEST_TIMEOUT_ZERO");
        }
    }
}

#[cfg(test)]
mod guest_read_tests {
    use super::{checked_len, copy_into_guest, guest_range, GuestReadError};

    #[test]
    fn host_responses_are_copied_in_full_however_large() {
        const MIB: usize = 1024 * 1024;
        let response = vec![7u8; 65 * MIB];
        let mut memory = vec![0u8; 66 * MIB];
        assert_eq!(
            copy_into_guest(&mut memory, 4096, 65 * MIB, &response),
            Ok(65 * MIB)
        );
        assert_eq!(&memory[4096..4096 + 65 * MIB], &response[..]);
        assert_eq!(
            copy_into_guest(&mut memory, 0, 16, &response),
            Ok(16),
            "a smaller buffer takes a prefix"
        );
        assert_eq!(
            copy_into_guest(&mut memory, 66 * MIB as i32 - 8, 16, &response),
            Err(GuestReadError::OutOfBounds)
        );
    }

    #[test]
    fn negative_and_oversized_lengths_are_invalid() {
        assert_eq!(checked_len(-1, 1024), Err(GuestReadError::InvalidLength));
        assert_eq!(
            checked_len(i32::MIN, 1024),
            Err(GuestReadError::InvalidLength)
        );
        assert_eq!(
            checked_len(i32::MAX, 1024),
            Err(GuestReadError::InvalidLength)
        );
        assert_eq!(checked_len(1025, 1024), Err(GuestReadError::InvalidLength));
        assert_eq!(checked_len(0, 1024), Ok(0));
        assert_eq!(checked_len(1024, 1024), Ok(1024));
        assert_eq!(GuestReadError::InvalidLength.code(), -1);
    }

    #[test]
    fn ranges_stay_inside_guest_memory() {
        assert_eq!(guest_range(0, 4, 8), Ok(0..4));
        assert_eq!(guest_range(4, 4, 8), Ok(4..8));
        assert_eq!(guest_range(5, 4, 8), Err(GuestReadError::OutOfBounds));
        assert_eq!(guest_range(-1, 1, 8), Err(GuestReadError::OutOfBounds));
        assert_eq!(
            guest_range(i32::MAX, usize::MAX, 8),
            Err(GuestReadError::OutOfBounds)
        );
    }
}

#[cfg(test)]
mod host_features_tests {
    use super::*;

    #[test]
    fn every_registered_host_fn_is_advertised_by_a_feature() {
        let engine = super::super::WasmEngine::new(
            WasmLimits::default(),
            crate::shutdown::Shutdown::new(std::time::Duration::from_secs(1)),
        )
        .expect("engine");
        let advertised: HashSet<&str> = HOST_FEATURES
            .iter()
            .flat_map(|(_, fns)| fns.iter().copied())
            .collect();
        let unadvertised: Vec<&str> = engine
            .host_fn_names()
            .iter()
            .copied()
            .filter(|f| !advertised.contains(f))
            .collect();
        assert!(
            unadvertised.is_empty(),
            "host fns outside HOST_FEATURES: {unadvertised:?}"
        );
        let unknown: Vec<&str> = advertised
            .iter()
            .copied()
            .filter(|f| !engine.host_fn_names().contains(f))
            .collect();
        assert!(
            unknown.is_empty(),
            "HOST_FEATURES names unregistered fns: {unknown:?}"
        );
        let supported = supported_features(engine.host_fn_names());
        assert_eq!(supported.len(), HOST_FEATURES.len());
        assert!(supported.contains(&"db_execute_all"));
        assert!(supported_features(&HashSet::new()).is_empty());
    }
}
