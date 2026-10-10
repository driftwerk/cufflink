//! Enqueue long-running jobs for the current service, running as the caller.
//! The platform fans them out across the worker fleet, so a coordinator
//! handler can spawn many chunk jobs that run in parallel.

#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};
use serde_json::Value;

/// Per-job overrides. `0` for either field means "use the platform
/// default" (timeout 600s, 3 attempts).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JobOptions {
    pub timeout_secs: u32,
    pub max_attempts: u32,
}

/// Enqueue `handler` (a `job(...)`-annotated route on the current service)
/// with `payload` as its body, using platform defaults. Returns the new
/// job id, or `None` if enqueue failed. Outside the platform it is `None`
/// unless a test host answers it.
///
/// ```rust,ignore
/// let id = jobs::enqueue("handle_backfill_chunk", &json!({ "chunk_id": id }));
/// ```
pub fn enqueue(handler: &str, payload: &Value) -> Option<String> {
    enqueue_with(handler, payload, JobOptions::default())
}

/// [`enqueue`] with explicit timeout / max-attempts overrides.
pub fn enqueue_with(handler: &str, payload: &Value, opts: JobOptions) -> Option<String> {
    #[cfg(target_arch = "wasm32")]
    {
        let handler_bytes = handler.as_bytes();
        let payload_str = payload.to_string();
        let payload_bytes = payload_str.as_bytes();
        let result = unsafe {
            ffi::jobs_enqueue(
                handler_bytes.as_ptr() as i32,
                handler_bytes.len() as i32,
                payload_bytes.as_ptr() as i32,
                payload_bytes.len() as i32,
                opts.timeout_secs as i32,
                opts.max_attempts as i32,
            )
        };
        if result != 0 {
            return None;
        }
        let id = host_buffer::read_host_string().unwrap_or_default();
        (!id.is_empty()).then_some(id)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::jobs().and_then(|host| host.enqueue(handler, payload, opts))
    }
}

#[cfg(test)]
mod tests {
    use crate::jobs;
    use serde_json::json;

    #[test]
    fn job_options_default_uses_platform_defaults() {
        let opts = jobs::JobOptions::default();
        assert_eq!((opts.timeout_secs, opts.max_attempts), (0, 0));
    }

    #[test]
    fn jobs_enqueue_is_inert_off_wasm() {
        assert!(jobs::enqueue("handle_chunk", &json!({ "chunk_id": "x" })).is_none());
    }
}
