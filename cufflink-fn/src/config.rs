//! Read service configuration values set via `cufflink config set`.
//!
//! Config values are stored in the platform's `service_configs` table,
//! scoped to your service. Use `cufflink config set KEY VALUE [--secret]`
//! to set values via the CLI.

#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};

/// Get a config value by key. Returns `None` if the key doesn't exist.
///
/// ```rust,ignore
/// let api_key = config::get("ANTHROPIC_API_KEY");
/// if let Some(key) = api_key {
///     log::info(&format!("API key loaded ({} chars)", key.len()));
/// }
/// ```
pub fn get(key: &str) -> Option<String> {
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = key.as_bytes();
        let result = unsafe { ffi::get_config(bytes.as_ptr() as i32, bytes.len() as i32) };
        if result < 0 {
            return None;
        }
        host_buffer::read_host_string()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::config().and_then(|host| host.get(key))
    }
}
