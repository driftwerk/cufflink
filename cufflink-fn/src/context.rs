//! Runtime context provided by the platform: tenant identity, service identity,
//! etc. Use these from client helpers that are called from inside handlers but
//! don't take a `Request` (e.g. shared cache wrappers).

#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};

/// Tenant slug of the currently-executing service (e.g. `"default"`).
/// Returns an empty string if no platform context is available: outside
/// WASM, unless a test host is installed.
pub fn tenant() -> String {
    #[cfg(target_arch = "wasm32")]
    {
        if unsafe { ffi::context_tenant() } != 0 {
            return String::new();
        }
        host_buffer::read_host_string().unwrap_or_default()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::tenant().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use crate::context;

    #[test]
    fn context_tenant_returns_empty_outside_wasm() {
        assert_eq!(context::tenant(), "");
    }
}
