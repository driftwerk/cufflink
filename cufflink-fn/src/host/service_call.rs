//! Calls to another service of the tenant.

use serde_json::Value;

/// What answers `http::call_service`.
pub trait ServiceCallHost {
    /// One `service_call`: the request envelope in, the response envelope
    /// out.
    #[cfg_attr(not(feature = "service-call"), allow(dead_code))]
    fn call(&self, request: &Value) -> Value;
}
