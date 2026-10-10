//! Extensions of the tenant.

use cufflink_types::host::ext::{CallRequest, CallResponse};

/// What answers `ext::call`.
pub trait ExtHost {
    #[cfg_attr(not(feature = "ext-call"), allow(dead_code))]
    fn call(&self, request: &CallRequest) -> CallResponse;
}
