//! NATS.

use cufflink_types::host::nats::{PublishRequest, PublishResponse};

/// What answers `nats::*`. The legacy functions read a failure as `false`
/// or `None`.
pub trait NatsHost {
    fn publish(&self, request: &PublishRequest) -> PublishResponse;
    /// The reply, or `None` when none came within `timeout_ms`.
    fn request(&self, subject: &str, payload: &[u8], timeout_ms: u32) -> Option<Vec<u8>>;
}
