//! Long-running jobs.

use crate::jobs::JobOptions;
use serde_json::Value;

/// What answers `jobs::enqueue` and `jobs::enqueue_with`: the id of the
/// job, or `None` for one that is refused.
pub trait JobsHost {
    fn enqueue(&self, handler: &str, payload: &Value, options: JobOptions) -> Option<String>;
}
