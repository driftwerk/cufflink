//! Outbound HTTP.

use cufflink_types::host::http::{FetchRequestV2, FetchResponseV2};

/// What answers `http::*`. The legacy functions reach it with redirects
/// followed and read any failure as `None`.
pub trait HttpHost {
    fn fetch(&self, request: &FetchRequestV2) -> FetchResponseV2;
}
