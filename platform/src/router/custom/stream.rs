use crate::errors::{PlatformError, PlatformResult};
use crate::registry::RegisteredService;
use crate::router::guest_response::{Bodies, S3Spec};
use crate::router::sse_bridge::{Bridge, Reducer, SseSpec};
use crate::tenant_catalog::TenantEntry;
use crate::wasm_runtime::storage_scope;
use crate::AppState;
use axum::response::Response;
use cufflink_storage::ObjectReader;
use std::time::Duration;

const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct GuestBodies<'a> {
    pub state: &'a AppState,
    pub service: &'a RegisteredService,
    pub tenant: &'a TenantEntry,
    pub sse: Option<(Bridge, Option<Reducer>)>,
}

fn failed(reason: &str) -> PlatformError {
    PlatformError::Internal(format!("WASM execution error: {reason}"))
}

impl Bodies for GuestBodies<'_> {
    fn sse(&mut self, spec: SseSpec) -> PlatformResult<Response> {
        self.sse
            .take()
            .map(|(bridge, reducer)| bridge.stream(spec, reducer, self.state))
            .ok_or_else(|| failed("an event stream from a route that declares no sse"))
    }

    async fn open(&self, spec: &S3Spec) -> PlatformResult<ObjectReader> {
        let storage = self
            .state
            .storage
            .as_ref()
            .ok_or_else(|| failed("object storage is not configured"))?;
        let guest = storage_scope::Guest {
            tenant_id: self.service.tenant_id,
            tenant_slug: &self.service.tenant_slug,
            service_name: &self.service.name,
            policies: &self.tenant.policies,
        };
        storage_scope::authorize_guest(self.state, &guest, &spec.bucket, &[&spec.key])
            .map_err(|_| failed("storage access denied"))?;
        tokio::time::timeout(OPEN_TIMEOUT, storage.open_read(&spec.bucket, &spec.key))
            .await
            .map_err(|_| failed("storage read timed out"))?
            .map_err(|e| {
                tracing::error!(
                    service = %self.service.name,
                    bucket = %spec.bucket,
                    error = %e,
                    "Opening an object to stream failed"
                );
                failed("storage read failed")
            })?
            .ok_or_else(|| PlatformError::NotFound("Not found".into()))
    }
}
