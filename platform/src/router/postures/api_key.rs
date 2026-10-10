use crate::auth::partner_keys::{self, PartnerPrincipal};
use crate::errors::PlatformResult;
use crate::registry::RegisteredService;
use crate::AppState;
use axum::http::HeaderMap;
use std::sync::Arc;

pub(crate) const HEADER: &str = "x-api-key";

pub(crate) async fn admit(
    state: &Arc<AppState>,
    service: &RegisteredService,
    headers: &HeaderMap,
    scope: &str,
) -> PlatformResult<PartnerPrincipal> {
    let presented = headers.get(HEADER).and_then(|value| value.to_str().ok());
    partner_keys::authenticate(state, presented, service.tenant_id, scope).await
}
