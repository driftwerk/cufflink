use super::Target;
use crate::auth::service_identity::{self, Claims, Expected, IdentityError};
use crate::errors::{PlatformError, PlatformResult};
use crate::registry::RegisteredService;
use crate::AppState;
use axum::http::HeaderMap;
use cufflink_types::RoutePosture;
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceCaller {
    pub service: String,
    pub on_behalf_of: Option<String>,
    pub depth: u8,
    pub remaining: Duration,
}

impl ServiceCaller {
    fn of(claims: Claims, now: i64) -> Self {
        let remaining = claims.remaining_budget(now);
        Self {
            service: claims.caller_service,
            on_behalf_of: claims.on_behalf_of,
            depth: claims.depth,
            remaining,
        }
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({
            "service": self.service,
            "on_behalf_of": self.on_behalf_of,
            "depth": self.depth,
        })
    }
}

pub(crate) fn identity(
    state: &AppState,
    service: &RegisteredService,
    headers: &HeaderMap,
    target: &Target<'_>,
    now: i64,
) -> Option<Result<Claims, IdentityError>> {
    let expected = Expected {
        tenant_id: service.tenant_id,
        target_service: &service.name,
        method: target.method.as_str(),
        path: target.path,
    };
    headers.get(service_identity::HEADER).map(|token| {
        token
            .to_str()
            .map_err(|_| IdentityError::Format)
            .and_then(|token| {
                service_identity::verify(&state.service_identity, token, &expected, now)
            })
    })
}

pub(crate) fn continued(
    service: &RegisteredService,
    target: &Target<'_>,
    identity: Option<Result<Claims, IdentityError>>,
    now: i64,
) -> PlatformResult<Option<ServiceCaller>> {
    match identity {
        None => Ok(None),
        Some(Err(error)) => {
            metrics::counter!("cufflink_service_identity_rejected_total", "reason" => error.reason())
                .increment(1);
            tracing::debug!(
                service = %service.name,
                path = %target.path,
                reason = error.reason(),
                "Ignored an invalid service identity"
            );
            Ok(None)
        }
        Some(Ok(claims)) => ServiceCaller::of(claims, now)
            .within_budget()
            .map(Some)
            .map_err(|refusal| refuse(service, target, refusal)),
    }
}

pub(crate) fn continued_call(
    state: &AppState,
    service: &RegisteredService,
    headers: &HeaderMap,
    target: &Target<'_>,
) -> PlatformResult<Option<ServiceCaller>> {
    let now = chrono::Utc::now().timestamp();
    continued(
        service,
        target,
        identity(state, service, headers, target, now),
        now,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Refusal {
    Missing,
    Invalid(IdentityError),
    NotListed(String),
    BudgetSpent,
}

impl ServiceCaller {
    pub(crate) fn bound(call: Option<&Self>, budget: Duration) -> Duration {
        call.map_or(budget, |call| budget.min(call.remaining))
    }

    fn within_budget(self) -> Result<Self, Refusal> {
        if self.remaining.is_zero() {
            Err(Refusal::BudgetSpent)
        } else {
            Ok(self)
        }
    }
}

fn decide(
    callers: &[String],
    identity: Option<Result<Claims, IdentityError>>,
    now: i64,
) -> Result<ServiceCaller, Refusal> {
    let claims = identity
        .ok_or(Refusal::Missing)?
        .map_err(Refusal::Invalid)?;
    if RoutePosture::admits_caller(callers, &claims.caller_service) {
        ServiceCaller::of(claims, now).within_budget()
    } else {
        Err(Refusal::NotListed(claims.caller_service))
    }
}

pub(crate) fn admit(
    service: &RegisteredService,
    target: &Target<'_>,
    callers: &[String],
    identity: Option<Result<Claims, IdentityError>>,
    now: i64,
) -> PlatformResult<ServiceCaller> {
    decide(callers, identity, now).map_err(|refusal| refuse(service, target, refusal))
}

fn refuse(service: &RegisteredService, target: &Target<'_>, refusal: Refusal) -> PlatformError {
    let reason = match &refusal {
        Refusal::Missing => "missing",
        Refusal::Invalid(error) => error.reason(),
        Refusal::NotListed(_) => "caller",
        Refusal::BudgetSpent => "budget",
    };
    metrics::counter!("cufflink_service_identity_rejected_total", "reason" => reason).increment(1);
    tracing::warn!(
        service = %service.name,
        path = %target.path,
        reason,
        "Refused a service call"
    );
    match refusal {
        Refusal::NotListed(caller) => {
            PlatformError::Forbidden(format!("Service '{caller}' may not call this route"))
        }
        Refusal::BudgetSpent => {
            PlatformError::GatewayTimeout("The calling service's time budget is spent".into())
        }
        Refusal::Missing | Refusal::Invalid(_) => PlatformError::Unauthorized(
            "This route accepts only calls from the tenant's services".into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    const NOW: i64 = 1_800_000_000;

    fn claims(caller: &str) -> Claims {
        Claims {
            tenant_id: Uuid::nil(),
            caller_service: caller.into(),
            caller_service_id: Uuid::nil(),
            caller_version: 1,
            target_service: "assets".into(),
            method: "POST".into(),
            path: "/_fn/reprice".into(),
            issued_at: 0,
            expires_at: NOW + 12,
            jti: Uuid::nil(),
            depth: 2,
            on_behalf_of: Some("kc-user-1".into()),
        }
    }

    #[test]
    fn listed_callers_are_admitted_with_their_depth_and_attribution() {
        let callers = vec!["pricing".to_string()];
        let admitted = decide(&callers, Some(Ok(claims("pricing"))), NOW).expect("admitted");
        assert_eq!(
            admitted.to_json(),
            json!({"service": "pricing", "on_behalf_of": "kc-user-1", "depth": 2})
        );
        assert_eq!(admitted.remaining, Duration::from_secs(12));
        assert!(decide(&["*".to_string()], Some(Ok(claims("anyone"))), NOW).is_ok());
    }

    #[test]
    fn missing_invalid_and_unlisted_identities_are_refused() {
        let callers = vec!["pricing".to_string()];
        assert_eq!(decide(&callers, None, NOW), Err(Refusal::Missing));
        assert_eq!(
            decide(&callers, Some(Err(IdentityError::Path)), NOW),
            Err(Refusal::Invalid(IdentityError::Path))
        );
        assert_eq!(
            decide(&callers, Some(Ok(claims("assets"))), NOW),
            Err(Refusal::NotListed("assets".into()))
        );
    }

    fn service() -> RegisteredService {
        RegisteredService {
            service_id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            name: "assets".into(),
            manifest: serde_json::from_value(json!({"name": "assets", "tables": []}))
                .expect("manifest"),
            version: 1,
            wasm_hash: None,
            commit_hash: None,
        }
    }

    #[test]
    fn other_routes_continue_a_verified_call_and_ignore_an_invalid_one() {
        let target = Target {
            method: &axum::http::Method::POST,
            path: "/_fn/reprice",
        };
        let continued_call = continued(&service(), &target, Some(Ok(claims("pricing"))), NOW)
            .expect("admitted")
            .expect("a call");
        assert_eq!(
            (continued_call.depth, continued_call.remaining),
            (2, Duration::from_secs(12))
        );
        for identity in [Some(Err(IdentityError::Signature)), None] {
            assert_eq!(
                continued(&service(), &target, identity, NOW).expect("admitted"),
                None
            );
        }
    }

    #[test]
    fn a_call_whose_budget_is_spent_is_refused_before_it_runs() {
        let target = Target {
            method: &axum::http::Method::POST,
            path: "/_fn/reprice",
        };
        let spent = Claims {
            expires_at: NOW,
            ..claims("pricing")
        };
        let refused =
            continued(&service(), &target, Some(Ok(spent.clone())), NOW).expect_err("refused");
        assert!(
            matches!(refused, PlatformError::GatewayTimeout(_)),
            "{refused}"
        );
        assert_eq!(
            decide(&["pricing".to_string()], Some(Ok(spent)), NOW),
            Err(Refusal::BudgetSpent)
        );
    }

    #[test]
    fn budgets_are_cut_to_what_the_caller_has_left() {
        let call = decide(&["*".to_string()], Some(Ok(claims("pricing"))), NOW).expect("call");
        assert_eq!(
            ServiceCaller::bound(Some(&call), Duration::from_secs(30)),
            Duration::from_secs(12)
        );
        assert_eq!(
            ServiceCaller::bound(Some(&call), Duration::from_secs(5)),
            Duration::from_secs(5)
        );
        assert_eq!(
            ServiceCaller::bound(None, Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }
}
