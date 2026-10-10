//! What the platform answers for a route before its handler runs: the
//! posture the route declares, applied to the principal the request
//! carries, in the words of the service's error profile.

use crate::{Auth, Request, Response};
use cufflink_types::grants::parse;
use cufflink_types::RoutePosture;
use serde_json::json;

/// The response the platform answers in place of running the handler of
/// `request`, where its route's posture refuses the request's principal.
/// A request without a posture is a legacy route's, which admits anyone.
pub(super) fn refused(request: &Request, error_key: &str) -> Option<Response> {
    let posture = serde_json::from_value::<RoutePosture>(request.posture()?.clone()).ok()?;
    refusal(&posture, request)
        .map(|(status, message)| Response::status_json(status, &json!({ error_key: message })))
}

fn refusal(posture: &RoutePosture, request: &Request) -> Option<(u16, String)> {
    let unauthenticated = || (401, "Authentication required for this route".to_string());
    match posture {
        RoutePosture::Public { .. } => None,
        RoutePosture::Authenticated | RoutePosture::Owner { .. } => {
            request.auth().is_none().then(unauthenticated)
        }
        RoutePosture::Permission { area, operation }
        | RoutePosture::Scoped {
            area, operation, ..
        } => match request.auth() {
            None => Some(unauthenticated()),
            Some(auth) if auth.is_service_account || granted(auth, area, operation) => None,
            Some(_) => Some((403, format!("Missing permission: {area}:{operation}"))),
        },
        RoutePosture::Internal { callers } => match request.caller() {
            None => Some((
                401,
                "This route accepts only calls from the tenant's services".to_string(),
            )),
            Some(caller) if callers.iter().any(|c| c == "*" || *c == caller.service) => None,
            Some(caller) => Some((
                403,
                format!("Service '{}' may not call this route", caller.service),
            )),
        },
        RoutePosture::ApiKey { .. } => request
            .api_key()
            .is_none()
            .then(|| (401, "Invalid API key".to_string())),
    }
}

/// Whether any grant of the caller covers `{area}:{operation}`, at any
/// scope, as the platform decides it.
fn granted(auth: &Auth, area: &str, operation: &str) -> bool {
    auth.permissions
        .iter()
        .chain(&auth.scoped_permissions)
        .filter_map(|permission| parse(permission).ok())
        .any(|grant| grant.covers(area, operation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{request, AuthBuilder, RequestBuilder};
    use serde_json::Value;

    fn route(posture: Value) -> RequestBuilder {
        request().handler("h").posture(posture)
    }

    fn answer(request: RequestBuilder) -> Option<(u16, Value)> {
        refused(&request.build().expect("request"), "error_message")
            .map(|response| (response.status(), response.json_as().expect("json")))
    }

    #[test]
    fn a_permission_route_needs_a_caller_holding_the_permission() {
        let perm = || route(json!({"kind": "permission", "area": "loans", "operation": "delete"}));
        assert_eq!(
            answer(perm()),
            Some((
                401,
                json!({"error_message": "Authentication required for this route"})
            ))
        );
        assert_eq!(
            answer(perm().auth(AuthBuilder::user("kc-1").permissions(&["loans:view"]))),
            Some((
                403,
                json!({"error_message": "Missing permission: loans:delete"})
            ))
        );
        for admitted in [
            AuthBuilder::user("kc-1").permissions(&["loans:delete"]),
            AuthBuilder::user("kc-1").permissions(&["loans:*"]),
            AuthBuilder::user("kc-1").scoped_permissions(&["loans:delete@all"]),
            AuthBuilder::service_account("svc"),
        ] {
            assert_eq!(answer(perm().auth(admitted.clone())), None, "{admitted:?}");
        }
        assert_eq!(
            answer(
                route(json!({"kind": "scoped", "area": "loans", "operation": "view"}))
                    .auth(AuthBuilder::user("kc-1").permissions(&["loans:delete"]))
            ),
            Some((
                403,
                json!({"error_message": "Missing permission: loans:view"})
            ))
        );
    }

    #[test]
    fn every_other_posture_admits_what_the_platform_admits() {
        let anonymous = |posture: Value| answer(route(posture)).map(|(status, _)| status);
        assert_eq!(
            anonymous(json!({"kind": "public", "reason": "webhook"})),
            None
        );
        assert_eq!(anonymous(json!({"kind": "authenticated"})), Some(401));
        assert_eq!(
            anonymous(json!({"kind": "owner", "field": "customer_id"})),
            Some(401)
        );
        assert_eq!(
            anonymous(json!({"kind": "api_key", "scope": "books"})),
            Some(401)
        );
        assert_eq!(
            answer(route(json!({"kind": "api_key", "scope": "books"})).api_key(
                "k",
                "acme",
                &["books"]
            )),
            None
        );
        let internal = || route(json!({"kind": "internal", "callers": ["catalog-service"]}));
        assert_eq!(answer(internal()).map(|(status, _)| status), Some(401));
        assert_eq!(answer(internal().caller("catalog-service", None, 1)), None);
        assert_eq!(
            answer(internal().caller("receipt-service", None, 1)),
            Some((
                403,
                json!({"error_message": "Service 'receipt-service' may not call this route"})
            ))
        );
        assert_eq!(
            answer(request().handler("legacy")),
            None,
            "a legacy route admits anyone"
        );
    }
}
