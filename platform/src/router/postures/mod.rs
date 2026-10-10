mod api_key;
mod internal;
mod scoped;

use crate::auth::binding::bind_principal;
use crate::auth::partner_keys::PartnerPrincipal;
use crate::auth::permissions::resolve_permissions_cached;
use crate::auth::scope::EffectiveScope;
use crate::auth::{authenticate, AuthOutcome, AuthUser};
use crate::errors::{PlatformError, PlatformResult};
use crate::registry::RegisteredService;
use crate::router::authz::permission_decision;
use crate::tenant_catalog::TenantEntry;
use crate::AppState;
use axum::http::{HeaderMap, Method};
use cufflink_types::RoutePosture;
use serde_json::Value;
use std::sync::Arc;

pub(crate) use api_key::HEADER as API_KEY_HEADER;
pub(crate) use internal::{continued_call, ServiceCaller};

pub(crate) struct Target<'a> {
    pub method: &'a Method,
    pub path: &'a str,
}

pub(crate) enum Principal {
    User(Option<Box<AuthUser>>),
    Service,
    Partner(PartnerPrincipal),
    Scoped(Box<AuthUser>, EffectiveScope),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NamedPrincipal {
    ApiKey(Value),
    Scope(Value),
}

impl Principal {
    pub(crate) fn into_parts(self) -> (Option<AuthUser>, Option<NamedPrincipal>) {
        match self {
            Principal::User(user) => (user.map(|user| *user), None),
            Principal::Service => (None, None),
            Principal::Partner(partner) => (None, Some(NamedPrincipal::ApiKey(partner.to_json()))),
            Principal::Scoped(user, scope) => {
                (Some(*user), Some(NamedPrincipal::Scope(scope.to_json())))
            }
        }
    }
}

pub(crate) enum PostureGate {
    Legacy,
    Public,
    RequirePrincipal,
    RequirePermission {
        area: String,
        operation: String,
    },
    ServiceIdentity {
        callers: Vec<String>,
    },
    PartnerKey {
        scope: String,
    },
    Scoped {
        area: String,
        operation: String,
        owner_field: Option<String>,
    },
}

pub(crate) enum Admission {
    Admit(Option<AuthUser>),
    NeedsGrant {
        user: AuthUser,
        area: String,
        operation: String,
    },
}

impl PostureGate {
    pub(crate) fn of(posture: Option<&RoutePosture>) -> Self {
        match posture {
            None => PostureGate::Legacy,
            Some(RoutePosture::Public { .. }) => PostureGate::Public,
            Some(RoutePosture::Authenticated) | Some(RoutePosture::Owner { .. }) => {
                PostureGate::RequirePrincipal
            }
            Some(RoutePosture::Permission { area, operation }) => PostureGate::RequirePermission {
                area: area.clone(),
                operation: operation.clone(),
            },
            Some(RoutePosture::Internal { callers }) => PostureGate::ServiceIdentity {
                callers: callers.clone(),
            },
            Some(RoutePosture::ApiKey { scope }) => PostureGate::PartnerKey {
                scope: scope.clone(),
            },
            Some(RoutePosture::Scoped {
                area,
                operation,
                owner_field,
            }) => PostureGate::Scoped {
                area: area.clone(),
                operation: operation.clone(),
                owner_field: owner_field.clone(),
            },
        }
    }

    fn tolerates_invalid_credential(&self) -> bool {
        matches!(self, PostureGate::Legacy | PostureGate::Public)
    }

    pub(crate) fn admit(
        &self,
        principal: PlatformResult<Option<AuthUser>>,
    ) -> PlatformResult<Option<AuthUser>> {
        match principal {
            Err(e) if self.tolerates_invalid_credential() => {
                tracing::debug!(error = %e, "Credential refused on a lenient route; continuing anonymously");
                Ok(None)
            }
            other => other,
        }
    }

    pub(crate) fn admission(self, principal: Option<AuthUser>) -> PlatformResult<Admission> {
        let required = |principal: Option<AuthUser>| {
            principal.ok_or_else(|| {
                PlatformError::Unauthorized("Authentication required for this route".into())
            })
        };
        match self {
            PostureGate::Legacy | PostureGate::Public => Ok(Admission::Admit(principal)),
            PostureGate::ServiceIdentity { .. } => Err(PlatformError::Unauthorized(
                "This route accepts only calls from the tenant's services".into(),
            )),
            PostureGate::PartnerKey { .. } => {
                Err(PlatformError::Unauthorized("Invalid API key".into()))
            }
            PostureGate::Scoped {
                area, operation, ..
            } => PostureGate::RequirePermission { area, operation }.admission(principal),
            PostureGate::RequirePrincipal => required(principal).map(|u| Admission::Admit(Some(u))),
            PostureGate::RequirePermission { area, operation } => required(principal).map(|user| {
                if user.is_service_account {
                    Admission::Admit(Some(user))
                } else {
                    Admission::NeedsGrant {
                        user,
                        area,
                        operation,
                    }
                }
            }),
        }
    }
}

pub(crate) struct Authorized {
    pub principal: Principal,
    pub call: Option<ServiceCaller>,
}

pub(crate) async fn authorize_route(
    state: &Arc<AppState>,
    service: &RegisteredService,
    tenant: &TenantEntry,
    headers: &HeaderMap,
    posture: Option<&RoutePosture>,
    target: &Target<'_>,
) -> PlatformResult<Authorized> {
    let now = chrono::Utc::now().timestamp();
    let identity = internal::identity(state, service, headers, target, now);
    match PostureGate::of(posture) {
        PostureGate::ServiceIdentity { callers } => {
            internal::admit(service, target, &callers, identity, now).map(|caller| Authorized {
                principal: Principal::Service,
                call: Some(caller),
            })
        }
        gate => {
            let call = internal::continued(service, target, identity, now)?;
            admit_principal(state, service, tenant, headers, gate)
                .await
                .map(|principal| Authorized { principal, call })
        }
    }
}

async fn admit_principal(
    state: &Arc<AppState>,
    service: &RegisteredService,
    tenant: &TenantEntry,
    headers: &HeaderMap,
    gate: PostureGate,
) -> PlatformResult<Principal> {
    match gate {
        PostureGate::ServiceIdentity { .. } => Err(PlatformError::Unauthorized(
            "This route accepts only calls from the tenant's services".into(),
        )),
        PostureGate::PartnerKey { scope } => api_key::admit(state, service, headers, &scope)
            .await
            .map(Principal::Partner),
        PostureGate::Scoped {
            area,
            operation,
            owner_field,
        } => scoped::admit(
            state,
            service,
            tenant,
            headers,
            &area,
            &operation,
            owner_field.as_deref(),
        )
        .await
        .map(|(user, scope)| Principal::Scoped(Box::new(user), scope)),
        gate => authorize_user(state, service, tenant, headers, gate)
            .await
            .map(|user| Principal::User(user.map(Box::new))),
    }
}

async fn authorize_user(
    state: &Arc<AppState>,
    service: &RegisteredService,
    tenant: &TenantEntry,
    headers: &HeaderMap,
    gate: PostureGate,
) -> PlatformResult<Option<AuthUser>> {
    let bound = match authenticate(headers, state).await {
        AuthOutcome::Absent => Ok(None),
        AuthOutcome::Present(result) => {
            result.and_then(|user| bind_principal(state, Some(*user), tenant, headers))
        }
    };
    let principal = gate.admit(bound)?;

    if matches!(gate, PostureGate::Legacy) {
        metrics::counter!(
            "cufflink_route_posture_undeclared_total",
            "service" => service.name.clone(),
        )
        .increment(1);
        tracing::debug!(
            service = %service.name,
            "Custom route has no declared posture — running in legacy (unenforced) mode."
        );
    }

    match gate.admission(principal)? {
        Admission::Admit(principal) => Ok(principal),
        Admission::NeedsGrant {
            user,
            area,
            operation,
        } => {
            let resolved =
                resolve_permissions_cached(state, service.tenant_id, &user.subject).await?;
            permission_decision(&area, &operation, &resolved).map(|()| Some(user))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_maps_each_posture() {
        assert!(matches!(PostureGate::of(None), PostureGate::Legacy));
        assert!(matches!(
            PostureGate::of(Some(&RoutePosture::Public {
                reason: "webhook".into()
            })),
            PostureGate::Public
        ));
        assert!(matches!(
            PostureGate::of(Some(&RoutePosture::Authenticated)),
            PostureGate::RequirePrincipal
        ));
        assert!(matches!(
            PostureGate::of(Some(&RoutePosture::Owner {
                field: "customer_id".into()
            })),
            PostureGate::RequirePrincipal
        ));
        match PostureGate::of(Some(&RoutePosture::Permission {
            area: "items".into(),
            operation: "edit".into(),
        })) {
            PostureGate::RequirePermission { area, operation } => {
                assert_eq!((area.as_str(), operation.as_str()), ("items", "edit"));
            }
            _ => panic!("expected RequirePermission"),
        }
    }

    #[test]
    fn lenient_gate_downgrades_rejected_binding() {
        let refused = || {
            Err(PlatformError::Forbidden(
                "tenant_binding: realm_mismatch".into(),
            ))
        };
        assert!(matches!(PostureGate::Public.admit(refused()), Ok(None)));
        assert!(matches!(PostureGate::Legacy.admit(refused()), Ok(None)));
        assert!(matches!(
            PostureGate::RequirePrincipal.admit(refused()),
            Err(PlatformError::Forbidden(_))
        ));
        assert!(matches!(
            PostureGate::RequirePrincipal.admit(Ok(None)),
            Ok(None)
        ));
    }

    #[test]
    fn only_lenient_gates_tolerate_a_bad_credential() {
        assert!(PostureGate::Legacy.tolerates_invalid_credential());
        assert!(PostureGate::Public.tolerates_invalid_credential());
        assert!(!PostureGate::RequirePrincipal.tolerates_invalid_credential());
        assert!(!PostureGate::RequirePermission {
            area: "a".into(),
            operation: "edit".into(),
        }
        .tolerates_invalid_credential());
        assert!(!PostureGate::ServiceIdentity { callers: vec![] }.tolerates_invalid_credential());
        assert!(!PostureGate::PartnerKey {
            scope: "books:ingest".into()
        }
        .tolerates_invalid_credential());
    }

    #[test]
    fn a_scoped_gate_needs_a_principal_and_falls_back_to_its_permission() {
        let gate = || {
            PostureGate::of(Some(&RoutePosture::Scoped {
                area: "assets".into(),
                operation: "view".into(),
                owner_field: Some("customer_id".into()),
            }))
        };
        assert!(!gate().tolerates_invalid_credential());
        assert!(matches!(
            gate().admission(None),
            Err(PlatformError::Unauthorized(_))
        ));
        let tenant = crate::tenant_catalog::tests::tenant("acme", None);
        let user = crate::auth::binding::tests::jwt_user(&tenant, "acme", "portal", &[]);
        assert!(matches!(
            gate().admission(Some(user)),
            Ok(Admission::NeedsGrant { area, operation, .. }) if area == "assets" && operation == "view"
        ));
    }

    #[test]
    fn a_partner_key_gate_admits_no_end_user() {
        let gate = PostureGate::of(Some(&RoutePosture::ApiKey {
            scope: "books:ingest".into(),
        }));
        assert!(matches!(&gate, PostureGate::PartnerKey { scope } if scope == "books:ingest"));
        assert!(matches!(
            gate.admission(None),
            Err(PlatformError::Unauthorized(_))
        ));
    }

    #[test]
    fn a_service_identity_gate_admits_no_end_user() {
        let internal = PostureGate::of(Some(&RoutePosture::Internal {
            callers: vec!["pricing".into()],
        }));
        assert!(matches!(
            &internal,
            PostureGate::ServiceIdentity { callers } if callers == &["pricing".to_string()]
        ));
        assert!(matches!(
            internal.admission(None),
            Err(PlatformError::Unauthorized(_))
        ));
    }
}
