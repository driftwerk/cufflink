use super::*;

#[test]
fn postures_unchanged_under_default_binding() {
    let t = tenants();
    let m = sample_manifest();
    let default = entry(&t.default, &[]);
    let grants = staff_grants(&t);
    let customer = || Some(jwt(&default, "orchard", "customer", "portal"));
    let staff = || Some(jwt(&default, "orchard", "staff", "backoffice"));
    let call = |handler, principal| {
        class(fn_route(
            posture(&m, handler),
            principal,
            None,
            &default,
            &grants,
        ))
    };
    assert_eq!(call("stripe_webhook", None), Class::Ok);
    assert_eq!(call("me", None), Class::Unauthorized);
    assert_eq!(call("list_items", None), Class::Unauthorized);
    assert_eq!(call("my_orders", None), Class::Unauthorized);
    assert_eq!(call("me", customer()), Class::Ok);
    assert_eq!(call("my_orders", customer()), Class::Ok);
    assert_eq!(call("list_items", customer()), Class::Forbidden);
    assert_eq!(call("list_items", staff()), Class::Ok);
}

#[test]
fn forwarded_jwt_hop_without_header_works_on_preview() {
    let t = tenants();
    let m = sample_manifest();
    for binding in [PolicyMode::Warn, PolicyMode::Enforce] {
        let preview = entry(&t.preview, &modes(&t.preview, binding, PolicyMode::Warn));
        let customer = jwt(&entry(&t.default, &[]), "orchard", "customer", "portal");
        let bound = fn_route(
            posture(&m, "me"),
            Some(customer),
            None,
            &preview,
            &staff_grants(&t),
        )
        .expect("admitted")
        .expect("principal");
        assert_eq!(bound.tenant_id, t.preview.id, "{binding}");
    }
}

fn credentials_seen(binding: PolicyMode, forward_auth: bool) -> (Vec<String>, String) {
    let t = tenants();
    let preview = entry(&t.preview, &modes(&t.preview, binding, PolicyMode::Warn));
    let route = cufflink_types::CustomRouteDefinition {
        forward_auth,
        ..sample_manifest().http_route("me").expect("me").clone()
    };
    let headers: axum::http::HeaderMap = [
        ("authorization", "Bearer eyJ"),
        ("cookie", "session=1"),
        ("proxy-authorization", "Basic dTpw"),
        ("x-request-id", "r-1"),
    ]
    .into_iter()
    .map(|(name, value)| (name.parse().expect("name"), value.parse().expect("value")))
    .collect();
    let recorder = cufflink_telemetry::metrics_builder()
        .expect("builder")
        .build_recorder();
    let handle = recorder.handle();
    let seen = metrics::with_local_recorder(&recorder, || {
        crate::router::custom::guest_headers(&preview, "stock-service", &route, &headers)
    });
    (seen.keys().cloned().collect(), handle.render())
}

const ALL_HEADERS: [&str; 4] = [
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-request-id",
];

#[test]
fn credentials_reach_a_route_without_forward_auth_under_warn_recorded() {
    let (seen, rendered) = credentials_seen(PolicyMode::Warn, false);
    assert_eq!(seen, ALL_HEADERS);
    assert!(
        rendered.contains(
            r#"cufflink_policy_violation_total{key="tenant_binding",tenant="preview-pr-7",outcome="warned",reason="auth_header_forwarded"} 1"#
        ),
        "{rendered}"
    );
    assert!(
        !rendered.contains("cufflink_route_credentials_stripped_total"),
        "{rendered}"
    );
}

#[test]
fn credentials_are_stripped_quietly_from_a_route_without_forward_auth_under_enforce() {
    let (seen, rendered) = credentials_seen(PolicyMode::Enforce, false);
    assert_eq!(seen, ["x-request-id"]);
    assert!(
        !rendered.contains("cufflink_policy_violation_total"),
        "{rendered}"
    );
    assert!(
        rendered.contains(
            r#"cufflink_route_credentials_stripped_total{tenant="preview-pr-7",service="stock-service"} 1"#
        ),
        "{rendered}"
    );
}

#[test]
fn credentials_follow_forward_auth_and_binding_off_unrecorded() {
    for (binding, forward_auth) in [
        (PolicyMode::Enforce, true),
        (PolicyMode::Warn, true),
        (PolicyMode::Off, false),
        (PolicyMode::Off, true),
    ] {
        let (seen, rendered) = credentials_seen(binding, forward_auth);
        assert_eq!(seen, ALL_HEADERS, "{binding} {forward_auth}");
        assert!(
            rendered.trim().is_empty(),
            "{binding} {forward_auth}: {rendered}"
        );
    }
}

#[test]
fn a_v2_envelope_from_a_hook_is_read_like_any_other_json() {
    use crate::router::crud::interpret_hook_response;
    let accepted = interpret_hook_response(
        serde_json::json!({"__v": 2, "__status": 201, "__headers": {"x-a": "1"}, "__body": {"id": 1}}),
    );
    assert!(matches!(accepted, Ok(body) if body == serde_json::json!({"id": 1})));
    let rejected = interpret_hook_response(
        serde_json::json!({"__v": 2, "__status": 200, "__body": {"error": "denied"}}),
    );
    assert!(
        matches!(&rejected, Err(crate::errors::PlatformError::BadRequest(m)) if m == "Hook error: denied"),
        "{rejected:?}"
    );
}
