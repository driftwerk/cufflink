use crate::deployer::manifest::ServiceManifest;
use crate::errors::PlatformError;
use crate::router::strict::{crud_shadow, RouteTable};
use cufflink_types::manifest::validate::{validate_audit_redact, validate_job_config};
use cufflink_types::route_path::RoutePattern;
use cufflink_types::{CustomRouteDefinition, ErrorProfile, RoutePosture, RoutingMode};

pub(super) fn validate(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    validate_route_postures(manifest, route_posture_enforced())?;
    if let Some(profile) = manifest
        .errors
        .as_ref()
        .filter(|profile| !ErrorProfile::is_valid_key(&profile.key))
    {
        return Err(PlatformError::Validation(format!(
            "Error profile key '{}' must match [a-z_][a-z0-9_]{{0,63}}",
            profile.key
        )));
    }
    cufflink_types::openapi::validate(manifest)?;
    manifest
        .custom_routes
        .iter()
        .find_map(|route| {
            route_limit_violation(route)
                .or_else(|| sse_violation(route, manifest.routing))
                .or_else(|| raw_bridge_violation(route, manifest))
                .or_else(|| job_violation(route))
        })
        .map_or(Ok(()), |reason| Err(PlatformError::Validation(reason)))?;
    match manifest.routing {
        RoutingMode::Legacy => Ok(()),
        RoutingMode::Strict => validate_strict_routes(manifest),
    }
}

fn declares_area(manifest: &ServiceManifest, area: &str) -> bool {
    manifest
        .authorization
        .iter()
        .flat_map(|authorization| &authorization.areas)
        .any(|declared| declared.name == area)
}

fn is_column_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn job_violation(route: &CustomRouteDefinition) -> Option<String> {
    route.job.as_ref().and_then(|job| {
        validate_job_config(&format!("custom_routes.{}.job", route.handler), job).err()
    })
}

fn route_limit_violation(route: &CustomRouteDefinition) -> Option<String> {
    let name = || {
        format!(
            "Custom route '{} {}' ({})",
            route.method, route.path, route.handler
        )
    };
    let body_limit_invalid = route
        .body_limit_bytes
        .is_some_and(|bytes| !(1..=CustomRouteDefinition::MAX_BODY_LIMIT_BYTES).contains(&bytes));
    let timeout_invalid = route
        .timeout_secs
        .is_some_and(|secs| !CustomRouteDefinition::TIMEOUT_SECS.contains(&secs));
    if body_limit_invalid {
        Some(format!(
            "{}: body_limit_bytes must be between 1 and {}",
            name(),
            CustomRouteDefinition::MAX_BODY_LIMIT_BYTES
        ))
    } else if timeout_invalid {
        Some(format!(
            "{}: timeout_secs must be between 1 and 120",
            name()
        ))
    } else if route.timeout_secs.is_some() && route.job.is_some() {
        Some(format!(
            "{}: timeout_secs cannot be combined with job; the job has its own timeout",
            name()
        ))
    } else if route.multipart
        && !CustomRouteDefinition::MULTIPART_METHODS.contains(&route.method.as_str())
    {
        Some(format!(
            "{}: multipart is only allowed on POST, PUT and PATCH",
            name()
        ))
    } else if let Err(e) = validate_audit_redact("audit_redact", &route.audit_redact) {
        Some(format!("{}: {e}", name()))
    } else {
        None
    }
}

fn sse_violation(route: &CustomRouteDefinition, routing: RoutingMode) -> Option<String> {
    let sse = route.sse.as_ref()?;
    let name = || {
        format!(
            "Custom route '{} {}' ({})",
            route.method, route.path, route.handler
        )
    };
    let params: Option<Vec<String>> = match routing {
        RoutingMode::Strict => Some(
            RoutePattern::parse(&route.path)
                .map(|pattern| pattern.params().map(str::to_string).collect())
                .unwrap_or_default(),
        ),
        RoutingMode::Legacy => None,
    };
    let anonymous_posture = matches!(route.posture, None | Some(RoutePosture::Public { .. }));
    if route.method != "GET" {
        Some(format!("{}: sse is only allowed on GET routes", name()))
    } else if route.job.is_some() {
        Some(format!("{}: sse cannot be combined with job", name()))
    } else {
        sse.subject_violation(params.as_deref())
            .or_else(|| {
                sse.anonymous_violation(anonymous_posture)
                    .map(str::to_string)
            })
            .map(|violation| format!("{}: {violation}", name()))
    }
}

fn raw_bridge_violation(
    route: &CustomRouteDefinition,
    manifest: &ServiceManifest,
) -> Option<String> {
    let sse = route.sse.as_ref().filter(|sse| sse.reducer.is_none())?;
    let mut named = sse.subject.split('.').skip(2);
    let table_token = named
        .next()
        .filter(|service| *service == manifest.name)
        .and_then(|_| named.next())?;
    let carries = |table: &str| {
        table_token == table || matches!(table_token, "*" | ">") || table_token.starts_with('{')
    };
    manifest
        .tables
        .iter()
        .find(|table| {
            carries(&table.name)
                && table
                    .restricted_columns()
                    .any(|(_, access)| access.read.is_some())
        })
        .map(|table| {
            format!(
                "Custom route '{} {}' ({}): sse forwards the events of table '{}' as they are, \
                 with the columns its column permissions withhold; declare a reducer",
                route.method, route.path, route.handler, table.name
            )
        })
}

fn validate_strict_routes(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    let routes = RouteTable::build(&manifest.custom_routes)
        .map_err(|e| PlatformError::Validation(format!("Strict routing: {e}")))?;
    match crud_shadow(&routes, manifest) {
        Some((index, table)) => {
            let route = manifest
                .custom_routes
                .get(index)
                .map_or_else(String::new, |r| {
                    format!("'{} {}' ({})", r.method, r.path, r.handler)
                });
            Err(PlatformError::Validation(format!(
                "Strict routing: custom route {route} shadows the CRUD routes of table '{table}'"
            )))
        }
        None => Ok(()),
    }
}

fn route_posture_enforced() -> bool {
    std::env::var("CUFFLINK_ENFORCE_ROUTE_POSTURE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

pub(super) fn validate_route_postures(
    manifest: &ServiceManifest,
    enforce_all: bool,
) -> Result<(), PlatformError> {
    let posture_aware = manifest.is_posture_aware();
    for route in &manifest.custom_routes {
        match &route.posture {
            None => {
                if posture_aware || enforce_all {
                    return Err(PlatformError::Validation(format!(
                        "Custom route '{} {}' ({}) has no declared posture. Every route must \
                         declare one of public(\"reason\") / auth / perm(\"area\",\"op\") / \
                         owner(\"field\").",
                        route.method, route.path, route.handler
                    )));
                }
            }
            Some(cufflink_types::RoutePosture::Internal { callers })
                if callers.is_empty()
                    || !callers.iter().all(|caller| {
                        caller == "*" || cufflink_types::slug::is_valid_service_name(caller)
                    }) =>
            {
                return Err(PlatformError::Validation(format!(
                    "Custom route '{} {}' ({}) declares internal() callers {:?}; name at least \
                     one service, or \"*\" for any.",
                    route.method, route.path, route.handler, callers
                )));
            }
            Some(cufflink_types::RoutePosture::Scoped {
                area, owner_field, ..
            }) if !declares_area(manifest, area)
                || owner_field
                    .as_deref()
                    .is_some_and(|field| !is_column_name(field)) =>
            {
                return Err(PlatformError::Validation(format!(
                    "Custom route '{} {}' ({}) declares scoped(\"{area}\", …); the area must be \
                     declared in authorization.areas and owner a column name",
                    route.method, route.path, route.handler
                )));
            }
            Some(cufflink_types::RoutePosture::ApiKey { scope })
                if !crate::auth::partner_keys::valid_scope(scope) =>
            {
                return Err(PlatformError::Validation(format!(
                    "Custom route '{} {}' ({}) declares api_key(\"{scope}\"); a scope is \
                     resource[:action] in lowercase letters, digits and _ . - (* as the action)",
                    route.method, route.path, route.handler
                )));
            }
            Some(cufflink_types::RoutePosture::Public { reason }) if reason.trim().is_empty() => {
                return Err(PlatformError::Validation(format!(
                    "Custom route '{} {}' ({}) declares public() with an empty reason; a public \
                     route must state why it is anonymous.",
                    route.method, route.path, route.handler
                )));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::minimal_manifest;
    use super::*;
    use cufflink_types::SseRouteDef;
    use cufflink_types::{CustomRouteDefinition, RoutePosture};

    fn route(handler: &str, posture: Option<RoutePosture>) -> CustomRouteDefinition {
        CustomRouteDefinition {
            method: "GET".into(),
            path: format!("/{handler}"),
            handler: handler.into(),
            job: None,
            posture,
            forward_auth: false,
            ..Default::default()
        }
    }

    #[test]
    fn posture_aware_manifest_rejects_undeclared_route() {
        let mut m = minimal_manifest();
        m.custom_routes
            .push(route("declared", Some(RoutePosture::Authenticated)));
        m.custom_routes.push(route("bare", None));
        let err = validate_route_postures(&m, false).unwrap_err().to_string();
        assert!(err.contains("no declared posture"), "got: {err}");
    }

    #[test]
    fn legacy_manifest_allows_undeclared_when_not_enforced() {
        let mut m = minimal_manifest();
        m.custom_routes.push(route("bare", None));
        assert!(validate_route_postures(&m, false).is_ok());
    }

    #[test]
    fn enforce_all_rejects_undeclared_even_in_legacy_manifest() {
        let mut m = minimal_manifest();
        m.custom_routes.push(route("bare", None));
        assert!(validate_route_postures(&m, true).is_err());
    }

    #[test]
    fn empty_public_reason_is_rejected() {
        let mut m = minimal_manifest();
        m.custom_routes.push(route(
            "hook",
            Some(RoutePosture::Public {
                reason: "  ".into(),
            }),
        ));
        let err = validate_route_postures(&m, false).unwrap_err().to_string();
        assert!(err.contains("empty reason"), "got: {err}");
    }

    fn strict(routes: &[(&str, &str, &str)]) -> ServiceManifest {
        let mut m = minimal_manifest();
        m.routing = RoutingMode::Strict;
        m.custom_routes = routes
            .iter()
            .map(|(method, path, handler)| CustomRouteDefinition {
                method: method.to_string(),
                path: path.to_string(),
                handler: handler.to_string(),
                job: None,
                posture: Some(RoutePosture::Authenticated),
                forward_auth: false,
                ..Default::default()
            })
            .collect();
        m
    }

    #[test]
    fn strict_routes_must_build_one_table_that_leaves_crud_reachable() {
        assert!(validate(&strict(&[
            ("GET", "/items/{id}/history", "history"),
            ("PUT", "/items/{id}/archive", "archive"),
            ("GET", "/reports/{*rest}", "report"),
        ]))
        .is_ok());
        for (routes, needle) in [
            (
                vec![("GET", "/a/{x}", "one"), ("GET", "/a/{y}", "two")],
                "conflicts",
            ),
            (
                vec![("GET", "/a", "one"), ("GET", "/a", "two")],
                "conflicts",
            ),
            (vec![("TRACE", "/a", "one")], "method 'TRACE'"),
            (vec![("GET", "/_fn/a", "one")], "reserved"),
            (vec![("GET", "a", "one")], "must start with '/'"),
            (
                vec![("GET", "/items/{id}", "shadow")],
                "'GET /items/{id}' (shadow) shadows the CRUD routes of table 'items'",
            ),
            (vec![("POST", "/{any}", "any")], "table 'items'"),
        ] {
            let err = validate(&strict(&routes)).unwrap_err().to_string();
            assert!(err.contains(needle), "{routes:?}: {err}");
        }
        let mut legacy = strict(&[("GET", "/a/{x}", "one"), ("GET", "/a/{y}", "two")]);
        legacy.routing = RoutingMode::Legacy;
        assert!(validate(&legacy).is_ok(), "legacy paths are informational");
    }

    #[test]
    fn route_limits_are_bounded_and_consistent() {
        let limited = |method: &str, edit: fn(&mut CustomRouteDefinition)| {
            let mut m = strict(&[(method, "/upload", "upload")]);
            m.routing = RoutingMode::Legacy;
            if let Some(route) = m.custom_routes.first_mut() {
                edit(route);
            }
            validate(&m)
        };
        assert!(limited("POST", |r| {
            r.body_limit_bytes = Some(64 * 1024 * 1024);
            r.timeout_secs = Some(120);
            r.multipart = true;
            r.audit_redact = vec!["card.number".into(), "lines[].sku".into()];
        })
        .is_ok());
        for (method, edit, needle) in [
            (
                "POST",
                (|r| r.body_limit_bytes = Some(64 * 1024 * 1024 + 1))
                    as fn(&mut CustomRouteDefinition),
                "body_limit_bytes",
            ),
            ("POST", |r| r.body_limit_bytes = Some(0), "body_limit_bytes"),
            ("POST", |r| r.timeout_secs = Some(0), "timeout_secs must be"),
            (
                "POST",
                |r| r.timeout_secs = Some(121),
                "timeout_secs must be",
            ),
            (
                "POST",
                |r| {
                    r.timeout_secs = Some(10);
                    r.job = Some(Default::default());
                },
                "cannot be combined with job",
            ),
            ("GET", |r| r.multipart = true, "multipart is only allowed"),
            (
                "POST",
                |r| r.audit_redact = vec!["card..number".into()],
                "audit_redact[0]: '' is not an identifier",
            ),
            (
                "DELETE",
                |r| r.multipart = true,
                "multipart is only allowed",
            ),
        ] {
            let err = limited(method, edit).unwrap_err().to_string();
            assert!(err.contains(needle), "{err}");
        }
    }

    #[test]
    fn event_streams_are_gets_reading_their_tenants_subjects() {
        let sse_route = |routing: RoutingMode, method: &str, path: &str, subject: &str| {
            let mut m = strict(&[(method, path, "progress")]);
            m.routing = routing;
            if let Some(route) = m.custom_routes.first_mut() {
                route.sse = Some(SseRouteDef {
                    subject: subject.into(),
                    reducer: Some("progress_reduce".into()),
                    anonymous: false,
                });
            }
            validate(&m)
        };
        assert!(sse_route(
            RoutingMode::Strict,
            "GET",
            "/receipts/book/{book_id}/progress",
            "dw.{tenant}.receipts.pdf.progress.{book_id}"
        )
        .is_ok());
        assert!(sse_route(RoutingMode::Legacy, "GET", "/jobs", "dw.{tenant}.jobs.>").is_ok());
        for (routing, method, path, subject, needle) in [
            (
                RoutingMode::Strict,
                "POST",
                "/q/{id}",
                "dw.{tenant}.q.{id}",
                "only allowed on GET",
            ),
            (
                RoutingMode::Strict,
                "GET",
                "/q/{id}",
                "dw.other.q",
                "must be dot-separated tokens starting dw.{tenant}.",
            ),
            (
                RoutingMode::Strict,
                "GET",
                "/q/{id}",
                "dw.{tenant}..q",
                "must be dot-separated tokens",
            ),
            (
                RoutingMode::Strict,
                "GET",
                "/q/{id}",
                "dw.{tenant}.q{id}",
                "must be dot-separated tokens",
            ),
            (
                RoutingMode::Strict,
                "GET",
                "/q/{id}",
                "dw.{tenant}.q.{other}",
                "placeholder '{other}'",
            ),
            (
                RoutingMode::Legacy,
                "GET",
                "/q/:id",
                "dw.{tenant}.q.{id}",
                "placeholder '{id}' needs routing: strict",
            ),
            (
                RoutingMode::Strict,
                "GET",
                "/audit",
                "dw.{tenant}.logs",
                "which the platform reserves",
            ),
            (
                RoutingMode::Strict,
                "GET",
                "/all",
                "dw.{tenant}.>",
                "must name its events with a literal token",
            ),
            (
                RoutingMode::Strict,
                "GET",
                "/any",
                "dw.{tenant}.*.x",
                "must name its events with a literal token",
            ),
            (
                RoutingMode::Strict,
                "GET",
                "/q/{service}",
                "dw.{tenant}.{service}.x",
                "must name its events with a literal token",
            ),
        ] {
            let err = sse_route(routing, method, path, subject)
                .unwrap_err()
                .to_string();
            assert!(err.contains(needle), "{subject}: {err}");
        }
        let mut job = strict(&[("GET", "/export", "export")]);
        if let Some(route) = job.custom_routes.first_mut() {
            route.job = Some(Default::default());
            route.sse = Some(SseRouteDef {
                subject: "dw.{tenant}.export".into(),
                reducer: None,
                anonymous: false,
            });
        }
        let err = validate(&job).unwrap_err().to_string();
        assert!(err.contains("cannot be combined with job"), "{err}");
    }

    #[test]
    fn a_stream_without_a_reducer_carries_no_table_whose_columns_are_withheld() {
        let stream = |subject: &str, reducer: Option<&str>, access: serde_json::Value| {
            let mut m = minimal_manifest();
            m.tables[0].permission_area = Some("items".into());
            m.tables[0].columns.push(
                serde_json::from_value(serde_json::json!({
                    "name": "notes", "column_type": "text", "nullable": true, "access": access
                }))
                .expect("column"),
            );
            let mut events = route("events", Some(RoutePosture::Authenticated));
            events.method = "GET".into();
            events.sse = Some(SseRouteDef {
                subject: subject.into(),
                reducer: reducer.map(str::to_string),
                anonymous: false,
            });
            m.custom_routes.push(events);
            validate(&m)
        };
        let withheld = || serde_json::json!({"read": "internal"});
        for subject in [
            "dw.{tenant}.svc.items.>",
            "dw.{tenant}.svc.items.created",
            "dw.{tenant}.svc.*.created",
            "dw.{tenant}.svc.>",
        ] {
            let err = stream(subject, None, withheld())
                .expect_err(subject)
                .to_string();
            assert!(
                err.contains("forwards the events of table 'items' as they are")
                    && err.contains("declare a reducer"),
                "{subject}: {err}"
            );
            assert!(
                stream(subject, Some("reduce"), withheld()).is_ok(),
                "{subject}"
            );
            assert!(
                stream(subject, None, serde_json::json!({"write": "internal"})).is_ok(),
                "{subject}: a column every caller reads is forwarded"
            );
        }
        for elsewhere in [
            "dw.{tenant}.svc.orders.>",
            "dw.{tenant}.other.items.>",
            "dw.{tenant}.svc",
        ] {
            assert!(stream(elsewhere, None, withheld()).is_ok(), "{elsewhere}");
        }
    }

    #[test]
    fn a_jobs_backoff_is_held_to_the_rules_the_macro_applies() {
        use cufflink_types::{BackoffSpec, JobConfig};
        let with_backoff = |base_secs, max_secs| {
            let mut manifest = strict(&[("POST", "/reports/run", "run_report")]);
            if let Some(route) = manifest.custom_routes.first_mut() {
                route.job = Some(JobConfig {
                    backoff: Some(BackoffSpec {
                        base_secs,
                        max_secs,
                    }),
                    ..Default::default()
                });
            }
            validate(&manifest)
        };
        assert!(with_backoff(5, 300).is_ok());
        let inverted = with_backoff(300, 5).unwrap_err().to_string();
        assert!(
            inverted.contains(
                "custom_routes.run_report.job.backoff.max_secs: 5 is below base_secs 300"
            ),
            "{inverted}"
        );
        let out_of_range = with_backoff(0, 5).unwrap_err().to_string();
        assert!(
            out_of_range.contains("custom_routes.run_report.job.backoff.base_secs"),
            "{out_of_range}"
        );
    }

    #[test]
    fn event_streams_open_to_anonymous_callers_must_opt_in() {
        let stream = |posture: Option<RoutePosture>, anonymous: bool| {
            let mut m = strict(&[("GET", "/progress", "progress")]);
            if let Some(route) = m.custom_routes.first_mut() {
                route.posture = posture;
                route.sse = Some(SseRouteDef {
                    subject: "dw.{tenant}.progress".into(),
                    reducer: None,
                    anonymous,
                });
            }
            validate(&m)
        };
        let public = || {
            Some(RoutePosture::Public {
                reason: "status page".into(),
            })
        };
        for posture in [None, public()] {
            let err = stream(posture.clone(), false).unwrap_err().to_string();
            assert!(err.contains("declare anonymous = true"), "{err}");
            assert!(stream(posture, true).is_ok());
        }
        assert!(stream(Some(RoutePosture::Authenticated), false).is_ok());
    }

    #[test]
    fn documented_manifests_must_generate_a_spec() {
        let mut m = strict(&[("GET", "/a", "same"), ("POST", "/b", "same")]);
        m.custom_routes
            .iter_mut()
            .for_each(|route| route.doc = Some(Default::default()));
        let err = validate(&m).unwrap_err().to_string();
        assert!(
            err.contains("OpenAPI docs: operation id 'same' is used more than once"),
            "{err}"
        );
        m.custom_routes
            .iter_mut()
            .for_each(|route| route.doc = None);
        assert!(
            validate(&m).is_ok(),
            "an undocumented manifest is not checked"
        );
    }

    #[test]
    fn an_error_profile_key_must_be_an_identifier() {
        let mut m = minimal_manifest();
        m.errors = Some(ErrorProfile {
            key: "error_message".into(),
        });
        assert!(validate(&m).is_ok());
        m.errors = Some(ErrorProfile {
            key: "Error Message".into(),
        });
        let err = validate(&m).unwrap_err().to_string();
        assert!(err.contains("'Error Message' must match"), "{err}");
    }

    #[test]
    fn internal_routes_name_valid_callers() {
        let internal = |callers: &[&str]| {
            let mut m = minimal_manifest();
            m.custom_routes.push(route(
                "reprice",
                Some(RoutePosture::Internal {
                    callers: callers.iter().map(|c| c.to_string()).collect(),
                }),
            ));
            validate(&m)
        };
        assert!(internal(&["pricing-service"]).is_ok());
        assert!(internal(&["*"]).is_ok());
        for bad in [&[][..], &["not a service"][..], &["pricing", ""][..]] {
            let err = internal(bad).unwrap_err().to_string();
            assert!(err.contains("declares internal() callers"), "{err}");
        }
    }

    #[test]
    fn api_key_routes_name_a_valid_scope() {
        let api_key = |scope: &str| {
            let mut m = minimal_manifest();
            m.custom_routes.push(route(
                "ingest",
                Some(RoutePosture::ApiKey {
                    scope: scope.into(),
                }),
            ));
            validate(&m)
        };
        for scope in ["books", "books:ingest", "books:*", "book-sync.v2:read"] {
            assert!(api_key(scope).is_ok(), "{scope}");
        }
        for scope in ["", "Books", "books:", ":ingest", "books:in gest"] {
            let err = api_key(scope).unwrap_err().to_string();
            assert!(err.contains("declares api_key"), "{scope}: {err}");
        }
    }

    #[test]
    fn scoped_routes_name_a_declared_area_and_a_column() {
        let scoped = |area: &str, owner: Option<&str>| {
            let mut m = minimal_manifest();
            m.authorization = Some(cufflink_types::AuthorizationConfig {
                areas: vec![cufflink_types::PermissionAreaDef {
                    name: "assets".into(),
                    operations: vec!["view".into()],
                }],
                default_roles: vec![],
            });
            m.custom_routes.push(route(
                "assets",
                Some(RoutePosture::Scoped {
                    area: area.into(),
                    operation: "view".into(),
                    owner_field: owner.map(str::to_string),
                }),
            ));
            validate(&m)
        };
        assert!(scoped("assets", None).is_ok());
        assert!(scoped("assets", Some("customer_id")).is_ok());
        for (area, owner) in [("reports", None), ("assets", Some("Customer Id"))] {
            let err = scoped(area, owner).unwrap_err().to_string();
            assert!(err.contains("declares scoped"), "{err}");
        }
    }

    #[test]
    fn fully_annotated_manifest_passes() {
        let mut m = minimal_manifest();
        m.custom_routes.push(route(
            "webhook",
            Some(RoutePosture::Public {
                reason: "stripe".into(),
            }),
        ));
        m.custom_routes.push(route(
            "edit",
            Some(RoutePosture::Permission {
                area: "items".into(),
                operation: "edit".into(),
            }),
        ));
        assert!(validate_route_postures(&m, true).is_ok());
    }
}
