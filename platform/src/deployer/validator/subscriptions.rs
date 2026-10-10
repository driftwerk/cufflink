use super::is_safe_identifier;
use crate::deployer::manifest::{ServiceManifest, SubscriptionDefinition};
use crate::errors::PlatformError;
use crate::nats_manager::consumers::naming::legacy;
use crate::nats_manager::subjects::{
    patterns_overlap, resolve_subject, subject_matches, Claim, Namespace,
};
use cufflink_types::manifest::validate::validate_subscription_delivery;
use cufflink_types::HandlerType;
use cufflink_types::StreamDefinition;

pub(super) fn validate(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    manifest
        .subscriptions
        .iter()
        .enumerate()
        .try_for_each(|(index, sub)| {
            validate_subscription(sub)?;
            validate_subscription_delivery(&format!("subscriptions[{index}]"), sub)
                .map_err(PlatformError::Validation)
        })
}

pub const SHARED_SUBJECT_PATTERNS: [&str; 1] = ["cufflink.tick.*"];
pub const SHARED_STREAMS: [&str; 1] = ["CUFFLINK_TICKS"];

pub fn validate_tenant_scope(
    manifest: &ServiceManifest,
    namespace: &Namespace,
    foreign_claims: &[(String, String)],
) -> Result<(), PlatformError> {
    manifest
        .subscriptions
        .iter()
        .find_map(|sub| {
            subscription_scope_violation(sub, namespace)
                .or_else(|| shared_binding_violation(&manifest.name, sub))
        })
        .or_else(|| {
            manifest
                .streams
                .iter()
                .enumerate()
                .find_map(|(index, stream)| stream_scope_violation(index, stream, namespace))
        })
        .or_else(|| publish_scope_violation(manifest, namespace, foreign_claims))
        .map_or(Ok(()), |reason| Err(PlatformError::Validation(reason)))
}

fn outside(namespace: &Namespace) -> String {
    let tenant = namespace.tenant_slug();
    match namespace.claims().next() {
        None => format!("is outside this tenant's dw.{tenant}.* namespace"),
        Some(_) => format!("is outside this tenant's dw.{tenant}.* namespace and its claims"),
    }
}

fn subject_in_scope(subject: &str, namespace: &Namespace) -> bool {
    namespace.covers(subject)
        || SHARED_SUBJECT_PATTERNS
            .iter()
            .any(|shared| subject_matches(shared, subject))
}

pub fn subscription_scope_violation(
    sub: &SubscriptionDefinition,
    namespace: &Namespace,
) -> Option<String> {
    let tenant_slug = namespace.tenant_slug();
    let subject = resolve_subject(&sub.subject, tenant_slug);
    if !subject_in_scope(&subject, namespace) {
        return Some(format!(
            "Subscription '{}': subject '{subject}' {}",
            sub.handler,
            outside(namespace)
        ));
    }
    let filters = sub
        .consumer
        .iter()
        .flat_map(|consumer| &consumer.filter_subjects)
        .map(|filter| resolve_subject(filter, tenant_slug));
    if let Some(filter) = filters
        .into_iter()
        .find(|f| !subject_in_scope(f, namespace))
    {
        return Some(format!(
            "Subscription '{}': filter subject '{filter}' {}",
            sub.handler,
            outside(namespace)
        ));
    }
    sub.stream
        .as_deref()
        .filter(|stream| !namespace.has_stream(stream) && !SHARED_STREAMS.contains(stream))
        .map(|stream| {
            format!(
                "Subscription '{}': stream '{stream}' belongs to another tenant (this tenant's is {})",
                sub.handler,
                namespace.own_stream()
            )
        })
}

fn shared_binding_violation(service_name: &str, sub: &SubscriptionDefinition) -> Option<String> {
    let stream = sub
        .stream
        .as_deref()
        .filter(|stream| SHARED_STREAMS.contains(stream))?;
    let bound = sub.consumer.as_ref()?.durable_name.as_deref()?;
    let own = legacy(service_name, &sub.handler);
    (bound != own).then(|| {
        format!(
            "Subscription '{}': durable '{bound}' on shared stream '{stream}' is not this \
             service's; the one it may bind there is '{own}'",
            sub.handler
        )
    })
}

fn stream_scope_violation(
    index: usize,
    stream: &StreamDefinition,
    namespace: &Namespace,
) -> Option<String> {
    let claimed = namespace
        .claims()
        .any(|claim| claim == Claim::stream(stream.name.as_str()));
    if !claimed {
        return Some(format!(
            "streams[{index}].name: '{}' is not a stream this tenant claims",
            stream.name
        ));
    }
    stream
        .subjects
        .iter()
        .map(|subject| resolve_subject(subject, namespace.tenant_slug()))
        .find(|subject| !namespace.covers(subject))
        .map(|subject| {
            format!(
                "streams[{index}].subjects: '{subject}' {}",
                outside(namespace)
            )
        })
}

fn publish_scope_violation(
    manifest: &ServiceManifest,
    namespace: &Namespace,
    foreign_claims: &[(String, String)],
) -> Option<String> {
    manifest
        .publishes
        .iter()
        .flatten()
        .map(|pattern| resolve_subject(pattern, namespace.tenant_slug()))
        .find_map(|pattern| {
            foreign_claims
                .iter()
                .find(|(_, theirs)| patterns_overlap(&pattern, theirs))
                .map(|(tenant, theirs)| {
                    format!(
                        "publishes: '{pattern}' overlaps '{theirs}', which tenant '{tenant}' claims"
                    )
                })
        })
}

fn validate_subscription(sub: &SubscriptionDefinition) -> Result<(), PlatformError> {
    match sub.handler_type {
        HandlerType::DeleteCascade => {
            let table = sub.config.get("table").ok_or_else(|| {
                PlatformError::Validation(format!(
                    "Subscription '{}': delete_cascade requires 'table' config",
                    sub.handler
                ))
            })?;
            if !is_safe_identifier(table) {
                return Err(PlatformError::Validation(format!(
                    "Subscription '{}': 'table' config '{}' is not a valid identifier",
                    sub.handler, table
                )));
            }
            let fk = sub.config.get("foreign_key").ok_or_else(|| {
                PlatformError::Validation(format!(
                    "Subscription '{}': delete_cascade requires 'foreign_key' config",
                    sub.handler
                ))
            })?;
            if !is_safe_identifier(fk) {
                return Err(PlatformError::Validation(format!(
                    "Subscription '{}': 'foreign_key' config '{}' is not a valid identifier",
                    sub.handler, fk
                )));
            }
        }
        HandlerType::UpdateField => {
            for key in &["table", "where_column", "set_column"] {
                let val = sub.config.get(*key).ok_or_else(|| {
                    PlatformError::Validation(format!(
                        "Subscription '{}': update_field requires '{}' config",
                        sub.handler, key
                    ))
                })?;
                if !is_safe_identifier(val) {
                    return Err(PlatformError::Validation(format!(
                        "Subscription '{}': '{}' config '{}' is not a valid identifier",
                        sub.handler, key, val
                    )));
                }
            }
            let set_value = sub.config.get("set_value").ok_or_else(|| {
                PlatformError::Validation(format!(
                    "Subscription '{}': update_field requires 'set_value' config",
                    sub.handler
                ))
            })?;
            if !is_safe_set_value(set_value) {
                return Err(PlatformError::Validation(format!(
                    "Subscription '{}': 'set_value' config '{}' is not a safe SQL expression. \
                     Use a column name, a literal value, or column +/- number (e.g., 'count + 1')",
                    sub.handler, set_value
                )));
            }
        }
        HandlerType::Webhook | HandlerType::Wasm => {}
    }
    if let Some(stream) = sub.stream.as_deref() {
        if !is_safe_identifier(stream) {
            return Err(PlatformError::Validation(format!(
                "Subscription '{}': stream name '{}' is not a valid identifier",
                sub.handler, stream
            )));
        }
    }
    Ok(())
}

fn is_safe_set_value(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == ' ' || c == '+' || c == '-' || c == '.')
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::*;

    fn manifest(subscriptions: &[(&str, Option<&str>)]) -> ServiceManifest {
        serde_json::from_value(serde_json::json!({
            "name": "svc",
            "tables": [],
            "subscriptions": subscriptions
                .iter()
                .map(|(subject, stream)| serde_json::json!({
                    "subject": subject,
                    "handler": "h",
                    "handler_type": "wasm",
                    "stream": stream,
                }))
                .collect::<Vec<_>>(),
        }))
        .expect("manifest")
    }

    #[test]
    fn tenant_scope_accepts_own_dw_and_ticks() {
        let m = manifest(&[
            ("dw.{tenant}.stock-service.items.updated", None),
            ("dw.{tenant}.shed.listings.*.status_changed", None),
            ("cufflink.tick.5min", Some("CUFFLINK_TICKS")),
            ("dw.{tenant}.x.y", Some("DW_PREVIEW_PR_7")),
        ]);
        assert!(validate_tenant_scope(&m, &Namespace::own("preview-pr-7"), &[]).is_ok());
    }

    #[test]
    fn tenant_scope_rejects_foreign_dw() {
        let m = manifest(&[("dw.victim.logs", None)]);
        let err = validate_tenant_scope(&m, &Namespace::own("acme"), &[]).unwrap_err();
        assert!(err.to_string().contains("dw.victim.logs"), "{err}");
    }

    #[test]
    fn tenant_scope_rejects_wildcard_tenant() {
        for subject in ["dw.*.logs", "dw.>", "cufflink.wasm.>"] {
            assert!(
                validate_tenant_scope(&manifest(&[(subject, None)]), &Namespace::own("acme"), &[])
                    .is_err(),
                "{subject}"
            );
        }
    }

    #[test]
    fn tenant_scope_rejects_foreign_stream() {
        let m = manifest(&[("dw.{tenant}.x.y", Some("DW_VICTIM"))]);
        assert!(validate_tenant_scope(&m, &Namespace::own("acme"), &[]).is_err());
    }

    #[test]
    fn a_durable_of_a_shared_stream_is_bound_by_its_own_legacy_name_alone() {
        let binding = |durable_name: &str| {
            let mut m = manifest(&[
                ("cufflink.tick.5min", Some("CUFFLINK_TICKS")),
                ("dw.{tenant}.x.y", None),
            ]);
            for sub in &mut m.subscriptions {
                sub.consumer = Some(cufflink_types::ConsumerSpec {
                    durable_name: Some(durable_name.to_string()),
                    ..Default::default()
                });
            }
            validate_tenant_scope(&m, &Namespace::own("preview-pr-7"), &[])
        };
        assert!(binding("svc_h").is_ok());
        assert_eq!(
            binding("other_service_h")
                .expect_err("another's tick durable")
                .to_string(),
            "Validation error: Subscription 'h': durable 'other_service_h' on shared stream \
             'CUFFLINK_TICKS' is not this service's; the one it may bind there is 'svc_h'"
        );
    }

    fn harbor() -> Namespace {
        Namespace::new(
            "harbor",
            [
                Claim::subject("books.>"),
                Claim::subject("audit.logs"),
                Claim::stream("BOOKS"),
            ],
        )
    }

    fn refused(manifest: &ServiceManifest, namespace: &Namespace) -> String {
        validate_tenant_scope(manifest, namespace, &[])
            .expect_err("refused")
            .to_string()
    }

    #[test]
    fn tenant_scope_reaches_what_the_tenant_claims() {
        let m = manifest(&[
            ("books.due_date.set", Some("BOOKS")),
            ("books.*.loans", None),
            ("audit.logs", None),
            ("dw.{tenant}.loans.created", Some("DW_HARBOR")),
            ("cufflink.tick.hourly", Some("CUFFLINK_TICKS")),
        ]);
        assert!(validate_tenant_scope(&m, &harbor(), &[]).is_ok());
        assert_eq!(
            refused(&m, &Namespace::own("harbor")),
            "Validation error: Subscription 'h': subject 'books.due_date.set' is outside \
             this tenant's dw.harbor.* namespace"
        );
        for (subject, stream) in [
            ("receipts.created", None),
            ("audit.*", None),
            ("*.created", None),
            ("books.created", Some("RECEIPTS")),
        ] {
            let error = refused(&manifest(&[(subject, stream)]), &harbor());
            assert!(
                error.contains(stream.unwrap_or(subject)),
                "{subject} {stream:?}: {error}"
            );
        }
        assert!(refused(&manifest(&[("receipts.created", None)]), &harbor())
            .ends_with("is outside this tenant's dw.harbor.* namespace and its claims"));
    }

    #[test]
    fn tenant_scope_holds_consumer_filters_to_the_namespace() {
        let with_filters = |filters: &[&str]| {
            let mut m = manifest(&[("books.due_date.set", Some("BOOKS"))]);
            m.subscriptions[0].consumer = Some(cufflink_types::ConsumerSpec {
                filter_subjects: filters.iter().map(|f| f.to_string()).collect(),
                ..Default::default()
            });
            m
        };
        assert!(
            validate_tenant_scope(&with_filters(&["books.deleted.loans"]), &harbor(), &[]).is_ok()
        );
        assert_eq!(
            refused(
                &with_filters(&["books.deleted.loans", "receipts.>"]),
                &harbor()
            ),
            "Validation error: Subscription 'h': filter subject 'receipts.>' is outside this \
             tenant's dw.harbor.* namespace and its claims"
        );
    }

    #[test]
    fn tenant_scope_holds_declared_streams_to_the_claims() {
        let declaring = |name: &str, subjects: &[&str]| {
            let mut m = manifest(&[]);
            m.streams = vec![cufflink_types::StreamDefinition {
                name: name.into(),
                subjects: subjects.iter().map(|s| s.to_string()).collect(),
                retention: cufflink_types::StreamRetention::Limits,
                max_age_secs: None,
                max_msgs: None,
                max_bytes: None,
                storage: Default::default(),
                duplicate_window_secs: None,
                discard: Default::default(),
                description: None,
            }];
            m
        };
        assert!(validate_tenant_scope(
            &declaring("BOOKS", &["books.created", "books.deleted.>"]),
            &harbor(),
            &[]
        )
        .is_ok());
        for (name, subjects, reason) in [
            (
                "RECEIPTS",
                vec!["books.created"],
                "streams[0].name: 'RECEIPTS' is not a stream this tenant claims",
            ),
            (
                "DW_HARBOR",
                vec!["dw.harbor.x"],
                "streams[0].name: 'DW_HARBOR' is not a stream this tenant claims",
            ),
            (
                "CUFFLINK_TICKS",
                vec!["books.x"],
                "streams[0].name: 'CUFFLINK_TICKS' is not a stream this tenant claims",
            ),
            (
                "BOOKS",
                vec!["books.created", "receipts.created"],
                "streams[0].subjects: 'receipts.created' is outside",
            ),
            (
                "BOOKS",
                vec!["dw.other.x"],
                "streams[0].subjects: 'dw.other.x' is outside",
            ),
        ] {
            let error = refused(&declaring(name, &subjects), &harbor());
            assert!(error.contains(reason), "{name} {subjects:?}: {error}");
        }
    }

    #[test]
    fn tenant_scope_keeps_publishes_clear_of_other_tenants_claims() {
        let publishing = |patterns: &[&str]| {
            let mut m = manifest(&[]);
            m.publishes = Some(patterns.iter().map(|p| p.to_string()).collect());
            m
        };
        let foreign = [("harbor".to_string(), "books.>".to_string())];
        let acme = Namespace::own("acme");
        assert!(validate_tenant_scope(
            &publishing(&["dw.{tenant}.>", "acme.orders.>", "books"]),
            &acme,
            &foreign
        )
        .is_ok());
        for pattern in ["books.created", "books.>", "*.created", ">"] {
            assert_eq!(
                validate_tenant_scope(&publishing(&[pattern]), &acme, &foreign)
                    .expect_err(pattern)
                    .to_string(),
                format!(
                    "Validation error: publishes: '{pattern}' overlaps 'books.>', which tenant \
                     'harbor' claims"
                )
            );
        }
        assert!(
            validate_tenant_scope(&publishing(&["books.created"]), &harbor(), &[]).is_ok(),
            "a tenant publishes on what it claims itself"
        );
    }

    #[test]
    fn no_subscription_or_declared_stream_binds_an_extensions_subject() {
        let subject = cufflink_types::ext::call_subject("acme", "google.token");
        for subscribed in [subject.as_str(), "cufflink.ext.>", "cufflink.ext.acme.*.*"] {
            assert!(
                validate_tenant_scope(
                    &manifest(&[(subscribed, None)]),
                    &Namespace::own("acme"),
                    &[]
                )
                .is_err(),
                "{subscribed}"
            );
        }
        let mut declaring = manifest(&[]);
        declaring.streams = vec![cufflink_types::StreamDefinition {
            name: "BOOKS".into(),
            subjects: vec![subject],
            retention: cufflink_types::StreamRetention::Limits,
            max_age_secs: None,
            max_msgs: None,
            max_bytes: None,
            storage: Default::default(),
            duplicate_window_secs: None,
            discard: Default::default(),
            description: None,
        }];
        assert!(validate_tenant_scope(&declaring, &harbor(), &[]).is_err());
    }

    #[test]
    fn tenant_scope_accepts_cufflink_ticks_stream() {
        let m = manifest(&[("cufflink.tick.1hour", Some("CUFFLINK_TICKS"))]);
        assert!(validate_tenant_scope(&m, &Namespace::own("default"), &[]).is_ok());
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::minimal_manifest;
    use super::super::validate_manifest;
    use super::*;
    use cufflink_types::{AckMode, ConsumerSpec};

    fn subscription(handler: &str, stream: Option<&str>) -> SubscriptionDefinition {
        SubscriptionDefinition {
            stream: stream.map(str::to_string),
            ..SubscriptionDefinition::new("x", handler, HandlerType::Wasm)
        }
    }

    #[test]
    fn subscription_without_stream_is_valid() {
        let mut m = minimal_manifest();
        m.subscriptions.push(subscription("h", None));
        assert!(validate_manifest(&m).is_ok());
    }

    #[test]
    fn subscription_with_valid_stream_is_valid() {
        let mut m = minimal_manifest();
        m.subscriptions
            .push(subscription("h", Some("CUFFLINK_TICKS")));
        assert!(validate_manifest(&m).is_ok());
    }

    #[test]
    fn subscription_with_empty_stream_is_rejected() {
        let mut m = minimal_manifest();
        m.subscriptions.push(subscription("h", Some("")));
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(err.contains("not a valid identifier"), "got error: {}", err);
    }

    #[test]
    fn subscription_with_dashed_stream_is_rejected() {
        let mut m = minimal_manifest();
        m.subscriptions.push(subscription("h", Some("my-stream")));
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(err.contains("not a valid identifier"), "got error: {}", err);
    }

    #[test]
    fn delivery_fields_are_validated_by_path() {
        let mut m = minimal_manifest();
        m.subscriptions.push(subscription("first", None));
        m.subscriptions.push(SubscriptionDefinition {
            ack: AckMode::OnSuccess,
            timeout_secs: Some(120),
            consumer: Some(ConsumerSpec {
                durable_name: Some("loans-service-book-consumer".into()),
                max_deliver: Some(3),
                ..Default::default()
            }),
            ..subscription("second", None)
        });
        assert!(validate_manifest(&m).is_ok());
        m.subscriptions[1].timeout_secs = Some(301);
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(err.contains("subscriptions[1].timeout_secs"), "{err}");
        m.subscriptions[1].timeout_secs = None;
        m.subscriptions[1].consumer = Some(ConsumerSpec {
            durable_name: Some("a.b".into()),
            ..Default::default()
        });
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(
            err.contains("subscriptions[1].consumer.durable_name"),
            "{err}"
        );
    }
}
