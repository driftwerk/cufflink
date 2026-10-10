use super::*;
use crate::router::authz::RowScope;
use crate::router::query::SqlParams;

#[test]
fn hook_rejection_shape_unchanged() {
    let rejected =
        crate::router::crud::interpret_hook_response(json!({"error": "name is required"}))
            .unwrap_err();
    assert!(
        matches!(&rejected, PlatformError::BadRequest(m) if m == "Hook error: name is required"),
        "{rejected:?}"
    );
}

#[test]
fn owner_self_service_tables_are_never_permission_gated() {
    let t = tenants();
    let m = sample_manifest();
    let default = entry(&t.default, &[]);
    let customer = jwt(&default, "orchard", "customer", "portal");
    let notifications = table(&m, "customer_notifications");
    assert!(grant_required(notifications, &Some(customer.clone()))
        .expect("authenticated")
        .is_none());
    for operation in ["view", "create", "edit", "delete"] {
        assert_eq!(
            class(data_route(
                notifications,
                operation,
                Some(customer.clone()),
                None,
                &default,
                &Grants(HashMap::new())
            )),
            Class::Ok
        );
    }
}

#[test]
fn sse_owner_table_filters_events_to_subject() {
    let event = |owner: &str| {
        json!({"action": "created", "data": {"id": "n", "customer_id": owner}})
            .to_string()
            .into_bytes()
    };
    let m = sample_manifest();
    let notifications = table(&m, "customer_notifications");
    let customer = jwt(
        &entry(&tenants().default, &[]),
        "orchard",
        "customer-1",
        "portal",
    );
    let delivered = crate::router::sse::Delivered {
        scope: RowScope::legacy(notifications, &Some(customer)).into_filter(),
        columns: notifications.columns.clone(),
        view: crate::auth::columns::ColumnView::ALL,
    };
    let visible = |owner| crate::router::sse::event_visible(&event(owner), &delivered);
    assert!(visible("customer-1").is_some());
    assert!(visible("customer-2").is_none());
}

#[test]
fn sse_staff_table_requires_view_grant() {
    let t = tenants();
    let m = sample_manifest();
    let default = entry(&t.default, &[]);
    let grants = staff_grants(&t);
    let stream = |sub| {
        class(data_route(
            table(&m, "items"),
            "view",
            Some(jwt(&default, "orchard", sub, "backoffice")),
            None,
            &default,
            &grants,
        ))
    };
    assert_eq!(stream("customer"), Class::Forbidden);
    assert_eq!(stream("staff"), Class::Ok);
}

#[test]
fn graphql_is_404_without_manifest_flag() {
    let t = tenants();
    let service = crate::registry::RegisteredService {
        service_id: uuid::Uuid::nil(),
        tenant_id: t.default.id,
        tenant_slug: t.default.slug.clone(),
        name: "stock-service".into(),
        manifest: sample_manifest(),
        version: 1,
        wasm_hash: None,
        commit_hash: None,
    };
    let err =
        crate::router::graphql::graphql_enabled(&service, &entry(&t.default, &[])).unwrap_err();
    assert!(matches!(err, PlatformError::NotFound(_)), "{err:?}");
}

const ROUTES: [&str; 16] = [
    "crud.list",
    "crud.create",
    "crud.get",
    "crud.update",
    "crud.delete",
    "batch",
    "graphql",
    "sse",
    "owner.list",
    "owner.create",
    "owner.sse",
    "fn.public",
    "fn.auth",
    "fn.perm",
    "fn.owner",
    "jobs",
];

fn evaluate(
    route: &str,
    principal: Option<AuthUser>,
    header: Option<&str>,
    path: &TenantEntry,
    grants: &Grants,
) -> Class {
    let m = sample_manifest();
    let items = table(&m, "items");
    let owned = table(&m, "customer_notifications");
    let data = |t, op| class(data_route(t, op, principal.clone(), header, path, grants));
    let call = |handler| {
        class(fn_route(
            posture(&m, handler),
            principal.clone(),
            header,
            path,
            grants,
        ))
    };
    match route {
        "crud.list" | "crud.get" | "sse" => data(items, "view"),
        "graphql" => class(
            data_route(items, "view", principal.clone(), header, path, grants)
                .and_then(|_| crate::router::graphql::graphql_enabled(&opted_in(path), path)),
        ),
        "crud.create" => data(items, "create"),
        "crud.update" | "batch" => data(items, "edit"),
        "crud.delete" => data(items, "delete"),
        "owner.list" | "owner.sse" => data(owned, "view"),
        "owner.create" => data(owned, "create"),
        "fn.public" => call("stripe_webhook"),
        "fn.auth" => call("me"),
        "fn.perm" => call("list_items"),
        "fn.owner" => call("my_orders"),
        "jobs" => call("reprice"),
        other => panic!("unknown route class {other}"),
    }
}

fn opted_in(path: &TenantEntry) -> crate::registry::RegisteredService {
    crate::registry::RegisteredService {
        service_id: uuid::Uuid::nil(),
        tenant_id: path.tenant.id,
        tenant_slug: path.tenant.slug.clone(),
        name: "stock-service".into(),
        manifest: ServiceManifest {
            graphql: true,
            ..sample_manifest()
        },
        version: 1,
        wasm_hash: None,
        commit_hash: None,
    }
}

fn parse(expected: &str) -> Vec<Class> {
    expected
        .chars()
        .filter(|c| !c.is_whitespace())
        .map(|c| match c {
            'A' => Class::Ok,
            'U' => Class::Unauthorized,
            'F' => Class::Forbidden,
            other => panic!("bad cell {other}"),
        })
        .collect()
}

struct Caller {
    name: &'static str,
    principal: fn(&Tenants, Modes) -> Option<AuthUser>,
    header: Option<&'static str>,
    on_preview: bool,
}

const ALLOWED: &str = "AAAAAAAA AAA AAAA A";
const NO_GRANT: &str = "FFFFFFFF AAA AAFA F";
const REFUSED_CROSS_TENANT: &str = "FFFFFFFF FFF AFFF F";
const ANONYMOUS: &str = "UUUUUUUU UUU AUUU U";

#[derive(Clone, Copy)]
struct Modes {
    binding: PolicyMode,
    service_accounts: PolicyMode,
}

impl Modes {
    fn on(self, tenant: &Tenant) -> TenantEntry {
        entry(tenant, &modes(tenant, self.binding, self.service_accounts))
    }
}

struct Row {
    caller: Caller,
    warn_warn: &'static str,
    warn_enforce: &'static str,
    enforce_warn: &'static str,
    enforce_enforce: &'static str,
}

fn row(caller: Caller, binding_warn: &'static str, binding_enforce: &'static str) -> Row {
    Row {
        caller,
        warn_warn: binding_warn,
        warn_enforce: binding_warn,
        enforce_warn: binding_enforce,
        enforce_enforce: binding_enforce,
    }
}

fn rows() -> Vec<Row> {
    vec![
        row(
            Caller {
                name: "anonymous",
                principal: |_, _| None,
                header: None,
                on_preview: false,
            },
            ANONYMOUS,
            ANONYMOUS,
        ),
        row(
            Caller {
                name: "same-tenant user without grant",
                principal: |t, m| Some(jwt(&m.on(&t.default), "orchard", "customer", "portal")),
                header: None,
                on_preview: false,
            },
            NO_GRANT,
            NO_GRANT,
        ),
        row(
            Caller {
                name: "same-tenant user with grant",
                principal: |t, m| Some(jwt(&m.on(&t.default), "orchard", "staff", "backoffice")),
                header: None,
                on_preview: false,
            },
            ALLOWED,
            ALLOWED,
        ),
        row(
            Caller {
                name: "same-realm preview user without header",
                principal: |t, m| Some(jwt(&m.on(&t.default), "orchard", "staff", "backoffice")),
                header: None,
                on_preview: true,
            },
            ALLOWED,
            ALLOWED,
        ),
        row(
            Caller {
                name: "cross-realm user",
                principal: |t, m| {
                    Some(jwt(
                        &m.on(&t.harbor),
                        "harbor",
                        "harbor-user",
                        "harbor-portal",
                    ))
                },
                header: None,
                on_preview: false,
            },
            ANONYMOUS,
            REFUSED_CROSS_TENANT,
        ),
        row(
            Caller {
                name: "tenant API key (own)",
                principal: |t, _| Some(api_key(&t.default, false)),
                header: None,
                on_preview: false,
            },
            NO_GRANT,
            NO_GRANT,
        ),
        row(
            Caller {
                name: "tenant API key (foreign)",
                principal: |t, _| Some(api_key(&t.harbor, false)),
                header: None,
                on_preview: false,
            },
            ANONYMOUS,
            REFUSED_CROSS_TENANT,
        ),
        row(
            Caller {
                name: "admin key (header = path)",
                principal: |t, _| Some(api_key(&t.default, true)),
                header: Some("default"),
                on_preview: false,
            },
            NO_GRANT,
            NO_GRANT,
        ),
        row(
            Caller {
                name: "admin key (header != path)",
                principal: |t, _| Some(api_key(&t.harbor, true)),
                header: Some("harbor"),
                on_preview: false,
            },
            ANONYMOUS,
            REFUSED_CROSS_TENANT,
        ),
        row(
            Caller {
                name: "service account listed on the path tenant",
                principal: |t, m| Some(jwt(&m.on(&t.default), "orchard", "bot", "default-bot")),
                header: None,
                on_preview: false,
            },
            ALLOWED,
            ALLOWED,
        ),
        Row {
            caller: Caller {
                name: "service account on the platform list only",
                principal: |t, m| Some(jwt(&m.on(&t.default), "orchard", "ops", ORCHARD_OPS)),
                header: None,
                on_preview: false,
            },
            warn_warn: ALLOWED,
            warn_enforce: NO_GRANT,
            enforce_warn: ALLOWED,
            enforce_enforce: NO_GRANT,
        },
        row(
            Caller {
                name: "service account listed on another tenant",
                principal: |t, m| Some(jwt(&m.on(&t.default), "orchard", "bot", "preview-bot")),
                header: None,
                on_preview: false,
            },
            NO_GRANT,
            NO_GRANT,
        ),
        row(
            Caller {
                name: "authz-test",
                principal: |t, m| {
                    Some(jwt(
                        &m.on(&t.default),
                        "orchard",
                        "authz-test",
                        "authz-test",
                    ))
                },
                header: None,
                on_preview: false,
            },
            NO_GRANT,
            NO_GRANT,
        ),
    ]
}

#[test]
fn every_caller_on_every_route_class_under_warn_and_enforce() {
    let t = tenants();
    let grants = staff_grants(&t);
    let combos = [
        (PolicyMode::Warn, PolicyMode::Warn),
        (PolicyMode::Warn, PolicyMode::Enforce),
        (PolicyMode::Enforce, PolicyMode::Warn),
        (PolicyMode::Enforce, PolicyMode::Enforce),
    ];
    let mut failures = Vec::new();
    for row in rows() {
        let expectations = [
            row.warn_warn,
            row.warn_enforce,
            row.enforce_warn,
            row.enforce_enforce,
        ];
        for ((binding, sa), expected) in combos.into_iter().zip(expectations) {
            let m = Modes {
                binding,
                service_accounts: sa,
            };
            let path = m.on(if row.caller.on_preview {
                &t.preview
            } else {
                &t.default
            });
            let actual: Vec<Class> = ROUTES
                .iter()
                .map(|route| {
                    let principal = (row.caller.principal)(&t, m);
                    evaluate(route, principal, row.caller.header, &path, &grants)
                })
                .collect();
            if actual != parse(expected) {
                failures.push(format!(
                    "{} (binding {binding}, service_accounts {sa}): expected {expected}, got {actual:?}",
                    row.caller.name
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn role_strings_without_scope_behave_as_before() {
    use crate::auth::grants::has_permission;
    let corpus: Vec<String> = [
        "items:*",
        "items:view",
        "batches:view",
        "media:edit",
        "notifications:view",
        "roles:manage",
        "webhooks:manage",
        "staff:create",
        "*",
    ]
    .iter()
    .map(|p| p.to_string())
    .collect();
    let exact = |p: &str, area: &str, op: &str| {
        p == format!("{area}:{op}") || p == format!("{area}:*") || p == "*"
    };
    for permission in &corpus {
        for (area, op) in [
            ("items", "view"),
            ("items", "delete"),
            ("batches", "view"),
            ("media", "view"),
            ("roles", "manage"),
            ("notifications", "view"),
        ] {
            assert_eq!(
                has_permission(std::slice::from_ref(permission), area, op),
                exact(permission, area, op),
                "{permission} for {area}:{op}"
            );
        }
    }

    let t = tenants();
    let staff = jwt(&entry(&t.default, &[]), "orchard", "staff", "backoffice");
    let auth = crate::router::authz::authenticated_guest_auth(
        &staff,
        &ResolvedPermissions {
            permissions: corpus.clone(),
            role_names: vec!["staff".into()],
        },
    );
    assert_eq!(auth["permissions"], json!(corpus));
    assert!(auth.get("scoped_permissions").is_none(), "{auth}");
}

#[test]
fn owner_field_tables_without_scoped_by_unchanged() {
    let m = sample_manifest();
    let notifications = table(&m, "customer_notifications");
    let customer = jwt(&entry(&tenants().default, &[]), "orchard", "c-1", "portal");
    let rows = RowScope::legacy(notifications, &Some(customer));
    let mut params = SqlParams::default();
    assert_eq!(
        rows.condition(notifications, &mut params)
            .expect("condition")
            .as_deref(),
        Some("customer_id = $1")
    );
    assert_eq!(format!("{params:?}"), r#"["c-1"]"#);
    let anonymous = RowScope::legacy(notifications, &None);
    assert!(matches!(
        anonymous.condition(notifications, &mut SqlParams::default()),
        Err(PlatformError::Unauthorized(_))
    ));
    let items = RowScope::legacy(table(&m, "items"), &None);
    assert_eq!(
        items
            .condition(table(&m, "items"), &mut SqlParams::default())
            .expect("condition"),
        None
    );
    assert!(m.tables.iter().all(|t| t.derived_features().count() == 0));
}
