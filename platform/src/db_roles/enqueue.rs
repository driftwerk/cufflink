use crate::db_roles::verify::Check;
use crate::deployer::manifest::ServiceManifest;
use cufflink_db::{quote_ident, quote_literal};
use deadpool_postgres::GenericClient;

pub const CUFFLINK_SCHEMA: &str = "_cufflink";
pub const SHARED: &str =
    "_cufflink.outbox_enqueue_many(TEXT[], TEXT[], TEXT[], BYTEA[], JSONB[], UUID[])";
pub const OWN: &str = "_cufflink.outbox_enqueue_own(TEXT[], TEXT[], BYTEA[], JSONB[], UUID[])";
pub const SERVICE_ROLES: &str = "_cufflink.service_roles";

/// Whether the platform may enqueue in the service's own session, and so
/// grants its role the outbox: a write to, or an audited read of, a table
/// it serves (with that table's webhooks). A service that declares events
/// it publishes is counted too, so a publish the outbox comes to carry finds
/// the grant in place. Every other row the platform enqueues for a service,
/// a route's audit record among them, is written in the platform's own
/// session when this is false.
pub fn in_session(manifest: &ServiceManifest) -> bool {
    !manifest.tables.is_empty()
        || manifest
            .publishes
            .as_ref()
            .is_some_and(|publishes| !publishes.is_empty())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enqueue {
    Absent,
    Shared {
        execute: bool,
    },
    Own {
        execute: bool,
        shared: bool,
        mapped: bool,
    },
}

pub async fn facts(
    client: &impl GenericClient,
    role: Option<&str>,
    service: &str,
) -> Result<Enqueue, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT to_regprocedure($1) IS NOT NULL,
                    to_regprocedure($2) IS NOT NULL AND to_regclass($3) IS NOT NULL,
                    COALESCE(has_function_privilege($4::name, to_regprocedure($1), 'EXECUTE'), false),
                    COALESCE(has_function_privilege($4::name, to_regprocedure($2), 'EXECUTE'), false)",
            &[&SHARED, &OWN, &SERVICE_ROLES, &role],
        )
        .await?;
    let (shared_exists, own_exists): (bool, bool) = (row.get(0), row.get(1));
    let (shared, own): (bool, bool) = (row.get(2), row.get(3));
    Ok(match (own_exists, shared_exists) {
        (true, _) => {
            let mapping = client
                .query_one(
                    &format!(
                        "SELECT EXISTS (SELECT 1 FROM {SERVICE_ROLES}
                                        WHERE service = $2 AND role = $1::name)"
                    ),
                    &[&role, &service],
                )
                .await?;
            Enqueue::Own {
                execute: own,
                shared,
                mapped: mapping.get(0),
            }
        }
        (false, true) => Enqueue::Shared { execute: shared },
        (false, false) => Enqueue::Absent,
    })
}

pub fn statements(role: &str, service: &str, enqueue: Enqueue) -> Vec<String> {
    let quoted = quote_ident(role);
    let grant = |function: &str| format!("GRANT EXECUTE ON FUNCTION {function} TO {quoted}");
    match enqueue {
        Enqueue::Absent => vec![],
        Enqueue::Shared { execute } => (!execute).then(|| grant(SHARED)).into_iter().collect(),
        Enqueue::Own {
            execute,
            shared,
            mapped,
            ..
        } => (!execute)
            .then(|| grant(OWN))
            .into_iter()
            .chain(shared.then(|| format!("REVOKE EXECUTE ON FUNCTION {SHARED} FROM {quoted}")))
            .chain((!mapped).then(|| map(role, service)))
            .collect(),
    }
}

/// What a role the platform never enqueues as is made to hold no more of:
/// the shared function, which enqueues for any service. It grants nothing.
pub fn narrowing(role: &str, enqueue: Enqueue) -> Vec<String> {
    match enqueue {
        Enqueue::Own { shared: true, .. } => vec![format!(
            "REVOKE EXECUTE ON FUNCTION {SHARED} FROM {}",
            quote_ident(role)
        )],
        Enqueue::Absent | Enqueue::Shared { .. } | Enqueue::Own { .. } => vec![],
    }
}

fn unmapping_others(role: &str, service: &str) -> String {
    format!(
        "DELETE FROM {SERVICE_ROLES} WHERE service = {} AND role <> {}",
        quote_literal(service),
        quote_literal(role)
    )
}

fn map(role: &str, service: &str) -> String {
    format!(
        "INSERT INTO {SERVICE_ROLES} (role, service) VALUES ({}, {})
         ON CONFLICT (role) DO UPDATE SET service = EXCLUDED.service",
        quote_literal(role),
        quote_literal(service)
    )
}

async fn mappings(client: &impl GenericClient) -> Result<bool, tokio_postgres::Error> {
    Ok(client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&SERVICE_ROLES])
        .await?
        .get(0))
}

/// Run once `role` is the service's binding, never before: until then the
/// role bound before keeps its mapping, and its enqueues go on working.
pub async fn unmap_others(
    client: &impl GenericClient,
    role: &str,
    service: &str,
) -> Result<(), tokio_postgres::Error> {
    match mappings(client).await? {
        true => client.batch_execute(&unmapping_others(role, service)).await,
        false => Ok(()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Held {
    pub role: bool,
    pub schema: bool,
    pub own: bool,
    pub shared: bool,
    pub mappings: bool,
}

/// What takes back from `role` all the platform grants it of the outbox,
/// and its mapping to `service`. The role itself is left as it is.
pub fn relinquishing(role: &str, service: &str, held: Held) -> Vec<String> {
    let quoted = quote_ident(role);
    let revoke = |function: &str| format!("REVOKE EXECUTE ON FUNCTION {function} FROM {quoted}");
    [
        (held.role && held.schema)
            .then(|| format!("REVOKE USAGE ON SCHEMA {CUFFLINK_SCHEMA} FROM {quoted}")),
        (held.role && held.own).then(|| revoke(OWN)),
        (held.role && held.shared).then(|| revoke(SHARED)),
        held.mappings.then(|| {
            format!(
                "DELETE FROM {SERVICE_ROLES} WHERE role = {} AND service = {}",
                quote_literal(role),
                quote_literal(service)
            )
        }),
    ]
    .into_iter()
    .flatten()
    .collect()
}

pub async fn relinquish(
    client: &impl GenericClient,
    role: &str,
    service: &str,
) -> Result<(), tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1),
                    to_regnamespace($2) IS NOT NULL,
                    to_regprocedure($3) IS NOT NULL,
                    to_regprocedure($4) IS NOT NULL,
                    to_regclass($5) IS NOT NULL",
            &[&role, &CUFFLINK_SCHEMA, &OWN, &SHARED, &SERVICE_ROLES],
        )
        .await?;
    let held = Held {
        role: row.get(0),
        schema: row.get(1),
        own: row.get(2),
        shared: row.get(3),
        mappings: row.get(4),
    };
    client
        .batch_execute(&relinquishing(role, service, held).join(";\n"))
        .await
}

pub async fn unmap(client: &impl GenericClient, role: &str) -> Result<(), tokio_postgres::Error> {
    match mappings(client).await? {
        true => client
            .execute(
                &format!("DELETE FROM {SERVICE_ROLES} WHERE role = $1::name"),
                &[&role],
            )
            .await
            .map(drop),
        false => Ok(()),
    }
}

pub fn checks(role: &str, service: &str, enqueue: Enqueue) -> Vec<Check> {
    let quoted = quote_ident(role);
    let execute = |function: &'static str, execute: bool| {
        Check::of("outbox_enqueue", execute, || {
            (
                format!("the role cannot execute {function}"),
                Some(format!("GRANT EXECUTE ON FUNCTION {function} TO {quoted};")),
            )
        })
    };
    match enqueue {
        Enqueue::Absent => vec![],
        Enqueue::Shared { execute: granted } => vec![execute(SHARED, granted)],
        Enqueue::Own {
            execute: granted,
            shared,
            mapped,
            ..
        } => vec![
            execute(OWN, granted),
            Check::of("outbox_confined", !shared, || {
                (
                    format!("the role may enqueue for any service through {SHARED}"),
                    Some(format!(
                        "REVOKE EXECUTE ON FUNCTION {SHARED} FROM {quoted};"
                    )),
                )
            }),
            Check::of("outbox_service", mapped, || {
                (
                    format!("the role is not the one mapped to service {service}"),
                    Some(format!(
                        "{}; {};",
                        map(role, service),
                        unmapping_others(role, service)
                    )),
                )
            }),
        ],
    }
}

/// What a role of a service the platform never enqueues for holds of the
/// outbox beyond its need, with what takes each back. It narrows nothing
/// itself: the operator decides.
pub fn excess(role: &str, usage: bool, enqueue: Enqueue) -> Vec<Check> {
    let quoted = quote_ident(role);
    let revoke = |function: &str| format!("REVOKE EXECUTE ON FUNCTION {function} FROM {quoted};");
    let executes = |function: &'static str, granted: bool, unmap: bool| {
        Check::excess("outbox_enqueue_unneeded", granted, || {
            (
                format!(
                    "the role may execute {function}, and the service serves no table and \
                     publishes nothing; a guest that enqueues from its SQL declares \
                     `publishes` rather than have this taken back"
                ),
                std::iter::once(revoke(function))
                    .chain(unmap.then(|| {
                        format!(
                            "DELETE FROM {SERVICE_ROLES} WHERE role = {};",
                            quote_literal(role)
                        )
                    }))
                    .collect::<Vec<_>>()
                    .join(" "),
            )
        })
    };
    std::iter::once(Check::excess("cufflink_schema_unneeded", usage, || {
        (
            format!(
                "the role has USAGE on schema {CUFFLINK_SCHEMA}, and the service serves no \
                 table and publishes nothing; a guest that enqueues from its SQL declares \
                 `publishes` rather than have this taken back"
            ),
            format!("REVOKE USAGE ON SCHEMA {CUFFLINK_SCHEMA} FROM {quoted};"),
        )
    }))
    .chain(match enqueue {
        Enqueue::Absent => vec![],
        Enqueue::Shared { execute } => vec![executes(SHARED, execute, false)],
        Enqueue::Own {
            execute,
            shared,
            mapped,
            ..
        } => vec![
            executes(OWN, execute, mapped),
            Check::of("outbox_confined", !shared, || {
                (
                    format!("the role may enqueue for any service through {SHARED}"),
                    Some(revoke(SHARED)),
                )
            }),
        ],
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROLE: &str = "svc_default_notes_0011223344556677";

    #[test]
    fn before_the_own_function_the_shared_one_is_granted_once() {
        assert_eq!(
            statements(ROLE, "loan-service", Enqueue::Shared { execute: false }),
            [format!("GRANT EXECUTE ON FUNCTION {SHARED} TO \"{ROLE}\"")]
        );
        assert!(statements(ROLE, "loan-service", Enqueue::Shared { execute: true }).is_empty());
        assert!(statements(ROLE, "loan-service", Enqueue::Absent).is_empty());
    }

    #[test]
    fn with_the_own_function_the_role_enqueues_for_its_service_alone() {
        let fresh = Enqueue::Own {
            execute: false,
            shared: true,
            mapped: false,
        };
        assert_eq!(
            statements(ROLE, "note's-service", fresh),
            [
                format!("GRANT EXECUTE ON FUNCTION {OWN} TO \"{ROLE}\""),
                format!("REVOKE EXECUTE ON FUNCTION {SHARED} FROM \"{ROLE}\""),
                format!(
                    "INSERT INTO _cufflink.service_roles (role, service) VALUES ('{ROLE}', 'note''s-service')
         ON CONFLICT (role) DO UPDATE SET service = EXCLUDED.service"
                ),
            ]
        );
        assert!(
            statements(ROLE, "loan-service", fresh)
                .iter()
                .all(|statement| !statement.contains("DELETE")),
            "no mapping of another role is taken before the role is bound"
        );
        let settled = Enqueue::Own {
            execute: true,
            shared: false,
            mapped: true,
        };
        assert!(statements(ROLE, "loan-service", settled).is_empty());
        assert!(checks(ROLE, "loan-service", settled).iter().all(|c| c.ok));
    }

    #[test]
    fn each_gap_is_reported_with_what_closes_it() {
        let checks = checks(
            ROLE,
            "loan-service",
            Enqueue::Own {
                execute: false,
                shared: true,
                mapped: false,
            },
        );
        assert_eq!(
            checks.iter().map(|c| c.check.as_str()).collect::<Vec<_>>(),
            ["outbox_enqueue", "outbox_confined", "outbox_service"]
        );
        assert!(checks.iter().all(|c| !c.ok && c.remedy.is_some()));
        assert_eq!(
            checks[1].remedy.as_deref(),
            Some(format!("REVOKE EXECUTE ON FUNCTION {SHARED} FROM \"{ROLE}\";").as_str())
        );
        assert!(
            checks[2]
                .remedy
                .as_deref()
                .is_some_and(|remedy| remedy.contains(&format!(
                "DELETE FROM {SERVICE_ROLES} WHERE service = 'loan-service' AND role <> '{ROLE}'"
            ))),
            "the remedy maps the service to the role alone: {:?}",
            checks[2].remedy
        );
    }

    #[test]
    fn the_platform_enqueues_in_the_session_of_a_service_with_tables_or_events() {
        let manifest = |extra: serde_json::Value| -> ServiceManifest {
            let mut manifest = serde_json::json!({"name": "loan-service", "tables": []});
            manifest
                .as_object_mut()
                .expect("object")
                .extend(extra.as_object().cloned().unwrap_or_default());
            serde_json::from_value(manifest).expect("manifest")
        };
        assert!(!in_session(&manifest(serde_json::json!({}))));
        assert!(!in_session(&manifest(serde_json::json!({"publishes": []}))));
        assert!(in_session(&manifest(
            serde_json::json!({"tables": [{"name": "loans", "columns": []}]})
        )));
        assert!(in_session(&manifest(
            serde_json::json!({"publishes": ["loan.created"]})
        )));
    }

    #[test]
    fn a_role_holding_nothing_of_the_outbox_is_what_a_service_without_it_needs() {
        let bare = Enqueue::Own {
            execute: false,
            shared: false,
            mapped: false,
        };
        let checks = excess(ROLE, false, bare);
        assert_eq!(
            checks.iter().map(|c| c.check.as_str()).collect::<Vec<_>>(),
            [
                "cufflink_schema_unneeded",
                "outbox_enqueue_unneeded",
                "outbox_confined"
            ]
        );
        assert!(checks
            .iter()
            .all(|c| c.ok && !c.excess && c.remedy.is_none()));
        assert_eq!(
            excess(ROLE, false, Enqueue::Absent)
                .iter()
                .map(|c| c.check.as_str())
                .collect::<Vec<_>>(),
            ["cufflink_schema_unneeded"]
        );
    }

    #[test]
    fn what_a_role_holds_beyond_its_need_is_reported_and_fails_nothing() {
        let granted = Enqueue::Own {
            execute: true,
            shared: false,
            mapped: true,
        };
        let checks = excess(ROLE, true, granted);
        assert!(checks.iter().all(|c| c.ok));
        assert_eq!(
            checks
                .iter()
                .filter(|c| c.excess)
                .map(|c| c.remedy.as_deref().unwrap_or_default())
                .collect::<Vec<_>>(),
            [
                format!("REVOKE USAGE ON SCHEMA _cufflink FROM \"{ROLE}\";"),
                format!(
                    "REVOKE EXECUTE ON FUNCTION {OWN} FROM \"{ROLE}\"; \
                     DELETE FROM {SERVICE_ROLES} WHERE role = '{ROLE}';"
                ),
            ]
        );
        assert_eq!(
            excess(ROLE, true, Enqueue::Shared { execute: true })[1].remedy,
            Some(format!(
                "REVOKE EXECUTE ON FUNCTION {SHARED} FROM \"{ROLE}\";"
            ))
        );
        let shared = excess(
            ROLE,
            false,
            Enqueue::Own {
                execute: false,
                shared: true,
                mapped: false,
            },
        );
        assert!(
            !shared[2].ok && !shared[2].excess,
            "enqueueing for any service stays a failure: {shared:?}"
        );
    }

    #[test]
    fn a_role_that_needs_no_outbox_is_narrowed_and_granted_nothing() {
        let held = Enqueue::Own {
            execute: true,
            shared: true,
            mapped: false,
        };
        assert_eq!(
            narrowing(ROLE, held),
            [format!(
                "REVOKE EXECUTE ON FUNCTION {SHARED} FROM \"{ROLE}\""
            )]
        );
        let own = Enqueue::Own {
            execute: true,
            shared: false,
            mapped: true,
        };
        assert!(
            narrowing(ROLE, own).is_empty(),
            "what the role holds of its own is reported, not taken back"
        );
        assert!(narrowing(ROLE, Enqueue::Shared { execute: true }).is_empty());
        assert!(narrowing(ROLE, Enqueue::Absent).is_empty());
    }

    #[test]
    fn another_roles_mapping_is_all_unmapping_others_takes() {
        assert_eq!(
            unmapping_others(ROLE, "note's-service"),
            format!(
                "DELETE FROM _cufflink.service_roles WHERE service = 'note''s-service' AND role <> '{ROLE}'"
            )
        );
    }

    #[test]
    fn relinquishing_takes_back_what_the_platform_grants_and_the_mapping_alone() {
        let all = Held {
            role: true,
            schema: true,
            own: true,
            shared: true,
            mappings: true,
        };
        assert_eq!(
            relinquishing(ROLE, "note's-service", all),
            [
                format!("REVOKE USAGE ON SCHEMA _cufflink FROM \"{ROLE}\""),
                format!("REVOKE EXECUTE ON FUNCTION {OWN} FROM \"{ROLE}\""),
                format!("REVOKE EXECUTE ON FUNCTION {SHARED} FROM \"{ROLE}\""),
                format!(
                    "DELETE FROM _cufflink.service_roles WHERE role = '{ROLE}' AND service = 'note''s-service'"
                ),
            ]
        );
        assert_eq!(
            relinquishing(ROLE, "loan-service", Held { role: false, ..all }),
            [format!(
                "DELETE FROM _cufflink.service_roles WHERE role = '{ROLE}' AND service = 'loan-service'"
            )],
            "a role that is gone holds no grant"
        );
        assert!(relinquishing(
            ROLE,
            "loan-service",
            Held {
                role: true,
                schema: false,
                own: false,
                shared: false,
                mappings: false,
            }
        )
        .is_empty());
    }
}
