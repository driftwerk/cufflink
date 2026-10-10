use crate::db_roles::verify::Check;
use cufflink_db::{quote_ident, quote_literal};
use deadpool_postgres::GenericClient;

pub const CUFFLINK_SCHEMA: &str = "_cufflink";
pub const SHARED: &str =
    "_cufflink.outbox_enqueue_many(TEXT[], TEXT[], TEXT[], BYTEA[], JSONB[], UUID[])";
pub const OWN: &str = "_cufflink.outbox_enqueue_own(TEXT[], TEXT[], BYTEA[], JSONB[], UUID[])";
pub const SERVICE_ROLES: &str = "_cufflink.service_roles";

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
        (true, _) => Enqueue::Own {
            execute: own,
            shared,
            mapped: client
                .query_one(
                    &format!(
                        "SELECT COALESCE(bool_and(role = $1::name), false)
                         FROM {SERVICE_ROLES} WHERE service = $2"
                    ),
                    &[&role, &service],
                )
                .await?
                .get(0),
        },
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
        } => (!execute)
            .then(|| grant(OWN))
            .into_iter()
            .chain(shared.then(|| format!("REVOKE EXECUTE ON FUNCTION {SHARED} FROM {quoted}")))
            .chain((!mapped).then(|| map(role, service)).into_iter().flatten())
            .collect(),
    }
}

fn map(role: &str, service: &str) -> [String; 2] {
    let (role, service) = (quote_literal(role), quote_literal(service));
    [
        format!(
            "INSERT INTO {SERVICE_ROLES} (role, service) VALUES ({role}, {service})
             ON CONFLICT (role) DO UPDATE SET service = EXCLUDED.service"
        ),
        format!("DELETE FROM {SERVICE_ROLES} WHERE service = {service} AND role <> {role}"),
    ]
}

pub async fn unmap(client: &impl GenericClient, role: &str) -> Result<(), tokio_postgres::Error> {
    let mapped: bool = client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&SERVICE_ROLES])
        .await?
        .get(0);
    match mapped {
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
                    Some(format!("{};", map(role, service).join("; "))),
                )
            }),
        ],
    }
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
                format!(
                    "DELETE FROM _cufflink.service_roles WHERE service = 'note''s-service' AND role <> '{ROLE}'"
                ),
            ]
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
    }
}
