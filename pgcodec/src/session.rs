//! The settings statements run under on a session: the service's schema
//! first on the search path, the statement's time bound, and the role it
//! runs as. Whatever holds the session sends these texts, so the
//! platform's sessions, its migration executor and the native test host
//! set up a session alike.

use cufflink_types::manifest::validate::folded_ident;
use std::time::Duration;

/// Where a session's settings apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// `SET LOCAL`: until the transaction ends.
    Transaction,
    /// `SET`: until the session's reset runs.
    Session,
}

impl Scope {
    pub fn set(self) -> &'static str {
        match self {
            Scope::Transaction => "SET LOCAL",
            Scope::Session => "SET",
        }
    }
}

/// `name` as a quoted SQL identifier, exactly as written.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `SET LOCAL ROLE` or `SET ROLE`, as `scope` says.
pub fn set_role(scope: Scope, role: &str) -> String {
    format!("{} ROLE {}", scope.set(), quote_ident(role))
}

/// What a guest's statements run under: the schema is written as every
/// statement writes it ([`folded_ident`]), so the path a guest's SQL
/// resolves in, the schema a deploy created and the one a migration ran in
/// are the same schema. `role` is the one a session imposes on its
/// connection, where it does.
pub fn prelude(
    schema: &str,
    role: Option<&str>,
    scope: Scope,
    statement_timeout: Duration,
) -> String {
    let set = scope.set();
    let settings = format!(
        "{set} search_path TO {}, public; {set} statement_timeout = {}",
        folded_ident(schema),
        statement_timeout.as_millis()
    );
    match role {
        Some(role) => format!("{settings}; {}", set_role(scope, role)),
        None => settings,
    }
}

/// Undoes a [`Scope::Session`] [`prelude`].
pub fn reset(role_imposed: bool) -> &'static str {
    if role_imposed {
        "RESET search_path; RESET statement_timeout; RESET ROLE"
    } else {
        "RESET search_path; RESET statement_timeout"
    }
}

/// What a service's SQL migrations run under: its schema, the statement
/// and lock time bounds, and the role the migrations run as where the run
/// imposes one.
pub fn migration_prelude(
    schema: &str,
    role: Option<&str>,
    scope: Scope,
    statement_timeout: Duration,
    lock_timeout: Duration,
) -> String {
    let set = scope.set();
    let role = role
        .map(|role| format!("{};", set_role(scope, role)))
        .unwrap_or_default();
    format!(
        "{set} search_path TO {}, public; {set} statement_timeout = {}; \
         {set} lock_timeout = {}; {role}",
        folded_ident(schema),
        statement_timeout.as_millis(),
        lock_timeout.as_millis(),
    )
}

/// Undoes a [`Scope::Session`] [`migration_prelude`].
pub const MIGRATION_RESET: &str =
    "RESET ROLE; RESET search_path; RESET statement_timeout; RESET lock_timeout";

/// Ends the role a migration ran as within its transaction, so the
/// history row is written as the session's own role.
pub const ROLE_NONE: &str = "SET LOCAL ROLE NONE";

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_millis(29_500);

    #[test]
    fn a_guest_prelude_puts_the_schema_first_and_bounds_the_statement() {
        assert_eq!(
            prelude("loan_service", None, Scope::Transaction, TIMEOUT),
            "SET LOCAL search_path TO \"loan_service\", public; SET LOCAL statement_timeout = 29500"
        );
        assert_eq!(
            prelude("loan_service", None, Scope::Session, TIMEOUT),
            "SET search_path TO \"loan_service\", public; SET statement_timeout = 29500"
        );
        for (schema, path) in [("Order_Desk_2", "\"order_desk_2\""), ("order", "\"order\"")] {
            assert_eq!(
                prelude(schema, None, Scope::Transaction, TIMEOUT),
                format!(
                    "SET LOCAL search_path TO {path}, public; SET LOCAL statement_timeout = 29500"
                ),
            );
        }
        assert_eq!(reset(false), "RESET search_path; RESET statement_timeout");
    }

    #[test]
    fn a_role_is_set_last_and_reset_with_the_rest() {
        let role = Some("svc_acme_loan_service");
        assert_eq!(
            prelude("loan_service", role, Scope::Transaction, TIMEOUT),
            "SET LOCAL search_path TO \"loan_service\", public; \
             SET LOCAL statement_timeout = 29500; \
             SET LOCAL ROLE \"svc_acme_loan_service\""
        );
        assert_eq!(
            prelude("loan_service", role, Scope::Session, TIMEOUT),
            "SET search_path TO \"loan_service\", public; SET statement_timeout = 29500; \
             SET ROLE \"svc_acme_loan_service\""
        );
        assert_eq!(
            set_role(Scope::Session, "Odd\"Role"),
            "SET ROLE \"Odd\"\"Role\""
        );
        assert_eq!(
            reset(true),
            "RESET search_path; RESET statement_timeout; RESET ROLE"
        );
    }

    #[test]
    fn a_migration_prelude_bounds_its_locks_and_names_its_role() {
        let (statement, lock) = (Duration::from_secs(600), Duration::from_secs(10));
        assert_eq!(
            migration_prelude("loan_service", None, Scope::Transaction, statement, lock),
            "SET LOCAL search_path TO \"loan_service\", public; SET LOCAL statement_timeout = \
             600000; SET LOCAL lock_timeout = 10000; "
        );
        assert_eq!(
            migration_prelude(
                "Notes",
                Some("svc_t_notes"),
                Scope::Session,
                statement,
                lock
            ),
            "SET search_path TO \"notes\", public; SET statement_timeout = 600000; \
             SET lock_timeout = 10000; SET ROLE \"svc_t_notes\";"
        );
    }

    #[test]
    fn identifiers_are_quoted_as_written() {
        assert_eq!(quote_ident("svc_a"), "\"svc_a\"");
        assert_eq!(quote_ident("Odd\"Name"), "\"Odd\"\"Name\"");
        assert_eq!(quote_ident(""), "\"\"");
    }
}
