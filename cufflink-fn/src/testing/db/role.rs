//! The role a service's statements run as under `db_roles`: made in a
//! test database as the platform makes it (a login that is no superuser,
//! owns the service's schema and what is in it, and reaches nothing else),
//! or one the test set up itself, as an operator binds a role of their own.

use super::super::error::TestHostError;
use cufflink_pgcodec::roles::OWNED_BY_THE_USER;
use cufflink_pgcodec::session::quote_ident;
use cufflink_types::manifest::validate::{folded, folded_ident};
use postgres::Client;
use std::fmt;

/// A role a service's statements run as, and how it logs in.
#[derive(Clone, PartialEq, Eq)]
pub struct ServiceRole {
    name: String,
    password: String,
    origin: Origin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// Made by [`TestDb::service_role`](super::TestDb::service_role).
    Made,
    /// Set up by the test, which owns everything about it.
    Existing,
}

impl ServiceRole {
    /// A role of its own, named once in the whole server, since roles are
    /// the server's and not a database's.
    pub(super) fn fresh() -> Self {
        Self {
            name: format!("svc_{}", uuid::Uuid::new_v4().simple()),
            password: uuid::Uuid::new_v4().simple().to_string(),
            origin: Origin::Made,
        }
    }

    /// The role `name`, which the test created and logs in with `password`,
    /// as a service bound with `cufflink services db-role set --role name
    /// --password-stdin` logs in under `db_roles: enforce`. Nothing is done
    /// to it: it is not created, given a schema or granted a read-only
    /// role, and it outlives the test database. Its own settings
    /// (`ALTER ROLE … SET`) apply wherever the session prelude does not set
    /// them.
    pub fn existing(name: &str, password: &str) -> Self {
        Self {
            name: name.to_string(),
            password: password.to_string(),
            origin: Origin::Existing,
        }
    }

    /// Whether the test database made the role, and so may grant it what
    /// the platform grants the roles it makes.
    pub(in crate::testing) fn made(&self) -> bool {
        self.origin == Origin::Made
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    /// Create the role, as the platform does: it logs in, is no superuser
    /// and inherits nothing, and the server's user it is created by may
    /// set it and hand it what it owns.
    pub(super) fn create(&self, admin: &mut Client, database: &str) -> Result<(), TestHostError> {
        let role = quote_ident(&self.name);
        admin
            .batch_execute(&format!(
                "CREATE ROLE {role} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE \
                 NOREPLICATION NOBYPASSRLS PASSWORD '{}'; \
                 GRANT {role} TO CURRENT_USER; \
                 GRANT CONNECT ON DATABASE {} TO {role}",
                self.password,
                quote_ident(database)
            ))
            .map_err(|e| {
                TestHostError::Role(format!(
                    "the test server's user could not create the service's role; it needs \
                     CREATEROLE: {}",
                    cufflink_pgcodec::server_message(&e)
                ))
            })
    }

    /// Give the role `schema` and what the server's user owns in it,
    /// creating the schema when it is not there, and nothing of `public`
    /// but its use.
    pub(super) fn own(&self, client: &mut Client, schema: &str) -> Result<(), TestHostError> {
        let role = quote_ident(&self.name);
        let path = folded_ident(schema);
        client.batch_execute(&format!(
            "GRANT USAGE ON SCHEMA public TO {role}; \
             CREATE SCHEMA IF NOT EXISTS {path} AUTHORIZATION {role}; \
             ALTER SCHEMA {path} OWNER TO {role}"
        ))?;
        let public_creates: bool = client
            .query_one(
                "SELECT has_schema_privilege('public', 'public', 'CREATE')",
                &[],
            )?
            .try_get(0)?;
        if public_creates {
            client.batch_execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")?;
        }
        let owned = client
            .query(OWNED_BY_THE_USER, &[&folded(schema)])?
            .iter()
            .map(|row| Ok((row.try_get::<_, String>(0)?, row.try_get::<_, String>(1)?)))
            .collect::<Result<Vec<_>, postgres::Error>>()?;
        owned.iter().try_for_each(|(kind, name)| {
            client
                .batch_execute(&format!("ALTER {kind} {name} OWNER TO {role}"))
                .map_err(TestHostError::from)
        })
    }

    /// Let the role set `read_only` for its read-only queries, as the
    /// platform does for the read-only role of a service's binding.
    pub(super) fn may_set(
        &self,
        client: &mut Client,
        read_only: &str,
    ) -> Result<(), TestHostError> {
        client
            .batch_execute(&format!(
                "GRANT {} TO {}",
                quote_ident(read_only),
                quote_ident(&self.name)
            ))
            .map_err(TestHostError::from)
    }

    /// Drop the role once its database is gone.
    pub(super) fn drop_statement(&self) -> String {
        format!("DROP ROLE IF EXISTS {}", quote_ident(&self.name))
    }
}

impl fmt::Debug for ServiceRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceRole")
            .field("name", &self.name)
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}
