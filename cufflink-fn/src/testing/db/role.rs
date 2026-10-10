//! The role a service's statements run as under `db_roles`, made in a
//! test database as the platform makes it: a login that is no superuser,
//! owns the service's schema and what is in it, and reaches nothing else.

use super::super::error::TestHostError;
use cufflink_pgcodec::roles::OWNED_BY_THE_USER;
use cufflink_pgcodec::session::quote_ident;
use cufflink_types::manifest::validate::{folded, folded_ident};
use postgres::Client;

/// A role the test database made for a service, and how it logs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRole {
    name: String,
    password: String,
}

impl ServiceRole {
    /// A role of its own, named once in the whole server, since roles are
    /// the server's and not a database's.
    pub(super) fn fresh() -> Self {
        Self {
            name: format!("svc_{}", uuid::Uuid::new_v4().simple()),
            password: uuid::Uuid::new_v4().simple().to_string(),
        }
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
