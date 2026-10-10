//! A Postgres database of a test's own, and the host that runs a handler's
//! SQL in it as the platform would.
//!
//! Both talk to the server through the synchronous client, which drives a
//! runtime of its own. Such a runtime cannot be started, used or dropped
//! inside another, so every use of a connection runs [`apart`]: a test of
//! a handler may be a plain `#[test]` or a `#[tokio::test]` of either
//! flavour.

mod embedded;
mod host;
mod migrate;
mod role;

pub use host::{PgHost, PgOptions, PgRole};
pub use role::ServiceRole;

use super::error::TestHostError;
use cufflink_migrate::{scan_dir, Plan};
use cufflink_types::manifest::validate::folded;
use cufflink_types::MigrationHistory;
use postgres::{Client, Config, NoTls};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Where a server is when the environment names one.
const DATABASE_URL: &str = "CUFFLINK_TEST_DATABASE_URL";

/// A database created empty for one test and dropped with the value, so a
/// failing test leaves nothing behind: the roles made for its services go
/// with it.
pub struct TestDb {
    name: String,
    config: Config,
    admin: Config,
    /// The role of each service, by its folded schema.
    roles: Mutex<BTreeMap<String, ServiceRole>>,
}

/// Run `work` on a thread of its own and wait for it, so that what it does
/// with the synchronous client (connect, run statements, drop the
/// connection) never happens on a thread an async runtime drives.
pub(super) fn apart<T: Send>(work: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| scope.spawn(work).join())
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

/// A lock whose holder panicked is still what the value is: every value
/// behind one is left whole between statements.
pub(super) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn parsed(url: &str, source: &str) -> Result<Config, TestHostError> {
    url.parse()
        .map_err(|e| TestHostError::Unavailable(format!("{source} is not a Postgres URL: {e}")))
}

impl TestDb {
    /// A database on the server of `CUFFLINK_TEST_DATABASE_URL`; `None`
    /// when the variable is not set.
    pub fn from_env() -> Result<Option<TestDb>, TestHostError> {
        std::env::var(DATABASE_URL)
            .ok()
            .filter(|url| !url.trim().is_empty())
            .map(|url| parsed(&url, DATABASE_URL).and_then(Self::create))
            .transpose()
    }

    /// A database on the process's own embedded server, which the first
    /// call starts and the process's exit stops. Postgres is installed
    /// once, under `CUFFLINK_TEST_POSTGRES_DIR` when that is set.
    pub fn embedded() -> Result<TestDb, TestHostError> {
        let url = embedded::admin_url()?;
        parsed(&url, "the embedded server's URL").and_then(Self::create)
    }

    /// A database on the server the environment names, else on the
    /// embedded one.
    pub fn start() -> Result<TestDb, TestHostError> {
        Self::from_env()?.map_or_else(Self::embedded, Ok)
    }

    fn create(admin: Config) -> Result<TestDb, TestHostError> {
        let name = format!("test_{}", uuid::Uuid::new_v4().simple());
        apart(|| {
            admin
                .connect(NoTls)?
                .batch_execute(&format!("CREATE DATABASE \"{name}\""))
        })?;
        let mut config = admin.clone();
        config.dbname(&name);
        Ok(TestDb {
            name,
            config,
            admin,
            roles: Mutex::default(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// How the database is reached.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// `work` on a connection of its own, with the server's defaults: no
    /// schema of a service is on its search path.
    fn connected<T>(
        &self,
        work: impl FnOnce(&mut Client) -> Result<T, TestHostError> + Send,
    ) -> Result<T, TestHostError>
    where
        T: Send,
    {
        apart(|| work(&mut self.config.connect(NoTls)?))
    }

    /// Run `sql`, any number of statements, to seed what a test needs.
    pub fn execute(&self, sql: &str) -> Result<(), TestHostError> {
        self.connected(|client| Ok(client.batch_execute(sql)?))
    }

    /// [`execute`](Self::execute) the statements of a file.
    pub fn execute_file(&self, path: &Path) -> Result<(), TestHostError> {
        let sql = std::fs::read_to_string(path).map_err(|source| TestHostError::File {
            path: path.to_path_buf(),
            source,
        })?;
        self.execute(&sql)
    }

    /// The rows of one statement, in the legacy encoding of `db::query`,
    /// for a test to assert on.
    pub fn query(&self, sql: &str) -> Result<Vec<Value>, TestHostError> {
        self.connected(|client| {
            Ok(cufflink_pgcodec::render::legacy_rows(
                &client.query(sql, &[])?,
            ))
        })
    }

    /// The role the platform makes under `db_roles` for the service whose
    /// schema is `schema`: made the first time, and given the schema (made
    /// when it is not there) and everything in it every time, as each
    /// deploy gives it. It is no superuser and reaches nothing else, so a
    /// statement it may not run fails in a test as it fails on the
    /// platform.
    pub fn service_role(&self, schema: &str) -> Result<ServiceRole, TestHostError> {
        let key = folded(schema);
        let roles = &mut *locked(&self.roles);
        self.connected(|client| {
            let role = match roles.get(&key) {
                Some(role) => role.clone(),
                None => {
                    let role = ServiceRole::fresh();
                    role.create(client, &self.name)?;
                    roles.insert(key, role.clone());
                    role
                }
            };
            role.own(client, schema)?;
            Ok(role)
        })
    }

    /// Let the service role `role` run read-only queries as `read_only`, as
    /// the platform lets a service's binding name its read-only role.
    pub fn grant_read_only(
        &self,
        role: &ServiceRole,
        read_only: &str,
    ) -> Result<(), TestHostError> {
        self.connected(|client| role.may_set(client, read_only))
    }

    /// Bring `schema` to what the migrations in `dir` describe, as a deploy
    /// of a service declaring `migrations: dir` and `history` does under
    /// `db_roles`: the migrations run as the [`service_role`](Self::service_role),
    /// which owns the schema and what they create, and the history is
    /// written as the server's user. The plan is what the history left to
    /// apply, and it was applied unless `history` is `verify_only`.
    pub fn migrate(
        &self,
        schema: &str,
        dir: &Path,
        history: &MigrationHistory,
    ) -> Result<Plan, TestHostError> {
        let migrations = scan_dir(dir).map_err(|e| TestHostError::Migration(e.to_string()))?;
        let role = self.service_role(schema)?;
        self.connected(|client| {
            migrate::run(
                client,
                &migrate::Run {
                    schema,
                    declared: history,
                    migrations: &migrations,
                    role: role.name(),
                },
            )
        })
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let (admin, name) = (&self.admin, &self.name);
        let roles = std::mem::take(self.roles.get_mut().unwrap_or_else(PoisonError::into_inner));
        let _ = apart(|| {
            admin.connect(NoTls).and_then(|mut client| {
                client
                    .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))?;
                roles
                    .values()
                    .try_for_each(|role| client.batch_execute(&role.drop_statement()))
            })
        });
    }
}
