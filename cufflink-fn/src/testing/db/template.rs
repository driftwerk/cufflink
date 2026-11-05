//! A database built once and cloned for each test with `CREATE DATABASE …
//! TEMPLATE`, so a schema that takes seconds to build is ready in each
//! test's own database in milliseconds.

use super::super::error::TestHostError;
use super::{apart, execute, execute_file, locked, query, reaching, server};
use cufflink_pgcodec::session::quote_ident;
use postgres::{Client, Config, NoTls};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};

/// What a template's database is named after its name.
const PREFIX: &str = "tmpl_";
/// What a template is built under before it is renamed; no name a template
/// may have begins so.
const BUILDING: &str = "tmpl__";
/// The longest name a template may have: a database's name is at most 63
/// bytes.
const MAX_NAME: usize = 63 - PREFIX.len();

/// Each template this process asked for, by name; a slot is held while its
/// template is built, so it is built once however many ask at once.
static TEMPLATES: LazyLock<Mutex<BTreeMap<String, Slot>>> = LazyLock::new(Mutex::default);

type Slot = Arc<Mutex<Option<TestTemplate>>>;

/// How Postgres copies a template into a new database (`STRATEGY`, from
/// Postgres 15); the server's default unless set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneStrategy {
    /// Block by block through the write-ahead log, the default.
    WalLog,
    /// The files as they are, after a checkpoint: usually faster for a
    /// small schema.
    FileCopy,
}

impl CloneStrategy {
    fn keyword(self) -> &'static str {
        match self {
            CloneStrategy::WalLog => "WAL_LOG",
            CloneStrategy::FileCopy => "FILE_COPY",
        }
    }
}

/// A template database on the server [`TestDb::start`](super::TestDb::start)
/// uses, from which [`TestDb::from_template`](super::TestDb::from_template)
/// clones each test's database.
#[derive(Debug, Clone)]
pub struct TestTemplate {
    database: String,
    server: Config,
    strategy: Option<CloneStrategy>,
}

/// The database a template's build runs in, as the server's user: what it
/// makes is owned by that user, so a clone's
/// [`service_role`](super::TestDb::service_role) is given it as on any
/// other database.
pub struct TemplateDb {
    name: String,
    config: Config,
}

impl TemplateDb {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// How the database is reached, for a tool that builds the schema
    /// itself. Every connection it opens must be closed before the build
    /// returns.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Run `sql`, any number of statements.
    pub fn execute(&self, sql: &str) -> Result<(), TestHostError> {
        execute(&self.config, sql)
    }

    /// [`execute`](Self::execute) the statements of a file.
    pub fn execute_file(&self, path: &Path) -> Result<(), TestHostError> {
        execute_file(&self.config, path)
    }

    /// The rows of one statement, in the legacy encoding of `db::query`.
    pub fn query(&self, sql: &str) -> Result<Vec<Value>, TestHostError> {
        query(&self.config, sql)
    }
}

fn refused(reason: impl Into<String>) -> TestHostError {
    TestHostError::Template(reason.into())
}

fn valid(name: &str) -> Result<(), TestHostError> {
    let well_formed = (1..=MAX_NAME).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !name.starts_with('_');
    match well_formed {
        true => Ok(()),
        false => Err(refused(format!(
            "template name '{name:.64}' is not 1 to {MAX_NAME} lowercase letters, digits and \
             underscores, beginning with a letter or digit"
        ))),
    }
}

/// The server-wide lock on building one template, held by a connection of
/// its own, which an ended process releases with it.
struct Lock {
    client: Option<Client>,
    key: String,
}

impl Lock {
    fn take(server: &Config, database: &str) -> Result<Self, TestHostError> {
        let key = format!("cufflink-fn template {database}");
        let mut client = apart(|| server.connect(NoTls))?;
        let held =
            apart(|| client.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&key]));
        let lock = Self {
            client: Some(client),
            key,
        };
        held?;
        Ok(lock)
    }

    fn client(&mut self) -> Result<&mut Client, TestHostError> {
        self.client
            .as_mut()
            .ok_or_else(|| refused("the template lock's connection is gone"))
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let (client, key) = (self.client.take(), &self.key);
        apart(move || {
            if let Some(mut client) = client {
                let _ =
                    client.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", &[key]);
            }
        });
    }
}

/// A database being built, dropped unless it became the template.
struct Building<'a> {
    server: &'a Config,
    name: String,
    kept: bool,
}

impl Drop for Building<'_> {
    fn drop(&mut self) {
        if self.kept {
            return;
        }
        let name = quote_ident(&self.name);
        let _ = apart(|| {
            self.server.connect(NoTls).and_then(|mut client| {
                let _ = client.batch_execute(&format!("ALTER DATABASE {name} IS_TEMPLATE false"));
                client.batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
            })
        });
    }
}

impl TestTemplate {
    /// The template `name`, built by `build` the first time it is asked
    /// for: in this process, and on the server. The build runs in a
    /// database of its own; once it returns, the database is closed to
    /// connections (`ALLOW_CONNECTIONS false`) and made a template
    /// (`IS_TEMPLATE true`) under the name `tmpl_<name>`.
    ///
    /// A server that outlives the process keeps the template, and a later
    /// process uses it without building it again, so a name that changes
    /// with what `build` makes (a hash of its migrations, say) never
    /// serves a stale one. Processes that ask at once wait for one build.
    /// A build that fails, or leaves a connection to its database open, is
    /// dropped, and the next call builds again.
    pub fn get_or_create(
        name: &str,
        build: impl FnOnce(&TemplateDb) -> Result<(), TestHostError>,
    ) -> Result<TestTemplate, TestHostError> {
        valid(name)?;
        let slot = Arc::clone(locked(&TEMPLATES).entry(name.to_string()).or_default());
        let mut slot = locked(&slot);
        if let Some(template) = slot.as_ref() {
            return Ok(template.clone());
        }
        let template = Self::on_server(server()?, name, build)?;
        *slot = Some(template.clone());
        Ok(template)
    }

    /// Clone with `strategy` rather than the server's default.
    pub fn strategy(self, strategy: CloneStrategy) -> Self {
        Self {
            strategy: Some(strategy),
            ..self
        }
    }

    /// The template's database.
    pub fn name(&self) -> &str {
        &self.database
    }

    pub(super) fn server(&self) -> &Config {
        &self.server
    }

    /// What follows `CREATE DATABASE "…"` to clone the template.
    pub(super) fn clause(&self) -> String {
        format!(
            " TEMPLATE {}{}",
            quote_ident(&self.database),
            self.strategy.map_or_else(String::new, |strategy| format!(
                " STRATEGY {}",
                strategy.keyword()
            ))
        )
    }

    fn on_server(
        server: Config,
        name: &str,
        build: impl FnOnce(&TemplateDb) -> Result<(), TestHostError>,
    ) -> Result<TestTemplate, TestHostError> {
        let database = format!("{PREFIX}{name}");
        let mut lock = Lock::take(&server, &database)?;
        let client = lock.client()?;
        match apart(|| standing(client, &database))? {
            Some(true) => Ok(()),
            Some(false) => Err(refused(format!(
                "database {database} exists and is not a template; drop it or name the \
                 template otherwise"
            ))),
            None => Self::build(&server, client, &database, build),
        }?;
        drop(lock);
        Ok(TestTemplate {
            database,
            server,
            strategy: None,
        })
    }

    fn build(
        server: &Config,
        client: &mut Client,
        database: &str,
        build: impl FnOnce(&TemplateDb) -> Result<(), TestHostError>,
    ) -> Result<(), TestHostError> {
        let mut building = Building {
            server,
            name: format!("{BUILDING}{}", uuid::Uuid::new_v4().simple()),
            kept: false,
        };
        let quoted = quote_ident(&building.name);
        apart(|| client.batch_execute(&format!("CREATE DATABASE {quoted}")))?;
        build(&TemplateDb {
            name: building.name.clone(),
            config: reaching(server, &building.name),
        })?;
        apart(|| seal(client, &building.name, database))?;
        building.kept = true;
        Ok(())
    }
}

/// Whether `database` exists, and if so whether it is a template.
fn standing(client: &mut Client, database: &str) -> Result<Option<bool>, TestHostError> {
    Ok(client
        .query_opt(
            "SELECT datistemplate FROM pg_database WHERE datname = $1",
            &[&database],
        )?
        .map(|row| row.try_get(0))
        .transpose()?)
}

/// Close `built` to connections, refuse it if one is still open, and make
/// it the template `database`.
fn seal(client: &mut Client, built: &str, database: &str) -> Result<(), TestHostError> {
    let quoted = quote_ident(built);
    client.batch_execute(&format!("ALTER DATABASE {quoted} ALLOW_CONNECTIONS false"))?;
    let open: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = $1",
            &[&built],
        )?
        .try_get(0)?;
    if open > 0 {
        return Err(refused(format!(
            "the build of template {database} left {open} connection(s) to its database open; \
             close every connection before the build returns"
        )));
    }
    client.batch_execute(&format!(
        "ALTER DATABASE {quoted} IS_TEMPLATE true; \
         ALTER DATABASE {quoted} RENAME TO {}",
        quote_ident(database)
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_template_is_named_so_its_database_fits() {
        for name in ["loans", "loan_service_v42", "0f3a9c", &"a".repeat(MAX_NAME)] {
            assert!(valid(name).is_ok(), "{name}");
        }
        for name in [
            "",
            "_loans",
            "Loans",
            "loan-service",
            "loan service",
            "loans;drop",
            &"a".repeat(MAX_NAME + 1),
        ] {
            assert!(
                matches!(valid(name), Err(TestHostError::Template(_))),
                "{name}"
            );
        }
    }

    #[test]
    fn a_clone_names_its_template_and_any_strategy() {
        let template = TestTemplate {
            database: "tmpl_loans".into(),
            server: Config::new(),
            strategy: None,
        };
        assert_eq!(template.clause(), " TEMPLATE \"tmpl_loans\"");
        assert_eq!(
            template.clone().strategy(CloneStrategy::FileCopy).clause(),
            " TEMPLATE \"tmpl_loans\" STRATEGY FILE_COPY"
        );
        assert_eq!(
            template.strategy(CloneStrategy::WalLog).clause(),
            " TEMPLATE \"tmpl_loans\" STRATEGY WAL_LOG"
        );
    }
}
