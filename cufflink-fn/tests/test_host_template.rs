#![cfg(feature = "test-host")]

mod support;

use cufflink_fn::testing::{CloneStrategy, TemplateDb, TestDb, TestHostError, TestTemplate};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use support::{database, loan_migrations};

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// What a test made on the server outside its databases, removed when it
/// ends: made before the clones, it drops after them.
struct Leftovers {
    admin: TestDb,
    templates: Mutex<Vec<String>>,
    roles: Vec<String>,
}

impl Leftovers {
    fn new(admin: TestDb) -> Self {
        Self {
            admin,
            templates: Mutex::default(),
            roles: Vec::new(),
        }
    }

    fn template(&self, name: &str) -> String {
        let database = format!("tmpl_{name}");
        self.templates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(database);
        name.to_string()
    }

    fn exists(&self, database: &str) -> bool {
        self.admin
            .query(&format!(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = '{database}') AS there"
            ))
            .expect("pg_database")
            == [json!({"there": true})]
    }
}

impl Drop for Leftovers {
    fn drop(&mut self) {
        let templates = std::mem::take(
            self.templates
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        templates.iter().for_each(|database| {
            let _ = self
                .admin
                .execute(&format!("ALTER DATABASE \"{database}\" IS_TEMPLATE false"));
            let _ = self.admin.execute(&format!(
                "DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"
            ));
        });
        self.roles.iter().for_each(|role| {
            let _ = self
                .admin
                .execute(&format!("DROP ROLE IF EXISTS \"{role}\""));
        });
    }
}

fn loans(db: &TemplateDb) -> Result<(), TestHostError> {
    db.execute(
        "CREATE SCHEMA loan_service; \
         CREATE TABLE loan_service.loans (id int PRIMARY KEY, memo text); \
         INSERT INTO loan_service.loans VALUES (1, 'from the template')",
    )
}

fn ids(db: &TestDb) -> Vec<Value> {
    db.query("SELECT id FROM loan_service.loans ORDER BY id")
        .expect("loans")
}

#[test]
fn each_clone_has_the_template_and_nothing_of_another_clone() {
    let Some(admin) = database() else {
        return;
    };
    let leftovers = Leftovers::new(admin);
    let template = TestTemplate::get_or_create(&leftovers.template(&unique("loans")), loans)
        .expect("template");
    let seen: Vec<Vec<Value>> = std::thread::scope(|scope| {
        let clones: Vec<_> = (2..34)
            .map(|id| {
                let template = &template;
                scope.spawn(move || {
                    let db = TestDb::from_template(template).expect("clone");
                    db.execute(&format!(
                        "INSERT INTO loan_service.loans VALUES ({id}, 'clone {id}')"
                    ))
                    .expect("insert");
                    std::thread::sleep(Duration::from_millis(50));
                    ids(&db)
                })
            })
            .collect();
        clones
            .into_iter()
            .map(|clone| clone.join().expect("clone thread"))
            .collect()
    });
    assert_eq!(seen.len(), 32);
    for (id, rows) in (2..).zip(&seen) {
        assert_eq!(rows, &[json!({"id": 1}), json!({"id": id})]);
    }
    let file_copy = TestDb::from_template(&template.clone().strategy(CloneStrategy::FileCopy))
        .expect("a clone by file copy");
    assert_eq!(ids(&file_copy), [json!({"id": 1})]);
}

#[test]
fn concurrent_first_calls_build_a_template_once() {
    let Some(admin) = database() else {
        return;
    };
    let leftovers = Leftovers::new(admin);
    let name = leftovers.template(&unique("loans"));
    let builds = AtomicUsize::new(0);
    let names: Vec<String> = std::thread::scope(|scope| {
        let callers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    TestTemplate::get_or_create(&name, |db| {
                        builds.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(200));
                        loans(db)
                    })
                    .expect("template")
                    .name()
                    .to_string()
                })
            })
            .collect();
        callers
            .into_iter()
            .map(|caller| caller.join().expect("caller"))
            .collect()
    });
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert!(names.iter().all(|built| *built == format!("tmpl_{name}")));
}

#[test]
fn a_template_on_the_server_is_used_without_building_it() {
    let Some(admin) = database() else {
        return;
    };
    let leftovers = Leftovers::new(admin);
    let name = leftovers.template(&unique("loans"));
    let database = format!("tmpl_{name}");
    leftovers
        .admin
        .execute(&format!("CREATE DATABASE \"{database}\""))
        .expect("a template another process built");
    leftovers
        .admin
        .execute(&format!(
            "ALTER DATABASE \"{database}\" IS_TEMPLATE true ALLOW_CONNECTIONS false"
        ))
        .expect("marked");
    let template = TestTemplate::get_or_create(&name, |_| {
        Err(TestHostError::Template("built again".into()))
    })
    .expect("the server's template");
    assert_eq!(template.name(), database);
    TestDb::from_template(&template).expect("a clone");
}

#[test]
fn a_database_of_the_name_that_is_no_template_is_refused() {
    let Some(admin) = database() else {
        return;
    };
    let leftovers = Leftovers::new(admin);
    let name = leftovers.template(&unique("loans"));
    leftovers
        .admin
        .execute(&format!("CREATE DATABASE \"tmpl_{name}\""))
        .expect("a plain database");
    match TestTemplate::get_or_create(&name, loans) {
        Err(TestHostError::Template(message)) => assert_eq!(
            message,
            format!(
                "database tmpl_{name} exists and is not a template; drop it or name the \
                 template otherwise"
            )
        ),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_build_that_fails_or_leaves_a_connection_open_is_dropped_and_built_again() {
    let Some(admin) = database() else {
        return;
    };
    let leftovers = Leftovers::new(admin);
    let name = leftovers.template(&unique("loans"));
    let built = Mutex::new(Vec::new());
    let building = |db: &TemplateDb| {
        built.lock().expect("built").push(db.name().to_string());
    };
    assert!(matches!(
        TestTemplate::get_or_create(&name, |db| {
            building(db);
            db.execute("SELECT * FROM missing")
        }),
        Err(TestHostError::Database(_))
    ));
    let held = Mutex::new(None);
    let refused = TestTemplate::get_or_create(&name, |db| {
        building(db);
        loans(db)?;
        let client = db.config().connect(postgres::NoTls)?;
        *held.lock().expect("held") = Some(client);
        Ok(())
    });
    match refused {
        Err(TestHostError::Template(message)) => assert_eq!(
            message,
            format!(
                "the build of template tmpl_{name} left 1 connection(s) to its database open; \
                 close every connection before the build returns"
            )
        ),
        other => panic!("{other:?}"),
    }
    drop(held);
    assert!(!leftovers.exists(&format!("tmpl_{name}")));
    let built = built.into_inner().expect("built");
    assert_eq!(built.len(), 2);
    for database in &built {
        assert!(
            !leftovers.exists(database),
            "a refused build is dropped: {database}"
        );
    }
    let template = TestTemplate::get_or_create(&name, loans).expect("built at last");
    assert_eq!(
        ids(&TestDb::from_template(&template).expect("clone")),
        [json!({"id": 1})]
    );
}

#[test]
fn a_clone_keeps_the_templates_owners_and_grants_and_its_service_role_takes_the_schema() {
    let Some(admin) = database() else {
        return;
    };
    let mut leftovers = Leftovers::new(admin);
    let reader = unique("loan_reader");
    leftovers.roles.push(reader.clone());
    let name = leftovers.template(&unique("loans"));
    let template = TestTemplate::get_or_create(&name, |db| {
        loans(db)?;
        db.execute(&format!(
            "CREATE ROLE \"{reader}\" NOLOGIN; \
             GRANT USAGE ON SCHEMA loan_service TO \"{reader}\"; \
             GRANT SELECT ON loan_service.loans TO \"{reader}\""
        ))
    })
    .expect("template");
    let clone = TestDb::from_template(&template).expect("clone");
    let standing = || {
        clone
            .query(&format!(
                "SELECT tableowner = current_user AS servers, tableowner AS owner, \
                        has_table_privilege('{reader}', 'loan_service.loans', 'SELECT') AS read \
                 FROM pg_tables WHERE schemaname = 'loan_service' AND tablename = 'loans'"
            ))
            .expect("standing")[0]
            .clone()
    };
    assert_eq!(standing()["servers"], true);
    assert_eq!(standing()["read"], true);
    let role = clone.service_role("loan_service").expect("the role");
    assert_eq!(standing()["owner"], role.name());
    assert_eq!(standing()["read"], true);
    assert_eq!(
        clone
            .query(
                "SELECT nspowner::regrole::text AS owner FROM pg_namespace \
                 WHERE nspname = 'loan_service'"
            )
            .expect("schema"),
        [json!({"owner": role.name()})]
    );
}

#[test]
fn cloning_a_template_takes_a_fraction_of_building_its_schema() {
    let Some(admin) = database() else {
        return;
    };
    let leftovers = Leftovers::new(admin);
    let statements: Vec<String> = std::iter::once("CREATE SCHEMA loan_service".to_string())
        .chain((0..250).map(|table| {
            format!("CREATE TABLE loan_service.ledger_{table} (id int PRIMARY KEY, amount int)")
        }))
        .collect();
    let migrate = |execute: &dyn Fn(&str) -> Result<(), TestHostError>| {
        statements
            .iter()
            .try_for_each(|statement| execute(statement))
    };
    let started = Instant::now();
    let rebuilt = database().expect("a database");
    migrate(&|statement| rebuilt.execute(statement)).expect("schema");
    let rebuilding = started.elapsed();
    let template = TestTemplate::get_or_create(&leftovers.template(&unique("ledger")), |db| {
        migrate(&|statement| db.execute(statement))
    })
    .expect("template");
    let started = Instant::now();
    let clones = (0..50)
        .map(|_| TestDb::from_template(&template).expect("clone"))
        .collect::<Vec<_>>();
    let cloning = started.elapsed() / 50;
    assert_eq!(
        clones[49]
            .query(
                "SELECT count(*) AS relations FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'loan_service'"
            )
            .expect("relations"),
        [json!({"relations": 500})]
    );
    assert!(
        cloning * 2 < rebuilding,
        "a clone took {cloning:?}, building the schema {rebuilding:?}"
    );
}

#[test]
fn a_template_built_by_running_migration_files_gives_each_clone_their_schema() {
    let Some(admin) = database() else {
        return;
    };
    let leftovers = Leftovers::new(admin);
    let mut files: Vec<_> = std::fs::read_dir(loan_migrations())
        .expect("migrations")
        .map(|entry| entry.expect("migration").path())
        .collect();
    files.sort();
    let migrate = |execute: &dyn Fn(&std::path::Path) -> Result<(), TestHostError>| {
        files.iter().try_for_each(|file| execute(file))
    };
    let template =
        TestTemplate::get_or_create(&leftovers.template(&unique("loan_migrations")), |db| {
            db.execute("CREATE SCHEMA loan_service")?;
            migrate(&|file| db.execute_file(file))
        })
        .expect("template");
    let migrated = database().expect("a database");
    migrated
        .execute("CREATE SCHEMA loan_service")
        .expect("schema");
    migrate(&|file| migrated.execute_file(file)).expect("migrated");
    let shape = |db: &TestDb| {
        db.query(
            "SELECT table_name, column_name, data_type FROM information_schema.columns \
             WHERE table_schema = 'loan_service' ORDER BY table_name, ordinal_position",
        )
        .expect("columns")
    };
    let cloned = shape(&TestDb::from_template(&template).expect("clone"));
    assert!(!cloned.is_empty());
    assert_eq!(cloned, shape(&migrated));
}
