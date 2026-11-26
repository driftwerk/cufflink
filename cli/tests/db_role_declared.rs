use cufflink_testkit as common;

use base64::Engine;
use common::{PlatformProcess, Tenant, TestDb};
use reqwest::Method;
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

const SERVICE: &str = "loan-service";
const SCHEMA: &str = "loan_service";
const SECRET: &str = "db:loan_service";
const PASSWORD: &str = "pw-declared-cli-1";

struct Ran {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl Ran {
    fn ok(self, what: &str) -> String {
        assert_eq!(self.code, Some(0), "{what}: {}{}", self.stdout, self.stderr);
        self.stdout
    }

    fn failed(self, what: &str) -> String {
        assert_eq!(self.code, Some(1), "{what}: {}{}", self.stdout, self.stderr);
        self.stderr
    }
}

struct World {
    db: TestDb,
    platform: PlatformProcess,
    tenant: Tenant,
    role: String,
    root: tempfile::TempDir,
}

impl World {
    async fn start() -> Option<Self> {
        common::require_binary("cufflink-platform")?;
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let kek = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let platform = PlatformProcess::start(
            &db.url,
            infra.nats_url(),
            &[
                ("CUFFLINK_SECRETS_KEK", kek.as_str()),
                ("CUFFLINK_SECRETS_KEK_ID", "test"),
            ],
        )
        .await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let role = db.role("loan_service_user");
        let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("directory");
        let world = Self {
            db,
            platform,
            tenant,
            role,
            root,
        };
        world.make_role().await;
        world.allow().await;
        world.lay_out();
        Some(world)
    }

    async fn tenant_db(&self) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(
            &self.db.tenant_url(&self.tenant.slug),
            tokio_postgres::NoTls,
        )
        .await
        .expect("tenant database");
        tokio::spawn(connection);
        client
    }

    async fn make_role(&self) {
        let role = &self.role;
        self.tenant_db()
            .await
            .batch_execute(&format!(
                "CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}';
                 DO $$ BEGIN
                     EXECUTE format('GRANT CONNECT ON DATABASE %I TO {role}', current_database());
                 END $$;
                 GRANT {role} TO CURRENT_USER;
                 CREATE SCHEMA {SCHEMA} AUTHORIZATION {role};"
            ))
            .await
            .expect("the role");
    }

    async fn allow(&self) {
        let admin = format!("ApiKey {}", self.db.mint_admin_key().await);
        let (status, body) = self
            .platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &admin,
                Some(&json!({"policies": {"db_roles": {
                    "mode": "enforce",
                    "config": {"declared_roles": [self.role]},
                }}})),
            )
            .await;
        assert_eq!(status, 200, "policy: {body}");
    }

    fn project(&self) -> PathBuf {
        self.root.path().join("project")
    }

    fn tools(&self) -> PathBuf {
        self.root.path().join("tools")
    }

    fn write_project(&self, declaring: bool) {
        let declared = format!(
            "\n[environments.test.db_role]\nrole = \"{}\"\npassword_secret = \"{SECRET}\"\n",
            self.role
        );
        std::fs::write(
            self.project().join("Cufflink.toml"),
            format!(
                "[service]\nname = \"{SERVICE}\"\ndefault_env = \"test\"\n\n\
                 [environments.test]\napi_url = \"{}\"\ntenant = \"{}\"\n{}",
                self.platform.base_url,
                self.tenant.slug,
                if declaring { declared.as_str() } else { "" }
            ),
        )
        .expect("Cufflink.toml");
    }

    fn lay_out(&self) {
        std::fs::create_dir_all(self.project()).expect("directory");
        std::fs::create_dir_all(self.tools()).expect("directory");
        self.write_project(true);
        let manifest = self.root.path().join("manifest.json");
        std::fs::write(
            &manifest,
            json!({
                "name": SERVICE,
                "version": "0.1.0",
                "mode": "crud",
                "tables": [{
                    "name": "loans",
                    "columns": [
                        {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                        {"name": "amount", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false}
                    ]
                }],
                "publishes": [],
            })
            .to_string(),
        )
        .expect("manifest");
        let cargo = self.tools().join("cargo");
        std::fs::write(
            &cargo,
            format!(
                "#!/bin/sh\ncase \"$*\" in\n  \"run -- --emit-manifest\") cat \"{}\" ;;\n  \
                 metadata*) echo '{{\"packages\":[]}}' ;;\n  *) exit 1 ;;\nesac\n",
                manifest.display()
            ),
        )
        .expect("cargo");
        std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).expect("mode");
    }

    fn run(&self, arguments: &[&str]) -> Ran {
        let path = format!(
            "{}:{}",
            self.tools().display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let output = Command::new(env!("CARGO_BIN_EXE_cufflink"))
            .args(arguments)
            .current_dir(self.project())
            .env("PATH", path)
            .env("HOME", self.root.path())
            .env("CUFFLINK_API_KEY", &self.tenant.api_key)
            .env_remove("CUFFLINK_SECRETS_KEY")
            .output()
            .expect("the cufflink binary runs");
        let ran = Ran {
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        };
        assert!(
            !ran.stdout.contains(PASSWORD) && !ran.stderr.contains(PASSWORD),
            "the password is never printed: {}{}",
            ran.stdout,
            ran.stderr
        );
        ran
    }

    async fn services(&self) -> Vec<Value> {
        let (status, body) = self.platform.api_get(&self.tenant, "/api/services").await;
        assert_eq!(status, 200, "{body}");
        body["services"].as_array().cloned().unwrap_or_default()
    }

    async fn declared_bindings(&self) -> i64 {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT count(*) FROM service_db_roles WHERE role_name = $1 AND declared",
                &[&self.role],
            )
            .await
            .expect("bindings")
            .get(0)
    }
}

fn line_of<'a>(stdout: &'a str, start: &str) -> Option<&'a str> {
    stdout.lines().find(|line| line.starts_with(start))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deploy_declares_the_role_its_environment_names_with_the_vaults_password() {
    let Some(world) = World::start().await else {
        return;
    };
    world.run(&["secrets", "init"]).ok("secrets init");

    let missing = world.run(&["deploy"]).failed("a password the vault lacks");
    assert!(
        missing.contains(&format!(
            "[environments.test.db_role] names {SECRET} as its password, and the 'test' vault holds no such entry"
        )),
        "{missing}"
    );
    assert!(world.services().await.is_empty(), "nothing is sent");

    world
        .run(&["secrets", "set", SECRET, PASSWORD])
        .ok("secrets set");
    let plan = world.run(&["deploy", "--plan"]).ok("deploy --plan");
    assert_eq!(
        line_of(&plan, "  Database role:"),
        Some(format!("  Database role:  {}, would be bound", world.role).as_str()),
        "{plan}"
    );
    assert_eq!(world.declared_bindings().await, 0, "a plan binds nothing");

    let deployed = world.run(&["deploy"]).ok("deploy");
    assert_eq!(
        line_of(&deployed, "  Database role:"),
        Some(
            format!(
                "  Database role:  {} (declared by the service, verified)",
                world.role
            )
            .as_str()
        ),
        "{deployed}"
    );
    assert_eq!(world.declared_bindings().await, 1);
    assert!(!deployed.contains("note:"), "{deployed}");

    world.write_project(false);
    let undeclared = world.run(&["deploy"]).ok("a deploy that declares no role");
    assert_eq!(
        line_of(&undeclared, "  note:"),
        Some(
            format!(
                "  note: {SERVICE} stays bound to the declared role {}; an administrator changes it with db-role set",
                world.role
            )
            .as_str()
        ),
        "{undeclared}"
    );
    assert_eq!(world.declared_bindings().await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declared_role_is_never_sent_for_another_service_or_handed_to_its_code() {
    let Some(world) = World::start().await else {
        return;
    };
    let toml = world.project().join("Cufflink.toml");
    let declaring = std::fs::read_to_string(&toml).expect("Cufflink.toml");

    std::fs::write(&toml, declaring.replace(SERVICE, "catalog-service")).expect("Cufflink.toml");
    let foreign = world.run(&["deploy"]).failed("another service's file");
    assert!(
        foreign.contains(&format!(
            "[environments.test.db_role] needs [service].name = \"{SERVICE}\""
        )),
        "{foreign}"
    );

    std::fs::write(
        &toml,
        declaring.replace(
            "[environments.test.db_role]",
            &format!("[environments.test.secrets]\nDB_PASSWORD = \"{SECRET}\"\n\n[environments.test.db_role]"),
        ),
    )
    .expect("Cufflink.toml");
    let handed = world
        .run(&["deploy"])
        .failed("the password mapped for the guest");
    assert!(
        handed.contains(&format!(
            "{SECRET} is the db_role's password; [secrets] would hand it to the service's code"
        )),
        "{handed}"
    );
    assert!(world.services().await.is_empty(), "nothing is sent");
}
