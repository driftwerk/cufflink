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

struct Project {
    root: tempfile::TempDir,
    api_url: String,
    tenant: String,
    api_key: String,
    role: String,
}

struct World {
    db: TestDb,
    platform: PlatformProcess,
    tenant: Tenant,
    admin: String,
    layout: Project,
}

impl std::ops::Deref for World {
    type Target = Project;

    fn deref(&self) -> &Project {
        &self.layout
    }
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
        let admin = db.mint_admin_key().await;
        let layout = Project::laid_out(
            &platform.base_url,
            &tenant.slug,
            &tenant.api_key,
            &db.role("loan_service_user"),
        );
        let world = Self {
            db,
            platform,
            tenant,
            admin,
            layout,
        };
        world.make_role().await;
        world.allow().await;
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
        let (status, body) = self
            .platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &format!("ApiKey {}", self.admin),
                Some(&json!({"policies": {"db_roles": {
                    "mode": "enforce",
                    "config": {"declared_roles": [self.role]},
                }}})),
            )
            .await;
        assert_eq!(status, 200, "policy: {body}");
    }

    async fn services(&self) -> Vec<Value> {
        let (status, body) = self.platform.api_get(&self.tenant, "/api/services").await;
        assert_eq!(status, 200, "{body}");
        body["services"].as_array().cloned().unwrap_or_default()
    }

    fn shown(&self) -> String {
        let output = Command::new(env!("CARGO_BIN_EXE_cufflink"))
            .args([
                "services",
                "db-role",
                "--api-url",
                &self.platform.base_url,
                "show",
            ])
            .args([self.tenant.slug.as_str(), SERVICE])
            .current_dir(self.root.path())
            .env("HOME", self.root.path())
            .env("CUFFLINK_PLATFORM_API_KEY", &self.admin)
            .output()
            .expect("the cufflink binary runs");
        assert!(output.status.success(), "db-role show: {output:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    async fn audited(&self, action: &str) -> i64 {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT count(*) FROM audit_log WHERE action = $1",
                &[&action],
            )
            .await
            .expect("audit")
            .get(0)
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

impl Project {
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
                self.api_url,
                self.tenant,
                if declaring { declared.as_str() } else { "" }
            ),
        )
        .expect("Cufflink.toml");
    }

    fn laid_out(api_url: &str, tenant: &str, api_key: &str, role: &str) -> Self {
        let laid = Self {
            root: tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("directory"),
            api_url: api_url.to_string(),
            tenant: tenant.to_string(),
            api_key: api_key.to_string(),
            role: role.to_string(),
        };
        laid.lay_out();
        laid
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

    fn bundle(&self) -> String {
        self.root
            .path()
            .join("loan-service.bundle")
            .display()
            .to_string()
    }

    fn run(&self, arguments: &[&str]) -> Ran {
        self.run_in(&self.project(), arguments)
    }

    fn run_in(&self, dir: &std::path::Path, arguments: &[&str]) -> Ran {
        let path = format!(
            "{}:{}",
            self.tools().display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let output = Command::new(env!("CARGO_BIN_EXE_cufflink"))
            .args(arguments)
            .current_dir(dir)
            .env("PATH", path)
            .env("HOME", self.root.path())
            .env("CUFFLINK_API_KEY", &self.api_key)
            .env_remove("CUFFLINK_PLATFORM_API_KEY")
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

    let bundle = world.bundle();
    world
        .run(&["build", "--bundle", &bundle])
        .ok("build --bundle");
    let bound = format!(
        "  Database role:  {} (declared by the service, verified)",
        world.role
    );
    let deployed = world
        .run(&["deploy", "--bundle", &bundle])
        .ok("deploy --bundle");
    assert_eq!(
        line_of(&deployed, "  Database role:"),
        Some(bound.as_str()),
        "{deployed}"
    );
    assert!(!deployed.contains("note:"), "{deployed}");
    assert_eq!(world.declared_bindings().await, 1);
    let shown = world.shown();
    assert!(
        shown.starts_with(&format!(
            "{}/{SERVICE}: role {} (declared by the service, password stored)\n  verified at ",
            world.tenant.slug, world.role
        )),
        "{shown}"
    );
    let (status, body) = world
        .platform
        .send(
            Method::POST,
            &format!("/svc/{}/{SERVICE}/loans", world.tenant.slug),
            &format!("ApiKey {}", world.tenant.api_key),
            Some(&json!({"amount": "1200"})),
        )
        .await;
    assert!(status < 300, "the declared role serves: {status} {body}");

    let again = world
        .run(&["deploy", "--bundle", &bundle])
        .ok("a second deploy");
    assert_eq!(
        line_of(&again, "  Database role:"),
        Some(bound.as_str()),
        "{again}"
    );
    assert_eq!(
        world.audited("db_role.declare").await,
        1,
        "the same role and password are taken as they are"
    );

    world.write_project(false);
    let undeclared = world
        .run(&["deploy", "--bundle", &bundle])
        .ok("a deploy that declares no role");
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
        declaring.replace(&format!("name = \"{SERVICE}\"\n"), ""),
    )
    .expect("Cufflink.toml");
    let nested = world.project().join("nested");
    std::fs::create_dir_all(&nested).expect("directory");
    let unnamed = world
        .run_in(&nested, &["deploy"])
        .failed("a parent's Cufflink.toml that names no service");
    assert!(
        unnamed.contains(
            "[environments.test.db_role] needs [service].name, the name of the service it is \
             declared for"
        ),
        "{unnamed}"
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
            "Cufflink.toml: [environments.test.db_role] names {SECRET} as its password, and \
             [environments.test.secrets] maps it, which would hand it to the service's code"
        )),
        "{handed}"
    );
    assert!(!handed.contains("Failed to parse"), "{handed}");
    assert!(world.services().await.is_empty(), "nothing is sent");
}

async fn older_platform() -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&asked);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut request = vec![0u8; 8192];
            let read = socket.read(&mut request).await.unwrap_or(0);
            let line = String::from_utf8_lossy(&request[..read])
                .lines()
                .next()
                .unwrap_or_default()
                .to_string();
            recorded
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(line);
            let body =
                json!({"api_version": 1, "platform_version": "0.40.6", "supports": []}).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
                 connection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (format!("http://{address}"), asked)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_platform_that_takes_no_declarations_is_told_so_before_anything_is_built_or_sent() {
    let (api_url, asked) = older_platform().await;
    let project = Project::laid_out(&api_url, "acme", "key", "loan_service_user");
    let refusal = "[environments.test.db_role] needs a platform that advertises \
                   db_role_declarations (v0.41.0 or later); platform v0.40.6 does not";

    let deployed = project.run(&["deploy"]).failed("a deploy");
    assert!(deployed.contains(refusal), "{deployed}");
    assert!(!deployed.contains("Deploying"), "{deployed}");

    project.write_project(false);
    let bundle = project.bundle();
    project
        .run(&["build", "--bundle", &bundle])
        .ok("build --bundle");
    project.write_project(true);
    let bundled = project
        .run(&["deploy", "--bundle", &bundle])
        .failed("a bundle's deploy");
    assert!(bundled.contains(refusal), "{bundled}");

    assert_eq!(
        *asked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        ["GET /api/version HTTP/1.1", "GET /api/version HTTP/1.1"],
        "the platform is asked its version and sent nothing"
    );
}
