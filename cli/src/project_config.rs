use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct ProjectConfig {
    #[serde(default)]
    pub service: ServiceConfig,
    #[serde(default)]
    pub environments: HashMap<String, EnvironmentConfig>,
    #[serde(skip)]
    pub project_dir: PathBuf,
}

#[derive(Debug, Deserialize, Default)]
pub struct ServiceConfig {
    pub name: Option<String>,
    pub mode: Option<String>,
    pub default_env: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EnvironmentConfig {
    pub api_url: String,
    pub tenant: String,
    pub api_key_env: Option<String>,
    pub api_key: Option<String>,
    pub keycloak_url: Option<String>,
    pub keycloak_realm: Option<String>,
    pub keycloak_client_id: Option<String>,
    #[serde(default)]
    pub config: HashMap<String, String>,
    #[serde(default)]
    pub secrets: HashMap<String, String>,
    pub db_role: Option<DeclaredRole>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredRole {
    pub role: String,
    pub password_secret: String,
}

impl EnvironmentConfig {
    fn handed_to_guest(&self, env: &str) -> Option<String> {
        self.db_role
            .as_ref()
            .filter(|declared| {
                self.secrets
                    .values()
                    .any(|entry| *entry == declared.password_secret)
            })
            .map(|declared| {
                format!(
                    "[environments.{env}.db_role] names {} as its password, and \
                     [environments.{env}.secrets] maps it, which would hand it to the service's code",
                    declared.password_secret
                )
            })
    }
}

pub(crate) struct Declaring<'a> {
    pub(crate) project: &'a ProjectConfig,
    pub(crate) env: String,
    pub(crate) own: &'a str,
    pub(crate) declared: &'a DeclaredRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Foreign<'a> {
    own: &'a str,
    service: &'a str,
}

impl Foreign<'_> {
    pub(crate) fn refusal(&self, does: &str, run: &str) -> String {
        let Self { own, service } = self;
        format!(
            "the Cufflink.toml here is of {own}, whose config {does} {service}. {run} it where \
             Cufflink.toml is of {service}, or names no service"
        )
    }
}

impl ProjectConfig {
    pub(crate) fn foreign_to<'a>(&'a self, service: &'a str) -> Option<Foreign<'a>> {
        self.service
            .name
            .as_deref()
            .filter(|own| *own != service)
            .map(|own| Foreign { own, service })
    }

    pub(crate) fn parsed(content: &str, dir: &std::path::Path) -> eyre::Result<Self> {
        let file = dir.join("Cufflink.toml");
        let mut config: ProjectConfig = toml::from_str(content)
            .map_err(|e| eyre::eyre!("Failed to parse {}: {e}", file.display()))?;
        if let Some(refusal) = config
            .environments
            .iter()
            .find_map(|(env, environment)| environment.handed_to_guest(env))
        {
            eyre::bail!("{}: {refusal}", file.display());
        }
        config.project_dir = dir.to_path_buf();
        config
            .environments
            .entry(crate::commands::dev::ENV.to_string())
            .or_insert_with(crate::commands::dev::environment);
        Ok(config)
    }

    pub fn find_and_load() -> eyre::Result<Option<Self>> {
        let cwd = std::env::current_dir()?;
        let mut dir = cwd.as_path();
        loop {
            let candidate = dir.join("Cufflink.toml");
            if candidate.exists() {
                let content = std::fs::read_to_string(&candidate)?;
                return Self::parsed(&content, dir).map(Some);
            }
            match dir.parent() {
                Some(parent) => dir = parent,
                None => return Ok(None),
            }
        }
    }

    pub fn get_env(&self, name: &str) -> eyre::Result<&EnvironmentConfig> {
        self.environments.get(name).ok_or_else(|| {
            let available: Vec<&str> = self.environments.keys().map(|k| k.as_str()).collect();
            eyre::eyre!(
                "Environment '{}' not found in Cufflink.toml. Available: {:?}",
                name,
                available
            )
        })
    }

    pub(crate) fn declaration(&self, env: Option<&str>) -> Option<(String, &DeclaredRole)> {
        let name = self.env_name(env)?;
        let declared = self.environments.get(&name)?.db_role.as_ref()?;
        Some((name, declared))
    }

    pub(crate) fn declaring(&self, env: Option<&str>) -> eyre::Result<Option<Declaring<'_>>> {
        let Some((env, declared)) = self.declaration(env) else {
            return Ok(None);
        };
        let Some(own) = self.service.name.as_deref() else {
            eyre::bail!(
                "[environments.{env}.db_role] needs [service].name, the name of the service it \
                 is declared for"
            );
        };
        Ok(Some(Declaring {
            project: self,
            env,
            own,
            declared,
        }))
    }

    pub fn env_name(&self, env: Option<&str>) -> Option<String> {
        env.map(str::to_string)
            .or_else(|| self.service.default_env.clone())
    }

    pub fn resolve_env(&self, env_arg: Option<&str>) -> eyre::Result<Option<&EnvironmentConfig>> {
        self.env_name(env_arg)
            .map(|name| self.get_env(&name))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn a_project_has_the_development_stack_as_dev_unless_it_declares_one() {
        let undeclared = ProjectConfig::parsed(
            "[service]\nname = \"notes\"\ndefault_env = \"staging\"\n\n\
             [environments.staging]\napi_url = \"https://api.staging.test\"\ntenant = \"acme\"\n",
            Path::new("/work/notes"),
        )
        .expect("a project");
        assert_eq!(undeclared.project_dir, Path::new("/work/notes"));
        let dev = undeclared.get_env("dev").expect("dev");
        assert_eq!(
            (dev.api_url.as_str(), dev.tenant.as_str()),
            ("http://localhost:8080", "dev")
        );
        let default = undeclared
            .resolve_env(None)
            .expect("resolved")
            .expect("staging");
        assert_eq!(default.api_url, "https://api.staging.test");

        let declared = ProjectConfig::parsed(
            "[environments.dev]\napi_url = \"http://localhost:9999\"\ntenant = \"mine\"\n",
            Path::new("/work/notes"),
        )
        .expect("a project");
        let dev = declared.get_env("dev").expect("dev");
        assert_eq!(
            (
                dev.api_url.as_str(),
                dev.tenant.as_str(),
                dev.keycloak_realm.as_deref()
            ),
            ("http://localhost:9999", "mine", None)
        );
        assert!(ProjectConfig::parsed("", Path::new("/"))
            .expect("empty")
            .resolve_env(None)
            .expect("none")
            .is_none());
    }

    #[test]
    fn a_project_is_foreign_to_any_service_but_the_one_it_names() {
        let project = |toml| ProjectConfig::parsed(toml, Path::new("/")).expect("a project");
        let unnamed = project("");
        assert_eq!(unnamed.foreign_to("billing"), None);
        let notes = project("[service]\nname = \"notes\"\n");
        assert_eq!(notes.foreign_to("notes"), None);
        assert_eq!(
            notes
                .foreign_to("billing")
                .map(|foreign| foreign.refusal("would be written to", "Run")),
            Some(
                "the Cufflink.toml here is of notes, whose config would be written to billing. \
                 Run it where Cufflink.toml is of billing, or names no service"
                    .to_string()
            )
        );
    }

    const DECLARING: &str = "[service]\nname = \"loan-service\"\n\n\
        [environments.staging]\napi_url = \"https://api.example.com\"\ntenant = \"acme\"\n\n\
        [environments.staging.db_role]\nrole = \"loan_app\"\npassword_secret = \"db:loan_app\"\n";

    #[test]
    fn an_environment_declares_its_role_for_the_service_the_file_names() {
        let project = ProjectConfig::parsed(DECLARING, Path::new("/")).expect("a project");
        let expected = DeclaredRole {
            role: "loan_app".to_string(),
            password_secret: "db:loan_app".to_string(),
        };
        assert_eq!(
            project.declaration(Some("staging")),
            Some(("staging".to_string(), &expected))
        );
        let declaring = project
            .declaring(Some("staging"))
            .expect("named")
            .expect("declared");
        assert_eq!(
            (declaring.env.as_str(), declaring.own, declaring.declared),
            ("staging", "loan-service", &expected)
        );
        assert_eq!(project.declaration(Some("dev")), None);
        assert!(project.declaring(Some("dev")).expect("none").is_none());
        let unnamed = ProjectConfig::parsed(
            DECLARING.replace("name = \"loan-service\"\n", "").as_str(),
            Path::new("/"),
        )
        .expect("a project");
        assert!(unnamed.declaration(Some("staging")).is_some());
        assert_eq!(
            unnamed
                .declaring(Some("staging"))
                .err()
                .expect("no [service].name")
                .to_string(),
            "[environments.staging.db_role] needs [service].name, the name of the service it is \
             declared for"
        );
    }

    #[test]
    fn a_declared_role_refuses_unknown_keys_and_a_password_handed_to_the_guest() {
        let unknown = ProjectConfig::parsed(
            &format!("{DECLARING}password = \"hunter2\"\n"),
            Path::new("/project"),
        )
        .expect_err("an unknown key")
        .to_string();
        assert!(
            unknown.starts_with("Failed to parse /project/Cufflink.toml: "),
            "{unknown}"
        );
        assert_eq!(
            ProjectConfig::parsed(
                &DECLARING.replace(
                    "[environments.staging.db_role]",
                    "[environments.staging.secrets]\nDB_PASSWORD = \"db:loan_app\"\n\n\
                     [environments.staging.db_role]"
                ),
                Path::new("/project")
            )
            .expect_err("handed to the guest")
            .to_string(),
            "/project/Cufflink.toml: [environments.staging.db_role] names db:loan_app as its \
             password, and [environments.staging.secrets] maps it, which would hand it to the \
             service's code"
        );
    }
}
