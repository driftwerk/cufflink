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

    pub(crate) fn parsed(content: &str, dir: &std::path::Path) -> Result<Self, toml::de::Error> {
        let mut config: ProjectConfig = toml::from_str(content)?;
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
                return Self::parsed(&content, dir)
                    .map(Some)
                    .map_err(|e| eyre::eyre!("Failed to parse {}: {}", candidate.display(), e));
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
}
