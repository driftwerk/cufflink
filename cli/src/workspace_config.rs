use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const WORKSPACE_FILE: &str = "cufflink-workspace.toml";

#[derive(Debug, Deserialize)]
pub struct WorkspaceConfig {
    pub workspace: WorkspaceInfo,
    #[serde(default)]
    pub services: Vec<WorkspaceService>,
    #[serde(default)]
    pub environments: HashMap<String, WorkspaceEnv>,
}

#[derive(Debug, Deserialize)]
pub struct WorkspaceInfo {
    pub name: String,
    #[serde(default)]
    pub default_env: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WorkspaceService {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub seed: Option<String>,
    #[serde(default)]
    pub preview_tables: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct WorkspaceEnv {
    #[serde(default)]
    pub deploy: Option<Vec<String>>,
    #[serde(default)]
    pub seed: Option<Vec<String>>,
}

impl WorkspaceConfig {
    pub fn find_and_load() -> eyre::Result<(Self, PathBuf)> {
        let mut dir = std::env::current_dir()?;
        loop {
            let candidate = dir.join(WORKSPACE_FILE);
            if candidate.is_file() {
                let content = std::fs::read_to_string(&candidate)?;
                let config: WorkspaceConfig = toml::from_str(&content)
                    .map_err(|e| eyre::eyre!("Failed to parse {}: {}", WORKSPACE_FILE, e))?;
                return Ok((config, dir));
            }
            if !dir.pop() {
                break;
            }
        }
        eyre::bail!(
            "No {} found in current directory or any parent",
            WORKSPACE_FILE
        )
    }

    pub fn resolve_env<'a>(&'a self, cli_env: Option<&'a str>) -> Option<&'a str> {
        cli_env.or(self.workspace.default_env.as_deref())
    }

    pub fn deploy_list(&self, env: Option<&str>) -> Vec<&WorkspaceService> {
        let env_config = env.and_then(|e| self.environments.get(e));
        let deploy_names = env_config.and_then(|e| e.deploy.as_ref());

        match deploy_names {
            Some(names) => self
                .services
                .iter()
                .filter(|s| names.iter().any(|n| n == &s.name))
                .collect(),
            None => self.services.iter().collect(),
        }
    }

    pub fn seed_list(&self, env: Option<&str>) -> Vec<&WorkspaceService> {
        let env_config = env.and_then(|e| self.environments.get(e));
        let seed_names = env_config.and_then(|e| e.seed.as_ref());

        match seed_names {
            Some(names) => self
                .services
                .iter()
                .filter(|s| names.iter().any(|n| n == &s.name) && s.seed.is_some())
                .collect(),
            None => Vec::new(),
        }
    }

    pub fn preview_allowlist_mode(&self) -> bool {
        self.services.iter().any(|s| s.preview_tables.is_some())
    }

    pub fn preview_tables_for(&self, service_name: &str) -> Option<&[String]> {
        self.services
            .iter()
            .find(|s| s.name == service_name)
            .and_then(|s| s.preview_tables.as_deref())
    }

    pub fn validate_paths(&self, root: &Path) -> eyre::Result<()> {
        for svc in &self.services {
            let path = root.join(&svc.path);
            if !path.is_dir() {
                eyre::bail!(
                    "Service '{}' path '{}' does not exist",
                    svc.name,
                    path.display()
                );
            }
            if let Some(ref seed) = svc.seed {
                let seed_path = root.join(seed);
                if !seed_path.is_file() {
                    eyre::bail!(
                        "Service '{}' seed file '{}' does not exist",
                        svc.name,
                        seed_path.display()
                    );
                }
            }
        }
        Ok(())
    }
}
