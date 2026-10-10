use comfy_table::{presets::NOTHING, Table, TableComponent};
use eyre::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct PackageManifest {
    pub package: PackageInfo,
    #[serde(default)]
    pub services: Vec<ServiceEntry>,
    #[serde(default)]
    pub frontends: Vec<FrontendEntry>,
    #[serde(default)]
    pub config: PackageConfig,
}

#[derive(Debug, Deserialize)]
pub struct PackageInfo {
    pub name: String,
    pub description: Option<String>,
    pub version: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ServiceEntry {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct FrontendEntry {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct PackageConfig {
    #[serde(default)]
    pub required: Vec<String>,
}

impl PackageManifest {
    pub fn load(dir: &Path) -> Result<Self> {
        let path = dir.join("cufflink-package.toml");
        let content = std::fs::read_to_string(&path).map_err(|_| {
            eyre::eyre!(
                "No cufflink-package.toml found. This repository is not a cufflink package."
            )
        })?;
        toml::from_str(&content)
            .map_err(|e| eyre::eyre!("Failed to parse cufflink-package.toml: {}", e))
    }

    pub fn print_summary(&self) {
        println!("Package: {}", self.package.name);
        if let Some(desc) = &self.package.description {
            println!("  {}", desc);
        }
        if let Some(version) = &self.package.version {
            println!("  Version: {}", version);
        }
        println!();

        if !self.services.is_empty() || !self.frontends.is_empty() {
            let mut table = Table::new();
            table.load_preset(NOTHING);
            table.set_style(TableComponent::HeaderLines, '-');
            table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
            table.set_header(vec!["COMPONENT", "TYPE", "PATH"]);

            for s in &self.services {
                table.add_row(vec![&s.name, "service", &s.path]);
            }
            for f in &self.frontends {
                table.add_row(vec![&f.name, "frontend", &f.path]);
            }

            println!("{table}");
            println!();
        }

        if !self.config.required.is_empty() {
            println!("Required config:");
            for key in &self.config.required {
                println!("  - {}", key);
            }
            println!();
        }
    }

    pub fn validate_paths(&self, base: &Path) -> Result<()> {
        let mut missing = Vec::new();

        for s in &self.services {
            let path = base.join(&s.path);
            if !path.exists() {
                missing.push(format!("service '{}' at {}", s.name, s.path));
            }
        }
        for f in &self.frontends {
            let path = base.join(&f.path);
            if !path.exists() {
                missing.push(format!("frontend '{}' at {}", f.name, f.path));
            }
        }

        if missing.is_empty() {
            Ok(())
        } else {
            eyre::bail!("Missing component paths:\n  {}", missing.join("\n  "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_manifest() {
        let toml = r#"
            [package]
            name = "test-pkg"
        "#;
        let manifest: PackageManifest = toml::from_str(toml).unwrap();
        assert_eq!(manifest.package.name, "test-pkg");
        assert!(manifest.services.is_empty());
        assert!(manifest.frontends.is_empty());
        assert!(manifest.config.required.is_empty());
    }

    #[test]
    fn test_parse_full_manifest() {
        let toml = r#"
            [package]
            name = "ecommerce"
            description = "Full e-commerce suite"
            version = "1.0.0"

            [[services]]
            name = "orders"
            path = "backend/orders"

            [[services]]
            name = "inventory"
            path = "backend/inventory"

            [[frontends]]
            name = "admin"
            path = "frontend/admin"

            [config]
            required = ["STRIPE_KEY", "WEBHOOK_SECRET"]
        "#;
        let manifest: PackageManifest = toml::from_str(toml).unwrap();
        assert_eq!(manifest.package.name, "ecommerce");
        assert_eq!(manifest.services.len(), 2);
        assert_eq!(manifest.frontends.len(), 1);
        assert_eq!(manifest.config.required.len(), 2);
        assert_eq!(manifest.services[0].name, "orders");
        assert_eq!(manifest.frontends[0].path, "frontend/admin");
    }

    #[test]
    fn test_parse_no_config_section() {
        let toml = r#"
            [package]
            name = "simple"

            [[services]]
            name = "api"
            path = "backend/api"
        "#;
        let manifest: PackageManifest = toml::from_str(toml).unwrap();
        assert!(manifest.config.required.is_empty());
    }
}
