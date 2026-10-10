use crate::credentials;
use crate::project_config::ProjectConfig;

#[derive(Clone)]
pub struct CliConfig {
    pub api_url: String,
    pub tenant_slug: String,
    pub api_key: Option<String>,
    pub env_name: Option<String>,
    pub keycloak_url: Option<String>,
    pub keycloak_realm: Option<String>,
    pub keycloak_client_id: Option<String>,
    pub tenant_override: Option<String>,
}

const PLATFORM_KEY_VAR: &str = "CUFFLINK_PLATFORM_API_KEY";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Acting<'a> {
    Own,
    For(&'a str),
    Unhonoured(&'a str),
}

impl<'a> Acting<'a> {
    pub(crate) fn named(self) -> Option<&'a str> {
        match self {
            Acting::For(tenant) => Some(tenant),
            Acting::Own | Acting::Unhonoured(_) => None,
        }
    }
}

impl CliConfig {
    fn platform_key() -> Option<String> {
        std::env::var(PLATFORM_KEY_VAR).ok()
    }

    pub(crate) fn has_platform_key() -> bool {
        Self::platform_key().is_some()
    }

    pub(crate) fn acting<'a>(&self, tenant: Option<&'a str>, platform_key: bool) -> Acting<'a> {
        match tenant {
            None => Acting::Own,
            Some(tenant) if platform_key => Acting::For(tenant),
            Some(tenant) if tenant == self.tenant_slug => Acting::Own,
            Some(tenant) => Acting::Unhonoured(tenant),
        }
    }

    pub(crate) fn for_tenant(self, tenant: Option<&str>, platform_key: bool) -> eyre::Result<Self> {
        match self.acting(tenant, platform_key) {
            Acting::Unhonoured(tenant) => eyre::bail!(
                "--tenant {tenant} needs {PLATFORM_KEY_VAR}: only a platform admin's key acts \
                 for a tenant other than its own. Without it the command would have acted on \
                 {}, the tenant of this environment in Cufflink.toml. Nothing was sent",
                self.tenant_slug
            ),
            acting => Ok(Self {
                tenant_override: acting.named().map(str::to_string),
                ..self
            }),
        }
    }

    pub fn load_with_env(env: Option<&str>) -> eyre::Result<Self> {
        let project = ProjectConfig::find_and_load()?;

        if let Some(ref project) = project {
            if let Some(env_config) = project.resolve_env(env)? {
                let env_name = env
                    .map(|s| s.to_string())
                    .or_else(|| project.service.default_env.clone());

                let api_key = env_config
                    .api_key
                    .clone()
                    .or_else(|| {
                        env_config
                            .api_key_env
                            .as_ref()
                            .and_then(|var| std::env::var(var).ok())
                    })
                    .or_else(|| std::env::var("CUFFLINK_API_KEY").ok())
                    .or_else(|| {
                        credentials::load_for(&env_config.api_url, &env_config.tenant)
                            .ok()
                            .flatten()
                            .map(|c| c.api_key)
                    });

                return Ok(Self {
                    api_url: env_config.api_url.clone(),
                    tenant_slug: env_config.tenant.clone(),
                    api_key,
                    env_name,
                    keycloak_url: env_config
                        .keycloak_url
                        .clone()
                        .or_else(|| std::env::var("CUFFLINK_KEYCLOAK_URL").ok()),
                    keycloak_realm: env_config
                        .keycloak_realm
                        .clone()
                        .or_else(|| std::env::var("CUFFLINK_KEYCLOAK_REALM").ok()),
                    keycloak_client_id: env_config
                        .keycloak_client_id
                        .clone()
                        .or_else(|| std::env::var("CUFFLINK_CLIENT_ID").ok()),
                    tenant_override: None,
                });
            }
        }

        match project {
            Some(_) => eyre::bail!(
                "No environment given: pass --env <name> or set [service].default_env in \
                 Cufflink.toml"
            ),
            None => eyre::bail!(
                "No Cufflink.toml found. Run from a cufflink project directory or use `cufflink init` to create one."
            ),
        }
    }

    pub fn for_platform(api_url_override: Option<&str>, env: Option<&str>) -> eyre::Result<Self> {
        if let Ok(Some(project)) = ProjectConfig::find_and_load() {
            if let Ok(Some(env_config)) = project.resolve_env(env) {
                let api_url = api_url_override
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| env_config.api_url.clone());
                let env_name = env
                    .map(|s| s.to_string())
                    .or_else(|| project.service.default_env.clone());

                return Ok(Self {
                    api_url,
                    tenant_slug: env_config.tenant.clone(),
                    api_key: None,
                    env_name,
                    keycloak_url: env_config
                        .keycloak_url
                        .clone()
                        .or_else(|| std::env::var("CUFFLINK_KEYCLOAK_URL").ok()),
                    keycloak_realm: env_config
                        .keycloak_realm
                        .clone()
                        .or_else(|| std::env::var("CUFFLINK_KEYCLOAK_REALM").ok()),
                    keycloak_client_id: env_config
                        .keycloak_client_id
                        .clone()
                        .or_else(|| std::env::var("CUFFLINK_CLIENT_ID").ok()),
                    tenant_override: None,
                });
            }
        }

        let api_url = api_url_override
            .map(|s| s.to_string())
            .or_else(|| std::env::var("CUFFLINK_API_URL").ok())
            .unwrap_or_else(|| "http://localhost:8080".to_string());

        Ok(Self {
            api_url,
            tenant_slug: String::new(),
            api_key: None,
            env_name: None,
            keycloak_url: std::env::var("CUFFLINK_KEYCLOAK_URL").ok(),
            keycloak_realm: std::env::var("CUFFLINK_KEYCLOAK_REALM").ok(),
            keycloak_client_id: std::env::var("CUFFLINK_CLIENT_ID").ok(),
            tenant_override: None,
        })
    }

    pub fn http_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .expect("Failed to build HTTP client")
    }

    pub async fn find_service_id(&self, service_name: &str) -> eyre::Result<String> {
        let client = self.http_client();
        let resp = self
            .auth_request(
                &client,
                reqwest::Method::GET,
                &format!("{}/api/services", self.api_url),
            )
            .send()
            .await?;

        let body: serde_json::Value = resp.json().await?;
        body["services"]
            .as_array()
            .and_then(|arr| {
                arr.iter()
                    .find(|s| s["name"].as_str() == Some(service_name))
                    .and_then(|s| s["id"].as_str())
            })
            .map(String::from)
            .ok_or_else(|| eyre::eyre!("Service '{}' not found on platform", service_name))
    }

    pub fn auth_request(
        &self,
        client: &reqwest::Client,
        method: reqwest::Method,
        url: &str,
    ) -> reqwest::RequestBuilder {
        if let Some(ref tenant) = self.tenant_override {
            if let Some(platform_key) = Self::platform_key() {
                return client
                    .request(method, url)
                    .header("Authorization", format!("ApiKey {}", platform_key))
                    .header("X-Tenant-Slug", tenant.as_str());
            }
        }

        let key = match &self.api_key {
            Some(k) => k,
            None => {
                eprintln!(
                    "Error: No credentials found for tenant '{}'.",
                    self.tenant_slug
                );
                eprintln!();
                eprintln!("  Run `cufflink login` to authenticate with your tenant.");
                eprintln!("  Or set CUFFLINK_API_KEY in your environment.");
                std::process::exit(1);
            }
        };
        client
            .request(method, url)
            .header("Authorization", format!("ApiKey {}", key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of_acme() -> CliConfig {
        CliConfig {
            api_url: "https://platform.test".into(),
            tenant_slug: "acme".into(),
            api_key: Some("key".into()),
            env_name: None,
            keycloak_url: None,
            keycloak_realm: None,
            keycloak_client_id: None,
            tenant_override: None,
        }
    }

    #[test]
    fn a_tenant_is_acted_for_only_with_a_platform_key_or_when_it_is_the_configurations_own() {
        let config = of_acme();
        for platform_key in [false, true] {
            assert_eq!(config.acting(None, platform_key), Acting::Own);
        }
        assert_eq!(config.acting(Some("other"), true), Acting::For("other"));
        assert_eq!(config.acting(Some("acme"), true), Acting::For("acme"));
        assert_eq!(config.acting(Some("acme"), false), Acting::Own);
        assert_eq!(
            config.acting(Some("other"), false),
            Acting::Unhonoured("other")
        );
        assert_eq!(Acting::For("other").named(), Some("other"));
        assert_eq!(Acting::Own.named(), None);
        assert_eq!(
            Acting::Unhonoured("other").named(),
            None,
            "a command named to act where this one did acts on the configuration's own tenant"
        );
    }

    #[test]
    fn a_configuration_overrides_its_tenant_only_where_it_acts_for_the_one_named() {
        let overridden = |tenant, platform_key| {
            of_acme()
                .for_tenant(tenant, platform_key)
                .map(|config| config.tenant_override)
        };
        assert_eq!(overridden(None, false).expect("its own"), None);
        assert_eq!(overridden(Some("acme"), false).expect("its own"), None);
        assert_eq!(
            overridden(Some("other"), true).expect("a platform key"),
            Some("other".to_string())
        );
        assert!(overridden(Some("other"), false).is_err());
    }
}
