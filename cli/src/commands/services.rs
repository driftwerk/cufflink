use crate::config::CliConfig;
use crate::project_config::ProjectConfig;
use cufflink_types::ServiceMode;
use reqwest::Method;
use serde_json::{json, Value};
use std::process::Command;

fn entry<'a>(body: &'a Value, name: &str) -> Option<&'a Value> {
    body["services"]
        .as_array()
        .and_then(|services| services.iter().find(|s| s["name"].as_str() == Some(name)))
}

fn listed(body: &Value, name: &str) -> eyre::Result<String> {
    entry(body, name).ok_or_else(|| eyre::eyre!("Service '{}' not found", name))?["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| eyre::eyre!("Service has no ID"))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Deployed {
    pub name: String,
    pub id: String,
    pub mode: Option<ServiceMode>,
    pub current_version: i32,
}

fn deployed(body: &Value, name: &str) -> eyre::Result<Deployed> {
    let service = entry(body, name)
        .ok_or_else(|| eyre::eyre!("Service '{name}' not found on platform: deploy it first"))?;
    Ok(Deployed {
        name: name.to_string(),
        id: service["id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| eyre::eyre!("Service has no ID"))?,
        mode: serde_json::from_value(service["mode"].clone()).ok(),
        current_version: service["current_version"]
            .as_i64()
            .and_then(|version| i32::try_from(version).ok())
            .unwrap_or(0),
    })
}

fn built_name() -> eyre::Result<String> {
    let project = ProjectConfig::find_and_load()?;
    let service = project.as_ref().map(|project| &project.service);
    if service.and_then(|service| service.mode.as_deref()) == Some("web") {
        return service
            .and_then(|service| service.name.clone())
            .ok_or_else(|| eyre::eyre!("Web mode requires [service].name in Cufflink.toml"));
    }
    let output = Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;
    if !output.status.success() {
        eyre::bail!("Failed to build service. Run from a cufflink service directory.");
    }
    let manifest: Value = serde_json::from_str(String::from_utf8(output.stdout)?.trim())?;
    manifest["name"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| eyre::eyre!("No service name in manifest"))
}

pub(crate) async fn built_here(
    config: &CliConfig,
    client: &reqwest::Client,
) -> eyre::Result<Deployed> {
    let name = built_name()?;
    deployed(&listing(config, client).await?, &name)
}

pub(crate) async fn acted_on(
    config: &CliConfig,
    client: &reqwest::Client,
    named: Option<&str>,
) -> eyre::Result<Deployed> {
    let name = match named {
        Some(name) => name.to_string(),
        None => built_name()
            .map_err(|unbuilt| eyre::eyre!("{unbuilt} Or name the service: --service <name>"))?,
    };
    deployed(&listing(config, client).await?, &name)
}

fn delete_request(api_url: &str, id: &str, force: bool) -> (Method, String) {
    let query = if force { "?force=true" } else { "" };
    (
        Method::DELETE,
        format!("{api_url}/api/services/{id}{query}"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Active,
    Inactive,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Active => "active",
            Status::Inactive => "inactive",
        }
    }

    fn done(self) -> &'static str {
        match self {
            Status::Active => "enabled",
            Status::Inactive => "disabled",
        }
    }

    pub(crate) fn named(value: &Value) -> Option<Self> {
        [Status::Active, Status::Inactive]
            .into_iter()
            .find(|status| value.as_str() == Some(status.as_str()))
    }
}

fn status_request(api_url: &str, id: &str, status: Status) -> (Method, String, Value) {
    (
        Method::PATCH,
        format!("{api_url}/api/services/{id}/status"),
        json!({ "status": status.as_str() }),
    )
}

async fn listing(config: &CliConfig, client: &reqwest::Client) -> eyre::Result<Value> {
    let resp = config
        .auth_request(
            client,
            Method::GET,
            &format!("{}/api/services", config.api_url),
        )
        .send()
        .await?;
    if !resp.status().is_success() {
        eyre::bail!("Failed to list services: {}", resp.status());
    }
    Ok(resp.json().await?)
}

async fn find(config: &CliConfig, client: &reqwest::Client, name: &str) -> eyre::Result<String> {
    if let Some(ref env_name) = config.env_name {
        println!("Environment: {}", env_name);
    }
    listed(&listing(config, client).await?, name)
}

fn listed_status(body: &Value, name: &str) -> eyre::Result<Option<Status>> {
    entry(body, name)
        .map(|service| {
            Status::named(&service["status"]).ok_or_else(|| {
                eyre::eyre!(
                    "Service '{name}' is listed with the status {}, which is neither \
                     active nor inactive",
                    service["status"]
                )
            })
        })
        .transpose()
}

pub(crate) async fn status_of(
    config: &CliConfig,
    client: &reqwest::Client,
    name: &str,
) -> eyre::Result<Option<Status>> {
    listed_status(&listing(config, client).await?, name)
}

pub(crate) async fn write_status(
    config: &CliConfig,
    client: &reqwest::Client,
    id: &str,
    status: Status,
) -> eyre::Result<()> {
    let (method, url, body) = status_request(&config.api_url, id, status);
    let resp = config
        .auth_request(client, method, &url)
        .json(&body)
        .send()
        .await?;
    if resp.status().is_success() {
        return Ok(());
    }
    let status_code = resp.status();
    let body = resp.text().await.unwrap_or_default();
    eyre::bail!(
        "Failed to update service status ({}): {}",
        status_code,
        body
    )
}

pub async fn set_status(
    name: &str,
    status: Status,
    env: Option<&str>,
    tenant: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    status_set(config, CliConfig::has_platform_key(), name, status, tenant).await
}

async fn status_set(
    config: CliConfig,
    platform_key: bool,
    name: &str,
    status: Status,
    tenant: Option<&str>,
) -> eyre::Result<()> {
    let config = config.for_tenant(tenant, platform_key)?;
    let client = config.http_client();
    let id = find(&config, &client, name).await?;
    write_status(&config, &client, &id, status).await?;
    println!("Service '{}' {}.", name, status.done());
    Ok(())
}

pub async fn delete(name: &str, yes: bool, force: bool, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let client = config.http_client();
    let id = find(&config, &client, name).await?;

    if !yes {
        println!(
            "This will permanently delete service '{}' and all its configs, deployments, and data.",
            name
        );
        print!("Are you sure? [y/N] ");
        use std::io::Write;
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let (method, url) = delete_request(&config.api_url, &id, force);
    let resp = config.auth_request(&client, method, &url).send().await?;

    if resp.status().is_success() {
        println!("Service '{}' deleted.", name);
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to delete service ({}): {}", status, body);
    }

    Ok(())
}

fn required_features(service: &serde_json::Value) -> String {
    let features: Vec<&str> = service["required_platform_features"]
        .as_array()
        .map(|features| features.iter().filter_map(|f| f.as_str()).collect())
        .unwrap_or_default();
    if features.is_empty() {
        "-".to_string()
    } else {
        features.join(",")
    }
}

pub async fn run(features: bool, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let body = listing(&config, &config.http_client()).await?;
    let services = body["services"].as_array();

    match services {
        Some(services) if !services.is_empty() => {
            use comfy_table::{presets::NOTHING, Table, TableComponent};

            let mut table = Table::new();
            table.load_preset(NOTHING);
            table.set_style(TableComponent::HeaderLines, '-');
            table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
            let header = ["NAME", "VERSION", "MODE", "STATUS", "TENANT", "COMMIT"];
            table.set_header(
                header
                    .into_iter()
                    .chain(features.then_some("FEATURES"))
                    .collect::<Vec<_>>(),
            );

            for svc in services {
                let commit = svc["commit_hash"]
                    .as_str()
                    .map(|h| if h.len() > 7 { &h[..7] } else { h })
                    .unwrap_or("-");
                let row = [
                    svc["name"].as_str().unwrap_or("?").to_string(),
                    format!("v{}", svc["current_version"]),
                    svc["mode"].as_str().unwrap_or("?").to_string(),
                    svc["status"].as_str().unwrap_or("?").to_string(),
                    svc["tenant_slug"].as_str().unwrap_or("?").to_string(),
                    commit.to_string(),
                ];
                table.add_row(
                    row.into_iter()
                        .chain(features.then(|| required_features(svc)))
                        .collect::<Vec<_>>(),
                );
            }

            println!("{table}");
            println!("\n{} service(s)", services.len());
        }
        _ => {
            println!("No services deployed yet.");
            println!("Use `cufflink deploy` to deploy your first service.");
        }
    }

    Ok(())
}

#[cfg(test)]
mod feature_tests {
    use super::required_features;
    use serde_json::json;

    #[test]
    fn required_features_are_listed_or_dashed() {
        assert_eq!(
            required_features(
                &json!({"required_platform_features": ["route_posture", "scoped_permissions"]})
            ),
            "route_posture,scoped_permissions"
        );
        assert_eq!(
            required_features(&json!({"required_platform_features": []})),
            "-"
        );
        assert_eq!(required_features(&json!({"name": "old"})), "-");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listing() -> Value {
        json!({"services": [
            {"id": "11", "name": "reports", "mode": "wasm"},
            {"id": "22", "name": "ext-google", "mode": "native"},
        ]})
    }

    #[test]
    fn the_listing_names_a_service_by_its_id_whatever_its_mode() {
        assert_eq!(listed(&listing(), "ext-google").expect("listed"), "22");
        assert_eq!(listed(&listing(), "reports").expect("listed"), "11");
        assert_eq!(
            listed(&listing(), "gone")
                .expect_err("unlisted")
                .to_string(),
            "Service 'gone' not found"
        );
    }

    #[test]
    fn a_service_acted_on_is_read_from_the_listing() {
        let body = json!({"services": [
            {"id": "11", "name": "reports", "mode": "wasm", "current_version": 4},
            {"id": "22", "name": "site", "mode": "web", "current_version": 2},
            {"id": "33", "name": "undeployed", "mode": "", "current_version": 0},
        ]});
        assert_eq!(
            deployed(&body, "reports").expect("listed"),
            Deployed {
                name: "reports".into(),
                id: "11".into(),
                mode: Some(ServiceMode::Wasm),
                current_version: 4,
            }
        );
        assert_eq!(
            deployed(&body, "site").expect("listed").mode,
            Some(ServiceMode::Web)
        );
        assert_eq!(deployed(&body, "undeployed").expect("listed").mode, None);
        assert_eq!(
            deployed(&body, "gone").expect_err("unlisted").to_string(),
            "Service 'gone' not found on platform: deploy it first"
        );
    }

    #[test]
    fn delete_and_status_requests_name_the_service() {
        assert_eq!(
            delete_request("https://platform.test", "22", false),
            (
                Method::DELETE,
                "https://platform.test/api/services/22".into()
            )
        );
        assert_eq!(
            delete_request("https://platform.test", "22", true),
            (
                Method::DELETE,
                "https://platform.test/api/services/22?force=true".into()
            )
        );
        assert_eq!(
            status_request("https://platform.test", "22", Status::Inactive),
            (
                Method::PATCH,
                "https://platform.test/api/services/22/status".into(),
                json!({"status": "inactive"})
            )
        );
        assert_eq!(
            status_request("https://platform.test", "22", Status::Active).2,
            json!({"status": "active"})
        );
    }

    #[tokio::test]
    async fn a_tenant_the_command_cannot_act_for_is_refused_before_anything_is_sent() {
        use super::super::stub::answering;

        for status in [Status::Active, Status::Inactive] {
            let (config, asked) = answering(Vec::new()).await;
            let refused = status_set(config, false, "notes", status, Some("other"))
                .await
                .expect_err("the tenant's own key acts on acme");
            assert_eq!(
                refused.to_string(),
                "--tenant other needs CUFFLINK_PLATFORM_API_KEY: only a platform admin's key \
                 acts for a tenant other than its own. Without it the command would have \
                 acted on acme, the tenant of this environment in Cufflink.toml. Nothing was \
                 sent"
            );
            assert_eq!(asked.count(), 0, "{status:?}");
        }

        let listed = json!({"services": [{"id": "11", "name": "notes", "status": "active"}]});
        for tenant in [None, Some("acme")] {
            let (config, asked) = answering(vec![
                Some((200, listed.clone())),
                Some((200, json!({"status": "inactive", "name": "notes"}))),
            ])
            .await;
            status_set(config, false, "notes", Status::Inactive, tenant)
                .await
                .expect("its own tenant");
            assert_eq!(
                asked.requests(),
                ["GET /api/services", "PATCH /api/services/11/status"],
                "{tenant:?}"
            );
        }
    }

    #[test]
    fn a_status_is_read_as_the_platform_words_it() {
        assert_eq!(Status::named(&json!("active")), Some(Status::Active));
        assert_eq!(Status::named(&json!("inactive")), Some(Status::Inactive));
        assert_eq!(Status::named(&json!("Active")), None);
        assert_eq!(Status::named(&Value::Null), None);
        assert_eq!(
            [Status::Active, Status::Inactive].map(Status::done),
            ["enabled", "disabled"]
        );
    }

    #[test]
    fn a_listed_service_has_a_status_and_one_not_listed_has_none() {
        let listing = json!({"services": [
            {"id": "11", "name": "reports", "status": "active"},
            {"id": "22", "name": "notes", "status": "inactive"},
            {"id": "33", "name": "odd", "status": "paused"},
        ]});
        let status = |name| listed_status(&listing, name);
        assert_eq!(status("reports").expect("listed"), Some(Status::Active));
        assert_eq!(status("notes").expect("listed"), Some(Status::Inactive));
        assert_eq!(status("gone").expect("not listed"), None);
        assert_eq!(listed_status(&json!({}), "notes").expect("empty"), None);
        assert_eq!(
            status("odd").expect_err("unknown").to_string(),
            "Service 'odd' is listed with the status \"paused\", which is neither \
             active nor inactive"
        );
    }
}
