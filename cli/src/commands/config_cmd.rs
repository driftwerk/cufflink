use super::services::{self, Deployed};
use super::version::{self, PlatformVersion};
use crate::config::CliConfig;
use crate::project_config::ProjectConfig;
use cufflink_types::features::CONFIG_APPLY;
use cufflink_types::ServiceMode;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

async fn get_service_id(config: &CliConfig) -> eyre::Result<String> {
    services::built_here(config, &config.http_client())
        .await
        .map(|service| service.id)
}

pub async fn list(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let service_id = get_service_id(&config).await?;

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services/{}/config", config.api_url, service_id),
        )
        .send()
        .await?;

    if resp.status().is_success() {
        let body: serde_json::Value = resp.json().await?;
        if let Some(configs) = body["configs"].as_array() {
            if configs.is_empty() {
                println!("No configuration values set.");
            } else {
                use comfy_table::{presets::NOTHING, Table, TableComponent};

                let mut table = Table::new();
                table.load_preset(NOTHING);
                table.set_style(TableComponent::HeaderLines, '-');
                table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
                table.set_header(vec!["KEY", "VALUE", "SECRET"]);

                for c in configs {
                    let secret = if c["is_secret"].as_bool() == Some(true) {
                        "yes"
                    } else {
                        "no"
                    };
                    table.add_row(vec![
                        c["key"].as_str().unwrap_or(""),
                        c["value"].as_str().unwrap_or(""),
                        secret,
                    ]);
                }

                println!("{table}");
            }
        }
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to list config ({}): {}", status, body);
    }

    Ok(())
}

pub async fn set(key: &str, value: &str, is_secret: bool, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let service_id = get_service_id(&config).await?;

    let payload = serde_json::json!({
        "key": key,
        "value": value,
        "is_secret": is_secret,
    });

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::PUT,
            &format!("{}/api/services/{}/config", config.api_url, service_id),
        )
        .json(&payload)
        .send()
        .await?;

    if resp.status().is_success() {
        println!("Config '{}' set successfully", key);
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to set config ({}): {}", status, body);
    }

    Ok(())
}

pub async fn delete(key: &str, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let service_id = get_service_id(&config).await?;

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::DELETE,
            &format!(
                "{}/api/services/{}/config/{}",
                config.api_url, service_id, key
            ),
        )
        .send()
        .await?;

    if resp.status().is_success() {
        println!("Config '{}' deleted", key);
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to delete config ({}): {}", status, body);
    }

    Ok(())
}

#[derive(Clone, PartialEq, Eq, Serialize)]
struct Entry {
    key: String,
    value: String,
    is_secret: bool,
}

fn declared(configs: &HashMap<String, String>, secrets: HashMap<String, String>) -> Vec<Entry> {
    configs
        .iter()
        .map(|(key, value)| (key.clone(), (value.clone(), false)))
        .chain(secrets.into_iter().map(|(key, value)| (key, (value, true))))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(key, (value, is_secret))| Entry {
            key,
            value,
            is_secret,
        })
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq, Deserialize)]
struct ConfigDiff {
    #[serde(default)]
    added: Vec<String>,
    #[serde(default)]
    changed: Vec<String>,
    #[serde(default)]
    removed: Vec<String>,
    #[serde(default)]
    unchanged: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tense {
    Done,
    Pending,
}

impl ConfigDiff {
    fn differs(&self) -> bool {
        [&self.added, &self.changed, &self.removed]
            .iter()
            .any(|keys| !keys.is_empty())
    }

    fn lines(&self) -> Vec<String> {
        [
            ("+", &self.added),
            ("~", &self.changed),
            ("-", &self.removed),
        ]
        .into_iter()
        .flat_map(|(mark, keys)| keys.iter().map(move |key| format!("  {mark} {key}")))
        .collect()
    }

    fn summary(&self, tense: Tense) -> String {
        let [added, changed, removed] = match tense {
            Tense::Done => ["added", "changed", "removed"],
            Tense::Pending => ["to add", "to change", "to remove"],
        };
        format!(
            "{} {added}, {} {changed}, {} {removed}, {} unchanged",
            self.added.len(),
            self.changed.len(),
            self.removed.len(),
            self.unchanged.len()
        )
    }
}

#[derive(Debug)]
pub struct Drifted {
    pub service: String,
}

impl Drifted {
    pub const EXIT_CODE: u8 = 2;
}

impl std::fmt::Display for Drifted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the config of {} differs from what Cufflink.toml declares; \
             `cufflink config apply` brings the platform to it",
            self.service
        )
    }
}

impl std::error::Error for Drifted {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyFlags {
    pub prune: bool,
    pub check: bool,
}

fn environment(project: &ProjectConfig, env: Option<&str>) -> eyre::Result<String> {
    let name = project
        .env_name(env)
        .ok_or_else(|| eyre::eyre!("No environment specified"))?;
    project.get_env(&name).map(|_| name)
}

fn unreferenced_warning(unreferenced: &[String], env_name: &str) -> Option<String> {
    let example = unreferenced.first()?;
    let names: String = unreferenced
        .iter()
        .map(|name| format!("    - {name}\n"))
        .collect();
    Some(format!(
        "\n  WARNING: {} secret(s) in the '{env_name}' vault are not mapped in \
         [environments.{env_name}.secrets] and will NOT be synced:\n{names}  \
         Add a mapping (e.g. MY_ENV_VAR = \"{example}\") or delete the entry if it is stale.\n",
        unreferenced.len()
    ))
}

fn unreferenced_refusal(unreferenced: &[String], env_name: &str) -> Option<String> {
    (!unreferenced.is_empty()).then(|| {
        format!(
            "{} secret(s) in the '{env_name}' vault are not mapped in \
             [environments.{env_name}.secrets]: {}. With --prune a forgotten mapping is a key \
             removed from the platform: map each, or delete the vault entry if it is stale.",
            unreferenced.len(),
            unreferenced.join(", ")
        )
    })
}

fn declared_by(project: &ProjectConfig, env_name: &str, prune: bool) -> eyre::Result<Vec<Entry>> {
    let env_config = project.get_env(env_name)?;
    let unreferenced = unreferenced_vault_secrets(project, env_name, &env_config.secrets);
    if let Some(refusal) = prune
        .then(|| unreferenced_refusal(&unreferenced, env_name))
        .flatten()
    {
        eyre::bail!(refusal);
    }
    if let Some(warning) = unreferenced_warning(&unreferenced, env_name) {
        eprintln!("{warning}");
    }
    resolve_secret_refs(project, env_name, &env_config.secrets)
        .map(|secrets| declared(&env_config.config, secrets))
}

async fn asked_to_apply(
    config: &CliConfig,
    client: &reqwest::Client,
    service_id: &str,
    entries: &[Entry],
    prune: bool,
    dry_run: bool,
) -> eyre::Result<reqwest::Response> {
    Ok(config
        .auth_request(
            client,
            reqwest::Method::POST,
            &format!("{}/api/services/{service_id}/config/apply", config.api_url),
        )
        .json(&serde_json::json!({"entries": entries, "prune": prune, "dry_run": dry_run}))
        .send()
        .await?)
}

async fn diff_of(answer: reqwest::Response) -> eyre::Result<ConfigDiff> {
    if answer.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
        eyre::bail!(
            "Failed to apply config (405 Method Not Allowed): the replica that answered \
             predates config_apply; is a platform upgrade in progress?"
        );
    }
    Ok(serde_json::from_value(
        super::json_answer(answer, "apply config").await?,
    )?)
}

async fn applied(
    config: &CliConfig,
    client: &reqwest::Client,
    service_id: &str,
    entries: &[Entry],
    prune: bool,
    dry_run: bool,
) -> eyre::Result<ConfigDiff> {
    diff_of(asked_to_apply(config, client, service_id, entries, prune, dry_run).await?).await
}

async fn put_each(
    config: &CliConfig,
    client: &reqwest::Client,
    service_id: &str,
    entries: &[Entry],
) -> eyre::Result<()> {
    let url = format!("{}/api/services/{service_id}/config", config.api_url);
    for entry in entries {
        let resp = config
            .auth_request(client, reqwest::Method::PUT, &url)
            .json(entry)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            eyre::bail!(
                "Failed to sync {} '{}' ({status}): {body}",
                if entry.is_secret { "secret" } else { "config" },
                entry.key
            );
        }
    }
    let secrets = entries.iter().filter(|entry| entry.is_secret).count();
    println!(
        "  Synced {} config(s) and {} secret(s)",
        entries.len() - secrets,
        secrets
    );
    Ok(())
}

fn check_report(service: &str, diff: &ConfigDiff, prune: bool) -> Vec<String> {
    match (diff.differs(), prune) {
        (true, _) => std::iter::once(format!("Config of {service} differs from Cufflink.toml:"))
            .chain(diff.lines())
            .chain([format!("  {}", diff.summary(Tense::Pending))])
            .collect(),
        (false, true) => vec![format!(
            "Config of {service} matches Cufflink.toml ({} key(s))",
            diff.unchanged.len()
        )],
        (false, false) => vec![format!(
            "Config of {service} holds what Cufflink.toml declares ({} key(s)); keys it does \
             not declare are not checked without --prune",
            diff.unchanged.len()
        )],
    }
}

fn apply_report(service: &Deployed, diff: &ConfigDiff) -> Vec<String> {
    let restarts = match &service.mode {
        Some(mode @ (ServiceMode::Web | ServiceMode::Native)) if diff.differs() => Some(format!(
            "  {} is a {} service: a process that is running reads this config when it next \
             starts",
            service.name,
            mode.as_str()
        )),
        _ => None,
    };
    std::iter::once(format!(
        "Applied config to {}: {}",
        service.name,
        diff.summary(Tense::Done)
    ))
    .chain(diff.lines())
    .chain(restarts)
    .collect()
}

fn removal_notice(
    service: &str,
    env_name: &str,
    declares: usize,
    removed: &[String],
) -> Vec<String> {
    let nothing_declared = (declares == 0).then(|| {
        format!(
            "[environments.{env_name}] declares no config: --prune removes every key of {service}"
        )
    });
    let removing = (!removed.is_empty()).then(|| {
        format!(
            "Removing {} key(s) of {service} that [environments.{env_name}] does not declare: {}",
            removed.len(),
            removed.join(", ")
        )
    });
    nothing_declared.into_iter().chain(removing).collect()
}

fn foreign_refusal(project: &ProjectConfig, service: &str) -> Option<String> {
    project.foreign_to(service).map(|foreign| {
        format!(
            "--service {service} is not this project's service: {}",
            foreign.refusal("is not applied to, or checked against,", "Run")
        )
    })
}

fn unsupported(platform: &PlatformVersion, flags: ApplyFlags) -> String {
    let remedy = match flags.check || flags.prune {
        true => {
            "Upgrade the platform: an older one can neither check a config nor remove a key \
             (`cufflink config sync` only adds and changes)."
        }
        false => "Upgrade the platform, or use `cufflink config sync`.",
    };
    format!(
        "platform {} cannot apply a config as one set (needs {CONFIG_APPLY}). {remedy}",
        platform.label()
    )
}

async fn apply_to(
    config: &CliConfig,
    client: &reqwest::Client,
    service: &Deployed,
    env_name: &str,
    entries: &[Entry],
    flags: ApplyFlags,
) -> eyre::Result<()> {
    let print = |lines: Vec<String>| lines.iter().for_each(|line| println!("{line}"));
    if flags.check || flags.prune {
        let pending = applied(config, client, &service.id, entries, flags.prune, true).await?;
        if flags.check {
            print(check_report(&service.name, &pending, flags.prune));
            return match pending.differs() {
                true => Err(Drifted {
                    service: service.name.clone(),
                }
                .into()),
                false => Ok(()),
            };
        }
        print(removal_notice(
            &service.name,
            env_name,
            entries.len(),
            &pending.removed,
        ));
    }
    let diff = applied(config, client, &service.id, entries, flags.prune, false).await?;
    print(apply_report(service, &diff));
    Ok(())
}

pub async fn apply(
    flags: ApplyFlags,
    service: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }
    let project =
        ProjectConfig::find_and_load()?.ok_or_else(|| eyre::eyre!("No Cufflink.toml found"))?;
    apply_from(&config, &project, flags, service, env).await
}

async fn apply_from(
    config: &CliConfig,
    project: &ProjectConfig,
    flags: ApplyFlags,
    service: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let env_name = environment(project, env)?;
    if let Some(refusal) = service.and_then(|service| foreign_refusal(project, service)) {
        eyre::bail!(refusal);
    }
    let entries = declared_by(project, &env_name, flags.prune)?;
    let client = config.http_client();
    let platform = version::asked(config, &client).await?;
    if !platform.offers(CONFIG_APPLY) {
        eyre::bail!(unsupported(&platform, flags));
    }
    let service = services::acted_on(config, &client, service).await?;
    apply_to(config, &client, &service, &env_name, &entries, flags).await
}

pub async fn sync(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let service_id = get_service_id(&config).await?;

    let project =
        ProjectConfig::find_and_load()?.ok_or_else(|| eyre::eyre!("No Cufflink.toml found"))?;
    let env_name = environment(&project, env)?;
    let platform = version::asked(&config, &config.http_client()).await?;

    sync_to_platform(&config, &service_id, &project, &env_name, &platform).await
}

pub async fn sync_to_platform(
    cli_config: &CliConfig,
    service_id: &str,
    project: &ProjectConfig,
    env_name: &str,
    platform: &PlatformVersion,
) -> eyre::Result<()> {
    let entries = declared_by(project, env_name, false)?;
    if entries.is_empty() {
        return Ok(());
    }
    synced(
        cli_config,
        &cli_config.http_client(),
        service_id,
        &entries,
        platform,
    )
    .await
}

async fn synced(
    config: &CliConfig,
    client: &reqwest::Client,
    service_id: &str,
    entries: &[Entry],
    platform: &PlatformVersion,
) -> eyre::Result<()> {
    if !platform.offers(CONFIG_APPLY) {
        return put_each(config, client, service_id, entries).await;
    }
    let answer = asked_to_apply(config, client, service_id, entries, false, false).await?;
    if answer.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
        return put_each(config, client, service_id, entries).await;
    }
    let diff = diff_of(answer).await?;
    println!("  Applied config: {}", diff.summary(Tense::Done));
    diff.lines().iter().for_each(|line| println!("  {line}"));
    Ok(())
}

pub(crate) fn unreferenced_vault_secrets(
    project: &ProjectConfig,
    env_name: &str,
    secret_refs: &HashMap<String, String>,
) -> Vec<String> {
    let Some(stored) = super::secrets_cmd::vault_secret_names(project, env_name) else {
        return Vec::new();
    };
    let referenced: std::collections::HashSet<&str> =
        secret_refs.values().map(String::as_str).collect();
    let mut unused: Vec<String> = stored
        .into_iter()
        .filter(|name| !referenced.contains(name.as_str()))
        .collect();
    unused.sort();
    unused
}

fn resolve_secret_refs(
    project: &ProjectConfig,
    env_name: &str,
    secret_refs: &HashMap<String, String>,
) -> eyre::Result<HashMap<String, String>> {
    if secret_refs.is_empty() {
        return Ok(HashMap::new());
    }
    let store = super::secrets_cmd::load_store(project, env_name)?;
    let mut out = HashMap::with_capacity(secret_refs.len());
    for (config_key, store_name) in secret_refs {
        let value: String = store.get(store_name).map_err(|e| {
            eyre::eyre!(
                "Secret '{}' not found in securestore for '{}': {}",
                store_name,
                env_name,
                e
            )
        })?;
        out.insert(config_key.clone(), value);
    }
    Ok(out)
}

pub fn resolve_local_envs(
    project: &ProjectConfig,
    env_name: &str,
) -> eyre::Result<HashMap<String, String>> {
    let env = project.get_env(env_name)?;
    let mut out = env.config.clone();
    out.extend(resolve_secret_refs(project, env_name, &env.secrets)?);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::super::stub::answering;
    use super::*;
    use crate::project_config::EnvironmentConfig;
    use securestore::{KeySource, SecretsManager};
    use tempfile::TempDir;

    fn make_project_with_vault(env_name: &str) -> (TempDir, ProjectConfig) {
        let tmp = tempfile::tempdir().unwrap();
        let project_dir = tmp.path().to_path_buf();
        let secrets_dir = project_dir.join("secrets");
        std::fs::create_dir_all(&secrets_dir).unwrap();

        let key_path = secrets_dir.join(format!("{}.key", env_name));
        let vault_path = secrets_dir.join(format!("{}.json", env_name));
        let store_for_key = SecretsManager::new(KeySource::Csprng).unwrap();
        store_for_key.export_key(&key_path).unwrap();

        let mut store = SecretsManager::new(KeySource::Path(&key_path)).unwrap();
        store.set("google_maps:api_key", "AIzaSyTEST");
        store.set("mail:api_key", "mail-test-token");
        store.save_as(&vault_path).unwrap();

        let project = ProjectConfig {
            service: Default::default(),
            environments: HashMap::new(),
            project_dir,
        };
        (tmp, project)
    }

    fn env_with(config: &[(&str, &str)], secrets: &[(&str, &str)]) -> EnvironmentConfig {
        EnvironmentConfig {
            api_url: "http://localhost:8080".to_string(),
            tenant: "default".to_string(),
            api_key_env: None,
            api_key: None,
            keycloak_url: None,
            keycloak_realm: None,
            keycloak_client_id: None,
            config: config
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            secrets: secrets
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn reports_vault_secrets_no_mapping_references() {
        let (_tmp, project) = make_project_with_vault("prod");
        let refs = HashMap::from([(
            "GOOGLE_MAPS_KEY".to_string(),
            "google_maps:api_key".to_string(),
        )]);
        assert_eq!(
            unreferenced_vault_secrets(&project, "prod", &refs),
            vec!["mail:api_key".to_string()]
        );
    }

    #[test]
    fn reports_every_secret_when_the_env_maps_none() {
        let (_tmp, project) = make_project_with_vault("prod");
        let mut all = unreferenced_vault_secrets(&project, "prod", &HashMap::new());
        all.sort();
        assert_eq!(
            all,
            vec![
                "google_maps:api_key".to_string(),
                "mail:api_key".to_string()
            ]
        );
    }

    #[test]
    fn says_nothing_when_every_secret_is_mapped() {
        let (_tmp, project) = make_project_with_vault("prod");
        let refs = HashMap::from([
            ("A".to_string(), "google_maps:api_key".to_string()),
            ("B".to_string(), "mail:api_key".to_string()),
        ]);
        assert!(unreferenced_vault_secrets(&project, "prod", &refs).is_empty());
    }

    #[test]
    fn says_nothing_when_there_is_no_vault_for_the_env() {
        let (_tmp, project) = make_project_with_vault("prod");
        assert!(unreferenced_vault_secrets(&project, "demo", &HashMap::new()).is_empty());
    }

    fn pairs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn entry(key: &str, value: &str, is_secret: bool) -> Entry {
        Entry {
            key: key.into(),
            value: value.into(),
            is_secret,
        }
    }

    fn keys(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|key| key.to_string()).collect()
    }

    fn diff(added: &[&str], changed: &[&str], removed: &[&str], unchanged: &[&str]) -> ConfigDiff {
        ConfigDiff {
            added: keys(added),
            changed: keys(changed),
            removed: keys(removed),
            unchanged: keys(unchanged),
        }
    }

    fn answer(diff: &ConfigDiff, applied: bool) -> Option<(u16, serde_json::Value)> {
        Some((
            200,
            serde_json::json!({
                "added": diff.added, "changed": diff.changed, "removed": diff.removed,
                "unchanged": diff.unchanged, "applied": applied,
            }),
        ))
    }

    fn service(mode: Option<ServiceMode>) -> Deployed {
        Deployed {
            name: "notes".into(),
            id: "7".into(),
            mode,
            current_version: 3,
        }
    }

    fn wasm() -> Deployed {
        service(Some(ServiceMode::Wasm))
    }

    fn platform(version: &str, supports: &[&str]) -> PlatformVersion {
        PlatformVersion {
            api_version: 1,
            platform_version: Some(version.into()),
            supports: keys(supports),
        }
    }

    const APPLY: &str = "POST /api/services/7/config/apply";
    const PUT: &str = "PUT /api/services/7/config";
    const CHECK: ApplyFlags = ApplyFlags {
        prune: false,
        check: true,
    };
    const PRUNE: ApplyFlags = ApplyFlags {
        prune: true,
        check: false,
    };

    fn asked_to(bodies: &[serde_json::Value]) -> Vec<(bool, bool)> {
        bodies
            .iter()
            .map(|body| (body["prune"] == true, body["dry_run"] == true))
            .collect()
    }

    #[test]
    fn a_key_is_declared_once_and_a_secret_wins() {
        let declared = declared(
            &pairs(&[
                ("ZONE", "eu"),
                ("TOKEN", "from-config"),
                ("API_URL", "https://x"),
            ]),
            pairs(&[("TOKEN", "from-vault"), ("KEY", "k")]),
        );
        assert!(
            declared
                == [
                    entry("API_URL", "https://x", false),
                    entry("KEY", "k", true),
                    entry("TOKEN", "from-vault", true),
                    entry("ZONE", "eu", false),
                ],
            "each key once, in key order, the secret over the config of its name"
        );
        assert!(super::declared(&HashMap::new(), HashMap::new()).is_empty());
    }

    #[test]
    fn a_diff_is_printed_as_its_keys_and_counts() {
        let some = diff(&["NEW"], &["URL", "ZONE"], &["OLD"], &["A", "B"]);
        assert!(some.differs());
        assert_eq!(some.lines(), ["  + NEW", "  ~ URL", "  ~ ZONE", "  - OLD"]);
        assert_eq!(
            some.summary(Tense::Done),
            "1 added, 2 changed, 1 removed, 2 unchanged"
        );
        assert_eq!(
            some.summary(Tense::Pending),
            "1 to add, 2 to change, 1 to remove, 2 unchanged"
        );
        let none = diff(&[], &[], &[], &["A"]);
        assert!(!none.differs());
        assert!(none.lines().is_empty());
        let read: ConfigDiff =
            serde_json::from_value(serde_json::json!({"added": ["A"], "applied": true}))
                .expect("an answer");
        assert_eq!(read, diff(&["A"], &[], &[], &[]));
    }

    #[test]
    fn a_check_says_whether_the_config_matches_how_it_differs_and_what_it_did_not_look_at() {
        let same = diff(&[], &[], &[], &["A", "B"]);
        assert_eq!(
            check_report("notes", &same, true),
            ["Config of notes matches Cufflink.toml (2 key(s))"]
        );
        assert_eq!(
            check_report("notes", &same, false),
            [
                "Config of notes holds what Cufflink.toml declares (2 key(s)); keys it does not \
              declare are not checked without --prune"
            ]
        );
        for prune in [true, false] {
            assert_eq!(
                check_report("notes", &diff(&["NEW"], &[], &["OLD"], &["A"]), prune),
                [
                    "Config of notes differs from Cufflink.toml:",
                    "  + NEW",
                    "  - OLD",
                    "  1 to add, 0 to change, 1 to remove, 1 unchanged",
                ]
            );
        }
    }

    #[test]
    fn an_apply_says_what_it_did_and_when_a_running_process_reads_it() {
        let changed = diff(&[], &["URL"], &[], &["A"]);
        for mode in [Some(ServiceMode::Wasm), Some(ServiceMode::Crud), None] {
            assert_eq!(
                apply_report(&service(mode), &changed),
                [
                    "Applied config to notes: 0 added, 1 changed, 0 removed, 1 unchanged",
                    "  ~ URL",
                ]
            );
        }
        for (mode, named) in [(ServiceMode::Web, "web"), (ServiceMode::Native, "native")] {
            let restarting = service(Some(mode));
            assert_eq!(
                apply_report(&restarting, &changed)
                    .last()
                    .map(String::as_str),
                Some(
                    format!(
                        "  notes is a {named} service: a process that is running reads this \
                         config when it next starts"
                    )
                    .as_str()
                )
            );
            assert_eq!(
                apply_report(&restarting, &diff(&[], &[], &[], &["A"])),
                ["Applied config to notes: 0 added, 0 changed, 0 removed, 1 unchanged"],
                "nothing changed, so nothing waits for a restart"
            );
        }
    }

    #[test]
    fn an_unmapped_vault_secret_is_a_warning_and_a_refusal_to_prune() {
        let unmapped = keys(&["mail:api_key", "stripe:key"]);
        assert_eq!(
            unreferenced_warning(&unmapped, "prod").as_deref(),
            Some(
                "\n  WARNING: 2 secret(s) in the 'prod' vault are not mapped in \
                 [environments.prod.secrets] and will NOT be synced:\n    \
                 - mail:api_key\n    - stripe:key\n  \
                 Add a mapping (e.g. MY_ENV_VAR = \"mail:api_key\") or delete the entry \
                 if it is stale.\n"
            )
        );
        assert_eq!(
            unreferenced_refusal(&unmapped, "prod").as_deref(),
            Some(
                "2 secret(s) in the 'prod' vault are not mapped in [environments.prod.secrets]: \
                 mail:api_key, stripe:key. With --prune a forgotten mapping is a key \
                 removed from the platform: map each, or delete the vault entry if it is stale."
            )
        );
        assert_eq!(unreferenced_warning(&[], "prod"), None);
        assert_eq!(unreferenced_refusal(&[], "prod"), None);
    }

    #[test]
    fn what_is_declared_is_read_whole_before_the_platform_is_asked_anything() {
        let (_tmp, mut project) = make_project_with_vault("prod");
        project.environments.insert(
            "prod".to_string(),
            env_with(
                &[("ZONE", "eu"), ("TOKEN", "from-config")],
                &[("TOKEN", "google_maps:api_key")],
            ),
        );
        let declared = declared_by(&project, "prod", false).expect("declared");
        assert!(
            declared
                == [
                    entry("TOKEN", "AIzaSyTEST", true),
                    entry("ZONE", "eu", false)
                ],
            "the vault's value for a key in both sections; the unmapped entry is only warned of"
        );
        assert_eq!(
            declared_by(&project, "prod", true)
                .err()
                .map(|refused| refused.to_string()),
            unreferenced_refusal(&keys(&["mail:api_key"]), "prod"),
            "a set that is to prune is refused for it, before the vault is opened"
        );

        project.environments.insert(
            "prod".to_string(),
            env_with(&[("ZONE", "eu")], &[("TOKEN", "missing:secret")]),
        );
        assert!(declared_by(&project, "prod", false)
            .err()
            .is_some_and(|missing| missing.to_string().contains("missing:secret")));
    }

    #[tokio::test]
    async fn a_sync_writes_nothing_when_the_vault_lacks_a_secret_or_nothing_is_declared() {
        let (_tmp, mut project) = make_project_with_vault("prod");
        project.environments.insert(
            "prod".to_string(),
            env_with(&[("ZONE", "eu")], &[("TOKEN", "missing:secret")]),
        );
        project
            .environments
            .insert("empty".to_string(), env_with(&[], &[]));
        let (config, asked) = answering(Vec::new()).await;
        let applying = platform("0.30.0", &["config_apply"]);

        assert!(sync_to_platform(&config, "7", &project, "prod", &applying)
            .await
            .is_err());
        sync_to_platform(&config, "7", &project, "empty", &applying)
            .await
            .expect("nothing to sync");
        assert_eq!(
            asked.count(),
            0,
            "no key is written before the vault is read"
        );
    }

    #[test]
    fn a_pruning_apply_says_what_it_is_about_to_remove_and_from_where() {
        assert_eq!(
            removal_notice("notes", "staging", 3, &keys(&["OLD", "STALE"])),
            ["Removing 2 key(s) of notes that [environments.staging] does not declare: OLD, STALE"]
        );
        assert_eq!(
            removal_notice("notes", "staging", 0, &keys(&["A"])),
            [
                "[environments.staging] declares no config: --prune removes every key of notes",
                "Removing 1 key(s) of notes that [environments.staging] does not declare: A",
            ]
        );
        assert!(removal_notice("notes", "staging", 3, &[]).is_empty());
    }

    #[tokio::test]
    async fn a_project_that_names_its_service_does_not_prune_anothers() {
        let (_tmp, mut project) = make_project_with_vault("prod");
        project.environments.insert(
            "prod".to_string(),
            env_with(&[("ZONE", "eu")], &[("TOKEN", "missing:secret")]),
        );
        assert_eq!(foreign_refusal(&project, "other"), None);
        project.service.name = Some("notes".into());
        assert_eq!(foreign_refusal(&project, "notes"), None);

        let both = ApplyFlags {
            prune: true,
            check: true,
        };
        for flags in [ApplyFlags::default(), PRUNE, CHECK, both] {
            let (config, asked) = answering(Vec::new()).await;
            let refused = apply_from(&config, &project, flags, Some("other"), Some("prod"))
                .await
                .expect_err("another service's config");
            assert_eq!(
                refused.to_string(),
                "--service other is not this project's service: the Cufflink.toml here is of \
                 notes, whose config is not applied to, or checked against, other. Run it \
                 where Cufflink.toml is of other, or names no service",
                "{flags:?}: refused before the vault, which lacks the secret, is read"
            );
            assert_eq!(asked.count(), 0, "{flags:?}");
        }
    }

    #[test]
    fn a_platform_that_cannot_apply_is_told_what_can_be_done_instead() {
        let older = platform("0.29.2", &["async_deploy"]);
        assert_eq!(
            unsupported(&older, ApplyFlags::default()),
            "platform v0.29.2 cannot apply a config as one set (needs config_apply). Upgrade \
             the platform, or use `cufflink config sync`."
        );
        for flags in [CHECK, PRUNE] {
            assert_eq!(
                unsupported(&older, flags),
                "platform v0.29.2 cannot apply a config as one set (needs config_apply). \
                 Upgrade the platform: an older one can neither check a config nor remove a \
                 key (`cufflink config sync` only adds and changes).",
                "a sync would write what a check must not, and removes nothing"
            );
        }
    }

    #[tokio::test]
    async fn a_check_changes_nothing_and_fails_only_when_the_config_differs() {
        let entries = [entry("A", "1", false), entry("TOKEN", "t", true)];
        let checking = |answers| {
            let entries = &entries;
            async move {
                let (config, asked) = answering(answers).await;
                let checked = apply_to(
                    &config,
                    &config.http_client(),
                    &wasm(),
                    "staging",
                    entries,
                    CHECK,
                )
                .await;
                (checked, asked)
            }
        };

        let same = diff(&[], &[], &[], &["A", "TOKEN"]);
        let (matched, asked) = checking(vec![answer(&same, false)]).await;
        matched.expect("a config that matches");
        assert_eq!(asked.requests(), [APPLY]);
        assert_eq!(
            asked.bodies(),
            [serde_json::json!({
                "entries": [
                    {"key": "A", "value": "1", "is_secret": false},
                    {"key": "TOKEN", "value": "t", "is_secret": true},
                ],
                "prune": false,
                "dry_run": true,
            })]
        );

        let drift = diff(&["A"], &[], &[], &["TOKEN"]);
        let (drifted, asked) = checking(vec![answer(&drift, false)]).await;
        assert_eq!(
            drifted
                .expect_err("a config that differs")
                .downcast_ref::<Drifted>()
                .map(ToString::to_string),
            Some(
                "the config of notes differs from what Cufflink.toml declares; \
                 `cufflink config apply` brings the platform to it"
                    .to_string()
            )
        );
        assert_eq!(asked.count(), 1, "a check applies nothing");

        let (failed, _) = checking(vec![Some((
            404,
            serde_json::json!({"error": "Service not found"}),
        ))])
        .await;
        let failed = failed.expect_err("a check that was not answered");
        assert!(failed.downcast_ref::<Drifted>().is_none(), "exit 1, not 2");
        assert_eq!(
            failed.to_string(),
            r#"Failed to apply config (404 Not Found): {"error":"Service not found"}"#
        );

        let (failed, _) = checking(vec![Some((405, serde_json::Value::Null))]).await;
        assert_eq!(
            failed
                .expect_err("a replica of an older platform")
                .to_string(),
            "Failed to apply config (405 Method Not Allowed): the replica that answered \
             predates config_apply; is a platform upgrade in progress?"
        );

        let (config, asked) = answering(vec![answer(&same, false)]).await;
        let pruning = ApplyFlags {
            prune: true,
            check: true,
        };
        apply_to(
            &config,
            &config.http_client(),
            &wasm(),
            "staging",
            &entries,
            pruning,
        )
        .await
        .expect("a config that matches");
        assert_eq!(asked_to(&asked.bodies()), [(true, true)]);
    }

    #[tokio::test]
    async fn an_apply_is_one_request_and_a_pruning_one_is_looked_at_first() {
        let entries = [entry("A", "1", false)];
        let applying = |flags, answers| {
            let entries = &entries;
            async move {
                let (config, asked) = answering(answers).await;
                let applied = apply_to(
                    &config,
                    &config.http_client(),
                    &wasm(),
                    "staging",
                    entries,
                    flags,
                )
                .await;
                (applied, asked)
            }
        };

        let done = diff(&["A"], &[], &[], &[]);
        let (applied, asked) = applying(ApplyFlags::default(), vec![answer(&done, true)]).await;
        applied.expect("applied");
        assert_eq!(asked_to(&asked.bodies()), [(false, false)]);

        let pruned = diff(&[], &[], &["OLD"], &["A"]);
        let (applied, asked) =
            applying(PRUNE, vec![answer(&pruned, false), answer(&pruned, true)]).await;
        applied.expect("applied");
        assert_eq!(asked.requests(), [APPLY, APPLY]);
        assert_eq!(
            asked_to(&asked.bodies()),
            [(true, true), (true, false)],
            "what a prune removes is read, and printed, before it is removed"
        );

        let (applied, asked) = applying(
            PRUNE,
            vec![Some((503, serde_json::json!({"error": "down"})))],
        )
        .await;
        assert!(applied.is_err());
        assert_eq!(
            asked.count(),
            1,
            "nothing is removed when what would be could not be read"
        );
    }

    #[tokio::test]
    async fn a_sync_is_one_set_where_the_platform_applies_one_and_key_by_key_elsewhere() {
        let entries = [entry("A", "1", false), entry("TOKEN", "t", true)];
        let saved = || Some((200, serde_json::json!({"status": "saved"})));
        let syncing = |platform: PlatformVersion, answers| {
            let entries = &entries;
            async move {
                let (config, asked) = answering(answers).await;
                let synced = synced(&config, &config.http_client(), "7", entries, &platform).await;
                (synced, asked)
            }
        };
        let each_key = [
            serde_json::json!({"key": "A", "value": "1", "is_secret": false}),
            serde_json::json!({"key": "TOKEN", "value": "t", "is_secret": true}),
        ];

        let (synced, asked) = syncing(
            platform("0.30.0", &["async_deploy", "config_apply"]),
            vec![answer(&diff(&["A", "TOKEN"], &[], &[], &[]), true)],
        )
        .await;
        synced.expect("applied");
        assert_eq!(asked.requests(), [APPLY]);
        let sent = &asked.bodies()[0];
        assert_eq!(
            (
                asked_to(&asked.bodies()),
                sent["entries"].as_array().map(Vec::len)
            ),
            (vec![(false, false)], Some(2)),
            "a deploy's config step removes nothing: a key set by hand survives it"
        );

        let (synced, asked) = syncing(
            platform("0.29.2", &["async_deploy"]),
            vec![saved(), saved()],
        )
        .await;
        synced.expect("synced");
        assert_eq!(
            asked.requests(),
            [PUT, PUT],
            "a platform that predates the apply is written to as it always was"
        );
        assert_eq!(asked.bodies(), each_key);

        let (synced, asked) = syncing(
            platform("0.30.0", &["config_apply"]),
            vec![Some((405, serde_json::Value::Null)), saved(), saved()],
        )
        .await;
        synced.expect("synced key by key");
        assert_eq!(
            asked.requests(),
            [APPLY, PUT, PUT],
            "a replica that predates the apply answered it: it is written to key by key"
        );
        assert_eq!(asked.bodies()[1..], each_key);

        let (failed, asked) = syncing(
            platform("0.29.2", &[]),
            vec![saved(), Some((500, serde_json::json!({"error": "down"})))],
        )
        .await;
        assert_eq!(
            failed.expect_err("a key that was not saved").to_string(),
            r#"Failed to sync secret 'TOKEN' (500 Internal Server Error): {"error":"down"}"#
        );
        assert_eq!(asked.count(), 2);

        let (failed, asked) = syncing(
            platform("0.30.0", &["config_apply"]),
            vec![Some((
                404,
                serde_json::json!({"error": "Service not found"}),
            ))],
        )
        .await;
        assert!(failed.is_err());
        assert_eq!(
            asked.count(),
            1,
            "only a 405 is a platform that predates it"
        );
    }

    #[test]
    fn resolves_config_only_passes_values_through() {
        let (_tmp, mut project) = make_project_with_vault("staging");
        project.environments.insert(
            "staging".to_string(),
            env_with(&[("NEXT_PUBLIC_API_URL", "https://api.example")], &[]),
        );

        let envs = resolve_local_envs(&project, "staging").unwrap();
        assert_eq!(
            envs.get("NEXT_PUBLIC_API_URL").map(String::as_str),
            Some("https://api.example")
        );
    }

    #[test]
    fn resolves_secrets_against_local_vault() {
        let (_tmp, mut project) = make_project_with_vault("staging");
        project.environments.insert(
            "staging".to_string(),
            env_with(
                &[],
                &[("NEXT_PUBLIC_GOOGLE_MAPS_API_KEY", "google_maps:api_key")],
            ),
        );

        let envs = resolve_local_envs(&project, "staging").unwrap();
        assert_eq!(
            envs.get("NEXT_PUBLIC_GOOGLE_MAPS_API_KEY")
                .map(String::as_str),
            Some("AIzaSyTEST")
        );
    }

    #[test]
    fn resolves_mixed_config_and_secrets() {
        let (_tmp, mut project) = make_project_with_vault("staging");
        project.environments.insert(
            "staging".to_string(),
            env_with(
                &[("NEXT_PUBLIC_API_URL", "https://api.example")],
                &[
                    ("NEXT_PUBLIC_GOOGLE_MAPS_API_KEY", "google_maps:api_key"),
                    ("MAIL_API_KEY", "mail:api_key"),
                ],
            ),
        );

        let envs = resolve_local_envs(&project, "staging").unwrap();
        assert_eq!(envs.len(), 3);
        assert_eq!(envs["NEXT_PUBLIC_API_URL"], "https://api.example");
        assert_eq!(envs["NEXT_PUBLIC_GOOGLE_MAPS_API_KEY"], "AIzaSyTEST");
        assert_eq!(envs["MAIL_API_KEY"], "mail-test-token");
    }

    #[test]
    fn errors_when_referenced_secret_missing_from_vault() {
        let (_tmp, mut project) = make_project_with_vault("staging");
        project.environments.insert(
            "staging".to_string(),
            env_with(&[], &[("MISSING_VAR", "missing:secret")]),
        );

        let err = resolve_local_envs(&project, "staging").unwrap_err();
        assert!(err.to_string().contains("missing:secret"));
        assert!(err.to_string().contains("staging"));
    }

    #[test]
    fn errors_for_unknown_environment() {
        let (_tmp, project) = make_project_with_vault("staging");

        let err = resolve_local_envs(&project, "nope").unwrap_err();
        assert!(err.to_string().contains("nope"));
    }
}
