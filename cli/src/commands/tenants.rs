use crate::config::CliConfig;
use cufflink_types::policy::{PolicyKey, PolicyMode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Deserialize)]
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
    verification_uri: Option<String>,
    verification_uri_complete: Option<String>,
    expires_in: u64,
    interval: Option<u64>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
}

#[derive(Deserialize)]
struct TokenErrorResponse {
    error: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct PlatformTokensFile(HashMap<String, PlatformTokens>);

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PlatformTokens {
    access_token: String,
    refresh_token: String,
    keycloak_url: String,
    realm: String,
    client_id: String,
}

fn tokens_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home)
        .join(".config")
        .join("cufflink")
        .join("platform-tokens.json")
}

fn load_tokens(api_url: &str) -> Option<PlatformTokens> {
    let path = tokens_path();
    let json = std::fs::read_to_string(path).ok()?;
    let file: PlatformTokensFile = serde_json::from_str(&json).ok()?;
    file.0.get(api_url).cloned()
}

fn save_tokens(api_url: &str, tokens: &PlatformTokens) {
    let path = tokens_path();
    let mut file: PlatformTokensFile = std::fs::read_to_string(&path)
        .ok()
        .and_then(|json| serde_json::from_str(&json).ok())
        .unwrap_or_default();

    file.0.insert(api_url.to_string(), tokens.clone());

    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(json) = serde_json::to_string_pretty(&file) {
        let _ = std::fs::write(&path, json);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

async fn try_refresh(tokens: &PlatformTokens) -> Option<TokenResponse> {
    let client = reqwest::Client::new();
    let token_url = format!(
        "{}/realms/{}/protocol/openid-connect/token",
        tokens.keycloak_url, tokens.realm
    );

    let resp = client
        .post(&token_url)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", &tokens.refresh_token),
            ("client_id", &tokens.client_id),
        ])
        .send()
        .await
        .ok()?;

    if resp.status().is_success() {
        resp.json::<TokenResponse>().await.ok()
    } else {
        None
    }
}

async fn discover_auth_config(api_url: &str) -> Option<(String, String, String)> {
    #[derive(Deserialize)]
    struct AuthConfig {
        keycloak_url: String,
        realm: String,
        cli_client_id: Option<String>,
        client_id: Option<String>,
    }

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/api/auth/config", api_url))
        .send()
        .await
        .ok()?;

    if resp.status().is_success() {
        let config: AuthConfig = resp.json().await.ok()?;
        let client_id = config
            .cli_client_id
            .or(config.client_id)
            .unwrap_or_else(|| "cufflink-cli".into());
        Some((config.keycloak_url, config.realm, client_id))
    } else {
        None
    }
}

pub(crate) async fn platform_auth(config: &CliConfig) -> eyre::Result<String> {
    let (keycloak_url, realm, client_id) =
        if let Some(discovered) = discover_auth_config(&config.api_url).await {
            discovered
        } else {
            (
                config
                    .keycloak_url
                    .clone()
                    .unwrap_or_else(|| "http://localhost:8180".to_string()),
                config
                    .keycloak_realm
                    .clone()
                    .unwrap_or_else(|| "cufflink".to_string()),
                config
                    .keycloak_client_id
                    .clone()
                    .unwrap_or_else(|| "cufflink-cli".to_string()),
            )
        };
    let keycloak_url = keycloak_url.as_str();
    let realm = realm.as_str();
    let client_id = client_id.as_str();

    if let Some(cached) = load_tokens(&config.api_url) {
        if let Some(refreshed) = try_refresh(&cached).await {
            let new_tokens = PlatformTokens {
                access_token: refreshed.access_token.clone(),
                refresh_token: refreshed
                    .refresh_token
                    .unwrap_or(cached.refresh_token.clone()),
                keycloak_url: cached.keycloak_url,
                realm: cached.realm,
                client_id: cached.client_id,
            };
            save_tokens(&config.api_url, &new_tokens);
            return Ok(refreshed.access_token);
        }
    }

    let client = reqwest::Client::new();

    println!("Authenticating with platform...");
    let device_url = format!(
        "{}/realms/{}/protocol/openid-connect/auth/device",
        keycloak_url, realm
    );

    let resp = client
        .post(&device_url)
        .form(&[("client_id", client_id)])
        .send()
        .await?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!(
            "Failed to start device auth (is the platform realm '{}' configured?): {}",
            realm,
            body
        );
    }

    let device: DeviceAuthResponse = resp.json().await?;

    let verify_url = device
        .verification_uri_complete
        .as_deref()
        .or(device.verification_uri.as_deref())
        .unwrap_or("(no verification URL)");

    println!();
    println!("  Open this URL in your browser:");
    println!("  {}", verify_url);
    println!();
    println!("  Enter code: {}", device.user_code);
    println!();
    println!("Waiting for authentication...");

    let token_url = format!(
        "{}/realms/{}/protocol/openid-connect/token",
        keycloak_url, realm
    );
    let interval = device.interval.unwrap_or(5);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(device.expires_in);

    let token_resp = loop {
        if std::time::Instant::now() > deadline {
            eyre::bail!("Authentication timed out. Please try again.");
        }

        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

        let resp = client
            .post(&token_url)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", &device.device_code),
                ("client_id", client_id),
            ])
            .send()
            .await?;

        if resp.status().is_success() {
            let token: TokenResponse = resp.json().await?;
            break token;
        }

        let err: TokenErrorResponse = resp.json().await?;
        match err.error.as_str() {
            "authorization_pending" => continue,
            "slow_down" => {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            other => eyre::bail!("Authentication failed: {}", other),
        }
    };

    if let Some(ref refresh_token) = token_resp.refresh_token {
        save_tokens(
            &config.api_url,
            &PlatformTokens {
                access_token: token_resp.access_token.clone(),
                refresh_token: refresh_token.clone(),
                keycloak_url: keycloak_url.to_string(),
                realm: realm.to_string(),
                client_id: client_id.to_string(),
            },
        );
    }

    println!("Authenticated.");
    Ok(token_resp.access_token)
}

#[derive(Debug, Default)]
pub struct NewTenant<'a> {
    pub name: &'a str,
    pub slug: &'a str,
    pub keycloak_url: Option<&'a str>,
    pub keycloak_realm: Option<&'a str>,
    pub deploy_role: Option<&'a str>,
    pub database_name: Option<&'a str>,
}

impl NewTenant<'_> {
    fn body(&self) -> serde_json::Value {
        let given = [
            ("keycloak_url", self.keycloak_url),
            ("keycloak_realm", self.keycloak_realm),
            ("deploy_role", self.deploy_role),
            ("database_name", self.database_name),
        ]
        .into_iter()
        .filter_map(|(field, value)| value.map(|value| (field.to_string(), value.into())));
        serde_json::Value::Object(
            [
                ("name".to_string(), self.name.into()),
                ("slug".to_string(), self.slug.into()),
            ]
            .into_iter()
            .chain(given)
            .collect(),
        )
    }
}

pub async fn create(
    tenant: &NewTenant<'_>,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::for_platform(api_url, env)?;
    let token = platform_auth(&config).await?;

    let client = reqwest::Client::new();
    let body = tenant.body();

    let resp = client
        .post(format!("{}/api/tenants", config.api_url))
        .header("Authorization", format!("Bearer {}", token))
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to create tenant ({}): {}", status, body);
    }

    let tenant: serde_json::Value = resp.json().await?;
    println!();
    println!("  Tenant created:");
    println!(
        "  Name:           {}",
        tenant["name"].as_str().unwrap_or("")
    );
    println!(
        "  Slug:           {}",
        tenant["slug"].as_str().unwrap_or("")
    );
    println!(
        "  Keycloak URL:   {}",
        tenant["keycloak_url"].as_str().unwrap_or("(not set)")
    );
    println!(
        "  Keycloak Realm: {}",
        tenant["keycloak_realm"].as_str().unwrap_or("(not set)")
    );
    println!(
        "  Deploy Role:    {}",
        tenant["deploy_role"].as_str().unwrap_or("")
    );
    if let Some(database) = tenant["database_name"].as_str() {
        println!("  Database:       {database} (adopted: never created or dropped)");
    }
    println!();
    println!("Developers can now run `cufflink login` with the tenant's Keycloak credentials.");

    Ok(())
}

#[derive(Debug, Default)]
pub struct TenantChanges<'a> {
    pub name: Option<&'a str>,
    pub keycloak_url: Option<&'a str>,
    pub keycloak_realm: Option<&'a str>,
    pub deploy_role: Option<&'a str>,
    pub keycloak_ops_client_id: Option<&'a str>,
    pub keycloak_ops_client_secret: Option<&'a str>,
    pub service_account_clients: Option<&'a str>,
    pub keycloak_issuer: Option<&'a str>,
}

impl TenantChanges<'_> {
    fn body(&self) -> serde_json::Map<String, serde_json::Value> {
        let strings = [
            ("name", self.name),
            ("keycloak_url", self.keycloak_url),
            ("keycloak_realm", self.keycloak_realm),
            ("deploy_role", self.deploy_role),
            ("keycloak_ops_client_id", self.keycloak_ops_client_id),
            (
                "keycloak_ops_client_secret",
                self.keycloak_ops_client_secret,
            ),
            ("keycloak_issuer", self.keycloak_issuer),
        ];
        let clients = self.service_account_clients.map(|raw| {
            let ids: Vec<&str> = raw
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .collect();
            ("service_account_clients", serde_json::json!(ids))
        });
        strings
            .into_iter()
            .filter_map(|(field, value)| value.map(|v| (field, serde_json::json!(v))))
            .chain(clients)
            .map(|(field, value)| (field.to_string(), value))
            .collect()
    }
}

pub async fn update(
    slug: &str,
    changes: &TenantChanges<'_>,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let body = changes.body();
    if body.is_empty() {
        eyre::bail!("Nothing to update. Provide at least one of: --name, --tenant-keycloak-url, --tenant-keycloak-realm, --deploy-role, --keycloak-ops-client-id, --keycloak-ops-client-secret, --service-account-clients, --keycloak-issuer");
    }

    let config = CliConfig::for_platform(api_url, env)?;
    let token = platform_auth(&config).await?;

    let client = reqwest::Client::new();
    let resp = client
        .put(format!("{}/api/tenants/{}", config.api_url, slug))
        .header("Authorization", format!("Bearer {}", token))
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to update tenant ({}): {}", status, body);
    }

    let tenant: serde_json::Value = resp.json().await?;
    println!("Tenant '{}' updated:", slug);
    println!(
        "  Name:           {}",
        tenant["name"].as_str().unwrap_or("")
    );
    println!(
        "  Keycloak URL:   {}",
        tenant["keycloak_url"].as_str().unwrap_or("(not set)")
    );
    println!(
        "  Keycloak Realm: {}",
        tenant["keycloak_realm"].as_str().unwrap_or("(not set)")
    );
    println!(
        "  Deploy Role:    {}",
        tenant["deploy_role"].as_str().unwrap_or("")
    );
    println!(
        "  Issuer:         {}",
        tenant["keycloak_issuer"]
            .as_str()
            .unwrap_or("(platform default)")
    );
    let clients: Vec<&str> = tenant["service_account_clients"]
        .as_array()
        .map(|ids| ids.iter().filter_map(|id| id.as_str()).collect())
        .unwrap_or_default();
    println!(
        "  Service accts:  {}",
        if clients.is_empty() {
            "(none)".to_string()
        } else {
            clients.join(", ")
        }
    );

    Ok(())
}

pub(crate) async fn admin_request(
    what: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<serde_json::Value>,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<serde_json::Value> {
    let config = CliConfig::for_platform(api_url, env)?;
    let auth = match platform_api_auth() {
        Ok(a) => a,
        Err(_) => format!("Bearer {}", platform_auth(&config).await?),
    };
    let request = reqwest::Client::new()
        .request(method, format!("{}{}", config.api_url, path))
        .header("Authorization", &auth);
    let request = match body {
        Some(body) => request.json(&body),
        None => request,
    };
    let resp = request.send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("{what} request failed ({}): {}", status, body);
    }
    Ok(resp.json().await?)
}

async fn policy_request(
    method: reqwest::Method,
    path: &str,
    body: Option<serde_json::Value>,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<serde_json::Value> {
    admin_request("Policy", method, path, body, api_url, env).await
}

fn claims_path(slug: &str) -> String {
    format!("/api/tenants/{slug}/nats-claims")
}

fn claim_lines(listing: &serde_json::Value) -> Vec<String> {
    let of = |kind: &'static str, field: &str| {
        listing[field]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|claim| claim.as_str())
            .map(move |claim| format!("  {kind:<8}{claim}"))
            .collect::<Vec<_>>()
    };
    of("subject", "subjects")
        .into_iter()
        .chain(of("stream", "streams"))
        .collect()
}

fn print_claims(slug: &str, listing: &serde_json::Value) {
    match claim_lines(listing).as_slice() {
        [] => println!("Tenant '{slug}' claims nothing beyond dw.{slug}.>"),
        lines => {
            println!("Tenant '{slug}' claims, beyond dw.{slug}.>:");
            lines.iter().for_each(|line| println!("{line}"));
        }
    }
}

pub async fn nats_claims_show(
    slug: &str,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let listing = admin_request(
        "NATS claims",
        reqwest::Method::GET,
        &claims_path(slug),
        None,
        api_url,
        env,
    )
    .await?;
    print_claims(slug, &listing);
    Ok(())
}

pub async fn nats_claims_set(
    slug: &str,
    subjects: &[String],
    streams: &[String],
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let listing = admin_request(
        "NATS claims",
        reqwest::Method::PUT,
        &claims_path(slug),
        Some(serde_json::json!({ "subjects": subjects, "streams": streams })),
        api_url,
        env,
    )
    .await?;
    print_claims(slug, &listing);
    Ok(())
}

fn print_policies(listing: &serde_json::Value) {
    use comfy_table::{presets::NOTHING, Table, TableComponent};

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(vec!["KEY", "MODE", "SOURCE", "CONFIG"]);
    if let Some(policies) = listing["policies"].as_object() {
        for (key, policy) in policies {
            table.add_row(vec![
                key.as_str(),
                policy["mode"].as_str().unwrap_or("?"),
                policy["source"].as_str().unwrap_or("?"),
                &policy["config"].to_string(),
            ]);
        }
    }
    println!("{table}");
}

pub async fn policy_get(slug: &str, api_url: Option<&str>, env: Option<&str>) -> eyre::Result<()> {
    let path = format!("/api/tenants/{slug}/policies");
    let listing = policy_request(reqwest::Method::GET, &path, None, api_url, env).await?;
    print_policies(&listing);
    Ok(())
}

pub async fn policy_set(
    slug: &str,
    key: PolicyKey,
    mode: PolicyMode,
    policy_config: Option<serde_json::Value>,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let path = format!("/api/tenants/{slug}/policies");
    let listing = policy_request(
        reqwest::Method::PUT,
        &path,
        Some(policy_body(key, mode, policy_config)),
        api_url,
        env,
    )
    .await?;
    println!("Policy '{key}' for tenant '{slug}' set to {mode}.");
    print_policies(&listing);
    Ok(())
}

fn policy_body(
    key: PolicyKey,
    mode: PolicyMode,
    policy_config: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut entry = serde_json::json!({ "mode": mode });
    if let Some(policy_config) = policy_config {
        entry["config"] = policy_config;
    }
    serde_json::json!({ "policies": { key.as_str(): entry } })
}

pub async fn policy_reset(
    slug: &str,
    key: PolicyKey,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let path = format!("/api/tenants/{slug}/policies/{key}");
    let listing = policy_request(reqwest::Method::DELETE, &path, None, api_url, env).await?;
    println!("Policy '{key}' for tenant '{slug}' reset to the platform default.");
    print_policies(&listing);
    Ok(())
}

pub async fn list(api_url: Option<&str>, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::for_platform(api_url, env)?;
    let token = platform_auth(&config).await?;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/api/tenants", config.api_url))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to list tenants ({}): {}", status, body);
    }

    let tenants: Vec<serde_json::Value> = resp.json().await?;

    if tenants.is_empty() {
        println!("No tenants found.");
        return Ok(());
    }

    use comfy_table::{presets::NOTHING, Table, TableComponent};

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(vec!["SLUG", "NAME", "KEYCLOAK", "DEPLOY ROLE"]);

    for t in &tenants {
        let kc = match (t["keycloak_url"].as_str(), t["keycloak_realm"].as_str()) {
            (Some(url), Some(realm)) => format!("{} / {}", url, realm),
            _ => "(not configured)".to_string(),
        };
        table.add_row(vec![
            t["slug"].as_str().unwrap_or(""),
            t["name"].as_str().unwrap_or(""),
            &kc,
            t["deploy_role"].as_str().unwrap_or(""),
        ]);
    }

    println!("{table}");

    Ok(())
}

pub async fn delete(
    slug: &str,
    yes: bool,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    if !yes {
        println!(
            "This will permanently delete tenant '{}' and all its data.",
            slug
        );
        print!("Are you sure? (y/N) ");
        use std::io::Write;
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    let config = CliConfig::for_platform(api_url, env)?;
    let token = platform_auth(&config).await?;

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}/api/tenants/{}", config.api_url, slug))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to delete tenant ({}): {}", status, body);
    }

    let deleted: serde_json::Value = resp.json().await.unwrap_or_default();
    println!("{}", deleted_line(slug, deleted["database"].as_str()));
    Ok(())
}

fn deleted_line(slug: &str, database: Option<&str>) -> String {
    match database {
        Some(outcome) => format!("Tenant '{slug}' deleted; database {outcome}."),
        None => format!("Tenant '{slug}' deleted."),
    }
}

pub(crate) fn platform_api_auth() -> eyre::Result<String> {
    std::env::var("CUFFLINK_PLATFORM_API_KEY")
        .map(|k| format!("ApiKey {}", k))
        .map_err(|_| eyre::eyre!("CUFFLINK_PLATFORM_API_KEY env var required"))
}

pub async fn create_platform_api_key(
    name: &str,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::for_platform(api_url, env)?;
    let auth = match platform_api_auth() {
        Ok(a) => a,
        Err(_) => format!("Bearer {}", platform_auth(&config).await?),
    };

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/platform/api-keys", config.api_url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({ "name": name }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to create API key ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    println!("Platform admin API key created:");
    println!("  Name: {}", body["name"].as_str().unwrap_or("?"));
    println!("  ID:   {}", body["id"].as_str().unwrap_or("?"));
    println!("  Key:  {}", body["key"].as_str().unwrap_or("?"));
    println!();
    println!("Save this key — it cannot be retrieved again.");

    Ok(())
}

pub async fn list_platform_api_keys(api_url: Option<&str>, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::for_platform(api_url, env)?;
    let auth = match platform_api_auth() {
        Ok(a) => a,
        Err(_) => format!("Bearer {}", platform_auth(&config).await?),
    };

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/api/platform/api-keys", config.api_url))
        .header("Authorization", &auth)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to list API keys ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let keys = body["keys"].as_array();

    if keys.map(|k| k.is_empty()).unwrap_or(true) {
        println!("No platform admin API keys.");
        return Ok(());
    }

    use comfy_table::{presets::NOTHING, Table, TableComponent};
    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(vec!["ID", "NAME", "CREATED", "EXPIRES"]);

    for key in keys.unwrap() {
        table.add_row(vec![
            key["id"].as_str().unwrap_or("?"),
            key["name"].as_str().unwrap_or("?"),
            key["created_at"].as_str().unwrap_or("?"),
            key["expires_at"].as_str().unwrap_or("never"),
        ]);
    }

    println!("{table}");
    Ok(())
}

pub async fn revoke_platform_api_key(
    id: &str,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::for_platform(api_url, env)?;
    let auth = match platform_api_auth() {
        Ok(a) => a,
        Err(_) => format!("Bearer {}", platform_auth(&config).await?),
    };

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}/api/platform/api-keys/{}", config.api_url, id))
        .header("Authorization", &auth)
        .send()
        .await?;

    if resp.status().is_success() {
        println!("API key revoked.");
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to revoke API key ({}): {}", status, body);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_body_carries_the_name_the_slug_and_the_fields_given() {
        let plain = NewTenant {
            name: "Acme",
            slug: "acme",
            ..Default::default()
        };
        assert_eq!(plain.body(), json!({"name": "Acme", "slug": "acme"}));
        let adopting = NewTenant {
            keycloak_realm: Some("harbor"),
            database_name: Some("harbor"),
            ..plain
        };
        assert_eq!(
            adopting.body(),
            json!({
                "name": "Acme",
                "slug": "acme",
                "keycloak_realm": "harbor",
                "database_name": "harbor"
            })
        );
    }

    #[test]
    fn a_deletion_says_what_became_of_the_database() {
        assert_eq!(
            deleted_line("harbor", Some("kept (adopted)")),
            "Tenant 'harbor' deleted; database kept (adopted)."
        );
        assert_eq!(
            deleted_line("acme", Some("dropped")),
            "Tenant 'acme' deleted; database dropped."
        );
        assert_eq!(deleted_line("acme", None), "Tenant 'acme' deleted.");
    }

    #[test]
    fn claims_are_listed_subjects_first() {
        assert_eq!(
            claim_lines(&json!({
                "tenant": "harbor",
                "subjects": ["books.>", "audit.logs"],
                "streams": ["BOOKS"],
            })),
            [
                "  subject books.>",
                "  subject audit.logs",
                "  stream  BOOKS"
            ]
        );
        assert!(claim_lines(&json!({"subjects": [], "streams": []})).is_empty());
        assert_eq!(claims_path("harbor"), "/api/tenants/harbor/nats-claims");
    }

    #[test]
    fn update_body_carries_only_the_given_fields() {
        let changes = TenantChanges {
            name: Some("Orchard"),
            service_account_clients: Some("orchard-ops, ci-bot"),
            ..Default::default()
        };
        assert_eq!(
            serde_json::Value::Object(changes.body()),
            json!({"name": "Orchard", "service_account_clients": ["orchard-ops", "ci-bot"]})
        );
        let clear = TenantChanges {
            service_account_clients: Some(""),
            keycloak_issuer: Some(""),
            ..Default::default()
        };
        assert_eq!(
            serde_json::Value::Object(clear.body()),
            json!({"service_account_clients": [], "keycloak_issuer": ""})
        );
        assert!(TenantChanges::default().body().is_empty());
    }

    #[test]
    fn policy_body_omits_config_unless_given() {
        assert_eq!(
            policy_body(PolicyKey::Egress, PolicyMode::Warn, None),
            json!({"policies": {"egress": {"mode": "warn"}}})
        );
        assert_eq!(
            policy_body(
                PolicyKey::Audience,
                PolicyMode::Enforce,
                Some(json!({"allowed": ["portal"]}))
            ),
            json!({"policies": {"audience": {"mode": "enforce", "config": {"allowed": ["portal"]}}}})
        );
    }
}
