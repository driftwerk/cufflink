use crate::config::CliConfig;

async fn get_tenant_id(config: &CliConfig) -> eyre::Result<String> {
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/auth/me", config.api_url),
        )
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to get auth info ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let tenant_id = body["tenant_id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("No tenant_id in /api/auth/me response"))?;

    Ok(tenant_id.to_string())
}

pub async fn list(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let tenant_id = get_tenant_id(&config).await?;

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/tenants/{}/roles", config.api_url, tenant_id),
        )
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to list roles ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let roles = body["roles"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("Unexpected response format"))?;

    if roles.is_empty() {
        println!("No roles defined.");
        println!("Deploy a service with an `authorization` block to create default roles.");
        return Ok(());
    }

    use comfy_table::{presets::NOTHING, Table, TableComponent};

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(vec!["ROLE", "PERMISSIONS"]);

    for role in roles {
        let name = role["name"].as_str().unwrap_or("?");
        let perms = role["permissions"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| p.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        table.add_row(vec![name, &perms]);
    }

    println!("{table}");

    Ok(())
}

pub async fn assign(user: &str, role_name: &str, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let tenant_id = get_tenant_id(&config).await?;
    let client = config.http_client();

    let user_id = lookup_user(&config, &client, &tenant_id, user).await?;

    let role_id = find_role_by_name(&config, &client, &tenant_id, role_name).await?;

    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!("{}/api/tenants/{}/roles/assign", config.api_url, tenant_id),
        )
        .json(&serde_json::json!({
            "user_id": user_id,
            "role_id": role_id,
        }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to assign role ({}): {}", status, body);
    }

    println!("Assigned role '{}' to user '{}'", role_name, user);
    Ok(())
}

pub async fn unassign(user: &str, role_name: &str, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let tenant_id = get_tenant_id(&config).await?;
    let client = config.http_client();

    let user_id = lookup_user(&config, &client, &tenant_id, user).await?;

    let role_id = find_role_by_name(&config, &client, &tenant_id, role_name).await?;

    let resp = config
        .auth_request(
            &client,
            reqwest::Method::DELETE,
            &format!(
                "{}/api/tenants/{}/roles/unassign",
                config.api_url, tenant_id
            ),
        )
        .json(&serde_json::json!({
            "user_id": user_id,
            "role_id": role_id,
        }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to unassign role ({}): {}", status, body);
    }

    println!("Removed role '{}' from user '{}'", role_name, user);
    Ok(())
}

async fn lookup_user(
    config: &CliConfig,
    client: &reqwest::Client,
    tenant_id: &str,
    query: &str,
) -> eyre::Result<String> {
    let resp = config
        .auth_request(
            client,
            reqwest::Method::GET,
            &format!(
                "{}/api/tenants/{}/users/search?q={}",
                config.api_url,
                tenant_id,
                urlencoding::encode(query)
            ),
        )
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to look up user ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let users = body["users"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("Unexpected response format"))?;

    match users.len() {
        0 => eyre::bail!(
            "No user found matching '{}'. The user must exist in the tenant's Keycloak realm.",
            query
        ),
        1 => {
            let user_id = users[0]["user_id"]
                .as_str()
                .ok_or_else(|| eyre::eyre!("User has no user_id"))?;
            let name = users[0]["name"].as_str().unwrap_or("?");
            let email = users[0]["email"].as_str().unwrap_or("?");
            println!("Found user: {} ({})", name, email);
            Ok(user_id.to_string())
        }
        n => {
            println!("Multiple users match '{}' ({} results):", query, n);
            for u in users {
                println!(
                    "  {} — {}",
                    u["name"].as_str().unwrap_or("?"),
                    u["email"].as_str().unwrap_or("?"),
                );
            }
            eyre::bail!("Ambiguous match. Refine your search query.")
        }
    }
}

async fn find_role_by_name(
    config: &CliConfig,
    client: &reqwest::Client,
    tenant_id: &str,
    role_name: &str,
) -> eyre::Result<String> {
    let resp = config
        .auth_request(
            client,
            reqwest::Method::GET,
            &format!("{}/api/tenants/{}/roles", config.api_url, tenant_id),
        )
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to list roles ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let roles = body["roles"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("Unexpected response format"))?;

    let role = roles
        .iter()
        .find(|r| r["name"].as_str() == Some(role_name))
        .ok_or_else(|| {
            let available: Vec<&str> = roles.iter().filter_map(|r| r["name"].as_str()).collect();
            eyre::eyre!(
                "Role '{}' not found. Available roles: {}",
                role_name,
                available.join(", ")
            )
        })?;

    let role_id = role["id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Role has no ID"))?;

    Ok(role_id.to_string())
}
