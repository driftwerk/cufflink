use crate::config::CliConfig;
use crate::credentials;

#[derive(serde::Deserialize)]
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
    verification_uri: Option<String>,
    verification_uri_complete: Option<String>,
    expires_in: u64,
    interval: Option<u64>,
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(serde::Deserialize)]
struct TokenErrorResponse {
    error: String,
}

pub async fn run(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let keycloak_url = config
        .keycloak_url
        .clone()
        .unwrap_or_else(|| "http://localhost:8180".into());
    let realm = config
        .keycloak_realm
        .clone()
        .unwrap_or_else(|| "cufflink".into());
    let client_id = config
        .keycloak_client_id
        .clone()
        .unwrap_or_else(|| "cufflink-cli".into());

    let client = reqwest::Client::new();

    println!("Authenticating with Cufflink...");
    let device_url = format!(
        "{}/realms/{}/protocol/openid-connect/auth/device",
        keycloak_url, realm
    );

    let resp = client
        .post(&device_url)
        .form(&[("client_id", &client_id)])
        .send()
        .await;

    let device_resp = match resp {
        Ok(r) if r.status().is_success() => {
            let body: DeviceAuthResponse = r.json().await?;
            Some(body)
        }
        _ => None,
    };

    if let Some(device) = device_resp {
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
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(device.expires_in);

        let access_token = loop {
            if std::time::Instant::now() > deadline {
                eyre::bail!("Authentication timed out. Please try again.");
            }

            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

            let resp = client
                .post(&token_url)
                .form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("device_code", &device.device_code),
                    ("client_id", &client_id),
                ])
                .send()
                .await?;

            if resp.status().is_success() {
                let token: TokenResponse = resp.json().await?;
                break token.access_token;
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

        let resp = client
            .post(format!("{}/api/auth/device/token", config.api_url))
            .json(&serde_json::json!({
                "access_token": access_token,
                "tenant_slug": config.tenant_slug,
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            eyre::bail!("Failed to exchange token: {}", body);
        }

        let body: serde_json::Value = resp.json().await?;
        let api_key = body["api_key"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("No api_key in response"))?;
        let username = body["username"].as_str().unwrap_or("unknown");

        credentials::save(&credentials::Credentials {
            api_key: api_key.to_string(),
            tenant_slug: config.tenant_slug.clone(),
            username: username.to_string(),
            api_url: config.api_url.clone(),
        })?;

        println!("Logged in as {} (tenant: {})", username, config.tenant_slug);
        println!("Credentials saved to ~/.config/cufflink/credentials.json");
    } else {
        println!("Keycloak device auth not available.");
        println!("Enter your API key manually (create one with `cufflink tenants create-api-key` or POST /api/auth/api-keys):");
        println!();

        let mut api_key = String::new();
        print!("  API Key: ");
        use std::io::Write;
        std::io::stdout().flush()?;
        std::io::stdin().read_line(&mut api_key)?;
        let api_key = api_key.trim().to_string();

        if api_key.is_empty() {
            eyre::bail!("No API key provided");
        }

        let resp = client
            .get(format!("{}/api/auth/me", config.api_url))
            .header("Authorization", format!("ApiKey {}", api_key))
            .send()
            .await?;

        if !resp.status().is_success() {
            eyre::bail!("Invalid API key");
        }

        let body: serde_json::Value = resp.json().await?;
        let username = body["name"]
            .as_str()
            .or(body["subject"].as_str())
            .unwrap_or("unknown")
            .to_string();

        credentials::save(&credentials::Credentials {
            api_key,
            tenant_slug: config.tenant_slug.clone(),
            username: username.clone(),
            api_url: config.api_url.clone(),
        })?;

        println!("Logged in as {} (tenant: {})", username, config.tenant_slug);
        println!("Credentials saved to ~/.config/cufflink/credentials.json");
    }

    Ok(())
}
