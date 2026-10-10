//! `cufflink auth keys`: the caller's tenant API keys, and rotation of
//! platform-admin keys.

use super::tenants::{platform_api_auth, platform_auth};
use super::{json_answer, listing};
use crate::config::CliConfig;
use clap::Subcommand;
use serde_json::Value;

#[derive(Subcommand)]
pub enum AuthAction {
    /// Manage API keys
    Keys {
        #[command(subcommand)]
        action: KeysAction,
    },
}

#[derive(Subcommand)]
pub enum KeysAction {
    /// List the tenant's API keys (never their secrets)
    List,
    /// Revoke one of the tenant's API keys
    Revoke {
        /// API key ID
        id: String,
    },
    /// Replace an API key; the old one keeps working for the grace period
    Rotate {
        /// API key ID
        id: String,
        /// Hours the replaced key keeps working (0-168)
        #[arg(long, default_value_t = 24)]
        grace_hours: u32,
        /// Rotate a platform-admin key (needs platform admin credentials)
        #[arg(long)]
        admin: bool,
        /// Platform API URL for --admin (overrides Cufflink.toml and CUFFLINK_API_URL)
        #[arg(long)]
        api_url: Option<String>,
    },
}

pub async fn run(action: AuthAction, env: Option<&str>) -> eyre::Result<()> {
    match action {
        AuthAction::Keys { action } => match action {
            KeysAction::List => list(env).await,
            KeysAction::Revoke { id } => revoke(&id, env).await,
            KeysAction::Rotate {
                id,
                grace_hours,
                admin,
                api_url,
            } => rotate(&id, grace_hours, admin, api_url.as_deref(), env).await,
        },
    }
}

async fn list(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/auth/api-keys", config.api_url),
        )
        .send()
        .await?;
    let body = json_answer(resp, "list API keys").await?;
    let keys = body["keys"].as_array().cloned().unwrap_or_default();
    if keys.is_empty() {
        println!("No API keys.");
        return Ok(());
    }

    let mut table = listing(&["ID", "NAME", "USER", "CREATED", "EXPIRES", "LAST USED"]);
    let text = |key: &Value, field: &str, absent: &'static str| {
        key[field].as_str().unwrap_or(absent).to_string()
    };
    for key in &keys {
        table.add_row(vec![
            text(key, "id", "?"),
            text(key, "name", "?"),
            text(key, "user_sub", "-"),
            text(key, "created_at", "?"),
            text(key, "expires_at", "never"),
            text(key, "last_used_at", "never"),
        ]);
    }
    println!("{table}");
    Ok(())
}

async fn revoke(id: &str, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::DELETE,
            &format!("{}/api/auth/api-keys/{id}", config.api_url),
        )
        .send()
        .await?;
    json_answer(resp, "revoke the API key").await?;
    println!("API key revoked.");
    Ok(())
}

async fn rotate(
    id: &str,
    grace_hours: u32,
    admin: bool,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let request = if admin {
        let config = CliConfig::for_platform(api_url, env)?;
        let auth = match platform_api_auth() {
            Ok(auth) => auth,
            Err(_) => format!("Bearer {}", platform_auth(&config).await?),
        };
        config
            .http_client()
            .post(format!(
                "{}/api/platform/api-keys/{id}/rotate",
                config.api_url
            ))
            .header("Authorization", auth)
    } else {
        let config = CliConfig::load_with_env(env)?;
        let client = config.http_client();
        config.auth_request(
            &client,
            reqwest::Method::POST,
            &format!("{}/api/auth/api-keys/{id}/rotate", config.api_url),
        )
    };
    let resp = request
        .query(&[("grace_hours", grace_hours)])
        .send()
        .await?;
    let body = json_answer(resp, "rotate the API key").await?;
    println!("API key rotated:");
    println!("  ID:      {}", body["id"].as_str().unwrap_or("?"));
    println!("  Key:     {}", body["key"].as_str().unwrap_or("?"));
    println!(
        "  Expires: {}",
        body["expires_at"].as_str().unwrap_or("never")
    );
    println!(
        "The replaced key {} works until {}.",
        body["replaces"].as_str().unwrap_or(id),
        body["replaced_key_expires_at"].as_str().unwrap_or("?")
    );
    println!();
    println!("Save this key — it cannot be retrieved again.");
    Ok(())
}
