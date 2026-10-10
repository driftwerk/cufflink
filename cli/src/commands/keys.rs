//! `cufflink keys partner`: the scoped partner keys a tenant issues for its
//! `api_key` routes.

use super::{json_answer, listing};
use crate::config::CliConfig;
use clap::Subcommand;
use serde_json::{json, Value};

#[derive(Subcommand)]
pub enum KeysAction {
    /// Manage partner keys (scoped keys for `api_key` routes)
    Partner {
        #[command(subcommand)]
        action: PartnerAction,
    },
}

#[derive(Subcommand)]
pub enum PartnerAction {
    /// Issue a partner key; it is shown once
    Create {
        /// A name identifying the partner
        name: String,
        /// Scopes the key grants, e.g. books:ingest or books:* (repeatable)
        #[arg(long = "scope", required = true)]
        scopes: Vec<String>,
        /// Days until the key expires (1-365)
        #[arg(long, default_value_t = 90)]
        expires_in_days: u32,
        /// Tenant slug (overrides Cufflink.toml)
        #[arg(long)]
        tenant: Option<String>,
    },
    /// List the tenant's partner keys (never their secrets)
    List {
        /// Tenant slug (overrides Cufflink.toml)
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Revoke a partner key
    Revoke {
        /// Partner key ID
        id: String,
        /// Tenant slug (overrides Cufflink.toml)
        #[arg(long)]
        tenant: Option<String>,
    },
}

pub async fn run(action: KeysAction, env: Option<&str>) -> eyre::Result<()> {
    let KeysAction::Partner { action } = action;
    match action {
        PartnerAction::Create {
            name,
            scopes,
            expires_in_days,
            tenant,
        } => create(&name, &scopes, expires_in_days, tenant, env).await,
        PartnerAction::List { tenant } => list(tenant, env).await,
        PartnerAction::Revoke { id, tenant } => revoke(&id, tenant, env).await,
    }
}

fn config(tenant: Option<String>, env: Option<&str>) -> eyre::Result<CliConfig> {
    let mut config = CliConfig::load_with_env(env)?;
    if let Some(tenant) = tenant {
        config.tenant_slug = tenant.clone();
        config.tenant_override = Some(tenant);
    }
    Ok(config)
}

async fn send(
    config: &CliConfig,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
    what: &str,
) -> eyre::Result<Value> {
    let client = config.http_client();
    let request = config.auth_request(
        &client,
        method,
        &format!(
            "{}/api/tenants/{}/partner-keys{path}",
            config.api_url, config.tenant_slug
        ),
    );
    let request = match body {
        Some(body) => request.json(&body),
        None => request,
    };
    json_answer(request.send().await?, what).await
}

async fn create(
    name: &str,
    scopes: &[String],
    expires_in_days: u32,
    tenant: Option<String>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = config(tenant, env)?;
    let body = send(
        &config,
        reqwest::Method::POST,
        "",
        Some(json!({"name": name, "scopes": scopes, "expires_in_days": expires_in_days})),
        "create the partner key",
    )
    .await?;
    println!("Partner key created:");
    println!("  ID:      {}", body["id"].as_str().unwrap_or("?"));
    println!("  Key:     {}", body["key"].as_str().unwrap_or("?"));
    println!("  Scopes:  {}", scopes.join(", "));
    println!("  Expires: {}", body["expires_at"].as_str().unwrap_or("?"));
    println!();
    println!("Save this key — it cannot be retrieved again.");
    Ok(())
}

async fn list(tenant: Option<String>, env: Option<&str>) -> eyre::Result<()> {
    let config = config(tenant, env)?;
    let body = send(&config, reqwest::Method::GET, "", None, "list partner keys").await?;
    let keys = body["keys"].as_array().cloned().unwrap_or_default();
    if keys.is_empty() {
        println!("No partner keys.");
        return Ok(());
    }

    let mut table = listing(&[
        "ID",
        "NAME",
        "PREFIX",
        "SCOPES",
        "EXPIRES",
        "REVOKED",
        "LAST USED",
    ]);
    for key in &keys {
        let text = |field: &str, absent: &str| key[field].as_str().unwrap_or(absent).to_string();
        let scopes: Vec<&str> = key["scopes"]
            .as_array()
            .map(|scopes| scopes.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        table.add_row(vec![
            text("id", "?"),
            text("name", "?"),
            text("prefix", "?"),
            scopes.join(","),
            text("expires_at", "?"),
            text("revoked_at", "-"),
            text("last_used_at", "never"),
        ]);
    }
    println!("{table}");
    Ok(())
}

async fn revoke(id: &str, tenant: Option<String>, env: Option<&str>) -> eyre::Result<()> {
    let config = config(tenant, env)?;
    send(
        &config,
        reqwest::Method::DELETE,
        &format!("/{id}"),
        None,
        "revoke the partner key",
    )
    .await?;
    println!("Partner key revoked.");
    Ok(())
}
