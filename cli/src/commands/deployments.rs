use crate::config::CliConfig;
use comfy_table::{presets::NOTHING, Table, TableComponent};
use std::io::IsTerminal;
use std::process::Command;

pub async fn list(limit: Option<usize>, env: Option<&str>) -> eyre::Result<()> {
    let (config, service_id, service_name) = resolve_service(env).await?;
    let url = match limit {
        Some(n) => format!(
            "{}/api/services/{}/deployments?limit={}",
            config.api_url, service_id, n
        ),
        None => format!("{}/api/services/{}/deployments", config.api_url, service_id),
    };
    let client = config.http_client();
    let resp = config
        .auth_request(&client, reqwest::Method::GET, &url)
        .send()
        .await?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await?;
    if !status.is_success() {
        eyre::bail!("List failed ({}): {}", status, body);
    }
    let deployments = body["deployments"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("Malformed response: missing `deployments`"))?;
    let total = body["total"].as_u64().unwrap_or(deployments.len() as u64);
    let current = body["current_version"].as_i64().unwrap_or(-1);

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(vec![
        "VERSION",
        "STATUS",
        "DEPLOYED_AT",
        "MANIFEST",
        "WASM",
        "COMMIT",
        "CURRENT",
    ]);
    for d in deployments {
        let version = d["version"].as_i64().unwrap_or(0);
        table.add_row(vec![
            format!("v{version}"),
            d["status"].as_str().unwrap_or("?").to_string(),
            truncate_timestamp(d["deployed_at"].as_str().unwrap_or("?")),
            short_hash(d["manifest_hash"].as_str()),
            short_hash(d["wasm_hash"].as_str()),
            short_hash(d["commit_hash"].as_str()),
            if version == current { "✓" } else { "" }.to_string(),
        ]);
    }
    println!("{table}");
    println!(
        "\n{} deployment(s) for '{}' · current: v{}",
        total, service_name, current
    );
    Ok(())
}

pub async fn prune(keep: usize, yes: bool, env: Option<&str>) -> eyre::Result<()> {
    if keep == 0 {
        eyre::bail!("--keep must be >= 1 (the currently-active version is always retained)");
    }
    let (config, service_id, service_name) = resolve_service(env).await?;
    let client = config.http_client();

    let preview_resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services/{}/deployments", config.api_url, service_id),
        )
        .send()
        .await?;
    let preview: serde_json::Value = preview_resp.json().await?;
    let deployments = preview["deployments"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("Malformed response: missing `deployments`"))?;
    let current = preview["current_version"].as_i64().unwrap_or(-1) as i32;
    let versions: Vec<i32> = deployments
        .iter()
        .filter_map(|d| d["version"].as_i64().map(|v| v as i32))
        .collect();
    let would_delete = compute_prune_preview(&versions, current, keep);

    if would_delete.is_empty() {
        println!(
            "Nothing to prune for '{}': {} deployment(s), keeping {}",
            service_name,
            versions.len(),
            keep
        );
        return Ok(());
    }

    println!(
        "Pruning '{}': will delete {} deployment(s) (keeping {} most recent + current v{}):",
        service_name,
        would_delete.len(),
        keep,
        current
    );
    for v in &would_delete {
        println!("  - v{v}");
    }
    confirm(
        yes,
        "This will permanently remove these deployments and their WASM artifacts.",
    )?;

    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!(
                "{}/api/services/{}/deployments/prune",
                config.api_url, service_id
            ),
        )
        .json(&serde_json::json!({ "keep": keep }))
        .send()
        .await?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await?;
    if !status.is_success() {
        eyre::bail!("Prune failed ({}): {}", status, body);
    }
    let deleted = body["deleted_count"].as_u64().unwrap_or(0);
    println!("Pruned {} deployment(s) of '{}'", deleted, service_name);
    Ok(())
}

pub async fn delete(version: i32, yes: bool, env: Option<&str>) -> eyre::Result<()> {
    let (config, service_id, service_name) = resolve_service(env).await?;
    println!("Delete deployment v{} of '{}'?", version, service_name);
    confirm(
        yes,
        "This will permanently remove the deployment record and its WASM artifact.",
    )?;

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::DELETE,
            &format!(
                "{}/api/services/{}/deployments/{}",
                config.api_url, service_id, version
            ),
        )
        .send()
        .await?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await?;
    if !status.is_success() {
        eyre::bail!("Delete failed ({}): {}", status, body);
    }
    println!(
        "Deleted v{} of '{}'",
        body["deleted_version"].as_i64().unwrap_or(version as i64),
        service_name
    );
    Ok(())
}

pub(crate) async fn resolve_service(
    env: Option<&str>,
) -> eyre::Result<(CliConfig, String, String)> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }
    let manifest = emit_manifest()?;
    let service_name = manifest["name"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Manifest is missing `name`"))?
        .to_string();
    let service_id = config.find_service_id(&service_name).await?;
    Ok((config, service_id, service_name))
}

fn emit_manifest() -> eyre::Result<serde_json::Value> {
    let output = Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;
    if !output.status.success() {
        eyre::bail!("Failed to emit manifest. Run from a cufflink service directory.");
    }
    let stdout = String::from_utf8(output.stdout)?;
    Ok(serde_json::from_str(stdout.trim())?)
}

fn confirm(skip: bool, warning: &str) -> eyre::Result<()> {
    if skip {
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        eyre::bail!(
            "stdin is not a TTY; pass --yes to skip the confirmation prompt non-interactively"
        );
    }
    eprintln!("{warning} Continue? [y/N] ");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
        eyre::bail!("Aborted");
    }
    Ok(())
}

fn compute_prune_preview(versions: &[i32], current_version: i32, keep: usize) -> Vec<i32> {
    let retain: std::collections::HashSet<i32> = versions
        .iter()
        .take(keep)
        .copied()
        .chain(std::iter::once(current_version))
        .collect();
    versions
        .iter()
        .copied()
        .filter(|v| !retain.contains(v))
        .collect()
}

fn short_hash(hash: Option<&str>) -> String {
    match hash {
        Some(h) if h.len() > 12 => h[..12].to_string(),
        Some(h) => h.to_string(),
        None => "-".to_string(),
    }
}

fn truncate_timestamp(ts: &str) -> String {
    ts.split('.').next().unwrap_or(ts).replace('T', " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_hash_truncates_long_inputs() {
        assert_eq!(short_hash(Some("abcdef1234567890abcdef")), "abcdef123456");
    }

    #[test]
    fn short_hash_passes_short_inputs() {
        assert_eq!(short_hash(Some("abc123")), "abc123");
    }

    #[test]
    fn short_hash_renders_dash_for_none() {
        assert_eq!(short_hash(None), "-");
    }

    #[test]
    fn truncate_timestamp_drops_fractional_seconds_and_normalizes_t() {
        assert_eq!(
            truncate_timestamp("2026-05-23T10:40:30.123456789Z"),
            "2026-05-23 10:40:30"
        );
    }

    #[test]
    fn compute_prune_preview_mirrors_platform_logic() {
        assert_eq!(compute_prune_preview(&[10, 9, 8, 7], 10, 2), vec![8, 7]);
        assert_eq!(compute_prune_preview(&[10, 9, 8, 7], 7, 2), vec![8]);
        assert_eq!(compute_prune_preview(&[5, 4, 3], 5, 5), Vec::<i32>::new());
        assert_eq!(compute_prune_preview(&[], 1, 3), Vec::<i32>::new());
    }
}
