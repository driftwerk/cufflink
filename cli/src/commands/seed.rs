use crate::config::CliConfig;
use std::process::Command;

pub async fn run(
    file: &str,
    clear: bool,
    tables: Option<Vec<String>>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let data = std::fs::read_to_string(file)
        .map_err(|e| eyre::eyre!("Failed to read seed file '{}': {}", file, e))?;

    let mut seed_data: serde_json::Value =
        serde_json::from_str(&data).map_err(|e| eyre::eyre!("Invalid JSON in seed file: {}", e))?;

    if seed_data.get("tables").is_none() {
        seed_data = serde_json::json!({ "tables": seed_data });
    }

    if let Some(ref only_tables) = tables {
        if let Some(tables_obj) = seed_data.get_mut("tables").and_then(|v| v.as_object_mut()) {
            for t in only_tables {
                if !tables_obj.contains_key(t) {
                    eprintln!("Warning: table '{}' not found in seed file", t);
                }
            }
            let keys: Vec<String> = tables_obj.keys().cloned().collect();
            for key in keys {
                if !only_tables.contains(&key) {
                    tables_obj.remove(&key);
                }
            }
        }
    }

    if clear {
        if let Some(obj) = seed_data.as_object_mut() {
            obj.insert("clear_existing".to_string(), serde_json::Value::Bool(true));
        }
    }

    println!("Building service...");
    let output = Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;

    if !output.status.success() {
        eyre::bail!("Failed to build service. Run from a cufflink service directory.");
    }

    let stdout = String::from_utf8(output.stdout)?;
    let manifest: serde_json::Value = serde_json::from_str(stdout.trim())?;
    let service_name = manifest["name"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("No service name in manifest"))?;

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services", config.api_url),
        )
        .send()
        .await?;

    let body: serde_json::Value = resp.json().await?;
    let services = body["services"].as_array().or_else(|| body.as_array());
    let service = services
        .and_then(|arr| {
            arr.iter()
                .find(|s| s["name"].as_str() == Some(service_name))
        })
        .ok_or_else(|| eyre::eyre!("Service '{}' not found. Deploy first.", service_name))?;

    let service_id = service["id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Service has no ID"))?;

    println!("Seeding data...");
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!("{}/api/services/{}/seed", config.api_url, service_id),
        )
        .json(&seed_data)
        .send()
        .await?;

    if resp.status().is_success() {
        let body: serde_json::Value = resp.json().await?;
        println!("Seeded {} total rows", body["total_rows"]);
        if let Some(tables) = body["tables"].as_object() {
            for (name, count) in tables {
                println!("  {} — {} rows", name, count);
            }
        }
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Seed failed ({}): {}", status, body);
    }

    Ok(())
}
