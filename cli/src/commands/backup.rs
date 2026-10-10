use crate::config::CliConfig;
use crate::project_config::ProjectConfig;
use comfy_table::{presets::NOTHING, Table, TableComponent};
use std::collections::{HashMap, HashSet};

fn format_tables_filter(job: &serde_json::Value) -> String {
    match job["tables_filter"].as_array() {
        Some(arr) if !arr.is_empty() => arr
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        _ => "all".to_string(),
    }
}

async fn resolve_service(config: &CliConfig) -> eyre::Result<(String, String)> {
    let name = resolve_service_name()?;
    let id = find_service_id(config, &name).await?;
    Ok((name, id))
}

fn resolve_service_name() -> eyre::Result<String> {
    if let Some(project) = ProjectConfig::find_and_load()? {
        if let Some(name) = project.service.name.as_deref() {
            return Ok(name.to_string());
        }
    }

    let output = std::process::Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;

    if !output.status.success() {
        eyre::bail!("Cannot determine service name. Run from a cufflink service directory.");
    }

    let stdout = String::from_utf8(output.stdout)?;
    let manifest: serde_json::Value = serde_json::from_str(stdout.trim())?;
    manifest["name"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| eyre::eyre!("No service name in manifest"))
}

async fn find_service_id(config: &CliConfig, service_name: &str) -> eyre::Result<String> {
    config.find_service_id(service_name).await
}

async fn fetch_schema(config: &CliConfig, service_id: &str) -> eyre::Result<serde_json::Value> {
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services/{}/schema", config.api_url, service_id),
        )
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to fetch schema ({}): {}", status, body);
    }

    Ok(resp.json().await?)
}

fn collect_fk_deps(tables: &[serde_json::Value]) -> HashMap<String, Vec<(String, String)>> {
    let mut deps: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for table in tables {
        let tname = table["name"].as_str().unwrap_or("");
        if let Some(cols) = table["columns"].as_array() {
            for col in cols {
                if let Some(ref_str) = col["references"].as_str() {
                    deps.entry(tname.to_string()).or_default().push((
                        col["name"].as_str().unwrap_or("").to_string(),
                        ref_str.to_string(),
                    ));
                }
            }
        }
    }
    deps
}

pub async fn schema(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let (service_name, service_id) = resolve_service(&config).await?;
    let schema_resp = fetch_schema(&config, &service_id).await?;
    let tables = schema_resp["tables"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("No tables"))?;

    let fk_map = collect_fk_deps(tables);

    println!("Service: {}", service_name);
    println!();

    for table in tables {
        let tname = table["name"].as_str().unwrap_or("?");
        println!("  {}", tname);

        if let Some(cols) = table["columns"].as_array() {
            for col in cols {
                let name = col["name"].as_str().unwrap_or("?");
                let col_type = col["column_type"].as_str().unwrap_or("?");
                let pk = col["primary_key"].as_bool().unwrap_or(false);
                let nullable = col["nullable"].as_bool().unwrap_or(false);
                let fk = col["references"].as_str();

                let mut flags = Vec::new();
                if pk {
                    flags.push("PK");
                }
                if nullable {
                    flags.push("nullable");
                }
                if fk.is_some() {
                    flags.push("FK");
                }

                let flags_str = if flags.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", flags.join(", "))
                };

                if let Some(ref_str) = fk {
                    println!("    {} {}{} -> {}", name, col_type, flags_str, ref_str);
                } else {
                    println!("    {} {}{}", name, col_type, flags_str);
                }
            }
        }

        if let Some(deps) = fk_map.get(tname) {
            let ref_tables: HashSet<&str> = deps
                .iter()
                .map(|(_, r)| r.split('.').next().unwrap_or("?"))
                .collect();
            println!(
                "    depends on: {}",
                ref_tables.into_iter().collect::<Vec<_>>().join(", ")
            );
        }
        println!();
    }

    Ok(())
}

pub async fn export(
    output_path: &str,
    tables_filter: Option<Vec<String>>,
    run_async: bool,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let (service_name, service_id) = resolve_service(&config).await?;
    println!("Service: {}", service_name);

    if let Some(ref filter) = tables_filter {
        warn_fk_deps(&config, &service_id, filter).await;
    }

    let payload = serde_json::json!({ "tables": tables_filter });
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!(
                "{}/api/services/{}/backup/export",
                config.api_url, service_id
            ),
        )
        .json(&payload)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Export failed ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let job_id = body["job_id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("No job_id in response"))?;

    if run_async {
        println!("Export job started: {}", job_id);
        println!("Check status: cufflink backup jobs");
        return Ok(());
    }

    println!("Exporting...");
    let job = poll_job(&config, &service_id, job_id).await?;

    if job["status"].as_str() != Some("completed") {
        let err = job["error_message"].as_str().unwrap_or("Unknown error");
        eyre::bail!("Export failed: {}", err);
    }

    println!("Downloading...");
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!(
                "{}/api/services/{}/backup/jobs/{}/download",
                config.api_url, service_id, job_id
            ),
        )
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Download failed ({}): {}", status, body);
    }

    let data = resp.bytes().await?;
    std::fs::write(output_path, &data)?;

    let rows = job["processed_rows"].as_i64().unwrap_or(0);
    println!("Backup saved to {} ({} rows)", output_path, rows);

    Ok(())
}

pub async fn restore(
    input_path: Option<&str>,
    from_backup: Option<&str>,
    tables_filter: Option<Vec<String>>,
    clear_existing: bool,
    force: bool,
    run_async: bool,
    env: Option<&str>,
) -> eyre::Result<()> {
    if input_path.is_none() && from_backup.is_none() {
        eyre::bail!("Provide a file path or --from-backup <job-id>");
    }

    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let (service_name, service_id) = resolve_service(&config).await?;
    println!("Service: {}", service_name);

    if !force {
        if let Some(ref filter) = tables_filter {
            validate_restore_tables(&config, &service_id, filter).await?;
        }
    }

    let client = config.http_client();

    let s3_key = if let Some(backup_job_id) = from_backup {
        let resp = config
            .auth_request(
                &client,
                reqwest::Method::GET,
                &format!(
                    "{}/api/services/{}/backup/jobs/{}",
                    config.api_url, service_id, backup_job_id
                ),
            )
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            eyre::bail!("Failed to fetch backup job ({}): {}", status, body);
        }

        let job: serde_json::Value = resp.json().await?;
        if job["status"].as_str() != Some("completed") {
            eyre::bail!("Backup job is not completed (status: {})", job["status"]);
        }
        if job["job_type"].as_str() != Some("export") {
            eyre::bail!("Job {} is not an export", backup_job_id);
        }

        println!(
            "Restoring from backup {}...",
            &backup_job_id[..8.min(backup_job_id.len())]
        );
        job["s3_key"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("Backup job has no S3 key"))?
            .to_string()
    } else {
        let input_path = input_path.unwrap();
        println!("Requesting upload URL...");
        let resp = config
            .auth_request(
                &client,
                reqwest::Method::POST,
                &format!(
                    "{}/api/services/{}/backup/upload",
                    config.api_url, service_id
                ),
            )
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            eyre::bail!("Failed to get upload URL ({}): {}", status, body);
        }

        let upload_resp: serde_json::Value = resp.json().await?;
        let upload_url = upload_resp["upload_url"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("No upload_url in response"))?;
        let key = upload_resp["s3_key"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("No s3_key in response"))?
            .to_string();

        println!("Uploading backup file...");
        let data = std::fs::read(input_path)?;
        let upload_client = reqwest::Client::new();
        let resp = upload_client
            .put(upload_url)
            .header("content-type", "application/x-ndjson")
            .body(data)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            eyre::bail!("Upload failed ({})", status);
        }

        key
    };

    let payload = serde_json::json!({
        "s3_key": s3_key,
        "tables": tables_filter,
        "clear_existing": clear_existing,
    });

    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!(
                "{}/api/services/{}/backup/restore",
                config.api_url, service_id
            ),
        )
        .json(&payload)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Restore failed ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let job_id = body["job_id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("No job_id"))?;

    if run_async {
        println!("Restore job started: {}", job_id);
        println!("Check status: cufflink backup jobs");
        return Ok(());
    }

    println!("Restoring...");
    let job = poll_job(&config, &service_id, job_id).await?;

    if job["status"].as_str() != Some("completed") {
        let err = job["error_message"].as_str().unwrap_or("Unknown error");
        eyre::bail!("Restore failed: {}", err);
    }

    let rows = job["processed_rows"].as_i64().unwrap_or(0);
    println!("Restore complete ({} rows)", rows);

    Ok(())
}

pub async fn list(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let (service_name, service_id) = resolve_service(&config).await?;
    println!("Service: {}", service_name);
    println!();

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services/{}/backup/jobs", config.api_url, service_id),
        )
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to list backups ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let exports: Vec<&serde_json::Value> = body["jobs"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter(|j| {
                    j["job_type"].as_str() == Some("export")
                        && j["status"].as_str() == Some("completed")
                })
                .collect()
        })
        .unwrap_or_default();

    if exports.is_empty() {
        println!("No completed backups.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(vec!["ID", "TABLES", "ROWS", "CREATED"]);

    for job in &exports {
        table.add_row(vec![
            job["id"].as_str().unwrap_or("?"),
            &format_tables_filter(job),
            &job["processed_rows"].to_string(),
            job["created_at"].as_str().unwrap_or("?"),
        ]);
    }

    println!("{table}");
    println!();
    println!("Restore from a backup: cufflink backup restore --from-backup <ID>");

    Ok(())
}

pub async fn jobs(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let (service_name, service_id) = resolve_service(&config).await?;
    println!("Service: {}", service_name);
    println!();

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services/{}/backup/jobs", config.api_url, service_id),
        )
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to list jobs ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let jobs = body["jobs"].as_array();

    if jobs.map(|j| j.is_empty()).unwrap_or(true) {
        println!("No backup jobs.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(vec!["ID", "TYPE", "STATUS", "TABLES", "ROWS", "CREATED"]);

    for job in jobs.unwrap() {
        table.add_row(vec![
            job["id"].as_str().unwrap_or("?"),
            job["job_type"].as_str().unwrap_or("?"),
            job["status"].as_str().unwrap_or("?"),
            &format_tables_filter(job),
            &job["processed_rows"].to_string(),
            job["created_at"].as_str().unwrap_or("?"),
        ]);
    }

    println!("{table}");
    Ok(())
}

pub async fn cancel(job_id: &str, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let (_service_name, service_id) = resolve_service(&config).await?;

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!(
                "{}/api/services/{}/backup/jobs/{}/cancel",
                config.api_url, service_id, job_id
            ),
        )
        .send()
        .await?;

    if resp.status().is_success() {
        println!("Job {} cancelled.", job_id);
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Cancel failed ({}): {}", status, body);
    }

    Ok(())
}

async fn poll_job(
    config: &CliConfig,
    service_id: &str,
    job_id: &str,
) -> eyre::Result<serde_json::Value> {
    let client = config.http_client();
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let resp = config
            .auth_request(
                &client,
                reqwest::Method::GET,
                &format!(
                    "{}/api/services/{}/backup/jobs/{}",
                    config.api_url, service_id, job_id
                ),
            )
            .send()
            .await?;

        if !resp.status().is_success() {
            continue;
        }

        let job: serde_json::Value = resp.json().await?;
        let status = job["status"].as_str().unwrap_or("");
        let rows = job["processed_rows"].as_i64().unwrap_or(0);

        eprint!("\r  {} rows processed... ", rows);

        match status {
            "completed" | "failed" | "cancelled" => {
                eprintln!();
                return Ok(job);
            }
            _ => continue,
        }
    }
}

async fn warn_fk_deps(config: &CliConfig, service_id: &str, filter: &[String]) {
    let schema = match fetch_schema(config, service_id).await {
        Ok(s) => s,
        Err(_) => return,
    };

    let tables = match schema["tables"].as_array() {
        Some(t) => t,
        None => return,
    };

    let filter_set: HashSet<&str> = filter.iter().map(|s| s.as_str()).collect();
    let all_tables: HashSet<&str> = tables.iter().filter_map(|t| t["name"].as_str()).collect();

    let mut missing = Vec::new();
    for table in tables {
        let tname = table["name"].as_str().unwrap_or("");
        if !filter_set.contains(tname) {
            continue;
        }
        if let Some(cols) = table["columns"].as_array() {
            for col in cols {
                if let Some(ref_str) = col["references"].as_str() {
                    let ref_table = ref_str.split('.').next().unwrap_or("");
                    if !filter_set.contains(ref_table) && all_tables.contains(ref_table) {
                        missing.push(ref_table.to_string());
                    }
                }
            }
        }
    }

    if !missing.is_empty() {
        let unique: HashSet<String> = missing.into_iter().collect();
        println!(
            "WARNING: Selected tables have FK dependencies on: {}",
            unique.into_iter().collect::<Vec<_>>().join(", ")
        );
        println!("  Consider including these tables for a complete backup.");
        println!();
    }
}

async fn validate_restore_tables(
    config: &CliConfig,
    service_id: &str,
    filter: &[String],
) -> eyre::Result<()> {
    let schema = fetch_schema(config, service_id).await?;
    let tables = schema["tables"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("No tables"))?;

    let schema_tables: HashSet<String> = tables
        .iter()
        .filter_map(|t| t["name"].as_str().map(String::from))
        .collect();

    let unknown: Vec<&str> = filter
        .iter()
        .filter(|t| !schema_tables.contains(t.as_str()))
        .map(|s| s.as_str())
        .collect();

    if !unknown.is_empty() {
        eyre::bail!(
            "Tables not in current schema: {}. Use --force to skip this check.",
            unknown.join(", ")
        );
    }

    Ok(())
}
