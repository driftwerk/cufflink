use crate::config::CliConfig;
use crate::workspace_config::WorkspaceConfig;
use comfy_table::{presets::NOTHING, Table, TableComponent};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn load_workspace() -> eyre::Result<(WorkspaceConfig, PathBuf)> {
    let (config, root) = WorkspaceConfig::find_and_load()?;
    config.validate_paths(&root)?;
    Ok((config, root))
}

fn load_config_from_workspace(
    root: &Path,
    ws: &WorkspaceConfig,
    env: Option<&str>,
) -> eyre::Result<CliConfig> {
    let first = ws
        .services
        .first()
        .ok_or_else(|| eyre::eyre!("No services in workspace"))?;
    let original_dir = std::env::current_dir()?;
    std::env::set_current_dir(root.join(&first.path))?;
    let config = CliConfig::load_with_env(env);
    std::env::set_current_dir(&original_dir)?;
    config
}

fn make_table(headers: Vec<&str>) -> Table {
    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(headers);
    table
}

pub async fn deploy(
    skip: &[String],
    env: Option<&str>,
    tenant_override: Option<&str>,
    parallel: bool,
) -> eyre::Result<()> {
    let (ws, root) = load_workspace()?;
    let env = ws.resolve_env(env);
    println!("Workspace: {}", ws.workspace.name);

    if let Some(name) = env {
        println!("Environment: {}", name);
    }

    let deploy_services = ws.deploy_list(env);
    let deploy_services: Vec<_> = deploy_services
        .into_iter()
        .filter(|s| !skip.contains(&s.name))
        .collect();

    if deploy_services.is_empty() {
        println!("Nothing to deploy.");
        return Ok(());
    }

    if !skip.is_empty() {
        println!("Skipping: {}", skip.join(", "));
    }

    let results = if parallel {
        println!(
            "\nDeploying {} services in parallel...",
            deploy_services.len()
        );
        deploy_parallel(&root, &deploy_services, env, tenant_override).await
    } else {
        deploy_sequential(&root, &deploy_services, env, tenant_override).await
    }?;

    println!();
    let mut table = make_table(vec!["SERVICE", "STATUS"]);
    let mut failures: Vec<(&String, &String)> = Vec::new();
    let deployed: Vec<String> = results
        .iter()
        .filter(|(_, r)| r.is_ok())
        .map(|(n, _)| n.clone())
        .collect();

    for (name, result) in &results {
        let status = match result {
            Ok(_) => "deployed",
            Err(e) => {
                failures.push((name, e));
                "FAILED"
            }
        };
        table.add_row(vec![name.as_str(), status]);
    }
    println!("{table}");

    if !failures.is_empty() {
        println!();
        println!("Failures:");
        for (name, err) in &failures {
            let trimmed = err.trim();
            if trimmed.is_empty() {
                println!("  {} — (no output captured)", name);
            } else {
                println!("  {} —", name);
                for line in trimmed.lines() {
                    println!("    {}", line);
                }
            }
        }
    }

    let seed_services = ws.seed_list(env);
    let seed_services: Vec<_> = seed_services
        .into_iter()
        .filter(|s| deployed.contains(&s.name))
        .collect();

    if !seed_services.is_empty() {
        println!();
        run_seed_phase(&root, &ws, &seed_services, env).await;
    }

    if !failures.is_empty() {
        eyre::bail!("{} service(s) failed to deploy", failures.len());
    }

    Ok(())
}

async fn deploy_sequential(
    root: &Path,
    services: &[&crate::workspace_config::WorkspaceService],
    env: Option<&str>,
    tenant_override: Option<&str>,
) -> eyre::Result<Vec<(String, Result<(), String>)>> {
    let original_dir = std::env::current_dir()?;
    let mut results = Vec::new();

    for svc in services {
        let service_dir = root.join(&svc.path);
        println!("\nDeploying '{}'...", svc.name);
        std::env::set_current_dir(&service_dir)?;

        match super::deploy::run(false, None, env, tenant_override).await {
            Ok(_) => results.push((svc.name.clone(), Ok(()))),
            Err(e) => {
                let msg = format!("{}", e);
                eprintln!("  Failed: {}", msg);
                results.push((svc.name.clone(), Err(msg)));
            }
        }
    }

    std::env::set_current_dir(&original_dir)?;
    Ok(results)
}

async fn deploy_parallel(
    root: &Path,
    services: &[&crate::workspace_config::WorkspaceService],
    env: Option<&str>,
    tenant_override: Option<&str>,
) -> eyre::Result<Vec<(String, Result<(), String>)>> {
    let mut handles = Vec::new();

    for svc in services {
        let name = svc.name.clone();
        let service_dir = root.join(&svc.path);

        let mut args = vec!["deploy".to_string()];
        if let Some(e) = env {
            args.extend(["--env".to_string(), e.to_string()]);
        }
        if let Some(t) = tenant_override {
            args.extend(["--tenant".to_string(), t.to_string()]);
        }

        let handle = tokio::spawn(async move {
            let output = tokio::process::Command::new("cufflink")
                .args(&args)
                .current_dir(&service_dir)
                .output()
                .await;

            match output {
                Ok(o) if o.status.success() => (name, Ok(())),
                Ok(o) => {
                    let stderr = String::from_utf8_lossy(&o.stderr);
                    let stdout = String::from_utf8_lossy(&o.stdout);
                    let msg = if !stderr.is_empty() {
                        stderr.to_string()
                    } else {
                        stdout.to_string()
                    };
                    (name, Err(msg.trim().to_string()))
                }
                Err(e) => (name, Err(format!("Failed to spawn: {}", e))),
            }
        });

        handles.push(handle);
    }

    let mut results = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(result) => results.push(result),
            Err(e) => results.push(("?".to_string(), Err(format!("Task panicked: {}", e)))),
        }
    }

    Ok(results)
}

async fn run_seed_phase(
    root: &std::path::Path,
    ws: &WorkspaceConfig,
    seed_services: &[&crate::workspace_config::WorkspaceService],
    env: Option<&str>,
) {
    println!("Seeding...");

    let config = match load_config_from_workspace(root, ws, env) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config for seeding: {}", e);
            return;
        }
    };

    for svc in seed_services {
        let seed_path = match &svc.seed {
            Some(p) => root.join(p),
            None => continue,
        };

        let service_id = match find_service_id(&config, &svc.name).await {
            Ok(id) => id,
            Err(e) => {
                eprintln!("  {} — failed to resolve: {}", svc.name, e);
                continue;
            }
        };

        match run_seed(&config, &service_id, &seed_path).await {
            Ok(rows) => println!("  {} — {} rows seeded", svc.name, rows),
            Err(e) => eprintln!("  {} — seed failed: {}", svc.name, e),
        }
    }
}

async fn find_service_id(config: &CliConfig, service_name: &str) -> eyre::Result<String> {
    config.find_service_id(service_name).await
}

async fn run_seed(
    config: &CliConfig,
    service_id: &str,
    seed_path: &std::path::Path,
) -> eyre::Result<u64> {
    let data = std::fs::read_to_string(seed_path)?;
    let mut payload: serde_json::Value = serde_json::from_str(&data)?;

    if payload.get("tables").is_none() {
        payload = serde_json::json!({ "tables": payload });
    }

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!("{}/api/services/{}/seed", config.api_url, service_id),
        )
        .json(&payload)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Seed failed ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    Ok(body["total_rows"].as_u64().unwrap_or(0))
}

pub async fn test(skip: &[String], run_all: bool) -> eyre::Result<()> {
    let (ws, root) = load_workspace()?;
    println!("Workspace: {}", ws.workspace.name);
    println!();

    let services: Vec<_> = ws
        .services
        .iter()
        .filter(|s| !skip.contains(&s.name))
        .collect();

    if services.is_empty() {
        println!("No services to test.");
        return Ok(());
    }

    let original_dir = std::env::current_dir()?;
    let mut results: Vec<(String, bool, String)> = Vec::new();

    for svc in &services {
        let service_dir = root.join(&svc.path);

        if !service_dir.join("Cargo.toml").exists() {
            results.push((svc.name.clone(), true, "skipped".to_string()));
            continue;
        }

        std::env::set_current_dir(&service_dir)?;
        print!("Testing {}... ", svc.name);

        let start = Instant::now();
        let mut args = vec!["test"];
        if !run_all {
            args.push("--quiet");
        }

        let output = std::process::Command::new("cargo").args(&args).output();

        let elapsed = start.elapsed();
        let time_str = format!("{:.1}s", elapsed.as_secs_f64());

        match output {
            Ok(o) if o.status.success() => {
                println!("passed ({})", time_str);
                results.push((svc.name.clone(), true, time_str));
            }
            Ok(o) => {
                println!("FAILED ({})", time_str);
                if run_all {
                    let stderr = String::from_utf8_lossy(&o.stderr);
                    let stdout = String::from_utf8_lossy(&o.stdout);
                    if !stdout.is_empty() {
                        eprintln!("{}", stdout);
                    }
                    if !stderr.is_empty() {
                        eprintln!("{}", stderr);
                    }
                }
                results.push((svc.name.clone(), false, time_str));
            }
            Err(e) => {
                println!("ERROR");
                results.push((svc.name.clone(), false, format!("error: {}", e)));
            }
        }
    }

    std::env::set_current_dir(&original_dir)?;

    println!();
    let mut table = make_table(vec!["SERVICE", "RESULT", "TIME"]);

    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    for (name, success, time) in &results {
        if time == "skipped" {
            skipped += 1;
            table.add_row(vec![name.as_str(), "skipped", "-"]);
        } else if *success {
            passed += 1;
            table.add_row(vec![name.as_str(), "passed", time.as_str()]);
        } else {
            failed += 1;
            table.add_row(vec![name.as_str(), "FAILED", time.as_str()]);
        }
    }

    println!("{table}");
    println!();

    let mut summary = format!("{} passed", passed);
    if failed > 0 {
        summary.push_str(&format!(", {} failed", failed));
    }
    if skipped > 0 {
        summary.push_str(&format!(", {} skipped", skipped));
    }
    println!("{}", summary);

    if failed > 0 {
        eyre::bail!("{} service(s) failed tests", failed);
    }

    Ok(())
}

pub async fn status(env: Option<&str>) -> eyre::Result<()> {
    let (ws, root) = load_workspace()?;
    let env = ws.resolve_env(env);
    println!("Workspace: {}", ws.workspace.name);

    let config = load_config_from_workspace(&root, &ws, env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }
    println!();

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
    let api_services = body["services"].as_array();

    let mut table = make_table(vec!["SERVICE", "VERSION", "STATUS", "MODE"]);

    for svc in &ws.services {
        let api_svc =
            api_services.and_then(|arr| arr.iter().find(|s| s["name"].as_str() == Some(&svc.name)));

        match api_svc {
            Some(s) => {
                let version = format!("v{}", s["current_version"]);
                let status = s["status"].as_str().unwrap_or("?");
                let mode = s["mode"].as_str().unwrap_or("?");
                table.add_row(vec![svc.name.as_str(), &version, status, mode]);
            }
            None => {
                table.add_row(vec![svc.name.as_str(), "-", "not deployed", "-"]);
            }
        }
    }

    println!("{table}");
    println!("\n{} service(s)", ws.services.len());

    Ok(())
}

pub async fn seed(env: Option<&str>) -> eyre::Result<()> {
    let (ws, root) = load_workspace()?;
    let env = ws.resolve_env(env);
    println!("Workspace: {}", ws.workspace.name);

    let seed_services = ws.seed_list(env);
    if seed_services.is_empty() {
        println!("No services configured for seeding in this environment.");
        return Ok(());
    }

    run_seed_phase(&root, &ws, &seed_services, env).await;
    Ok(())
}

fn platform_api_key() -> eyre::Result<String> {
    std::env::var("CUFFLINK_PLATFORM_API_KEY")
        .map_err(|_| eyre::eyre!("CUFFLINK_PLATFORM_API_KEY env var required for preview commands"))
}

fn platform_auth_header(api_key: &str) -> String {
    format!("ApiKey {}", api_key)
}

#[derive(serde::Deserialize)]
struct TenantSummary {
    slug: String,
    keycloak_url: Option<String>,
    keycloak_realm: Option<String>,
}

pub async fn preview_create(
    name: &str,
    source_tenant: &str,
    env: Option<&str>,
) -> eyre::Result<()> {
    let (ws, root) = load_workspace()?;
    let env = ws.resolve_env(env);
    let config = load_config_from_workspace(&root, &ws, env)?;
    let api_key = platform_api_key()?;
    let preview_slug = format!("preview-{}", name);

    println!("Creating preview environment '{}'...", preview_slug);

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/api/tenants", config.api_url))
        .header("Authorization", platform_auth_header(&api_key))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to list tenants ({}): {}", status, body);
    }

    let tenants: Vec<TenantSummary> = resp.json().await?;
    let source = tenants
        .iter()
        .find(|t| t.slug == source_tenant)
        .ok_or_else(|| eyre::eyre!("Source tenant '{}' not found", source_tenant))?;

    println!("  Creating tenant '{}'...", preview_slug);
    let create_body = serde_json::json!({
        "name": format!("Preview {}", name),
        "slug": preview_slug,
        "keycloak_url": source.keycloak_url,
        "keycloak_realm": source.keycloak_realm,
        "clone_rbac_from": source_tenant,
    });

    let resp = http
        .post(format!("{}/api/tenants", config.api_url))
        .header("Authorization", platform_auth_header(&api_key))
        .json(&create_body)
        .send()
        .await?;

    if resp.status() == reqwest::StatusCode::CONFLICT {
        println!("  Tenant already exists — reusing it");
        let sync_resp = http
            .post(format!(
                "{}/api/tenants/{}/rbac/sync",
                config.api_url, preview_slug
            ))
            .header("Authorization", platform_auth_header(&api_key))
            .json(&serde_json::json!({ "source_tenant": source_tenant }))
            .send()
            .await?;
        if !sync_resp.status().is_success() {
            let status = sync_resp.status();
            let body = sync_resp.text().await.unwrap_or_default();
            eyre::bail!("Failed to sync preview RBAC ({}): {}", status, body);
        }
        println!("  RBAC synced from '{}'", source_tenant);
    } else if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to create tenant ({}): {}", status, body);
    } else {
        println!("  Tenant created (RBAC cloned from '{}')", source_tenant);
    }

    let deploy_services = ws.deploy_list(env);
    println!(
        "  Deploying {} services in parallel...",
        deploy_services.len()
    );
    let results = deploy_parallel(&root, &deploy_services, env, Some(&preview_slug)).await?;

    for (name, result) in &results {
        match result {
            Ok(_) => println!("    {} — deployed", name),
            Err(e) => println!("    {} — FAILED: {}", name, e),
        }
    }

    let allowlist_mode = ws.preview_allowlist_mode();
    if allowlist_mode {
        println!("  Cloning preview tables from '{}'...", source_tenant);
    } else {
        println!("  Cloning data from '{}'...", source_tenant);
    }

    let source_services =
        list_tenant_services(&http, &config.api_url, &api_key, source_tenant).await?;
    let preview_services =
        list_tenant_services(&http, &config.api_url, &api_key, &preview_slug).await?;

    for svc in &source_services {
        let svc_name = svc["name"].as_str().unwrap_or("?");
        let tables = if allowlist_mode {
            match ws.preview_tables_for(svc_name) {
                Some(tables) if !tables.is_empty() => Some(tables),
                _ => {
                    println!("    {} — no preview_tables, skipping clone", svc_name);
                    continue;
                }
            }
        } else {
            None
        };
        clone_service(
            &http,
            &config.api_url,
            &api_key,
            source_tenant,
            &preview_slug,
            svc,
            &preview_services,
            tables,
        )
        .await?;
    }

    println!();
    println!("Preview environment '{}' ready", preview_slug);

    Ok(())
}

async fn list_tenant_services(
    http: &reqwest::Client,
    api_url: &str,
    api_key: &str,
    tenant_slug: &str,
) -> eyre::Result<Vec<serde_json::Value>> {
    let resp = http
        .get(format!("{}/api/services", api_url))
        .header("Authorization", platform_auth_header(api_key))
        .header("X-Tenant-Slug", tenant_slug)
        .send()
        .await?;

    if !resp.status().is_success() {
        eyre::bail!(
            "Failed to list services for tenant '{}' (HTTP {})",
            tenant_slug,
            resp.status()
        );
    }

    let body: serde_json::Value = resp.json().await?;
    Ok(body["services"].as_array().cloned().unwrap_or_default())
}

#[allow(clippy::too_many_arguments)]
async fn clone_service(
    http: &reqwest::Client,
    api_url: &str,
    api_key: &str,
    source_tenant: &str,
    preview_slug: &str,
    svc: &serde_json::Value,
    preview_services: &[serde_json::Value],
    tables: Option<&[String]>,
) -> eyre::Result<()> {
    let Some(svc_id) = svc["id"].as_str() else {
        return Ok(());
    };
    let svc_name = svc["name"].as_str().unwrap_or("?");

    let Some(preview_svc_id) = preview_services
        .iter()
        .find(|s| s["name"].as_str() == Some(svc_name))
        .and_then(|s| s["id"].as_str())
    else {
        println!("    {} — preview service not found, skipping", svc_name);
        return Ok(());
    };

    let mut export_body = serde_json::json!({});
    let mut restore_extra = serde_json::json!({});
    if let Some(tables) = tables {
        export_body["tables"] = serde_json::json!(tables);
        restore_extra["tables"] = serde_json::json!(tables);
        restore_extra["clear_existing"] = serde_json::json!(true);
        println!("    {} — exporting {}…", svc_name, tables.join(", "));
    } else {
        println!("    {} — exporting…", svc_name);
    }

    let Some(export_job_id) = start_backup_job(
        http,
        api_url,
        api_key,
        source_tenant,
        svc_id,
        "export",
        &export_body,
    )
    .await?
    else {
        println!("    {} — export failed, skipping", svc_name);
        return Ok(());
    };

    let export_job = poll_platform_job(
        http,
        api_url,
        api_key,
        source_tenant,
        svc_id,
        &export_job_id,
    )
    .await?;

    if export_job["status"].as_str() != Some("completed") {
        println!(
            "    {} — export {}",
            svc_name,
            export_job["status"].as_str().unwrap_or("failed")
        );
        return Ok(());
    }

    let Some(s3_key) = export_job["s3_key"].as_str() else {
        println!("    {} — export missing s3_key, skipping", svc_name);
        return Ok(());
    };

    let exported_rows = export_job["processed_rows"].as_i64().unwrap_or(0);
    println!(
        "    {} — exported {} rows, restoring…",
        svc_name, exported_rows
    );

    let mut restore_body = restore_extra;
    restore_body["s3_key"] = serde_json::json!(s3_key);

    let Some(restore_job_id) = start_backup_job(
        http,
        api_url,
        api_key,
        preview_slug,
        preview_svc_id,
        "restore",
        &restore_body,
    )
    .await?
    else {
        println!("    {} — restore failed, skipping", svc_name);
        return Ok(());
    };

    let restore_job = poll_platform_job(
        http,
        api_url,
        api_key,
        preview_slug,
        preview_svc_id,
        &restore_job_id,
    )
    .await?;

    if restore_job["status"].as_str() != Some("completed") {
        println!(
            "    {} — restore {}",
            svc_name,
            restore_job["status"].as_str().unwrap_or("failed")
        );
        return Ok(());
    }

    let rows = restore_job["processed_rows"].as_i64().unwrap_or(0);
    println!("    {} — {} rows cloned", svc_name, rows);
    Ok(())
}

async fn start_backup_job(
    http: &reqwest::Client,
    api_url: &str,
    api_key: &str,
    tenant_slug: &str,
    service_id: &str,
    op: &str,
    body: &serde_json::Value,
) -> eyre::Result<Option<String>> {
    let resp = http
        .post(format!(
            "{}/api/services/{}/backup/{}",
            api_url, service_id, op
        ))
        .header("Authorization", platform_auth_header(api_key))
        .header("X-Tenant-Slug", tenant_slug)
        .json(body)
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(None);
    }

    let payload: serde_json::Value = resp.json().await?;
    Ok(payload["job_id"].as_str().map(String::from))
}

pub async fn preview_destroy(name: &str, env: Option<&str>) -> eyre::Result<()> {
    let (ws, root) = load_workspace()?;
    let env = ws.resolve_env(env);
    let config = load_config_from_workspace(&root, &ws, env)?;
    let api_key = platform_api_key()?;
    let preview_slug = format!("preview-{}", name);

    println!(
        "Destroying preview environment '{}' via {}...",
        preview_slug, config.api_url
    );

    let http = reqwest::Client::new();
    let resp = http
        .delete(format!("{}/api/tenants/{}", config.api_url, preview_slug))
        .header("Authorization", platform_auth_header(&api_key))
        .send()
        .await?;

    if resp.status().is_success() {
        println!("Preview environment '{}' destroyed", preview_slug);
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!(
            "Failed to destroy preview environment ({}): {}",
            status,
            body
        );
    }

    Ok(())
}

const JOB_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
const JOB_POLL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(600);
const JOB_POLL_MAX_CONSECUTIVE_ERRORS: u32 = 5;
const JOB_POLL_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(30);

fn is_terminal_status(status: Option<&str>) -> bool {
    matches!(status, Some("completed" | "failed" | "cancelled"))
}

async fn poll_platform_job(
    http: &reqwest::Client,
    api_url: &str,
    api_key: &str,
    tenant_slug: &str,
    service_id: &str,
    job_id: &str,
) -> eyre::Result<serde_json::Value> {
    let started = Instant::now();
    let mut last_heartbeat = started;
    let mut consecutive_errors: u32 = 0;
    let mut last_status: Option<String> = None;

    loop {
        if started.elapsed() >= JOB_POLL_DEADLINE {
            eyre::bail!(
                "backup job {} for tenant '{}' did not complete within {}s (last status: {})",
                job_id,
                tenant_slug,
                JOB_POLL_DEADLINE.as_secs(),
                last_status.as_deref().unwrap_or("unknown")
            );
        }

        tokio::time::sleep(JOB_POLL_INTERVAL).await;

        let resp = http
            .get(format!(
                "{}/api/services/{}/backup/jobs/{}",
                api_url, service_id, job_id
            ))
            .header("Authorization", platform_auth_header(api_key))
            .header("X-Tenant-Slug", tenant_slug)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                consecutive_errors += 1;
                if consecutive_errors >= JOB_POLL_MAX_CONSECUTIVE_ERRORS {
                    eyre::bail!(
                        "backup job {} for tenant '{}' polling failed {} times consecutively: {}",
                        job_id,
                        tenant_slug,
                        consecutive_errors,
                        e
                    );
                }
                continue;
            }
        };

        consecutive_errors = 0;
        let job: serde_json::Value = resp.json().await?;
        let status = job["status"].as_str();

        if is_terminal_status(status) {
            return Ok(job);
        }

        if last_heartbeat.elapsed() >= JOB_POLL_HEARTBEAT {
            println!(
                "      … job {} status={} progress={}/{} ({}s elapsed)",
                job_id.get(..8).unwrap_or(job_id),
                status.unwrap_or("unknown"),
                job["processed_rows"].as_i64().unwrap_or(0),
                job["total_rows"].as_i64().unwrap_or(0),
                started.elapsed().as_secs()
            );
            last_heartbeat = Instant::now();
        }

        last_status = status.map(String::from);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_statuses_short_circuit_polling() {
        assert!(is_terminal_status(Some("completed")));
        assert!(is_terminal_status(Some("failed")));
        assert!(is_terminal_status(Some("cancelled")));
    }

    #[test]
    fn non_terminal_statuses_keep_polling() {
        assert!(!is_terminal_status(Some("queued")));
        assert!(!is_terminal_status(Some("running")));
        assert!(!is_terminal_status(Some("")));
        assert!(!is_terminal_status(None));
    }

    #[test]
    fn preview_tables_switch_to_allowlist_mode() {
        let toml = r#"
            [workspace]
            name = "test"

            [[services]]
            name = "user-service"
            path = "backend/user-service"
            preview_tables = ["staff", "site_settings"]

            [[services]]
            name = "stock-service"
            path = "backend/stock-service"
        "#;
        let ws: WorkspaceConfig = toml::from_str(toml).unwrap();
        assert!(ws.preview_allowlist_mode());
        assert_eq!(
            ws.preview_tables_for("user-service"),
            Some(&["staff".to_string(), "site_settings".to_string()][..]),
        );
        assert_eq!(ws.preview_tables_for("stock-service"), None);
        assert_eq!(ws.preview_tables_for("unknown-service"), None);
    }

    #[test]
    fn no_preview_tables_means_full_clone_mode() {
        let toml = r#"
            [workspace]
            name = "test"

            [[services]]
            name = "user-service"
            path = "backend/user-service"
        "#;
        let ws: WorkspaceConfig = toml::from_str(toml).unwrap();
        assert!(!ws.preview_allowlist_mode());
    }

    #[test]
    fn parses_list_tenants_bare_array() {
        let body = serde_json::json!([
            {
                "id": "00000000-0000-0000-0000-000000000000",
                "name": "Default",
                "slug": "default",
                "keycloak_url": "https://auth.staging.example",
                "keycloak_realm": "example",
                "deploy_role": "cufflink-deploy",
                "created_at": "2026-01-01T00:00:00Z"
            }
        ]);
        let tenants: Vec<TenantSummary> = serde_json::from_value(body).unwrap();
        assert_eq!(tenants.len(), 1);
        assert_eq!(tenants[0].slug, "default");
        assert_eq!(
            tenants[0].keycloak_url.as_deref(),
            Some("https://auth.staging.example"),
        );
        assert_eq!(tenants[0].keycloak_realm.as_deref(), Some("example"));
    }
}
