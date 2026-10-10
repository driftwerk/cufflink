use crate::config::CliConfig;
use std::process::Command;

pub async fn run(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let output = Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;

    if !output.status.success() {
        eyre::bail!("Failed to build service. Run from a cufflink service directory.");
    }

    let stdout = String::from_utf8(output.stdout)?;
    let manifest: serde_json::Value = serde_json::from_str(stdout.trim())?;
    let service_name = manifest["name"].as_str().unwrap_or("unknown");

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
    let services = body["services"].as_array();

    let service = services.and_then(|svcs| {
        svcs.iter()
            .find(|s| s["name"].as_str() == Some(service_name))
    });

    match service {
        Some(svc) => {
            let id = svc["id"].as_str().unwrap_or("?");
            println!("Service: {}", service_name);
            println!("  ID:      {}", id);
            println!("  Version: v{}", svc["current_version"]);
            println!("  Status:  {}", svc["status"].as_str().unwrap_or("?"));
            println!("  Tenant:  {}", svc["tenant_slug"].as_str().unwrap_or("?"));
            println!("  Created: {}", svc["created_at"].as_str().unwrap_or("?"));
            println!("  Updated: {}", svc["updated_at"].as_str().unwrap_or("?"));

            let deploy_resp = config
                .auth_request(
                    &client,
                    reqwest::Method::GET,
                    &format!("{}/api/services/{}/deployments", config.api_url, id),
                )
                .send()
                .await?;

            if deploy_resp.status().is_success() {
                let deploy_body: serde_json::Value = deploy_resp.json().await?;
                if let Some(deployments) = deploy_body["deployments"].as_array() {
                    println!("\n  Deployments ({}):", deployments.len());
                    for d in deployments.iter().take(5) {
                        let commit_suffix = d["commit_hash"]
                            .as_str()
                            .map(|h| {
                                let short = if h.len() > 7 { &h[..7] } else { h };
                                format!(" ({})", short)
                            })
                            .unwrap_or_default();
                        println!(
                            "    v{} — {} ({}){}",
                            d["version"],
                            d["status"].as_str().unwrap_or("?"),
                            d["deployed_at"].as_str().unwrap_or("?"),
                            commit_suffix,
                        );
                    }
                }
            }

            let tenant = svc["tenant_slug"].as_str().unwrap_or("default");
            table_lines(&manifest, tenant, service_name)
                .iter()
                .for_each(|line| println!("{line}"));
        }
        None => {
            println!("Service '{}' not found on the platform.", service_name);
            println!("Use `cufflink deploy` to deploy it.");
        }
    }

    Ok(())
}

fn table_lines(manifest: &serde_json::Value, tenant: &str, service: &str) -> Vec<String> {
    let name = |table: &serde_json::Value| table["name"].as_str().unwrap_or("?").to_string();
    let listed = |heading: &str, tables: &[serde_json::Value]| {
        std::iter::once(format!("\n  {heading}:"))
            .chain(tables.iter().map(|table| {
                format!(
                    "    {} ({} columns)",
                    name(table),
                    table["columns"].as_array().map_or(0, Vec::len)
                )
            }))
            .collect::<Vec<_>>()
    };
    let declared = |list: &str| {
        manifest[list]
            .as_array()
            .filter(|tables| !tables.is_empty())
    };
    let (served, private) = (declared("tables"), declared("private_tables"));
    let endpoints = served.into_iter().flat_map(|tables| {
        std::iter::once("\n  Endpoints:".to_string()).chain(tables.iter().flat_map(|table| {
            let collection = format!("/svc/{tenant}/{service}/{}", name(table));
            [
                format!("    GET    {collection}"),
                format!("    POST   {collection}"),
                format!("    GET    {collection}/:id"),
                format!("    PUT    {collection}/:id"),
                format!("    DELETE {collection}/:id"),
            ]
        }))
    });
    served
        .map(|tables| listed("Tables", tables))
        .into_iter()
        .chain(private.map(|tables| listed(super::deploy::PRIVATE_TABLES, tables)))
        .flatten()
        .chain(endpoints)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn served_tables_are_printed_with_their_endpoints_as_before() {
        let manifest = json!({"name": "svc", "tables": [
            {"name": "items", "columns": [{"name": "id"}, {"name": "title"}]}
        ]});
        assert_eq!(
            table_lines(&manifest, "acme", "svc"),
            [
                "\n  Tables:",
                "    items (2 columns)",
                "\n  Endpoints:",
                "    GET    /svc/acme/svc/items",
                "    POST   /svc/acme/svc/items",
                "    GET    /svc/acme/svc/items/:id",
                "    PUT    /svc/acme/svc/items/:id",
                "    DELETE /svc/acme/svc/items/:id",
            ]
        );
        for none in [json!({"name": "svc", "tables": []}), json!({"name": "svc"})] {
            assert!(table_lines(&none, "acme", "svc").is_empty(), "{none}");
        }
    }

    #[test]
    fn status_prints_no_endpoint_for_a_private_table() {
        let manifest = json!({
            "name": "loan-service",
            "tables": [{"name": "labels", "columns": [{"name": "id"}]}],
            "private_tables": [{"name": "loans", "columns": [{"name": "id"}, {"name": "body"}]}]
        });
        let lines = table_lines(&manifest, "harbor", "loan-service");
        assert_eq!(
            lines[..4],
            [
                "\n  Tables:",
                "    labels (1 columns)",
                "\n  Private tables (checked, not served):",
                "    loans (2 columns)",
            ]
        );
        assert_eq!(lines.len(), 10);
        assert!(
            lines[4..].iter().all(|line| !line.contains("loans")),
            "{lines:?}"
        );
        assert!(lines[5..].iter().all(|line| line.contains("/labels")));

        let private_only = json!({
            "name": "loan-service",
            "tables": [],
            "private_tables": [{"name": "loans", "columns": [{"name": "id"}]}]
        });
        assert_eq!(
            table_lines(&private_only, "harbor", "loan-service"),
            [
                "\n  Private tables (checked, not served):",
                "    loans (1 columns)"
            ]
        );
    }
}
