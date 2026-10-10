use crate::package_manifest::PackageManifest;
use comfy_table::{presets::NOTHING, Table, TableComponent};

const PACKAGES_DIR: &str = "cufflink-packages";

pub async fn run(name: &str, skip: &[String], env: Option<&str>) -> eyre::Result<()> {
    let package_dir = std::env::current_dir()?.join(PACKAGES_DIR).join(name);
    if !package_dir.exists() {
        eyre::bail!(
            "Package '{}' not found in {}/. Run `cufflink install` first.",
            name,
            PACKAGES_DIR
        );
    }

    let manifest = PackageManifest::load(&package_dir)?;

    let services: Vec<_> = manifest
        .services
        .iter()
        .filter(|s| !skip.contains(&s.name))
        .collect();
    let frontends: Vec<_> = manifest
        .frontends
        .iter()
        .filter(|f| !skip.contains(&f.name))
        .collect();

    if services.is_empty() && frontends.is_empty() {
        println!("Nothing to deploy (all components skipped).");
        return Ok(());
    }

    let skipped: Vec<&str> = manifest
        .services
        .iter()
        .map(|s| s.name.as_str())
        .chain(manifest.frontends.iter().map(|f| f.name.as_str()))
        .filter(|n| skip.contains(&n.to_string()))
        .collect();
    if !skipped.is_empty() {
        println!("Skipping: {}", skipped.join(", "));
    }

    if !manifest.config.required.is_empty() {
        println!("Note: Set these config keys after deployment:");
        for key in &manifest.config.required {
            println!("  cufflink config set {} <value>", key);
        }
        println!();
    }

    let original_dir = std::env::current_dir()?;
    let mut results: Vec<(String, &str, Result<(), String>)> = Vec::new();

    for service in &services {
        let service_dir = package_dir.join(&service.path);
        println!("Deploying service '{}'...", service.name);
        std::env::set_current_dir(&service_dir)?;

        match super::deploy::run(false, None, env, None).await {
            Ok(_) => results.push((service.name.clone(), "service", Ok(()))),
            Err(e) => {
                let msg = format!("{}", e);
                eprintln!("  Failed: {}", msg);
                results.push((service.name.clone(), "service", Err(msg)));
            }
        }
    }

    for frontend in &frontends {
        let frontend_dir = package_dir.join(&frontend.path);
        println!("Deploying frontend '{}'...", frontend.name);
        std::env::set_current_dir(&frontend_dir)?;

        match super::deploy::run(false, None, env, None).await {
            Ok(_) => results.push((frontend.name.clone(), "frontend", Ok(()))),
            Err(e) => {
                let msg = format!("{}", e);
                eprintln!("  Failed: {}", msg);
                results.push((frontend.name.clone(), "frontend", Err(msg)));
            }
        }
    }

    std::env::set_current_dir(&original_dir)?;

    println!();

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(vec!["COMPONENT", "TYPE", "STATUS"]);

    let mut failures = 0;
    for (name, kind, result) in &results {
        let status = match result {
            Ok(_) => "deployed",
            Err(_) => {
                failures += 1;
                "FAILED"
            }
        };
        table.add_row(vec![name.as_str(), *kind, status]);
    }

    println!("{table}");

    if failures > 0 {
        eyre::bail!("{} component(s) failed to deploy", failures);
    }

    Ok(())
}
