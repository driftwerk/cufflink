use std::path::Path;

pub fn init(provider: &str) -> eyre::Result<()> {
    match provider {
        "github" => generate_github_actions()?,
        other => eyre::bail!("Unknown CI provider: {}. Supported: github", other),
    }
    Ok(())
}

fn generate_github_actions() -> eyre::Result<()> {
    let dir = Path::new(".github/workflows");
    std::fs::create_dir_all(dir)?;

    let workflow = include_str!("../../templates/ci/github-actions.yml");
    std::fs::write(dir.join("cufflink.yml"), workflow)?;

    println!("Generated .github/workflows/cufflink.yml");
    println!();
    println!("Required GitHub secrets:");
    println!("  CUFFLINK_STAGING_API_KEY  — API key for staging environment");
    println!("  CUFFLINK_PROD_API_KEY     — API key for production environment");
    println!();
    println!("Pipeline: test -> deploy staging -> integration test -> deploy production");
    println!();
    println!("Ensure your Cufflink.toml has staging and production environments configured.");

    Ok(())
}
