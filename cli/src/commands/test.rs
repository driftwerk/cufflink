use crate::config::CliConfig;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Command;

const HASH_FILE: &str = ".cufflink-test-hash";

pub async fn run(run_all: bool, env: Option<&str>) -> eyre::Result<()> {
    if env.is_some() {
        run_integration_tests(env).await
    } else {
        run_local_tests(run_all)
    }
}

async fn run_integration_tests(env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Running integration tests against environment: {}", name);
    }

    let status = Command::new("cargo")
        .args(["test", "--test", "*"])
        .env("CUFFLINK_API_URL", &config.api_url)
        .env("CUFFLINK_TENANT", &config.tenant_slug)
        .env("CUFFLINK_API_KEY", config.api_key.as_deref().unwrap_or(""))
        .env(
            "CUFFLINK_ENV",
            config.env_name.as_deref().unwrap_or("unknown"),
        )
        .status()?;

    if !status.success() {
        eyre::bail!(
            "Integration tests failed (exit code: {})",
            status.code().unwrap_or(-1)
        );
    }

    println!("All integration tests passed.");
    Ok(())
}

fn run_local_tests(run_all: bool) -> eyre::Result<()> {
    let cwd = std::env::current_dir()?;

    if run_all {
        println!("Running all tests...");
        run_tests(&cwd)?;
        save_hash(&cwd)?;
        return Ok(());
    }

    let current_hash = compute_source_hash(&cwd)?;
    let previous_hash = load_hash(&cwd);

    if Some(&current_hash) == previous_hash.as_ref() {
        println!("No source changes detected since last test run. Use --all to force.");
        return Ok(());
    }

    println!("Source changes detected, running tests...");
    run_tests(&cwd)?;
    save_hash(&cwd)?;

    Ok(())
}

fn run_tests(cwd: &Path) -> eyre::Result<()> {
    let status = Command::new("cargo")
        .arg("test")
        .current_dir(cwd)
        .status()?;

    if !status.success() {
        eyre::bail!("Tests failed (exit code: {})", status.code().unwrap_or(-1));
    }

    println!("All tests passed.");
    Ok(())
}

fn compute_source_hash(cwd: &Path) -> eyre::Result<String> {
    let mut hasher = Sha256::new();
    let src_dir = cwd.join("src");

    if src_dir.exists() {
        hash_directory(&src_dir, &mut hasher)?;
    }

    let cargo_toml = cwd.join("Cargo.toml");
    if cargo_toml.exists() {
        let content = std::fs::read(&cargo_toml)?;
        hasher.update(&content);
    }

    Ok(hex::encode(hasher.finalize()))
}

fn hash_directory(dir: &Path, hasher: &mut Sha256) -> eyre::Result<()> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    entries.sort();

    for entry in entries {
        if entry.is_dir() {
            hash_directory(&entry, hasher)?;
        } else if entry.extension().map(|e| e == "rs").unwrap_or(false) {
            let content = std::fs::read(&entry)?;
            hasher.update(entry.to_string_lossy().as_bytes());
            hasher.update(&content);
        }
    }
    Ok(())
}

fn save_hash(cwd: &Path) -> eyre::Result<()> {
    let hash = compute_source_hash(cwd)?;
    let hash_file = cwd.join(HASH_FILE);
    std::fs::write(hash_file, hash)?;
    Ok(())
}

fn load_hash(cwd: &Path) -> Option<String> {
    let hash_file = cwd.join(HASH_FILE);
    std::fs::read_to_string(hash_file).ok()
}
