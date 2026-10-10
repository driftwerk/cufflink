mod builtin;
mod generator;

use crate::config::CliConfig;
use std::path::Path;
use std::process::Command;

/// Parse the local manifest by running `cargo run -- --emit-manifest`.
fn load_local_manifest() -> eyre::Result<cufflink_types::ServiceManifest> {
    let output = Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;

    if !output.status.success() {
        eyre::bail!(
            "Failed to build the service or emit its manifest (run from a cufflink service \
             directory):\n{}",
            String::from_utf8_lossy(&output.stderr).trim_end()
        );
    }

    let stdout = String::from_utf8(output.stdout)?;
    let manifest: cufflink_types::ServiceManifest = serde_json::from_str(stdout.trim())?;
    Ok(manifest)
}

/// Generate the OpenAPI spec locally from the manifest (no deployment required).
fn generate_local_spec(tenant_slug: Option<&str>) -> eyre::Result<serde_json::Value> {
    let manifest = load_local_manifest()?;
    Ok(cufflink_types::openapi::service_spec(
        &manifest,
        tenant_slug.unwrap_or("local"),
    )?)
}

/// Fetch the OpenAPI spec from the deployed platform.
async fn fetch_remote_spec(env: Option<&str>) -> eyre::Result<serde_json::Value> {
    let config = CliConfig::load_with_env(env)?;
    let manifest = load_local_manifest()?;

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
    let services = body["services"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("No services found"))?;

    let service = services
        .iter()
        .find(|s| s["name"].as_str() == Some(&manifest.name))
        .ok_or_else(|| {
            eyre::eyre!(
                "Service '{}' not found. Deploy it first, or use --local.",
                manifest.name
            )
        })?;

    let service_id = service["id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Service has no ID"))?;

    let spec_resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!(
                "{}/api/services/{}/openapi.json",
                config.api_url, service_id
            ),
        )
        .send()
        .await?;

    if !spec_resp.status().is_success() {
        let status = spec_resp.status();
        let body = spec_resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to fetch OpenAPI spec ({}): {}", status, body);
    }

    Ok(spec_resp.json().await?)
}

/// Fetch and display the OpenAPI spec for the current service
pub async fn run(local: bool, tenant_slug: Option<&str>, env: Option<&str>) -> eyre::Result<()> {
    let spec = if local {
        generate_local_spec(tenant_slug)?
    } else {
        fetch_remote_spec(env).await?
    };

    println!("{}", serde_json::to_string_pretty(&spec)?);
    Ok(())
}

/// Fetch OpenAPI spec and write to a file
pub async fn save(
    output_path: &str,
    local: bool,
    tenant_slug: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let spec = if local {
        generate_local_spec(tenant_slug)?
    } else {
        fetch_remote_spec(env).await?
    };

    std::fs::write(output_path, serde_json::to_string_pretty(&spec)?)?;
    println!("OpenAPI spec written to {}", output_path);
    Ok(())
}

/// `cufflink generate-client` options.
#[derive(clap::Args)]
pub struct GenerateClientArgs {
    /// Output directory (default: ./generated)
    #[arg(short, long, default_value = "./generated")]
    pub output: String,
    /// Generate locally from the manifest (no deployment required)
    #[arg(long)]
    pub local: bool,
    /// Tenant slug for URL paths (used with --local, default: "local")
    #[arg(long)]
    pub tenant_slug: Option<String>,
    /// The client generator: the built-in emitter, or openapi-generator's
    /// typescript-fetch through npx
    #[arg(long, value_enum, default_value_t = Generator::Builtin)]
    pub generator: Generator,
    /// npm package name (openapi-generator only)
    #[arg(long)]
    pub npm_name: Option<String>,
    /// Generator properties added to or replacing the defaults, as
    /// K=V,K=V (openapi-generator only)
    #[arg(long)]
    pub additional_properties: Option<String>,
    /// openapi-generator version (openapi-generator only; default 7.21.0)
    #[arg(long)]
    pub generator_version: Option<String>,
    /// Client class name (builtin only; default CufflinkClient)
    #[arg(long)]
    pub class_name: Option<String>,
    /// Write package.json with "type": "module" (builtin only)
    #[arg(long)]
    pub esm: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Generator {
    Builtin,
    OpenapiGenerator,
}

/// The options that only one generator takes, given to the other.
fn check_generator_options(args: &GenerateClientArgs) -> eyre::Result<()> {
    let misplaced = match args.generator {
        Generator::Builtin => vec![
            ("--npm-name", args.npm_name.is_some()),
            (
                "--additional-properties",
                args.additional_properties.is_some(),
            ),
            ("--generator-version", args.generator_version.is_some()),
        ],
        Generator::OpenapiGenerator => vec![
            ("--class-name", args.class_name.is_some()),
            ("--esm", args.esm),
        ],
    };
    match misplaced.iter().find(|(_, given)| *given) {
        Some((flag, _)) => eyre::bail!(
            "{flag} does not apply to --generator {}",
            match args.generator {
                Generator::Builtin => "builtin",
                Generator::OpenapiGenerator => "openapi-generator",
            }
        ),
        None => Ok(()),
    }
}

pub async fn generate_client(args: &GenerateClientArgs, env: Option<&str>) -> eyre::Result<()> {
    check_generator_options(args)?;
    let properties = match args.generator {
        Generator::OpenapiGenerator => {
            generator::require_npx()?;
            Some(generator::properties(
                args.npm_name.as_deref(),
                args.additional_properties.as_deref(),
            )?)
        }
        Generator::Builtin => None,
    };
    let output_dir = Path::new(&args.output);
    let manifest = load_local_manifest()?;
    let spec = if args.local {
        cufflink_types::openapi::service_spec(
            &manifest,
            args.tenant_slug.as_deref().unwrap_or("local"),
        )?
    } else {
        fetch_remote_spec(env).await?
    };

    std::fs::create_dir_all(output_dir)?;
    let spec_path = output_dir.join("openapi.json");
    std::fs::write(&spec_path, serde_json::to_string_pretty(&spec)?)?;
    println!("OpenAPI spec written to {}", spec_path.display());

    match properties {
        Some(properties) => generator::run(
            &spec_path,
            output_dir,
            &properties,
            args.generator_version
                .as_deref()
                .unwrap_or(generator::DEFAULT_GENERATOR_VERSION),
        ),
        None => builtin::write(
            output_dir,
            &spec,
            &manifest,
            args.class_name.as_deref(),
            args.esm,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Command {
        #[command(flatten)]
        args: GenerateClientArgs,
    }

    fn parse(args: &[&str]) -> GenerateClientArgs {
        Command::try_parse_from(std::iter::once("generate-client").chain(args.iter().copied()))
            .expect("parses")
            .args
    }

    #[test]
    fn the_generator_defaults_to_builtin_and_takes_its_own_options() {
        let defaults = parse(&[]);
        assert_eq!(defaults.generator, Generator::Builtin);
        assert!(check_generator_options(&defaults).is_ok());
        let pinned = parse(&[
            "--local",
            "--generator",
            "openapi-generator",
            "--npm-name",
            "@acme/catalog-service-client",
            "--additional-properties",
            "withInterfaces=true",
            "--output",
            "clients/typescript",
            "--generator-version",
            "7.22.0",
        ]);
        assert_eq!(pinned.generator, Generator::OpenapiGenerator);
        assert_eq!(
            pinned.npm_name.as_deref(),
            Some("@acme/catalog-service-client")
        );
        assert_eq!(pinned.generator_version.as_deref(), Some("7.22.0"));
        assert!(check_generator_options(&pinned).is_ok());
        let builtin = parse(&["--class-name", "BooksClient", "--esm"]);
        assert_eq!(builtin.class_name.as_deref(), Some("BooksClient"));
        assert!(builtin.esm && check_generator_options(&builtin).is_ok());
        for (args, flag) in [
            (
                &["--npm-name", "x"][..],
                "--npm-name does not apply to --generator builtin",
            ),
            (
                &["--additional-properties", "a=b"][..],
                "--additional-properties does not apply",
            ),
            (
                &["--generator", "openapi-generator", "--esm"][..],
                "--esm does not apply to --generator openapi-generator",
            ),
            (
                &["--generator", "openapi-generator", "--class-name", "C"][..],
                "--class-name",
            ),
            (
                &["--generator-version", "7.0.0"][..],
                "--generator-version does not apply to --generator builtin",
            ),
        ] {
            let err = check_generator_options(&parse(args))
                .unwrap_err()
                .to_string();
            assert!(err.starts_with(flag), "{err}");
        }
        assert!(Command::try_parse_from(["generate-client", "--generator", "swagger"]).is_err());
    }
}
