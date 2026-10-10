use std::path::Path;
use std::process::Command;

pub(crate) const GENERATOR_PACKAGE: &str = "@openapitools/openapi-generator-cli@2.41.0";

pub(crate) const DEFAULT_GENERATOR_VERSION: &str = "7.21.0";

const DEFAULT_PROPERTIES: [(&str, &str); 5] = [
    ("supportsES6", "true"),
    ("nullSafeAdditionalProps", "true"),
    ("modelPropertyNaming", "camelCase"),
    ("enumPropertyNaming", "camelCase"),
    ("removeEnumValuePrefix", "true"),
];

pub(super) fn properties(npm_name: Option<&str>, overrides: Option<&str>) -> eyre::Result<String> {
    let overrides = overrides
        .into_iter()
        .flat_map(|list| list.split(','))
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            pair.split_once('=')
                .filter(|(key, _)| !key.is_empty())
                .ok_or_else(|| eyre::eyre!("--additional-properties takes K=V pairs, got '{pair}'"))
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    let merged = DEFAULT_PROPERTIES
        .into_iter()
        .chain(npm_name.map(|name| ("npmName", name)))
        .chain(overrides)
        .fold(Vec::<(&str, &str)>::new(), |mut merged, (key, value)| {
            match merged.iter_mut().find(|(existing, _)| *existing == key) {
                Some(entry) => entry.1 = value,
                None => merged.push((key, value)),
            }
            merged
        });
    Ok(merged
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(","))
}

pub(super) fn require_npx() -> eyre::Result<()> {
    let found = std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|dir| {
            ["npx", "npx.cmd"]
                .iter()
                .any(|name| dir.join(name).is_file())
        })
    });
    if found {
        Ok(())
    } else {
        eyre::bail!(
            "--generator openapi-generator runs {GENERATOR_PACKAGE} through npx, which is not on \
             PATH; install Node.js (and a Java runtime, which the generator needs)"
        )
    }
}

pub(super) fn run(spec: &Path, output: &Path, properties: &str, version: &str) -> eyre::Result<()> {
    let config = output.join("openapitools.json");
    std::fs::write(
        &config,
        serde_json::json!({"generator-cli": {"version": version}}).to_string(),
    )?;
    let status = Command::new("npx")
        .args(["--yes", GENERATOR_PACKAGE, "--openapitools"])
        .arg(&config)
        .args(["generate", "-i"])
        .arg(spec)
        .args(["-g", "typescript-fetch", "-o"])
        .arg(output)
        .args(["--additional-properties", properties])
        .status()?;
    if status.success() {
        println!(
            "Generated the typescript-fetch client in {} (openapi-generator {version})",
            output.display()
        );
        Ok(())
    } else {
        eyre::bail!("openapi-generator failed ({status})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_are_the_defaults_with_the_package_and_overrides() {
        assert_eq!(
            properties(Some("@acme/loan-service-client"), None).expect("properties"),
            "supportsES6=true,nullSafeAdditionalProps=true,modelPropertyNaming=camelCase,\
             enumPropertyNaming=camelCase,removeEnumValuePrefix=true,\
             npmName=@acme/loan-service-client"
        );
        assert_eq!(
            properties(None, Some("supportsES6=false, withInterfaces=true")).expect("properties"),
            "supportsES6=false,nullSafeAdditionalProps=true,modelPropertyNaming=camelCase,\
             enumPropertyNaming=camelCase,removeEnumValuePrefix=true,withInterfaces=true"
        );
        for bad in ["noequals", "=value"] {
            assert!(properties(None, Some(bad)).is_err(), "{bad}");
        }
    }
}
