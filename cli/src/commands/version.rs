use crate::config::CliConfig;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::BTreeSet;
use std::process::Command;

const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");
const SDK: &str = "cufflink-fn";

#[derive(Debug, Default, PartialEq, Eq, Deserialize)]
pub struct PlatformVersion {
    #[serde(default, deserialize_with = "number")]
    pub api_version: u32,
    #[serde(default, deserialize_with = "text")]
    pub platform_version: Option<String>,
    #[serde(default, deserialize_with = "names")]
    pub supports: Vec<String>,
}

fn number<'de, D: Deserializer<'de>>(field: D) -> Result<u32, D::Error> {
    Ok(Value::deserialize(field)?
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0))
}

fn text<'de, D: Deserializer<'de>>(field: D) -> Result<Option<String>, D::Error> {
    Ok(Value::deserialize(field)?.as_str().map(str::to_string))
}

fn names<'de, D: Deserializer<'de>>(field: D) -> Result<Vec<String>, D::Error> {
    Ok(Value::deserialize(field)?
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.as_str().map(str::to_string))
        .collect())
}

fn labelled(version: &str) -> String {
    if version.starts_with(|c: char| c.is_ascii_digit()) {
        format!("v{version}")
    } else {
        version.to_string()
    }
}

pub(crate) fn listed(names: &[String]) -> String {
    if names.is_empty() {
        "none".to_string()
    } else {
        names.join(", ")
    }
}

impl PlatformVersion {
    pub(crate) fn label(&self) -> String {
        self.platform_version
            .as_deref()
            .map_or_else(|| "of an unknown version".to_string(), labelled)
    }

    pub(crate) fn offers(&self, feature: &str) -> bool {
        self.supports.iter().any(|offered| offered == feature)
    }
}

#[derive(Debug)]
pub(crate) enum Unanswered {
    Unreachable {
        url: String,
        error: String,
    },
    Refused {
        status: reqwest::StatusCode,
        body: String,
    },
    NotAPlatform {
        url: String,
        error: String,
    },
}

impl std::fmt::Display for Unanswered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unanswered::Unreachable { url, error } => write!(
                f,
                "Could not reach platform at {url}: {error}\n\
                 Check your CUFFLINK_API_URL / Cufflink.toml api_url and network connectivity."
            ),
            Unanswered::Refused { status, body } => {
                write!(f, "Platform /api/version returned {status}: {body}")
            }
            Unanswered::NotAPlatform { url, error } => {
                write!(f, "{url} did not answer as a platform does: {error}")
            }
        }
    }
}

impl std::error::Error for Unanswered {}

pub(crate) async fn asked(
    config: &CliConfig,
    client: &reqwest::Client,
) -> Result<PlatformVersion, Unanswered> {
    let url = format!("{}/api/version", config.api_url);
    let answered = match client.get(&url).send().await {
        Ok(answered) => answered,
        Err(e) => {
            return Err(Unanswered::Unreachable {
                url,
                error: e.to_string(),
            })
        }
    };
    let status = answered.status();
    if !status.is_success() {
        return Err(Unanswered::Refused {
            status,
            body: answered.text().await.unwrap_or_default(),
        });
    }
    answered.json().await.map_err(|e| Unanswered::NotAPlatform {
        url,
        error: e.to_string(),
    })
}

fn sdk_versions(metadata: &Value) -> BTreeSet<String> {
    metadata["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|package| package["name"] == SDK)
        .filter_map(|package| package["version"].as_str())
        .map(labelled)
        .collect()
}

pub fn sdk_version() -> Option<String> {
    Command::new("cargo")
        .args(["metadata", "--format-version", "1"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| serde_json::from_slice::<Value>(&output.stdout).ok())
        .map(|metadata| sdk_versions(&metadata))
        .filter(|versions| !versions.is_empty())
        .map(|versions| versions.into_iter().collect::<Vec<_>>().join(", "))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Asking<'a> {
    Sdk(Option<&'a str>),
    Bundle { cli: &'a str },
}

pub fn negotiated(platform: &PlatformVersion, asking: &Asking<'_>, manifest: &Value) -> String {
    let needs: Vec<String> = manifest["required_platform_features"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    let asking = match asking {
        Asking::Sdk(version) => format!("sdk {}", version.unwrap_or("of an unknown version")),
        Asking::Bundle { cli } => format!("bundle from cufflink v{cli}"),
    };
    format!(
        "platform {} (features {}) ← {asking} (needs {})",
        platform.label(),
        listed(&platform.supports),
        listed(&needs)
    )
}

fn described(api_url: &str, platform: &PlatformVersion) -> String {
    format!(
        "platform {} at {api_url} (deploy API {})\nfeatures {}",
        platform.label(),
        platform.api_version,
        listed(&platform.supports)
    )
}

pub async fn print(env: Option<&str>) -> eyre::Result<()> {
    println!("cufflink {CLI_VERSION}");
    if env.is_some() {
        let config = CliConfig::load_with_env(env)?;
        let platform = asked(&config, &config.http_client()).await?;
        println!("{}", described(&config.api_url, &platform));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn platform(version: Option<&str>, supports: &[&str]) -> PlatformVersion {
        PlatformVersion {
            api_version: 1,
            platform_version: version.map(str::to_string),
            supports: supports.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn a_deploy_says_what_the_platform_offers_and_the_service_needs() {
        let offered = platform(
            Some("sha-4f2a"),
            &["bundled_wasm", "db_v2", "route_posture"],
        );
        assert_eq!(
            negotiated(
                &offered,
                &Asking::Sdk(Some("v0.27.0")),
                &json!({"required_platform_features": ["db_v2", "route_posture"]})
            ),
            "platform sha-4f2a (features bundled_wasm, db_v2, route_posture) ← sdk v0.27.0 \
             (needs db_v2, route_posture)"
        );
        assert_eq!(
            negotiated(
                &platform(Some("0.27.0"), &[]),
                &Asking::Sdk(None),
                &json!({"name": "notes"})
            ),
            "platform v0.27.0 (features none) ← sdk of an unknown version (needs none)"
        );
        assert_eq!(
            negotiated(
                &platform(None, &["streams"]),
                &Asking::Sdk(Some("v0.26.1")),
                &json!({})
            ),
            "platform of an unknown version (features streams) ← sdk v0.26.1 (needs none)"
        );
        assert_eq!(
            negotiated(
                &platform(Some("0.30.0"), &["db_v2"]),
                &Asking::Bundle { cli: "0.21.0" },
                &json!({"required_platform_features": ["db_v2"]})
            ),
            "platform v0.30.0 (features db_v2) ← bundle from cufflink v0.21.0 (needs db_v2)"
        );
    }

    #[test]
    fn the_sdk_is_every_version_of_it_cargo_resolved() {
        let metadata = json!({"packages": [
            {"name": "cufflink-fn", "version": "0.27.0"},
            {"name": "cufflink", "version": "0.11.0"},
            {"name": "cufflink-fn", "version": "0.26.1"},
            {"name": "cufflink-fn", "version": "0.27.0"},
        ]});
        assert_eq!(
            sdk_versions(&metadata).into_iter().collect::<Vec<_>>(),
            ["v0.26.1", "v0.27.0"]
        );
        assert!(sdk_versions(&json!({})).is_empty());
    }

    #[test]
    fn a_platform_is_read_from_what_it_answers() {
        let answered: PlatformVersion = serde_json::from_value(json!({
            "api_version": 1,
            "platform_version": "sha-4f2a",
            "supports": ["bundled_wasm", "streams"],
        }))
        .expect("a platform");
        assert_eq!(
            answered,
            platform(Some("sha-4f2a"), &["bundled_wasm", "streams"])
        );
        assert_eq!(
            described("http://localhost:8080", &answered),
            "platform sha-4f2a at http://localhost:8080 (deploy API 1)\n\
             features bundled_wasm, streams"
        );
        let silent: PlatformVersion = serde_json::from_value(json!({})).expect("a platform");
        assert_eq!(silent, PlatformVersion::default());
        let odd: PlatformVersion = serde_json::from_value(json!({
            "api_version": "1",
            "platform_version": 7,
            "supports": ["bundled_wasm", 7, {"streams": true}, "streams"],
        }))
        .expect("a platform that answers what is not of its type");
        assert_eq!(
            odd,
            PlatformVersion {
                api_version: 0,
                ..platform(None, &["bundled_wasm", "streams"])
            }
        );
        let unlisted: PlatformVersion =
            serde_json::from_value(json!({"supports": "all"})).expect("a platform");
        assert!(unlisted.supports.is_empty());
    }
}
