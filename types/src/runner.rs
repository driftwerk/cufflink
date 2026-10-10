//! The runner API: what a tenant's runner reads of the one tenant its
//! runner key is bound to. The extension runner reads the native services
//! it runs, their binaries and their environments; a tenant-scoped web
//! runner the web apps it serves, their bundles and their environments.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Whether `hash` is a SHA-256 as the runner API addresses an artifact by:
/// 64 lowercase hex digits.
pub fn is_sha256(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// What a tenant's extension runner runs; a service's binary and
/// environment are under it.
pub const RUNNER_NATIVE_PATH: &str = "/api/runner/native";

/// What a tenant-scoped web runner serves; an app's bundle and environment
/// are under it.
pub const RUNNER_WEB_PATH: &str = "/api/runner/web";

/// The native services the extension runner of `tenant`, the tenant its
/// key is bound to, runs: each active one at its current version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSet {
    pub tenant: String,
    pub services: Vec<NativeService>,
}

/// A native service's current deployment, as its runner runs it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeService {
    pub service_id: Uuid,
    pub service_name: String,
    pub version: i32,
    /// The SHA-256 of its binary, hex; `None` when the deployment records
    /// no binary.
    pub sha256: Option<String>,
    /// The size of its binary in bytes.
    pub size: Option<u64>,
    /// The extensions its manifest provides.
    pub extensions: Vec<String>,
}

/// The web apps the web runner of `tenant`, the tenant its key is bound to,
/// serves: each one's current version with a bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSet {
    pub tenant: String,
    pub apps: Vec<WebApp>,
}

/// A web app's current deployment, as its runner serves it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebApp {
    pub service_id: Uuid,
    pub service_name: String,
    pub version: i32,
    /// The SHA-256 of its bundle, hex; `None` for a bundle uploaded before
    /// bundles were hashed, which the runner API cannot serve.
    pub sha256: Option<String>,
    /// The size of its bundle in bytes.
    pub size: Option<u64>,
}

/// A service's configs and secrets, opened: the variables its runner
/// starts it with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Environment {
    pub environment: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::tests::round_trip;
    use serde_json::json;

    #[test]
    fn an_artifact_is_addressed_by_its_lowercase_sha256() {
        let hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        assert!(is_sha256(hash));
        for other in [
            &hash[1..],
            &format!("{hash}0"),
            &hash.to_uppercase(),
            &format!("../{}", &hash[3..]),
            "environment",
        ] {
            assert!(!is_sha256(other), "{other}");
        }
    }

    #[test]
    fn what_a_runner_is_answered_round_trips() {
        round_trip(
            &NativeSet {
                tenant: "acme".into(),
                services: vec![NativeService {
                    service_id: Uuid::nil(),
                    service_name: "ext-pdf".into(),
                    version: 4,
                    sha256: Some("ab".into()),
                    size: Some(9),
                    extensions: vec!["pdf.render".into()],
                }],
            },
            json!({
                "tenant": "acme",
                "services": [{
                    "service_id": "00000000-0000-0000-0000-000000000000",
                    "service_name": "ext-pdf",
                    "version": 4,
                    "sha256": "ab",
                    "size": 9,
                    "extensions": ["pdf.render"],
                }],
            }),
        );
        round_trip(
            &WebSet {
                tenant: "acme".into(),
                apps: vec![WebApp {
                    service_id: Uuid::nil(),
                    service_name: "portal".into(),
                    version: 3,
                    sha256: None,
                    size: None,
                }],
            },
            json!({
                "tenant": "acme",
                "apps": [{
                    "service_id": "00000000-0000-0000-0000-000000000000",
                    "service_name": "portal",
                    "version": 3,
                    "sha256": null,
                    "size": null,
                }],
            }),
        );
        round_trip(
            &Environment {
                environment: [("GOOGLE_KEY".to_string(), "k".to_string())].into(),
            },
            json!({"environment": {"GOOGLE_KEY": "k"}}),
        );
    }
}
