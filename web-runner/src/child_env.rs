use cufflink_supervisor::runner_api::KEY_FILE_VAR;
use cufflink_supervisor::{EnvMode, EnvPolicy};

pub(crate) const WEB_ENV: EnvPolicy = EnvPolicy {
    passthrough: "WEB_ENV_PASSTHROUGH",
    allow: &[
        "NODE_OPTIONS",
        "NODE_EXTRA_CA_CERTS",
        "CUFFLINK_BASE_DOMAIN",
        "CUFFLINK_PUBLIC_SCHEME",
        "NEXT_TELEMETRY_DISABLED",
    ],
    credentials: &[KEY_FILE_VAR],
};

pub fn env_mode(raw: Option<&str>) -> eyre::Result<EnvMode> {
    if raw.map(str::trim) == Some("*") {
        eyre::bail!(
            "WEB_ENV_PASSTHROUGH=* is no longer accepted: it passed this runner's whole environment, credentials included, to every web app. \
             Apps always see {}; list the other variables they need as NAME,PREFIX_* (a credential never passes)",
            WEB_ENV.allowlist().collect::<Vec<_>>().join(", ")
        );
    }
    EnvMode::parse(WEB_ENV, raw)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildIdentity {
    pub port: u16,
    pub public_url: String,
    pub tenant: String,
    pub service: String,
}

impl ChildIdentity {
    pub fn vars(&self) -> impl Iterator<Item = (String, String)> {
        [
            ("PORT", self.port.to_string()),
            ("NODE_ENV", "production".to_string()),
            ("HOSTNAME", "127.0.0.1".to_string()),
            ("CUFFLINK_PUBLIC_URL", self.public_url.clone()),
            ("CUFFLINK_TENANT", self.tenant.clone()),
            ("CUFFLINK_SERVICE", self.service.clone()),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_supervisor::child_env;
    use std::collections::{BTreeMap, HashMap};

    fn identity() -> ChildIdentity {
        ChildIdentity {
            port: 3207,
            public_url: "https://portal--preview-pr-7.staging.example.com".into(),
            tenant: "preview-pr-7".into(),
            service: "portal".into(),
        }
    }

    fn runner_env() -> Vec<(String, String)> {
        [
            ("PATH", "/usr/bin"),
            ("HOME", "/home/runner"),
            ("CUFFLINK_INTERNAL_URL", "http://cufflink-platform:8080"),
            ("DATABASE_URL", "postgres://cufflink:secret@db/cufflink"),
            ("NATS_PASSWORD", "nats-secret"),
            ("CUFFLINK_SECRETS_KEK", "k"),
            ("OTEL_SERVICE_NAME", "web"),
            ("SENTRY_DSN", "https://sentry"),
            ("WEB_STATUS_TOKEN", "ops-secret"),
            ("CUFFLINK_RUNNER_KEY", "cfr_AAAAAAAAAAAA_s3cret"),
            ("CUFFLINK_RUNNER_KEY_FILE", "/run/secrets/runner-key"),
            ("PORT", "3100"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn app_env(mode: &EnvMode) -> BTreeMap<String, String> {
        child_env(runner_env(), mode, &HashMap::new(), None, identity().vars())
    }

    #[test]
    fn the_default_allowlist_passes_no_credential() {
        let env = app_env(&env_mode(None).expect("default"));
        assert_eq!(
            env.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "CUFFLINK_INTERNAL_URL",
                "CUFFLINK_PUBLIC_URL",
                "CUFFLINK_SERVICE",
                "CUFFLINK_TENANT",
                "HOME",
                "HOSTNAME",
                "NODE_ENV",
                "OTEL_SERVICE_NAME",
                "PATH",
                "PORT",
            ]
        );
        assert_eq!(
            env["PORT"], "3207",
            "the child's own port, not the runner's"
        );
    }

    #[test]
    fn web_runner_env_contract() {
        let env = app_env(&env_mode(None).expect("default"));
        assert_eq!(
            env["CUFFLINK_PUBLIC_URL"],
            "https://portal--preview-pr-7.staging.example.com"
        );
        assert_eq!(env["CUFFLINK_TENANT"], "preview-pr-7");
        assert_eq!(
            env["CUFFLINK_INTERNAL_URL"],
            "http://cufflink-platform:8080"
        );
    }

    #[test]
    fn the_status_token_and_the_runner_key_never_pass() {
        let env = app_env(
            &env_mode(Some(
                "SENTRY_*,WEB_*,CUFFLINK_*,CUFFLINK_RUNNER_KEY,CUFFLINK_RUNNER_KEY_FILE",
            ))
            .expect("patterns"),
        );
        assert!(env.contains_key("SENTRY_DSN"));
        for credential in [
            "WEB_STATUS_TOKEN",
            "CUFFLINK_RUNNER_KEY",
            "CUFFLINK_RUNNER_KEY_FILE",
        ] {
            assert!(!env.contains_key(credential), "{credential}");
        }
    }

    #[test]
    fn inherit_all_is_refused_with_the_allowlist() {
        for raw in ["*", " * "] {
            let error = env_mode(Some(raw)).expect_err(raw).to_string();
            assert!(
                error.starts_with("WEB_ENV_PASSTHROUGH=* is no longer accepted"),
                "{error}"
            );
            for name in WEB_ENV.allowlist() {
                assert!(error.contains(name), "{error} lacks {name}");
            }
        }
        let error = env_mode(Some("*,PATH")).expect_err("a wildcard among names");
        assert!(error.to_string().starts_with("WEB_ENV_PASSTHROUGH: '*'"));
    }
}
