use crate::child::EXT_ENV;
use crate::socket;
use cufflink_settings::Vars;
use cufflink_supervisor::runner_api::{platform_url, RunnerKey, RunnerTenant};
use cufflink_supervisor::{ChildSettings, EnvMode};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub tenant: RunnerTenant,
    pub platform_url: String,
    pub runner_key: RunnerKey,
    pub nats_url: String,
    pub nats_auth: cufflink_nats::NatsAuth,
    pub runner_port: u16,
    pub apps_dir: PathBuf,
    pub socket_dir: PathBuf,
    pub env_mode: EnvMode,
    pub children: ChildSettings,
    pub artifact_max_bytes: u64,
    pub reconcile_every: Duration,
    pub ready_timeout: Duration,
    pub health_interval: Duration,
    pub child_stop_timeout: Duration,
    pub max_concurrent_calls: usize,
    pub shutdown_grace: Duration,
    pub status_token: Option<String>,
}

const MIB: u64 = 1 << 20;

impl Config {
    pub fn load(vars: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let config = Self::from_vars(&Vars(vars))?;
        Ok(Self {
            apps_dir: std::path::absolute(&config.apps_dir)?,
            socket_dir: socket::fitting(&std::path::absolute(&config.socket_dir)?)?,
            ..config
        })
    }

    fn from_vars(vars: &Vars<impl Fn(&str) -> Option<String>>) -> eyre::Result<Self> {
        let secs = |name: &str, default: u64| vars.positive(name, default).map(Duration::from_secs);
        let dir = |name: &str, default: &str| {
            vars.get(name)
                .filter(|dir| !dir.trim().is_empty())
                .map_or_else(|| PathBuf::from(default), PathBuf::from)
        };
        Ok(Self {
            tenant: vars
                .required("EXT_TENANT")
                .and_then(|tenant| RunnerTenant::parse("EXT_TENANT", tenant))?,
            platform_url: vars.required("PLATFORM_URL").and_then(platform_url)?,
            runner_key: RunnerKey::from_vars(vars)?,
            nats_url: vars
                .get("NATS_URL")
                .unwrap_or_else(|| "nats://localhost:4222".into()),
            nats_auth: cufflink_nats::NatsAuth::from_vars(|name| vars.get(name))?,
            runner_port: vars.parsed("RUNNER_PORT", 3200)?,
            apps_dir: dir("APPS_DIR", "./apps"),
            socket_dir: dir("EXT_SOCKET_DIR", "/tmp/cufflink-ext"),
            env_mode: EnvMode::parse(EXT_ENV, vars.get(EXT_ENV.passthrough).as_deref())?,
            children: ChildSettings::from_vars(vars)?,
            artifact_max_bytes: vars.positive("EXT_ARTIFACT_MAX_BYTES", 128 * MIB)?,
            reconcile_every: secs("EXT_RECONCILE_SECS", 30)?,
            ready_timeout: secs("EXT_READY_TIMEOUT_SECS", 30)?,
            health_interval: secs("EXT_HEALTH_INTERVAL_SECS", 10)?,
            child_stop_timeout: vars
                .parsed("EXT_CHILD_STOP_TIMEOUT_SECS", 10)
                .map(Duration::from_secs)?,
            max_concurrent_calls: vars.positive("EXT_MAX_CONCURRENT_CALLS", 64)?,
            shutdown_grace: vars
                .parsed("SHUTDOWN_GRACE_SECS", 45)
                .map(Duration::from_secs)?,
            status_token: vars
                .get("EXT_STATUS_TOKEN")
                .filter(|token| !token.trim().is_empty()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(pairs: &[(&str, &str)]) -> eyre::Result<Config> {
        Config::from_vars(&Vars(cufflink_settings::fixed(pairs)))
    }

    const REQUIRED: [(&str, &str); 3] = [
        ("EXT_TENANT", "acme"),
        ("PLATFORM_URL", "http://platform:8080/"),
        ("CUFFLINK_RUNNER_KEY", " cfr_AAAAAAAAAAAA_s3cret "),
    ];

    fn without(name: &str) -> Vec<(&'static str, &'static str)> {
        REQUIRED
            .into_iter()
            .filter(|(set, _)| *set != name)
            .collect()
    }

    #[test]
    fn defaults_are_the_documented_ones() {
        let config = load(&REQUIRED).expect("defaults");
        assert_eq!(config.tenant.slug, "acme");
        assert_eq!(config.platform_url, "http://platform:8080");
        assert_eq!(
            config.runner_key,
            RunnerKey::Value("cfr_AAAAAAAAAAAA_s3cret".into())
        );
        assert_eq!(config.nats_url, "nats://localhost:4222");
        assert_eq!(config.nats_auth, cufflink_nats::NatsAuth::Anonymous);
        assert_eq!(config.runner_port, 3200);
        assert_eq!(config.apps_dir, PathBuf::from("./apps"));
        assert_eq!(config.socket_dir, PathBuf::from("/tmp/cufflink-ext"));
        assert_eq!(config.artifact_max_bytes, 128 * MIB);
        assert_eq!(config.reconcile_every, Duration::from_secs(30));
        assert_eq!(config.ready_timeout, Duration::from_secs(30));
        assert_eq!(config.health_interval, Duration::from_secs(10));
        assert_eq!(config.child_stop_timeout, Duration::from_secs(10));
        assert_eq!(config.max_concurrent_calls, 64);
        assert_eq!(config.shutdown_grace, Duration::from_secs(45));
        assert_eq!(config.status_token, None);
        assert_eq!(
            config.env_mode,
            EnvMode::parse(EXT_ENV, None).expect("the allowlist")
        );
        assert!(
            !format!("{config:?}").contains("s3cret"),
            "the key is never printed"
        );
    }

    #[test]
    fn every_setting_is_read() {
        let config = load(&[
            ("EXT_TENANT", "globex"),
            ("PLATFORM_URL", "https://platform.internal"),
            ("CUFFLINK_RUNNER_KEY_FILE", "/run/secrets/runner-key"),
            ("NATS_URL", "nats://a:4222,nats://b:4222"),
            ("NATS_USER", "runner"),
            ("NATS_PASSWORD", "pw"),
            ("RUNNER_PORT", "4200"),
            ("APPS_DIR", "/var/lib/ext"),
            ("EXT_SOCKET_DIR", "/run/ext"),
            ("EXT_ENV_PASSTHROUGH", "RUST_LOG,SENTRY_*"),
            ("EXT_ARTIFACT_MAX_BYTES", "1024"),
            ("EXT_RECONCILE_SECS", "5"),
            ("EXT_READY_TIMEOUT_SECS", "6"),
            ("EXT_HEALTH_INTERVAL_SECS", "7"),
            ("EXT_CHILD_STOP_TIMEOUT_SECS", "0"),
            ("EXT_MAX_CONCURRENT_CALLS", "8"),
            ("SHUTDOWN_GRACE_SECS", "9"),
            ("EXT_STATUS_TOKEN", "ops"),
        ])
        .expect("settings");
        assert_eq!(config.tenant.slug, "globex");
        assert_eq!(config.platform_url, "https://platform.internal");
        assert_eq!(
            config.runner_key,
            RunnerKey::File("/run/secrets/runner-key".into())
        );
        assert_eq!(config.nats_url, "nats://a:4222,nats://b:4222");
        assert_eq!(
            config.nats_auth,
            cufflink_nats::NatsAuth::UserPassword {
                user: "runner".into(),
                password: "pw".into()
            }
        );
        assert_eq!(config.runner_port, 4200);
        assert_eq!(config.apps_dir, PathBuf::from("/var/lib/ext"));
        assert_eq!(config.socket_dir, PathBuf::from("/run/ext"));
        assert_eq!(
            config.env_mode,
            EnvMode::parse(EXT_ENV, Some("RUST_LOG,SENTRY_*")).expect("patterns")
        );
        assert_eq!(config.artifact_max_bytes, 1024);
        assert_eq!(config.reconcile_every, Duration::from_secs(5));
        assert_eq!(config.ready_timeout, Duration::from_secs(6));
        assert_eq!(config.health_interval, Duration::from_secs(7));
        assert_eq!(config.child_stop_timeout, Duration::ZERO);
        assert_eq!(config.max_concurrent_calls, 8);
        assert_eq!(config.shutdown_grace, Duration::from_secs(9));
        assert_eq!(config.status_token.as_deref(), Some("ops"));
        assert!(
            load(&[&REQUIRED[..], &[("EXT_STATUS_TOKEN", " ")]].concat())
                .expect("blank")
                .status_token
                .is_none()
        );
    }

    #[test]
    fn secrets_given_as_files_are_read() {
        let pairs = [
            &REQUIRED[..],
            &[
                ("NATS_USER", "runner"),
                ("NATS_PASSWORD_FILE", "/run/secrets/nats-password"),
                ("EXT_STATUS_TOKEN_FILE", "/run/secrets/status-token"),
            ],
        ]
        .concat();
        let config =
            cufflink_settings::with_secret_files(cufflink_settings::fixed(&pairs), |path| {
                Ok(format!("in {}\n", path.display()).into_bytes())
            })
            .and_then(|vars| Config::from_vars(&Vars(vars)))
            .expect("settings");
        assert_eq!(
            config.nats_auth,
            cufflink_nats::NatsAuth::UserPassword {
                user: "runner".into(),
                password: "in /run/secrets/nats-password".into()
            }
        );
        assert_eq!(
            config.status_token.as_deref(),
            Some("in /run/secrets/status-token")
        );
    }

    #[test]
    fn invalid_settings_are_refused_by_name() {
        let refused = |pairs: &[(&str, &str)]| {
            load(&[&REQUIRED[..], pairs].concat())
                .expect_err("refused")
                .to_string()
        };
        for name in ["EXT_TENANT", "PLATFORM_URL"] {
            assert_eq!(
                load(&without(name)).expect_err(name).to_string(),
                format!("Missing required environment variable: {name}")
            );
        }
        assert!(load(&without("CUFFLINK_RUNNER_KEY"))
            .expect_err("no key")
            .to_string()
            .starts_with(
                "Missing required environment variable: CUFFLINK_RUNNER_KEY (or \
                 CUFFLINK_RUNNER_KEY_FILE)"
            ));
        assert_eq!(
            refused(&[("CUFFLINK_RUNNER_KEY_FILE", "/run/key")]),
            "CUFFLINK_RUNNER_KEY and CUFFLINK_RUNNER_KEY_FILE are both set; set one"
        );
        for tenant in ["Acme", "acme,globex", "*"] {
            assert!(
                refused(&[("EXT_TENANT", tenant)])
                    .starts_with(&format!("EXT_TENANT: '{tenant}' is not a tenant slug")),
                "{tenant}"
            );
        }
        for url in ["platform:8080", "ftp://platform", "http://platform/?x=1"] {
            assert!(
                refused(&[("PLATFORM_URL", url)]).starts_with(&format!("PLATFORM_URL: '{url}'")),
                "{url}"
            );
        }
        for zero in [
            "EXT_ARTIFACT_MAX_BYTES",
            "EXT_RECONCILE_SECS",
            "EXT_READY_TIMEOUT_SECS",
            "EXT_HEALTH_INTERVAL_SECS",
            "EXT_MAX_CONCURRENT_CALLS",
        ] {
            assert_eq!(
                refused(&[(zero, "0")]),
                format!("{zero} must be greater than 0")
            );
        }
        for (name, value) in [
            ("RUNNER_PORT", "70000"),
            ("EXT_RECONCILE_SECS", "soon"),
            ("EXT_CHILD_STOP_TIMEOUT_SECS", "-1"),
            ("SHUTDOWN_GRACE_SECS", "1.5"),
        ] {
            assert!(
                refused(&[(name, value)]).starts_with(&format!("{name} has an invalid value")),
                "{name}"
            );
        }
        assert!(refused(&[("EXT_ENV_PASSTHROUGH", "*")]).starts_with("EXT_ENV_PASSTHROUGH: '"));
        assert_eq!(
            refused(&[("NATS_USER", "runner")]),
            "NATS_USER and NATS_PASSWORD must be set together"
        );
    }
}
