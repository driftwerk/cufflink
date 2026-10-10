use crate::artifact::ArchiveCaps;
use crate::child_env::{env_mode, WEB_ENV};
use crate::host::HostnameFormat;
use cufflink_settings::Vars;
use cufflink_supervisor::runner_api::{
    platform_url, RunnerKey, RunnerTenant, KEY_FILE_VAR, KEY_VAR,
};
use cufflink_supervisor::{ChildSettings, EnvMode};
use std::ops::RangeInclusive;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    Shared {
        database_url: String,
    },
    TenantScoped {
        tenant: RunnerTenant,
        platform_url: String,
        runner_key: RunnerKey,
        poll_every: Duration,
    },
}

impl Scope {
    pub fn tenant(&self) -> Option<&RunnerTenant> {
        match self {
            Self::Shared { .. } => None,
            Self::TenantScoped { tenant, .. } => Some(tenant),
        }
    }

    fn from_vars(vars: &Vars<impl Fn(&str) -> Option<String>>) -> eyre::Result<Self> {
        match (
            vars.required("WEB_TENANT").ok(),
            RunnerKey::configured(vars)?,
        ) {
            (Some(tenant), key) => Ok(Self::TenantScoped {
                tenant: RunnerTenant::parse("WEB_TENANT", tenant)?,
                platform_url: vars.required("PLATFORM_URL").and_then(platform_url)?,
                runner_key: key.map_or_else(|| RunnerKey::from_vars(vars), Ok)?,
                poll_every: vars
                    .positive("WEB_POLL_SECS", 30)
                    .map(Duration::from_secs)?,
            }),
            (None, Some(_)) => Err(eyre::eyre!(
                "{KEY_VAR} (or {KEY_FILE_VAR}) is set without WEB_TENANT: set WEB_TENANT to serve \
                 that tenant alone from the runner API, or unset the key"
            )),
            (None, None) => vars
                .required("DATABASE_URL")
                .map(|database_url| Self::Shared { database_url }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebRunnerConfig {
    pub scope: Scope,
    pub nats_url: String,
    pub nats_auth: cufflink_nats::NatsAuth,
    pub runner_port: u16,
    pub apps_dir: String,
    pub base_domain: String,
    pub public_scheme: String,
    pub hostname_format: HostnameFormat,
    pub shutdown_grace: Duration,
    pub child_stop_timeout: Duration,
    pub env_mode: EnvMode,
    pub children: ChildSettings,
    pub artifact_caps: ArchiveCaps,
    pub port_range: RangeInclusive<u16>,
    pub status_token: Option<String>,
}

impl WebRunnerConfig {
    pub fn load(vars: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let vars = Vars(vars);
        let or = |name: &str, default: &str| vars.get(name).unwrap_or_else(|| default.to_string());
        let secs = |name: &str, default: u64| vars.parsed(name, default).map(Duration::from_secs);
        let runner_port: u16 = or("RUNNER_PORT", "3100").parse()?;
        let port_range = app_ports(vars.get("WEB_PORT_RANGE").as_deref(), runner_port)?;
        Ok(Self {
            scope: Scope::from_vars(&vars)?,
            nats_url: or("NATS_URL", "nats://localhost:4222"),
            nats_auth: cufflink_nats::NatsAuth::from_vars(|name| vars.get(name))?,
            runner_port,
            apps_dir: or("APPS_DIR", "./apps"),
            base_domain: vars.get("CUFFLINK_BASE_DOMAIN").ok_or_else(|| {
                eyre::eyre!("Missing required environment variable: CUFFLINK_BASE_DOMAIN")
            })?,
            public_scheme: or("CUFFLINK_PUBLIC_SCHEME", "https"),
            hostname_format: or("CUFFLINK_HOSTNAME_FORMAT", "flat").parse()?,
            shutdown_grace: secs("SHUTDOWN_GRACE_SECS", 45)?,
            child_stop_timeout: secs("WEB_CHILD_STOP_TIMEOUT_SECS", 10)?,
            env_mode: env_mode(vars.get(WEB_ENV.passthrough).as_deref())?,
            children: ChildSettings::from_vars(&vars)?,
            artifact_caps: ArchiveCaps {
                max_entries: vars.parsed("WEB_ARTIFACT_MAX_ENTRIES", 200_000)?,
                max_bytes: vars.parsed("WEB_ARTIFACT_MAX_BYTES", 2 << 30)?,
            },
            port_range,
            status_token: vars
                .get("WEB_STATUS_TOKEN")
                .filter(|token| !token.trim().is_empty()),
        })
    }
}

fn app_ports(raw: Option<&str>, runner_port: u16) -> eyre::Result<RangeInclusive<u16>> {
    let range = parse_port_range(raw)?;
    if range.contains(&runner_port) {
        return Err(eyre::eyre!(
            "WEB_PORT_RANGE {}-{} contains RUNNER_PORT {runner_port}; move one of them",
            range.start(),
            range.end()
        ));
    }
    Ok(range)
}

fn parse_port_range(raw: Option<&str>) -> eyre::Result<RangeInclusive<u16>> {
    let Some(raw) = raw else {
        return Ok(3200..=3999);
    };
    let invalid = || eyre::eyre!("WEB_PORT_RANGE must be first-last (e.g. 3200-3999), not '{raw}'");
    let (first, last) = raw.trim().split_once('-').ok_or_else(invalid)?;
    let first: u16 = first.trim().parse().map_err(|_| invalid())?;
    let last: u16 = last.trim().parse().map_err(|_| invalid())?;
    (first > 0 && first <= last)
        .then_some(first..=last)
        .ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(pairs: &[(&str, &str)]) -> eyre::Result<Scope> {
        Scope::from_vars(&Vars(cufflink_settings::fixed(pairs)))
    }

    #[test]
    fn the_settings_default_as_documented() {
        let config = WebRunnerConfig::load(cufflink_settings::fixed(&[
            ("DATABASE_URL", "postgres://platform"),
            ("CUFFLINK_BASE_DOMAIN", "example.com"),
        ]))
        .expect("defaults");
        assert_eq!(config.nats_url, "nats://localhost:4222");
        assert_eq!(config.nats_auth, cufflink_nats::NatsAuth::Anonymous);
        assert_eq!(config.runner_port, 3100);
        assert_eq!(config.apps_dir, "./apps");
        assert_eq!(config.base_domain, "example.com");
        assert_eq!(config.public_scheme, "https");
        assert_eq!(config.hostname_format, HostnameFormat::Flat);
        assert_eq!(config.shutdown_grace, Duration::from_secs(45));
        assert_eq!(config.child_stop_timeout, Duration::from_secs(10));
        assert_eq!(config.env_mode, env_mode(None).expect("the allowlist"));
        assert_eq!(config.artifact_caps.max_entries, 200_000);
        assert_eq!(config.artifact_caps.max_bytes, 2 << 30);
        assert_eq!(config.port_range, 3200..=3999);
        assert_eq!(config.status_token, None);
        assert_eq!(
            WebRunnerConfig::load(cufflink_settings::fixed(&[(
                "DATABASE_URL",
                "postgres://platform"
            )]))
            .expect_err("no domain")
            .to_string(),
            "Missing required environment variable: CUFFLINK_BASE_DOMAIN"
        );
        for (name, value) in [
            ("RUNNER_PORT", "70000"),
            ("SHUTDOWN_GRACE_SECS", "soon"),
            ("WEB_ARTIFACT_MAX_BYTES", "-1"),
        ] {
            assert!(
                WebRunnerConfig::load(cufflink_settings::fixed(&[
                    ("DATABASE_URL", "postgres://platform"),
                    ("CUFFLINK_BASE_DOMAIN", "example.com"),
                    (name, value),
                ]))
                .is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn secrets_given_as_files_are_read() {
        let config = cufflink_settings::with_secret_files(
            cufflink_settings::fixed(&[
                ("CUFFLINK_BASE_DOMAIN", "example.com"),
                ("DATABASE_URL_FILE", "/run/secrets/database-url"),
                ("NATS_NKEY_SEED_FILE", "/run/secrets/nats-seed"),
                ("WEB_STATUS_TOKEN", " "),
                ("WEB_STATUS_TOKEN_FILE", "/run/secrets/status-token"),
            ]),
            |path| Ok(format!("in {}\n", path.display()).into_bytes()),
        )
        .and_then(WebRunnerConfig::load)
        .expect("settings");
        assert_eq!(
            config.scope,
            Scope::Shared {
                database_url: "in /run/secrets/database-url".into()
            }
        );
        assert_eq!(
            config.nats_auth,
            cufflink_nats::NatsAuth::NKeySeed("in /run/secrets/nats-seed".into())
        );
        assert_eq!(
            config.status_token.as_deref(),
            Some("in /run/secrets/status-token")
        );
    }

    #[test]
    fn a_runner_serves_every_tenant_from_the_database_unless_it_names_one() {
        assert_eq!(
            scope(&[
                ("DATABASE_URL", "postgres://platform"),
                ("PLATFORM_URL", "http://platform:8080"),
            ])
            .expect("shared"),
            Scope::Shared {
                database_url: "postgres://platform".into()
            },
            "PLATFORM_URL alone is a value apps are passed"
        );
        assert_eq!(
            scope(&[]).expect_err("no database").to_string(),
            "Missing required environment variable: DATABASE_URL"
        );
        assert_eq!(
            scope(&[
                ("WEB_TENANT", "acme"),
                ("PLATFORM_URL", "http://platform:8080/"),
                ("CUFFLINK_RUNNER_KEY_FILE", "/run/secrets/runner-key"),
                ("WEB_POLL_SECS", "5"),
            ])
            .expect("tenant-scoped"),
            Scope::TenantScoped {
                tenant: RunnerTenant::parse("WEB_TENANT", "acme".into()).expect("a slug"),
                platform_url: "http://platform:8080".into(),
                runner_key: RunnerKey::File("/run/secrets/runner-key".into()),
                poll_every: Duration::from_secs(5),
            },
            "and needs no database"
        );
        assert!(matches!(
            scope(&[
                ("WEB_TENANT", "acme"),
                ("PLATFORM_URL", "http://platform:8080"),
                ("CUFFLINK_RUNNER_KEY", "cfr_AAAAAAAAAAAA_s3cret"),
            ])
            .expect("tenant-scoped"),
            Scope::TenantScoped { poll_every, .. } if poll_every == Duration::from_secs(30)
        ));
    }

    #[test]
    fn only_a_tenant_scoped_runner_names_a_tenant() {
        let tenant = |pairs: &[(&str, &str)]| {
            scope(pairs)
                .expect("a scope")
                .tenant()
                .map(|tenant| tenant.slug.clone())
        };
        assert_eq!(tenant(&[("DATABASE_URL", "postgres://platform")]), None);
        assert_eq!(
            tenant(&[
                ("WEB_TENANT", "acme"),
                ("PLATFORM_URL", "http://platform:8080"),
                ("CUFFLINK_RUNNER_KEY", "cfr_AAAAAAAAAAAA_s3cret"),
            ])
            .as_deref(),
            Some("acme")
        );
    }

    #[test]
    fn a_partial_tenant_scope_is_refused_by_name() {
        let refused = |pairs: &[(&str, &str)]| scope(pairs).expect_err("refused").to_string();
        assert_eq!(
            refused(&[("WEB_TENANT", "acme"), ("CUFFLINK_RUNNER_KEY", "k")]),
            "Missing required environment variable: PLATFORM_URL"
        );
        assert!(refused(&[
            ("WEB_TENANT", "acme"),
            ("PLATFORM_URL", "http://platform:8080")
        ])
        .starts_with("Missing required environment variable: CUFFLINK_RUNNER_KEY"));
        assert!(refused(&[
            ("DATABASE_URL", "postgres://platform"),
            ("CUFFLINK_RUNNER_KEY", "k")
        ])
        .starts_with(
            "CUFFLINK_RUNNER_KEY (or CUFFLINK_RUNNER_KEY_FILE) is set without WEB_TENANT"
        ));
        assert!(refused(&[
            ("WEB_TENANT", "Acme,Globex"),
            ("PLATFORM_URL", "http://platform:8080"),
            ("CUFFLINK_RUNNER_KEY", "k"),
        ])
        .starts_with("WEB_TENANT: 'Acme,Globex' is not a tenant slug"));
        assert!(refused(&[
            ("WEB_TENANT", "acme"),
            ("PLATFORM_URL", "platform:8080"),
            ("CUFFLINK_RUNNER_KEY", "k"),
        ])
        .starts_with("PLATFORM_URL: 'platform:8080'"));
        assert_eq!(
            refused(&[
                ("WEB_TENANT", "acme"),
                ("PLATFORM_URL", "http://platform:8080"),
                ("CUFFLINK_RUNNER_KEY", "k"),
                ("WEB_POLL_SECS", "0"),
            ]),
            "WEB_POLL_SECS must be greater than 0"
        );
    }

    #[test]
    fn the_runner_port_stays_out_of_the_app_ports() {
        assert_eq!(app_ports(None, 3100).expect("default"), 3200..=3999);
        let err = app_ports(None, 3500).expect_err("overlap").to_string();
        assert!(err.contains("contains RUNNER_PORT 3500"), "{err}");
        assert!(app_ports(Some("4000-4999"), 3500).is_ok());
    }

    #[test]
    fn port_ranges_parse_or_fail_by_name() {
        assert_eq!(parse_port_range(None).expect("default"), 3200..=3999);
        assert_eq!(
            parse_port_range(Some(" 4000 - 4001 ")).expect("set"),
            4000..=4001
        );
        for invalid in ["4000", "4001-4000", "0-10", "a-b", "1-70000"] {
            let err = parse_port_range(Some(invalid))
                .expect_err(invalid)
                .to_string();
            assert!(err.contains("WEB_PORT_RANGE"), "{err}");
        }
    }
}
