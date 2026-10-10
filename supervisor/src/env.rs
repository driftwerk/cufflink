use cufflink_settings::{secret_file_var, Vars, SECRET_VARS};
use cufflink_telemetry::{Component, Scope, COMPONENT};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

const SERVICE_NAME_VAR: &str = "OTEL_SERVICE_NAME";
const RESOURCE_ATTRIBUTES_VAR: &str = "OTEL_RESOURCE_ATTRIBUTES";

pub(crate) const CORE_ALLOWLIST: &[&str] = &[
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "TZ",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "CUFFLINK_INTERNAL_URL",
    "PLATFORM_URL",
    "OTEL_SERVICE_NAME",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_RESOURCE_ATTRIBUTES",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvPolicy {
    pub passthrough: &'static str,
    pub allow: &'static [&'static str],
    pub credentials: &'static [&'static str],
}

impl EnvPolicy {
    pub fn allowlist(&self) -> impl Iterator<Item = &'static str> {
        CORE_ALLOWLIST.iter().chain(self.allow).copied()
    }

    fn is_credential(&self, name: &str) -> bool {
        const NAMES: &[&str] = &[
            "GOOGLE_APPLICATION_CREDENTIALS",
            "CUFFLINK_SECRETS_KEY",
            "OTEL_EXPORTER_OTLP_HEADERS",
        ];
        const PREFIXES: &[&str] = &["DATABASE_", "NATS_", "AWS_", "S3_", "GCS_", "STORAGE_"];
        NAMES.contains(&name)
            || SECRET_VARS
                .iter()
                .any(|secret| name == *secret || name == secret_file_var(secret))
            || self.credentials.contains(&name)
            || PREFIXES.iter().any(|prefix| name.starts_with(prefix))
            || (name.starts_with("CUFFLINK_")
                && (name.ends_with("_KEY") || name.ends_with("_KEYS")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EnvPattern {
    Name(String),
    Prefix(String),
}

impl EnvPattern {
    fn parse(raw: &str, setting: &str) -> eyre::Result<Self> {
        let valid =
            |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
        match raw.strip_suffix('*') {
            Some(prefix) if valid(prefix) => Ok(Self::Prefix(prefix.to_string())),
            None if valid(raw) => Ok(Self::Name(raw.to_string())),
            _ => Err(eyre::eyre!(
                "{setting}: '{raw}' is not a variable name or PREFIX_*"
            )),
        }
    }

    fn matches(&self, name: &str) -> bool {
        match self {
            Self::Name(exact) => name == exact,
            Self::Prefix(prefix) => name.starts_with(prefix.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvMode {
    policy: EnvPolicy,
    extra: Vec<EnvPattern>,
}

impl EnvMode {
    pub fn parse(policy: EnvPolicy, raw: Option<&str>) -> eyre::Result<Self> {
        raw.unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| EnvPattern::parse(entry, policy.passthrough))
            .collect::<eyre::Result<Vec<_>>>()
            .map(|extra| Self { policy, extra })
    }

    fn passes(&self, name: &str) -> bool {
        !self.policy.is_credential(name)
            && (self.policy.allowlist().any(|allowed| allowed == name)
                || self.extra.iter().any(|p| p.matches(name)))
    }
}

pub fn runner_env() -> impl Iterator<Item = (String, String)> {
    std::env::vars_os()
        .filter_map(|(name, value)| name.into_string().ok().zip(value.into_string().ok()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChildTelemetry {
    #[default]
    Legacy,
    Scoped,
}

impl FromStr for ChildTelemetry {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "legacy" => Ok(Self::Legacy),
            "scoped" => Ok(Self::Scoped),
            other => Err(format!("'{other}' is not one of legacy, scoped")),
        }
    }
}

impl ChildTelemetry {
    pub fn naming<'a>(self, scope: &'a Scope, runner: Component) -> Option<Child<'a>> {
        (self == Self::Scoped).then_some(Child { scope, runner })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputLevel {
    #[default]
    Debug,
    Info,
}

impl FromStr for OutputLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            other => Err(format!("'{other}' is not one of debug, info")),
        }
    }
}

#[macro_export]
macro_rules! output_event {
    ($level:expr, $($event:tt)*) => {
        match $level {
            $crate::OutputLevel::Debug => $crate::__tracing::debug!($($event)*),
            $crate::OutputLevel::Info => $crate::__tracing::info!($($event)*),
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChildSettings {
    pub telemetry: ChildTelemetry,
    pub output_level: OutputLevel,
}

impl ChildSettings {
    pub fn from_vars(vars: &Vars<impl Fn(&str) -> Option<String>>) -> eyre::Result<Self> {
        Ok(Self {
            telemetry: vars.parsed("CUFFLINK_CHILD_TELEMETRY", ChildTelemetry::default())?,
            output_level: vars.parsed("CUFFLINK_CHILD_OUTPUT_LEVEL", OutputLevel::default())?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Child<'a> {
    pub scope: &'a Scope,
    pub runner: Component,
}

fn merged_attributes<'a>(lists: impl IntoIterator<Item = &'a str>) -> String {
    lists
        .into_iter()
        .flat_map(|list| list.split(','))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .fold(Vec::<(Option<&str>, &str)>::new(), |mut entries, entry| {
            let key = attribute_key(entry);
            match entries
                .iter_mut()
                .find(|(known, _)| key.is_some() && *known == key)
            {
                Some(known) => known.1 = entry,
                None => entries.push((key, entry)),
            }
            entries
        })
        .into_iter()
        .map(|(_, entry)| entry)
        .collect::<Vec<_>>()
        .join(",")
}

fn attribute_key(entry: &str) -> Option<&str> {
    entry
        .split_once('=')
        .map(|(key, _)| key.trim())
        .filter(|key| !key.is_empty())
}

fn attribute<'a>(list: &'a str, name: &str) -> Option<&'a str> {
    list.split(',')
        .filter_map(|entry| entry.trim().split_once('='))
        .find(|(key, _)| key.trim() == name)
        .map(|(_, value)| value.trim())
}

fn is_malformed(list: &str) -> bool {
    list.split(',')
        .map(str::trim)
        .any(|entry| !entry.is_empty() && attribute_key(entry).is_none())
}

fn child_telemetry(
    child: Child<'_>,
    runner_attributes: Option<&str>,
    service_configs: &HashMap<String, String>,
) -> [(String, String); 2] {
    let Child { scope, runner } = child;
    let configured = service_configs
        .get(RESOURCE_ATTRIBUTES_VAR)
        .map(String::as_str);
    let scoped = [
        Some(format!("service.namespace={}", scope.tenant)),
        scope
            .version_label()
            .map(|version| format!("service.version={version}")),
        Some(format!("{COMPONENT}={}", runner.as_str())),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",");
    let service_name = service_configs
        .get(SERVICE_NAME_VAR)
        .map(String::as_str)
        .or_else(|| attribute(configured?, "service.name"))
        .or(scope.service.as_deref())
        .unwrap_or_default();
    [
        (SERVICE_NAME_VAR.to_string(), service_name.to_string()),
        (
            RESOURCE_ATTRIBUTES_VAR.to_string(),
            merged_attributes(
                [runner_attributes, Some(scoped.as_str()), configured]
                    .into_iter()
                    .flatten(),
            ),
        ),
    ]
}

pub fn child_env(
    runner_env: impl IntoIterator<Item = (String, String)>,
    mode: &EnvMode,
    service_configs: &HashMap<String, String>,
    scoped: Option<Child<'_>>,
    identity: impl IntoIterator<Item = (String, String)>,
) -> BTreeMap<String, String> {
    let passed: BTreeMap<String, String> = runner_env
        .into_iter()
        .filter(|(name, _)| mode.passes(name))
        .collect();
    let telemetry = scoped.map(|child| {
        let runner_attributes = passed.get(RESOURCE_ATTRIBUTES_VAR).map(String::as_str);
        if [runner_attributes, service_configs.get(RESOURCE_ATTRIBUTES_VAR).map(String::as_str)]
            .into_iter()
            .flatten()
            .any(is_malformed)
        {
            tracing::warn!(
                tenant = %child.scope.tenant,
                service = child.scope.service.as_deref(),
                "{RESOURCE_ATTRIBUTES_VAR} holds an entry that is not key=value; the child is given it as it is"
            );
        }
        child_telemetry(child, runner_attributes, service_configs)
    });
    passed
        .into_iter()
        .chain(
            service_configs
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        )
        .chain(telemetry.into_iter().flatten())
        .chain(identity)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: EnvPolicy = EnvPolicy {
        passthrough: "TEST_ENV_PASSTHROUGH",
        allow: &["TEST_RUNTIME"],
        credentials: &["TEST_STATUS_TOKEN"],
    };

    fn identity() -> [(String, String); 2] {
        [
            ("CUFFLINK_TENANT".into(), "preview-pr-7".into()),
            ("CUFFLINK_SERVICE".into(), "portal".into()),
        ]
    }

    fn runner_env() -> Vec<(String, String)> {
        [
            ("PATH", "/usr/bin"),
            ("HOME", "/home/runner"),
            ("TEST_RUNTIME", "1"),
            ("NODE_OPTIONS", "--max-old-space-size=512"),
            ("CUFFLINK_INTERNAL_URL", "http://cufflink-platform:8080"),
            ("DATABASE_URL", "postgres://cufflink:secret@db/cufflink"),
            ("DATABASE_POOL_SIZE", "4"),
            ("NATS_PASSWORD", "nats-secret"),
            ("REDIS_URL", "redis://redis"),
            ("AWS_SECRET_ACCESS_KEY", "aws-secret"),
            ("S3_BUCKET", "platform"),
            ("GCS_BUCKET", "platform"),
            ("STORAGE_BACKEND", "s3"),
            ("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/gcp.json"),
            ("CUFFLINK_SECRETS_KEY", "k"),
            ("CUFFLINK_SECRETS_KEK", "k"),
            ("CUFFLINK_SECRETS_KEK_ID", "2026-09"),
            ("CUFFLINK_SECRETS_KEK_PREVIOUS", "2026-03:k"),
            ("CUFFLINK_CACHE_SIGNING_KEY", "k"),
            ("CUFFLINK_SERVICE_IDENTITY_KEYS", "k"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer x"),
            ("OTEL_SERVICE_NAME", "web"),
            ("SENTRY_DSN", "https://sentry"),
            ("SENTRY_ENV", "staging"),
            ("TEST_STATUS_TOKEN", "ops-secret"),
            ("PORT", "3100"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn names(env: &BTreeMap<String, String>) -> Vec<&str> {
        env.keys().map(String::as_str).collect()
    }

    #[test]
    fn the_allowlist_is_the_core_and_the_runners_own() {
        let env = child_env(
            runner_env(),
            &EnvMode::parse(POLICY, None).expect("default"),
            &HashMap::new(),
            None,
            identity(),
        );
        assert_eq!(
            names(&env),
            [
                "CUFFLINK_INTERNAL_URL",
                "CUFFLINK_SERVICE",
                "CUFFLINK_TENANT",
                "HOME",
                "OTEL_SERVICE_NAME",
                "PATH",
                "TEST_RUNTIME",
            ]
        );
        assert_eq!(
            POLICY.allowlist().collect::<Vec<_>>(),
            [CORE_ALLOWLIST, &["TEST_RUNTIME"]].concat()
        );
    }

    #[test]
    fn extra_patterns_widen_the_allowlist_but_never_to_credentials() {
        let mode = EnvMode::parse(
            POLICY,
            Some(" SENTRY_* , DATABASE_URL,NATS_*,CUFFLINK_CACHE_SIGNING_KEY,CUFFLINK_SECRETS_*,TEST_*"),
        )
        .expect("patterns");
        let env = child_env(runner_env(), &mode, &HashMap::new(), None, identity());
        assert!(env.contains_key("SENTRY_DSN") && env.contains_key("SENTRY_ENV"));
        for credential in [
            "DATABASE_URL",
            "NATS_PASSWORD",
            "CUFFLINK_CACHE_SIGNING_KEY",
            "CUFFLINK_SECRETS_KEK",
            "CUFFLINK_SECRETS_KEK_PREVIOUS",
            "TEST_STATUS_TOKEN",
        ] {
            assert!(!env.contains_key(credential), "{credential}");
        }
    }

    #[test]
    fn every_secret_and_its_file_form_is_denied_under_any_passthrough() {
        let secrets: Vec<String> = SECRET_VARS
            .iter()
            .flat_map(|secret| [secret.to_string(), secret_file_var(secret)])
            .collect();
        let runner: Vec<(String, String)> = secrets
            .iter()
            .map(String::as_str)
            .chain(["SSL_CERT_FILE"])
            .map(|name| (name.to_string(), "value".to_string()))
            .collect();
        let exact = secrets.join(",");
        for passthrough in [
            exact.as_str(),
            "DATABASE_*,TENANT_*,REDIS_*,NATS_*,S3_*,CUFFLINK_*,WEB_*,EXT_*,TEST_*",
        ] {
            let mode = EnvMode::parse(POLICY, Some(passthrough)).expect("patterns");
            let env = child_env(runner.clone(), &mode, &HashMap::new(), None, []);
            assert_eq!(names(&env), ["SSL_CERT_FILE"], "{passthrough}");
        }
    }

    #[test]
    fn a_runners_credentials_are_its_own() {
        let other = EnvPolicy {
            credentials: &[],
            ..POLICY
        };
        let mode = EnvMode::parse(other, Some("TEST_STATUS_TOKEN")).expect("pattern");
        let env = child_env(runner_env(), &mode, &HashMap::new(), None, identity());
        assert!(env.contains_key("TEST_STATUS_TOKEN"));
    }

    #[test]
    fn service_configs_pass_and_the_identity_wins() {
        let configs = HashMap::from([
            ("NEXTAUTH_SECRET".to_string(), "app-secret".to_string()),
            ("DATABASE_URL".to_string(), "postgres://app-own".to_string()),
            ("CUFFLINK_TENANT".to_string(), "stale".to_string()),
            ("PATH".to_string(), "/app/bin".to_string()),
        ]);
        let env = child_env(
            runner_env(),
            &EnvMode::parse(POLICY, None).expect("default"),
            &configs,
            None,
            identity(),
        );
        assert_eq!(env["NEXTAUTH_SECRET"], "app-secret");
        assert_eq!(
            env["DATABASE_URL"], "postgres://app-own",
            "the app's own config"
        );
        assert_eq!(env["PATH"], "/app/bin", "configs override the runner");
        assert_eq!(env["CUFFLINK_TENANT"], "preview-pr-7");
    }

    fn portal() -> Scope {
        Scope::service("preview-pr-7", "portal", 12)
    }

    fn named(runner_attributes: Option<&str>, configs: &[(&str, &str)]) -> [(String, String); 2] {
        child_telemetry(
            Child {
                scope: &portal(),
                runner: Component::WebRunner,
            },
            runner_attributes,
            &configs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        )
    }

    fn pair(name: &str, attributes: &str) -> [(String, String); 2] {
        [
            (SERVICE_NAME_VAR.to_string(), name.to_string()),
            (RESOURCE_ATTRIBUTES_VAR.to_string(), attributes.to_string()),
        ]
    }

    #[test]
    fn a_scoped_child_is_named_as_its_service() {
        const SCOPED: &str =
            "service.namespace=preview-pr-7,service.version=v12,cufflink.component=web-runner";
        assert_eq!(named(None, &[]), pair("portal", SCOPED));
        assert_eq!(
            named(
                Some("deployment.environment.name=dev, service.namespace=runners ,service.version=r1"),
                &[]
            ),
            pair(
                "portal",
                "deployment.environment.name=dev,service.namespace=preview-pr-7,service.version=v12,cufflink.component=web-runner"
            ),
            "what the runner says of itself never wins over the child's"
        );
        let unversioned = child_telemetry(
            Child {
                scope: &Scope::service("acme", "notes", None),
                runner: Component::ExtRunner,
            },
            None,
            &HashMap::new(),
        );
        assert_eq!(
            unversioned,
            pair(
                "notes",
                "service.namespace=acme,cufflink.component=ext-runner"
            )
        );
    }

    #[test]
    fn the_services_own_config_wins_key_by_key() {
        assert_eq!(
            named(
                Some("deployment.environment.name=dev"),
                &[
                    (SERVICE_NAME_VAR, "portal-web"),
                    (RESOURCE_ATTRIBUTES_VAR, "service.version=2026.10,team=growth"),
                ]
            ),
            pair(
                "portal-web",
                "deployment.environment.name=dev,service.namespace=preview-pr-7,service.version=2026.10,cufflink.component=web-runner,team=growth"
            )
        );
        assert_eq!(
            named(
                None,
                &[(RESOURCE_ATTRIBUTES_VAR, "service.name=portal-next")]
            )[0],
            (SERVICE_NAME_VAR.to_string(), "portal-next".to_string()),
            "a name the config gives as an attribute is not overridden by the variable"
        );
    }

    #[test]
    fn an_entry_that_is_not_key_value_is_kept_as_it_is() {
        assert!(is_malformed("a=1,oops"));
        assert!(is_malformed("=1"));
        assert!(!is_malformed("a=1, b=2,,"));
        assert!(!is_malformed(""));
        assert_eq!(
            named(Some("oops,env=dev"), &[(RESOURCE_ATTRIBUTES_VAR, "also bad,env=prod")])[1].1,
            "oops,env=prod,service.namespace=preview-pr-7,service.version=v12,cufflink.component=web-runner,also bad"
        );
    }

    #[test]
    fn legacy_leaves_the_environment_as_it_was_and_scoped_names_the_child() {
        let runner = || {
            runner_env().into_iter().chain([(
                "OTEL_EXPORTER_OTLP_ENDPOINT".to_string(),
                "http://collector:4318".to_string(),
            )])
        };
        let mode = EnvMode::parse(POLICY, None).expect("default");
        let scope = portal();
        let env = |telemetry: ChildTelemetry| {
            child_env(
                runner(),
                &mode,
                &HashMap::new(),
                telemetry.naming(&scope, Component::WebRunner),
                identity(),
            )
        };
        let legacy = env(ChildTelemetry::Legacy);
        assert_eq!(
            legacy,
            child_env(runner(), &mode, &HashMap::new(), None, identity())
        );
        assert_eq!(
            names(&legacy),
            [
                "CUFFLINK_INTERNAL_URL",
                "CUFFLINK_SERVICE",
                "CUFFLINK_TENANT",
                "HOME",
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "OTEL_SERVICE_NAME",
                "PATH",
                "TEST_RUNTIME",
            ]
        );
        assert_eq!(legacy["OTEL_SERVICE_NAME"], "web");

        let scoped = env(ChildTelemetry::Scoped);
        assert_eq!(scoped["OTEL_SERVICE_NAME"], "portal");
        assert_eq!(
            scoped["OTEL_RESOURCE_ATTRIBUTES"],
            "service.namespace=preview-pr-7,service.version=v12,cufflink.component=web-runner"
        );
        assert_eq!(
            scoped["OTEL_EXPORTER_OTLP_ENDPOINT"],
            "http://collector:4318"
        );
        assert_eq!(
            scoped
                .iter()
                .filter(|(name, _)| !name.starts_with("OTEL_"))
                .collect::<Vec<_>>(),
            legacy
                .iter()
                .filter(|(name, _)| !name.starts_with("OTEL_"))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn the_child_settings_default_to_today_and_refuse_what_they_do_not_know() {
        let settings = |pairs: &[(&str, &str)]| {
            ChildSettings::from_vars(&Vars(cufflink_settings::fixed(pairs)))
        };
        assert_eq!(settings(&[]).expect("defaults"), ChildSettings::default());
        assert_eq!(
            ChildSettings::default(),
            ChildSettings {
                telemetry: ChildTelemetry::Legacy,
                output_level: OutputLevel::Debug,
            }
        );
        assert_eq!(
            settings(&[
                ("CUFFLINK_CHILD_TELEMETRY", " Scoped "),
                ("CUFFLINK_CHILD_OUTPUT_LEVEL", "info"),
            ])
            .expect("set"),
            ChildSettings {
                telemetry: ChildTelemetry::Scoped,
                output_level: OutputLevel::Info,
            }
        );
        for (pairs, refused) in [
            (
                [("CUFFLINK_CHILD_TELEMETRY", "on")],
                "CUFFLINK_CHILD_TELEMETRY has an invalid value \"on\": 'on' is not one of legacy, scoped",
            ),
            (
                [("CUFFLINK_CHILD_OUTPUT_LEVEL", "warn")],
                "CUFFLINK_CHILD_OUTPUT_LEVEL has an invalid value \"warn\": 'warn' is not one of debug, info",
            ),
        ] {
            assert_eq!(settings(&pairs).expect_err("refused").to_string(), refused);
        }
    }

    #[test]
    fn passthrough_values_are_validated_by_their_setting() {
        assert_eq!(
            EnvMode::parse(POLICY, Some("")).expect("empty"),
            EnvMode {
                policy: POLICY,
                extra: vec![]
            }
        );
        assert_eq!(
            EnvMode::parse(POLICY, Some("A_B,C_*")).expect("patterns"),
            EnvMode {
                policy: POLICY,
                extra: vec![
                    EnvPattern::Name("A_B".into()),
                    EnvPattern::Prefix("C_".into())
                ]
            }
        );
        for invalid in ["*", "*,PATH", "A-B", "A*B", "**"] {
            let error = EnvMode::parse(POLICY, Some(invalid))
                .expect_err(invalid)
                .to_string();
            assert!(error.starts_with("TEST_ENV_PASSTHROUGH: '"), "{error}");
        }
    }
}
