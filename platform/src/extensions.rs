use crate::errors::{PlatformError, PlatformResult};
use cufflink_types::ext::ANY_CALLER;
use cufflink_types::{ProvidedExtension, ServiceManifest};
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtensionsConfig {
    pub enabled: bool,
    pub call_max_bytes: usize,
    pub call_timeout: Duration,
    pub native_max_bytes: usize,
}

impl Default for ExtensionsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            call_max_bytes: 1024 * 1024,
            call_timeout: Duration::from_secs(30),
            native_max_bytes: 128 * 1024 * 1024,
        }
    }
}

impl ExtensionsConfig {
    pub fn from_env() -> eyre::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let (defaults, vars) = (Self::default(), cufflink_db::Vars(var));
        Ok(Self {
            enabled: vars.parsed("CUFFLINK_EXTENSIONS", defaults.enabled)?,
            call_max_bytes: vars.positive("EXT_CALL_MAX_BYTES", defaults.call_max_bytes)?,
            call_timeout: Duration::from_secs(
                vars.positive("EXT_CALL_TIMEOUT_SECS", defaults.call_timeout.as_secs())?,
            ),
            native_max_bytes: vars
                .positive("NATIVE_ARTIFACT_MAX_BYTES", defaults.native_max_bytes)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backend {
    Nats,
    Native { service_id: Uuid },
}

impl Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Backend::Nats => "nats",
            Backend::Native { .. } => "native",
        }
    }

    pub fn service_id(self) -> Option<Uuid> {
        match self {
            Backend::Nats => None,
            Backend::Native { service_id } => Some(service_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub backend: Backend,
    pub callers: Vec<String>,
}

impl Registration {
    pub fn admits(&self, service: &str) -> bool {
        admits(&self.callers, service)
    }
}

fn admits(callers: &[String], service: &str) -> bool {
    callers
        .iter()
        .any(|caller| caller == ANY_CALLER || caller == service)
}

pub fn unadmitted(
    manifest: &ServiceManifest,
    registrations: &BTreeMap<String, Registration>,
) -> Option<String> {
    manifest
        .extensions
        .iter()
        .find_map(|name| match registrations.get(name) {
            None => Some(format!(
                "extensions: '{name}' is not registered for this tenant"
            )),
            Some(registration) if !registration.admits(&manifest.name) => Some(format!(
                "extensions: the registration of '{name}' does not list '{}' among its callers",
                manifest.name
            )),
            Some(_) => None,
        })
}

pub fn require_admitted(
    manifest: &ServiceManifest,
    registrations: &BTreeMap<String, Registration>,
) -> PlatformResult<()> {
    unadmitted(manifest, registrations)
        .map_or(Ok(()), |reason| Err(PlatformError::Validation(reason)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declaration {
    pub service: String,
    pub extension: String,
}

impl Declaration {
    pub fn provides_refusal(&self) -> String {
        format!(
            "provides: service '{}' is deployed calling extension '{}'; redeploy it without the \
             extension first",
            self.service, self.extension
        )
    }
}

pub fn stranded_by<'a>(
    declarations: &'a [Declaration],
    name: &str,
    after: Option<&[String]>,
) -> Option<&'a Declaration> {
    declarations
        .iter()
        .filter(|declared| declared.extension == name)
        .find(|declared| !after.is_some_and(|callers| admits(callers, &declared.service)))
}

pub fn unprovidable(
    provides: &[ProvidedExtension],
    service_id: Option<Uuid>,
    registrations: &BTreeMap<String, Registration>,
) -> Option<String> {
    provides.iter().find_map(|provided| {
        let name = &provided.name;
        match registrations
            .get(name)
            .map(|registration| registration.backend)
        {
            None => None,
            Some(Backend::Native { service_id: owner }) if Some(owner) == service_id => None,
            Some(Backend::Native { .. }) => Some(format!(
                "provides: '{name}' is provided by another native service of this tenant"
            )),
            Some(Backend::Nats) => Some(format!(
                "provides: '{name}' is registered for this tenant as a NATS provider"
            )),
        }
    })
}

pub fn stranded_by_provides<'a>(
    declarations: &'a [Declaration],
    service_id: Uuid,
    provides: &[ProvidedExtension],
    registrations: &BTreeMap<String, Registration>,
) -> Option<&'a Declaration> {
    provided_by(registrations, service_id).find_map(|name| {
        let after = provides
            .iter()
            .find(|provided| provided.name == name)
            .map(|provided| provided.callers.as_slice());
        stranded_by(declarations, name, after)
    })
}

pub fn provided_by(
    registrations: &BTreeMap<String, Registration>,
    service_id: Uuid,
) -> impl Iterator<Item = &str> {
    registrations
        .iter()
        .filter(move |(_, registration)| registration.backend == Backend::Native { service_id })
        .map(|(name, _)| name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(vars: &[(&str, &str)]) -> eyre::Result<ExtensionsConfig> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        ExtensionsConfig::from_vars(|name| vars.get(name).cloned())
    }

    #[test]
    fn the_configuration_defaults_and_names_what_it_refuses() {
        assert_eq!(
            config(&[]).expect("defaults"),
            ExtensionsConfig {
                enabled: false,
                call_max_bytes: 1_048_576,
                call_timeout: Duration::from_secs(30),
                native_max_bytes: 134_217_728,
            }
        );
        assert_eq!(
            config(&[
                ("CUFFLINK_EXTENSIONS", "true"),
                ("EXT_CALL_MAX_BYTES", "65536"),
                ("EXT_CALL_TIMEOUT_SECS", "5"),
                ("NATIVE_ARTIFACT_MAX_BYTES", "1048576"),
            ])
            .expect("set"),
            ExtensionsConfig {
                enabled: true,
                call_max_bytes: 65_536,
                call_timeout: Duration::from_secs(5),
                native_max_bytes: 1_048_576,
            }
        );
        for (name, value) in [
            ("CUFFLINK_EXTENSIONS", "yes"),
            ("EXT_CALL_MAX_BYTES", "0"),
            ("EXT_CALL_MAX_BYTES", "-1"),
            ("EXT_CALL_MAX_BYTES", "1MiB"),
            ("EXT_CALL_TIMEOUT_SECS", "0"),
            ("EXT_CALL_TIMEOUT_SECS", "soon"),
            ("NATIVE_ARTIFACT_MAX_BYTES", "0"),
            ("NATIVE_ARTIFACT_MAX_BYTES", "big"),
        ] {
            let refused = config(&[(name, value)]).expect_err(name).to_string();
            assert!(refused.contains(name), "{refused}");
        }
    }

    fn nats(callers: &[&str]) -> Registration {
        Registration {
            backend: Backend::Nats,
            callers: callers.iter().map(|c| c.to_string()).collect(),
        }
    }

    fn calling(service: &str, extensions: &[&str]) -> ServiceManifest {
        ServiceManifest {
            name: service.into(),
            extensions: extensions.iter().map(|e| e.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn a_deploy_calls_only_what_is_registered_for_it() {
        let registrations = BTreeMap::from([
            ("google.token".to_string(), nats(&["reports"])),
            ("pdf.render".to_string(), nats(&["*"])),
        ]);
        for admitted in [
            calling("reports", &[]),
            calling("reports", &["google.token", "pdf.render"]),
            calling("billing", &["pdf.render"]),
        ] {
            assert_eq!(unadmitted(&admitted, &registrations), None);
        }
        assert_eq!(
            unadmitted(
                &calling("billing", &["pdf.render", "google.token"]),
                &registrations
            ),
            Some(
                "extensions: the registration of 'google.token' does not list 'billing' among \
                 its callers"
                    .into()
            )
        );
        assert_eq!(
            unadmitted(
                &calling("reports", &["google.token", "maps.geocode"]),
                &registrations
            ),
            Some("extensions: 'maps.geocode' is not registered for this tenant".into())
        );
        assert_eq!(
            unadmitted(&calling("reports", &["google.token"]), &BTreeMap::new()),
            Some("extensions: 'google.token' is not registered for this tenant".into())
        );
    }

    fn declared(pairs: &[(&str, &str)]) -> Vec<Declaration> {
        pairs
            .iter()
            .map(|(service, extension)| Declaration {
                service: service.to_string(),
                extension: extension.to_string(),
            })
            .collect()
    }

    #[test]
    fn a_registration_is_not_taken_from_a_service_deployed_calling_it() {
        let declarations = declared(&[("reports", "google.token"), ("billing", "pdf.render")]);
        let callers = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        for kept in [
            callers(&["reports"]),
            callers(&["*"]),
            callers(&["billing", "reports"]),
        ] {
            assert_eq!(
                stranded_by(&declarations, "google.token", Some(&kept)),
                None,
                "{kept:?}"
            );
        }
        let stranded = Some(&declarations[0]);
        assert_eq!(
            stranded_by(&declarations, "google.token", Some(&callers(&["billing"]))),
            stranded
        );
        assert_eq!(stranded_by(&declarations, "google.token", None), stranded);
        assert_eq!(stranded_by(&declarations, "maps.geocode", None), None);
        assert_eq!(stranded_by(&[], "google.token", None), None);
    }

    fn provided(name: &str, callers: &[&str]) -> ProvidedExtension {
        ProvidedExtension {
            name: name.into(),
            callers: callers.iter().map(|c| c.to_string()).collect(),
        }
    }

    fn native(service_id: Uuid, callers: &[&str]) -> Registration {
        Registration {
            backend: Backend::Native { service_id },
            callers: callers.iter().map(|c| c.to_string()).collect(),
        }
    }

    #[test]
    fn a_native_service_provides_only_names_that_are_free_or_its_own() {
        let (own, other) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let registrations = BTreeMap::from([
            ("google.token".to_string(), native(own, &["reports"])),
            ("pdf.render".to_string(), native(other, &["*"])),
            ("maps.geocode".to_string(), nats(&["*"])),
        ]);
        for admitted in [
            vec![],
            vec![provided("google.token", &["*"])],
            vec![
                provided("google.token", &["*"]),
                provided("new.one", &["*"]),
            ],
        ] {
            assert_eq!(unprovidable(&admitted, Some(own), &registrations), None);
        }
        assert_eq!(
            unprovidable(&[provided("pdf.render", &["*"])], Some(own), &registrations),
            Some(
                "provides: 'pdf.render' is provided by another native service of this tenant"
                    .into()
            )
        );
        assert_eq!(
            unprovidable(
                &[provided("maps.geocode", &["*"])],
                Some(own),
                &registrations
            ),
            Some(
                "provides: 'maps.geocode' is registered for this tenant as a NATS provider".into()
            )
        );
        assert_eq!(
            unprovidable(&[provided("google.token", &["*"])], None, &registrations),
            Some(
                "provides: 'google.token' is provided by another native service of this tenant"
                    .into()
            ),
            "a service deployed for the first time owns nothing yet"
        );
    }

    #[test]
    fn a_native_deploy_keeps_what_deployed_services_call() {
        let (own, other) = (Uuid::from_u128(2), Uuid::from_u128(3));
        let declarations = declared(&[("reports", "google.token"), ("billing", "pdf.render")]);
        let registrations = BTreeMap::from([
            ("google.token".to_string(), native(own, &["reports"])),
            ("pdf.render".to_string(), native(other, &["billing"])),
            ("maps.geocode".to_string(), native(own, &["*"])),
        ]);
        assert_eq!(
            provided_by(&registrations, own).collect::<Vec<_>>(),
            ["google.token", "maps.geocode"]
        );
        for kept in [
            vec![provided("google.token", &["reports"])],
            vec![
                provided("google.token", &["*"]),
                provided("new.one", &["*"]),
            ],
        ] {
            assert_eq!(
                stranded_by_provides(&declarations, own, &kept, &registrations),
                None,
                "{kept:?}"
            );
        }
        let stranded = Some(&declarations[0]);
        assert_eq!(
            stranded_by_provides(&declarations, own, &[], &registrations),
            stranded,
            "dropped"
        );
        assert_eq!(
            stranded_by_provides(
                &declarations,
                own,
                &[provided("google.token", &["billing"])],
                &registrations
            ),
            stranded,
            "narrowed"
        );
        assert_eq!(
            stranded_by_provides(&declarations, other, &[], &registrations)
                .map(Declaration::provides_refusal),
            Some(
                "provides: service 'billing' is deployed calling extension 'pdf.render'; \
                 redeploy it without the extension first"
                    .into()
            )
        );
        assert_eq!(
            stranded_by_provides(&[], own, &[], &registrations),
            None,
            "nothing deployed calls any of these"
        );
    }
}
