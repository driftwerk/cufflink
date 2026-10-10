use crate::deployer::manifest::ServiceManifest;
use crate::errors::PlatformError;
use cufflink_types::manifest::validate::validate_provides;

pub(super) fn validate(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    let declared: Vec<&str> = [
        (!manifest.tables.is_empty()).then_some("tables"),
        (!manifest.private_tables.is_empty()).then_some("private_tables"),
        (!manifest.cells.is_empty()).then_some("cells"),
        (!manifest.events.is_empty()).then_some("events"),
        (!manifest.subscriptions.is_empty()).then_some("subscriptions"),
        (!manifest.custom_routes.is_empty()).then_some("custom_routes"),
        (!manifest.schedules.is_empty()).then_some("schedules"),
        (!manifest.streams.is_empty()).then_some("streams"),
        (!manifest.migrations.is_empty()).then_some("migrations"),
        (!manifest.extensions.is_empty()).then_some("extensions"),
        manifest.authorization.is_some().then_some("authorization"),
        manifest.on_migrate.is_some().then_some("on_migrate"),
    ]
    .into_iter()
    .flatten()
    .collect();
    if !declared.is_empty() {
        return Err(PlatformError::Validation(format!(
            "a native service declares only the extensions it provides, not {}",
            declared.join(", ")
        )));
    }
    validate_provides("provides", &manifest.provides).map_err(PlatformError::Validation)
}

pub(super) fn refuse_provides(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    if manifest.provides.is_empty() {
        Ok(())
    } else {
        Err(PlatformError::Validation(
            "provides: only a native service (mode: native) provides extensions".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::minimal_manifest;
    use super::super::validate_manifest;
    use cufflink_types::{ProvidedExtension, ServiceManifest, ServiceMode};

    fn native() -> ServiceManifest {
        ServiceManifest {
            name: "ext-google".into(),
            tables: vec![],
            mode: ServiceMode::Native,
            provides: vec![ProvidedExtension {
                name: "google.token".into(),
                callers: vec!["reports-service".into()],
            }],
            ..Default::default()
        }
    }

    fn refused(manifest: &ServiceManifest) -> String {
        validate_manifest(manifest)
            .expect_err("refused")
            .to_string()
    }

    #[test]
    fn a_native_service_provides_extensions_and_nothing_else() {
        assert!(validate_manifest(&native()).is_ok());
        let table = minimal_manifest().tables;
        let declaring = ServiceManifest {
            tables: table,
            extensions: vec!["pdf.render".into()],
            on_migrate: Some("backfill".into()),
            ..native()
        };
        assert!(
            refused(&declaring).contains(
                "a native service declares only the extensions it provides, not tables, \
                 extensions, on_migrate"
            ),
            "{}",
            refused(&declaring)
        );
    }

    #[test]
    fn what_a_native_service_provides_is_checked() {
        for (provides, reason) in [
            (
                vec![],
                "provides: a native service provides at least one extension",
            ),
            (
                vec![ProvidedExtension {
                    name: "google.token".into(),
                    callers: vec!["a b".into()],
                }],
                "provides[0].callers: 'a b' is neither a service name nor '*'",
            ),
        ] {
            let manifest = ServiceManifest {
                provides,
                ..native()
            };
            assert!(
                refused(&manifest).contains(reason),
                "{}",
                refused(&manifest)
            );
        }
    }

    #[test]
    fn only_a_native_service_provides_extensions() {
        for mode in [
            ServiceMode::Crud,
            ServiceMode::Wasm,
            ServiceMode::Container,
            ServiceMode::Web,
        ] {
            let manifest = ServiceManifest {
                mode: mode.clone(),
                provides: native().provides,
                ..minimal_manifest()
            };
            assert!(
                refused(&manifest)
                    .contains("provides: only a native service (mode: native) provides extensions"),
                "{mode:?}"
            );
        }
    }
}
