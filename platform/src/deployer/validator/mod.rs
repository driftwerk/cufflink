mod authorization;
mod declarations;
mod native;
mod routes;
mod subscriptions;
mod tables;

use super::manifest::ServiceManifest;
use crate::errors::PlatformError;
use cufflink_types::manifest::validate::validate_extensions;
use cufflink_types::wasm_features::WasmFeaturesError;
use cufflink_types::ServiceMode;
use std::collections::BTreeSet;

pub use subscriptions::{
    subscription_scope_violation, validate_tenant_scope, SHARED_STREAMS, SHARED_SUBJECT_PATTERNS,
};

pub fn validate_manifest(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    if manifest.name.is_empty() {
        return Err(PlatformError::Validation(
            "Service name cannot be empty".into(),
        ));
    }

    if !cufflink_types::slug::is_valid_service_name(&manifest.name) {
        return Err(PlatformError::Validation(format!(
            "Service name can only contain ASCII letters, digits, hyphens and underscores (at most {} characters)",
            cufflink_types::slug::SERVICE_NAME_MAX
        )));
    }

    validate_extensions("extensions", &manifest.extensions).map_err(PlatformError::Validation)?;

    match manifest.mode {
        ServiceMode::Native => native::validate(manifest),
        ServiceMode::Web => {
            native::refuse_provides(manifest)?;
            tables::refuse_private(manifest)?;
            refuse_extensions(manifest)
        }
        ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container => {
            native::refuse_provides(manifest)?;
            validate_platform_run(manifest)
        }
    }
}

fn refuse_extensions(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    if manifest.extensions.is_empty() {
        Ok(())
    } else {
        Err(PlatformError::Validation(
            "extensions: a web app calls no extensions; only a service the platform runs does"
                .into(),
        ))
    }
}

fn validate_platform_run(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    tables::validate_all(manifest)?;
    authorization::validate(manifest)?;
    subscriptions::validate(manifest)?;
    declarations::validate(manifest)?;

    for handler in manifest.declared_wasm_handlers() {
        if !is_valid_handler_name(handler) {
            return Err(PlatformError::Validation(format!(
                "Handler name '{}' is not a valid identifier (expected ASCII letters, digits, \
                 and underscores; must not start with a digit)",
                handler
            )));
        }
    }

    routes::validate(manifest)?;

    Ok(())
}

pub const PLATFORM_FEATURES: &[&str] = &[
    "route_posture",
    "nats_publish_authz",
    "forward_auth",
    "graphql_opt_in",
];

pub fn validate_platform_requirements<'a>(
    required: impl IntoIterator<Item = &'a str>,
    supports: &[&str],
) -> Result<(), PlatformError> {
    match required
        .into_iter()
        .find(|feature| !supports.contains(feature))
    {
        Some(feature) => Err(PlatformError::Validation(format!(
            "Service requires platform feature '{}' which this platform does not provide. \
             Upgrade the platform (advertised features: {}).",
            feature,
            supports.join(", ")
        ))),
        None => Ok(()),
    }
}

pub fn module_features(wasm: &[u8]) -> Result<BTreeSet<String>, PlatformError> {
    cufflink_types::wasm_features::declared(wasm).map_err(|e| match e {
        WasmFeaturesError::Malformed(_) => {
            PlatformError::Internal(format!("Failed to load WASM module: {e}"))
        }
        WasmFeaturesError::InvalidName(_) => PlatformError::Validation(e.to_string()),
    })
}

fn is_valid_handler_name(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub(crate) fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

#[cfg(test)]
pub(crate) mod fixtures {
    use cufflink_types::{
        ColumnDefinition, ColumnType, ServiceManifest, ServiceMode, TableDefinition,
    };

    pub(crate) fn accepted_by_v0_27_0() -> Vec<ServiceManifest> {
        [
            include_str!("../../../../types/fixtures/sample_manifest.json"),
            include_str!("../../../../types/fixtures/todo_manifest.json"),
            include_str!("../../../../types/fixtures/blog_manifest.json"),
            include_str!("../../../../types/fixtures/legacy_shapes_manifest.json"),
        ]
        .into_iter()
        .map(|json| serde_json::from_str(json).expect("fixture"))
        .collect()
    }

    pub(in crate::deployer::validator) fn private_loans() -> TableDefinition {
        let text = |name: &str| ColumnDefinition {
            name: name.into(),
            column_type: ColumnType::Text,
            ..Default::default()
        };
        TableDefinition {
            name: "loans".into(),
            managed: false,
            columns: vec![text("owner"), text("tag")],
            ..Default::default()
        }
    }

    pub(in crate::deployer::validator) fn minimal_manifest() -> ServiceManifest {
        ServiceManifest {
            name: "svc".into(),
            version: None,
            tables: vec![TableDefinition {
                name: "items".into(),
                columns: vec![ColumnDefinition {
                    name: "id".into(),
                    column_type: ColumnType::Uuid,
                    primary_key: true,
                    nullable: false,
                    auto_generate: true,
                    default_value: None,
                    references: None,
                    on_delete: None,
                    unique: false,
                    validations: vec![],
                    ..Default::default()
                }],
                indexes: vec![],
                soft_delete: false,
                owner_field: None,
                auth_required: false,
                permission_area: None,
                owner_self_service: false,
                hooks: None,
                scoped_by: None,
                ..Default::default()
            }],
            cells: vec![],
            events: vec![],
            subscriptions: vec![],
            custom_routes: vec![],
            mode: ServiceMode::Crud,
            authorization: None,
            on_migrate: None,
            publishes: None,
            required_platform_features: vec![],
            graphql: false,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{minimal_manifest, private_loans};
    use super::*;
    use cufflink_types::{CustomRouteDefinition, TableDefinition, TableHooks};

    #[test]
    fn requirements_are_checked_against_the_advertised_list() {
        let supports = ["on_migrate", "route_posture", "db_execute_all"];
        assert!(validate_platform_requirements(["on_migrate"], &supports).is_ok());
        assert!(validate_platform_requirements([], &supports).is_ok());
        let err = validate_platform_requirements(["route_posture", "time_travel"], &supports)
            .expect_err("unknown feature")
            .to_string();
        assert!(
            err.contains("time_travel") && err.contains("db_execute_all"),
            "{err}"
        );
    }

    #[test]
    fn module_features_are_read_and_their_errors_classified() {
        let module = |section: &str| {
            wat::parse_str(format!(
                r#"(module (@custom "cufflink.features" "{section}"))"#
            ))
            .expect("wat")
        };
        assert_eq!(
            module_features(&module(r"db_v2\n")).expect("declared"),
            BTreeSet::from(["db_v2".to_string()])
        );
        assert!(matches!(
            module_features(&module(r"DB\n")),
            Err(PlatformError::Validation(_))
        ));
        assert!(matches!(
            module_features(b"\0asm junk"),
            Err(PlatformError::Internal(_))
        ));
    }

    #[test]
    fn valid_manifest_passes() {
        assert!(validate_manifest(&minimal_manifest()).is_ok());
    }

    #[test]
    fn every_manifest_v0_27_0_accepted_is_accepted() {
        for manifest in fixtures::accepted_by_v0_27_0() {
            validate_manifest(&manifest).unwrap_or_else(|e| panic!("{}: {e}", manifest.name));
            validate_tenant_scope(
                &manifest,
                &crate::nats_manager::subjects::Namespace::own("acme"),
                &[],
            )
            .unwrap_or_else(|e| panic!("{}: {e}", manifest.name));
        }
    }

    #[test]
    fn a_web_app_calls_no_extensions() {
        let mut m = minimal_manifest();
        m.mode = ServiceMode::Web;
        assert!(validate_manifest(&m).is_ok());
        m.extensions = vec!["google.token".into()];
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(
            err.contains("extensions: a web app calls no extensions"),
            "{err}"
        );
    }

    #[test]
    fn extensions_are_named_once_whatever_the_mode() {
        for mode in [ServiceMode::Crud, ServiceMode::Wasm, ServiceMode::Container] {
            let mut m = minimal_manifest();
            m.mode = mode.clone();
            m.extensions = vec!["google.token".into(), "pdf.render-v2".into()];
            assert!(validate_manifest(&m).is_ok(), "{mode:?}");
            m.extensions.push("Google.Token".into());
            let err = validate_manifest(&m).unwrap_err().to_string();
            assert!(
                err.contains("extensions[2]: 'Google.Token' is not an extension name"),
                "{err}"
            );
            m.extensions = vec!["pdf.render".into(), "pdf.render".into()];
            let err = validate_manifest(&m).unwrap_err().to_string();
            assert!(
                err.contains("extensions: extension 'pdf.render' is listed twice"),
                "{err}"
            );
        }
    }

    #[test]
    fn handler_name_with_space_is_rejected() {
        let mut m = minimal_manifest();
        m.tables[0].hooks = Some(TableHooks {
            before_create: Some("validate item".into()),
            ..Default::default()
        });
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(err.contains("not a valid identifier"), "got error: {}", err);
    }

    #[test]
    fn empty_handler_name_is_rejected() {
        let mut m = minimal_manifest();
        m.tables[0].hooks = Some(TableHooks {
            after_create: Some("".into()),
            ..Default::default()
        });
        assert!(validate_manifest(&m).is_err());
    }

    #[test]
    fn handler_name_starting_with_digit_is_rejected() {
        let mut m = minimal_manifest();
        m.on_migrate = Some("1bad".into());
        assert!(validate_manifest(&m).is_err());
    }

    #[test]
    fn valid_handler_names_pass() {
        let mut m = minimal_manifest();
        m.on_migrate = Some("on_migrate_v2".into());
        m.tables[0].hooks = Some(TableHooks {
            before_create: Some("validate_item".into()),
            after_create: Some("AuditCreate".into()),
            before_update: Some("_internal_check".into()),
            ..Default::default()
        });
        m.custom_routes.push(CustomRouteDefinition {
            method: "GET".into(),
            path: "/h".into(),
            handler: "h".into(),
            job: None,
            posture: None,
            forward_auth: false,
            ..Default::default()
        });
        assert!(validate_manifest(&m).is_ok());
    }

    #[test]
    fn a_native_or_web_service_may_not_declare_private_tables() {
        let native = ServiceManifest {
            name: "ext-google".into(),
            mode: ServiceMode::Native,
            provides: vec![cufflink_types::ProvidedExtension {
                name: "google.token".into(),
                callers: vec!["reports-service".into()],
            }],
            ..Default::default()
        };
        assert!(validate_manifest(&native).is_ok());
        let err = validate_manifest(&ServiceManifest {
            private_tables: vec![private_loans()],
            ..native
        })
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(
                "a native service declares only the extensions it provides, not private_tables"
            ),
            "{err}"
        );

        let mut web = minimal_manifest();
        web.mode = ServiceMode::Web;
        assert!(validate_manifest(&web).is_ok());
        web.private_tables = vec![private_loans()];
        let err = validate_manifest(&web).unwrap_err().to_string();
        assert!(
            err.contains("private_tables: a web app declares no tables"),
            "{err}"
        );
    }

    #[test]
    fn a_private_table_the_platform_would_make_is_refused() {
        let mut m = minimal_manifest();
        m.private_tables = vec![private_loans()];
        assert!(validate_manifest(&m).is_ok(), "an unmanaged table");
        m.private_tables[0].managed = true;
        let err = validate_manifest(&m).unwrap_err().to_string();
        assert!(
            err.contains(
                "Private table 'loans' would be made by the platform; declare it \
                 #[table(managed = false)] and create it in an SQL migration"
            ),
            "{err}"
        );
        m.schema = cufflink_types::SchemaOwnership::Adopted;
        assert!(validate_manifest(&m).is_ok(), "a schema that is adopted");
    }

    #[test]
    fn a_private_table_sharing_a_folded_name_is_refused() {
        let refused = |tables: &[&str], private_tables: &[&str]| {
            let mut m = minimal_manifest();
            m.tables = tables
                .iter()
                .map(|name| cufflink_types::TableDefinition {
                    name: name.to_string(),
                    ..minimal_manifest().tables.remove(0)
                })
                .collect();
            m.private_tables = private_tables
                .iter()
                .map(|name| TableDefinition {
                    name: name.to_string(),
                    ..private_loans()
                })
                .collect();
            validate_manifest(&m).map_err(|e| e.to_string())
        };
        assert_eq!(refused(&["items"], &["loans", "loan_tags"]), Ok(()));
        for (tables, private_tables, pair) in [
            (
                vec!["items", "Loans"],
                vec!["loans"],
                "'loans' names the same table as 'Loans'",
            ),
            (
                vec!["items"],
                vec!["loans", "LOANS"],
                "'LOANS' names the same table as 'loans'",
            ),
            (
                vec!["items"],
                vec!["items"],
                "'items' names the same table as 'items'",
            ),
        ] {
            let err = refused(&tables, &private_tables).expect_err(pair);
            assert!(err.contains("Private table") && err.contains(pair), "{err}");
        }
    }

    #[test]
    fn two_served_tables_validate_as_before() {
        let mut m = minimal_manifest();
        let mut second = m.tables[0].clone();
        second.name = "Items".into();
        m.tables.push(second.clone());
        m.tables.push(second);
        assert!(
            validate_manifest(&m).is_ok(),
            "served tables are not compared with each other"
        );
    }
}
