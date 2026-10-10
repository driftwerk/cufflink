use crate::auth::grants;
use crate::deployer::manifest::{ServiceManifest, TableDefinition};
use crate::errors::PlatformError;

pub(super) fn validate(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    validate_scoped_areas(manifest)?;
    validate_column_operations(manifest)?;
    manifest
        .authorization
        .iter()
        .flat_map(|authorization| &authorization.default_roles)
        .try_for_each(|role| {
            grants::validate_scoped(&role.permissions).map_err(|invalid| {
                PlatformError::Validation(format!(
                    "Default role '{}': {}",
                    role.name,
                    grants::describe(&invalid)
                ))
            })
        })
}

fn validate_scoped_areas(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    let declared = |area: &str| {
        manifest
            .authorization
            .iter()
            .flat_map(|authorization| &authorization.areas)
            .any(|declared| declared.name == area)
    };
    manifest
        .declared_tables()
        .find_map(|table| {
            table
                .scoped_by
                .as_deref()
                .filter(|area| !declared(area))
                .map(|area| (table, area))
        })
        .map_or(Ok(()), |(table, area)| {
            Err(PlatformError::Validation(format!(
                "Table '{}': scoped_by '{area}' is not an area declared in authorization.areas",
                table.name
            )))
        })
}

fn undeclared_operation(manifest: &ServiceManifest, table: &TableDefinition) -> Option<String> {
    let area = table.access_area()?;
    let declared = manifest
        .authorization
        .iter()
        .flat_map(|authorization| &authorization.areas)
        .find(|declared| declared.name == area);
    table
        .restricted_columns()
        .flat_map(|(column, access)| {
            [&access.read, &access.write]
                .into_iter()
                .flatten()
                .map(move |operation| (column, operation))
        })
        .find_map(|(column, operation)| {
            match declared {
                None => Some(format!(
                    "'{area}' is not an area declared in authorization.areas"
                )),
                Some(declared) if !declared.operations.contains(operation) => Some(format!(
                    "'{operation}' is not an operation of area '{area}' (declared: {})",
                    declared.operations.join(", ")
                )),
                Some(_) => None,
            }
            .map(|reason| {
                format!(
                    "Table '{}': column '{}' asks {area}:{operation}, and {reason}",
                    table.name, column.name
                )
            })
        })
}

fn validate_column_operations(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    manifest
        .tables
        .iter()
        .find_map(|table| undeclared_operation(manifest, table))
        .map_or(Ok(()), |reason| Err(PlatformError::Validation(reason)))
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::minimal_manifest;
    use super::*;
    use cufflink_types::{AuthorizationConfig, DefaultRoleDef, PermissionAreaDef};

    fn with_roles(permissions: &[&str]) -> ServiceManifest {
        ServiceManifest {
            authorization: Some(AuthorizationConfig {
                areas: vec![],
                default_roles: vec![DefaultRoleDef {
                    name: "staff".into(),
                    description: None,
                    permissions: permissions.iter().map(|p| p.to_string()).collect(),
                }],
            }),
            ..minimal_manifest()
        }
    }

    #[test]
    fn default_roles_accept_every_unscoped_string_and_valid_scopes() {
        assert!(validate(&minimal_manifest()).is_ok());
        assert!(validate(&with_roles(&[
            "items:view",
            "Odd String:*",
            "items:view@all",
            "items:edit@field(org_id=org)",
        ]))
        .is_ok());
    }

    #[test]
    fn a_malformed_scope_in_a_default_role_is_rejected_by_name() {
        let err = validate(&with_roles(&["items:view", "items:view@everyone"]))
            .expect_err("malformed")
            .to_string();
        assert!(
            err.contains("Default role 'staff'") && err.contains("items:view@everyone"),
            "{err}"
        );
    }

    #[test]
    fn a_column_permission_names_a_declared_operation_of_a_declared_area() {
        let restricted = |areas: &[(&str, &[&str])]| {
            let mut m = minimal_manifest();
            let items = &mut m.tables[0];
            items.permission_area = Some("items".into());
            items.columns.push(
                serde_json::from_value(serde_json::json!({
                    "name": "notes", "column_type": "text", "nullable": true,
                    "access": {"read": "internal", "write": "manage"}
                }))
                .expect("column"),
            );
            m.authorization = Some(AuthorizationConfig {
                areas: areas
                    .iter()
                    .map(|(name, operations)| PermissionAreaDef {
                        name: name.to_string(),
                        operations: operations.iter().map(|o| o.to_string()).collect(),
                    })
                    .collect(),
                default_roles: vec![],
            });
            m
        };
        assert!(validate(&restricted(&[("items", &["view", "internal", "manage"])])).is_ok());
        for (areas, reason) in [
            (
                &[("orders", &["internal", "manage"][..])][..],
                "column 'notes' asks items:internal, and 'items' is not an area declared",
            ),
            (
                &[("items", &["view", "internal"][..])][..],
                "column 'notes' asks items:manage, and 'manage' is not an operation of area \
                 'items' (declared: view, internal)",
            ),
        ] {
            let err = validate(&restricted(areas)).expect_err(reason).to_string();
            assert!(
                err.contains("Table 'items'") && err.contains(reason),
                "{err}"
            );
        }
        let mut undeclared = restricted(&[]);
        undeclared.authorization = None;
        assert!(validate(&undeclared).is_err());
    }

    #[test]
    fn scoped_by_must_name_a_declared_area() {
        let scoped = |areas: &[&str]| {
            let mut m = minimal_manifest();
            m.tables[0].scoped_by = Some("items".into());
            m.authorization = Some(AuthorizationConfig {
                areas: areas
                    .iter()
                    .map(|name| PermissionAreaDef {
                        name: name.to_string(),
                        operations: vec![],
                    })
                    .collect(),
                default_roles: vec![],
            });
            m
        };
        assert!(validate(&scoped(&["items"])).is_ok());
        let err = validate(&scoped(&["orders"]))
            .expect_err("undeclared")
            .to_string();
        assert!(
            err.contains("scoped_by 'items' is not an area declared"),
            "{err}"
        );
        let mut bare = scoped(&[]);
        bare.authorization = None;
        assert!(validate(&bare).is_err());
    }

    #[test]
    fn a_private_tables_scoped_by_must_name_a_declared_area() {
        let scoped = |area: &str| {
            let mut m = minimal_manifest();
            let mut private = m.tables[0].clone();
            private.name = "notes".into();
            private.managed = false;
            private.scoped_by = Some("notes".into());
            m.private_tables = vec![private];
            m.authorization = Some(AuthorizationConfig {
                areas: vec![PermissionAreaDef {
                    name: area.into(),
                    operations: vec![],
                }],
                default_roles: vec![],
            });
            m
        };
        assert!(validate(&scoped("notes")).is_ok());
        let err = validate(&scoped("items"))
            .expect_err("undeclared")
            .to_string();
        assert!(
            err.contains("Table 'notes': scoped_by 'notes' is not an area declared"),
            "{err}"
        );
    }
}
