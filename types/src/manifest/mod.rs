//! The service manifest: everything the SDK macros emit about a service and
//! the platform deploys from.

mod authorization;
mod migration;
mod route;
mod schedule;
mod schema_diff;
mod service;
mod stream;
mod subscription;
mod table;
pub mod validate;

pub use authorization::*;
pub use migration::*;
pub use route::*;
pub use schedule::*;
pub use schema_diff::*;
pub use service::*;
pub use stream::*;
pub use subscription::*;
pub use table::*;

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;

    pub(crate) fn sample_manifest() -> ServiceManifest {
        ServiceManifest {
            name: "test-service".into(),
            version: Some("1.0.0".into()),
            tables: vec![TableDefinition {
                name: "todos".into(),
                columns: vec![
                    ColumnDefinition {
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
                    },
                    ColumnDefinition {
                        name: "title".into(),
                        column_type: ColumnType::Text,
                        primary_key: false,
                        nullable: false,
                        auto_generate: false,
                        default_value: None,
                        references: None,
                        on_delete: None,
                        unique: false,
                        validations: vec![],
                        ..Default::default()
                    },
                    ColumnDefinition {
                        name: "user_id".into(),
                        column_type: ColumnType::Uuid,
                        primary_key: false,
                        nullable: false,
                        auto_generate: false,
                        default_value: None,
                        references: Some("users.id".into()),
                        on_delete: Some(ForeignKeyAction::Cascade),
                        unique: false,
                        validations: vec![],
                        ..Default::default()
                    },
                ],
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
