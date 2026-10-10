use super::{authorize_user, PostureGate};
use crate::auth::scope::{self, EffectiveScope};
use crate::auth::AuthUser;
use crate::deployer::manifest::{ColumnDefinition, ServiceManifest};
use crate::errors::{PlatformError, PlatformResult};
use crate::registry::RegisteredService;
use crate::tenant_catalog::TenantEntry;
use crate::AppState;
use axum::http::HeaderMap;
use std::sync::Arc;

pub(crate) async fn admit(
    state: &Arc<AppState>,
    service: &RegisteredService,
    tenant: &TenantEntry,
    headers: &HeaderMap,
    area: &str,
    operation: &str,
    owner_field: Option<&str>,
) -> PlatformResult<(AuthUser, EffectiveScope)> {
    let user = authorize_user(
        state,
        service,
        tenant,
        headers,
        PostureGate::RequirePrincipal,
    )
    .await?
    .ok_or_else(|| PlatformError::Unauthorized("Authentication required for this route".into()))?;
    let target = scope::Target {
        area,
        owner_column: owner_field,
        columns: None,
        self_service: false,
    };
    let principal = Some(user);
    let scope = scope::for_target(
        state,
        service.tenant_id,
        target,
        area,
        operation,
        &principal,
        None,
    )
    .await?;
    let columns = area_columns(&service.manifest, area);
    principal
        .map(|user| (user, scope.typed_for(&columns)))
        .ok_or_else(|| PlatformError::Unauthorized("Authentication required for this route".into()))
}

fn area_columns(manifest: &ServiceManifest, area: &str) -> Vec<ColumnDefinition> {
    let declared: Vec<&ColumnDefinition> = manifest
        .declared_tables()
        .filter(|table| table.scoped_by.as_deref() == Some(area))
        .flat_map(|table| &table.columns)
        .collect();
    declared
        .iter()
        .enumerate()
        .filter(|(index, column)| {
            declared[..*index]
                .iter()
                .all(|earlier| earlier.name != column.name)
                && declared.iter().all(|other| {
                    other.name != column.name || other.column_type == column.column_type
                })
        })
        .map(|(_, column)| (*column).clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn names(manifest: &ServiceManifest, area: &str) -> Vec<String> {
        area_columns(manifest, area)
            .into_iter()
            .map(|column| column.name)
            .collect()
    }

    #[test]
    fn an_areas_columns_are_those_its_tables_agree_on() {
        let manifest: ServiceManifest = serde_json::from_value(json!({"name": "svc", "tables": [
            {"name": "a", "scoped_by": "books", "columns": [
                {"name": "org", "column_type": "integer"},
                {"name": "code", "column_type": "text"}]},
            {"name": "b", "scoped_by": "books", "columns": [
                {"name": "org", "column_type": "integer"},
                {"name": "code", "column_type": "uuid"}]},
            {"name": "c", "scoped_by": "other", "columns": [
                {"name": "region", "column_type": "text"}]},
        ]}))
        .expect("manifest");
        assert_eq!(names(&manifest, "books"), ["org"]);
        assert!(names(&manifest, "none").is_empty());
    }

    #[test]
    fn an_area_whose_only_table_is_private_types_its_scope() {
        let manifest: ServiceManifest = serde_json::from_value(json!({
            "name": "loan-service",
            "schema": "adopted",
            "tables": [{"name": "labels", "scoped_by": "labels", "columns": [
                {"name": "org", "column_type": "integer"}]}],
            "private_tables": [{"name": "notes", "scoped_by": "notes", "columns": [
                {"name": "owner", "column_type": "uuid"},
                {"name": "org", "column_type": "text"}]}]
        }))
        .expect("manifest");
        assert_eq!(names(&manifest, "notes"), ["owner", "org"]);
        assert_eq!(
            area_columns(&manifest, "notes")[1].column_type,
            cufflink_types::ColumnType::Text,
            "the private table's own type, not that of a served table's column of the same name"
        );
        assert_eq!(names(&manifest, "labels"), ["org"]);
    }
}
