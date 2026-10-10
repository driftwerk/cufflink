use crate::auth::grants::has_permission;
use crate::auth::permissions::resolve_permissions_cached;
use crate::auth::AuthUser;
use crate::deployer::manifest::TableDefinition;
use crate::errors::{PlatformError, PlatformResult};
use crate::AppState;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) const FEATURES: &[&str] = &[cufflink_types::features::COLUMN_PERMISSIONS];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnView {
    unreadable: BTreeSet<String>,
    unwritable: BTreeMap<String, String>,
}

pub fn view(
    table: &TableDefinition,
    permissions: &[String],
    is_service_account: bool,
) -> ColumnView {
    if is_service_account {
        return ColumnView::ALL;
    }
    let area = table.access_area();
    let missing = |operation: &Option<String>| {
        operation
            .as_deref()
            .filter(|operation| {
                !area.is_some_and(|area| has_permission(permissions, area, operation))
            })
            .map(|operation| format!("{}:{operation}", area.unwrap_or_default()))
    };
    ColumnView {
        unreadable: table
            .restricted_columns()
            .filter(|(_, access)| missing(&access.read).is_some())
            .map(|(column, _)| column.name.clone())
            .collect(),
        unwritable: table
            .restricted_columns()
            .filter_map(|(column, access)| {
                missing(&access.write).map(|permission| (column.name.clone(), permission))
            })
            .collect(),
    }
}

impl ColumnView {
    pub const ALL: ColumnView = ColumnView {
        unreadable: BTreeSet::new(),
        unwritable: BTreeMap::new(),
    };

    pub fn withholds_reads(&self) -> bool {
        !self.unreadable.is_empty()
    }

    pub fn reads(&self, name: &str) -> bool {
        !self.unreadable.contains(name)
    }

    pub fn named<'t>(&self, table: &'t TableDefinition) -> Cow<'t, TableDefinition> {
        if !self.withholds_reads() {
            return Cow::Borrowed(table);
        }
        Cow::Owned(TableDefinition {
            columns: table
                .columns
                .iter()
                .filter(|column| self.reads(&column.name))
                .cloned()
                .collect(),
            ..table.clone()
        })
    }

    pub fn shown(&self, row: Value) -> Value {
        match row {
            Value::Object(fields) if self.withholds_reads() => Value::Object(
                fields
                    .into_iter()
                    .filter(|(name, _)| self.reads(name))
                    .collect(),
            ),
            other => other,
        }
    }

    pub fn admit(&self, body: &Value) -> PlatformResult<()> {
        body.as_object()
            .into_iter()
            .flat_map(|fields| fields.keys())
            .find_map(|name| self.unwritable.get_key_value(name))
            .map_or(Ok(()), |(column, permission)| {
                Err(PlatformError::Forbidden(format!(
                    "Missing permission: {permission} (to set '{column}')"
                )))
            })
    }

    fn decision_label(&self) -> &'static str {
        if *self == ColumnView::ALL {
            "all"
        } else {
            "withheld"
        }
    }
}

pub(crate) async fn for_request(
    state: &Arc<AppState>,
    service_tenant_id: Uuid,
    table: &TableDefinition,
    principal: &Option<AuthUser>,
) -> PlatformResult<ColumnView> {
    if table.restricted_columns().next().is_none() {
        return Ok(ColumnView::ALL);
    }
    let resolved = match principal {
        Some(user) if user.is_service_account => view(table, &[], true),
        Some(user) => view(
            table,
            &resolve_permissions_cached(state, service_tenant_id, &user.subject)
                .await?
                .permissions,
            false,
        ),
        None => view(table, &[], false),
    };
    metrics::counter!(
        "cufflink_column_decisions_total",
        "table" => table.name.clone(),
        "columns" => resolved.decision_label(),
    )
    .increment(1);
    tracing::debug!(
        target: "cufflink::columns",
        subject = principal.as_ref().map(|user| user.subject.as_str()),
        table = %table.name,
        view = ?resolved,
        "Column decision"
    );
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn listings() -> TableDefinition {
        serde_json::from_value(json!({
            "name": "listings",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true},
                {"name": "customer_id", "column_type": "uuid"},
                {"name": "title", "column_type": "text"},
                {"name": "ops_notes", "column_type": "text", "nullable": true,
                 "access": {"read": "internal", "write": "internal"}},
                {"name": "status", "column_type": "text", "default_value": "draft",
                 "access": {"write": "manage"}},
                {"name": "storage", "column_type": "text", "nullable": true,
                 "access": {"read": "internal"}}
            ],
            "owner_field": "customer_id",
            "owner_self_service": true,
            "permission_area": "listings"
        }))
        .expect("table")
    }

    fn view_of(permissions: &[&str]) -> ColumnView {
        let permissions: Vec<String> = permissions.iter().map(|p| p.to_string()).collect();
        view(&listings(), &permissions, false)
    }

    fn names(table: &TableDefinition) -> Vec<&str> {
        table.columns.iter().map(|c| c.name.as_str()).collect()
    }

    #[test]
    fn a_caller_without_the_grant_is_withheld_each_restricted_column() {
        let withheld = view_of(&["listings:view", "listings:edit", "orders:internal"]);
        assert_eq!(
            names(&withheld.named(&listings())),
            ["id", "customer_id", "title", "status"]
        );
        assert_eq!(
            withheld.shown(json!({"id": "1", "title": "t", "ops_notes": "n", "storage": "s"})),
            json!({"id": "1", "title": "t"})
        );
    }

    #[test]
    fn a_grant_of_the_operation_its_area_or_everything_withholds_nothing_of_it() {
        for granted in [
            vec!["listings:internal", "listings:manage"],
            vec!["listings:internal@all", "listings:manage@own"],
            vec!["listings:*"],
            vec!["*"],
        ] {
            assert_eq!(view_of(&granted), ColumnView::ALL, "{granted:?}");
        }
        let reads_only = view_of(&["listings:internal"]);
        assert_eq!(names(&reads_only.named(&listings())).len(), 6);
        assert!(reads_only.admit(&json!({"ops_notes": "n"})).is_ok());
        assert!(reads_only.admit(&json!({"status": "live"})).is_err());
    }

    #[test]
    fn a_service_account_is_withheld_nothing() {
        assert_eq!(view(&listings(), &[], true), ColumnView::ALL);
    }

    #[test]
    fn a_table_without_an_area_withholds_its_restricted_columns_from_every_grant() {
        let unareaed = TableDefinition {
            permission_area: None,
            ..listings()
        };
        let withheld = view(&unareaed, &["*".to_string()], false);
        assert_eq!(
            names(&withheld.named(&unareaed)),
            ["id", "customer_id", "title", "status"]
        );
        assert!(withheld.admit(&json!({"status": "live"})).is_err());
        assert_eq!(view(&unareaed, &[], true), ColumnView::ALL);
    }

    #[test]
    fn a_scoped_table_reads_its_column_grants_from_its_scope_area() {
        let scoped = TableDefinition {
            permission_area: None,
            scoped_by: Some("listings".into()),
            ..listings()
        };
        assert_ne!(view(&scoped, &[], false), ColumnView::ALL);
        assert_eq!(
            view(&scoped, &["listings:*@all".to_string()], false),
            ColumnView::ALL
        );
    }

    #[test]
    fn a_malformed_grant_grants_no_column() {
        assert_eq!(view_of(&["listings:internal@everyone"]), view_of(&[]));
    }

    #[test]
    fn a_write_that_sets_a_withheld_column_is_refused_by_its_permission() {
        let withheld = view_of(&[]);
        assert!(withheld.admit(&json!({"title": "t"})).is_ok());
        assert!(withheld.admit(&json!("not an object")).is_ok());
        for (body, permission, column) in [
            (
                json!({"title": "t", "ops_notes": null}),
                "listings:internal",
                "ops_notes",
            ),
            (json!({"status": "live"}), "listings:manage", "status"),
        ] {
            let refused = withheld.admit(&body).expect_err("refused").to_string();
            assert!(
                refused.contains(&format!(
                    "Missing permission: {permission} (to set '{column}')"
                )),
                "{refused}"
            );
        }
        assert!(withheld.admit(&json!({"storage": "s"})).is_ok());
    }

    #[test]
    fn nothing_withheld_borrows_the_table_and_leaves_rows_as_they_are() {
        let table = listings();
        let full = ColumnView::ALL;
        assert!(matches!(full.named(&table), Cow::Borrowed(_)));
        let row = json!({"ops_notes": "n", "unknown": 1});
        assert_eq!(full.shown(row.clone()), row);
        assert_eq!(view_of(&[]).shown(json!([1, 2])), json!([1, 2]));
    }
}
