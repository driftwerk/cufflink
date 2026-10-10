use crate::auth::binding::bind_principal;
use crate::auth::columns::{self, ColumnView};
use crate::auth::grants::{self, has_permission};
use crate::auth::permissions::{resolve_permissions_cached, ResolvedPermissions};
use crate::auth::scope::{self, EffectiveScope, RequestedScope, RowPredicate};
use crate::auth::AuthUser;
use crate::deployer::manifest::{ColumnDefinition, ServiceManifest, TableDefinition};
use crate::errors::{PlatformError, PlatformResult};
use crate::registry::RegisteredService;
use crate::router::bind::{self, SqlValue};
use crate::router::postures::{continued_call, ServiceCaller, Target};
use crate::router::query::SqlParams;
use crate::router::strict::split_svc_path;
use crate::tenant_catalog::TenantEntry;
use crate::AppState;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderMap, Method};
use cufflink_telemetry::Kind;
use serde_json::{json, Value};
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct DataTarget {
    method: Method,
    path: String,
}

impl<S: Send + Sync> FromRequestParts<S> for DataTarget {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Infallible> {
        let path = parts.uri.path();
        Ok(Self {
            method: parts.method.clone(),
            path: split_svc_path(path)
                .map_or(path, |(_, _, rest)| rest)
                .to_string(),
        })
    }
}

pub(crate) struct DataRoute {
    pub service: Arc<RegisteredService>,
    pub tenant: Arc<TenantEntry>,
    pub principal: Option<AuthUser>,
    pub call: Option<ServiceCaller>,
}

fn data_leaf<'a>(manifest: &ServiceManifest, path: &'a str) -> &'a str {
    path.split('/')
        .find(|segment| !segment.is_empty())
        .filter(|first| *first == "graphql" || manifest.get_table(first).is_some())
        .unwrap_or(":id")
}

pub(crate) async fn resolve_data_route(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    target: &DataTarget,
    tenant_slug: &str,
    service_name: &str,
    principal: Option<AuthUser>,
) -> PlatformResult<DataRoute> {
    let service = state
        .registry
        .get_by_slug(tenant_slug, service_name)
        .await
        .ok_or_else(|| PlatformError::NotFound(format!("Service '{}' not found", service_name)))?;
    crate::telemetry::resolve(
        &service.scope().of_kind(Kind::Crud),
        data_leaf(&service.manifest, &target.path),
    );
    let tenant = state
        .tenants
        .get_or_load(service.tenant_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound(format!("Service '{}' not found", service_name)))?;
    let call = continued_call(
        state,
        &service,
        headers,
        &Target {
            method: &target.method,
            path: &target.path,
        },
    )?;
    let principal = bind_principal(state, principal, &tenant, headers)?;
    Ok(DataRoute {
        service,
        tenant,
        principal,
        call,
    })
}

pub(crate) fn owner_param(
    columns: &[ColumnDefinition],
    owner_col: &str,
    subject: &str,
) -> Result<SqlValue, PlatformError> {
    let col = columns
        .iter()
        .find(|c| c.name == owner_col)
        .ok_or_else(|| {
            PlatformError::Internal(format!(
                "owner_field '{}' is not a declared column on the table",
                owner_col
            ))
        })?;
    bind::bind_column_value(col, &Value::String(subject.to_string())).map_err(|e| {
        PlatformError::Unauthorized(format!(
            "authenticated subject is not {} as owner_field '{}' requires",
            e.expected, owner_col
        ))
    })
}

pub(crate) fn enforce_auth_required(
    table: &TableDefinition,
    auth_user: &Option<AuthUser>,
) -> PlatformResult<()> {
    if (table.auth_required || table.owner_self_service) && auth_user.is_none() {
        Err(PlatformError::Unauthorized(
            "Authentication required for this resource".into(),
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn owner_self_service_authorized(table: &TableDefinition) -> bool {
    table.owner_self_service && table.owner_field.is_some()
}

pub(crate) fn grant_required<'a>(
    table: &'a TableDefinition,
    auth_user: &'a Option<AuthUser>,
) -> PlatformResult<Option<(&'a str, &'a AuthUser)>> {
    if owner_self_service_authorized(table) {
        return Ok(None);
    }
    let Some(area) = table.permission_area.as_deref() else {
        return Ok(None);
    };
    let user = auth_user.as_ref().ok_or_else(|| {
        PlatformError::Unauthorized("Authentication required for this resource".into())
    })?;
    Ok((!user.is_service_account).then_some((area, user)))
}

pub(crate) fn permission_decision(
    area: &str,
    operation: &str,
    resolved: &ResolvedPermissions,
) -> PlatformResult<()> {
    if has_permission(&resolved.permissions, area, operation) {
        Ok(())
    } else {
        Err(PlatformError::Forbidden(format!(
            "Missing permission: {}:{}",
            area, operation
        )))
    }
}

pub(crate) async fn authorize_table<'a, F, Fut>(
    table: &'a TableDefinition,
    operations: &[&str],
    principal: &'a Option<AuthUser>,
    resolve: F,
) -> PlatformResult<()>
where
    F: FnOnce(&'a AuthUser) -> Fut,
    Fut: Future<Output = PlatformResult<ResolvedPermissions>>,
{
    enforce_auth_required(table, principal)?;
    let Some((area, user)) = grant_required(table, principal)? else {
        return Ok(());
    };
    if operations.is_empty() {
        return Ok(());
    }
    let resolved = resolve(user).await?;
    tracing::debug!(
        user = %user.subject,
        area,
        ?operations,
        permissions = ?resolved.permissions,
        roles = ?resolved.role_names,
        "Permission check"
    );
    operations
        .iter()
        .try_for_each(|operation| permission_decision(area, operation, &resolved))
}

pub(crate) async fn authorize(
    state: &Arc<AppState>,
    service_tenant_id: Uuid,
    table: &TableDefinition,
    operations: &[&str],
    principal: &Option<AuthUser>,
) -> PlatformResult<()> {
    authorize_table(table, operations, principal, |user| {
        resolve_permissions_cached(state, service_tenant_id, &user.subject)
    })
    .await
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RowScope {
    Legacy {
        owner: Option<(String, Option<String>)>,
    },
    Scoped(EffectiveScope),
}

impl RowScope {
    pub(crate) fn legacy(table: &TableDefinition, principal: &Option<AuthUser>) -> Self {
        RowScope::Legacy {
            owner: table
                .owner_field
                .clone()
                .map(|column| (column, principal.as_ref().map(|user| user.subject.clone()))),
        }
    }

    pub(crate) fn condition(
        &self,
        table: &TableDefinition,
        params: &mut SqlParams,
    ) -> PlatformResult<Option<String>> {
        match self {
            RowScope::Legacy { owner: None } => Ok(None),
            RowScope::Legacy {
                owner: Some((column, subject)),
            } => {
                let subject = subject.as_deref().ok_or_else(|| {
                    PlatformError::Unauthorized("Authentication required for this resource".into())
                })?;
                let placeholder = params.push(owner_param(&table.columns, column, subject)?);
                Ok(Some(format!("{column} = {placeholder}")))
            }
            RowScope::Scoped(scope) => scope.where_sql(&table.columns, params),
        }
    }

    pub(crate) fn scoped(&self) -> Option<&EffectiveScope> {
        match self {
            RowScope::Scoped(scope) => Some(scope),
            RowScope::Legacy { .. } => None,
        }
    }

    pub(crate) fn confining(&self) -> Option<&EffectiveScope> {
        self.scoped().filter(|scope| **scope != EffectiveScope::All)
    }

    pub(crate) fn into_filter(self) -> EffectiveScope {
        match self {
            RowScope::Scoped(scope) => scope,
            RowScope::Legacy { owner: None } => EffectiveScope::All,
            RowScope::Legacy {
                owner: Some((column, Some(subject))),
            } => EffectiveScope::Rows(vec![RowPredicate::Owner { column, subject }]),
            RowScope::Legacy {
                owner: Some((_, None)),
            } => EffectiveScope::Denied,
        }
    }
}

async fn row_scopes(
    state: &Arc<AppState>,
    service_tenant_id: Uuid,
    table: &TableDefinition,
    operations: &[&str],
    principal: &Option<AuthUser>,
    requested: Option<RequestedScope>,
) -> PlatformResult<Vec<RowScope>> {
    if table.scoped_by.is_none() {
        authorize(state, service_tenant_id, table, operations, principal).await?;
        return Ok(operations
            .iter()
            .map(|_| RowScope::legacy(table, principal))
            .collect());
    }
    enforce_auth_required(table, principal)?;
    let mut scopes = Vec::with_capacity(operations.len());
    for operation in operations {
        scopes.push(RowScope::Scoped(
            scope::for_request(
                state,
                service_tenant_id,
                table,
                operation,
                principal,
                requested,
            )
            .await?,
        ));
    }
    Ok(scopes)
}

pub(crate) async fn authorize_rows(
    state: &Arc<AppState>,
    service_tenant_id: Uuid,
    table: &TableDefinition,
    operations: &[&str],
    principal: &Option<AuthUser>,
    requested: Option<RequestedScope>,
) -> PlatformResult<(Vec<RowScope>, ColumnView)> {
    let scopes = row_scopes(
        state,
        service_tenant_id,
        table,
        operations,
        principal,
        requested,
    )
    .await?;
    let columns = columns::for_request(state, service_tenant_id, table, principal).await?;
    Ok((scopes, columns))
}

#[derive(Debug)]
pub(crate) struct Access {
    pub rows: RowScope,
    pub columns: ColumnView,
}

pub(crate) async fn authorize_access(
    state: &Arc<AppState>,
    service_tenant_id: Uuid,
    table: &TableDefinition,
    operation: &str,
    principal: &Option<AuthUser>,
    requested: Option<RequestedScope>,
) -> PlatformResult<Access> {
    let (mut scopes, columns) = authorize_rows(
        state,
        service_tenant_id,
        table,
        &[operation],
        principal,
        requested,
    )
    .await?;
    scopes
        .pop()
        .map(|rows| Access { rows, columns })
        .ok_or_else(|| PlatformError::Internal("no row scope resolved".into()))
}

pub(crate) fn requested_scope(
    table: &TableDefinition,
    raw: Option<&str>,
) -> PlatformResult<Option<RequestedScope>> {
    match table.scoped_by {
        Some(_) => RequestedScope::parse(raw),
        None => Ok(None),
    }
}

pub(crate) async fn guest_auth_json(
    state: &Arc<AppState>,
    auth_user: &Option<AuthUser>,
    service_tenant_id: Uuid,
) -> Value {
    match auth_user {
        Some(user) => {
            let resolved = resolve_permissions_cached(state, service_tenant_id, &user.subject)
                .await
                .unwrap_or_else(|_| ResolvedPermissions::empty());
            authenticated_guest_auth(user, &resolved)
        }
        None => Value::Null,
    }
}

pub(crate) fn authenticated_guest_auth(user: &AuthUser, resolved: &ResolvedPermissions) -> Value {
    let grants::GuestPermissions {
        permissions,
        scoped,
    } = grants::guest_view(&resolved.permissions);
    let mut auth = user.to_auth_json();
    if let Some(obj) = auth.as_object_mut() {
        obj.insert("permissions".to_string(), json!(permissions));
        obj.insert("role_names".to_string(), json!(resolved.role_names));
        if !scoped.is_empty() {
            obj.insert("scoped_permissions".to_string(), json!(scoped));
        }
    }
    auth
}

#[cfg(test)]
mod owner_param_tests {
    use super::*;
    use crate::deployer::manifest::ColumnType;

    fn uuid_col(name: &str) -> ColumnDefinition {
        ColumnDefinition {
            name: name.to_string(),
            column_type: ColumnType::Uuid,
            primary_key: false,
            nullable: false,
            auto_generate: false,
            default_value: None,
            references: None,
            on_delete: None,
            unique: false,
            validations: vec![],
            ..Default::default()
        }
    }

    fn text_col(name: &str) -> ColumnDefinition {
        ColumnDefinition {
            column_type: ColumnType::Text,
            ..uuid_col(name)
        }
    }

    #[test]
    fn a_data_route_is_named_by_what_the_service_serves() {
        let manifest = ServiceManifest {
            tables: vec![TableDefinition {
                name: "notes".into(),
                ..Default::default()
            }],
            private_tables: vec![TableDefinition {
                name: "ledger".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        for (path, leaf) in [
            ("/notes", "notes"),
            ("/notes/97c9e5c1-3f4a-4d0f-a53a-2273c4be3aad", "notes"),
            ("/notes/stream", "notes"),
            ("/graphql", "graphql"),
            ("/graphql/schema", "graphql"),
            ("/ledger", ":id"),
            ("/zzz-9f3c", ":id"),
            ("/", ":id"),
            ("", ":id"),
        ] {
            assert_eq!(data_leaf(&manifest, path), leaf, "{path}");
        }
    }

    #[test]
    fn uuid_subject_binds_as_uuid_for_uuid_column() {
        let cols = vec![uuid_col("customer_id")];
        let result = owner_param(&cols, "customer_id", "1245dc39-fc68-48b2-8e94-60a8eb0b83f5");
        assert!(result.is_ok());
    }

    #[test]
    fn non_uuid_subject_rejected_for_uuid_column() {
        let cols = vec![uuid_col("customer_id")];
        let err = owner_param(&cols, "customer_id", "google-oauth2|abc123").unwrap_err();
        assert!(matches!(err, PlatformError::Unauthorized(_)));
    }

    #[test]
    fn text_column_accepts_any_subject() {
        let cols = vec![text_col("owner")];
        assert!(owner_param(&cols, "owner", "google-oauth2|abc123").is_ok());
        assert!(owner_param(&cols, "owner", "1245dc39-fc68-48b2-8e94-60a8eb0b83f5").is_ok());
    }

    #[test]
    fn unknown_owner_column_errors() {
        let cols = vec![uuid_col("customer_id")];
        let err = owner_param(&cols, "not_a_column", "anything").unwrap_err();
        assert!(matches!(err, PlatformError::Internal(_)));
    }
}

#[cfg(test)]
mod owner_self_service_tests {
    use super::*;

    fn table(
        owner_field: Option<&str>,
        owner_self_service: bool,
        auth_required: bool,
    ) -> TableDefinition {
        TableDefinition {
            name: "items".into(),
            columns: vec![],
            indexes: vec![],
            soft_delete: false,
            owner_field: owner_field.map(String::from),
            auth_required,
            permission_area: Some("items".into()),
            owner_self_service,
            hooks: None,
            scoped_by: None,
            ..Default::default()
        }
    }

    fn user() -> Option<crate::auth::AuthUser> {
        Some(crate::auth::AuthUser {
            subject: "c1".into(),
            name: None,
            email: None,
            tenant_id: Uuid::nil(),
            tenant_slug: "default".into(),
            auth_method: crate::auth::extractor::AuthMethod::Jwt,
            token_info: None,
            is_service_account: false,
            is_platform_admin: false,
            has_deploy_authority: false,
            realm: None,
        })
    }

    #[test]
    fn self_service_with_owner_field_authorizes() {
        assert!(owner_self_service_authorized(&table(
            Some("customer_id"),
            true,
            true
        )));
    }

    #[test]
    fn self_service_without_owner_field_does_not_authorize() {
        assert!(!owner_self_service_authorized(&table(None, true, true)));
    }

    #[test]
    fn non_self_service_does_not_authorize() {
        assert!(!owner_self_service_authorized(&table(
            Some("customer_id"),
            false,
            true
        )));
    }

    #[test]
    fn self_service_implies_authentication() {
        let t = table(Some("customer_id"), true, false);
        assert!(enforce_auth_required(&t, &None).is_err());
        assert!(enforce_auth_required(&t, &user()).is_ok());
    }
}
