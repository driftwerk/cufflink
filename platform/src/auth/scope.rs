use crate::auth::grants::{self, Grant, Scope};
use crate::auth::permissions::resolve_permissions_cached;
use crate::auth::AuthUser;
use crate::deployer::manifest::{ColumnDefinition, TableDefinition};
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::record::{LogThrottle, LOG_THROTTLE};
use crate::router::bind;
use crate::router::query::SqlParams;
use crate::router::row_convert::manifest_ident;
use crate::AppState;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use uuid::Uuid;

pub(crate) const FEATURES: &[&str] = &[cufflink_types::features::SCOPED_PERMISSIONS];

#[derive(Debug, Clone, PartialEq)]
pub enum RowPredicate {
    Owner { column: String, subject: String },
    Claim { column: String, values: Vec<Value> },
    And(Box<RowPredicate>, Vec<RowPredicate>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum EffectiveScope {
    All,
    Rows(Vec<RowPredicate>),
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestedScope {
    Own,
    All,
}

impl RequestedScope {
    pub fn parse(raw: Option<&str>) -> PlatformResult<Option<Self>> {
        match raw {
            None => Ok(None),
            Some("own") => Ok(Some(Self::Own)),
            Some("all") => Ok(Some(Self::All)),
            Some(other) => Err(PlatformError::BadRequest(format!(
                "scope must be 'own' or 'all', not '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeError {
    Widen,
    OwnUnavailable,
}

#[derive(Debug, Clone, Copy)]
pub struct Target<'a> {
    pub area: &'a str,
    pub owner_column: Option<&'a str>,
    pub columns: Option<&'a [ColumnDefinition]>,
    pub self_service: bool,
}

impl<'a> Target<'a> {
    pub fn table(table: &'a TableDefinition) -> Option<Self> {
        table.scoped_by.as_deref().map(|area| Self {
            area,
            owner_column: table.owner_field.as_deref(),
            columns: Some(&table.columns),
            self_service: table.owner_self_service,
        })
    }

    fn knows(&self, column: &str) -> bool {
        self.columns
            .is_none_or(|columns| columns.iter().any(|c| c.name == column))
    }

    pub fn owner(&self, subject: &str) -> Option<RowPredicate> {
        self.owner_column.map(|column| RowPredicate::Owner {
            column: column.to_string(),
            subject: subject.to_string(),
        })
    }
}

pub struct ScopeInput<'a> {
    pub target: Target<'a>,
    pub op: &'a str,
    pub grants: &'a [Grant<'a>],
    pub subject: &'a str,
    pub claims: &'a HashMap<String, Value>,
    pub is_service_account: bool,
}

static UNKNOWN_COLUMNS: LazyLock<LogThrottle<(String, String)>> =
    LazyLock::new(|| LogThrottle::new(LOG_THROTTLE));
static MALFORMED_GRANTS: LazyLock<LogThrottle<String>> =
    LazyLock::new(|| LogThrottle::new(LOG_THROTTLE));

fn claim_values(claim: Option<&Value>) -> Vec<Value> {
    match claim {
        Some(v @ (Value::String(_) | Value::Number(_))) => vec![v.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .filter(|v| v.is_string() || v.is_number())
            .cloned()
            .collect(),
        _ => vec![],
    }
}

pub fn effective(input: &ScopeInput) -> EffectiveScope {
    if input.is_service_account {
        return EffectiveScope::All;
    }
    let target = &input.target;
    let matching: Vec<&Grant> = input
        .grants
        .iter()
        .filter(|g| g.covers(target.area, input.op))
        .collect();
    if matching.iter().any(|g| g.scope == Scope::All) {
        return EffectiveScope::All;
    }
    let predicates = matching
        .iter()
        .filter_map(|g| match &g.scope {
            Scope::Own => target.owner(input.subject),
            Scope::Field { column, claim } if target.knows(column) => Some(RowPredicate::Claim {
                column: column.clone(),
                values: claim_values(grants::claim(input.claims, claim)),
            }),
            Scope::Field { column, .. } => {
                if UNKNOWN_COLUMNS
                    .should_log((target.area.to_string(), column.clone()), Instant::now())
                {
                    tracing::warn!(
                        target: "cufflink::scope",
                        area = target.area,
                        column,
                        "A field grant names a column the table does not have; it grants nothing here"
                    );
                }
                None
            }
            Scope::All => None,
        })
        .fold(Vec::new(), |mut unique, p| {
            if !unique.contains(&p) {
                unique.push(p);
            }
            unique
        });
    match (predicates.is_empty(), target.self_service) {
        (false, _) => EffectiveScope::Rows(predicates),
        (true, true) => target
            .owner(input.subject)
            .map_or(EffectiveScope::Denied, |owner| {
                EffectiveScope::Rows(vec![owner])
            }),
        (true, false) => EffectiveScope::Denied,
    }
}

pub fn narrow(
    effective: EffectiveScope,
    requested: Option<RequestedScope>,
    owner: Option<RowPredicate>,
) -> Result<EffectiveScope, ScopeError> {
    match (requested, effective) {
        (None, e) | (Some(_), e @ EffectiveScope::Denied) => Ok(e),
        (Some(RequestedScope::All), EffectiveScope::All) => Ok(EffectiveScope::All),
        (Some(RequestedScope::All), EffectiveScope::Rows(_)) => Err(ScopeError::Widen),
        (Some(RequestedScope::Own), e) => {
            let owner = owner.ok_or(ScopeError::OwnUnavailable)?;
            Ok(EffectiveScope::Rows(vec![match e {
                EffectiveScope::Rows(ps) if !ps.contains(&owner) => {
                    RowPredicate::And(Box::new(owner), ps)
                }
                _ => owner,
            }]))
        }
    }
}

pub fn resolve(
    input: &ScopeInput,
    requested: Option<RequestedScope>,
) -> Result<EffectiveScope, ScopeError> {
    narrow(
        effective(input),
        requested,
        input.target.owner(input.subject),
    )
}

pub fn answer(
    area: &str,
    op: &str,
    resolved: Result<EffectiveScope, ScopeError>,
) -> PlatformResult<EffectiveScope> {
    match resolved {
        Ok(EffectiveScope::Denied) => Err(PlatformError::Forbidden(format!(
            "Missing permission: {area}:{op}"
        ))),
        Ok(scope) => Ok(scope),
        Err(ScopeError::Widen) => Err(PlatformError::Forbidden(format!(
            "scope=all not granted for {area}:{op}"
        ))),
        Err(ScopeError::OwnUnavailable) => Err(PlatformError::BadRequest(format!(
            "scope=own is not available for {area}"
        ))),
    }
}

fn column<'c>(columns: &'c [ColumnDefinition], name: &str) -> PlatformResult<&'c ColumnDefinition> {
    columns
        .iter()
        .find(|c| c.name == name)
        .ok_or_else(|| PlatformError::Internal(format!("scope column '{name}' is not declared")))
}

impl RowPredicate {
    pub fn to_sql(
        &self,
        columns: &[ColumnDefinition],
        params: &mut SqlParams,
    ) -> PlatformResult<String> {
        match self {
            RowPredicate::Owner {
                column: name,
                subject,
            } => {
                let col = column(columns, name)?;
                Ok(
                    match bind::bind_column_value(col, &Value::String(subject.clone())) {
                        Ok(value) => format!("{} = {}", manifest_ident(name), params.push(value)),
                        Err(_) => "FALSE".to_string(),
                    },
                )
            }
            RowPredicate::Claim {
                column: name,
                values,
            } => {
                let col = column(columns, name)?;
                Ok(match bind::bind_column_array(col, values) {
                    Some(list) => {
                        format!("{} = ANY({})", manifest_ident(name), params.push(list))
                    }
                    None => "FALSE".to_string(),
                })
            }
            RowPredicate::And(first, rest) => Ok(format!(
                "({} AND {})",
                first.to_sql(columns, params)?,
                any_sql(rest, columns, params)?
            )),
        }
    }

    pub fn matches(&self, row: &Value, columns: &[ColumnDefinition]) -> bool {
        let same = |name: &str, value: &Value| {
            columns
                .iter()
                .find(|c| c.name == name)
                .zip(row.get(name))
                .is_some_and(|(col, cell)| bind::same_value(&col.column_type, cell, value))
        };
        match self {
            RowPredicate::Owner { column, subject } => {
                same(column, &Value::String(subject.clone()))
            }
            RowPredicate::Claim { column, values } => values.iter().any(|v| same(column, v)),
            RowPredicate::And(first, rest) => {
                first.matches(row, columns) && rest.iter().any(|p| p.matches(row, columns))
            }
        }
    }

    fn typed_for(self, columns: &[ColumnDefinition]) -> Self {
        let column_type = |name: &str| {
            columns
                .iter()
                .find(|c| c.name == name)
                .map(|c| c.column_type.clone())
        };
        match self {
            RowPredicate::Owner { column, subject } => match column_type(&column) {
                Some(ty) if bind::typed(&ty, &Value::String(subject.clone())).is_none() => {
                    RowPredicate::Claim {
                        column,
                        values: Vec::new(),
                    }
                }
                _ => RowPredicate::Owner { column, subject },
            },
            RowPredicate::Claim { column, values } => match column_type(&column) {
                Some(ty) => RowPredicate::Claim {
                    values: values
                        .iter()
                        .filter_map(|v| bind::typed(&ty, v).map(bind::Typed::into_json))
                        .collect(),
                    column,
                },
                None => RowPredicate::Claim { column, values },
            },
            RowPredicate::And(first, rest) => RowPredicate::And(
                Box::new(first.typed_for(columns)),
                rest.into_iter().map(|p| p.typed_for(columns)).collect(),
            ),
        }
    }

    fn to_json(&self) -> Value {
        match self {
            RowPredicate::Owner { column, subject } => {
                json!({"owner": {"column": column, "subject": subject}})
            }
            RowPredicate::Claim { column, values } => {
                json!({"claim": {"column": column, "values": values}})
            }
            RowPredicate::And(first, rest) => json!({"and": [
                first.to_json(),
                {"or": rest.iter().map(RowPredicate::to_json).collect::<Vec<_>>()},
            ]}),
        }
    }
}

fn any_sql(
    predicates: &[RowPredicate],
    columns: &[ColumnDefinition],
    params: &mut SqlParams,
) -> PlatformResult<String> {
    if predicates.is_empty() {
        return Ok("FALSE".to_string());
    }
    predicates
        .iter()
        .map(|p| p.to_sql(columns, params))
        .collect::<PlatformResult<Vec<_>>>()
        .map(|sql| format!("({})", sql.join(" OR ")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfScope;

impl From<OutOfScope> for PlatformError {
    fn from(_: OutOfScope) -> Self {
        PlatformError::Forbidden("The row is outside the caller's scope".into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    AsIs,
    ForceOwner { column: String, subject: String },
}

impl EffectiveScope {
    pub fn where_sql(
        &self,
        columns: &[ColumnDefinition],
        params: &mut SqlParams,
    ) -> PlatformResult<Option<String>> {
        match self {
            EffectiveScope::All => Ok(None),
            EffectiveScope::Rows(predicates) => any_sql(predicates, columns, params).map(Some),
            EffectiveScope::Denied => Ok(Some("FALSE".to_string())),
        }
    }

    pub fn matches(&self, row: &Value, columns: &[ColumnDefinition]) -> bool {
        match self {
            EffectiveScope::All => true,
            EffectiveScope::Rows(predicates) => predicates.iter().any(|p| p.matches(row, columns)),
            EffectiveScope::Denied => false,
        }
    }

    pub fn place(
        &self,
        row: &mut Map<String, Value>,
        columns: &[ColumnDefinition],
    ) -> Result<Placement, OutOfScope> {
        match self {
            EffectiveScope::All => Ok(Placement::AsIs),
            EffectiveScope::Rows(predicates) => match predicates.as_slice() {
                [RowPredicate::Owner { column, subject }] => Ok(Placement::ForceOwner {
                    column: column.clone(),
                    subject: subject.clone(),
                }),
                [RowPredicate::Claim { column, values }]
                    if values.len() == 1 && row.get(column).is_none_or(Value::is_null) =>
                {
                    row.insert(column.clone(), values[0].clone());
                    Ok(Placement::AsIs)
                }
                _ if self.matches(&Value::Object(row.clone()), columns) => Ok(Placement::AsIs),
                _ => Err(OutOfScope),
            },
            EffectiveScope::Denied => Err(OutOfScope),
        }
    }

    pub fn typed_for(self, columns: &[ColumnDefinition]) -> Self {
        match self {
            EffectiveScope::Rows(predicates) => EffectiveScope::Rows(
                predicates
                    .into_iter()
                    .map(|p| p.typed_for(columns))
                    .collect(),
            ),
            other => other,
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            EffectiveScope::All => json!({"kind": "all"}),
            EffectiveScope::Rows(predicates) => json!({
                "kind": "rows",
                "predicates": predicates.iter().map(RowPredicate::to_json).collect::<Vec<_>>(),
            }),
            EffectiveScope::Denied => json!({"kind": "denied"}),
        }
    }

    pub fn audit_kind(&self) -> &'static str {
        match self {
            EffectiveScope::All => "all",
            EffectiveScope::Rows(predicates)
                if predicates
                    .iter()
                    .any(|p| matches!(p, RowPredicate::Claim { .. })) =>
            {
                "field"
            }
            EffectiveScope::Rows(_) => "own",
            EffectiveScope::Denied => "denied",
        }
    }
}

fn decision_label(resolved: &Result<EffectiveScope, ScopeError>) -> &'static str {
    match resolved {
        Ok(EffectiveScope::All) => "all",
        Ok(EffectiveScope::Rows(_)) => "rows",
        Ok(EffectiveScope::Denied) => "denied",
        Err(ScopeError::Widen) => "widen",
        Err(ScopeError::OwnUnavailable) => "invalid",
    }
}

pub fn principal_claims(user: &AuthUser) -> HashMap<String, Value> {
    let standard = [
        ("sub", Some(user.subject.clone())),
        ("email", user.email.clone()),
        (
            "preferred_username",
            user.token_info
                .as_ref()
                .and_then(|t| t.preferred_username.clone()),
        ),
    ];
    let mut claims: HashMap<String, Value> = user
        .token_info
        .as_ref()
        .map(|t| t.claims.clone())
        .unwrap_or_default();
    for (name, value) in standard {
        if let Some(value) = value {
            claims
                .entry(name.to_string())
                .or_insert(Value::String(value));
        }
    }
    claims
}

fn parsed_grants(permissions: &[String]) -> Vec<Grant<'_>> {
    permissions
        .iter()
        .filter_map(|p| match grants::parse(p) {
            Ok(grant) => Some(grant),
            Err(e) => {
                if MALFORMED_GRANTS.should_log(p.clone(), Instant::now()) {
                    tracing::warn!(target: "cufflink::scope", permission = %p, error = %e, "Ignoring a permission with a malformed scope");
                }
                None
            }
        })
        .collect()
}

pub(crate) async fn for_request(
    state: &Arc<AppState>,
    service_tenant_id: Uuid,
    table: &TableDefinition,
    op: &str,
    principal: &Option<AuthUser>,
    requested: Option<RequestedScope>,
) -> PlatformResult<EffectiveScope> {
    let target = Target::table(table)
        .ok_or_else(|| PlatformError::Internal(format!("table '{}' is not scoped", table.name)))?;
    for_target(
        state,
        service_tenant_id,
        target,
        &table.name,
        op,
        principal,
        requested,
    )
    .await
}

pub(crate) async fn for_target(
    state: &Arc<AppState>,
    service_tenant_id: Uuid,
    target: Target<'_>,
    label: &str,
    op: &str,
    principal: &Option<AuthUser>,
    requested: Option<RequestedScope>,
) -> PlatformResult<EffectiveScope> {
    let user = principal.as_ref().ok_or_else(|| {
        PlatformError::Unauthorized("Authentication required for this resource".into())
    })?;
    let permissions = if user.is_service_account {
        vec![]
    } else {
        resolve_permissions_cached(state, service_tenant_id, &user.subject)
            .await?
            .permissions
    };
    let grants = parsed_grants(&permissions);
    let claims = principal_claims(user);
    let resolved = resolve(
        &ScopeInput {
            target,
            op,
            grants: &grants,
            subject: &user.subject,
            claims: &claims,
            is_service_account: user.is_service_account,
        },
        requested,
    );
    metrics::counter!(
        "cufflink_scope_decisions_total",
        "table" => label.to_string(),
        "op" => op.to_string(),
        "scope" => decision_label(&resolved),
    )
    .increment(1);
    tracing::debug!(target: "cufflink::scope", subject = %user.subject, target = label, op, effective = ?resolved, "Scope decision");
    answer(target.area, op, resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORG_A: &str = "0f8fad5b-d9cb-469f-a165-70867728950e";
    const ORG_B: &str = "5c3b5f1e-0000-4000-8000-000000000000";
    const USER: &str = "7d5bd1a8-4ab6-4b25-9a2e-7a3a3b7a9d11";

    fn books() -> TableDefinition {
        serde_json::from_value(json!({
            "name": "books",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true},
                {"name": "user_id", "column_type": "uuid"},
                {"name": "branch_id", "column_type": "uuid"},
                {"name": "region", "column_type": "text"},
                {"name": "rank", "column_type": "integer"}
            ],
            "owner_field": "user_id",
            "scoped_by": "books"
        }))
        .expect("table")
    }

    fn claims() -> HashMap<String, Value> {
        HashMap::from([
            ("branch_id".to_string(), json!(ORG_A)),
            ("regions".to_string(), json!(["north", 7, null, {"x": 1}])),
            ("profile".to_string(), json!({"org": ORG_A})),
        ])
    }

    fn scope_of(table: &TableDefinition, permissions: &[&str], op: &str) -> EffectiveScope {
        let permissions: Vec<String> = permissions.iter().map(|p| p.to_string()).collect();
        let grants = parsed_grants(&permissions);
        let claims = claims();
        effective(&ScopeInput {
            target: Target::table(table).expect("scoped"),
            op,
            grants: &grants,
            subject: USER,
            claims: &claims,
            is_service_account: false,
        })
    }

    fn owner() -> RowPredicate {
        RowPredicate::Owner {
            column: "user_id".into(),
            subject: USER.into(),
        }
    }

    fn org(values: &[&str]) -> RowPredicate {
        RowPredicate::Claim {
            column: "branch_id".into(),
            values: values.iter().map(|v| json!(v)).collect(),
        }
    }

    #[test]
    fn effective_scope_from_grants() {
        let table = books();
        let cases: Vec<(&[&str], EffectiveScope)> = vec![
            (&["books:view@all", "books:view@own"], EffectiveScope::All),
            (&["*@all"], EffectiveScope::All),
            (&["books:view"], EffectiveScope::Rows(vec![owner()])),
            (
                &["books:view@own", "books:*"],
                EffectiveScope::Rows(vec![owner()]),
            ),
            (
                &["books:view@own", "books:view@field(branch_id=branch_id)"],
                EffectiveScope::Rows(vec![owner(), org(&[ORG_A])]),
            ),
            (
                &["books:view@field(region=regions)"],
                EffectiveScope::Rows(vec![RowPredicate::Claim {
                    column: "region".into(),
                    values: vec![json!("north"), json!(7)],
                }]),
            ),
            (
                &["books:view@field(branch_id=missing)"],
                EffectiveScope::Rows(vec![org(&[])]),
            ),
            (
                &["books:view@field(branch_id=profile)"],
                EffectiveScope::Rows(vec![org(&[])]),
            ),
            (
                &["books:view@field(branch_id=profile.org)"],
                EffectiveScope::Rows(vec![org(&[ORG_A])]),
            ),
            (
                &["books:view@field(branch_id=profile.team)"],
                EffectiveScope::Rows(vec![org(&[])]),
            ),
            (
                &["books:view@field(ghost=branch_id)"],
                EffectiveScope::Denied,
            ),
            (
                &["books:edit@all", "receipts:view@all"],
                EffectiveScope::Denied,
            ),
            (&["books:view@broken"], EffectiveScope::Denied),
            (&[], EffectiveScope::Denied),
        ];
        for (permissions, expected) in cases {
            assert_eq!(
                scope_of(&table, permissions, "view"),
                expected,
                "{permissions:?}"
            );
        }
    }

    #[test]
    fn service_accounts_and_self_service_owners() {
        let table = books();
        let claims = claims();
        let input = |is_service_account| ScopeInput {
            target: Target::table(&table).expect("scoped"),
            op: "delete",
            grants: &[],
            subject: USER,
            claims: &claims,
            is_service_account,
        };
        assert_eq!(effective(&input(true)), EffectiveScope::All);
        assert_eq!(effective(&input(false)), EffectiveScope::Denied);

        let self_service = TableDefinition {
            owner_self_service: true,
            ..books()
        };
        assert_eq!(
            scope_of(&self_service, &[], "view"),
            EffectiveScope::Rows(vec![owner()])
        );
        assert!(Target::table(&TableDefinition {
            scoped_by: None,
            ..books()
        })
        .is_none());
    }

    #[test]
    fn a_route_target_keeps_field_grants_and_needs_an_owner_column_for_own() {
        let claims = claims();
        let scope = |permissions: &[&str], owner_column| {
            let permissions: Vec<String> = permissions.iter().map(|p| p.to_string()).collect();
            let grants = parsed_grants(&permissions);
            effective(&ScopeInput {
                target: Target {
                    area: "books",
                    owner_column,
                    columns: None,
                    self_service: false,
                },
                op: "view",
                grants: &grants,
                subject: USER,
                claims: &claims,
                is_service_account: false,
            })
        };
        assert_eq!(
            scope(
                &["books:view@own", "books:view@field(any_column=branch_id)"],
                None
            ),
            EffectiveScope::Rows(vec![RowPredicate::Claim {
                column: "any_column".into(),
                values: vec![json!(ORG_A)],
            }])
        );
        assert_eq!(scope(&["books:view@own"], None), EffectiveScope::Denied);
        assert_eq!(
            scope(&["books:view"], Some("user_id")),
            EffectiveScope::Rows(vec![owner()])
        );
    }

    #[test]
    fn narrowing_never_widens() {
        let rows = EffectiveScope::Rows(vec![org(&[ORG_A])]);
        let cases = [
            (EffectiveScope::All, None, Ok(EffectiveScope::All)),
            (
                EffectiveScope::All,
                Some(RequestedScope::All),
                Ok(EffectiveScope::All),
            ),
            (
                EffectiveScope::All,
                Some(RequestedScope::Own),
                Ok(EffectiveScope::Rows(vec![owner()])),
            ),
            (
                rows.clone(),
                Some(RequestedScope::All),
                Err(ScopeError::Widen),
            ),
            (
                rows.clone(),
                Some(RequestedScope::Own),
                Ok(EffectiveScope::Rows(vec![RowPredicate::And(
                    Box::new(owner()),
                    vec![org(&[ORG_A])],
                )])),
            ),
            (
                EffectiveScope::Rows(vec![owner(), org(&[ORG_A])]),
                Some(RequestedScope::Own),
                Ok(EffectiveScope::Rows(vec![owner()])),
            ),
            (
                EffectiveScope::Denied,
                Some(RequestedScope::All),
                Ok(EffectiveScope::Denied),
            ),
        ];
        for (effective, requested, expected) in cases {
            assert_eq!(
                narrow(effective.clone(), requested, Some(owner())),
                expected,
                "{effective:?} {requested:?}"
            );
        }
        assert_eq!(
            narrow(EffectiveScope::All, Some(RequestedScope::Own), None),
            Err(ScopeError::OwnUnavailable)
        );
    }

    #[test]
    fn answers_are_the_route_errors() {
        let refused = |resolved| answer("books", "view", resolved).map_err(|e| e.to_string());
        assert_eq!(
            refused(Ok(EffectiveScope::Denied)),
            Err("Forbidden: Missing permission: books:view".into())
        );
        assert_eq!(
            refused(Err(ScopeError::Widen)),
            Err("Forbidden: scope=all not granted for books:view".into())
        );
        assert_eq!(
            refused(Err(ScopeError::OwnUnavailable)),
            Err("Bad request: scope=own is not available for books".into())
        );
        assert_eq!(refused(Ok(EffectiveScope::All)), Ok(EffectiveScope::All));
        assert!(RequestedScope::parse(Some("everything")).is_err());
        assert_eq!(
            RequestedScope::parse(Some("own")).expect("own"),
            Some(RequestedScope::Own)
        );
    }

    #[test]
    fn predicates_render_as_bound_sql() {
        let table = books();
        let render = |scope: EffectiveScope| {
            let mut params = SqlParams::default();
            let sql = scope.where_sql(&table.columns, &mut params).expect("sql");
            (sql, format!("{params:?}"))
        };
        assert_eq!(render(EffectiveScope::All), (None, "[]".into()));
        assert_eq!(
            render(EffectiveScope::Denied),
            (Some("FALSE".into()), "[]".into())
        );
        assert_eq!(
            render(EffectiveScope::Rows(vec![
                owner(),
                org(&[ORG_A, "not-a-uuid"])
            ])),
            (
                Some(r#"("user_id" = $1 OR "branch_id" = ANY($2))"#.into()),
                format!("[{USER}, [{ORG_A}]]")
            )
        );
        assert_eq!(
            render(EffectiveScope::Rows(vec![RowPredicate::And(
                Box::new(owner()),
                vec![
                    org(&[]),
                    RowPredicate::Claim {
                        column: "rank".into(),
                        values: vec![json!("3"), json!(4)],
                    }
                ],
            )])),
            (
                Some(
                    r#"(("user_id" = $1 AND ("branch_id" = ANY($2) OR "rank" = ANY($3))))"#.into()
                ),
                format!("[{USER}, [], [3, 4]]")
            )
        );
        let text_subject = EffectiveScope::Rows(vec![RowPredicate::Owner {
            column: "user_id".into(),
            subject: "google-oauth2|x".into(),
        }]);
        assert_eq!(
            render(text_subject),
            (Some("(FALSE)".into()), "[]".into()),
            "a subject that is not the owner column's type owns nothing"
        );
    }

    #[test]
    fn empty_scopes_render_as_no_row() {
        let mut params = SqlParams::default();
        assert_eq!(
            EffectiveScope::Rows(Vec::new())
                .where_sql(&books().columns, &mut params)
                .expect("sql"),
            Some("FALSE".into())
        );
    }

    #[test]
    fn guest_scopes_are_typed_as_their_columns() {
        let table = books();
        let rank = |values: Value| RowPredicate::Claim {
            column: "rank".into(),
            values: values.as_array().cloned().unwrap_or_default(),
        };
        let typed = EffectiveScope::Rows(vec![
            rank(json!(["3", 4, "four", 1.5])),
            org(&[&ORG_A.to_uppercase(), "not-a-uuid"]),
            RowPredicate::Owner {
                column: "user_id".into(),
                subject: "google-oauth2|x".into(),
            },
            RowPredicate::Claim {
                column: "undeclared".into(),
                values: vec![json!("kept")],
            },
            RowPredicate::And(Box::new(owner()), vec![rank(json!(["5"]))]),
        ])
        .typed_for(&table.columns);
        assert_eq!(
            typed.to_json(),
            json!({"kind": "rows", "predicates": [
                {"claim": {"column": "rank", "values": [3, 4]}},
                {"claim": {"column": "branch_id", "values": [ORG_A]}},
                {"claim": {"column": "user_id", "values": []}},
                {"claim": {"column": "undeclared", "values": ["kept"]}},
                {"and": [{"owner": {"column": "user_id", "subject": USER}},
                    {"or": [{"claim": {"column": "rank", "values": [5]}}]}]},
            ]})
        );
        assert_eq!(
            EffectiveScope::All.typed_for(&table.columns),
            EffectiveScope::All
        );
    }

    #[test]
    fn rows_match_as_the_sql_compares() {
        let table = books();
        let row = |user: &str, org: &str| json!({"id": ORG_B, "user_id": user, "branch_id": org, "rank": 3});
        let branch = EffectiveScope::Rows(vec![org(&[ORG_A])]);
        assert!(branch.matches(&row(ORG_B, &ORG_A.to_uppercase()), &table.columns));
        assert!(!branch.matches(&row(ORG_B, ORG_B), &table.columns));
        let mine = EffectiveScope::Rows(vec![owner()]);
        assert!(mine.matches(&row(USER, ORG_B), &table.columns));
        assert!(!mine.matches(&json!({"branch_id": ORG_A}), &table.columns));
        let ranked = RowPredicate::Claim {
            column: "rank".into(),
            values: vec![json!("3")],
        };
        assert!(ranked.matches(&row(USER, ORG_A), &table.columns));
        let narrowed = RowPredicate::And(Box::new(owner()), vec![org(&[ORG_A])]);
        assert!(narrowed.matches(&row(USER, ORG_A), &table.columns));
        assert!(!narrowed.matches(&row(USER, ORG_B), &table.columns));
        assert!(!narrowed.matches(&row(ORG_B, ORG_A), &table.columns));
        assert!(EffectiveScope::All.matches(&Value::Null, &table.columns));
        assert!(!EffectiveScope::Denied.matches(&row(USER, ORG_A), &table.columns));
    }

    #[test]
    fn creates_are_placed_inside_the_scope() {
        let table = books();
        let place = |scope: &EffectiveScope, row: Value| {
            let mut row = row.as_object().cloned().expect("object");
            scope
                .place(&mut row, &table.columns)
                .map(|placement| (placement, Value::Object(row)))
        };
        assert_eq!(
            place(&EffectiveScope::All, json!({"region": "x"})),
            Ok((Placement::AsIs, json!({"region": "x"})))
        );
        assert_eq!(
            place(
                &EffectiveScope::Rows(vec![owner()]),
                json!({"user_id": ORG_B})
            ),
            Ok((
                Placement::ForceOwner {
                    column: "user_id".into(),
                    subject: USER.into()
                },
                json!({"user_id": ORG_B})
            ))
        );
        let branch = EffectiveScope::Rows(vec![org(&[ORG_A])]);
        assert_eq!(
            place(&branch, json!({"region": "x"})),
            Ok((Placement::AsIs, json!({"region": "x", "branch_id": ORG_A})))
        );
        assert_eq!(
            place(&branch, json!({"branch_id": ORG_A.to_uppercase()})),
            Ok((Placement::AsIs, json!({"branch_id": ORG_A.to_uppercase()})))
        );
        assert_eq!(place(&branch, json!({"branch_id": ORG_B})), Err(OutOfScope));
        let either = EffectiveScope::Rows(vec![owner(), org(&[ORG_A])]);
        assert_eq!(place(&either, json!({"region": "x"})), Err(OutOfScope));
        assert_eq!(
            place(&either, json!({"user_id": USER})),
            Ok((Placement::AsIs, json!({"user_id": USER})))
        );
        let no_claim = EffectiveScope::Rows(vec![org(&[])]);
        assert_eq!(place(&no_claim, json!({})), Err(OutOfScope));
        assert_eq!(place(&EffectiveScope::Denied, json!({})), Err(OutOfScope));
    }

    #[test]
    fn guests_and_audit_see_the_scope() {
        assert_eq!(EffectiveScope::All.to_json(), json!({"kind": "all"}));
        assert_eq!(
            EffectiveScope::Rows(vec![
                RowPredicate::And(Box::new(owner()), vec![org(&[ORG_A])]),
                org(&[ORG_B]),
            ])
            .to_json(),
            json!({"kind": "rows", "predicates": [
                {"and": [
                    {"owner": {"column": "user_id", "subject": USER}},
                    {"or": [{"claim": {"column": "branch_id", "values": [ORG_A]}}]}
                ]},
                {"claim": {"column": "branch_id", "values": [ORG_B]}}
            ]})
        );
        assert_eq!(EffectiveScope::All.audit_kind(), "all");
        assert_eq!(EffectiveScope::Rows(vec![owner()]).audit_kind(), "own");
        assert_eq!(
            EffectiveScope::Rows(vec![owner(), org(&[])]).audit_kind(),
            "field"
        );
        assert_eq!(
            EffectiveScope::Rows(vec![RowPredicate::And(Box::new(owner()), vec![org(&[])])])
                .audit_kind(),
            "own"
        );
    }

    #[test]
    fn claims_include_the_token_and_standard_fields() {
        let user = crate::auth::AuthUser {
            subject: "kc-1".into(),
            name: None,
            email: Some("ada@example.com".into()),
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            auth_method: crate::auth::extractor::AuthMethod::Jwt,
            token_info: Some(crate::auth::keycloak::TokenInfo {
                sub: "kc-1".into(),
                preferred_username: Some("ada".into()),
                name: None,
                email: Some("ada@example.com".into()),
                realm_roles: vec![],
                claims: HashMap::from([
                    ("branch_id".to_string(), json!(ORG_A)),
                    ("sub".to_string(), json!("kc-1")),
                ]),
            }),
            is_service_account: false,
            is_platform_admin: false,
            has_deploy_authority: false,
            realm: None,
        };
        let claims = principal_claims(&user);
        assert_eq!(claims["branch_id"], json!(ORG_A));
        assert_eq!(claims["sub"], json!("kc-1"));
        assert_eq!(claims["email"], json!("ada@example.com"));
        assert_eq!(claims["preferred_username"], json!("ada"));
    }
}
