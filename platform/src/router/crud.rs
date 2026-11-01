use crate::audit::Read;
use crate::auth::scope::{EffectiveScope, OutOfScope, Placement};
use crate::auth::OptionalAuth;
use crate::deployer::manifest::{ColumnDefinition, ColumnType, TableDefinition, ValidationRule};
use crate::errors::{CrudOp, PlatformError, PlatformResult};
use crate::outbox::emit::{Change, ChangeKind, Emission, Outlet, Target};
use crate::policy::{PolicyKey, PolicyMode, Violation};
use crate::registry::RegisteredService;
use crate::router::authz::{
    authorize_access, authorize_rows, guest_auth_json, owner_param, requested_scope,
    resolve_data_route, Access, DataRoute, DataTarget, RowScope,
};
use crate::router::bind::{self, SqlValue};
use crate::router::crud_pk::{parse_pk, PkValue};
use crate::router::filtering::ListParams;
use crate::router::guest_response::parse_output;
use crate::router::map_execution_error;
use crate::router::postures::ServiceCaller;
use crate::router::query::{
    self, response::Listing, CountMode, FilterMode, ListOptions, ListPlan, SqlParams,
};
use crate::router::row_convert::{json_param, manifest_ident, row_to_json};
use crate::schema_manager::adopt::{AdoptedColumn, AdoptedSchema};
use crate::schema_manager::introspect::TypeKind;
use crate::tenant_catalog::TenantEntry;
use crate::tenant_sessions::{Purpose, ServiceSessions, Statements};
use crate::wasm_runtime::budget::{ChargedJson, ChargedJsonError, Hold, Refused};
use crate::wasm_runtime::{Class, ExecutionError, Invocation, InvocationKind, WasmEngine};
use crate::AppState;
use axum::{
    extract::{Path, Query, RawQuery, State},
    http::HeaderMap,
    Json,
};
use cufflink_types::host::request::{GuestRequest, METHOD_HOOK};
use cufflink_types::manifest::validate::folded_ident;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use uuid::Uuid;

const HOOK_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

pub(crate) async fn execute_hook(
    state: &Arc<AppState>,
    service: &RegisteredService,
    hook_name: &str,
    body: Value,
    auth_json: Option<Value>,
    call: Option<&ServiceCaller>,
) -> PlatformResult<Value> {
    execute_hook_as(
        InvocationKind::Hook,
        Class::of_call(call.map(|call| call.depth)),
        state,
        service,
        hook_name,
        body,
        auth_json,
        call,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_hook_as(
    kind: InvocationKind,
    class: Class,
    state: &Arc<AppState>,
    service: &RegisteredService,
    hook_name: &str,
    body: Value,
    auth_json: Option<Value>,
    call: Option<&ServiceCaller>,
) -> PlatformResult<Value> {
    let hold = state
        .wasm_engine
        .as_ref()
        .map_or_else(Hold::none, |engine| {
            engine.hold(&service.identity(), class, hook_name, kind)
        });
    let request = hook_request(body, hook_name, service, hold, auth_json, call)
        .map_err(|e| crate::router::request_refused(state, "Hook", e))?;
    run_hook(state, kind, class, service, hook_name, request, call).await
}

pub(crate) struct HookRequest {
    text: String,
    hold: Hold,
}

fn hook_request(
    body: Value,
    hook_name: &str,
    service: &RegisteredService,
    mut hold: Hold,
    auth_json: Option<Value>,
    call: Option<&ServiceCaller>,
) -> Result<HookRequest, ChargedJsonError> {
    let text = hook_request_text(body, hook_name, service, auth_json, call, &mut hold)?;
    hold.shrink_to(text.capacity());
    Ok(HookRequest { text, hold })
}

struct KeptHook {
    name: String,
    request: HookRequest,
}

struct Keeper<'a> {
    state: &'a Arc<AppState>,
    service: &'a RegisteredService,
    auth: Value,
    call: Option<&'a ServiceCaller>,
    before_ran: bool,
    initial_memory: usize,
}

impl<'a> Keeper<'a> {
    async fn new(
        state: &'a Arc<AppState>,
        service: &'a RegisteredService,
        auth_user: &Option<crate::auth::AuthUser>,
        call: Option<&'a ServiceCaller>,
        before_ran: bool,
    ) -> Keeper<'a> {
        let initial_memory = match state
            .wasm_engine
            .as_ref()
            .filter(|engine| engine.admits_before_running())
        {
            Some(engine) => engine
                .get_module(
                    service.service_id,
                    service.version,
                    service.wasm_hash.as_deref(),
                )
                .await
                .map_or(0, |compiled| compiled.initial_memory),
            None => 0,
        };
        Keeper {
            auth: guest_auth_json(state, auth_user, service.tenant_id).await,
            state,
            service,
            call,
            before_ran,
            initial_memory,
        }
    }

    fn refuses(&self) -> bool {
        self.state
            .wasm_engine
            .as_ref()
            .is_some_and(WasmEngine::admits_before_running)
    }

    fn keep(&self, name: &str, body: Value) -> Result<Option<KeptHook>, Refused> {
        let hold = self
            .state
            .wasm_engine
            .as_ref()
            .map_or_else(Hold::none, |engine| {
                engine.kept_hold(&self.service.identity(), name, self.initial_memory)
            });
        match hook_request(
            body,
            name,
            self.service,
            hold,
            Some(self.auth.clone()),
            self.call,
        ) {
            Ok(request) => Ok(Some(KeptHook {
                name: name.to_string(),
                request,
            })),
            Err(ChargedJsonError::Capacity(refused)) => Err(refused),
            Err(ChargedJsonError::Syntax(e)) => {
                tracing::warn!("after hook '{}' error: {}", name, e);
                Ok(None)
            }
        }
    }

    fn refused(&self, refused: Refused, needs: usize) -> PlatformError {
        let Some(engine) = self.state.wasm_engine.as_ref() else {
            return PlatformError::Internal(format!("Hook request refused: {refused}"));
        };
        match engine.refusal(refused.needing(needs), self.before_ran) {
            ExecutionError::TooLarge { requested, share } => {
                PlatformError::PayloadTooLarge(format!(
                    "the after-hook requests of this write take {requested} bytes, more than \
                     the {share} bytes its service may hold; nothing was committed"
                ))
            }
            ExecutionError::Overloaded {
                retry_after,
                ran: true,
                ..
            } => crate::router::retry_uncommitted(retry_after),
            error => crate::router::map_execution_error("Hook", error),
        }
    }
}

struct AfterHook<'a> {
    keeper: Keeper<'a>,
    name: &'a str,
}

impl<'a> AfterHook<'a> {
    async fn of(
        name: Option<&'a str>,
        before_ran: bool,
        state: &'a Arc<AppState>,
        service: &'a RegisteredService,
        auth_user: &Option<crate::auth::AuthUser>,
        call: Option<&'a ServiceCaller>,
    ) -> Option<AfterHook<'a>> {
        match name {
            Some(name) => Some(AfterHook {
                keeper: Keeper::new(state, service, auth_user, call, before_ran).await,
                name,
            }),
            None => None,
        }
    }

    fn keep(&self, body: Value) -> PlatformResult<Option<KeptHook>> {
        self.keeper
            .keep(self.name, body)
            .map_err(|refused| self.keeper.refused(refused, 0))
    }
}

async fn run_hook(
    state: &Arc<AppState>,
    kind: InvocationKind,
    class: Class,
    service: &RegisteredService,
    hook_name: &str,
    request: HookRequest,
    call: Option<&ServiceCaller>,
) -> PlatformResult<Value> {
    let budget = ServiceCaller::bound(call, HOOK_BUDGET);
    let HookRequest { text, hold } = request;

    tracing::debug!(
        "Executing hook '{}' for {}/{}",
        hook_name,
        service.tenant_slug,
        service.manifest.name
    );

    let engine = state.wasm_engine.as_ref();
    let worker_routed = kind != InvocationKind::OnMigrate
        && state
            .config
            .is_worker_routed(&service.manifest.name, hook_name);
    let output = if worker_routed {
        let job = crate::worker_dispatch::WasmJob::of(service, hook_name, text);
        crate::worker_dispatch::execute_on_worker(
            &state.nats,
            &service.manifest.name,
            job,
            budget,
            &hold,
            engine,
        )
        .await?
    } else {
        let wasm_engine = engine.ok_or_else(|| {
            PlatformError::Internal(format!(
                "Hook '{}' declared on service '{}' but the WASM runtime is unavailable on \
                 this platform instance",
                hook_name, service.manifest.name
            ))
        })?;

        let compiled = wasm_engine
            .get_module(
                service.service_id,
                service.version,
                service.wasm_hash.as_deref(),
            )
            .await
            .ok_or_else(|| {
                PlatformError::Internal(format!(
                    "Hook '{}' declared on service '{}' v{} but no WASM module is loaded \
                     (binary may be missing from object storage)",
                    hook_name, service.manifest.name, service.version
                ))
            })?;

        wasm_engine
            .run(
                state.clone(),
                Invocation {
                    compiled,
                    handler: hook_name.to_string(),
                    request_json: text,
                    identity: service.identity(),
                    publishes: service.manifest.publishes.clone(),
                    extensions: service.manifest.extensions.clone(),
                    kind,
                    scope_kind: None,
                    budget,
                    cancel: None,
                    class,
                    admission: None,
                },
            )
            .await
            .map_err(|e| map_execution_error("Hook", e))?
    };
    drop(hold);

    let (response, _response) = parse_output(output).map_err(|refused| match engine {
        Some(engine) => map_execution_error("Hook", engine.refusal(refused, true)),
        None => PlatformError::Internal(format!("Hook execution error: {refused}")),
    })?;
    interpret_hook_response(response)
}

pub(crate) fn interpret_hook_response(response: Value) -> PlatformResult<Value> {
    let (status, body) = match response.get("__status").and_then(|s| s.as_u64()) {
        Some(status) => {
            let body = response.get("__body").cloned().unwrap_or(Value::Null);
            (status, body)
        }
        None => (200, response),
    };

    let message = || {
        body.get("error")
            .and_then(|e| e.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| match &body {
                Value::String(s) => s.clone(),
                Value::Null => "hook rejected the request".to_string(),
                other => other.to_string(),
            })
    };

    match status {
        200..=299 => {
            if let Some(error) = body.get("error").and_then(|e| e.as_str()) {
                return Err(PlatformError::BadRequest(format!("Hook error: {}", error)));
            }
            Ok(body)
        }
        401 => Err(PlatformError::Unauthorized(format!(
            "Hook error: {}",
            message()
        ))),
        403 => Err(PlatformError::Forbidden(format!(
            "Hook error: {}",
            message()
        ))),
        404 => Err(PlatformError::NotFound(format!(
            "Hook error: {}",
            message()
        ))),
        422 => Err(PlatformError::Validation(format!(
            "Hook error: {}",
            message()
        ))),
        _ => Err(PlatformError::BadRequest(format!(
            "Hook error: {}",
            message()
        ))),
    }
}

fn wrap_hook_request(
    body: Value,
    hook_name: &str,
    service: &RegisteredService,
    auth_json: Option<Value>,
    call: Option<&ServiceCaller>,
) -> GuestRequest {
    GuestRequest {
        method: METHOD_HOOK.to_string(),
        handler: hook_name.to_string(),
        headers: Some(Map::new()),
        body,
        tenant: service.tenant_slug.clone(),
        service: service.name.clone(),
        auth: Some(auth_json.unwrap_or(Value::Null)),
        caller: call.map(ServiceCaller::to_json),
        ..GuestRequest::default()
    }
}

pub(crate) fn hook_request_text(
    body: Value,
    hook_name: &str,
    service: &RegisteredService,
    auth_json: Option<Value>,
    call: Option<&ServiceCaller>,
    hold: &mut Hold,
) -> Result<String, ChargedJsonError> {
    crate::router::charged_guest_text(
        &wrap_hook_request(body, hook_name, service, auth_json, call),
        hold,
    )
}

async fn execute_after_hook(
    state: &Arc<AppState>,
    service: &RegisteredService,
    hook_name: &str,
    request: HookRequest,
    call: Option<&ServiceCaller>,
) {
    if state
        .config
        .is_worker_routed(&service.manifest.name, hook_name)
    {
        let job = crate::worker_dispatch::WasmJob::of(service, hook_name, request.text);
        if let Err(e) =
            crate::worker_dispatch::publish_async_job(&state.jetstream, &service.manifest.name, job)
                .await
        {
            tracing::warn!("Failed to publish async after-hook '{}': {}", hook_name, e);
        }
    } else if let Err(e) = run_hook(
        state,
        InvocationKind::Hook,
        Class::Committed,
        service,
        hook_name,
        request,
        call,
    )
    .await
    {
        tracing::warn!("after hook '{}' error: {}", hook_name, e);
    }
}

pub(crate) fn db(op: CrudOp) -> impl FnOnce(tokio_postgres::Error) -> PlatformError {
    move |source| PlatformError::DatabaseOp { op, source }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DataQuery {
    pub scope: Option<String>,
    pub filter_mode: Option<String>,
}

impl DataQuery {
    pub(crate) fn from_raw(raw: Option<&str>) -> Self {
        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()).fold(
            Self::default(),
            |mut query, (name, value)| {
                let slot = match name.as_ref() {
                    "scope" => &mut query.scope,
                    "filter_mode" => &mut query.filter_mode,
                    _ => return query,
                };
                slot.get_or_insert_with(|| value.into_owned());
                query
            },
        )
    }

    fn strict_writes(&self, tenant: &TenantEntry) -> PlatformResult<bool> {
        Ok(tenant.mode(PolicyKey::StrictFilters) == PolicyMode::Enforce
            || FilterMode::parse(self.filter_mode.as_deref())? == Some(FilterMode::V2))
    }
}

fn take_reserved(
    extra: &mut std::collections::HashMap<String, String>,
    table: &TableDefinition,
    name: &str,
) -> Option<String> {
    (!table.columns.iter().any(|c| c.name == name))
        .then(|| extra.remove(name))
        .flatten()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteBinding {
    Coerced,
    Typed,
}

impl WriteBinding {
    fn of(strict: bool, rows: &RowScope) -> Self {
        if strict || rows.scoped().is_some() {
            WriteBinding::Typed
        } else {
            WriteBinding::Coerced
        }
    }

    fn bind(self, column: &ColumnDefinition, value: &Value) -> PlatformResult<SqlValue> {
        match self {
            WriteBinding::Typed if !value.is_null() => bind::bind_column_value(column, value)
                .map_err(|e| PlatformError::BadRequest(e.to_string())),
            _ => json_param(column, value),
        }
    }
}

fn record_would_reject(
    tenant: &TenantEntry,
    table: &TableDefinition,
    named: &TableDefinition,
    params: &ListParams,
    schema: &str,
    rows: &RowScope,
    options: &ListOptions<'_>,
) {
    let v2 = ListOptions {
        mode: FilterMode::V2,
        v1_max_in: None,
        ..*options
    };
    if let Err(PlatformError::BadRequest(reason)) =
        ListPlan::build(table, named, params, schema, rows, &v2)
    {
        let _ = tenant.apply(
            PolicyKey::StrictFilters,
            Some(Violation::new(
                "would_reject",
                format!("table {}: {reason}", table.name),
            )),
        );
    }
}

fn requested_table<'a>(
    service: &'a RegisteredService,
    table_name: &str,
    operation: &'static str,
) -> PlatformResult<&'a TableDefinition> {
    let table = service
        .manifest
        .get_table(table_name)
        .ok_or_else(|| PlatformError::NotFound(format!("Table '{}' not found", table_name)))?;
    tracing::info!(tenant = %service.tenant_slug, service = %service.name, table = %table.name, operation, "crud request");
    metrics::counter!(
        "cufflink_crud_requests_total",
        "tenant" => service.tenant_slug.clone(),
        "service" => service.name.clone(),
        "operation" => operation,
        "table" => table.name.clone(),
    )
    .increment(1);
    Ok(table)
}

fn primary_key(table: &TableDefinition) -> PlatformResult<&ColumnDefinition> {
    table
        .primary_key()
        .ok_or_else(|| PlatformError::Internal("Table has no primary key".into()))
}

fn with_scope(mut body: Value, rows: &RowScope) -> Value {
    if let (Some(scope), Some(envelope)) = (rows.scoped(), body.as_object_mut()) {
        envelope.insert("scope".to_string(), scope.to_json());
    }
    body
}

fn row_filter(
    table: &TableDefinition,
    rows: &RowScope,
    live: bool,
    params: &mut SqlParams,
) -> PlatformResult<String> {
    let soft_delete = (live && table.soft_delete).then(|| "deleted_at IS NULL".to_string());
    let reachable = rows.condition(table, params)?;
    Ok(soft_delete
        .into_iter()
        .chain(reachable)
        .map(|condition| format!(" AND {condition}"))
        .collect())
}

fn place_row(
    table: &TableDefinition,
    rows: &RowScope,
    row: &mut Map<String, Value>,
) -> PlatformResult<Option<(String, Option<String>)>> {
    match rows {
        RowScope::Legacy { owner } => Ok(owner.clone()),
        RowScope::Scoped(scope) => Ok(match scope.place(row, &table.columns)? {
            Placement::ForceOwner { column, subject } => Some((column, Some(subject))),
            Placement::AsIs => None,
        }),
    }
}

pub(crate) struct Shape<'a> {
    table: &'a TableDefinition,
    adopted: Option<Arc<AdoptedSchema>>,
}

impl<'a> Shape<'a> {
    pub(crate) fn made(table: &'a TableDefinition) -> Self {
        Self {
            table,
            adopted: None,
        }
    }

    pub(crate) fn adopted(table: &'a TableDefinition, adopted: Arc<AdoptedSchema>) -> Self {
        Self {
            table,
            adopted: Some(adopted),
        }
    }

    pub(crate) async fn of(
        state: &AppState,
        service: &RegisteredService,
        table: &'a TableDefinition,
    ) -> PlatformResult<Self> {
        if service.manifest.schema.is_managed() && table.managed {
            return Ok(Self::made(table));
        }
        state
            .adopted
            .of(
                &state.platform_db,
                &service.name,
                service.service_id,
                service.version,
            )
            .await
            .map(|adopted| Self::adopted(table, adopted))
    }

    fn column(&self, name: &str) -> Option<&AdoptedColumn> {
        self.adopted.as_ref()?.column(&self.table.name, name)
    }

    pub(crate) fn expressions(&self, of: Option<&str>) -> Option<Vec<(String, String)>> {
        let read = |column: &ColumnDefinition| {
            let value = self.value(column, of);
            match self.is_cast(column) {
                true => format!("{value} AS {}", manifest_ident(&column.name)),
                false => value,
            }
        };
        self.adopted.as_ref().map(|_| {
            self.table
                .columns
                .iter()
                .map(|column| (column.name.clone(), read(column)))
                .collect()
        })
    }

    fn is_cast(&self, column: &ColumnDefinition) -> bool {
        matches!(
            self.column(&column.name).map(|adopted| adopted.typtype),
            Some(TypeKind::Enum | TypeKind::Domain)
        )
    }

    fn value(&self, column: &ColumnDefinition, of: Option<&str>) -> String {
        let qualifier = of.map(|of| format!("{of}.")).unwrap_or_default();
        let name = manifest_ident(&column.name);
        match self.is_cast(column) {
            true => format!("{qualifier}{name}::{}", column.column_type.to_sql()),
            false => format!("{qualifier}{name}"),
        }
    }

    fn before(&self) -> String {
        self.table
            .columns
            .iter()
            .map(|column| {
                format!(
                    "{} AS \"{BEFORE}{}\"",
                    self.value(column, Some("old")),
                    column.name.replace('"', "\"\"")
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn columns(&self, of: Option<&str>) -> String {
        match self.expressions(of) {
            None => format!("{}*", of.map(|of| format!("{of}.")).unwrap_or_default()),
            Some(expressions) => expressions
                .into_iter()
                .map(|(_, expression)| expression)
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    fn bound(&self, column: &str, placeholder: String) -> String {
        match &self.adopted {
            Some(adopted) => adopted.bound(&self.table.name, column, placeholder),
            None => placeholder,
        }
    }

    fn created_key<'r>(&self, row: &'r Value, pk: &ColumnDefinition) -> Cow<'r, str> {
        match (&self.adopted, row.get(&pk.name)) {
            (None, _) => Cow::Borrowed(made_key(row)),
            (Some(_), Some(Value::String(key))) => Cow::Borrowed(key),
            (Some(_), Some(key @ Value::Number(_))) => Cow::Owned(key.to_string()),
            (Some(_), _) => Cow::Borrowed(""),
        }
    }

    fn generates_key(&self, pk: &ColumnDefinition) -> bool {
        pk.auto_generate
            && self
                .column(&pk.name)
                .is_none_or(|adopted| !adopted.has_default && pk.column_type == ColumnType::Uuid)
    }

    fn requires(&self, column: &ColumnDefinition) -> bool {
        match self.column(&column.name) {
            Some(adopted) => !adopted.may_be_omitted(),
            None => !column.nullable && column.default_value.is_none() && !column.primary_key,
        }
    }
}

fn insert_statement(
    schema: &str,
    table_name: &str,
    shape: &Shape<'_>,
    row: &Map<String, Value>,
    owned: Option<&(String, Option<String>)>,
    binding: WriteBinding,
) -> PlatformResult<(String, SqlParams)> {
    let table = shape.table;
    let pk = primary_key(table)?;
    let mut params = SqlParams::default();
    let mut columns = Vec::new();
    let mut placeholders = Vec::new();
    if shape.generates_key(pk) && !row.contains_key(&pk.name) {
        columns.push(pk.name.clone());
        placeholders.push(params.push(Box::new(Uuid::new_v4())));
    }
    if let Some((column, Some(subject))) = owned {
        columns.push(column.clone());
        placeholders.push(params.push(owner_param(&table.columns, column, subject)?));
    }
    let owner_column = owned.map(|(column, _)| column.as_str());
    for col in &table.columns {
        if col.auto_generate || owner_column == Some(col.name.as_str()) {
            continue;
        }
        match row.get(&col.name) {
            Some(val) => {
                columns.push(col.name.clone());
                placeholders.push(shape.bound(&col.name, params.push(binding.bind(col, val)?)));
            }
            None if shape.requires(col) => {
                return Err(PlatformError::Validation(format!(
                    "Missing required field: {}",
                    col.name
                )))
            }
            None => {}
        }
    }
    let values = if columns.is_empty() {
        "DEFAULT VALUES".to_string()
    } else {
        format!(
            "({}) VALUES ({})",
            columns.join(", "),
            placeholders.join(", ")
        )
    };
    let sql = format!(
        "INSERT INTO {}.{} {} RETURNING {}",
        schema,
        table_name,
        values,
        shape.columns(None),
    );
    Ok((sql, params))
}

const BEFORE: &str = "__previous.";

fn before_of(row: &tokio_postgres::Row, table: &TableDefinition) -> Value {
    let named: Vec<ColumnDefinition> = table
        .columns
        .iter()
        .map(|column| ColumnDefinition {
            name: format!("{BEFORE}{}", column.name),
            ..column.clone()
        })
        .collect();
    match row_to_json(row, &named, None) {
        Value::Object(fields) => fields
            .into_iter()
            .map(|(name, value)| match name.strip_prefix(BEFORE) {
                Some(column) => (column.to_string(), value),
                None => (name, value),
            })
            .collect(),
        other => other,
    }
}

#[allow(clippy::too_many_arguments)]
fn update_statement(
    schema: &str,
    table_name: &str,
    shape: &Shape<'_>,
    row: &Map<String, Value>,
    id: PkValue,
    rows: &RowScope,
    binding: WriteBinding,
    recorded: bool,
) -> PlatformResult<(String, SqlParams)> {
    let table = shape.table;
    let pk = primary_key(table)?;
    let fixed_owner = match rows {
        RowScope::Legacy { .. } => table.owner_field.as_deref(),
        RowScope::Scoped(_) => None,
    };
    let mut params = SqlParams::default();
    let mut set_parts: Vec<String> = table
        .columns
        .iter()
        .filter(|col| !col.primary_key && fixed_owner != Some(col.name.as_str()))
        .filter_map(|col| row.get(&col.name).map(|val| (col, val)))
        .map(|(col, val)| {
            binding.bind(col, val).map(|value| {
                format!(
                    "{} = {}",
                    col.name,
                    shape.bound(&col.name, params.push(value))
                )
            })
        })
        .collect::<PlatformResult<_>>()?;
    if table.columns.iter().any(|c| c.name == "updated_at") && !row.contains_key("updated_at") {
        set_parts.push("updated_at = now()".into());
    }
    if set_parts.is_empty() {
        return Err(PlatformError::BadRequest("No fields to update".into()));
    }
    let id_placeholder = shape.bound(&pk.name, params.push(id.into_param()));
    let filter = row_filter(table, rows, false, &mut params)?;
    let (key, sets) = (&pk.name, set_parts.join(", "));
    let sql = if recorded {
        format!(
            "WITH old AS (SELECT * FROM {schema}.{table_name} \
             WHERE {key} = {id_placeholder}{filter} FOR UPDATE) \
             UPDATE {schema}.{table_name} AS live SET {sets} FROM old \
             WHERE live.{key} = old.{key} RETURNING {}, {}",
            shape.columns(Some("live")),
            shape.before(),
        )
    } else {
        format!(
            "UPDATE {schema}.{table_name} SET {sets} WHERE {key} = {id_placeholder}{filter} \
             RETURNING {}",
            shape.columns(None),
        )
    };
    Ok((sql, params))
}

fn confined(stored: Value, table: &TableDefinition, rows: &RowScope) -> PlatformResult<Value> {
    match rows.confining() {
        Some(scope) if !scope.matches(&stored, &table.columns) => Err(OutOfScope.into()),
        _ => Ok(stored),
    }
}

pub(crate) fn made_key(row: &Value) -> &str {
    row.get("id").and_then(Value::as_str).unwrap_or_default()
}

pub(crate) fn created_row<'a>(
    row: &'a Value,
    record_id: &'a str,
    scope: Option<&'a EffectiveScope>,
) -> Change<'a> {
    Change {
        kind: ChangeKind::Created,
        row,
        record_id,
        scope,
        before: None,
    }
}

pub(crate) struct Asked<'a> {
    pub path: (&'a str, &'a str, &'a str),
    pub caller: &'a Option<crate::auth::AuthUser>,
    pub call: Option<&'a ServiceCaller>,
    pub headers: &'a HeaderMap,
}

pub(crate) fn outlet_of<'a>(
    state: &'a Arc<AppState>,
    tenant: &'a TenantEntry,
    service: &'a RegisteredService,
    table: &'a TableDefinition,
    asked: Asked<'a>,
) -> Outlet<'a> {
    let (tenant_slug, service_name, table_name) = asked.path;
    Outlet::of(
        state,
        tenant,
        service,
        Target {
            calling: asked.call.map(|call| call.service.as_str()),
            request_id: crate::middleware::request_id::request_id(asked.headers),
            redact: &table.audit_redact,
            ..Target::of(
                tenant_slug,
                service.tenant_id,
                service_name,
                table_name,
                asked.caller,
            )
        },
    )
}

fn named_in(filter: &Value) -> Vec<&str> {
    match filter {
        Value::Array(conditions) => conditions.iter().flat_map(named_in).collect(),
        Value::Object(condition) => {
            let column = match (condition.get("n"), condition.get("op")) {
                (Some(name), _) => name.as_str(),
                (None, Some(_)) => condition.get("f").and_then(Value::as_str),
                (None, None) => None,
            };
            let nested = ["and", "or", "not"]
                .into_iter()
                .filter_map(|joined| condition.get(joined))
                .flat_map(named_in);
            column.into_iter().chain(nested).collect()
        }
        _ => Vec::new(),
    }
}

fn filter_columns(table: &TableDefinition, params: &ListParams) -> Vec<String> {
    let filter: Option<Value> = params
        .filter
        .as_deref()
        .and_then(|filter| serde_json::from_str(filter).ok());
    let named = filter.as_ref().map(named_in).unwrap_or_default();
    table
        .columns
        .iter()
        .filter(|column| {
            named.contains(&column.name.as_str()) || params.extra.contains_key(&column.name)
        })
        .map(|column| column.name.clone())
        .collect()
}

fn delete_returning(
    schema: &str,
    table_name: &str,
    shape: &Shape<'_>,
    pk: &ColumnDefinition,
    filter: &str,
) -> String {
    let key = &pk.name;
    let id = shape.bound(key, "$1".to_string());
    if shape.table.soft_delete {
        format!(
            "UPDATE {schema}.{table_name} AS live SET deleted_at = now() \
             FROM (SELECT * FROM {schema}.{table_name} WHERE {key} = {id}{filter} FOR UPDATE) AS was \
             WHERE live.{key} = was.{key} RETURNING {}",
            shape.columns(Some("was"))
        )
    } else {
        format!(
            "DELETE FROM {schema}.{table_name} WHERE {key} = {id}{filter} RETURNING {}",
            shape.columns(None)
        )
    }
}

fn select_statement(
    schema: &str,
    table_name: &str,
    shape: &Shape<'_>,
    pk: &ColumnDefinition,
    filter: &str,
) -> String {
    format!(
        "SELECT {} FROM {}.{} WHERE {} = {}{}",
        shape.columns(None),
        schema,
        table_name,
        pk.name,
        shape.bound(&pk.name, "$1".to_string()),
        filter
    )
}

fn named_key(table: &TableDefinition, raw: &str) -> PlatformResult<PkValue> {
    parse_pk(table, raw).map_err(|refused| match refused {
        PlatformError::BadRequest(reason) => PlatformError::BadRequest(format!("{reason}: {raw}")),
        other => other,
    })
}

fn keyed(key: PkValue) -> SqlParams {
    let mut params = SqlParams::default();
    params.push(key.into_param());
    params
}

async fn run_insert<C: Statements>(
    client: &C,
    sql: &str,
    params: &SqlParams,
    table: &TableDefinition,
    rows: &RowScope,
) -> PlatformResult<Value> {
    let row = client
        .query_one(sql, &params.as_refs())
        .await
        .map_err(db(CrudOp::Create))?;
    confined(row_to_json(&row, &table.columns, None), table, rows)
}

struct Updated {
    row: Value,
    before: Option<Value>,
}

async fn run_update<C: Statements>(
    client: &C,
    sql: &str,
    params: &SqlParams,
    table: &TableDefinition,
    rows: &RowScope,
) -> PlatformResult<Option<Updated>> {
    client
        .query_opt(sql, &params.as_refs())
        .await
        .map_err(db(CrudOp::Update))?
        .map(|row| {
            confined(row_to_json(&row, &table.columns, None), table, rows).map(|updated| Updated {
                row: updated,
                before: row
                    .columns()
                    .iter()
                    .any(|column| column.name().starts_with(BEFORE))
                    .then(|| before_of(&row, table)),
            })
        })
        .transpose()
}

pub(crate) async fn list_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name, table_name)): Path<(String, String, String)>,
    Query(mut params): Query<ListParams>,
    OptionalAuth(auth_user): OptionalAuth,
    headers: HeaderMap,
    target: DataTarget,
) -> PlatformResult<Json<Value>> {
    let DataRoute {
        service,
        tenant,
        principal: auth_user,
        call,
    } = resolve_data_route(
        &state,
        &headers,
        &target,
        &tenant_slug,
        &service_name,
        auth_user,
    )
    .await?;

    let table = requested_table(&service, &table_name, "list")?;
    let raw_scope = table
        .scoped_by
        .is_some()
        .then(|| params.extra.remove("scope"))
        .flatten();
    let requested_mode =
        FilterMode::parse(take_reserved(&mut params.extra, table, "filter_mode").as_deref())?;
    let count = CountMode::parse(take_reserved(&mut params.extra, table, "count").as_deref())?;
    let policy = tenant.mode(PolicyKey::StrictFilters);
    let mode = match policy {
        PolicyMode::Enforce => FilterMode::V2,
        _ => requested_mode.unwrap_or(FilterMode::V1),
    };
    metrics::counter!("cufflink_crud_filter_mode_total", "mode" => mode.as_str()).increment(1);
    let Access { rows, columns } = authorize_access(
        &state,
        service.tenant_id,
        table,
        "view",
        &auth_user,
        requested_scope(table, raw_scope.as_deref())?,
    )
    .await?;

    let tenant_pool = state
        .tenant_sessions
        .service(&service.identity(), Purpose::Crud)
        .await?;
    let schema = folded_ident(&service.manifest.schema());
    let client = tenant_pool.get().await?;

    let shape = Shape::of(&state, &service, table).await?;
    let named = columns.named(table);
    let options = ListOptions {
        mode,
        count,
        v1_max_in: state.config.http.crud_max_in_items,
        adopted: shape.adopted.as_deref(),
    };
    if mode == FilterMode::V1 && policy == PolicyMode::Warn {
        record_would_reject(&tenant, table, &named, &params, &schema, &rows, &options);
    }
    let plan = ListPlan::build(table, &named, &params, &schema, &rows, &options)?;
    let plan = match shape.expressions(None) {
        Some(expressions) => plan.reading(expressions),
        None => plan,
    };
    let listing = Listing {
        table: &named,
        mode,
        count,
    };
    let listed = query::response::list(&client, &plan, listing).await?;
    if table.sensitive {
        let answered = listed["results"].as_array().map_or(&[][..], Vec::as_slice);
        let read = Read {
            filter_columns: filter_columns(&named, &params),
            search: params.search.is_some() || params.fts.is_some(),
            sort: params.sort.clone(),
            ..Read::of_rows(answered, &primary_key(table)?.name)
        };
        let asked = Asked {
            path: (&tenant_slug, &service_name, &table_name),
            caller: &auth_user,
            call: call.as_ref(),
            headers: &headers,
        };
        outlet_of(&state, &tenant, &service, table, asked)
            .read(&client, read, rows.scoped())
            .await?;
    }
    Ok(Json(listed))
}

pub(crate) async fn create_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name, table_name)): Path<(String, String, String)>,
    RawQuery(raw_query): RawQuery,
    OptionalAuth(auth_user): OptionalAuth,
    headers: HeaderMap,
    target: DataTarget,
    Json(mut body): Json<Value>,
) -> PlatformResult<Json<Value>> {
    let query = DataQuery::from_raw(raw_query.as_deref());
    let DataRoute {
        service,
        tenant,
        principal: auth_user,
        call,
    } = resolve_data_route(
        &state,
        &headers,
        &target,
        &tenant_slug,
        &service_name,
        auth_user,
    )
    .await?;

    let table = requested_table(&service, &table_name, "create")?;
    let Access { rows, columns } = authorize_access(
        &state,
        service.tenant_id,
        table,
        "create",
        &auth_user,
        requested_scope(table, query.scope.as_deref())?,
    )
    .await?;
    columns.admit(&body)?;

    if let Some(ref hooks) = table.hooks {
        if let Some(ref hook_name) = hooks.before_create {
            let auth_json = guest_auth_json(&state, &auth_user, service.tenant_id).await;
            let hook_body = with_scope(
                json!({
                    "hook": "before_create",
                    "table": table_name,
                    "data": body,
                }),
                &rows,
            );
            let response = execute_hook(
                &state,
                &service,
                hook_name,
                hook_body,
                Some(auth_json),
                call.as_ref(),
            )
            .await?;
            if let Some(modified) = response.get("data") {
                body = modified.clone();
            }
        }
    }

    let tenant_pool = state
        .tenant_sessions
        .service(&service.identity(), Purpose::Crud)
        .await?;
    let schema = folded_ident(&service.manifest.schema());
    let mut row = body
        .as_object()
        .cloned()
        .ok_or_else(|| PlatformError::BadRequest("Request body must be a JSON object".into()))?;
    let owned = place_row(table, &rows, &mut row)?;
    let binding = WriteBinding::of(query.strict_writes(&tenant)?, &rows);
    validate_fields(&row, &table.columns)?;
    let shape = Shape::of(&state, &service, table).await?;
    let (sql, params) =
        insert_statement(&schema, &table_name, &shape, &row, owned.as_ref(), binding)?;
    let outlet = outlet_of(
        &state,
        &tenant,
        &service,
        table,
        Asked {
            path: (&tenant_slug, &service_name, &table_name),
            caller: &auth_user,
            call: call.as_ref(),
            headers: &headers,
        },
    );
    let pk = primary_key(table)?;
    let created = |row: &Value| {
        outlet.emission(&created_row(
            row,
            &shape.created_key(row, pk),
            rows.scoped(),
        ))
    };
    let after = AfterHook::of(
        table
            .hooks
            .as_ref()
            .and_then(|hooks| hooks.after_create.as_deref()),
        table
            .hooks
            .as_ref()
            .is_some_and(|hooks| hooks.before_create.is_some()),
        &state,
        &service,
        &auth_user,
        call.as_ref(),
    )
    .await;
    let kept = |result: &Value| {
        after
            .as_ref()
            .map(|after| {
                after.keep(with_scope(
                    json!({
                        "hook": "after_create",
                        "table": table_name,
                        "data": row,
                        "result": result,
                    }),
                    &rows,
                ))
            })
            .transpose()
            .map(Option::flatten)
    };
    let mut client = tenant_pool.get().await?;
    let (result, emission, kept) =
        if outlet.enforced() || rows.confining().is_some() || keeps_first(after.as_ref()) {
            let txn = client.transaction().await?;
            let inserted = run_insert(&txn, &sql, &params, table, &rows).await?;
            let emission = created(&inserted);
            outlet
                .enqueue(&txn, std::slice::from_ref(&emission))
                .await?;
            let kept = kept(&inserted)?;
            txn.commit().await.map_err(db(CrudOp::Commit))?;
            (inserted, emission, kept)
        } else {
            let inserted = run_insert(&client, &sql, &params, table, &rows).await?;
            let emission = created(&inserted);
            let kept = kept(&inserted)?;
            (inserted, emission, kept)
        };
    outlet.publish(emission).await;
    spawn_after_hooks(&state, &service, kept, call.as_ref());

    Ok(Json(columns.shown(result)))
}

pub(crate) async fn get_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name, table_name, id)): Path<(String, String, String, String)>,
    RawQuery(raw_query): RawQuery,
    OptionalAuth(auth_user): OptionalAuth,
    headers: HeaderMap,
    target: DataTarget,
) -> PlatformResult<Json<Value>> {
    let query = DataQuery::from_raw(raw_query.as_deref());
    let DataRoute {
        service,
        tenant,
        principal: auth_user,
        call,
    } = resolve_data_route(
        &state,
        &headers,
        &target,
        &tenant_slug,
        &service_name,
        auth_user,
    )
    .await?;

    let table = requested_table(&service, &table_name, "get")?;
    let Access { rows, columns } = authorize_access(
        &state,
        service.tenant_id,
        table,
        "view",
        &auth_user,
        requested_scope(table, query.scope.as_deref())?,
    )
    .await?;
    let pk = primary_key(table)?;

    let tenant_pool = state
        .tenant_sessions
        .service(&service.identity(), Purpose::Crud)
        .await?;
    let schema = folded_ident(&service.manifest.schema());
    let client = tenant_pool.get().await?;

    let mut params = keyed(parse_pk(table, &id)?);
    let filter = row_filter(table, &rows, true, &mut params)?;
    let shape = Shape::of(&state, &service, table).await?;
    let sql = select_statement(&schema, &table_name, &shape, pk, &filter);
    let row = client
        .query_opt(&sql, &params.as_refs())
        .await
        .map_err(db(CrudOp::Read))?
        .ok_or_else(|| PlatformError::NotFound(format!("{} not found", table_name)))?;
    if table.sensitive {
        let asked = Asked {
            path: (&tenant_slug, &service_name, &table_name),
            caller: &auth_user,
            call: call.as_ref(),
            headers: &headers,
        };
        outlet_of(&state, &tenant, &service, table, asked)
            .read(&client, Read::of_row(&id), rows.scoped())
            .await?;
    }

    Ok(Json(columns.shown(row_to_json(&row, &table.columns, None))))
}

pub(crate) async fn update_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name, table_name, id)): Path<(String, String, String, String)>,
    RawQuery(raw_query): RawQuery,
    OptionalAuth(auth_user): OptionalAuth,
    headers: HeaderMap,
    target: DataTarget,
    Json(mut body): Json<Value>,
) -> PlatformResult<Json<Value>> {
    let query = DataQuery::from_raw(raw_query.as_deref());
    let DataRoute {
        service,
        tenant,
        principal: auth_user,
        call,
    } = resolve_data_route(
        &state,
        &headers,
        &target,
        &tenant_slug,
        &service_name,
        auth_user,
    )
    .await?;

    let table = requested_table(&service, &table_name, "update")?;
    let Access { rows, columns } = authorize_access(
        &state,
        service.tenant_id,
        table,
        "edit",
        &auth_user,
        requested_scope(table, query.scope.as_deref())?,
    )
    .await?;
    columns.admit(&body)?;
    let pk = primary_key(table)?;

    let tenant_pool = state
        .tenant_sessions
        .service(&service.identity(), Purpose::Crud)
        .await?;
    let schema = folded_ident(&service.manifest.schema());
    let shape = Shape::of(&state, &service, table).await?;

    if let Some(ref hooks) = table.hooks {
        if let Some(ref hook_name) = hooks.before_update {
            let existing = {
                let client = tenant_pool.get().await?;
                fetch_existing_for_hook(
                    &client,
                    &schema,
                    &table_name,
                    &shape,
                    pk,
                    &id,
                    RowVisibility::All,
                    rows.scoped(),
                )
                .await?
            };

            let auth_json = guest_auth_json(&state, &auth_user, service.tenant_id).await;
            let hook_body = with_scope(
                json!({
                    "hook": "before_update",
                    "table": table_name,
                    "id": id,
                    "data": body,
                    "existing": existing,
                }),
                &rows,
            );
            let response = execute_hook(
                &state,
                &service,
                hook_name,
                hook_body,
                Some(auth_json),
                call.as_ref(),
            )
            .await?;
            if let Some(modified) = response.get("data") {
                body = modified.clone();
            }
        }
    }

    let body_map = body
        .as_object()
        .ok_or_else(|| PlatformError::BadRequest("Request body must be a JSON object".into()))?;
    let binding = WriteBinding::of(query.strict_writes(&tenant)?, &rows);
    validate_fields(body_map, &table.columns)?;
    let outlet = outlet_of(
        &state,
        &tenant,
        &service,
        table,
        Asked {
            path: (&tenant_slug, &service_name, &table_name),
            caller: &auth_user,
            call: call.as_ref(),
            headers: &headers,
        },
    );
    let (sql, params) = update_statement(
        &schema,
        &table_name,
        &shape,
        body_map,
        parse_pk(table, &id)?,
        &rows,
        binding,
        table.sensitive && outlet.durable(),
    )?;
    let changed = |updated: Updated| {
        let emission = outlet.emission(&Change {
            kind: ChangeKind::Updated,
            row: &updated.row,
            record_id: &id,
            scope: rows.scoped(),
            before: updated.before.as_ref(),
        });
        (emission, updated.row)
    };
    let after = AfterHook::of(
        table
            .hooks
            .as_ref()
            .and_then(|hooks| hooks.after_update.as_deref()),
        table
            .hooks
            .as_ref()
            .is_some_and(|hooks| hooks.before_update.is_some()),
        &state,
        &service,
        &auth_user,
        call.as_ref(),
    )
    .await;
    let kept = |updated: Option<&(Emission, Value)>| {
        after
            .as_ref()
            .zip(updated)
            .map(|(after, (_, result))| {
                after.keep(with_scope(
                    json!({
                        "hook": "after_update",
                        "table": table_name,
                        "id": id,
                        "data": body,
                        "result": result,
                    }),
                    &rows,
                ))
            })
            .transpose()
            .map(Option::flatten)
    };
    let mut client = tenant_pool.get().await?;
    let (updated, kept) =
        if outlet.enforced() || rows.confining().is_some() || keeps_first(after.as_ref()) {
            let txn = client.transaction().await?;
            let updated = run_update(&txn, &sql, &params, table, &rows)
                .await?
                .map(changed);
            if let Some((emission, _)) = &updated {
                outlet.enqueue(&txn, std::slice::from_ref(emission)).await?;
            }
            let kept = kept(updated.as_ref())?;
            txn.commit().await.map_err(db(CrudOp::Commit))?;
            (updated, kept)
        } else {
            let updated = run_update(&client, &sql, &params, table, &rows)
                .await?
                .map(changed);
            let kept = kept(updated.as_ref())?;
            (updated, kept)
        };
    let (emission, result) =
        updated.ok_or_else(|| PlatformError::NotFound(format!("{} not found", table_name)))?;
    outlet.publish(emission).await;
    spawn_after_hooks(&state, &service, kept, call.as_ref());

    Ok(Json(columns.shown(result)))
}

pub(crate) async fn delete_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name, table_name, id)): Path<(String, String, String, String)>,
    RawQuery(raw_query): RawQuery,
    OptionalAuth(auth_user): OptionalAuth,
    headers: HeaderMap,
    target: DataTarget,
) -> PlatformResult<Json<Value>> {
    let query = DataQuery::from_raw(raw_query.as_deref());
    let DataRoute {
        service,
        tenant,
        principal: auth_user,
        call,
    } = resolve_data_route(
        &state,
        &headers,
        &target,
        &tenant_slug,
        &service_name,
        auth_user,
    )
    .await?;

    let table = requested_table(&service, &table_name, "delete")?;
    let Access { rows, .. } = authorize_access(
        &state,
        service.tenant_id,
        table,
        "delete",
        &auth_user,
        requested_scope(table, query.scope.as_deref())?,
    )
    .await?;
    let pk = primary_key(table)?;

    let tenant_pool = state
        .tenant_sessions
        .service(&service.identity(), Purpose::Crud)
        .await?;
    let schema = folded_ident(&service.manifest.schema());

    let mut params = keyed(parse_pk(table, &id)?);
    let filter = row_filter(table, &rows, true, &mut params)?;
    let param_refs = params.as_refs();
    let shape = Shape::of(&state, &service, table).await?;

    let get_sql = select_statement(&schema, &table_name, &shape, pk, &filter);
    let row = {
        let client = tenant_pool.get().await?;
        client
            .query_opt(&get_sql, &param_refs)
            .await
            .map_err(db(CrudOp::Read))?
            .ok_or_else(|| PlatformError::NotFound(format!("{} not found", table_name)))?
    };
    let deleted_data = row_to_json(&row, &table.columns, None);

    if let Some(ref hooks) = table.hooks {
        if let Some(ref hook_name) = hooks.before_delete {
            let auth_json = guest_auth_json(&state, &auth_user, service.tenant_id).await;
            let hook_body = with_scope(
                json!({
                    "hook": "before_delete",
                    "table": table_name,
                    "id": id,
                    "existing": deleted_data,
                }),
                &rows,
            );
            execute_hook(
                &state,
                &service,
                hook_name,
                hook_body,
                Some(auth_json),
                call.as_ref(),
            )
            .await?;
        }
    }

    let key = shape.bound(&pk.name, "$1".to_string());
    let delete_sql = if table.soft_delete {
        format!(
            "UPDATE {}.{} SET deleted_at = now() WHERE {} = {}{}",
            schema, table_name, pk.name, key, filter
        )
    } else {
        format!(
            "DELETE FROM {}.{} WHERE {} = {}{}",
            schema, table_name, pk.name, key, filter
        )
    };
    let outlet = outlet_of(
        &state,
        &tenant,
        &service,
        table,
        Asked {
            path: (&tenant_slug, &service_name, &table_name),
            caller: &auth_user,
            call: call.as_ref(),
            headers: &headers,
        },
    );
    let emission = outlet.emission(&Change {
        kind: ChangeKind::Deleted,
        row: &deleted_data,
        record_id: &id,
        scope: rows.scoped(),
        before: None,
    });
    let after = AfterHook::of(
        table
            .hooks
            .as_ref()
            .and_then(|hooks| hooks.after_delete.as_deref()),
        table
            .hooks
            .as_ref()
            .is_some_and(|hooks| hooks.before_delete.is_some()),
        &state,
        &service,
        &auth_user,
        call.as_ref(),
    )
    .await;
    let kept = after
        .as_ref()
        .map(|after| {
            after.keep(with_scope(
                json!({
                    "hook": "after_delete",
                    "table": table_name,
                    "id": id,
                }),
                &rows,
            ))
        })
        .transpose()?
        .flatten();
    let mut client = tenant_pool.get().await?;
    if outlet.enforced() {
        let txn = client.transaction().await?;
        let deleted = txn
            .execute(&delete_sql, &param_refs)
            .await
            .map_err(db(CrudOp::Delete))?;
        if deleted == 0 {
            return Err(PlatformError::NotFound(format!("{} not found", table_name)));
        }
        outlet
            .enqueue(&txn, std::slice::from_ref(&emission))
            .await?;
        txn.commit().await.map_err(db(CrudOp::Commit))?;
    } else {
        client
            .execute(&delete_sql, &param_refs)
            .await
            .map_err(db(CrudOp::Delete))?;
    }
    outlet.publish(emission).await;
    spawn_after_hooks(&state, &service, kept, call.as_ref());

    Ok(Json(json!({ "deleted": true })))
}

#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    pub operations: Vec<BatchOperation>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum BatchOperation {
    Create { data: Value },
    Update { id: String, data: Value },
    Delete { id: String },
}

impl BatchOperation {
    fn permission(&self) -> &'static str {
        match self {
            BatchOperation::Create { .. } => "create",
            BatchOperation::Update { .. } => "edit",
            BatchOperation::Delete { .. } => "delete",
        }
    }

    fn data(&self) -> Option<&Value> {
        match self {
            BatchOperation::Create { data } | BatchOperation::Update { data, .. } => Some(data),
            BatchOperation::Delete { .. } => None,
        }
    }
}

struct BatchRows(BTreeMap<&'static str, RowScope>);

impl BatchRows {
    fn of(&self, op: &BatchOperation) -> PlatformResult<&RowScope> {
        self.0
            .get(op.permission())
            .ok_or_else(|| PlatformError::Internal("batch operation without a row scope".into()))
    }
}

enum PreparedOp {
    Create {
        data: Map<String, Value>,
        owned: Option<(String, Option<String>)>,
    },
    Update {
        id: String,
        data: Value,
    },
    Delete {
        id: String,
        existing: Value,
    },
}

pub(crate) async fn batch_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name, table_name)): Path<(String, String, String)>,
    RawQuery(raw_query): RawQuery,
    OptionalAuth(auth_user): OptionalAuth,
    headers: HeaderMap,
    target: DataTarget,
    Json(batch): Json<BatchRequest>,
) -> PlatformResult<Json<Value>> {
    let query = DataQuery::from_raw(raw_query.as_deref());
    const MAX_BATCH_SIZE: usize = 1000;
    if batch.operations.len() > MAX_BATCH_SIZE {
        return Err(PlatformError::BadRequest(format!(
            "Batch size {} exceeds maximum of {}",
            batch.operations.len(),
            MAX_BATCH_SIZE
        )));
    }

    let DataRoute {
        service,
        tenant,
        principal: auth_user,
        call,
    } = resolve_data_route(
        &state,
        &headers,
        &target,
        &tenant_slug,
        &service_name,
        auth_user,
    )
    .await?;

    let table = requested_table(&service, &table_name, "batch")?;

    let operations: Vec<&'static str> = batch
        .operations
        .iter()
        .map(BatchOperation::permission)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let (scopes, columns) = authorize_rows(
        &state,
        service.tenant_id,
        table,
        &operations,
        &auth_user,
        requested_scope(table, query.scope.as_deref())?,
    )
    .await?;
    batch
        .operations
        .iter()
        .filter_map(BatchOperation::data)
        .try_for_each(|data| columns.admit(data))?;
    let batch_rows = BatchRows(operations.into_iter().zip(scopes).collect());
    let strict = query.strict_writes(&tenant)?;

    let pk = primary_key(table)?;

    let tenant_pool = state
        .tenant_sessions
        .service(&service.identity(), Purpose::Crud)
        .await?;
    let schema = folded_ident(&service.manifest.schema());

    let shape = Shape::of(&state, &service, table).await?;
    let prepared = run_batch_before_hooks(
        &state,
        &service,
        &shape,
        pk,
        &schema,
        &table_name,
        &tenant_pool,
        &auth_user,
        call.as_ref(),
        &batch.operations,
        &batch_rows,
    )
    .await?;

    let keeper = match table.hooks.as_ref().filter(|hooks| fires_after(hooks)) {
        Some(hooks) => {
            let before_ran = batch.operations.iter().any(|op| fires_before(op, hooks));
            Some(Keeper::new(&state, &service, &auth_user, call.as_ref(), before_ran).await)
        }
        None => None,
    };
    let mut client = tenant_pool.get().await?;
    let txn = client.transaction().await?;

    let outlet = outlet_of(
        &state,
        &tenant,
        &service,
        table,
        Asked {
            path: (&tenant_slug, &service_name, &table_name),
            caller: &auth_user,
            call: call.as_ref(),
            headers: &headers,
        },
    );
    let mut emissions: Vec<Emission> = Vec::with_capacity(prepared.len());
    let mut op_results: Vec<Option<Value>> = Vec::with_capacity(prepared.len());
    let mut created = 0u64;
    let mut updated = 0u64;
    let mut deleted = 0u64;

    for (op, prepared_op) in batch.operations.iter().zip(&prepared) {
        let rows = batch_rows.of(op)?;
        let binding = WriteBinding::of(strict, rows);
        match prepared_op {
            PreparedOp::Create { data, owned } => {
                validate_fields(data, &table.columns)?;
                let (sql, params) =
                    insert_statement(&schema, &table_name, &shape, data, owned.as_ref(), binding)?;
                let row = run_insert(&txn, &sql, &params, table, rows).await?;
                emissions.push(outlet.emission(&created_row(
                    &row,
                    &shape.created_key(&row, pk),
                    rows.scoped(),
                )));
                op_results.push(Some(row));
                created += 1;
            }
            PreparedOp::Update { id, data } => {
                let body_map = data.as_object().ok_or_else(|| {
                    PlatformError::BadRequest(
                        "Each update operation data must be a JSON object".into(),
                    )
                })?;
                validate_fields(body_map, &table.columns)?;
                let (sql, params) = update_statement(
                    &schema,
                    &table_name,
                    &shape,
                    body_map,
                    named_key(table, id)?,
                    rows,
                    binding,
                    table.sensitive && outlet.durable(),
                )?;
                let Updated { row, before } = run_update(&txn, &sql, &params, table, rows)
                    .await?
                    .ok_or_else(|| {
                    PlatformError::NotFound(format!("{} '{}' not found", table_name, id))
                })?;
                emissions.push(outlet.emission(&Change {
                    kind: ChangeKind::Updated,
                    row: &row,
                    record_id: id,
                    scope: rows.scoped(),
                    before: before.as_ref(),
                }));
                op_results.push(Some(row));
                updated += 1;
            }
            PreparedOp::Delete { id, existing: _ } => {
                let mut params = keyed(named_key(table, id)?);
                let filter = row_filter(table, rows, true, &mut params)?;
                let sql = delete_returning(&schema, &table_name, &shape, pk, &filter);
                let gone = txn
                    .query_opt(&sql, &params.as_refs())
                    .await
                    .map_err(db(CrudOp::Delete))?
                    .map(|row| row_to_json(&row, &table.columns, None));
                match gone {
                    Some(row) => emissions.push(outlet.emission(&Change {
                        kind: ChangeKind::Deleted,
                        row: &row,
                        record_id: id,
                        scope: rows.scoped(),
                        before: None,
                    })),
                    None if rows.scoped().is_some() => {
                        return Err(PlatformError::NotFound(format!(
                            "{} '{}' not found",
                            table_name, id
                        )))
                    }
                    None => {}
                }
                op_results.push(None);
                deleted += 1;
            }
        }
    }

    let kept = keeper
        .as_ref()
        .map(|keeper| {
            keep_batch_after_hooks(
                keeper,
                table,
                &table_name,
                &batch.operations,
                &batch_rows,
                &prepared,
                &op_results,
            )
        })
        .transpose()?
        .unwrap_or_default();
    outlet.enqueue(&txn, &emissions).await?;
    txn.commit().await.map_err(db(CrudOp::Commit))?;
    outlet.publish_detached(emissions);
    spawn_after_hooks(&state, &service, kept, call.as_ref());

    let results: Vec<Value> = op_results
        .into_iter()
        .flatten()
        .map(|row| columns.shown(row))
        .collect();
    Ok(Json(json!({
        "results": results,
        "summary": {
            "created": created,
            "updated": updated,
            "deleted": deleted,
            "total": batch.operations.len(),
        }
    })))
}

#[allow(clippy::too_many_arguments)]
async fn run_batch_before_hooks(
    state: &Arc<AppState>,
    service: &RegisteredService,
    shape: &Shape<'_>,
    pk: &ColumnDefinition,
    schema: &str,
    table_name: &str,
    tenant_pool: &ServiceSessions,
    auth_user: &Option<crate::auth::AuthUser>,
    call: Option<&ServiceCaller>,
    operations: &[BatchOperation],
    batch_rows: &BatchRows,
) -> PlatformResult<Vec<PreparedOp>> {
    let table = shape.table;
    let existing_rows = fetch_existing_for_batch(
        tenant_pool,
        shape,
        schema,
        table_name,
        pk,
        operations,
        batch_rows,
    )
    .await?;

    let auth_json = if table.hooks.is_some() {
        Some(guest_auth_json(state, auth_user, service.tenant_id).await)
    } else {
        None
    };

    let mut prepared = Vec::with_capacity(operations.len());
    for (op, existing) in operations.iter().zip(existing_rows) {
        let rows = batch_rows.of(op)?;
        match op {
            BatchOperation::Create { data } => {
                let mut data = data.clone();
                if let Some(ref hooks) = table.hooks {
                    if let Some(ref hook_name) = hooks.before_create {
                        let resp = execute_hook(
                            state,
                            service,
                            hook_name,
                            with_scope(
                                json!({
                                    "hook": "before_create",
                                    "table": table_name,
                                    "data": data,
                                }),
                                rows,
                            ),
                            auth_json.clone(),
                            call,
                        )
                        .await?;
                        if let Some(modified) = resp.get("data") {
                            data = modified.clone();
                        }
                    }
                }
                let mut data = data.as_object().cloned().ok_or_else(|| {
                    PlatformError::BadRequest(
                        "Each create operation data must be a JSON object".into(),
                    )
                })?;
                let owned = place_row(table, rows, &mut data)?;
                if let Some((_, None)) = owned {
                    return Err(PlatformError::Unauthorized(
                        "Authentication required for this resource".into(),
                    ));
                }
                prepared.push(PreparedOp::Create { data, owned });
            }
            BatchOperation::Update { id, data } => {
                let mut data = data.clone();
                if let Some(ref hooks) = table.hooks {
                    if let Some(ref hook_name) = hooks.before_update {
                        let existing = existing.unwrap_or(Value::Null);
                        let resp = execute_hook(
                            state,
                            service,
                            hook_name,
                            with_scope(
                                json!({
                                    "hook": "before_update",
                                    "table": table_name,
                                    "id": id,
                                    "data": data,
                                    "existing": existing,
                                }),
                                rows,
                            ),
                            auth_json.clone(),
                            call,
                        )
                        .await?;
                        if let Some(modified) = resp.get("data") {
                            data = modified.clone();
                        }
                    }
                }
                prepared.push(PreparedOp::Update {
                    id: id.clone(),
                    data,
                });
            }
            BatchOperation::Delete { id } => {
                let existing = existing.unwrap_or(Value::Null);

                if let Some(ref hooks) = table.hooks {
                    if let Some(ref hook_name) = hooks.before_delete {
                        execute_hook(
                            state,
                            service,
                            hook_name,
                            with_scope(
                                json!({
                                    "hook": "before_delete",
                                    "table": table_name,
                                    "id": id,
                                    "existing": existing,
                                }),
                                rows,
                            ),
                            auth_json.clone(),
                            call,
                        )
                        .await?;
                    }
                }
                prepared.push(PreparedOp::Delete {
                    id: id.clone(),
                    existing,
                });
            }
        }
    }
    Ok(prepared)
}

fn fires_after(hooks: &cufflink_types::TableHooks) -> bool {
    hooks.after_create.is_some() || hooks.after_update.is_some() || hooks.after_delete.is_some()
}

fn fires_before(op: &BatchOperation, hooks: &cufflink_types::TableHooks) -> bool {
    match op {
        BatchOperation::Create { .. } => hooks.before_create.is_some(),
        BatchOperation::Update { .. } => hooks.before_update.is_some(),
        BatchOperation::Delete { .. } => hooks.before_delete.is_some(),
    }
}

struct BatchHooks<'a> {
    hooks: &'a cufflink_types::TableHooks,
    table_name: &'a str,
    batch_rows: &'a BatchRows,
}

impl<'a> BatchHooks<'a> {
    fn body(
        &self,
        op: &BatchOperation,
        prepared: &PreparedOp,
        result: &Option<Value>,
    ) -> Option<(&'a str, Value)> {
        let table_name = self.table_name;
        let (hook, body) = match prepared {
            PreparedOp::Create { data, .. } => (
                &self.hooks.after_create,
                json!({
                    "hook": "after_create",
                    "table": table_name,
                    "data": data,
                    "result": result.clone().unwrap_or(Value::Null),
                }),
            ),
            PreparedOp::Update { id, data } => (
                &self.hooks.after_update,
                json!({
                    "hook": "after_update",
                    "table": table_name,
                    "id": id,
                    "data": data,
                    "result": result.clone().unwrap_or(Value::Null),
                }),
            ),
            PreparedOp::Delete { id, existing } => (
                &self.hooks.after_delete,
                json!({
                    "hook": "after_delete",
                    "table": table_name,
                    "id": id,
                    "existing": existing,
                }),
            ),
        };
        hook.as_deref().map(|name| {
            let body = match self.batch_rows.of(op) {
                Ok(rows) => with_scope(body, rows),
                Err(_) => body,
            };
            (name, body)
        })
    }
}

fn keep_batch_after_hooks(
    keeper: &Keeper<'_>,
    table: &TableDefinition,
    table_name: &str,
    operations: &[BatchOperation],
    batch_rows: &BatchRows,
    prepared: &[PreparedOp],
    op_results: &[Option<Value>],
) -> PlatformResult<Vec<KeptHook>> {
    let Some(hooks) = table.hooks.as_ref() else {
        return Ok(Vec::new());
    };
    let batch = BatchHooks {
        hooks,
        table_name,
        batch_rows,
    };
    let bodies = || {
        operations
            .iter()
            .zip(prepared)
            .zip(op_results)
            .filter_map(|((op, prepared), result)| batch.body(op, prepared, result))
    };
    bodies()
        .map(|(name, body)| keeper.keep(name, body))
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|refused| {
            let needs = bodies().map(|(_, body)| ChargedJson::measure(&body)).sum();
            keeper.refused(refused, needs)
        })
}

fn keeps_first(after: Option<&AfterHook<'_>>) -> bool {
    after.is_some_and(|after| after.keeper.refuses())
}

fn spawn_after_hooks(
    state: &Arc<AppState>,
    service: &Arc<RegisteredService>,
    kept: impl IntoIterator<Item = KeptHook>,
    call: Option<&ServiceCaller>,
) {
    for hook in kept {
        let (state, service, call) = (state.clone(), service.clone(), call.cloned());
        let shutdown = state.shutdown.clone();
        shutdown.spawn("after_hook", async move {
            execute_after_hook(&state, &service, &hook.name, hook.request, call.as_ref()).await;
        });
    }
}

fn needs_existing_row(op: &BatchOperation, hooks: Option<&cufflink_types::TableHooks>) -> bool {
    hooks.is_some_and(|h| match op {
        BatchOperation::Create { .. } => false,
        BatchOperation::Update { .. } => h.before_update.is_some(),
        BatchOperation::Delete { .. } => h.before_delete.is_some() || h.after_delete.is_some(),
    })
}

async fn fetch_existing_for_batch(
    tenant_pool: &ServiceSessions,
    shape: &Shape<'_>,
    schema: &str,
    table_name: &str,
    pk: &ColumnDefinition,
    operations: &[BatchOperation],
    batch_rows: &BatchRows,
) -> PlatformResult<Vec<Option<Value>>> {
    let hooks = shape.table.hooks.as_ref();
    if !operations.iter().any(|op| needs_existing_row(op, hooks)) {
        return Ok(vec![None; operations.len()]);
    }
    let client = tenant_pool.get().await?;
    let mut rows = Vec::with_capacity(operations.len());
    for op in operations {
        let row = match op {
            BatchOperation::Update { id, .. } | BatchOperation::Delete { id }
                if needs_existing_row(op, hooks) =>
            {
                Some(
                    fetch_existing_for_hook(
                        &client,
                        schema,
                        table_name,
                        shape,
                        pk,
                        id,
                        RowVisibility::NotSoftDeleted,
                        batch_rows.of(op)?.scoped(),
                    )
                    .await?,
                )
            }
            _ => None,
        };
        rows.push(row);
    }
    Ok(rows)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowVisibility {
    All,
    NotSoftDeleted,
}

#[allow(clippy::too_many_arguments)]
async fn fetch_existing_for_hook<C: Statements>(
    client: &C,
    schema: &str,
    table_name: &str,
    shape: &Shape<'_>,
    pk: &ColumnDefinition,
    id: &str,
    visibility: RowVisibility,
    scope: Option<&EffectiveScope>,
) -> PlatformResult<Value> {
    let table = shape.table;
    let mut params = keyed(named_key(table, id)?);
    let soft_delete = (table.soft_delete && visibility == RowVisibility::NotSoftDeleted)
        .then(|| "deleted_at IS NULL".to_string());
    let reachable = scope
        .map(|scope| scope.where_sql(&table.columns, &mut params))
        .transpose()?
        .flatten();
    let extra: String = soft_delete
        .into_iter()
        .chain(reachable)
        .map(|condition| format!(" AND {condition}"))
        .collect();
    let sql = select_statement(schema, table_name, shape, pk, &extra);
    let row = client
        .query_opt(&sql, &params.as_refs())
        .await
        .map_err(db(CrudOp::Read))?
        .ok_or_else(|| PlatformError::NotFound(format!("{} '{}' not found", table_name, id)))?;
    Ok(row_to_json(&row, &table.columns, None))
}

fn validate_fields(
    body: &Map<String, Value>,
    columns: &[ColumnDefinition],
) -> Result<(), PlatformError> {
    for col in columns {
        if col.validations.is_empty() {
            continue;
        }
        if let Some(val) = body.get(&col.name) {
            for rule in &col.validations {
                validate_value(&col.name, val, rule)?;
            }
        }
    }
    Ok(())
}

fn validate_value(field: &str, val: &Value, rule: &ValidationRule) -> Result<(), PlatformError> {
    match rule {
        ValidationRule::Regex { pattern } => {
            if let Some(s) = val.as_str() {
                let re = regex::Regex::new(pattern).map_err(|e| {
                    PlatformError::Validation(format!(
                        "Invalid regex pattern for field '{}': {}",
                        field, e
                    ))
                })?;
                if !re.is_match(s) {
                    return Err(PlatformError::Validation(format!(
                        "Field '{}' does not match pattern",
                        field
                    )));
                }
            }
        }
        ValidationRule::Min { value } => {
            if let Some(n) = val.as_f64() {
                if n < *value {
                    return Err(PlatformError::Validation(format!(
                        "Field '{}' must be >= {}",
                        field, value
                    )));
                }
            }
        }
        ValidationRule::Max { value } => {
            if let Some(n) = val.as_f64() {
                if n > *value {
                    return Err(PlatformError::Validation(format!(
                        "Field '{}' must be <= {}",
                        field, value
                    )));
                }
            }
        }
        ValidationRule::MinLength { value } => {
            if let Some(s) = val.as_str() {
                if s.len() < *value {
                    return Err(PlatformError::Validation(format!(
                        "Field '{}' must be at least {} characters",
                        field, value
                    )));
                }
            }
        }
        ValidationRule::MaxLength { value } => {
            if let Some(s) = val.as_str() {
                if s.len() > *value {
                    return Err(PlatformError::Validation(format!(
                        "Field '{}' must be at most {} characters",
                        field, value
                    )));
                }
            }
        }
        ValidationRule::OneOf { values } => {
            if let Some(s) = val.as_str() {
                if !values.contains(&s.to_string()) {
                    return Err(PlatformError::Validation(format!(
                        "Field '{}' must be one of: {}",
                        field,
                        values.join(", ")
                    )));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod wrap_hook_request_tests {
    use super::*;
    use cufflink_types::{ServiceManifest, ServiceMode};

    fn hook_json(
        body: Value,
        hook_name: &str,
        service: &RegisteredService,
        auth_json: Option<Value>,
        call: Option<&ServiceCaller>,
    ) -> Value {
        wrap_hook_request(body, hook_name, service, auth_json, call)
            .to_value()
            .expect("request")
    }

    #[test]
    fn a_hook_reads_the_text_it_always_read() {
        for (body, hook, auth, text) in [
            (
                json!({"hook": "before_create", "table": "items", "data": {"title": "x", "amount": 3}}),
                "validate_item",
                None,
                r#"{"auth":null,"body":{"data":{"amount":3,"title":"x"},"hook":"before_create","table":"items"},"handler":"validate_item","headers":{},"method":"HOOK","service":"ferry-service","tenant":"acme"}"#,
            ),
            (
                json!({"hook": "after_update", "table": "items", "data": {"title": "x"}, "previous": {"title": "w"}}),
                "audit_item",
                Some(json!({"sub": "u-1", "roles": ["staff"]})),
                r#"{"auth":{"roles":["staff"],"sub":"u-1"},"body":{"data":{"title":"x"},"hook":"after_update","previous":{"title":"w"},"table":"items"},"handler":"audit_item","headers":{},"method":"HOOK","service":"ferry-service","tenant":"acme"}"#,
            ),
        ] {
            assert_eq!(
                hook_request_text(body, hook, &fake_service(), auth, None, &mut Hold::none())
                    .expect("text"),
                text
            );
        }
    }

    #[test]
    fn a_row_too_deep_for_its_envelope_fails_as_syntax_and_never_as_capacity() {
        let deep = (0..125).fold(json!(1), |inner, _| Value::Array(vec![inner]));
        let row: Value = serde_json::from_str(&json!({"meta": deep}).to_string())
            .expect("the request body parses");
        let body = json!({"hook": "after_create", "table": "items", "data": row, "result": row});
        assert!(matches!(
            hook_request(body, "h", &fake_service(), Hold::none(), None, None),
            Err(ChargedJsonError::Syntax(_))
        ));
    }

    #[test]
    fn a_hook_request_holds_its_text_and_not_a_whole_step() {
        use crate::wasm_runtime::budget::testing;

        let (ledger, hold) = testing::hold(4 * testing::MIB);
        let request = hook_request(
            json!({"hook": "after_create"}),
            "h",
            &fake_service(),
            hold,
            None,
            None,
        )
        .expect("request");
        assert_eq!(request.hold.bytes(), request.text.capacity());
        assert!(request.hold.bytes() < 1024, "{}", request.hold.bytes());
        assert_eq!(ledger.in_use(), request.hold.bytes());
    }

    fn fake_service() -> RegisteredService {
        RegisteredService {
            service_id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            name: "ferry-service".into(),
            manifest: ServiceManifest {
                name: "ferry-service".into(),
                version: None,
                tables: vec![],
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
            },
            version: 1,
            wasm_hash: None,
            commit_hash: None,
        }
    }

    #[test]
    fn wraps_envelope_under_body_with_handler_metadata() {
        let envelope = json!({
            "hook": "after_create",
            "table": "sailing_requests",
            "result": {"id": "abc", "customer_id": "c1"},
        });
        let auth = json!({"sub": "c1"});

        let wrapped = hook_json(
            envelope.clone(),
            "handle_sailing_after_create",
            &fake_service(),
            Some(auth.clone()),
            None,
        );

        assert_eq!(wrapped["body"], envelope);
        assert_eq!(wrapped["auth"], auth);
        assert_eq!(wrapped["handler"], "handle_sailing_after_create");
        assert_eq!(wrapped["method"], "HOOK");
        assert_eq!(wrapped["tenant"], "acme");
        assert_eq!(wrapped["service"], "ferry-service");
        assert!(wrapped.get("caller").is_none());
    }

    #[test]
    fn a_hook_request_has_the_keys_it_always_had() {
        assert_eq!(
            hook_json(
                json!({"hook": "before_create", "data": {"id": 7}}),
                "validate",
                &fake_service(),
                None,
                None,
            ),
            json!({
                "method": "HOOK",
                "handler": "validate",
                "headers": {},
                "body": {"hook": "before_create", "data": {"id": 7}},
                "tenant": "acme",
                "service": "ferry-service",
                "auth": null,
            })
        );
    }

    #[test]
    fn missing_auth_serialises_as_null() {
        let wrapped = hook_json(
            json!({"hook": "on_migrate"}),
            "h",
            &fake_service(),
            None,
            None,
        );
        assert_eq!(wrapped["auth"], Value::Null);
    }

    #[test]
    fn a_hook_for_a_service_call_carries_its_caller() {
        let call = ServiceCaller {
            service: "pricing".into(),
            on_behalf_of: None,
            depth: 3,
            remaining: std::time::Duration::from_secs(9),
        };
        let wrapped = hook_json(
            json!({"hook": "before_create"}),
            "h",
            &fake_service(),
            None,
            Some(&call),
        );
        assert_eq!(
            wrapped["caller"],
            json!({"service": "pricing", "on_behalf_of": null, "depth": 3})
        );
        assert_eq!(
            crate::wasm_runtime::host::caller_depth(&wrapped["caller"]),
            3
        );
    }

    #[test]
    fn hook_method_is_not_a_real_http_verb() {
        let wrapped = hook_json(
            json!({"hook": "after_create"}),
            "h",
            &fake_service(),
            None,
            None,
        );
        assert_eq!(wrapped["method"], "HOOK");
        for verb in ["GET", "POST", "PUT", "PATCH", "DELETE"] {
            assert_ne!(
                wrapped["method"], verb,
                "hook method must not be forgeable as {verb}"
            );
        }
    }
}

#[cfg(test)]
mod data_query_tests {
    use super::DataQuery;

    #[test]
    fn query_strings_never_fail_and_the_first_value_wins() {
        let parsed = |raw: Option<&str>| DataQuery::from_raw(raw);
        assert_eq!(parsed(None), DataQuery::default());
        assert_eq!(
            parsed(Some("scope=own&filter_mode=v2&scope=all&x=%ZZ")),
            DataQuery {
                scope: Some("own".into()),
                filter_mode: Some("v2".into()),
            }
        );
        assert_eq!(parsed(Some("%%%&=&&title=a")), DataQuery::default());
    }
}

#[cfg(test)]
mod hook_response_tests {
    use super::interpret_hook_response;
    use crate::errors::PlatformError;
    use serde_json::json;

    #[test]
    fn plain_reply_passes_through() {
        let out = interpret_hook_response(json!({"data": {"name": "x"}})).unwrap();
        assert_eq!(out["data"]["name"], "x");
    }

    #[test]
    fn bare_error_key_rejects() {
        let err = interpret_hook_response(json!({"error": "name is required"})).unwrap_err();
        assert!(matches!(err, PlatformError::BadRequest(m) if m.contains("name is required")));
    }

    #[test]
    fn wrapped_response_error_rejects_with_status() {
        let err = interpret_hook_response(json!({
            "__status": 400,
            "__body": {"error": "name is required"}
        }))
        .unwrap_err();
        assert!(matches!(err, PlatformError::BadRequest(m) if m.contains("name is required")));

        let err = interpret_hook_response(json!({
            "__status": 403,
            "__body": {"error": "not the owner"}
        }))
        .unwrap_err();
        assert!(matches!(err, PlatformError::Forbidden(_)));

        let err = interpret_hook_response(json!({"__status": 404, "__body": "gone"})).unwrap_err();
        assert!(matches!(err, PlatformError::NotFound(m) if m.contains("gone")));
    }

    #[test]
    fn wrapped_success_is_unwrapped() {
        let out =
            interpret_hook_response(json!({"__status": 201, "__body": {"data": {"ok": true}}}))
                .unwrap();
        assert_eq!(out["data"]["ok"], true);
    }
}

#[cfg(test)]
mod write_tests {
    use super::*;
    use crate::auth::scope::RowPredicate;

    fn accounts() -> TableDefinition {
        serde_json::from_value(json!({
            "name": "accounts",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "auto_generate": true},
                {"name": "user_id", "column_type": "text"},
                {"name": "org_no", "column_type": "integer", "nullable": true}
            ],
            "owner_field": "user_id",
            "scoped_by": "accounts"
        }))
        .expect("table")
    }

    fn org(values: &[i64]) -> RowScope {
        RowScope::Scoped(EffectiveScope::Rows(vec![RowPredicate::Claim {
            column: "org_no".into(),
            values: values.iter().map(|v| json!(v)).collect(),
        }]))
    }

    #[test]
    fn a_stored_row_outside_a_confining_scope_is_refused() {
        let table = accounts();
        let stored = |org_no: i64| json!({"user_id": "u", "org_no": org_no});
        assert_eq!(
            confined(stored(5), &table, &org(&[5])).ok(),
            Some(stored(5))
        );
        assert!(matches!(
            confined(stored(0), &table, &org(&[5])),
            Err(PlatformError::Forbidden(_))
        ));
        for unconfined in [
            RowScope::Scoped(EffectiveScope::All),
            RowScope::Legacy { owner: None },
        ] {
            assert!(confined(stored(0), &table, &unconfined).is_ok());
        }
    }

    #[test]
    fn scoped_and_strict_writes_bind_typed_values_and_nulls() {
        let table = accounts();
        let org_no = &table.columns[2];
        assert_eq!(WriteBinding::of(false, &org(&[5])), WriteBinding::Typed);
        assert_eq!(
            WriteBinding::of(true, &RowScope::Legacy { owner: None }),
            WriteBinding::Typed
        );
        assert_eq!(
            WriteBinding::of(false, &RowScope::Legacy { owner: None }),
            WriteBinding::Coerced
        );
        let bound = |binding: WriteBinding, value: Value| {
            binding.bind(org_no, &value).map(|v| format!("{v:?}"))
        };
        assert_eq!(
            bound(WriteBinding::Typed, json!(" 5")).ok(),
            Some("5".into())
        );
        assert_eq!(
            bound(WriteBinding::Typed, json!("5x")).map_err(|e| e.to_string()),
            Err(r#"Bad request: column 'org_no': expected a 32-bit integer, got "5x""#.into())
        );
        assert_eq!(
            bound(WriteBinding::Typed, Value::Null).ok(),
            Some("None".into())
        );
        assert_eq!(
            bound(WriteBinding::Coerced, json!(" 5")).ok(),
            Some("0".into())
        );
    }

    #[test]
    fn statements_bind_through_the_write_binding() {
        let table = accounts();
        let made = Shape::made(&table);
        let row = json!({"user_id": "u", "org_no": "5x"});
        let row = row.as_object().expect("object");
        assert!(matches!(
            insert_statement("s", "accounts", &made, row, None, WriteBinding::Typed),
            Err(PlatformError::BadRequest(_))
        ));
        assert!(insert_statement("s", "accounts", &made, row, None, WriteBinding::Coerced).is_ok());
        let scoped = org(&[5]);
        assert!(matches!(
            update_statement(
                "s",
                "accounts",
                &made,
                row,
                PkValue::Uuid(Uuid::nil()),
                &scoped,
                WriteBinding::Typed,
                false,
            ),
            Err(PlatformError::BadRequest(_))
        ));
    }

    fn statement(built: PlatformResult<(String, SqlParams)>) -> String {
        built.expect("a statement").0
    }

    #[test]
    fn a_table_the_platform_made_is_written_as_it_always_was() {
        let table = accounts();
        let made = Shape::made(&table);
        let row = json!({"user_id": "u", "org_no": 5});
        let row = row.as_object().expect("object");
        let unscoped = RowScope::Legacy { owner: None };
        assert_eq!(
            statement(insert_statement(
                "s",
                "accounts",
                &made,
                row,
                None,
                WriteBinding::Coerced
            )),
            "INSERT INTO s.accounts (id, user_id, org_no) VALUES ($1, $2, $3) RETURNING *"
        );
        assert_eq!(
            statement(update_statement(
                "s",
                "accounts",
                &made,
                row,
                PkValue::Uuid(Uuid::nil()),
                &unscoped,
                WriteBinding::Coerced,
                false,
            )),
            "UPDATE s.accounts SET org_no = $1 WHERE id = $2 RETURNING *"
        );
        let pk = primary_key(&table).expect("key");
        assert_eq!(
            select_statement("s", "accounts", &made, pk, " AND deleted_at IS NULL"),
            "SELECT * FROM s.accounts WHERE id = $1 AND deleted_at IS NULL"
        );
        assert_eq!(
            delete_returning("s", "accounts", &made, pk, ""),
            "DELETE FROM s.accounts WHERE id = $1 RETURNING *"
        );
        let soft = TableDefinition {
            soft_delete: true,
            ..accounts()
        };
        assert_eq!(
            delete_returning("s", "accounts", &Shape::made(&soft), pk, " AND org_no = $2"),
            "UPDATE s.accounts AS live SET deleted_at = now() \
             FROM (SELECT * FROM s.accounts WHERE id = $1 AND org_no = $2 FOR UPDATE) AS was \
             WHERE live.id = was.id RETURNING was.*"
        );
    }

    fn books() -> TableDefinition {
        serde_json::from_value(json!({
            "name": "book",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "auto_generate": true},
                {"name": "kind", "column_type": "text"},
                {"name": "status", "column_type": "text"},
                {"name": "title", "column_type": "text", "nullable": true},
                {"name": "rank", "column_type": "integer"}
            ],
            "soft_delete": true
        }))
        .expect("table")
    }

    fn adopted(tables: Value) -> Arc<AdoptedSchema> {
        Arc::new(serde_json::from_value(tables).expect("an adopted schema"))
    }

    fn adopted_books() -> Arc<AdoptedSchema> {
        let column = |type_name: &str, typtype: &str, nullable: bool, has_default: bool| json!({"type": type_name, "typtype": typtype, "nullable": nullable, "has_default": has_default});
        adopted(json!({"book": {
            "id": column("uuid", "base", false, true),
            "kind": column("catalog_service.book_kind", "enum", false, false),
            "status": column("catalog_service.status", "domain", false, true),
            "title": column("text", "base", true, false),
            "rank": column("integer", "base", false, true),
        }}))
    }

    #[test]
    fn an_adopted_table_is_written_as_the_database_holds_it() {
        let table = books();
        let shape = Shape::adopted(&table, adopted_books());
        let row = json!({"kind": "sale", "title": "first"});
        let row = row.as_object().expect("object");
        let unscoped = RowScope::Legacy { owner: None };
        let read = "\"id\", \"kind\"::TEXT AS \"kind\", \"status\"::TEXT AS \"status\", \
                    \"title\", \"rank\"";
        assert_eq!(
            statement(insert_statement(
                "s",
                "book",
                &shape,
                row,
                None,
                WriteBinding::Coerced
            )),
            format!(
                "INSERT INTO s.book (kind, title) \
                 VALUES ($1::text::catalog_service.book_kind, $2) RETURNING {read}"
            ),
            "the database gives the key, the status and the rank"
        );
        assert_eq!(
            statement(update_statement(
                "s",
                "book",
                &shape,
                json!({"status": "won"}).as_object().expect("object"),
                PkValue::Uuid(Uuid::nil()),
                &unscoped,
                WriteBinding::Coerced,
                false,
            )),
            format!("UPDATE s.book SET status = $1 WHERE id = $2 RETURNING {read}"),
            "a domain takes what its base type takes"
        );
        let pk = primary_key(&table).expect("key");
        assert_eq!(
            select_statement("s", "book", &shape, pk, ""),
            format!("SELECT {read} FROM s.book WHERE id = $1")
        );
        assert!(delete_returning("s", "book", &shape, pk, "").ends_with(
            "RETURNING was.\"id\", was.\"kind\"::TEXT AS \"kind\", \
             was.\"status\"::TEXT AS \"status\", was.\"title\", was.\"rank\""
        ));
        let missing = insert_statement(
            "s",
            "book",
            &shape,
            json!({"title": "no kind"}).as_object().expect("object"),
            None,
            WriteBinding::Coerced,
        );
        assert!(
            matches!(&missing, Err(PlatformError::Validation(m)) if m == "Missing required field: kind"),
            "{:?}",
            missing.map(|built| built.0)
        );
    }

    #[test]
    fn an_adopted_key_is_given_by_the_platform_only_where_the_database_gives_none() {
        let table = books();
        let column = |has_default: bool| json!({"type": "uuid", "typtype": "base", "nullable": false, "has_default": has_default});
        let pk = primary_key(&table).expect("key");
        let keyed = |has_default| {
            Shape::adopted(
                &table,
                adopted(json!({"book": {"id": column(has_default)}})),
            )
            .generates_key(pk)
        };
        assert!(keyed(false));
        assert!(!keyed(true));
        assert!(Shape::made(&table).generates_key(pk));
        let singleton: TableDefinition = serde_json::from_value(json!({
            "name": "settings",
            "columns": [
                {"name": "id", "column_type": "integer", "primary_key": true, "auto_generate": true},
                {"name": "hot_book_days", "column_type": "integer"}
            ]
        }))
        .expect("table");
        let integer = json!({"settings": {
            "id": {"type": "integer", "typtype": "base", "nullable": false, "has_default": false},
        }});
        let pk = primary_key(&singleton).expect("key");
        assert!(
            !Shape::adopted(&singleton, adopted(integer)).generates_key(pk),
            "the platform makes no key but a uuid"
        );
    }

    #[test]
    fn an_update_of_a_table_that_records_it_answers_the_row_as_it_was() {
        let update = |shape: &Shape<'_>, rows: &RowScope, recorded: bool| {
            statement(update_statement(
                "s",
                &shape.table.name,
                shape,
                json!({"org_no": 7}).as_object().expect("object"),
                PkValue::Uuid(Uuid::nil()),
                rows,
                WriteBinding::Typed,
                recorded,
            ))
        };
        let sensitive = TableDefinition {
            sensitive: true,
            ..accounts()
        };
        let unscoped = RowScope::Legacy { owner: None };
        assert_eq!(
            update(&Shape::made(&sensitive), &unscoped, false),
            "UPDATE s.accounts SET org_no = $1 WHERE id = $2 RETURNING *",
            "a change that is recorded nowhere durably reads nothing of what it was"
        );
        let before = "old.\"id\" AS \"__previous.id\", old.\"user_id\" AS \"__previous.user_id\", \
                      old.\"org_no\" AS \"__previous.org_no\"";
        assert_eq!(
            update(&Shape::made(&sensitive), &unscoped, true),
            format!(
                "WITH old AS (SELECT * FROM s.accounts WHERE id = $2 FOR UPDATE) \
                 UPDATE s.accounts AS live SET org_no = $1 FROM old \
                 WHERE live.id = old.id RETURNING live.*, {before}"
            )
        );
        assert_eq!(
            update(&Shape::made(&sensitive), &org(&[5]), true),
            format!(
                "WITH old AS (SELECT * FROM s.accounts \
                 WHERE id = $2 AND (\"org_no\" = ANY($3)) FOR UPDATE) \
                 UPDATE s.accounts AS live SET org_no = $1 FROM old \
                 WHERE live.id = old.id RETURNING live.*, {before}"
            )
        );
        let recorded_books = TableDefinition {
            sensitive: true,
            soft_delete: false,
            ..books()
        };
        assert!(
            statement(update_statement(
                "s",
                "book",
                &Shape::adopted(&recorded_books, adopted_books()),
                json!({"status": "won"}).as_object().expect("object"),
                PkValue::Uuid(Uuid::nil()),
                &unscoped,
                WriteBinding::Coerced,
                true,
            ))
            .ends_with(
                "RETURNING live.\"id\", live.\"kind\"::TEXT AS \"kind\", \
                 live.\"status\"::TEXT AS \"status\", live.\"title\", live.\"rank\", \
                 old.\"id\" AS \"__previous.id\", old.\"kind\"::TEXT AS \"__previous.kind\", \
                 old.\"status\"::TEXT AS \"__previous.status\", \
                 old.\"title\" AS \"__previous.title\", old.\"rank\" AS \"__previous.rank\""
            ),
            "the row as it was is read as the row is: its declared columns, as they are declared"
        );
    }

    #[test]
    fn a_listing_is_recorded_with_the_columns_it_was_filtered_by() {
        let table = accounts();
        let filtered = |filter: Option<&str>, extra: &[(&str, &str)]| {
            filter_columns(
                &table,
                &ListParams {
                    filter: filter.map(str::to_string),
                    extra: extra
                        .iter()
                        .map(|(name, value)| (name.to_string(), value.to_string()))
                        .collect(),
                    ..ListParams::default()
                },
            )
        };
        assert!(filtered(None, &[]).is_empty());
        assert_eq!(filtered(None, &[("org_no", "5")]), ["org_no"]);
        assert_eq!(
            filtered(
                Some(r#"[{"n":"org_no","f":"=","v":"5"},{"n":"absent","f":"=","v":"x"}]"#),
                &[
                    ("user_id", "u-1"),
                    ("org_nothing", "1"),
                    ("page_token", "x")
                ]
            ),
            ["user_id", "org_no"],
            "in the order of the table's columns, and none it has not"
        );
        assert_eq!(
            filtered(
                Some(
                    r#"{"and": [{"f": "org_no", "op": "gte", "v": 5},
                                {"or": [{"not": {"f": "user_id", "op": "is_null"}},
                                        {"f": "absent", "op": "eq", "v": 1}]}]}"#
                ),
                &[]
            ),
            ["user_id", "org_no"],
            "of a tree of conditions, each that is nested in it"
        );
        assert_eq!(
            filtered(
                Some(
                    r#"[{"f": "org_no", "op": "eq", "v": 5},
                        {"n": "user_id", "f": "=", "v": "u"}]"#
                ),
                &[]
            ),
            ["user_id", "org_no"]
        );
        assert!(filtered(Some("not json"), &[("idle", "1")]).is_empty());
        assert!(
            filtered(Some(r#"{"f": "org_no", "v": 5}"#), &[]).is_empty(),
            "what has neither a name nor an operator is no condition"
        );
    }

    #[test]
    fn a_created_row_is_recorded_under_the_key_its_table_has() {
        let keyed = |name: &str, column_type: &str| -> TableDefinition {
            serde_json::from_value(json!({
                "name": "kept",
                "columns": [{"name": name, "column_type": column_type, "primary_key": true}]
            }))
            .expect("table")
        };
        let held = |name: &str, type_name: &str| {
            adopted(json!({"kept": {
                name: {"type": type_name, "typtype": "base", "nullable": false, "has_default": false},
            }}))
        };
        let (by_key, by_number) = (keyed("key", "text"), keyed("id", "integer"));
        for (table, adopted, row, made, held) in [
            (
                &by_key,
                held("key", "text"),
                json!({"key": "purchase"}),
                "",
                "purchase",
            ),
            (&by_number, held("id", "integer"), json!({"id": 7}), "", "7"),
            (
                &by_key,
                held("key", "text"),
                json!({"id": "a", "key": "b"}),
                "a",
                "b",
            ),
            (&by_number, held("id", "integer"), json!({"n": 1}), "", ""),
        ] {
            let pk = primary_key(table).expect("key");
            assert_eq!(Shape::made(table).created_key(&row, pk), made, "{row}");
            assert_eq!(
                Shape::adopted(table, adopted).created_key(&row, pk),
                held,
                "{row}"
            );
        }
    }

    #[test]
    fn a_key_is_refused_as_it_always_was_and_a_batch_names_it() {
        let table = accounts();
        let refused = |key: PlatformResult<PkValue>| match key {
            Err(PlatformError::BadRequest(reason)) => reason,
            other => panic!("{other:?}"),
        };
        assert_eq!(refused(parse_pk(&table, "seven")), "Invalid UUID");
        assert_eq!(refused(named_key(&table, "seven")), "Invalid UUID: seven");
        assert!(named_key(&table, &Uuid::nil().to_string()).is_ok());
    }
}
