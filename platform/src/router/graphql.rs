use crate::audit::Read;
use crate::auth::{AuthUser, OptionalAuth};
use crate::deployer::manifest::{ColumnType, ServiceManifest};
use crate::errors::{CrudOp, PlatformError, PlatformResult};
use crate::policy::{PolicyKey, Violation};
use crate::registry::RegisteredService;
use crate::router::authz::{authorize_access, resolve_data_route, Access, DataRoute, DataTarget};
use crate::router::crud::{outlet_of, Asked, Shape};
use crate::router::query::SqlParams;
use crate::router::row_convert::{row_to_json, string_param};
use crate::tenant_catalog::TenantEntry;
use crate::tenant_sessions::{Purpose, Statements};
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct GraphQLRequest {
    pub query: String,
}

pub(crate) fn graphql_enabled(
    service: &RegisteredService,
    tenant: &TenantEntry,
) -> PlatformResult<()> {
    let disabled = || PlatformError::NotFound("GraphQL is not enabled for this service".into());
    if !service.manifest.graphql {
        return Err(disabled());
    }
    tenant
        .apply(
            PolicyKey::Graphql,
            Some(Violation::new(
                "graphql_request",
                format!("service {}", service.name),
            )),
        )
        .map_err(|_| disabled())
}

pub(crate) async fn graphql_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name)): Path<(String, String)>,
    headers: HeaderMap,
    target: DataTarget,
    OptionalAuth(auth_user): OptionalAuth,
    Json(req): Json<GraphQLRequest>,
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
    graphql_enabled(&service, &tenant)?;

    let client = state
        .tenant_sessions
        .acquire(&service.identity(), Purpose::Crud, None)
        .await?;
    let schema = client.schema_ident();

    let parsed = parse_query(&req.query, &service.manifest)?;

    let mut result_map = serde_json::Map::new();

    for table_query in &parsed {
        let table = service
            .manifest
            .get_table(&table_query.table)
            .ok_or_else(|| {
                PlatformError::BadRequest(format!("Unknown table: {}", table_query.table))
            })?;

        let Access { rows, columns } =
            authorize_access(&state, service.tenant_id, table, "view", &auth_user, None).await?;
        let named = columns.named(table);

        let fields = &table_query.fields;
        if let Some(unknown) = fields
            .iter()
            .find(|field| !named.columns.iter().any(|c| c.name == **field))
        {
            return Err(PlatformError::BadRequest(format!(
                "Unknown field '{}' on table '{}'",
                unknown, table_query.table
            )));
        }
        let asked = |name: &str| fields.is_empty() || fields.iter().any(|field| field == name);
        let select_cols = match Shape::of(&state, &service, table).await?.expressions(None) {
            Some(expressions) => expressions
                .into_iter()
                .filter(|(name, _)| asked(name))
                .map(|(_, expression)| expression)
                .collect::<Vec<_>>()
                .join(", "),
            None if fields.is_empty() => "*".to_string(),
            None => fields.join(", "),
        };

        let mut params = SqlParams::default();
        let mut conditions: Vec<String> = Vec::new();

        if table.soft_delete {
            conditions.push("deleted_at IS NULL".to_string());
        }

        for (key, val) in &table_query.args {
            if let Some(col) = named.columns.iter().find(|c| c.name == *key) {
                let placeholder = params.push(string_param(col, val)?);
                conditions.push(format!("{} = {}", key, placeholder));
            }
        }

        conditions.extend(rows.condition(table, &mut params)?);

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let limit = query_limit(&table_query.args);

        let sql = format!(
            "SELECT {} FROM {}.{} {} LIMIT {}",
            select_cols, schema, table_query.table, where_clause, limit,
        );

        let found = client
            .query(&sql, &params.as_refs())
            .await
            .map_err(crate::router::crud::db(CrudOp::Read))?;

        let results: Vec<Value> = found
            .iter()
            .map(|row| row_to_json(row, &named.columns, Some(fields)))
            .collect();
        if table.sensitive {
            let asked = Asked {
                path: (&tenant_slug, &service_name, &table_query.table),
                caller: &auth_user,
                call: call.as_ref(),
                headers: &headers,
            };
            let key = table.primary_key().map_or("id", |key| key.name.as_str());
            let read = Read {
                filter_columns: named
                    .columns
                    .iter()
                    .filter(|column| table_query.args.contains_key(&column.name))
                    .map(|column| column.name.clone())
                    .collect(),
                ..Read::of_rows(&results, key)
            };
            outlet_of(&state, &tenant, &service, table, asked)
                .read(&client, read, rows.scoped())
                .await?;
        }

        result_map.insert(table_query.table.clone(), json!(results));
    }

    Ok(Json(json!({ "data": result_map })))
}

pub(crate) async fn graphql_schema_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name)): Path<(String, String)>,
    headers: HeaderMap,
    target: DataTarget,
    user: AuthUser,
) -> PlatformResult<String> {
    let DataRoute {
        service, tenant, ..
    } = resolve_data_route(
        &state,
        &headers,
        &target,
        &tenant_slug,
        &service_name,
        Some(user),
    )
    .await?;
    graphql_enabled(&service, &tenant)?;
    Ok(generate_graphql_schema(&service.manifest))
}

#[derive(Debug)]
struct TableQuery {
    table: String,
    fields: Vec<String>,
    args: HashMap<String, String>,
}

type Scanner<'a> = std::iter::Peekable<std::str::Chars<'a>>;

fn skip_whitespace(chars: &mut Scanner) {
    while chars.next_if(|c| c.is_whitespace()).is_some() {}
}

fn delimited(chars: &mut Scanner, open: char, close: char) -> Option<String> {
    chars.next_if_eq(&open)?;
    let mut depth = 1usize;
    Some(
        std::iter::from_fn(|| {
            let c = chars.next()?;
            if c == open {
                depth += 1;
            } else if c == close {
                depth -= 1;
            }
            (depth > 0).then_some(c)
        })
        .collect(),
    )
}

fn parse_args(args: &str) -> HashMap<String, String> {
    args.split(',')
        .filter_map(|part| part.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().trim_matches('"').to_string()))
        .collect()
}

fn parse_query(query: &str, manifest: &ServiceManifest) -> Result<Vec<TableQuery>, PlatformError> {
    let invalid = || PlatformError::BadRequest("Invalid GraphQL query: expected { ... }".into());
    let trimmed = query.trim();
    let body = match trimmed.strip_prefix("query") {
        Some(rest) => rest.split_once('{').map(|(_, body)| body),
        None => trimmed.strip_prefix('{'),
    }
    .ok_or_else(invalid)?;
    let inner = body.strip_suffix('}').ok_or_else(invalid)?;

    let mut queries = Vec::new();
    let mut chars = inner.chars().peekable();
    loop {
        skip_whitespace(&mut chars);
        let table_name: String =
            std::iter::from_fn(|| chars.next_if(|c| c.is_alphanumeric() || *c == '_')).collect();
        if table_name.is_empty() {
            break;
        }
        if manifest.get_table(&table_name).is_none() {
            return Err(PlatformError::BadRequest(format!(
                "Unknown table: {}",
                table_name
            )));
        }
        skip_whitespace(&mut chars);
        let args = delimited(&mut chars, '(', ')')
            .map(|args| parse_args(&args))
            .unwrap_or_default();
        skip_whitespace(&mut chars);
        let fields = delimited(&mut chars, '{', '}')
            .map(|fields| fields.split_whitespace().map(str::to_string).collect())
            .unwrap_or_default();
        queries.push(TableQuery {
            table: table_name,
            fields,
            args,
        });
    }

    if queries.is_empty() {
        return Err(PlatformError::BadRequest("No tables queried".into()));
    }

    Ok(queries)
}

fn query_limit(args: &HashMap<String, String>) -> i64 {
    args.get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(100)
        .clamp(1, 200)
}

fn generate_graphql_schema(manifest: &ServiceManifest) -> String {
    let mut schema = String::new();

    for table in &manifest.tables {
        schema.push_str(&format!("type {} {{\n", pascal_case(&table.name)));
        for col in &table.columns {
            let gql_type =
                column_type_to_graphql(&col.column_type, !col.nullable && !table.withholds(col));
            let asks = table
                .permissions_of(col)
                .and_then(|permissions| permissions.read)
                .map(|permission| format!(" # read with {permission}"))
                .unwrap_or_default();
            schema.push_str(&format!("  {}: {}{}\n", col.name, gql_type, asks));
        }
        schema.push_str("}\n\n");
    }

    schema.push_str("type Query {\n");
    for table in &manifest.tables {
        let type_name = pascal_case(&table.name);
        schema.push_str(&format!("  {}(limit: Int", table.name));
        for col in &table.columns {
            schema.push_str(&format!(
                ", {}: {}",
                col.name,
                column_type_to_graphql_input(&col.column_type)
            ));
        }
        schema.push_str(&format!("): [{}!]!\n", type_name));
    }
    schema.push_str("}\n");

    schema
}

fn column_type_to_graphql(ct: &ColumnType, required: bool) -> String {
    let base = column_type_to_graphql_input(ct);
    if required {
        format!("{}!", base)
    } else {
        base.to_string()
    }
}

fn column_type_to_graphql_input(ct: &ColumnType) -> &'static str {
    match ct {
        ColumnType::Uuid => "ID",
        ColumnType::Text | ColumnType::Varchar { .. } => "String",
        ColumnType::Integer | ColumnType::BigInteger => "Int",
        ColumnType::Float | ColumnType::Double => "Float",
        ColumnType::Boolean => "Boolean",
        ColumnType::Timestamp
        | ColumnType::Date
        | ColumnType::TimestampNaive
        | ColumnType::Time
        | ColumnType::Numeric { .. }
        | ColumnType::Bytea => "String",
        ColumnType::Jsonb => "JSON",
        ColumnType::TextArray => "[String]",
        ColumnType::UuidArray => "[ID]",
        ColumnType::IntegerArray | ColumnType::BigIntegerArray => "[Int]",
        ColumnType::JsonbArray => "[JSON]",
    }
}

fn pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut c = part.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().to_string() + c.as_str(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployer::manifest::*;

    fn test_manifest() -> ServiceManifest {
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

    #[test]
    fn test_parse_simple_query() {
        let manifest = test_manifest();
        let q = "{ todos { id title } }";
        let parsed = parse_query(q, &manifest).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].table, "todos");
        assert_eq!(parsed[0].fields, vec!["id", "title"]);
    }

    #[test]
    fn test_parse_query_with_args() {
        let manifest = test_manifest();
        let q = r#"{ todos(limit: "10") { id title } }"#;
        let parsed = parse_query(q, &manifest).unwrap();
        assert_eq!(parsed[0].args.get("limit"), Some(&"10".to_string()));
    }

    #[test]
    fn test_parse_query_no_fields() {
        let manifest = test_manifest();
        let q = "{ todos }";
        let parsed = parse_query(q, &manifest).unwrap();
        assert_eq!(parsed[0].fields.len(), 0);
    }

    #[test]
    fn malformed_queries_are_refused_not_panicked_on() {
        let manifest = test_manifest();
        for q in [
            "query{",
            "query",
            "{",
            "}",
            "",
            "{ todos é",
            "query é",
            "{ todos(é: \"ü\") { idé }é",
        ] {
            assert!(
                matches!(parse_query(q, &manifest), Err(PlatformError::BadRequest(_))),
                "{q}"
            );
        }
        let parsed = parse_query("{ todos(limit: \"5\") { id title } }", &manifest).expect("valid");
        assert_eq!(parsed[0].fields, vec!["id", "title"]);
        assert_eq!(
            parse_query("query Todos { todos { id } }", &manifest).expect("named")[0].table,
            "todos"
        );
    }

    #[test]
    fn limit_is_clamped() {
        let limit = |value: &str| query_limit(&[("limit".to_string(), value.to_string())].into());
        assert_eq!(limit("-1"), 1);
        assert_eq!(limit("0"), 1);
        assert_eq!(limit("50"), 50);
        assert_eq!(limit("5000"), 200);
        assert_eq!(limit("many"), 100);
        assert_eq!(query_limit(&HashMap::new()), 100);
    }

    #[test]
    fn test_generate_schema() {
        let manifest = test_manifest();
        let schema = generate_graphql_schema(&manifest);
        assert!(schema.contains("type Todos"));
        assert!(schema.contains("id: ID!"));
        assert!(schema.contains("title: String!"));
        assert!(schema.contains("type Query"));
    }

    #[test]
    fn the_schema_names_what_a_restricted_field_is_read_with() {
        let mut manifest = test_manifest();
        let todos = &mut manifest.tables[0];
        todos.permission_area = Some("todos".into());
        todos.columns[1].access = Some(cufflink_types::ColumnAccess {
            read: Some("internal".into()),
            write: Some("internal".into()),
        });
        let schema = generate_graphql_schema(&manifest);
        assert!(
            schema.contains("  title: String # read with todos:internal\n"),
            "{schema}"
        );
        assert!(schema.contains("  id: ID!\n"), "{schema}");
    }

    fn registered(graphql: bool) -> RegisteredService {
        RegisteredService {
            service_id: uuid::Uuid::nil(),
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "default".into(),
            name: "test-service".into(),
            manifest: ServiceManifest {
                graphql,
                ..test_manifest()
            },
            version: 1,
            wasm_hash: None,
            commit_hash: None,
        }
    }

    fn tenant_with(mode: crate::policy::PolicyMode) -> TenantEntry {
        let t = crate::tenant_catalog::tests::tenant("default", Some("orchard"));
        let rows = [crate::tenant_catalog::tests::policy_row(
            t.id,
            PolicyKey::Graphql,
            mode,
        )];
        TenantEntry::new(
            t,
            std::sync::Arc::new(
                crate::policy::PolicySet::builtin_defaults().with_tenant_rows(&rows),
            ),
            Vec::new(),
            Default::default(),
        )
    }

    #[test]
    fn graphql_enabled_requires_manifest_flag() {
        use crate::policy::PolicyMode;
        let err = graphql_enabled(&registered(false), &tenant_with(PolicyMode::Off)).unwrap_err();
        assert!(
            matches!(err, PlatformError::NotFound(ref m) if m == "GraphQL is not enabled for this service")
        );
        assert!(graphql_enabled(&registered(true), &tenant_with(PolicyMode::Off)).is_ok());
        assert!(graphql_enabled(&registered(true), &tenant_with(PolicyMode::Warn)).is_ok());
    }

    #[test]
    fn graphql_enabled_policy_enforce_forces_off() {
        let err = graphql_enabled(
            &registered(true),
            &tenant_with(crate::policy::PolicyMode::Enforce),
        )
        .unwrap_err();
        assert!(matches!(err, PlatformError::NotFound(_)));
    }

    #[test]
    fn owner_condition_uses_owner_param() {
        let mut columns = test_manifest().tables.remove(0).columns;
        columns[1].column_type = ColumnType::Uuid;
        let subject = "1245dc39-fc68-48b2-8e94-60a8eb0b83f5";
        let param =
            crate::router::authz::owner_param(&columns, "title", subject).expect("uuid subject");
        assert_eq!(
            format!("{param:?}"),
            format!("{subject:?}").replace('"', "")
        );
        assert!(crate::router::authz::owner_param(&columns, "title", "google-oauth2|x").is_err());
    }

    #[test]
    fn test_pascal_case() {
        assert_eq!(pascal_case("todos"), "Todos");
        assert_eq!(pascal_case("blog_posts"), "BlogPosts");
        assert_eq!(pascal_case("user_profile_settings"), "UserProfileSettings");
    }
}
