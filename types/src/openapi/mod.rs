//! OpenAPI spec generation from a [`ServiceManifest`].
//!
//! This module is used by both the platform (server-side) and the CLI
//! (local generation without deployment).

mod check;
mod crud;
mod custom;
mod schema;

pub use check::validate;

use crate::{ServiceManifest, TableDefinition};
use custom::Components;
use schema::Nullable;
use serde_json::{json, Map, Value};
use std::fmt;

/// Where a spec's paths are rooted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathStyle<'a> {
    /// Full `/svc/{tenant}/{service}/…` paths: the spec of an undocumented
    /// service, unchanged.
    Absolute { tenant: &'a str },
    /// Paths relative to the `servers`, which default to the service's own
    /// base under `tenant`: what a client generator needs.
    ServerRelative { tenant: &'a str },
}

/// Server-relative once the service documents itself (`openapi` options or
/// any route `doc`), absolute otherwise.
pub fn default_style<'a>(manifest: &ServiceManifest, tenant: &'a str) -> PathStyle<'a> {
    let documented = manifest.openapi.is_some()
        || manifest
            .custom_routes
            .iter()
            .any(|route| route.doc.is_some());
    if documented {
        PathStyle::ServerRelative { tenant }
    } else {
        PathStyle::Absolute { tenant }
    }
}

/// A manifest whose docs cannot form a spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenApiError(pub String);

impl fmt::Display for OpenApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OpenApiError {}

/// The service's OpenAPI 3.1 document.
pub fn generate_openapi(
    manifest: &ServiceManifest,
    style: PathStyle<'_>,
) -> Result<Value, OpenApiError> {
    match style {
        PathStyle::Absolute { tenant } => Ok(absolute(manifest, tenant)),
        PathStyle::ServerRelative { tenant } => server_relative(manifest, tenant),
    }
}

/// The spec a deployed or local service is served with: in its
/// [`default_style`].
pub fn service_spec(manifest: &ServiceManifest, tenant: &str) -> Result<Value, OpenApiError> {
    generate_openapi(manifest, default_style(manifest, tenant))
}

fn insert_operations(
    paths: &mut Map<String, Value>,
    operations: impl IntoIterator<Item = (String, String, Value)>,
) {
    for (path, method, operation) in operations {
        if let Some(obj) = paths
            .entry(path)
            .or_insert_with(|| json!({}))
            .as_object_mut()
        {
            obj.insert(method, operation);
        }
    }
}

fn absolute(manifest: &ServiceManifest, tenant_slug: &str) -> Value {
    let mut paths: Map<String, Value> = manifest
        .tables
        .iter()
        .flat_map(|table| {
            let base = format!("/svc/{}/{}/{}", tenant_slug, manifest.name, table.name);
            crud::table_paths(base.clone(), format!("{base}/:id"), table)
        })
        .collect();
    insert_operations(
        &mut paths,
        manifest
            .custom_routes
            .iter()
            .filter(|route| custom::published(route))
            .map(|route| custom::route_operation(tenant_slug, &manifest.name, route)),
    );
    let schemas: Map<String, Value> = manifest
        .tables
        .iter()
        .flat_map(|table| schema::table_schemas(table, Nullable::Flag))
        .chain(std::iter::once(("Pagination".into(), schema::pagination())))
        .collect();

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": manifest.name,
            "version": manifest.version.as_deref().unwrap_or("0.1.0")
        },
        "paths": Value::Object(paths),
        "components": {
            "schemas": Value::Object(schemas)
        }
    })
}

/// The schemes a CRUD table's operations require.
fn table_security(table: &TableDefinition, scheme: &str) -> Vec<Value> {
    if table.auth_required {
        vec![json!({ scheme: [] })]
    } else {
        Vec::new()
    }
}

fn server_relative(manifest: &ServiceManifest, tenant: &str) -> Result<Value, OpenApiError> {
    validate(manifest)?;
    let options = manifest.openapi.clone().unwrap_or_default();
    let scheme = options.security_scheme();
    let components = Map::from_iter(manifest.components.clone());
    let lookup = Components(&components);

    let custom: Vec<(String, String, Value)> = manifest
        .custom_routes
        .iter()
        .filter(|route| custom::published(route))
        .map(|route| {
            let security = check::route_security(route, scheme);
            custom::relative_operation(manifest, route, &security, &lookup)
        })
        .collect();
    let crud = manifest.tables.iter().flat_map(|table| {
        let security = table_security(table, scheme);
        crud::table_paths(
            format!("/{}", table.name),
            format!("/{}/{{id}}", table.name),
            table,
        )
        .into_iter()
        .map(move |(path, operations)| (path, with_security(operations, &security)))
    });
    let mut paths: Map<String, Value> = crud.collect();
    insert_operations(&mut paths, custom);

    let uses = |name: &str| {
        paths.values().any(|operations| {
            operations
                .as_object()
                .into_iter()
                .flatten()
                .any(|(_, operation)| {
                    operation["security"]
                        .as_array()
                        .is_some_and(|requirements| {
                            requirements.iter().any(|r| r.get(name).is_some())
                        })
                })
        })
    };
    let security_schemes: Map<String, Value> = [
        uses(scheme).then(|| {
            (
                scheme.to_string(),
                json!({ "type": "http", "scheme": "bearer" }),
            )
        }),
        uses(check::API_KEY_SCHEME).then(|| {
            (
                check::API_KEY_SCHEME.to_string(),
                json!({ "type": "apiKey", "in": "header", "name": "X-Api-Key" }),
            )
        }),
    ]
    .into_iter()
    .flatten()
    .collect();
    let schemas: Map<String, Value> = manifest
        .tables
        .iter()
        .flat_map(|table| schema::table_schemas(table, Nullable::TypeUnion))
        .chain(
            (!manifest.tables.is_empty()).then(|| ("Pagination".to_string(), schema::pagination())),
        )
        .chain(components)
        .collect();
    let mut component_map = Map::from_iter([("schemas".to_string(), Value::Object(schemas))]);
    if !security_schemes.is_empty() {
        component_map.insert("securitySchemes".into(), Value::Object(security_schemes));
    }

    let mut info = Map::from_iter([
        (
            "title".to_string(),
            json!(options.title.as_deref().unwrap_or(&manifest.name)),
        ),
        (
            "version".to_string(),
            json!(options
                .version
                .as_deref()
                .or(manifest.version.as_deref())
                .unwrap_or("0.1.0")),
        ),
    ]);
    if let Some(description) = &options.description {
        info.insert("description".into(), json!(description));
    }
    let servers = if options.servers.is_empty() {
        json!([{
            "url": format!("{{origin}}/svc/{{tenant}}/{}", manifest.name),
            "variables": {
                "origin": { "default": "" },
                "tenant": { "default": tenant },
            },
        }])
    } else {
        json!(options.servers)
    };
    let mut document = Map::from_iter([
        ("openapi".to_string(), json!("3.1.0")),
        ("info".to_string(), Value::Object(info)),
        ("servers".to_string(), servers),
        ("paths".to_string(), Value::Object(paths)),
        ("components".to_string(), Value::Object(component_map)),
    ]);
    if !options.tags.is_empty() {
        document.insert("tags".into(), json!(options.tags));
    }
    Ok(Value::Object(document))
}

/// Each operation of a path item with `security`, when there is any.
fn with_security(mut operations: Value, security: &[Value]) -> Value {
    if !security.is_empty() {
        operations
            .as_object_mut()
            .into_iter()
            .flat_map(|operations| operations.values_mut())
            .filter_map(Value::as_object_mut)
            .for_each(|operation| {
                operation.insert("security".into(), json!(security));
            });
    }
    operations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ColumnDefinition, ColumnType, CustomRouteDefinition, ServiceManifest, ServiceMode,
        TableDefinition,
    };

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
                    ColumnDefinition {
                        name: "done".into(),
                        column_type: ColumnType::Boolean,
                        primary_key: false,
                        nullable: false,
                        auto_generate: false,
                        default_value: Some("false".into()),
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
    fn test_generates_valid_openapi() {
        let manifest = test_manifest();
        let spec = generate_openapi(
            &manifest,
            PathStyle::Absolute {
                tenant: "test-tenant",
            },
        )
        .expect("spec");

        assert_eq!(spec["openapi"], "3.1.0");
        assert_eq!(spec["info"]["title"], "test-service");
        assert_eq!(spec["info"]["version"], "1.0.0");
    }

    #[test]
    fn test_generates_crud_paths() {
        let manifest = test_manifest();
        let spec =
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).expect("spec");

        let paths = spec["paths"].as_object().unwrap();
        assert!(paths.contains_key("/svc/acme/test-service/todos"));
        assert!(paths.contains_key("/svc/acme/test-service/todos/:id"));
    }

    #[test]
    fn test_generates_schemas() {
        let manifest = test_manifest();
        let spec =
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).expect("spec");

        let schemas = spec["components"]["schemas"].as_object().unwrap();
        assert!(schemas.contains_key("Todos"));
        assert!(schemas.contains_key("TodosCreate"));
        assert!(schemas.contains_key("Pagination"));
    }

    #[test]
    fn a_restricted_column_carries_its_permissions_and_is_not_required_of_a_response() {
        let mut manifest = test_manifest();
        let todos = &mut manifest.tables[0];
        todos.permission_area = Some("todos".into());
        todos.columns[1].access = Some(crate::ColumnAccess {
            read: Some("internal".into()),
            write: None,
        });
        todos.columns[2].access = Some(crate::ColumnAccess {
            read: None,
            write: Some("manage".into()),
        });
        let spec =
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).expect("spec");
        let schemas = &spec["components"]["schemas"];
        for schema in ["Todos", "TodosCreate"] {
            let properties = &schemas[schema]["properties"];
            assert_eq!(
                properties["title"]["x-cufflink-access"],
                serde_json::json!({"read": "todos:internal"}),
                "{schema}"
            );
            assert_eq!(
                properties["done"]["x-cufflink-access"],
                serde_json::json!({"write": "todos:manage"}),
                "{schema}"
            );
        }
        assert_eq!(
            schemas["Todos"]["required"],
            serde_json::json!(["id", "done"])
        );
        assert_eq!(
            schemas["TodosCreate"]["required"],
            serde_json::json!(["title"])
        );
        let plain = generate_openapi(&test_manifest(), PathStyle::Absolute { tenant: "acme" })
            .expect("spec");
        assert_eq!(
            plain["components"]["schemas"]["Todos"]["required"],
            serde_json::json!(["id", "title", "done"])
        );
        assert!(!plain.to_string().contains("x-cufflink-access"));
    }

    #[test]
    fn rows_require_every_column_and_bodies_follow_the_platform() {
        let mut manifest = test_manifest();
        let table = &mut manifest.tables[0];
        table.owner_field = Some("owner_id".into());
        table.columns.push(ColumnDefinition {
            name: "owner_id".into(),
            column_type: ColumnType::Uuid,
            ..Default::default()
        });
        table.columns.push(ColumnDefinition {
            name: "notes".into(),
            column_type: ColumnType::Jsonb,
            nullable: true,
            ..Default::default()
        });
        let spec = generate_openapi(&manifest, PathStyle::ServerRelative { tenant: "acme" })
            .expect("spec");
        let schemas = &spec["components"]["schemas"];
        assert_eq!(
            schemas["Todos"]["required"],
            serde_json::json!(["id", "title", "done", "owner_id", "notes"])
        );
        assert_eq!(
            schemas["Todos"]["properties"]["notes"],
            serde_json::json!({})
        );
        assert_eq!(
            schemas["TodosCreate"]["required"],
            serde_json::json!(["title"])
        );
        assert_eq!(schemas["TodosUpdate"]["required"], serde_json::json!([]));
        assert!(schemas["TodosUpdate"]["properties"].get("id").is_none());
        let put = spec["paths"]
            .as_object()
            .unwrap()
            .iter()
            .find_map(|(_, item)| item.get("put"));
        assert_eq!(
            put.expect("put")["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/TodosUpdate"
        );
    }

    #[test]
    fn test_create_schema_excludes_auto_generated() {
        let manifest = test_manifest();
        let spec =
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).expect("spec");

        let create_props = spec["components"]["schemas"]["TodosCreate"]["properties"]
            .as_object()
            .unwrap();
        assert!(!create_props.contains_key("id"));
        assert!(create_props.contains_key("title"));
    }

    #[test]
    fn test_custom_routes() {
        let mut manifest = test_manifest();
        manifest.custom_routes.push(CustomRouteDefinition {
            method: "POST".into(),
            path: "/complete/:id".into(),
            handler: "complete_todo".into(),
            job: None,
            posture: None,
            forward_auth: false,
            ..Default::default()
        });

        let spec =
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).expect("spec");
        let paths = spec["paths"].as_object().unwrap();
        assert!(paths.contains_key("/svc/acme/test-service/_fn/complete_todo"));
    }

    #[test]
    fn test_default_version() {
        let mut manifest = test_manifest();
        manifest.version = None;
        let spec =
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).expect("spec");
        assert_eq!(spec["info"]["version"], "0.1.0");
    }

    #[test]
    fn output_matches_the_golden_specs() {
        for (manifest, golden) in [
            (
                include_str!("../../fixtures/sample_manifest.json"),
                include_str!("../../fixtures/openapi/sample.json"),
            ),
            (
                include_str!("../../fixtures/todo_manifest.json"),
                include_str!("../../fixtures/openapi/todo.json"),
            ),
            (
                include_str!("../../fixtures/blog_manifest.json"),
                include_str!("../../fixtures/openapi/blog.json"),
            ),
        ] {
            let manifest: ServiceManifest = serde_json::from_str(manifest).expect("manifest");
            let golden: Value = serde_json::from_str(golden).expect("golden");
            assert_eq!(
                generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).expect("spec"),
                golden,
                "{}",
                manifest.name
            );
        }
    }

    #[test]
    fn test_local_slug() {
        let manifest = test_manifest();
        let spec =
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "local" }).expect("spec");

        let paths = spec["paths"].as_object().unwrap();
        assert!(paths.contains_key("/svc/local/test-service/todos"));
    }

    fn documented_route(
        method: &str,
        path: &str,
        handler: &str,
        posture: Option<crate::RoutePosture>,
        doc: crate::RouteDoc,
    ) -> CustomRouteDefinition {
        CustomRouteDefinition {
            method: method.into(),
            path: path.into(),
            handler: handler.into(),
            posture,
            doc: Some(doc),
            ..Default::default()
        }
    }

    fn relative(manifest: &ServiceManifest) -> Value {
        generate_openapi(manifest, PathStyle::ServerRelative { tenant: "acme" }).expect("spec")
    }

    #[test]
    fn documented_services_default_to_server_relative_paths() {
        let mut manifest = test_manifest();
        assert_eq!(
            default_style(&manifest, "acme"),
            PathStyle::Absolute { tenant: "acme" }
        );
        manifest.custom_routes.push(documented_route(
            "GET",
            "/x",
            "x",
            None,
            crate::RouteDoc::default(),
        ));
        assert_eq!(
            default_style(&manifest, "acme"),
            PathStyle::ServerRelative { tenant: "acme" }
        );
    }

    #[test]
    fn server_relative_specs_default_their_server_and_use_3_1_nullability() {
        let mut manifest = test_manifest();
        manifest.tables[0].columns[1].nullable = true;
        manifest.openapi = Some(crate::OpenApiOptions::default());
        let spec = relative(&manifest);
        assert_eq!(
            spec["servers"],
            json!([{"url": "{origin}/svc/{tenant}/test-service",
                "variables": {"origin": {"default": ""}, "tenant": {"default": "acme"}}}])
        );
        let paths = spec["paths"].as_object().expect("paths");
        assert!(paths.contains_key("/todos") && paths.contains_key("/todos/{id}"));
        assert_eq!(
            spec["components"]["schemas"]["Todos"]["properties"]["title"],
            json!({"type": ["string", "null"]})
        );
        assert!(spec.get("tags").is_none());
        assert!(spec["components"].get("securitySchemes").is_none());

        manifest.openapi = Some(crate::OpenApiOptions {
            title: Some("Loans".into()),
            version: Some("2.0.0".into()),
            description: Some(String::new()),
            servers: vec![crate::ServerDoc {
                url: "https://api.example.com/api".into(),
                description: Some("Production".into()),
            }],
            tags: vec![crate::TagDoc {
                name: "Loans Service".into(),
                description: Some("Loans Service API".into()),
            }],
            security_scheme: None,
        });
        let spec = relative(&manifest);
        assert_eq!(
            spec["info"],
            json!({"title": "Loans", "version": "2.0.0", "description": ""})
        );
        assert_eq!(
            spec["servers"],
            json!([{"url": "https://api.example.com/api", "description": "Production"}])
        );
        assert_eq!(
            spec["tags"],
            json!([{"name": "Loans Service", "description": "Loans Service API"}])
        );
    }

    #[test]
    fn documented_routes_carry_parameters_bodies_responses_and_security() {
        use crate::{BodyDoc, MediaDoc, ResponseDoc, RouteDoc, RoutePosture, RoutingMode};
        let mut manifest = test_manifest();
        manifest.tables.clear();
        manifest.routing = RoutingMode::Strict;
        manifest.components.insert(
            "ApiLoan".into(),
            json!({"type": "object", "properties": {"id": {"type": "string"}}}),
        );
        let doc = RouteDoc {
            tags: vec!["loan".into()],
            summary: Some("Update a loan".into()),
            deprecated: true,
            path_params: Some(json!({"type": "object", "required": ["loan_id"],
                "properties": {"loan_id": {"type": "string", "description": "Loan identifier"}}})),
            query: Some(
                json!({"type": "object", "required": ["force"], "properties": {
                "force": {"type": "boolean"}, "page": {"type": ["integer", "null"], "format": "int32"}}}),
            ),
            request: Some(BodyDoc {
                content_type: "application/json".into(),
                schema: Some(json!({"$ref": "#/components/schemas/ApiLoan"})),
                required: true,
            }),
            responses: [
                (
                    200,
                    ResponseDoc {
                        description: "Updated".into(),
                        content: vec![MediaDoc {
                            content_type: "application/json".into(),
                            schema: Some(json!({"$ref": "#/components/schemas/ApiLoan"})),
                        }],
                    },
                ),
                (
                    403,
                    ResponseDoc {
                        description: "Forbidden".into(),
                        content: vec![],
                    },
                ),
            ]
            .into(),
            ..Default::default()
        };
        manifest.custom_routes = vec![
            documented_route(
                "PUT",
                "/loans/{loan_id}",
                "update_loan",
                Some(RoutePosture::Permission {
                    area: "loans".into(),
                    operation: "edit".into(),
                }),
                doc,
            ),
            documented_route(
                "GET",
                "/loans/{loan_id}/pdf",
                "loan_pdf",
                Some(RoutePosture::Public {
                    reason: "shared link".into(),
                }),
                RouteDoc {
                    responses: [(
                        200,
                        ResponseDoc {
                            description: "PDF".into(),
                            content: vec![MediaDoc {
                                content_type: "application/pdf".into(),
                                schema: None,
                            }],
                        },
                    )]
                    .into(),
                    ..Default::default()
                },
            ),
        ];
        let spec = relative(&manifest);
        let update = &spec["paths"]["/loans/{loan_id}"]["put"];
        assert_eq!(update["operationId"], "update_loan");
        assert_eq!(update["tags"], json!(["loan"]));
        assert_eq!(update["summary"], "Update a loan");
        assert_eq!(update["deprecated"], true);
        assert_eq!(
            update["parameters"],
            json!([
                {"name": "loan_id", "in": "path", "required": true, "schema": {"type": "string"},
                    "description": "Loan identifier"},
                {"name": "force", "in": "query", "required": true, "schema": {"type": "boolean"}},
                {"name": "page", "in": "query", "required": false,
                    "schema": {"type": ["integer", "null"], "format": "int32"}},
            ])
        );
        assert_eq!(
            update["requestBody"],
            json!({"content": {"application/json": {"schema": {"$ref": "#/components/schemas/ApiLoan"}}}, "required": true})
        );
        assert_eq!(
            update["responses"]["403"],
            json!({"description": "Forbidden"})
        );
        assert_eq!(update["security"], json!([{"bearer_auth": []}]));

        let pdf = &spec["paths"]["/loans/{loan_id}/pdf"]["get"];
        assert_eq!(pdf["tags"], json!(["test-service"]));
        assert_eq!(pdf["parameters"][0]["schema"], json!({"type": "string"}));
        assert_eq!(
            pdf["responses"]["200"],
            json!({"description": "PDF", "content": {"application/pdf": {}}})
        );
        assert!(
            pdf.get("security").is_none(),
            "public routes need no credentials"
        );
        assert!(pdf.get("requestBody").is_none());
        assert_eq!(
            spec["components"]["securitySchemes"],
            json!({"bearer_auth": {"type": "http", "scheme": "bearer"}})
        );
        assert_eq!(spec["components"]["schemas"]["ApiLoan"]["type"], "object");
    }

    #[test]
    fn legacy_routes_of_a_documented_service_use_their_fn_alias() {
        let mut manifest = test_manifest();
        manifest.custom_routes.push(CustomRouteDefinition {
            method: "POST".into(),
            path: "/complete/:id".into(),
            handler: "complete_todo".into(),
            posture: Some(crate::RoutePosture::Authenticated),
            ..Default::default()
        });
        manifest.openapi = Some(crate::OpenApiOptions::default());
        let spec = relative(&manifest);
        let operation = &spec["paths"]["/_fn/complete_todo"]["post"];
        assert_eq!(operation["operationId"], "complete_todo");
        assert_eq!(operation["requestBody"]["required"], false);
        assert_eq!(operation["responses"]["200"]["description"], "Success");
        assert_eq!(
            spec["paths"]["/todos"]["get"]["operationId"], "list_todos",
            "CRUD operations are server-relative too"
        );
    }

    #[test]
    fn internal_routes_are_left_out_of_both_specs() {
        let mut manifest = test_manifest();
        manifest.custom_routes.push(CustomRouteDefinition {
            method: "POST".into(),
            path: "/reprice".into(),
            handler: "reprice".into(),
            posture: Some(crate::RoutePosture::Internal {
                callers: vec!["pricing".into()],
            }),
            ..Default::default()
        });
        let absolute =
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).expect("spec");
        assert!(absolute["paths"]
            .get("/svc/acme/test-service/_fn/reprice")
            .is_none());
        manifest.openapi = Some(crate::OpenApiOptions::default());
        assert!(relative(&manifest)["paths"].get("/_fn/reprice").is_none());
    }

    #[test]
    fn api_key_routes_require_the_api_key_scheme() {
        let mut manifest = test_manifest();
        manifest.openapi = Some(crate::OpenApiOptions::default());
        manifest.custom_routes.push(CustomRouteDefinition {
            method: "POST".into(),
            path: "/ingest".into(),
            handler: "ingest_book".into(),
            posture: Some(crate::RoutePosture::ApiKey {
                scope: "books:ingest".into(),
            }),
            ..Default::default()
        });
        let spec = relative(&manifest);
        assert_eq!(
            spec["paths"]["/_fn/ingest_book"]["post"]["security"],
            json!([{"api_key": []}])
        );
        assert_eq!(
            spec["components"]["securitySchemes"],
            json!({"api_key": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}})
        );
    }

    #[test]
    fn a_crud_operation_id_clash_is_a_generation_error() {
        let mut manifest = test_manifest();
        manifest.custom_routes.push(documented_route(
            "GET",
            "/x",
            "list_todos",
            None,
            crate::RouteDoc::default(),
        ));
        assert_eq!(
            generate_openapi(&manifest, PathStyle::ServerRelative { tenant: "acme" }),
            Err(OpenApiError(
                "operation id 'list_todos' is used more than once".into()
            ))
        );
        assert!(
            generate_openapi(&manifest, PathStyle::Absolute { tenant: "acme" }).is_ok(),
            "the absolute spec is today's and never refused"
        );
    }

    fn private_loans() -> crate::TableDefinition {
        serde_json::from_value(json!({
            "name": "loans", "managed": false,
            "columns": [{"name": "id", "column_type": "uuid", "primary_key": true}]
        }))
        .expect("table")
    }

    #[test]
    fn a_private_table_has_no_paths_schemas_or_operation_ids() {
        let served = test_manifest();
        let mut manifest = served.clone();
        manifest.private_tables = vec![private_loans()];
        manifest.custom_routes.push(documented_route(
            "GET",
            "/loans",
            "list_loans",
            None,
            crate::RouteDoc::default(),
        ));
        for style in [
            PathStyle::Absolute { tenant: "acme" },
            PathStyle::ServerRelative { tenant: "acme" },
        ] {
            let spec = generate_openapi(&manifest, style).expect("list_loans is the route's alone");
            let text = spec.to_string();
            for absent in [
                "\"Loans\"",
                "LoansCreate",
                "create_loan",
                "/loans/{id}",
                "/loans/:id",
            ] {
                assert!(!text.contains(absent), "{absent} in {text}");
            }
            let mut without = manifest.clone();
            without.private_tables.clear();
            assert_eq!(spec, generate_openapi(&without, style).expect("spec"));
        }

        let only_private = ServiceManifest {
            tables: vec![],
            ..manifest.clone()
        };
        let undeclared = ServiceManifest {
            private_tables: vec![],
            ..only_private.clone()
        };
        for style in [
            PathStyle::Absolute { tenant: "acme" },
            PathStyle::ServerRelative { tenant: "acme" },
        ] {
            assert_eq!(
                generate_openapi(&only_private, style),
                generate_openapi(&undeclared, style)
            );
        }
    }
}
