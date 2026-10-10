//! What a documented manifest must satisfy for its spec to be generated:
//! checked when the SDK emits the manifest, at deploy, and before
//! generation.

use super::custom::{self, Components};
use super::schema::schema_names;
use super::{crud, default_style, OpenApiError, PathStyle};
use crate::{CustomRouteDefinition, RoutePosture, ServiceManifest};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

/// The security schemes `route` requires: its doc's, else its posture's.
/// The scheme of partner-key routes: the `X-Api-Key` header.
pub(super) const API_KEY_SCHEME: &str = "api_key";

pub(super) fn route_security<'a>(
    route: &'a CustomRouteDefinition,
    scheme: &'a str,
) -> Vec<&'a str> {
    match route.doc.as_ref().and_then(|doc| doc.security.as_ref()) {
        Some(names) => names.iter().map(String::as_str).collect(),
        None => match &route.posture {
            None | Some(RoutePosture::Public { .. }) | Some(RoutePosture::Internal { .. }) => {
                Vec::new()
            }
            Some(RoutePosture::ApiKey { .. }) => vec![API_KEY_SCHEME],
            Some(RoutePosture::Authenticated)
            | Some(RoutePosture::Permission { .. })
            | Some(RoutePosture::Owner { .. })
            | Some(RoutePosture::Scoped { .. }) => vec![scheme],
        },
    }
}

/// Check a documented manifest: operation ids unique across its published
/// custom and CRUD operations, no two published routes documenting the same
/// method and path, `path = T` naming exactly the route's path parameters,
/// query and path parameters that are scalars or arrays, security schemes
/// the document defines, and no DTO sharing a table schema's name. An
/// undocumented manifest passes: its spec is today's.
pub fn validate(manifest: &ServiceManifest) -> Result<(), OpenApiError> {
    if matches!(default_style(manifest, ""), PathStyle::Absolute { .. }) {
        return Ok(());
    }
    unique_operation_ids(manifest)?;
    unique_operations(manifest)?;
    let components = Map::from_iter(manifest.components.clone());
    let lookup = Components(&components);
    let scheme = manifest
        .openapi
        .as_ref()
        .map_or(crate::OpenApiOptions::DEFAULT_SECURITY_SCHEME, |o| {
            o.security_scheme()
        });
    manifest
        .custom_routes
        .iter()
        .try_for_each(|route| check_route(manifest, route, &lookup, scheme))?;
    manifest
        .tables
        .iter()
        .flat_map(|table| {
            let (row, create) = schema_names(table);
            [row, create]
        })
        .chain(
            (!manifest.tables.is_empty())
                .then(|| "Pagination".to_string()),
        )
        .find(|name| manifest.components.contains_key(name))
        .map_or(Ok(()), |name| {
            Err(OpenApiError(format!(
                "schema name \"{name}\" is both a table schema and a documented type; rename the type with #[schemars(rename = \"…\")]"
            )))
        })
}

fn unique_operation_ids(manifest: &ServiceManifest) -> Result<(), OpenApiError> {
    let mut seen = BTreeSet::new();
    manifest
        .custom_routes
        .iter()
        .filter(|route| custom::published(route))
        .map(|route| custom::operation_id(route).to_string())
        .chain(manifest.tables.iter().flat_map(crud::operation_ids))
        .find(|id| !seen.insert(id.clone()))
        .map_or(Ok(()), |id| {
            Err(OpenApiError(format!(
                "operation id '{id}' is used more than once"
            )))
        })
}

/// A legacy route's operation is its handler's `_fn` path, so two routes of
/// one handler and method would document one operation.
fn unique_operations(manifest: &ServiceManifest) -> Result<(), OpenApiError> {
    let mut seen = BTreeSet::new();
    manifest
        .custom_routes
        .iter()
        .filter(|route| custom::published(route))
        .map(|route| {
            (
                route.method.to_ascii_uppercase(),
                custom::relative_path(route, manifest.routing),
            )
        })
        .find(|operation| !seen.insert(operation.clone()))
        .map_or(Ok(()), |(method, path)| {
            Err(OpenApiError(format!(
                "two routes document {method} {path}; route a handler once per method"
            )))
        })
}

fn check_route(
    manifest: &ServiceManifest,
    route: &CustomRouteDefinition,
    components: &Components<'_>,
    scheme: &str,
) -> Result<(), OpenApiError> {
    let Some(doc) = &route.doc else {
        return Ok(());
    };
    let name = || {
        format!(
            "route '{} {}' ({})",
            route.method, route.path, route.handler
        )
    };
    if let Some(path) = &doc.path_params {
        let declared: BTreeSet<String> = custom::properties(components.resolve(path))
            .map(|(property, _, _)| property.clone())
            .collect();
        let params: BTreeSet<String> = custom::path_param_names(route, manifest.routing)
            .into_iter()
            .collect();
        if declared != params {
            return Err(OpenApiError(format!(
                "{}: path = … declares {:?} but the path has parameters {:?}",
                name(),
                declared,
                params
            )));
        }
    }
    [("path", &doc.path_params), ("query", &doc.query)]
        .into_iter()
        .filter_map(|(location, schema)| schema.as_ref().map(|schema| (location, schema)))
        .try_for_each(|(location, schema)| {
            let object = components.resolve(schema);
            if object["properties"].as_object().is_none() {
                return Err(OpenApiError(format!(
                    "{}: {location} = … must be a struct with named fields",
                    name()
                )));
            }
            custom::properties(object)
                .find(|(_, property, _)| !is_parameter_schema(property, components))
                .map_or(Ok(()), |(property, _, _)| {
                    Err(OpenApiError(format!(
                        "{}: {location} parameter '{property}' must be a scalar or an array",
                        name()
                    )))
                })
        })?;
    route_security(route, scheme)
        .into_iter()
        .find(|security| *security != scheme && *security != API_KEY_SCHEME)
        .map_or(Ok(()), |unknown| {
            Err(OpenApiError(format!(
                "{}: security scheme '{unknown}' is not defined; the document defines '{scheme}' and '{API_KEY_SCHEME}'",
                name()
            )))
        })
}

/// A schema a parameter can take: scalars, arrays, enums, and their
/// nullable forms, directly or through a `$ref`.
fn is_parameter_schema(schema: &Value, components: &Components<'_>) -> bool {
    let schema = components.resolve(schema);
    let scalar = |kind: &Value| {
        matches!(
            kind.as_str(),
            Some("string" | "integer" | "number" | "boolean" | "array" | "null")
        )
    };
    let branches = ["anyOf", "oneOf"]
        .iter()
        .find_map(|key| schema[*key].as_array());
    match (&schema["type"], branches) {
        (Value::String(_), _) => scalar(&schema["type"]),
        (Value::Array(kinds), _) => kinds.iter().all(scalar),
        (_, Some(branches)) => branches
            .iter()
            .all(|branch| is_parameter_schema(branch, components)),
        _ => schema.get("enum").is_some() || schema.get("const").is_some(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OpenApiOptions, RouteDoc, RoutingMode};
    use serde_json::json;

    fn documented(routes: Vec<CustomRouteDefinition>) -> ServiceManifest {
        ServiceManifest {
            name: "notes".into(),
            routing: RoutingMode::Strict,
            openapi: Some(OpenApiOptions::default()),
            custom_routes: routes,
            ..Default::default()
        }
    }

    fn route(method: &str, path: &str, handler: &str, doc: RouteDoc) -> CustomRouteDefinition {
        CustomRouteDefinition {
            method: method.into(),
            path: path.into(),
            handler: handler.into(),
            posture: Some(RoutePosture::Authenticated),
            doc: Some(doc),
            ..Default::default()
        }
    }

    #[test]
    fn undocumented_manifests_are_not_checked() {
        let mut legacy = documented(vec![
            route("GET", "/a", "same", RouteDoc::default()),
            route("POST", "/a", "same", RouteDoc::default()),
        ]);
        assert!(validate(&legacy).is_err());
        legacy.openapi = None;
        legacy.custom_routes.iter_mut().for_each(|r| r.doc = None);
        assert!(validate(&legacy).is_ok());
    }

    #[test]
    fn operation_ids_are_unique_across_routes_and_crud() {
        let m = documented(vec![
            route("GET", "/a", "one", RouteDoc::default()),
            route(
                "GET",
                "/b",
                "two",
                RouteDoc {
                    operation_id: Some("one".into()),
                    ..Default::default()
                },
            ),
        ]);
        assert_eq!(
            validate(&m),
            Err(OpenApiError(
                "operation id 'one' is used more than once".into()
            ))
        );
    }

    #[test]
    fn internal_routes_are_not_published_so_share_no_operation_id() {
        let mut internal = route("POST", "/sync", "one", RouteDoc::default());
        internal.posture = Some(RoutePosture::Internal {
            callers: vec!["*".into()],
        });
        let m = documented(vec![
            route("GET", "/a", "one", RouteDoc::default()),
            internal,
        ]);
        assert_eq!(validate(&m), Ok(()));
    }

    #[test]
    fn a_legacy_handler_documents_one_operation_per_method() {
        let named = |id: &str| RouteDoc {
            operation_id: Some(id.into()),
            ..Default::default()
        };
        let mut m = documented(vec![
            route("POST", "/a", "same", named("first")),
            route("POST", "/b", "same", named("second")),
        ]);
        m.routing = RoutingMode::Legacy;
        assert_eq!(
            validate(&m),
            Err(OpenApiError(
                "two routes document POST /_fn/same; route a handler once per method".into()
            ))
        );
        m.custom_routes[1].method = "GET".into();
        assert_eq!(validate(&m), Ok(()));
    }

    #[test]
    fn path_structs_name_exactly_the_path_parameters() {
        let path_doc = |properties: Value| RouteDoc {
            path_params: Some(
                json!({"type": "object", "properties": properties, "required": ["note_id"]}),
            ),
            ..Default::default()
        };
        let ok = documented(vec![route(
            "GET",
            "/notes/{note_id}",
            "get_note",
            path_doc(json!({"note_id": {"type": "string"}})),
        )]);
        assert!(validate(&ok).is_ok());
        let missing = documented(vec![route(
            "GET",
            "/notes/{note_id}/{rev}",
            "get_note",
            path_doc(json!({"note_id": {"type": "string"}})),
        )]);
        let err = validate(&missing).unwrap_err().to_string();
        assert!(
            err.contains("path has parameters {\"note_id\", \"rev\"}"),
            "{err}"
        );
    }

    #[test]
    fn parameters_are_scalars_or_arrays() {
        let query = |property: Value| RouteDoc {
            query: Some(json!({"type": "object", "properties": {"q": property}})),
            ..Default::default()
        };
        let mut m = documented(vec![]);
        m.components
            .insert("Kind".into(), json!({"type": "string", "enum": ["a", "b"]}));
        for property in [
            json!({"type": ["integer", "null"], "format": "int32"}),
            json!({"type": "array", "items": {"type": "string"}}),
            json!({"$ref": "#/components/schemas/Kind"}),
            json!({"anyOf": [{"$ref": "#/components/schemas/Kind"}, {"type": "null"}]}),
            json!({"enum": ["x"]}),
        ] {
            m.custom_routes = vec![route("GET", "/s", "search", query(property.clone()))];
            assert!(validate(&m).is_ok(), "{property}");
        }
        m.custom_routes = vec![route(
            "GET",
            "/s",
            "search",
            query(json!({"type": "object", "properties": {}})),
        )];
        let err = validate(&m).unwrap_err().to_string();
        assert!(
            err.contains("query parameter 'q' must be a scalar or an array"),
            "{err}"
        );
        m.custom_routes = vec![route(
            "GET",
            "/s",
            "search",
            RouteDoc {
                query: Some(json!({"type": "string"})),
                ..Default::default()
            },
        )];
        assert!(validate(&m).is_err());
    }

    #[test]
    fn security_names_a_defined_scheme() {
        let m = documented(vec![route(
            "GET",
            "/a",
            "a",
            RouteDoc {
                security: Some(vec!["oauth".into()]),
                ..Default::default()
            },
        )]);
        let err = validate(&m).unwrap_err().to_string();
        assert!(
            err.contains("security scheme 'oauth' is not defined"),
            "{err}"
        );
    }

    #[test]
    fn a_documented_type_may_share_a_private_tables_schema_name() {
        let notes: crate::TableDefinition = serde_json::from_value(json!({
            "name": "note", "managed": false,
            "columns": [{"name": "id", "column_type": "uuid", "primary_key": true}]
        }))
        .expect("table");
        let typed = |tables: Vec<crate::TableDefinition>, private_tables| ServiceManifest {
            tables,
            private_tables,
            components: [("Note".to_string(), json!({"type": "object"}))].into(),
            ..documented(vec![route("GET", "/notes", "notes", RouteDoc::default())])
        };
        assert_eq!(validate(&typed(vec![], vec![notes.clone()])), Ok(()));
        assert_eq!(
            validate(&typed(vec![notes], vec![])),
            Err(OpenApiError(
                "schema name \"Note\" is both a table schema and a documented type; rename the type with #[schemars(rename = \"…\")]".into()
            ))
        );
    }
}
