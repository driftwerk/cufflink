use crate::route_path::RoutePattern;
use crate::{CustomRouteDefinition, ResponseDoc, RoutingMode, ServiceManifest};
use serde_json::{json, Map, Value};

/// The path, lowercase method and operation object of a custom route, served
/// through the `_fn` dispatcher.
pub(super) fn route_operation(
    tenant_slug: &str,
    service: &str,
    route: &CustomRouteDefinition,
) -> (String, String, Value) {
    let full_path = format!("/svc/{}/{}/_fn/{}", tenant_slug, service, route.handler);
    let method = route.method.to_lowercase();
    let operation_id = route.handler.clone();

    let mut parameters = Vec::new();
    for segment in route.path.split('/') {
        if let Some(param) = segment.strip_prefix(':') {
            parameters.push(json!({
                "name": param,
                "in": "path",
                "required": true,
                "schema": { "type": "string" }
            }));
        }
    }

    let mut operation = json!({
        "summary": format!("Custom: {}", route.handler),
        "operationId": operation_id,
        "tags": ["custom"],
        "parameters": parameters,
        "responses": {
            "200": {
                "description": "Success",
                "content": {
                    "application/json": {
                        "schema": { "type": "object" }
                    }
                }
            }
        }
    });

    if matches!(method.as_str(), "post" | "put" | "patch") {
        operation["requestBody"] = json!({
            "required": false,
            "content": {
                "application/json": {
                    "schema": { "type": "object" }
                }
            }
        });
    }

    (full_path, method, operation)
}

/// Whether a route belongs in the spec: internal routes are for the
/// tenant's other services, not a client.
pub(super) fn published(route: &CustomRouteDefinition) -> bool {
    !matches!(route.posture, Some(crate::RoutePosture::Internal { .. }))
}

/// Where a documented route's schemas resolve their `$ref`s.
pub(super) struct Components<'a>(pub &'a Map<String, Value>);

impl Components<'_> {
    /// `schema`, or the component it refers to.
    pub(super) fn resolve<'s>(&'s self, schema: &'s Value) -> &'s Value {
        schema
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|reference| reference.strip_prefix("#/components/schemas/"))
            .and_then(|name| self.0.get(name))
            .unwrap_or(schema)
    }
}

/// The OpenAPI path of `route`: its pattern under strict routing, else its
/// `_fn` alias.
pub(super) fn relative_path(route: &CustomRouteDefinition, routing: RoutingMode) -> String {
    match routing {
        RoutingMode::Strict => RoutePattern::parse(&route.path)
            .map(|pattern| pattern.to_openapi())
            .unwrap_or_else(|_| route.path.clone()),
        RoutingMode::Legacy => format!("/_fn/{}", route.handler),
    }
}

/// The path parameter names of `route` under `routing`.
pub(super) fn path_param_names(route: &CustomRouteDefinition, routing: RoutingMode) -> Vec<String> {
    match routing {
        RoutingMode::Strict => RoutePattern::parse(&route.path)
            .map(|pattern| pattern.params().map(str::to_string).collect())
            .unwrap_or_default(),
        RoutingMode::Legacy => Vec::new(),
    }
}

/// The effective operation id of a custom route.
pub(super) fn operation_id(route: &CustomRouteDefinition) -> &str {
    route
        .doc
        .as_ref()
        .and_then(|doc| doc.operation_id.as_deref())
        .unwrap_or(&route.handler)
}

/// Each property of an object schema as `(name, schema, required)`.
pub(super) fn properties<'a>(
    object: &'a Value,
) -> impl Iterator<Item = (&'a String, &'a Value, bool)> + 'a {
    let required = object["required"].as_array();
    object["properties"]
        .as_object()
        .into_iter()
        .flatten()
        .map(move |(name, schema)| {
            let is_required = required.is_some_and(|r| r.iter().any(|n| n == name.as_str()));
            (name, schema, is_required)
        })
}

/// A parameter object: the property's description moves out of its schema.
fn parameter(name: &str, location: &str, required: bool, schema: &Value) -> Value {
    let mut schema = schema.clone();
    let description = schema
        .as_object_mut()
        .and_then(|schema| schema.remove("description"));
    let mut parameter = json!({
        "name": name,
        "in": location,
        "required": required,
        "schema": schema,
    });
    if let (Some(description), Some(parameter)) = (description, parameter.as_object_mut()) {
        parameter.insert("description".into(), description);
    }
    parameter
}

/// The path, lowercase method and operation object of a documented or
/// server-relative custom route; `security` is the scheme names it
/// requires.
pub(super) fn relative_operation(
    manifest: &ServiceManifest,
    route: &CustomRouteDefinition,
    security: &[&str],
    components: &Components<'_>,
) -> (String, String, Value) {
    let method = route.method.to_lowercase();
    let doc = route.doc.clone().unwrap_or_default();
    let path_schema = doc
        .path_params
        .as_ref()
        .map(|schema| components.resolve(schema));
    let path_parameters = path_param_names(route, manifest.routing)
        .into_iter()
        .map(|name| {
            let schema = path_schema
                .and_then(|schema| schema["properties"].get(&name))
                .cloned()
                .unwrap_or_else(|| json!({ "type": "string" }));
            parameter(&name, "path", true, &schema)
        });
    let query_parameters = doc
        .query
        .as_ref()
        .map(|schema| components.resolve(schema))
        .into_iter()
        .flat_map(properties)
        .map(|(name, schema, required)| parameter(name, "query", required, schema));
    let parameters: Vec<Value> = path_parameters.chain(query_parameters).collect();

    let mut operation = Map::new();
    let tags = if doc.tags.is_empty() {
        vec![manifest.name.clone()]
    } else {
        doc.tags.clone()
    };
    operation.insert("tags".into(), json!(tags));
    operation.insert("operationId".into(), json!(operation_id(route)));
    if let Some(summary) = &doc.summary {
        operation.insert("summary".into(), json!(summary));
    }
    if let Some(description) = &doc.description {
        operation.insert("description".into(), json!(description));
    }
    if doc.deprecated {
        operation.insert("deprecated".into(), json!(true));
    }
    if !parameters.is_empty() {
        operation.insert("parameters".into(), json!(parameters));
    }
    let request = match (&doc.request, &route.doc) {
        (Some(body), _) => Some(json!({
            "content": { body.content_type.clone(): { "schema": body.schema.clone().unwrap_or_else(|| json!({})) } },
            "required": body.required,
        })),
        (None, None) if matches!(method.as_str(), "post" | "put" | "patch") => Some(json!({
            "required": false,
            "content": { "application/json": { "schema": { "type": "object" } } },
        })),
        (None, _) => None,
    };
    if let Some(request) = request {
        operation.insert("requestBody".into(), request);
    }
    let responses: Map<String, Value> = if doc.responses.is_empty() {
        Map::from_iter([(
            "200".to_string(),
            json!({
                "description": "Success",
                "content": { "application/json": { "schema": { "type": "object" } } },
            }),
        )])
    } else {
        doc.responses
            .iter()
            .map(|(status, response)| (status.to_string(), response_object(response)))
            .collect()
    };
    operation.insert("responses".into(), Value::Object(responses));
    if !security.is_empty() {
        let requirements: Vec<Value> = security
            .iter()
            .map(|scheme| json!({ *scheme: [] }))
            .collect();
        operation.insert("security".into(), json!(requirements));
    }
    (
        relative_path(route, manifest.routing),
        method,
        Value::Object(operation),
    )
}

fn response_object(response: &ResponseDoc) -> Value {
    let mut object = Map::from_iter([("description".to_string(), json!(response.description))]);
    if !response.content.is_empty() {
        let content: Map<String, Value> = response
            .content
            .iter()
            .map(|media| {
                let object = match &media.schema {
                    Some(schema) => json!({ "schema": schema }),
                    None => json!({}),
                };
                (media.content_type.clone(), object)
            })
            .collect();
        object.insert("content".into(), Value::Object(content));
    }
    Value::Object(object)
}
