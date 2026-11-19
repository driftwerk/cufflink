//! Public documents: tag-selected subsets of a service's spec, served
//! without authentication under a mount of their own.

use super::check::API_KEY_SCHEME;
use super::{custom, server_relative, servers_of, OpenApiError};
use crate::route_path::{self, RoutePattern, Segment};
use crate::{
    CustomRouteDefinition, PublicDoc, PublicDocCors, RoutePosture, RoutingMode, ServiceManifest,
};
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::fmt;

const COMPONENT_SCHEMAS: &str = "#/components/schemas/";

/// Why a public document cannot be built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicDocError {
    /// The service declares no document of this name.
    Unknown { name: String, declared: Vec<String> },
    /// The service's spec cannot be generated.
    Spec(OpenApiError),
}

impl fmt::Display for PublicDocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { name, declared } if declared.is_empty() => {
                write!(f, "no public document '{name}': the service declares none")
            }
            Self::Unknown { name, declared } => write!(
                f,
                "no public document '{name}': the service declares {}",
                declared.join(", ")
            ),
            Self::Spec(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for PublicDocError {}

/// The public document `name` of `manifest`.
pub fn public_doc<'a>(
    manifest: &'a ServiceManifest,
    name: &str,
) -> Result<&'a PublicDoc, PublicDocError> {
    manifest
        .public_docs()
        .find(|doc| doc.name == name)
        .ok_or_else(|| PublicDocError::Unknown {
            name: name.to_string(),
            declared: manifest.public_docs().map(|doc| doc.name.clone()).collect(),
        })
}

/// The public document `name`: the operations of the service's
/// server-relative spec tagged with any of its `include_tags`, the schemas
/// and security schemes they reach, and the document's own info, servers
/// and tags.
pub fn public_doc_spec(
    manifest: &ServiceManifest,
    tenant: &str,
    name: &str,
) -> Result<Value, PublicDocError> {
    let doc = public_doc(manifest, name)?;
    let spec = server_relative(manifest, tenant).map_err(PublicDocError::Spec)?;
    let fallback = manifest
        .openapi
        .as_ref()
        .map_or(&[][..], |options| options.servers.as_slice());
    let servers = servers_of(&[&doc.servers, fallback], &manifest.name, tenant);
    Ok(filtered(&spec, doc, servers))
}

fn filtered(spec: &Value, doc: &PublicDoc, servers: Value) -> Value {
    let selected = |operation: &Value| {
        operation["tags"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .any(|tag| doc.include_tags.iter().any(|included| included == tag))
    };
    let prefix = doc.path_prefix.as_deref().unwrap_or_default();
    let paths: Map<String, Value> = spec["paths"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(path, item)| {
            let kept: Map<String, Value> = item
                .as_object()?
                .iter()
                .filter(|(_, operation)| selected(operation))
                .map(|(method, operation)| (method.clone(), operation.clone()))
                .collect();
            (!kept.is_empty()).then(|| (format!("{prefix}{path}"), Value::Object(kept)))
        })
        .collect();
    let operations = || {
        paths
            .values()
            .filter_map(Value::as_object)
            .flat_map(Map::values)
    };

    let all_schemas = spec["components"]["schemas"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    let schemas: Map<String, Value> = reachable(operations().flat_map(references), &all_schemas)
        .into_iter()
        .filter_map(|name| all_schemas.get(&name).map(|schema| (name, schema.clone())))
        .collect();

    let used: BTreeSet<&str> = operations()
        .filter_map(|operation| operation["security"].as_array())
        .flatten()
        .filter_map(Value::as_object)
        .flat_map(Map::keys)
        .map(String::as_str)
        .collect();
    let security_schemes: Map<String, Value> = spec["components"]["securitySchemes"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, _)| used.contains(name.as_str()))
        .map(|(name, scheme)| {
            let scheme = match &doc.api_key {
                Some(api_key) if name == API_KEY_SCHEME => {
                    let mut presented = Map::from_iter([
                        ("type".to_string(), json!("apiKey")),
                        ("in".to_string(), json!("header")),
                        ("name".to_string(), json!(api_key.header)),
                    ]);
                    if let Some(description) = &api_key.description {
                        presented.insert("description".into(), json!(description));
                    }
                    Value::Object(presented)
                }
                _ => scheme.clone(),
            };
            (name.clone(), scheme)
        })
        .collect();

    let mut components = Map::from_iter([("schemas".to_string(), Value::Object(schemas))]);
    if !security_schemes.is_empty() {
        components.insert("securitySchemes".into(), Value::Object(security_schemes));
    }
    let mut document = Map::from_iter([
        ("openapi".to_string(), spec["openapi"].clone()),
        ("info".to_string(), json!(doc.info)),
        ("servers".to_string(), servers),
        ("paths".to_string(), Value::Object(paths)),
        ("components".to_string(), Value::Object(components)),
    ]);
    if !doc.tags.is_empty() {
        document.insert("tags".into(), json!(doc.tags));
    }
    Value::Object(document)
}

/// The component schema names `value` refers to.
fn references(value: &Value) -> Vec<String> {
    match value {
        Value::Object(object) => object
            .iter()
            .flat_map(|(key, value)| match (key.as_str(), value) {
                ("$ref", Value::String(reference)) => reference
                    .strip_prefix(COMPONENT_SCHEMAS)
                    .map(str::to_string)
                    .into_iter()
                    .collect(),
                _ => references(value),
            })
            .collect(),
        Value::Array(items) => items.iter().flat_map(references).collect(),
        _ => Vec::new(),
    }
}

/// `roots` and every schema they reach through `schemas`.
fn reachable(
    roots: impl IntoIterator<Item = String>,
    schemas: &Map<String, Value>,
) -> BTreeSet<String> {
    let mut reached = BTreeSet::new();
    let mut pending: Vec<String> = roots.into_iter().collect();
    while let Some(name) = pending.pop() {
        if let Some(schema) = schemas.get(&name).filter(|_| !reached.contains(&name)) {
            pending.extend(references(schema));
            reached.insert(name);
        }
    }
    reached
}

/// The literal segments of a document's mount.
pub fn mount_segments(doc: &PublicDoc) -> Result<Vec<&str>, OpenApiError> {
    let refused = |reason: String| {
        OpenApiError(format!(
            "public document '{}': mount '{}' {reason}",
            doc.name, doc.mount
        ))
    };
    let pattern =
        RoutePattern::parse(&doc.mount).map_err(|e| refused(format!("is not a path: {e}")))?;
    let literal = pattern
        .segments()
        .iter()
        .all(|segment| matches!(segment, Segment::Literal(_)));
    match doc.mount.strip_prefix('/') {
        Some(below) if literal && !below.is_empty() => Ok(below.split('/').collect()),
        _ => Err(refused("must be one or more literal segments".to_string())),
    }
}

/// What the service's public documents must satisfy, beside its spec.
pub(super) fn check(manifest: &ServiceManifest) -> Result<(), OpenApiError> {
    let docs: Vec<&PublicDoc> = manifest.public_docs().collect();
    if let (Some(doc), RoutingMode::Legacy) = (docs.first(), manifest.routing) {
        return Err(OpenApiError(format!(
            "public document '{}' needs routing: strict",
            doc.name
        )));
    }
    docs.iter().try_for_each(|doc| check_doc(manifest, doc))?;
    docs.iter()
        .enumerate()
        .flat_map(|(at, first)| docs[at + 1..].iter().map(move |second| (*first, *second)))
        .try_for_each(|(first, second)| disjoint(first, second))
}

fn check_doc(manifest: &ServiceManifest, doc: &PublicDoc) -> Result<(), OpenApiError> {
    let refused = |reason: String| {
        Err(OpenApiError(format!(
            "public document '{}': {reason}",
            doc.name
        )))
    };
    if !is_doc_name(&doc.name) {
        return refused("a document name matches [a-z][a-z0-9-]{0,62}".into());
    }
    if doc.include_tags.is_empty() {
        return refused("include_tags is empty".into());
    }
    let mount = mount_segments(doc)?;
    if let Some(table) = mount
        .first()
        .filter(|first| manifest.get_table(first).is_some())
    {
        return refused(format!(
            "mount '{}' is under the data routes of table '{table}'",
            doc.mount
        ));
    }
    if let Some(route) = manifest
        .custom_routes
        .iter()
        .find(|route| reaches_under(route, &mount))
    {
        return refused(format!(
            "{} reaches under its mount '{}'",
            route_name(route),
            doc.mount
        ));
    }
    let published = published_tags(manifest);
    if let Some(tag) = doc
        .include_tags
        .iter()
        .find(|tag| !published.contains(tag.as_str()))
    {
        return refused(format!("tag '{tag}' selects no published operation"));
    }
    if let Some(route) = doc.api_key.as_ref().and_then(|_| {
        manifest.custom_routes.iter().find(|route| {
            custom::published(route)
                && matches!(route.posture, Some(RoutePosture::ApiKey { .. }))
                && selects(doc, &custom::tags(route, &manifest.name))
        })
    }) {
        return refused(format!(
            "{} has its key checked in X-Api-Key by the platform, so api_key cannot present another header",
            route_name(route)
        ));
    }
    if let Some(api_key) = doc
        .api_key
        .as_ref()
        .filter(|api_key| !is_field_name(&api_key.header))
    {
        return refused(format!(
            "api_key.header '{}' is not a header field name",
            api_key.header
        ));
    }
    if let Some(prefix) = doc.path_prefix.as_deref() {
        if !is_path_prefix(prefix) {
            return refused(format!(
                "path_prefix '{prefix}' must start with '/' and hold literal segments without a trailing slash"
            ));
        }
        if doc.servers.is_empty() {
            return refused(
                "path_prefix needs the document's own servers, which address the gateway that publishes the prefix"
                    .into(),
            );
        }
    }
    if doc.servers.iter().any(|server| server.url.is_empty()) {
        return refused("a server has an empty url".into());
    }
    if doc.info.title.is_empty() || doc.info.version.is_empty() {
        return refused("info needs a title and a version".into());
    }
    if doc.cors == PublicDocCors::Unknown {
        return refused("its cors mode is not one this platform knows".into());
    }
    Ok(())
}

fn disjoint(first: &PublicDoc, second: &PublicDoc) -> Result<(), OpenApiError> {
    if first.name == second.name {
        return Err(OpenApiError(format!(
            "two public documents are named '{}'",
            first.name
        )));
    }
    let (Ok(a), Ok(b)) = (mount_segments(first), mount_segments(second)) else {
        return Ok(());
    };
    let shorter = a.len().min(b.len());
    if a[..shorter] == b[..shorter] {
        return Err(OpenApiError(format!(
            "public documents '{}' and '{}' overlap: mount '{}' and mount '{}'",
            first.name, second.name, first.mount, second.mount
        )));
    }
    Ok(())
}

fn route_name(route: &CustomRouteDefinition) -> String {
    format!(
        "route '{} {}' ({})",
        route.method, route.path, route.handler
    )
}

fn selects(doc: &PublicDoc, tags: &[String]) -> bool {
    tags.iter().any(|tag| doc.include_tags.contains(tag))
}

/// Every tag a published operation of the service's spec carries.
fn published_tags(manifest: &ServiceManifest) -> BTreeSet<String> {
    manifest
        .custom_routes
        .iter()
        .filter(|route| custom::published(route))
        .flat_map(|route| custom::tags(route, &manifest.name))
        .chain(manifest.tables.iter().map(|table| table.name.clone()))
        .collect()
}

/// Whether `route` can match the bare mount, the mount followed by `/`, or
/// any path below it.
fn reaches_under(route: &CustomRouteDefinition, mount: &[&str]) -> bool {
    fn walk(route: &[Segment], mount: &[&str]) -> bool {
        match (route.split_first(), mount.split_first()) {
            (Some((Segment::CatchAll(_), _)), _) | (_, None) => true,
            (None, Some(_)) => false,
            (Some((Segment::Param(_), route)), Some((_, mount))) => walk(route, mount),
            (Some((Segment::Literal(literal), route)), Some((segment, mount))) => {
                literal == segment && walk(route, mount)
            }
        }
    }
    RoutePattern::parse(&route.path).is_ok_and(|pattern| walk(pattern.segments(), mount))
}

fn is_doc_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= 63
        && bytes.next().is_some_and(|b| b.is_ascii_lowercase())
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// An RFC 9110 field name: a non-empty token.
fn is_field_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

fn is_path_prefix(prefix: &str) -> bool {
    prefix
        .strip_prefix('/')
        .is_some_and(|below| below.split('/').all(route_path::is_literal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openapi::validate;

    fn route(method: &str, path: &str, handler: &str, posture: Value, doc: Value) -> Value {
        json!({"method": method, "path": path, "handler": handler, "posture": posture, "doc": doc})
    }

    fn public() -> Value {
        json!({"kind": "public", "reason": "catalog"})
    }

    fn catalog() -> ServiceManifest {
        let schema = |name: &str| json!({"$ref": format!("#/components/schemas/{name}")});
        serde_json::from_value(json!({
            "name": "catalog-service",
            "version": "0.1.0",
            "mode": "wasm",
            "routing": "strict",
            "tables": [],
            "openapi": {
                "title": "Catalog",
                "servers": [{"url": "https://internal.example.com"}],
                "docs": [{
                    "name": "public-api",
                    "mount": "/docs/public-api",
                    "include_tags": ["Catalog"],
                    "info": {"title": "Catalog Public API", "version": "1.0.0"},
                }],
            },
            "components": {
                "Product": {"type": "object", "properties": {
                    "price": schema("Price"),
                    "variants": {"type": "array", "items": schema("Product")},
                }},
                "Price": {"type": "object", "properties": {"amount": {"type": "integer"}}},
                "Order": {"type": "object", "properties": {
                    "lines": {"type": "array", "items": schema("OrderLine")},
                }},
                "OrderLine": {"type": "object", "properties": {"product": schema("Product")}},
                "Audit": {"type": "object"},
            },
            "custom_routes": [
                route("GET", "/products", "list_products", public(), json!({
                    "tags": ["Catalog"],
                    "responses": {"200": {"description": "Products",
                        "content": [{"content_type": "application/json", "schema": schema("Product")}]}},
                })),
                route("POST", "/orders", "place_order", public(), json!({
                    "tags": ["Orders", "Catalog"],
                    "security": ["api_key"],
                    "request": {"content_type": "application/json", "schema": schema("Order"), "required": true},
                })),
                route("GET", "/audit", "audit", json!({"kind": "authenticated"}), json!({
                    "tags": ["Admin"],
                    "responses": {"200": {"description": "Audit",
                        "content": [{"content_type": "application/json", "schema": schema("Audit")}]}},
                })),
                route("POST", "/sync", "sync", json!({"kind": "internal", "callers": ["*"]}), json!({
                    "tags": ["Catalog", "Sync"],
                })),
                route("POST", "/feeds", "ingest_feed", json!({"kind": "api_key", "scope": "feeds"}), json!({
                    "tags": ["Feeds"],
                })),
            ],
        }))
        .expect("manifest")
    }

    fn doc(manifest: &mut ServiceManifest) -> &mut PublicDoc {
        &mut manifest.openapi.as_mut().expect("openapi").docs[0]
    }

    fn spec_of(manifest: &ServiceManifest) -> Value {
        public_doc_spec(manifest, "acme", "public-api").expect("document")
    }

    fn paths(spec: &Value) -> Vec<&str> {
        spec["paths"]
            .as_object()
            .expect("paths")
            .keys()
            .map(String::as_str)
            .collect()
    }

    fn keys(value: &Value) -> Vec<&str> {
        value
            .as_object()
            .map(|object| object.keys().map(String::as_str).collect())
            .unwrap_or_default()
    }

    #[test]
    fn operations_are_selected_by_any_of_their_tags_and_internal_routes_never() {
        let mut manifest = catalog();
        assert_eq!(validate(&manifest), Ok(()));
        let spec = spec_of(&manifest);
        assert_eq!(paths(&spec), ["/orders", "/products"]);
        assert_eq!(spec["openapi"], "3.1.0");
        assert_eq!(
            spec["paths"]["/orders"]["post"]["operationId"],
            "place_order"
        );
        assert!(spec["paths"]["/products"].get("post").is_none());

        doc(&mut manifest).include_tags = vec!["Orders".into(), "Catalog".into()];
        assert_eq!(paths(&spec_of(&manifest)), ["/orders", "/products"]);
        doc(&mut manifest).include_tags = vec!["Orders".into()];
        assert_eq!(paths(&spec_of(&manifest)), ["/orders"]);
        doc(&mut manifest).include_tags = vec!["Admin".into()];
        assert_eq!(paths(&spec_of(&manifest)), ["/audit"]);
    }

    #[test]
    fn the_path_prefix_comes_before_every_path() {
        let mut manifest = catalog();
        doc(&mut manifest).path_prefix = Some("/api/v1".into());
        assert_eq!(
            paths(&spec_of(&manifest)),
            ["/api/v1/orders", "/api/v1/products"]
        );
    }

    #[test]
    fn schemas_are_the_closure_of_the_kept_operations_references() {
        let mut manifest = catalog();
        assert_eq!(
            keys(&spec_of(&manifest)["components"]["schemas"]),
            ["Order", "OrderLine", "Price", "Product"]
        );
        doc(&mut manifest).include_tags = vec!["Admin".into()];
        assert_eq!(
            keys(&spec_of(&manifest)["components"]["schemas"]),
            ["Audit"]
        );
        doc(&mut manifest).include_tags = vec!["Catalog".into()];
        manifest.custom_routes.remove(1);
        let spec = spec_of(&manifest);
        assert_eq!(keys(&spec["components"]["schemas"]), ["Price", "Product"]);
        assert_eq!(
            spec["components"]["schemas"]["Product"]["properties"]["variants"]["items"],
            json!({"$ref": "#/components/schemas/Product"})
        );
    }

    #[test]
    fn only_the_security_schemes_the_kept_operations_name_are_kept() {
        let mut manifest = catalog();
        let spec = spec_of(&manifest);
        assert_eq!(
            spec["components"]["securitySchemes"],
            json!({"api_key": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}})
        );
        assert_eq!(
            spec["paths"]["/orders"]["post"]["security"],
            json!([{"api_key": []}])
        );
        doc(&mut manifest).include_tags = vec!["Admin".into()];
        assert_eq!(
            spec_of(&manifest)["components"]["securitySchemes"],
            json!({"bearer_auth": {"type": "http", "scheme": "bearer"}})
        );
        manifest
            .custom_routes
            .retain(|route| route.handler == "list_products");
        doc(&mut manifest).include_tags = vec!["Catalog".into()];
        assert!(spec_of(&manifest)["components"]
            .get("securitySchemes")
            .is_none());
    }

    #[test]
    fn an_api_key_override_presents_the_scheme_in_its_header() {
        let mut manifest = catalog();
        doc(&mut manifest).api_key = Some(crate::ApiKeyDoc {
            header: "Authorization".into(),
            description: None,
        });
        assert_eq!(
            spec_of(&manifest)["components"]["securitySchemes"]["api_key"],
            json!({"type": "apiKey", "in": "header", "name": "Authorization"})
        );
        doc(&mut manifest).api_key = Some(crate::ApiKeyDoc {
            header: "Authorization".into(),
            description: Some("The key issued to you, without a prefix".into()),
        });
        assert_eq!(
            spec_of(&manifest)["components"]["securitySchemes"]["api_key"],
            json!({"type": "apiKey", "in": "header", "name": "Authorization",
                "description": "The key issued to you, without a prefix"})
        );
    }

    #[test]
    fn info_contact_and_tags_are_the_documents_own() {
        let mut manifest = catalog();
        let spec = spec_of(&manifest);
        assert_eq!(
            spec["info"],
            json!({"title": "Catalog Public API", "version": "1.0.0"})
        );
        assert!(spec.get("tags").is_none());
        let declared = doc(&mut manifest);
        declared.info.description = Some("Products and orders".into());
        declared.info.contact = Some(crate::ContactDoc {
            name: Some("API Support".into()),
            email: Some("support@example.com".into()),
            url: None,
        });
        declared.tags = vec![crate::TagDoc {
            name: "Catalog".into(),
            description: Some("Products and prices".into()),
        }];
        let spec = spec_of(&manifest);
        assert_eq!(
            spec["info"],
            json!({"title": "Catalog Public API", "version": "1.0.0",
                "description": "Products and orders",
                "contact": {"name": "API Support", "email": "support@example.com"}})
        );
        assert_eq!(
            spec["tags"],
            json!([{"name": "Catalog", "description": "Products and prices"}])
        );
    }

    #[test]
    fn servers_are_the_documents_then_the_services_then_its_base() {
        let mut manifest = catalog();
        assert_eq!(
            spec_of(&manifest)["servers"],
            json!([{"url": "https://internal.example.com"}])
        );
        doc(&mut manifest).servers = vec![crate::ServerDoc {
            url: "https://api.example.com".into(),
            description: Some("Production".into()),
        }];
        assert_eq!(
            spec_of(&manifest)["servers"],
            json!([{"url": "https://api.example.com", "description": "Production"}])
        );
        doc(&mut manifest).servers.clear();
        manifest.openapi.as_mut().expect("openapi").servers.clear();
        assert_eq!(
            spec_of(&manifest)["servers"],
            json!([{"url": "{origin}/svc/{tenant}/catalog-service",
                "variables": {"origin": {"default": ""}, "tenant": {"default": "acme"}}}])
        );
    }

    #[test]
    fn an_unknown_document_names_the_declared_ones() {
        let manifest = catalog();
        let error = public_doc_spec(&manifest, "acme", "admin-api").expect_err("unknown");
        assert_eq!(
            error.to_string(),
            "no public document 'admin-api': the service declares public-api"
        );
        let mut undocumented = manifest;
        undocumented.openapi.as_mut().expect("openapi").docs.clear();
        assert_eq!(
            public_doc_spec(&undocumented, "acme", "public-api")
                .expect_err("none")
                .to_string(),
            "no public document 'public-api': the service declares none"
        );
    }

    #[test]
    fn a_spec_that_cannot_be_generated_is_the_documents_error() {
        let mut manifest = catalog();
        manifest.custom_routes[1]
            .doc
            .as_mut()
            .expect("doc")
            .operation_id = Some("list_products".into());
        assert_eq!(
            public_doc_spec(&manifest, "acme", "public-api"),
            Err(PublicDocError::Spec(OpenApiError(
                "operation id 'list_products' is used more than once".into()
            )))
        );
    }

    fn refused(edit: impl FnOnce(&mut ServiceManifest)) -> String {
        let mut manifest = catalog();
        edit(&mut manifest);
        validate(&manifest).expect_err("refused").to_string()
    }

    fn accepted(edit: impl FnOnce(&mut ServiceManifest)) {
        let mut manifest = catalog();
        edit(&mut manifest);
        assert_eq!(validate(&manifest), Ok(()));
    }

    fn second(name: &str, mount: &str) -> PublicDoc {
        PublicDoc {
            name: name.into(),
            mount: mount.into(),
            ..doc(&mut catalog()).clone()
        }
    }

    fn add_route(manifest: &mut ServiceManifest, method: &str, path: &str) {
        manifest.custom_routes.push(CustomRouteDefinition {
            method: method.into(),
            path: path.into(),
            handler: "h".into(),
            posture: Some(RoutePosture::Public {
                reason: "catalog".into(),
            }),
            ..Default::default()
        });
    }

    #[test]
    fn documents_need_strict_routing() {
        assert_eq!(
            refused(|m| m.routing = RoutingMode::Legacy),
            "public document 'public-api' needs routing: strict"
        );
    }

    #[test]
    fn a_document_name_is_a_unique_lowercase_slug() {
        accepted(|m| doc(m).name = format!("a{}", "-".repeat(62)));
        for name in [
            "Public",
            "1api",
            "",
            "api_v1",
            &format!("a{}", "b".repeat(63)),
        ] {
            assert_eq!(
                refused(|m| doc(m).name = name.into()),
                format!("public document '{name}': a document name matches [a-z][a-z0-9-]{{0,62}}")
            );
        }
        assert_eq!(
            refused(|m| {
                let twin = second("public-api", "/docs/other");
                m.openapi.as_mut().expect("openapi").docs.push(twin);
            }),
            "two public documents are named 'public-api'"
        );
    }

    #[test]
    fn a_document_includes_some_tag() {
        assert_eq!(
            refused(|m| doc(m).include_tags.clear()),
            "public document 'public-api': include_tags is empty"
        );
    }

    #[test]
    fn a_mount_is_literal_segments_outside_the_platforms_own() {
        for (mount, reason) in [
            ("/docs/{version}", "must be one or more literal segments"),
            ("/", "must be one or more literal segments"),
            (
                "/docs/",
                "is not a path: a route path may not contain an empty segment",
            ),
            (
                "//docs",
                "is not a path: a route path may not contain an empty segment",
            ),
            ("docs", "is not a path: a route path must start with '/'"),
            (
                "/_meta/docs",
                "is not a path: the first segment '_meta' is reserved by the platform",
            ),
            (
                "/graphql",
                "is not a path: the first segment 'graphql' is reserved by the platform",
            ),
        ] {
            assert_eq!(
                refused(|m| doc(m).mount = mount.into()),
                format!("public document 'public-api': mount '{mount}' {reason}")
            );
        }
        assert_eq!(
            mount_segments(&second("x", "/docs/v1.2/public-api")),
            Ok(vec!["docs", "v1.2", "public-api"])
        );
    }

    #[test]
    fn a_mount_is_not_under_a_tables_data_routes() {
        let table: crate::TableDefinition = serde_json::from_value(json!({
            "name": "items",
            "columns": [{"name": "id", "column_type": "uuid", "primary_key": true}]
        }))
        .expect("table");
        accepted(|m| m.tables.push(table.clone()));
        assert_eq!(
            refused(|m| {
                m.tables.push(table);
                doc(m).mount = "/items/docs".into();
            }),
            "public document 'public-api': mount '/items/docs' is under the data routes of table 'items'"
        );
    }

    #[test]
    fn no_route_reaches_under_a_mount() {
        for path in [
            "/docs/public-api",
            "/docs/{name}",
            "/{*rest}",
            "/docs/{*rest}",
            "/docs/public-api/x",
            "/docs/public-api/{file}",
            "/{section}/public-api/openapi.json",
        ] {
            assert_eq!(
                refused(|m| add_route(m, "GET", path)),
                format!(
                    "public document 'public-api': route 'GET {path}' (h) reaches under its mount '/docs/public-api'"
                )
            );
        }
        for path in [
            "/docs",
            "/{id}",
            "/docs/public-api-v2/x",
            "/docs/public",
            "/",
        ] {
            accepted(|m| add_route(m, "POST", path));
        }
    }

    #[test]
    fn mounts_are_disjoint() {
        for mount in ["/docs", "/docs/public-api", "/docs/public-api/v2"] {
            assert_eq!(
                refused(|m| {
                    let other = second("admin-api", mount);
                    m.openapi.as_mut().expect("openapi").docs.push(other);
                }),
                format!(
                    "public documents 'public-api' and 'admin-api' overlap: mount '/docs/public-api' and mount '{mount}'"
                )
            );
        }
        accepted(|m| {
            let other = second("admin-api", "/docs/public-api-v2");
            m.openapi.as_mut().expect("openapi").docs.push(other);
        });
    }

    #[test]
    fn every_included_tag_selects_a_published_operation() {
        assert_eq!(
            refused(|m| doc(m).include_tags.push("Shipping".into())),
            "public document 'public-api': tag 'Shipping' selects no published operation"
        );
        assert_eq!(
            refused(|m| doc(m).include_tags = vec!["Sync".into()]),
            "public document 'public-api': tag 'Sync' selects no published operation"
        );
        accepted(|m| doc(m).include_tags = vec!["Feeds".into(), "Orders".into()]);
        accepted(|m| {
            add_route(m, "GET", "/brands");
            doc(m).include_tags = vec!["catalog-service".into()];
        });
    }

    #[test]
    fn an_api_key_override_selects_no_route_whose_key_the_platform_checks() {
        let header = |m: &mut ServiceManifest| {
            doc(m).api_key = Some(crate::ApiKeyDoc {
                header: "Authorization".into(),
                description: None,
            })
        };
        accepted(header);
        assert_eq!(
            refused(|m| {
                header(m);
                doc(m).include_tags.push("Feeds".into());
            }),
            "public document 'public-api': route 'POST /feeds' (ingest_feed) has its key checked in X-Api-Key by the platform, so api_key cannot present another header"
        );
        accepted(|m| doc(m).include_tags.push("Feeds".into()));
    }

    #[test]
    fn an_api_key_header_is_a_field_name() {
        for header in ["X Key", "", "X-Key:", "Ключ"] {
            assert_eq!(
                refused(|m| {
                    doc(m).api_key = Some(crate::ApiKeyDoc {
                        header: header.into(),
                        description: None,
                    })
                }),
                format!("public document 'public-api': api_key.header '{header}' is not a header field name")
            );
        }
    }

    #[test]
    fn a_path_prefix_is_literal_and_needs_the_documents_servers() {
        let servers = |m: &mut ServiceManifest| {
            doc(m).servers = vec![crate::ServerDoc {
                url: "https://api.example.com".into(),
                description: None,
            }]
        };
        accepted(|m| {
            servers(m);
            doc(m).path_prefix = Some("/api/v1".into());
        });
        for prefix in ["api", "/api/", "/", "/a b", "//api"] {
            assert_eq!(
                refused(|m| {
                    servers(m);
                    doc(m).path_prefix = Some(prefix.into());
                }),
                format!("public document 'public-api': path_prefix '{prefix}' must start with '/' and hold literal segments without a trailing slash")
            );
        }
        assert_eq!(
            refused(|m| doc(m).path_prefix = Some("/api".into())),
            "public document 'public-api': path_prefix needs the document's own servers, which address the gateway that publishes the prefix"
        );
    }

    #[test]
    fn servers_and_info_are_not_empty() {
        assert_eq!(
            refused(|m| {
                doc(m).servers = vec![crate::ServerDoc {
                    url: String::new(),
                    description: None,
                }]
            }),
            "public document 'public-api': a server has an empty url"
        );
        for edit in [
            (|m: &mut ServiceManifest| doc(m).info.title.clear()) as fn(&mut ServiceManifest),
            |m| doc(m).info.version.clear(),
        ] {
            assert_eq!(
                refused(edit),
                "public document 'public-api': info needs a title and a version"
            );
        }
    }

    #[test]
    fn a_cors_mode_this_platform_does_not_know_is_refused() {
        for mode in [
            PublicDocCors::Restricted,
            PublicDocCors::Any,
            PublicDocCors::Mirror,
        ] {
            accepted(|m| doc(m).cors = mode);
        }
        assert_eq!(
            refused(|m| doc(m).cors = PublicDocCors::Unknown),
            "public document 'public-api': its cors mode is not one this platform knows"
        );
    }
}
