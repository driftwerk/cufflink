use super::*;
use crate::router::swagger_ui::ASSETS;
use serde_json::json;
use uuid::Uuid;

pub(crate) fn catalog(docs: serde_json::Value) -> RegisteredService {
    RegisteredService {
        service_id: Uuid::new_v4(),
        tenant_id: Uuid::new_v4(),
        tenant_slug: "acme".into(),
        name: "catalog-service".into(),
        manifest: serde_json::from_value(json!({
            "name": "catalog-service",
            "version": "0.1.0",
            "mode": "wasm",
            "routing": "strict",
            "tables": [],
            "openapi": {"title": "Catalog", "docs": docs},
            "custom_routes": [{
                "method": "GET",
                "path": "/orders",
                "handler": "list_orders",
                "posture": {"kind": "public", "reason": "catalog"},
                "doc": {"tags": ["Catalog"]},
            }],
        }))
        .expect("manifest"),
        version: 3,
        wasm_hash: None,
        commit_hash: None,
    }
}

pub(crate) fn public_api(cors: &str) -> serde_json::Value {
    json!({
        "name": "public-api",
        "mount": "/docs/public-api",
        "include_tags": ["Catalog"],
        "info": {"title": "Catalog Public API", "version": "1.0.0"},
        "cors": cors,
    })
}

fn docs() -> PublicDocs {
    PublicDocs::build(&catalog(json!([public_api("mirror")])))
}

fn file(rest: &str) -> Option<DocFile> {
    docs().target(rest).map(|target| target.file)
}

#[test]
fn a_path_is_under_a_mount_only_segment_by_segment() {
    let page = |name: &str| Some(DocFile::Page(name.to_string()));
    assert_eq!(
        file("/docs/public-api/openapi.json"),
        Some(DocFile::OpenApi)
    );
    assert_eq!(
        file("/docs/public-api/swagger-ui"),
        Some(DocFile::SwaggerUi)
    );
    assert_eq!(file("/docs/public-api/swagger-ui/"), page(""));
    assert_eq!(
        file("/docs/public-api/swagger-ui/index.css"),
        page("index.css")
    );
    for missing in [
        "/docs/public-api",
        "/docs/public-api/",
        "/docs/public-api/openapi.json/",
        "/docs/public-api/swagger-ui/a/b",
        "/docs/public-api/elsewhere",
    ] {
        assert_eq!(file(missing), Some(DocFile::Missing), "{missing}");
    }
    for outside in [
        "/docs/public-api-v2/x",
        "/docs/public-apix",
        "/docs/public",
        "/docs",
        "/orders",
        "",
        "docs/public-api/openapi.json",
    ] {
        assert_eq!(file(outside), None, "{outside}");
    }
}

#[test]
fn documents_are_built_for_one_version_and_tenant() {
    let service = catalog(json!([public_api("restricted")]));
    let docs = PublicDocs::build(&service);
    assert!(docs.built_for(&service));
    assert!(!docs.is_open());
    assert!(!docs.built_for(&RegisteredService {
        version: 4,
        ..service.clone()
    }));
    assert!(!docs.built_for(&RegisteredService {
        tenant_slug: "globex".into(),
        ..service
    }));
    assert!(self::docs().is_open());
}

#[test]
fn a_document_that_cannot_be_built_is_left_out_and_the_others_stay() {
    let mut unmountable = public_api("any");
    unmountable["name"] = json!("status-api");
    unmountable["mount"] = json!("/docs/{version}");
    let docs = PublicDocs::build(&catalog(json!([unmountable, public_api("restricted")])));
    assert_eq!(
        docs.documents
            .iter()
            .map(|document| document.name.as_str())
            .collect::<Vec<_>>(),
        ["public-api"]
    );
    assert!(!docs.is_open());
}

#[test]
fn an_unknown_cors_mode_is_served_as_restricted() {
    let docs = PublicDocs::build(&catalog(json!([public_api("credentialed")])));
    let target = docs
        .target("/docs/public-api/openapi.json")
        .expect("mounted");
    assert_eq!(docs.cors(&target), PublicDocCors::Restricted);
    assert!(!docs.is_open());
}

async fn answered(method: Method, rest: &str, headers: &[(&str, &str)]) -> (Response, Vec<u8>) {
    let docs = docs();
    let target = docs.target(rest).expect("mounted");
    let headers: HeaderMap = headers
        .iter()
        .map(|(name, value)| {
            (
                header::HeaderName::from_bytes(name.as_bytes()).expect("name"),
                HeaderValue::from_str(value).expect("value"),
            )
        })
        .collect();
    let response = answer(&docs, &target, &method, &headers);
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("body")
        .to_vec();
    (Response::from_parts(parts, Body::empty()), bytes)
}

fn header_of(response: &Response, name: header::HeaderName) -> Option<&str> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

#[tokio::test]
async fn every_served_path_answers_its_file() {
    let (response, body) = answered(Method::GET, "/docs/public-api/openapi.json", &[]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header_of(&response, header::CONTENT_TYPE),
        Some("application/json")
    );
    let spec: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(spec["info"]["title"], "Catalog Public API");
    assert_eq!(
        spec["paths"]
            .as_object()
            .expect("paths")
            .keys()
            .collect::<Vec<_>>(),
        ["/orders"]
    );
    assert_eq!(
        header_of(&response, header::CONTENT_LENGTH),
        Some(body.len().to_string().as_str())
    );
    assert_eq!(
        header_of(&response, header::ETAG),
        Some(format!("\"{}\"", hex::encode(Sha256::digest(&body))).as_str())
    );
    assert_eq!(
        header_of(&response, header::CACHE_CONTROL),
        Some("no-cache")
    );

    let (index, index_body) = answered(Method::GET, "/docs/public-api/swagger-ui/", &[]).await;
    let (named, named_body) =
        answered(Method::GET, "/docs/public-api/swagger-ui/index.html", &[]).await;
    assert_eq!(
        (index.status(), named.status()),
        (StatusCode::OK, StatusCode::OK)
    );
    assert_eq!(index_body, named_body);
    assert_eq!(
        index_body,
        include_bytes!("../../../assets/swagger-ui/index.html")
    );
    assert_eq!(
        header_of(&index, header::ETAG),
        header_of(&named, header::ETAG)
    );

    for asset in ASSETS.iter() {
        let (response, body) = answered(
            Method::GET,
            &format!("/docs/public-api/swagger-ui/{}", asset.name),
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{}", asset.name);
        assert_eq!(
            header_of(&response, header::CONTENT_TYPE),
            Some(asset.content_type)
        );
        assert_eq!(body, asset.bytes, "{}", asset.name);
    }

    let (redirect, body) = answered(Method::GET, "/docs/public-api/swagger-ui", &[]).await;
    assert_eq!(redirect.status(), StatusCode::SEE_OTHER);
    assert_eq!(header_of(&redirect, header::LOCATION), Some("swagger-ui/"));
    assert!(body.is_empty());
}

#[tokio::test]
async fn a_head_answer_carries_the_length_and_no_body() {
    let (get, body) = answered(Method::GET, "/docs/public-api/openapi.json", &[]).await;
    let (head, empty) = answered(Method::HEAD, "/docs/public-api/openapi.json", &[]).await;
    assert_eq!(head.status(), StatusCode::OK);
    assert!(empty.is_empty());
    assert_eq!(
        header_of(&head, header::CONTENT_LENGTH),
        Some(body.len().to_string().as_str())
    );
    assert_eq!(
        header_of(&head, header::ETAG),
        header_of(&get, header::ETAG)
    );
}

#[tokio::test]
async fn anything_else_under_the_mount_is_not_found() {
    for rest in [
        "/docs/public-api",
        "/docs/public-api/",
        "/docs/public-api/elsewhere/deeper",
        "/docs/public-api/swagger-ui/unknown.js",
        "/docs/public-api/swagger-ui/swagger-ui.js",
        "/docs/public-api/swagger-ui/swagger-ui.js.map",
        "/docs/public-api/swagger-ui/oauth2-redirect.html",
        "/docs/public-api/swagger-ui/LICENSE",
    ] {
        for method in [Method::GET, Method::POST] {
            let (response, body) = answered(method.clone(), rest, &[]).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {rest}");
            assert_eq!(body, br#"{"error":"Not found"}"#, "{method} {rest}");
        }
    }
}

#[tokio::test]
async fn other_methods_are_not_allowed_on_the_served_paths() {
    for rest in [
        "/docs/public-api/openapi.json",
        "/docs/public-api/swagger-ui",
        "/docs/public-api/swagger-ui/",
        "/docs/public-api/swagger-ui/swagger-initializer.js",
    ] {
        for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
            let (response, _) = answered(method.clone(), rest, &[]).await;
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {rest}"
            );
            assert_eq!(header_of(&response, header::ALLOW), Some("GET,HEAD"));
        }
    }
}

#[tokio::test]
async fn a_matching_if_none_match_answers_not_modified() {
    let (fresh, _) = answered(Method::GET, "/docs/public-api/openapi.json", &[]).await;
    let etag = header_of(&fresh, header::ETAG).expect("etag").to_string();
    let weak = format!("W/{etag}");
    let listed = format!("\"other\", {etag}, W/\"more\"");
    for value in [etag.as_str(), weak.as_str(), listed.as_str(), "*"] {
        let (response, body) = answered(
            Method::GET,
            "/docs/public-api/openapi.json",
            &[("if-none-match", value)],
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED, "{value}");
        assert_eq!(header_of(&response, header::ETAG), Some(etag.as_str()));
        assert_eq!(
            header_of(&response, header::CACHE_CONTROL),
            Some("no-cache")
        );
        assert!(body.is_empty());
    }
    let unquoted = etag.trim_matches('"').to_string();
    let starred = format!("*, {etag}");
    for value in [
        "\"other\", W/\"more\"",
        unquoted.as_str(),
        starred.as_str(),
        "\"open",
        "",
    ] {
        let (response, _) = answered(
            Method::GET,
            "/docs/public-api/openapi.json",
            &[("if-none-match", value)],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{value}");
    }
}

#[tokio::test]
async fn the_meta_page_serves_only_its_shared_files() {
    let css = meta_asset("swagger-ui.css", &HeaderMap::new());
    assert_eq!(css.status(), StatusCode::OK);
    assert_eq!(header_of(&css, header::CONTENT_TYPE), Some("text/css"));
    for name in ["index.html", "swagger-initializer.js", "", "LICENSE"] {
        assert_eq!(
            meta_asset(name, &HeaderMap::new()).status(),
            StatusCode::NOT_FOUND,
            "{name}"
        );
    }
}

#[test]
fn a_platform_that_does_not_advertise_public_docs_refuses_a_documented_service() {
    let manifest = catalog(json!([public_api("restricted")])).manifest;
    let demanded = manifest.derived_features();
    assert!(demanded.contains(PUBLIC_DOCS));
    let older: Vec<&str> = demanded
        .iter()
        .copied()
        .filter(|feature| *feature != PUBLIC_DOCS)
        .collect();
    let refused = crate::deployer::validator::validate_platform_requirements(
        demanded.iter().copied(),
        &older,
    )
    .expect_err("refused");
    assert!(
        refused
            .to_string()
            .contains("requires platform feature 'public_docs'"),
        "{refused}"
    );
}

#[tokio::test]
async fn an_if_none_match_with_obs_text_tags_is_read_as_bytes() {
    let docs = docs();
    let target = docs
        .target("/docs/public-api/openapi.json")
        .expect("mounted");
    let etag = docs.documents[0].openapi.etag.clone();
    let status = |value: Vec<u8>| {
        let headers = HeaderMap::from_iter([(
            header::IF_NONE_MATCH,
            HeaderValue::from_bytes(&value).expect("obs-text is a valid header value"),
        )]);
        answer(&docs, &target, &Method::GET, &headers).status()
    };
    let foreign = b"\"caf\xc3\xa9\"".to_vec();
    assert_eq!(
        status([foreign.as_slice(), b", ", etag.as_bytes()].concat()),
        StatusCode::NOT_MODIFIED
    );
    assert_eq!(status(foreign), StatusCode::OK);
}
