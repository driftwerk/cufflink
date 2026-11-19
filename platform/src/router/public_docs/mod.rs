pub(crate) mod cors;

use crate::errors::PlatformError;
use crate::registry::RegisteredService;
use crate::router::swagger_ui::{self, ASSETS};
use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use cufflink_types::features::PUBLIC_DOCS;
use cufflink_types::openapi::{mount_segments, public_doc_spec};
use cufflink_types::{PublicDoc, PublicDocCors};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::{error, warn};

pub(crate) const FEATURES: &[&str] = &[PUBLIC_DOCS];

const OPENAPI: &str = "openapi.json";
const SWAGGER_UI: &str = "swagger-ui";
const ALLOWED: &str = "GET,HEAD";

pub(crate) struct PublicDocs {
    version: i32,
    tenant_slug: String,
    documents: Vec<Document>,
}

struct Document {
    name: String,
    mount: Vec<String>,
    cors: PublicDocCors,
    openapi: Entity,
}

struct Entity {
    content_type: &'static str,
    body: Bytes,
    etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocTarget {
    document: usize,
    file: DocFile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DocFile {
    OpenApi,
    SwaggerUi,
    Page(String),
    Missing,
}

static PAGE_FILES: LazyLock<HashMap<&'static str, Entity>> = LazyLock::new(|| {
    ASSETS
        .iter()
        .map(|asset| {
            (
                asset.name,
                Entity::new(asset.content_type, Bytes::from_static(asset.bytes)),
            )
        })
        .collect()
});

impl Entity {
    fn new(content_type: &'static str, body: Bytes) -> Self {
        let etag = format!("\"{}\"", hex::encode(Sha256::digest(&body)));
        Self {
            content_type,
            body,
            etag,
        }
    }
}

impl PublicDocs {
    pub(crate) fn build(service: &RegisteredService) -> Self {
        let documents = service
            .manifest
            .public_docs()
            .filter_map(|doc| match Document::build(service, doc) {
                Ok(document) => Some(document),
                Err(error) => {
                    metrics::counter!("cufflink_public_docs_build_errors_total").increment(1);
                    error!(
                        service = %service.name,
                        tenant = %service.tenant_slug,
                        document = %doc.name,
                        error = %error,
                        "public document not built; it answers 404"
                    );
                    None
                }
            })
            .collect();
        Self {
            version: service.version,
            tenant_slug: service.tenant_slug.clone(),
            documents,
        }
    }

    pub(crate) fn built_for(&self, service: &RegisteredService) -> bool {
        self.version == service.version && self.tenant_slug == service.tenant_slug
    }

    pub(crate) fn is_open(&self) -> bool {
        self.documents
            .iter()
            .any(|document| !document.cors.is_restricted())
    }

    pub(crate) fn target(&self, rest: &str) -> Option<DocTarget> {
        let segments: Vec<&str> = rest.strip_prefix('/')?.split('/').collect();
        self.documents
            .iter()
            .enumerate()
            .find_map(|(at, document)| {
                let below = below_mount(&segments, &document.mount)?;
                Some(DocTarget {
                    document: at,
                    file: DocFile::of(below),
                })
            })
    }

    pub(crate) fn cors(&self, target: &DocTarget) -> PublicDocCors {
        self.documents[target.document].cors
    }

    pub(crate) fn name(&self, target: &DocTarget) -> &str {
        &self.documents[target.document].name
    }
}

fn below_mount<'a>(segments: &'a [&'a str], mount: &[String]) -> Option<&'a [&'a str]> {
    (segments.len() >= mount.len() && segments.iter().zip(mount).all(|(seg, at)| seg == at))
        .then(|| &segments[mount.len()..])
}

impl Document {
    fn build(service: &RegisteredService, doc: &PublicDoc) -> Result<Self, String> {
        let mount = mount_segments(doc)
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(str::to_string)
            .collect();
        let spec = public_doc_spec(&service.manifest, &service.tenant_slug, &doc.name)
            .map_err(|e| e.to_string())?;
        let body = serde_json::to_vec(&spec).map_err(|e| e.to_string())?;
        let cors = match doc.cors {
            PublicDocCors::Unknown => {
                warn!(
                    service = %service.name,
                    tenant = %service.tenant_slug,
                    document = %doc.name,
                    "public document's cors mode is not one this platform knows; it is served as restricted"
                );
                PublicDocCors::Restricted
            }
            known => known,
        };
        Ok(Self {
            name: doc.name.clone(),
            mount,
            cors,
            openapi: Entity::new("application/json", Bytes::from(body)),
        })
    }
}

impl DocFile {
    fn of(below: &[&str]) -> Self {
        match below {
            [OPENAPI] => Self::OpenApi,
            [SWAGGER_UI] => Self::SwaggerUi,
            [SWAGGER_UI, file] => Self::Page((*file).to_string()),
            _ => Self::Missing,
        }
    }
}

enum Answer<'a> {
    Entity(&'a Entity),
    Redirect,
}

pub(crate) fn answer(
    docs: &PublicDocs,
    target: &DocTarget,
    method: &Method,
    headers: &HeaderMap,
) -> Response {
    let found = match &target.file {
        DocFile::OpenApi => Some(Answer::Entity(&docs.documents[target.document].openapi)),
        DocFile::SwaggerUi => Some(Answer::Redirect),
        DocFile::Page(file) => page_file(file).map(Answer::Entity),
        DocFile::Missing => None,
    };
    match (found, method) {
        (None, _) => not_found(),
        (Some(_), method) if method != Method::GET && method != Method::HEAD => {
            (StatusCode::METHOD_NOT_ALLOWED, [(header::ALLOW, ALLOWED)]).into_response()
        }
        (Some(Answer::Redirect), _) => {
            (StatusCode::SEE_OTHER, [(header::LOCATION, "swagger-ui/")]).into_response()
        }
        (Some(Answer::Entity(entity)), method) => serve(entity, method, headers),
    }
}

pub(crate) fn meta_asset(name: &str, headers: &HeaderMap) -> Response {
    swagger_ui::meta_file(name)
        .and_then(|asset| PAGE_FILES.get(asset.name))
        .map_or_else(not_found, |entity| serve(entity, &Method::GET, headers))
}

fn page_file(name: &str) -> Option<&'static Entity> {
    swagger_ui::page_file(name).and_then(|asset| PAGE_FILES.get(asset.name))
}

fn not_found() -> Response {
    PlatformError::NotFound("Not found".into()).into_response()
}

fn serve(entity: &Entity, method: &Method, headers: &HeaderMap) -> Response {
    let fresh = if_none_match(headers).is_some_and(|tags| tags.matches(&entity.etag));
    let builder = Response::builder()
        .header(header::ETAG, entity.etag.as_str())
        .header(header::CACHE_CONTROL, "no-cache");
    let built = match (fresh, method == Method::HEAD) {
        (true, _) => builder.status(StatusCode::NOT_MODIFIED).body(Body::empty()),
        (false, head) => builder
            .header(header::CONTENT_TYPE, entity.content_type)
            .header(header::CONTENT_LENGTH, entity.body.len())
            .body(match head {
                true => Body::empty(),
                false => Body::from(entity.body.clone()),
            }),
    };
    built.unwrap_or_else(|e| {
        PlatformError::Internal(format!("public document: {e}")).into_response()
    })
}

enum IfNoneMatch<'a> {
    Any,
    Tags(Vec<&'a str>),
}

impl IfNoneMatch<'_> {
    fn matches(&self, etag: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Tags(tags) => tags.contains(&etag),
        }
    }
}

fn if_none_match(headers: &HeaderMap) -> Option<IfNoneMatch<'_>> {
    let values = headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .map(|value| value.to_str().ok())
        .collect::<Option<Vec<_>>>()?;
    match values.as_slice() {
        [] => None,
        [value] if value.trim() == "*" => Some(IfNoneMatch::Any),
        values => values
            .iter()
            .flat_map(|value| value.split(','))
            .map(str::trim)
            .filter(|element| !element.is_empty())
            .map(opaque_tag)
            .collect::<Option<Vec<_>>>()
            .filter(|tags| !tags.is_empty())
            .map(IfNoneMatch::Tags),
    }
}

fn opaque_tag(entity_tag: &str) -> Option<&str> {
    let opaque = entity_tag.strip_prefix("W/").unwrap_or(entity_tag);
    let inner = opaque.strip_prefix('"')?.strip_suffix('"')?;
    inner
        .bytes()
        .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte) || byte >= 0x80)
        .then_some(opaque)
}

#[cfg(test)]
pub(crate) mod tests;
