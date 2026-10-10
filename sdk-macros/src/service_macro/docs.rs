//! OpenAPI documentation keywords: a route's `request`, `response(s)`,
//! `query`, `path`, `tags`, `operation_id`, `summary`, `description`,
//! `deprecated` and `security`, and the service's `openapi: { … }`.

use super::emit::optional_string;
use cufflink_types::ResponseDoc;
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitInt, LitStr, Token, Type};

#[derive(Default)]
pub(super) struct RouteDocOpts {
    operation_id: Option<LitStr>,
    summary: Option<LitStr>,
    description: Option<LitStr>,
    tags: Option<Vec<LitStr>>,
    deprecated: bool,
    path: Option<Type>,
    query: Option<Type>,
    request: Option<Request>,
    responses: Option<Vec<ResponseItem>>,
    security: Option<Vec<LitStr>>,
}

enum Request {
    Typed(Box<Type>, Option<LitStr>),
    MultipartForm,
}

struct ResponseItem {
    status: u16,
    body: Option<Body>,
    description: Option<LitStr>,
}

enum Body {
    Typed(Box<Type>, Option<LitStr>),
    Bytes(LitStr),
}

/// A route keyword this module owns.
#[derive(Clone, Copy)]
pub(super) enum DocKeyword {
    OperationId,
    Summary,
    Description,
    Tags,
    Deprecated,
    Path,
    Query,
    Request,
    Response,
    Responses,
    Security,
}

impl DocKeyword {
    pub(super) fn of(keyword: &str) -> Option<Self> {
        Some(match keyword {
            "operation_id" => DocKeyword::OperationId,
            "summary" => DocKeyword::Summary,
            "description" => DocKeyword::Description,
            "tags" => DocKeyword::Tags,
            "deprecated" => DocKeyword::Deprecated,
            "path" => DocKeyword::Path,
            "query" => DocKeyword::Query,
            "request" => DocKeyword::Request,
            "response" => DocKeyword::Response,
            "responses" => DocKeyword::Responses,
            "security" => DocKeyword::Security,
            _ => return None,
        })
    }
}

fn set<T>(slot: &mut Option<T>, value: T, kw: &Ident) -> syn::Result<()> {
    match slot.replace(value) {
        Some(_) => Err(syn::Error::new(
            kw.span(),
            format!("duplicate {kw} modifier"),
        )),
        None => Ok(()),
    }
}

fn assigned<T: syn::parse::Parse>(input: ParseStream) -> syn::Result<T> {
    input.parse::<Token![=]>()?;
    input.parse()
}

fn string_list(input: ParseStream) -> syn::Result<Vec<LitStr>> {
    input.parse::<Token![=]>()?;
    let content;
    syn::bracketed!(content in input);
    Ok(content
        .parse_terminated(|s| s.parse::<LitStr>(), Token![,])?
        .into_iter()
        .collect())
}

impl RouteDocOpts {
    pub(super) fn apply(
        &mut self,
        keyword: DocKeyword,
        kw: &Ident,
        input: ParseStream,
    ) -> syn::Result<()> {
        match keyword {
            DocKeyword::OperationId => set(&mut self.operation_id, assigned(input)?, kw),
            DocKeyword::Summary => set(&mut self.summary, assigned(input)?, kw),
            DocKeyword::Description => set(&mut self.description, assigned(input)?, kw),
            DocKeyword::Tags => set(&mut self.tags, string_list(input)?, kw),
            DocKeyword::Security => set(&mut self.security, string_list(input)?, kw),
            DocKeyword::Path => set(&mut self.path, assigned(input)?, kw),
            DocKeyword::Query => set(&mut self.query, assigned(input)?, kw),
            DocKeyword::Deprecated if self.deprecated => {
                Err(syn::Error::new(kw.span(), "duplicate deprecated modifier"))
            }
            DocKeyword::Deprecated => {
                self.deprecated = true;
                Ok(())
            }
            DocKeyword::Request => {
                input.parse::<Token![=]>()?;
                set(&mut self.request, parse_request(input)?, kw)
            }
            DocKeyword::Response => {
                input.parse::<Token![=]>()?;
                let item = parse_response(input)?;
                self.push_responses(vec![item], kw)
            }
            DocKeyword::Responses => {
                input.parse::<Token![=]>()?;
                let content;
                syn::bracketed!(content in input);
                let items = content
                    .parse_terminated(parse_response, Token![,])?
                    .into_iter()
                    .collect();
                self.push_responses(items, kw)
            }
        }
    }

    fn push_responses(&mut self, items: Vec<ResponseItem>, kw: &Ident) -> syn::Result<()> {
        match &mut self.responses {
            Some(_) => Err(syn::Error::new(
                kw.span(),
                "a route declares its responses once: response = … or responses = [...]",
            )),
            None => {
                self.responses = Some(items);
                Ok(())
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.operation_id.is_none()
            && self.summary.is_none()
            && self.description.is_none()
            && self.tags.is_none()
            && !self.deprecated
            && self.path.is_none()
            && self.query.is_none()
            && self.request.is_none()
            && self.responses.is_none()
            && self.security.is_none()
    }

    /// Whether the doc names a type, whose schema needs the collector.
    pub(super) fn uses_types(&self) -> bool {
        self.path.is_some()
            || self.query.is_some()
            || matches!(self.request, Some(Request::Typed(..)))
            || self
                .responses
                .iter()
                .flatten()
                .any(|item| matches!(item.body, Some(Body::Typed(..))))
    }

    /// The `Option<RouteDoc>` expression; `schemas` names the generated
    /// main's `SchemaCollector`.
    pub(super) fn tokens(&self, schemas: &Ident) -> TokenStream {
        if self.is_empty() {
            return quote! { ::std::option::Option::None };
        }
        let strings = |values: &[LitStr]| quote! { vec![#(#values.to_string()),*] };
        let operation_id = optional_string(self.operation_id.as_ref());
        let summary = optional_string(self.summary.as_ref());
        let description = optional_string(self.description.as_ref());
        let tags = strings(self.tags.as_deref().unwrap_or_default());
        let deprecated = self.deprecated;
        let inline = |ty: &Option<Type>| match ty {
            Some(ty) => quote! { ::std::option::Option::Some(#schemas.inline_schema_for::<#ty>()) },
            None => quote! { ::std::option::Option::None },
        };
        let path = inline(&self.path);
        let query = inline(&self.query);
        let request = match &self.request {
            None => quote! { ::std::option::Option::None },
            Some(Request::Typed(ty, media)) => {
                let content_type = media_type(media.as_ref());
                quote! {
                    ::std::option::Option::Some(::cufflink::manifest::BodyDoc {
                        content_type: #content_type.to_string(),
                        schema: ::std::option::Option::Some(#schemas.schema_for::<#ty>()),
                        required: true,
                    })
                }
            }
            Some(Request::MultipartForm) => quote! {
                ::std::option::Option::Some(::cufflink::manifest::BodyDoc {
                    content_type: "multipart/form-data".to_string(),
                    schema: ::std::option::Option::Some(::serde_json::json!({
                        "type": "object",
                        "additionalProperties": { "type": "string", "format": "binary" }
                    })),
                    required: true,
                })
            },
        };
        let responses = self
            .responses
            .iter()
            .flatten()
            .map(|item| response_tokens(item, schemas));
        let security = match &self.security {
            Some(names) => {
                let names = strings(names);
                quote! { ::std::option::Option::Some(#names) }
            }
            None => quote! { ::std::option::Option::None },
        };
        quote! {
            ::std::option::Option::Some(::cufflink::manifest::RouteDoc {
                operation_id: #operation_id,
                summary: #summary,
                description: #description,
                tags: #tags,
                deprecated: #deprecated,
                path_params: #path,
                query: #query,
                request: #request,
                responses: [#(#responses),*].into_iter().collect(),
                security: #security,
            })
        }
    }
}

fn media_type(media: Option<&LitStr>) -> TokenStream {
    match media {
        Some(media) => quote! { #media },
        None => quote! { "application/json" },
    }
}

fn response_tokens(item: &ResponseItem, schemas: &Ident) -> TokenStream {
    let status = item.status;
    let description = match &item.description {
        Some(description) => quote! { #description },
        None => {
            let default = ResponseDoc::default_description(status);
            quote! { #default }
        }
    };
    let content = match &item.body {
        None => quote! { vec![] },
        Some(Body::Typed(ty, media)) => {
            let content_type = media_type(media.as_ref());
            quote! {
                vec![::cufflink::manifest::MediaDoc {
                    content_type: #content_type.to_string(),
                    schema: ::std::option::Option::Some(#schemas.schema_for::<#ty>()),
                }]
            }
        }
        Some(Body::Bytes(media)) => quote! {
            vec![::cufflink::manifest::MediaDoc {
                content_type: #media.to_string(),
                schema: ::std::option::Option::None,
            }]
        },
    };
    quote! {
        (#status, ::cufflink::manifest::ResponseDoc {
            description: #description.to_string(),
            content: #content,
        })
    }
}

/// `T`, `(T, "mime")` or `multipart_form`.
fn parse_request(input: ParseStream) -> syn::Result<Request> {
    if input.peek(Ident) && input.fork().parse::<Ident>()? == "multipart_form" {
        input.parse::<Ident>()?;
        return Ok(Request::MultipartForm);
    }
    parse_typed(input).map(|(ty, media)| Request::Typed(Box::new(ty), media))
}

/// `T` or `(T, "mime")`.
fn parse_typed(input: ParseStream) -> syn::Result<(Type, Option<LitStr>)> {
    if input.peek(syn::token::Paren) {
        let inner;
        syn::parenthesized!(inner in input);
        let ty: Type = inner.parse()?;
        inner.parse::<Token![,]>()?;
        let media: LitStr = inner.parse()?;
        Ok((ty, Some(media)))
    } else {
        Ok((input.parse()?, None))
    }
}

/// `(status)`, `(status, "description")`, `(status, body)` or
/// `(status, body, "description")`, where the body is `T`, `(T, "mime")` or
/// `bytes("mime")`.
fn parse_response(input: ParseStream) -> syn::Result<ResponseItem> {
    let inner;
    syn::parenthesized!(inner in input);
    let status_lit: LitInt = inner.parse()?;
    let status: u16 = status_lit.base10_parse()?;
    if !(100..=599).contains(&status) {
        return Err(syn::Error::new(
            status_lit.span(),
            "a response status is 100 to 599",
        ));
    }
    let mut item = ResponseItem {
        status,
        body: None,
        description: None,
    };
    if inner.parse::<Option<Token![,]>>()?.is_none() || inner.is_empty() {
        return Ok(item);
    }
    if !inner.peek(LitStr) {
        let is_bytes = inner.peek(Ident)
            && inner.peek2(syn::token::Paren)
            && inner.fork().parse::<Ident>()? == "bytes";
        item.body = Some(if is_bytes {
            inner.parse::<Ident>()?;
            let media;
            syn::parenthesized!(media in inner);
            Body::Bytes(media.parse()?)
        } else {
            let (ty, media) = parse_typed(&inner)?;
            Body::Typed(Box::new(ty), media)
        });
        if inner.parse::<Option<Token![,]>>()?.is_none() || inner.is_empty() {
            return Ok(item);
        }
    }
    item.description = Some(inner.parse()?);
    if inner.peek(Token![,]) {
        inner.parse::<Token![,]>()?;
    }
    if !inner.is_empty() {
        return Err(inner.error("expected (status, body, \"description\")"));
    }
    Ok(item)
}

/// The service-level `openapi: { … }` block.
#[derive(Default)]
pub(super) struct OpenApiBlock {
    title: Option<LitStr>,
    version: Option<LitStr>,
    description: Option<LitStr>,
    servers: Option<Vec<(LitStr, Option<LitStr>)>>,
    tags: Option<Vec<(LitStr, Option<LitStr>)>>,
    security_scheme: Option<LitStr>,
    schemas: Option<Vec<Type>>,
}

/// `[("value", "description"), …]` with the description optional.
fn described_list(input: ParseStream) -> syn::Result<Vec<(LitStr, Option<LitStr>)>> {
    let content;
    syn::bracketed!(content in input);
    Ok(content
        .parse_terminated(
            |item| {
                let pair;
                syn::parenthesized!(pair in item);
                let value: LitStr = pair.parse()?;
                let description = match pair.parse::<Option<Token![,]>>()? {
                    Some(_) if !pair.is_empty() => Some(pair.parse()?),
                    _ => None,
                };
                Ok((value, description))
            },
            Token![,],
        )?
        .into_iter()
        .collect())
}

pub(super) fn parse_openapi(input: ParseStream) -> syn::Result<OpenApiBlock> {
    let content;
    syn::braced!(content in input);
    let mut block = OpenApiBlock::default();
    while !content.is_empty() {
        let key: Ident = content.parse()?;
        content.parse::<Token![:]>()?;
        match key.to_string().as_str() {
            "title" => set(&mut block.title, content.parse()?, &key)?,
            "version" => set(&mut block.version, content.parse()?, &key)?,
            "description" => set(&mut block.description, content.parse()?, &key)?,
            "security_scheme" => set(&mut block.security_scheme, content.parse()?, &key)?,
            "servers" => set(&mut block.servers, described_list(&content)?, &key)?,
            "tags" => set(&mut block.tags, described_list(&content)?, &key)?,
            "schemas" => {
                let types;
                syn::bracketed!(types in content);
                let schemas = types
                    .parse_terminated(Type::parse, Token![,])?
                    .into_iter()
                    .collect();
                set(&mut block.schemas, schemas, &key)?
            }
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "unknown openapi field '{other}'; expected title, version, description, \
                         servers, tags, security_scheme or schemas"
                    ),
                ))
            }
        }
        if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
        }
    }
    Ok(block)
}

impl OpenApiBlock {
    pub(super) fn uses_types(&self) -> bool {
        self.schemas
            .as_ref()
            .is_some_and(|schemas| !schemas.is_empty())
    }

    /// Statements registering the extra component schemas.
    pub(super) fn schema_registrations(&self, schemas: &Ident) -> TokenStream {
        let types = self.schemas.as_deref().unwrap_or_default();
        quote! { #( #schemas.schema_for::<#types>(); )* }
    }

    pub(super) fn tokens(&self) -> TokenStream {
        let title = optional_string(self.title.as_ref());
        let version = optional_string(self.version.as_ref());
        let description = optional_string(self.description.as_ref());
        let security_scheme = optional_string(self.security_scheme.as_ref());
        let described = |items: &[(LitStr, Option<LitStr>)], ty: TokenStream, key: TokenStream| {
            let items = items.iter().map(|(value, description)| {
                let description = optional_string(description.as_ref());
                quote! { #ty { #key: #value.to_string(), description: #description } }
            });
            quote! { vec![#(#items),*] }
        };
        let servers = described(
            self.servers.as_deref().unwrap_or_default(),
            quote! { ::cufflink::manifest::ServerDoc },
            quote! { url },
        );
        let tags = described(
            self.tags.as_deref().unwrap_or_default(),
            quote! { ::cufflink::manifest::TagDoc },
            quote! { name },
        );
        quote! {
            ::cufflink::manifest::OpenApiOptions {
                title: #title,
                version: #version,
                description: #description,
                servers: #servers,
                tags: #tags,
                security_scheme: #security_scheme,
            }
        }
    }
}
