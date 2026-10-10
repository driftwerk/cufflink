//! `service! { … }`: parses a service definition and emits a `main` that
//! prints the manifest under `--emit-manifest`.

mod authorization;
mod docs;
mod emit;
mod routes;
mod schedules;
mod schema;
mod streams;
mod subscriptions;

use authorization::AuthorizationBlock;
use cufflink_migrate::MigrationFile;
use cufflink_types::manifest::validate::refused_extension;
use cufflink_types::route_path::RoutePattern;
use cufflink_types::{
    ConsumerNaming, ErrorProfile, MigrationHistory, RoutingMode, SchemaOwnership,
};
use proc_macro2::TokenStream;
use routes::{Posture, RouteEntry};
use schedules::ScheduleEntry;
use streams::StreamEntry;
use subscriptions::SubscriptionEntry;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitStr, Token};

/// Parsed contents of service! { name: "...", tables: [...], mode: wasm, custom_routes: [...], subscriptions: [...], authorization: [...], on_migrate: "..." }
struct ServiceDef {
    name: LitStr,
    tables: Vec<Ident>,
    private_tables: Vec<Ident>,
    mode: Option<Ident>,
    custom_routes: Vec<RouteEntry>,
    subscriptions: Vec<SubscriptionEntry>,
    authorization: Option<AuthorizationBlock>,
    on_migrate: Option<LitStr>,
    publishes: Vec<LitStr>,
    extensions: Vec<LitStr>,
    graphql: bool,
    routing: RoutingMode,
    errors: Option<LitStr>,
    openapi: Option<docs::OpenApiBlock>,
    consumers: ConsumerNaming,
    streams: Vec<StreamEntry>,
    schedules: Vec<ScheduleEntry>,
    schema: SchemaOwnership,
    schema_name: Option<LitStr>,
    migrations: Vec<MigrationFile>,
    migration_history: Option<MigrationHistory>,
}

impl Parse for ServiceDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name: Option<LitStr> = None;
        let mut tables: Vec<Ident> = Vec::new();
        let mut mode: Option<Ident> = None;
        let mut custom_routes: Vec<RouteEntry> = Vec::new();
        let mut subscriptions: Vec<SubscriptionEntry> = Vec::new();
        let mut authorization: Option<AuthorizationBlock> = None;
        let mut on_migrate: Option<LitStr> = None;
        let mut publishes: Vec<LitStr> = Vec::new();
        let mut extensions: Option<Vec<LitStr>> = None;
        let mut graphql = false;
        let mut routing = RoutingMode::Legacy;
        let mut errors: Option<LitStr> = None;
        let mut openapi: Option<docs::OpenApiBlock> = None;
        let mut consumers: Option<ConsumerNaming> = None;
        let mut streams: Option<Vec<StreamEntry>> = None;
        let mut schedules: Option<Vec<ScheduleEntry>> = None;
        let mut private_tables: Option<Vec<Ident>> = None;
        let mut schema: Option<SchemaOwnership> = None;
        let mut schema_name: Option<LitStr> = None;
        let mut migrations: Option<Vec<MigrationFile>> = None;
        let mut migration_history: Option<MigrationHistory> = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![:]>()?;

            match key.to_string().as_str() {
                "name" => {
                    name = Some(input.parse()?);
                }
                "tables" => {
                    tables = parse_tables(input)?;
                }
                "private_tables" => {
                    crate::keywords::once(&mut private_tables, &key, parse_tables(input)?)?
                }
                "mode" => {
                    mode = Some(parse_mode(input)?);
                }
                "custom_routes" => custom_routes.extend(routes::parse_routes(input)?),
                "subscriptions" => subscriptions.extend(subscriptions::parse_subscriptions(input)?),
                "authorization" => {
                    authorization = Some(authorization::parse_authorization(input)?);
                }
                "on_migrate" => {
                    on_migrate = Some(input.parse()?);
                }
                "publishes" => {
                    let content;
                    syn::bracketed!(content in input);
                    publishes = content
                        .parse_terminated(|s| s.parse::<LitStr>(), Token![,])?
                        .into_iter()
                        .collect();
                }
                "extensions" => {
                    crate::keywords::once(&mut extensions, &key, parse_extensions(input)?)?
                }
                "graphql" => {
                    graphql = input.parse::<syn::LitBool>()?.value;
                }
                "routing" => {
                    routing = parse_routing(input)?;
                }
                "errors" => {
                    errors = Some(parse_errors(input)?);
                }
                "openapi" => {
                    openapi = Some(docs::parse_openapi(input)?);
                }
                "consumers" => {
                    crate::keywords::once(&mut consumers, &key, schema::parse_consumers(input)?)?
                }
                "streams" => {
                    crate::keywords::once(&mut streams, &key, streams::parse_streams(input)?)?
                }
                "schedules" => {
                    crate::keywords::once(&mut schedules, &key, schedules::parse_schedules(input)?)?
                }
                "schema" => {
                    crate::keywords::once(&mut schema, &key, schema::parse_ownership(input)?)?
                }
                "schema_name" => crate::keywords::once(
                    &mut schema_name,
                    &key,
                    schema::parse_schema_name(input)?,
                )?,
                "migrations" => crate::keywords::once(
                    &mut migrations,
                    &key,
                    schema::parse_migrations(input, schema::crate_dir().as_deref())?,
                )?,
                "migration_history" => crate::keywords::once(
                    &mut migration_history,
                    &key,
                    schema::parse_migration_history(input)?,
                )?,
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "Unknown service field: '{}'; expected one of name, tables, private_tables, mode, \
                             custom_routes, subscriptions, authorization, on_migrate, publishes, extensions, graphql, routing, errors, openapi, \
                             consumers, streams, schedules, schema, schema_name, migrations, migration_history",
                            other
                        ),
                    ));
                }
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        let name = name.ok_or_else(|| input.error("service! requires a 'name' field"))?;
        if routing == RoutingMode::Strict {
            custom_routes.iter().try_for_each(validate_strict_path)?;
        }
        custom_routes
            .iter()
            .try_for_each(|route| validate_sse(route, routing))?;
        let streams = streams.unwrap_or_default();
        streams::validate_streams(&streams)?;
        let schedules = schedules.unwrap_or_default();
        schedules::validate_schedules(&schedules)?;

        Ok(ServiceDef {
            name,
            tables,
            private_tables: private_tables.unwrap_or_default(),
            mode,
            custom_routes,
            subscriptions,
            authorization,
            on_migrate,
            publishes,
            extensions: extensions.unwrap_or_default(),
            graphql,
            routing,
            errors,
            openapi,
            consumers: consumers.unwrap_or_default(),
            streams,
            schedules,
            schema: schema.unwrap_or_default(),
            schema_name,
            migrations: migrations.unwrap_or_default(),
            migration_history,
        })
    }
}

/// `[Loan, Book]`: the `#[derive(Table)]` structs of a table list.
fn parse_tables(input: ParseStream) -> syn::Result<Vec<Ident>> {
    let content;
    syn::bracketed!(content in input);
    Ok(content
        .parse_terminated(Ident::parse, Token![,])?
        .into_iter()
        .collect())
}

/// `errors: { key: "error_message" }`: the key the platform's error bodies
/// carry their message under.
fn parse_errors(input: ParseStream) -> syn::Result<LitStr> {
    let content;
    syn::braced!(content in input);
    let field: Ident = content.parse()?;
    if field != "key" {
        return Err(syn::Error::new(
            field.span(),
            format!("unknown errors field '{field}'; expected key"),
        ));
    }
    content.parse::<Token![:]>()?;
    let key: LitStr = content.parse()?;
    if content.peek(Token![,]) {
        content.parse::<Token![,]>()?;
    }
    if !content.is_empty() {
        return Err(content.error("errors takes only key"));
    }
    if !ErrorProfile::is_valid_key(&key.value()) {
        return Err(syn::Error::new(
            key.span(),
            "an error key must match [a-z_][a-z0-9_]{0,63}",
        ));
    }
    Ok(key)
}

/// `mode: <mode>`. A native service is a binary of its own, declared with
/// `cufflink_native::service`.
fn parse_mode(input: ParseStream) -> syn::Result<Ident> {
    let mode: Ident = input.parse()?;
    match mode.to_string().as_str() {
        "native" => Err(syn::Error::new(
            mode.span(),
            "a native service is not declared with service!; build it with \
             cufflink_native::service(\"...\") in its own binary",
        )),
        _ => Ok(mode),
    }
}

/// `extensions: ["google.token", …]`: the extensions the handlers call.
fn parse_extensions(input: ParseStream) -> syn::Result<Vec<LitStr>> {
    let names = crate::keywords::strings(input)?;
    let values: Vec<String> = names.iter().map(LitStr::value).collect();
    match refused_extension("extensions", &values) {
        Some((i, reason)) => Err(syn::Error::new(names[i].span(), reason)),
        None => Ok(names),
    }
}

/// `routing: legacy` or `routing: strict`.
fn parse_routing(input: ParseStream) -> syn::Result<RoutingMode> {
    let mode: Ident = input.parse()?;
    match mode.to_string().as_str() {
        "legacy" => Ok(RoutingMode::Legacy),
        "strict" => Ok(RoutingMode::Strict),
        other => Err(syn::Error::new(
            mode.span(),
            format!("unknown routing mode '{other}'; expected legacy or strict"),
        )),
    }
}

/// An event stream the platform would refuse: a subject it cannot bridge
/// for the route, or an anonymous posture that does not opt in.
fn validate_sse(route: &RouteEntry, routing: RoutingMode) -> syn::Result<()> {
    let Some(sse) = &route.sse else {
        return Ok(());
    };
    let definition = cufflink_types::SseRouteDef {
        subject: sse.subject.value(),
        reducer: None,
        anonymous: sse.anonymous,
    };
    let params: Option<Vec<String>> = match routing {
        RoutingMode::Strict => Some(
            RoutePattern::parse(&route.path.value())
                .map(|pattern| pattern.params().map(str::to_string).collect())
                .unwrap_or_default(),
        ),
        RoutingMode::Legacy => None,
    };
    let anonymous_posture = matches!(route.posture, None | Some(Posture::Public(_)));
    definition
        .subject_violation(params.as_deref())
        .or_else(|| {
            definition
                .anonymous_violation(anonymous_posture)
                .map(str::to_string)
        })
        .map_or(Ok(()), |violation| {
            Err(syn::Error::new(sse.subject.span(), violation))
        })
}

/// A strict route's path must parse as a route pattern: the platform
/// matches requests against it.
fn validate_strict_path(route: &RouteEntry) -> syn::Result<()> {
    RoutePattern::parse(&route.path.value())
        .map(drop)
        .map_err(|e| syn::Error::new(route.path.span(), format!("invalid strict route path: {e}")))
}

pub fn impl_service(input: TokenStream) -> syn::Result<TokenStream> {
    syn::parse2(input).map(|def| emit::service(&def))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn expand(input: TokenStream) -> syn::Result<String> {
        impl_service(input).map(|tokens| tokens.to_string())
    }

    #[test]
    fn forward_auth_is_emitted_only_where_declared() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            mode: wasm,
            custom_routes: [
                ("POST", "/proxy", "proxy", auth, forward_auth),
                ("GET", "/plain", "plain", auth),
            ],
        })
        .expect("expands");
        assert_eq!(out.matches("forward_auth : true").count(), 1, "{out}");
        assert_eq!(out.matches("forward_auth : false").count(), 1, "{out}");
    }

    #[test]
    fn graphql_is_off_unless_the_service_opts_in() {
        let off = expand(quote! { name: "svc", tables: [] }).expect("expands");
        assert!(off.contains("graphql : false"), "{off}");
        let on = expand(quote! { name: "svc", tables: [], graphql: true }).expect("expands");
        assert!(on.contains("graphql : true"), "{on}");
        let err = expand(quote! { name: "svc", tables: [], grapql: true }).unwrap_err();
        assert!(err.to_string().contains("graphql"), "{err}");
    }

    #[test]
    fn strict_routing_is_emitted_and_validates_every_path() {
        let legacy = expand(quote! { name: "svc", tables: [] }).expect("expands");
        assert!(legacy.contains("RoutingMode :: Legacy"), "{legacy}");
        let strict = expand(quote! {
            name: "svc",
            tables: [],
            routing: strict,
            custom_routes: [("GET", "/books/{book_id}", "get_book", auth)],
        })
        .expect("expands");
        assert!(strict.contains("RoutingMode :: Strict"), "{strict}");
        let err = expand(quote! {
            name: "svc",
            tables: [],
            routing: strict,
            custom_routes: [("GET", "/_fn/x", "x", auth)],
        })
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid strict route path: the first segment '_fn' is reserved by the platform"
        );
        let unvalidated = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("GET", "not a path", "x", auth)],
        });
        assert!(unvalidated.is_ok(), "legacy paths are informational");
        let err = expand(quote! { name: "svc", tables: [], routing: loose }).unwrap_err();
        assert!(err.to_string().contains("legacy or strict"), "{err}");
    }

    #[test]
    fn route_limits_are_emitted_and_checked() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [
                ("POST", "/import", "import", perm("t", "edit"), multipart, body_limit = 10_485_760, timeout = 60),
                ("GET", "/plain", "plain", auth),
            ],
        })
        .expect("expands");
        assert!(
            out.contains("body_limit_bytes : :: std :: option :: Option :: Some (10485760u64)"),
            "{out}"
        );
        assert!(
            out.contains("timeout_secs : :: std :: option :: Option :: Some (60u32)"),
            "{out}"
        );
        assert_eq!(out.matches("multipart : true").count(), 1, "{out}");
        for (routes, message) in [
            (
                quote! { ("POST", "/j", "j", auth, job, timeout = 5) },
                "timeout cannot be combined with job(...); set the job's own timeout",
            ),
            (
                quote! { ("GET", "/m", "m", auth, multipart) },
                "multipart is only allowed on POST, PUT and PATCH routes",
            ),
            (
                quote! { ("POST", "/t", "t", auth, timeout = 121) },
                "timeout must be between 1 and 120 seconds",
            ),
            (
                quote! { ("POST", "/t", "t", auth, timeout = 0) },
                "timeout must be between 1 and 120 seconds",
            ),
            (
                quote! { ("POST", "/b", "b", auth, body_limit = 67108865) },
                "body_limit must be between 1 and 67108864 bytes",
            ),
            (
                quote! { ("POST", "/b", "b", auth, body_limit = 1, body_limit = 2) },
                "duplicate body_limit modifier",
            ),
            (
                quote! { ("POST", "/b", "b", auth, multipart, multipart) },
                "duplicate multipart modifier",
            ),
        ] {
            let err =
                expand(quote! { name: "svc", tables: [], custom_routes: [#routes] }).unwrap_err();
            assert_eq!(err.to_string(), message);
        }
    }

    #[test]
    fn what_a_routes_audit_records_hold_not_is_emitted_and_checked() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [
                ("POST", "/pay", "pay", auth, audit_redact = ["card.number", "lines[].sku"]),
                ("GET", "/plain", "plain", auth),
            ],
        })
        .expect("expands");
        assert!(
            out.contains(
                "audit_redact : :: std :: vec ! [\"card.number\" . to_string () , \"lines[].sku\" . to_string ()]"
            ),
            "{out}"
        );
        assert_eq!(
            out.matches("audit_redact : :: std :: vec ! []").count(),
            1,
            "{out}"
        );
        for (routes, message) in [
            (
                quote! { ("POST", "/p", "p", auth, audit_redact = ["card..number"]) },
                "audit_redact[0]: '' is not an identifier",
            ),
            (
                quote! { ("POST", "/p", "p", auth, audit_redact = ["a"], audit_redact = ["b"]) },
                "duplicate audit_redact modifier",
            ),
            (
                quote! { ("POST", "/p", "p", audit_redact = ["a"], auth) },
                "the posture comes before",
            ),
        ] {
            let err =
                expand(quote! { name: "svc", tables: [], custom_routes: [#routes] }).unwrap_err();
            assert!(err.to_string().starts_with(message), "{err}");
        }
    }

    #[test]
    fn an_error_profile_is_emitted_and_its_key_checked() {
        let none = expand(quote! { name: "svc", tables: [] }).expect("expands");
        assert!(
            none.contains("errors : :: std :: option :: Option :: None"),
            "{none}"
        );
        let profiled = expand(quote! { name: "svc", tables: [], errors: { key: "error_message" } })
            .expect("expands");
        assert!(
            profiled.contains("key : \"error_message\" . to_string ()"),
            "{profiled}"
        );
        for (errors, message) in [
            (
                quote! { { key: "Error" } },
                "an error key must match [a-z_][a-z0-9_]{0,63}",
            ),
            (
                quote! { { name: "e" } },
                "unknown errors field 'name'; expected key",
            ),
            (quote! { { key: "e", extra: 1 } }, "errors takes only key"),
        ] {
            let err = expand(quote! { name: "svc", tables: [], errors: #errors }).unwrap_err();
            assert_eq!(err.to_string(), message);
        }
    }

    #[test]
    fn sse_routes_are_emitted_and_checked() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            routing: strict,
            custom_routes: [(
                "GET", "/receipts/book/{book_id}/progress", "receipt_progress", perm("receipts", "view"),
                sse(subject = "dw.{tenant}.receipts.pdf.progress.{book_id}", reducer = "receipt_progress_reduce")
            )],
        })
        .expect("expands");
        assert!(
            out.contains(
                "subject : \"dw.{tenant}.receipts.pdf.progress.{book_id}\" . to_string ()"
            ),
            "{out}"
        );
        assert!(out.contains("reducer : :: std :: option :: Option :: Some (\"receipt_progress_reduce\" . to_string ())"), "{out}");
        assert!(out.contains("anonymous : false"), "{out}");
        let open = expand(quote! {
            name: "svc",
            tables: [],
            routing: strict,
            custom_routes: [(
                "GET", "/status", "status", public("status page"),
                sse(subject = "dw.{tenant}.status", anonymous = true)
            )],
        })
        .expect("an anonymous stream that opts in");
        assert!(open.contains("anonymous : true"), "{open}");
        for (routing, route, message) in [
            (
                quote! { legacy },
                quote! { ("POST", "/p", "p", auth, sse(subject = "dw.{tenant}.x")) },
                "sse(...) is only allowed on GET routes",
            ),
            (
                quote! { legacy },
                quote! { ("GET", "/p", "p", auth, job, sse(subject = "dw.{tenant}.x")) },
                "sse(...) cannot be combined with job(...)",
            ),
            (
                quote! { legacy },
                quote! { ("GET", "/p", "p", auth, sse(subject = "dw.other.x")) },
                "an sse subject must start with dw.{tenant}.",
            ),
            (
                quote! { legacy },
                quote! { ("GET", "/p", "p", auth, sse(reducer = "r")) },
                "sse(...) requires subject = \"...\"",
            ),
            (
                quote! { legacy },
                quote! { ("GET", "/p", "p", auth, sse(subject = "dw.{tenant}.x", topic = "t")) },
                "unknown sse option 'topic'; expected subject, reducer or anonymous",
            ),
            (
                quote! { legacy },
                quote! { ("GET", "/p/:id", "p", auth, sse(subject = "dw.{tenant}.x.{id}")) },
                "sse subject placeholder '{id}' needs routing: strict, where it names a path parameter",
            ),
            (
                quote! { strict },
                quote! { ("GET", "/p/{id}", "p", auth, sse(subject = "dw.{tenant}.x.{other}")) },
                "sse subject placeholder '{other}' is not a path parameter of this strict route",
            ),
            (
                quote! { strict },
                quote! { ("GET", "/p", "p", auth, sse(subject = "dw.{tenant}.logs")) },
                "sse subject 'dw.{tenant}.logs' reads dw.{tenant}.logs, which the platform reserves",
            ),
            (
                quote! { strict },
                quote! { ("GET", "/p", "p", public("status page"), sse(subject = "dw.{tenant}.x")) },
                "sse on a public or undeclared posture streams to anonymous callers; \
                 declare anonymous = true to allow it",
            ),
        ] {
            let err = expand(
                quote! { name: "svc", tables: [], routing: #routing, custom_routes: [#route] },
            )
            .unwrap_err();
            assert_eq!(err.to_string(), message);
        }
    }

    #[test]
    fn route_docs_are_emitted_and_types_use_the_collector() {
        let untyped = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("GET", "/f", "f", auth, tags = ["files"], summary = "S", deprecated,
                responses = [(200, bytes("application/pdf"), "PDF"), (404)])],
        })
        .expect("expands");
        assert!(!untyped.contains("SchemaCollector"), "{untyped}");
        assert!(
            untyped.contains("description : \"Not Found\" . to_string ()"),
            "{untyped}"
        );
        assert!(
            untyped.contains("content_type : \"application/pdf\" . to_string ()"),
            "{untyped}"
        );
        let typed = expand(quote! {
            name: "svc",
            tables: [],
            openapi: { title: "Svc", servers: [("https://x/api", "Prod")], schemas: [PageParams] },
            custom_routes: [("PUT", "/p/{id}", "p", auth, path = IdPath, query = PageParams,
                request = (Upload, "application/octet-stream"),
                responses = [(200, Loan, "Updated"), (422, (Errors, "application/problem+json"))])],
        })
        .expect("expands");
        for needle in [
            "SchemaCollector :: new ()",
            "__schemas . inline_schema_for :: < IdPath > ()",
            "__schemas . schema_for :: < Upload > ()",
            "__schemas . schema_for :: < PageParams > () ;",
            "manifest . components = __schemas . into_components () ?",
        ] {
            assert!(typed.contains(needle), "{needle}: {typed}");
        }
        for (routes, message) in [
            (
                quote! { ("GET", "/a", "a", auth, response = (200), responses = [(404)]) },
                "a route declares its responses once: response = … or responses = [...]",
            ),
            (
                quote! { ("GET", "/a", "a", auth, tags = ["a"], tags = ["b"]) },
                "duplicate tags modifier",
            ),
            (
                quote! { ("GET", "/a", "a", auth, responses = [(600)]) },
                "a response status is 100 to 599",
            ),
        ] {
            let err =
                expand(quote! { name: "svc", tables: [], custom_routes: [#routes] }).unwrap_err();
            assert_eq!(err.to_string(), message);
        }
        let err = expand(quote! { name: "svc", tables: [], openapi: { host: "x" } }).unwrap_err();
        assert!(
            err.to_string().starts_with("unknown openapi field 'host'"),
            "{err}"
        );
        for (block, key) in [
            (quote! { servers: [("a")], servers: [("b")] }, "servers"),
            (quote! { tags: [("a")], tags: [] }, "tags"),
            (quote! { schemas: [A], schemas: [B] }, "schemas"),
        ] {
            let err = expand(quote! { name: "svc", tables: [], openapi: { #block } }).unwrap_err();
            assert_eq!(err.to_string(), format!("duplicate {key} modifier"));
        }
    }

    #[test]
    fn internal_routes_name_their_callers() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("POST", "/reprice", "reprice", internal("pricing-service", "*"))],
        })
        .expect("expands");
        assert!(
            out.contains("RoutePosture :: Internal { callers : vec ! [\"pricing-service\" . to_string () , \"*\" . to_string ()] , }"),
            "{out}"
        );
        let err = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("POST", "/reprice", "reprice", internal())],
        })
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "internal(...) names the services that may call it, or \"*\""
        );
    }

    #[test]
    fn api_key_routes_name_their_scope() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("POST", "/ingest", "ingest_book", api_key("books:ingest"))],
        })
        .expect("expands");
        assert!(
            out.contains("RoutePosture :: ApiKey { scope : \"books:ingest\" . to_string () , }"),
            "{out}"
        );
        let err = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("POST", "/ingest", "ingest_book", api_key(""))],
        })
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "api_key(\"...\") requires the scope a partner key must hold"
        );
    }

    #[test]
    fn scoped_routes_name_their_area_and_owner_column() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [
                ("GET", "/assets", "list_assets", scoped("assets", "view", owner = "customer_id")),
                ("GET", "/reports", "reports", scoped("assets", "view")),
            ],
        })
        .expect("expands");
        assert!(
            out.contains(
                "owner_field : :: std :: option :: Option :: Some (\"customer_id\" . to_string ())"
            ),
            "{out}"
        );
        assert!(
            out.contains("owner_field : :: std :: option :: Option :: None"),
            "{out}"
        );
        let err = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("GET", "/a", "a", scoped("assets", "view", column = "x"))],
        })
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "unknown scoped option 'column'; expected owner = \"field\""
        );
    }

    #[test]
    fn new_keywords_trail_the_posture() {
        for route in [
            quote! { ("POST", "/x", "x", body_limit = 10, perm("a", "b")) },
            quote! { ("POST", "/x", "x", tags = ["a"], auth) },
            quote! { ("GET", "/x", "x", sse(subject = "dw.{tenant}.x"), auth) },
        ] {
            let err =
                expand(quote! { name: "svc", tables: [], custom_routes: [#route] }).unwrap_err();
            assert!(
                err.to_string().starts_with("the posture comes before"),
                "{err}"
            );
        }
        assert!(expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("POST", "/x", "x", job, forward_auth, perm("a", "b"), timeout = 5)],
        })
        .is_err(), "job with timeout stays refused");
        assert!(expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("POST", "/x", "x", job, forward_auth, perm("a", "b"), body_limit = 5)],
        })
        .is_ok(), "the legacy modifiers may precede the posture");
    }

    #[test]
    fn duplicate_forward_auth_is_an_error() {
        let err = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("POST", "/p", "p", forward_auth, forward_auth)],
        })
        .unwrap_err();
        assert_eq!(err.to_string(), "duplicate forward_auth modifier");
    }

    #[test]
    fn unknown_modifier_error_lists_forward_auth() {
        let err = expand(quote! {
            name: "svc",
            tables: [],
            custom_routes: [("POST", "/p", "p", forwarded)],
        })
        .unwrap_err();
        assert!(err.to_string().contains("forward_auth"), "{err}");
    }

    #[test]
    fn a_subscription_without_keywords_is_emitted_as_before() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            subscriptions: [
                ("dw.{tenant}.a.created", "on_created", wasm),
                ("cufflink.tick.hourly", "sweep", wasm, "CUFFLINK_TICKS"),
                ("dw.{tenant}.a.deleted", "cascade", delete_cascade,),
                ("dw.{tenant}.a.updated", "on_updated", lambda),
            ],
        })
        .expect("expands");
        assert_eq!(out.matches("AckMode :: Always").count(), 4, "{out}");
        assert_eq!(
            out.matches("consumer : :: std :: option :: Option :: None")
                .count(),
            4,
            "{out}"
        );
        assert_eq!(
            out.matches("timeout_secs : :: std :: option :: Option :: None")
                .count(),
            4,
            "{out}"
        );
        assert_eq!(out.matches("HandlerType :: Wasm").count(), 3, "{out}");
        assert_eq!(
            out.matches("HandlerType :: DeleteCascade").count(),
            1,
            "{out}"
        );
        assert!(
            out.contains(
                "stream : :: std :: option :: Option :: Some (\"CUFFLINK_TICKS\" . to_string ())"
            ),
            "{out}"
        );
        assert!(out.contains("ConsumerNaming :: Legacy"), "{out}");
        assert!(out.contains("SchemaOwnership :: Managed"), "{out}");
        assert!(out.contains("streams : vec ! []"), "{out}");
        assert!(out.contains("schedules : vec ! []"), "{out}");
        assert!(
            out.contains("schema_name : :: std :: option :: Option :: None"),
            "{out}"
        );
    }

    #[test]
    fn subscription_keywords_follow_the_positional_items() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            subscriptions: [
                ("books.created", "on_book", wasm, "BOOKS", durable = "books-consumer",
                 ack = on_success, filters = ["books.deleted"]),
                ("dw.{tenant}.a.b", "h", wasm, timeout = 30),
            ],
        })
        .expect("expands");
        assert_eq!(out.matches("AckMode :: OnSuccess").count(), 1, "{out}");
        assert!(
            out.contains("durable_name : :: std :: option :: Option :: Some (\"books-consumer\" . to_string ())"),
            "{out}"
        );
        assert!(
            out.contains("filter_subjects : vec ! [\"books.deleted\" . to_string ()]"),
            "{out}"
        );
        assert!(
            out.contains("timeout_secs : :: std :: option :: Option :: Some (30u32)"),
            "{out}"
        );
        for (subscription, message) in [
            (
                quote! { ("a.b", "h", wasm, ack = on_success, "BOOKS") },
                "expected identifier",
            ),
            (
                quote! { ("a.b", "h", wasm, "BOOKS", "OTHER") },
                "expected identifier",
            ),
            (
                quote! { ("a.b", "h", wasm, timeout = 0) },
                "subscription.timeout_secs: 0 is outside 1..=300",
            ),
            (
                quote! { ("a.b", "h", wasm, filters = ["a..b"]) },
                "subscription.consumer.filter_subjects[0]: 'a..b' is not a subject pattern",
            ),
            (
                quote! { ("a.b", "h", wasm, backoff = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]) },
                "subscription.consumer.backoff_secs: 11 steps, more than the 10 allowed",
            ),
        ] {
            let err = expand(quote! { name: "svc", tables: [], subscriptions: [#subscription] })
                .unwrap_err();
            assert_eq!(err.to_string(), message);
        }
    }

    #[test]
    fn service_level_declarations_are_given_once() {
        for (fields, message) in [
            (
                quote! { schema: adopted, schema: managed },
                "duplicate schema keyword",
            ),
            (
                quote! { consumers: scoped, consumers: scoped },
                "duplicate consumers keyword",
            ),
            (
                quote! { schema_name: "a", schema_name: "b" },
                "duplicate schema_name keyword",
            ),
            (
                quote! { streams: [], streams: [] },
                "duplicate streams keyword",
            ),
            (
                quote! { schedules: [], schedules: [] },
                "duplicate schedules keyword",
            ),
            (
                quote! { extensions: ["a"], extensions: ["b"] },
                "duplicate extensions keyword",
            ),
        ] {
            let err = expand(quote! { name: "svc", tables: [], #fields }).unwrap_err();
            assert_eq!(err.to_string(), message);
        }
    }

    #[test]
    fn extensions_are_emitted_and_checked() {
        let none = expand(quote! { name: "svc", tables: [] }).expect("expands");
        assert!(none.contains("extensions : vec ! []"), "{none}");
        let declared = expand(quote! {
            name: "svc",
            tables: [],
            extensions: ["google.token", "pdf.render-v2",],
        })
        .expect("expands");
        assert!(
            declared.contains(
                "extensions : vec ! [\"google.token\" . to_string () , \"pdf.render-v2\" . to_string ()]"
            ),
            "{declared}"
        );
        for (extensions, message) in [
            (
                quote! { ["google.token", "Google.Token"] },
                "extensions[1]: 'Google.Token' is not an extension name (segments of a-z, 0-9 \
                 and '-' joined by '.', at most 128 bytes)",
            ),
            (
                quote! { ["google.*"] },
                "extensions[0]: 'google.*' is not an extension name (segments of a-z, 0-9 \
                 and '-' joined by '.', at most 128 bytes)",
            ),
            (
                quote! { ["a.b", "c", "a.b"] },
                "extensions: extension 'a.b' is listed twice",
            ),
            (quote! { "google.token" }, "expected square brackets"),
        ] {
            let err =
                expand(quote! { name: "svc", tables: [], extensions: #extensions }).unwrap_err();
            assert_eq!(err.to_string(), message);
        }
    }

    #[test]
    fn a_native_service_is_not_a_service_macro() {
        let err = expand(quote! { name: "svc", tables: [], mode: native }).unwrap_err();
        assert!(
            err.to_string().contains("cufflink_native::service"),
            "{err}"
        );
        for mode in [quote! { wasm }, quote! { container }, quote! { crud }] {
            assert!(expand(quote! { name: "svc", tables: [], mode: #mode }).is_ok());
        }
    }

    #[test]
    fn schedules_and_streams_are_emitted() {
        let out = expand(quote! {
            name: "svc",
            tables: [],
            schema: adopted,
            schema_name: "loan_service",
            consumers: scoped,
            streams: [("LOANS", ["loans.created"], interest, max_age = 60)],
            schedules: [
                ("sweep", every(30), "sweep", job),
                ("hourly", cron("0 * * * *"), "hourly", timezone = "Europe/London", catch_up = none),
            ],
        })
        .expect("expands");
        for needle in [
            "SchemaOwnership :: Adopted",
            "ConsumerNaming :: Scoped",
            "schema_name : :: std :: option :: Option :: Some (\"loan_service\" . to_string ())",
            "StreamRetention :: Interest",
            "max_age_secs : :: std :: option :: Option :: Some (60u64)",
            "ScheduleSpec :: Every { secs : 30u32 }",
            "ScheduleSpec :: Cron { expr : \"0 * * * *\" . to_string () }",
            "CatchUp :: None",
            "Overlap :: Skip",
            "timeout_secs : 600u32 , max_attempts : 3u32 , retry_on : vec ! [] ,",
        ] {
            assert!(out.contains(needle), "{needle}: {out}");
        }
        for (schedule, message) in [
            (
                quote! { ("s", hourly(1), "h") },
                "unknown schedule 'hourly'; expected every(secs) or cron(\"expr\")",
            ),
            (
                quote! { ("s", every(30, 60), "h") },
                "every(...) takes one argument",
            ),
            (
                quote! { ("s", every(30), "h", timeout = 30, job) },
                "schedule.timeout_secs: applies to an inline handler; a job's is job.timeout_secs",
            ),
            (
                quote! { ("s", every(30), "h", timezone = "Europe/London") },
                "schedule.timezone: applies to a cron schedule, not to every(..)",
            ),
            (
                quote! { ("S", every(30), "h") },
                "schedule.name: 'S' is not a schedule name (a-z, 0-9 and '_', at most 64 characters)",
            ),
        ] {
            let err =
                expand(quote! { name: "svc", tables: [], schedules: [#schedule] }).unwrap_err();
            assert_eq!(err.to_string(), message);
        }
    }
}
