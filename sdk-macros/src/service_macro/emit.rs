use super::authorization::AuthorizationBlock;
use super::routes::{Posture, RouteEntry, SseOpts};
use super::schema::{consumers_tokens, history_tokens, migration_tokens, ownership_tokens};
use super::ServiceDef;
use cufflink_types::{BackoffSpec, JobConfig, RetryOn};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::LitStr;

/// The service's `main`: prints the manifest under `--emit-manifest`.
pub(super) fn service(def: &ServiceDef) -> TokenStream {
    let service_name = &def.name;
    let table_defs = |tables: &[Ident]| -> Vec<TokenStream> {
        tables
            .iter()
            .map(|t| quote! { #t::__cufflink_table_def() })
            .collect()
    };
    let table_calls = table_defs(&def.tables);
    let private_table_calls = table_defs(&def.private_tables);

    let mode_token = match def.mode.as_ref().map(|m| m.to_string()).as_deref() {
        Some("wasm") => quote! { ::cufflink::manifest::ServiceMode::Wasm },
        Some("container") => quote! { ::cufflink::manifest::ServiceMode::Container },
        _ => quote! { ::cufflink::manifest::ServiceMode::Crud },
    };

    let schemas = Ident::new("__schemas", Span::call_site());
    let route_tokens: Vec<_> = def
        .custom_routes
        .iter()
        .map(|r| route(r, &schemas))
        .collect();
    let typed = def.custom_routes.iter().any(|r| r.doc.uses_types())
        || def.openapi.as_ref().is_some_and(|o| o.uses_types());
    let collector = typed.then(|| {
        quote! { let mut #schemas = ::cufflink::openapi::SchemaCollector::new(); }
    });
    let components = typed.then(|| {
        let registrations = def
            .openapi
            .as_ref()
            .map(|o| o.schema_registrations(&schemas));
        quote! {
            #registrations
            manifest.components = #schemas.into_components()?;
        }
    });
    let openapi = optional(def.openapi.as_ref().map(|o| o.tokens()));
    let subscription_tokens: Vec<_> = def.subscriptions.iter().map(|s| s.tokens()).collect();
    let stream_tokens: Vec<_> = def.streams.iter().map(|s| s.tokens()).collect();
    let schedule_tokens: Vec<_> = def.schedules.iter().map(|s| s.tokens()).collect();
    let consumers = consumers_tokens(def.consumers);
    let schema = ownership_tokens(def.schema);
    let schema_name = optional_string(def.schema_name.as_ref());
    let migration_tokens: Vec<_> = def.migrations.iter().map(migration_tokens).collect();
    let migration_history = optional(def.migration_history.as_ref().map(history_tokens));
    let authorization_token = authorization(def.authorization.as_ref());

    let on_migrate_token = optional(
        def.on_migrate
            .as_ref()
            .map(|name| quote! { #name.to_string() }),
    );

    let graphql = def.graphql;
    let errors = optional(def.errors.as_ref().map(|key| {
        quote! { ::cufflink::manifest::ErrorProfile { key: #key.to_string() } }
    }));
    let routing = match def.routing {
        cufflink_types::RoutingMode::Legacy => {
            quote! { ::cufflink::manifest::RoutingMode::Legacy }
        }
        cufflink_types::RoutingMode::Strict => {
            quote! { ::cufflink::manifest::RoutingMode::Strict }
        }
    };
    let publish_tokens: Vec<_> = def
        .publishes
        .iter()
        .map(|p| quote! { #p.to_string() })
        .collect();
    let extensions = &def.extensions;

    quote! {
        fn __cufflink_manifest(
        ) -> ::std::result::Result<::cufflink::manifest::ServiceManifest, ::std::string::String> {
            #collector
            let mut manifest = ::cufflink::manifest::ServiceManifest {
                name: #service_name.to_string(),
                version: ::std::option::Option::Some(env!("CARGO_PKG_VERSION").to_string()),
                tables: vec![#(#table_calls),*],
                private_tables: vec![#(#private_table_calls),*],
                cells: vec![],
                events: vec![],
                subscriptions: vec![#(#subscription_tokens),*],
                custom_routes: vec![#(#route_tokens),*],
                mode: #mode_token,
                authorization: #authorization_token,
                on_migrate: #on_migrate_token,
                publishes: ::std::option::Option::Some(vec![#(#publish_tokens),*]),
                extensions: vec![#(#extensions.to_string()),*],
                provides: ::std::vec::Vec::new(),
                required_platform_features: ::std::vec::Vec::new(),
                graphql: #graphql,
                routing: #routing,
                errors: #errors,
                openapi: #openapi,
                components: ::std::collections::BTreeMap::new(),
                consumers: #consumers,
                streams: vec![#(#stream_tokens),*],
                schedules: vec![#(#schedule_tokens),*],
                schema: #schema,
                schema_name: #schema_name,
                migrations: vec![#(#migration_tokens),*],
                migration_history: #migration_history,
            };
            #components
            manifest.required_platform_features = manifest
                .derived_features()
                .into_iter()
                .map(::std::string::ToString::to_string)
                .collect();
            ::cufflink::manifest::openapi::validate(&manifest).map_err(|e| e.to_string())?;
            ::std::result::Result::Ok(manifest)
        }

        fn main() {
            let args: ::std::vec::Vec<::std::string::String> = ::std::env::args().collect();
            if args.iter().any(|a| a == "--emit-manifest") {
                let emitted = __cufflink_manifest().and_then(|manifest| {
                    ::serde_json::to_string_pretty(&manifest).map_err(|e| e.to_string())
                });
                match emitted {
                    ::std::result::Result::Ok(json) => println!("{}", json),
                    ::std::result::Result::Err(e) => {
                        eprintln!("cufflink: cannot emit the service manifest: {}", e);
                        ::std::process::exit(1);
                    }
                }
                return;
            }
            println!("Cufflink service '{}' — use `cufflink deploy` to deploy.", #service_name);
        }
    }
}

fn route(r: &RouteEntry, schemas: &Ident) -> TokenStream {
    let method = &r.method;
    let path = &r.path;
    let handler = &r.handler;
    let job_token = job_config(r.job.as_ref());
    let posture_token = posture(r.posture.as_ref());
    let forward_auth = r.forward_auth;
    let body_limit = optional(r.body_limit.map(|bytes| quote! { #bytes }));
    let timeout = optional(r.timeout.map(|secs| quote! { #secs }));
    let max_concurrent = optional(r.max_concurrent.map(|max| quote! { #max }));
    let multipart = r.multipart;
    let sse = optional(r.sse.as_ref().map(sse));
    let audit_redact = r.audit_redact.iter().flatten();
    let doc = r.doc.tokens(schemas);
    quote! {
        ::cufflink::manifest::CustomRouteDefinition {
            method: #method.to_string(),
            path: #path.to_string(),
            handler: #handler.to_string(),
            job: #job_token,
            posture: #posture_token,
            forward_auth: #forward_auth,
            body_limit_bytes: #body_limit,
            timeout_secs: #timeout,
            max_concurrent: #max_concurrent,
            multipart: #multipart,
            sse: #sse,
            doc: #doc,
            audit_redact: ::std::vec![#(#audit_redact.to_string()),*],
        }
    }
}

fn sse(sse: &SseOpts) -> TokenStream {
    let subject = &sse.subject;
    let reducer = optional_string(sse.reducer.as_ref());
    let anonymous = sse.anonymous;
    quote! {
        ::cufflink::manifest::SseRouteDef {
            subject: #subject.to_string(),
            reducer: #reducer,
            anonymous: #anonymous,
        }
    }
}

pub(super) fn optional(value: Option<TokenStream>) -> TokenStream {
    match value {
        Some(value) => quote! { ::std::option::Option::Some(#value) },
        None => quote! { ::std::option::Option::None },
    }
}

/// An `Option<String>` expression of a literal.
pub(super) fn optional_string(value: Option<&LitStr>) -> TokenStream {
    optional(value.map(|value| quote! { #value.to_string() }))
}

/// An `Option<JobConfig>` expression.
pub(super) fn job_config(job: Option<&JobConfig>) -> TokenStream {
    optional(job.map(|job| {
        let JobConfig {
            timeout_secs,
            max_attempts,
            retry_on,
            backoff,
        } = job;
        let retry_on = retry_on.iter().map(|failure| match failure {
            RetryOn::Error => quote! { ::cufflink::manifest::RetryOn::Error },
            RetryOn::Timeout => quote! { ::cufflink::manifest::RetryOn::Timeout },
        });
        let backoff = optional(backoff.map(
            |BackoffSpec {
                 base_secs,
                 max_secs,
             }| {
                quote! {
                    ::cufflink::manifest::BackoffSpec {
                        base_secs: #base_secs,
                        max_secs: #max_secs,
                    }
                }
            },
        ));
        quote! {
            ::cufflink::manifest::JobConfig {
                timeout_secs: #timeout_secs,
                max_attempts: #max_attempts,
                retry_on: vec![#(#retry_on),*],
                backoff: #backoff,
            }
        }
    }))
}

/// The `Option<RoutePosture>` of a parsed posture modifier.
fn posture(posture: Option<&Posture>) -> TokenStream {
    match posture {
        None => quote! { ::std::option::Option::None },
        Some(Posture::Authenticated) => {
            quote! { ::std::option::Option::Some(::cufflink::manifest::RoutePosture::Authenticated) }
        }
        Some(Posture::Public(reason)) => quote! {
            ::std::option::Option::Some(::cufflink::manifest::RoutePosture::Public {
                reason: #reason.to_string(),
            })
        },
        Some(Posture::Permission(area, op)) => quote! {
            ::std::option::Option::Some(::cufflink::manifest::RoutePosture::Permission {
                area: #area.to_string(),
                operation: #op.to_string(),
            })
        },
        Some(Posture::Owner(field)) => quote! {
            ::std::option::Option::Some(::cufflink::manifest::RoutePosture::Owner {
                field: #field.to_string(),
            })
        },
        Some(Posture::Scoped {
            area,
            operation,
            owner,
        }) => {
            let owner = optional_string(owner.as_ref());
            quote! {
                ::std::option::Option::Some(::cufflink::manifest::RoutePosture::Scoped {
                    area: #area.to_string(),
                    operation: #operation.to_string(),
                    owner_field: #owner,
                })
            }
        }
        Some(Posture::ApiKey(scope)) => quote! {
            ::std::option::Option::Some(::cufflink::manifest::RoutePosture::ApiKey {
                scope: #scope.to_string(),
            })
        },
        Some(Posture::Internal(callers)) => quote! {
            ::std::option::Option::Some(::cufflink::manifest::RoutePosture::Internal {
                callers: vec![#(#callers.to_string()),*],
            })
        },
    }
}

fn authorization(auth: Option<&AuthorizationBlock>) -> TokenStream {
    let Some(auth) = auth else {
        return quote! { ::std::option::Option::None };
    };
    let area_tokens: Vec<_> = auth
        .areas
        .iter()
        .map(|a| {
            let name = &a.name;
            let ops: Vec<_> = a
                .operations
                .iter()
                .map(|o| {
                    quote! { #o.to_string() }
                })
                .collect();
            quote! {
                ::cufflink::manifest::PermissionAreaDef {
                    name: #name.to_string(),
                    operations: vec![#(#ops),*],
                }
            }
        })
        .collect();

    let role_tokens: Vec<_> = auth
        .default_roles
        .iter()
        .map(|r| {
            let name = &r.name;
            let desc = &r.description;
            let perms: Vec<_> = r
                .permissions
                .iter()
                .map(|p| {
                    quote! { #p.to_string() }
                })
                .collect();
            quote! {
                ::cufflink::manifest::DefaultRoleDef {
                    name: #name.to_string(),
                    description: ::std::option::Option::Some(#desc.to_string()),
                    permissions: vec![#(#perms),*],
                }
            }
        })
        .collect();

    quote! {
        ::std::option::Option::Some(::cufflink::manifest::AuthorizationConfig {
            areas: vec![#(#area_tokens),*],
            default_roles: vec![#(#role_tokens),*],
        })
    }
}
