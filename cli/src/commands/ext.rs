//! `cufflink ext` — the extensions a tenant registers (platform admin
//! only). NATS providers are registered and removed here; a native
//! service's extensions come and go with its deploys.

use crate::commands::tenants::admin_request;
use clap::Subcommand;
use reqwest::Method;
use serde_json::{json, Value};

#[derive(Subcommand)]
pub enum ExtAction {
    /// List the extensions a tenant registers
    List {
        /// Tenant slug
        #[arg(long)]
        tenant: String,
    },
    /// Register a NATS provider of an extension, or give the one registered
    /// other callers
    Register {
        /// Extension name (e.g. google.token)
        name: String,
        /// Tenant slug
        #[arg(long)]
        tenant: String,
        /// A service that may call the extension, or '*' for every service
        /// of the tenant; repeat for several
        #[arg(long = "caller", required = true)]
        callers: Vec<String>,
        /// Narrow the callers although a deployed service calls the
        /// extension
        #[arg(long)]
        force: bool,
    },
    /// Remove a NATS provider of an extension
    Unregister {
        /// Extension name
        name: String,
        /// Tenant slug
        #[arg(long)]
        tenant: String,
        /// Remove it although a deployed service calls the extension
        #[arg(long)]
        force: bool,
    },
}

/// One request to the platform: what it is called in an error, and its
/// method, path and body.
#[derive(Debug, PartialEq)]
struct ExtRequest {
    what: &'static str,
    method: Method,
    path: String,
    body: Option<Value>,
}

fn extensions_path(tenant: &str) -> String {
    format!("/api/tenants/{tenant}/extensions")
}

fn extension_path(tenant: &str, name: &str, force: bool) -> String {
    let path = format!("{}/{name}", extensions_path(tenant));
    if force {
        format!("{path}?force=true")
    } else {
        path
    }
}

fn request(action: &ExtAction) -> ExtRequest {
    match action {
        ExtAction::List { tenant } => ExtRequest {
            what: "List extensions",
            method: Method::GET,
            path: extensions_path(tenant),
            body: None,
        },
        ExtAction::Register {
            name,
            tenant,
            callers,
            force,
        } => ExtRequest {
            what: "Register extension",
            method: Method::PUT,
            path: extension_path(tenant, name, *force),
            body: Some(json!({ "callers": callers })),
        },
        ExtAction::Unregister {
            name,
            tenant,
            force,
        } => ExtRequest {
            what: "Unregister extension",
            method: Method::DELETE,
            path: extension_path(tenant, name, *force),
            body: None,
        },
    }
}

/// A registration's callers, as one list.
fn callers(extension: &Value) -> String {
    extension["callers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

/// One line per registration: its name, backend, callers and the native
/// service that provides it.
fn registration_rows(listing: &Value) -> Vec<[String; 4]> {
    listing["extensions"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|extension| {
            let text = |field: &str| extension[field].as_str().unwrap_or("-").to_string();
            [
                text("name"),
                text("backend"),
                callers(extension),
                text("service_id"),
            ]
        })
        .collect()
}

pub async fn run(action: ExtAction, api_url: Option<&str>, env: Option<&str>) -> eyre::Result<()> {
    let ExtRequest {
        what,
        method,
        path,
        body,
    } = request(&action);
    let answer = admin_request(what, method, &path, body, api_url, env).await?;
    match action {
        ExtAction::List { tenant } => {
            let rows = registration_rows(&answer);
            if rows.is_empty() {
                println!("Tenant '{tenant}' registers no extensions");
            } else {
                let mut table =
                    crate::commands::listing(&["NAME", "BACKEND", "CALLERS", "SERVICE"]);
                rows.into_iter().for_each(|row| {
                    table.add_row(row);
                });
                println!("{table}");
            }
        }
        ExtAction::Register { name, tenant, .. } => println!(
            "Registered extension '{name}' for tenant '{tenant}' as a NATS provider (callers: {})",
            callers(&answer["extension"])
        ),
        ExtAction::Unregister { name, tenant, .. } => {
            println!("Removed extension '{name}' from tenant '{tenant}'");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Ext {
        #[command(subcommand)]
        action: ExtAction,
    }

    fn requested(args: &[&str]) -> ExtRequest {
        let parsed = Ext::try_parse_from(["ext"].iter().chain(args)).expect("parsed");
        request(&parsed.action)
    }

    #[test]
    fn listing_asks_for_every_registration_of_the_tenant() {
        assert_eq!(
            requested(&["list", "--tenant", "acme"]),
            ExtRequest {
                what: "List extensions",
                method: Method::GET,
                path: "/api/tenants/acme/extensions".into(),
                body: None,
            }
        );
    }

    #[test]
    fn registering_puts_the_callers_and_forces_only_when_asked() {
        assert_eq!(
            requested(&[
                "register",
                "google.token",
                "--tenant",
                "acme",
                "--caller",
                "reports",
                "--caller",
                "billing",
            ]),
            ExtRequest {
                what: "Register extension",
                method: Method::PUT,
                path: "/api/tenants/acme/extensions/google.token".into(),
                body: Some(json!({"callers": ["reports", "billing"]})),
            }
        );
        assert_eq!(
            requested(&[
                "register",
                "google.token",
                "--tenant",
                "acme",
                "--caller",
                "*",
                "--force",
            ])
            .path,
            "/api/tenants/acme/extensions/google.token?force=true"
        );
        assert!(
            Ext::try_parse_from(["ext", "register", "google.token", "--tenant", "acme"]).is_err(),
            "a caller is required"
        );
    }

    #[test]
    fn unregistering_deletes_the_extension_and_forces_only_when_asked() {
        assert_eq!(
            requested(&["unregister", "google.token", "--tenant", "acme"]),
            ExtRequest {
                what: "Unregister extension",
                method: Method::DELETE,
                path: "/api/tenants/acme/extensions/google.token".into(),
                body: None,
            }
        );
        assert_eq!(
            requested(&["unregister", "google.token", "--tenant", "acme", "--force"]).path,
            "/api/tenants/acme/extensions/google.token?force=true"
        );
    }

    #[test]
    fn a_registration_is_listed_by_name_backend_callers_and_service() {
        assert_eq!(
            registration_rows(&json!({"extensions": [
                {"name": "google.token", "backend": "nats", "callers": ["reports", "billing"], "service_id": null},
                {"name": "pdf.render", "backend": "native", "callers": ["*"], "service_id": "7d1c"},
            ]})),
            [
                [
                    "google.token".to_string(),
                    "nats".into(),
                    "reports, billing".into(),
                    "-".into()
                ],
                [
                    "pdf.render".to_string(),
                    "native".into(),
                    "*".into(),
                    "7d1c".into()
                ],
            ]
        );
    }
}
