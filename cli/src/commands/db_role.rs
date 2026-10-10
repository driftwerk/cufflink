//! `cufflink services db-role` — the database role a service runs as
//! (platform admin only).

use crate::commands::tenants::admin_request;
use clap::Subcommand;
use serde_json::{json, Value};
use std::io::Read;

#[derive(Subcommand)]
pub enum DbRoleAction {
    /// Bind a service to a database role, or change what its binding holds
    Set {
        /// Tenant slug
        tenant: String,
        /// Service name
        service: String,
        /// The role the service runs as; kept when omitted
        #[arg(long)]
        role: Option<String>,
        /// Read the role's password from stdin
        #[arg(long)]
        password_stdin: bool,
        /// A role the service switches to for read-only queries
        #[arg(long)]
        read_only_role: Option<String>,
    },
    /// Check the binding against the tenant database and record the report
    Verify {
        /// Tenant slug
        tenant: String,
        /// Service name
        service: String,
    },
    /// Give the role the platform made for a service a new password
    Rotate {
        /// Tenant slug
        tenant: String,
        /// Service name
        service: String,
    },
    /// Show the binding and its last verification
    Show {
        /// Tenant slug
        tenant: String,
        /// Service name
        service: String,
    },
}

fn path(tenant: &str, service: &str) -> String {
    format!("/api/tenants/{tenant}/services/{service}/db-role")
}

/// The password `input` holds, without the line ending a shell or a file
/// leaves after it; nothing at all is refused.
fn password_from(mut input: impl Read) -> eyre::Result<String> {
    let mut password = String::new();
    input.read_to_string(&mut password)?;
    let password = password.trim_end_matches(['\r', '\n']);
    if password.is_empty() {
        eyre::bail!("--password-stdin: nothing was read from stdin");
    }
    Ok(password.to_string())
}

fn set_body(role: Option<&str>, password: Option<&str>, read_only_role: Option<&str>) -> Value {
    json!({
        "role": role,
        "password": password,
        "read_only_role": read_only_role,
    })
}

/// A failure while the report says the binding is not verified, so the
/// command exits non-zero.
fn verified(report: &Value) -> eyre::Result<()> {
    match report["verified"].as_bool() {
        Some(true) => Ok(()),
        _ => eyre::bail!("the binding is not verified"),
    }
}

fn print_binding(body: &Value) {
    let binding = &body["binding"];
    if binding.is_null() {
        println!(
            "{}/{}: no database role bound",
            body["tenant"].as_str().unwrap_or_default(),
            body["service"].as_str().unwrap_or_default()
        );
        return;
    }
    println!(
        "{}/{}: role {} ({}, {})",
        body["tenant"].as_str().unwrap_or_default(),
        body["service"].as_str().unwrap_or_default(),
        binding["role"].as_str().unwrap_or_default(),
        if binding["managed"].as_bool().unwrap_or(false) {
            "managed by the platform"
        } else {
            "bound by an admin"
        },
        if binding["login"].as_bool().unwrap_or(false) {
            "password stored"
        } else {
            "no password stored"
        },
    );
    if let Some(read_only) = binding["read_only_role"].as_str() {
        println!("  read-only role: {read_only}");
    }
    match binding["verified_at"].as_str() {
        Some(at) => println!("  verified at {at}"),
        None => println!("  not verified"),
    }
}

fn print_report(report: &Value) {
    for check in report["checks"].as_array().into_iter().flatten() {
        let name = check["check"].as_str().unwrap_or_default();
        match check["ok"].as_bool().unwrap_or(false) {
            true => println!("  ok      {name}"),
            false => println!(
                "  FAILED  {name}: {}",
                check["detail"].as_str().unwrap_or_default()
            ),
        }
    }
    let remediation: Vec<&str> = report["remediation"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    if !remediation.is_empty() {
        println!("To mend, run on the tenant database:");
        remediation
            .iter()
            .for_each(|statement| println!("  {statement}"));
    }
}

/// Ask the platform for `step` of the binding and print the verification
/// it answers with, which fails the command while the binding is not
/// verified.
async fn check(
    what: &str,
    step: &str,
    tenant: &str,
    service: &str,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let body = admin_request(
        what,
        reqwest::Method::POST,
        &format!("{}/{step}", path(tenant, service)),
        None,
        api_url,
        env,
    )
    .await?;
    print_binding(&body);
    print_report(&body["report"]);
    verified(&body["report"])
}

pub async fn run(
    action: DbRoleAction,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    match action {
        DbRoleAction::Set {
            tenant,
            service,
            role,
            password_stdin,
            read_only_role,
        } => {
            let password = password_stdin
                .then(|| password_from(std::io::stdin()))
                .transpose()?;
            let body = admin_request(
                "Database role",
                reqwest::Method::PUT,
                &path(&tenant, &service),
                Some(set_body(
                    role.as_deref(),
                    password.as_deref(),
                    read_only_role.as_deref(),
                )),
                api_url,
                env,
            )
            .await?;
            print_binding(&body);
            println!("Run `cufflink services db-role verify {tenant} {service}` to check it.");
        }
        DbRoleAction::Verify { tenant, service } => {
            let checked = check(
                "Database role verification",
                "verify",
                &tenant,
                &service,
                api_url,
                env,
            );
            checked.await?;
        }
        DbRoleAction::Rotate { tenant, service } => {
            let checked = check(
                "Database role rotation",
                "rotate",
                &tenant,
                &service,
                api_url,
                env,
            );
            checked.await?;
            println!("The role has a new password; sessions open before it keep theirs.");
        }
        DbRoleAction::Show { tenant, service } => {
            let body = admin_request(
                "Database role",
                reqwest::Method::GET,
                &path(&tenant, &service),
                None,
                api_url,
                env,
            )
            .await?;
            print_binding(&body);
            if !body["binding"]["verify_report"].is_null() {
                print_report(&body["binding"]["verify_report"]);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_on_stdin_loses_its_line_ending_and_nothing_else() {
        for (input, password) in [
            ("s3cret\n", "s3cret"),
            ("s3cret\r\n", "s3cret"),
            ("s3cret", "s3cret"),
            (" spaced out \n", " spaced out "),
            ("two\nlines\n", "two\nlines"),
        ] {
            assert_eq!(password_from(input.as_bytes()).expect(input), password);
        }
    }

    #[test]
    fn an_empty_password_is_refused() {
        for input in ["", "\n", "\r\n\n"] {
            let refused = password_from(input.as_bytes()).expect_err(input);
            assert!(
                refused.to_string().contains("nothing was read"),
                "{refused}"
            );
        }
    }

    #[test]
    fn what_is_not_given_is_sent_as_null_and_kept() {
        assert_eq!(
            set_body(None, Some("pw"), None),
            json!({"role": null, "password": "pw", "read_only_role": null})
        );
        assert_eq!(
            set_body(Some("loan_service_user"), None, Some("reports_ro")),
            json!({"role": "loan_service_user", "password": null, "read_only_role": "reports_ro"})
        );
    }

    #[test]
    fn a_binding_not_verified_fails_the_command() {
        assert!(verified(&json!({"verified": true})).is_ok());
        for report in [json!({"verified": false}), json!({}), Value::Null] {
            assert!(verified(&report).is_err(), "{report}");
        }
    }

    #[test]
    fn the_routes_are_the_tenants_and_the_services() {
        assert_eq!(
            path("acme", "loan-service"),
            "/api/tenants/acme/services/loan-service/db-role"
        );
    }
}
