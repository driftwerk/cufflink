//! `cufflink tenants runner-keys` — the keys a tenant's runners (its
//! extension runner and a tenant-scoped web runner) authenticate to the
//! platform with (platform admin only). A key is minted for one kind of
//! runner and shown once, when it is created; rotating one is creating its
//! successor, pointing the runner at it and revoking the old one.

use crate::commands::tenants::admin_request;
use clap::{Subcommand, ValueEnum};
use reqwest::Method;
use serde_json::{json, Value};

/// The kind of runner a key is minted for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Runner {
    /// The tenant's extension runner, which runs its native services
    Native,
    /// The tenant's tenant-scoped web runner, which serves its web apps
    Web,
}

impl Runner {
    /// What the platform calls it.
    fn runs(self) -> &'static str {
        match self {
            Runner::Native => "native",
            Runner::Web => "web",
        }
    }

    /// The name a key of it is given unless one is.
    fn default_name(self) -> &'static str {
        match self {
            Runner::Native => "ext-runner",
            Runner::Web => "web-runner",
        }
    }

    /// How the runner is pointed at its key, and at its tenant.
    fn setup(self, slug: &str) -> String {
        match self {
            Runner::Native => format!(
                "Run the tenant's ext-runner with EXT_TENANT={slug}, PLATFORM_URL and \
                 CUFFLINK_RUNNER_KEY set to this key."
            ),
            Runner::Web => format!(
                "Run the tenant's web-runner with WEB_TENANT={slug}, PLATFORM_URL and \
                 CUFFLINK_RUNNER_KEY set to this key."
            ),
        }
    }
}

#[derive(Subcommand)]
pub enum RunnerKeysAction {
    /// Create a runner key for one of the tenant's runners; it is shown once
    Create {
        /// Tenant slug
        slug: String,
        /// The runner it is for: `native` (the extension runner) or `web`
        /// (a tenant-scoped web runner)
        #[arg(long = "for", value_enum)]
        runner: Runner,
        /// Key name (for identification); `ext-runner` or `web-runner` unless
        /// given
        #[arg(long)]
        name: Option<String>,
    },
    /// List the tenant's runner keys
    List {
        /// Tenant slug
        slug: String,
    },
    /// Revoke a runner key: the runner API refuses it from its next request
    Revoke {
        /// Tenant slug
        slug: String,
        /// Runner key ID
        id: String,
    },
}

/// One request to the platform: what it is called in an error, and its
/// method, path and body.
#[derive(Debug, PartialEq)]
struct KeyRequest {
    what: &'static str,
    method: Method,
    path: String,
    body: Option<Value>,
}

fn keys_path(slug: &str) -> String {
    format!("/api/tenants/{slug}/runner-keys")
}

fn request(action: &RunnerKeysAction) -> KeyRequest {
    match action {
        RunnerKeysAction::Create { slug, runner, name } => KeyRequest {
            what: "Create runner key",
            method: Method::POST,
            path: keys_path(slug),
            body: Some(json!({
                "name": name.as_deref().unwrap_or(runner.default_name()),
                "runs": runner.runs(),
            })),
        },
        RunnerKeysAction::List { slug } => KeyRequest {
            what: "List runner keys",
            method: Method::GET,
            path: keys_path(slug),
            body: None,
        },
        RunnerKeysAction::Revoke { slug, id } => KeyRequest {
            what: "Revoke runner key",
            method: Method::DELETE,
            path: format!("{}/{id}", keys_path(slug)),
            body: None,
        },
    }
}

/// One line per key: its id, name, the runner it is for, its prefix, and
/// when it was created, last used and revoked.
fn key_rows(listing: &Value) -> Vec<[String; 7]> {
    listing["keys"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|key| {
            let text = |field: &str| key[field].as_str().unwrap_or("-").to_string();
            [
                text("id"),
                text("name"),
                text("runs"),
                text("prefix"),
                text("created_at"),
                text("last_used_at"),
                text("revoked_at"),
            ]
        })
        .collect()
}

pub async fn run(
    action: RunnerKeysAction,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let KeyRequest {
        what,
        method,
        path,
        body,
    } = request(&action);
    let answer = admin_request(what, method, &path, body, api_url, env).await?;
    match action {
        RunnerKeysAction::Create { slug, runner, .. } => {
            println!(
                "Runner key created for tenant '{slug}''s {} runner:",
                runner.runs()
            );
            println!(
                "  ID:   {}",
                answer["runner_key"]["id"].as_str().unwrap_or("?")
            );
            println!("  Key:  {}", answer["key"].as_str().unwrap_or("?"));
            println!();
            println!(
                "{} Save it now: it cannot be retrieved again.",
                runner.setup(&slug)
            );
        }
        RunnerKeysAction::List { slug } => match key_rows(&answer).as_slice() {
            [] => println!("Tenant '{slug}' has no runner keys"),
            rows => {
                let mut table = crate::commands::listing(&[
                    "ID",
                    "NAME",
                    "RUNNER",
                    "PREFIX",
                    "CREATED",
                    "LAST USED",
                    "REVOKED",
                ]);
                rows.iter().for_each(|row| {
                    table.add_row(row.to_vec());
                });
                println!("{table}");
            }
        },
        RunnerKeysAction::Revoke { slug, id } => {
            println!("Revoked runner key {id} of tenant '{slug}'");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Keys {
        #[command(subcommand)]
        action: RunnerKeysAction,
    }

    fn requested(args: &[&str]) -> KeyRequest {
        let parsed = Keys::try_parse_from(["runner-keys"].iter().chain(args)).expect("parsed");
        request(&parsed.action)
    }

    #[test]
    fn creating_posts_the_runner_it_is_for_and_the_name_or_its_default() {
        assert_eq!(
            requested(&["create", "acme", "--for", "web", "--name", "acme-web-2"]),
            KeyRequest {
                what: "Create runner key",
                method: Method::POST,
                path: "/api/tenants/acme/runner-keys".into(),
                body: Some(json!({"name": "acme-web-2", "runs": "web"})),
            }
        );
        assert_eq!(
            requested(&["create", "acme", "--for", "native"]).body,
            Some(json!({"name": "ext-runner", "runs": "native"}))
        );
        assert_eq!(
            requested(&["create", "acme", "--for", "web"]).body,
            Some(json!({"name": "web-runner", "runs": "web"}))
        );
        for refused in [
            &["runner-keys", "create"][..],
            &["runner-keys", "create", "acme"],
            &["runner-keys", "create", "acme", "--for", "wasm"],
        ] {
            assert!(Keys::try_parse_from(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn a_new_key_says_how_its_runner_takes_it() {
        assert!(Runner::Native
            .setup("acme")
            .starts_with("Run the tenant's ext-runner with EXT_TENANT=acme"));
        assert!(Runner::Web
            .setup("acme")
            .starts_with("Run the tenant's web-runner with WEB_TENANT=acme"));
    }

    #[test]
    fn listing_and_revoking_name_the_tenant_and_the_key() {
        assert_eq!(
            requested(&["list", "acme"]),
            KeyRequest {
                what: "List runner keys",
                method: Method::GET,
                path: "/api/tenants/acme/runner-keys".into(),
                body: None,
            }
        );
        assert_eq!(
            requested(&["revoke", "acme", "7d1c"]),
            KeyRequest {
                what: "Revoke runner key",
                method: Method::DELETE,
                path: "/api/tenants/acme/runner-keys/7d1c".into(),
                body: None,
            }
        );
    }

    #[test]
    fn a_key_is_listed_with_its_runner_its_prefix_and_its_dates() {
        assert_eq!(
            key_rows(&json!({"keys": [
                {"id": "7d1c", "name": "ext-runner", "runs": "native", "prefix": "AbC123dEf456",
                 "created_at": "2026-09-30T10:00:00Z", "last_used_at": null, "revoked_at": null},
                {"id": "8e2d", "name": "old", "runs": "web", "prefix": "XyZ987wVu654",
                 "created_at": "2026-09-01T10:00:00Z", "last_used_at": "2026-09-29T10:00:00Z",
                 "revoked_at": "2026-09-30T10:00:00Z"},
            ]})),
            [
                [
                    "7d1c",
                    "ext-runner",
                    "native",
                    "AbC123dEf456",
                    "2026-09-30T10:00:00Z",
                    "-",
                    "-"
                ]
                .map(String::from),
                [
                    "8e2d",
                    "old",
                    "web",
                    "XyZ987wVu654",
                    "2026-09-01T10:00:00Z",
                    "2026-09-29T10:00:00Z",
                    "2026-09-30T10:00:00Z"
                ]
                .map(String::from),
            ]
        );
        assert!(key_rows(&json!({"keys": []})).is_empty());
    }
}
