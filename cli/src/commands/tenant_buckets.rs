//! `cufflink tenants buckets` — the buckets registered to a tenant, with the
//! key prefixes and canned ACLs its guests may use there and the credential
//! that reaches them (platform admin only). A secret is read from stdin
//! only, and never shown again.

use crate::commands::secret_from;
use crate::commands::tenants::admin_request;
use clap::Subcommand;
use cufflink_types::host::storage::CannedAcl;
use reqwest::Method;
use serde_json::{json, Value};

#[derive(Debug, Subcommand)]
pub enum BucketsAction {
    /// List the tenant's buckets
    List {
        /// Tenant slug
        slug: String,
    },
    /// Register a bucket to the tenant, replacing its rules and credential
    Set {
        /// Tenant slug
        slug: String,
        /// Bucket name
        bucket: String,
        /// A key prefix the tenant's guests may use (repeatable); any key
        /// when none is given
        #[arg(long = "key-prefix")]
        key_prefixes: Vec<String>,
        /// A canned ACL the tenant's guests may store objects with
        /// (repeatable): private or public-read
        #[arg(long = "acl", value_parser = parse_acl)]
        acls: Vec<CannedAcl>,
        /// The access key id that reaches the bucket; the platform's
        /// credentials when omitted
        #[arg(long, requires = "secret_access_key_stdin")]
        access_key_id: Option<String>,
        /// Read the access key's secret from stdin
        #[arg(long, requires = "access_key_id")]
        secret_access_key_stdin: bool,
    },
    /// Remove a bucket from the tenant
    Delete {
        /// Tenant slug
        slug: String,
        /// Bucket name
        bucket: String,
    },
}

fn parse_acl(raw: &str) -> Result<CannedAcl, String> {
    CannedAcl::parse(raw).ok_or_else(|| {
        let known: Vec<&str> = CannedAcl::ALL.iter().map(|acl| acl.as_str()).collect();
        format!("unknown canned ACL {raw:?}; one of {}", known.join(", "))
    })
}

fn buckets_path(slug: &str) -> String {
    format!("/api/tenants/{slug}/buckets")
}

fn bucket_path(slug: &str, bucket: &str) -> String {
    format!("{}/{bucket}", buckets_path(slug))
}

fn set_body(
    key_prefixes: &[String],
    acls: &[CannedAcl],
    credential: Option<(&str, &str)>,
) -> Value {
    let mut body = json!({
        "key_prefixes": (!key_prefixes.is_empty()).then_some(key_prefixes),
        "acls": acls,
    });
    if let Some((access_key_id, secret_access_key)) = credential {
        body["credential"] = json!({
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
        });
    }
    body
}

fn joined(value: &Value, none: &str) -> String {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|joined| !joined.is_empty())
        .unwrap_or_else(|| none.to_string())
}

/// One line per bucket: its name, key prefixes, ACLs, the access key id
/// that reaches it, and when and by whom it was last set.
fn bucket_rows(listing: &Value) -> Vec<[String; 6]> {
    listing["buckets"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|bucket| {
            let text = |field: &str| bucket[field].as_str().unwrap_or("-").to_string();
            [
                text("bucket"),
                joined(&bucket["key_prefixes"], "(any)"),
                joined(&bucket["acls"], "-"),
                bucket["credential"]["access_key_id"]
                    .as_str()
                    .unwrap_or("(platform)")
                    .to_string(),
                text("updated_at"),
                text("updated_by"),
            ]
        })
        .collect()
}

fn print_rows(slug: &str, rows: &[[String; 6]]) {
    match rows {
        [] => println!("Tenant '{slug}' has no buckets"),
        rows => {
            let mut table = crate::commands::listing(&[
                "BUCKET",
                "KEY PREFIXES",
                "ACLS",
                "ACCESS KEY",
                "UPDATED",
                "BY",
            ]);
            rows.iter().for_each(|row| {
                table.add_row(row.to_vec());
            });
            println!("{table}");
        }
    }
}

pub async fn run(
    action: BucketsAction,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    match action {
        BucketsAction::List { slug } => {
            let answer = admin_request(
                "List tenant buckets",
                Method::GET,
                &buckets_path(&slug),
                None,
                api_url,
                env,
            )
            .await?;
            print_rows(&slug, &bucket_rows(&answer));
        }
        BucketsAction::Set {
            slug,
            bucket,
            key_prefixes,
            acls,
            access_key_id,
            secret_access_key_stdin,
        } => {
            let secret = secret_access_key_stdin
                .then(|| secret_from(std::io::stdin(), "--secret-access-key-stdin"))
                .transpose()?;
            let credential = access_key_id.as_deref().zip(secret.as_deref());
            let answer = admin_request(
                "Set tenant bucket",
                Method::PUT,
                &bucket_path(&slug, &bucket),
                Some(set_body(&key_prefixes, &acls, credential)),
                api_url,
                env,
            )
            .await?;
            println!("Registered bucket '{bucket}' to tenant '{slug}':");
            print_rows(&slug, &bucket_rows(&json!({ "buckets": [answer] })));
        }
        BucketsAction::Delete { slug, bucket } => {
            admin_request(
                "Delete tenant bucket",
                Method::DELETE,
                &bucket_path(&slug, &bucket),
                None,
                api_url,
                env,
            )
            .await?;
            println!("Removed bucket '{bucket}' from tenant '{slug}'");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Buckets {
        #[command(subcommand)]
        action: BucketsAction,
    }

    fn parsed(args: &[&str]) -> Result<BucketsAction, clap::Error> {
        Buckets::try_parse_from(["buckets"].iter().chain(args)).map(|parsed| parsed.action)
    }

    #[test]
    fn set_takes_prefixes_acls_and_a_credential_whose_secret_is_on_stdin() {
        match parsed(&[
            "set",
            "harbor",
            "example-assets",
            "--key-prefix",
            "logos/",
            "--key-prefix",
            "banners/",
            "--acl",
            "public-read",
            "--access-key-id",
            "AKEXAMPLE",
            "--secret-access-key-stdin",
        ])
        .expect("parses")
        {
            BucketsAction::Set {
                slug,
                bucket,
                key_prefixes,
                acls,
                access_key_id,
                secret_access_key_stdin,
            } => {
                assert_eq!(
                    (slug.as_str(), bucket.as_str()),
                    ("harbor", "example-assets")
                );
                assert_eq!(key_prefixes, ["logos/", "banners/"]);
                assert_eq!(acls, [CannedAcl::PublicRead]);
                assert_eq!(access_key_id.as_deref(), Some("AKEXAMPLE"));
                assert!(secret_access_key_stdin);
            }
            other => panic!("not a set: {other:?}"),
        }
    }

    #[test]
    fn a_secret_is_never_an_argument_and_a_credential_needs_both_halves() {
        for refused in [
            &[
                "set",
                "harbor",
                "example-assets",
                "--secret-access-key",
                "s3cr3t",
            ][..],
            &[
                "set",
                "harbor",
                "example-assets",
                "--access-key-id",
                "AKEXAMPLE",
            ],
            &[
                "set",
                "harbor",
                "example-assets",
                "--secret-access-key-stdin",
            ],
            &[
                "set",
                "harbor",
                "example-assets",
                "--acl",
                "public-read-write",
            ],
            &["set", "harbor"],
        ] {
            assert!(parsed(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn the_body_leaves_out_what_is_not_given() {
        assert_eq!(
            set_body(&[], &[], None),
            json!({"key_prefixes": null, "acls": []})
        );
        assert_eq!(
            set_body(
                &["leads/".to_string()],
                &[CannedAcl::Private, CannedAcl::PublicRead],
                Some(("AKEXAMPLE", "s3cr3t"))
            ),
            json!({
                "key_prefixes": ["leads/"],
                "acls": ["private", "public-read"],
                "credential": {"access_key_id": "AKEXAMPLE", "secret_access_key": "s3cr3t"}
            })
        );
        assert_eq!(
            bucket_path("harbor", "example-documents"),
            "/api/tenants/harbor/buckets/example-documents"
        );
    }

    #[test]
    fn a_bucket_is_listed_with_its_rules_and_access_key_never_a_secret() {
        assert_eq!(
            bucket_rows(&json!({"buckets": [
                {"bucket": "example-assets", "key_prefixes": null, "acls": ["public-read"],
                 "credential": {"access_key_id": "AKEXAMPLE"},
                 "updated_at": "2026-11-05T10:00:00Z", "updated_by": "admin"},
                {"bucket": "example-documents", "key_prefixes": ["leads/", "archive/"], "acls": [],
                 "credential": null, "updated_at": "2026-11-04T10:00:00Z", "updated_by": null},
            ]})),
            [
                [
                    "example-assets",
                    "(any)",
                    "public-read",
                    "AKEXAMPLE",
                    "2026-11-05T10:00:00Z",
                    "admin"
                ]
                .map(String::from),
                [
                    "example-documents",
                    "leads/,archive/",
                    "-",
                    "(platform)",
                    "2026-11-04T10:00:00Z",
                    "-"
                ]
                .map(String::from),
            ]
        );
        assert!(bucket_rows(&json!({"buckets": []})).is_empty());
    }
}
