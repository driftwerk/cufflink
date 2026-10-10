//! `cufflink admin` — platform-wide administration.

use crate::commands::tenants::admin_request;
use clap::{Subcommand, ValueEnum};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Subcommand)]
pub enum AdminAction {
    /// Stored secrets
    Secrets {
        #[command(subcommand)]
        action: SecretsAction,
    },
}

#[derive(Subcommand)]
pub enum SecretsAction {
    /// Reseal stored secrets, or verify that the key ring opens them
    ///
    /// encrypt stores every secret as the platform's storage mode does,
    /// rotate_kek wraps every data key under the current key-encryption
    /// key, rotate_dek gives every tenant a new data key. These run in
    /// batches and may be run again if interrupted. verify changes
    /// nothing: it counts what the platform's key ring and storage mode
    /// do not read, and exits 1 if it counts anything.
    Reseal {
        /// What to do (--help describes each)
        #[arg(long, value_enum)]
        operation: Operation,
        /// One tenant's slug; every tenant when omitted
        #[arg(long)]
        tenant: Option<String>,
    },
}

/// What a reseal does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum Operation {
    /// Store every secret as the storage mode does: sealed under the
    /// active data key, with its plaintext under dual (written back where
    /// encrypted storage removed it) and without it under encrypted
    Encrypt,
    /// Wrap every data key under the current key-encryption key
    RotateKek,
    /// Give every tenant a new data key and seal its secrets under it
    RotateDek,
    /// Change nothing: unwrap every data key anew, open every sealed
    /// secret, and count what does not open, is not sealed or differs
    /// from its plaintext. Exits 1 if any is counted
    Verify,
}

impl Operation {
    fn as_str(self) -> &'static str {
        match self {
            Operation::Encrypt => "encrypt",
            Operation::RotateKek => "rotate_kek",
            Operation::RotateDek => "rotate_dek",
            Operation::Verify => "verify",
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq, Deserialize)]
struct Verified {
    data_keys_checked: u64,
    data_keys_unopenable: u64,
    data_keys_under_previous_kek: u64,
    unopenable: BTreeMap<String, u64>,
    not_sealed: BTreeMap<String, u64>,
    rows_under_retired_data_key: u64,
    plain_differs: BTreeMap<String, u64>,
}

/// What the platform answers a verify.
#[derive(Debug, Deserialize)]
struct Answer {
    rows_checked: u64,
    verified: Verified,
}

/// A verify that ran and found what the platform does not read. It is an
/// answer, not a fault of the CLI's, and exits 1 as any failure does.
#[derive(Debug)]
pub struct Unverified(String);

impl std::fmt::Display for Unverified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unverified {}

fn total(by_table: &BTreeMap<String, u64>) -> u64 {
    by_table.values().sum()
}

impl Verified {
    /// The three counts of rows that fail a verify, each with what the
    /// CLI calls it.
    fn failing(&self) -> [(&'static str, &BTreeMap<String, u64>); 3] {
        [
            ("unopenable", &self.unopenable),
            ("not sealed", &self.not_sealed),
            ("whose plaintext differs", &self.plain_differs),
        ]
    }

    /// The counts, each table that holds what is wrong, and what is left
    /// of a rotation. A count is always followed by its plural, so a line
    /// reads the same to a script whatever it counts.
    fn lines(&self, tenant: Option<&str>, rows_checked: u64) -> Vec<String> {
        let [unopenable, not_sealed, plain_differs] = self.failing().map(|(_, rows)| total(rows));
        let tables = self.failing().into_iter().flat_map(|(what, by_table)| {
            by_table
                .iter()
                .filter(|(_, rows)| **rows > 0)
                .map(move |(table, rows)| format!("    {table}: {rows} {what}"))
        });
        let unfinished = [
            (
                self.data_keys_under_previous_kek,
                "data keys are not under the current key-encryption key. Run `reseal --operation \
                 rotate_kek` before dropping a previous key.",
            ),
            (
                self.rows_under_retired_data_key,
                "rows are under a retired data key. Run `reseal --operation encrypt`.",
            ),
        ]
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .map(|(count, what)| format!("  Not finished: {count} {what}"));
        [
            format!(
                "Verified secrets ({}): {rows_checked} rows checked",
                tenant.unwrap_or("every tenant")
            ),
            format!(
                "  Data keys: {} checked, {} unopenable, {} not under the current key-encryption \
                 key",
                self.data_keys_checked,
                self.data_keys_unopenable,
                self.data_keys_under_previous_kek
            ),
            format!(
                "  Rows:      {unopenable} unopenable, {not_sealed} not sealed, {plain_differs} \
                 whose plaintext differs, {} under a retired data key",
                self.rows_under_retired_data_key
            ),
        ]
        .into_iter()
        .chain(tables)
        .chain(unfinished)
        .collect()
    }

    /// What the platform does not read, and what to do about each; `None`
    /// when it reads everything. A key not under the current
    /// key-encryption key and a row under a retired data key still open.
    fn failed(&self) -> Option<String> {
        let [unopenable, not_sealed, plain_differs] = self.failing().map(|(_, rows)| total(rows));
        let found: Vec<String> = [
            (
                self.data_keys_unopenable,
                "data keys do not unwrap: put the key-encryption key each names back in \
                 CUFFLINK_SECRETS_KEK_PREVIOUS (the platform logs \
                 secret_verify_data_key_unopenable with key_id and kek_id); the rows under them \
                 are counted unopenable too",
            ),
            (
                unopenable,
                "rows do not open (the platform logs secret_verify_row with the table and id of \
                 those whose own value is at fault)",
            ),
            (
                not_sealed,
                "rows are stored in plaintext alone, which `encrypted` storage does not read: \
                 return to `dual`, run `reseal --operation encrypt`, then `encrypted` again",
            ),
            (
                plain_differs,
                "rows hold a plaintext that differs from their sealed value: run `reseal \
                 --operation encrypt`",
            ),
        ]
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .map(|(count, what)| format!("{count} {what}"))
        .collect();
        (!found.is_empty()).then(|| format!("Not verified: {}", found.join("; ")))
    }
}

/// What a verify prints on stdout, and what it failed for.
fn verified(report: Value, tenant: Option<&str>) -> eyre::Result<(Vec<String>, Option<String>)> {
    let Answer {
        rows_checked,
        verified,
    } = serde_json::from_value(report).map_err(|e| {
        eyre::eyre!(
            "the platform's answer holds no verification: is the platform older than v0.30.0? \
             ({e})"
        )
    })?;
    Ok((verified.lines(tenant, rows_checked), verified.failed()))
}

fn reseal_body(operation: Operation, tenant: Option<&str>) -> Value {
    json!({ "operation": operation.as_str(), "tenant": tenant })
}

pub async fn run(
    action: AdminAction,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    match action {
        AdminAction::Secrets {
            action: SecretsAction::Reseal { operation, tenant },
        } => reseal(operation, tenant.as_deref(), api_url, env).await,
    }
}

async fn reseal(
    operation: Operation,
    tenant: Option<&str>,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let what = match operation {
        Operation::Verify => "Verify",
        Operation::Encrypt | Operation::RotateKek | Operation::RotateDek => "Reseal",
    };
    let report = admin_request(
        what,
        reqwest::Method::POST,
        "/api/admin/secrets/reseal",
        Some(reseal_body(operation, tenant)),
        api_url,
        env,
    )
    .await?;
    match operation {
        Operation::Verify => {
            let (lines, failed) = verified(report, tenant)?;
            lines.iter().for_each(|line| println!("{line}"));
            failed.map_or(Ok(()), |failed| Err(Unverified(failed).into()))
        }
        Operation::Encrypt | Operation::RotateKek | Operation::RotateDek => {
            println!(
                "Resealed secrets ({}, {}): {} rows checked, {} rewritten, {} data keys rewrapped, {} tenants rotated",
                operation.as_str(),
                tenant.unwrap_or("every tenant"),
                report["rows_checked"],
                report["rows_rewritten"],
                report["keys_rewrapped"],
                report["tenants_rotated"],
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Admin {
        #[command(subcommand)]
        action: AdminAction,
    }

    fn parsed(args: &[&str]) -> Result<(Operation, Option<String>), clap::Error> {
        Admin::try_parse_from(["admin"].iter().chain(args)).map(|admin| match admin.action {
            AdminAction::Secrets {
                action: SecretsAction::Reseal { operation, tenant },
            } => (operation, tenant),
        })
    }

    #[test]
    fn an_operation_is_one_of_four_and_goes_to_the_platform_by_its_name() {
        for (given, operation) in [
            ("encrypt", Operation::Encrypt),
            ("rotate_kek", Operation::RotateKek),
            ("rotate_dek", Operation::RotateDek),
            ("verify", Operation::Verify),
        ] {
            let (parsed, tenant) =
                parsed(&["secrets", "reseal", "--operation", given]).expect(given);
            assert_eq!((parsed, tenant), (operation, None));
            assert_eq!(
                reseal_body(parsed, None),
                json!({ "operation": given, "tenant": null })
            );
        }
        assert_eq!(
            parsed(&[
                "secrets",
                "reseal",
                "--operation",
                "encrypt",
                "--tenant",
                "acme"
            ])
            .expect("a tenant"),
            (Operation::Encrypt, Some("acme".into()))
        );
    }

    #[test]
    fn another_operation_is_refused_before_anything_is_asked() {
        let refused = parsed(&["secrets", "reseal", "--operation", "rotate"])
            .expect_err("an unknown operation");
        assert_eq!(refused.kind(), clap::error::ErrorKind::InvalidValue);
        assert!(refused.to_string().contains("rotate_kek"), "{refused}");
        assert!(
            parsed(&["secrets", "reseal"]).is_err(),
            "the operation is required"
        );
    }

    fn report(verified: Value) -> Value {
        json!({"operation": "verify", "rows_checked": 12, "rows_rewritten": 0,
               "keys_rewrapped": 0, "tenants_rotated": 0, "verified": verified})
    }

    fn counts(changed: &[(&str, Value)]) -> Value {
        let mut verified = json!({
            "data_keys_checked": 4, "data_keys_unopenable": 0,
            "data_keys_under_previous_kek": 0, "unopenable": {}, "not_sealed": {},
            "rows_under_retired_data_key": 0, "plain_differs": {},
        });
        for (field, value) in changed {
            verified[*field] = value.clone();
        }
        report(verified)
    }

    #[test]
    fn a_verify_that_opened_everything_prints_its_counts_and_succeeds() {
        assert_eq!(
            verified(counts(&[]), None).expect("a verification"),
            (
                vec![
                    "Verified secrets (every tenant): 12 rows checked".to_string(),
                    "  Data keys: 4 checked, 0 unopenable, 0 not under the current \
                     key-encryption key"
                        .to_string(),
                    "  Rows:      0 unopenable, 0 not sealed, 0 whose plaintext differs, 0 under \
                     a retired data key"
                        .to_string(),
                ],
                None
            )
        );
        assert_eq!(
            verified(counts(&[]), Some("acme"))
                .expect("a verification")
                .0[0],
            "Verified secrets (acme): 12 rows checked"
        );
    }

    #[test]
    fn a_rotation_that_is_not_finished_is_said_and_is_no_failure() {
        let unfinished = counts(&[
            ("data_keys_under_previous_kek", json!(1)),
            ("rows_under_retired_data_key", json!(3)),
        ]);
        let (lines, failed) = verified(unfinished, None).expect("a verification");
        assert_eq!(failed, None);
        assert_eq!(
            lines[1..],
            [
                "  Data keys: 4 checked, 0 unopenable, 1 not under the current key-encryption key",
                "  Rows:      0 unopenable, 0 not sealed, 0 whose plaintext differs, 3 under a \
                 retired data key",
                "  Not finished: 1 data keys are not under the current key-encryption key. Run \
                 `reseal --operation rotate_kek` before dropping a previous key.",
                "  Not finished: 3 rows are under a retired data key. Run `reseal --operation \
                 encrypt`.",
            ]
        );
    }

    #[test]
    fn a_verify_fails_for_what_is_not_read_and_says_what_to_do_about_each() {
        let (lines, failed) = verified(
            counts(&[
                ("data_keys_unopenable", json!(1)),
                (
                    "unopenable",
                    json!({"webhooks": 2, "service_configs": 38, "tenants": 0}),
                ),
                ("not_sealed", json!({"tenants": 1})),
                ("plain_differs", json!({"webhooks": 1})),
            ]),
            None,
        )
        .expect("a verification");
        assert_eq!(
            lines[1..],
            [
                "  Data keys: 4 checked, 1 unopenable, 0 not under the current key-encryption key",
                "  Rows:      40 unopenable, 1 not sealed, 1 whose plaintext differs, 0 under a \
                 retired data key",
                "    service_configs: 38 unopenable",
                "    webhooks: 2 unopenable",
                "    tenants: 1 not sealed",
                "    webhooks: 1 whose plaintext differs",
            ],
            "each table that holds something wrong, by what is wrong with it"
        );
        assert_eq!(
            failed.as_deref(),
            Some(
                "Not verified: 1 data keys do not unwrap: put the key-encryption key each names \
                 back in CUFFLINK_SECRETS_KEK_PREVIOUS (the platform logs \
                 secret_verify_data_key_unopenable with key_id and kek_id); the rows under them \
                 are counted unopenable too; 40 rows do not open (the platform logs \
                 secret_verify_row with the table and id of those whose own value is at fault); \
                 1 rows are stored in plaintext alone, which `encrypted` storage does not read: \
                 return to `dual`, run `reseal --operation encrypt`, then `encrypted` again; 1 \
                 rows hold a plaintext that differs from their sealed value: run `reseal \
                 --operation encrypt`"
            )
        );
        for (field, value) in [
            ("data_keys_unopenable", json!(1)),
            ("unopenable", json!({"webhooks": 1})),
            ("not_sealed", json!({"tenants": 1})),
            ("plain_differs", json!({"webhooks": 1})),
        ] {
            let (_, failed) = verified(counts(&[(field, value)]), None).expect("a verification");
            assert!(failed.is_some(), "{field} alone fails a verify");
        }
    }

    #[test]
    fn an_answer_that_is_no_verification_is_not_taken_for_a_clean_one() {
        for answer in [
            json!({"operation": "verify", "rows_checked": 0}),
            json!({"verified": {"data_keys_checked": 1}, "rows_checked": 0}),
            json!({"verified": counts(&[])["verified"]}),
        ] {
            assert!(
                verified(answer.clone(), None)
                    .expect_err("no verification")
                    .to_string()
                    .starts_with(
                        "the platform's answer holds no verification: is the platform older \
                         than v0.30.0? ("
                    ),
                "{answer}"
            );
        }
    }
}
