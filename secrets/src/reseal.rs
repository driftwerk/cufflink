use crate::columns::{self, key_bytes, Lock, SealedColumn};
use crate::tenant_keys::TenantKeys;
pub use crate::verify::Verified;
use crate::{envelope, verify, SecretBox, SecretError, StorageMode, Stored};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const BATCH: i64 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Encrypt,
    RotateKek,
    RotateDek,
    Verify,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::Encrypt => "encrypt",
            Operation::RotateKek => "rotate_kek",
            Operation::RotateDek => "rotate_dek",
            Operation::Verify => "verify",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Report {
    pub operation: Operation,
    pub rows_checked: u64,
    pub rows_rewritten: u64,
    pub keys_rewrapped: u64,
    pub tenants_rotated: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified: Option<Verified>,
}

impl Report {
    fn new(operation: Operation) -> Self {
        Self {
            operation,
            rows_checked: 0,
            rows_rewritten: 0,
            keys_rewrapped: 0,
            tenants_rotated: 0,
            verified: None,
        }
    }
}

fn needs_seal(stored: &Stored, stale: bool, active: Uuid) -> bool {
    match stored.sealed() {
        None => true,
        Some(sealed) => stale || envelope::key_id(sealed).ok() != Some(active),
    }
}

fn progress(report: &Report, table: &str) {
    tracing::info!(
        event = "secret_reseal_progress",
        operation = report.operation.as_str(),
        table,
        rows_checked = report.rows_checked,
        rows_rewritten = report.rows_rewritten,
        keys_rewrapped = report.keys_rewrapped,
        tenants_rotated = report.tenants_rotated,
        "Resealing secrets"
    );
}

async fn seal_column(
    secrets: &SecretBox,
    column: &SealedColumn,
    tenant: Option<Uuid>,
    report: &mut Report,
) -> Result<(), SecretError> {
    let mut after: Option<Uuid> = None;
    loop {
        let mut client = secrets.inner.pool.get().await?;
        let tx = client.transaction().await?;
        let rows = tx
            .query(
                &column.select_batch(Lock::ForRewrite),
                &[&after, &tenant, &BATCH],
            )
            .await?;
        let Some(last) = rows.last() else {
            return Ok(());
        };
        after = Some(last.get("id"));
        for row in &rows {
            let id: Uuid = row.get("id");
            let tenant_id: Uuid = row.get("tenant_id");
            let aad = column.aad(row.get::<_, Vec<String>>("row"));
            let stored = Stored::new(row.get("plain"), row.get("sealed"));
            report.rows_checked += 1;
            let in_row = |source| SecretError::Row {
                table: column.table,
                id,
                source: Box::new(source),
            };
            let read = secrets
                .read_whether_stale(&tx, tenant_id, &aad, &stored)
                .await
                .map_err(in_row)?;
            let Some(secret) = read.secret else {
                continue;
            };
            let active = secrets.active_key_within(&tx, tenant_id).await?;
            let sealed = match needs_seal(&stored, read.stale, active.id()) {
                true => envelope::seal(&active, &aad, secret.expose().as_bytes())?,
                false => stored.sealed().unwrap_or_default().to_vec(),
            };
            let desired = secrets.stored(secret.expose(), Some(sealed));
            if desired != stored {
                column.write(&tx, id, &desired).await?;
                report.rows_rewritten += 1;
            }
        }
        tx.commit().await?;
        progress(report, column.table);
    }
}

async fn encrypt(
    secrets: &SecretBox,
    tenant: Option<Uuid>,
    report: &mut Report,
) -> Result<(), SecretError> {
    for column in columns::ALL {
        seal_column(secrets, column, tenant, report).await?;
    }
    Ok(())
}

async fn rotate_kek(
    secrets: &SecretBox,
    tenant: Option<Uuid>,
    report: &mut Report,
) -> Result<(), SecretError> {
    loop {
        let mut client = secrets.inner.pool.get().await?;
        let tx = client.transaction().await?;
        let rewrapped = TenantKeys::rewrap(&tx, &secrets.inner.ring, tenant, BATCH).await?;
        tx.commit().await?;
        report.keys_rewrapped += rewrapped;
        progress(report, "tenant_data_keys");
        if rewrapped < BATCH as u64 {
            return Ok(());
        }
    }
}

async fn sealed_under_another_key(
    secrets: &SecretBox,
    tenant_id: Uuid,
    active: Uuid,
) -> Result<bool, SecretError> {
    let client = secrets.inner.pool.get().await?;
    let active = key_bytes(active);
    for column in columns::ALL {
        let row = client
            .query_one(&column.sealed_under_another(), &[&tenant_id, &active])
            .await?;
        if row.get::<_, bool>(0) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn rotate_dek(
    secrets: &SecretBox,
    tenant: Option<Uuid>,
    report: &mut Report,
) -> Result<(), SecretError> {
    let tenants = match tenant {
        Some(tenant) => vec![tenant],
        None => {
            let client = secrets.inner.pool.get().await?;
            client
                .query(
                    "SELECT DISTINCT tenant_id FROM tenant_data_keys ORDER BY tenant_id",
                    &[],
                )
                .await?
                .iter()
                .map(|row| row.get(0))
                .collect()
        }
    };
    for tenant_id in tenants {
        let mut client = secrets.inner.pool.get().await?;
        let Some(active) = TenantKeys::active_id(&client, tenant_id).await? else {
            continue;
        };
        if !sealed_under_another_key(secrets, tenant_id, active).await? {
            let tx = client.transaction().await?;
            TenantKeys::rotate(&tx, &secrets.inner.ring, tenant_id).await?;
            tx.commit().await?;
            secrets.inner.keys.forget_active(tenant_id);
            report.tenants_rotated += 1;
        }
        drop(client);
        encrypt(secrets, Some(tenant_id), report).await?;
    }
    Ok(())
}

pub async fn run(
    secrets: &SecretBox,
    operation: Operation,
    tenant: Option<Uuid>,
) -> Result<Report, SecretError> {
    if secrets.mode() == StorageMode::Plaintext {
        return Err(SecretError::Config(
            "secrets are stored in plaintext: configure CUFFLINK_SECRETS_KEK and CUFFLINK_SECRETS_KEK_ID (CUFFLINK_SECRET_STORAGE=dual) before resealing or verifying".into(),
        ));
    }
    let mut report = Report::new(operation);
    match operation {
        Operation::Encrypt => encrypt(secrets, tenant, &mut report).await?,
        Operation::RotateKek => rotate_kek(secrets, tenant, &mut report).await?,
        Operation::RotateDek => rotate_dek(secrets, tenant, &mut report).await?,
        Operation::Verify => {
            let (verified, rows_checked) = verify::run(secrets, tenant).await?;
            report.rows_checked = rows_checked;
            report.verified = Some(verified);
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::tests::data_key;
    use crate::SecretAad;

    #[test]
    fn a_row_is_sealed_again_when_it_has_no_seal_a_stale_one_or_a_retired_key() {
        let (active, retired) = (data_key(1, 10), data_key(1, 11));
        let aad = SecretAad::new("t", "c", ["r"]);
        let under = |key| Stored::new(None, Some(envelope::seal(key, &aad, b"v").expect("seal")));
        assert!(needs_seal(
            &Stored::new(Some("v".into()), None),
            false,
            active.id()
        ));
        assert!(!needs_seal(&under(&active), false, active.id()));
        assert!(needs_seal(&under(&active), true, active.id()));
        assert!(needs_seal(&under(&retired), false, active.id()));
        assert!(needs_seal(
            &Stored::new(None, Some(vec![9])),
            false,
            active.id()
        ));
    }

    #[test]
    fn operations_are_named_as_the_api_spells_them() {
        for (operation, name) in [
            (Operation::Encrypt, "encrypt"),
            (Operation::RotateKek, "rotate_kek"),
            (Operation::RotateDek, "rotate_dek"),
            (Operation::Verify, "verify"),
        ] {
            assert_eq!(operation.as_str(), name);
            assert_eq!(
                serde_json::to_value(operation).expect("json"),
                serde_json::Value::String(name.into())
            );
        }
    }

    #[test]
    fn the_other_operations_report_as_before() {
        let report = Report {
            rows_checked: 7,
            rows_rewritten: 3,
            keys_rewrapped: 2,
            tenants_rotated: 1,
            ..Report::new(Operation::RotateDek)
        };
        assert_eq!(
            serde_json::to_string(&report).expect("json"),
            r#"{"operation":"rotate_dek","rows_checked":7,"rows_rewritten":3,"keys_rewrapped":2,"tenants_rotated":1}"#
        );
        let verified = Report {
            rows_checked: 7,
            verified: Some(Verified::default()),
            ..Report::new(Operation::Verify)
        };
        assert_eq!(
            serde_json::to_value(&verified).expect("json")["verified"]["data_keys_checked"],
            0
        );
    }
}
