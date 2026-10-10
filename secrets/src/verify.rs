use crate::columns::{self, Lock, SealedColumn};
use crate::envelope::{self, DataKey};
use crate::reseal::BATCH;
use crate::{tenant_keys, SecretAad, SecretBox, SecretError, StorageMode, Stored};
use deadpool_postgres::Transaction;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio_postgres::IsolationLevel;
use uuid::Uuid;

const LOGGED_ROWS: u64 = 20;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Verified {
    pub data_keys_checked: u64,
    pub data_keys_unopenable: u64,
    pub data_keys_under_previous_kek: u64,
    pub unopenable: BTreeMap<&'static str, u64>,
    pub not_sealed: BTreeMap<&'static str, u64>,
    pub rows_under_retired_data_key: u64,
    pub plain_differs: BTreeMap<&'static str, u64>,
}

impl Verified {
    pub fn is_clean(&self) -> bool {
        self.data_keys_unopenable == 0
            && [&self.unopenable, &self.not_sealed, &self.plain_differs]
                .into_iter()
                .all(|by_table| by_table.values().all(|rows| *rows == 0))
    }
}

#[derive(Default)]
struct FreshKeys {
    opened: HashMap<Uuid, Fresh>,
    unopenable: HashSet<Uuid>,
}

struct Fresh {
    key: DataKey,
    active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Opened { retired: bool, plain_differs: bool },
    KeyUnopenable,
    Unopenable,
    NotSealed,
    Plain,
}

impl Verdict {
    fn fault(self) -> Option<&'static str> {
        match self {
            Verdict::Unopenable => Some("unopenable"),
            Verdict::NotSealed => Some("not_sealed"),
            Verdict::Opened {
                plain_differs: true,
                ..
            } => Some("plain_differs"),
            Verdict::Opened { .. } | Verdict::KeyUnopenable | Verdict::Plain => None,
        }
    }
}

fn verdict(
    mode: StorageMode,
    keys: &FreshKeys,
    tenant_id: Uuid,
    aad: &SecretAad,
    stored: &Stored,
) -> Verdict {
    let Some(sealed) = stored.sealed() else {
        return match mode {
            StorageMode::Encrypted => Verdict::NotSealed,
            StorageMode::Dual | StorageMode::Plaintext => Verdict::Plain,
        };
    };
    let Ok(key_id) = envelope::key_id(sealed) else {
        return Verdict::Unopenable;
    };
    if keys.unopenable.contains(&key_id) {
        return Verdict::KeyUnopenable;
    }
    keys.opened
        .get(&key_id)
        .filter(|fresh| fresh.key.tenant_id == tenant_id)
        .and_then(|fresh| {
            let secret = envelope::open(&fresh.key, aad, sealed).ok()?;
            Some(Verdict::Opened {
                retired: !fresh.active,
                plain_differs: mode == StorageMode::Dual
                    && stored.plain().is_some_and(|plain| plain != secret.expose()),
            })
        })
        .unwrap_or(Verdict::Unopenable)
}

impl Verified {
    fn count(&mut self, table: &'static str, verdict: Verdict) {
        let one_more = |by_table: &mut BTreeMap<&'static str, u64>| {
            *by_table.entry(table).or_default() += 1;
        };
        match verdict {
            Verdict::Opened {
                retired,
                plain_differs,
            } => {
                self.rows_under_retired_data_key += u64::from(retired);
                if plain_differs {
                    one_more(&mut self.plain_differs);
                }
            }
            Verdict::KeyUnopenable | Verdict::Unopenable => one_more(&mut self.unopenable),
            Verdict::NotSealed => one_more(&mut self.not_sealed),
            Verdict::Plain => {}
        }
    }
}

async fn data_keys(
    secrets: &SecretBox,
    snapshot: &Transaction<'_>,
    tenant: Option<Uuid>,
    verified: &mut Verified,
) -> Result<FreshKeys, SecretError> {
    let ring = &secrets.inner.ring;
    let mut keys = FreshKeys::default();
    let mut after: Option<Uuid> = None;
    loop {
        let page = snapshot
            .query(
                "SELECT key_id, tenant_id, kek_id, wrapped_dek, active FROM tenant_data_keys
                 WHERE ($1::uuid IS NULL OR key_id > $1) AND ($2::uuid IS NULL OR tenant_id = $2)
                 ORDER BY key_id LIMIT $3",
                &[&after, &tenant, &BATCH],
            )
            .await?;
        let Some(last) = page.last() else {
            return Ok(keys);
        };
        after = Some(last.get("key_id"));
        for row in &page {
            let (key_id, tenant_id): (Uuid, Uuid) = (row.get("key_id"), row.get("tenant_id"));
            let kek_id: &str = row.get("kek_id");
            verified.data_keys_checked += 1;
            verified.data_keys_under_previous_kek += u64::from(ring.current_id() != Some(kek_id));
            match tenant_keys::unwrapped(ring, tenant_id, row) {
                Ok(key) => {
                    let active = row.get("active");
                    keys.opened.insert(key_id, Fresh { key, active });
                }
                Err(unopenable) => {
                    verified.data_keys_unopenable += 1;
                    keys.unopenable.insert(key_id);
                    tracing::warn!(
                        event = "secret_verify_data_key_unopenable",
                        tenant_id = %tenant_id,
                        key_id = %key_id,
                        kek_id,
                        error = %unopenable,
                        "A data key does not unwrap under this key ring; the rows sealed under it are counted unopenable and not named one by one"
                    );
                }
            }
        }
    }
}

async fn column_rows(
    secrets: &SecretBox,
    snapshot: &Transaction<'_>,
    column: &SealedColumn,
    tenant: Option<Uuid>,
    keys: &FreshKeys,
    verified: &mut Verified,
) -> Result<u64, SecretError> {
    let select = column.select_batch(Lock::None);
    let mut after: Option<Uuid> = None;
    let (mut checked, mut logged) = (0, 0);
    loop {
        let page = snapshot.query(&select, &[&after, &tenant, &BATCH]).await?;
        let Some(last) = page.last() else {
            return Ok(checked);
        };
        after = Some(last.get("id"));
        checked += page.len() as u64;
        for row in &page {
            let id: Uuid = row.get("id");
            let tenant_id: Uuid = row.get("tenant_id");
            let aad = column.aad(row.get::<_, Vec<String>>("row"));
            let stored = Stored::new(row.get("plain"), row.get("sealed"));
            let found = verdict(secrets.mode(), keys, tenant_id, &aad, &stored);
            if let Some(fault) = found.fault().filter(|_| logged < LOGGED_ROWS) {
                logged += 1;
                tracing::warn!(
                    event = "secret_verify_row",
                    table = column.table,
                    id = %id,
                    tenant_id = %tenant_id,
                    fault,
                    "A stored secret is not one this key ring and storage mode read; a run names the first {LOGGED_ROWS} such rows of a table"
                );
            }
            verified.count(column.table, found);
        }
    }
}

pub(crate) async fn run(
    secrets: &SecretBox,
    tenant: Option<Uuid>,
) -> Result<(Verified, u64), SecretError> {
    let mut client = secrets.inner.pool.get().await?;
    let snapshot = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let mut verified = Verified::default();
    let keys = data_keys(secrets, &snapshot, tenant, &mut verified).await?;
    let mut checked = 0;
    for column in columns::ALL {
        checked += column_rows(secrets, &snapshot, column, tenant, &keys, &mut verified).await?;
    }
    Ok((verified, checked))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::tests::data_key;

    const TENANT: u128 = 1;

    fn aad() -> SecretAad {
        SecretAad::new("service_configs", "value", ["svc", "TOKEN"])
    }

    fn keys() -> FreshKeys {
        FreshKeys {
            opened: [(TENANT, 10, true), (TENANT, 11, false), (2, 20, true)]
                .into_iter()
                .map(|(tenant, id, active)| {
                    (
                        Uuid::from_u128(id),
                        Fresh {
                            key: data_key(tenant, id),
                            active,
                        },
                    )
                })
                .collect(),
            unopenable: [Uuid::from_u128(12)].into(),
        }
    }

    fn sealed_under(keys: &FreshKeys, id: u128, secret: &str) -> Vec<u8> {
        envelope::seal(
            &keys.opened[&Uuid::from_u128(id)].key,
            &aad(),
            secret.as_bytes(),
        )
        .expect("seal")
    }

    #[test]
    fn a_row_opens_with_its_tenants_fresh_key_or_is_counted_as_what_is_wrong_with_it() {
        let keys = keys();
        let judged = |mode, plain: Option<&str>, sealed: Option<Vec<u8>>| {
            verdict(
                mode,
                &keys,
                Uuid::from_u128(TENANT),
                &aad(),
                &Stored::new(plain.map(str::to_string), sealed),
            )
        };
        let opened = |retired, plain_differs| Verdict::Opened {
            retired,
            plain_differs,
        };
        let (dual, encrypted) = (StorageMode::Dual, StorageMode::Encrypted);
        let active = || Some(sealed_under(&keys, 10, "v"));

        assert_eq!(judged(dual, Some("v"), active()), opened(false, false));
        assert_eq!(judged(encrypted, None, active()), opened(false, false));
        assert_eq!(
            judged(dual, None, Some(sealed_under(&keys, 11, "v"))),
            opened(true, false),
            "a retired key still opens what it sealed"
        );
        assert_eq!(
            judged(dual, Some("newer"), active()),
            opened(false, true),
            "under dual the two forms are compared"
        );
        assert_eq!(
            judged(encrypted, Some("left over"), active()),
            opened(false, false),
            "under encrypted the plaintext is not what is read"
        );

        assert_eq!(judged(dual, Some("v"), None), Verdict::Plain);
        assert_eq!(judged(encrypted, Some("v"), None), Verdict::NotSealed);

        let altered = active().map(|mut sealed| {
            let last = sealed.len() - 1;
            sealed[last] ^= 1;
            sealed
        });
        let unknown_key = envelope::seal(&data_key(TENANT, 99), &aad(), b"v").expect("seal");
        let other_tenants = envelope::seal(&keys.opened[&Uuid::from_u128(20)].key, &aad(), b"v");
        for unopenable in [
            altered,
            Some(unknown_key),
            Some(other_tenants.expect("seal")),
            Some(vec![9, 9, 9]),
        ] {
            assert_eq!(judged(dual, Some("v"), unopenable), Verdict::Unopenable);
        }
        assert_eq!(
            judged(
                dual,
                Some("v"),
                Some(envelope::seal(&data_key(TENANT, 12), &aad(), b"v").expect("seal"))
            ),
            Verdict::KeyUnopenable,
            "a row under a key that did not unwrap is its key's fault"
        );
    }

    #[test]
    fn a_row_is_named_in_the_log_for_its_own_fault_and_never_for_its_keys() {
        let opened = |plain_differs| Verdict::Opened {
            retired: true,
            plain_differs,
        };
        assert_eq!(Verdict::Unopenable.fault(), Some("unopenable"));
        assert_eq!(Verdict::NotSealed.fault(), Some("not_sealed"));
        assert_eq!(opened(true).fault(), Some("plain_differs"));
        for unnamed in [opened(false), Verdict::KeyUnopenable, Verdict::Plain] {
            assert_eq!(unnamed.fault(), None, "{unnamed:?}");
        }
    }

    #[test]
    fn a_rotation_that_is_not_finished_is_clean_and_anything_unread_is_not() {
        let unfinished = Verified {
            data_keys_checked: 3,
            data_keys_under_previous_kek: 3,
            rows_under_retired_data_key: 9,
            unopenable: [("webhooks", 0)].into(),
            ..Verified::default()
        };
        assert!(unfinished.is_clean());
        for unread in [
            Verified {
                data_keys_unopenable: 1,
                ..Verified::default()
            },
            Verified {
                unopenable: [("webhooks", 1)].into(),
                ..Verified::default()
            },
            Verified {
                not_sealed: [("tenants", 1)].into(),
                ..Verified::default()
            },
            Verified {
                plain_differs: [("service_configs", 1)].into(),
                ..Verified::default()
            },
        ] {
            assert!(!unread.is_clean(), "{unread:?}");
        }
    }

    #[test]
    fn what_is_wrong_is_counted_by_table_and_what_opened_is_not() {
        let mut verified = Verified::default();
        let opened = |retired, plain_differs| Verdict::Opened {
            retired,
            plain_differs,
        };
        for (table, verdict) in [
            ("service_configs", opened(false, false)),
            ("service_configs", opened(true, false)),
            ("service_configs", opened(true, true)),
            ("service_configs", Verdict::Unopenable),
            ("webhooks", Verdict::Unopenable),
            ("webhooks", Verdict::KeyUnopenable),
            ("tenants", Verdict::NotSealed),
            ("tenants", Verdict::Plain),
        ] {
            verified.count(table, verdict);
        }
        assert_eq!(
            serde_json::to_value(&verified).expect("json"),
            serde_json::json!({
                "data_keys_checked": 0,
                "data_keys_unopenable": 0,
                "data_keys_under_previous_kek": 0,
                "unopenable": {"service_configs": 1, "webhooks": 2},
                "not_sealed": {"tenants": 1},
                "rows_under_retired_data_key": 2,
                "plain_differs": {"service_configs": 1},
            })
        );
    }
}
