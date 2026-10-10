mod cipher;
pub mod columns;
pub mod configs;
mod envelope;
mod error;
mod key_ring;
mod mac;
pub mod reseal;
mod string;
mod tenant_keys;
mod verify;

pub use columns::SealedColumn;
pub use envelope::{DataKey, SecretAad};
pub use error::SecretError;
pub use key_ring::{KeyRing, StorageMode};
pub use mac::MacKeys;
pub use string::SecretString;

use deadpool_postgres::{GenericClient, Pool};
use std::fmt;
use std::sync::Arc;
use tenant_keys::{Durability, TenantKeys};
use uuid::Uuid;
use zeroize::Zeroize;

pub fn random_bytes<const N: usize>() -> Result<[u8; N], SecretError> {
    let mut bytes = [0u8; N];
    cipher::fill(&mut bytes).map(|()| bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Plaintext {
    pub table: &'static str,
    pub alone: i64,
    pub beside_sealed: i64,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Stored {
    plain: Option<String>,
    sealed: Option<Vec<u8>>,
}

impl Stored {
    pub fn new(plain: Option<String>, sealed: Option<Vec<u8>>) -> Self {
        Self { plain, sealed }
    }

    pub fn plain(&self) -> Option<&str> {
        self.plain.as_deref()
    }

    pub fn sealed(&self) -> Option<&[u8]> {
        self.sealed.as_deref()
    }
}

impl Drop for Stored {
    fn drop(&mut self) {
        self.plain.zeroize();
    }
}

impl fmt::Debug for Stored {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stored")
            .field("plain", &self.plain.as_ref().map(|_| "[redacted]"))
            .field("sealed", &self.sealed.as_ref().map(Vec::len))
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadPlan {
    Absent,
    Plain,
    Sealed,
    SealedUnlessStale,
}

fn read_plan(mode: StorageMode, aad: &SecretAad, stored: &Stored) -> Result<ReadPlan, SecretError> {
    match (mode, stored.plain.is_some(), stored.sealed.is_some()) {
        (_, false, false) => Ok(ReadPlan::Absent),
        (StorageMode::Encrypted, true, false) => Err(SecretError::NotSealed {
            table: aad.table,
            column: aad.column,
        }),
        (StorageMode::Dual, true, true) => Ok(ReadPlan::SealedUnlessStale),
        (StorageMode::Plaintext, true, _) | (StorageMode::Dual, true, false) => Ok(ReadPlan::Plain),
        (_, _, true) => Ok(ReadPlan::Sealed),
    }
}

pub(crate) struct Read {
    pub(crate) secret: Option<SecretString>,
    pub(crate) stale: bool,
}

fn count(op: &'static str, result: &'static str) {
    metrics::counter!("cufflink_secret_operations_total", "op" => op, "result" => result)
        .increment(1);
}

fn counted<T>(op: &'static str, result: Result<T, SecretError>) -> Result<T, SecretError> {
    count(op, if result.is_ok() { "ok" } else { "error" });
    result
}

struct Inner {
    pool: Pool,
    ring: KeyRing,
    mode: StorageMode,
    keys: TenantKeys,
}

#[derive(Clone)]
pub struct SecretBox {
    inner: Arc<Inner>,
}

impl fmt::Debug for SecretBox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretBox")
            .field("ring", &self.inner.ring)
            .field("mode", &self.inner.mode)
            .finish()
    }
}

impl SecretBox {
    pub fn new(pool: Pool, ring: KeyRing, mode: StorageMode) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                ring,
                mode,
                keys: TenantKeys::default(),
            }),
        }
    }

    pub fn from_vars(
        pool: Pool,
        var: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, SecretError> {
        let ring = KeyRing::from_vars(&var)?;
        let mode = ring.storage_mode_from_vars(&var)?;
        Ok(Self::new(pool, ring, mode))
    }

    pub fn mode(&self) -> StorageMode {
        self.inner.mode
    }

    pub fn ring(&self) -> &KeyRing {
        &self.inner.ring
    }

    async fn seal_through(
        &self,
        client: &impl GenericClient,
        durability: Durability,
        tenant_id: Uuid,
        aad: &SecretAad,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, SecretError> {
        let key = self
            .inner
            .keys
            .active(client, durability, &self.inner.ring, tenant_id)
            .await?;
        envelope::seal(&key, aad, plaintext)
    }

    pub async fn seal(
        &self,
        client: &impl GenericClient,
        tenant_id: Uuid,
        aad: &SecretAad,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, SecretError> {
        counted(
            "seal",
            self.seal_through(client, Durability::Committed, tenant_id, aad, plaintext)
                .await,
        )
    }

    pub async fn seal_within(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        tenant_id: Uuid,
        aad: &SecretAad,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, SecretError> {
        counted(
            "seal",
            self.seal_through(tx, Durability::Pending, tenant_id, aad, plaintext)
                .await,
        )
    }

    pub async fn open(
        &self,
        client: &impl GenericClient,
        tenant_id: Uuid,
        aad: &SecretAad,
        sealed: &[u8],
    ) -> Result<SecretString, SecretError> {
        let opened = async {
            let key = self
                .inner
                .keys
                .get(
                    client,
                    Durability::Committed,
                    &self.inner.ring,
                    tenant_id,
                    envelope::key_id(sealed)?,
                )
                .await?;
            envelope::open(&key, aad, sealed)
        };
        counted("open", opened.await)
    }

    pub async fn reseal(
        &self,
        client: &impl GenericClient,
        from: (Uuid, &SecretAad),
        to: (Uuid, &SecretAad),
        sealed: &[u8],
    ) -> Result<Vec<u8>, SecretError> {
        let resealed = async {
            let secret = self.open(client, from.0, from.1, sealed).await?;
            self.seal(client, to.0, to.1, secret.expose().as_bytes())
                .await
        };
        counted("reseal", resealed.await)
    }

    pub(crate) fn stored(&self, plaintext: &str, sealed: Option<Vec<u8>>) -> Stored {
        Stored {
            plain: self
                .inner
                .mode
                .stores_plain()
                .then(|| plaintext.to_string()),
            sealed,
        }
    }

    pub async fn store(
        &self,
        client: &impl GenericClient,
        tenant_id: Uuid,
        aad: &SecretAad,
        plaintext: &str,
    ) -> Result<Stored, SecretError> {
        let sealed = match self.inner.mode.stores_sealed() {
            true => Some(
                self.seal(client, tenant_id, aad, plaintext.as_bytes())
                    .await?,
            ),
            false => None,
        };
        Ok(self.stored(plaintext, sealed))
    }

    pub async fn store_within(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        tenant_id: Uuid,
        aad: &SecretAad,
        plaintext: &str,
    ) -> Result<Stored, SecretError> {
        let sealed = match self.inner.mode.stores_sealed() {
            true => Some(
                self.seal_within(tx, tenant_id, aad, plaintext.as_bytes())
                    .await?,
            ),
            false => None,
        };
        Ok(self.stored(plaintext, sealed))
    }

    pub(crate) async fn active_key_within(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        tenant_id: Uuid,
    ) -> Result<Arc<DataKey>, SecretError> {
        self.inner
            .keys
            .active(tx, Durability::Pending, &self.inner.ring, tenant_id)
            .await
    }

    pub(crate) async fn read_whether_stale(
        &self,
        client: &impl GenericClient,
        tenant_id: Uuid,
        aad: &SecretAad,
        stored: &Stored,
    ) -> Result<Read, SecretError> {
        let plain = || stored.plain().map(SecretString::new);
        let fresh = |secret| Read {
            secret,
            stale: false,
        };
        let open = || async {
            match stored.sealed() {
                Some(sealed) => self.open(client, tenant_id, aad, sealed).await.map(Some),
                None => Ok(None),
            }
        };
        match read_plan(self.inner.mode, aad, stored)? {
            ReadPlan::Absent => Ok(fresh(None)),
            ReadPlan::Plain => Ok(fresh(plain())),
            ReadPlan::Sealed => open().await.map(fresh),
            ReadPlan::SealedUnlessStale => {
                let opened = open().await?;
                match opened == plain() {
                    true => Ok(fresh(opened)),
                    false => {
                        count("open", "stale");
                        tracing::warn!(
                            event = "secret_stale",
                            table = aad.table,
                            column = aad.column,
                            tenant_id = %tenant_id,
                            "A sealed secret is older than its plaintext, written by a release that does not seal; `cufflink admin secrets reseal --operation encrypt` seals it again"
                        );
                        Ok(Read {
                            secret: plain(),
                            stale: true,
                        })
                    }
                }
            }
        }
    }

    pub async fn read(
        &self,
        client: &impl GenericClient,
        tenant_id: Uuid,
        aad: &SecretAad,
        stored: &Stored,
    ) -> Result<Option<SecretString>, SecretError> {
        self.read_whether_stale(client, tenant_id, aad, stored)
            .await
            .map(|read| read.secret)
    }

    pub async fn plaintext(&self) -> Result<Vec<Plaintext>, SecretError> {
        let client = self.inner.pool.get().await?;
        let mut found = Vec::new();
        for column in columns::ALL {
            if let Some(sql) = column.count_plaintext() {
                let row = client.query_one(&sql, &[]).await?;
                found.push(Plaintext {
                    table: column.table,
                    alone: row.get(0),
                    beside_sealed: row.get(1),
                });
            }
        }
        Ok(found)
    }

    pub async fn check_startup(&self) -> Result<(), SecretError> {
        if self.inner.mode != StorageMode::Encrypted {
            return Ok(());
        }
        let plaintext = self.plaintext().await?;
        let listed = |count: fn(&Plaintext) -> i64| {
            plaintext
                .iter()
                .filter(|found| count(found) > 0)
                .map(|found| format!("{}: {}", found.table, count(found)))
                .collect::<Vec<_>>()
        };
        let alone = listed(|found| found.alone);
        if !alone.is_empty() {
            return Err(SecretError::Config(format!(
                "CUFFLINK_SECRET_STORAGE=encrypted, and secrets are stored in plaintext alone ({}); start with CUFFLINK_SECRET_STORAGE=dual and run `cufflink admin secrets reseal --operation encrypt`",
                alone.join(", ")
            )));
        }
        for found in &plaintext {
            metrics::gauge!("cufflink_secret_plaintext_rows", "table" => found.table)
                .set(found.beside_sealed as f64);
        }
        let beside = listed(|found| found.beside_sealed);
        if !beside.is_empty() {
            tracing::warn!(
                event = "secret_plaintext_kept",
                rows = %beside.join(", "),
                "Secrets stored under dual keep their plaintext beside the sealed value until `cufflink admin secrets reseal --operation encrypt` runs under encrypted"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aad() -> SecretAad {
        columns::SERVICE_CONFIG_VALUE.aad(["s", "k"])
    }

    fn stored(plain: bool, sealed: bool) -> Stored {
        Stored::new(plain.then(|| "v".to_string()), sealed.then(|| vec![1]))
    }

    #[test]
    fn where_a_secret_is_read_from_in_each_mode() {
        use ReadPlan::*;
        use StorageMode::*;
        for (mode, plain, sealed, plan) in [
            (Plaintext, false, false, Absent),
            (Plaintext, true, false, Plain),
            (Plaintext, true, true, Plain),
            (Plaintext, false, true, Sealed),
            (Dual, false, false, Absent),
            (Dual, true, false, Plain),
            (Dual, true, true, SealedUnlessStale),
            (Dual, false, true, Sealed),
            (Encrypted, false, false, Absent),
            (Encrypted, true, true, Sealed),
            (Encrypted, false, true, Sealed),
        ] {
            assert_eq!(
                read_plan(mode, &aad(), &stored(plain, sealed)).expect("plan"),
                plan,
                "{mode:?} plain={plain} sealed={sealed}"
            );
        }
        assert!(matches!(
            read_plan(Encrypted, &aad(), &stored(true, false)),
            Err(SecretError::NotSealed {
                table: "service_configs",
                column: "value"
            })
        ));
    }

    #[test]
    fn what_is_stored_never_prints() {
        let stored = Stored::new(Some("hunter2".into()), Some(vec![1, 2, 3]));
        assert_eq!(
            format!("{stored:?}"),
            "Stored { plain: Some(\"[redacted]\"), sealed: Some(3) }"
        );
    }

    #[test]
    fn random_bytes_differ() {
        let (a, b) = (
            random_bytes::<32>().expect("random"),
            random_bytes::<32>().expect("random"),
        );
        assert_ne!(a, b);
        assert_ne!(a, [0u8; 32]);
    }
}
