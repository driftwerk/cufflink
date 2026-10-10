use crate::cipher::{self, Key, KEY_LEN};
use crate::SecretError;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;
use zeroize::Zeroizing;

const KEK_VAR: &str = "CUFFLINK_SECRETS_KEK";
const KEK_ID_VAR: &str = "CUFFLINK_SECRETS_KEK_ID";
const KEK_PREVIOUS_VAR: &str = "CUFFLINK_SECRETS_KEK_PREVIOUS";
const STORAGE_VAR: &str = "CUFFLINK_SECRET_STORAGE";
const KEK_ID_MAX: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Plaintext,
    Dual,
    Encrypted,
}

impl StorageMode {
    pub fn as_str(self) -> &'static str {
        match self {
            StorageMode::Plaintext => "plaintext",
            StorageMode::Dual => "dual",
            StorageMode::Encrypted => "encrypted",
        }
    }

    pub fn stores_plain(self) -> bool {
        !matches!(self, StorageMode::Encrypted)
    }

    pub fn stores_sealed(self) -> bool {
        !matches!(self, StorageMode::Plaintext)
    }

    pub fn resolve(raw: Option<&str>, ring: &KeyRing) -> Result<Self, SecretError> {
        let mode = match raw.map(str::trim).filter(|raw| !raw.is_empty()) {
            None if ring.has_key() => StorageMode::Dual,
            None => StorageMode::Plaintext,
            Some(raw) => raw.parse()?,
        };
        if mode.stores_sealed() && !ring.has_key() {
            return Err(SecretError::Config(format!(
                "{STORAGE_VAR}={} needs {KEK_VAR} and {KEK_ID_VAR}",
                mode.as_str()
            )));
        }
        Ok(mode)
    }
}

impl FromStr for StorageMode {
    type Err = SecretError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "plaintext" => Ok(StorageMode::Plaintext),
            "dual" => Ok(StorageMode::Dual),
            "encrypted" => Ok(StorageMode::Encrypted),
            other => Err(SecretError::Config(format!(
                "{STORAGE_VAR}: '{other}' is not plaintext, dual or encrypted"
            ))),
        }
    }
}

struct Kek {
    id: String,
    key: Key,
}

#[derive(Clone, Default)]
pub struct KeyRing {
    current: Option<Arc<Kek>>,
    previous: Arc<[Kek]>,
}

impl fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyRing")
            .field("current", &self.current_id())
            .field(
                "previous",
                &self.previous.iter().map(|kek| &kek.id).collect::<Vec<_>>(),
            )
            .finish()
    }
}

fn valid_id(name: &str, id: &str) -> Result<String, SecretError> {
    let valid = !id.is_empty()
        && id.len() <= KEK_ID_MAX
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    valid.then(|| id.to_string()).ok_or_else(|| {
        SecretError::Config(format!(
            "{name}: a key id is 1 to {KEK_ID_MAX} characters of [A-Za-z0-9._-]"
        ))
    })
}

fn decode_key(name: &str, encoded: &str) -> Result<Key, SecretError> {
    let bytes = Zeroizing::new(
        STANDARD
            .decode(encoded.trim())
            .map_err(|_| SecretError::Config(format!("{name}: a key is not base64")))?,
    );
    Key::from_slice(&bytes).ok_or_else(|| {
        SecretError::Config(format!(
            "{name}: a key is {} bytes, not {KEY_LEN}",
            bytes.len()
        ))
    })
}

fn previous_key(entry: &str) -> Result<Kek, SecretError> {
    let (id, key) = entry.split_once(':').ok_or_else(|| {
        SecretError::Config(format!("{KEK_PREVIOUS_VAR}: an entry is not id:base64"))
    })?;
    Ok(Kek {
        id: valid_id(KEK_PREVIOUS_VAR, id.trim())?,
        key: decode_key(KEK_PREVIOUS_VAR, key)?,
    })
}

fn set(raw: Option<&str>) -> Option<&str> {
    raw.map(str::trim).filter(|raw| !raw.is_empty())
}

impl KeyRing {
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self, SecretError> {
        Self::parse(
            var(KEK_VAR).as_deref(),
            var(KEK_ID_VAR).as_deref(),
            var(KEK_PREVIOUS_VAR).as_deref(),
        )
    }

    pub fn parse(
        kek: Option<&str>,
        id: Option<&str>,
        previous: Option<&str>,
    ) -> Result<Self, SecretError> {
        let current = match (set(kek), set(id)) {
            (None, None) => None,
            (Some(kek), Some(id)) => Some(Kek {
                id: valid_id(KEK_ID_VAR, id)?,
                key: decode_key(KEK_VAR, kek)?,
            }),
            (Some(_), None) => {
                return Err(SecretError::Config(format!(
                    "{KEK_VAR} needs {KEK_ID_VAR}, the name data keys record it under"
                )))
            }
            (None, Some(_)) => {
                return Err(SecretError::Config(format!(
                    "{KEK_ID_VAR} is set and {KEK_VAR} is not"
                )))
            }
        };
        let previous = set(previous)
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|entry| !entry.is_empty())
                    .map(previous_key)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        if current.is_none() && !previous.is_empty() {
            return Err(SecretError::Config(format!(
                "{KEK_PREVIOUS_VAR} is set and {KEK_VAR} is not"
            )));
        }
        let ids = current.iter().chain(&previous).map(|kek| kek.id.as_str());
        if let Some(duplicate) = ids
            .clone()
            .enumerate()
            .find_map(|(index, id)| ids.clone().take(index).any(|seen| seen == id).then_some(id))
        {
            return Err(SecretError::Config(format!(
                "key id '{duplicate}' names two keys ({KEK_ID_VAR}, {KEK_PREVIOUS_VAR})"
            )));
        }
        Ok(Self {
            current: current.map(Arc::new),
            previous: previous.into(),
        })
    }

    pub fn storage_mode_from_vars(
        &self,
        var: impl Fn(&str) -> Option<String>,
    ) -> Result<StorageMode, SecretError> {
        StorageMode::resolve(var(STORAGE_VAR).as_deref(), self)
    }

    pub fn has_key(&self) -> bool {
        self.current.is_some()
    }

    pub fn current_id(&self) -> Option<&str> {
        self.current.as_deref().map(|kek| kek.id.as_str())
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &Key> {
        self.current
            .as_deref()
            .into_iter()
            .chain(self.previous.iter())
            .map(|kek| &kek.key)
    }

    fn key(&self, id: &str) -> Result<&Kek, SecretError> {
        self.current
            .as_deref()
            .into_iter()
            .chain(self.previous.iter())
            .find(|kek| kek.id == id)
            .ok_or_else(|| SecretError::UnknownKek(id.to_string()))
    }

    pub(crate) fn wrap(
        &self,
        tenant_id: Uuid,
        key_id: Uuid,
        data_key: &Key,
    ) -> Result<(&str, Vec<u8>), SecretError> {
        let kek = self.current.as_deref().ok_or(SecretError::NoKey)?;
        cipher::seal(&kek.key, &wrap_aad(tenant_id, key_id), data_key.bytes())
            .map(|wrapped| (kek.id.as_str(), wrapped))
    }

    pub(crate) fn unwrap(
        &self,
        kek_id: &str,
        tenant_id: Uuid,
        key_id: Uuid,
        wrapped: &[u8],
    ) -> Result<Key, SecretError> {
        let opened = cipher::open(
            &self.key(kek_id)?.key,
            &wrap_aad(tenant_id, key_id),
            wrapped,
        )?;
        Key::from_slice(&opened).ok_or(SecretError::Malformed("a data key is not 32 bytes"))
    }
}

fn wrap_aad(tenant_id: Uuid, key_id: Uuid) -> Vec<u8> {
    format!("cufflink:dek:{tenant_id}:{key_id}").into_bytes()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn encoded(byte: u8) -> String {
        STANDARD.encode([byte; KEY_LEN])
    }

    pub(crate) fn ring(current: (&str, u8), previous: &[(&str, u8)]) -> KeyRing {
        let previous = previous
            .iter()
            .map(|(id, byte)| format!("{id}:{}", encoded(*byte)))
            .collect::<Vec<_>>()
            .join(",");
        KeyRing::parse(Some(&encoded(current.1)), Some(current.0), Some(&previous)).expect("ring")
    }

    fn config_error(result: Result<KeyRing, SecretError>) -> String {
        match result {
            Err(SecretError::Config(message)) => message,
            other => panic!("expected a configuration error, got {other:?}"),
        }
    }

    #[test]
    fn no_variables_is_a_ring_without_keys() {
        for empty in [None, Some(""), Some("  ")] {
            let ring = KeyRing::parse(empty, empty, empty).expect("ring");
            assert!(!ring.has_key());
            assert_eq!(ring.current_id(), None);
        }
    }

    #[test]
    fn a_key_and_its_id_come_together() {
        assert!(config_error(KeyRing::parse(Some(&encoded(1)), None, None))
            .contains("CUFFLINK_SECRETS_KEK_ID"));
        assert!(config_error(KeyRing::parse(None, Some("k1"), None))
            .contains("CUFFLINK_SECRETS_KEK is not"));
        assert!(config_error(KeyRing::parse(
            None,
            None,
            Some(&format!("k0:{}", encoded(0)))
        ))
        .contains("CUFFLINK_SECRETS_KEK_PREVIOUS is set"));
    }

    #[test]
    fn a_key_that_does_not_parse_is_not_echoed() {
        let short = STANDARD.encode([7u8; 16]);
        for (key, reason) in [("not base64 !", "not base64"), (short.as_str(), "16 bytes")] {
            let message = config_error(KeyRing::parse(Some(key), Some("k1"), None));
            assert!(message.contains(reason), "{message}");
            assert!(!message.contains(key), "{message}");
        }
        let message = config_error(KeyRing::parse(
            Some(&encoded(1)),
            Some("k1"),
            Some("k0:not base64 !"),
        ));
        assert!(!message.contains("not base64 !"), "{message}");
    }

    #[test]
    fn ids_are_names_and_name_one_key_each() {
        for id in ["", "has space", "a:b", "a,b", &"k".repeat(65)] {
            assert!(
                KeyRing::parse(Some(&encoded(1)), Some(id), None).is_err(),
                "'{id}'"
            );
        }
        let message = config_error(KeyRing::parse(
            Some(&encoded(1)),
            Some("k1"),
            Some(&format!("k0:{},k1:{}", encoded(0), encoded(2))),
        ));
        assert!(message.contains("'k1' names two keys"), "{message}");
        let ring = ring(("2026-09", 2), &[("2026-03", 1), ("2025.09_a", 0)]);
        assert_eq!(ring.current_id(), Some("2026-09"));
        assert_eq!(
            format!("{ring:?}"),
            "KeyRing { current: Some(\"2026-09\"), previous: [\"2026-03\", \"2025.09_a\"] }"
        );
    }

    #[test]
    fn a_data_key_unwraps_under_the_key_that_wrapped_it_and_for_its_tenant() {
        let old = ring(("k1", 1), &[]);
        let new = ring(("k2", 2), &[("k1", 1)]);
        let (tenant, key_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let data_key = Key::generate().expect("key");
        let (kek_id, wrapped) = old.wrap(tenant, key_id, &data_key).expect("wrap");
        assert_eq!(kek_id, "k1");
        assert_eq!(wrapped.len(), 12 + KEY_LEN + 16);

        assert!(new.unwrap("k1", tenant, key_id, &wrapped).expect("unwrap") == data_key);
        assert!(matches!(
            new.unwrap("k2", tenant, key_id, &wrapped),
            Err(SecretError::Authentication)
        ));
        assert!(matches!(
            old.unwrap("k2", tenant, key_id, &wrapped),
            Err(SecretError::UnknownKek(id)) if id == "k2"
        ));
        assert!(matches!(
            new.unwrap("k1", Uuid::from_u128(9), key_id, &wrapped),
            Err(SecretError::Authentication)
        ));
        assert!(matches!(
            new.unwrap("k1", tenant, Uuid::from_u128(9), &wrapped),
            Err(SecretError::Authentication)
        ));
        assert!(matches!(
            KeyRing::default().wrap(tenant, key_id, &data_key),
            Err(SecretError::NoKey)
        ));
    }

    #[test]
    fn the_storage_mode_follows_the_key_unless_it_is_set() {
        let (without, with) = (KeyRing::default(), ring(("k1", 1), &[]));
        let resolve = |raw, ring| StorageMode::resolve(raw, ring);
        assert_eq!(
            resolve(None, &without).expect("mode"),
            StorageMode::Plaintext
        );
        assert_eq!(
            resolve(Some(" "), &without).expect("mode"),
            StorageMode::Plaintext
        );
        assert_eq!(resolve(None, &with).expect("mode"), StorageMode::Dual);
        assert_eq!(
            resolve(Some("plaintext"), &with).expect("mode"),
            StorageMode::Plaintext
        );
        assert_eq!(
            resolve(Some("encrypted"), &with).expect("mode"),
            StorageMode::Encrypted
        );
        for mode in ["dual", "encrypted"] {
            assert!(
                matches!(resolve(Some(mode), &without), Err(SecretError::Config(m)) if m.contains("CUFFLINK_SECRETS_KEK")),
                "{mode}"
            );
        }
        assert!(resolve(Some("Dual"), &with).is_err());
    }
}
