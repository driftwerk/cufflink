use crate::cipher::{self, Key};
use crate::{SecretError, SecretString};
use uuid::Uuid;

const VERSION: u8 = 1;
const HEADER_LEN: usize = 1 + 16;

pub struct DataKey {
    pub(crate) id: Uuid,
    pub(crate) tenant_id: Uuid,
    pub(crate) key: Key,
}

impl DataKey {
    pub fn id(&self) -> Uuid {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretAad {
    pub table: &'static str,
    pub column: &'static str,
    pub row: Vec<String>,
}

impl SecretAad {
    pub fn new(
        table: &'static str,
        column: &'static str,
        row: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            table,
            column,
            row: row.into_iter().map(Into::into).collect(),
        }
    }

    fn render(&self, tenant_id: Uuid) -> Vec<u8> {
        let tenant = tenant_id.to_string();
        ["cufflink:secret", tenant.as_str(), self.table, self.column]
            .into_iter()
            .chain(self.row.iter().map(String::as_str))
            .flat_map(|part| {
                (part.len() as u64)
                    .to_be_bytes()
                    .into_iter()
                    .chain(part.bytes())
            })
            .collect()
    }
}

pub fn key_id(sealed: &[u8]) -> Result<Uuid, SecretError> {
    match sealed.split_at_checked(HEADER_LEN) {
        Some(([VERSION, id @ ..], _)) => {
            Uuid::from_slice(id).map_err(|_| SecretError::Malformed("the key id is not 16 bytes"))
        }
        Some(_) => Err(SecretError::Malformed("an unknown format version")),
        None => Err(SecretError::Malformed("shorter than its header")),
    }
}

pub fn seal(key: &DataKey, aad: &SecretAad, plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
    let body = cipher::seal(&key.key, &aad.render(key.tenant_id), plaintext)?;
    Ok(std::iter::once(VERSION)
        .chain(key.id.into_bytes())
        .chain(body)
        .collect())
}

pub fn open(key: &DataKey, aad: &SecretAad, sealed: &[u8]) -> Result<SecretString, SecretError> {
    let sealed_under = key_id(sealed)?;
    if sealed_under != key.id {
        return Err(SecretError::UnknownDataKey {
            tenant_id: key.tenant_id,
            key_id: sealed_under,
        });
    }
    let opened = cipher::open(&key.key, &aad.render(key.tenant_id), &sealed[HEADER_LEN..])?;
    std::str::from_utf8(&opened)
        .map(SecretString::new)
        .map_err(|_| SecretError::Malformed("the secret is not UTF-8"))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn data_key(tenant: u128, id: u128) -> DataKey {
        DataKey {
            id: Uuid::from_u128(id),
            tenant_id: Uuid::from_u128(tenant),
            key: Key::generate().expect("key"),
        }
    }

    fn config(service: u128, key: &str) -> SecretAad {
        SecretAad::new(
            "service_configs",
            "value",
            [Uuid::from_u128(service).to_string(), key.to_string()],
        )
    }

    #[test]
    fn a_secret_opens_in_the_row_it_was_sealed_for() {
        let key = data_key(1, 10);
        let sealed = seal(&key, &config(5, "STRIPE_KEY"), b"sk_live_1").expect("seal");
        assert_eq!(sealed[0], 1);
        assert_eq!(key_id(&sealed).expect("key id"), key.id());
        assert_eq!(
            open(&key, &config(5, "STRIPE_KEY"), &sealed)
                .expect("open")
                .expose(),
            "sk_live_1"
        );
    }

    #[test]
    fn a_secret_moved_to_another_row_column_or_tenant_does_not_open() {
        let key = data_key(1, 10);
        let sealed = seal(&key, &config(5, "STRIPE_KEY"), b"sk_live_1").expect("seal");
        let elsewhere = [
            config(5, "STRIPE_KEY_2"),
            config(6, "STRIPE_KEY"),
            SecretAad {
                column: "value_enc",
                ..config(5, "STRIPE_KEY")
            },
            SecretAad {
                table: "webhooks",
                ..config(5, "STRIPE_KEY")
            },
        ];
        for aad in elsewhere {
            assert!(
                matches!(open(&key, &aad, &sealed), Err(SecretError::Authentication)),
                "{aad:?}"
            );
        }
        let same_key_other_tenant = DataKey {
            tenant_id: Uuid::from_u128(2),
            id: key.id,
            key: key.key.clone(),
        };
        assert!(matches!(
            open(&same_key_other_tenant, &config(5, "STRIPE_KEY"), &sealed),
            Err(SecretError::Authentication)
        ));
    }

    #[test]
    fn row_keys_that_join_to_the_same_text_are_different_rows() {
        let tenant = Uuid::from_u128(1);
        let aad = |row: &[&str]| SecretAad::new("t", "c", row.iter().copied()).render(tenant);
        assert_ne!(aad(&["a", "bc"]), aad(&["ab", "c"]));
        assert_ne!(aad(&["a:b"]), aad(&["a", "b"]));
        assert_ne!(aad(&["a", ""]), aad(&["a"]));
        assert_ne!(
            SecretAad::new("t", "cd", ["e"]).render(tenant),
            SecretAad::new("tc", "d", ["e"]).render(tenant)
        );
    }

    #[test]
    fn a_value_sealed_under_another_key_names_that_key() {
        let (key, other) = (data_key(1, 10), data_key(1, 11));
        let sealed = seal(&other, &config(5, "K"), b"v").expect("seal");
        assert!(matches!(
            open(&key, &config(5, "K"), &sealed),
            Err(SecretError::UnknownDataKey { key_id, .. }) if key_id == other.id()
        ));
    }

    #[test]
    fn what_is_not_a_sealed_value_is_malformed() {
        let key = data_key(1, 10);
        let sealed = seal(&key, &config(5, "K"), b"v").expect("seal");
        let unknown_version: Vec<u8> = std::iter::once(2)
            .chain(sealed[1..].iter().copied())
            .collect();
        for (value, reason) in [
            (&b""[..], "shorter than its header"),
            (&sealed[..16], "shorter than its header"),
            (&unknown_version[..], "an unknown format version"),
            (&sealed[..HEADER_LEN + 12], "shorter than a nonce and a tag"),
        ] {
            assert!(
                matches!(open(&key, &config(5, "K"), value), Err(SecretError::Malformed(r)) if r == reason),
                "{reason}"
            );
        }
        let not_text = seal(&key, &config(5, "K"), &[0xff, 0xfe]).expect("seal");
        assert!(matches!(
            open(&key, &config(5, "K"), &not_text),
            Err(SecretError::Malformed("the secret is not UTF-8"))
        ));
    }

    #[test]
    fn an_error_names_no_secret() {
        let key = data_key(1, 10);
        let sealed = seal(&key, &config(5, "K"), b"sk_live_1").expect("seal");
        let error = open(&key, &config(6, "K"), &sealed).expect_err("another row");
        assert!(!format!("{error} {error:?}").contains("sk_live_1"));
    }
}
