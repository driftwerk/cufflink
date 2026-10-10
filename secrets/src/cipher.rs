use crate::SecretError;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use zeroize::Zeroizing;

pub const KEY_LEN: usize = 32;
const TAG_LEN: usize = 16;

#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct Key(Zeroizing<[u8; KEY_LEN]>);

impl Key {
    pub(crate) fn from_slice(bytes: &[u8]) -> Option<Self> {
        <[u8; KEY_LEN]>::try_from(bytes)
            .ok()
            .map(|key| Self(Zeroizing::new(key)))
    }

    pub(crate) fn generate() -> Result<Self, SecretError> {
        let mut key = Zeroizing::new([0u8; KEY_LEN]);
        fill(key.as_mut())?;
        Ok(Self(key))
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    fn cipher(&self) -> Result<LessSafeKey, SecretError> {
        UnboundKey::new(&AES_256_GCM, self.bytes())
            .map(LessSafeKey::new)
            .map_err(|_| SecretError::Malformed("a key is not 32 bytes"))
    }
}

pub(crate) fn fill(bytes: &mut [u8]) -> Result<(), SecretError> {
    SystemRandom::new()
        .fill(bytes)
        .map_err(|_| SecretError::Random)
}

pub(crate) fn seal(key: &Key, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
    let mut nonce = [0u8; NONCE_LEN];
    fill(&mut nonce)?;
    let mut sealed = Vec::with_capacity(NONCE_LEN + plaintext.len() + TAG_LEN);
    sealed.extend_from_slice(&nonce);
    let mut body = Zeroizing::new(plaintext.to_vec());
    key.cipher()?
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad),
            &mut *body,
        )
        .map_err(|_| SecretError::Malformed("a value is too long to seal"))?;
    sealed.extend_from_slice(&body);
    Ok(sealed)
}

pub(crate) fn open(
    key: &Key,
    aad: &[u8],
    sealed: &[u8],
) -> Result<Zeroizing<Vec<u8>>, SecretError> {
    let (nonce, body) = sealed
        .split_at_checked(NONCE_LEN)
        .filter(|(_, body)| body.len() >= TAG_LEN)
        .ok_or(SecretError::Malformed("shorter than a nonce and a tag"))?;
    let nonce = Nonce::try_assume_unique_for_key(nonce)
        .map_err(|_| SecretError::Malformed("the nonce is not 12 bytes"))?;
    let mut body = Zeroizing::new(body.to_vec());
    let length = key
        .cipher()?
        .open_in_place(nonce, Aad::from(aad), &mut body)
        .map_err(|_| SecretError::Authentication)?
        .len();
    body.truncate(length);
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_was_sealed_opens_under_the_same_key_and_data() {
        let key = Key::generate().expect("key");
        let sealed = seal(&key, b"row 1", b"hunter2").expect("seal");
        assert_eq!(sealed.len(), NONCE_LEN + 7 + TAG_LEN);
        assert_eq!(
            open(&key, b"row 1", &sealed).expect("open").as_slice(),
            b"hunter2"
        );
        let empty = seal(&key, b"row 1", b"").expect("seal");
        assert!(open(&key, b"row 1", &empty).expect("open").is_empty());
    }

    #[test]
    fn two_seals_of_one_value_differ() {
        let key = Key::generate().expect("key");
        assert_ne!(
            seal(&key, b"", b"hunter2").expect("seal"),
            seal(&key, b"", b"hunter2").expect("seal")
        );
    }

    #[test]
    fn another_key_other_data_or_a_changed_byte_fail_authentication() {
        let key = Key::generate().expect("key");
        let sealed = seal(&key, b"row 1", b"hunter2").expect("seal");
        let other = Key::generate().expect("key");
        assert!(matches!(
            open(&other, b"row 1", &sealed),
            Err(SecretError::Authentication)
        ));
        assert!(matches!(
            open(&key, b"row 2", &sealed),
            Err(SecretError::Authentication)
        ));
        for index in 0..sealed.len() {
            let mut altered = sealed.clone();
            altered[index] ^= 1;
            assert!(
                matches!(
                    open(&key, b"row 1", &altered),
                    Err(SecretError::Authentication)
                ),
                "byte {index}"
            );
        }
    }

    #[test]
    fn a_truncated_value_is_malformed() {
        let key = Key::generate().expect("key");
        for length in [0, NONCE_LEN, NONCE_LEN + TAG_LEN - 1] {
            assert!(
                matches!(
                    open(&key, b"", &vec![0u8; length]),
                    Err(SecretError::Malformed(_))
                ),
                "{length} bytes"
            );
        }
    }
}
