use crate::cipher::Key;
use crate::KeyRing;
use ring::hmac;
use std::fmt;
use std::sync::Arc;

const SALT: &[u8] = b"cufflink:mac-keys:v1";

pub const TAG_LEN: usize = 32;

fn sub_key(ikm: &[u8], info: &str) -> hmac::Key {
    let prk = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, SALT), ikm);
    let expand = hmac::Key::new(hmac::HMAC_SHA256, prk.as_ref());
    let okm = hmac::sign(&expand, &[info.as_bytes(), &[1]].concat());
    hmac::Key::new(hmac::HMAC_SHA256, okm.as_ref())
}

#[derive(Clone)]
pub struct MacKeys {
    keys: Arc<[hmac::Key]>,
}

impl fmt::Debug for MacKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MacKeys")
            .field("keys", &self.keys.len())
            .finish()
    }
}

impl MacKeys {
    pub fn derive(label: &str, ring: &KeyRing, fallback: Option<&[u8]>) -> Option<Self> {
        let keys: Arc<[hmac::Key]> = ring
            .keys()
            .map(Key::bytes)
            .chain(fallback)
            .map(|ikm| sub_key(ikm, label))
            .collect();
        (!keys.is_empty()).then_some(Self { keys })
    }

    pub fn sign(&self, message: &[u8]) -> [u8; TAG_LEN] {
        let mut tag = [0u8; TAG_LEN];
        if let Some(newest) = self.keys.first() {
            tag.copy_from_slice(hmac::sign(newest, message).as_ref());
        }
        tag
    }

    pub fn verify(&self, message: &[u8], tag: &[u8]) -> bool {
        self.keys
            .iter()
            .any(|key| hmac::verify(key, message, tag).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_ring::tests::ring;
    use ring::hkdf;

    #[test]
    fn a_sub_key_is_hkdf_sha256_of_its_key_under_its_label() {
        struct Len;
        impl hkdf::KeyType for Len {
            fn len(&self) -> usize {
                TAG_LEN
            }
        }
        let mut expected = [0u8; TAG_LEN];
        hkdf::Salt::new(hkdf::HKDF_SHA256, SALT)
            .extract(b"input key")
            .expand(&[b"label"], Len)
            .expect("one block")
            .fill(&mut expected)
            .expect("fill");
        let derived = hmac::sign(&sub_key(b"input key", "label"), b"m");
        let reference = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &expected), b"m");
        assert_eq!(derived.as_ref(), reference.as_ref());
    }

    #[test]
    fn without_a_key_there_is_nothing_to_sign_with() {
        assert!(MacKeys::derive("outbox", &KeyRing::default(), None).is_none());
        let keys = MacKeys::derive("outbox", &KeyRing::default(), Some(b"0123456789abcdef"));
        assert_eq!(format!("{keys:?}"), "Some(MacKeys { keys: 1 })");
    }

    #[test]
    fn a_tag_verifies_under_its_label_and_message_alone() {
        let keys = MacKeys::derive("outbox", &ring(("k1", 1), &[]), None).expect("keys");
        let tag = keys.sign(b"row");
        assert!(keys.verify(b"row", &tag));
        assert!(!keys.verify(b"rows", &tag));
        assert!(!keys.verify(b"row", &tag[..TAG_LEN - 1]));
        let other = MacKeys::derive("cache", &ring(("k1", 1), &[]), None).expect("keys");
        assert!(!other.verify(b"row", &tag));
        let mut altered = tag;
        altered[0] ^= 1;
        assert!(!keys.verify(b"row", &altered));
    }

    #[test]
    fn the_newest_key_signs_and_every_key_given_verifies() {
        let fallback = b"a cache signing key".as_slice();
        let before =
            MacKeys::derive("outbox", &ring(("k1", 1), &[]), Some(fallback)).expect("keys");
        let after = MacKeys::derive("outbox", &ring(("k2", 2), &[("k1", 1)]), Some(fallback))
            .expect("keys");
        let only_fallback =
            MacKeys::derive("outbox", &KeyRing::default(), Some(fallback)).expect("keys");
        let only_new = MacKeys::derive("outbox", &ring(("k2", 2), &[]), None).expect("keys");

        let old = before.sign(b"row");
        assert!(after.verify(b"row", &old));
        assert!(!only_new.verify(b"row", &old));
        let new = after.sign(b"row");
        assert!(only_new.verify(b"row", &new));
        assert!(!before.verify(b"row", &new));
        assert_ne!(old, new);
        let fallen_back = only_fallback.sign(b"row");
        assert!(before.verify(b"row", &fallen_back) && after.verify(b"row", &fallen_back));
        assert!(!only_new.verify(b"row", &fallen_back));
    }
}
