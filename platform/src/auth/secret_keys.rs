use crate::errors::{PlatformError, PlatformResult};
use sha2::{Digest, Sha256};
use std::future::Future;
use subtle::ConstantTimeEq;

const PREFIX_LEN: usize = 12;
const PREFIX_ATTEMPTS: usize = 3;
const SECRET_LEN: usize = 43;
pub const MAX_NAME_LEN: usize = 200;

const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const UNBIASED_BELOW: u8 = 248;

fn random_base62(len: usize) -> PlatformResult<String> {
    let mut out = String::with_capacity(len);
    let mut bytes = [0u8; 64];
    while out.len() < len {
        getrandom::fill(&mut bytes)
            .map_err(|e| PlatformError::Internal(format!("no OS randomness: {e}")))?;
        out.extend(
            bytes
                .iter()
                .filter(|b| **b < UNBIASED_BELOW)
                .map(|b| char::from(BASE62[usize::from(b % 62)]))
                .take(len - out.len()),
        );
    }
    Ok(out)
}

pub fn generate() -> PlatformResult<(String, String)> {
    Ok((random_base62(PREFIX_LEN)?, random_base62(SECRET_LEN)?))
}

pub fn secret_hash(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

pub fn proves(secret: &str, stored_hash: &str) -> bool {
    secret_hash(secret)
        .as_bytes()
        .ct_eq(stored_hash.as_bytes())
        .into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyKind {
    tag: &'static str,
}

impl KeyKind {
    pub const fn new(tag: &'static str) -> Self {
        Self { tag }
    }

    pub fn format(&self, prefix: &str, secret: &str) -> String {
        format!("{}{prefix}_{secret}", self.tag)
    }

    pub fn parse<'a>(&self, raw: &'a str) -> Option<(&'a str, &'a str)> {
        let (prefix, secret) = raw.trim().strip_prefix(self.tag)?.split_once('_')?;
        let alphanumeric = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric());
        (prefix.len() == PREFIX_LEN && alphanumeric(prefix) && alphanumeric(secret))
            .then_some((prefix, secret))
    }
}

pub async fn mint<T, Stored>(
    kind: KeyKind,
    store: impl Fn(String, String) -> Stored,
) -> PlatformResult<(T, String)>
where
    Stored: Future<Output = PlatformResult<Option<T>>>,
{
    for _ in 0..PREFIX_ATTEMPTS {
        let (prefix, secret) = generate()?;
        let key = kind.format(&prefix, &secret);
        if let Some(stored) = store(prefix, secret).await? {
            return Ok((stored, key));
        }
    }
    Err(PlatformError::Internal(format!(
        "no free {} key prefix after {PREFIX_ATTEMPTS} attempts",
        kind.tag
    )))
}

pub fn name(raw: &str, kind: &str) -> PlatformResult<String> {
    match raw.trim() {
        "" => Err(PlatformError::Validation(format!(
            "{kind} name cannot be empty"
        ))),
        name if name.chars().count() > MAX_NAME_LEN => Err(PlatformError::Validation(format!(
            "{kind} name is longer than {MAX_NAME_LEN} characters"
        ))),
        name => Ok(name.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIND: KeyKind = KeyKind::new("cft_");

    #[test]
    fn keys_are_generated_in_their_format_and_parse_back() {
        let (prefix, secret) = generate().expect("randomness");
        assert_eq!((prefix.len(), secret.len()), (PREFIX_LEN, SECRET_LEN));
        let raw = KIND.format(&prefix, &secret);
        assert!(raw.starts_with("cft_"));
        assert_eq!(KIND.parse(&raw), Some((prefix.as_str(), secret.as_str())));
        assert_eq!(
            KeyKind::new("cfo_").parse(&raw),
            None,
            "a key of one kind is never taken for another"
        );
        assert_ne!(generate().ok(), generate().ok());
        let spread: std::collections::HashSet<char> =
            random_base62(4096).expect("randomness").chars().collect();
        assert_eq!(spread.len(), 62, "every base-62 digit is drawn");
        for malformed in [
            "",
            "ck_abc",
            "cft_short_secret",
            "cft_AAAAAAAAAAAA_",
            "cft_AAAAAAAAAAAA_sec-ret",
            "cft_AAAAAAAAAAA!_secret",
        ] {
            assert_eq!(KIND.parse(malformed), None, "{malformed}");
        }
    }

    #[test]
    fn a_secret_proves_only_its_own_hash() {
        let stored = secret_hash("s3cret");
        assert_eq!(stored, hex::encode(Sha256::digest(b"s3cret")));
        assert!(proves("s3cret", &stored));
        assert!(!proves("s3cre", &stored));
        assert!(!proves("s3cret", ""));
    }

    #[tokio::test]
    async fn a_mint_draws_a_fresh_prefix_while_one_collides_and_then_gives_up() {
        let tried = std::sync::Mutex::new(Vec::new());
        let (stored, key) = mint(KIND, |prefix, secret| {
            let collides = {
                let mut tried = tried.lock().expect("tried");
                tried.push(prefix.clone());
                tried.len() < PREFIX_ATTEMPTS
            };
            async move { Ok((!collides).then(|| (prefix, secret_hash(&secret)))) }
        })
        .await
        .expect("minted on the last attempt");
        let tried = tried.into_inner().expect("tried");
        assert_eq!(tried.len(), PREFIX_ATTEMPTS);
        assert_eq!(stored.0, tried[PREFIX_ATTEMPTS - 1]);
        let (prefix, secret) = KIND.parse(&key).expect("a key of its kind");
        assert_eq!((prefix, secret_hash(secret)), (stored.0.as_str(), stored.1));
        let refused = mint(KIND, |_, _| async { Ok(None::<()>) })
            .await
            .expect_err("every prefix collides");
        assert!(
            matches!(&refused, PlatformError::Internal(m) if m.starts_with("no free cft_")),
            "{refused:?}"
        );
    }

    #[test]
    fn a_name_is_trimmed_and_bounded() {
        assert_eq!(name(" runner ", "Runner key").expect("named"), "runner");
        for (raw, reason) in [
            (" ".to_string(), "Runner key name cannot be empty"),
            (
                "n".repeat(MAX_NAME_LEN + 1),
                "Runner key name is longer than 200 characters",
            ),
        ] {
            let refused = name(&raw, "Runner key").expect_err(reason);
            assert!(
                matches!(&refused, PlatformError::Validation(m) if m == reason),
                "{refused:?}"
            );
        }
        assert!(name(&"n".repeat(MAX_NAME_LEN), "Runner key").is_ok());
    }
}
