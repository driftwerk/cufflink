use cufflink_storage::{is_exact_key, is_exact_prefix, CannedAcl, Credential};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

pub const MAX_KEY_PREFIXES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    #[serde(default)]
    pub key_prefixes: Option<Vec<String>>,
    #[serde(default)]
    pub acls: BTreeSet<CannedAcl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Access {
    Platform,
    Own(Credential),
    Unopenable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantBucket {
    pub tenant_id: Uuid,
    pub bucket: String,
    pub rules: Rules,
    pub access: Access,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    KeyNotExact,
    KeyOutsidePrefix,
    AclNotAdmitted,
}

impl Refusal {
    pub fn code(self) -> &'static str {
        match self {
            Refusal::KeyNotExact => "key_not_exact",
            Refusal::KeyOutsidePrefix => "key_outside_prefix",
            Refusal::AclNotAdmitted => "acl_not_admitted",
        }
    }
}

impl Rules {
    pub fn admit(&self, keys: &[&str], acl: Option<CannedAcl>) -> Result<(), Refusal> {
        if !keys.iter().all(|key| is_exact_key(key)) {
            return Err(Refusal::KeyNotExact);
        }
        let within = |key: &str| {
            self.key_prefixes
                .as_ref()
                .is_none_or(|prefixes| prefixes.iter().any(|prefix| key.starts_with(prefix)))
        };
        if !keys.iter().all(|key| within(key)) {
            return Err(Refusal::KeyOutsidePrefix);
        }
        match acl {
            Some(acl) if !self.acls.contains(&acl) => Err(Refusal::AclNotAdmitted),
            _ => Ok(()),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match &self.key_prefixes {
            Some(prefixes) if prefixes.is_empty() => {
                Err("key_prefixes must list at least one prefix, or be null for any key".into())
            }
            Some(prefixes) if prefixes.len() > MAX_KEY_PREFIXES => Err(format!(
                "key_prefixes lists {} prefixes; at most {MAX_KEY_PREFIXES} are allowed",
                prefixes.len()
            )),
            Some(prefixes) => match prefixes.iter().find(|prefix| !is_exact_prefix(prefix)) {
                Some(bad) => Err(format!(
                    "key_prefixes entry {bad:?} must be an exact key, optionally ending in '/'"
                )),
                None => Ok(()),
            },
            None => Ok(()),
        }
    }
}

pub fn validate_bucket_name(bucket: &str) -> Result<(), String> {
    let edge = |c: Option<char>| c.is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let named = (3..=63).contains(&bucket.len())
        && bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '-' | '_'))
        && edge(bucket.chars().next())
        && edge(bucket.chars().last());
    match named {
        true => Ok(()),
        false => Err(format!(
            "bucket {bucket:?} must be 3 to 63 lowercase letters, digits, '.', '-' or '_', \
             beginning and ending with a letter or digit"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(prefixes: Option<&[&str]>, acls: &[CannedAcl]) -> Rules {
        Rules {
            key_prefixes: prefixes.map(|p| p.iter().map(|s| s.to_string()).collect()),
            acls: acls.iter().copied().collect(),
        }
    }

    #[test]
    fn keys_must_be_exact_and_within_the_prefixes() {
        let documents = rules(Some(&["leads/", "archive/2026"]), &[]);
        for admitted in [
            "leads/7/Café façade #2 50%+off?.pdf",
            "leads/a b",
            "archive/2026-01/x",
        ] {
            assert_eq!(documents.admit(&[admitted], None), Ok(()), "{admitted}");
        }
        assert_eq!(
            documents.admit(&["other/a.pdf"], None),
            Err(Refusal::KeyOutsidePrefix)
        );
        assert_eq!(
            documents.admit(&["leads/a.pdf", "other/a.pdf"], None),
            Err(Refusal::KeyOutsidePrefix),
            "every key of a call"
        );
        for not_exact in [
            "leads//a",
            "leads/",
            "/leads/a",
            "leads/../x",
            "leads/\u{7}",
        ] {
            assert_eq!(
                documents.admit(&[not_exact], None),
                Err(Refusal::KeyNotExact),
                "{not_exact:?}"
            );
        }
        let any = rules(None, &[]);
        assert_eq!(any.admit(&["anything/at all.png"], None), Ok(()));
        assert_eq!(any.admit(&[""], None), Err(Refusal::KeyNotExact));
    }

    #[test]
    fn an_acl_is_admitted_only_where_the_bucket_lists_it() {
        let assets = rules(None, &[CannedAcl::PublicRead]);
        assert_eq!(
            assets.admit(&["a.png"], Some(CannedAcl::PublicRead)),
            Ok(())
        );
        assert_eq!(
            assets.admit(&["a.png"], Some(CannedAcl::Private)),
            Err(Refusal::AclNotAdmitted)
        );
        assert_eq!(assets.admit(&["a.png"], None), Ok(()));
        assert_eq!(
            rules(None, &[]).admit(&["a.png"], Some(CannedAcl::PublicRead)),
            Err(Refusal::AclNotAdmitted)
        );
    }

    #[test]
    fn rules_are_validated_before_they_are_stored() {
        assert!(rules(None, &[]).validate().is_ok());
        assert!(rules(Some(&["leads/", "a"]), &[]).validate().is_ok());
        assert!(rules(Some(&[]), &[]).validate().is_err());
        for bad in ["", "/leads", "leads//", "a/../b", "x\ny"] {
            assert!(rules(Some(&[bad]), &[]).validate().is_err(), "{bad:?}");
        }
        let many: Vec<String> = (0..=MAX_KEY_PREFIXES).map(|i| format!("p{i}/")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert!(rules(Some(&many), &[]).validate().is_err());
        let wire: Rules =
            serde_json::from_value(serde_json::json!({"acls": ["public-read"]})).expect("rules");
        assert_eq!(wire, rules(None, &[CannedAcl::PublicRead]));
        assert!(serde_json::from_value::<Rules>(
            serde_json::json!({"acls": ["public-read-write"]})
        )
        .is_err());
        assert!(serde_json::from_value::<Rules>(serde_json::json!({"prefixes": []})).is_err());
    }

    #[test]
    fn bucket_names_are_those_a_store_accepts() {
        for good in ["example-documents", "a.b_c", "abc", &"a".repeat(63)] {
            assert!(validate_bucket_name(good).is_ok(), "{good}");
        }
        for bad in [
            "ab",
            "Example",
            "-abc",
            "abc-",
            "a/b",
            "a b",
            &"a".repeat(64),
            "",
        ] {
            assert!(validate_bucket_name(bad).is_err(), "{bad}");
        }
    }
}
