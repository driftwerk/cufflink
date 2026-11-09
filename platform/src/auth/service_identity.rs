use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

pub const HEADER: &str = "x-cufflink-service-identity";
pub const MAX_CALL_DEPTH: u8 = 4;
pub const MAX_LIFETIME_SECS: i64 = 60;
const VERSION: &str = "cfsi1";
pub(crate) const CLOCK_SKEW_SECS: i64 = 5;
pub(crate) const KEYS_ENV: &str = "CUFFLINK_SERVICE_IDENTITY_KEYS";

pub struct ServiceIdentityKeys {
    active: (String, [u8; 32]),
    verify: HashMap<String, [u8; 32]>,
    shared: bool,
}

impl ServiceIdentityKeys {
    pub fn from_setting(setting: Option<&str>) -> eyre::Result<Self> {
        match setting.map(str::trim).filter(|s| !s.is_empty()) {
            Some(setting) => Self::parse(setting),
            None => {
                tracing::warn!(
                    "{KEYS_ENV} is unset: service identities are verifiable only by the process that minted them"
                );
                Ok(Self::ephemeral())
            }
        }
    }

    pub fn ephemeral() -> Self {
        let key: [u8; 32] = Sha256::digest(
            [Uuid::new_v4(), Uuid::new_v4()]
                .iter()
                .flat_map(|u| u.into_bytes())
                .collect::<Vec<u8>>(),
        )
        .into();
        let kid = "local".to_string();
        Self {
            active: (kid.clone(), key),
            verify: HashMap::from([(kid, key)]),
            shared: false,
        }
    }

    fn parse(setting: &str) -> eyre::Result<Self> {
        let keys = setting
            .split(',')
            .zip(1..)
            .map(|(entry, position): (&str, usize)| {
                let refused = |why: &str| eyre::eyre!("{KEYS_ENV}: entry {position} {why}");
                let (kid, hex_key) = entry
                    .trim()
                    .split_once(':')
                    .ok_or_else(|| refused("is not kid:hex"))?;
                let valid_kid = !kid.is_empty()
                    && kid
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
                if !valid_kid {
                    return Err(refused(
                        "has a key id that is not letters, digits, '-' or '_'",
                    ));
                }
                let key: [u8; 32] = hex::decode(hex_key)
                    .ok()
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| refused("has a key that is not 64 hex characters"))?;
                Ok((kid.to_string(), key))
            })
            .collect::<eyre::Result<Vec<_>>>()?;
        let active = keys
            .first()
            .cloned()
            .ok_or_else(|| eyre::eyre!("{KEYS_ENV} names no key"))?;
        let verify: HashMap<_, _> = keys.iter().cloned().collect();
        if verify.len() != keys.len() {
            return Err(eyre::eyre!("{KEYS_ENV} names a key id twice"));
        }
        Ok(Self {
            active,
            verify,
            shared: true,
        })
    }

    pub fn is_shared(&self) -> bool {
        self.shared
    }

    fn hmac_key(key: &[u8; 32]) -> hmac::Key {
        hmac::Key::new(hmac::HMAC_SHA256, key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    #[serde(rename = "tid")]
    pub tenant_id: Uuid,
    #[serde(rename = "cs")]
    pub caller_service: String,
    #[serde(rename = "csid")]
    pub caller_service_id: Uuid,
    #[serde(rename = "cv")]
    pub caller_version: i32,
    #[serde(rename = "ts")]
    pub target_service: String,
    #[serde(rename = "m")]
    pub method: String,
    #[serde(rename = "p")]
    pub path: String,
    #[serde(rename = "iat")]
    pub issued_at: i64,
    #[serde(rename = "exp")]
    pub expires_at: i64,
    pub jti: Uuid,
    #[serde(rename = "d")]
    pub depth: u8,
    #[serde(rename = "obo")]
    pub on_behalf_of: Option<String>,
    #[serde(rename = "act", default, skip_serializing_if = "Option::is_none")]
    pub acting_for: Option<Delegation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delegation {
    #[serde(rename = "sub")]
    pub subject: String,
    #[serde(rename = "am")]
    pub method: DelegatedMethod,
    #[serde(rename = "nm", default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "em", default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(rename = "sa", default, skip_serializing_if = "std::ops::Not::not")]
    pub service_account: bool,
    #[serde(rename = "rlm", default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    #[serde(rename = "uexp", default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(rename = "tok", default, skip_serializing_if = "Option::is_none")]
    pub token: Option<DelegatedToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "k", rename_all = "snake_case", deny_unknown_fields)]
pub enum DelegatedMethod {
    Jwt {},
    ApiKey {
        id: Uuid,
        #[serde(rename = "us", default, skip_serializing_if = "Option::is_none")]
        user_sub: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegatedToken {
    #[serde(rename = "pu", default, skip_serializing_if = "Option::is_none")]
    pub preferred_username: Option<String>,
    #[serde(rename = "rr", default, skip_serializing_if = "Vec::is_empty")]
    pub realm_roles: Vec<String>,
    #[serde(rename = "cl", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub claims: BTreeMap<String, Value>,
}

impl Claims {
    pub fn remaining_budget(&self, now: i64) -> std::time::Duration {
        std::time::Duration::from_secs(
            u64::try_from(self.expires_at.saturating_sub(now)).unwrap_or(0),
        )
    }
}

pub fn mint(keys: &ServiceIdentityKeys, claims: &Claims) -> serde_json::Result<String> {
    serde_json::to_vec(claims).map(|payload| sign(keys, &payload))
}

fn sign(keys: &ServiceIdentityKeys, payload: &[u8]) -> String {
    let (kid, key) = &keys.active;
    let signed = format!("{VERSION}.{kid}.{}", URL_SAFE_NO_PAD.encode(payload));
    let signature = URL_SAFE_NO_PAD.encode(hmac::sign(
        &ServiceIdentityKeys::hmac_key(key),
        signed.as_bytes(),
    ));
    format!("{signed}.{signature}")
}

#[derive(Debug, Clone, Copy)]
pub struct Expected<'a> {
    pub tenant_id: Uuid,
    pub target_service: &'a str,
    pub method: &'a str,
    pub path: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    #[error("malformed service identity")]
    Format,
    #[error("service identity signed with an unknown key")]
    UnknownKey,
    #[error("service identity signature does not verify")]
    Signature,
    #[error("service identity expired")]
    Expired,
    #[error("service identity issued in the future")]
    NotYetValid,
    #[error("service identity lives longer than allowed")]
    Lifetime,
    #[error("service identity minted for another tenant")]
    Tenant,
    #[error("service identity minted for another service")]
    Service,
    #[error("service identity minted for another method")]
    Method,
    #[error("service identity minted for another path")]
    Path,
    #[error("service call chain too deep")]
    Depth,
}

impl IdentityError {
    pub fn reason(self) -> &'static str {
        match self {
            IdentityError::Format => "format",
            IdentityError::UnknownKey => "unknown_key",
            IdentityError::Signature => "signature",
            IdentityError::Expired => "expired",
            IdentityError::NotYetValid => "not_yet_valid",
            IdentityError::Lifetime => "lifetime",
            IdentityError::Tenant => "tenant",
            IdentityError::Service => "service",
            IdentityError::Method => "method",
            IdentityError::Path => "path",
            IdentityError::Depth => "depth",
        }
    }
}

pub fn verify(
    keys: &ServiceIdentityKeys,
    token: &str,
    expected: &Expected<'_>,
    now: i64,
) -> Result<Claims, IdentityError> {
    let (signed, signature) = token.rsplit_once('.').ok_or(IdentityError::Format)?;
    let mut parts = signed.splitn(3, '.');
    let (version, kid, payload) = match (parts.next(), parts.next(), parts.next()) {
        (Some(version), Some(kid), Some(payload)) => (version, kid, payload),
        _ => return Err(IdentityError::Format),
    };
    if version != VERSION {
        return Err(IdentityError::Format);
    }
    let key = keys.verify.get(kid).ok_or(IdentityError::UnknownKey)?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| IdentityError::Format)?;
    hmac::verify(
        &ServiceIdentityKeys::hmac_key(key),
        signed.as_bytes(),
        &signature,
    )
    .map_err(|_| IdentityError::Signature)?;
    let claims: Claims = URL_SAFE_NO_PAD
        .decode(payload)
        .ok()
        .and_then(|json| serde_json::from_slice(&json).ok())
        .ok_or(IdentityError::Format)?;
    [
        (
            claims.expires_at < now.saturating_sub(CLOCK_SKEW_SECS),
            IdentityError::Expired,
        ),
        (
            claims.issued_at > now.saturating_add(CLOCK_SKEW_SECS),
            IdentityError::NotYetValid,
        ),
        (
            claims
                .expires_at
                .checked_sub(claims.issued_at)
                .is_none_or(|lifetime| {
                    !(0..=MAX_LIFETIME_SECS + CLOCK_SKEW_SECS).contains(&lifetime)
                }),
            IdentityError::Lifetime,
        ),
        (
            claims.tenant_id != expected.tenant_id,
            IdentityError::Tenant,
        ),
        (
            claims.target_service != expected.target_service,
            IdentityError::Service,
        ),
        (
            !claims.method.eq_ignore_ascii_case(expected.method),
            IdentityError::Method,
        ),
        (claims.path != expected.path, IdentityError::Path),
        (claims.depth > MAX_CALL_DEPTH, IdentityError::Depth),
        (
            claims.acting_for.is_some() && claims.depth == 0,
            IdentityError::Format,
        ),
    ]
    .into_iter()
    .find_map(|(failed, error)| failed.then_some(error))
    .map_or(Ok(claims), Err)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000;

    fn claims() -> Claims {
        Claims {
            tenant_id: Uuid::from_u128(1),
            caller_service: "pricing".into(),
            caller_service_id: Uuid::from_u128(2),
            caller_version: 3,
            target_service: "assets".into(),
            method: "POST".into(),
            path: "/_fn/reprice".into(),
            issued_at: NOW,
            expires_at: NOW + 30,
            jti: Uuid::from_u128(4),
            depth: 1,
            on_behalf_of: Some("user-1".into()),
            acting_for: None,
        }
    }

    fn delegated() -> Claims {
        Claims {
            acting_for: Some(Delegation {
                subject: "clara".into(),
                method: DelegatedMethod::Jwt {},
                name: Some("Clara Clerk".into()),
                email: Some("clara@example.com".into()),
                service_account: false,
                realm: Some("acme".into()),
                expires_at: Some(NOW + 300),
                token: Some(DelegatedToken {
                    preferred_username: Some("clara".into()),
                    realm_roles: vec!["loans-officer".into()],
                    claims: BTreeMap::from([
                        ("azp".to_string(), Value::from("portal")),
                        ("exp".to_string(), Value::from(NOW + 300)),
                    ]),
                }),
            }),
            ..claims()
        }
    }

    fn expected() -> Expected<'static> {
        Expected {
            tenant_id: Uuid::from_u128(1),
            target_service: "assets",
            method: "POST",
            path: "/_fn/reprice",
        }
    }

    fn keys(setting: &str) -> ServiceIdentityKeys {
        ServiceIdentityKeys::from_setting(Some(setting)).expect("keys")
    }

    const K1: &str = "k1:0000000000000000000000000000000000000000000000000000000000000001";
    const K2: &str = "k2:0000000000000000000000000000000000000000000000000000000000000002";

    #[test]
    fn a_token_is_signed_as_every_release_signs_it() {
        assert_eq!(
            mint(&keys(K1), &claims()).expect("mint"),
            "cfsi1.k1.eyJ0aWQiOiIwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMDEiLCJjcyI6InByaWNpbmciLCJjc2lkIjoiMDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAyIiwiY3YiOjMsInRzIjoiYXNzZXRzIiwibSI6IlBPU1QiLCJwIjoiL19mbi9yZXByaWNlIiwiaWF0IjoxODAwMDAwMDAwLCJleHAiOjE4MDAwMDAwMzAsImp0aSI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwNCIsImQiOjEsIm9ibyI6InVzZXItMSJ9.D7mytIa6X3dxLHir-Cu3bWhftxtyKqPViN5Oz08aNm8"
        );
    }

    #[test]
    fn a_minted_token_verifies_for_its_exact_call() {
        let keys = keys(K1);
        let token = mint(&keys, &claims()).expect("mint");
        assert!(token.starts_with("cfsi1.k1."), "{token}");
        assert_eq!(verify(&keys, &token, &expected(), NOW + 10), Ok(claims()));
        assert_eq!(
            verify(
                &keys,
                &token,
                &Expected {
                    method: "post",
                    ..expected()
                },
                NOW
            ),
            Ok(claims())
        );
    }

    #[test]
    fn each_mismatch_is_its_own_rejection() {
        let keys = keys(K1);
        let token = mint(&keys, &claims()).expect("mint");
        let rejected = |expected: Expected<'_>, now: i64| verify(&keys, &token, &expected, now);
        assert_eq!(rejected(expected(), NOW + 36), Err(IdentityError::Expired));
        assert!(rejected(expected(), NOW + 35).is_ok(), "within the skew");
        assert_eq!(
            rejected(
                Expected {
                    tenant_id: Uuid::from_u128(9),
                    ..expected()
                },
                NOW
            ),
            Err(IdentityError::Tenant)
        );
        assert_eq!(
            rejected(
                Expected {
                    target_service: "billing",
                    ..expected()
                },
                NOW
            ),
            Err(IdentityError::Service)
        );
        assert_eq!(
            rejected(
                Expected {
                    method: "DELETE",
                    ..expected()
                },
                NOW
            ),
            Err(IdentityError::Method)
        );
        assert_eq!(
            rejected(
                Expected {
                    path: "/_fn/other",
                    ..expected()
                },
                NOW
            ),
            Err(IdentityError::Path)
        );
        let long = mint(
            &keys,
            &Claims {
                expires_at: NOW + 66,
                ..claims()
            },
        )
        .expect("mint");
        assert_eq!(
            verify(&keys, &long, &expected(), NOW),
            Err(IdentityError::Lifetime)
        );
        let early = mint(
            &keys,
            &Claims {
                issued_at: NOW + 6,
                expires_at: NOW + 30,
                ..claims()
            },
        )
        .expect("mint");
        assert_eq!(
            verify(&keys, &early, &expected(), NOW),
            Err(IdentityError::NotYetValid)
        );
        assert!(
            verify(&keys, &early, &expected(), NOW + 1).is_ok(),
            "within the skew"
        );
        for (issued_at, expires_at) in [(NOW, NOW - 1), (i64::MIN, NOW), (i64::MAX, NOW)] {
            let malformed = mint(
                &keys,
                &Claims {
                    issued_at,
                    expires_at,
                    ..claims()
                },
            )
            .expect("mint");
            assert!(
                matches!(
                    verify(&keys, &malformed, &expected(), NOW),
                    Err(IdentityError::Lifetime | IdentityError::NotYetValid)
                ),
                "{issued_at} {expires_at}"
            );
        }
        let deep = mint(
            &keys,
            &Claims {
                depth: MAX_CALL_DEPTH + 1,
                ..claims()
            },
        )
        .expect("mint");
        assert_eq!(
            verify(&keys, &deep, &expected(), NOW),
            Err(IdentityError::Depth)
        );
    }

    #[test]
    fn tampering_and_foreign_keys_are_refused() {
        let keys = keys(K1);
        let token = mint(&keys, &claims()).expect("mint");
        let (signed, signature) = token.rsplit_once('.').expect("signature");
        let (prefix, _) = signed.rsplit_once('.').expect("payload");
        let forged_payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&Claims {
                caller_service: "admin".into(),
                ..claims()
            })
            .expect("json"),
        );
        let forged = format!("{prefix}.{forged_payload}.{signature}");
        assert_eq!(
            verify(&keys, &forged, &expected(), NOW),
            Err(IdentityError::Signature)
        );
        let other = mint(&self::keys(K2), &claims()).expect("mint");
        assert_eq!(
            verify(&keys, &other, &expected(), NOW),
            Err(IdentityError::UnknownKey)
        );
        let foreign_same_kid = mint(
            &self::keys("k1:00000000000000000000000000000000000000000000000000000000000000ff"),
            &claims(),
        )
        .expect("mint");
        assert_eq!(
            verify(&keys, &foreign_same_kid, &expected(), NOW),
            Err(IdentityError::Signature)
        );
        for malformed in [
            "",
            "cfsi1",
            "cfsi1.k1.x",
            "cfsi2.k1.e30.AAAA",
            &format!("{signed}.!!"),
            &token.replacen("cfsi1.k1.", "cfsi1.k1.x", 1),
        ] {
            assert!(
                matches!(
                    verify(&keys, malformed, &expected(), NOW),
                    Err(IdentityError::Format | IdentityError::Signature)
                ),
                "{malformed:?}"
            );
        }
    }

    #[test]
    fn keys_rotate_old_verifies_new_signs() {
        let old = keys(K1);
        let rotated = keys(&format!("{K2},{K1}"));
        let before = mint(&old, &claims()).expect("mint");
        assert!(verify(&rotated, &before, &expected(), NOW).is_ok());
        let after = mint(&rotated, &claims()).expect("mint");
        assert!(after.starts_with("cfsi1.k2."), "{after}");
        assert_eq!(
            verify(&old, &after, &expected(), NOW),
            Err(IdentityError::UnknownKey)
        );
        assert!(rotated.is_shared());
    }

    #[test]
    fn configuration_is_validated_and_unset_is_per_process() {
        for bad in [
            "k1",
            "k1:abc",
            ":00",
            "k 1:0000000000000000000000000000000000000000000000000000000000000001",
            &format!("{K1},{K1}"),
        ] {
            assert!(
                ServiceIdentityKeys::from_setting(Some(bad)).is_err(),
                "{bad}"
            );
        }
        let local = ServiceIdentityKeys::from_setting(None).expect("ephemeral");
        assert!(!local.is_shared());
        let token = mint(&local, &claims()).expect("mint");
        assert!(verify(&local, &token, &expected(), NOW).is_ok());
        assert_eq!(
            verify(&ServiceIdentityKeys::ephemeral(), &token, &expected(), NOW),
            Err(IdentityError::Signature)
        );
    }

    #[test]
    fn a_refused_setting_names_the_entry_and_never_what_it_holds() {
        const KEY: &str = "00000000000000000000000000000000000000000000000000000000000000a1";
        let refusal = |setting: &str| {
            ServiceIdentityKeys::from_setting(Some(setting))
                .err()
                .map(|error| format!("{error:#}"))
                .expect("refused")
        };
        for (setting, refused) in [
            (KEY.to_string(), "entry 1 is not kid:hex"),
            (format!("{K1},{KEY}"), "entry 2 is not kid:hex"),
            (
                format!("{KEY}:k1"),
                "entry 1 has a key that is not 64 hex characters",
            ),
            (
                format!("{K1},k2:{KEY}00"),
                "entry 2 has a key that is not 64 hex characters",
            ),
            (
                format!("k 1/{KEY}:{KEY}"),
                "entry 1 has a key id that is not letters, digits, '-' or '_'",
            ),
        ] {
            assert_eq!(
                refusal(&setting),
                format!("CUFFLINK_SERVICE_IDENTITY_KEYS: {refused}")
            );
        }
    }

    #[test]
    fn a_delegated_token_is_signed_and_verifies_for_its_exact_call() {
        let keys = keys(K1);
        let token = mint(&keys, &delegated()).expect("mint");
        assert_eq!(verify(&keys, &token, &expected(), NOW), Ok(delegated()));
        assert_eq!(
            verify(
                &keys,
                &token,
                &Expected {
                    path: "/_fn/other",
                    ..expected()
                },
                NOW
            ),
            Err(IdentityError::Path)
        );
        let payload = |token: &str| {
            token
                .split('.')
                .nth(2)
                .and_then(|payload| URL_SAFE_NO_PAD.decode(payload).ok())
                .and_then(|json| serde_json::from_slice::<Value>(&json).ok())
                .expect("payload")
        };
        assert_eq!(
            payload(&token)["act"],
            serde_json::json!({
                "sub": "clara",
                "am": {"k": "jwt"},
                "nm": "Clara Clerk",
                "em": "clara@example.com",
                "rlm": "acme",
                "uexp": NOW + 300,
                "tok": {"pu": "clara", "rr": ["loans-officer"], "cl": {"azp": "portal", "exp": NOW + 300}},
            })
        );
        assert!(payload(&mint(&keys, &claims()).expect("mint"))
            .get("act")
            .is_none());
    }

    #[test]
    fn a_delegated_api_key_keeps_the_user_it_was_minted_for() {
        let keys = keys(K1);
        let key = Claims {
            acting_for: delegated().acting_for.map(|delegation| Delegation {
                method: DelegatedMethod::ApiKey {
                    id: Uuid::from_u128(9),
                    user_sub: Some("clara".into()),
                },
                token: None,
                expires_at: None,
                ..delegation
            }),
            ..claims()
        };
        let token = mint(&keys, &key).expect("mint");
        assert_eq!(verify(&keys, &token, &expected(), NOW), Ok(key));
    }

    #[test]
    fn an_unknown_field_in_a_delegation_is_refused_rather_than_ignored() {
        let with = |keys: &ServiceIdentityKeys, claims: Claims, pointer: &str| {
            let mut payload = serde_json::to_value(claims).expect("json");
            payload
                .pointer_mut(pointer)
                .and_then(Value::as_object_mut)
                .expect("an object")
                .insert("narrows".into(), Value::from("only-reads"));
            verify(
                keys,
                &sign(keys, payload.to_string().as_bytes()),
                &expected(),
                NOW,
            )
        };
        let keys = keys(K1);
        for pointer in ["/act", "/act/am", "/act/tok"] {
            assert_eq!(
                with(&keys, delegated(), pointer),
                Err(IdentityError::Format),
                "{pointer}"
            );
        }
        let key = Claims {
            acting_for: delegated().acting_for.map(|delegation| Delegation {
                method: DelegatedMethod::ApiKey {
                    id: Uuid::from_u128(9),
                    user_sub: Some("clara".into()),
                },
                ..delegation
            }),
            ..claims()
        };
        assert_eq!(
            with(&keys, key, "/act/am"),
            Err(IdentityError::Format),
            "an API key's method too"
        );
        assert_eq!(
            with(&keys, delegated(), ""),
            Ok(delegated()),
            "the identity's own claims still admit fields a later release adds"
        );
    }

    #[test]
    fn a_delegation_at_depth_zero_is_malformed() {
        let keys = keys(K1);
        let at_zero = |claims: Claims| {
            let token = mint(&keys, &Claims { depth: 0, ..claims }).expect("mint");
            verify(&keys, &token, &expected(), NOW)
        };
        assert_eq!(at_zero(delegated()), Err(IdentityError::Format));
        assert!(
            at_zero(claims()).is_ok(),
            "a service-only identity at depth 0"
        );
    }

    #[test]
    fn a_delegation_swapped_under_the_signature_is_refused() {
        let keys = keys(K1);
        let token = mint(&keys, &delegated()).expect("mint");
        let (signed, signature) = token.rsplit_once('.').expect("signature");
        let (prefix, _) = signed.rsplit_once('.').expect("payload");
        let resigned = |claims: &Claims| {
            let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("json"));
            format!("{prefix}.{payload}.{signature}")
        };
        let swapped = Claims {
            acting_for: delegated().acting_for.map(|delegation| Delegation {
                subject: "dana".into(),
                ..delegation
            }),
            ..delegated()
        };
        let extended = Claims {
            acting_for: delegated().acting_for.map(|delegation| Delegation {
                expires_at: Some(NOW + 86_400),
                ..delegation
            }),
            ..delegated()
        };
        for forged in [&swapped, &extended, &claims()] {
            assert_eq!(
                verify(&keys, &resigned(forged), &expected(), NOW),
                Err(IdentityError::Signature)
            );
        }
        let service_only = mint(&keys, &claims()).expect("mint");
        let (signed, signature) = service_only.rsplit_once('.').expect("signature");
        let (prefix, _) = signed.rsplit_once('.').expect("payload");
        let grafted = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&delegated()).expect("json"));
        assert_eq!(
            verify(
                &keys,
                &format!("{prefix}.{grafted}.{signature}"),
                &expected(),
                NOW
            ),
            Err(IdentityError::Signature),
            "a delegation grafted onto a service-only token's signature"
        );
    }

    #[test]
    fn a_verifier_without_the_act_field_reads_a_delegated_token_as_service_only() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct ReleasedClaims {
            #[serde(rename = "tid")]
            tenant_id: Uuid,
            #[serde(rename = "cs")]
            caller_service: String,
            #[serde(rename = "csid")]
            caller_service_id: Uuid,
            #[serde(rename = "cv")]
            caller_version: i32,
            #[serde(rename = "ts")]
            target_service: String,
            #[serde(rename = "m")]
            method: String,
            #[serde(rename = "p")]
            path: String,
            #[serde(rename = "iat")]
            issued_at: i64,
            #[serde(rename = "exp")]
            expires_at: i64,
            jti: Uuid,
            #[serde(rename = "d")]
            depth: u8,
            #[serde(rename = "obo")]
            on_behalf_of: Option<String>,
        }
        let token = mint(&keys(K1), &delegated()).expect("mint");
        let read: ReleasedClaims = token
            .split('.')
            .nth(2)
            .and_then(|payload| URL_SAFE_NO_PAD.decode(payload).ok())
            .and_then(|json| serde_json::from_slice(&json).ok())
            .expect("an older verifier reads the claims it knows");
        let claims = claims();
        assert_eq!(
            read,
            ReleasedClaims {
                tenant_id: claims.tenant_id,
                caller_service: claims.caller_service,
                caller_service_id: claims.caller_service_id,
                caller_version: claims.caller_version,
                target_service: claims.target_service,
                method: claims.method,
                path: claims.path,
                issued_at: claims.issued_at,
                expires_at: claims.expires_at,
                jti: claims.jti,
                depth: claims.depth,
                on_behalf_of: claims.on_behalf_of,
            }
        );
    }

    #[test]
    fn rejection_reasons_are_stable_labels() {
        assert_eq!(IdentityError::UnknownKey.reason(), "unknown_key");
        assert_eq!(IdentityError::Depth.reason(), "depth");
        assert_eq!(IdentityError::NotYetValid.reason(), "not_yet_valid");
    }

    #[test]
    fn a_callee_runs_no_longer_than_the_token_lives() {
        let claims = claims();
        assert_eq!(
            claims.remaining_budget(NOW + 10),
            std::time::Duration::from_secs(20)
        );
        assert_eq!(claims.remaining_budget(NOW + 31), std::time::Duration::ZERO);
        assert_eq!(
            claims.remaining_budget(i64::MIN),
            std::time::Duration::from_secs(u64::try_from(i64::MAX).expect("positive"))
        );
    }
}
