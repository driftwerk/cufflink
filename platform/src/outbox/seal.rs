use super::route::PendingRow;
use super::writer::{OutboxKind, OutboxRow};
use crate::policy::PolicyMode;
use crate::secrets::{KeyRing, MacKeys};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const MAC_HEADER: &str = "Cufflink-Outbox-Mac";
const LABEL: &str = "cufflink outbox row v1";

pub const NO_KEY: &str = "the platform has no key to authenticate the audit and webhook rows it \
     writes to an outbox (CUFFLINK_SECRETS_KEK or CUFFLINK_CACHE_SIGNING_KEY)";

pub fn require_key(mode: PolicyMode, seal: Option<&OutboxSeal>) -> Result<(), String> {
    match (mode, seal) {
        (PolicyMode::Enforce, None) => Err(format!("outbox: enforce needs a key: {NO_KEY}")),
        _ => Ok(()),
    }
}

pub fn is_sealed(kind: OutboxKind) -> bool {
    matches!(kind, OutboxKind::Audit | OutboxKind::Webhook)
}

fn is_mac_header(name: &str) -> bool {
    name.eq_ignore_ascii_case(MAC_HEADER)
}

fn content(
    tenant_id: Uuid,
    kind: OutboxKind,
    service: &str,
    subject: &str,
    msg_id: Uuid,
    payload: &[u8],
) -> Vec<u8> {
    let texts = [kind.as_str(), service, subject];
    texts
        .iter()
        .flat_map(|text| {
            (text.len() as u64)
                .to_be_bytes()
                .into_iter()
                .chain(text.bytes())
        })
        .chain(tenant_id.into_bytes())
        .chain(msg_id.into_bytes())
        .chain(Sha256::digest(payload))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unauthenticated {
    NoKey,
    Missing,
    Invalid,
}

impl std::fmt::Display for Unauthenticated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Unauthenticated::NoKey => NO_KEY,
            Unauthenticated::Missing => "the row carries no MAC: the platform did not write it",
            Unauthenticated::Invalid => {
                "the row's MAC does not match what it holds: the platform did not write it as it is"
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct OutboxSeal {
    keys: MacKeys,
}

impl OutboxSeal {
    pub fn of(ring: &KeyRing, cache_signing_key: Option<&[u8]>) -> Option<Self> {
        MacKeys::derive(LABEL, ring, cache_signing_key).map(|keys| Self { keys })
    }

    pub fn mac_header(&self, tenant_id: Uuid, row: &OutboxRow) -> Option<(String, String)> {
        is_sealed(row.kind).then(|| {
            let mac = self.keys.sign(&content(
                tenant_id,
                row.kind,
                &row.service,
                &row.subject,
                row.msg_id,
                &row.payload,
            ));
            (MAC_HEADER.to_string(), hex::encode(mac))
        })
    }

    pub fn headers(&self, tenant_id: Uuid, row: &OutboxRow) -> Vec<(String, String)> {
        row.headers
            .iter()
            .filter(|(name, _)| !is_mac_header(name))
            .cloned()
            .chain(self.mac_header(tenant_id, row))
            .collect()
    }

    pub fn seal(&self, tenant_id: Uuid, row: OutboxRow) -> OutboxRow {
        OutboxRow {
            headers: self.headers(tenant_id, &row),
            ..row
        }
    }

    fn verify(&self, tenant_id: Uuid, row: &PendingRow) -> Result<(), Unauthenticated> {
        let message = content(
            tenant_id,
            row.kind,
            &row.service,
            &row.subject,
            row.msg_id,
            &row.payload,
        );
        let macs: Vec<&str> = row
            .headers
            .iter()
            .filter(|(name, _)| is_mac_header(name))
            .map(|(_, value)| value.as_str())
            .collect();
        match macs.as_slice() {
            [] => Err(Unauthenticated::Missing),
            [mac] if hex::decode(mac).is_ok_and(|mac| self.keys.verify(&message, &mac)) => Ok(()),
            _ => Err(Unauthenticated::Invalid),
        }
    }
}

pub fn authenticate(
    seal: Option<&OutboxSeal>,
    tenant_id: Uuid,
    row: &PendingRow,
) -> Result<(), Unauthenticated> {
    match (is_sealed(row.kind), seal) {
        (false, _) => Ok(()),
        (true, None) => Err(Unauthenticated::NoKey),
        (true, Some(seal)) => seal.verify(tenant_id, row),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::outbox::route::tests::row as pending;

    pub(crate) fn seal() -> OutboxSeal {
        OutboxSeal::of(&KeyRing::default(), Some(b"a key of sixteen bytes")).expect("a seal")
    }

    pub(crate) fn tenant() -> Uuid {
        Uuid::from_u128(42)
    }

    pub(crate) fn sealed(row: PendingRow) -> PendingRow {
        written(&seal(), row)
    }

    fn written(seal: &OutboxSeal, row: PendingRow) -> PendingRow {
        let sealed = seal.seal(
            tenant(),
            OutboxRow {
                kind: row.kind,
                service: row.service.clone(),
                subject: row.subject.clone(),
                payload: row.payload.clone(),
                headers: row.headers.clone(),
                msg_id: row.msg_id,
            },
        );
        PendingRow {
            headers: sealed.headers,
            ..row
        }
    }

    #[test]
    fn audit_and_webhook_rows_are_sealed_and_events_are_not() {
        for kind in [OutboxKind::Event, OutboxKind::Audit, OutboxKind::Webhook] {
            let row = written(&seal(), pending(1, kind, "cufflink.audit.harbor.loans"));
            let macs = row.headers.iter().filter(|(name, _)| is_mac_header(name));
            assert_eq!(macs.count(), usize::from(is_sealed(kind)), "{kind:?}");
            assert_eq!(row.headers[0], ("traceparent".into(), "00-1-2-01".into()));
            assert_eq!(authenticate(Some(&seal()), tenant(), &row), Ok(()));
        }
    }

    #[test]
    fn a_sealed_row_holds_only_as_it_was_written_and_for_its_tenant() {
        let seal = seal();
        let row = written(&seal, pending(1, OutboxKind::Audit, "dw.harbor.logs"));
        let check = |row: &PendingRow| authenticate(Some(&seal), tenant(), row);
        assert_eq!(check(&row), Ok(()));
        for altered in [
            PendingRow {
                payload: b"{\"n\":2}".to_vec(),
                ..row.clone()
            },
            PendingRow {
                subject: "cufflink.audit.harbor.loans".into(),
                ..row.clone()
            },
            PendingRow {
                service: "books".into(),
                ..row.clone()
            },
            PendingRow {
                kind: OutboxKind::Webhook,
                ..row.clone()
            },
            PendingRow {
                msg_id: Uuid::from_u128(99),
                ..row.clone()
            },
        ] {
            assert_eq!(check(&altered), Err(Unauthenticated::Invalid));
        }
        assert_eq!(
            authenticate(Some(&seal), Uuid::from_u128(7), &row),
            Err(Unauthenticated::Invalid)
        );
        let mac = |value: &str| (MAC_HEADER.to_ascii_lowercase(), value.to_string());
        let with = |headers: Vec<(String, String)>| PendingRow {
            headers,
            ..row.clone()
        };
        let forged = pending(1, OutboxKind::Audit, "dw.harbor.logs");
        assert_eq!(check(&forged), Err(Unauthenticated::Missing));
        for headers in [
            vec![mac("zz")],
            vec![mac(&"0".repeat(64))],
            [row.headers.clone(), vec![mac(&"0".repeat(64))]].concat(),
        ] {
            assert_eq!(check(&with(headers)), Err(Unauthenticated::Invalid));
        }
    }

    #[test]
    fn a_row_is_sealed_once_whatever_its_headers_held() {
        let seal = seal();
        let row = PendingRow {
            headers: vec![(MAC_HEADER.into(), "forged".into())],
            ..pending(1, OutboxKind::Webhook, "cufflink.webhook.harbor.1")
        };
        let sealed = written(&seal, row);
        assert_eq!(sealed.headers.len(), 1);
        assert_eq!(authenticate(Some(&seal), tenant(), &sealed), Ok(()));
    }

    #[test]
    fn an_outbox_is_enforced_only_with_a_key() {
        let seal = seal();
        for mode in [PolicyMode::Off, PolicyMode::Warn] {
            assert_eq!(require_key(mode, None), Ok(()));
        }
        assert_eq!(require_key(PolicyMode::Enforce, Some(&seal)), Ok(()));
        let refused = require_key(PolicyMode::Enforce, None).expect_err("no key");
        assert!(
            refused.starts_with("outbox: enforce needs a key")
                && refused.contains("CUFFLINK_SECRETS_KEK")
                && refused.contains("CUFFLINK_CACHE_SIGNING_KEY"),
            "{refused}"
        );
    }

    #[test]
    fn without_a_key_a_sealed_kind_is_never_believed() {
        let event = pending(1, OutboxKind::Event, "dw.harbor.loans.loans.created");
        assert_eq!(authenticate(None, tenant(), &event), Ok(()));
        let audit = written(&seal(), pending(1, OutboxKind::Audit, "dw.harbor.logs"));
        assert_eq!(
            authenticate(None, tenant(), &audit),
            Err(Unauthenticated::NoKey)
        );
        assert!(OutboxSeal::of(&KeyRing::default(), None).is_none());
    }

    #[test]
    fn a_row_sealed_before_a_rotation_holds_while_its_key_is_given() {
        let ring = |current: &str, previous: Option<&str>| {
            let key = |byte: u8| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode([byte; 32])
            };
            let (a, b) = (key(1), key(2));
            let named = |id: &str| if id == "a" { a.clone() } else { b.clone() };
            KeyRing::parse(
                Some(&named(current)),
                Some(current),
                previous.map(|id| format!("{id}:{}", named(id))).as_deref(),
            )
            .expect("ring")
        };
        let before = OutboxSeal::of(&ring("a", None), None).expect("seal");
        let during = OutboxSeal::of(&ring("b", Some("a")), None).expect("seal");
        let after = OutboxSeal::of(&ring("b", None), None).expect("seal");
        let row = || pending(1, OutboxKind::Audit, "dw.harbor.logs");
        let old = written(&before, row());
        assert_eq!(authenticate(Some(&during), tenant(), &old), Ok(()));
        assert_eq!(
            authenticate(Some(&after), tenant(), &old),
            Err(Unauthenticated::Invalid)
        );
        let new = written(&during, row());
        assert_eq!(authenticate(Some(&after), tenant(), &new), Ok(()));
        assert_eq!(
            authenticate(Some(&before), tenant(), &new),
            Err(Unauthenticated::Invalid)
        );
    }
}
