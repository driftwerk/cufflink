use crate::registry::ServiceIdentity;
use cufflink_types::nats_names::CONSUMER_NAME_MAX;
use cufflink_types::{ConsumerNaming, SubscriptionDefinition};
use sha2::{Digest, Sha256};

const HASH_LEN: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DurableName {
    Legacy(String),
    Scoped(String),
    Named(String),
}

impl DurableName {
    pub fn as_str(&self) -> &str {
        match self {
            DurableName::Legacy(name) | DurableName::Scoped(name) | DurableName::Named(name) => {
                name
            }
        }
    }

    pub fn is_bound(&self) -> bool {
        matches!(self, DurableName::Named(_))
    }

    pub fn is_legacy(&self) -> bool {
        matches!(self, DurableName::Legacy(_))
    }
}

impl std::fmt::Display for DurableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn durable_name(
    identity: &ServiceIdentity,
    sub: &SubscriptionDefinition,
    naming: ConsumerNaming,
    stream: &str,
    resolved_subject: &str,
) -> DurableName {
    let declared = sub
        .consumer
        .as_ref()
        .and_then(|consumer| consumer.durable_name.clone());
    match (declared, naming) {
        (Some(name), _) => DurableName::Named(name),
        (None, ConsumerNaming::Legacy) => {
            DurableName::Legacy(legacy(&identity.service_name, &sub.handler))
        }
        (None, ConsumerNaming::Scoped) => DurableName::Scoped(scoped(
            &[&identity.tenant_slug, &identity.service_name, &sub.handler],
            &[stream, resolved_subject],
        )),
    }
}

pub fn legacy(service_name: &str, handler: &str) -> String {
    format!(
        "{}_{}",
        service_name.replace('-', "_"),
        handler.replace('-', "_")
    )
}

pub fn token(part: &str) -> String {
    part.chars()
        .map(|c| match c {
            c if c.is_ascii_alphanumeric() || c == '_' || c == '-' => c,
            _ => '_',
        })
        .collect()
}

pub(super) fn scoped(parts: &[&str], consumed: &[&str]) -> String {
    let digest = parts
        .iter()
        .chain(consumed)
        .fold(Sha256::new(), |hash, part| {
            hash.chain_update(part.as_bytes()).chain_update(b"\n")
        })
        .finalize();
    let hash = &hex::encode(digest)[..HASH_LEN];
    let readable = parts
        .iter()
        .map(|part| token(part))
        .collect::<Vec<_>>()
        .join("_");
    let kept: String = readable
        .chars()
        .take(CONSUMER_NAME_MAX - HASH_LEN - 1)
        .collect();
    format!("{kept}_{hash}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_types::nats_names::is_valid_consumer_name;
    use cufflink_types::{ConsumerSpec, HandlerType};
    use uuid::Uuid;

    fn identity(tenant: &str, service: &str) -> ServiceIdentity {
        ServiceIdentity {
            tenant_id: Uuid::nil(),
            tenant_slug: tenant.into(),
            service_id: Uuid::nil(),
            service_name: service.into(),
            schema: service.replace('-', "_"),
        }
    }

    fn sub(handler: &str) -> SubscriptionDefinition {
        SubscriptionDefinition::new("dw.{tenant}.stocks.created", handler, HandlerType::Wasm)
    }

    fn named(durable: &str) -> SubscriptionDefinition {
        SubscriptionDefinition {
            consumer: Some(ConsumerSpec {
                durable_name: Some(durable.into()),
                ..Default::default()
            }),
            ..sub("on_book_event")
        }
    }

    fn scoped_name(tenant: &str, service: &str, handler: &str, subject: &str) -> String {
        durable_name(
            &identity(tenant, service),
            &sub(handler),
            ConsumerNaming::Scoped,
            "DW_DEFAULT",
            subject,
        )
        .as_str()
        .to_string()
    }

    #[test]
    fn a_legacy_name_is_the_one_every_durable_had() {
        for (service, handler, name) in [
            ("stock-service", "on_created", "stock_service_on_created"),
            ("stock-service", "on-created", "stock_service_on_created"),
            (
                "reports",
                "handle_tick_resync",
                "reports_handle_tick_resync",
            ),
            ("Blog_Service", "OnPost", "Blog_Service_OnPost"),
        ] {
            assert_eq!(
                durable_name(
                    &identity("default", service),
                    &sub(handler),
                    ConsumerNaming::Legacy,
                    "DW_DEFAULT",
                    "dw.default.stocks.created",
                ),
                DurableName::Legacy(name.into())
            );
        }
    }

    #[test]
    fn a_scoped_name_reads_as_what_it_is_for() {
        let name = scoped_name(
            "default",
            "stock-service",
            "on_created",
            "dw.default.stocks.created",
        );
        let (readable, hash) = name.rsplit_once('_').expect("a hash");
        assert_eq!(readable, "default_stock-service_on_created");
        assert_eq!(hash.len(), 8);
        assert!(hash.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(
            name,
            scoped_name(
                "default",
                "stock-service",
                "on_created",
                "dw.default.stocks.created"
            ),
            "the same wherever and whenever it is computed"
        );
    }

    #[test]
    fn scoped_names_differ_whenever_what_they_name_does() {
        let names = [
            scoped_name("default", "stock-service", "on_created", "dw.default.a"),
            scoped_name("default", "stock-service", "on_created", "dw.default.b"),
            scoped_name(
                "preview-pr-1",
                "stock-service",
                "on_created",
                "dw.default.a",
            ),
            scoped_name("default", "stock_service", "on_created", "dw.default.a"),
            scoped_name("default", "stock", "service_on_created", "dw.default.a"),
            scoped_name("default", "stock-service", "on", "dw.default.a"),
        ];
        let distinct: std::collections::HashSet<&String> = names.iter().collect();
        assert_eq!(distinct.len(), names.len(), "{names:?}");
        let on_other_stream = durable_name(
            &identity("default", "stock-service"),
            &sub("on_created"),
            ConsumerNaming::Scoped,
            "CUFFLINK_TICKS",
            "dw.default.a",
        );
        assert_ne!(on_other_stream.as_str(), names[0]);
    }

    #[test]
    fn a_scoped_name_has_only_the_characters_a_consumer_may() {
        let name = durable_name(
            &identity("default", "stock-service"),
            &sub("on.created*>/ü"),
            ConsumerNaming::Scoped,
            "DW_DEFAULT",
            "dw.default.stocks.>",
        );
        assert!(is_valid_consumer_name(name.as_str()), "{name}");
        assert!(name
            .as_str()
            .starts_with("default_stock-service_on_created____"));
    }

    #[test]
    fn a_long_scoped_name_is_cut_and_keeps_its_hash() {
        let long = |handler: &str| {
            durable_name(
                &identity(&"t".repeat(40), &"s".repeat(64)),
                &sub(handler),
                ConsumerNaming::Scoped,
                "DW_DEFAULT",
                "dw.default.a",
            )
        };
        let (a, b) = (
            long(&format!("{}a", "h".repeat(200))),
            long(&format!("{}b", "h".repeat(200))),
        );
        assert_eq!(a.as_str().len(), CONSUMER_NAME_MAX);
        assert!(is_valid_consumer_name(a.as_str()));
        assert_ne!(a, b, "the handlers differ past the cut");
        assert_eq!(a.as_str()[..191], b.as_str()[..191]);
    }

    #[test]
    fn a_declared_name_is_the_durables_name_under_either_naming() {
        for naming in [ConsumerNaming::Legacy, ConsumerNaming::Scoped] {
            let name = durable_name(
                &identity("harbor", "loan-service"),
                &named("loans-service-book-consumer"),
                naming,
                "BOOKS",
                "books.due_date.set",
            );
            assert_eq!(
                name,
                DurableName::Named("loans-service-book-consumer".into())
            );
            assert!(name.is_bound());
        }
        assert!(!DurableName::Legacy("a_b".into()).is_bound());
        assert!(!DurableName::Scoped("t_a_b_00000000".into()).is_bound());
    }
}
