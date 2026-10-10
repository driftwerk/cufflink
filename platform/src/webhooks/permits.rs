use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryConfig {
    pub concurrency: usize,
    pub per_tenant: usize,
    pub per_webhook: usize,
    pub timeout: Duration,
    pub retention: Duration,
}

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            concurrency: 16,
            per_tenant: 8,
            per_webhook: 4,
            timeout: Duration::from_secs(10),
            retention: DAY * 30,
        }
    }
}

impl DeliveryConfig {
    pub fn from_env() -> eyre::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let (defaults, vars) = (Self::default(), cufflink_db::Vars(var));
        let retention_days = u32::try_from(defaults.retention.as_secs() / DAY.as_secs())?;
        let concurrency = vars.positive("WEBHOOK_DELIVERY_CONCURRENCY", defaults.concurrency)?;
        let per_tenant = vars.positive(
            "WEBHOOK_DELIVERY_PER_TENANT",
            defaults.per_tenant.min(concurrency),
        )?;
        let per_webhook = vars.positive(
            "WEBHOOK_DELIVERY_PER_WEBHOOK",
            defaults.per_webhook.min(per_tenant),
        )?;
        if per_webhook > per_tenant || per_tenant > concurrency {
            eyre::bail!(
                "WEBHOOK_DELIVERY_PER_WEBHOOK ({per_webhook}) must be at most \
                 WEBHOOK_DELIVERY_PER_TENANT ({per_tenant}), and that at most \
                 WEBHOOK_DELIVERY_CONCURRENCY ({concurrency})"
            );
        }
        Ok(Self {
            concurrency,
            per_tenant,
            per_webhook,
            timeout: Duration::from_secs(
                vars.positive("WEBHOOK_DELIVERY_TIMEOUT_SECS", defaults.timeout.as_secs())?,
            ),
            retention: DAY * vars.positive("WEBHOOK_DELIVERY_RETENTION_DAYS", retention_days)?,
        })
    }
}

#[derive(Debug, Default)]
struct Held {
    tenants: HashMap<String, usize>,
    webhooks: HashMap<Uuid, usize>,
}

fn release<K: Hash + Eq>(held: &mut HashMap<K, usize>, key: &K) {
    if let Some(count) = held.get_mut(key) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            held.remove(key);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Permits {
    per_tenant: usize,
    per_webhook: usize,
    held: Arc<Mutex<Held>>,
}

impl Permits {
    pub fn new(config: &DeliveryConfig) -> Self {
        Self {
            per_tenant: config.per_tenant,
            per_webhook: config.per_webhook,
            held: Arc::default(),
        }
    }

    pub fn take(&self, tenant: &str, webhook: Uuid) -> Option<Permit> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let of_tenant = held.tenants.get(tenant).copied().unwrap_or_default();
        let of_webhook = held.webhooks.get(&webhook).copied().unwrap_or_default();
        (of_tenant < self.per_tenant && of_webhook < self.per_webhook).then(|| {
            *held.tenants.entry(tenant.to_string()).or_default() += 1;
            *held.webhooks.entry(webhook).or_default() += 1;
            Permit {
                held: Arc::clone(&self.held),
                tenant: tenant.to_string(),
                webhook,
            }
        })
    }
}

#[derive(Debug)]
pub struct Permit {
    held: Arc<Mutex<Held>>,
    tenant: String,
    webhook: Uuid,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        release(&mut held.tenants, &self.tenant);
        release(&mut held.webhooks, &self.webhook);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(vars: &[(&str, &str)]) -> eyre::Result<DeliveryConfig> {
        DeliveryConfig::from_vars(|name| {
            vars.iter()
                .find(|(var, _)| *var == name)
                .map(|(_, value)| value.to_string())
        })
    }

    #[test]
    fn a_replica_delivers_as_it_is_told_and_no_more_to_one_than_to_all() {
        assert_eq!(with(&[]).expect("defaults"), DeliveryConfig::default());
        assert_eq!(
            with(&[
                ("WEBHOOK_DELIVERY_CONCURRENCY", "4"),
                ("WEBHOOK_DELIVERY_PER_TENANT", "2"),
                ("WEBHOOK_DELIVERY_PER_WEBHOOK", "1"),
                ("WEBHOOK_DELIVERY_TIMEOUT_SECS", "3"),
                ("WEBHOOK_DELIVERY_RETENTION_DAYS", "7"),
            ])
            .expect("set"),
            DeliveryConfig {
                concurrency: 4,
                per_tenant: 2,
                per_webhook: 1,
                timeout: Duration::from_secs(3),
                retention: DAY * 7,
            }
        );
        assert_eq!(DeliveryConfig::default().retention, DAY * 30);
        let within = |vars: &[(&str, &str)]| {
            with(vars).map(|config| (config.concurrency, config.per_tenant, config.per_webhook))
        };
        assert_eq!(
            within(&[("WEBHOOK_DELIVERY_CONCURRENCY", "4")]).expect("set"),
            (4, 4, 4),
            "a limit left unset is its default within the one it is part of"
        );
        assert_eq!(
            within(&[("WEBHOOK_DELIVERY_PER_TENANT", "2")]).expect("set"),
            (16, 2, 2)
        );
        assert_eq!(
            within(&[("WEBHOOK_DELIVERY_CONCURRENCY", "1")]).expect("set"),
            (1, 1, 1)
        );
        for (vars, named) in [
            (
                vec![("WEBHOOK_DELIVERY_PER_WEBHOOK", "9")],
                "PER_WEBHOOK (9)",
            ),
            (
                vec![("WEBHOOK_DELIVERY_PER_TENANT", "17")],
                "PER_TENANT (17)",
            ),
            (
                vec![
                    ("WEBHOOK_DELIVERY_CONCURRENCY", "4"),
                    ("WEBHOOK_DELIVERY_PER_TENANT", "6"),
                ],
                "PER_TENANT (6)",
            ),
            (vec![("WEBHOOK_DELIVERY_TIMEOUT_SECS", "0")], "TIMEOUT_SECS"),
            (
                vec![("WEBHOOK_DELIVERY_RETENTION_DAYS", "0")],
                "RETENTION_DAYS",
            ),
        ] {
            let refused = with(&vars).expect_err("refused").to_string();
            assert!(refused.contains(named), "{refused}");
        }
    }

    #[test]
    fn a_delivery_is_made_while_its_webhook_and_its_tenant_have_room() {
        let permits = Permits::new(&DeliveryConfig {
            per_tenant: 3,
            per_webhook: 2,
            ..DeliveryConfig::default()
        });
        let (slow, other) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let first = permits.take("acme", slow).expect("room");
        let second = permits.take("acme", slow).expect("room");
        assert!(permits.take("acme", slow).is_none(), "the webhook is full");
        let third = permits.take("acme", other).expect("room");
        assert!(permits.take("acme", other).is_none(), "the tenant is full");
        assert!(permits.take("globex", other).is_some(), "another tenant");
        drop(first);
        let again = permits.take("acme", slow).expect("given back");
        assert!(permits.take("acme", other).is_none(), "the tenant is full");
        drop((second, third, again));
        let held = permits.held.lock().expect("held");
        assert!(held.tenants.is_empty() && held.webhooks.is_empty());
    }
}
