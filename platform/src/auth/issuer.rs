use crate::config::PlatformConfig;
use crate::repo::tenants::Tenant;

pub fn issuer_base<'a>(config: &'a PlatformConfig, tenant: &'a Tenant) -> Option<&'a str> {
    tenant
        .keycloak_issuer
        .as_deref()
        .or(config.keycloak_issuer.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant_catalog::tests::tenant;

    #[test]
    fn tenant_override_wins_over_the_platform_issuer_else_discovery() {
        let mut config = PlatformConfig::for_tests("postgres://x", "nats://x");
        let mut t = tenant("default", Some("orchard"));
        assert_eq!(issuer_base(&config, &t), None);

        config.keycloak_issuer = Some("https://auth.platform.test".into());
        assert_eq!(issuer_base(&config, &t), Some("https://auth.platform.test"));

        t.keycloak_issuer = Some("https://auth.tenant.test".into());
        assert_eq!(issuer_base(&config, &t), Some("https://auth.tenant.test"));
    }
}
