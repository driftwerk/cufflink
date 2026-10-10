use cufflink_types::manifest::validate::is_lowercase_identifier;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const ROLE_NAME_MAX: usize = 63;
const MANAGED_PREFIX: &str = "svc_";
const DIGEST_HEX: usize = 16;
const READABLE_MAX: usize = ROLE_NAME_MAX - MANAGED_PREFIX.len() - 1 - DIGEST_HEX;

fn role_token(raw: &str) -> String {
    raw.chars()
        .map(|c| match c.to_ascii_lowercase() {
            c if c.is_ascii_lowercase() || c.is_ascii_digit() => c,
            _ => '_',
        })
        .collect()
}

pub fn managed_role(tenant_slug: &str, schema: &str, tenant_id: Uuid, service_id: Uuid) -> String {
    let readable = format!("{}_{}", role_token(tenant_slug), role_token(schema));
    let digest = hex::encode(Sha256::digest(
        [
            b"cufflink:db_role:".as_slice(),
            tenant_id.as_bytes(),
            service_id.as_bytes(),
        ]
        .concat(),
    ));
    format!(
        "{MANAGED_PREFIX}{}_{}",
        &readable[..readable.len().min(READABLE_MAX)],
        &digest[..DIGEST_HEX]
    )
}

pub fn marker(tenant_id: Uuid, service_id: Uuid) -> String {
    format!("cufflink:{tenant_id}:{service_id}")
}

pub fn validate_role_name(what: &str, name: &str) -> Result<(), String> {
    match is_lowercase_identifier(name) {
        true => Ok(()),
        false => Err(format!(
            "{what} '{name}' must be a role name of lowercase letters, digits and '_', not starting with a digit, at most 63 bytes"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(tenant: u128, service: u128) -> (Uuid, Uuid) {
        (Uuid::from_u128(tenant), Uuid::from_u128(service))
    }

    #[test]
    fn a_managed_role_names_the_tenant_and_the_schema_and_is_the_services_alone() {
        let (tenant, service) = ids(1, 2);
        let name = managed_role("preview-pr-7", "Order_Desk", tenant, service);
        assert!(name.starts_with("svc_preview_pr_7_order_desk_"), "{name}");
        assert_eq!(
            name.len(),
            "svc_preview_pr_7_order_desk_".len() + DIGEST_HEX
        );
        assert!(validate_role_name("role", &name).is_ok());
        assert_eq!(
            name,
            managed_role("preview-pr-7", "Order_Desk", tenant, service),
            "the same service is given the same name"
        );
    }

    #[test]
    fn names_that_read_alike_are_told_apart_by_the_ids() {
        let (acme, orders) = ids(1, 2);
        let (acme_prod, prod_orders) = ids(3, 4);
        let one = managed_role("acme-prod", "orders", acme_prod, orders);
        let other = managed_role("acme", "prod_orders", acme, prod_orders);
        assert_eq!(
            one[..one.len() - DIGEST_HEX],
            other[..other.len() - DIGEST_HEX],
            "the readable parts are one"
        );
        assert_ne!(one, other);
        assert_ne!(
            managed_role("t", "s", acme, orders),
            managed_role("t", "s", orders, acme),
            "the tenant's id and the service's are not interchangeable"
        );
    }

    #[test]
    fn a_name_too_long_keeps_its_digest_whole() {
        let (tenant, service) = ids(5, 6);
        let slug = "a-tenant-with-a-very-long-slug-indeed";
        let schema = "and_a_service_schema_that_is_longer_still";
        let long = managed_role(slug, schema, tenant, service);
        assert_eq!(long.len(), ROLE_NAME_MAX, "{long}");
        assert!(long.starts_with("svc_a_tenant_with_a_very_long_slug_indeed_an"));
        assert!(validate_role_name("role", &long).is_ok());
        let other = managed_role(slug, schema, tenant, Uuid::from_u128(7));
        assert_ne!(long, other);
        assert_eq!(
            long[..ROLE_NAME_MAX - DIGEST_HEX],
            other[..ROLE_NAME_MAX - DIGEST_HEX]
        );
    }

    #[test]
    fn a_marker_names_the_tenant_and_the_service() {
        let (tenant, service) = ids(1, 2);
        assert_eq!(
            marker(tenant, service),
            "cufflink:00000000-0000-0000-0000-000000000001:00000000-0000-0000-0000-000000000002"
        );
    }

    #[test]
    fn a_role_name_is_a_lowercase_identifier() {
        for name in ["loan_service_user", "svc_default_stock_service", "_x", "r1"] {
            assert!(validate_role_name("role", name).is_ok(), "{name}");
        }
        for name in ["", "Note", "1x", "a-b", "a b", "a\"b", &"x".repeat(64)] {
            let error = validate_role_name("read_only_role", name).expect_err(name);
            assert!(error.starts_with("read_only_role '"), "{error}");
        }
    }
}
