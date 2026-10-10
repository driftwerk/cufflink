pub mod fire;
pub mod scheduler;
pub mod work;

use crate::nats_manager::dlq::token;
use crate::registry::ServiceIdentity;
use chrono::{DateTime, Utc};

pub const SUBJECT_PREFIX: &str = "cufflink.schedule.";

pub fn subject(identity: &ServiceIdentity, schedule: &str) -> String {
    format!(
        "{SUBJECT_PREFIX}{}.{}.{}",
        token(&identity.tenant_slug),
        token(&identity.service_name),
        token(schedule)
    )
}

pub fn fire_id(identity: &ServiceIdentity, schedule: &str, at: DateTime<Utc>) -> String {
    format!(
        "sched:{}:{}:{schedule}:{}",
        identity.tenant_slug,
        identity.service_name,
        at.timestamp()
    )
}

pub fn job_key(schedule: &str, at: DateTime<Utc>) -> String {
    format!("sched:{schedule}:{}", at.timestamp())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use uuid::Uuid;

    pub(crate) fn identity(tenant: &str, service: &str) -> ServiceIdentity {
        ServiceIdentity {
            tenant_id: Uuid::nil(),
            tenant_slug: tenant.into(),
            service_id: Uuid::nil(),
            service_name: service.into(),
            schema: service.replace('-', "_"),
        }
    }

    pub(crate) fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn a_schedule_has_a_subject_of_its_own() {
        assert_eq!(
            subject(&identity("default", "order-service"), "order_retry"),
            "cufflink.schedule.default.order-service.order_retry"
        );
        assert_eq!(
            subject(&identity("default", "order.service"), "order_retry"),
            "cufflink.schedule.default.order_service.order_retry",
            "a name is one token of the subject"
        );
    }

    #[test]
    fn a_fire_is_named_for_its_instant() {
        let fire = at("2026-09-28T10:00:30Z");
        assert_eq!(
            fire_id(&identity("default", "order-service"), "order_retry", fire),
            "sched:default:order-service:order_retry:1790589630"
        );
        assert_eq!(job_key("order_retry", fire), "sched:order_retry:1790589630");
    }
}
