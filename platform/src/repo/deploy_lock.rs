use crate::errors::PlatformResult;
use crate::repo::advisory_lock::AdvisoryLock;
use deadpool_postgres::Pool;
use uuid::Uuid;

pub struct DeployLock(AdvisoryLock);

impl DeployLock {
    pub fn key_for(service_id: Uuid) -> i64 {
        (service_id.as_u128() & 0x7FFF_FFFF_FFFF_FFFF) as i64
    }

    pub async fn try_acquire(pool: &Pool, service_id: Uuid) -> PlatformResult<Option<Self>> {
        AdvisoryLock::try_acquire(
            pool,
            Self::key_for(service_id),
            "cufflink_deploy_lock_dropped_total",
        )
        .await
        .map(|lock| lock.map(Self))
    }

    pub async fn release(self) {
        self.0.release().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_the_low_63_bits_of_the_service_id() {
        assert_eq!(DeployLock::key_for(Uuid::nil()), 0);
        assert_eq!(DeployLock::key_for(Uuid::max()), i64::MAX);
        let id = Uuid::from_u128(0x8000_0000_0000_0000_0000_0000_0000_0042);
        assert_eq!(DeployLock::key_for(id), 0x42);
    }
}
