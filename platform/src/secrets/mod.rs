pub use cufflink_secrets::columns::{
    DB_ROLE_PASSWORD, SERVICE_CONFIG_VALUE, TENANT_OPS_SECRET, WEBHOOK_SECRET,
};
pub use cufflink_secrets::{
    configs, random_bytes, reseal, KeyRing, MacKeys, SealedColumn, SecretAad, SecretBox,
    SecretError, SecretString, StorageMode, Stored,
};

use crate::app::Permanence;
use crate::errors::PlatformError;

impl Permanence for SecretError {
    fn is_permanent(&self) -> bool {
        !self.is_transient()
    }
}

impl From<SecretError> for PlatformError {
    fn from(error: SecretError) -> Self {
        match error {
            SecretError::Config(message) => PlatformError::Conflict(message),
            SecretError::Database(e) => PlatformError::Database(e),
            SecretError::Pool(e) => PlatformError::Pool(e),
            SecretError::Row { source, .. } if source.is_transient() => Self::from(*source),
            row @ SecretError::Row { .. } => PlatformError::Conflict(row.to_string()),
            other => PlatformError::Eyre(eyre::Report::new(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_misconfiguration_is_told_and_a_failure_is_not() {
        assert!(matches!(
            PlatformError::from(SecretError::Config("secrets are stored in plaintext".into())),
            PlatformError::Conflict(m) if m == "secrets are stored in plaintext"
        ));
        assert!(matches!(
            PlatformError::from(SecretError::Authentication),
            PlatformError::Eyre(_)
        ));
        let row = SecretError::Row {
            table: "webhooks",
            id: uuid::Uuid::nil(),
            source: Box::new(SecretError::Authentication),
        };
        assert!(row.is_permanent());
        assert!(matches!(
            PlatformError::from(row),
            PlatformError::Conflict(m) if m.starts_with("webhooks row 00000000-0000-0000-0000-000000000000: a sealed value failed authentication")
        ));
    }
}
