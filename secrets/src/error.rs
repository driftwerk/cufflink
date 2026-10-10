use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("{0}")]
    Config(String),
    #[error("no key-encryption key is configured (CUFFLINK_SECRETS_KEK)")]
    NoKey,
    #[error("key-encryption key '{0}' is not in the key ring (CUFFLINK_SECRETS_KEK_PREVIOUS)")]
    UnknownKek(String),
    #[error("data key {key_id} is not a key of tenant {tenant_id}")]
    UnknownDataKey { tenant_id: Uuid, key_id: Uuid },
    #[error("a sealed value is malformed: {0}")]
    Malformed(&'static str),
    #[error("a sealed value failed authentication: it was altered, or belongs to another row")]
    Authentication,
    #[error("{table}.{column} holds a value that is not sealed; run `cufflink admin secrets reseal --operation encrypt`")]
    NotSealed {
        table: &'static str,
        column: &'static str,
    },
    #[error("{table}.{column} holds no secret")]
    Absent {
        table: &'static str,
        column: &'static str,
    },
    #[error("{table} row {id}: {source}")]
    Row {
        table: &'static str,
        id: Uuid,
        #[source]
        source: Box<SecretError>,
    },
    #[error("the operating system gave no random bytes")]
    Random,
    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database connection: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
}

impl SecretError {
    pub fn is_transient(&self) -> bool {
        match self {
            SecretError::Database(_) | SecretError::Pool(_) => true,
            SecretError::Row { source, .. } => source.is_transient(),
            _ => false,
        }
    }
}
