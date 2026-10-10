use crate::{SecretAad, Stored};
use deadpool_postgres::GenericClient;
use uuid::Uuid;

const TENANT_OF_SERVICE: &str = "(SELECT s.tenant_id FROM services s WHERE s.id = r.service_id)";

#[derive(Debug, PartialEq, Eq)]
pub struct SealedColumn {
    pub table: &'static str,
    pub name: &'static str,
    pub plain: Option<&'static str>,
    pub sealed: &'static str,
    id: &'static str,
    tenant: &'static str,
    row: &'static str,
}

pub const SERVICE_CONFIG_VALUE: SealedColumn = SealedColumn {
    table: "service_configs",
    name: "value",
    plain: Some("value"),
    sealed: "value_enc",
    id: "id",
    tenant: TENANT_OF_SERVICE,
    row: "ARRAY[r.service_id::text, r.key]",
};

pub const TENANT_OPS_SECRET: SealedColumn = SealedColumn {
    table: "tenants",
    name: "keycloak_ops_client_secret",
    plain: Some("keycloak_ops_client_secret"),
    sealed: "keycloak_ops_client_secret_enc",
    id: "id",
    tenant: "r.id",
    row: "ARRAY[r.id::text]",
};

pub const WEBHOOK_SECRET: SealedColumn = SealedColumn {
    table: "webhooks",
    name: "secret",
    plain: Some("secret"),
    sealed: "secret_enc",
    id: "id",
    tenant: TENANT_OF_SERVICE,
    row: "ARRAY[r.id::text]",
};

pub const DB_ROLE_PASSWORD: SealedColumn = SealedColumn {
    table: "service_db_roles",
    name: "password",
    plain: None,
    sealed: "password_enc",
    id: "service_id",
    tenant: TENANT_OF_SERVICE,
    row: "ARRAY[r.service_id::text]",
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lock {
    ForRewrite,
    None,
}

impl Lock {
    fn clause(self) -> &'static str {
        match self {
            Lock::ForRewrite => " FOR NO KEY UPDATE OF r",
            Lock::None => "",
        }
    }
}

pub const ALL: [&SealedColumn; 4] = [
    &SERVICE_CONFIG_VALUE,
    &TENANT_OPS_SECRET,
    &WEBHOOK_SECRET,
    &DB_ROLE_PASSWORD,
];

impl SealedColumn {
    pub fn aad(&self, row: impl IntoIterator<Item = impl Into<String>>) -> SecretAad {
        SecretAad::new(self.table, self.name, row)
    }

    fn plain_or_null(&self) -> String {
        self.plain
            .map_or_else(|| "NULL::text".to_string(), |plain| format!("r.{plain}"))
    }

    pub(crate) fn select_batch(&self, lock: Lock) -> String {
        let Self {
            table,
            sealed,
            id,
            tenant,
            row,
            ..
        } = self;
        let plain = self.plain_or_null();
        let lock = lock.clause();
        format!(
            "SELECT r.{id} AS id, {tenant} AS tenant_id, {row} AS row, {plain} AS plain, r.{sealed} AS sealed
             FROM {table} r
             WHERE ($1::uuid IS NULL OR r.{id} > $1)
               AND ($2::uuid IS NULL OR {tenant} = $2)
               AND ({plain} IS NOT NULL OR r.{sealed} IS NOT NULL)
             ORDER BY r.{id} LIMIT $3{lock}"
        )
    }

    pub(crate) fn sealed_under_another(&self) -> String {
        let Self {
            table,
            sealed,
            tenant,
            ..
        } = self;
        format!(
            "SELECT EXISTS (SELECT 1 FROM {table} r
             WHERE {tenant} = $1 AND r.{sealed} IS NOT NULL
               AND substring(r.{sealed} FROM 2 FOR 16) <> $2)"
        )
    }

    pub async fn write(
        &self,
        client: &impl GenericClient,
        id: Uuid,
        stored: &Stored,
    ) -> Result<u64, tokio_postgres::Error> {
        let Self {
            table,
            sealed,
            id: id_column,
            ..
        } = self;
        match self.plain {
            Some(plain) => {
                client
                    .execute(
                        &format!(
                            "UPDATE {table} SET {plain} = $1, {sealed} = $2 WHERE {id_column} = $3"
                        ),
                        &[&stored.plain(), &stored.sealed(), &id],
                    )
                    .await
            }
            None => {
                client
                    .execute(
                        &format!("UPDATE {table} SET {sealed} = $1 WHERE {id_column} = $2"),
                        &[&stored.sealed(), &id],
                    )
                    .await
            }
        }
    }

    pub(crate) fn count_plaintext(&self) -> Option<String> {
        let Self { table, sealed, .. } = self;
        self.plain.map(|plain| {
            format!(
                "SELECT count(*) FILTER (WHERE {sealed} IS NULL),
                        count(*) FILTER (WHERE {sealed} IS NOT NULL)
                 FROM {table} WHERE {plain} IS NOT NULL"
            )
        })
    }
}

pub(crate) fn key_bytes(key_id: Uuid) -> Vec<u8> {
    key_id.into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_column_names_its_rows_apart_from_every_other() {
        let row = [Uuid::nil().to_string()];
        let aads: Vec<_> = ALL.iter().map(|column| column.aad(row.clone())).collect();
        for (index, aad) in aads.iter().enumerate() {
            assert!(!aads[..index].contains(aad), "{aad:?}");
        }
    }

    #[test]
    fn a_batch_reads_a_secret_never_stored_in_plaintext_as_having_none() {
        let sql = DB_ROLE_PASSWORD.select_batch(Lock::ForRewrite);
        assert!(sql.contains("NULL::text AS plain"), "{sql}");
        assert!(sql.contains("FROM service_db_roles r"), "{sql}");
        assert!(
            sql.ends_with("ORDER BY r.service_id LIMIT $3 FOR NO KEY UPDATE OF r"),
            "{sql}"
        );
        let unlocked = DB_ROLE_PASSWORD.select_batch(Lock::None);
        assert_eq!(
            Some(unlocked.as_str()),
            sql.strip_suffix(" FOR NO KEY UPDATE OF r"),
            "the same rows, and no lock"
        );
        assert_eq!(DB_ROLE_PASSWORD.count_plaintext(), None);
        assert_eq!(
            SERVICE_CONFIG_VALUE.count_plaintext().as_deref(),
            Some(
                "SELECT count(*) FILTER (WHERE value_enc IS NULL),
                        count(*) FILTER (WHERE value_enc IS NOT NULL)
                 FROM service_configs WHERE value IS NOT NULL"
            )
        );
    }
}
