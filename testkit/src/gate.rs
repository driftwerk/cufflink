//! A gate a guest waits at: the guest's `SELECT pg_advisory_xact_lock(key)`
//! blocks while the test holds the same lock on the tenant database, so a
//! test decides exactly when a guest that holds memory lets go of it.

use tokio_postgres::{Client, NoTls};

pub const GATE_KEY: i64 = 4242;

fn halves(key: i64) -> (i64, i64) {
    const LOW: i64 = 0xffff_ffff;
    ((key >> 32) & LOW, key & LOW)
}

pub struct Gate {
    client: Client,
    key: i64,
}

impl Gate {
    pub async fn closed(tenant_database_url: &str) -> Self {
        Self::closed_at(tenant_database_url, GATE_KEY).await
    }

    pub async fn closed_at(tenant_database_url: &str, key: i64) -> Self {
        let (client, connection) = tokio_postgres::connect(tenant_database_url, NoTls)
            .await
            .expect("the tenant database");
        tokio::spawn(connection);
        client
            .execute("SELECT pg_advisory_lock($1)", &[&key])
            .await
            .expect("close the gate");
        Self { client, key }
    }

    pub async fn waiting(&self) -> i64 {
        let (high, low) = halves(self.key);
        self.client
            .query_one(
                "SELECT count(*) FROM pg_locks
                  WHERE locktype = 'advisory' AND objsubid = 1
                    AND classid::bigint = $1 AND objid::bigint = $2 AND NOT granted
                    AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
                &[&high, &low],
            )
            .await
            .expect("pg_locks")
            .get(0)
    }

    pub async fn open(self) {
        self.client
            .execute("SELECT pg_advisory_unlock($1)", &[&self.key])
            .await
            .expect("open the gate");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_key_is_matched_by_both_of_its_halves() {
        assert_eq!(super::halves(4242), (0, 4242));
        assert_eq!(super::halves((1 << 32) + 5), (1, 5));
        assert_eq!(super::halves(-1), (0xffff_ffff, 0xffff_ffff));
    }

    #[test]
    fn the_guest_waits_on_the_gates_key() {
        assert!(crate::guest::module_wat()
            .contains(&format!("pg_advisory_xact_lock({})", super::GATE_KEY)));
    }
}
