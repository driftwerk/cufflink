use cufflink_testkit as common;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{Tenant, TestDb};
use cufflink_platform::repo::service_configs;
use cufflink_platform::repo::services::{self, Status};
use cufflink_platform::secrets::reseal::{self, Operation, Report, Verified};
use cufflink_platform::secrets::{
    KeyRing, SecretBox, StorageMode, DB_ROLE_PASSWORD, TENANT_OPS_SECRET, WEBHOOK_SECRET,
};
use deadpool_postgres::Pool;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

fn kek(byte: u8) -> String {
    STANDARD.encode([byte; 32])
}

fn ring(current: u8, previous: &[u8]) -> KeyRing {
    let previous: Vec<String> = previous
        .iter()
        .map(|byte| format!("k{byte}:{}", kek(*byte)))
        .collect();
    KeyRing::parse(
        Some(&kek(current)),
        Some(&format!("k{current}")),
        Some(previous.join(",").as_str()),
    )
    .expect("ring")
}

struct Database {
    db: TestDb,
    pool: Pool,
}

struct Owner {
    tenant: Tenant,
    service: Uuid,
}

impl Database {
    async fn new() -> Option<Self> {
        let infra = common::infra().await?;
        let db = infra.create_migrated_database().await;
        let pool = db.pool();
        Some(Self { db, pool })
    }

    fn secrets(&self, ring: KeyRing, mode: StorageMode) -> SecretBox {
        SecretBox::new(self.pool.clone(), ring, mode)
    }

    fn dual(&self) -> SecretBox {
        self.secrets(ring(1, &[]), StorageMode::Dual)
    }

    async fn owner(&mut self) -> Owner {
        let tenant = self.db.mint_tenant().await;
        let service = services::find_or_create(&self.pool, tenant.id, "cfg", "cfg", Status::Active)
            .await
            .expect("service")
            .id;
        Owner { tenant, service }
    }

    async fn set(&self, secrets: &SecretBox, owner: &Owner, key: &str, value: &str) {
        let client = self.pool.get().await.expect("checkout");
        service_configs::set(
            secrets,
            &client,
            owner.tenant.id,
            owner.service,
            key,
            value,
            true,
        )
        .await
        .expect("set");
    }

    async fn sql(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> u64 {
        self.pool
            .get()
            .await
            .expect("checkout")
            .execute(sql, params)
            .await
            .expect(sql)
    }

    async fn count(&self, table: &str) -> i64 {
        self.pool
            .get()
            .await
            .expect("checkout")
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .expect("count")
            .get(0)
    }

    async fn untouched(&self) -> Vec<String> {
        let client = self.pool.get().await.expect("checkout");
        let mut rows = Vec::new();
        for table in [
            "service_configs",
            "tenants",
            "webhooks",
            "service_db_roles",
            "tenant_data_keys",
        ] {
            let read = client
                .query(
                    &format!("SELECT '{table}' || ' ' || r::text || ' ' || xmin::text || ' ' || xmax::text FROM {table} r ORDER BY 1"),
                    &[],
                )
                .await
                .expect("rows");
            rows.extend(read.iter().map(|row| row.get::<_, String>(0)));
        }
        rows
    }
}

async fn verified(secrets: &SecretBox, tenant: Option<Uuid>) -> (Report, Verified) {
    let report = reseal::run(secrets, Operation::Verify, tenant)
        .await
        .expect("a verify ends with a report, whatever it finds");
    let verified = report.verified.clone().expect("what it found");
    (report, verified)
}

fn by_table(counts: &[(&'static str, u64)]) -> std::collections::BTreeMap<&'static str, u64> {
    counts.iter().copied().collect()
}

const ALTER_SEALED: &str =
    "UPDATE service_configs SET value_enc = set_byte(value_enc, 24, get_byte(value_enc, 24) # 1)
     WHERE service_id = $1 AND key = $2";

#[tokio::test]
async fn verify_counts_a_row_that_does_not_open_and_goes_on() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let secrets = stored.dual();
    let owner = stored.owner().await;
    for key in ["A", "B", "C"] {
        stored.set(&secrets, &owner, key, "v").await;
    }
    let (report, clean) = verified(&secrets, None).await;
    assert_eq!(report.rows_checked, 3);
    assert_eq!(
        clean,
        Verified {
            data_keys_checked: 1,
            ..Verified::default()
        }
    );
    assert_eq!(
        json!(report),
        json!({"operation": "verify", "rows_checked": 3, "rows_rewritten": 0,
               "keys_rewrapped": 0, "tenants_rotated": 0, "verified": json!(clean)})
    );

    stored.sql(ALTER_SEALED, &[&owner.service, &"A"]).await;
    stored
        .sql(
            "UPDATE service_configs SET value = 'newer' WHERE service_id = $1 AND key = 'C'",
            &[&owner.service],
        )
        .await;

    let (report, found) = verified(&secrets, None).await;

    assert_eq!(
        report.rows_checked, 3,
        "the rows after the one that does not open are read too"
    );
    assert_eq!(
        found,
        Verified {
            data_keys_checked: 1,
            unopenable: by_table(&[("service_configs", 1)]),
            plain_differs: by_table(&[("service_configs", 1)]),
            ..Verified::default()
        }
    );
    assert!(
        reseal::run(&secrets, Operation::Encrypt, None)
            .await
            .is_err(),
        "the same row stops an operation that seals"
    );
}

#[tokio::test]
async fn verify_unwraps_data_keys_without_the_cache() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let secrets = stored.dual();
    let owner = stored.owner().await;
    stored.set(&secrets, &owner, "TOKEN", "t").await;
    stored
        .sql(
            "UPDATE tenant_data_keys SET wrapped_dek = '\\x00010203'::bytea",
            &[],
        )
        .await;

    let client = stored.pool.get().await.expect("checkout");
    let read = service_configs::get(&secrets, &client, owner.service, "TOKEN")
        .await
        .expect("the box still remembers the key it unwrapped");
    assert_eq!(
        read.map(|value| value.expose().to_string()).as_deref(),
        Some("t")
    );

    let (_, found) = verified(&secrets, None).await;

    assert_eq!(
        found,
        Verified {
            data_keys_checked: 1,
            data_keys_unopenable: 1,
            unopenable: by_table(&[("service_configs", 1)]),
            ..Verified::default()
        },
        "the wrap the database holds is what is verified, and a row is opened by nothing else"
    );
}

#[tokio::test]
async fn verify_names_keys_under_a_previous_kek() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let owner = stored.owner().await;
    stored.set(&stored.dual(), &owner, "TOKEN", "t").await;

    let rotating = stored.secrets(ring(2, &[1]), StorageMode::Dual);
    let (_, before) = verified(&rotating, None).await;
    assert_eq!(
        before,
        Verified {
            data_keys_checked: 1,
            data_keys_under_previous_kek: 1,
            ..Verified::default()
        },
        "a key the ring still opens is no failure: it says a rotation is to be made"
    );

    let without_the_old = stored.secrets(ring(2, &[]), StorageMode::Dual);
    let (_, lost) = verified(&without_the_old, None).await;
    assert_eq!(
        (lost.data_keys_unopenable, lost.data_keys_under_previous_kek),
        (1, 1)
    );
    assert_eq!(lost.unopenable, by_table(&[("service_configs", 1)]));

    reseal::run(&rotating, Operation::RotateKek, None)
        .await
        .expect("rotated");
    for ring_after in [&rotating, &without_the_old] {
        let (_, after) = verified(ring_after, None).await;
        assert_eq!(
            after,
            Verified {
                data_keys_checked: 1,
                ..Verified::default()
            }
        );
    }

    stored
        .sql("UPDATE tenant_data_keys SET active = false", &[])
        .await;
    let fresh = stored.secrets(ring(2, &[]), StorageMode::Dual);
    stored.set(&fresh, &owner, "LATER", "l").await;
    let (_, retired) = verified(&fresh, None).await;
    assert_eq!(
        retired,
        Verified {
            data_keys_checked: 2,
            rows_under_retired_data_key: 1,
            ..Verified::default()
        },
        "a key no longer active is checked, and still opens the row sealed under it"
    );
}

#[tokio::test]
async fn verify_makes_no_data_key_for_a_tenant_without_one() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let (sealed, unsealed) = (stored.owner().await, stored.owner().await);
    stored.set(&stored.dual(), &sealed, "TOKEN", "t").await;
    stored
        .sql(
            "INSERT INTO service_configs (service_id, key, value) VALUES ($1, 'LEGACY', 'plain')",
            &[&unsealed.service],
        )
        .await;

    let (report, found) = verified(&stored.dual(), None).await;

    assert_eq!(report.rows_checked, 2);
    assert_eq!(
        found,
        Verified {
            data_keys_checked: 1,
            ..Verified::default()
        },
        "under dual a row with no sealed form is read from its plaintext"
    );
    assert_eq!(stored.count("tenant_data_keys").await, 1);

    let (scoped, of_one) = verified(&stored.dual(), Some(unsealed.tenant.id)).await;
    assert_eq!((scoped.rows_checked, of_one), (1, Verified::default()));
    assert_eq!(stored.count("tenant_data_keys").await, 1);

    let encrypted = stored.secrets(ring(1, &[]), StorageMode::Encrypted);
    let (_, found) = verified(&encrypted, None).await;
    assert_eq!(
        found,
        Verified {
            data_keys_checked: 1,
            not_sealed: by_table(&[("service_configs", 1)]),
            ..Verified::default()
        },
        "under encrypted the same row is one the platform cannot read"
    );
    assert_eq!(stored.count("tenant_data_keys").await, 1);

    let plaintext = stored.secrets(KeyRing::default(), StorageMode::Plaintext);
    assert!(
        reseal::run(&plaintext, Operation::Verify, None)
            .await
            .is_err(),
        "with no key ring there is nothing to verify, as there is nothing to reseal"
    );
}

#[tokio::test]
async fn verify_takes_no_row_lock() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let secrets = stored.dual();
    let owner = stored.owner().await;
    stored.set(&secrets, &owner, "TOKEN", "t").await;

    let mut holder = stored.pool.get().await.expect("checkout");
    let holding = holder.transaction().await.expect("begin");
    for table in ["service_configs", "tenant_data_keys", "tenants"] {
        holding
            .execute(&format!("SELECT 1 FROM {table} FOR UPDATE"), &[])
            .await
            .expect("locked");
    }

    let (report, found) = tokio::time::timeout(Duration::from_secs(10), verified(&secrets, None))
        .await
        .expect("a verify waits for no row another transaction holds");
    assert_eq!(report.rows_checked, 1);
    assert_eq!(found.data_keys_checked, 1);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            reseal::run(&secrets, Operation::Encrypt, None)
        )
        .await
        .is_err(),
        "an operation that locks its rows waits for them"
    );

    holding.rollback().await.expect("rollback");
}

#[tokio::test]
async fn verify_changes_no_row() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let secrets = stored.dual();
    let owner = stored.owner().await;
    for key in ["A", "B"] {
        stored.set(&secrets, &owner, key, "v").await;
    }
    stored
        .sql(
            "INSERT INTO webhooks (service_id, table_name, event, url, secret)
             VALUES ($1, 'items', 'created', 'https://hooks.example', 'plain-only')",
            &[&owner.service],
        )
        .await;
    stored.sql(ALTER_SEALED, &[&owner.service, &"A"]).await;
    let before = stored.untouched().await;
    assert!(before.len() >= 5, "{before:?}");

    let rotating = stored.secrets(ring(2, &[1]), StorageMode::Dual);
    for secrets in [&secrets, &rotating] {
        let (report, found) = verified(secrets, None).await;
        assert_eq!(report.rows_checked, 3);
        assert_eq!(found.unopenable, by_table(&[("service_configs", 1)]));
    }

    assert_eq!(
        stored.untouched().await,
        before,
        "no row is written, locked, added or removed, whatever was found"
    );
    let answered: Value = json!(verified(&secrets, None).await.0);
    assert!(
        !answered.to_string().contains("plain-only"),
        "counts, never a secret: {answered}"
    );
}

#[tokio::test]
async fn verify_reads_every_table_that_holds_a_secret() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let secrets = stored.dual();
    let owner = stored.owner().await;
    let client = stored.pool.get().await.expect("checkout");
    let webhook = Uuid::new_v4();
    let sealed = |aad, secret: &'static str| {
        let (secrets, client, tenant) = (&secrets, &client, owner.tenant.id);
        async move {
            secrets
                .store(client, tenant, &aad, secret)
                .await
                .expect("sealed")
        }
    };
    let hook = sealed(WEBHOOK_SECRET.aad([webhook.to_string()]), "whsec").await;
    let ops = sealed(TENANT_OPS_SECRET.aad([owner.tenant.id.to_string()]), "ops").await;
    let role = sealed(DB_ROLE_PASSWORD.aad([owner.service.to_string()]), "pw").await;
    stored.set(&secrets, &owner, "TOKEN", "t").await;
    stored
        .sql(
            "INSERT INTO webhooks (id, service_id, table_name, event, url, secret, secret_enc)
             VALUES ($1, $2, 'items', 'created', 'https://hooks.example', $3, $4)",
            &[&webhook, &owner.service, &hook.plain(), &hook.sealed()],
        )
        .await;
    stored
        .sql(
            "UPDATE tenants SET keycloak_ops_client_secret = $2,
                                keycloak_ops_client_secret_enc = $3 WHERE id = $1",
            &[&owner.tenant.id, &ops.plain(), &ops.sealed()],
        )
        .await;
    stored
        .sql(
            "INSERT INTO service_db_roles (service_id, role_name, managed, password_enc, updated_by)
             VALUES ($1, 'cfg_role', true, $2, 'test')",
            &[&owner.service, &role.sealed()],
        )
        .await;

    let (report, clean) = verified(&secrets, None).await;
    assert_eq!(report.rows_checked, 4, "one row of each of the four tables");
    assert_eq!(
        clean,
        Verified {
            data_keys_checked: 1,
            ..Verified::default()
        },
        "each is opened as the row its table names it"
    );

    for table_and_column in [
        "webhooks SET secret_enc = set_byte(secret_enc, 24, get_byte(secret_enc, 24) # 1)",
        "tenants SET keycloak_ops_client_secret_enc = set_byte(keycloak_ops_client_secret_enc, 24, get_byte(keycloak_ops_client_secret_enc, 24) # 1) WHERE keycloak_ops_client_secret_enc IS NOT NULL",
        "service_db_roles SET password_enc = set_byte(password_enc, 24, get_byte(password_enc, 24) # 1)",
    ] {
        stored.sql(&format!("UPDATE {table_and_column}"), &[]).await;
    }
    let (_, found) = verified(&secrets, None).await;
    assert_eq!(
        found.unopenable,
        by_table(&[("service_db_roles", 1), ("tenants", 1), ("webhooks", 1)])
    );
    assert!(!found.is_clean());
}

#[tokio::test]
async fn verify_reads_past_one_batch_of_rows_and_of_keys() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let secrets = stored.dual();
    let owner = stored.owner().await;
    stored.set(&secrets, &owner, "TOKEN", "t").await;
    let more = reseal::BATCH + 1;
    stored
        .sql(
            "INSERT INTO service_configs (service_id, key, value)
             SELECT $1, 'PLAIN_' || n, 'v' FROM generate_series(1, $2::bigint) n",
            &[&owner.service, &more],
        )
        .await;
    stored
        .sql(
            "INSERT INTO tenant_data_keys (key_id, tenant_id, kek_id, wrapped_dek, active)
             SELECT gen_random_uuid(), $1, 'k1', '\\x0102'::bytea, false
             FROM generate_series(1, $2::bigint)",
            &[&owner.tenant.id, &more],
        )
        .await;

    let (report, found) = verified(&secrets, None).await;

    assert_eq!(report.rows_checked, more as u64 + 1);
    assert_eq!(
        found,
        Verified {
            data_keys_checked: more as u64 + 1,
            data_keys_unopenable: more as u64,
            ..Verified::default()
        },
        "every key and row beyond the first query is read, and the row under the one key that \
         unwraps still opens"
    );
}

#[tokio::test]
async fn verify_of_one_tenant_reads_that_tenants_keys_and_rows() {
    let Some(mut stored) = Database::new().await else {
        return;
    };
    let secrets = stored.dual();
    let (sound, broken) = (stored.owner().await, stored.owner().await);
    for owner in [&sound, &broken] {
        stored.set(&secrets, owner, "TOKEN", "t").await;
    }
    stored
        .sql(
            "UPDATE tenant_data_keys SET wrapped_dek = '\\x00010203'::bytea WHERE tenant_id = $1",
            &[&broken.tenant.id],
        )
        .await;
    let one_key = Verified {
        data_keys_checked: 1,
        ..Verified::default()
    };

    let (report, of_the_sound) = verified(&secrets, Some(sound.tenant.id)).await;
    assert_eq!((report.rows_checked, of_the_sound), (1, one_key.clone()));

    let (report, of_the_broken) = verified(&secrets, Some(broken.tenant.id)).await;
    assert_eq!(report.rows_checked, 1);
    assert_eq!(
        of_the_broken,
        Verified {
            data_keys_unopenable: 1,
            unopenable: by_table(&[("service_configs", 1)]),
            ..one_key
        }
    );

    let (report, of_both) = verified(&secrets, None).await;
    assert_eq!(
        (
            report.rows_checked,
            of_both.data_keys_checked,
            of_both.data_keys_unopenable
        ),
        (2, 2, 1)
    );
}
