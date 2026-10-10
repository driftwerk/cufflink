use cufflink_testkit as common;

use common::TestDb;
use cufflink_platform::policy::PolicySet;
use cufflink_platform::registry::ServiceIdentity;
use cufflink_platform::repo::service_db_roles::{self, NewBinding, Verification};
use cufflink_platform::secrets::{KeyRing, SecretBox, StorageMode};
use cufflink_platform::tenant_catalog::TenantCatalog;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_platform::tenant_sessions::{Purpose, RoleMode, Scope, Statements, TenantSessions};
use serde_json::json;
use std::time::Duration;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

struct Bound {
    sessions: TenantSessions,
    loans: ServiceIdentity,
    role: String,
    tenant_db: tokio_postgres::Client,
    _db: TestDb,
}

async fn bound(connections: usize) -> Option<Bound> {
    let infra = common::infra().await?;
    let nats_url = infra.require_nats()?;
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let role = db.role("sessions");
    let platform = db.pool().get().await.expect("checkout");
    let service_id: Uuid = platform
        .query_one(
            "INSERT INTO services (tenant_id, name, schema_name)
             VALUES ($1, 'loan-service', 'loan_service') RETURNING id",
            &[&tenant.id],
        )
        .await
        .expect("service")
        .get(0);
    platform
        .execute(
            "INSERT INTO tenant_policies (tenant_id, key, mode) VALUES ($1, 'db_roles', 'warn')",
            &[&tenant.id],
        )
        .await
        .expect("policy");
    service_db_roles::write(
        &platform,
        &NewBinding {
            service_id,
            role_name: &role,
            managed: false,
            password_enc: None,
            read_only_role: None,
            updated_by: "test",
            verification: Verification::Verified,
            verify_report: &json!({}),
        },
    )
    .await
    .expect("binding");

    let url =
        common::infra::with_dbname(&db.url, &TenantPoolManager::managed_db_name(&tenant.slug));
    let (tenant_db, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("tenant database");
    tokio::spawn(connection);
    tenant_db
        .batch_execute(&format!(
            "CREATE ROLE {role} NOLOGIN;
             GRANT {role} TO CURRENT_USER;
             CREATE SCHEMA loan_service AUTHORIZATION {role};
             CREATE TABLE loan_service.loans (id int PRIMARY KEY, body text);
             ALTER TABLE loan_service.loans OWNER TO {role};
             CREATE SCHEMA other;
             CREATE TABLE other.things (id int);
             INSERT INTO other.things VALUES (1), (2);"
        ))
        .await
        .expect("fixture");

    let nats = async_nats::connect(nats_url).await.expect("nats");
    let sessions = TenantSessions::new(
        TenantPoolManager::new(&db.url, 4, connections).expect("tenant pools"),
        TenantCatalog::new(db.pool(), nats, PolicySet::builtin_defaults()),
    )
    .with_roles(
        db.pool(),
        SecretBox::new(db.pool(), KeyRing::default(), StorageMode::Plaintext),
        4,
    );
    Some(Bound {
        sessions,
        loans: ServiceIdentity {
            tenant_id: tenant.id,
            tenant_slug: tenant.slug.clone(),
            service_id,
            service_name: "loan-service".into(),
            schema: "loan_service".into(),
        },
        role,
        tenant_db,
        _db: db,
    })
}

const WHO: &str = "SELECT current_user::text";
const THINGS: &str = "SELECT count(*) FROM other.things";

async fn loans(tenant_db: &tokio_postgres::Client) -> Vec<i32> {
    tenant_db
        .query("SELECT id FROM loan_service.loans ORDER BY id", &[])
        .await
        .expect("loans")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

#[tokio::test]
async fn statements_sent_together_each_run_as_the_role_and_fall_back_alone() {
    let Some(test) = bound(3).await else {
        return;
    };
    let mut session = test
        .sessions
        .acquire(&test.loans, Purpose::Crud, None)
        .await
        .expect("session");
    assert_eq!(
        session.mode(),
        &RoleMode::SetLocal {
            role: test.role.clone(),
            fallback: true
        }
    );

    let (who, things, inserted) = tokio::try_join!(
        session.query_one(WHO, &[]),
        session.query_one(THINGS, &[]),
        session.execute("INSERT INTO loan_service.loans VALUES (1, 'alone')", &[]),
    )
    .expect("each in a transaction of its own");
    assert_eq!(who.get::<_, String>(0), test.role);
    assert_eq!(
        things.get::<_, i64>(0),
        2,
        "refused, and run as the platform"
    );
    assert_eq!(inserted, 1);

    let tx = session.transaction().await.expect("begin");
    let (who, things, inserted) = tokio::try_join!(
        tx.query_one(WHO, &[]),
        tx.query_one(THINGS, &[]),
        tx.execute("INSERT INTO loan_service.loans VALUES (2, 'together')", &[]),
    )
    .expect("each behind a savepoint of its own");
    assert_eq!(who.get::<_, String>(0), test.role);
    assert_eq!(things.get::<_, i64>(0), 2);
    assert_eq!(inserted, 1);
    let after: String = tx.query_one(WHO, &[]).await.expect("who").get(0);
    assert_eq!(after, test.role, "the role is set again after a fallback");
    let duplicate = tx
        .execute("INSERT INTO loan_service.loans VALUES (2, 'again')", &[])
        .await
        .expect_err("a duplicate key");
    assert_eq!(duplicate.code(), Some(&SqlState::UNIQUE_VIOLATION));
    let aborted = tx.query_one(WHO, &[]).await.expect_err("aborted");
    assert_eq!(
        aborted.code(),
        Some(&SqlState::IN_FAILED_SQL_TRANSACTION),
        "a failure that is no refusal leaves the transaction aborted"
    );
    tx.rollback().await.expect("rollback");
    assert_eq!(loans(&test.tenant_db).await, [1]);

    let prelude = session.prelude(Scope::Transaction, Duration::from_secs(5));
    session
        .begin_by_hand(&format!("BEGIN; {prelude}"))
        .await
        .expect("a transaction driven by hand");
    let (inserted, prepared) = tokio::try_join!(
        session.execute("INSERT INTO loans VALUES (3, 'by hand')", &[]),
        session.prepare(THINGS),
    )
    .expect("each behind a savepoint of the holder's transaction");
    assert_eq!(inserted, 1);
    let things = session.query_one(&prepared, &[]).await.expect("things");
    assert_eq!(things.get::<_, i64>(0), 2);
    assert_eq!(
        loans(&test.tenant_db).await,
        [1],
        "nothing is committed yet"
    );
    session.end_by_hand("COMMIT").await.expect("commit");
    assert_eq!(loans(&test.tenant_db).await, [1, 3]);

    assert_eq!(
        test.sessions
            .role_of(&test.loans, Purpose::Ddl)
            .await
            .expect("role")
            .role(),
        Some(test.role.as_str()),
        "what runs a deploy's SQL on a connection of its own is told the role"
    );
    let deploy = test
        .sessions
        .acquire(&test.loans, Purpose::Ddl, None)
        .await
        .expect("session");
    let who: String = deploy.query_one(WHO, &[]).await.expect("who").get(0);
    assert_ne!(
        who, test.role,
        "outside its transactions a deploy is the platform's"
    );
    let platform = test
        .sessions
        .acquire(&test.loans, Purpose::Platform, None)
        .await
        .expect("session");
    assert_eq!(platform.mode(), &RoleMode::Platform);
}

#[tokio::test]
async fn a_statement_given_up_leaves_no_transaction_on_the_connection() {
    let Some(test) = bound(1).await else {
        return;
    };
    let pid = "SELECT pg_backend_pid()";
    let session = test
        .sessions
        .acquire(&test.loans, Purpose::Crud, None)
        .await
        .expect("session");
    let first: i32 = session.query_one(pid, &[]).await.expect("pid").get(0);
    let given_up = tokio::time::timeout(
        Duration::from_millis(200),
        session.execute(
            "INSERT INTO loan_service.loans SELECT 9, 'late' FROM pg_sleep(1)",
            &[],
        ),
    )
    .await;
    assert!(given_up.is_err(), "the statement was still running");
    drop(session);

    let session = test
        .sessions
        .acquire(&test.loans, Purpose::Crud, Some(Duration::from_secs(10)))
        .await
        .expect("the same connection");
    let second: i32 = session.query_one(pid, &[]).await.expect("pid").get(0);
    assert_eq!(first, second);
    let outside = session
        .batch_execute("SAVEPOINT probe")
        .await
        .expect_err("a savepoint needs a transaction");
    assert_eq!(
        outside.code(),
        Some(&SqlState::NO_ACTIVE_SQL_TRANSACTION),
        "the transaction the statement was given ended with it"
    );
    assert_eq!(
        loans(&test.tenant_db).await,
        Vec::<i32>::new(),
        "and what it wrote was rolled back"
    );
}

async fn rolled_back(tenant_db: &tokio_postgres::Client) -> i64 {
    tenant_db
        .query_one(
            "SELECT xact_rollback FROM pg_stat_database WHERE datname = current_database()",
            &[],
        )
        .await
        .expect("pg_stat_database")
        .get(0)
}

#[tokio::test]
async fn a_statement_outside_a_transaction_is_sent_nothing_postgres_refuses() {
    let Some(test) = bound(1).await else {
        return;
    };
    let before = rolled_back(&test.tenant_db).await;
    let session = test
        .sessions
        .acquire(&test.loans, Purpose::Crud, None)
        .await
        .expect("session");
    let pid: i32 = session
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    for _ in 0..5 {
        let who: String = session.query_one(WHO, &[]).await.expect("who").get(0);
        assert_eq!(who, test.role);
    }
    session.discard();
    let gone = async {
        while test
            .tenant_db
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = $1",
                &[&pid],
            )
            .await
            .expect("pg_stat_activity")
            .get::<_, i64>(0)
            > 0
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), gone)
        .await
        .expect("the session's backend exits, and its statistics with it");
    assert_eq!(
        rolled_back(&test.tenant_db).await,
        before,
        "no statement failed on the server: none probed for a transaction with a SAVEPOINT"
    );
}
