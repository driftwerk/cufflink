use cufflink_testkit as common;

use cufflink_platform::policy::PolicySet;
use cufflink_platform::registry::ServiceIdentity;
use cufflink_platform::tenant_catalog::TenantCatalog;
use cufflink_platform::tenant_db::TenantPoolManager;
use cufflink_platform::tenant_sessions::{
    AcquireError, Purpose, Scope, Statements, TenantSessions,
};
use std::time::Duration;
use uuid::Uuid;

async fn sessions(db: &common::TestDb, nats_url: &str) -> TenantSessions {
    let nats = async_nats::connect(nats_url).await.expect("nats");
    TenantSessions::new(
        TenantPoolManager::new(&db.url, 4, 2).expect("tenant pools"),
        TenantCatalog::new(db.pool(), nats, PolicySet::builtin_defaults()),
    )
}

fn identity(tenant: &common::Tenant, schema: &str) -> ServiceIdentity {
    ServiceIdentity {
        tenant_id: tenant.id,
        tenant_slug: tenant.slug.clone(),
        service_id: Uuid::new_v4(),
        service_name: schema.replace('_', "-"),
        schema: schema.to_string(),
    }
}

async fn show(client: &impl Statements, setting: &str) -> String {
    client
        .query_one(&format!("SHOW {setting}"), &[])
        .await
        .expect("show")
        .get(0)
}

#[tokio::test]
async fn a_session_is_on_the_tenants_database_under_the_services_settings() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let sessions = sessions(&db, nats_url).await;
    let loans = identity(&tenant, "loan_service");

    let mut session = sessions
        .acquire(&loans, Purpose::Guest, Some(Duration::from_secs(5)))
        .await
        .expect("session");
    assert_eq!(session.schema(), "loan_service");
    assert_eq!(
        show(&session, "search_path").await,
        "\"$user\", public",
        "a session sets nothing until its prelude runs"
    );
    let prelude = session.prelude(Scope::Transaction, Duration::from_millis(1500));
    let tx = session.transaction().await.expect("begin");
    tx.batch_execute(&prelude).await.expect("prelude");
    assert_eq!(show(&tx, "search_path").await, "loan_service, public");
    assert_eq!(show(&tx, "statement_timeout").await, "1500ms");
    let database: String = tx
        .query_one("SELECT current_database()::text", &[])
        .await
        .expect("database")
        .get(0);
    assert_eq!(database, TenantPoolManager::managed_db_name(&tenant.slug));
    tx.rollback().await.expect("rollback");
    assert_eq!(show(&session, "search_path").await, "\"$user\", public");

    drop(session);

    let service = sessions
        .service(&loans, Purpose::Crud)
        .await
        .expect("service sessions");
    let (first, second) = (
        service.get().await.expect("first"),
        service.get().await.expect("second"),
    );
    let pid = |session| async move {
        let session: cufflink_platform::tenant_sessions::Session = session;
        session
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get::<_, i32>(0)
    };
    assert_ne!(pid(first).await, pid(second).await);

    let direct = sessions.direct(tenant.id).await.expect("direct");
    let database: String = direct
        .query_one("SELECT current_database()::text", &[])
        .await
        .expect("database")
        .get(0);
    assert_eq!(database, TenantPoolManager::managed_db_name(&tenant.slug));
    let platform = sessions.platform(tenant.id, None).await.expect("platform");
    let reachable: i32 = platform
        .query_one("SELECT 1", &[])
        .await
        .expect("select")
        .get(0);
    assert_eq!(reachable, 1);
}

#[tokio::test]
async fn a_tenant_that_does_not_exist_has_no_sessions() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let db = infra.create_migrated_database().await;
    let sessions = sessions(&db, nats_url).await;
    let nobody = ServiceIdentity {
        tenant_id: Uuid::new_v4(),
        tenant_slug: "nobody".into(),
        service_id: Uuid::new_v4(),
        service_name: "loans".into(),
        schema: "loans".into(),
    };
    let refused = sessions
        .acquire(&nobody, Purpose::Crud, None)
        .await
        .err()
        .expect("no session");
    assert!(
        matches!(refused, AcquireError::UnknownTenant(id) if id == nobody.tenant_id),
        "{refused}"
    );
    assert!(matches!(
        sessions.direct(nobody.tenant_id).await,
        Err(AcquireError::UnknownTenant(_))
    ));
}

#[tokio::test]
async fn a_checkout_waits_no_longer_than_it_is_given() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let sessions = TenantSessions::over_pool(
        cufflink_db::build_pool(&db.url, 1).expect("pool"),
        cufflink_db::Pooler::Direct,
    );
    let anyone = ServiceIdentity {
        tenant_id: Uuid::nil(),
        tenant_slug: "acme".into(),
        service_id: Uuid::nil(),
        service_name: "loans".into(),
        schema: "loans".into(),
    };
    let held = sessions
        .acquire(&anyone, Purpose::Guest, None)
        .await
        .expect("the only connection");
    let started = std::time::Instant::now();
    let refused = sessions
        .acquire(&anyone, Purpose::Guest, Some(Duration::from_millis(300)))
        .await
        .err()
        .expect("the pool is exhausted");
    assert!(matches!(refused, AcquireError::Checkout(_)), "{refused}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    held.discard();
    sessions
        .acquire(&anyone, Purpose::Guest, Some(Duration::from_secs(5)))
        .await
        .expect("a discarded connection frees its slot");
}
