use super::sample_manifest;
use crate::registry::replica::{module, replica, Replica};
use crate::registry::ServiceRegistry;
use crate::repo::services::Status;
use crate::wasm_runtime::sha256_hex;
use cufflink_types::ServiceManifest;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

fn active_service() -> ServiceManifest {
    let mut manifest = sample_manifest();
    manifest
        .subscriptions
        .retain(|subscription| subscription.stream.is_none());
    manifest
}

impl Replica {
    fn as_v0_29_2_registered(
        &self,
        manifest: &ServiceManifest,
        service_id: &str,
        version: i64,
        wasm: &[u8],
    ) -> Value {
        json!({
            "service_id": service_id,
            "tenant_id": self.tenant.id,
            "tenant_slug": self.tenant.slug,
            "name": manifest.name,
            "version": version,
            "wasm_hash": sha256_hex(wasm),
            "manifest": manifest.clone().without_migration_bodies(),
        })
    }

    async fn reloaded_elsewhere(&self, name: &str) -> Option<Value> {
        let elsewhere =
            ServiceRegistry::new(self.state.platform_db.clone(), self.state.nats.clone());
        elsewhere.load_all().await.expect("reloaded");
        elsewhere
            .get_by_slug(&self.tenant.slug, name)
            .await
            .map(|service| service.projected())
    }
}

#[tokio::test]
async fn an_active_service_deploys_and_registers_as_v0_29_2_did() {
    let Some(replica) = replica().await else {
        return;
    };
    let manifest = active_service();
    let name = manifest.name.as_str();
    let modules = [1, 2, 3].map(|variant| module(&manifest, variant));

    let deployed = replica
        .deploy(&manifest, &modules[0])
        .await
        .expect("deployed");
    let service_id = deployed["service_id"].as_str().expect("service id");
    let version = deployed["version"].as_i64().expect("version");
    assert_eq!(deployed.get("skipped"), None, "{deployed}");
    let registered = replica.routed(name).await.expect("registered");
    assert_eq!(
        registered.projected(),
        replica.as_v0_29_2_registered(&manifest, service_id, version, &modules[0])
    );
    assert_eq!(
        replica.reloaded_elsewhere(name).await,
        Some(registered.projected())
    );
    assert_eq!(replica.state.registry.count().await, 1);
    let at = |rest: &str| format!("/svc/{}/{name}/{rest}", replica.tenant.slug);
    for table in &manifest.tables {
        assert_eq!(
            replica.get(&at(&table.name)).await,
            (
                401,
                json!({"error": "Authentication required for this resource"})
            ),
            "{} is served, and guarded as it declares",
            table.name
        );
    }
    assert_eq!(
        replica.get(&at("no_such_table")).await,
        (404, json!({"error": "Table 'no_such_table' not found"}))
    );
    let (status, health) = replica.get(&at("_meta/health")).await;
    assert_eq!(
        (status, &health["table_count"]),
        (200, &json!(manifest.tables.len())),
        "{health}"
    );
    assert_eq!(
        replica.state.registry.current_versions().await,
        HashMap::from([(registered.service_id, registered.version)])
    );

    let again = replica
        .deploy(&manifest, &modules[0])
        .await
        .expect("skipped");
    assert_eq!(again["skipped"], true, "{again}");
    assert!(
        Arc::ptr_eq(
            &registered,
            &replica.routed(name).await.expect("registered")
        ),
        "a skipped deploy registers nothing"
    );

    let redeployed = replica
        .deploy(&manifest, &modules[1])
        .await
        .expect("redeployed");
    assert_eq!(redeployed["version"], version + 1);
    let rolled_back = replica
        .rollback(service_id, version)
        .await
        .expect("rolled back");
    assert_eq!(rolled_back["new_version"], version + 2, "{rolled_back}");
    assert_eq!(
        replica.routed(name).await.expect("registered").projected(),
        replica.as_v0_29_2_registered(&manifest, service_id, version + 2, &modules[0]),
        "a rollback registers the version it restored"
    );

    let uploaded = replica
        .upload(service_id, &modules[2])
        .await
        .expect("uploaded");
    assert_eq!(uploaded["version"], version + 3, "{uploaded}");
    let registered = replica.routed(name).await.expect("registered");
    assert_eq!(
        registered.projected(),
        replica.as_v0_29_2_registered(&manifest, service_id, version + 3, &modules[2]),
        "a module upload registers the version it became"
    );
    assert_eq!(
        replica.reloaded_elsewhere(name).await,
        Some(registered.projected())
    );
    assert_eq!(
        replica.stored(service_id).await,
        (registered.version, Status::Active),
        "and none of them touched its status"
    );
}
