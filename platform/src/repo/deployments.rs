use crate::errors::{PlatformError, PlatformResult};
use crate::registry::ServiceIdentity;
use crate::schema_manager::adopt::AdoptedSchema;
use chrono::{DateTime, Utc};
use cufflink_types::{ServiceManifest, ServiceMode, TableDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct Deployment {
    pub id: Uuid,
    pub service_id: Uuid,
    pub version: i32,
    pub manifest_hash: String,
    pub manifest: Value,
    pub wasm_hash: Option<String>,
    pub status: String,
    pub deployed_at: DateTime<Utc>,
    pub commit_hash: Option<String>,
    pub schema_hash: Option<String>,
    pub artifact: Option<Artifact>,
}

impl Deployment {
    pub fn mode(&self) -> PlatformResult<ServiceMode> {
        manifest_mode(&self.manifest)
    }
}

fn manifest_mode(manifest: &Value) -> PlatformResult<ServiceMode> {
    manifest
        .get("mode")
        .map_or(Ok(ServiceMode::default()), ServiceMode::deserialize)
        .map_err(|e| PlatformError::Internal(format!("stored manifest mode: {e}")))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Artifact {
    pub s3_key: String,
    pub hash: String,
    pub size: i64,
}

pub struct NativeVersion<'a> {
    pub artifact: &'a Artifact,
    pub actor: &'a str,
}

pub struct ActivateVersion<'a> {
    pub service_id: Uuid,
    pub version: i32,
    pub manifest_hash: &'a str,
    pub manifest: &'a ServiceManifest,
    pub wasm_hash: Option<&'a str>,
    pub commit_hash: Option<&'a str>,
    pub schema_json: Option<&'a Value>,
    pub schema_hash: &'a str,
    pub native: Option<NativeVersion<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub enum Adoption<'a> {
    Read(&'a AdoptedSchema),
    Kept,
    Nothing,
}

#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub tables: Vec<TableDefinition>,
    pub adopted: AdoptedSchema,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredSnapshot {
    Tables(Vec<TableDefinition>),
    Adopting {
        tables: Vec<TableDefinition>,
        adopted: AdoptedSchema,
    },
}

impl From<StoredSnapshot> for Snapshot {
    fn from(stored: StoredSnapshot) -> Self {
        match stored {
            StoredSnapshot::Tables(tables) => Snapshot {
                tables,
                adopted: AdoptedSchema::default(),
            },
            StoredSnapshot::Adopting { tables, adopted } => Snapshot { tables, adopted },
        }
    }
}

pub fn stored(tables: &[TableDefinition], adopted: &AdoptedSchema) -> serde_json::Result<Value> {
    if adopted.is_empty() {
        serde_json::to_value(tables)
    } else {
        Ok(serde_json::json!({ "tables": tables, "adopted": adopted }))
    }
}

impl Adoption<'_> {
    pub async fn snapshot(
        self,
        pool: &deadpool_postgres::Pool,
        service: &str,
        service_id: Uuid,
        tables: &[TableDefinition],
    ) -> PlatformResult<Value> {
        let kept;
        let adopted = match self {
            Adoption::Read(adopted) => adopted,
            Adoption::Kept => {
                let running = crate::repo::services::get_by_id(pool, service_id)
                    .await?
                    .map_or(0, |running| running.current_version);
                kept = snapshot(pool, service, service_id, running).await?.adopted;
                &kept
            }
            Adoption::Nothing => return Ok(serde_json::to_value(tables)?),
        };
        Ok(stored(tables, adopted)?)
    }
}

pub async fn activate_version(
    pool: &deadpool_postgres::Pool,
    req: ActivateVersion<'_>,
) -> PlatformResult<Deployment> {
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    let tenant_id: Uuid = tx
        .query_opt(
            "UPDATE services SET current_version = $1, updated_at = now() WHERE id = $2
             RETURNING tenant_id",
            &[&req.version, &req.service_id],
        )
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service disappeared".into()))?
        .get(0);
    readmit(&tx, tenant_id, req.manifest).await?;
    let schema_hash = req.schema_json.map(|_| req.schema_hash);
    let artifact = req.native.as_ref().map(|native| native.artifact);
    let row = tx
        .query_one(
            "INSERT INTO deployments (service_id, version, manifest_hash, manifest, wasm_hash, commit_hash,
                                      artifact_s3_key, artifact_hash, artifact_size)
             VALUES ($1, $2, $3, $4, $5, $6, $8, $9, $10)
             ON CONFLICT (service_id, version) DO UPDATE SET
                 manifest_hash = EXCLUDED.manifest_hash,
                 manifest = EXCLUDED.manifest,
                 wasm_hash = EXCLUDED.wasm_hash,
                 commit_hash = EXCLUDED.commit_hash,
                 artifact_s3_key = EXCLUDED.artifact_s3_key,
                 artifact_hash = EXCLUDED.artifact_hash,
                 artifact_size = EXCLUDED.artifact_size,
                 status = DEFAULT,
                 deployed_at = now()
             RETURNING id, service_id, version, manifest_hash, manifest, wasm_hash, status, deployed_at,
                       commit_hash, $7::text AS schema_hash,
                       artifact_s3_key, artifact_hash, artifact_size",
            &[
                &req.service_id,
                &req.version,
                &req.manifest_hash,
                &serde_json::to_value(req.manifest)?,
                &req.wasm_hash,
                &req.commit_hash,
                &schema_hash,
                &artifact.map(|artifact| &artifact.s3_key),
                &artifact.map(|artifact| &artifact.hash),
                &artifact.map(|artifact| artifact.size),
            ],
        )
        .await?;
    if let Some(native) = &req.native {
        crate::repo::extensions::replace_native(
            &tx,
            tenant_id,
            req.service_id,
            &req.manifest.provides,
            native.actor,
        )
        .await?;
    }
    if let Some(schema_json) = req.schema_json {
        tx.execute(
            "INSERT INTO schema_snapshots (service_id, version, schema_json, hash)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (service_id, version) DO UPDATE SET schema_json = $3, hash = $4",
            &[&req.service_id, &req.version, schema_json, &req.schema_hash],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(row_to_deployment(&row))
}

async fn readmit(
    client: &impl deadpool_postgres::GenericClient,
    tenant_id: Uuid,
    manifest: &ServiceManifest,
) -> PlatformResult<()> {
    if manifest.extensions.is_empty() {
        return Ok(());
    }
    let registrations =
        crate::repo::extensions::shared(client, tenant_id, &manifest.extensions).await?;
    crate::extensions::require_admitted(manifest, &registrations)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionContext {
    pub tenant_id: Uuid,
    pub tenant_slug: String,
    pub service_id: Uuid,
    pub service_name: String,
    pub schema: String,
    pub wasm_hash: Option<String>,
    pub publishes: Option<Vec<String>>,
    pub extensions: Vec<String>,
}

impl ExecutionContext {
    pub fn identity(&self) -> ServiceIdentity {
        ServiceIdentity {
            tenant_id: self.tenant_id,
            tenant_slug: self.tenant_slug.clone(),
            service_id: self.service_id,
            service_name: self.service_name.clone(),
            schema: self.schema.clone(),
        }
    }
}

pub async fn execution_context(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
    version: i32,
) -> PlatformResult<Option<ExecutionContext>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT s.tenant_id, t.slug, s.name, d.wasm_hash, d.manifest->'publishes' AS publishes,
                    d.manifest->'extensions' AS extensions,
                    d.manifest->>'schema_name' AS schema_name
             FROM deployments d
             JOIN services s ON s.id = d.service_id
             JOIN tenants t ON t.id = s.tenant_id
             WHERE d.service_id = $1 AND d.version = $2",
            &[&service_id, &version],
        )
        .await?;
    Ok(row.map(|r| ExecutionContext {
        tenant_id: r.get("tenant_id"),
        tenant_slug: r.get("slug"),
        service_id,
        schema: cufflink_types::service_schema(
            r.get("name"),
            r.get::<_, Option<&str>>("schema_name"),
        ),
        service_name: r.get("name"),
        wasm_hash: r.get("wasm_hash"),
        publishes: r
            .get::<_, Option<Value>>("publishes")
            .and_then(|v| serde_json::from_value::<Vec<String>>(v).ok()),
        extensions: r
            .get::<_, Option<Value>>("extensions")
            .and_then(|v| serde_json::from_value::<Vec<String>>(v).ok())
            .unwrap_or_default(),
    }))
}

const SELECT_DEPLOYMENTS: &str =
    "SELECT d.id, d.service_id, d.version, d.manifest_hash, d.manifest, d.wasm_hash, d.status,
            d.deployed_at, d.commit_hash, ss.hash AS schema_hash,
            d.artifact_s3_key, d.artifact_hash, d.artifact_size
     FROM deployments d
     LEFT JOIN schema_snapshots ss ON ss.service_id = d.service_id AND ss.version = d.version";

pub async fn current(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
) -> PlatformResult<Option<Deployment>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            &format!(
                "{SELECT_DEPLOYMENTS}
                 JOIN services s ON s.id = d.service_id AND s.current_version = d.version
                 WHERE s.id = $1"
            ),
            &[&service_id],
        )
        .await?;
    Ok(row.as_ref().map(row_to_deployment))
}

pub async fn current_mode(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
) -> PlatformResult<Option<ServiceMode>> {
    current(pool, service_id)
        .await?
        .map(|deployment| deployment.mode())
        .transpose()
}

pub async fn current_manifest(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
) -> PlatformResult<Option<Value>> {
    Ok(current(pool, service_id).await?.map(|d| d.manifest))
}

pub async fn list_by_service(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
) -> PlatformResult<Vec<Deployment>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!("{SELECT_DEPLOYMENTS} WHERE d.service_id = $1 ORDER BY d.version DESC"),
            &[&service_id],
        )
        .await?;

    Ok(rows.iter().map(row_to_deployment).collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deleted {
    pub native_key: Option<String>,
}

pub async fn delete_by_version(
    client: &impl deadpool_postgres::GenericClient,
    service_id: Uuid,
    version: i32,
) -> PlatformResult<Option<Deleted>> {
    Ok(client
        .query_opt(
            "DELETE FROM deployments WHERE service_id = $1 AND version = $2
             RETURNING CASE WHEN manifest->>'mode' = 'native' THEN artifact_s3_key END",
            &[&service_id, &version],
        )
        .await?
        .map(|row| Deleted {
            native_key: row.get(0),
        }))
}

pub async fn artifact_keys(
    client: &impl deadpool_postgres::GenericClient,
    service_id: Uuid,
    mode: &ServiceMode,
) -> PlatformResult<Vec<String>> {
    Ok(client
        .query(
            "SELECT DISTINCT artifact_s3_key FROM deployments
             WHERE service_id = $1 AND manifest->>'mode' = $2 AND artifact_s3_key IS NOT NULL
             ORDER BY 1",
            &[&service_id, &mode.as_str()],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

pub async fn records_artifact(
    client: &impl deadpool_postgres::GenericClient,
    key: &str,
) -> PlatformResult<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM deployments WHERE artifact_s3_key = $1)",
            &[&key],
        )
        .await?
        .get(0))
}

pub async fn get_schema_snapshot(
    pool: &deadpool_postgres::Pool,
    service_id: Uuid,
    version: i32,
) -> PlatformResult<Option<Value>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "SELECT schema_json FROM schema_snapshots WHERE service_id = $1 AND version = $2",
            &[&service_id, &version],
        )
        .await?;
    Ok(row.map(|r| r.get("schema_json")))
}

pub async fn snapshot(
    pool: &deadpool_postgres::Pool,
    service: &str,
    service_id: Uuid,
    version: i32,
) -> PlatformResult<Snapshot> {
    parse_snapshot(
        service,
        version,
        get_schema_snapshot(pool, service_id, version).await?,
    )
}

pub fn parse_snapshot(
    service: &str,
    version: i32,
    json: Option<Value>,
) -> PlatformResult<Snapshot> {
    json.map_or(Ok(Snapshot::default()), |json| {
        serde_json::from_value::<StoredSnapshot>(json)
            .map(Snapshot::from)
            .map_err(|e| {
                PlatformError::Internal(format!(
                    "stored schema snapshot for {service} v{version} cannot be decoded: {e}; \
                 fix the row before deploying"
                ))
            })
    })
}

fn row_to_deployment(row: &tokio_postgres::Row) -> Deployment {
    Deployment {
        id: row.get("id"),
        service_id: row.get("service_id"),
        version: row.get("version"),
        manifest_hash: row.get("manifest_hash"),
        manifest: row.get("manifest"),
        wasm_hash: row.get("wasm_hash"),
        status: row.get("status"),
        deployed_at: row.get("deployed_at"),
        commit_hash: row.get("commit_hash"),
        schema_hash: row.get("schema_hash"),
        artifact: match (
            row.get("artifact_s3_key"),
            row.get("artifact_hash"),
            row.get("artifact_size"),
        ) {
            (Some(s3_key), Some(hash), Some(size)) => Some(Artifact { s3_key, hash, size }),
            _ => None,
        },
    }
}

pub fn running(deployments: &[Deployment], current_version: i32) -> Option<&Deployment> {
    deployments.iter().find(|d| d.version == current_version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn deployment_at(version: i32) -> Deployment {
        Deployment {
            id: Uuid::new_v4(),
            service_id: Uuid::new_v4(),
            version,
            manifest_hash: format!("m{version}"),
            manifest: json!({}),
            wasm_hash: Some(format!("w{version}")),
            status: "active".to_string(),
            deployed_at: chrono::Utc::now(),
            commit_hash: None,
            schema_hash: None,
            artifact: None,
        }
    }

    #[test]
    fn a_stored_manifest_names_its_mode_or_runs_in_the_default() {
        for (manifest, mode) in [
            (json!({"mode": "native"}), ServiceMode::Native),
            (json!({"mode": "web"}), ServiceMode::Web),
            (json!({"mode": "wasm"}), ServiceMode::Wasm),
            (json!({"name": "legacy"}), ServiceMode::Crud),
        ] {
            assert_eq!(
                manifest_mode(&manifest).expect("a mode"),
                mode,
                "{manifest}"
            );
        }
        for unknown in [json!({"mode": "lambda"}), json!({"mode": 3})] {
            assert!(manifest_mode(&unknown).is_err(), "{unknown}");
        }
    }

    #[test]
    fn running_picks_the_current_version_not_the_highest() {
        let deployments = vec![deployment_at(18), deployment_at(17)];
        let live = running(&deployments, 17).expect("running row should be found");
        assert_eq!(live.version, 17);
        assert_eq!(live.manifest_hash, "m17");
        assert!(running(&[], 0).is_none());
    }

    #[test]
    fn a_snapshot_that_adopts_nothing_is_stored_as_every_release_stored_it() {
        let tables: Vec<TableDefinition> =
            serde_json::from_value(json!([{ "name": "items", "columns": [] }])).unwrap();
        assert_eq!(
            stored(&tables, &AdoptedSchema::default()).unwrap(),
            serde_json::to_value(&tables).unwrap()
        );
    }

    #[test]
    fn a_snapshot_keeps_what_its_version_adopted() {
        let tables: Vec<TableDefinition> =
            serde_json::from_value(json!([{ "name": "notes", "columns": [], "managed": false }]))
                .unwrap();
        let adopted: AdoptedSchema = serde_json::from_value(json!({"notes": {"kind": {
            "type": "loan_service.loan_kind", "typtype": "enum", "nullable": false, "has_default": true
        }}}))
        .unwrap();
        let json = stored(&tables, &adopted).unwrap();
        assert_eq!(json["adopted"]["notes"]["kind"]["typtype"], "enum");
        assert_eq!(json["tables"][0]["name"], "notes");
        let read = parse_snapshot("loan-service", 4, Some(json)).unwrap();
        assert_eq!(read.adopted, adopted);
        assert!(!read.tables[0].managed);
    }

    #[test]
    fn parse_snapshot_names_the_row_it_cannot_decode() {
        assert!(parse_snapshot("orders", 3, None).unwrap().tables.is_empty());
        let snapshot = parse_snapshot(
            "orders",
            3,
            Some(json!([{ "name": "items", "columns": [] }])),
        )
        .unwrap();
        assert_eq!(snapshot.tables.len(), 1);
        assert_eq!(snapshot.tables[0].name, "items");
        assert!(snapshot.adopted.is_empty());
        let err = parse_snapshot("orders", 3, Some(json!({ "bogus": true })))
            .expect_err("undecodable snapshot")
            .to_string();
        assert!(err.contains("orders v3"), "{err}");
        assert!(err.contains("fix the row"), "{err}");
    }
}
