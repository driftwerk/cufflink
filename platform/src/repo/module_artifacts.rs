use crate::errors::PlatformResult;
use deadpool_postgres::GenericClient;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleArtifact {
    pub service_id: Uuid,
    pub version: i32,
    pub engine_fingerprint: String,
    pub wasm_sha256: String,
    pub sha256: String,
    pub size_bytes: i64,
}

impl ModuleArtifact {
    pub fn object_key(&self) -> String {
        format!(
            "wasm/{}/v{}/{}/{}.cwasm",
            self.service_id, self.version, self.engine_fingerprint, self.sha256
        )
    }

    fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            service_id: row.get("service_id"),
            version: row.get("version"),
            engine_fingerprint: row.get("engine_fingerprint"),
            wasm_sha256: row.get("wasm_sha256"),
            sha256: row.get("sha256"),
            size_bytes: row.get("size_bytes"),
        }
    }
}

const COLUMNS: &str = "service_id, version, engine_fingerprint, wasm_sha256, sha256, size_bytes";

pub async fn find(
    client: &impl GenericClient,
    service_id: Uuid,
    version: i32,
    engine_fingerprint: &str,
) -> PlatformResult<Option<ModuleArtifact>> {
    let row = client
        .query_opt(
            &format!(
                "SELECT {COLUMNS} FROM module_artifacts \
                 WHERE service_id = $1 AND version = $2 AND engine_fingerprint = $3"
            ),
            &[&service_id, &version, &engine_fingerprint],
        )
        .await?;
    Ok(row.as_ref().map(ModuleArtifact::from_row))
}

pub async fn insert(
    client: &impl GenericClient,
    artifact: &ModuleArtifact,
) -> PlatformResult<bool> {
    let inserted = client
        .execute(
            &format!(
                "INSERT INTO module_artifacts ({COLUMNS}) \
                 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING"
            ),
            &[
                &artifact.service_id,
                &artifact.version,
                &artifact.engine_fingerprint,
                &artifact.wasm_sha256,
                &artifact.sha256,
                &artifact.size_bytes,
            ],
        )
        .await?;
    Ok(inserted == 1)
}

pub async fn replace(
    client: &impl GenericClient,
    stale: &ModuleArtifact,
    artifact: &ModuleArtifact,
) -> PlatformResult<bool> {
    let replaced = client
        .execute(
            "UPDATE module_artifacts \
             SET wasm_sha256 = $4, sha256 = $5, size_bytes = $6, created_at = now() \
             WHERE service_id = $1 AND version = $2 AND engine_fingerprint = $3 \
             AND wasm_sha256 = $7 AND sha256 = $8",
            &[
                &artifact.service_id,
                &artifact.version,
                &artifact.engine_fingerprint,
                &artifact.wasm_sha256,
                &artifact.sha256,
                &artifact.size_bytes,
                &stale.wasm_sha256,
                &stale.sha256,
            ],
        )
        .await?;
    Ok(replaced == 1)
}

pub async fn delete_version(
    client: &impl GenericClient,
    service_id: Uuid,
    version: i32,
) -> PlatformResult<Vec<ModuleArtifact>> {
    let rows = client
        .query(
            &format!(
                "DELETE FROM module_artifacts WHERE service_id = $1 AND version = $2 RETURNING {COLUMNS}"
            ),
            &[&service_id, &version],
        )
        .await?;
    Ok(rows.iter().map(ModuleArtifact::from_row).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_keys_name_the_version_engine_and_hash() {
        let artifact = ModuleArtifact {
            service_id: Uuid::nil(),
            version: 3,
            engine_fingerprint: "ab".into(),
            wasm_sha256: "ef".into(),
            sha256: "cd".into(),
            size_bytes: 10,
        };
        assert_eq!(
            artifact.object_key(),
            "wasm/00000000-0000-0000-0000-000000000000/v3/ab/cd.cwasm"
        );
    }
}
