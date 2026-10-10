use crate::errors::PlatformResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Runs {
    Native,
    Web,
}

impl Runs {
    pub const ALL: [Runs; 2] = [Runs::Native, Runs::Web];

    pub fn as_str(self) -> &'static str {
        match self {
            Runs::Native => "native",
            Runs::Web => "web",
        }
    }

    pub fn parse(runs: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|known| known.as_str() == runs)
    }

    fn current(self) -> &'static str {
        match self {
            Runs::Native => "s.status = 'active' AND d.status = 'active'",
            Runs::Web => "d.status = 'active' AND d.artifact_s3_key IS NOT NULL",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Current {
    pub service_id: Uuid,
    pub service_name: String,
    pub version: i32,
    pub sha256: Option<String>,
    pub size: Option<i64>,
    pub provides: Option<Value>,
}

pub async fn current(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    runs: Runs,
) -> PlatformResult<Vec<Current>> {
    let rows = pool
        .get()
        .await?
        .query(
            &format!(
                "SELECT s.id, s.name, d.version, d.artifact_hash, d.artifact_size,
                        d.manifest->'provides'
                 FROM deployments d
                 JOIN services s ON s.id = d.service_id AND d.version = s.current_version
                 WHERE s.tenant_id = $1
                   AND d.manifest->>'mode' = $2
                   AND {}
                 ORDER BY s.name",
                runs.current()
            ),
            &[&tenant_id, &runs.as_str()],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| Current {
            service_id: row.get(0),
            service_name: row.get(1),
            version: row.get(2),
            sha256: row.get(3),
            size: row.get(4),
            provides: row.get(5),
        })
        .collect())
}

pub async fn artifact(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    service_id: Uuid,
    sha256: &str,
    runs: Runs,
) -> PlatformResult<Option<(String, i64)>> {
    let row = pool
        .get()
        .await?
        .query_opt(
            "SELECT d.artifact_s3_key, d.artifact_size
             FROM deployments d JOIN services s ON s.id = d.service_id
             WHERE s.id = $1 AND s.tenant_id = $2
               AND d.artifact_hash = $3
               AND d.artifact_s3_key IS NOT NULL
               AND d.artifact_size IS NOT NULL
               AND d.manifest->>'mode' = $4
             ORDER BY d.version DESC
             LIMIT 1",
            &[&service_id, &tenant_id, &sha256, &runs.as_str()],
        )
        .await?;
    Ok(row.map(|row| (row.get(0), row.get(1))))
}

pub async fn runs(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    service_id: Uuid,
    runs: Runs,
) -> PlatformResult<bool> {
    Ok(pool
        .get()
        .await?
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM deployments d JOIN services s ON s.id = d.service_id
                 WHERE s.id = $1 AND s.tenant_id = $2 AND d.manifest->>'mode' = $3
             )",
            &[&service_id, &tenant_id, &runs.as_str()],
        )
        .await?
        .get(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kind_is_the_text_the_tables_hold() {
        for runs in Runs::ALL {
            assert_eq!(Runs::parse(runs.as_str()), Some(runs));
            assert_eq!(
                serde_json::to_value(runs).expect("serialize"),
                serde_json::json!(runs.as_str())
            );
        }
        assert_eq!(Runs::parse("wasm"), None);
    }
}
