use crate::errors::PlatformResult;
use crate::nats_manager::consumers::apply::Handover;
use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HandoverRow {
    pub deploy_version: i32,
    pub stream: String,
    pub from_name: String,
    pub to_name: String,
    pub mode: String,
    pub start_sequence: Option<i64>,
    pub created_at: DateTime<Utc>,
}

fn sequence(handover: &Handover) -> Option<i64> {
    handover
        .start_sequence
        .and_then(|sequence| i64::try_from(sequence).ok())
}

pub async fn record(
    client: &impl GenericClient,
    service_id: Uuid,
    deploy_version: i32,
    handover: &Handover,
) -> PlatformResult<()> {
    client
        .execute(
            "INSERT INTO consumer_handovers
                 (service_id, deploy_version, stream, from_name, to_name, mode, start_sequence)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &service_id,
                &deploy_version,
                &handover.stream,
                &handover.from,
                &handover.to,
                &handover.mode,
                &sequence(handover),
            ],
        )
        .await?;
    Ok(())
}

pub async fn list_for_service(
    client: &impl GenericClient,
    service_id: Uuid,
    limit: i64,
) -> PlatformResult<Vec<HandoverRow>> {
    Ok(client
        .query(
            "SELECT deploy_version, stream, from_name, to_name, mode, start_sequence, created_at
             FROM consumer_handovers
             WHERE service_id = $1
             ORDER BY created_at DESC, id
             LIMIT $2",
            &[&service_id, &limit],
        )
        .await?
        .iter()
        .map(|row| HandoverRow {
            deploy_version: row.get("deploy_version"),
            stream: row.get("stream"),
            from_name: row.get("from_name"),
            to_name: row.get("to_name"),
            mode: row.get("mode"),
            start_sequence: row.get("start_sequence"),
            created_at: row.get("created_at"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_start_sequence_is_recorded_when_the_column_holds_it() {
        let handover = |start_sequence| Handover {
            stream: "DW_DEFAULT".into(),
            from: "stock_service_on_created".into(),
            to: "default_stock-service_on_created_1a2b3c4d".into(),
            mode: "create_then_delete",
            start_sequence,
        };
        assert_eq!(sequence(&handover(Some(81_234))), Some(81_234));
        assert_eq!(sequence(&handover(None)), None);
        assert_eq!(sequence(&handover(Some(u64::MAX))), None);
    }
}
