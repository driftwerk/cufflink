use cufflink_db::PoolSettings;
use std::time::Duration;
use tokio_postgres::Client;
use tracing::{info, warn};

mod embedded {
    refinery::embed_migrations!("migrations");
}

pub const MIGRATION_LOCK_KEY: i64 = 0x6375_6666_6c69_6e6b;
pub const MIGRATION_LOCK_WAIT: Duration = Duration::from_secs(10 * 60);

#[derive(Clone)]
pub struct Target {
    pub config: tokio_postgres::Config,
    pub settings: PoolSettings,
}

struct OnlineIndex {
    name: &'static str,
    create: &'static str,
}

const ONLINE_INDEXES: [OnlineIndex; 5] = [
    OnlineIndex {
        name: "wasm_jobs_idempotency_idx",
        create: "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS wasm_jobs_idempotency_idx \
                 ON wasm_jobs (service_id, idempotency_key) WHERE idempotency_key IS NOT NULL",
    },
    OnlineIndex {
        name: "wasm_jobs_due_idx",
        create: "CREATE INDEX CONCURRENTLY IF NOT EXISTS wasm_jobs_due_idx \
                 ON wasm_jobs (next_attempt_at) WHERE status = 'pending'",
    },
    OnlineIndex {
        name: "wasm_jobs_lease_idx",
        create: "CREATE INDEX CONCURRENTLY IF NOT EXISTS wasm_jobs_lease_idx \
                 ON wasm_jobs (lease_until) WHERE status = 'running'",
    },
    OnlineIndex {
        name: "wasm_jobs_schedule_idx",
        create: "CREATE INDEX CONCURRENTLY IF NOT EXISTS wasm_jobs_schedule_idx \
                 ON wasm_jobs (service_id, schedule_name) \
                 WHERE schedule_name IS NOT NULL AND status IN ('pending', 'running')",
    },
    OnlineIndex {
        name: "wasm_jobs_dead_letter_idx",
        create: "CREATE INDEX CONCURRENTLY IF NOT EXISTS wasm_jobs_dead_letter_idx \
                 ON wasm_jobs (finished_at) \
                 WHERE status = 'failed' AND dead_lettered_at IS NULL \
                   AND error_class IS NOT NULL",
    },
];

pub async fn run_online(client: &Client) -> Result<(), tokio_postgres::Error> {
    for index in &ONLINE_INDEXES {
        let interrupted = client
            .query_opt(
                "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                 WHERE c.relname = $1 AND c.relnamespace = current_schema()::regnamespace
                   AND NOT i.indisvalid",
                &[&index.name],
            )
            .await?
            .is_some();
        if interrupted {
            warn!(
                index = index.name,
                "Dropping the index an interrupted build left"
            );
            client
                .batch_execute(&format!(
                    "DROP INDEX CONCURRENTLY IF EXISTS {}",
                    cufflink_db::quote_ident(index.name)
                ))
                .await?;
        }
        client.batch_execute(index.create).await?;
    }
    Ok(())
}

pub async fn run(target: &Target) -> eyre::Result<()> {
    let settings = target.settings.clone().statement_timeout(None);
    let mut client = cufflink_db::connect_single(target.config.clone(), &settings).await?;
    tokio::time::timeout(
        MIGRATION_LOCK_WAIT,
        client.execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_KEY]),
    )
    .await
    .map_err(|_| {
        eyre::eyre!("timed out waiting {MIGRATION_LOCK_WAIT:?} for the migration lock")
    })??;

    let outcome = apply(&mut client).await;
    let unlocked = client
        .execute("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_KEY])
        .await;
    let applied = outcome?;
    unlocked?;
    match applied {
        0 => info!("Platform migrations up to date"),
        n => info!(applied = n, "Platform migrations applied"),
    }
    Ok(())
}

async fn apply(client: &mut Client) -> eyre::Result<usize> {
    let report = embedded::migrations::runner()
        .set_grouped(false)
        .set_abort_missing(false)
        .run_async(client)
        .await?;
    run_online(client).await?;
    Ok(report.applied_migrations().len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_key_spells_cufflink() {
        assert_eq!(MIGRATION_LOCK_KEY.to_be_bytes(), *b"cufflink");
        assert_eq!(MIGRATION_LOCK_WAIT, Duration::from_secs(600));
    }

    #[test]
    fn an_online_index_is_built_without_locking_writers_out_and_only_once() {
        for index in &ONLINE_INDEXES {
            let head = index
                .create
                .split(" ON ")
                .next()
                .unwrap_or_default()
                .trim_end();
            assert!(
                head == format!("CREATE INDEX CONCURRENTLY IF NOT EXISTS {}", index.name)
                    || head
                        == format!(
                            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {}",
                            index.name
                        ),
                "{head}"
            );
        }
    }

    #[test]
    fn a_migration_builds_no_index_on_a_table_with_rows_and_rewrites_none() {
        let v4: String = include_str!("../../migrations/V4__r3.sql")
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let statements: Vec<&str> = v4
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
            .collect();
        let of_jobs: Vec<&str> = statements
            .iter()
            .copied()
            .filter(|statement| statement.contains("wasm_jobs"))
            .collect();
        assert!(
            of_jobs
                .iter()
                .all(|statement| statement.starts_with("ALTER TABLE wasm_jobs")),
            "wasm_jobs is only altered"
        );
        assert_eq!(
            of_jobs
                .iter()
                .filter(|statement| statement.contains("ADD COLUMN"))
                .count(),
            1,
            "its columns are added under one lock"
        );
        assert_eq!(
            statements.get(1).copied(),
            of_jobs.first().copied(),
            "before any other table is locked"
        );
        assert_eq!(
            of_jobs.get(1).copied(),
            Some("ALTER TABLE wasm_jobs ALTER COLUMN published_at DROP DEFAULT"),
            "only the jobs an earlier release enqueued were published when V4 ran"
        );
        assert!(statements
            .iter()
            .all(|statement| !statement.to_uppercase().starts_with("UPDATE ")));
        assert_eq!(statements.first(), Some(&"SET LOCAL lock_timeout = '10s'"));
    }

    #[test]
    fn migrations_are_versioned_in_order() {
        let runner = embedded::migrations::runner();
        let mut names: Vec<(u32, &str)> = runner
            .get_migrations()
            .iter()
            .map(|m| (m.version(), m.name()))
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![
                (1, "baseline"),
                (2, "r1a_policy_and_isolation"),
                (3, "r2"),
                (4, "r3"),
                (5, "extensions"),
                (6, "runner_keys")
            ]
        );
    }
}
