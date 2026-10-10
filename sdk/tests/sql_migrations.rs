use cufflink::manifest::{HistoryMode, MigrationHistory, MigrationLock};

cufflink::service! {
    name: "loan-service",
    tables: [],
    migrations: "tests/fixtures/migrations",
    migration_history: ("schema_history", baseline = 6, lock = legacy_table_hash, mode = verify_only),
}

#[test]
fn the_manifest_carries_the_directory() {
    let manifest = __cufflink_manifest().expect("manifest");
    assert_eq!(
        manifest
            .migrations
            .iter()
            .map(|m| (m.version, m.name.as_str(), m.sql.as_str(), m.no_transaction))
            .collect::<Vec<_>>(),
        [
            (
                1,
                "initial_tables",
                include_str!("fixtures/migrations/V1__initial_tables.sql"),
                false
            ),
            (
                6,
                "add_loan_kind",
                "ALTER TABLE loans ADD COLUMN kind TEXT;\r\n",
                false
            ),
            (
                7,
                "kind_index",
                include_str!("fixtures/migrations/V7__kind_index.sql"),
                true
            ),
        ]
    );
    assert_eq!(
        manifest.migration_history,
        Some(MigrationHistory {
            table: "schema_history".into(),
            baseline: Some(6),
            lock: MigrationLock::LegacyTableHash,
            mode: HistoryMode::VerifyOnly,
        })
    );
    assert_eq!(manifest.required_platform_features, ["sql_migrations"]);
}
