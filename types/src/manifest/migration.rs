use serde::{Deserialize, Serialize};

/// One `V{version}__{name}.sql` file of the service's migrations directory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlMigration {
    pub version: u32,
    pub name: String,
    /// The file's exact text: its checksum covers every byte, so it is
    /// never normalised.
    pub sql: String,
    /// Run outside a transaction, as `CREATE INDEX CONCURRENTLY` requires:
    /// the file's first line is [`SqlMigration::NO_TRANSACTION_MARKER`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_transaction: bool,
}

impl SqlMigration {
    pub const NO_TRANSACTION_MARKER: &'static str = "-- cufflink:no-transaction";

    /// Whether `sql` opts out of the transaction a migration runs in.
    pub fn declares_no_transaction(sql: &str) -> bool {
        sql.trim_start_matches('\u{feff}')
            .lines()
            .next()
            .is_some_and(|line| line.trim() == Self::NO_TRANSACTION_MARKER)
    }

    /// `V{version}__{name}`, the name the migration's checksum covers.
    pub fn file_stem(&self) -> String {
        format!("V{}__{}", self.version, self.name)
    }
}

/// Where the service's applied migrations are recorded, for a service
/// continuing a history another runner began.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationHistory {
    /// `table`, in the service's schema, or `schema.table`.
    pub table: String,
    /// Applied versions up to this one are accepted without a local file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<u32>,
    #[serde(default, skip_serializing_if = "MigrationLock::is_platform")]
    pub lock: MigrationLock,
    #[serde(default, skip_serializing_if = "HistoryMode::is_apply")]
    pub mode: HistoryMode,
}

impl MigrationHistory {
    /// The history table of a service that declares none.
    pub const DEFAULT_TABLE: &'static str = "refinery_schema_history";
}

impl Default for MigrationHistory {
    fn default() -> Self {
        Self {
            table: Self::DEFAULT_TABLE.to_string(),
            baseline: None,
            lock: MigrationLock::default(),
            mode: HistoryMode::default(),
        }
    }
}

/// The advisory lock a migration run holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationLock {
    #[default]
    Platform,
    /// The key a legacy refinery runner derives from the history table's
    /// name, so the two never run at once.
    LegacyTableHash,
}

impl MigrationLock {
    pub fn is_platform(&self) -> bool {
        *self == MigrationLock::Platform
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryMode {
    #[default]
    Apply,
    /// Check the history against the local files and apply nothing: another
    /// runner still owns the migrations.
    VerifyOnly,
}

impl HistoryMode {
    pub fn is_apply(&self) -> bool {
        *self == HistoryMode::Apply
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_migration_keeps_its_bytes() {
        let sql = "\u{feff}CREATE TABLE a (\r\n  id INT\r\n);\r\n";
        let migration = SqlMigration {
            version: 6,
            name: "add_loan_kind".into(),
            sql: sql.into(),
            no_transaction: false,
        };
        let wire = json!({"version": 6, "name": "add_loan_kind", "sql": sql});
        assert_eq!(serde_json::to_value(&migration).unwrap(), wire);
        let parsed: SqlMigration = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed.sql.as_bytes(), sql.as_bytes());
        assert_eq!(parsed.file_stem(), "V6__add_loan_kind");
    }

    #[test]
    fn the_marker_is_the_first_line() {
        for sql in [
            "-- cufflink:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (c);",
            "-- cufflink:no-transaction",
            "  -- cufflink:no-transaction  \r\nCREATE INDEX CONCURRENTLY i ON t (c);",
            "\u{feff}-- cufflink:no-transaction\nSELECT 1;",
        ] {
            assert!(SqlMigration::declares_no_transaction(sql), "{sql:?}");
        }
        for sql in [
            "",
            "SELECT 1;\n-- cufflink:no-transaction",
            "\n-- cufflink:no-transaction\nSELECT 1;",
            "-- cufflink:no-transaction please",
            "--cufflink:no-transaction",
        ] {
            assert!(!SqlMigration::declares_no_transaction(sql), "{sql:?}");
        }
        let migration = SqlMigration {
            version: 7,
            name: "renewal_index".into(),
            sql: "-- cufflink:no-transaction\nSELECT 1;".into(),
            no_transaction: true,
        };
        assert_eq!(
            serde_json::to_value(&migration).unwrap()["no_transaction"],
            json!(true)
        );
    }

    #[test]
    fn history_round_trips() {
        let default = MigrationHistory::default();
        assert_eq!(
            serde_json::to_value(&default).unwrap(),
            json!({"table": "refinery_schema_history"})
        );
        let legacy = MigrationHistory {
            table: "public.schema_history_deferred".into(),
            baseline: Some(15),
            lock: MigrationLock::LegacyTableHash,
            mode: HistoryMode::VerifyOnly,
        };
        let wire = json!({
            "table": "public.schema_history_deferred",
            "baseline": 15,
            "lock": "legacy_table_hash",
            "mode": "verify_only"
        });
        assert_eq!(serde_json::to_value(&legacy).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<MigrationHistory>(wire).unwrap(),
            legacy
        );
    }
}
