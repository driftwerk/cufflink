use serde::{Deserialize, Serialize};

/// Description of the changes a deploy is about to apply, passed as the
/// payload to the `on_migrate` hook so user code can decide which backfill
/// blocks should fire.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaDiff {
    #[serde(default)]
    pub added_tables: Vec<String>,
    #[serde(default)]
    pub dropped_tables: Vec<String>,
    /// (table, column) pairs for newly added columns. In phase 2 these
    /// already exist in the database (added in phase 1) but may be NULL on
    /// existing rows; the hook is the place to backfill them.
    #[serde(default)]
    pub added_columns: Vec<(String, String)>,
    /// (table, column) pairs for columns that will be dropped in phase 3.
    /// During phase 2 these are still present in the database, so the hook
    /// can read them to populate new columns before they disappear.
    #[serde(default)]
    pub dropped_columns: Vec<(String, String)>,
    /// (table, column, old_type, new_type)
    #[serde(default)]
    pub type_changes: Vec<(String, String, String, String)>,
    /// (table, column, new_nullable)
    #[serde(default)]
    pub nullability_changes: Vec<(String, String, bool)>,
}

impl SchemaDiff {
    pub fn added_column(&self, table: &str, col: &str) -> bool {
        self.added_columns
            .iter()
            .any(|(t, c)| t == table && c == col)
    }

    pub fn dropped_column(&self, table: &str, col: &str) -> bool {
        self.dropped_columns
            .iter()
            .any(|(t, c)| t == table && c == col)
    }

    pub fn added_table(&self, table: &str) -> bool {
        self.added_tables.iter().any(|t| t == table)
    }

    pub fn dropped_table(&self, table: &str) -> bool {
        self.dropped_tables.iter().any(|t| t == table)
    }

    pub fn is_empty(&self) -> bool {
        self.added_tables.is_empty()
            && self.dropped_tables.is_empty()
            && self.added_columns.is_empty()
            && self.dropped_columns.is_empty()
            && self.type_changes.is_empty()
            && self.nullability_changes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_diff_helpers() {
        let diff = SchemaDiff {
            added_tables: vec!["batches".into()],
            dropped_tables: vec!["legacy_batches".into()],
            added_columns: vec![("sailings".into(), "min".into())],
            dropped_columns: vec![("sailings".into(), "midpoint".into())],
            type_changes: vec![],
            nullability_changes: vec![],
        };
        assert!(!diff.is_empty());
        assert!(diff.added_table("batches"));
        assert!(!diff.added_table("nope"));
        assert!(diff.dropped_table("legacy_batches"));
        assert!(diff.added_column("sailings", "min"));
        assert!(!diff.added_column("sailings", "max"));
        assert!(diff.dropped_column("sailings", "midpoint"));
    }

    #[test]
    fn test_schema_diff_is_empty_default() {
        assert!(SchemaDiff::default().is_empty());
    }

    #[test]
    fn test_schema_diff_serialization_roundtrip() {
        let diff = SchemaDiff {
            added_tables: vec![],
            dropped_tables: vec![],
            added_columns: vec![("t".into(), "c".into())],
            dropped_columns: vec![],
            type_changes: vec![("t".into(), "c".into(), "TEXT".into(), "INTEGER".into())],
            nullability_changes: vec![("t".into(), "c".into(), false)],
        };
        let json = serde_json::to_string(&diff).unwrap();
        let parsed: SchemaDiff = serde_json::from_str(&json).unwrap();
        assert!(parsed.added_column("t", "c"));
        assert_eq!(parsed.type_changes.len(), 1);
        assert_eq!(parsed.nullability_changes.len(), 1);
    }
}
