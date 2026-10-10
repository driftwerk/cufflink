//! What a runner that applies migrations sends and checks, whichever
//! client it holds: the lock, the history's rows, the time bounds, how a
//! migration is caught taking the transaction or the role from the
//! runner, and which invalid indexes an interrupted build left.

use crate::history::QualifiedName;
use crate::lock::{legacy_table_lock_key, platform_lock_key};
use cufflink_types::{MigrationLock, SqlMigration};
use std::time::Duration;

/// How long a run waits for its lock and how long its statements may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    pub lock_wait: Duration,
    pub statement: Duration,
    pub lock: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            lock_wait: Duration::from_secs(600),
            statement: Duration::from_secs(600),
            lock: Duration::from_secs(10),
        }
    }
}

/// Takes the run's advisory lock if it is free, answering whether it did.
pub const TRY_LOCK: &str = "SELECT pg_try_advisory_lock($1)";
pub const UNLOCK: &str = "SELECT pg_advisory_unlock($1)";

/// The key of the advisory lock a run holds on `history`.
pub fn lock_key(lock: MigrationLock, history: &QualifiedName) -> i64 {
    let table = history.dotted();
    match lock {
        MigrationLock::Platform => platform_lock_key(&table),
        MigrationLock::LegacyTableHash => legacy_table_lock_key(&table),
    }
}

/// The rows of `history`: `version`, `name`, `applied_on`, `checksum`, as
/// text but the version, in version order.
pub fn applied_rows(history: &QualifiedName) -> String {
    format!(
        "SELECT version::int4, name::text, applied_on::text, checksum::text \
         FROM {} ORDER BY version",
        history.quoted()
    )
}

/// Records a migration in `history`, taking `version`, `name`,
/// `applied_on` and `checksum`.
pub fn record_statement(history: &QualifiedName) -> String {
    format!(
        "INSERT INTO {} (version, name, applied_on, checksum) VALUES ($1, $2, $3, $4)",
        history.quoted()
    )
}

/// The transaction and the role statements run in now, answered as
/// [`Standing`]'s two fields.
pub const STANDING: &str = "SELECT txid_current()::text, current_user::text";

/// The transaction and the role statements run in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Standing {
    pub transaction: String,
    pub role: String,
}

impl Standing {
    /// How a migration that began in `self` and ended in `after` escaped.
    /// The role is the runner's to keep only when the run imposes one.
    pub fn escape(&self, after: &Self, role_imposed: bool) -> Option<Escape> {
        if self.transaction != after.transaction {
            Some(Escape::Transaction)
        } else if role_imposed && self.role != after.role {
            Some(Escape::Role)
        } else {
            None
        }
    }
}

/// How a migration left what the runner ran it in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Escape {
    Transaction,
    Role,
}

impl std::fmt::Display for Escape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Escape::Transaction => "ended the transaction it ran in",
            Escape::Role => "changed the role it ran as",
        })
    }
}

/// Why a migration that escaped is refused rather than recorded.
pub fn escaped(migration: &SqlMigration, escape: Escape) -> String {
    format!(
        "migration V{}__{} {escape}; it is not recorded, and what it committed itself stays. \
         Transaction and role control belong to the runner: a migration that must run \
         outside a transaction begins with '{}'",
        migration.version,
        migration.name,
        SqlMigration::NO_TRANSACTION_MARKER
    )
}

/// The invalid indexes of a schema, taking its folded name.
pub const INVALID_INDEXES: &str = "SELECT c.relname::text FROM pg_index i \
     JOIN pg_class c ON c.oid = i.indexrelid \
     JOIN pg_namespace n ON n.oid = c.relnamespace \
     WHERE n.nspname = $1 AND NOT i.indisvalid ORDER BY 1";

/// The invalid indexes of the schema that `sql` names: a concurrent build
/// the last attempt left unfinished, which would make this attempt fail.
pub fn interrupted_builds<'a>(invalid: &'a [String], sql: &str) -> Vec<&'a str> {
    let named = |index: &str| {
        sql.match_indices(index).any(|(at, _)| {
            let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
            let before = sql[..at].chars().next_back();
            let after = sql[at + index.len()..].chars().next();
            !before.is_some_and(is_word) && !after.is_some_and(is_word)
        })
    };
    invalid
        .iter()
        .map(String::as_str)
        .filter(|index| !index.is_empty() && named(index))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history() -> QualifiedName {
        QualifiedName::parse("schema_history", "loan_service").expect("a history")
    }

    #[test]
    fn the_history_is_read_and_written_by_its_quoted_name() {
        assert_eq!(
            applied_rows(&history()),
            "SELECT version::int4, name::text, applied_on::text, checksum::text \
             FROM \"loan_service\".\"schema_history\" ORDER BY version"
        );
        assert_eq!(
            record_statement(&history()),
            "INSERT INTO \"loan_service\".\"schema_history\" (version, name, applied_on, \
             checksum) VALUES ($1, $2, $3, $4)"
        );
    }

    #[test]
    fn the_lock_is_the_one_the_history_declares() {
        let history = history();
        assert_eq!(
            lock_key(MigrationLock::LegacyTableHash, &history),
            legacy_table_lock_key("loan_service.schema_history")
        );
        assert_eq!(
            lock_key(MigrationLock::Platform, &history),
            platform_lock_key("loan_service.schema_history")
        );
    }

    #[test]
    fn timeouts_default_to_the_documented_values() {
        assert_eq!(
            Timeouts::default(),
            Timeouts {
                lock_wait: Duration::from_secs(600),
                statement: Duration::from_secs(600),
                lock: Duration::from_secs(10),
            }
        );
    }

    #[test]
    fn a_migration_may_not_take_the_transaction_or_an_imposed_role() {
        let standing = |transaction: &str, role: &str| Standing {
            transaction: transaction.into(),
            role: role.into(),
        };
        let before = standing("811", "loan_service_user");
        assert_eq!(before.escape(&before.clone(), true), None);
        assert_eq!(
            before.escape(&standing("812", "loan_service_user"), false),
            Some(Escape::Transaction)
        );
        assert_eq!(
            before.escape(&standing("811", "platform"), true),
            Some(Escape::Role)
        );
        assert_eq!(
            before.escape(&standing("811", "platform"), false),
            None,
            "with no role imposed there is none to escape"
        );
        let migration = SqlMigration {
            version: 8,
            name: "commits".into(),
            sql: String::new(),
            no_transaction: false,
        };
        assert!(escaped(&migration, Escape::Transaction).starts_with(
            "migration V8__commits ended the transaction it ran in; it is not recorded"
        ));
        assert!(escaped(&migration, Escape::Role)
            .starts_with("migration V8__commits changed the role it ran as;"));
    }

    #[test]
    fn an_invalid_index_is_recovered_only_when_the_migration_names_it() {
        let invalid = vec![
            "notes_kind".to_string(),
            "notes_kind_created".to_string(),
            "other".to_string(),
        ];
        assert_eq!(
            interrupted_builds(
                &invalid,
                "CREATE INDEX CONCURRENTLY notes_kind ON notes (kind);"
            ),
            ["notes_kind"]
        );
        assert_eq!(
            interrupted_builds(
                &invalid,
                "CREATE INDEX CONCURRENTLY \"notes_kind_created\" ON notes (kind, created_at);"
            ),
            ["notes_kind_created"]
        );
        assert!(interrupted_builds(&invalid, "CREATE INDEX another ON notes (id);").is_empty());
        assert!(interrupted_builds(&[String::new()], "SELECT 1;").is_empty());
    }
}
