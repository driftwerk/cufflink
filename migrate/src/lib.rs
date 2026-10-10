//! SQL migrations of a service: which files a directory holds, what a
//! history table records, and what is left to apply. Checksums, the
//! history table and the rows written to it are refinery's, so a history
//! refinery began is continued and refinery reads one begun here.
//!
//! Nothing here touches a database; the runners that do share these rules.

mod exec;
mod history;
mod lock;
mod plan;
mod scan;

pub use exec::{
    applied_rows, escaped, interrupted_builds, lock_key, record_statement, Escape, Standing,
    Timeouts, INVALID_INDEXES, STANDING, TRY_LOCK, UNLOCK,
};
pub use history::{
    applied_on_now, history_ddl, history_version, parse_applied, Applied, QualifiedName,
    HISTORY_DDL,
};
pub use lock::{legacy_table_lock_key, platform_key, platform_lock_key};
pub use plan::{checksum, plan, Plan, PlanError};
pub use scan::{scan_dir, scan_files, MigrationFile, ScanError};
