use cufflink_fn::testing::TestDb;
use std::path::{Path, PathBuf};

pub fn database() -> Option<TestDb> {
    match TestDb::start() {
        Ok(db) => Some(db),
        Err(reason) => {
            use std::io::Write;
            let _ = writeln!(std::io::stderr().lock(), "skipped: {reason}");
            if std::env::var_os("CUFFLINK_CI").is_some_and(|set| !set.is_empty()) {
                panic!("test skipped in CI: {reason}");
            }
            None
        }
    }
}

pub fn loan_migrations() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/loan_service/migrations")
}
