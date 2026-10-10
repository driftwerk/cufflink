//! The Postgres a process starts for itself when it is given none: one
//! server for the whole process, stopped when the process exits.

use super::super::error::TestHostError;
use super::apart;
use postgresql_embedded::blocking::PostgreSQL;
use postgresql_embedded::Settings;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
const ADMIN_DATABASE: &str = "postgres";
/// A test's data is dropped with its database, so nothing waits for a disk.
const CONFIGURATION: [(&str, &str); 3] = [
    ("fsync", "off"),
    ("synchronous_commit", "off"),
    ("full_page_writes", "off"),
];

static ADMIN_URL: OnceLock<Result<String, String>> = OnceLock::new();
static RUNNING: Mutex<Option<PostgreSQL>> = Mutex::new(None);

/// Where Postgres is installed, when not where `postgresql_embedded`
/// installs it by default.
const INSTALLATION_DIR: &str = "CUFFLINK_TEST_POSTGRES_DIR";

/// The URL of the server's `postgres` database; the first call starts the
/// server, [`apart`], since the blocking installer drives a runtime of its
/// own.
pub(super) fn admin_url() -> Result<String, TestHostError> {
    ADMIN_URL
        .get_or_init(|| apart(start))
        .clone()
        .map_err(TestHostError::Unavailable)
}

fn start() -> Result<String, String> {
    let defaults = Settings::default();
    let settings = Settings {
        installation_dir: std::env::var_os(INSTALLATION_DIR)
            .filter(|dir| !dir.is_empty())
            .map_or(defaults.installation_dir.clone(), PathBuf::from),
        timeout: Some(COMMAND_TIMEOUT),
        configuration: CONFIGURATION
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect::<HashMap<_, _>>(),
        ..defaults
    };
    let mut server = PostgreSQL::new(settings);
    server
        .setup()
        .map_err(|e| format!("embedded Postgres could not be set up: {e}"))?;
    server
        .start()
        .map_err(|e| format!("embedded Postgres could not be started: {e}"))?;
    let url = server.settings().url(ADMIN_DATABASE);
    let mut running = RUNNING
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // SAFETY: `stop` is an `extern "C" fn` without arguments that does not
    if unsafe { libc::atexit(stop) } != 0 {
        return Err("the embedded Postgres could not be registered to stop at exit".to_string());
    }
    *running = Some(server);
    Ok(url)
}

/// Dropping the server stops it and removes its data directory.
extern "C" fn stop() {
    if let Ok(mut running) = RUNNING.lock() {
        drop(running.take());
    }
}
