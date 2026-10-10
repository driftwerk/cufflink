//! What setting a test host up can fail with.

use std::fmt;
use std::path::PathBuf;

#[derive(Debug)]
pub enum TestHostError {
    /// No Postgres could be reached or started.
    Unavailable(String),
    Database(postgres::Error),
    File {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The migrations of a directory, or the history they meet, are not
    /// ones a deploy would accept.
    Migration(String),
    /// A request the platform would never send.
    Request(String),
    /// The role a service's statements run as could not be made.
    Role(String),
}

impl fmt::Display for TestHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestHostError::Unavailable(reason) => write!(f, "no test database: {reason}"),
            TestHostError::Database(error) => {
                write!(f, "{}", cufflink_pgcodec::server_message(error))
            }
            TestHostError::File { path, source } => write!(f, "{}: {source}", path.display()),
            TestHostError::Migration(reason)
            | TestHostError::Request(reason)
            | TestHostError::Role(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for TestHostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TestHostError::Database(error) => Some(error),
            TestHostError::File { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<postgres::Error> for TestHostError {
    fn from(error: postgres::Error) -> Self {
        TestHostError::Database(error)
    }
}
