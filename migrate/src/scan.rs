//! The migration files of a directory.

use cufflink_types::manifest::validate::{is_migration_name, MAX_MIGRATION_VERSION};
use cufflink_types::SqlMigration;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ScanError {
    #[error("cannot read {}: {reason}", path.display())]
    Unreadable { path: PathBuf, reason: String },
    #[error("{file}: is not named V{{version}}__{{name}}.sql")]
    BadName { file: String },
    #[error(
        "{file}: unversioned (U) migrations are not supported; name it V{{version}}__{{name}}.sql"
    )]
    Unversioned { file: String },
    #[error("{file}: version {version} is above {MAX_MIGRATION_VERSION}")]
    VersionTooLarge { file: String, version: u64 },
    #[error("{file}: is not UTF-8")]
    NotUtf8 { file: String },
    #[error("{second}: version {version} is already taken by {first}")]
    Duplicate {
        version: u32,
        first: String,
        second: String,
    },
}

/// The version and name of `V{version}__{name}.sql`.
fn parse_file_name(file: &str) -> Result<(u32, String), ScanError> {
    let bad = || ScanError::BadName {
        file: file.to_string(),
    };
    let stem = file.strip_suffix(".sql").ok_or_else(bad)?;
    let (prefix, name) = stem.split_once("__").ok_or_else(bad)?;
    let digits = match prefix.split_at_checked(1) {
        Some(("V", digits)) => Ok(digits),
        Some(("U", digits)) if is_version(digits) => Err(ScanError::Unversioned {
            file: file.to_string(),
        }),
        _ => Err(bad()),
    }?;
    if !is_version(digits) || !is_migration_name(name) {
        return Err(bad());
    }
    let version = digits.parse::<u64>().unwrap_or(u64::MAX);
    u32::try_from(version)
        .ok()
        .filter(|version| *version <= MAX_MIGRATION_VERSION)
        .map(|version| (version, name.to_string()))
        .ok_or_else(|| ScanError::VersionTooLarge {
            file: file.to_string(),
            version,
        })
}

fn is_version(digits: &str) -> bool {
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

fn unreadable(path: &Path) -> impl Fn(std::io::Error) -> ScanError + '_ {
    move |e| ScanError::Unreadable {
        path: path.to_path_buf(),
        reason: e.to_string(),
    }
}

/// A migration and the file it was read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationFile {
    pub path: PathBuf,
    pub migration: SqlMigration,
}

fn read(path: &Path, file: &str) -> Result<SqlMigration, ScanError> {
    let (version, name) = parse_file_name(file)?;
    let sql = String::from_utf8(std::fs::read(path).map_err(unreadable(path))?).map_err(|_| {
        ScanError::NotUtf8 {
            file: file.to_string(),
        }
    })?;
    Ok(SqlMigration {
        version,
        name,
        no_transaction: SqlMigration::declares_no_transaction(&sql),
        sql,
    })
}

/// The `.sql` files directly in `dir`, by name.
fn sql_files(dir: &Path) -> Result<Vec<(PathBuf, String)>, ScanError> {
    let mut files: Vec<(PathBuf, String)> = std::fs::read_dir(dir)
        .map_err(unreadable(dir))?
        .map(|entry| entry.map(|entry| entry.path()).map_err(unreadable(dir)))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|path| path.is_file())
        .filter_map(|path| {
            let file = path.file_name()?.to_string_lossy().into_owned();
            file.ends_with(".sql").then_some((path, file))
        })
        .collect();
    files.sort();
    Ok(files)
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The migration files of `dir`, ascending. Every `.sql` file there is a
/// migration; anything else is left alone.
pub fn scan_files(dir: &Path) -> Result<Vec<MigrationFile>, ScanError> {
    let mut found: Vec<MigrationFile> = sql_files(dir)?
        .into_iter()
        .map(|(path, file)| read(&path, &file).map(|migration| MigrationFile { path, migration }))
        .collect::<Result<_, _>>()?;
    found.sort_by_key(|file| file.migration.version);
    match found
        .windows(2)
        .find(|pair| pair[0].migration.version == pair[1].migration.version)
    {
        Some(pair) => Err(ScanError::Duplicate {
            version: pair[0].migration.version,
            first: file_name(&pair[0].path),
            second: file_name(&pair[1].path),
        }),
        None => Ok(found),
    }
}

/// The migrations of `dir`, ascending, each with the exact text of its
/// file.
pub fn scan_dir(dir: &Path) -> Result<Vec<SqlMigration>, ScanError> {
    scan_files(dir).map(|files| files.into_iter().map(|file| file.migration).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(files: &[(&str, &[u8])]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("temp dir");
        for (name, bytes) in files {
            std::fs::write(dir.path().join(name), bytes).expect("write");
        }
        dir
    }

    #[test]
    fn files_are_read_in_version_order_with_their_exact_text() {
        let crlf = "ALTER TABLE loans ADD COLUMN kind TEXT;\r\n\r\n";
        let bom =
            "\u{feff}-- cufflink:no-transaction\nCREATE INDEX CONCURRENTLY i ON loans (id);\n";
        let dir = dir(&[
            ("V10__later.sql", b"SELECT 10;"),
            ("V6__add_loan_kind.sql", crlf.as_bytes()),
            ("V1__initial_tables.sql", b"CREATE TABLE loans ();\n"),
            ("V007__renewal_index.sql", bom.as_bytes()),
            ("README.md", b"not a migration"),
            ("loans.sql.bak", b"nor this"),
        ]);
        std::fs::create_dir(dir.path().join("archive.sql")).expect("subdirectory");
        let found = scan_dir(dir.path()).expect("scan");
        assert_eq!(
            found
                .iter()
                .map(|m| (m.version, m.name.as_str(), m.no_transaction))
                .collect::<Vec<_>>(),
            [
                (1, "initial_tables", false),
                (6, "add_loan_kind", false),
                (7, "renewal_index", true),
                (10, "later", false)
            ]
        );
        assert_eq!(found[1].sql, crlf);
        assert_eq!(found[2].sql, bom);
        assert_eq!(
            scan_dir(tempfile::tempdir().expect("dir").path()),
            Ok(vec![])
        );
    }

    #[test]
    fn a_file_that_is_not_a_migration_is_refused_by_name() {
        for (file, expected) in [
            (
                "U1__repeatable.sql",
                "U1__repeatable.sql: unversioned (U) migrations are not supported; \
                 name it V{version}__{name}.sql",
            ),
            (
                "initial.sql",
                "initial.sql: is not named V{version}__{name}.sql",
            ),
            (
                "V1_initial.sql",
                "V1_initial.sql: is not named V{version}__{name}.sql",
            ),
            (
                "Vx__initial.sql",
                "Vx__initial.sql: is not named V{version}__{name}.sql",
            ),
            (
                "V-1__initial.sql",
                "V-1__initial.sql: is not named V{version}__{name}.sql",
            ),
            (
                "V1__bad-name.sql",
                "V1__bad-name.sql: is not named V{version}__{name}.sql",
            ),
            (
                "V1__.sql",
                "V1__.sql: is not named V{version}__{name}.sql",
            ),
            (
                "V2147483648__big.sql",
                "V2147483648__big.sql: version 2147483648 is above 2147483647",
            ),
            (
                "V99999999999999999999999__huge.sql",
                "V99999999999999999999999__huge.sql: version 18446744073709551615 is above 2147483647",
            ),
        ] {
            let dir = dir(&[(file, b"SELECT 1;")]);
            assert_eq!(
                scan_dir(dir.path()).expect_err(file).to_string(),
                expected
            );
        }
    }

    #[test]
    fn a_version_belongs_to_one_file() {
        let dir = dir(&[
            ("V1__initial.sql", b"SELECT 1;"),
            ("V01__again.sql", b"SELECT 1;"),
        ]);
        assert_eq!(
            scan_dir(dir.path()),
            Err(ScanError::Duplicate {
                version: 1,
                first: "V01__again.sql".into(),
                second: "V1__initial.sql".into()
            })
        );
    }

    #[test]
    fn unreadable_input_is_an_error() {
        let dir = dir(&[("V1__latin1.sql", &[0x53, 0x45, 0x4c, 0xe9][..])]);
        assert_eq!(
            scan_dir(dir.path()),
            Err(ScanError::NotUtf8 {
                file: "V1__latin1.sql".into()
            })
        );
        let missing = dir.path().join("absent");
        assert!(matches!(
            scan_dir(&missing),
            Err(ScanError::Unreadable { path, .. }) if path == missing
        ));
    }
}
