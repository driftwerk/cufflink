//! The feature names a module declares in its `cufflink.features` custom
//! sections: one name per line, written by `cufflink-fn`'s cargo features.

use std::collections::BTreeSet;
use std::fmt;
use wasmparser::{Parser, Payload};

pub const SECTION: &str = "cufflink.features";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmFeaturesError {
    /// The bytes are not a well-formed WASM module.
    Malformed(String),
    /// A declared name is not a feature name (`[a-z0-9_]+`).
    InvalidName(String),
}

impl fmt::Display for WasmFeaturesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(e) => write!(f, "malformed WASM module: {e}"),
            Self::InvalidName(name) => {
                write!(f, "invalid feature name {name:?} in the {SECTION} section")
            }
        }
    }
}

impl std::error::Error for WasmFeaturesError {}

/// Feature names declared by a module's `cufflink.features` custom sections.
pub fn declared(wasm: &[u8]) -> Result<BTreeSet<String>, WasmFeaturesError> {
    Parser::new(0)
        .parse_all(wasm)
        .filter_map(|payload| match payload {
            Ok(Payload::CustomSection(section)) if section.name() == SECTION => {
                Some(Ok(section.data()))
            }
            Ok(_) => None,
            Err(e) => Some(Err(WasmFeaturesError::Malformed(e.to_string()))),
        })
        .try_fold(BTreeSet::new(), |mut names, data| {
            names.extend(section_names(data?)?);
            Ok(names)
        })
}

fn section_names(data: &[u8]) -> Result<Vec<String>, WasmFeaturesError> {
    String::from_utf8_lossy(data)
        .split('\n')
        .filter(|line| !line.is_empty())
        .map(|name| {
            is_feature_name(name)
                .then(|| name.to_string())
                .ok_or_else(|| WasmFeaturesError::InvalidName(name.to_string()))
        })
        .collect()
}

fn is_feature_name(name: &str) -> bool {
    name.bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn module(sections: &str) -> Vec<u8> {
        wat::parse_str(format!("(module {sections} (func (export \"h\")))")).expect("wat")
    }

    fn names(list: &[&str]) -> BTreeSet<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn reads_concatenated_section() {
        let wasm = module(r#"(@custom "cufflink.features" "response_v2\ndb_v2\n")"#);
        assert_eq!(declared(&wasm), Ok(names(&["db_v2", "response_v2"])));
    }

    #[test]
    fn merges_repeated_sections_and_duplicates() {
        let wasm = module(
            r#"(@custom "cufflink.features" "db_v2\n") (@custom "cufflink.features" "cache\ndb_v2\n")"#,
        );
        assert_eq!(declared(&wasm), Ok(names(&["cache", "db_v2"])));
    }

    #[test]
    fn ignores_other_custom_sections() {
        let wasm = module(r#"(@custom "producers" "db_v2\n") (@custom "cufflink" "cache\n")"#);
        assert_eq!(declared(&wasm), Ok(BTreeSet::new()));
    }

    #[test]
    fn module_without_section_is_empty() {
        assert_eq!(declared(&module("")), Ok(BTreeSet::new()));
    }

    #[test]
    fn rejects_invalid_names() {
        for bad in ["DB_V2\n", "db-v2\n", "db v2\n", "db_v2\r\n"] {
            let wasm = module(&format!(
                r#"(@custom "cufflink.features" "{}")"#,
                bad.escape_default()
            ));
            assert!(
                matches!(declared(&wasm), Err(WasmFeaturesError::InvalidName(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn rejects_bytes_that_are_not_wasm() {
        assert!(matches!(
            declared(b"not a module"),
            Err(WasmFeaturesError::Malformed(_))
        ));
    }
}
