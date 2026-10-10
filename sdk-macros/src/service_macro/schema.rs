use super::emit::optional;
use crate::keywords::{assigned_choice, assigned_number, choice, once, one_of, trailing};
use cufflink_migrate::{scan_files, MigrationFile};
use cufflink_types::manifest::validate::{
    validate_migration_history, validate_schema_name, validate_sql_migrations,
};
use cufflink_types::{
    ConsumerNaming, HistoryMode, MigrationHistory, MigrationLock, SchemaOwnership, SqlMigration,
};
use proc_macro2::TokenStream;
use quote::quote;
use std::path::{Path, PathBuf};
use syn::parse::ParseStream;
use syn::{Ident, LitStr};

const OWNERSHIPS: [(&str, SchemaOwnership); 2] = [
    ("managed", SchemaOwnership::Managed),
    ("adopted", SchemaOwnership::Adopted),
];

const NAMINGS: [(&str, ConsumerNaming); 2] = [
    ("legacy", ConsumerNaming::Legacy),
    ("scoped", ConsumerNaming::Scoped),
];

/// `schema: managed` or `schema: adopted`.
pub(super) fn parse_ownership(input: ParseStream) -> syn::Result<SchemaOwnership> {
    choice(&input.parse::<Ident>()?, "schema ownership", &OWNERSHIPS)
}

/// `schema_name: "loan_service"`.
pub(super) fn parse_schema_name(input: ParseStream) -> syn::Result<LitStr> {
    let name: LitStr = input.parse()?;
    validate_schema_name("schema_name", &name.value())
        .map(|()| name.clone())
        .map_err(|reason| syn::Error::new(name.span(), reason))
}

/// `consumers: legacy` or `consumers: scoped`.
pub(super) fn parse_consumers(input: ParseStream) -> syn::Result<ConsumerNaming> {
    choice(&input.parse::<Ident>()?, "consumer naming", &NAMINGS)
}

pub(super) fn ownership_tokens(ownership: SchemaOwnership) -> TokenStream {
    match ownership {
        SchemaOwnership::Managed => quote! { ::cufflink::manifest::SchemaOwnership::Managed },
        SchemaOwnership::Adopted => quote! { ::cufflink::manifest::SchemaOwnership::Adopted },
    }
}

pub(super) fn consumers_tokens(naming: ConsumerNaming) -> TokenStream {
    match naming {
        ConsumerNaming::Legacy => quote! { ::cufflink::manifest::ConsumerNaming::Legacy },
        ConsumerNaming::Scoped => quote! { ::cufflink::manifest::ConsumerNaming::Scoped },
    }
}

const LOCKS: [(&str, MigrationLock); 2] = [
    ("platform", MigrationLock::Platform),
    ("legacy_table_hash", MigrationLock::LegacyTableHash),
];

const MODES: [(&str, HistoryMode); 2] = [
    ("apply", HistoryMode::Apply),
    ("verify_only", HistoryMode::VerifyOnly),
];

const HISTORY_KEYWORDS: [&str; 3] = ["baseline", "lock", "mode"];

/// The directory `service!` resolves `migrations:` against.
pub(super) fn crate_dir() -> Option<PathBuf> {
    std::env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from)
}

/// `migrations: "migrations"`, a directory of `base` holding the
/// `V{version}__{name}.sql` files.
pub(super) fn parse_migrations(
    input: ParseStream,
    base: Option<&Path>,
) -> syn::Result<Vec<MigrationFile>> {
    let dir: LitStr = input.parse()?;
    let refuse = |reason: String| syn::Error::new(dir.span(), reason);
    let base = base.ok_or_else(|| {
        refuse("migrations: CARGO_MANIFEST_DIR is not set, so the directory cannot be found".into())
    })?;
    let files =
        scan_files(&base.join(dir.value())).map_err(|e| refuse(format!("migrations: {e}")))?;
    let migrations: Vec<SqlMigration> = files.iter().map(|f| f.migration.clone()).collect();
    validate_sql_migrations("migrations", &migrations, None).map_err(refuse)?;
    Ok(files)
}

/// `migration_history: ("schema_history", baseline = 6,
/// lock = legacy_table_hash, mode = verify_only)`.
pub(super) fn parse_migration_history(input: ParseStream) -> syn::Result<MigrationHistory> {
    let tuple;
    syn::parenthesized!(tuple in input);
    let table: LitStr = tuple.parse()?;
    let (mut baseline, mut lock, mut mode) = (None, None, None);
    trailing(&tuple, |key, input| match key.to_string().as_str() {
        "baseline" => once(&mut baseline, key, assigned_number(input)?),
        "lock" => once(
            &mut lock,
            key,
            assigned_choice(input, "migration lock", &LOCKS)?,
        ),
        "mode" => once(
            &mut mode,
            key,
            assigned_choice(input, "history mode", &MODES)?,
        ),
        other => Err(syn::Error::new(
            key.span(),
            format!(
                "unknown migration_history keyword '{other}'; expected {}",
                one_of(&HISTORY_KEYWORDS)
            ),
        )),
    })?;
    let history = MigrationHistory {
        table: table.value(),
        baseline,
        lock: lock.unwrap_or_default(),
        mode: mode.unwrap_or_default(),
    };
    validate_migration_history("migration_history", &history)
        .map(|()| history)
        .map_err(|reason| syn::Error::new(table.span(), reason))
}

/// The file is included, so cargo rebuilds the manifest when it changes.
pub(super) fn migration_tokens(file: &MigrationFile) -> TokenStream {
    let SqlMigration {
        version,
        name,
        no_transaction,
        ..
    } = &file.migration;
    let path = file.path.to_string_lossy();
    quote! {
        ::cufflink::manifest::SqlMigration {
            version: #version,
            name: #name.to_string(),
            sql: ::std::include_str!(#path).to_string(),
            no_transaction: #no_transaction,
        }
    }
}

pub(super) fn history_tokens(history: &MigrationHistory) -> TokenStream {
    let MigrationHistory {
        table,
        baseline,
        lock,
        mode,
    } = history;
    let baseline = optional(baseline.map(|version| quote! { #version }));
    let lock = match lock {
        MigrationLock::Platform => quote! { ::cufflink::manifest::MigrationLock::Platform },
        MigrationLock::LegacyTableHash => {
            quote! { ::cufflink::manifest::MigrationLock::LegacyTableHash }
        }
    };
    let mode = match mode {
        HistoryMode::Apply => quote! { ::cufflink::manifest::HistoryMode::Apply },
        HistoryMode::VerifyOnly => quote! { ::cufflink::manifest::HistoryMode::VerifyOnly },
    };
    quote! {
        ::cufflink::manifest::MigrationHistory {
            table: #table.to_string(),
            baseline: #baseline,
            lock: #lock,
            mode: #mode,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse::Parser;

    fn migrations(dir: &Path, declared: &str) -> syn::Result<Vec<MigrationFile>> {
        let parser = |input: ParseStream| parse_migrations(input, Some(dir));
        parser.parse_str(&format!("{declared:?}"))
    }

    fn history(declared: &str) -> syn::Result<MigrationHistory> {
        parse_migration_history.parse_str(declared)
    }

    #[test]
    fn migrations_are_the_files_of_the_directory() {
        let base = tempfile::tempdir().expect("dir");
        let dir = base.path().join("migrations");
        std::fs::create_dir(&dir).expect("migrations");
        std::fs::write(dir.join("V6__add_loan_kind.sql"), "SELECT 6;\r\n").expect("V6");
        std::fs::write(dir.join("V1__initial_tables.sql"), "SELECT 1;").expect("V1");
        let found = migrations(base.path(), "migrations").expect("scans");
        assert_eq!(
            found
                .iter()
                .map(|f| (f.migration.version, f.migration.sql.as_str()))
                .collect::<Vec<_>>(),
            [(1, "SELECT 1;"), (6, "SELECT 6;\r\n")]
        );
        let emitted = migration_tokens(&found[1]).to_string();
        assert!(emitted.contains("version : 6u32"), "{emitted}");
        assert!(emitted.contains("name : \"add_loan_kind\" . to_string ()"));
        assert!(
            emitted.contains(&format!(
                "include_str ! ({:?})",
                dir.join("V6__add_loan_kind.sql").to_string_lossy()
            )),
            "{emitted}"
        );
    }

    #[test]
    fn a_directory_that_cannot_be_used_is_refused_naming_the_file() {
        let base = tempfile::tempdir().expect("dir");
        std::fs::write(base.path().join("U1__repeatable.sql"), "SELECT 1;").expect("file");
        for (dir, reason) in [
            (
                ".",
                "migrations: U1__repeatable.sql: unversioned (U) migrations",
            ),
            ("absent", "migrations: cannot read "),
        ] {
            let error = migrations(base.path(), dir).expect_err(reason).to_string();
            assert!(error.starts_with(reason), "{error}");
        }
        std::fs::remove_file(base.path().join("U1__repeatable.sql")).expect("remove");
        std::fs::write(base.path().join("V1__empty.sql"), "  \n").expect("file");
        assert_eq!(
            migrations(base.path(), ".").expect_err("empty").to_string(),
            "migrations[0].sql: is empty"
        );
        let parser = |input: ParseStream| parse_migrations(input, None);
        assert!(parser
            .parse_str("\"migrations\"")
            .expect_err("no base")
            .to_string()
            .contains("CARGO_MANIFEST_DIR is not set"));
    }

    #[test]
    fn a_history_names_its_table_and_how_it_is_continued() {
        assert_eq!(
            history("(\"schema_history\")").expect("parses"),
            MigrationHistory {
                table: "schema_history".into(),
                ..Default::default()
            }
        );
        let declared = history(
            "(\"public.schema_history_deferred\", baseline = 15, lock = legacy_table_hash, \
             mode = verify_only)",
        )
        .expect("parses");
        assert_eq!(
            declared,
            MigrationHistory {
                table: "public.schema_history_deferred".into(),
                baseline: Some(15),
                lock: MigrationLock::LegacyTableHash,
                mode: HistoryMode::VerifyOnly,
            }
        );
        let emitted = history_tokens(&declared).to_string();
        for needle in [
            "table : \"public.schema_history_deferred\" . to_string ()",
            "baseline : :: std :: option :: Option :: Some (15u32)",
            "MigrationLock :: LegacyTableHash",
            "HistoryMode :: VerifyOnly",
        ] {
            assert!(emitted.contains(needle), "{needle}: {emitted}");
        }
        for (declared, reason) in [
            (
                "(\"a.b.c\")",
                "migration_history.table: 'a.b.c' is not 'table' or 'schema.table' in lowercase",
            ),
            (
                "(\"Schema_History\")",
                "migration_history.table: 'Schema_History' is not 'table' or 'schema.table' in lowercase",
            ),
            (
                "(\"t\", baseline = 1, baseline = 2)",
                "duplicate baseline keyword",
            ),
            (
                "(\"t\", lock = session)",
                "unknown migration lock 'session'; expected platform or legacy_table_hash",
            ),
            (
                "(\"t\", mode = dry_run)",
                "unknown history mode 'dry_run'; expected apply or verify_only",
            ),
            (
                "(\"t\", retries = 2)",
                "unknown migration_history keyword 'retries'; expected baseline, lock or mode",
            ),
        ] {
            assert_eq!(history(declared).expect_err(reason).to_string(), reason);
        }
    }
}
