use crate::bundle::{Bundle, File};
use std::path::Path;

fn made_ready(path: &Path) -> eyre::Result<()> {
    let unwritable = |why: String| eyre::eyre!("Cannot write the bundle {}: {why}", path.display());
    if path.is_dir() {
        return Err(unwritable("it is a directory".into()));
    }
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or(Ok(()), std::fs::create_dir_all)
        .map_err(|e| unwritable(format!("its directory cannot be made ({e})")))
}

fn report(path: &Path, bundle: &Bundle, file: &File) -> Vec<String> {
    let or_none = |value: &Option<String>| value.clone().unwrap_or_else(|| "none".into());
    vec![
        format!("Wrote {} ({} bytes)", path.display(), file.size),
        format!(
            "  Service: {} (mode: {})",
            bundle.service,
            bundle.mode.as_str()
        ),
        format!("  Commit:  {}", or_none(&bundle.commit_hash)),
        format!("  Module:  {}", or_none(&bundle.wasm_hash)),
        format!("  SHA-256: {}", file.sha256),
    ]
}

const NO_COMMIT: &str = "warning: the bundle names no commit (this is not a git checkout, and \
                         no --commit-hash was given): a deploy of it reports none, and \
                         `cufflink rollback --commit` cannot find it";

pub fn bundle(path: &Path, commit_hash: Option<String>) -> eyre::Result<()> {
    made_ready(path)?;
    let bundle = super::deploy::bundle_of_sources(commit_hash)?;
    let file = bundle.write(path)?;
    report(path, &bundle, &file)
        .iter()
        .for_each(|line| println!("{line}"));
    if bundle.commit_hash.is_none() {
        eprintln!("{NO_COMMIT}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_types::ServiceMode;

    fn bundle() -> Bundle {
        Bundle {
            service: "notes".into(),
            mode: ServiceMode::Wasm,
            manifest: serde_json::json!({"name": "notes"}),
            wasm_base64: Some("AGFzbQ==".into()),
            wasm_hash: Some("30602f71".into()),
            features: Vec::new(),
            commit_hash: Some("4f2a9c1".into()),
            cli_version: "0.21.0".into(),
        }
    }

    #[test]
    fn a_build_says_what_it_wrote_and_the_digest_that_names_it() {
        let file = File {
            size: 48213,
            sha256: "9e8d7c".into(),
        };
        assert_eq!(
            report(Path::new("out/notes.bundle"), &bundle(), &file),
            [
                "Wrote out/notes.bundle (48213 bytes)",
                "  Service: notes (mode: wasm)",
                "  Commit:  4f2a9c1",
                "  Module:  30602f71",
                "  SHA-256: 9e8d7c",
            ]
        );
        let bare = Bundle {
            mode: ServiceMode::Crud,
            wasm_base64: None,
            wasm_hash: None,
            commit_hash: None,
            ..bundle()
        };
        assert_eq!(
            report(Path::new("todos.bundle"), &bare, &file)[1..4],
            [
                "  Service: notes (mode: crud)",
                "  Commit:  none",
                "  Module:  none"
            ]
        );
    }

    #[test]
    fn where_a_bundle_goes_is_made_or_refused_before_anything_is_built() {
        let dir = tempfile::tempdir().expect("dir");
        let nested = dir.path().join("out").join("ci").join("notes.bundle");
        made_ready(&nested).expect("made");
        assert!(dir.path().join("out").join("ci").is_dir());
        assert!(!nested.exists(), "the directory, not the file");
        made_ready(Path::new("notes.bundle")).expect("the working directory is there");

        assert_eq!(
            made_ready(dir.path()).expect_err("a directory").to_string(),
            format!(
                "Cannot write the bundle {}: it is a directory",
                dir.path().display()
            )
        );
        let file = dir.path().join("file");
        std::fs::write(&file, b"x").expect("file");
        let under_a_file = file.join("notes.bundle");
        assert!(made_ready(&under_a_file)
            .expect_err("a file is no directory")
            .to_string()
            .starts_with(&format!(
                "Cannot write the bundle {}: its directory cannot be made (",
                under_a_file.display()
            )));
    }
}
