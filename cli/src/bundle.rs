use crate::digest::sha256_hex;
use cufflink_types::ServiceMode;
use flate2::{bufread::GzDecoder, Compression, GzBuilder};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::{Path, PathBuf};

const FORMAT: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Bundle {
    pub service: String,
    pub mode: ServiceMode,
    pub manifest: serde_json::Value,
    pub wasm_base64: Option<String>,
    pub wasm_hash: Option<String>,
    pub features: Vec<String>,
    pub commit_hash: Option<String>,
    pub cli_version: String,
}

#[derive(Serialize)]
struct Numbered<'a> {
    bundle: u32,
    #[serde(flatten)]
    contents: &'a Bundle,
}

#[derive(Deserialize)]
struct Number {
    bundle: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct File {
    pub size: usize,
    pub sha256: String,
}

impl File {
    fn of(bytes: &[u8]) -> Self {
        Self {
            size: bytes.len(),
            sha256: sha256_hex(bytes),
        }
    }
}

pub(crate) fn refused(path: &Path, why: impl std::fmt::Display) -> eyre::Report {
    eyre::eyre!("Cannot deploy the bundle {}: {why}", path.display())
}

fn partial(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.partial", std::process::id()));
    path.with_file_name(name)
}

fn write_whole(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let partial = partial(path);
    std::fs::write(&partial, bytes)
        .and_then(|()| std::fs::rename(&partial, path))
        .inspect_err(|_| {
            let _ = std::fs::remove_file(&partial);
        })
}

impl Bundle {
    fn encoded(&self) -> eyre::Result<Vec<u8>> {
        let mut gzip = GzBuilder::new()
            .mtime(0)
            .write(Vec::new(), Compression::default());
        serde_json::to_writer(
            &mut gzip,
            &Numbered {
                bundle: FORMAT,
                contents: self,
            },
        )?;
        Ok(gzip.finish()?)
    }

    fn decoded(bytes: &[u8]) -> eyre::Result<Self> {
        let mut json = Vec::new();
        let mut gzip = GzDecoder::new(bytes);
        gzip.read_to_end(&mut json)
            .map_err(|e| eyre::eyre!("it is not a whole gzip file ({e})"))?;
        if !gzip.into_inner().is_empty() {
            eyre::bail!("it has bytes after its end");
        }
        let Number { bundle } =
            serde_json::from_slice(&json).map_err(|e| eyre::eyre!("it is not a bundle ({e})"))?;
        if bundle != FORMAT {
            eyre::bail!(
                "it is bundle format {bundle}, and this CLI (v{}) reads format {FORMAT}: deploy \
                 it with the CLI that built it, or build it again with this one",
                env!("CARGO_PKG_VERSION")
            );
        }
        let bundle: Self = serde_json::from_slice(&json)
            .map_err(|e| eyre::eyre!("it is not a format {FORMAT} bundle ({e})"))?;
        bundle.verified_module().map(|()| bundle)
    }

    fn verified_module(&self) -> eyre::Result<()> {
        use base64::Engine as _;
        match (&self.wasm_base64, &self.wasm_hash) {
            (None, None) => Ok(()),
            (Some(encoded), Some(expected)) => {
                let module = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| eyre::eyre!("its module is not base64 ({e})"))?;
                let found = sha256_hex(&module);
                if found != *expected {
                    eyre::bail!(
                        "its module was altered: the bundle says its SHA-256 is {expected}, \
                         and it is {found}"
                    );
                }
                Ok(())
            }
            _ => eyre::bail!("it has a module without its hash, or a hash without its module"),
        }
    }

    pub(crate) fn write(&self, path: &Path) -> eyre::Result<File> {
        let bytes = self.encoded()?;
        write_whole(path, &bytes)
            .map_err(|e| eyre::eyre!("Cannot write the bundle {}: {e}", path.display()))?;
        Ok(File::of(&bytes))
    }

    pub(crate) fn read(path: &Path) -> eyre::Result<(Self, File)> {
        let bytes = std::fs::read(path).map_err(|e| refused(path, e))?;
        Self::decoded(&bytes)
            .map(|bundle| (bundle, File::of(&bytes)))
            .map_err(|why| refused(path, why))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use serde_json::json;

    const MODULE: &[u8] = b"\0asm\x01\0\0\0";

    fn bundle() -> Bundle {
        Bundle {
            service: "loan-service".into(),
            mode: ServiceMode::Wasm,
            manifest: json!({"name": "loan-service", "mode": "wasm", "tables": [],
                             "required_platform_features": ["db_v2"]}),
            wasm_base64: Some(base64::engine::general_purpose::STANDARD.encode(MODULE)),
            wasm_hash: Some(sha256_hex(MODULE)),
            features: vec!["db_v2".into()],
            commit_hash: Some("4f2a9c1".into()),
            cli_version: "0.21.0".into(),
        }
    }

    fn gzip(json: &serde_json::Value) -> Vec<u8> {
        let mut gzip = GzBuilder::new().write(Vec::new(), Compression::default());
        serde_json::to_writer(&mut gzip, json).expect("json");
        gzip.finish().expect("gzip")
    }

    fn refusal(bytes: &[u8]) -> String {
        Bundle::decoded(bytes)
            .expect_err("a file that is no bundle")
            .to_string()
    }

    #[test]
    fn a_bundle_is_read_as_it_was_written() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("notes.bundle");
        for bundle in [
            bundle(),
            Bundle {
                mode: ServiceMode::Crud,
                wasm_base64: None,
                wasm_hash: None,
                commit_hash: None,
                features: Vec::new(),
                ..bundle()
            },
        ] {
            let written = bundle.write(&path).expect("written");
            let bytes = std::fs::read(&path).expect("file");
            assert_eq!(written, File::of(&bytes));
            assert_eq!(
                Bundle::read(&path).expect("read"),
                (bundle, written),
                "what was written, and the digest the write printed"
            );
        }
        assert_eq!(
            std::fs::read_dir(dir.path()).expect("dir").count(),
            1,
            "nothing is left beside the bundle"
        );
    }

    #[test]
    fn two_writes_of_one_input_are_byte_identical() {
        let first = bundle().encoded().expect("encoded");
        assert_eq!(bundle().encoded().expect("encoded"), first);
        assert_eq!(&first[4..8], [0u8; 4], "the gzip header carries no time");

        let mut json = String::new();
        GzDecoder::new(first.as_slice())
            .read_to_string(&mut json)
            .expect("gzip");
        let field = |name: &str| json.find(&format!("\"{name}\":")).expect(name);
        let order = [
            "bundle",
            "service",
            "mode",
            "manifest",
            "wasm_base64",
            "wasm_hash",
            "features",
            "commit_hash",
            "cli_version",
        ]
        .map(field);
        assert!(order.is_sorted(), "the fields are in one order: {json}");
        assert!(json.starts_with(r#"{"bundle":1,"service":"loan-service","mode":"wasm","#));
    }

    #[test]
    fn a_bundle_of_another_format_is_refused_by_both_numbers() {
        let mut newer = serde_json::to_value(bundle()).expect("json");
        newer["bundle"] = json!(2);
        newer["signature"] = json!("…");
        assert_eq!(
            refusal(&gzip(&newer)),
            format!(
                "it is bundle format 2, and this CLI (v{}) reads format 1: deploy it with the \
                 CLI that built it, or build it again with this one",
                env!("CARGO_PKG_VERSION")
            )
        );
        assert!(refusal(&gzip(&json!({"service": "notes"}))).starts_with("it is not a bundle ("));
        assert!(refusal(&gzip(&json!("a tarball, say"))).starts_with("it is not a bundle ("));
        assert!(refusal(&gzip(&json!({"bundle": 1, "service": "notes"})))
            .starts_with("it is not a format 1 bundle (missing field"),);
    }

    #[test]
    fn a_bundle_whose_module_was_altered_is_refused() {
        let altered = Bundle {
            wasm_base64: Some(base64::engine::general_purpose::STANDARD.encode(b"\0asm\x02")),
            ..bundle()
        };
        assert_eq!(
            refusal(&altered.encoded().expect("encoded")),
            format!(
                "its module was altered: the bundle says its SHA-256 is {}, and it is {}",
                sha256_hex(MODULE),
                sha256_hex(b"\0asm\x02")
            )
        );
        for half in [
            Bundle {
                wasm_hash: None,
                ..bundle()
            },
            Bundle {
                wasm_base64: None,
                ..bundle()
            },
        ] {
            assert_eq!(
                refusal(&half.encoded().expect("encoded")),
                "it has a module without its hash, or a hash without its module"
            );
        }
        let garbled = Bundle {
            wasm_base64: Some("not base64!".into()),
            ..bundle()
        };
        assert!(
            refusal(&garbled.encoded().expect("encoded")).starts_with("its module is not base64")
        );
    }

    #[test]
    fn a_truncated_file_and_one_that_is_not_gzip_are_refused_with_their_path() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("notes.bundle");
        let whole = bundle().encoded().expect("encoded");
        for (bytes, why) in [
            (&whole[..whole.len() / 2], "it is not a whole gzip file"),
            (&whole[..whole.len() - 4], "it is not a whole gzip file"),
            (&b"{\"bundle\": 1}"[..], "it is not a whole gzip file"),
            (&[][..], "it is not a whole gzip file"),
        ] {
            std::fs::write(&path, bytes).expect("file");
            let refused = Bundle::read(&path).expect_err("refused").to_string();
            assert!(
                refused.starts_with(&format!(
                    "Cannot deploy the bundle {}: {why}",
                    path.display()
                )),
                "{refused}"
            );
        }
        std::fs::write(&path, [whole.as_slice(), b"more"].concat()).expect("file");
        assert_eq!(
            Bundle::read(&path).expect_err("refused").to_string(),
            format!(
                "Cannot deploy the bundle {}: it has bytes after its end",
                path.display()
            ),
            "two files with two digests are never one bundle"
        );
        let missing = dir.path().join("none.bundle");
        assert!(Bundle::read(&missing)
            .expect_err("no file")
            .to_string()
            .starts_with(&format!("Cannot deploy the bundle {}: ", missing.display())));
    }

    #[test]
    fn a_write_that_fails_leaves_no_file_and_keeps_the_one_that_was_there() {
        let dir = tempfile::tempdir().expect("dir");
        let taken = dir.path().join("taken");
        std::fs::create_dir(&taken).expect("a directory where the bundle is to go");
        std::fs::write(taken.join("kept"), b"kept").expect("file");

        let failed = bundle()
            .write(&taken)
            .expect_err("a directory is in the way");

        assert!(failed
            .to_string()
            .starts_with(&format!("Cannot write the bundle {}: ", taken.display())));
        let left: Vec<_> = std::fs::read_dir(dir.path())
            .expect("dir")
            .map(|entry| entry.expect("entry").file_name())
            .collect();
        assert_eq!(left, ["taken"], "no partial file is left");
        assert_eq!(std::fs::read(taken.join("kept")).expect("file"), b"kept");

        let nowhere = dir.path().join("no-such-dir").join("notes.bundle");
        assert!(bundle()
            .write(&nowhere)
            .expect_err("no directory to write in")
            .to_string()
            .starts_with(&format!("Cannot write the bundle {}: ", nowhere.display())));
        assert!(!nowhere.exists());
    }
}
