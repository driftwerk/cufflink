use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;
use tracing::info;

pub fn ensure_writable_dir(dir: &Path, setting: &str, runner: &str) -> eyre::Result<()> {
    std::fs::create_dir_all(dir)
        .and_then(|()| tempfile::tempfile_in(dir).map(drop))
        .map_err(|e| {
            eyre::eyre!(
                "{setting} {} is not writable by this process ({e}). A volume created while \
                 the {runner} ran as root keeps root ownership: run \
                 `chown -R 10001:10001` on it once, then restart",
                dir.display()
            )
        })
}

pub trait Unpack: Send + 'static {
    fn unpack(self, data: &[u8], dir: &Path) -> eyre::Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Executable(pub &'static str);

impl Unpack for Executable {
    fn unpack(self, data: &[u8], dir: &Path) -> eyre::Result<()> {
        let name = Path::new(self.0);
        if name.file_name() != Some(name.as_os_str()) {
            eyre::bail!("executable name '{}' is not a plain file name", self.0);
        }
        let mut file = std::fs::File::create_new(dir.join(name))?;
        file.write_all(data)?;
        #[cfg(unix)]
        file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o755))?;
        file.sync_all()?;
        Ok(())
    }
}

struct Staged {
    dir: TempDir,
}

impl Staged {
    #[cfg(test)]
    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn place(self, target_dir: &Path) -> eyre::Result<()> {
        if let Some(parent) = target_dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::rename(self.dir.path(), target_dir) {
            Ok(()) => {
                let _ = self.dir.keep();
                info!(dir = %target_dir.display(), "Staged artifact placed");
                Ok(())
            }
            Err(_) if target_dir.is_dir() => {
                info!(dir = %target_dir.display(), "The artifact is already in place");
                Ok(())
            }
            Err(e) => Err(eyre::eyre!(
                "Failed to rename staged dir {} -> {}: {}",
                self.dir.path().display(),
                target_dir.display(),
                e
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactCap {
    pub max_bytes: u64,
    pub setting: &'static str,
}

impl ArtifactCap {
    pub fn admit(&self, key: &str, size: u64) -> eyre::Result<()> {
        match size > self.max_bytes {
            true => Err(eyre::eyre!(
                "artifact {key} is {size} bytes, more than {} ({})",
                self.max_bytes,
                self.setting
            )),
            false => Ok(()),
        }
    }

    async fn stage(
        &self,
        key: &str,
        data: Vec<u8>,
        staging_parent: &Path,
        expected_hash: Option<&str>,
        unpack: impl Unpack,
    ) -> eyre::Result<Staged> {
        self.admit(key, data.len() as u64)?;
        if let Some(expected) = expected_hash {
            let actual = hex::encode(Sha256::digest(&data));
            if actual != expected {
                return Err(eyre::eyre!(
                    "Artifact hash mismatch for {}: expected {}, got {}",
                    key,
                    expected,
                    actual
                ));
            }
            info!(key = %key, hash = %expected, "Artifact hash verified");
        }

        std::fs::create_dir_all(staging_parent)?;
        let dir = TempDir::new_in(staging_parent)?;
        let target = dir.path().to_path_buf();

        info!(
            staging_dir = %target.display(),
            "Extracting artifact into staging directory"
        );

        tokio::task::spawn_blocking(move || unpack.unpack(&data, &target))
            .await
            .map_err(|e| eyre::eyre!("Extract task panicked: {}", e))??;

        info!(
            path = %dir.path().display(),
            "Staging complete, artifact ready to place"
        );

        Ok(Staged { dir })
    }

    pub async fn install(
        &self,
        key: &str,
        data: Vec<u8>,
        target_dir: &Path,
        expected_hash: Option<&str>,
        unpack: impl Unpack,
    ) -> eyre::Result<()> {
        let staging_parent = target_dir
            .parent()
            .ok_or_else(|| eyre::eyre!("target_dir has no parent"))?;

        self.stage(key, data, staging_parent, expected_hash, unpack)
            .await?
            .place(target_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const KEY: &str = "native/svc/abc";
    const BINARY: &[u8] = b"\x7fELF not really";

    fn cap(max_bytes: u64) -> ArtifactCap {
        ArtifactCap {
            max_bytes,
            setting: "TEST_ARTIFACT_MAX_BYTES",
        }
    }

    fn hash(data: &[u8]) -> String {
        hex::encode(Sha256::digest(data))
    }

    fn entries(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .map(|entries| entries.map(|e| e.expect("entry").path()).collect())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn a_single_executable_is_staged_with_mode_0755() {
        let root = TempDir::new().expect("temp dir");
        let staged = cap(1 << 20)
            .stage(
                KEY,
                BINARY.to_vec(),
                root.path(),
                Some(&hash(BINARY)),
                Executable("service"),
            )
            .await
            .expect("staged");
        let binary = staged.path().join("service");
        assert_eq!(std::fs::read(&binary).expect("binary"), BINARY);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&binary)
                .expect("metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o755);
        }
        assert_eq!(entries(staged.path()), [binary]);
        drop(staged);
        assert!(entries(root.path()).is_empty(), "dropped, it is removed");
    }

    #[tokio::test]
    async fn an_artifact_that_does_not_match_its_hash_leaves_nothing() {
        let root = TempDir::new().expect("temp dir");
        let target = root.path().join("t").join("svc").join("abc");
        let tampered = hash(b"what the platform deployed");
        let err = cap(1 << 20)
            .install(
                KEY,
                BINARY.to_vec(),
                &target,
                Some(&tampered),
                Executable("service"),
            )
            .await
            .expect_err("hash mismatch");
        assert!(err.to_string().contains("hash mismatch"), "{err}");
        assert!(!target.exists(), "nothing is installed");
        assert!(entries(&root.path().join("t").join("svc")).is_empty());
    }

    #[tokio::test]
    async fn an_artifact_past_the_byte_cap_is_refused() {
        let root = TempDir::new().expect("temp dir");
        let limited = cap(BINARY.len() as u64 - 1);
        let err = limited
            .stage(
                KEY,
                BINARY.to_vec(),
                root.path(),
                None,
                Executable("service"),
            )
            .await
            .err()
            .expect("too large")
            .to_string();
        assert_eq!(
            err,
            format!(
                "artifact {KEY} is {} bytes, more than {} (TEST_ARTIFACT_MAX_BYTES)",
                BINARY.len(),
                BINARY.len() - 1
            )
        );
        assert!(entries(root.path()).is_empty());
        assert!(limited.admit(KEY, BINARY.len() as u64 - 1).is_ok());
        assert!(limited.admit(KEY, BINARY.len() as u64).is_err());
    }

    #[tokio::test]
    async fn an_executable_name_must_be_a_plain_file_name() {
        let root = TempDir::new().expect("temp dir");
        for name in ["", "..", "bin/service", "/service", "service/"] {
            let err = cap(1 << 20)
                .stage(KEY, BINARY.to_vec(), root.path(), None, Executable(name))
                .await
                .err()
                .expect(name)
                .to_string();
            assert!(err.contains("not a plain file name"), "{name}: {err}");
        }
        assert!(entries(root.path()).is_empty());
    }

    #[tokio::test]
    async fn an_artifact_is_placed_unless_its_directory_is_there_which_stays_as_it_is() {
        let root = TempDir::new().expect("temp dir");
        let target = root.path().join("svc").join("abc");
        let placed = cap(1 << 20);
        placed
            .install(KEY, BINARY.to_vec(), &target, None, Executable("service"))
            .await
            .expect("installed");
        assert_eq!(entries(&target), [target.join("service")]);
        let running = std::fs::File::open(target.join("service")).expect("in use");
        placed
            .install(
                KEY,
                b"other bytes".to_vec(),
                &target,
                None,
                Executable("service"),
            )
            .await
            .expect("already in place");
        assert_eq!(
            std::fs::read(target.join("service")).expect("the binary"),
            BINARY,
            "what is in place is never replaced"
        );
        assert_eq!(
            entries(&root.path().join("svc")),
            [target],
            "the second staging is removed"
        );
        drop(running);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_place_that_fails_leaves_no_staged_directory_behind() {
        let root = TempDir::new().expect("temp dir");
        let staging = root.path().join("staging");
        let target = root.path().join("v1");
        std::os::unix::fs::symlink(root.path().join("nowhere"), &target)
            .expect("a target no directory can be renamed onto");
        let staged = cap(1 << 20)
            .stage(KEY, BINARY.to_vec(), &staging, None, Executable("service"))
            .await
            .expect("staged");
        let err = staged.place(&target).expect_err("rename fails");
        assert!(err.to_string().contains("Failed to rename"), "{err}");
        assert!(entries(&staging).is_empty(), "the staged dir is removed");
    }

    #[test]
    fn apps_dir_is_created_and_must_be_writable() {
        let root = TempDir::new().expect("temp dir");
        let apps = root.path().join("apps");
        ensure_writable_dir(&apps, "APPS_DIR", "web-runner")
            .expect("a fresh directory is writable");
        assert!(apps.is_dir());

        let file = tempfile::NamedTempFile::new().expect("temp file");
        let err = ensure_writable_dir(&file.path().join("apps"), "APPS_DIR", "web-runner")
            .expect_err("not a directory");
        let message = err.to_string();
        assert!(message.contains("APPS_DIR"), "{message}");
        assert!(
            message.contains("while the web-runner ran as root"),
            "{message}"
        );
        assert!(message.contains("chown -R 10001:10001"), "{message}");
    }
}
