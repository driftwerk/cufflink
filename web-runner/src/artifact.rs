use cufflink_supervisor::{ArtifactCap, Unpack};
use flate2::read::GzDecoder;
use std::path::{Component, Path};
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveCaps {
    pub max_entries: u64,
    pub max_bytes: u64,
}

impl ArchiveCaps {
    pub fn download(self) -> ArtifactCap {
        ArtifactCap {
            max_bytes: self.max_bytes,
            setting: "WEB_ARTIFACT_MAX_BYTES",
        }
    }
}

fn stays_inside(path: &Path) -> bool {
    path.components()
        .try_fold(0usize, |depth, component| match component {
            Component::Normal(_) => Some(depth + 1),
            Component::CurDir => Some(depth),
            Component::ParentDir => depth.checked_sub(1),
            Component::RootDir | Component::Prefix(_) => None,
        })
        .is_some()
}

fn extract(data: &[u8], target: &Path, caps: ArchiveCaps) -> eyre::Result<()> {
    let mut archive = tar::Archive::new(GzDecoder::new(data));
    let mut bytes = 0u64;
    for (index, entry) in archive.entries()?.enumerate() {
        let mut entry = entry?;
        if index as u64 >= caps.max_entries {
            eyre::bail!(
                "artifact has more than {} entries (WEB_ARTIFACT_MAX_ENTRIES)",
                caps.max_entries
            );
        }
        bytes = bytes.saturating_add(entry.size());
        if bytes > caps.max_bytes {
            eyre::bail!(
                "artifact extracts to more than {} bytes (WEB_ARTIFACT_MAX_BYTES)",
                caps.max_bytes
            );
        }
        let path = entry.path()?.into_owned();
        if !stays_inside(&path) {
            eyre::bail!("artifact entry {} leaves the app directory", path.display());
        }
        if let Some(link) = entry.link_name()? {
            let resolved = match entry.header().entry_type() {
                tar::EntryType::Symlink => path.parent().unwrap_or(Path::new("")).join(&link),
                _ => link.into_owned(),
            };
            if !stays_inside(&resolved) {
                eyre::bail!(
                    "artifact link {} points outside the app directory",
                    path.display()
                );
            }
        }
        if !entry.unpack_in(target)? {
            eyre::bail!("artifact entry {} leaves the app directory", path.display());
        }
    }
    links_stay_inside(target)
}

fn links_stay_inside(root: &Path) -> eyre::Result<()> {
    let root = root.canonicalize()?;
    let mut pending = vec![root.clone()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            let kind = std::fs::symlink_metadata(&path)?.file_type();
            if kind.is_symlink() {
                if path
                    .canonicalize()
                    .is_ok_and(|resolved| !resolved.starts_with(&root))
                {
                    eyre::bail!(
                        "artifact link {} points outside the app directory",
                        path.strip_prefix(&root).unwrap_or(&path).display()
                    );
                }
            } else if kind.is_dir() {
                pending.push(path);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct NextBuild(ArchiveCaps);

impl Unpack for NextBuild {
    fn unpack(self, data: &[u8], target: &Path) -> eyre::Result<()> {
        extract(data, target, self.0)?;
        next_build_present(target)
    }
}

fn next_build_present(target: &Path) -> eyre::Result<()> {
    let build_id_path = target.join(".next/BUILD_ID");
    if !build_id_path.exists() {
        return Err(eyre::eyre!(
            "Extracted artifact missing .next/BUILD_ID at {}",
            target.display()
        ));
    }
    let build_id =
        std::fs::read_to_string(&build_id_path).unwrap_or_else(|_| "unknown".to_string());
    info!(build_id = %build_id.trim(), "Artifact BUILD_ID");

    let node_next = target.join("node_modules/next");
    if !node_next.exists() {
        return Err(eyre::eyre!(
            "Extracted artifact missing node_modules/next at {}",
            target.display()
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct ArtifactStore {
    caps: ArchiveCaps,
}

impl ArtifactStore {
    pub fn new(caps: ArchiveCaps) -> Self {
        Self { caps }
    }

    pub async fn install(
        &self,
        label: &str,
        data: Vec<u8>,
        target_dir: &Path,
        expected_hash: Option<&str>,
    ) -> eyre::Result<()> {
        self.caps
            .download()
            .install(label, data, target_dir, expected_hash, NextBuild(self.caps))
            .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tempfile::TempDir;

    const CAPS: ArchiveCaps = ArchiveCaps {
        max_entries: 100,
        max_bytes: 1 << 20,
    };

    fn raw_tarball(entries: &[(&str, tar::EntryType, Option<&str>, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        for (path, kind, link, contents) in entries {
            let mut header = tar::Header::new_old();
            let name = &mut header.as_old_mut().name;
            name[..path.len()].copy_from_slice(path.as_bytes());
            if let Some(link) = link {
                let target = &mut header.as_old_mut().linkname;
                target[..link.len()].copy_from_slice(link.as_bytes());
            }
            header.set_entry_type(*kind);
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append(&header, *contents).expect("append");
        }
        builder
            .into_inner()
            .and_then(|gz| gz.finish())
            .expect("finish tarball")
    }

    fn extracted(archive: &[u8], caps: ArchiveCaps) -> (eyre::Result<()>, TempDir) {
        let root = TempDir::new().expect("temp dir");
        let target = root.path().join("app");
        std::fs::create_dir_all(&target).expect("target");
        (extract(archive, &target, caps), root)
    }

    #[test]
    fn paths_resolve_inside_or_not() {
        for inside in ["a/b", "./a", "a/../b", "a/b/../../c"] {
            assert!(stays_inside(Path::new(inside)), "{inside}");
        }
        for outside in ["../a", "a/../../b", "/etc/passwd"] {
            assert!(!stays_inside(Path::new(outside)), "{outside}");
        }
    }

    #[test]
    fn entries_and_links_that_leave_the_app_are_refused() {
        use tar::EntryType::{Link, Regular, Symlink};
        let file = b"x".as_slice();
        for (entries, reason) in [
            (
                vec![("../evil", Regular, None, file)],
                "leaves the app directory",
            ),
            (
                vec![("/tmp/evil", Regular, None, file)],
                "leaves the app directory",
            ),
            (
                vec![(
                    "node_modules/x",
                    Symlink,
                    Some("../../../etc"),
                    b"".as_slice(),
                )],
                "points outside",
            ),
            (
                vec![("pwd", Symlink, Some("/etc/passwd"), b"".as_slice())],
                "points outside",
            ),
            (
                vec![("hard", Link, Some("../outside"), b"".as_slice())],
                "points outside",
            ),
        ] {
            let (result, root) = extracted(&raw_tarball(&entries), CAPS);
            let err = result.expect_err(reason).to_string();
            assert!(err.contains(reason), "{err}");
            assert!(!root.path().join("evil").exists());
        }
        let (result, root) = extracted(
            &raw_tarball(&[
                ("node_modules/.pnpm/next/index.js", Regular, None, file),
                (
                    "node_modules/next",
                    Symlink,
                    Some(".pnpm/next"),
                    b"".as_slice(),
                ),
            ]),
            CAPS,
        );
        result.expect("a link inside the app is fine");
        assert!(root.path().join("app/node_modules/next/index.js").is_file());

        let (result, _root) = extracted(
            &raw_tarball(&[
                ("sub/up", Symlink, Some(".."), b"".as_slice()),
                ("out", Symlink, Some("sub/up/.."), b"".as_slice()),
            ]),
            CAPS,
        );
        let err = result.expect_err("a link out through a link").to_string();
        assert!(err.contains("artifact link out points outside"), "{err}");
    }

    #[test]
    fn the_byte_cap_counts_what_a_pax_size_writes() {
        let data = vec![b'x'; 3000];
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        builder
            .append_pax_extensions([("size", b"3000".as_slice())])
            .expect("pax");
        let mut header = tar::Header::new_ustar();
        header.set_path("big").expect("path");
        header.set_size(0);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, data.as_slice()).expect("append");
        let archive = builder
            .into_inner()
            .and_then(|gz| gz.finish())
            .expect("finish tarball");
        let (result, _root) = extracted(
            &archive,
            ArchiveCaps {
                max_entries: 10,
                max_bytes: 2000,
            },
        );
        assert!(result
            .expect_err("the header says 0, the entry writes 3000")
            .to_string()
            .contains("WEB_ARTIFACT_MAX_BYTES"));
    }

    #[test]
    fn extraction_stops_at_the_caps() {
        use tar::EntryType::Regular;
        let many: Vec<(String, Vec<u8>)> =
            (0..4).map(|i| (format!("f{i}"), vec![b'x'; 600])).collect();
        let entries: Vec<(&str, tar::EntryType, Option<&str>, &[u8])> = many
            .iter()
            .map(|(name, data)| (name.as_str(), Regular, None, data.as_slice()))
            .collect();
        let archive = raw_tarball(&entries);
        let (result, _root) = extracted(
            &archive,
            ArchiveCaps {
                max_entries: 3,
                max_bytes: 1 << 20,
            },
        );
        assert!(result
            .expect_err("entries")
            .to_string()
            .contains("WEB_ARTIFACT_MAX_ENTRIES"));
        let (result, _root) = extracted(
            &archive,
            ArchiveCaps {
                max_entries: 100,
                max_bytes: 2000,
            },
        );
        assert!(result
            .expect_err("bytes")
            .to_string()
            .contains("WEB_ARTIFACT_MAX_BYTES"));
        let (result, _root) = extracted(
            &archive,
            ArchiveCaps {
                max_entries: 4,
                max_bytes: 2400,
            },
        );
        result.expect("exactly at the caps");
    }

    pub(crate) fn tarball(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        for (path, contents) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, path, contents.as_bytes())
                .expect("append");
        }
        builder
            .into_inner()
            .and_then(|gz| gz.finish())
            .expect("finish tarball")
    }

    #[tokio::test]
    async fn only_a_next_build_is_installed() {
        let build = [
            (".next/BUILD_ID", "build-1"),
            ("node_modules/next/package.json", "{}"),
        ];
        let store = ArtifactStore::new(CAPS);
        let root = TempDir::new().expect("temp dir");
        let target = root.path().join("t").join("svc").join("v1");

        for (entries, missing) in [
            (&build[1..], ".next/BUILD_ID"),
            (&build[..1], "node_modules/next"),
        ] {
            let err = store
                .install("svc v1", tarball(entries), &target, None)
                .await
                .expect_err(missing)
                .to_string();
            assert!(
                err.contains(&format!("Extracted artifact missing {missing}")),
                "{err}"
            );
            assert!(!target.exists(), "nothing is extracted");
        }

        store
            .install("svc v1", tarball(&build), &target, None)
            .await
            .expect("a Next.js build");
        assert!(target.join(".next/BUILD_ID").is_file());
    }

    #[tokio::test]
    async fn a_bundle_past_the_byte_cap_or_of_another_hash_is_not_installed() {
        let artifact = tarball(&[(".next/BUILD_ID", "build-1")]);
        let small = ArchiveCaps {
            max_bytes: artifact.len() as u64 - 1,
            ..CAPS
        };
        let root = TempDir::new().expect("temp dir");
        let target = root.path().join("svc");
        let err = ArtifactStore::new(small)
            .install("svc v1", artifact.clone(), &target, None)
            .await
            .expect_err("too large")
            .to_string();
        assert_eq!(
            err,
            format!(
                "artifact svc v1 is {} bytes, more than {} (WEB_ARTIFACT_MAX_BYTES)",
                artifact.len(),
                small.max_bytes
            )
        );
        assert!(ArtifactStore::new(CAPS)
            .install("svc v1", artifact, &target, Some(&"0".repeat(64)))
            .await
            .is_err());
        assert!(std::fs::read_dir(root.path())
            .expect("dir")
            .next()
            .is_none());
    }
}
