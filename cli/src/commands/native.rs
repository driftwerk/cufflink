use std::path::{Path, PathBuf};
use std::process::Command;

pub const TARGET: &str = "x86_64-unknown-linux-musl";

const ADD_TARGET: &str = "rustup target add x86_64-unknown-linux-musl";
const INSTALL_ZIGBUILD: &str = "cargo install cargo-zigbuild (and install zig)";
const INSTALL_MUSL_TOOLS: &str =
    "install musl-tools (musl-gcc), the C compiler crates with C code such as ring need for the \
     target, or cargo install cargo-zigbuild";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Builder {
    Cargo,
    Zigbuild,
}

impl Builder {
    pub fn args(self, bin: &str) -> Vec<String> {
        let subcommand = match self {
            Builder::Cargo => "build",
            Builder::Zigbuild => "zigbuild",
        };
        [subcommand, "--release", "--target", TARGET, "--bin", bin]
            .map(String::from)
            .to_vec()
    }

    fn remedy(self) -> Option<&'static str> {
        match self {
            Builder::Cargo => Some(INSTALL_MUSL_TOOLS),
            Builder::Zigbuild => None,
        }
    }
}

pub fn builder(
    os: &str,
    arch: &str,
    target_installed: bool,
    zigbuild: bool,
) -> Result<Builder, String> {
    let chosen = match (zigbuild, os == "linux" && arch == "x86_64") {
        (true, _) => Some(Builder::Zigbuild),
        (false, true) => Some(Builder::Cargo),
        (false, false) => None,
    };
    match (chosen, target_installed) {
        (Some(builder), true) => Ok(builder),
        (chosen, _) => {
            let remedies: Vec<&str> = [
                (!target_installed).then_some(ADD_TARGET),
                chosen.map_or(Some(INSTALL_ZIGBUILD), Builder::remedy),
            ]
            .into_iter()
            .flatten()
            .collect();
            Err(format!(
                "a native service is built as a static {TARGET} binary; on this host ({os} \
                 {arch}) that needs: {}",
                remedies.join("; ")
            ))
        }
    }
}

pub fn binary_target(metadata: &serde_json::Value, dir: &Path) -> eyre::Result<String> {
    let package = metadata["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|package| {
            package["manifest_path"]
                .as_str()
                .and_then(|manifest| Path::new(manifest).parent())
                == Some(dir)
        })
        .ok_or_else(|| eyre::eyre!("no package has its Cargo.toml in {}", dir.display()))?;
    let name = package["name"].as_str().unwrap_or("?");
    let bins: Vec<&str> = package["targets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|target| {
            target["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"))
        })
        .filter_map(|target| target["name"].as_str())
        .collect();
    match (bins.as_slice(), package["default_run"].as_str()) {
        ([bin], _) => Ok(bin.to_string()),
        ([], _) => eyre::bail!("package '{name}' has no binary target to build"),
        (_, Some(default)) if bins.contains(&default) => Ok(default.to_string()),
        (_, _) => eyre::bail!(
            "package '{name}' has several binary targets ({}); set `default-run` to the service's",
            bins.join(", ")
        ),
    }
}

pub struct NativeArtifact {
    pub base64: String,
    pub size: usize,
    pub hash: String,
}

impl NativeArtifact {
    fn of(bytes: &[u8]) -> Self {
        use base64::Engine as _;
        Self {
            base64: base64::engine::general_purpose::STANDARD.encode(bytes),
            size: bytes.len(),
            hash: crate::digest::sha256_hex(bytes),
        }
    }
}

fn succeeds(program: &str, args: &[&str]) -> Option<std::process::Output> {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
}

fn target_installed() -> bool {
    succeeds("rustup", &["target", "list", "--installed"]).is_some_and(|output| {
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.trim() == TARGET)
    })
}

fn zigbuild_available() -> bool {
    succeeds("cargo", &["zigbuild", "--version"]).is_some()
}

pub fn build() -> eyre::Result<NativeArtifact> {
    let builder = builder(
        std::env::consts::OS,
        std::env::consts::ARCH,
        target_installed(),
        zigbuild_available(),
    )
    .map_err(|remedy| eyre::eyre!(remedy))?;
    let metadata = succeeds("cargo", &["metadata", "--format-version", "1", "--no-deps"])
        .ok_or_else(|| eyre::eyre!("Failed to run cargo metadata"))?;
    let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout)?;
    let bin = binary_target(&metadata, &std::env::current_dir()?.canonicalize()?)?;
    let args = builder.args(&bin);
    println!("  Building {bin} for {TARGET}: cargo {}", args.join(" "));
    let built = Command::new("cargo").args(&args).output()?;
    if !built.status.success() {
        eyre::bail!(
            "Native build failed:\n{}{}",
            String::from_utf8_lossy(&built.stderr),
            builder.remedy().map_or_else(String::new, |remedy| format!(
                "\nIf a C compiler is missing: {remedy}"
            ))
        );
    }
    let path = metadata["target_directory"]
        .as_str()
        .map(PathBuf::from)
        .ok_or_else(|| eyre::eyre!("Cannot determine target directory"))?
        .join(TARGET)
        .join("release")
        .join(&bin);
    let bytes = std::fs::read(&path)
        .map_err(|e| eyre::eyre!("Built binary not found at {}: {e}", path.display()))?;
    Ok(NativeArtifact::of(&bytes))
}

pub fn provided_lines(manifest: &cufflink_types::ServiceManifest) -> Vec<String> {
    manifest
        .provides
        .iter()
        .map(|provided| {
            format!(
                "{} (callers: {})",
                provided.name,
                provided.callers.join(", ")
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_builder_is_zigbuild_where_it_is_else_cargo_on_linux_x86_64_and_either_needs_the_target()
    {
        for (os, arch, target, zigbuild, built) in [
            ("linux", "x86_64", true, false, Ok(Builder::Cargo)),
            ("linux", "x86_64", true, true, Ok(Builder::Zigbuild)),
            ("macos", "aarch64", true, true, Ok(Builder::Zigbuild)),
            ("linux", "aarch64", true, true, Ok(Builder::Zigbuild)),
            ("macos", "x86_64", true, true, Ok(Builder::Zigbuild)),
        ] {
            assert_eq!(builder(os, arch, target, zigbuild), built, "{os} {arch}");
        }
        for (os, arch, target, zigbuild, needs) in [
            (
                "linux",
                "x86_64",
                false,
                false,
                vec![ADD_TARGET, INSTALL_MUSL_TOOLS],
            ),
            ("linux", "x86_64", false, true, vec![ADD_TARGET]),
            ("macos", "aarch64", false, true, vec![ADD_TARGET]),
            ("macos", "aarch64", true, false, vec![INSTALL_ZIGBUILD]),
            ("linux", "aarch64", true, false, vec![INSTALL_ZIGBUILD]),
            (
                "macos",
                "aarch64",
                false,
                false,
                vec![ADD_TARGET, INSTALL_ZIGBUILD],
            ),
        ] {
            let refused = builder(os, arch, target, zigbuild).expect_err(os);
            assert!(refused.contains(TARGET), "{refused}");
            for remedy in [ADD_TARGET, INSTALL_ZIGBUILD, INSTALL_MUSL_TOOLS] {
                assert_eq!(
                    refused.contains(remedy),
                    needs.contains(&remedy),
                    "{os} {arch} {target} {zigbuild}: {refused}"
                );
            }
        }
        assert_eq!(Builder::Cargo.remedy(), Some(INSTALL_MUSL_TOOLS));
        assert_eq!(Builder::Zigbuild.remedy(), None);
    }

    #[test]
    fn each_builder_builds_the_release_binary_for_the_target() {
        assert_eq!(
            Builder::Cargo.args("ext-google"),
            [
                "build",
                "--release",
                "--target",
                "x86_64-unknown-linux-musl",
                "--bin",
                "ext-google"
            ]
        );
        assert_eq!(Builder::Zigbuild.args("ext-google")[0], "zigbuild");
        assert_eq!(
            Builder::Zigbuild.args("ext-google")[1..],
            Builder::Cargo.args("ext-google")[1..]
        );
    }

    fn metadata(bins: &[&str], default_run: Option<&str>) -> serde_json::Value {
        let targets: Vec<_> = bins
            .iter()
            .map(|bin| json!({"name": bin, "kind": ["bin"]}))
            .chain([json!({"name": "helpers", "kind": ["lib"]})])
            .collect();
        json!({
            "target_directory": "/work/target",
            "packages": [
                {"name": "shared", "manifest_path": "/work/Cargo.toml", "targets": [{"name": "shared", "kind": ["bin"]}]},
                {"name": "ext-google", "manifest_path": "/work/ext-google/Cargo.toml",
                 "default_run": default_run, "targets": targets},
            ],
        })
    }

    #[test]
    fn the_binary_is_the_target_of_the_package_in_the_directory() {
        let dir = Path::new("/work/ext-google");
        assert_eq!(
            binary_target(&metadata(&["ext-google"], None), dir).expect("one bin"),
            "ext-google"
        );
        assert_eq!(
            binary_target(&metadata(&["tool", "serve"], Some("serve")), dir).expect("default"),
            "serve"
        );
        assert_eq!(
            binary_target(&metadata(&["shared"], None), Path::new("/work")).expect("root"),
            "shared",
            "a package above is not the one in the directory"
        );
        for (bins, default_run, says) in [
            (&[][..], None, "has no binary target"),
            (
                &["tool", "serve"][..],
                None,
                "several binary targets (tool, serve)",
            ),
            (
                &["tool", "serve"][..],
                Some("other"),
                "several binary targets",
            ),
        ] {
            let refused = binary_target(&metadata(bins, default_run), dir)
                .expect_err(says)
                .to_string();
            assert!(refused.contains(says), "{refused}");
        }
        assert!(binary_target(&metadata(&["x"], None), Path::new("/elsewhere")).is_err());
    }

    #[test]
    fn a_binary_is_carried_with_its_hash() {
        let artifact = NativeArtifact::of(b"hello");
        assert_eq!(artifact.base64, "aGVsbG8=");
        assert_eq!(artifact.size, 5);
        assert_eq!(
            artifact.hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
