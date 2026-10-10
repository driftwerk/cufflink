#[path = "src/profile.rs"]
mod profile;

use profile::Profile;
use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const TARGET: &str = "wasm32-unknown-unknown";

struct Guest {
    name: &'static str,
    source: &'static str,
    features: &'static [&'static str],
}

const GUESTS: &[Guest] = &[
    Guest {
        name: "features",
        source: "features",
        features: &["db-v2", "cache", "service-call", "response-v2"],
    },
    Guest {
        name: "no-features",
        source: "features",
        features: &[],
    },
    Guest {
        name: "responses",
        source: "responses",
        features: &["response-v2"],
    },
    Guest {
        name: "sse",
        source: "sse",
        features: &["response-v2"],
    },
    Guest {
        name: "internal",
        source: "internal",
        features: &["service-call"],
    },
    Guest {
        name: "db-v2",
        source: "db_v2",
        features: &["db-v2"],
    },
    Guest {
        name: "cache",
        source: "cache",
        features: &["cache"],
    },
    Guest {
        name: "service-call",
        source: "service_call",
        features: &["service-call"],
    },
    Guest {
        name: "eventing",
        source: "eventing",
        features: &["delivery-control"],
    },
    Guest {
        name: "jobs",
        source: "jobs",
        features: &["delivery-control"],
    },
    Guest {
        name: "schedules",
        source: "schedules",
        features: &["util-v2"],
    },
    Guest {
        name: "pgbouncer",
        source: "pgbouncer",
        features: &["db-v2"],
    },
    Guest {
        name: "nats-v2",
        source: "nats_v2",
        features: &["nats-publish-v2"],
    },
    Guest {
        name: "http-v2",
        source: "http_v2",
        features: &["http-v2"],
    },
    Guest {
        name: "storage-v2",
        source: "storage_v2",
        features: &["storage-v2", "http-v2"],
    },
    Guest {
        name: "util-v2",
        source: "util_v2",
        features: &["util-v2"],
    },
    Guest {
        name: "config",
        source: "config",
        features: &[],
    },
    Guest {
        name: "ext-call",
        source: "ext_call",
        features: &["ext-call"],
    },
    Guest {
        name: "db-roles",
        source: "db_roles",
        features: &["db-v2"],
    },
    Guest {
        name: "library-example",
        source: "library_example",
        features: &[
            "db-v2",
            "nats-publish-v2",
            "http-v2",
            "storage-v2",
            "util-v2",
            "delivery-control",
        ],
    },
    Guest {
        name: "uuid-js",
        source: "uuid_js",
        features: &[
            "stripe",
            "db-v2",
            "cache",
            "service-call",
            "delivery-control",
            "nats-publish-v2",
            "http-v2",
            "storage-v2",
            "util-v2",
            "ext-call",
        ],
    },
];

const DEPENDENCIES: &[(&str, &str)] = &[(
    "uuid_js",
    r#"uuid = { version = "1", features = ["v4", "js"] }"#,
)];

const OUTER_BUILD_ENV: &[&str] = &[
    "CARGO_ENCODED_RUSTFLAGS",
    "RUSTFLAGS",
    "RUSTC_WORKSPACE_WRAPPER",
    "CLIPPY_ARGS",
];

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let workspace = manifest_dir.parent().ok_or("the crate has no parent")?;
    let out = PathBuf::from(env::var("OUT_DIR")?).join("guests");
    for watched in [
        "guests",
        "src/profile.rs",
        "../cufflink-fn",
        "../types",
        "../Cargo.lock",
    ] {
        println!("cargo:rerun-if-changed={watched}");
    }
    println!("cargo:rustc-env=CUFFLINK_TEST_GUESTS_DIR={}", out.display());
    println!(
        "cargo:rustc-env=CUFFLINK_TEST_GUESTS={}",
        GUESTS
            .iter()
            .map(|guest| guest.name)
            .collect::<Vec<_>>()
            .join(",")
    );

    if !target_installed(&env::var("RUSTC")?)? {
        let reason = format!("the {TARGET} target is not installed");
        println!("cargo:warning=guests not built: {reason}");
        println!("cargo:rustc-env=CUFFLINK_TEST_GUESTS_UNAVAILABLE={reason}");
        return Ok(());
    }

    GUESTS
        .iter()
        .map(|guest| guest.source)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .try_for_each(|source| write_package(source, &manifest_dir, workspace, &out))?;
    let cargo = env::var("CARGO")?;
    GUESTS.iter().try_for_each(|guest| {
        Profile::ALL
            .into_iter()
            .try_for_each(|profile| build(&cargo, guest, profile, &out))
    })
}

fn target_installed(rustc: &str) -> Result<bool, Box<dyn Error>> {
    let output = Command::new(rustc)
        .args(["--print", "target-libdir", "--target", TARGET])
        .output()?;
    Ok(output.status.success()
        && Path::new(String::from_utf8_lossy(&output.stdout).trim()).is_dir())
}

fn write_package(
    source: &str,
    manifest_dir: &Path,
    workspace: &Path,
    out: &Path,
) -> Result<(), Box<dyn Error>> {
    let dir = out.join(source);
    fs::create_dir_all(&dir)?;
    let features: String = GUESTS
        .iter()
        .flat_map(|guest| guest.features)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|feature| format!("{feature} = [\"cufflink-fn/{feature}\"]\n"))
        .collect();
    let dependencies: String = DEPENDENCIES
        .iter()
        .filter(|(of, _)| *of == source)
        .map(|(_, dependency)| format!("{dependency}\n"))
        .collect();
    fs::write(
        dir.join("Cargo.toml"),
        format!(
            "[package]\nname = \"cufflink-guest-{source}\"\nversion = \"0.0.0\"\n\
             edition = \"2021\"\npublish = false\n\n\
             [lib]\ncrate-type = [\"cdylib\"]\npath = {lib:?}\n\n\
             [dependencies]\ncufflink-fn = {{ path = {sdk:?} }}\n{dependencies}\n\
             [features]\n{features}\n[workspace]\n",
            lib = manifest_dir.join("guests").join(format!("{source}.rs")),
            sdk = workspace.join("cufflink-fn"),
        ),
    )?;
    fs::copy(workspace.join("Cargo.lock"), dir.join("Cargo.lock"))?;
    Ok(())
}

fn build(cargo: &str, guest: &Guest, profile: Profile, out: &Path) -> Result<(), Box<dyn Error>> {
    let target_dir = out.join("target");
    let (cargo_args, output_dir, profile_env): (&[&str], _, &[(&str, &str)]) = match profile {
        Profile::Debug => (&[], "debug", &[]),
        Profile::ReleaseSplit => (
            &["--release"],
            "release",
            &[
                ("CARGO_INCREMENTAL", "1"),
                ("CARGO_PROFILE_RELEASE_INCREMENTAL", "true"),
                ("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "256"),
                ("CARGO_PROFILE_RELEASE_LTO", "false"),
            ],
        ),
    };
    let status = OUTER_BUILD_ENV
        .iter()
        .fold(Command::new(cargo), |mut command, name| {
            command.env_remove(name);
            command
        })
        .current_dir(out.join(guest.source))
        .args(["build", "--lib", "--target", TARGET, "--target-dir"])
        .arg(&target_dir)
        .args(cargo_args)
        .args(["--features", &guest.features.join(",")])
        .env("RUSTC_WRAPPER", "")
        .envs(profile_env.iter().copied())
        .status()?;
    if !status.success() {
        return Err(format!(
            "building guest {} ({}): {status}",
            guest.name,
            profile.name()
        )
        .into());
    }
    fs::copy(
        target_dir
            .join(TARGET)
            .join(output_dir)
            .join(format!("cufflink_guest_{}.wasm", guest.source)),
        out.join(format!("{}-{}.wasm", guest.name, profile.name())),
    )?;
    Ok(())
}
