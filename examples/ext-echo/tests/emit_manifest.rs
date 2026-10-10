use cufflink_types::features::NATIVE_SERVICES;
use cufflink_types::{ProvidedExtension, ServiceManifest, ServiceMode};
use std::process::Command;

#[test]
fn the_binary_emits_its_native_manifest() {
    let output = Command::new(env!("CARGO_BIN_EXE_ext-echo"))
        .arg("--emit-manifest")
        .env_remove("CUFFLINK_EXT_SOCKET")
        .env_remove("EXT_ECHO_EXTENSIONS")
        .output()
        .expect("run ext-echo");
    assert!(output.status.success(), "{output:?}");
    let manifest: ServiceManifest =
        serde_json::from_slice(&output.stdout).expect("a manifest on stdout");
    assert_eq!(manifest.name, "ext-echo");
    assert_eq!(manifest.mode, ServiceMode::Native);
    assert!(manifest.tables.is_empty());
    assert_eq!(
        manifest.provides,
        ["echo.fail", "echo.reply", "echo.sleep"].map(|name| ProvidedExtension {
            name: name.into(),
            callers: vec!["*".into()],
        })
    );
    assert_eq!(manifest.required_platform_features, [NATIVE_SERVICES]);
}

#[test]
fn the_binary_provides_the_extensions_it_is_told_to() {
    let output = Command::new(env!("CARGO_BIN_EXE_ext-echo"))
        .arg("--emit-manifest")
        .env("EXT_ECHO_EXTENSIONS", "other.sleep, other.reply,")
        .output()
        .expect("run ext-echo");
    assert!(output.status.success(), "{output:?}");
    let manifest: ServiceManifest =
        serde_json::from_slice(&output.stdout).expect("a manifest on stdout");
    assert_eq!(
        manifest.provides,
        ["other.reply", "other.sleep"].map(|name| ProvidedExtension {
            name: name.into(),
            callers: vec!["*".into()],
        })
    );
}

#[test]
fn without_a_socket_the_binary_fails_saying_why() {
    let output = Command::new(env!("CARGO_BIN_EXE_ext-echo"))
        .env_remove("CUFFLINK_EXT_SOCKET")
        .output()
        .expect("run ext-echo");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("CUFFLINK_EXT_SOCKET must name the socket to serve on"),
        "{output:?}"
    );
}

#[test]
fn without_the_runners_token_the_binary_fails_saying_why() {
    let output = Command::new(env!("CARGO_BIN_EXE_ext-echo"))
        .env("CUFFLINK_EXT_SOCKET", "/tmp/ext-echo-never-bound.sock")
        .env_remove("CUFFLINK_EXT_TOKEN")
        .output()
        .expect("run ext-echo");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("CUFFLINK_EXT_TOKEN must hold the token the runner sends with each request"),
        "{output:?}"
    );
    assert!(!std::path::Path::new("/tmp/ext-echo-never-bound.sock").exists());
}
