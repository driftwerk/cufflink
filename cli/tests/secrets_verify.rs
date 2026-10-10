use cufflink_testkit as common;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::PlatformProcess;
use reqwest::Method;
use serde_json::json;
use std::process::Command;

fn kek(byte: u8) -> String {
    STANDARD.encode([byte; 32])
}

struct Ran {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn reseal(platform: &PlatformProcess, admin_key: &str, arguments: &[&str]) -> Ran {
    let output = Command::new(env!("CARGO_BIN_EXE_cufflink"))
        .args([
            "admin",
            "--api-url",
            &platform.base_url,
            "secrets",
            "reseal",
        ])
        .args(arguments)
        .current_dir(env!("CARGO_TARGET_TMPDIR"))
        .env("HOME", env!("CARGO_TARGET_TMPDIR"))
        .env("CUFFLINK_PLATFORM_API_KEY", admin_key)
        .output()
        .expect("the cufflink binary runs");
    Ran {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn verify(platform: &PlatformProcess, admin_key: &str) -> Ran {
    reseal(platform, admin_key, &["--operation", "verify"])
}

const CLEAN_ROWS: &str =
    "  Rows:      0 unopenable, 0 not sealed, 0 whose plaintext differs, 0 under a retired data key";

#[tokio::test]
async fn cf9_verify_across_a_kek_rotation() {
    if common::require_binary("cufflink-platform").is_none() {
        return;
    }
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let (old, new) = (kek(1), kek(2));
    let previous = format!("a:{old}");
    let database = db.url.clone();
    let start = |env: Vec<(&'static str, String)>| {
        let (url, nats) = (database.clone(), infra.nats_url().map(str::to_string));
        async move {
            let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
            let platform = PlatformProcess::start(&url, nats.as_deref(), &env).await;
            platform.wait_ready().await;
            platform
        }
    };
    let under = |key: &str, id: &str| {
        vec![
            ("CUFFLINK_SECRETS_KEK", key.to_string()),
            ("CUFFLINK_SECRETS_KEK_ID", id.to_string()),
        ]
    };

    let platform = start(under(&old, "a")).await;
    let tenant = db.mint_tenant().await;
    let admin = db.mint_admin_key().await;
    let manifest = common::wasm_manifest("cfg", &[("echo", "/echo")]);
    let (status, deployed) = platform.deploy(&tenant, &manifest).await;
    assert_eq!(status, 200, "{deployed}");
    let service = deployed["service_id"].as_str().expect("service id");
    let (status, set) = platform
        .send(
            Method::PUT,
            &format!("/api/services/{service}/config"),
            &format!("ApiKey {}", tenant.api_key),
            Some(&json!({"key": "TOKEN", "value": "sealed-under-a", "is_secret": true})),
        )
        .await;
    assert_eq!(status, 200, "{set}");

    let clean = verify(&platform, &admin);
    assert_eq!(clean.code, Some(0), "{}{}", clean.stdout, clean.stderr);
    assert_eq!(
        clean.stdout.lines().collect::<Vec<_>>(),
        [
            "Verified secrets (every tenant): 1 rows checked",
            "  Data keys: 1 checked, 0 unopenable, 0 not under the current key-encryption key",
            CLEAN_ROWS,
        ]
    );
    let scoped = reseal(
        &platform,
        &admin,
        &["--operation", "verify", "--tenant", &tenant.slug],
    );
    assert_eq!(scoped.code, Some(0), "{}", scoped.stderr);
    assert!(
        scoped.stdout.starts_with(&format!(
            "Verified secrets ({}): 1 rows checked",
            tenant.slug
        )),
        "{}",
        scoped.stdout
    );
    drop(platform);

    let without_the_old = start(under(&new, "b")).await;
    let lost = verify(&without_the_old, &admin);
    assert_eq!(
        lost.code,
        Some(1),
        "a ring that lost the key the data keys are wrapped under: {}{}",
        lost.stdout,
        lost.stderr
    );
    assert_eq!(
        lost.stdout.lines().collect::<Vec<_>>(),
        [
            "Verified secrets (every tenant): 1 rows checked",
            "  Data keys: 1 checked, 1 unopenable, 1 not under the current key-encryption key",
            "  Rows:      1 unopenable, 0 not sealed, 0 whose plaintext differs, 0 under a retired \
             data key",
            "    service_configs: 1 unopenable",
            "  Not finished: 1 data keys are not under the current key-encryption key. Run `reseal \
             --operation rotate_kek` before dropping a previous key.",
        ],
        "the counts are on stdout whatever was found"
    );
    assert!(
        lost.stderr.starts_with(
            "Not verified: 1 data keys do not unwrap: put the key-encryption key each names \
             back in CUFFLINK_SECRETS_KEK_PREVIOUS"
        ),
        "what was found is an answer, told bare and on stderr: {}",
        lost.stderr
    );
    assert!(!lost.stderr.contains("Verified secrets") && !lost.stdout.contains("Not verified"));
    assert!(!lost.stdout.contains("sealed-under-a") && !lost.stderr.contains("sealed-under-a"));
    drop(without_the_old);

    let mut rotating_env = under(&new, "b");
    rotating_env.push(("CUFFLINK_SECRETS_KEK_PREVIOUS", previous));
    let rotating = start(rotating_env).await;
    let unfinished = verify(&rotating, &admin);
    assert_eq!(unfinished.code, Some(0), "{}", unfinished.stderr);
    assert!(
        unfinished.stdout.contains(
            "  Data keys: 1 checked, 0 unopenable, 1 not under the current key-encryption key\n"
        ) && unfinished
            .stdout
            .contains("  Not finished: 1 data keys are not under"),
        "before the rotation the old key is still in use: {}",
        unfinished.stdout
    );
    let rotated = reseal(&rotating, &admin, &["--operation", "rotate_kek"]);
    assert_eq!(rotated.code, Some(0), "{}", rotated.stderr);
    assert!(
        rotated.stdout.contains("1 data keys rewrapped"),
        "{}",
        rotated.stdout
    );
    drop(rotating);

    let only_the_new = start(under(&new, "b")).await;
    let after = verify(&only_the_new, &admin);
    assert_eq!(
        after.code,
        Some(0),
        "the old key can be dropped: {}{}",
        after.stdout,
        after.stderr
    );
    assert_eq!(
        after.stdout.lines().collect::<Vec<_>>(),
        [
            "Verified secrets (every tenant): 1 rows checked",
            "  Data keys: 1 checked, 0 unopenable, 0 not under the current key-encryption key",
            CLEAN_ROWS,
        ]
    );
    let audited: i64 = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM audit_log
             WHERE action = 'secrets.reseal' AND details->>'operation' = 'verify'
               AND details ? 'verified'",
            &[],
        )
        .await
        .expect("audit")
        .get(0);
    assert_eq!(audited, 5, "each verify is audited with what it found");
}
