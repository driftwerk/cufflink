use std::process::Command;

fn said(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn a_secret_file_that_is_missing_or_beside_its_variable_stops_the_runner() {
    let runner = |database_url: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cufflink-web-runner"));
        command
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("CUFFLINK_BASE_DOMAIN", "example.com")
            .env("DATABASE_URL_FILE", "/run/secrets/absent")
            .envs(database_url.map(|url| ("DATABASE_URL", url)));
        command.output().expect("the runner runs")
    };
    let both = runner(Some("postgres://nobody:s3cret@127.0.0.1:1/nowhere"));
    assert!(!both.status.success());
    let both = said(&both);
    assert!(
        both.contains("DATABASE_URL and DATABASE_URL_FILE are both set; set one"),
        "{both}"
    );
    assert!(!both.contains("s3cret"), "no secret is echoed: {both}");

    let missing = runner(None);
    assert!(!missing.status.success());
    let missing = said(&missing);
    assert!(
        missing.contains("DATABASE_URL_FILE names /run/secrets/absent, which cannot be read"),
        "{missing}"
    );
}

#[test]
fn a_key_ring_that_does_not_parse_stops_the_runner_before_it_connects() {
    let output = Command::new(env!("CARGO_BIN_EXE_cufflink-web-runner"))
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:1/nowhere")
        .env("CUFFLINK_BASE_DOMAIN", "example.com")
        .env("CUFFLINK_SECRETS_KEK", "not base64 !")
        .env("CUFFLINK_SECRETS_KEK_ID", "k1")
        .output()
        .expect("the runner runs");
    assert!(!output.status.success());
    let said = said(&output);
    assert!(said.contains("CUFFLINK_SECRETS_KEK"), "{said}");
    assert!(
        !said.contains("not base64 !"),
        "the key is never echoed: {said}"
    );
    assert!(
        !said.contains("Connected to PostgreSQL"),
        "it stops before it connects: {said}"
    );
}
