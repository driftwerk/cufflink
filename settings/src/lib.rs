use eyre::eyre;
use std::collections::HashMap;
use std::env::VarError;
use std::io;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

pub const SECRET_VARS: &[&str] = &[
    "DATABASE_URL",
    "TENANT_DATABASE_URL",
    "REDIS_URL",
    "NATS_PASSWORD",
    "NATS_NKEY_SEED",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "CUFFLINK_SECRETS_KEK",
    "CUFFLINK_SECRETS_KEK_PREVIOUS",
    "CUFFLINK_CACHE_SIGNING_KEY",
    "CUFFLINK_SERVICE_IDENTITY_KEYS",
    "CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET",
    "WEB_STATUS_TOKEN",
    "EXT_STATUS_TOKEN",
];

pub fn secret_file_var(name: &str) -> String {
    format!("{name}_FILE")
}

pub fn secret_var(
    name: &str,
    get: impl Fn(&str) -> Option<String>,
    read: impl Fn(&Path) -> io::Result<Vec<u8>>,
) -> eyre::Result<Option<String>> {
    let file_var = secret_file_var(name);
    let set = |value: &String| !value.trim().is_empty();
    match (get(name), get(&file_var).filter(set)) {
        (plain, None) => Ok(plain),
        (Some(plain), Some(_)) if set(&plain) => {
            Err(eyre!("{name} and {file_var} are both set; set one"))
        }
        (_, Some(path)) => {
            let refused = |why: &str| eyre!("{file_var} names {path}, which {why}");
            let content = read(Path::new(&path))
                .map_err(|e| refused(&format!("cannot be read: {e}")))
                .and_then(|bytes| String::from_utf8(bytes).map_err(|_| refused("is not UTF-8")))?;
            let secret = content
                .strip_suffix('\n')
                .map(|line| line.strip_suffix('\r').unwrap_or(line))
                .unwrap_or(&content);
            match secret.trim().is_empty() {
                true => Err(refused("is empty")),
                false => Ok(Some(secret.to_string())),
            }
        }
    }
}

pub fn with_secret_files(
    get: impl Fn(&str) -> Option<String>,
    read: impl Fn(&Path) -> io::Result<Vec<u8>>,
) -> eyre::Result<impl Fn(&str) -> Option<String>> {
    let secrets = SECRET_VARS
        .iter()
        .map(|name| secret_var(name, &get, &read).map(|secret| (*name, secret)))
        .collect::<eyre::Result<HashMap<_, _>>>()?;
    Ok(move |name: &str| match secrets.get(name) {
        Some(secret) => secret.clone(),
        None => get(name),
    })
}

pub fn from_env() -> eyre::Result<impl Fn(&str) -> Option<String>> {
    secret_file_vars_are_utf8(|name| std::env::var(name))?;
    with_secret_files(process_var, |path| std::fs::read(path))
}

pub fn process_var(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

pub fn fixed(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let vars: HashMap<String, String> = pairs
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    move |name: &str| vars.get(name).cloned()
}

fn secret_file_vars_are_utf8(var: impl Fn(&str) -> Result<String, VarError>) -> eyre::Result<()> {
    SECRET_VARS
        .iter()
        .map(|name| secret_file_var(name))
        .try_for_each(|file_var| match var(&file_var) {
            Err(VarError::NotUnicode(_)) => Err(eyre!("{file_var} is not UTF-8")),
            Ok(_) | Err(VarError::NotPresent) => Ok(()),
        })
}

pub fn parse_var<T>(name: &str, raw: Option<String>, default: T) -> eyre::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match raw {
        Some(raw) => raw
            .trim()
            .parse()
            .map_err(|e| eyre!("{name} has an invalid value {raw:?}: {e}")),
        None => Ok(default),
    }
}

pub fn env_var<T>(name: &str, default: T) -> eyre::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    parse_var(name, process_var(name), default)
}

pub fn env_secs(name: &str, default_secs: u64) -> eyre::Result<Duration> {
    env_var(name, default_secs).map(Duration::from_secs)
}

pub fn positive_var<T>(name: &str, raw: Option<String>, default: T) -> eyre::Result<T>
where
    T: FromStr + PartialOrd + Default,
    T::Err: std::fmt::Display,
{
    parse_var(name, raw, default).and_then(|value| match value > T::default() {
        true => Ok(value),
        false => Err(eyre!("{name} must be greater than 0")),
    })
}

pub struct Vars<F>(pub F);

impl<F: Fn(&str) -> Option<String>> Vars<F> {
    pub fn get(&self, name: &str) -> Option<String> {
        (self.0)(name)
    }

    pub fn required(&self, name: &str) -> eyre::Result<String> {
        self.get(name)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| eyre!("Missing required environment variable: {name}"))
    }

    pub fn parsed<T>(&self, name: &str, default: T) -> eyre::Result<T>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        parse_var(name, self.get(name), default)
    }

    pub fn positive<T>(&self, name: &str, default: T) -> eyre::Result<T>
    where
        T: FromStr + PartialOrd + Default,
        T::Err: std::fmt::Display,
    {
        positive_var(name, self.get(name), default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "s3cr3t-value";

    fn lookup<'a>(
        vars: &'a [(&'a str, &'a str)],
        files: &'a [(&'a str, &'a [u8])],
    ) -> eyre::Result<impl Fn(&str) -> Option<String> + 'a> {
        with_secret_files(
            |name| {
                vars.iter()
                    .find(|(var, _)| *var == name)
                    .map(|(_, value)| value.to_string())
            },
            |path| {
                files
                    .iter()
                    .find(|(file, _)| Path::new(file) == path)
                    .map(|(_, content)| content.to_vec())
                    .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
            },
        )
    }

    fn refusal(vars: &[(&str, &str)], files: &[(&str, &[u8])]) -> String {
        lookup(vars, files)
            .err()
            .map(|error| format!("{error:#}"))
            .expect("refused")
    }

    #[test]
    fn a_secret_is_read_from_its_variable() {
        let vars = lookup(
            &[("DATABASE_URL", " postgres://db "), ("REDIS_URL", "")],
            &[],
        )
        .expect("lookup");
        assert_eq!(vars("DATABASE_URL").as_deref(), Some(" postgres://db "));
        assert_eq!(vars("REDIS_URL").as_deref(), Some(""));
        assert_eq!(vars("NATS_PASSWORD"), None);
    }

    #[test]
    fn a_secret_is_read_from_its_file() {
        let vars = lookup(
            &[
                ("DATABASE_URL_FILE", "/run/secrets/db"),
                ("NATS_URL", "nats://nats"),
            ],
            &[("/run/secrets/db", b"postgres://db")],
        )
        .expect("lookup");
        assert_eq!(vars("DATABASE_URL").as_deref(), Some("postgres://db"));
        assert_eq!(
            vars("DATABASE_URL_FILE").as_deref(),
            Some("/run/secrets/db")
        );
        assert_eq!(vars("NATS_URL").as_deref(), Some("nats://nats"));
        for name in SECRET_VARS {
            let file_var = secret_file_var(name);
            let given = [(file_var.as_str(), "/run/secrets/one")];
            let files = [("/run/secrets/one", SECRET.as_bytes())];
            let vars = lookup(&given, &files).expect(name);
            assert_eq!(vars(name).as_deref(), Some(SECRET), "{name}");
        }
    }

    #[test]
    fn both_set_is_refused() {
        assert_eq!(
            refusal(
                &[
                    ("CUFFLINK_SECRETS_KEK", "a2V5"),
                    ("CUFFLINK_SECRETS_KEK_FILE", "/run/secrets/kek"),
                ],
                &[("/run/secrets/kek", b"a2V5")],
            ),
            "CUFFLINK_SECRETS_KEK and CUFFLINK_SECRETS_KEK_FILE are both set; set one"
        );
    }

    #[test]
    fn a_blank_variable_beside_a_file_is_unset() {
        let vars = lookup(
            &[
                ("REDIS_URL", "  "),
                ("REDIS_URL_FILE", "/run/secrets/redis"),
                ("NATS_PASSWORD", "pw"),
                ("NATS_PASSWORD_FILE", " "),
            ],
            &[("/run/secrets/redis", b"redis://cache")],
        )
        .expect("lookup");
        assert_eq!(vars("REDIS_URL").as_deref(), Some("redis://cache"));
        assert_eq!(vars("NATS_PASSWORD").as_deref(), Some("pw"));
    }

    #[test]
    fn a_missing_file_is_refused() {
        let refused = refusal(&[("S3_SECRET_KEY_FILE", "/run/secrets/absent")], &[]);
        assert!(
            refused.starts_with(
                "S3_SECRET_KEY_FILE names /run/secrets/absent, which cannot be read: "
            ),
            "{refused}"
        );
    }

    #[test]
    fn an_empty_file_is_refused() {
        for content in [&b""[..], b"\n", b"\r\n", b" \n", b"\n\n", b" \t\r\n "] {
            assert_eq!(
                refusal(
                    &[("WEB_STATUS_TOKEN_FILE", "/run/secrets/token")],
                    &[("/run/secrets/token", content)],
                ),
                "WEB_STATUS_TOKEN_FILE names /run/secrets/token, which is empty"
            );
        }
    }

    #[test]
    fn a_file_that_is_not_utf8_is_refused() {
        assert_eq!(
            refusal(
                &[("NATS_NKEY_SEED_FILE", "/run/secrets/seed")],
                &[("/run/secrets/seed", &[b'S', 0xff, 0xfe])],
            ),
            "NATS_NKEY_SEED_FILE names /run/secrets/seed, which is not UTF-8"
        );
    }

    #[test]
    fn one_trailing_newline_is_dropped() {
        for (content, secret) in [
            (&b"key\n"[..], "key"),
            (b"key\r\n", "key"),
            (b"key\n\n", "key\n"),
            (b"key\r\n\r\n", "key\r\n"),
            (b" key \n", " key "),
            (b"key\r", "key\r"),
            (b"key", "key"),
        ] {
            let files = [("/run/secrets/token", content)];
            let vars =
                lookup(&[("EXT_STATUS_TOKEN_FILE", "/run/secrets/token")], &files).expect("lookup");
            assert_eq!(vars("EXT_STATUS_TOKEN").as_deref(), Some(secret));
        }
    }

    #[test]
    fn no_error_names_the_content() {
        let file = [("/run/secrets/s", SECRET.as_bytes())];
        let not_utf8 = [("/run/secrets/s", &b"s3cr3t-value\xff"[..])];
        for refused in [
            refusal(
                &[
                    ("CUFFLINK_CACHE_SIGNING_KEY", SECRET),
                    ("CUFFLINK_CACHE_SIGNING_KEY_FILE", "/run/secrets/s"),
                ],
                &file,
            ),
            refusal(
                &[("CUFFLINK_CACHE_SIGNING_KEY_FILE", "/run/secrets/s")],
                &not_utf8,
            ),
        ] {
            assert!(refused.contains("CUFFLINK_CACHE_SIGNING_KEY"), "{refused}");
            assert!(!refused.contains("s3cr3t"), "{refused}");
        }
    }

    #[test]
    fn a_setting_that_is_no_secret_ignores_its_file() {
        let vars = lookup(
            &[
                ("PLATFORM_HOST_FILE", "/run/secrets/absent"),
                ("CUFFLINK_RUNNER_KEY_FILE", "/run/secrets/absent"),
                ("CUFFLINK_SECRETS_KEK_ID_FILE", "/run/secrets/absent"),
            ],
            &[],
        )
        .expect("lookup");
        for name in [
            "PLATFORM_HOST",
            "CUFFLINK_RUNNER_KEY",
            "CUFFLINK_SECRETS_KEK_ID",
        ] {
            assert_eq!(vars(name), None, "{name}");
        }
        assert!(!SECRET_VARS.contains(&"AWS_SECRET_ACCESS_KEY"));
    }

    #[test]
    fn a_file_variable_that_is_not_utf8_is_refused_by_name() {
        use std::os::unix::ffi::OsStringExt;
        let not_utf8 = |name: &str| {
            secret_file_vars_are_utf8(|var| match var == name {
                true => Err(VarError::NotUnicode(std::ffi::OsString::from_vec(vec![
                    b'/', 0xff,
                ]))),
                false => Err(VarError::NotPresent),
            })
            .map_err(|error| error.to_string())
        };
        assert_eq!(
            not_utf8("REDIS_URL_FILE"),
            Err("REDIS_URL_FILE is not UTF-8".to_string())
        );
        assert_eq!(not_utf8("REDIS_URL"), Ok(()));
        assert_eq!(not_utf8("PLATFORM_HOST_FILE"), Ok(()));
        assert_eq!(
            secret_file_vars_are_utf8(|_| Ok("/run/secrets/one".to_string()))
                .map_err(|error| error.to_string()),
            Ok(())
        );
    }

    #[test]
    fn a_positive_variable_is_refused_at_zero_or_below_and_when_it_does_not_parse() {
        assert_eq!(positive_var("N", None, 32usize).expect("default"), 32);
        assert_eq!(
            positive_var("N", Some(" 8 ".into()), 32usize).expect("set"),
            8
        );
        let zero = positive_var("WORKER_MAX_CONCURRENT_SYNC", Some("0".into()), 32usize)
            .expect_err("zero");
        assert_eq!(
            zero.to_string(),
            "WORKER_MAX_CONCURRENT_SYNC must be greater than 0"
        );
        assert!(positive_var("N", Some("-1".into()), 32usize).is_err());
        assert!(positive_var("N", Some("-1".into()), 32i64).is_err());
        assert!(positive_var("N", Some("soon".into()), 32u64).is_err());
        let vars = Vars(|name: &str| (name == "N").then(|| "3".to_string()));
        assert_eq!(vars.positive("N", 1u32).expect("set"), 3);
        assert_eq!(vars.positive("M", 1u32).expect("unset"), 1);
        assert_eq!(vars.parsed("M", 0u32).expect("unset"), 0);
    }

    #[test]
    fn a_required_variable_is_trimmed_and_refused_by_name_when_unset_or_blank() {
        let vars = Vars(|name: &str| match name {
            "SET" => Some(" value ".to_string()),
            "BLANK" => Some("  ".to_string()),
            _ => None,
        });
        assert_eq!(vars.required("SET").expect("set"), "value");
        for name in ["BLANK", "UNSET"] {
            assert_eq!(
                vars.required(name).expect_err(name).to_string(),
                format!("Missing required environment variable: {name}")
            );
        }
    }
}
