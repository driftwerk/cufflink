//! `cufflink dev`: a platform on this machine, run by Docker Compose from
//! files the CLI carries, with a tenant, a key and users that are there
//! when it is ready.

use crate::commands::{listing, version};
use crate::credentials::{self, Credentials};
use crate::project_config::{EnvironmentConfig, ProjectConfig};
use clap::Subcommand;
use eyre::{bail, eyre, WrapErr};
use notify::{EventKind, RecursiveMode, Watcher};
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

/// The environment a project has without naming it.
pub const ENV: &str = "dev";
const TENANT: &str = "dev";
const REALM: &str = "cufflink-dev";
const CLIENT: &str = "cufflink-cli";
const KEY_FILE: &str = "dev-key";
const RUNNER_KEY_FILE: &str = "runner-key";
const READY_WITHIN: Duration = Duration::from_secs(120);
const READY_POLL: Duration = Duration::from_secs(2);
const QUIET: Duration = Duration::from_millis(500);
/// The longest a watch gathers changes before it deploys what it has.
const SETTLE_AT_MOST: Duration = Duration::from_secs(5);
const PROBE: Duration = Duration::from_millis(300);
/// The files and directories of a project whose changes are deployed.
const WATCHED: [&str; 4] = ["src", "migrations", "Cargo.toml", "Cufflink.toml"];

const COMPOSE: &str = include_str!("../../assets/dev-compose.yml");
const BUILT_FROM_SOURCE: &str = include_str!("../../assets/dev-compose.source.yml");
/// The tag this CLI was built for: CI stamps the CLI and the images it
/// publishes with the same `sha-<commit>`. A local build has none.
const BUILD_TAG: Option<&str> = option_env!("CUFFLINK_VERSION");
/// The platform's own services, and the placeholder of each one's image.
const OWN_SERVICES: [(&str, &str); 4] = [
    ("platform", "platform_image"),
    ("worker", "worker_image"),
    ("web-runner", "web_runner_image"),
    ("ext-runner", "ext_runner_image"),
];
const REALM_EXPORT: &str = include_str!("../../assets/cufflink-dev-realm.json");

#[derive(Subcommand)]
pub enum DevAction {
    /// Start the stack and wait until the platform is ready
    Up {
        /// Build the platform, worker and runners from a checkout of Cufflink
        #[arg(long, value_name = "REPO", conflicts_with_all = ["platform_image", "worker_image", "web_runner_image", "ext_runner_image"])]
        from_source: Option<PathBuf>,
        /// The platform image, in place of the published one
        #[arg(long, value_name = "REF")]
        platform_image: Option<String>,
        /// The worker image, in place of the published one
        #[arg(long, value_name = "REF")]
        worker_image: Option<String>,
        /// The web-runner image, in place of the published one
        #[arg(long, value_name = "REF")]
        web_runner_image: Option<String>,
        /// The ext-runner image, in place of the published one
        #[arg(long, value_name = "REF")]
        ext_runner_image: Option<String>,
        /// Run the web-runner too, for `mode = "web"` services
        #[arg(long)]
        with_web_runner: bool,
        /// Run the ext-runner too, for `mode = "native"` services, and let
        /// the platform take extensions
        #[arg(long)]
        with_ext_runner: bool,
    },
    /// Stop the stack
    Down {
        /// Remove its data too
        #[arg(long)]
        volumes: bool,
    },
    /// Print the logs of the stack, or of one of its services
    Logs {
        service: Option<String>,
        /// Keep printing as they are written
        #[arg(short, long)]
        follow: bool,
    },
    /// Print an access token of a user of the development realm
    Token {
        /// `alice` may deploy, `bob` may not
        #[arg(long)]
        user: String,
        /// The user's password; a user of the stack has their name as it
        #[arg(long)]
        password: Option<String>,
    },
    /// Deploy the project to the stack
    Deploy {
        /// Deploy again whenever the project changes
        #[arg(long)]
        watch: bool,
    },
    /// Check that this machine can run the stack
    Doctor {
        /// Check the web-runner's port and image too, as `up --with-web-runner` runs it
        #[arg(long)]
        with_web_runner: bool,
        /// Check the ext-runner's port and image too, as `up --with-ext-runner` runs it
        #[arg(long)]
        with_ext_runner: bool,
    },
}

/// A service of the stack that runs only when asked for, as the Compose
/// profile of its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Profile {
    WebRunner,
    ExtRunner,
}

impl Profile {
    const ALL: [Self; 2] = [Self::WebRunner, Self::ExtRunner];

    fn name(self) -> &'static str {
        match self {
            Self::WebRunner => "web-runner",
            Self::ExtRunner => "ext-runner",
        }
    }

    /// The profiles of the flags `up` and `doctor` take.
    fn asked(web_runner: bool, ext_runner: bool) -> Vec<Self> {
        Self::ALL
            .into_iter()
            .zip([web_runner, ext_runner])
            .filter_map(|(profile, asked)| asked.then_some(profile))
            .collect()
    }
}

/// A port of the stack that this machine publishes.
struct Port {
    of: &'static str,
    placeholder: &'static str,
    number: u16,
}

const PLATFORM_PORT: u16 = 8080;
const KEYCLOAK_PORT: u16 = 8180;
const PORTS: [Port; 6] = [
    Port {
        of: "platform",
        placeholder: "platform_port",
        number: PLATFORM_PORT,
    },
    Port {
        of: "keycloak",
        placeholder: "keycloak_port",
        number: KEYCLOAK_PORT,
    },
    Port {
        of: "postgres",
        placeholder: "postgres_port",
        number: 5432,
    },
    Port {
        of: "nats",
        placeholder: "nats_port",
        number: 4222,
    },
    Port {
        of: "web-runner",
        placeholder: "web_runner_port",
        number: 3100,
    },
    Port {
        of: "ext-runner",
        placeholder: "ext_runner_port",
        number: 3200,
    },
];

fn api_url() -> String {
    format!("http://localhost:{PLATFORM_PORT}")
}

fn keycloak_url() -> String {
    format!("http://localhost:{KEYCLOAK_PORT}")
}

/// The environment `dev` of a project that does not declare one.
pub fn environment() -> EnvironmentConfig {
    EnvironmentConfig {
        api_url: api_url(),
        tenant: TENANT.to_string(),
        api_key_env: None,
        api_key: None,
        keycloak_url: Some(keycloak_url()),
        keycloak_realm: Some(REALM.to_string()),
        keycloak_client_id: Some(CLIENT.to_string()),
        config: Default::default(),
        secrets: Default::default(),
    }
}

/// Where the images of the platform's own services come from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Images {
    Published {
        platform: String,
        worker: String,
        web_runner: String,
        ext_runner: String,
    },
    Source(PathBuf),
}

/// The image CI publishes of `service` for this CLI's build, `latest`
/// for a local build.
fn published(service: &str) -> String {
    format!(
        "kingsleyh/cufflink-{service}:{}",
        BUILD_TAG.unwrap_or("latest")
    )
}

fn built(service: &str) -> String {
    format!("cufflink-dev-{service}:source")
}

impl Images {
    fn chosen(
        from_source: Option<PathBuf>,
        platform: Option<String>,
        worker: Option<String>,
        web_runner: Option<String>,
        ext_runner: Option<String>,
    ) -> eyre::Result<Self> {
        match from_source {
            Some(repo) => {
                let repo = repo
                    .canonicalize()
                    .wrap_err_with(|| format!("{} is not a directory", repo.display()))?;
                if !repo.join("Dockerfile.platform").is_file() {
                    bail!("{} is not a checkout of Cufflink", repo.display());
                }
                Ok(Self::Source(repo))
            }
            None => Ok(Self::Published {
                platform: platform.unwrap_or_else(|| published("platform")),
                worker: worker.unwrap_or_else(|| published("worker")),
                web_runner: web_runner.unwrap_or_else(|| published("web-runner")),
                ext_runner: ext_runner.unwrap_or_else(|| published("ext-runner")),
            }),
        }
    }

    /// Each of the platform's own services with its image.
    fn named(&self) -> [(&'static str, &'static str, String); 4] {
        let images = match self {
            Self::Published {
                platform,
                worker,
                web_runner,
                ext_runner,
            } => [
                platform.clone(),
                worker.clone(),
                web_runner.clone(),
                ext_runner.clone(),
            ],
            Self::Source(_) => OWN_SERVICES.map(|(service, _)| built(service)),
        };
        let [(platform, p), (worker, w), (web_runner, r), (ext_runner, e)] = OWN_SERVICES;
        let [platform_image, worker_image, web_runner_image, ext_runner_image] = images;
        [
            (platform, p, platform_image),
            (worker, w, worker_image),
            (web_runner, r, web_runner_image),
            (ext_runner, e, ext_runner_image),
        ]
    }

    /// Whether the stack runs an image of a local build's defaults, which
    /// need not be of the CLI's version.
    fn runs_latest(&self) -> bool {
        BUILD_TAG.is_none()
            && self
                .named()
                .iter()
                .any(|(service, _, image)| *image == published(service))
    }
}

/// The Compose file that runs each published image on `linux/amd64`, the
/// one platform CI builds them for; an image named in its place runs as it
/// was built.
fn published_overlay(images: &Images) -> String {
    let emulated: String = images
        .named()
        .iter()
        .filter(|(service, _, image)| *image == published(service))
        .map(|(service, _, _)| format!("  {service}:\n    platform: linux/amd64\n"))
        .collect();
    let services = if emulated.is_empty() {
        "services: {}\n".to_string()
    } else {
        format!("services:\n{emulated}")
    };
    format!(
        "# Written by `cufflink dev up`: the published images are built for\n\
         # linux/amd64 alone.\n{services}"
    )
}

/// A value as a YAML scalar, whatever it holds.
fn scalar(value: &str) -> eyre::Result<String> {
    Ok(serde_json::to_string(value)?)
}

/// `template` with every `{{name}}` replaced by its value.
fn rendered(template: &str, values: &[(&str, String)]) -> eyre::Result<String> {
    let text = values
        .iter()
        .fold(template.to_string(), |text, (name, value)| {
            text.replace(&format!("{{{{{name}}}}}"), value)
        });
    match text.split_once("{{") {
        Some((_, unfilled)) => bail!(
            "the development stack has no value for '{}'",
            unfilled.split("}}").next().unwrap_or_default()
        ),
        None => Ok(text),
    }
}

/// The files of the stack: what Compose is told, and what the realm holds.
struct Files {
    compose: String,
    mode: String,
}

/// The files of a stack of `images` whose containers write as `user`, with
/// extensions taken by its platform when it runs the ext-runner.
fn files(images: &Images, user: &str, profiles: &[Profile]) -> eyre::Result<Files> {
    let extensions = profiles.contains(&Profile::ExtRunner).to_string();
    let values = images
        .named()
        .into_iter()
        .map(|(_, placeholder, image)| scalar(&image).map(|image| (placeholder, image)))
        .chain(
            PORTS
                .iter()
                .map(|port| Ok((port.placeholder, port.number.to_string()))),
        )
        .chain(
            [
                ("tenant", TENANT),
                ("realm", REALM),
                ("key_file", KEY_FILE),
                ("runner_key_file", RUNNER_KEY_FILE),
                ("user", user),
                ("extensions", extensions.as_str()),
            ]
            .map(|(name, value)| Ok((name, value.to_string()))),
        )
        .collect::<eyre::Result<Vec<_>>>()?;
    let mode = match images {
        Images::Published { .. } => published_overlay(images),
        Images::Source(repo) => rendered(
            BUILT_FROM_SOURCE,
            &[("source", scalar(&repo.to_string_lossy())?)],
        )?,
    };
    Ok(Files {
        compose: rendered(COMPOSE, &values)?,
        mode,
    })
}

/// A service a stack of these files runs.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Service {
    name: String,
    image: String,
    /// Built by `up` from a checkout rather than pulled.
    built: bool,
}

/// The services a stack of `compose` and `mode` runs with `profiles`, as
/// `up` runs it.
fn services_of(compose: &str, mode: &str, profiles: &[Profile]) -> eyre::Result<Vec<Service>> {
    let stack: serde_yaml::Value = serde_yaml::from_str(compose)?;
    let overlay: serde_yaml::Value = serde_yaml::from_str(mode)?;
    let active = |service: &serde_yaml::Value| {
        service["profiles"].as_sequence().is_none_or(|named| {
            named
                .iter()
                .any(|name| profiles.iter().any(|profile| name == profile.name()))
        })
    };
    Ok(stack["services"]
        .as_mapping()
        .into_iter()
        .flatten()
        .filter(|(_, service)| active(service))
        .filter_map(|(name, service)| {
            let name = name.as_str()?;
            Some(Service {
                name: name.to_string(),
                image: service["image"].as_str()?.to_string(),
                built: !overlay["services"][name]["build"].is_null(),
            })
        })
        .collect())
}

/// The user a container runs as for the owner of `directory` to read what
/// it writes there.
#[cfg(unix)]
fn owner(directory: &Path) -> eyre::Result<String> {
    use std::os::unix::fs::MetadataExt;
    let held = std::fs::metadata(directory)?;
    Ok(format!("{}:{}", held.uid(), held.gid()))
}

#[cfg(not(unix))]
fn owner(_directory: &Path) -> eyre::Result<String> {
    Ok("10001:10001".to_string())
}

/// Where the stack's files are: `~/.cufflink/dev`.
struct Stack {
    home: PathBuf,
}

impl Stack {
    fn locate() -> eyre::Result<Self> {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .ok_or_else(|| eyre!("this user has no home directory (HOME is unset)"))?;
        Ok(Self {
            home: PathBuf::from(home).join(".cufflink").join("dev"),
        })
    }

    fn compose_file(&self) -> PathBuf {
        self.home.join("compose.yml")
    }

    fn mode_file(&self) -> PathBuf {
        self.home.join("compose.mode.yml")
    }

    fn run_directory(&self) -> PathBuf {
        self.home.join("run")
    }

    /// Where the extension runner keeps its binaries.
    fn native_directory(&self) -> PathBuf {
        self.home.join("native")
    }

    fn key_file(&self) -> PathBuf {
        self.run_directory().join(KEY_FILE)
    }

    /// Where the platform writes the runner key: the one directory of
    /// `run` the ext-runner sees.
    fn runner_key_directory(&self) -> PathBuf {
        self.run_directory().join("ext-runner")
    }

    fn write(&self, images: &Images, profiles: &[Profile]) -> eyre::Result<()> {
        let realm = self.home.join("realm");
        [
            &realm,
            &self.run_directory(),
            &self.runner_key_directory(),
            &self.native_directory(),
        ]
        .into_iter()
        .try_for_each(std::fs::create_dir_all)?;
        let files = files(images, &owner(&self.run_directory())?, profiles)?;
        [
            (self.compose_file(), files.compose),
            (self.mode_file(), files.mode),
            (
                realm.join("cufflink-dev-realm.json"),
                REALM_EXPORT.to_string(),
            ),
        ]
        .into_iter()
        .try_for_each(|(path, text)| {
            std::fs::write(&path, text).wrap_err_with(|| format!("writing {}", path.display()))
        })
    }

    fn written(&self) -> eyre::Result<()> {
        if self.compose_file().is_file() && self.mode_file().is_file() {
            Ok(())
        } else {
            bail!("there is no development stack; start one with `cufflink dev up`")
        }
    }

    /// `docker`'s arguments for `action` on the stack, with the services
    /// of `profiles` among its own.
    fn arguments(&self, profiles: &[Profile], action: &[&str]) -> Vec<OsString> {
        let files = [self.compose_file(), self.mode_file()]
            .into_iter()
            .flat_map(|file| [OsString::from("-f"), file.into_os_string()]);
        let profiles = profiles
            .iter()
            .flat_map(|profile| ["--profile", profile.name()])
            .map(OsString::from);
        std::iter::once(OsString::from("compose"))
            .chain(files)
            .chain(profiles)
            .chain(action.iter().map(OsString::from))
            .collect()
    }

    fn compose(&self, profiles: &[Profile], action: &[&str]) -> eyre::Result<()> {
        let status = Command::new("docker")
            .args(self.arguments(profiles, action))
            .status()
            .wrap_err("Docker could not be run; `cufflink dev doctor` says what is missing")?;
        if status.success() {
            Ok(())
        } else {
            bail!("docker compose {} failed ({status})", action.join(" "))
        }
    }

    fn running(&self) -> bool {
        self.written().is_ok()
            && answered(Command::new("docker").args(self.arguments(&Profile::ALL, &["ps", "-q"])))
                .is_some_and(|containers| !containers.is_empty())
    }
}

/// What a command printed, when it succeeded.
fn answered(command: &mut Command) -> Option<String> {
    command
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn ready(client: &reqwest::Client) -> bool {
    client
        .get(format!("{}/ready", api_url()))
        .send()
        .await
        .is_ok_and(|answer| answer.status().is_success())
}

/// Wait until the platform is ready, answering the client that asked.
async fn wait_until_ready() -> eyre::Result<reqwest::Client> {
    let client = reqwest::Client::builder()
        .timeout(READY_POLL)
        .build()
        .wrap_err("building an HTTP client")?;
    let deadline = Instant::now() + READY_WITHIN;
    while !ready(&client).await {
        if Instant::now() >= deadline {
            bail!(
                "the platform was not ready within {} s; `cufflink dev logs platform` says why",
                READY_WITHIN.as_secs()
            );
        }
        tokio::time::sleep(READY_POLL).await;
    }
    Ok(client)
}

/// The version the stack's platform says it is.
async fn platform_version(client: &reqwest::Client) -> String {
    let answered = client
        .get(format!("{}/api/version", api_url()))
        .send()
        .await
        .ok()
        .filter(|answer| answer.status().is_success());
    let version = match answered {
        Some(answer) => answer.json::<version::PlatformVersion>().await.ok(),
        None => None,
    };
    version
        .and_then(|version| version.platform_version)
        .map_or_else(
            || "of an unknown version".to_string(),
            |version| format!("version {version}"),
        )
}

fn registered(stack: &Stack) -> eyre::Result<()> {
    let key_file = stack.key_file();
    let api_key = std::fs::read_to_string(&key_file)
        .map(|key| key.trim().to_string())
        .ok()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            eyre!(
                "the platform is ready and wrote no key to {}: its image predates the \
                 development bootstrap; name a newer one with --platform-image, or build it \
                 with --from-source",
                key_file.display()
            )
        })?;
    credentials::save(&Credentials {
        api_key,
        tenant_slug: TENANT.to_string(),
        username: ENV.to_string(),
        api_url: api_url(),
    })
}

async fn up(images: Images, profiles: &[Profile]) -> eyre::Result<()> {
    let stack = Stack::locate()?;
    if images.runs_latest() {
        println!(
            "This CLI is a local build: the stack runs the newest published images (:latest), \
             which need not be of its version. Name images with --platform-image and the like, \
             or build them with --from-source."
        );
    }
    stack.write(&images, profiles)?;
    let action: &[&str] = match images {
        Images::Published { .. } => &["up", "-d"],
        Images::Source(_) => &["up", "-d", "--build"],
    };
    stack.compose(profiles, action)?;
    println!("Waiting for the platform at {} ...", api_url());
    let client = wait_until_ready().await?;
    registered(&stack)?;
    println!();
    println!("The development stack is ready.");
    println!(
        "  Platform   {}  (tenant '{TENANT}', {})",
        api_url(),
        platform_version(&client).await
    );
    println!(
        "  Keycloak   {}  (realm '{REALM}', users alice and bob)",
        keycloak_url()
    );
    println!();
    println!("  cufflink dev deploy --watch       deploy the project as it changes");
    println!("  cufflink dev token --user alice   a token to call its routes with");
    println!("  cufflink --env {ENV} <command>       any other command, against the stack");
    Ok(())
}

#[derive(serde::Deserialize)]
struct Granted {
    access_token: String,
}

async fn token(user: &str, password: Option<&str>) -> eyre::Result<()> {
    let asked = reqwest::Client::new()
        .post(format!(
            "{}/realms/{REALM}/protocol/openid-connect/token",
            keycloak_url()
        ))
        .form(&[
            ("grant_type", "password"),
            ("client_id", CLIENT),
            ("username", user),
            ("password", password.unwrap_or(user)),
        ])
        .send()
        .await
        .wrap_err("Keycloak could not be reached; is the stack up (`cufflink dev up`)?")?;
    let status = asked.status();
    if !status.is_success() {
        bail!(
            "Keycloak gave '{user}' no token ({status}): {}",
            asked.text().await.unwrap_or_default()
        );
    }
    println!("{}", asked.json::<Granted>().await?.access_token);
    Ok(())
}

/// Deploy the project of the working directory as `cufflink --env dev
/// deploy` does, in a process of its own: a deploy that fails, however it
/// fails, leaves a watch running.
fn deployed() -> eyre::Result<bool> {
    Ok(Command::new(std::env::current_exe()?)
        .args(["--env", ENV, "deploy"])
        .status()
        .wrap_err("starting the deploy")?
        .success())
}

/// Whether a change to `changed` is a change to what is deployed.
fn relevant(project: &Path, changed: &Path) -> bool {
    changed
        .strip_prefix(project)
        .ok()
        .and_then(|within| within.components().next())
        .and_then(|first| first.as_os_str().to_str())
        .is_some_and(|first| WATCHED.contains(&first))
}

/// `first` and what follows it until nothing has for `quiet`, or for
/// `longest` in all: a stream of changes that never stops is deployed as
/// it goes.
fn settled<T>(first: T, following: &Receiver<T>, quiet: Duration, longest: Duration) -> Vec<T> {
    let deadline = Instant::now() + longest;
    std::iter::once(first)
        .chain(std::iter::from_fn(|| {
            let left = deadline.saturating_duration_since(Instant::now());
            (!left.is_zero())
                .then(|| following.recv_timeout(quiet.min(left)).ok())
                .flatten()
        }))
        .collect()
}

fn changes(project: &Path, events: Vec<notify::Result<notify::Event>>) -> BTreeSet<PathBuf> {
    events
        .into_iter()
        .filter_map(Result::ok)
        .filter(|event| !matches!(event.kind, EventKind::Access(_)))
        .flat_map(|event| event.paths)
        .filter(|changed| relevant(project, changed))
        .collect()
}

/// Watch the directories of the project that are there and not yet
/// watched. The project itself is watched for its files and for the
/// directories it gains; `target` is never watched.
fn watch_what_is_there(
    watcher: &mut impl Watcher,
    project: &Path,
    watched: BTreeSet<PathBuf>,
) -> eyre::Result<BTreeSet<PathBuf>> {
    std::iter::once((project.to_path_buf(), RecursiveMode::NonRecursive))
        .chain(
            WATCHED
                .iter()
                .map(|name| (project.join(name), RecursiveMode::Recursive))
                .filter(|(directory, _)| directory.is_dir()),
        )
        .try_fold(watched, |mut watched, (directory, mode)| {
            if !watched.contains(&directory) {
                watcher.watch(&directory, mode)?;
                watched.insert(directory);
            }
            Ok(watched)
        })
}

fn report(deployed: eyre::Result<bool>) {
    match deployed {
        Ok(true) => println!("Deployed. Watching for changes ..."),
        Ok(false) => println!("The deploy failed. Watching for changes ..."),
        Err(error) => println!("The deploy could not be run: {error:#}. Watching for changes ..."),
    }
}

fn watch(project: &Path) -> eyre::Result<()> {
    let project = project.canonicalize()?;
    let (sender, events) = std::sync::mpsc::channel();
    let mut watcher = notify::recommended_watcher(sender)?;
    let mut watched = watch_what_is_there(&mut watcher, &project, BTreeSet::new())?;
    report(deployed());
    while let Ok(first) = events.recv() {
        let changed = changes(&project, settled(first, &events, QUIET, SETTLE_AT_MOST));
        watched = watch_what_is_there(&mut watcher, &project, watched)?;
        if let Some(path) = changed.first() {
            println!();
            println!(
                "{} changed{}",
                path.strip_prefix(&project).unwrap_or(path).display(),
                match changed.len() {
                    1 => String::new(),
                    several => format!(" (and {} more)", several - 1),
                }
            );
            report(deployed());
        }
    }
    Ok(())
}

/// Whether `url` is the stack's platform: its port on this machine, by any
/// name of it.
fn is_the_stack(url: &str) -> bool {
    reqwest::Url::parse(url).is_ok_and(|url| {
        let local = url.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
        local
            && url.scheme() == "http"
            && url.port_or_known_default() == Some(PLATFORM_PORT)
            && url.path().trim_end_matches('/').is_empty()
    })
}

/// The directory of `project` when its environment `dev` is the stack's.
fn stack_project(project: ProjectConfig) -> eyre::Result<PathBuf> {
    let declared = &project.get_env(ENV)?.api_url;
    if is_the_stack(declared) {
        Ok(project.project_dir)
    } else {
        bail!(
            "this project's environment '{ENV}' is {declared}, not the development stack at {}; \
             deploy to it with `cufflink --env {ENV} deploy`",
            api_url()
        )
    }
}

async fn deploy(watching: bool) -> eyre::Result<()> {
    let project =
        stack_project(ProjectConfig::find_and_load()?.ok_or_else(|| {
            eyre!("No Cufflink.toml found. Run from a cufflink project directory.")
        })?)?;
    if watching {
        tokio::task::spawn_blocking(move || watch(&project)).await?
    } else if deployed()? {
        Ok(())
    } else {
        bail!("the deploy failed")
    }
}

/// One thing `doctor` looked at: what it found, or what is wrong.
struct Check {
    of: String,
    found: Result<String, String>,
}

fn check(of: impl Into<String>, found: Result<String, String>) -> Check {
    Check {
        of: of.into(),
        found,
    }
}

/// Whether anything answers on `port` of this machine, over IPv4 or IPv6:
/// a bind would succeed beside a listener on every address, which is not
/// a free port.
fn listened_on(port: u16) -> bool {
    [
        std::net::IpAddr::from(std::net::Ipv4Addr::LOCALHOST),
        std::net::IpAddr::from(std::net::Ipv6Addr::LOCALHOST),
    ]
    .into_iter()
    .any(|address| {
        std::net::TcpStream::connect_timeout(&std::net::SocketAddr::new(address, port), PROBE)
            .is_ok()
    })
}

fn port_check(port: &Port, stack_running: bool) -> Check {
    let free = !listened_on(port.number);
    check(
        format!("port {} ({})", port.number, port.of),
        match (free, stack_running) {
            (true, _) => Ok("free".to_string()),
            (false, true) => Ok("held, as the stack is up".to_string()),
            (false, false) => Err("in use by another program".to_string()),
        },
    )
}

fn image_check(service: &Service) -> Check {
    let docker = |arguments: &[&str]| {
        Command::new("docker")
            .args(arguments)
            .arg(&service.image)
            .output()
            .is_ok_and(|output| output.status.success())
    };
    check(
        format!("image {}", service.image),
        if docker(&["image", "inspect"]) {
            Ok("present".to_string())
        } else if service.built {
            Ok("built from the checkout by `cufflink dev up`".to_string())
        } else if docker(&["manifest", "inspect"]) {
            Ok("can be pulled".to_string())
        } else {
            Err("neither present nor to be pulled".to_string())
        },
    )
}

fn doctor(profiles: &[Profile]) -> eyre::Result<()> {
    let stack = Stack::locate()?;
    let tool = |name: &str, arguments: &[&str], missing: &str| {
        check(
            name,
            answered(Command::new("docker").args(arguments)).ok_or_else(|| missing.to_string()),
        )
    };
    let tools = [
        tool(
            "docker",
            &["version", "--format", "{{.Server.Version}}"],
            "not installed, or its daemon is not running",
        ),
        tool(
            "docker compose",
            &["compose", "version", "--short"],
            "the Compose plugin is not installed",
        ),
    ];
    let docker = tools.iter().all(|tool| tool.found.is_ok());
    let running = docker && stack.running();
    let written = std::fs::read_to_string(stack.compose_file())
        .and_then(|compose| Ok((compose, std::fs::read_to_string(stack.mode_file())?)));
    let (compose, mode) = match written {
        Ok(written) => written,
        Err(_) => {
            let files = files(
                &Images::chosen(None, None, None, None, None)?,
                "0:0",
                profiles,
            )?;
            (files.compose, files.mode)
        }
    };
    let services = services_of(&compose, &mode, profiles)?;
    let checks: Vec<Check> = tools
        .into_iter()
        .chain(
            PORTS
                .iter()
                .filter(|port| services.iter().any(|service| service.name == port.of))
                .map(|port| port_check(port, running)),
        )
        .chain(services.iter().filter(|_| docker).map(image_check))
        .collect();
    let mut table = listing(&["CHECK", "", "FOUND"]);
    checks.iter().for_each(|check| {
        let (mark, found) = match &check.found {
            Ok(found) => ("ok", found),
            Err(wrong) => ("FAILED", wrong),
        };
        table.add_row(vec![check.of.as_str(), mark, found.as_str()]);
    });
    println!("{table}");
    match checks.iter().filter(|check| check.found.is_err()).count() {
        0 => Ok(()),
        failed => bail!("{failed} of {} checks failed", checks.len()),
    }
}

pub async fn run(action: DevAction) -> eyre::Result<()> {
    match action {
        DevAction::Up {
            from_source,
            platform_image,
            worker_image,
            web_runner_image,
            ext_runner_image,
            with_web_runner,
            with_ext_runner,
        } => {
            let images = Images::chosen(
                from_source,
                platform_image,
                worker_image,
                web_runner_image,
                ext_runner_image,
            )?;
            up(images, &Profile::asked(with_web_runner, with_ext_runner)).await
        }
        DevAction::Down { volumes } => {
            let stack = Stack::locate()?;
            stack.written()?;
            stack.compose(
                &Profile::ALL,
                if volumes {
                    &["down", "--volumes"]
                } else {
                    &["down"]
                },
            )
        }
        DevAction::Logs { service, follow } => {
            let stack = Stack::locate()?;
            stack.written()?;
            let action: Vec<&str> = std::iter::once("logs")
                .chain(follow.then_some("--follow"))
                .chain(service.as_deref())
                .collect();
            stack.compose(&Profile::ALL, &action)
        }
        DevAction::Token { user, password } => token(&user, password.as_deref()).await,
        DevAction::Deploy { watch } => deploy(watch).await,
        DevAction::Doctor {
            with_web_runner,
            with_ext_runner,
        } => doctor(&Profile::asked(with_web_runner, with_ext_runner)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::sync::mpsc::channel;

    fn published_images() -> Images {
        Images::chosen(None, None, None, None, None).expect("images")
    }

    fn stack_of(images: &Images) -> serde_yaml::Value {
        let files = files(images, "501:20", &[]).expect("files");
        let mut stack: serde_yaml::Value = serde_yaml::from_str(&files.compose).expect("compose");
        let mode: serde_yaml::Value = serde_yaml::from_str(&files.mode).expect("mode");
        for (name, overridden) in mode["services"].as_mapping().expect("services") {
            let service = stack["services"][name].as_mapping_mut().expect("service");
            for (key, value) in overridden.as_mapping().expect("a service") {
                service.insert(key.clone(), value.clone());
            }
        }
        stack
    }

    #[test]
    fn the_stack_is_every_service_the_platform_needs() {
        let stack = stack_of(&published_images());
        let services: Vec<&str> = stack["services"]
            .as_mapping()
            .expect("services")
            .keys()
            .filter_map(serde_yaml::Value::as_str)
            .collect();
        assert_eq!(
            services,
            [
                "postgres",
                "nats",
                "redis",
                "storage",
                "bucket-init",
                "keycloak",
                "platform",
                "worker",
                "web-runner",
                "ext-runner"
            ]
        );
        assert_eq!(stack["name"], "cufflink-dev");
        let platform = &stack["services"]["platform"];
        assert_eq!(platform["image"], published("platform").as_str());
        assert_eq!(platform["platform"], "linux/amd64");
        assert_eq!(platform["user"], "501:20");
        assert_eq!(platform["ports"][0], "127.0.0.1:8080:8080");
        assert_eq!(platform["volumes"][0], "./run:/run/cufflink");
        assert_eq!(
            platform["environment"]["CUFFLINK_DEV_BOOTSTRAP"],
            "tenant=dev,realm=cufflink-dev,key_file=/run/cufflink/dev-key,\
             runner_key_file=/run/cufflink/ext-runner/runner-key"
        );
        assert_eq!(platform["environment"]["RUN_ENV"], "development");
        assert_eq!(
            platform["environment"]["KEYCLOAK_ISSUER"],
            "http://localhost:8180"
        );
        assert_eq!(
            stack["services"]["keycloak"]["ports"][0],
            "127.0.0.1:8180:8080"
        );
        Profile::ALL.iter().for_each(|profile| {
            assert_eq!(
                stack["services"][profile.name()]["profiles"],
                serde_yaml::Value::from(vec![profile.name()])
            );
        });
        assert!(stack["services"]["worker"]["profiles"].is_null());
        assert_eq!(platform["environment"]["CUFFLINK_EXTENSIONS"], "false");
    }

    #[test]
    fn the_ext_runner_is_the_dev_tenants_holding_its_runner_key_alone() {
        let stack = stack_of(&published_images());
        let runner = &stack["services"]["ext-runner"];
        assert_eq!(runner["user"], "501:20");
        let environment: BTreeMap<&str, &str> = runner["environment"]
            .as_mapping()
            .expect("an environment")
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().expect("a name"),
                    value.as_str().expect("a value"),
                )
            })
            .collect();
        assert_eq!(environment["EXT_TENANT"], TENANT);
        assert_eq!(environment["PLATFORM_URL"], "http://platform:8080");
        assert_eq!(
            environment["CUFFLINK_RUNNER_KEY_FILE"],
            "/run/cufflink/runner-key"
        );
        assert!(
            environment
                .keys()
                .all(|name| !["DATABASE_URL", "STORAGE_BACKEND"].contains(name)
                    && !name.starts_with("S3_")
                    && !name.starts_with("CUFFLINK_SECRETS")),
            "{environment:?}"
        );
        assert_eq!(
            runner["volumes"],
            serde_yaml::Value::from(vec!["./run/ext-runner:/run/cufflink:ro", "./native:/apps"]),
            "the runner never sees the tenant's API key ({KEY_FILE}) beside its own"
        );
        assert!(
            stack["services"]["platform"]["environment"]["CUFFLINK_DEV_BOOTSTRAP"]
                .as_str()
                .is_some_and(|bootstrap| bootstrap.ends_with(&format!(
                    "runner_key_file=/run/cufflink/ext-runner/{RUNNER_KEY_FILE}"
                ))),
            "the platform writes the runner key where the runner reads it"
        );
    }

    #[test]
    fn the_ext_runner_reaches_the_platform_and_nats_alone() {
        let stack = stack_of(&published_images());
        let networks = |service: &str| stack["services"][service]["networks"].clone();
        assert_eq!(
            networks("ext-runner"),
            serde_yaml::Value::from(vec!["runner"])
        );
        for beside in ["platform", "nats"] {
            assert_eq!(
                networks(beside),
                serde_yaml::Value::from(vec!["default", "runner"]),
                "{beside}"
            );
        }
        let services = stack["services"].as_mapping().expect("services");
        let on_the_runners: Vec<&str> = services
            .iter()
            .filter(|(_, service)| {
                service["networks"]
                    .as_sequence()
                    .is_some_and(|networks| networks.iter().any(|network| network == "runner"))
            })
            .map(|(name, _)| name.as_str().expect("a name"))
            .collect();
        assert_eq!(
            on_the_runners,
            ["nats", "platform", "ext-runner"],
            "no route from a runner to Postgres, Redis or object storage"
        );
    }

    #[test]
    fn every_port_of_the_stack_is_published_on_this_machine_alone() {
        let stack = stack_of(&published_images());
        let published: Vec<(&str, &str)> = stack["services"]
            .as_mapping()
            .expect("services")
            .iter()
            .flat_map(|(name, service)| {
                service["ports"]
                    .as_sequence()
                    .into_iter()
                    .flatten()
                    .map(move |port| {
                        (
                            name.as_str().expect("a name"),
                            port.as_str().expect("a port as text"),
                        )
                    })
            })
            .collect();
        assert_eq!(
            published
                .iter()
                .map(|(name, _)| *name)
                .collect::<BTreeSet<_>>(),
            PORTS.iter().map(|port| port.of).collect::<BTreeSet<_>>()
        );
        published.iter().for_each(|(name, port)| {
            assert!(port.starts_with("127.0.0.1:"), "{name} publishes {port}");
        });
    }

    #[test]
    fn images_are_the_published_ones_unless_named_or_built() {
        let named = Images::chosen(
            None,
            Some("registry.test/platform:1 # not a comment".into()),
            None,
            None,
            None,
        )
        .expect("images");
        let stack = stack_of(&named);
        assert_eq!(
            stack["services"]["platform"]["image"],
            "registry.test/platform:1 # not a comment"
        );
        assert!(
            stack["services"]["platform"]["platform"].is_null(),
            "an image named runs as it was built"
        );
        assert_eq!(
            stack["services"]["worker"]["image"],
            published("worker").as_str()
        );
        assert_eq!(stack["services"]["worker"]["platform"], "linux/amd64");
        let all_named = Images::chosen(
            None,
            Some("p:1".into()),
            Some("w:1".into()),
            Some("r:1".into()),
            Some("e:1".into()),
        )
        .expect("images");
        let overlay: serde_yaml::Value =
            serde_yaml::from_str(&files(&all_named, "0:0", &[]).expect("files").mode)
                .expect("YAML");
        assert_eq!(
            overlay["services"]
                .as_mapping()
                .map(serde_yaml::Mapping::len),
            Some(0)
        );

        let checkout = tempfile::tempdir().expect("directory");
        let refused = Images::chosen(Some(checkout.path().to_path_buf()), None, None, None, None)
            .expect_err("not a checkout");
        assert!(
            refused
                .to_string()
                .ends_with("is not a checkout of Cufflink"),
            "{refused}"
        );
        assert!(
            Images::chosen(Some(checkout.path().join("absent")), None, None, None, None).is_err()
        );

        std::fs::write(checkout.path().join("Dockerfile.platform"), "").expect("a Dockerfile");
        let source = Images::chosen(Some(checkout.path().to_path_buf()), None, None, None, None)
            .expect("a checkout");
        let stack = stack_of(&source);
        let context = checkout.path().canonicalize().expect("canonical");
        for (service, dockerfile) in [
            ("platform", "Dockerfile.platform"),
            ("worker", "Dockerfile.worker"),
            ("web-runner", "Dockerfile.web-runner"),
            ("ext-runner", "Dockerfile.ext-runner"),
        ] {
            let built = &stack["services"][service];
            assert_eq!(
                built["image"],
                format!("cufflink-dev-{service}:source").as_str()
            );
            assert_eq!(built["build"]["context"], context.to_str().expect("a path"));
            assert_eq!(built["build"]["dockerfile"], dockerfile);
            assert!(built["platform"].is_null(), "built for this machine");
        }
    }

    #[test]
    fn the_platform_takes_extensions_when_the_ext_runner_runs() {
        let extensions = |profiles: &[Profile]| {
            let files = files(&published_images(), "0:0", profiles).expect("files");
            let stack: serde_yaml::Value = serde_yaml::from_str(&files.compose).expect("YAML");
            stack["services"]["platform"]["environment"]["CUFFLINK_EXTENSIONS"]
                .as_str()
                .map(str::to_string)
        };
        assert_eq!(extensions(&[]).as_deref(), Some("false"));
        assert_eq!(extensions(&[Profile::WebRunner]).as_deref(), Some("false"));
        assert_eq!(extensions(&[Profile::ExtRunner]).as_deref(), Some("true"));
        assert_eq!(extensions(&Profile::ALL).as_deref(), Some("true"));
        assert_eq!(Profile::asked(false, false), []);
        assert_eq!(Profile::asked(true, false), [Profile::WebRunner]);
        assert_eq!(Profile::asked(false, true), [Profile::ExtRunner]);
        assert_eq!(Profile::asked(true, true), Profile::ALL);
    }

    #[test]
    fn every_image_built_from_source_copies_every_workspace_member() {
        let checkout = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the checkout");
        let workspace: toml::Value = std::fs::read_to_string(checkout.join("Cargo.toml"))
            .expect("the workspace manifest")
            .parse()
            .expect("TOML");
        let members: Vec<&str> = workspace["workspace"]["members"]
            .as_array()
            .expect("members")
            .iter()
            .filter_map(toml::Value::as_str)
            .collect();
        let overlay: serde_yaml::Value = serde_yaml::from_str(BUILT_FROM_SOURCE).expect("YAML");
        let dockerfiles: Vec<&str> = OWN_SERVICES
            .iter()
            .map(|(service, _)| {
                overlay["services"][*service]["build"]["dockerfile"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{service} is built from a Dockerfile"))
            })
            .collect();
        dockerfiles.iter().for_each(|dockerfile| {
            let text = std::fs::read_to_string(checkout.join(dockerfile))
                .unwrap_or_else(|error| panic!("{dockerfile}: {error}"));
            let copied: Vec<&Path> = text
                .lines()
                .filter_map(|line| line.strip_prefix("COPY "))
                .filter(|arguments| !arguments.starts_with("--"))
                .flat_map(|arguments| {
                    let sources: Vec<&str> = arguments.split_whitespace().collect();
                    sources[..sources.len().saturating_sub(1)].to_vec()
                })
                .map(Path::new)
                .collect();
            let missing: Vec<&&str> = members
                .iter()
                .filter(|member| {
                    !copied
                        .iter()
                        .any(|source| Path::new(member).starts_with(source))
                })
                .collect();
            assert!(
                missing.is_empty(),
                "{dockerfile} does not copy the workspace members {missing:?}"
            );
            assert!(copied.contains(&Path::new("Cargo.lock*")), "{dockerfile}");
        });
    }

    #[test]
    fn a_template_is_rendered_whole_or_not_at_all() {
        assert_eq!(
            rendered(
                "a: {{one}}\nb: {{two}}{{one}}",
                &[("one", "1".into()), ("two", "2".into())]
            )
            .expect("rendered"),
            "a: 1\nb: 21"
        );
        assert_eq!(
            rendered("a: {{one}}\nb: {{two}}", &[("one", "1".into())])
                .expect_err("unfilled")
                .to_string(),
            "the development stack has no value for 'two'"
        );
    }

    #[test]
    fn the_services_checked_are_those_the_stack_runs() {
        let stack = files(&published_images(), "0:0", &[]).expect("files");
        let images = |services: Vec<Service>| {
            services
                .into_iter()
                .map(|service| (service.name, service.image, service.built))
                .collect::<Vec<_>>()
        };
        let without = images(services_of(&stack.compose, &stack.mode, &[]).expect("services"));
        assert_eq!(
            without
                .iter()
                .map(|(name, image, built)| (name.as_str(), image.clone(), *built))
                .collect::<Vec<_>>(),
            [
                ("postgres", "postgres:16".to_string(), false),
                ("nats", "nats:2.10-alpine".to_string(), false),
                ("redis", "redis:7-alpine".to_string(), false),
                ("storage", "rustfs/rustfs:1.0.0".to_string(), false),
                ("bucket-init", "amazon/aws-cli:2.37.5".to_string(), false),
                (
                    "keycloak",
                    "quay.io/keycloak/keycloak:26.0".to_string(),
                    false
                ),
                ("platform", published("platform"), false),
                ("worker", published("worker"), false),
            ]
        );
        let with =
            images(services_of(&stack.compose, &stack.mode, &Profile::ALL).expect("services"));
        assert_eq!(
            with[without.len()..]
                .iter()
                .map(|(name, image, _)| (name.as_str(), image.clone()))
                .collect::<Vec<_>>(),
            [
                ("web-runner", published("web-runner")),
                ("ext-runner", published("ext-runner"))
            ]
        );
        let only = |profile: Profile| {
            services_of(&stack.compose, &stack.mode, &[profile])
                .expect("services")
                .into_iter()
                .skip(without.len())
                .map(|service| service.name)
                .collect::<Vec<_>>()
        };
        assert_eq!(only(Profile::WebRunner), ["web-runner"]);
        assert_eq!(only(Profile::ExtRunner), ["ext-runner"]);

        let checkout = tempfile::tempdir().expect("directory");
        std::fs::write(checkout.path().join("Dockerfile.platform"), "").expect("a Dockerfile");
        let source = files(
            &Images::chosen(Some(checkout.path().to_path_buf()), None, None, None, None)
                .expect("a checkout"),
            "0:0",
            &[],
        )
        .expect("files");
        let built = services_of(&source.compose, &source.mode, &[]).expect("services");
        assert_eq!(
            built
                .iter()
                .filter(|service| service.built)
                .map(|service| service.name.as_str())
                .collect::<Vec<_>>(),
            ["platform", "worker"],
            "a runner is neither built nor checked without its profile"
        );
    }

    #[test]
    fn the_realm_has_a_user_who_may_deploy_and_administer_and_one_who_may_not() {
        let realm: Value = serde_json::from_str(REALM_EXPORT).expect("realm");
        assert_eq!(realm["realm"], REALM);
        let client = &realm["clients"][0];
        assert_eq!(
            (
                &client["clientId"],
                &client["publicClient"],
                &client["directAccessGrantsEnabled"]
            ),
            (&Value::from(CLIENT), &Value::Bool(true), &Value::Bool(true))
        );
        let holds = |user: &Value, role: &str| {
            user["realmRoles"]
                .as_array()
                .is_some_and(|roles| roles.contains(&Value::from(role)))
        };
        let users: Vec<(&str, &str, bool, bool)> = realm["users"]
            .as_array()
            .expect("users")
            .iter()
            .map(|user| {
                (
                    user["username"].as_str().expect("username"),
                    user["credentials"][0]["value"].as_str().expect("password"),
                    holds(user, "cufflink-deploy"),
                    holds(user, "cufflink-platform-admin"),
                )
            })
            .collect();
        assert_eq!(
            users,
            [("alice", "alice", true, true), ("bob", "bob", false, false)]
        );
        let defined: Vec<&str> = realm["roles"]["realm"]
            .as_array()
            .expect("realm roles")
            .iter()
            .filter_map(|role| role["name"].as_str())
            .collect();
        assert_eq!(defined, ["cufflink-deploy", "cufflink-platform-admin"]);
    }

    #[test]
    fn the_environment_is_the_stacks() {
        let dev = environment();
        assert_eq!(
            (
                dev.api_url.as_str(),
                dev.tenant.as_str(),
                dev.keycloak_url.as_deref(),
                dev.keycloak_realm.as_deref(),
                dev.keycloak_client_id.as_deref(),
            ),
            (
                "http://localhost:8080",
                "dev",
                Some("http://localhost:8180"),
                Some("cufflink-dev"),
                Some("cufflink-cli"),
            )
        );
        assert!(dev.api_key.is_none() && dev.api_key_env.is_none());
    }

    #[test]
    fn compose_is_told_its_files_and_the_profile_asked_for() {
        let stack = Stack {
            home: PathBuf::from("/home/ada/.cufflink/dev"),
        };
        let told = |profiles: &[Profile], action: &[&str]| {
            stack
                .arguments(profiles, action)
                .into_iter()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            told(&[], &["up", "-d"]),
            [
                "compose",
                "-f",
                "/home/ada/.cufflink/dev/compose.yml",
                "-f",
                "/home/ada/.cufflink/dev/compose.mode.yml",
                "up",
                "-d"
            ]
        );
        assert_eq!(
            told(&Profile::ALL, &["down", "--volumes"])[5..],
            [
                "--profile",
                "web-runner",
                "--profile",
                "ext-runner",
                "down",
                "--volumes"
            ]
        );
        assert_eq!(
            told(&[Profile::ExtRunner], &["up", "-d"])[5..],
            ["--profile", "ext-runner", "up", "-d"]
        );
        assert_eq!(
            stack.key_file(),
            PathBuf::from("/home/ada/.cufflink/dev/run/dev-key")
        );
    }

    #[test]
    fn the_files_of_a_stack_are_written_where_compose_finds_them() {
        let home = tempfile::tempdir().expect("directory");
        let stack = Stack {
            home: home.path().join("dev"),
        };
        assert!(stack.written().is_err());
        stack.write(&published_images(), &[]).expect("written");
        stack.written().expect("a stack");
        assert!(stack.run_directory().is_dir());
        assert!(stack.native_directory().is_dir());
        assert!(stack.runner_key_directory().is_dir());
        let realm: Value = serde_json::from_str(
            &std::fs::read_to_string(stack.home.join("realm/cufflink-dev-realm.json"))
                .expect("realm"),
        )
        .expect("JSON");
        assert_eq!(realm["realm"], REALM);
        let compose = std::fs::read_to_string(stack.compose_file()).expect("compose");
        assert!(!compose.contains("{{"), "{compose}");
        assert_eq!(
            std::fs::read_to_string(stack.mode_file()).expect("mode"),
            published_overlay(&published_images())
        );
        #[cfg(unix)]
        {
            let stack: serde_yaml::Value = serde_yaml::from_str(&compose).expect("YAML");
            assert_eq!(
                stack["services"]["platform"]["user"]
                    .as_str()
                    .map(str::to_string),
                owner(home.path()).ok()
            );
        }
    }

    #[test]
    fn a_key_the_platform_wrote_is_needed_to_register() {
        let home = tempfile::tempdir().expect("directory");
        let stack = Stack {
            home: home.path().to_path_buf(),
        };
        std::fs::create_dir_all(stack.run_directory()).expect("run");
        for absent in [None, Some(""), Some(" \n")] {
            if let Some(blank) = absent {
                std::fs::write(stack.key_file(), blank).expect("a key file");
            }
            let refused = registered(&stack).expect_err("no key").to_string();
            assert!(
                refused.contains("its image predates the development bootstrap"),
                "{refused}"
            );
        }
    }

    #[test]
    fn only_what_is_deployed_makes_a_deploy() {
        let project = Path::new("/work/notes");
        for changed in [
            "src/main.rs",
            "src/handlers/create.rs",
            "migrations/V2__kinds.sql",
            "Cargo.toml",
            "Cufflink.toml",
        ] {
            assert!(relevant(project, &project.join(changed)), "{changed}");
        }
        for ignored in [
            "target/debug/notes",
            "Cargo.lock",
            "README.md",
            "srcs/main.rs",
            ".git/index",
        ] {
            assert!(!relevant(project, &project.join(ignored)), "{ignored}");
        }
        assert!(!relevant(project, Path::new("/work/other/src/main.rs")));
        assert!(!relevant(project, project));
    }

    #[test]
    fn changes_are_gathered_until_they_stop() {
        let (sender, following) = channel();
        [2, 3].into_iter().for_each(|change| {
            sender.send(change).expect("sent");
        });
        let long = Duration::from_secs(60);
        assert_eq!(
            settled(1, &following, Duration::from_millis(20), long),
            [1, 2, 3]
        );
        assert_eq!(settled(4, &following, Duration::from_millis(20), long), [4]);
        let late = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            sender.send(6).expect("sent");
            std::thread::sleep(Duration::from_secs(3));
            sender.send(7).expect("sent");
        });
        assert_eq!(settled(5, &following, Duration::from_secs(1), long), [5, 6]);
        late.join().expect("joined");
        assert_eq!(following.recv().expect("kept for the next"), 7);
        assert_eq!(settled(8, &following, Duration::from_secs(5), long), [8]);

        let (sender, following) = channel();
        let steady = std::thread::spawn(move || {
            (0..40).try_for_each(|change| {
                std::thread::sleep(Duration::from_millis(25));
                sender.send(change)
            })
        });
        let started = Instant::now();
        let gathered = settled(
            -1,
            &following,
            Duration::from_millis(200),
            Duration::from_millis(300),
        );
        assert!(
            started.elapsed() < Duration::from_millis(800),
            "{:?}",
            started.elapsed()
        );
        assert!(
            gathered.len() < 41,
            "a stream that never stops is deployed as it goes"
        );
        drop(following);
        let _ = steady.join();
    }

    #[test]
    fn reads_and_what_is_not_deployed_are_no_change() {
        use notify::event::{AccessKind, CreateKind, ModifyKind};
        let project = Path::new("/work/notes");
        let event =
            |kind: EventKind, path: &str| Ok(notify::Event::new(kind).add_path(project.join(path)));
        let changed = changes(
            project,
            vec![
                event(EventKind::Modify(ModifyKind::Any), "src/main.rs"),
                event(EventKind::Modify(ModifyKind::Any), "src/main.rs"),
                event(EventKind::Create(CreateKind::File), "migrations/V3__x.sql"),
                event(EventKind::Access(AccessKind::Any), "Cargo.toml"),
                event(EventKind::Modify(ModifyKind::Any), "target/debug/notes"),
                Err(notify::Error::generic("the watch overflowed")),
            ],
        );
        assert_eq!(
            changed.into_iter().collect::<Vec<_>>(),
            [
                project.join("migrations/V3__x.sql"),
                project.join("src/main.rs")
            ]
        );
    }

    #[test]
    fn a_directory_is_watched_once_and_when_it_appears() {
        let project = tempfile::tempdir().expect("directory");
        let root = project.path().canonicalize().expect("canonical");
        std::fs::create_dir(root.join("src")).expect("src");
        std::fs::write(root.join("Cargo.toml"), "").expect("Cargo.toml");
        let (sender, _events) = channel();
        let mut watcher = notify::recommended_watcher(sender).expect("watcher");
        let watched = watch_what_is_there(&mut watcher, &root, BTreeSet::new()).expect("watched");
        assert_eq!(
            watched.iter().collect::<Vec<_>>(),
            [&root, &root.join("src")]
        );
        std::fs::create_dir(root.join("migrations")).expect("migrations");
        let watched = watch_what_is_there(&mut watcher, &root, watched).expect("watched");
        assert_eq!(
            watched.into_iter().collect::<Vec<_>>(),
            [root.clone(), root.join("migrations"), root.join("src")]
        );
    }

    #[test]
    fn only_a_project_whose_dev_is_the_stack_is_deployed_to_it() {
        let project =
            |toml: &str| ProjectConfig::parsed(toml, Path::new("/work/notes")).expect("a project");
        assert_eq!(
            stack_project(project("[service]\nname = \"notes\"\n")).expect("the stack's"),
            Path::new("/work/notes")
        );
        for stack in [
            "http://localhost:8080/",
            "http://127.0.0.1:8080",
            "http://[::1]:8080",
            "http://LOCALHOST:8080",
        ] {
            let named = project(&format!(
                "[environments.dev]\napi_url = \"{stack}\"\ntenant = \"dev\"\n"
            ));
            assert!(stack_project(named).is_ok(), "{stack}");
        }
        for elsewhere in [
            "http://localhost:8081",
            "https://localhost:8080",
            "http://10.0.0.7:8080",
            "http://localhost:8080/api",
        ] {
            assert!(!is_the_stack(elsewhere), "{elsewhere}");
        }
        let remote = project(
            "[environments.dev]\napi_url = \"https://api.dev.example.test\"\ntenant = \"dev\"\n",
        );
        let refused = stack_project(remote)
            .expect_err("not the stack")
            .to_string();
        assert!(
            refused.contains("is https://api.dev.example.test, not the development stack"),
            "{refused}"
        );
    }

    #[test]
    fn a_port_a_listener_holds_on_every_address_is_in_use() {
        let held = std::net::TcpListener::bind("0.0.0.0:0").expect("bind");
        let port = held.local_addr().expect("address").port();
        assert!(listened_on(port), "a wildcard listener holds the port");
        drop(held);
        assert!(!listened_on(port));
    }

    #[test]
    fn a_port_in_use_is_wrong_unless_the_stack_holds_it() {
        let held = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = Port {
            of: "platform",
            placeholder: "platform_port",
            number: held.local_addr().expect("address").port(),
        };
        assert_eq!(
            port_check(&port, false).found,
            Err("in use by another program".to_string())
        );
        assert_eq!(
            port_check(&port, true).found,
            Ok("held, as the stack is up".to_string())
        );
        drop(held);
        let check = port_check(&port, false);
        assert_eq!(check.found, Ok("free".to_string()));
        assert_eq!(check.of, format!("port {} (platform)", port.number));
    }
}
