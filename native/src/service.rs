use crate::dispatch::Dispatcher;
use crate::nats::{NatsProvider, NatsServer};
use crate::socket::SocketServer;
use crate::Extension;
use cufflink_types::ext::{callers, SOCKET_VAR, TOKEN_VAR};
use cufflink_types::manifest::validate::validate_provides;
use cufflink_types::slug::{is_valid_service_name, SERVICE_NAME_MAX};
use cufflink_types::{ProvidedExtension, ServiceManifest, ServiceMode};
use std::future::Future;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

const DEFAULT_CONCURRENCY: usize = 64;

/// A native service named `name`: add its extensions, then
/// [`run`](Service::run) it.
pub fn service(name: impl Into<String>) -> Service {
    Service {
        name: name.into(),
        extensions: Vec::new(),
        concurrency: DEFAULT_CONCURRENCY,
    }
}

/// A native service and the extensions it provides.
#[must_use]
pub struct Service {
    name: String,
    extensions: Vec<(ProvidedExtension, Arc<dyn Extension>)>,
    concurrency: usize,
}

impl Service {
    /// Provide `extension` as `name` to `callers`: service names, or `*` for
    /// every service of the tenant.
    pub fn extension<C: Into<String>>(
        mut self,
        name: impl Into<String>,
        extension: impl Extension,
        callers: impl IntoIterator<Item = C>,
    ) -> Self {
        let provided = ProvidedExtension {
            name: name.into(),
            callers: callers.into_iter().map(Into::into).collect(),
        };
        self.extensions.push((provided, Arc::new(extension)));
        self
    }

    /// Run at most `calls` calls at once (64 unless set); the others wait
    /// for a slot until their deadline.
    pub fn concurrency(mut self, calls: usize) -> Self {
        self.concurrency = calls;
        self
    }

    /// The `mode: native` manifest of the service, which the CLI deploys:
    /// its name and what it provides, each extension's callers in order,
    /// once each.
    pub fn manifest(&self) -> eyre::Result<ServiceManifest> {
        eyre::ensure!(
            is_valid_service_name(&self.name),
            "'{}' is not a service name (1 to {SERVICE_NAME_MAX} of A-Z, a-z, 0-9, '-' and '_')",
            self.name
        );
        let declared: Vec<ProvidedExtension> =
            self.extensions.iter().map(|(p, _)| p.clone()).collect();
        validate_provides("provides", &declared).map_err(eyre::Report::msg)?;
        let mut provides = declared
            .into_iter()
            .map(|p| {
                callers(p.callers).map(|callers| ProvidedExtension {
                    name: p.name,
                    callers,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(eyre::Report::msg)?;
        provides.sort_by(|a, b| a.name.cmp(&b.name));
        let manifest = ServiceManifest {
            name: self.name.clone(),
            mode: ServiceMode::Native,
            provides,
            ..Default::default()
        };
        Ok(ServiceManifest {
            required_platform_features: manifest
                .derived_features()
                .into_iter()
                .map(str::to_string)
                .collect(),
            ..manifest
        })
    }

    /// The manifest of a service that can serve.
    fn checked(&self) -> eyre::Result<ServiceManifest> {
        eyre::ensure!(
            self.concurrency > 0,
            "concurrency: at least one call must be able to run"
        );
        self.manifest()
    }

    fn validated(self) -> eyre::Result<(String, Dispatcher)> {
        self.checked()?;
        let extensions = self
            .extensions
            .into_iter()
            .map(|(provided, extension)| (provided.name, extension))
            .collect();
        Ok((self.name, Dispatcher::new(extensions, self.concurrency)))
    }

    /// Run the service as its binary's `main`: print its manifest under
    /// `--emit-manifest`, else serve the Unix socket `CUFFLINK_EXT_SOCKET`
    /// names, to requests bearing the token `CUFFLINK_EXT_TOKEN` holds, until
    /// SIGTERM or SIGINT. Fails, saying why, when the service or its
    /// environment is invalid.
    ///
    /// On Linux the process first makes itself undumpable, so other
    /// processes of its user (the runner's other children) cannot read its
    /// environment or memory.
    pub fn run(self) -> ExitCode {
        let emit = std::env::args_os()
            .skip(1)
            .any(|arg| arg == "--emit-manifest");
        let served = self.checked().and_then(|manifest| {
            if emit {
                println!("{}", serde_json::to_string_pretty(&manifest)?);
                return Ok(());
            }
            forbid_inspection()?;
            init_tracing();
            let path = std::env::var_os(SOCKET_VAR)
                .ok_or_else(|| eyre::eyre!("{SOCKET_VAR} must name the socket to serve on"))?;
            let token = std::env::var(TOKEN_VAR)
                .ok()
                .filter(|token| !token.is_empty())
                .ok_or_else(|| {
                    eyre::eyre!(
                        "{TOKEN_VAR} must hold the token the runner sends with each request"
                    )
                })?;
            block_on(async move {
                let shutdown = shutdown_signal()?;
                self.bind_socket(path)
                    .await?
                    .require_token(token)
                    .serve(shutdown)
                    .await
            })
        });
        served.map_or_else(
            |e| {
                eprintln!("cufflink-native: {e:#}");
                ExitCode::FAILURE
            },
            |()| ExitCode::SUCCESS,
        )
    }

    /// Serve the extensions from NATS as `provider` until SIGTERM or
    /// SIGINT, undumpable on Linux as [`run`](Self::run) is.
    pub fn run_nats(self, provider: NatsProvider) -> eyre::Result<()> {
        forbid_inspection()?;
        init_tracing();
        block_on(async move { self.serve_nats(provider, shutdown_signal()?).await })
    }

    /// Listen on the Unix socket `path`, replacing a stale one.
    pub async fn bind_socket(self, path: impl AsRef<Path>) -> eyre::Result<SocketServer> {
        let (_, dispatcher) = self.validated()?;
        SocketServer::bind(path.as_ref(), dispatcher)
    }

    /// Connect to `provider`'s NATS and subscribe to the calls of each
    /// extension in the queue group every provider shares.
    pub async fn bind_nats(self, provider: NatsProvider) -> eyre::Result<NatsServer> {
        let (name, dispatcher) = self.validated()?;
        NatsServer::bind(&name, provider, dispatcher).await
    }

    /// [`bind_nats`](Self::bind_nats), then [`serve`](NatsServer::serve)
    /// until `shutdown`, which also ends a bind still waiting for NATS.
    pub async fn serve_nats(
        self,
        provider: NatsProvider,
        shutdown: impl Future<Output = ()>,
    ) -> eyre::Result<()> {
        let mut shutdown = std::pin::pin!(shutdown);
        let bound = tokio::select! {
            bound = self.bind_nats(provider) => bound?,
            () = &mut shutdown => return Ok(()),
        };
        bound.serve(shutdown).await
    }
}

/// Keep other processes of this user from reading this one's environment
/// (`/proc/<pid>/environ`) or memory: execve makes every process dumpable
/// again, so each must do it itself.
#[cfg(target_os = "linux")]
fn forbid_inspection() -> eyre::Result<()> {
    // SAFETY: PR_SET_DUMPABLE takes plain integers and touches no memory.
    match unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } {
        0 => Ok(()),
        _ => Err(eyre::Report::new(std::io::Error::last_os_error())
            .wrap_err("cannot make the process undumpable")),
    }
}

#[cfg(not(target_os = "linux"))]
fn forbid_inspection() -> eyre::Result<()> {
    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

fn block_on(served: impl Future<Output = eyre::Result<()>>) -> eyre::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(served);
    runtime.shutdown_background();
    result
}

fn shutdown_signal() -> eyre::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{signal, SignalKind};
    let (mut terminate, mut interrupt) = (
        signal(SignalKind::terminate())?,
        signal(SignalKind::interrupt())?,
    );
    Ok(async move {
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
        tracing::info!("shutting down");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CallContext, Result};
    use cufflink_types::features::NATIVE_SERVICES;
    use serde_json::Value;

    fn token(_: &CallContext, payload: Value) -> Result<Value> {
        Ok(payload)
    }

    fn refused(service: Service) -> String {
        match service.validated() {
            Ok(_) => panic!("accepted"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn the_manifest_is_native_and_lists_what_it_provides() {
        let manifest = service("ext-google")
            .extension("pdf.render", token, ["*"])
            .extension(
                "google.token",
                token,
                ["reports-service", "billing", "reports-service"],
            )
            .manifest()
            .expect("manifest");
        let emitted: Value =
            serde_json::from_str(&serde_json::to_string_pretty(&manifest).expect("json"))
                .expect("parse");
        assert_eq!(emitted["name"], "ext-google");
        assert_eq!(emitted["mode"], "native");
        assert_eq!(emitted["tables"], serde_json::json!([]));
        assert_eq!(
            emitted["provides"],
            serde_json::json!([
                {"name": "google.token", "callers": ["billing", "reports-service"]},
                {"name": "pdf.render", "callers": ["*"]},
            ])
        );
        assert_eq!(
            emitted["required_platform_features"],
            serde_json::json!([NATIVE_SERVICES])
        );
        let parsed: ServiceManifest = serde_json::from_value(emitted).expect("a manifest");
        assert!(!parsed.requires_wasm());
    }

    #[test]
    fn an_invalid_service_says_why() {
        for (service, reason) in [
            (
                service("ext google").extension("google.token", token, ["*"]),
                "'ext google' is not a service name (1 to 64 of A-Z, a-z, 0-9, '-' and '_')",
            ),
            (
                service("ext-google"),
                "provides: a native service provides at least one extension",
            ),
            (
                service("ext-google").extension("Google.Token", token, ["*"]),
                "provides[0]: 'Google.Token' is not an extension name (segments of a-z, 0-9 and \
                 '-' joined by '.', at most 128 bytes)",
            ),
            (
                service("ext-google")
                    .extension("google.token", token, ["*"])
                    .extension("google.token", token, ["billing"]),
                "provides: extension 'google.token' is listed twice",
            ),
            (
                service("ext-google").extension("google.token", token, Vec::<String>::new()),
                "provides[0].callers: at least one caller is required",
            ),
            (
                service("ext-google").extension("google.token", token, ["reports.service"]),
                "provides[0].callers: 'reports.service' is neither a service name nor '*'",
            ),
            (
                service("ext-google")
                    .extension("google.token", token, ["*"])
                    .concurrency(0),
                "concurrency: at least one call must be able to run",
            ),
        ] {
            assert_eq!(refused(service), reason);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_process_can_make_itself_undumpable() {
        forbid_inspection().expect("undumpable");
        // SAFETY: PR_GET_DUMPABLE reads a flag of this process.
        assert_eq!(unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) }, 0);
    }

    #[test]
    fn a_valid_service_dispatches_its_extensions() {
        let (name, dispatcher) = service("ext-google")
            .extension("google.token", token, ["*"])
            .validated()
            .expect("valid");
        assert_eq!(name, "ext-google");
        assert_eq!(dispatcher.names().collect::<Vec<_>>(), ["google.token"]);
    }
}
