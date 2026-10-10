use crate::deployment::Deployment;
use crate::fleet::Shared;
use crate::socket::Endpoint;
use cufflink_supervisor::runner_api::KEY_FILE_VAR;
use cufflink_supervisor::{output_event, EnvPolicy, Exit, OutputLevel, Runner, Stream};
use cufflink_telemetry::Scope;
use cufflink_types::ext::{SOCKET_VAR, TOKEN_VAR};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

pub const EXT_ENV: EnvPolicy = EnvPolicy {
    passthrough: "EXT_ENV_PASSTHROUGH",
    allow: &[],
    credentials: &[KEY_FILE_VAR],
};

pub const BINARY: &str = "service";

#[derive(Debug, Default)]
pub struct Starts(u64);

impl Starts {
    pub fn next(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}

pub struct NativeChildren {
    shared: Arc<Shared>,
    output_level: OutputLevel,
}

impl NativeChildren {
    pub fn new(shared: Arc<Shared>, output_level: OutputLevel) -> Self {
        Self {
            shared,
            output_level,
        }
    }
}

impl Runner for NativeChildren {
    type Key = Uuid;
    type Endpoint = PathBuf;
    type Endpoints = Starts;
    type Info = Arc<Deployment>;

    const OUTPUT_TASK: &'static str = "ext_child_output";
    const SUPERVISOR_TASK: &'static str = "ext_child_supervisor";
    const TRACKS_CRASH_LOOPS: bool = false;

    fn release(_: &mut Starts, socket: &PathBuf) {
        if let Err(e) = std::fs::remove_file(socket) {
            if e.kind() != ErrorKind::NotFound {
                warn!(socket = %socket.display(), error = %e, "Cannot remove a child's socket");
            }
        }
    }

    fn scope(&self, _: &Uuid, deployment: &Arc<Deployment>) -> Scope {
        deployment.scope()
    }

    fn output_line(&self, _: &Uuid, deployment: &Arc<Deployment>, stream: Stream, line: &[u8]) {
        output_event!(self.output_level, target: "ext_child", tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, stream = stream.as_str(), "{}", String::from_utf8_lossy(line));
    }

    fn output_dropped(&self, _: &Uuid, deployment: &Arc<Deployment>, stream: Stream, lines: u64) {
        output_event!(self.output_level, target: "ext_child", tenant = %deployment.tenant, service = %deployment.service, version = deployment.version, stream = stream.as_str(), lines, "Lines of the service's output over the log budget were not logged");
    }

    fn exited(&self, exit: Exit<Self>) {
        self.shared.exited(exit);
    }
}

pub fn identity(endpoint: &Endpoint, deployment: &Deployment) -> [(String, String); 4] {
    [
        (SOCKET_VAR, endpoint.socket().to_string_lossy().into_owned()),
        (TOKEN_VAR, endpoint.token().to_string()),
        ("CUFFLINK_TENANT", deployment.tenant.clone()),
        ("CUFFLINK_SERVICE", deployment.service.clone()),
    ]
    .map(|(name, value)| (name.to_string(), value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restart::tests::deployment;
    use cufflink_supervisor::{child_env, EnvMode};
    use std::collections::HashMap;
    use std::path::Path;

    #[test]
    fn a_child_sees_the_allowlist_its_configs_and_its_identity() {
        let runner = [
            ("PATH", "/usr/bin"),
            ("DATABASE_URL", "postgres://runner"),
            ("NATS_PASSWORD", "secret"),
            ("EXT_STATUS_TOKEN", "ops"),
            ("CUFFLINK_RUNNER_KEY", "cfr_AAAAAAAAAAAA_s3cret"),
            ("CUFFLINK_RUNNER_KEY_FILE", "/run/cufflink/runner-key"),
            ("RUST_LOG", "debug"),
            ("CUFFLINK_TENANT", "runner-own"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()));
        let configs = HashMap::from([
            ("GOOGLE_KEY".to_string(), "k".to_string()),
            ("CUFFLINK_EXT_SOCKET".to_string(), "/elsewhere".to_string()),
            ("CUFFLINK_EXT_TOKEN".to_string(), "forged".to_string()),
        ]);
        let mode =
            EnvMode::parse(EXT_ENV, Some("RUST_LOG,EXT_*,CUFFLINK_RUNNER_*")).expect("passthrough");
        let env = child_env(
            runner,
            &mode,
            &configs,
            None,
            identity(
                &Endpoint::new(Path::new("/tmp/cufflink-ext/a-1-1.sock"), "t0ken"),
                &deployment(1),
            ),
        );
        assert_eq!(
            env.into_iter().collect::<Vec<_>>(),
            [
                ("CUFFLINK_EXT_SOCKET", "/tmp/cufflink-ext/a-1-1.sock"),
                ("CUFFLINK_EXT_TOKEN", "t0ken"),
                ("CUFFLINK_SERVICE", "ext-pdf"),
                ("CUFFLINK_TENANT", "acme"),
                ("GOOGLE_KEY", "k"),
                ("PATH", "/usr/bin"),
                ("RUST_LOG", "debug"),
            ]
            .map(|(name, value)| (name.to_string(), value.to_string()))
        );
    }

    #[test]
    fn every_start_is_numbered_afresh() {
        let mut starts = Starts::default();
        assert_eq!([starts.next(), starts.next(), starts.next()], [1, 2, 3]);
    }
}
