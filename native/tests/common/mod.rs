use cufflink_native::{CallContext, Extension};
use cufflink_types::ext::{CallerService, ExtRequest, PROTOCOL_VERSION};
use futures_util::future::BoxFuture;
use serde_json::Value;
use std::future::Future;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct Serving {
    pub stop: oneshot::Sender<()>,
    pub served: JoinHandle<eyre::Result<()>>,
}

impl Serving {
    pub fn start<F>(serve: impl FnOnce(BoxFuture<'static, ()>) -> F) -> Self
    where
        F: Future<Output = eyre::Result<()>> + Send + 'static,
    {
        let (stop, stopped) = oneshot::channel::<()>();
        let served = tokio::spawn(serve(Box::pin(async {
            let _ = stopped.await;
        })));
        Self { stop, served }
    }

    pub async fn stop(self) {
        self.stop.send(()).expect("still serving");
        self.served.await.expect("serve task").expect("served");
    }
}

pub fn in_ms(ms: u64) -> u64 {
    u64::try_from(
        (SystemTime::now() + Duration::from_millis(ms))
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("millis")
}

pub fn request(tenant: &str, extension: &str, payload: Value, deadline_ms: u64) -> Vec<u8> {
    serde_json::to_vec(&ExtRequest {
        v: PROTOCOL_VERSION,
        call_id: uuid::Uuid::new_v4(),
        extension: extension.into(),
        context: CallContext {
            tenant: tenant.into(),
            service: CallerService {
                name: "reports-service".into(),
                version: 1,
            },
            auth: None,
            deadline_ms,
        },
        payload,
    })
    .expect("encode")
}

const HELD_AT_MOST: Duration = Duration::from_secs(10);

pub struct Held {
    started: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}

impl Held {
    pub async fn started(self) -> mpsc::Sender<()> {
        let Held { started, release } = self;
        tokio::task::spawn_blocking(move || started.recv_timeout(HELD_AT_MOST))
            .await
            .expect("waited")
            .expect("the held call started");
        release
    }
}

pub fn hold() -> (impl Extension, Held) {
    let (started_tx, started) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let (started_tx, released) = (Mutex::new(started_tx), Mutex::new(released));
    let extension = move |_: &CallContext, payload: Value| -> cufflink_native::Result<Value> {
        started_tx.lock().expect("started").send(())?;
        released
            .lock()
            .expect("released")
            .recv_timeout(HELD_AT_MOST)?;
        Ok(payload)
    };
    (extension, Held { started, release })
}
