use crate::error::TestkitError;
use crate::infra::Tenant;
use async_nats::jetstream::consumer::{Info, PullConsumer};
use futures_util::StreamExt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

pub fn tenant_stream(tenant: &Tenant) -> String {
    format!("DW_{}", tenant.slug.to_uppercase().replace('-', "_"))
}

pub async fn acknowledged(consumer: &mut PullConsumer, floor: u64, within: Duration) -> Info {
    let deadline = Instant::now() + within;
    loop {
        let info = consumer.info().await.expect("consumer info").clone();
        let settled = (info.ack_floor.stream_sequence, info.num_ack_pending) == (floor, 0);
        if settled || Instant::now() >= deadline {
            return info;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub struct Foreign {
    taken: Arc<AtomicU64>,
    pulling: tokio::task::JoinHandle<()>,
}

impl Foreign {
    const BATCH: usize = 10_000;
    const WAITS: Duration = Duration::from_secs(900);

    pub async fn pulling(consumer: &PullConsumer) -> Result<Self, TestkitError> {
        let mut messages = consumer
            .batch()
            .max_messages(Self::BATCH)
            .expires(Self::WAITS)
            .messages()
            .await
            .map_err(|e| TestkitError::Nats(format!("a pull of the durable: {e}")))?;
        let taken = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&taken);
        let pulling = tokio::spawn(async move {
            while let Some(Ok(message)) = messages.next().await {
                if message.ack().await.is_ok() {
                    counted.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        Ok(Self { taken, pulling })
    }

    pub fn taken(&self) -> u64 {
        self.taken.load(Ordering::SeqCst)
    }

    pub async fn stop(self) -> u64 {
        self.pulling.abort();
        let _ = self.pulling.await;
        self.taken.load(Ordering::SeqCst)
    }
}

pub async fn most_waiting<T>(
    consumer: &PullConsumer,
    doing: impl Future<Output = T>,
) -> Result<(T, usize), TestkitError> {
    let mut consumer = consumer.clone();
    let mut most = 0;
    tokio::pin!(doing);
    loop {
        let waiting = consumer
            .info()
            .await
            .map_err(|e| TestkitError::Nats(format!("the durable's state: {e}")))?
            .num_waiting;
        most = most.max(waiting);
        tokio::select! {
            done = &mut doing => return Ok((done, most)),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
}
