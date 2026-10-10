pub mod delivery;
pub mod index;
pub mod permits;
pub mod retention;

pub use index::WebhookIndex;
pub use permits::{DeliveryConfig, Permits};

use tokio::sync::watch;

#[derive(Clone)]
pub struct Webhooks {
    pub index: WebhookIndex,
    pub permits: Permits,
    delivering: watch::Sender<bool>,
}

impl Webhooks {
    pub fn new(config: &DeliveryConfig) -> Self {
        Self {
            index: WebhookIndex::default(),
            permits: Permits::new(config),
            delivering: watch::Sender::new(false),
        }
    }

    pub fn deliver(&self) {
        self.delivering.send_if_modified(|delivering| {
            let began = !*delivering;
            *delivering = true;
            began
        });
    }

    async fn delivering(&self) {
        let mut delivering = self.delivering.subscribe();
        let _ = delivering.wait_for(|delivering| *delivering).await;
    }
}
