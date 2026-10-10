//! The NATS transport: a provider the project runs itself, answering its
//! tenant's calls on their subjects.

use crate::deadline::CallDeadline;
use crate::dispatch::{decode, Dispatcher};
use async_nats::client::{PublishError, PublishErrorKind};
use async_nats::{Client, Message, Subject, Subscriber};
use cufflink_nats::{parse_server_list, redact_urls, NatsAuth};
use cufflink_types::ext::{call_subject, ExtReply, QUEUE_GROUP};
use cufflink_types::host::ext::CallErrorKind;
use cufflink_types::slug::{is_valid_tenant_slug, TENANT_SLUG_MAX};
use eyre::WrapErr;
use futures_util::stream::{select_all, SelectAll};
use futures_util::StreamExt;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::task::TaskTracker;

const DEFAULT_URL: &str = "nats://localhost:4222";
/// How long a stopping provider takes to answer the calls NATS delivered
/// to it, and then to write its last replies.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Where a NATS provider serves: its tenant and the NATS it connects to.
#[derive(Clone)]
pub struct NatsProvider {
    tenant: String,
    url: String,
    auth: NatsAuth,
}

impl fmt::Debug for NatsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsProvider")
            .field("tenant", &self.tenant)
            .field("url", &redact_urls(&self.url))
            .field("auth", &self.auth)
            .finish()
    }
}

impl NatsProvider {
    /// A provider of the tenant `tenant` on the NATS servers `url` (a
    /// comma-separated list), authenticated by `auth`.
    pub fn new(
        tenant: impl Into<String>,
        url: impl Into<String>,
        auth: NatsAuth,
    ) -> eyre::Result<Self> {
        let (tenant, url) = (tenant.into(), url.into());
        eyre::ensure!(
            is_valid_tenant_slug(&tenant),
            "'{tenant}' is not a tenant slug (1 to {TENANT_SLUG_MAX} of a-z, 0-9 and '-')"
        );
        parse_server_list(&url)?;
        Ok(Self { tenant, url, auth })
    }

    /// The provider `CUFFLINK_TENANT`, `NATS_URL` (`nats://localhost:4222`
    /// when unset) and the NATS credentials of the environment describe.
    pub fn from_env() -> eyre::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok().filter(|v| !v.is_empty()))
    }

    fn from_vars(lookup: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let tenant = lookup("CUFFLINK_TENANT").ok_or_else(|| {
            eyre::eyre!("CUFFLINK_TENANT must name the tenant whose calls to serve")
        })?;
        let url = lookup("NATS_URL").unwrap_or_else(|| DEFAULT_URL.into());
        Self::new(tenant, url, NatsAuth::from_vars(&lookup)?)
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

/// A service subscribed to its calls, ready to [`serve`](Self::serve).
pub struct NatsServer {
    client: Client,
    tenant: Arc<str>,
    calls: SelectAll<Subscriber>,
    dispatcher: Arc<Dispatcher>,
}

impl NatsServer {
    pub(crate) async fn bind(
        service: &str,
        provider: NatsProvider,
        dispatcher: Dispatcher,
    ) -> eyre::Result<Self> {
        let client = cufflink_nats::connect(&provider.url, service, &provider.auth).await?;
        let subscribed = dispatcher.names().map(|name| {
            client.queue_subscribe(call_subject(&provider.tenant, name), QUEUE_GROUP.into())
        });
        let calls = select_all(
            futures_util::future::try_join_all(subscribed)
                .await
                .wrap_err("cannot subscribe to the calls")?,
        );
        client
            .flush()
            .await
            .wrap_err("cannot send the subscriptions to NATS")?;
        Ok(Self {
            client,
            tenant: provider.tenant.into(),
            calls,
            dispatcher: Arc::new(dispatcher),
        })
    }

    /// Answer calls until `shutdown` completes, then unsubscribe, answer the
    /// calls already delivered and write the last replies. A call NATS
    /// routes here while the unsubscribe is on its way is lost (the client
    /// forgets the subscription once the unsubscribe is sent), and its
    /// caller times out.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) -> eyre::Result<()> {
        let Self {
            client,
            tenant,
            mut calls,
            dispatcher,
        } = self;
        tracing::info!(
            %tenant,
            extensions = ?dispatcher.ready().extensions,
            "serving"
        );
        let tasks = TaskTracker::new();
        let answer_in = |message: Message| {
            tasks.spawn(answer(
                Arc::clone(&dispatcher),
                client.clone(),
                Arc::clone(&tenant),
                message,
            ));
        };
        let mut shutdown = std::pin::pin!(shutdown);
        let ended = loop {
            tokio::select! {
                () = &mut shutdown => break false,
                message = calls.next() => match message {
                    Some(message) => answer_in(message),
                    None => break true,
                },
            }
        };
        for subscriber in calls.iter_mut() {
            subscriber.drain().await.wrap_err("cannot unsubscribe")?;
        }
        let drained = tokio::time::timeout(
            STOP_TIMEOUT,
            calls.for_each(|message| {
                answer_in(message);
                std::future::ready(())
            }),
        )
        .await;
        tasks.close();
        tasks.wait().await;
        drained.wrap_err("the calls already delivered were not answered in time")?;
        tokio::time::timeout(STOP_TIMEOUT, client.flush())
            .await
            .wrap_err("the last replies were not written in time")?
            .wrap_err("cannot write the last replies")?;
        tracing::info!("stopped");
        eyre::ensure!(!ended, "the NATS subscriptions closed");
        Ok(())
    }
}

/// Answer `message` on its reply subject, unless its caller no longer
/// waits: a reply after the deadline could only race the caller's own
/// timeout.
async fn answer(dispatcher: Arc<Dispatcher>, client: Client, tenant: Arc<str>, message: Message) {
    let Some(reply_to) = message.reply else {
        tracing::warn!(subject = %message.subject, "a call without a reply subject is dropped");
        return;
    };
    let (reply, deadline) = match decode(&message.payload) {
        Ok(request) if !request.matches(&tenant, &message.subject) => {
            tracing::warn!(subject = %message.subject, "a call for another subject is refused");
            (
                ExtReply::failure(
                    CallErrorKind::Invalid.as_str(),
                    "the call does not match the subject it came on",
                ),
                None,
            )
        }
        Ok(request) => {
            let deadline = request.context.deadline();
            (dispatcher.dispatch(request, deadline).await, Some(deadline))
        }
        Err(refused) => (refused, None),
    };
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        tracing::debug!(subject = %message.subject, "the caller no longer waits; no reply");
        return;
    }
    publish(&client, reply_to, &reply).await;
}

async fn publish(client: &Client, reply_to: Subject, reply: &ExtReply) {
    let sent = match send(client, reply_to.clone(), reply).await {
        Err(e)
            if e.downcast_ref::<PublishError>()
                .is_some_and(|e| e.kind() == PublishErrorKind::MaxPayloadExceeded) =>
        {
            let too_large = CallErrorKind::TooLarge.as_str();
            send(
                client,
                reply_to,
                &ExtReply::failure(too_large, e.to_string()),
            )
            .await
        }
        sent => sent,
    };
    if let Err(e) = sent {
        tracing::warn!(error = %e, "cannot send a reply");
    }
}

async fn send(client: &Client, reply_to: Subject, reply: &ExtReply) -> eyre::Result<()> {
    Ok(client
        .publish(reply_to, serde_json::to_vec(reply)?.into())
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn a_provider_serves_the_tenant_of_its_environment() {
        let provider = NatsProvider::from_vars(vars(&[("CUFFLINK_TENANT", "harbor")])).unwrap();
        assert_eq!(provider.tenant(), "harbor");
        assert_eq!(provider.url, "nats://localhost:4222");
        assert_eq!(provider.auth, NatsAuth::Anonymous);
        let provider = NatsProvider::from_vars(vars(&[
            ("CUFFLINK_TENANT", "harbor"),
            ("NATS_URL", "nats://a:4222,nats://b:4222"),
            ("NATS_USER", "provider"),
            ("NATS_PASSWORD", "pw"),
        ]))
        .unwrap();
        assert_eq!(provider.url, "nats://a:4222,nats://b:4222");
        assert_eq!(
            provider.auth,
            NatsAuth::UserPassword {
                user: "provider".into(),
                password: "pw".into()
            }
        );
    }

    #[test]
    fn a_provider_needs_a_tenant_a_server_and_whole_credentials() {
        for (pairs, reason) in [
            (
                vec![],
                "CUFFLINK_TENANT must name the tenant whose calls to serve",
            ),
            (
                vec![("CUFFLINK_TENANT", "Harbor")],
                "'Harbor' is not a tenant slug (1 to 40 of a-z, 0-9 and '-')",
            ),
            (
                vec![("CUFFLINK_TENANT", "harbor"), ("NATS_URL", " , ")],
                "NATS_URL lists no servers",
            ),
            (
                vec![("CUFFLINK_TENANT", "harbor"), ("NATS_USER", "provider")],
                "NATS_USER and NATS_PASSWORD must be set together",
            ),
        ] {
            let refused = NatsProvider::from_vars(vars(&pairs)).unwrap_err();
            assert_eq!(refused.to_string(), reason);
        }
    }

    #[test]
    fn debug_never_prints_credentials() {
        let provider = NatsProvider::new(
            "harbor",
            "nats://user:s3cret@nats:4222",
            NatsAuth::NKeySeed("SUASECRET".into()),
        )
        .unwrap();
        let rendered = format!("{provider:?}");
        assert!(
            !rendered.contains("s3cret") && !rendered.contains("SUASECRET"),
            "{rendered}"
        );
    }
}
